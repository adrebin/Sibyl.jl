module Sibyl

using SHA
using Zlib
using AWS
using AWS.S3

include("base62.jl")

export asbytes,frombytes
export empty

typealias Bytes Array{UInt8,1}
const empty=Bytes()

type GlobalEnvironment
    awsenv::Nullable{AWSEnv}
    s3connections::Base.Semaphore
end

function __init__()
    global const globalenv=GlobalEnvironment(Nullable{AWSEnv}(),Base.Semaphore(128))
end

function getnewawsenv()
    global globalenv::GlobalEnvironment
    if haskey(ENV,"AWS_ID")
        globalenv.awsenv=Nullable{AWSEnv}(AWSEnv(id=ENV["AWS_ID"],key=ENV["AWS_SECKEY"]))
    else
        globalenv.awsenv=Nullable{AWSEnv}(AWSEnv(ec2_creds=true))
    end
    return get(globalenv.awsenv)
end

function getawsenv()
    global globalenv::GlobalEnvironment
    if isnull(globalenv.awsenv)
        return getnewawsenv()
    end
    return get(globalenv.awsenv)
end

function acquires3connection()
    global globalenv::GlobalEnvironment
    Base.acquire(globalenv.s3connections)
end

function releases3connection()
    global globalenv::GlobalEnvironment
    Base.release(globalenv.s3connections)
end

type Connection
    bucket::UTF8String
    space::UTF8String
end

function Connection(bucket,space)
    Connection(UTF8String(bucket),UTF8String(space))
end

function writebytes(io,xs...)
    for x in xs
        if typeof(x)<:AbstractString
            b=IOBuffer()
            write(b,UTF8String(x))
            b=takebuf_array(b)
            write(io,Int16(length(b)))
            write(io,b)
        elseif typeof(x)<:Array
            b=reinterpret(UInt8,x)
            write(io,Int64(length(b)))
            write(io,b)
        else
            write(io,x)
        end
    end
end

frombytesarray{T}(data::Bytes,typ::Type{Array{T,1}})=reinterpret(T,data)

function readbytes(io,typs...)
    r=[]
    for typ in typs
        if typ==UTF8String
            l=read(io,Int16)
            b=read(io,UInt8,l)
            push!(r,UTF8String(b))
        elseif typ<:Array
            l=read(io,Int64)
            b=read(io,UInt8,l)
            push!(r,frombytesarray(b,typ))
        else
            push!(r,read(io,typ))
        end
    end
    return r
end

function asbytes(xs...)
    io=IOBuffer()
    writebytes(io,xs...)
    return takebuf_array(io)
end

function frombytes(data,typs...)
    io=IOBuffer(data)
    return readbytes(io,typs...)
end
    
type BlockTransaction
    data::Dict{Bytes,Bytes}
    deleted::Set{Bytes}
    s3keystodelete::Array{UTF8String,1}
end

BlockTransaction()=BlockTransaction(Dict{Bytes,Bytes}(),Set{Bytes}(),Array{UTF8String,1}())

function upsert!(t::BlockTransaction,subkey::Bytes,value::Bytes)
    delete!(t.deleted,subkey)
    t.data[subkey]=value
end

function message(t::BlockTransaction)
    io=IOBuffer()
    writebytes(io,Int64(length(keys(t.data))))
    for (k,v) in t.data
        writebytes(io,k,v)
    end
    writebytes(io,Int64(length(t.deleted)))
    for k in t.deleted
        writebytes(io,k)
    end
    writebytes(io,Int64(length(t.s3keystodelete)))
    for s in t.s3keystodelete
        writebytes(io,s)
    end
    r=takebuf_array(io)
    return Zlib.compress(r,9)
end

function interpret!(t::BlockTransaction,message::Bytes)
    if length(message)==0
        return
    end
    io=IOBuffer(Zlib.decompress(message))
    n=readbytes(io,Int64)[1]
    for i=1:n
        x=readbytes(io,Bytes,Bytes)
        t.data[x[1]]=x[2]
    end
    n=readbytes(io,Int64)[1]
    for i=1:n
        delete!(t.data,readbytes(io,Bytes))
    end
    n=readbytes(io,Int64)[1]
    for i=1:n
        push!(t.s3keystodelete,readbytes(io,UTF8String)[1])
    end
end

type Transaction
    connection::Connection
    tables::Dict{UTF8String,Dict{Bytes,BlockTransaction}}
end

Transaction(connection)=Transaction(connection,Dict{UTF8String,Dict{Bytes,BlockTransaction}}())

function upsert!(t::Transaction,table::AbstractString,key::Bytes,subkey::Bytes,value::Bytes)
    if !(haskey(t.tables,table))
        t.tables[table]=Dict{Bytes,BlockTransaction}()
    end
    if !(haskey(t.tables[table],key))
        t.tables[table][key]=BlockTransaction()
    end
    upsert!(t.tables[table][key],subkey,value)
end

function s3keyprefix(space,table,key)
    hash=sha256(key)[1:4]
    return "$(space)/$(table)/$(hash)/$(Base62.encode(key))"
end

function save(t::Transaction)
    @sync for (table,blocktransactions) in t.tables
        for (key,blocktransaction) in blocktransactions
            s3prefix=s3keyprefix(t.connection.space,table,key)
            m=message(blocktransaction)
            timestamp=Base62.encode(asbytes(Int64(round(time()))))
            nonce=Base62.encode(hex2bytes(sha256(m)))
            s3key="$(s3prefix)/$(timestamp)/$(nonce)"
            @async begin
                acquires3connection()
                env=getawsenv()
                S3.put_object(env,t.connection.bucket,s3key,ASCIIString(m))
                releases3connection()
            end
        end
    end
end

function s3listobjects(connection,prefix)
    acquires3connection()
    env=getawsenv()
    list=S3.get_bkt(env,connection.bucket,options=GetBucketOptions(prefix=prefix)).obj.contents
    releases3connection()
    r=[x.key for x in list]
    return r
end

function s3getobject(connection,s3key)
    acquires3connection()
    env=getawsenv()
    resp=S3.get_object(env,connection.bucket,s3key)
    releases3connection()
    if resp.http_code==200
        return takebuf_array(resp.obj)
    end
    return empty
end

function readblock(connection::Connection,table::AbstractString,key::Bytes)
    objects=[(frombytes(Base62.decode(ASCIIString(split(x,"/")[5])),Int64)[1],
              split(x,"/")[6],x)
             for x in s3listobjects(connection,s3keyprefix(connection.space,table,key))]
    sort!(objects)
    results=[]
    fetchindicies=Int[]
    for i=1:length(objects)
        push!(fetchindicies,i)
        r=Future()
        @async put!(r,s3getobject(connection,objects[i][3]))
        push!(results,r)
    end
    for i in fetchindicies
        results[i]=fetch(results[i])
    end
    r=BlockTransaction()
    for result in results
        interpret!(r,result)
    end
    return r
end

function loadblocks!(t::Transaction,tablekeys)
    results=[]
    for (table,key) in tablekeys
        r=Future()
        push!(results,r)
        @async put!(r,readblock(t.connection,table,key))
    end
    for i=1:length(tablekeys)
        if !haskey(t.tables,tablekeys[i][1])
            t.tables[tablekeys[i][1]]=Dict{Bytes,BlockTransaction}()
        end
        t.tables[tablekeys[i][1]][tablekeys[i][2]]=fetch(results[i])
    end
end

end

