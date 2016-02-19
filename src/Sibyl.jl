module Sibyl

using SHA
using Zlib
import AWSCore
import AWSS3

include("base62.jl")

AWSEnv=Dict

export asbytes,frombytes
export empty

typealias Bytes Array{UInt8,1}
const empty=Bytes()

abstract SibylCache
writecache(cache::SibylCache,key::UTF8String,expiry::Int,data::Bytes)=error("writecache not implemented")
readcache(cache::SibylCache,key::UTF8String)=error("readcache not implemented")

include("nocache.jl")
include("sqlitecache.jl")

type GlobalEnvironment
    awsenv::Nullable{AWSEnv}
    s3connections::Base.Semaphore
    cache::SibylCache
end

function __init__()
    global const globalenv=GlobalEnvironment(Nullable{AWSEnv}(),Base.Semaphore(1024),SQLiteCache.Cache())
end

function getnewawsenv()
    global globalenv::GlobalEnvironment
    if haskey(ENV,"AWS_ID")
        globalenv.awsenv=Nullable{AWSEnv}(AWSCore.aws_config(creds=AWSCore.AWSCredentials(ENV["AWS_ID"],ENV["AWS_SECKEY"])))
    else
        globalenv.awsenv=Nullable{AWSEnv}(AWSCore.aws_config())
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

function s3putobject(bucket,s3key,m)
    trycount=0
    acquires3connection()
    while true
        try
            env=getawsenv()
            AWSS3.s3_put(env,bucket,s3key,m)
            releases3connection()
        catch
        end
        if trycount>0
            try
                getnewawsenv()
            catch
            end
            sleep(trycount)
        end
        trycount=trycount+1
        if trycount>15
            releases3connection()
            error("s3putobject timed out.")
        end
    end 
end

function s3getobject1(bucket,s3key)
    trycount=0
    acquires3connection()
    while true
        try
            env=getawsenv()
            r=AWSS3.s3_get(env,bucket,s3key)
            releases3connection()
            return r
        catch e
            if isa(e,AWSCore.NoSuchKey)
                releases3connection()
                return empty
            end
        end
        if trycount>0
            try
                getnewawsenv()
            catch
            end
            sleep(trycount)
        end
        trycount=trycount+1
        if trycount>15
            releases3connection()
            error("s3getobject timed out.")
        end
    end
end

function s3getobject(bucket,s3key)
    cachekey="OBJ:$(bucket):$(s3key)"
    cached=readcache(globalenv.cache,cachekey)
    if !isnull(cached)
        return get(cached)
    end
    value=s3getobject1(bucket,s3key)
    writecache(globalenv.cache,cachekey,86400*32,value)
    return value
end

function s3deleteobject(bucket,s3key)
    acquires3connection()
    try
        env=getawsenv()
        AWSS3.s3_delete(env,bucket,s3key)
    catch
    end
    releases3connection()
end

function s3listobjects1(bucket,prefix)
    trycount=0
    acquires3connection()
    while true
        try
            env=getawsenv()
            r=UTF8String[]
            while true
                q=Dict("prefix"=>prefix)
                resp=AWSS3.s3(env,"GET",bucket;query=q)
                for x in resp["Contents"]
                    push!(r,x["Key"])
                    q["marker"]=x["Key"]
                end
                if resp["IsTruncated"]!="true"
                    releases3connection()
                    return r
                end
            end
        catch
        end
        if trycount>0
            try
                getnewawsenv()
            catch
            end
            sleep(trycount)
        end
        trycount=trycount+1
        if trycount>15
            releases3connection()
            error("s3listobjects timed out.")
        end
    end
end

function s3listobjects(bucket,prefix)
    cachekey="LIST:$(bucket):$(prefix)"
    cached=readcache(globalenv.cache,cachekey)
    if !isnull(cached)
        return frombytes(get(cached),Array{UTF8String,1})[1]
    end
    value=convert(Array{UTF8String,1},s3listobjects1(bucket,prefix))
    writecache(globalenv.cache,cachekey,5*60,asbytes(value))
    return value    
end

type Connection
    bucket::UTF8String
    space::UTF8String
end


function writebytes(io,xs...)
    for x in xs
        if typeof(x)<:AbstractString
            b=IOBuffer()
            write(b,UTF8String(x))
            b=takebuf_array(b)
            write(io,Int16(length(b)))
            write(io,b)
        elseif typeof(x)==Array{UTF8String,1}
            write(io,Int16(length(x)))
            for e in x
                writebytes(io,e)
            end
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
        elseif typ==Array{UTF8String,1}
            l=read(io,Int16)
            a=Array{UTF8String,1}()
            for i=1:l
                push!(a,readbytes(io,UTF8String)[1])
            end
            push!(r,a)
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

function saveblock(blocktransaction::BlockTransaction,connection,table,key)
    s3prefix=s3keyprefix(connection.space,table,key)
    m=message(blocktransaction)
    timestamp=Base62.encode(asbytes(Int64(round(time()))))
    nonce=Base62.encode(hex2bytes(sha256(m)))
    s3key="$(s3prefix)/$(timestamp)/$(nonce)"
    s3putobject(connection.bucket,s3key,m)
end

function save(t::Transaction)
    @sync for (table,blocktransactions) in t.tables
        for (key,blocktransaction) in blocktransactions
            @async saveblock(blocktransaction,t.connection,table,key)
        end
    end
end

function readblock(connection::Connection,table::AbstractString,key::Bytes;forcecompact=false)
    objects=[(frombytes(Base62.decode(ASCIIString(split(x,"/")[5])),Int64)[1],
              split(x,"/")[6],x)
             for x in s3listobjects(connection.bucket,s3keyprefix(connection.space,table,key))]
    sort!(objects)
    results=[]
    for i=1:length(objects)
        result=Future()
        push!(results,result)
        @async put!(result,s3getobject(connection.bucket,objects[i][3]))
    end
    for i=1:length(objects)
        results[i]=fetch(results[i])
    end
    r=BlockTransaction()
    for result in results
        interpret!(r,result)
    end
    s3livekeys=UTF8String[]
    # Should only delete objects if we are reading say 10 minutes after the instructions
    @sync for x in objects
        if x[3] in r.s3keystodelete
            @async s3deleteobject(connection.bucket,x[3])
        else
            push!(s3livekeys,x[3])
        end
    end
    compactprobability=(length(s3livekeys)-1)/(length(s3livekeys)+100)
    if (length(s3livekeys)>=2)&&(forcecompact)
        compactprobability=1.0
    end
    if rand()<compactprobability
        newblock=BlockTransaction(r.data,r.deleted,s3livekeys)
        saveblock(newblock,connection,table,key)
    end
    return r
end

function loadblocks!(t::Transaction,tablekeys)
    results=[]
    for (table,key) in tablekeys
        result=Future()
        push!(results,result)
        @async put!(result,readblock(t.connection,table,key))
    end
    for i=1:length(tablekeys)
        if !haskey(t.tables,tablekeys[i][1])
            t.tables[tablekeys[i][1]]=Dict{Bytes,BlockTransaction}()
        end
        t.tables[tablekeys[i][1]][tablekeys[i][2]]=fetch(results[i])
    end
end

end

