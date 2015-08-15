module Sibyl

typealias Bytes Array{UInt8,1}

type BlockTransaction
    data::Dict{Bytes,Bytes}
    deleted::Set{Bytes}
end

BlockTransaction()=BlockTransaction(Dict{Bytes,Bytes}(),Set{Bytes}())

function upsert!(t::BlockTransaction,subkey::Bytes,value::Bytes)
    delete!(deleted,subkey)
    data[subkey]=value
end

type Transaction
    bucket::ASCIIString
    space::ASCIIString
    tables::Dict{ASCIIString,Dict{Bytes,BlockTransaction}}
end

Transaction(bucket,space)=Transaction(bucket,space,Dict{ASCIIString,Dict{Bytes,BlockTransaction}}())

function upsert!(t::Transaction,table::ASCIIString,key::Bytes,subkey::Bytes,value::Bytes)
    if !(haskey(t.tables,table))
        t.tables[table]=Dict{Bytes,BlockTransaction}()
    end
    if !(haskey(t.tables[table],key))
        t.tables[table][key]=BlockTransaction()
    end
    upsert!(t.tables[table][key],subkey,value)
end

end
