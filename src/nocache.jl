module NoCache

import ..SibylCache
import ..Bytes
import ..writecache
import ..readcache

type Cache <: SibylCache
end

function writecache(cache::Cache,key::String,expiry::Int,data::Bytes)
#    println("Write to NoCache $(key)")
end

function readcache(cache::Cache,key::String)
#    println("Read from NoCache $(key)")
    return Nullable{Bytes}()
end

end
