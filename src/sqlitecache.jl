module SQLiteCache

using SQLite

import ..SibylCache
import ..Bytes
import ..writecache
import ..readcache

type Cache <: SibylCache
    db::SQLite.DB
    lock::Base.Semaphore
end

function Cache()
    home=ENV["HOME"]
    db=SQLite.DB("$(home)/.sibyl.sqlite")
    try
        SQLite.execute!(db,"""CREATE TABLE IF NOT EXISTS Cache (
                        ky TEXT PRIMARY KEY,
                        expires INTEGER,
                        value BLOB)""")
        SQLite.execute!(db,"""CREATE INDEX IF NOT EXISTS Cache_index ON Cache (ky)""")
        SQLite.execute!(db,"""VACUUM""")
    catch
    end
    lock=Base.Semaphore(1)
    return Cache(db,lock)

end

function writecache(cache::Cache,key::UTF8String,expiry::Int,data::Bytes)
    Base.acquire(cache.lock)
    try
        st=SQLite.Stmt(cache.db,"INSERT OR REPLACE INTO Cache (ky,expires,value) VALUES (?,?,?)")
        SQLite.bind!(st,1,key)
        SQLite.bind!(st,2,Int(round(time()))+expiry)
        SQLite.bind!(st,3,data)
        SQLite.execute!(st)
    catch
    end
    Base.release(cache.lock)
end

function readcache(cache::Cache,key::UTF8String)
    Base.acquire(cache.lock)
    try
        results=SQLite.query(cache.db,"SELECT expires,value FROM Cache where ky=?",[key])
    catch
        Base.release(cache.lock)
        return Nullable{Bytes}()
    end
    Base.release(cache.lock)
#    println(key)
    if size(results,1)==0 # If nothing was found
#        println("Cache Miss")
        return Nullable{Bytes}()
    end
    if get(results[1,1])<time() # If what was found is expired
#        println("Cache Miss Expired")
        return Nullable{Bytes}()
    end
#    println("Cache Hit")
    return Nullable{Bytes}(get(results[1,2]))
end


end
