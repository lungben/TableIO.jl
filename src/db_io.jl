
# poor man's approach to prevent SQL injections
_checktablename(tablename) = match(r"^[a-zA-Z0-9_]*$", tablename) === nothing && error("tablename must only contain alphanumeric characters and underscores")

#SQLite

using SQLite

function read_table(::SQLiteFormat, filename:: AbstractString, tablename:: AbstractString; kwargs...)
    db = SQLite.DB(filename)
    return read_table(db, tablename; kwargs...)
end

function read_table(db:: SQLite.DB, tablename:: AbstractString; kwargs...)
     _checktablename(tablename)
    return DBInterface.execute(db, "select * from $tablename") # SQL parameters cannot be used for table names
end

function write_table(::SQLiteFormat, filename:: AbstractString, tablename:: AbstractString, table; kwargs...)
    _checktable(table)
    db = SQLite.DB(filename)
    return write_table(db, tablename, table; kwargs...)
end


function write_table(db:: SQLite.DB, tablename:: AbstractString, table; kwargs...)
    _checktable(table)
    table |> SQLite.load!(db, tablename; kwargs...)
    return tablename
end


# PostgreSQL
using LibPQ

