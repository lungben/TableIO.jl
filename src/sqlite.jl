#SQLite

@info "SQLite.jl is available - including functionality to read / write SQLite databases"

using .SQLite

function read_table(::SQLiteFormat, filename:: AbstractString, tablename:: AbstractString; kwargs...)
    db = SQLite.DB(filename)
    return read_table(db, tablename; kwargs...)
end

"""
    read_table(db:: SQLite.DB, tablename:: AbstractString; kwargs...)

Reads the content of a whole table and returns a Tables.jl compatible object.
This method takes an instance of an SQLite database connection as input.
"""
function read_table(db:: SQLite.DB, tablename:: AbstractString; kwargs...)
     _checktablename(tablename)
    return DBInterface.execute(db, "select * from $tablename") # SQL parameters cannot be used for table names
end

read_sql(db:: SQLite.DB, sql:: AbstractString) = DBInterface.execute(db, sql)

function write_table!(::SQLiteFormat, filename:: AbstractString, tablename:: AbstractString, table; kwargs...)
    _checktable(table)
    db = SQLite.DB(filename)
    write_table!(db, tablename, table; kwargs...)
end

"""
    write_table(db:: SQLite.DB, tablename:: AbstractString, table; kwargs...)

Writes the data into a new table in the SQLite database. 
As default, this operation will fail if the table already exists. Different behavior can be specified using
the `kwargs` passed to `SQLite.load!`.

Note that this behavior is different to the one for PostgreSQL, where the table must already exist.
"""
function write_table!(db:: SQLite.DB, tablename:: AbstractString, table; kwargs...)
    _checktable(table)
    table |> SQLite.load!(db, tablename; kwargs...)
    nothing
end

