
# poor man's approach to prevent SQL injections
_checktablename(tablename) = match(r"^[a-zA-Z0-9_]*$", tablename) === nothing && error("tablename must only contain alphanumeric characters and underscores")

#SQLite

using SQLite

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

function write_table(::SQLiteFormat, filename:: AbstractString, tablename:: AbstractString, table; kwargs...)
    _checktable(table)
    db = SQLite.DB(filename)
    return write_table(db, tablename, table; kwargs...)
end

"""
    write_table(db:: SQLite.DB, tablename:: AbstractString, table; kwargs...)

Writes the data into a new table in the SQLite database. 
As default, this operation will fail if the table already exists. Different behavior can be specified using
the `kwargs` passed to `SQLite.load!`.

Note that this behavior is different to the one for PostgreSQL, where the table must already exist.
"""
function write_table(db:: SQLite.DB, tablename:: AbstractString, table; kwargs...)
    _checktable(table)
    table |> SQLite.load!(db, tablename; kwargs...)
    return tablename
end


# PostgreSQL
using LibPQ, CSV

"""
    read_table(conn:: LibPQ.Connection, tablename:: AbstractString)

Reads the content of a whole table and returns a Tables.jl compatible object.
"""
function read_table(conn:: LibPQ.Connection, tablename:: AbstractString; kwargs...)
    _checktablename(tablename)
   return execute(conn, "select * from $tablename") # SQL parameters cannot be used for table names
end

"""
    write_table(conn:: LibPQ.Connection, tablename:: AbstractString, table)

Writes data into an existing PostgreSQL table.
The table columns must have the same names as in the input table and the types must be compliant. It is OK to have more types in the PostgreSQL table
than in the input table if these columns are nullable.

Note that this method does not create a non-existing table (in contrast to the corresponding SQLite method). This is a design decision because PostgreSQL databases are usually more persistant 
than (often "throw-away") SQLite databases.

This method is using `COPY FROM STDIN` on CSV data, which is much faster than uploading using SQL statements.
"""
function write_table(conn:: LibPQ.Connection, tablename:: AbstractString, table; kwargs...)
    # Uploading data to P
    _checktable(table)
    _checktablename(tablename)
    iter = CSV.RowWriter(table)
    column_names = first(iter)
    copyin = LibPQ.CopyIn("COPY $tablename ($column_names) FROM STDIN (FORMAT CSV, HEADER);", iter)
    execute(conn, copyin)
    return tablename
end
