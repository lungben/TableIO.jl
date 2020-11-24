# PostgreSQL

@info "LibPQ.jl is available - including functionality to read / write PostgreSQL databases"

using .LibPQ
using .CSV

"""
    read_table(conn:: LibPQ.Connection, tablename:: AbstractString)

Reads the content of a whole table and returns a Tables.jl compatible object.
"""
function read_table(conn:: LibPQ.Connection, tablename:: AbstractString; kwargs...)
    _checktablename(tablename)
   return execute(conn, "select * from $tablename") # SQL parameters cannot be used for table names
end

read_sql(conn:: LibPQ.Connection, sql:: AbstractString) = execute(conn, sql)

"""
    write_table(conn:: LibPQ.Connection, tablename:: AbstractString, table)

Writes data into an existing PostgreSQL table.
The table columns must have the same names as in the input table and the types must be compliant. It is OK to have more types in the PostgreSQL table
than in the input table if these columns are nullable.

Note that this method does not create a non-existing table (in contrast to the corresponding SQLite method). This is a design decision because PostgreSQL databases are usually more persistant 
than (often "throw-away") SQLite databases.

This method is using `COPY FROM STDIN` on CSV data, which is much faster than uploading using SQL statements.
"""
function write_table!(conn:: LibPQ.Connection, tablename:: AbstractString, table; kwargs...)
    # Uploading data to P
    _checktable(table)
    _checktablename(tablename)
    iter = CSV.RowWriter(table)
    column_names = first(iter)
    copyin = LibPQ.CopyIn("COPY $tablename ($column_names) FROM STDIN (FORMAT CSV, HEADER);", iter)
    execute(conn, copyin)
    nothing
end
