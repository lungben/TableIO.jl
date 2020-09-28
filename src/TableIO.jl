module TableIO

export read_table, write_table

using Tables

_checktable(table) = Tables.istable(typeof(table)) || error("table has no Tables.jl compatible interface")

include("file_io.jl")
include("db_io.jl")

end
