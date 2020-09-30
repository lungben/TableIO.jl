module TableIO

export read_table, write_table, read_sql

using Tables

# definition of file formats and extensions

abstract type AbstractFormat end

struct CSVFormat <: AbstractFormat end
struct ZippedCSVFormat <: AbstractFormat end
struct JDFFormat <: AbstractFormat end
struct ParquetFormat <: AbstractFormat end
struct ExcelFormat <: AbstractFormat end
struct SQLiteFormat <: AbstractFormat end
struct StataFormat <: AbstractFormat end
struct SPSSFormat <: AbstractFormat end
struct SASFormat <: AbstractFormat end

const FILE_EXTENSIONS = Dict(
    "zip" => ZippedCSVFormat,
    "csv" => CSVFormat,
    "jdf" => JDFFormat,
    "parquet" => ParquetFormat,
    "xlsx" => ExcelFormat,
    "db" => SQLiteFormat,
    "sqlite" => SQLiteFormat,
    "sqlite3" => SQLiteFormat,
    "dta" => StataFormat,
    "sav" => SPSSFormat,
    "sas7bdat" => SASFormat,
)

_get_file_extension(filename) = lowercase(splitext(filename)[2][2:end])
_get_file_type(filename) = FILE_EXTENSIONS[_get_file_extension(filename)]

# dispatching on file extensions

"""
    read_table(filename:: AbstractString; kwargs...)

`filename`: path and filename of the input file
`kwargs...`: keyword arguments passed to the underlying file reading function (e.g. `CSV.File`)

Returns a Tables.jl interface compatible object.

Example:

    df = read_table("my_data.csv") |> DataFrame!


"""
function read_table(filename:: AbstractString, args...; kwargs...)
    data_type = _get_file_type(filename)()
    read_table(data_type, filename, args...; kwargs...)
end


"""
    write_table(filename:: AbstractString, table; kwargs...):: AbstractString

`filename`: path and filename of the output file
`table`: a Tables.jl compatible object (e.g. a DataFrame) for storage
`kwargs...`: keyword arguments passed to the underlying file writing function (e.g. `CSV.write`)

Returns `filename`.

Example:

    write_table("my_output.csv", df)

"""
function write_table(filename:: AbstractString, table, args...; kwargs...):: AbstractString
    data_type = _get_file_type(filename)()
    write_table(data_type, filename, table, args...; kwargs...)
end


"""
    read_sql(db, sql:: AbstractString)

Returns the result of the SQL query as a Tables.jl compatible object.
"""
function read_sql end

_checktable(table) = Tables.istable(typeof(table)) || error("table has no Tables.jl compatible interface")

include("file_io.jl")
include("db_io.jl")

end
