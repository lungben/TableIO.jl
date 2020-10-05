module TableIO

export read_table, write_table!, read_sql

using Tables, Requires
using CSV, DataFrames # required for multiple file types, therefore currently not optional

## definition of file formats and extensions

abstract type AbstractFormat end

struct CSVFormat <: AbstractFormat end
struct ZippedFormat <: AbstractFormat end
struct JDFFormat <: AbstractFormat end
struct ParquetFormat <: AbstractFormat end
struct ExcelFormat <: AbstractFormat end
struct SQLiteFormat <: AbstractFormat end
struct StataFormat <: AbstractFormat end
struct SPSSFormat <: AbstractFormat end
struct SASFormat <: AbstractFormat end
struct JSONFormat <: AbstractFormat end
struct ArrowFormat <: AbstractFormat end

const FILE_EXTENSIONS = Dict(
    "zip" => ZippedFormat,
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
    "json" => JSONFormat,
    "arrow" => ArrowFormat,
)

## Dispatching on file extensions

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
    write_table!(filename:: AbstractString, table; kwargs...):: AbstractString

`filename`: path and filename of the output file
`table`: a Tables.jl compatible object (e.g. a DataFrame) for storage
`kwargs...`: keyword arguments passed to the underlying file writing function (e.g. `CSV.write`)

Example:

    write_table!("my_output.csv", df)

"""
function write_table!(filename:: AbstractString, table, args...; kwargs...)
    data_type = _get_file_type(filename)()
    write_table!(data_type, filename, table, args...; kwargs...)
    nothing
end


"""
    read_sql(db, sql:: AbstractString)

Returns the result of the SQL query as a Tables.jl compatible object.
"""
function read_sql end

## CSV - always supported because CSV.jl is required for multiple other file formats, too

read_table(::CSVFormat, filename:: AbstractString; kwargs...) = CSV.File(filename; kwargs...)
read_table(::CSVFormat, io:: IO; kwargs...) = CSV.File(read(io); kwargs...)

function write_table!(::CSVFormat, output:: Union{AbstractString, IO}, table; kwargs...)
    _checktable(table)
    table |> CSV.write(output; kwargs...)
    nothing
end


## conditional dependencies

function __init__()
    @require ZipFile = "a5390f91-8eb1-5f08-bee0-b1d1ffed6cea" include("zip.jl")
    @require JDF = "babc3d20-cd49-4f60-a736-a8f9c08892d3" include("jdf.jl")
    @require Parquet = "626c502c-15b0-58ad-a749-f091afb673ae" include("parquet.jl")
    @require XLSX = "fdbf4ff8-1666-58a4-91e7-1b58723a45e0" include("xlsx.jl")
    @require StatFiles = "1463e38c-9381-5320-bcd4-4134955f093a" include("stat_files.jl")
    @require SQLite = "0aa819cd-b072-5ff4-a722-6bc24af294d9" include("sqlite.jl")
    @require LibPQ = "194296ae-ab2e-5f79-8cd4-7183a0a5a0d1" include("postgresql.jl")
    @require JSONTables = "b9914132-a727-11e9-1322-f18e41205b0b" include("json.jl")
    @require Arrow = "69666777-d1a9-59fb-9406-91d4454c9d45" include("arrow.jl")
end


## Utilities

_get_file_extension(filename) = lowercase(splitext(filename)[2][2:end])
_get_file_type(filename) = FILE_EXTENSIONS[_get_file_extension(filename)]

_checktable(table) = Tables.istable(typeof(table)) || error("table has no Tables.jl compatible interface")

# poor man's approach to prevent SQL injections / garbage inputs
_checktablename(tablename) = match(r"^[a-zA-Z0-9_]*$", tablename) === nothing && error("tablename must only contain alphanumeric characters and underscores")


end
