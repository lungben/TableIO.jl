module TableIO

export read_table, write_table!, read_sql, list_tables

using TableIOInterface
using Tables, Requires
using DataFrames # required for multiple file types, therefore currently not optional

# specify if a reader accepts an io buffer as input or if creation of a temp file is required
supports_io_input(::TableIOInterface.AbstractFormat) = false

supports_io_input(::TableIOInterface.CSVFormat) = true
supports_io_input(::TableIOInterface.JSONFormat) = true
supports_io_input(::TableIOInterface.ArrowFormat) = true

# definition of the required packages for the specific formats
# if a format requires multiple packages, define them as a list
const PACKAGE_REQUIREMENTS = Dict{DataType, Union{Symbol, Vector{Symbol}}}(
    TableIOInterface.CSVFormat => :CSV,
    TableIOInterface.ZippedFormat => :ZipFile,
    TableIOInterface.JDFFormat => :JDF,
    TableIOInterface.ParquetFormat => :Parquet,
    TableIOInterface.ExcelFormat => :XLSX,
    TableIOInterface.SQLiteFormat => :SQLite,
    TableIOInterface.StataFormat => :StatFiles,
    TableIOInterface.SPSSFormat => :StatFiles,
    TableIOInterface.SASFormat => :StatFiles,
    TableIOInterface.JSONFormat => :JSONTables,
    TableIOInterface.ArrowFormat => :Arrow,
    TableIOInterface.PostgresFormat => [:LibPQ, :CSV],
    TableIOInterface.HDF5Format => :Pandas,
    TableIOInterface.JLD2Format => :JLD2,
)

## Dispatching on file extensions

"""
    read_table(filename:: AbstractString; kwargs...)

`filename`: path and filename of the input file
`kwargs...`: keyword arguments passed to the underlying file reading function (e.g. `CSV.File`)

Returns a Tables.jl interface compatible object.

Example:

    df = DataFrame(read_table("my_data.csv"); copycols=false)


"""
function read_table(filename:: AbstractString, args...; kwargs...)
    data_type = get_file_type(filename)
    return read_table(data_type, filename, args...; kwargs...)
end

"""
    read_table(file_picker:: Dict, args...; kwargs...)

Reading tabular data from a PlutoUI.jl FilePicker.

Usage (in a Pluto.jl notebook):

    using PlutoUI, TableIO, DataFrames
    using XLSX # import the packages required for the uploaded file formats
    @bind f PlutoUI.FilePicker()
    df = DataFrame(read_table(f); copycols=false)

"""
function read_table(file_picker:: Dict, args...; kwargs...)
    filename, data = _get_file_picker_data(file_picker)
    data_type = get_file_type(filename)
    data_buffer = IOBuffer(data)

    if supports_io_input(data_type)
        data_object = data_buffer # if it is supported by the corresponding package, creation of a temporary file is avoided and the IOBuffer is used directly
    else
        tmp_file = joinpath(mktempdir(), filename)
        write(tmp_file, data_buffer)
        data_object = tmp_file
    end  

    return read_table(data_type, data_object, args...; kwargs...)
end

"""
    list_tables(filename:: AbstractString)

Returns a list of all tables inside a file.
"""
function list_tables(filename:: AbstractString)
    data_type = get_file_type(filename)
    TableIOInterface.multiple_tables(data_type) || error("The data type $data_type does not support multiple tables per file.")
    return list_tables(data_type, filename)
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
    data_type = get_file_type(filename)
    write_table!(data_type, filename, table, args...; kwargs...)
    nothing
end

"""
    read_sql(db, sql:: AbstractString)

Returns the result of the SQL query as a Tables.jl compatible object.
"""
function read_sql end

include("julia.jl")

## conditional dependencies

function __init__()
    @require CSV = "336ed68f-0bac-5ca0-87d4-7b16caf5d00b" begin
        include("csv.jl")
        @require LibPQ = "194296ae-ab2e-5f79-8cd4-7183a0a5a0d1" include("postgresql.jl")
    end
    @require ZipFile = "a5390f91-8eb1-5f08-bee0-b1d1ffed6cea" include("zip.jl")
    @require JDF = "babc3d20-cd49-4f60-a736-a8f9c08892d3" include("jdf.jl")
    @require Parquet = "626c502c-15b0-58ad-a749-f091afb673ae" include("parquet.jl")
    @require XLSX = "fdbf4ff8-1666-58a4-91e7-1b58723a45e0" include("xlsx.jl")
    @require StatFiles = "1463e38c-9381-5320-bcd4-4134955f093a" include("stat_files.jl")
    @require SQLite = "0aa819cd-b072-5ff4-a722-6bc24af294d9" include("sqlite.jl")
    @require JSONTables = "b9914132-a727-11e9-1322-f18e41205b0b" include("json.jl")
    @require Arrow = "69666777-d1a9-59fb-9406-91d4454c9d45" include("arrow.jl")
    @require Pandas = "eadc2687-ae89-51f9-a5d9-86b5a6373a9c" include("pandas.jl")
    @require JLD2 = "033835bb-8acc-5ee8-8aae-3f567f8a3819" include("jld2.jl")
end

## Utilities

get_package_requirements(::T) where {T <: TableIOInterface.AbstractFormat} = PACKAGE_REQUIREMENTS[T]
get_package_requirements(filename:: AbstractString) = get_package_requirements(get_file_type(filename))

_checktable(table) = Tables.istable(typeof(table)) || error("table has no Tables.jl compatible interface")

# poor man's approach to prevent SQL injections / garbage inputs
_checktablename(tablename) = match(r"^[a-zA-Z0-9_.]*$", tablename) === nothing && error("tablename must only contain alphanumeric characters and underscores")

function _get_file_picker_data(file_picker:: Dict)
    data = file_picker["data"]:: Vector{UInt8} # brings back type stability
    length(data) == 0 && error("no file selected yet")
    filename = file_picker["name"]:: String
    return filename, data
end

end
