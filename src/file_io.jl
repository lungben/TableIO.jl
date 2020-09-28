# definition of file formats and extensions

abstract type AbstractFormat end

struct CSVFormat <: AbstractFormat end
struct ZippedCSVFormat <: AbstractFormat end
struct JDFFormat <: AbstractFormat end
struct ParquetFormat <: AbstractFormat end

const FILE_EXTENSIONS = Dict(
    "zip" => ZippedCSVFormat,
    "csv" => CSVFormat,
    "jdf" => JDFFormat,
    "parquet" => ParquetFormat,
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

# CSV Format

using CSV

function read_table(::CSVFormat, filename:: AbstractString; kwargs...)
    return CSV.File(filename; kwargs...)
end

function write_table(::CSVFormat, filename:: AbstractString, table; kwargs...)
    _checktable(table)
    table |> CSV.write(filename; kwargs...)
    return filename
end

# Zipped CSV Format

using ZipFile, CSV


"""
This method assumes that there is a single csv file inside the zip file. If this is not the case, an error is raised.
"""
function read_table(::ZippedCSVFormat, zip_filename:: AbstractString; kwargs...)
    zf = ZipFile.Reader(zip_filename)
    length(zf.files) == 1 || error("The zip file must contain exactly one file")
    _get_file_extension(zf.files[1].name) == "csv" || error("the zip file must contain a file with `csv` extension")
    return CSV.File(read(zf.files[1]); kwargs...)
end

"""
This method supports multiple files inside the zip file. The name of the csv file inside the zip file must be given.
"""
function read_table(::ZippedCSVFormat, zip_filename:: AbstractString, csv_filename:: AbstractString; kwargs...)
    zf = ZipFile.Reader(zip_filename)
    file_in_zip = filter(x->x.name == csv_filename, zf.files)[1]
    return CSV.File(read(file_in_zip); kwargs...)
end

"""
The csv file inside the zip archive is named analogue to the zip file, but with `.csv` extension.
"""
function write_table(::ZippedCSVFormat, zip_filename:: AbstractString, table; kwargs...)
    _checktable(table)
    csv_filename = string(splitext(basename(zip_filename))[1], ".csv")
    zf = ZipFile.Writer(zip_filename)
    file = ZipFile.addfile(zf, csv_filename, method=ZipFile.Deflate)
    table |> CSV.write(file; kwargs...)
    close(zf)
    return zip_filename
end

# JDF

using JDF, DataFrames

function read_table(::JDFFormat, filename:: AbstractString; kwargs...):: DataFrame
    return loadjdf(filename; kwargs...)
end

function write_table(::JDFFormat, filename:: AbstractString, table:: DataFrame; kwargs...)
    # JDF supports only DataFrames, not arbitrary Tables.jl inputs
    savejdf(filename, table; kwargs...)
    return filename
end

write_table(::JDFFormat, filename:: AbstractString, table; kwargs...) = write_table(JDFFormat(), filename, DataFrame(table); kwargs...)

# Parquet

using Parquet

function read_table(::ParquetFormat, filename:: AbstractString; string_cols = [], kwargs...)
    if length(string_cols) > 0
        str_mapping = Dict(string.(string_cols) => (String, Parquet.logical_string))
        parfile = ParFile(filename; map_logical_types=str_mapping, kwargs...)
    else
        parfile = ParFile(filename; kwargs...)
    end
    return RecordCursor(parfile)
end

function write_table(::ParquetFormat, filename:: AbstractString, table; kwargs...)
    _checktable(table)
    write_parquet(filename, table; kwargs...)
    return filename
end
