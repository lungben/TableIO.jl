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
# see https://juliadata.github.io/CSV.jl/stable/#Reading-CSV-from-gzip-(.gz)-and-zip-files-1

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
    savejdf(filename, table; kwargs...)
    return filename
end

# JDF supports only DataFrames, not arbitrary Tables.jl inputs. For export, the table is converted to a DataFrame first.
write_table(::JDFFormat, filename:: AbstractString, table; kwargs...) = write_table(JDFFormat(), filename, DataFrame(table); kwargs...)

# Parquet

using Parquet

"""
For strings (plus potentially additional data types), the binary representation in Parquet must be specified when reading the file.
A general mapping can be given with the keyword argument `map_logical_types`.
Alternatively, the string columns can be given 

"""
function read_table(::ParquetFormat, filename:: AbstractString; string_cols = [], kwargs...)
    
    if length(string_cols) > 0
        str_mapping = Dict(string.(string_cols) => (String, Parquet.logical_string))
    else
        str_mapping = nothing
    end

    if hasproperty(kwargs, :map_logical_types)
        map_logical_types = kwargs.map_logical_types
        if str_mapping === nothing
            map_logical_types = kwargs.map_logical_types
        else
            map_logical_types = kwargs.map_logical_types ∪ str_mapping
        end
    else
        map_logical_types = str_mapping
    end
    
    parfile = ParFile(filename; map_logical_types=map_logical_types, kwargs...)
    return RecordCursor(parfile)
end

function write_table(::ParquetFormat, filename:: AbstractString, table; kwargs...)
    _checktable(table)
    write_parquet(filename, table; kwargs...)
    return filename
end

# Excel

using XLSX, DataFrames

function read_table(::ExcelFormat, filename:: AbstractString, sheetname:: AbstractString; kwargs...)
    f = XLSX.readxlsx(filename)
    sheet = f[sheetname]
    return XLSX.eachtablerow(sheet)
end

function read_table(::ExcelFormat, filename:: AbstractString; kwargs...)
    f = XLSX.readxlsx(filename)
    sheet = first(f.workbook.sheets)
    return XLSX.eachtablerow(sheet) |> DataFrame! # this would be no valid Table.jl output if not converted to DataFrame
end   


function write_table(::ExcelFormat, filename:: AbstractString, sheetname:: AbstractString, table:: DataFrame; kwargs...)
    _checktable(table)
    XLSX.writetable(filename, table; overwrite=true, sheetname=sheetname, kwargs...)
    return filename
end

const DEFAULT_SHEETNAME = "sheet_1"
write_table(::ExcelFormat, filename:: AbstractString, table; kwargs...) = write_table(ExcelFormat(), filename, DEFAULT_SHEETNAME, table; kwargs...)

# XLSX supports only DataFrames, not arbitrary Tables.jl inputs. For export, the table is converted to a DataFrame first.
write_table(::ExcelFormat, filename:: AbstractString, sheetname:: AbstractString, table; kwargs...) = write_table(ExcelFormat(), filename:: AbstractString, sheetname:: AbstractString, DataFrame(table); kwargs...)
