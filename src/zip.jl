## Zipped CSV Format
# see https://juliadata.github.io/CSV.jl/stable/#Reading-CSV-from-gzip-(.gz)-and-zip-files-1

@info "ZipFile.jl is available - including functionality to read / write zipped files"

using .ZipFile
using CSV

"""
This method assumes that there is a single data file inside the zip file. If this is not the case, an error is raised.
"""
function read_table(::TableIOInterface.ZippedFormat, zip_filename:: AbstractString; kwargs...)
    zf = ZipFile.Reader(zip_filename)
    try
        length(zf.files) == 1 || error("The zip file must contain exactly one file")
        file_in_zip = zf.files[1]
        output = read_table(file_in_zip; kwargs...)
        return output
    finally
        close(zf)
    end
end

"""
This method supports multiple files inside the zip file. The name of the file inside the zip file must be given.
"""
function read_table(::TableIOInterface.ZippedFormat, zip_filename:: AbstractString, csv_filename:: AbstractString; kwargs...)
    zf = ZipFile.Reader(zip_filename)
    try
        file_in_zip = filter(x->x.name == csv_filename, zf.files)[1]
        output = read_table(file_in_zip; kwargs...)
        return output
    finally
        close(zf)
    end
end

function read_table(file_in_zip:: ZipFile.ReadableFile; kwargs...)
    file_type = get_file_type(file_in_zip.name)
    return read_table(file_type, file_in_zip; kwargs...)
end


"""
The csv file inside the zip archive is named analogue to the zip file, but with `.csv` extension.
"""
function write_table!(::TableIOInterface.ZippedFormat, zip_filename:: AbstractString, table; kwargs...)
    _checktable(table)
    csv_filename = string(splitext(basename(zip_filename))[1], ".csv")
    write_table!(TableIOInterface.ZippedFormat(), zip_filename, csv_filename, table; kwargs...)
    nothing
end

"""
Writing as arbitrary file name and file format in a zip file.
"""
function write_table!(::TableIOInterface.ZippedFormat, zip_filename:: AbstractString, filename_in_zip:: AbstractString, table; kwargs...)
    _checktable(table)
    zf = ZipFile.Writer(zip_filename)
    try
        file_in_zip = ZipFile.addfile(zf, filename_in_zip, method=ZipFile.Deflate)
        file_type_in_zip = get_file_type(filename_in_zip)
        write_table!(file_type_in_zip, file_in_zip, table; kwargs...)
    finally
        close(zf)
    end
    nothing
end

function list_tables(::TableIOInterface.ZippedFormat, filename:: AbstractString)
    zf = ZipFile.Reader(filename)
    try
        files = [f.name for f in zf.files]
        return files |> sort
    finally
        close(zf)
    end
end
