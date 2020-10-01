## Zipped CSV Format
# see https://juliadata.github.io/CSV.jl/stable/#Reading-CSV-from-gzip-(.gz)-and-zip-files-1

@info "ZipFile.jl is available - including functionality to read / write zipped csv files"

using .ZipFile
using CSV

"""
This method assumes that there is a single csv file inside the zip file. If this is not the case, an error is raised.
"""
function read_table(::ZippedCSVFormat, zip_filename:: AbstractString; kwargs...)
    zf = ZipFile.Reader(zip_filename)
    length(zf.files) == 1 || error("The zip file must contain exactly one file")
    _get_file_extension(zf.files[1].name) == "csv" || error("the zip file must contain a file with `csv` extension")
    output = CSV.File(read(zf.files[1]); kwargs...)
    close(zf)
    return output
end

"""
This method supports multiple files inside the zip file. The name of the csv file inside the zip file must be given.
"""
function read_table(::ZippedCSVFormat, zip_filename:: AbstractString, csv_filename:: AbstractString; kwargs...)
    zf = ZipFile.Reader(zip_filename)
    file_in_zip = filter(x->x.name == csv_filename, zf.files)[1]
    output = CSV.File(read(file_in_zip); kwargs...)
    close(zf)
    return output
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
