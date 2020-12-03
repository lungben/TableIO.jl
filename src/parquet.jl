## Parquet

@info "Parquet.jl is available - including functionality to read / write Parquet files"

using .Parquet

"""
For strings (plus potentially additional data types), the binary representation in Parquet must be specified when reading the file.
A general mapping can be given with the keyword argument `map_logical_types`.
Alternatively, the string columns can be given 

"""
function read_table(::TableIOInterface.ParquetFormat, filename; kwargs...)
    parfile = Parquet.File(filename; kwargs...)
    try
        return RecordCursor(parfile)
    finally 
        close(parfile)
    end
end

function write_table!(::TableIOInterface.ParquetFormat, filename, table; kwargs...)
    _checktable(table)
    write_parquet(filename, table; kwargs...)
    nothing
end
