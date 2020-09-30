## Parquet

@info "Parquet.jl is available - including functionality to read / write Parquet files"

using .Parquet

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
