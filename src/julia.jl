# stores Table as Julia code, precisely as NamedTuple of Arrays.

_convert_to_named_tuple(table) = (; pairs(eachcol(table))...) # e.g. for DataFrames
_convert_to_named_tuple(table:: T) where {T <: NamedTuple{<: AbstractArray}} = table # Do not change anything for NamedTuple of arrays

function _convert_to_named_tuple(table:: T) where {T <: AbstractArray{<: NamedTuple}}
    column_names = keys(first(table))
    data = [getproperty.(table, col) for col in column_names]
    return NamedTuple{column_names}(data)
end

function write_table!(::TableIOInterface.JuliaFormat, filename:: AbstractString, tablename:: AbstractString, table)
    _checktable(table)
    _checktablename(tablename)
    table_as_tuple = _convert_to_named_tuple(table)
    open(filename, "a") do file
        println(file, tablename, " = ", table_as_tuple)
    end
    nothing
end
