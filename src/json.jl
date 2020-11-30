using .JSONTables

function read_table(::TableIOInterface.JSONFormat, filename:: AbstractString)
    local output
    open(filename, "r") do file
        output = read_table(TableIOInterface.JSONFormat(), file)
    end
    return output 
end

read_table(::TableIOInterface.JSONFormat, io:: IO) = jsontable(io)

function write_table!(::TableIOInterface.JSONFormat, filename:: AbstractString, table; orientation=:objecttable)
    open(filename, "w") do file
        write_table!(TableIOInterface.JSONFormat(), file, table; orientation=orientation)
    end
    nothing
end

function write_table!(::TableIOInterface.JSONFormat, io:: IO, table; orientation=:objecttable)
    _checktable(table)
    if orientation == :objecttable
        export_func = JSONTables.objecttable
    elseif orientation == :arraytable
        export_func = JSONTables.arraytable
    else
        error("`orientation` must be  `:objecttable` (default) or `:arraytable`")
    end
    export_func(io, table)
    nothing
end
