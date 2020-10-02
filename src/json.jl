using .JSONTables

function read_table(::JSONFormat, filename:: AbstractString)
    local output
    open(filename, "r") do file
        output = read_table(JSONFormat(), file)
    end
    return output 
end

read_table(::JSONFormat, io:: IO) = jsontable(io)

function write_table!(::JSONFormat, filename:: AbstractString, table; orientation=:objecttable)
    open(filename, "w") do file
        write_table!(JSONFormat(), file, table; orientation=orientation)
    end
    nothing
end

function write_table!(::JSONFormat, io:: IO, table; orientation=:objecttable)
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
