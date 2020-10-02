using .JSONTables

function read_table(::JSONFormat, filename:: AbstractString)
    local output
    open(filename, "r") do file
        output = jsontable(file)
    end
    return output 
end

function write_table(::JSONFormat, filename:: AbstractString, table; orientation=:objecttable)
    if orientation == :objecttable
        export_func = JSONTables.objecttable
    elseif orientation == :arraytable
        export_func = JSONTables.arraytable
    else
        error("`orientation` must be  `:objecttable` (default) or `:arraytable`")
    end
    _checktable(table)

    open(filename, "w") do file
        export_func(file, table)
    end
    return filename
end
