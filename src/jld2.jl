@info "JLD2.jl is available - including functionality to read / write JLD2 files"

import .JLD2

function read_table(::TableIOInterface.JLD2Format, filename:: AbstractString, tablename:: AbstractString; kwargs...)
    return JLD2.jldopen(filename, "r") do file
        file[tablename]
    end
end

function read_table(::TableIOInterface.JLD2Format, filename:: AbstractString; kwargs...)
    return JLD2.jldopen(filename, "r") do file
        table_list = keys(file) |> sort
        file[first(table_list)]
    end
end

function write_table!(::TableIOInterface.JLD2Format, filename:: AbstractString, tablename:: AbstractString, table; kwargs...)
    _checktable(table)
    _checktablename(tablename)
    JLD2.jldopen(filename, "a+") do file
        file[tablename] = table
    end
    nothing
end

function list_tables(::TableIOInterface.JLD2Format, filename:: AbstractString)
    return JLD2.jldopen(filename, "r") do file
        keys(file) |> sort
    end
end
