## Excel

@info "XLSX.jl is available - including functionality to read / write Excel (xlsx) files"

using .XLSX

function read_table(::TableIOInterface.ExcelFormat, filename:: AbstractString, sheetname:: AbstractString; kwargs...)
    xf = XLSX.readxlsx(filename)
    try
        sheet = xf[sheetname]
        return _eachtablerow(sheet; kwargs...)
    finally
        close(xf)
    end
end

function read_table(t::TableIOInterface.ExcelFormat, filename:: AbstractString; kwargs...)
    xf = XLSX.readxlsx(filename)
    try
        table_list = _list_tables(t, xf)
        length(table_list) > 1 && @warn "File contains more than one table, the alphabetically first one is taken"
        sheet = xf[first(table_list)]
        return _eachtablerow(sheet; kwargs...)
    finally
        close(xf)
    end
end

_eachtablerow(sheet; columns=nothing, kwargs...) = isnothing(columns) ? XLSX.eachtablerow(sheet; kwargs...) : XLSX.eachtablerow(sheet, columns; kwargs...)

function write_table!(::TableIOInterface.ExcelFormat, filename:: AbstractString, tablename:: AbstractString, table:: DataFrame; kwargs...)
    _checktable(table)
    _checktablename(tablename)
    XLSX.writetable(filename, table; overwrite=true, sheetname=tablename, kwargs...)
    nothing
end

# XLSX supports only DataFrames, not arbitrary Tables.jl inputs. For export, the table is converted to a DataFrame first.
write_table!(::TableIOInterface.ExcelFormat, filename:: AbstractString, tablename:: AbstractString, table; kwargs...) = write_table!(TableIOInterface.ExcelFormat(), filename, tablename, DataFrame(table); kwargs...)

function list_tables(t::TableIOInterface.ExcelFormat, filename:: AbstractString)
    xf = XLSX.readxlsx(filename)
    try
        return _list_tables(t, xf)
    finally
        close(xf)
    end
end

_list_tables(::TableIOInterface.ExcelFormat, xf) = XLSX.sheetnames(xf) |> sort
