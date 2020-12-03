## Excel

@info "XLSX.jl is available - including functionality to read / write Excel (xlsx) files"

using .XLSX

function read_table(::TableIOInterface.ExcelFormat, filename:: AbstractString, sheetname:: AbstractString; kwargs...)
    xf = XLSX.readxlsx(filename)
    try
        sheet = xf[sheetname]
        return XLSX.eachtablerow(sheet)
    finally
        close(xf)
    end
end

function read_table(::TableIOInterface.ExcelFormat, filename:: AbstractString; kwargs...)
    xf = XLSX.readxlsx(filename)
    try
        sheet = first(xf.workbook.sheets)
        return DataFrame(XLSX.eachtablerow(sheet); copycols=false) # this would be no valid Table.jl output if not converted to DataFrame
    finally
        close(xf)
    end
end


function write_table!(::TableIOInterface.ExcelFormat, filename:: AbstractString, sheetname:: AbstractString, table:: DataFrame; kwargs...)
    _checktable(table)
    XLSX.writetable(filename, table; overwrite=true, sheetname=sheetname, kwargs...)
    nothing
end

const DEFAULT_SHEETNAME = "sheet_1"
write_table!(::TableIOInterface.ExcelFormat, filename:: AbstractString, table; kwargs...) = write_table!(TableIOInterface.ExcelFormat(), filename, DEFAULT_SHEETNAME, table; kwargs...)

# XLSX supports only DataFrames, not arbitrary Tables.jl inputs. For export, the table is converted to a DataFrame first.
write_table!(::TableIOInterface.ExcelFormat, filename:: AbstractString, sheetname:: AbstractString, table; kwargs...) = write_table!(TableIOInterface.ExcelFormat(), filename:: AbstractString, sheetname:: AbstractString, DataFrame(table); kwargs...)

function list_tables(::TableIOInterface.ExcelFormat, filename:: AbstractString)
    xf = XLSX.readxlsx(filename)
    try
        files = XLSX.sheetnames(xf)
        return files |> sort
    finally
        close(xf)
    end
end
