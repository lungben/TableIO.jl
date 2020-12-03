## Excel

@info "XLSX.jl is available - including functionality to read / write Excel (xlsx) files"

using .XLSX

function read_table(::TableIOInterface.ExcelFormat, filename:: AbstractString, sheetname:: AbstractString; kwargs...)
    f = XLSX.readxlsx(filename)
    sheet = f[sheetname]
    return XLSX.eachtablerow(sheet)
end

function read_table(::TableIOInterface.ExcelFormat, filename:: AbstractString; kwargs...)
    f = XLSX.readxlsx(filename)
    sheet = first(f.workbook.sheets)
    return DataFrame(XLSX.eachtablerow(sheet); copycols=false) # this would be no valid Table.jl output if not converted to DataFrame
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
    files = XLSX.sheetnames(xf)
    close(xf)
    return files |> sort
end
