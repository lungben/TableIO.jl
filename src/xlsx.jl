## Excel

@info "XLSX.jl is available - including functionality to read / write Excel (xlsx) files"

using .XLSX

function read_table(::ExcelFormat, filename:: AbstractString, sheetname:: AbstractString; kwargs...)
    f = XLSX.readxlsx(filename)
    sheet = f[sheetname]
    return XLSX.eachtablerow(sheet)
end

function read_table(::ExcelFormat, filename:: AbstractString; kwargs...)
    f = XLSX.readxlsx(filename)
    sheet = first(f.workbook.sheets)
    return XLSX.eachtablerow(sheet) |> DataFrame! # this would be no valid Table.jl output if not converted to DataFrame
end   


function write_table(::ExcelFormat, filename:: AbstractString, sheetname:: AbstractString, table:: DataFrame; kwargs...)
    _checktable(table)
    XLSX.writetable(filename, table; overwrite=true, sheetname=sheetname, kwargs...)
    return filename
end

const DEFAULT_SHEETNAME = "sheet_1"
write_table(::ExcelFormat, filename:: AbstractString, table; kwargs...) = write_table(ExcelFormat(), filename, DEFAULT_SHEETNAME, table; kwargs...)

# XLSX supports only DataFrames, not arbitrary Tables.jl inputs. For export, the table is converted to a DataFrame first.
write_table(::ExcelFormat, filename:: AbstractString, sheetname:: AbstractString, table; kwargs...) = write_table(ExcelFormat(), filename:: AbstractString, sheetname:: AbstractString, DataFrame(table); kwargs...)
