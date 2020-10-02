## JDF

@info "JDF.jl is available - including functionality to read / write JDF files"

using .JDF
using DataFrames

function read_table(::JDFFormat, filename:: AbstractString; kwargs...):: DataFrame
    return loadjdf(filename; kwargs...)
end

function write_table!(::JDFFormat, filename:: AbstractString, table:: DataFrame; kwargs...)
    savejdf(filename, table; kwargs...)
    nothing
end

# JDF supports only DataFrames, not arbitrary Tables.jl inputs. For export, the table is converted to a DataFrame first.
write_table!(::JDFFormat, filename:: AbstractString, table; kwargs...) = write_table!(JDFFormat(), filename, DataFrame(table); kwargs...)
