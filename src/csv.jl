## CSV

using .CSV

read_table(::TableIOInterface.CSVFormat, filename:: AbstractString; kwargs...) = CSV.File(filename; kwargs...)
read_table(::TableIOInterface.CSVFormat, io:: IO; kwargs...) = CSV.File(read(io); kwargs...)

function write_table!(::TableIOInterface.CSVFormat, output:: Union{AbstractString, IO}, table; kwargs...)
    _checktable(table)
    table |> CSV.write(output; kwargs...)
    nothing
end
