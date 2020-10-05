## Apache Arrow

@info "Arrow.jl is available - including functionality to read / write JDF files"

using .Arrow

function read_table(::ArrowFormat, filename:: Union{AbstractString, IO}; kwargs...)
    return Arrow.Table(filename; kwargs...)
end

function write_table!(::ArrowFormat, filename:: Union{AbstractString, IO}, table; kwargs...)
    Arrow.write(filename, table; kwargs...)
    nothing
end
