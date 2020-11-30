## StatFiles.jl - Stata, SPSS, SAS

@info "StatFiles.jl is available - including functionality to read / write Stata, SAS and SPSS files"

import .StatFiles

const StatFilesTypes = Union{TableIOInterface.StataFormat, TableIOInterface.SPSSFormat, TableIOInterface.SASFormat} # dispatching to the concrete format is done in StatFiles.jl

function read_table(::StatFilesTypes, filename; kwargs...)
    return StatFiles.load(filename; kwargs...)
end
