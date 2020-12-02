@info "Pandas.jl is available - including additional file formats using Python Pandas"

import .Pandas
import DataFrames

# HDF5 formats

"""
For reading data, Pandas DataFrames are converted to Julia DataFrames.
"""
function read_table(::TableIOInterface.HDF5Format, filename, key; kwargs...):: DataFrames.DataFrame
    df_pandas = Pandas.read_hdf(filename, key; kwargs)
    return DataFrames.DataFrame(df_pandas)
end

function write_table!(::TableIOInterface.HDF5Format, filename, key, table:: Pandas.DataFrame; kwargs...)
    Pandas.to_hdf(table, filename, key; kwargs...)
    nothing
end

write_table!(::TableIOInterface.HDF5Format, filename, key, table; kwargs...) = write_table!(TableIOInterface.HDF5Format(), filename, key, Pandas.DataFrame(table); kwargs...)
