@info "Pandas.jl is available - including additional file formats using Python Pandas"

import .Pandas
import DataFrames

# HDF5 formats

function read_table(::TableIOInterface.HDF5Format, filename, key; kwargs...):: Pandas.DataFrame
    df_pandas = Pandas.read_hdf(filename, key; kwargs)
    return df_pandas
end

function read_table(t::TableIOInterface.HDF5Format, filename; kwargs...):: Pandas.DataFrame
    hdf = Pandas.HDFStore(filename)
    try
        table_list = _list_tables(t, hdf)
        length(table_list) > 1 && @warn "File contains more than one table, the alphabetically first one is taken"
        key = first(table_list)
        return Pandas.read_hdf(filename, key; kwargs)
    finally
        close(hdf)
    end
end

function write_table!(::TableIOInterface.HDF5Format, filename, tablename, table:: Pandas.DataFrame; kwargs...)
    # for HDF5 files, the table name may contain a path, therefore _checktablename is not executed.
    # Pandas.DataFrame has no Tables.jl compatible interface, therefore _checktable is not executed.
    Pandas.to_hdf(table, filename, tablename; kwargs...)
    nothing
end

write_table!(::TableIOInterface.HDF5Format, filename, tablename, table; kwargs...) = write_table!(TableIOInterface.HDF5Format(), filename, tablename, Pandas.DataFrame(table); kwargs...)

function list_tables(t::TableIOInterface.HDF5Format, filename:: AbstractString)
    hdf = Pandas.HDFStore(filename)
    try
        _list_tables(t, hdf)
    finally
        close(hdf)
    end
end

_list_tables(::TableIOInterface.HDF5Format, hdf) = keys(hdf) |> sort
