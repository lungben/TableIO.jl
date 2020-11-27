# install Python dependencies
ENV["PYTHON"] = ""
using Pkg
Pkg.build("PyCall")
using Conda
Conda.add("pandas")
Conda.add("pytables")


@testset "HDF5" begin
    fname = joinpath(testpath, "test.hdf")
    write_table!(fname, df)
    @test filesize(fname) > 0
    df_recovered = DataFrame(read_table(fname); copycols=false)
    @test compare_df_ignore_order(df,df_recovered)
    fname = joinpath(testpath, "test2.hdf")
    write_table!(fname, nt)
    @test filesize(fname) > 0
    nt_recovered = read_table(fname)
    @test compare_df_ignore_order(DataFrame(nt), nt_recovered)
end
