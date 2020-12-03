# install Python dependencies
ENV["PYTHON"] = ""
using Pkg
Pkg.activate(".")
Pkg.add(["PyCall", "Conda"])
Pkg.build("PyCall")
using Conda
Conda.add("pandas")
Conda.add("pytables")


@testset "HDF5" begin
    fname = joinpath(testpath, "test.hdf")
    write_table!(fname, "/data", df)
    @test filesize(fname) > 0
    @test list_tables(fname) == ["/data"]
    df_recovered = DataFrame(read_table(fname, "/data"); copycols=false)
    @test compare_df_ignore_order(df,df_recovered)
    fname = joinpath(testpath, "test2.hdf")
    write_table!(fname, "my_data", nt)
    @test filesize(fname) > 0
    nt_recovered = DataFrame(read_table(fname, "my_data"); copycols=false)
    @test compare_df_ignore_order(DataFrame(nt), nt_recovered)
end
