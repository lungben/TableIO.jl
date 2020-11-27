
@testset "HDF5" begin
    fname = joinpath(testpath, "test.hdf")
    write_table!(fname, df)
    @test filesize(fname) > 0
    df_recovered = DataFrame(read_table(fname); copycols=false)
    @test df == df_recovered
    fname = joinpath(testpath, "test2.hdf")
    write_table!(fname, nt)
    @test filesize(fname) > 0
    nt_recovered = read_table(fname)
    @test DataFrame(nt) == nt_recovered
end
