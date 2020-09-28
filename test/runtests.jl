using TableIO
using Test
using DataFrames
using Parquet

@testset "TableIO.jl" begin
    testpath = mktempdir()
    println("Temporary directory for test files: ", testpath)

    # defining Tables.jl compatible test data
    df = DataFrame(a=1:10, b=rand(10), c="hello".* string.(1:10))
    nt = [(a=1, b=0.5, c="hello"), (a=2, b=0.9, c="world"), (a=3, b=5.5, c="!")]

    # CSV
    fname = joinpath(testpath, "test.csv")
    write_table(fname, df)
    df_recovered = read_table(fname) |> DataFrame!
    @test df == df_recovered
    fname = joinpath(testpath, "test2.csv")
    write_table(fname, nt)
    nt_recovered = read_table(fname)
    @test DataFrame(nt) == DataFrame(nt_recovered)

    # zipped CSV
    fname = joinpath(testpath, "test.zip")
    write_table(fname, df)
    df_recovered = read_table(fname) |> DataFrame!
    @test df == df_recovered
    fname = joinpath(testpath, "test2.zip")
    write_table(fname, nt)
    nt_recovered = read_table(fname, "test2.csv")
    @test DataFrame(nt) == DataFrame(nt_recovered)

    # JDF
    fname = joinpath(testpath, "test.jdf")
    write_table(fname, df)
    df_recovered = read_table(fname) |> DataFrame!
    @test df == df_recovered
    fname = joinpath(testpath, "test2.jdf")
    write_table(fname, nt)
    nt_recovered = read_table(fname)
    @test DataFrame(nt) == nt_recovered # is already a DataFrame for JDF

    # Parquet
    fname = joinpath(testpath, "test.parquet")
    write_table(fname, df)
    df_recovered = read_table(fname; string_cols = ["c"]) |> DataFrame! # use convenience function for string column mapping
    @test df == df_recovered
    fname = joinpath(testpath, "test2.parquet")
    write_table(fname, nt)
    mapping = Dict(["c"] => (String, Parquet.logical_string)) # manually define the mapping of string columns
    nt_recovered = read_table(fname; map_logical_types=mapping)
    @test DataFrame(nt) == DataFrame(nt_recovered)

end
