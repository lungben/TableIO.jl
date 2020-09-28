using TableIO
using Test
using DataFrames
using Parquet
using SQLite
using LibPQ

@testset "TableIO.jl" begin
    testpath = mktempdir()
    println("Temporary directory for test files: ", testpath)

    # defining Tables.jl compatible test data
    df = DataFrame(a=1:10, b=rand(10), c="hello".* string.(1:10))
    nt = [(a=1, b=0.5, c="hello"), (a=2, b=0.9, c="world"), (a=3, b=5.5, c="!")]

    @testset "File IO" begin

        @testset "CSV" begin
            fname = joinpath(testpath, "test.csv")
            write_table(fname, df)
            df_recovered = read_table(fname) |> DataFrame!
            @test df == df_recovered
            fname = joinpath(testpath, "test2.csv")
            write_table(fname, nt)
            nt_recovered = read_table(fname)
            @test DataFrame(nt) == DataFrame(nt_recovered)
        end

        @testset "zipped CSV" begin
            fname = joinpath(testpath, "test.zip")
            write_table(fname, df)
            df_recovered = read_table(fname) |> DataFrame!
            @test df == df_recovered
            fname = joinpath(testpath, "test2.zip")
            write_table(fname, nt)
            nt_recovered = read_table(fname, "test2.csv")
            @test DataFrame(nt) == DataFrame(nt_recovered)
        end

        @testset "JDF" begin
            fname = joinpath(testpath, "test.jdf")
            write_table(fname, df)
            df_recovered = read_table(fname) |> DataFrame!
            @test df == df_recovered
            fname = joinpath(testpath, "test2.jdf")
            write_table(fname, nt)
            nt_recovered = read_table(fname)
            @test DataFrame(nt) == nt_recovered # is already a DataFrame for JDF
        end

        @testset "Parquet" begin
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

        @testset "XLSX" begin
            fname = joinpath(testpath, "test.xlsx")
            write_table(fname, "test_sheet_42", df)
            df_recovered = read_table(fname) |> DataFrame!
            @test df == df_recovered
            fname = joinpath(testpath, "test2.xlsx")
            write_table(fname, nt)
            nt_recovered = read_table(fname, "sheet_1") # default name
            @test DataFrame(nt) == DataFrame(nt_recovered)
        end
    end

    @testset "Database IO" begin
        
        @testset "SQLite" begin
            fname = joinpath(testpath, "test.db")
            db = SQLite.DB(fname)

            write_table(db, "test1", df)
            df_recovered = read_table(fname, "test1") |> DataFrame!
            @test df == df_recovered
            
            df_sql = read_sql(db, "select * from test1 where a < 5") |> DataFrame!
            @test df[df.a .< 5, :] == df_sql

            write_table(fname, "test2", nt)
            nt_recovered = read_table(db, "test2")
            @test DataFrame(nt) == DataFrame(nt_recovered)
        end

        @testset "PostgreSQL" begin
            # the following tests require a running PostgreSQL database.
            # `docker run --rm --detach --name test-libpqjl -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres`
            conn = LibPQ.Connection("dbname=postgres user=postgres")

            execute(conn, """CREATE TEMPORARY TABLE test1 (
                a integer PRIMARY KEY,
                b numeric,
                c character varying
                );""")
            write_table(conn, "test1", df)
            df_recovered = read_table(conn, "test1") |> DataFrame!
            @test df == df_recovered

            df_sql = read_sql(conn, "select * from test1 where a < 5") |> DataFrame!
            @test df[df.a .< 5, :] == df_sql

            execute(conn, """CREATE TEMPORARY TABLE test2 (
                a integer PRIMARY KEY,
                b numeric,
                c character varying
                );""")
            write_table(conn, "test2", nt)
            nt_recovered = read_table(conn, "test2")
            @test DataFrame(nt) == DataFrame(nt_recovered)

            close(conn)
        end
    end

end
