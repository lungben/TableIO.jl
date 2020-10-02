using TableIO
using Test
using DataFrames
using Dates

using Parquet
using JDF
using XLSX
using StatFiles
using ZipFile
using SQLite
using LibPQ
using JSONTables


@testset "TableIO.jl" begin
    testpath = mktempdir()
    println("Temporary directory for test files: ", testpath)

    # defining Tables.jl compatible test data
    df = DataFrame(a=1:10, b=rand(10), c="hello".* string.(1:10), d=Bool.((1:10) .% 2), e=Date("2020-08-15") .+ Day.(1:10), f="world!" .* string.(1:10))
    nt = [(a=1, b=0.5, c="hello"), (a=2, b=0.9, c="world"), (a=3, b=5.5, c="!")]

    @testset "File IO" begin

        @testset "CSV" begin
            fname = joinpath(testpath, "test.csv")
            write_table!(fname, df)
            @test filesize(fname) > 0
            df_recovered = read_table(fname) |> DataFrame!
            @test df == df_recovered
            fname = joinpath(testpath, "test2.csv")
            write_table!(fname, nt)
            @test filesize(fname) > 0
            nt_recovered = read_table(fname)
            @test DataFrame(nt) == DataFrame(nt_recovered)
        end

        @testset "JDF" begin
            fname = joinpath(testpath, "test.jdf")
            write_table!(fname, df)
            @test isdir(fname) # JDF creates a directory, not a single file
            df_recovered = read_table(fname) |> DataFrame!
            @test df == df_recovered
            fname = joinpath(testpath, "test2.jdf")
            write_table!(fname, nt)
            @test isdir(fname) # JDF creates a directory, not a single file
            nt_recovered = read_table(fname)
            @test DataFrame(nt) == nt_recovered # is already a DataFrame for JDF
        end

        @testset "Parquet" begin
            df_parquet = df[!, Not(:e)] # Parquet currently does not support Date element type

            mapping = Dict(["c"] => (String, Parquet.logical_string), ["f"] => (String, Parquet.logical_string)) # String field types must be mapped to appropriate data types
            fname = joinpath(testpath, "test.parquet")
            write_table!(fname, df_parquet)
            @test filesize(fname) > 0
            df_recovered = read_table(fname; map_logical_types=mapping) |> DataFrame!
            @test df_parquet == df_recovered

            fname = joinpath(testpath, "test2.parquet")
            write_table!(fname, nt)
            @test filesize(fname) > 0
            mapping = Dict(["c"] => (String, Parquet.logical_string))
            nt_recovered = read_table(fname; map_logical_types=mapping)
            @test DataFrame(nt) == DataFrame(nt_recovered)
        end

        @testset "XLSX" begin
            fname = joinpath(testpath, "test.xlsx")
            write_table!(fname, "test_sheet_42", df)
            @test filesize(fname) > 0
            df_recovered = read_table(fname) |> DataFrame!
            @test df == df_recovered
            fname = joinpath(testpath, "test2.xlsx")
            write_table!(fname, nt)
            @test filesize(fname) > 0
            nt_recovered = read_table(fname, "sheet_1") # default name
            @test DataFrame(nt) == DataFrame(nt_recovered)
        end

        @testset "JSON" begin
            fname = joinpath(testpath, "test_obj.json")
            write_table!(fname, df, orientation=:objecttable)
            @test filesize(fname) > 0
            df_recovered = read_table(fname) |> DataFrame # note that |> DataFrame! gives wrong column types!
            df_recovered.e = Date.(df_recovered.e) # Date format is not automatically detected, need to be converted manually
            @test df == df_recovered

            fname = joinpath(testpath, "test_array.json")
            write_table!(fname, df, orientation=:arraytable)
            @test filesize(fname) > 0
            df_recovered = read_table(fname) |> DataFrame # note that |> DataFrame! gives wrong column types!
            df_recovered.e = Date.(df_recovered.e) # Date format is not automatically detected, need to be converted manually
            @test df == df_recovered

            fname = joinpath(testpath, "test2.json")
            write_table!(fname, nt)
            @test filesize(fname) > 0
            nt_recovered = read_table(fname) # default name
            @test DataFrame(nt) == DataFrame(nt_recovered)

            @test_throws ErrorException write_table!(fname, df, orientation=:xyz)
        end

        @testset "StatFiles" begin
            # test files taken from https://github.com/queryverse/StatFiles.jl
            df_recovered = DataFrame[]
            for ext in ("dta", "sav", "sas7bdat")
                fname = joinpath(@__DIR__, "types.$ext")
                push!(df_recovered, read_table(fname) |> DataFrame!)
            end
            @test size(df_recovered[1]) == size(df_recovered[2]) == size(df_recovered[3]) == (3, 6)
            @test dropmissing(df_recovered[1]) ==  dropmissing(df_recovered[2]) ==  dropmissing(df_recovered[3]) 

        end

        @testset "zipped" begin
            fname = joinpath(testpath, "test.zip")
            write_table!(fname, df)
            @test filesize(fname) > 0
            df_recovered = read_table(fname) |> DataFrame!
            @test df == df_recovered

            fname = joinpath(testpath, "test2.zip")
            write_table!(fname, nt)
            @test filesize(fname) > 0
            nt_recovered = read_table(fname, "test2.csv")
            @test DataFrame(nt) == DataFrame(nt_recovered)
            
            fname = joinpath(testpath, "test3.zip")
            write_table!(fname, "test.json", df)
            @test filesize(fname) > 0
            df_recovered = read_table(fname, "test.json") |> DataFrame # note that |> DataFrame! gives wrong column types!
            df_recovered.e = Date.(df_recovered.e) # Date format is not automatically detected, need to be converted manually
            @test df == df_recovered

        end

        @testset "conversions" begin
            # file formats
            name1 = joinpath(testpath, "test.zip")
            name2 = joinpath(testpath, "testx.jdf")
            name3 = joinpath(testpath, "testx.xlsx")
            write_table!(name2, read_table(name1))
            write_table!(name3, read_table(name2))
            df_recovered = read_table(name3) |> DataFrame!
            @test df == df_recovered

            # SQLite from JDF
            name4 = joinpath(testpath, "testx.db")
            write_table!(name4, "my_table", read_table(name2))
            df_recovered = read_table(name3) |> DataFrame!
            @test df == df_recovered

            # SQLite from XLSX
            name4 = joinpath(testpath, "testx.db")
            write_table!(name4, "my_table", read_table(name3))
            df_recovered = read_table(name3) |> DataFrame!
            @test df == df_recovered
        end
    end

    @testset "Database IO" begin
        
        @testset "SQLite" begin
            fname = joinpath(testpath, "test.db")
            db = SQLite.DB(fname)

            write_table!(db, "test1", df)
            @test filesize(fname) > 0
            df_recovered = read_table(fname, "test1") |> DataFrame!
            @test df == df_recovered
            
            df_sql = read_sql(db, "select * from test1 where a < 5") |> DataFrame!
            @test df[df.a .< 5, :] == df_sql

            write_table!(fname, "test2", nt)
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
                c character varying,
                d boolean,
                e date,
                f character varying
                );""")
            write_table!(conn, "test1", df)
            df_recovered = read_table(conn, "test1") |> DataFrame!
            @test df == df_recovered

            df_sql = read_sql(conn, "select * from test1 where a < 5") |> DataFrame!
            @test df[df.a .< 5, :] == df_sql

            execute(conn, """CREATE TEMPORARY TABLE test2 (
                a integer PRIMARY KEY,
                b numeric,
                c character varying
                );""")
            write_table!(conn, "test2", nt)
            nt_recovered = read_table(conn, "test2")
            @test DataFrame(nt) == DataFrame(nt_recovered)

            close(conn)
        end
    end

end
