@testset "File IO" begin

    @testset "CSV" begin
        fname = joinpath(testpath, "test.csv")
        write_table!(fname, df)
        @test filesize(fname) > 0
        df_recovered = DataFrame(read_table(fname); copycols=false)
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
        df_recovered = DataFrame(read_table(fname); copycols=false)
        @test df == df_recovered
        fname = joinpath(testpath, "test2.jdf")
        write_table!(fname, nt)
        @test isdir(fname) # JDF creates a directory, not a single file
        nt_recovered = read_table(fname)
        @test DataFrame(nt) == DataFrame(nt_recovered)
    end

    @testset "Parquet" begin
        df_parquet = df[!, Not(:e)] # Parquet currently does not support Date element type

        fname = joinpath(testpath, "test.parquet")
        write_table!(fname, df_parquet)
        @test filesize(fname) > 0
        df_recovered = DataFrame(read_table(fname); copycols=false)
        @test df_parquet == df_recovered

        fname = joinpath(testpath, "test2.parquet")
        write_table!(fname, nt)
        @test filesize(fname) > 0
        nt_recovered = read_table(fname)
        @test DataFrame(nt) == DataFrame(nt_recovered)
    end

    @testset "Arrow" begin
        fname = joinpath(testpath, "test.arrow")
        write_table!(fname, df)
        @test filesize(fname) > 0
        df_recovered = DataFrame(read_table(fname); copycols=false)
        @test df == df_recovered
        fname = joinpath(testpath, "test2.arrow")
        write_table!(fname, nt)
        @test filesize(fname) > 0
        nt_recovered = read_table(fname)
        @test DataFrame(nt) == DataFrame(nt_recovered)
    end

    @testset "XLSX" begin
        fname = joinpath(testpath, "test.xlsx")
        write_table!(fname, "test_sheet_42", df)
        @test filesize(fname) > 0
        df_recovered = DataFrame(read_table(fname); copycols=false)
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
        df_recovered = DataFrame(read_table(fname)) # note that DataFrame(; copycols=false) gives wrong column types!
        df_recovered.e = Date.(df_recovered.e) # Date format is not automatically detected, need to be converted manually
        @test df == df_recovered

        fname = joinpath(testpath, "test_array.json")
        write_table!(fname, df, orientation=:arraytable)
        @test filesize(fname) > 0
        df_recovered = DataFrame(read_table(fname)) # note that DataFrame(; copycols=false) gives wrong column types!
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
            push!(df_recovered, DataFrame(read_table(fname); copycols=false))
        end
        @test size(df_recovered[1]) == size(df_recovered[2]) == size(df_recovered[3]) == (3, 6)
        @test dropmissing(df_recovered[1]) ==  dropmissing(df_recovered[2]) ==  dropmissing(df_recovered[3]) 

    end

    @testset "zipped" begin
        fname = joinpath(testpath, "test.zip")
        write_table!(fname, df)
        @test filesize(fname) > 0
        df_recovered = DataFrame(read_table(fname); copycols=false)
        @test df == df_recovered

        fname = joinpath(testpath, "test2.zip")
        write_table!(fname, nt)
        @test filesize(fname) > 0
        nt_recovered = read_table(fname, "test2.csv")
        @test DataFrame(nt) == DataFrame(nt_recovered)
        
        fname = joinpath(testpath, "test3.zip")
        write_table!(fname, "test.json", df; orientation=:arraytable)
        @test filesize(fname) > 0
        df_recovered = read_table(fname, "test.json") |> DataFrame # note that DataFrame(; copycols=false) gives wrong column types!
        df_recovered.e = Date.(df_recovered.e) # Date format is not automatically detected, need to be converted manually
        @test df == df_recovered
        df_recovered = read_table(fname) |> DataFrame # note that DataFrame(; copycols=false) gives wrong column types!
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
        df_recovered = DataFrame(read_table(name3); copycols=false)
        @test df == df_recovered

        # SQLite from JDF
        name4 = joinpath(testpath, "testx.db")
        write_table!(name4, "my_table", read_table(name2))
        df_recovered = DataFrame(read_table(name3); copycols=false)
        @test df == df_recovered

        # SQLite from XLSX
        name4 = joinpath(testpath, "testx.db")
        write_table!(name4, "my_table", read_table(name3))
        df_recovered = DataFrame(read_table(name3); copycols=false)
        @test df == df_recovered
    end
end