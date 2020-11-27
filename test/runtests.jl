using TableIO
using Test
using DataFrames
using Dates

# the following packages are imported automatically when a corresponding file type is used
# using JDF
# using XLSX
# using StatFiles
# using ZipFile
# using JSONTables
# using Arrow


function compare_df_ignore_order(df1:: DataFrame, df2:: DataFrame)
    sort(names(df1)) == sort(names(df2)) || return false
    for col in names(df1)
        df1[!, col] == df2[!, col] || return false
    end
    return true
end

testpath = mktempdir()
println("Temporary directory for test files: ", testpath)

# defining Tables.jl compatible test data
df = DataFrame(a=1:10, b=rand(10), c="hello".* string.(1:10), d=Bool.((1:10) .% 2), e=Date("2020-08-15") .+ Day.(1:10), f="world!" .* string.(1:10))
nt = [(a=1, b=0.5, c="hello"), (a=2, b=0.9, c="world"), (a=3, b=5.5, c="!")]

@testset "TableIO.jl" begin

    include("file_io.jl")
    include("database_io.jl")
    include("plutoui_file_picker.jl")

end
