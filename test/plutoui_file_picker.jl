@testset "PlutoUI" begin

    df = DataFrame(a=1:10, b=rand(10), c="hello".* string.(1:10), d=Bool.((1:10) .% 2), e=Date("2020-08-15") .+ Day.(1:10), f="world!" .* string.(1:10))

    empty_file_picker = Dict{Any, Any}("name" => "", "data" => UInt8[], "type" => "")
    @test_throws ErrorException read_table(empty_file_picker)

    filenames = ["test.csv", "test.xlsx", "test_array.json", "test_obj.json", "test.arrow", "test.zip", "test3.zip"]

    for filename ∈ filenames
        file_picker = Dict{Any, Any}("name" => filename, 
            "data" => read(joinpath(testpath, filename)),
            "type" => "")
        df_recovered = DataFrame(read_table(file_picker); copycols=false)
        df_recovered[!, :e] = Date.(df_recovered.e) # some impots do not convert date columns automatically

        @test df_recovered == df 
    end

end
