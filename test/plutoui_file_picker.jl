@testset "PlutoUI" begin

    df = DataFrame(a=1:10, b=0.1:0.1:1, c="hello".* string.(1:10), d=Bool.((1:10) .% 2), e=Date("2020-08-15") .+ Day.(1:10), f="world!" .* string.(1:10))

    empty_file_picker = Dict{Any, Any}("name" => "", "data" => UInt8[], "type" => "")
    @test_throws ErrorException read_table(empty_file_picker)

    filenames = ["test.csv", "test.xlsx", "test_array.json", "test.arrow", "test.zip", "test3.zip"]
    # "test_obj.json" not checked here - strange behaviour with Julia 1.6 nightly. To be checked after Julia 1.6 release.

    for filename ∈ filenames
        @info "testing import of file $filename from PlutoUI.FilePicker"
        file_picker = Dict{Any, Any}("name" => filename, 
            "data" => read(joinpath(testpath, filename)),
            "type" => "")
        df_recovered = DataFrame(read_table(file_picker); copycols=false)
        df_recovered[!, :e] = Date.(df_recovered.e) # some impots do not convert date columns automatically

        @test df_recovered == df 
    end

end
