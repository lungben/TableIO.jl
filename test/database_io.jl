using SQLite
using LibPQ

@testset "Database IO" begin
        
    @testset "SQLite" begin
        fname = joinpath(testpath, "test.db")
        db = SQLite.DB(fname)

        write_table!(db, "test1", df)
        @test filesize(fname) > 0
        @test list_tables(fname) == ["test1"]
        df_recovered = DataFrame(read_table(fname, "test1"); copycols=false)
        @test df == df_recovered
        
        df_sql = DataFrame(read_sql(db, "select * from test1 where a < 5"); copycols=false)
        @test df[df.a .< 5, :] == df_sql

        write_table!(fname, "test2", nt)
        nt_recovered = read_table(db, "test2")
        @test DataFrame(nt) == DataFrame(nt_recovered)
        @test_logs (:warn, "File contains more than one table, the alphabetically first one is taken") df_recovered = DataFrame(read_table(fname); copycols=false) # fetch alphabetiacally first table
        @test df == df_recovered

        @test list_tables(db) == ["test1", "test2"]
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
        df_recovered = DataFrame(read_table(conn, "test1"); copycols=false)
        @test df == df_recovered

        df_sql = DataFrame(read_sql(conn, "select * from test1 where a < 5"); copycols=false)
        @test df[df.a .< 5, :] == df_sql

        execute(conn, """CREATE TEMPORARY TABLE test2 (
            a integer PRIMARY KEY,
            b numeric,
            c character varying
            );""")
        write_table!(conn, "test2", nt)
        nt_recovered = read_table(conn, "test2")
        @test DataFrame(nt) == DataFrame(nt_recovered)

        @test list_tables(conn) == ["test1", "test2"]

        close(conn)
    end
end
