[![Build Status](https://travis-ci.com/lungben/TableIO.jl.svg?branch=master)](https://travis-ci.com/lungben/TableIO.jl)
[![codecov](https://codecov.io/gh/lungben/TableIO.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/lungben/TableIO.jl)

# TableIO

A small "glue" package for reading and writing tabular data. It aims to provide a uniform api for reading and writing tabular data from and to multiple sources.
This package is "intelligent" in this sense that it automatically selects the right reading / writing methods depending on the file extension.

## Supported Formats

* CSV via https://github.com/JuliaData/CSV.jl
* Zipped CSV via https://github.com/fhs/ZipFile.jl
* JDF via https://github.com/xiaodaigh/JDF.jl
* Parquet via https://github.com/JuliaIO/Parquet.jl
* Excel (xlsx) via https://github.com/felipenoris/XLSX.jl
* SQLite via https://github.com/JuliaDatabases/SQLite.jl
* PostgreSQL via https://github.com/invenia/LibPQ.jl

## Reading Data

The function

    read_table

reads a data source (file or database) and returns a Table.jl interface, e.g. for creating a DataFrame.

CSV Format:

    df = read_table("my_data.csv") |> DataFrame! # Keyword arguments can be passed to the CSV reader (CSV.jl)
    df = read_table("my_data.zip") |> DataFrame! # zipped CSV format (assuming there is only 1 file in the archive)

Binary Formats:

    df = read_table("my_data.jdf") |> DataFrame! # JDF (compressed binary format)
    df = read_table("my_data.parquet", string_cols=["col_3"]) |> DataFrame! # Parquet

Excel:

    df = read_table("my_data.xlsx") |> DataFrame! # imports 1st sheet
    df = read_table("my_data.xlsx", "MyAwesomeSheet") |> DataFrame! # imports named sheet

SQLite:

    df = read_table("my_data.db", "my_table") |> DataFrame! # SQLite from file, table name must be given
    sqlite_db = SQLite.DB("my_data.db")
    df = read_table(sqlite_db, "my_table") |> DataFrame! # SQLite from database connection, table name must be given

PostgreSQL:

    postgres_conn = LibPQ.Connection("dbname=postgres user=postgres")
    df = read_table(postgres_conn, "my_table") |> DataFrame! # reading from Postgres connection

## Writing Data

The function

    write_table

writes a Table.jl compatible data source into a file or databse.

CSV Format:

    write_table("my_data.csv", df)
    write_table("my_data.zip", df) # zipped CSV

Binary Formats:

    write_table("my_data.jdf", df) # JDF (compressed binary format)
    write_table("my_data.parquet", df) # Parquet

Excel:

    write_table("my_data.xlsx", df) # creates sheet with default name
    write_table("my_data.xlsx", "test_sheet_42", df) # creates sheet with defined name

SQLite:

     write_table("my_data.db", "my_table", df) # SQLite from file, table must not exist
     sqlite_db = SQLite.DB("my_data.db")
     write_table(sqlite_db, "my_table", df) # SQLite from database connection

PostgreSQL:

    postgres_conn = LibPQ.Connection("dbname=postgres user=postgres")
    write_table(postgres_conn, "my_table", df) # table must exist and be compatible with the input data

## Conversions

It is possible to pass the output of `read_table` directly as input to `write_table` for converting tabular data between different formats:

    name1 = joinpath(testpath, "test.zip") # zipped CSV
    name2 = joinpath(testpath, "testx.jdf") # binary
    name3 = joinpath(testpath, "testx.xlsx") # Excel
    name4 = joinpath(testpath, "testx.db") # SQLite

    write_table(name2, read_table(name1))
    write_table(name3, read_table(name2))
    write_table(name4, "my_table", read_table(name3))

    df_recovered = read_table(name3) |> DataFrame!

## Disclaimer

This package is still experimental.
