[![Build Status](https://travis-ci.com/lungben/TableIO.jl.svg?branch=master)](https://travis-ci.com/lungben/TableIO.jl)
[![codecov](https://codecov.io/gh/lungben/TableIO.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/lungben/TableIO.jl)

# TableIO

A small "glue" package for reading and writing tabular data. It aims to provide a uniform api for reading and writing tabular data from and to multiple sources.
This package is "intelligent" in this sense that it automatically selects the right reading / writing methods depending on the file extension.

## Supported Formats

* CSV via https://github.com/JuliaData/CSV.jl (installed as core depencency of TableIO)
* Zipped CSV via https://github.com/fhs/ZipFile.jl
* JSON via https://github.com/JuliaData/JSONTables.jl
* JDF via https://github.com/xiaodaigh/JDF.jl
* Parquet via https://github.com/JuliaIO/Parquet.jl
* Excel (xlsx) via https://github.com/felipenoris/XLSX.jl
* SQLite via https://github.com/JuliaDatabases/SQLite.jl
* PostgreSQL via https://github.com/invenia/LibPQ.jl
* Read-only: Stata (dta), SPSS (dat) and SAS (sas7bdat) via https://github.com/queryverse/StatFiles.jl

Most of the underlying packages (except CSV.jl and DataFrames.jl) are not direct dependencies of TableIO and are therefore not installed automatically with it.
This is for reduction of installation size and package load time.

## Installation

    using Pkg
    pkg"add https://github.com/lungben/TableIO.jl"
    using TableIO

Before using a specific format, the corresponding package needs to be installed and imported (not required for CSV.jl):

    Pkg.add("JDF")
    using JDF

TableIO then automatically loads the corresponding wrapper code (using Requires.jl).

## Reading Data

The function

    read_table

reads a data source (file or database) and returns a Table.jl interface, e.g. for creating a DataFrame.

    using TableIO, DataFrames

CSV Format:

    df = read_table("my_data.csv") |> DataFrame! # Keyword arguments can be passed to the CSV reader (CSV.jl)

    using ZipFile
    df = read_table("my_data.zip") |> DataFrame! # zipped CSV format (assuming there is only 1 file in the archive)

JSON Format:

    using JSONTables, Dates
    df = read_table("my_data.json") |> DataFrame # note that |> DataFrame! gives wrong column types!
    df.my_date_col = Dates.(df.my_date_col) # Dates are imported as strings by default, need to be manually converted

Binary Formats:

    using JDF
    df = read_table("my_data.jdf") |> DataFrame! # JDF (compressed binary format)

    using Parquet
    mapping = Dict(["col_3"] => (String, Parquet.logical_string)) # String field types must be mapped to appropriate data types
    df = read_table("my_data.parquet"; map_logical_types=mapping) |> DataFrame! # Parquet

Excel:

    using XLSX
    df = read_table("my_data.xlsx") |> DataFrame! # imports 1st sheet
    df = read_table("my_data.xlsx", "MyAwesomeSheet") |> DataFrame! # imports named sheet

SQLite:

    using SQLite
    df = read_table("my_data.db", "my_table") |> DataFrame! # SQLite from file, table name must be given
    sqlite_db = SQLite.DB("my_data.db")
    df = read_table(sqlite_db, "my_table") |> DataFrame! # SQLite from database connection, table name must be given

PostgreSQL:

    using LibPQ
    postgres_conn = LibPQ.Connection("dbname=postgres user=postgres")
    df = read_table(postgres_conn, "my_table") |> DataFrame! # reading from Postgres connection

StatFiles.jl integration:

    using StatFiles
    df = read_table("my_data.dta") |> DataFrame! # Stata
    df = read_table("my_data.sav") |> DataFrame! # SPSS
    df = read_table("my_data.sas7bdat") |> DataFrame! # SAS

## Writing Data

The function

    write_table!

writes a Table.jl compatible data source into a file or databse.

    using TableIO, DataFrames

CSV Format:

    write_table!("my_data.csv", df)

    using ZipFile
    write_table!("my_data.zip", df) # zipped CSV

JSON Format:

    using JSONTables
    write_table!("my_data.json", df) # dictionary of arrays
    write_table!("my_data.json", df, orientation=:objecttable) # dictionary of arrays
    write_table!("my_data.json", df, orientation=:arraytable) # array of dictionaries

Binary Formats:

    using JDF
    write_table!("my_data.jdf", df) # JDF (compressed binary format)

    using Parquet
    write_table!("my_data.parquet", df) # Parquet - note that Date element type is not supported yet

Excel:

    using XLSX
    write_table!("my_data.xlsx", df) # creates sheet with default name
    write_table!("my_data.xlsx", "test_sheet_42", df) # creates sheet with defined name

SQLite:

    using SQLite
    write_table!("my_data.db", "my_table", df) # SQLite from file, table must not exist
    sqlite_db = SQLite.DB("my_data.db")
    write_table!(sqlite_db, "my_table", df) # SQLite from database connection

PostgreSQL:

    using LibPQ
    postgres_conn = LibPQ.Connection("dbname=postgres user=postgres")
    write_table!(postgres_conn, "my_table", df) # table must exist and be compatible with the input data

StatFiles.jl integration: `write_table!` is not supported.

## Conversions

It is possible to pass the output of `read_table` directly as input to `write_table!` for converting tabular data between different formats:

    using ZipFiles, JDF, XLSX, SQLite

    name1 = joinpath(testpath, "test.zip") # zipped CSV
    name2 = joinpath(testpath, "testx.jdf") # binary
    name3 = joinpath(testpath, "testx.xlsx") # Excel
    name4 = joinpath(testpath, "testx.db") # SQLite

    write_table!(name2, read_table(name1))
    write_table!(name3, read_table(name2))
    write_table!(name4, "my_table", read_table(name3))

    df_recovered = read_table(name4, "my_table") |> DataFrame!

## Testing

The PostgreSQL component requires a running PostgreSQL database for unit tests. This database can be started using the following command:

`docker run --rm --detach --name test-libpqjl -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres`

## Disclaimer

This package is still experimental.
