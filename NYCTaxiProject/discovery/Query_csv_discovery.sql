--CREATE and use a database for our discovery
CREATE DATABASE nyc_taxi_discovery;
USE nyc_taxi_discovery;


-- Query CSV file from Datalake using OPENRWOSET. In case of not clear errors switch the parser to 1.0 and rerun the code to get a better error description.
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@wsmaindl.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',', --in case of TSV replace the fieldterminator \t
        ROWTERMINATOR = '\n'
    ) AS [result]
--Stored procedure to examine datatypes for the columns

-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://wsmaindl.dfs.core.windows.net/nyc-taxi-data/raw/trip_data_green_parquet/year=2020/month=01/part-00000-tid-6133789922049958496-2e489315-890a-4453-ae93-a104be9a6f06-106-1-c000.snappy.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]


--Getting the max  value for each column
SELECT
    MAX(LEN(LocationID)) AS len_LocationID,
    MAX(LEN(Borough)) AS len_Borough,
    MAX(LEN(Zone)) AS len_Zone,
    MAX(LEN(service_zone)) AS len_service_zone
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@wsmaindl.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) AS [result]

--- Explicit data types using the With
SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@wsmaindl.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        LocationID SMALLINT,
        Borough VARCHAR(15),
        Zone VARCHAR(50),
        service_zone  VARCHAR(15)  
    )AS [result]
--EXAMIE DATATYPES
EXEC sp_describe_first_result_set N'SELECT
    *
FROM
    OPENROWSET(
        BULK ''abfss://nyc-taxi-data@wsmaindl.dfs.core.windows.net/raw/taxi_zone.csv'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = '','',
        ROWTERMINATOR = ''\n''
    ) 
    WITH (
        LocationID SMALLINT,
        Borough VARCHAR(15),
        Zone VARCHAR(50),
        service_zone  VARCHAR(15)  
    )AS [result]'

--FIND DEFAULT COLLATION 
SELECT name, collation_name FROM sys.databases;

--Specify collation for varchar columns 

SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@wsmaindl.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        LocationID SMALLINT,
        Borough VARCHAR(15) COLLATE Latin1_General_100_CI_AI_SC_UTF8,
        Zone VARCHAR(50) COLLATE Latin1_General_100_CI_AI_SC_UTF8,
        service_zone  VARCHAR(15) COLLATE Latin1_General_100_CI_AI_SC_UTF8 
    )AS [result];

--Apply UTF8 collation on database level

ALTER DATABASE nyc_taxi_discovery COLLATE Latin1_General_100_CI_AI_SC_UTF8;

-- Select subset of columns

SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@wsmaindl.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        Borough VARCHAR(15),
        Zone VARCHAR(50)
    )AS [result];

---Read Data from a file without a header specifiy the cardinal order
SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@wsmaindl.dfs.core.windows.net/raw/taxi_zone_without_header.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) WITH (
        LocationID SMALLINT 1,
        Borough VARCHAR(15) 2,
        Zone VARCHAR(50) 3,
        service_zone  VARCHAR(15) 4  
    )
   AS [result]

--fix column names and specify the first row of data.
SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@wsmaindl.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        location_id SMALLINT 1,
        borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone  VARCHAR(15) 4 
    )AS [result]

--Create External DataSource for raw, use the same process to silver/gold/ staging :
CREATE EXTERNAL DATA SOURCE nyc_taxi_data_raw
WITH (
    LOCATION = 'abfss://nyc-taxi-data@wsmaindl.dfs.core.windows.net/raw'
)


SELECT
    *
FROM
    OPENROWSET(
        BULK 'taxi_zone.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        ESCAPECHAR = '\\',
        FIELDQUOTE = '"'
    ) 
    WITH (
        location_id SMALLINT 1,
        borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone  VARCHAR(15) 4 
    )AS [result]

--trace back the location of External data source using 
SELECT namE, location FROM sys.external_data_sources;

--Credentials : Todo later.
