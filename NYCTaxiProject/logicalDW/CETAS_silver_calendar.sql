
IF OBJECT_ID('silver.calendar') IS NOT NULL
    DROP EXTERNAL TABLE silver.calendar
GO
CREATE EXTERNAL TABLE silver.calendar
    WITH(
        DATA_SOURCE = nyc_taxi_src,
        LOCATION = 'silver/calendar',
        FILE_FORMAT = parquet_format
    )
AS 
SELECT *
FROM bronze.calendar
GO

SELECT * FROM silver.calendar

