
IF OBJECT_ID('silver.trip_data_green') IS NOT NULL
    DROP EXTERNAL TABLE silver.trip_data_green
GO
CREATE EXTERNAL TABLE silver.trip_data_green
    WITH 
    (
        DATA_SOURCE = nyc_taxi_src,
        LOCATION = 'silver/trip_data_green',
        FILE_FORMAT = parquet_format
    )
AS 
SELECT *
FROM bronze.trip_data
GO

SELECT * FROM silver.trip_data_green;


EXEC silver.trip_data_silver_usp '2020', '01'
EXEC silver.trip_data_silver_usp '2020', '02'
EXEC silver.trip_data_silver_usp '2020', '03'
EXEC silver.trip_data_silver_usp '2020', '04'
EXEC silver.trip_data_silver_usp '2020', '05'
EXEC silver.trip_data_silver_usp '2020', '06'
EXEC silver.trip_data_silver_usp '2020', '07'
EXEC silver.trip_data_silver_usp '2020', '08'
EXEC silver.trip_data_silver_usp '2020', '09'
EXEC silver.trip_data_silver_usp '2020', '10'
EXEC silver.trip_data_silver_usp '2020', '11'
EXEC silver.trip_data_silver_usp '2020', '12'
EXEC silver.trip_data_silver_usp '2021', '01'
EXEC silver.trip_data_silver_usp '2021', '02'
EXEC silver.trip_data_silver_usp '2021', '03'
EXEC silver.trip_data_silver_usp '2021', '04'
EXEC silver.trip_data_silver_usp '2021', '05'
EXEC silver.trip_data_silver_usp '2021', '06'




