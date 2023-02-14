DROP VIEW IF EXISTS gold.trip_data_green_vw
GO

CREATE VIEW gold.trip_data_green_vw
AS
SELECT
    result.filepath(1) AS year, 
    result.filepath(2) AS month, 
    result.*
FROM
    OPENROWSET(
        BULK 'gold/trip_data_green/year=*/month=*/*.parquet',
        DATA_SOURCE = 'nyc_taxi_src',
        FORMAT = 'PARQUET'
    ) WITH(
        borough VARCHAR(15),
        trip_date DATE,
        trip_day VARCHAR(10),
        trip_day_weekend_ind CHAR(1),
        card_trip_amount INT,
        cash_trip_amount INT,
        dispatch_trip_count INT,
        streethail_trip_count INT,
        total_trip_distance FLOAT,
        trip_duration INT,
        total_fare_amount FLOAT

    )AS [result]
GO

SELECT  *  FROM gold.trip_data_green_vw
GO

SELECT COUNT(1)
 FROM gold.trip_data_green_vw
 WHERE year = '2020' AND month= '01' 
GO