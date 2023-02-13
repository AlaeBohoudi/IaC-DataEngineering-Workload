-- The same as parquet plus adding the partitions as columns, they always need to be selected. It looks always for a delta log.
SELECT TOP 100
    *
    FROM OPENROWSET(
        BULK 'trip_data_green_delta',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'DELTA'
    )WITH(
        tip_amount FLOAT,
        trip_type INT,
        year varchar(4),
        month varchar(2)
    ) 
    AS trip_data;
 



SELECT COUNT(DISTINCT payment_type)
    FROM OPENROWSET(
        BULK 'trip_data_green_delta',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'DELTA'
    )
    AS trip_data;
 
SELECT COUNT(DISTINCT payment_type)
    FROM OPENROWSET(
        BULK 'trip_data_green_delta',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'DELTA'
    )
    AS trip_data
    WHERE year = '2020' AND month= '01';