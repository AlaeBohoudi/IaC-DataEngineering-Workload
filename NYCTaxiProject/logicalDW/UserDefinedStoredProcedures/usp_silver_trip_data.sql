CREATE OR ALTER PROCEDURE silver.trip_data_silver_usp
@year VARCHAR(4),
@month VARCHAR(2)
AS
BEGIN
    DECLARE @create_sql_stat NVARCHAR(MAX),
            @drop_sql_stat   NVARCHAR(MAX);
    
    SET @create_sql_stat = 
    'CREATE EXTERNAL TABLE silver.trip_data_green_' + @year + '_' + @month +
    ' WITH(
        DATA_SOURCE = nyc_taxi_src,
        LOCATION = ''silver/trip_data_green/year=' + @year + '/month=' + @month + ''',
        FILE_FORMAT = parquet_format
    )
    AS 
    SELECT VendorID AS vendor_id,
        lpep_pickup_datetime ,
        lpep_dropoff_datetime ,
        store_and_fwd_flag ,
        RatecodeID  AS rate_code_id,
        PULocationID AS pu_location_id,
        DOLocationID AS do_location_id,
        passenger_count ,
        trip_distance ,
        fare_amount ,
        extra ,
        mta_tax ,
        tip_amount ,
        tolls_amount ,
        ehail_fee ,
        improvement_surcharge ,
        total_amount ,
        payment_type ,
        trip_type,
        congestion_surcharge
    FROM bronze.trip_data_green_csv
    WHERE year = '''+ @year + '''
          AND month = ''' + @month + ''''
    PRINT(@create_sql_stat)
    EXEC sp_executesql @create_sql_stat;

    SET @drop_sql_stat = 
        'DROP EXTERNAL TABLE silver.trip_data_green_' + @year+ '_' + @month ;

    PRINT(@drop_sql_stat)
    EXEC sp_executesql @drop_sql_stat;
END;