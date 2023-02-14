CREATE OR ALTER PROCEDURE gold.trip_data_gold_usp
@year VARCHAR(4),
@month VARCHAR(2)
AS
BEGIN
    DECLARE @create_sql_stat NVARCHAR(MAX),
            @drop_sql_stat   NVARCHAR(MAX);
    
    SET @create_sql_stat = 
    'CREATE EXTERNAL TABLE gold.trip_data_green_' + @year + '_' + @month +
    ' WITH(
        DATA_SOURCE = nyc_taxi_src,
        LOCATION = ''gold/trip_data_green/year=' + @year + '/month=' + @month + ''',
        FILE_FORMAT = parquet_format
    )
    AS 
    SELECT trip_data.year, trip_data.month, taxi_zone.borough, 
                CONVERT(DATE, trip_data.lpep_pickup_datetime) AS trip_date, 
                calendar.day_name AS trip_day,
                CASE WHEN calendar.day_name IN(''Saturday'', ''Sunday'') THEN ''Y'' ELSE ''N'' END AS trip_day_weekend_ind,
                SUM(CASE WHEN payment_type.description = ''Credit card'' THEN 1 ELSE 0 END) AS card_trip_amount,
                SUM(CASE WHEN payment_type.description = ''Cash'' THEN 1 ELSE 0 END) AS cash_trip_amount,
                SUM(CASE WHEN trip_type.trip_type_desc = ''Dispatch'' THEN 1 ELSE 0 END) AS dispatch_trip_count,
                SUM(CASE WHEN trip_type.trip_type_desc = ''Street-hail'' THEN 1 ELSE 0 END) AS streethail_trip_count,
                SUM(trip_data.trip_distance) AS total_trip_distance,
                SUM(DATEDIFF(MINUTE, trip_data.lpep_pickup_datetime, trip_data.lpep_dropoff_datetime)) AS trip_duration,
                SUM(trip_data.fare_amount) AS total_fare_amount
     FROM silver.trip_data_green_vw AS trip_data
    JOIN silver.taxi_zone AS taxi_zone ON (trip_data.pu_location_id = taxi_zone.location_id)
    JOIN silver.calendar AS calendar ON (calendar.date = CONVERT(DATE, trip_data.lpep_pickup_datetime))
    JOIN silver.payment_type AS payment_type ON (payment_type.payment_type = trip_data.payment_type)
    JOIN silver.trip_type AS trip_type ON (trip_type.trip_type = trip_data.trip_type)            
    WHERE trip_data.year = '''+ @year + '''
          AND trip_data.month = ''' + @month + '''
          GROUP BY trip_data.year, trip_data.month, taxi_zone.borough, CONVERT(DATE, trip_data.lpep_pickup_datetime),calendar.day_name';
    PRINT(@create_sql_stat)
    EXEC sp_executesql @create_sql_stat;

    SET @drop_sql_stat = 
        'DROP EXTERNAL TABLE gold.trip_data_green_' + @year+ '_' + @month ;

    PRINT(@drop_sql_stat)
    EXEC sp_executesql @drop_sql_stat;
END;