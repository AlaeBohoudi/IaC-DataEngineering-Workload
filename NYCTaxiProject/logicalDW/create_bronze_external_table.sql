USE nyc_taxi_logicaldw
GO
IF OBJECT_ID('bronze.taxi_zone') IS NOT NULL
    DROP EXTERNAL TABLE bronze.taxi_zone;
--with rejection
CREATE EXTERNAL TABLE bronze.taxi_zone 
    ( location_id SMALLINT ,
        borough VARCHAR(15) ,
        zone VARCHAR(50) ,
        service_zone  VARCHAR(15))  
    WITH (
        LOCATION = 'raw/taxi_zone.csv',  
        DATA_SOURCE = nyc_taxi_src,  
        FILE_FORMAT = csv_format_parser_version_1,
        REJECT_VALUE = 10,
        REJECTED_ROW_LOCATION = 'raw/rejections/taxizone'
    
    );
-- without rejections
IF OBJECT_ID('bronze.taxi_zone') IS NOT NULL
    DROP EXTERNAL TABLE bronze.taxi_zone;
CREATE EXTERNAL TABLE bronze.taxi_zone 
    ( location_id SMALLINT ,
        borough VARCHAR(15) ,
        zone VARCHAR(50) ,
        service_zone  VARCHAR(15))  
    WITH (
        LOCATION = 'raw/taxi_zone.csv',  
        DATA_SOURCE = nyc_taxi_src,  
        FILE_FORMAT = csv_format
    
    );
SELECT * FROM bronze.taxi_zone;


--create calendar external table
IF OBJECT_ID('bronze.calendar') IS NOT NULL
    DROP EXTERNAL TABLE bronze.calendar;

CREATE EXTERNAL TABLE bronze.calendar
    (
        date_key        INT,
        date            DATE,
        year            SMALLINT,
        month           TINYINT,
        day             TINYINT,
        day_name        VARCHAR(10),
        day_of_year     SMALLINT,
        week_of_month   TINYINT,
        week_of_year    TINYINT,
        month_name      VARCHAR(10),
        year_month      INT,
        year_week       INT
    ) 
    WITH (
        LOCATION = 'raw/calendar.csv',  
        DATA_SOURCE = nyc_taxi_src,  
        FILE_FORMAT = csv_format,
        REJECT_VALUE = 10,
        REJECTED_ROW_LOCATION = 'raw/rejections/calendar'
    
    );

SELECT * FROM bronze.calendar;


--Create Trip type TSV

IF OBJECT_ID('bronze.trip_type') IS NOT NULL
    DROP EXTERNAL TABLE bronze.trip_type;

CREATE EXTERNAL TABLE bronze.trip_type
    (
        trip_type       TINYINT,
        trip_type_desc  VARCHAR(20)
    ) 
    WITH (
        LOCATION = 'raw/trip_type.tsv',  
        DATA_SOURCE = nyc_taxi_src,  
        FILE_FORMAT = tsv_format,
    );

SELECT * FROM bronze.trip_type;
--Create Vendor external table
IF OBJECT_ID('bronze.vendor') IS NOT NULL
    DROP EXTERNAL TABLE bronze.vendor;
CREATE EXTERNAL TABLE bronze.vendor 
    ( vendor_id TINYINT ,
        vendor_name VARCHAR(50))  
    WITH (
        LOCATION = 'raw/vendor.csv',  
        DATA_SOURCE = nyc_taxi_src,  
        FILE_FORMAT = csv_format
    
    );
SELECT * FROM bronze.vendor;

--CREATE trip_Data
IF OBJECT_ID('bronze.trip_data') IS NOT NULL
    DROP EXTERNAL TABLE bronze.trip_data;
CREATE EXTERNAL TABLE bronze.trip_data( 

        VendorID INT,
        lpep_pickup_datetime datetime2(7),
        lpep_dropoff_datetime datetime2(7),
        store_and_fwd_flag CHAR(1),
        RatecodeID INT,
        PULocationID INT,
        DOLocationID INT,
        passenger_count INT,
        trip_distance FLOAT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        ehail_fee INT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        payment_type INT,
        trip_type INT,
        congestion_surcharge FLOAT)
        WITH(
             LOCATION = 'raw/trip_data_green_csv/**',  
        DATA_SOURCE = nyc_taxi_src,  
        FILE_FORMAT = csv_format
        );

SELECT * FROM bronze.trip_data;

--Create parquet trip_Data
IF OBJECT_ID('bronze.trip_data_parquet') IS NOT NULL
    DROP EXTERNAL TABLE bronze.trip_data_parquet;
CREATE EXTERNAL TABLE bronze.trip_data_parquet( 

        VendorID INT,
        lpep_pickup_datetime datetime2(7),
        lpep_dropoff_datetime datetime2(7),
        store_and_fwd_flag CHAR(1),
        RatecodeID INT,
        PULocationID INT,
        DOLocationID INT,
        passenger_count INT,
        trip_distance FLOAT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        ehail_fee INT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        payment_type INT,
        trip_type INT,
        congestion_surcharge FLOAT)
        WITH(
             LOCATION = 'raw/trip_data_green_parquet/**',  
        DATA_SOURCE = nyc_taxi_src,  
        FILE_FORMAT = parquet_format
        );

SELECT TOP 100 * FROM bronze.trip_data_parquet;


--Create delta trip_Data

IF OBJECT_ID('bronze.trip_data_delta') IS NOT NULL
    DROP EXTERNAL TABLE bronze.trip_data_delta;
CREATE EXTERNAL TABLE bronze.trip_data_delta( 

        VendorID INT,
        lpep_pickup_datetime datetime2(7),
        lpep_dropoff_datetime datetime2(7),
        store_and_fwd_flag CHAR(1),
        RatecodeID INT,
        PULocationID INT,
        DOLocationID INT,
        passenger_count INT,
        trip_distance FLOAT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        ehail_fee INT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        payment_type INT,
        trip_type INT,
        congestion_surcharge FLOAT)
        WITH(
             LOCATION = 'raw/trip_data_green_delta',  
        DATA_SOURCE = nyc_taxi_src,  
        FILE_FORMAT = delta_format
        );

SELECT TOP 100 * FROM bronze.trip_data_delta
