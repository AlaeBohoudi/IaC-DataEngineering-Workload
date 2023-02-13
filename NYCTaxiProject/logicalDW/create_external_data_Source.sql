USE nyc_taxi_logicaldw
GO
IF NOT EXISTS ( SELECT * FROM sys.external_data_sources WHERE name = 'nyc_taxi_src')
CREATE EXTERNAL DATA SOURCE nyc_taxi_src
WITH
(    LOCATION         = 'abfss://nyc-taxi-data@wsmaindl.dfs.core.windows.net'
     
);