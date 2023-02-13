USE nyc_taxi_logicaldw
GO

IF NOT EXISTS ( SELECT * FROM sys.external_file_formats WHERE name = 'csv_format')
  CREATE EXTERNAL FILE FORMAT csv_format  
  WITH (  
    FORMAT_TYPE = DELIMITEDTEXT, 
    FORMAT_OPTIONS ( 
      FIELD_TERMINATOR = ',',  
      STRING_DELIMITER = '"',
      FIRST_ROW = 2,
      USE_TYPE_DEFAULT = FALSE,
      ENCODING = 'UTF8',
      PARSER_VERSION = '2.0' )
    );  

--Parser version 1
IF NOT EXISTS ( SELECT * FROM sys.external_file_formats WHERE name = 'csv_format_parser_version_1')
CREATE EXTERNAL FILE FORMAT csv_format_parser_version_1  
WITH (  
    FORMAT_TYPE = DELIMITEDTEXT, 
    FORMAT_OPTIONS ( 
      FIELD_TERMINATOR = ',',  
      STRING_DELIMITER = '"',
      FIRST_ROW = 2,
      USE_TYPE_DEFAULT = FALSE,
      ENCODING = 'UTF8',
      PARSER_VERSION = '1.0' )
    );  
--TSV

IF NOT EXISTS ( SELECT * FROM sys.external_file_formats WHERE name = 'tsv_format')
  CREATE EXTERNAL FILE FORMAT tsv_format  
  WITH (  
    FORMAT_TYPE = DELIMITEDTEXT, 
    FORMAT_OPTIONS ( 
      FIELD_TERMINATOR = '\t',  
      STRING_DELIMITER = '"',
      FIRST_ROW = 2,
      USE_TYPE_DEFAULT = FALSE,
      ENCODING = 'UTF8',
      PARSER_VERSION = '2.0' )
    );  
--Parquet 
IF NOT EXISTS ( SELECT * FROM sys.external_file_formats WHERE name = 'parquet_format')
CREATE EXTERNAL FILE FORMAT parquet_format  
WITH (  
    FORMAT_TYPE = PARQUET,  
    DATA_COMPRESSION =  'org.apache.hadoop.io.compress.SnappyCodec'  
    );  

--Delta

IF NOT EXISTS ( SELECT * FROM sys.external_file_formats WHERE name = 'delta_format')
CREATE EXTERNAL FILE FORMAT delta_format  
WITH (  
    FORMAT_TYPE = DELTA,  
    DATA_COMPRESSION =  'org.apache.hadoop.io.compress.SnappyCodec'  
    );  