USE master
GO

CREATE DATABASE nyc_taxi_logicaldw
GO

ALTER DATABASE nyc_taxi_logicaldw COLLATE Latin1_General_100_CI_AI_SC_UTF8
GO

USE nyc_taxi_logicaldw
GO

CREATE SCHEMA bronze
GO

CREATE SCHEMA silver
GO

CREATE SCHEMA gold
GO