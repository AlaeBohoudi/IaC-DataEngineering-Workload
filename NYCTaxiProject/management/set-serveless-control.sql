
--Check data processed using servless pool
SELECT * FROM sys.dm_external_data_processed

SELECT * FROM sys.configurations
WHERE name LIKE 'Data Processed %';

--Set Daily limit in TB
EXEC sp_set_data_processed_limit
	@type = N'daily',
	@limit_tb = 1
--Set Weekly limit in TB
EXEC sp_set_data_processed_limit
    @type = N'weekly',
    @limit_tb = 2;
--Set Monthly limit in TB
EXEC sp_set_data_processed_limit
    @type = N'monthly',
    @limit_tb = 2;
--Apply UTF8 collation on database level
ALTER DATABASE nyc_taxi_discovery COLLATE Latin1_General_100_CI_AI_SC_UTF8;