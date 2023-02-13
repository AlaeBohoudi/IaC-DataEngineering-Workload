
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

