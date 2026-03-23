-- =========================
-- 00_setup_external_storage.sql
-- =========================

-- Schemas
IF SCHEMA_ID('silver') IS NULL EXEC('CREATE SCHEMA silver;');
IF SCHEMA_ID('gold')   IS NULL EXEC('CREATE SCHEMA gold;');
GO

-- (A) Master key (only required if you later use secrets; safe to create once)
-- IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
-- BEGIN
--   CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Strong_Passw0rd_Just_for_Demo!';
-- END
-- GO

-- (B) Managed Identity credential (works great in demo; no secrets)
IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'dsc_adls_mi')
BEGIN
  CREATE DATABASE SCOPED CREDENTIAL dsc_adls_mi
  WITH IDENTITY = 'Managed Identity';
END
GO

-- (C) External data source to your container (EDIT container/account)
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'eds_adls')
BEGIN
  CREATE EXTERNAL DATA SOURCE eds_adls
  WITH (
    TYPE = HADOOP,
    LOCATION = 'abfss://fabmigation1@synpasetofabric.dfs.core.windows.net',
    CREDENTIAL = dsc_adls_mi
  );
END
GO

-- (D) Parquet file format
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'eff_parquet')
BEGIN
  CREATE EXTERNAL FILE FORMAT eff_parquet
  WITH ( FORMAT_TYPE = PARQUET );
END
GO

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
	WITH ( FORMAT_TYPE = PARQUET)
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'fabmigration_fabmigration_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [fabmigration_fabmigration_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://fabmigation1@synpasetofabric.dfs.core.windows.net'
	)
GO