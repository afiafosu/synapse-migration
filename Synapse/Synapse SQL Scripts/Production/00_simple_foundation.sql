/* =========================================================
   00_simple_foundation.sql
   Purpose : Bootstrap foundational objects for Gold layer
             - Schemas: gold, ctl, ref
             - Watermark table (for ETL incremental loads)
             - SHA2_256 deterministic hash function (SCD2)
   Notes   : Run once per environment
========================================================= */

-- 1) Create schemas if they do not exist
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold') EXEC('CREATE SCHEMA gold');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ctl')  EXEC('CREATE SCHEMA ctl');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ref')  EXEC('CREATE SCHEMA ref');
GO

-- 2) Create watermark table for ETL tracking
IF OBJECT_ID('ctl.Watermark','U') IS NOT NULL
    DROP TABLE ctl.Watermark;
GO

CREATE TABLE ctl.Watermark
(
    EntityName        sysname       NOT NULL,        -- Name of the entity/table
    LastLoadTS        datetime2(7)  NULL,           -- Last incremental load timestamp
    LastFullRefreshTS datetime2(7)  NULL,           -- Last full load timestamp
    UpdatedAt         datetime2(7)  NOT NULL        -- Last update timestamp
)
WITH (DISTRIBUTION = REPLICATE, HEAP);              -- Replicated table for small control data
GO

-- Initialize with no rows (optional default)
UPDATE ctl.Watermark SET UpdatedAt = sysdatetime() WHERE 1=0;
GO

-- 3) Create deterministic hash function for SCD Type 2
IF OBJECT_ID('dbo.fn_SCD_Hash_SHA256','FN') IS NOT NULL
    DROP FUNCTION dbo.fn_SCD_Hash_SHA256;
GO

CREATE FUNCTION dbo.fn_SCD_Hash_SHA256(@payload NVARCHAR(MAX))
RETURNS VARBINARY(32)
AS
BEGIN
    -- Returns SHA2_256 hash of the string payload
    RETURN HASHBYTES('SHA2_256', CONVERT(varbinary(max), @payload));
END
GO
