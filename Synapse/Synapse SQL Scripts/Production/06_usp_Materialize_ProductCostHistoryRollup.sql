-- =====================================================
-- SP: Materialize Product Cost History Rollup
-- Complexity: complex
-- Rollups for BI, using Production_ProductCostHistory
-- =====================================================

IF OBJECT_ID('gold.usp_Materialize_ProductCostHistoryRollup', 'P') IS NOT NULL
    DROP PROCEDURE gold.usp_Materialize_ProductCostHistoryRollup;
GO

CREATE PROCEDURE gold.usp_Materialize_ProductCostHistoryRollup
AS
BEGIN
    SET NOCOUNT ON;

    -- ===============================================
    -- Watermark setup
    -- ===============================================
    IF NOT EXISTS (SELECT 1 FROM ctl.Watermark WHERE EntityName = 'ProductCostHistoryRollup')
        INSERT INTO ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'ProductCostHistoryRollup', NULL, SYSUTCDATETIME();

    DECLARE @lastLoad datetime2(7);
    SELECT @lastLoad = LastLoadTS FROM ctl.Watermark WHERE EntityName = 'ProductCostHistoryRollup';

    -- ===============================================
    -- Drop table if exists
    -- ===============================================
    IF OBJECT_ID('gold.ProductCostHistoryRollup', 'U') IS NOT NULL
        DROP TABLE gold.ProductCostHistoryRollup;

    -- ===============================================
    -- CTAS: Rollup by ProductID
    -- ===============================================
    DECLARE @sql NVARCHAR(MAX) = N'
    CREATE TABLE gold.ProductCostHistoryRollup
    WITH
    (
        DISTRIBUTION = HASH(ProductID),
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    SELECT
        pch.ProductID,
        p.Name AS ProductName,
        COUNT(*) AS NumCostChanges,
        MIN(pch.StandardCost) AS MinCost,
        MAX(pch.StandardCost) AS MaxCost,
        AVG(pch.StandardCost) AS AvgCost,
        dbo.fn_SCD_Hash_SHA256(
            CONCAT(
                CAST(pch.ProductID AS NVARCHAR(MAX)),
                CAST(MIN(pch.StandardCost) AS NVARCHAR(MAX)),
                CAST(MAX(pch.StandardCost) AS NVARCHAR(MAX))
            )
        ) AS RowHash,
        SYSUTCDATETIME() AS LoadTS,
        1 AS IsCurrent
    FROM silver.production_productcosthistory pch
    LEFT JOIN silver.production_product p
      ON p.ProductID = pch.ProductID
    WHERE @lastLoad IS NULL OR p.ModifiedDate > @lastLoad OR pch.ModifiedDate > @lastLoad
    GROUP BY pch.ProductID, p.Name;
    ';

    EXEC sp_executesql @sql, N'@lastLoad datetime2', @lastLoad=@lastLoad;

    -- ===============================================
    -- Update Watermark
    -- ===============================================
    UPDATE ctl.Watermark
    SET LastLoadTS = SYSUTCDATETIME(),
        UpdatedAt = SYSUTCDATETIME()
    WHERE EntityName = 'ProductCostHistoryRollup';
END
GO
