-- =====================================================
-- SP: Materialize Product Inventory KPIs
-- Complexity: complex
-- Aggregations and precomputed metrics for BI
-- Tables used: Production_ProductInventory + Production_Product
-- =====================================================

IF OBJECT_ID('gold.usp_Materialize_ProductInventoryKPIs', 'P') IS NOT NULL
    DROP PROCEDURE gold.usp_Materialize_ProductInventoryKPIs;
GO

CREATE PROCEDURE gold.usp_Materialize_ProductInventoryKPIs
AS
BEGIN
    SET NOCOUNT ON;

    -- ===============================================
    -- Watermark setup
    -- ===============================================
    IF NOT EXISTS (SELECT 1 FROM ctl.Watermark WHERE EntityName = 'ProductInventoryKPIs')
        INSERT INTO ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'ProductInventoryKPIs', NULL, SYSUTCDATETIME();

    DECLARE @lastLoad datetime2(7);
    SELECT @lastLoad = LastLoadTS FROM ctl.Watermark WHERE EntityName = 'ProductInventoryKPIs';

    -- ===============================================
    -- Drop table if exists
    -- ===============================================
    IF OBJECT_ID('gold.ProductInventoryKPIs', 'U') IS NOT NULL
        DROP TABLE gold.ProductInventoryKPIs;

    -- ===============================================
    -- CTAS: aggregate inventory metrics
    -- ===============================================
    DECLARE @sql NVARCHAR(MAX) = N'
    CREATE TABLE gold.ProductInventoryKPIs
    WITH
    (
        DISTRIBUTION = HASH(ProductID),
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    SELECT
        pi.ProductID,
        p.Name AS ProductName,
        SUM(pi.Quantity) AS TotalQuantity,
        AVG(pi.Quantity) AS AvgQuantityPerLocation,
        MIN(pi.Quantity) AS MinQuantity,
        MAX(pi.Quantity) AS MaxQuantity,
        dbo.fn_SCD_Hash_SHA256(
            CONCAT(
                CAST(pi.ProductID AS NVARCHAR(MAX)),
                CAST(SUM(pi.Quantity) AS NVARCHAR(MAX))
            )
        ) AS RowHash,
        SYSUTCDATETIME() AS LoadTS,
        1 AS IsCurrent
    FROM silver.production_productinventory pi
    LEFT JOIN silver.production_product p
      ON p.ProductID = pi.ProductID
    WHERE @lastLoad IS NULL OR p.ModifiedDate > @lastLoad OR pi.ModifiedDate > @lastLoad
    GROUP BY pi.ProductID, p.Name;
    ';

    EXEC sp_executesql @sql, N'@lastLoad datetime2', @lastLoad=@lastLoad;

    -- ===============================================
    -- Update Watermark
    -- ===============================================
    UPDATE ctl.Watermark
    SET LastLoadTS = SYSUTCDATETIME(),
        UpdatedAt = SYSUTCDATETIME()
    WHERE EntityName = 'ProductInventoryKPIs';
END
GO
