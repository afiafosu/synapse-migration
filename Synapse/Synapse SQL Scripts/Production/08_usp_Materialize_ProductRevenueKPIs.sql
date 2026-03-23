-- =====================================================
-- SP: Materialize Product Revenue KPIs
-- Complexity: very complex
-- Rollups + aggregations for BI dashboards
-- Tables used: TransactionHistory + Product + Location
-- =====================================================

IF OBJECT_ID('gold.usp_Materialize_ProductRevenueKPIs', 'P') IS NOT NULL
    DROP PROCEDURE gold.usp_Materialize_ProductRevenueKPIs;
GO

CREATE PROCEDURE gold.usp_Materialize_ProductRevenueKPIs
AS
BEGIN
    SET NOCOUNT ON;

    -- ===============================================
    -- Watermark setup
    -- ===============================================
    IF NOT EXISTS (SELECT 1 FROM ctl.Watermark WHERE EntityName = 'ProductRevenueKPIs')
        INSERT INTO ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'ProductRevenueKPIs', NULL, SYSUTCDATETIME();

    DECLARE @lastLoad datetime2(7);
    SELECT @lastLoad = LastLoadTS FROM ctl.Watermark WHERE EntityName = 'ProductRevenueKPIs';

    -- ===============================================
    -- Drop table if exists
    -- ===============================================
    IF OBJECT_ID('gold.ProductRevenueKPIs', 'U') IS NOT NULL
        DROP TABLE gold.ProductRevenueKPIs;

    -- ===============================================
    -- CTAS: Revenue KPIs
    -- ===============================================
    DECLARE @sql NVARCHAR(MAX) = N'
    CREATE TABLE gold.ProductRevenueKPIs
    WITH
    (
        DISTRIBUTION = HASH(ProductID),
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    SELECT
        th.ProductID,
        p.Name AS ProductName,
        COUNT(DISTINCT th.TransactionID) AS NumTransactions,
        SUM(th.Quantity * th.ActualCost) AS TotalRevenue,
        AVG(th.ActualCost) AS AvgUnitPrice,
        MAX(th.ActualCost) AS MaxUnitPrice,
        MIN(th.ActualCost) AS MinUnitPrice,
        dbo.fn_SCD_Hash_SHA256(
            CONCAT(
                CAST(th.ProductID AS NVARCHAR(MAX)),                
                CAST(SUM(th.Quantity * th.ActualCost) AS NVARCHAR(MAX))
            )
        ) AS RowHash,
        SYSUTCDATETIME() AS LoadTS,
        1 AS IsCurrent
    FROM silver.production_TransactionHistory th
    LEFT JOIN silver.production_Product p
      ON p.ProductID = th.ProductID
    WHERE @lastLoad IS NULL OR th.ModifiedDate > @lastLoad OR p.ModifiedDate > @lastLoad
    GROUP BY th.ProductID, p.Name;
    ';

    EXEC sp_executesql @sql, N'@lastLoad datetime2', @lastLoad=@lastLoad;

    -- ===============================================
    -- Update Watermark
    -- ===============================================
    UPDATE ctl.Watermark
    SET LastLoadTS = SYSUTCDATETIME(),
        UpdatedAt = SYSUTCDATETIME()
    WHERE EntityName = 'ProductRevenueKPIs';
END
GO
