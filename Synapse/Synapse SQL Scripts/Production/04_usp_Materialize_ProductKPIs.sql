-- ================================================
-- SP: Materialize Product KPIs
-- Complex complexity: aggregations, rollups, precomputed metrics
-- Only uses Production schema tables
-- Implements watermark + SCD2 hash
-- ================================================

IF OBJECT_ID('gold.usp_Materialize_ProductKPIs', 'P') IS NOT NULL
    DROP PROCEDURE gold.usp_Materialize_ProductKPIs;
GO

CREATE PROCEDURE gold.usp_Materialize_ProductKPIs
AS
BEGIN
    SET NOCOUNT ON;

    -- =================================================
    -- 1) Watermark setup
    -- =================================================
    IF NOT EXISTS (SELECT 1 FROM ctl.Watermark WHERE EntityName = 'ProductKPIs')
        INSERT INTO ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT  'ProductKPIs', NULL, SYSUTCDATETIME();

    DECLARE @lastLoad datetime2(7);
    SELECT @lastLoad = LastLoadTS FROM ctl.Watermark WHERE EntityName = 'ProductKPIs';

    -- =================================================
    -- 2) Drop table if exists
    -- =================================================
    IF OBJECT_ID('gold.ProductKPIs', 'U') IS NOT NULL
        DROP TABLE gold.ProductKPIs;

    -- =================================================
    -- 3) CTAS with aggregations and KPIs
    -- =================================================
    DECLARE @sql NVARCHAR(MAX) = N'
    CREATE TABLE gold.ProductKPIs
    WITH
    (
        DISTRIBUTION = HASH(ProductID),
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    SELECT
        p.ProductID,
        p.Name AS ProductName,
        COUNT(DISTINCT sod.SalesOrderID) AS TotalOrders,
        SUM(sod.OrderQty) AS TotalQuantitySold,
        SUM(sod.LineTotal) AS TotalRevenue,
        AVG(sod.UnitPrice) AS AvgUnitPrice,
        MAX(sod.LineTotal) AS MaxLineTotal,
        MIN(sod.LineTotal) AS MinLineTotal,
        dbo.fn_SCD_Hash_SHA256(
            CONCAT(
                CAST(p.ProductID AS NVARCHAR(MAX)),
                CAST(SUM(sod.LineTotal) AS NVARCHAR(MAX)),
                CAST(COUNT(DISTINCT sod.SalesOrderID) AS NVARCHAR(MAX))
            )
        ) AS RowHash,
        SYSUTCDATETIME() AS LoadTS,
        1 AS IsCurrent
    FROM silver.production_product p
    LEFT JOIN silver.sales_salesorderdetail sod
        ON sod.ProductID = p.ProductID
    WHERE @lastLoad IS NULL OR p.ModifiedDate > @lastLoad OR sod.ModifiedDate > @lastLoad
    GROUP BY p.ProductID, p.Name;
    ';

    EXEC sp_executesql @sql, N'@lastLoad datetime2', @lastLoad=@lastLoad;

    -- =================================================
    -- 4) Update Watermark
    -- =================================================
    UPDATE ctl.Watermark
    SET LastLoadTS = SYSUTCDATETIME(),
        UpdatedAt = SYSUTCDATETIME()
    WHERE EntityName = 'ProductKPIs';
END
GO
