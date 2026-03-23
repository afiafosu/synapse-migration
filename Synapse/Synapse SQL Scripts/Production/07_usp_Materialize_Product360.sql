-- =====================================================
-- SP: Materialize Product 360
-- Complexity: very complex
-- Combines product, inventory, sales, cost, reviews, work orders, documents
-- Tables used: Production_Product, ProductInventory, TransactionHistory,
-- ProductCostHistory, ProductReview, WorkOrder, ProductDocument
-- =====================================================

IF OBJECT_ID('gold.usp_Materialize_Product360', 'P') IS NOT NULL
    DROP PROCEDURE gold.usp_Materialize_Product360;
GO

CREATE PROCEDURE gold.usp_Materialize_Product360
AS
BEGIN
    SET NOCOUNT ON;

    -- ===============================================
    -- Watermark setup
    -- ===============================================
    IF NOT EXISTS (SELECT 1 FROM ctl.Watermark WHERE EntityName = 'Product360')
        INSERT INTO ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'Product360', NULL, SYSUTCDATETIME();

    DECLARE @lastLoad datetime2(7);
    SELECT @lastLoad = LastLoadTS FROM ctl.Watermark WHERE EntityName = 'Product360';

    -- ===============================================
    -- Drop table if exists
    -- ===============================================
    IF OBJECT_ID('gold.Product360', 'U') IS NOT NULL
        DROP TABLE gold.Product360;

    -- ===============================================
    -- CTAS: build full product 360
    -- ===============================================
    DECLARE @sql NVARCHAR(MAX) = N'
    CREATE TABLE gold.Product360
    WITH
    (
        DISTRIBUTION = HASH(ProductID),
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    SELECT
        p.ProductID,
        p.Name AS ProductName,
        p.ProductNumber,
        p.StandardCost,
        p.ListPrice,
        SUM(pi.Quantity) AS TotalInventory,
        COUNT(DISTINCT th.TransactionID) AS NumTransactions,
        SUM(th.Quantity * th.ActualCost) AS TotalRevenue,
        AVG(th.ActualCost) AS AvgUnitPrice,
        COUNT(DISTINCT pr.ProductReviewID) AS NumReviews,
        AVG(pr.Rating) AS AvgRating,
        COUNT(DISTINCT wo.WorkOrderID) AS NumWorkOrders,
        MIN(pch.StandardCost) AS MinCostHistory,
        MAX(pch.StandardCost) AS MaxCostHistory,
        dbo.fn_SCD_Hash_SHA256(
            CONCAT(
                CAST(p.ProductID AS NVARCHAR(MAX)),
                CAST(SUM(pi.Quantity) AS NVARCHAR(MAX)),
                CAST(SUM(th.Quantity * th.Actualcost) AS NVARCHAR(MAX)),
                CAST(AVG(th.Actualcost) AS NVARCHAR(MAX))
            )
        ) AS RowHash,
        SYSUTCDATETIME() AS LoadTS,
        1 AS IsCurrent
    FROM silver.production_Product p
    LEFT JOIN silver.production_ProductInventory pi
      ON pi.ProductID = p.ProductID
      AND (@lastLoad IS NULL OR pi.ModifiedDate > @lastLoad)
    LEFT JOIN silver.production_TransactionHistory th
      ON th.ProductID = p.ProductID
      AND (@lastLoad IS NULL OR th.ModifiedDate > @lastLoad)
    LEFT JOIN silver.production_ProductReview pr
      ON pr.ProductID = p.ProductID
      AND (@lastLoad IS NULL OR pr.ModifiedDate > @lastLoad)
    LEFT JOIN silver.production_WorkOrder wo
      ON wo.ProductID = p.ProductID
      AND (@lastLoad IS NULL OR wo.ModifiedDate > @lastLoad)
    LEFT JOIN silver.production_ProductCostHistory pch
      ON pch.ProductID = p.ProductID
      AND (@lastLoad IS NULL OR pch.ModifiedDate > @lastLoad)
    GROUP BY p.ProductID, p.Name, p.ProductNumber, p.StandardCost, p.ListPrice;
    ';

    EXEC sp_executesql @sql, N'@lastLoad datetime2', @lastLoad=@lastLoad;

    -- ===============================================
    -- Update Watermark
    -- ===============================================
    UPDATE ctl.Watermark
    SET LastLoadTS = SYSUTCDATETIME(),
        UpdatedAt = SYSUTCDATETIME()
    WHERE EntityName = 'Product360';
END
GO
