-- =====================================================
-- SP: Materialize Product Review Analytics
-- Complexity: very complex
-- Aggregations + precomputed KPIs for BI
-- Tables used: ProductReview + Product
-- =====================================================

IF OBJECT_ID('gold.usp_Materialize_ProductReviewAnalytics', 'P') IS NOT NULL
    DROP PROCEDURE gold.usp_Materialize_ProductReviewAnalytics;
GO

CREATE PROCEDURE gold.usp_Materialize_ProductReviewAnalytics
AS
BEGIN
    SET NOCOUNT ON;

    -- ===============================================
    -- Watermark setup
    -- ===============================================
    IF NOT EXISTS (SELECT 1 FROM ctl.Watermark WHERE EntityName = 'ProductReviewAnalytics')
        INSERT INTO ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'ProductReviewAnalytics', NULL, SYSUTCDATETIME();

    DECLARE @lastLoad datetime2(7);
    SELECT @lastLoad = LastLoadTS FROM ctl.Watermark WHERE EntityName = 'ProductReviewAnalytics';

    -- ===============================================
    -- Drop table if exists
    -- ===============================================
    IF OBJECT_ID('gold.ProductReviewAnalytics', 'U') IS NOT NULL
        DROP TABLE gold.ProductReviewAnalytics;

    -- ===============================================
    -- CTAS: review KPIs
    -- ===============================================
    DECLARE @sql NVARCHAR(MAX) = N'
    CREATE TABLE gold.ProductReviewAnalytics
    WITH
    (
        DISTRIBUTION = HASH(ProductID),
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    SELECT
        pr.ProductID,
        p.Name AS ProductName,
        COUNT(pr.ProductReviewID) AS NumReviews,
        AVG(pr.Rating) AS AvgRating,
        SUM(CASE WHEN pr.Rating >= 4 THEN 1 ELSE 0 END) AS NumPositiveReviews,
        SUM(CASE WHEN pr.Rating <= 2 THEN 1 ELSE 0 END) AS NumNegativeReviews,
        dbo.fn_SCD_Hash_SHA256(
            CONCAT(
                CAST(pr.ProductID AS NVARCHAR(MAX)),
                CAST(COUNT(pr.ProductReviewID) AS NVARCHAR(MAX)),
                CAST(AVG(pr.Rating) AS NVARCHAR(MAX))
            )
        ) AS RowHash,
        SYSUTCDATETIME() AS LoadTS,
        1 AS IsCurrent
    FROM silver.production_ProductReview pr
    LEFT JOIN silver.production_Product p
      ON p.ProductID = pr.ProductID
    WHERE @lastLoad IS NULL OR pr.ModifiedDate > @lastLoad OR p.ModifiedDate > @lastLoad
    GROUP BY pr.ProductID, p.Name;
    ';

    EXEC sp_executesql @sql, N'@lastLoad datetime2', @lastLoad=@lastLoad;

    -- ===============================================
    -- Update Watermark
    -- ===============================================
    UPDATE ctl.Watermark
    SET LastLoadTS = SYSUTCDATETIME(),
        UpdatedAt = SYSUTCDATETIME()
    WHERE EntityName = 'ProductReviewAnalytics';
END
GO
