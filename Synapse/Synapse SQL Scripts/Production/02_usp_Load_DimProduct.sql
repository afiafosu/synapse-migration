-- ================================================
-- SP: Load gold.DimProduct (incremental by ModifiedDate)
-- Source: ref.vw_Product
-- ================================================

IF OBJECT_ID('gold.usp_Load_DimProduct', 'P') IS NOT NULL
    DROP PROCEDURE gold.usp_Load_DimProduct;
GO

CREATE PROCEDURE gold.usp_Load_DimProduct
AS
BEGIN
    SET NOCOUNT ON;

    -- Watermark init (Dedicated pool: avoid VALUES with functions)
    IF NOT EXISTS (SELECT 1 FROM ctl.Watermark WHERE EntityName = 'DimProduct')
    BEGIN
        INSERT INTO ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'DimProduct', NULL, SYSUTCDATETIME();
    END

    DECLARE @lastLoad datetime2(7);
    SELECT @lastLoad = LastLoadTS
    FROM ctl.Watermark
    WHERE EntityName = 'DimProduct';

    -- Recreate table (CTAS)
    IF OBJECT_ID('gold.DimProduct', 'U') IS NOT NULL
        DROP TABLE gold.DimProduct;

    DECLARE @sql nvarchar(max) = N'
    CREATE TABLE gold.DimProduct
    WITH
    (
        DISTRIBUTION = HASH(ProductID),
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    SELECT
        p.ProductID,
        p.Name,
        p.ProductNumber,
        p.ProductModelID,
        p.Color,
        p.[Class],
        p.[Style],
        p.[Size],
        p.SizeUnitMeasureCode,
        p.WeightUnitMeasureCode,
        p.StandardCost,
        p.ListPrice,
        p.Weight,
        p.MakeFlag,
        p.FinishedGoodsFlag,
        p.DaysToManufacture,
        p.ReorderPoint,
        p.SafetyStockLevel,
        p.ModifiedDate,
        SYSUTCDATETIME() AS LoadTS
    FROM ref.vw_Product p
    WHERE @lastLoad IS NULL OR p.ModifiedDate > @lastLoad;
    ';

    EXEC sp_executesql
        @sql,
        N'@lastLoad datetime2(7)',
        @lastLoad = @lastLoad;

    -- Update watermark
    UPDATE ctl.Watermark
    SET LastLoadTS = SYSUTCDATETIME(),
        UpdatedAt  = SYSUTCDATETIME()
    WHERE EntityName = 'DimProduct';
END
GO