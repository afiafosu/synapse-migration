-- ================================================
-- SP: Load DimProduct with associated Document metadata
-- Moderate complexity: joins between Product and Document
-- Implements watermark and SCD2 safe logic
-- ================================================

IF OBJECT_ID('gold.usp_Load_DimProductWithDocument', 'P') IS NOT NULL
    DROP PROCEDURE gold.usp_Load_DimProductWithDocument;
GO

CREATE PROCEDURE gold.usp_Load_DimProductWithDocument
AS
BEGIN
    SET NOCOUNT ON;

    -- =================================================
    -- 1) Watermark setup
    -- =================================================
    IF NOT EXISTS (SELECT 1 FROM ctl.Watermark WHERE EntityName = 'DimProductDocument')
        INSERT INTO ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'DimProductDocument', NULL, SYSUTCDATETIME();

    DECLARE @lastLoad datetime2(7);
    SELECT @lastLoad = LastLoadTS FROM ctl.Watermark WHERE EntityName = 'DimProductDocument';

    -- =================================================
    -- 2) Drop table if exists
    -- =================================================
    IF OBJECT_ID('gold.DimProductDocument', 'U') IS NOT NULL
        DROP TABLE gold.DimProductDocument;

    -- =================================================
    -- 3) CTAS with join and SCD2 hash
    -- =================================================
    DECLARE @sql NVARCHAR(MAX) = N'
    CREATE TABLE gold.DimProductDocument
    WITH
    (
        DISTRIBUTION = HASH(ProductID),
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    SELECT
        p.ProductID,
        p.Name AS ProductName,
        d.DocumentNode,
        d.Title AS DocumentTitle,
        d.FileName,
        d.FileExtension,
        d.Revision,
        dbo.fn_SCD_Hash_SHA256(
            CONCAT(
                CAST(p.ProductID AS NVARCHAR(MAX)),
                p.Name,
                d.DocumentNode,
                d.Title,
                d.FileName
            )
        ) AS RowHash,
        SYSUTCDATETIME() AS LoadTS,
        1 AS IsCurrent
    FROM ref.vw_Product p
    LEFT JOIN ref.vw_ProductDocument pd
        ON pd.ProductID = p.ProductID
    LEFT JOIN ref.vw_Document d
        ON d.DocumentNode = pd.DocumentNode
    WHERE @lastLoad IS NULL OR p.ModifiedDate > @lastLoad OR d.ModifiedDate > @lastLoad;
    ';

    EXEC sp_executesql @sql, N'@lastLoad datetime2', @lastLoad=@lastLoad;

    -- =================================================
    -- 4) Update Watermark
    -- =================================================
    UPDATE ctl.Watermark
    SET LastLoadTS = SYSUTCDATETIME(),
        UpdatedAt = SYSUTCDATETIME()
    WHERE EntityName = 'DimProductDocument';
END
GO
