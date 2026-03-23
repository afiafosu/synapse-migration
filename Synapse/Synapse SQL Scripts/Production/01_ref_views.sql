/* =========================================================
   01_ref_views.sql
   Purpose : Create reference views pointing to Silver layer
             - Abstraction layer for ETL/Gold procedures
   Notes   : Keep views lightweight; do not include business logic
========================================================= */

-- Product view
IF OBJECT_ID('ref.vw_Product','V') IS NOT NULL
    DROP VIEW ref.vw_Product;
GO
CREATE VIEW ref.vw_Product AS
SELECT
    CAST(ProductID             AS int)            AS ProductID,
    CAST(Name                  AS nvarchar(200))  AS Name,
    CAST(ProductNumber         AS nvarchar(50))   AS ProductNumber,
    CAST(ProductModelID        AS int)            AS ProductModelID,
    CAST(Color                 AS nvarchar(50))   AS Color,
    CAST([Class]               AS nvarchar(5))    AS [Class],
    CAST([Style]               AS nvarchar(5))    AS [Style],
    CAST([Size]                AS nvarchar(20))   AS [Size],
    CAST(SizeUnitMeasureCode   AS nvarchar(5))    AS SizeUnitMeasureCode,
    CAST(WeightUnitMeasureCode AS nvarchar(5))    AS WeightUnitMeasureCode,
    CAST(StandardCost          AS decimal(19,4))  AS StandardCost,
    CAST(ListPrice             AS decimal(19,4))  AS ListPrice,
    CAST(Weight                AS decimal(18,3))  AS Weight,
    CAST(MakeFlag              AS bit)            AS MakeFlag,
    CAST(FinishedGoodsFlag     AS bit)            AS FinishedGoodsFlag,
    CAST(DaysToManufacture     AS int)            AS DaysToManufacture,
    CAST(ReorderPoint          AS int)            AS ReorderPoint,
    CAST(SafetyStockLevel      AS int)            AS SafetyStockLevel,
    CAST(ModifiedDate          AS datetime2(7))   AS ModifiedDate
FROM silver.production_product;
GO

-- ProductDocument view
IF OBJECT_ID('ref.vw_ProductDocument','V') IS NOT NULL
    DROP VIEW ref.vw_ProductDocument;
GO
CREATE VIEW ref.vw_ProductDocument AS
SELECT
    CAST(ProductID    AS int)           AS ProductID,
    CAST(DocumentNode AS nvarchar(200)) AS DocumentNode,
    CAST(ModifiedDate AS datetime2(7))  AS ModifiedDate
FROM silver.production_productdocument;
GO

-- Document view (without binary payload)
IF OBJECT_ID('ref.vw_Document','V') IS NOT NULL
    DROP VIEW ref.vw_Document;
GO
CREATE VIEW ref.vw_Document AS
SELECT
    CAST(DocumentNode    AS nvarchar(200)) AS DocumentNode,
    CAST(Title           AS nvarchar(200)) AS Title,
    CAST([Owner]         AS int)           AS [Owner],
    CASE WHEN TRY_CONVERT(int, FolderFlag) IN (1) THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END AS FolderFlag,
    CAST(FileName        AS nvarchar(400))  AS FileName,
    CAST(FileExtension   AS nvarchar(20))   AS FileExtension,
    CAST(Revision        AS nvarchar(10))   AS Revision,
    CAST(ChangeNumber    AS int)            AS ChangeNumber,
    CAST([Status]        AS smallint)       AS [Status],
    CAST(DocumentSummary AS nvarchar(max))  AS DocumentSummary,
    CAST(rowguid         AS nvarchar(50))   AS rowguid,
    CAST(ModifiedDate    AS datetime2(7))   AS ModifiedDate
FROM silver.production_document;
GO

-- ProductModel–ProductDescription–Culture
IF OBJECT_ID('ref.vw_ProductModelProductDescriptionCulture','V') IS NOT NULL
    DROP VIEW ref.vw_ProductModelProductDescriptionCulture;
GO
CREATE VIEW ref.vw_ProductModelProductDescriptionCulture AS
SELECT
    CAST(ProductModelID       AS int)          AS ProductModelID,
    CAST(ProductDescriptionID AS int)          AS ProductDescriptionID,
    CAST(CultureID            AS nvarchar(6))  AS CultureID,
    CAST(ModifiedDate         AS datetime2(7)) AS ModifiedDate
FROM silver.production_productmodelproductdescriptionculture;
GO
