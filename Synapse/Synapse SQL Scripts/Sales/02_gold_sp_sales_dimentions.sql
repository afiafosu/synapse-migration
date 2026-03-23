CREATE PROCEDURE gold.sp_gold_sales_dimensions
AS 
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------- 
    -- Customer Dimension 
    -----------------------------------------

    IF OBJECT_ID('gold.dimsalescustomer') IS NOT NULL DROP TABLE gold.dimsalescustomer; 

    CREATE TABLE gold.dimsalescustomer 
    WITH
        (
            DISTRIBUTION = HASH(CountryRegion),
            CLUSTERED COLUMNSTORE INDEX
        )
    AS
    SELECT 
        c.CustomerID
        , p.FirstName
        , p.LastName
        , p.preferred_email
        , ISNULL(p.AddressLine1, 'Unknown') AS AddressLine1
        , ISNULL(p.City, 'Unknown') AS City
        , ISNULL(p.StateProvinceCode, 'Unknown') AS StateProvince
        , ISNULL(p.CountryRegionCode, 'Unknown') AS CountryRegion
        , GETDATE() AS LoadDate
    FROM [silver].[sales_customer] c
    LEFT JOIN gold.person_360 p ON 
        c.PersonID = p.business_entity_id;

    ----------------------------------------- 
    -- Product Dimension 
    ----------------------------------------- 

    IF OBJECT_ID('gold.dimsalesproduct') IS NOT NULL DROP TABLE gold.dimsalesproduct;

    CREATE TABLE gold.dimsalesproduct 
    WITH
        (
            DISTRIBUTION = HASH(Category),
            CLUSTERED COLUMNSTORE INDEX
        )
    AS
    SELECT 
        p.ProductID
        , p.Name
        , p.ProductNumber
        , ISNULL(CASE p.Color WHEN '' THEN NULL ELSE p.Color END, 'Unknown') AS Color
        , p.StandardCost
        , p.ListPrice
        , ISNULL(CASE p.Size WHEN '' THEN NULL ELSE p.Size END, 'Unknown') AS Size
        , ISNULL(p.Weight, 0.0) AS Weight
        , sc.Name AS Subcategory
        , pc.Name AS Category
        , GETDATE() AS LoadDate 
    FROM silver.production_Product p 
    LEFT JOIN silver.production_productsubcategory sc ON 
        p.ProductSubcategoryID = sc.ProductSubcategoryID 
    LEFT JOIN silver.production_productcategory pc ON 
        sc.ProductCategoryID = pc.ProductCategoryID;

    ----------------------------------------- 
    -- Sales Territory Dimension 
    ----------------------------------------- 

    IF OBJECT_ID('gold.dimsalesterritory') IS NOT NULL DROP TABLE gold.dimsalesterritory; 

    CREATE TABLE gold.dimsalesterritory 
    WITH
        (
            DISTRIBUTION = HASH(CountryRegionCode),
            CLUSTERED COLUMNSTORE INDEX
        )
    AS
    SELECT 
        TerritoryID
        , Name
        , CountryRegionCode
        , [Group] AS TerritoryGroup
        , GETDATE() AS LoadDate 
    FROM silver.sales_salesterritory;
END;
GO