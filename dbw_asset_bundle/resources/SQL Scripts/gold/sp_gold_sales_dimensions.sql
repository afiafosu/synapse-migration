CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_gold_sales_dimensions`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN
    ----------------------------------------- 
    -- Customer Dimension 
    -----------------------------------------
    DROP TABLE IF EXISTS main.gold.dimsalescustomer;
    
    CREATE OR REPLACE TABLE main.gold.dimsalescustomer AS
    SELECT 
        c.CustomerID
        , p.FirstName
        , p.LastName
        , p.preferred_email
        , COALESCE(p.AddressLine1, 'Unknown') AS AddressLine1
        , COALESCE(p.City, 'Unknown') AS City
        , COALESCE(p.StateProvinceCode, 'Unknown') AS StateProvince
        , COALESCE(p.CountryRegionCode, 'Unknown') AS CountryRegion
        , current_timestamp() AS LoadDate
    FROM main.silver.sales_customer c
    LEFT JOIN main.gold.person_360 p ON 
        c.PersonID = p.business_entity_id;
        
    ----------------------------------------- 
    -- Product Dimension 
    ----------------------------------------- 
    DROP TABLE IF EXISTS main.gold.dimsalesproduct;
    
    CREATE OR REPLACE TABLE main.gold.dimsalesproduct AS
    SELECT 
        p.ProductID
        , p.Name
        , p.ProductNumber
        , COALESCE(CASE p.Color WHEN '' THEN NULL ELSE p.Color END, 'Unknown') AS Color
        , p.StandardCost
        , p.ListPrice
        , COALESCE(CASE p.Size WHEN '' THEN NULL ELSE p.Size END, 'Unknown') AS Size
        , COALESCE(p.Weight, 0.0) AS Weight
        , sc.Name AS Subcategory
        , pc.Name AS Category
        , current_timestamp() AS LoadDate 
    FROM main.silver.production_Product p 
    LEFT JOIN main.silver.production_productsubcategory sc ON 
        p.ProductSubcategoryID = sc.ProductSubcategoryID 
    LEFT JOIN main.silver.production_productcategory pc ON 
        sc.ProductCategoryID = pc.ProductCategoryID;
    
    ----------------------------------------- 
    -- Sales Territory Dimension 
    ----------------------------------------- 
    DROP TABLE IF EXISTS main.gold.dimsalesterritory;
    
    CREATE OR REPLACE TABLE main.gold.dimsalesterritory AS
    SELECT 
        TerritoryID
        , Name
        , CountryRegionCode
        , `Group` AS TerritoryGroup
        , current_timestamp() AS LoadDate 
    FROM main.silver.sales_salesterritory;
END;
