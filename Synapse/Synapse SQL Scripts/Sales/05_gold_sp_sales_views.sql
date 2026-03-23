CREATE PROCEDURE gold.sp_gold_sales_views 
AS 
BEGIN
    SET NOCOUNT ON; 

    ----------------------------------------- 
    -- vwSalesOrder 
    ----------------------------------------- 

    IF OBJECT_ID('gold.vwSalesOrder') IS NOT NULL DROP VIEW gold.vwSalesOrder;

    EXEC('CREATE VIEW gold.vwSalesOrder AS
    SELECT 
        f.*
        , c.FirstName
        , c.LastName
        , t.Name AS TerritoryName 
    FROM gold.factSalesOrder f 
    LEFT JOIN gold.dimsalescustomer c ON 
        f.CustomerID = c.CustomerID 
    LEFT JOIN gold.dimsalesterritory t ON 
        f.TerritoryID = t.TerritoryID;')

    ----------------------------------------- 
    -- vwSalesOrderDetail 
    ----------------------------------------- 

    IF OBJECT_ID('gold.vwSalesOrderDetail') IS NOT NULL DROP VIEW gold.vwSalesOrderDetail; 

    EXEC('CREATE VIEW gold.vwSalesOrderDetail AS
    SELECT 
        d.*
        , p.Name AS ProductName
        , p.ProductNumber 
    FROM gold.factSalesOrderDetail d 
    LEFT JOIN gold.dimsalesproduct p ON 
        d.ProductID = p.ProductID;')
END;
GO