CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_gold_sales_views`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    ----------------------------------------- 
    -- vwSalesOrder 
    ----------------------------------------- 
    DROP VIEW IF EXISTS main.gold.vwSalesOrder;
    
    EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW main.gold.vwSalesOrder AS
    SELECT 
        f.*
        , c.FirstName
        , c.LastName
        , t.Name AS TerritoryName 
    FROM main.gold.factSalesOrder f 
    LEFT JOIN main.gold.dimsalescustomer c ON 
        f.CustomerID = c.CustomerID 
    LEFT JOIN main.gold.dimsalesterritory t ON 
        f.TerritoryID = t.TerritoryID;';

    ----------------------------------------- 
    -- vwSalesOrderDetail 
    ----------------------------------------- 
    DROP VIEW IF EXISTS main.gold.vwSalesOrderDetail;
    
    EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW main.gold.vwSalesOrderDetail AS
    SELECT 
        d.*
        , p.Name AS ProductName
        , p.ProductNumber 
    FROM main.gold.factSalesOrderDetail d 
    LEFT JOIN main.gold.dimsalesproduct p ON 
        d.ProductID = p.ProductID;';
END;
