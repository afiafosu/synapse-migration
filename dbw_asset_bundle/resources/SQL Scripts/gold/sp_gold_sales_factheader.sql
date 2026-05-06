CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_gold_sales_factheader`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DROP TABLE IF EXISTS main.gold.factSalesOrder;
    
    CREATE OR REPLACE TABLE main.gold.factSalesOrder AS
    WITH detailAgg AS (
        SELECT 
            SalesOrderID
            , SUM(OrderQty) AS TotalItems
            , SUM(LineTotal) AS TotalLineAmount
            , SUM(UnitPrice * OrderQty) AS GrossSales
            , SUM(UnitPrice * OrderQty * UnitPriceDiscount) AS TotalDiscount 
        FROM main.silver.sales_salesorderdetail 
        GROUP BY SalesOrderID)
    SELECT 
        soh.SalesOrderID
        , soh.OrderDate
        , YEAR(soh.OrderDate) AS OrderYear
        , MONTH(soh.OrderDate) AS OrderMonth
        , EXTRACT(QUARTER from soh.OrderDate) AS OrderQuarter
        , soh.CustomerID
        , soh.SalesPersonID
        , soh.TerritoryID
        , da.TotalItems
        , da.GrossSales
        , da.TotalDiscount
        , da.TotalLineAmount
        , soh.SubTotal
        , soh.TaxAmt
        , soh.Freight
        , soh.TotalDue
        , CASE 
            WHEN da.GrossSales = 0 THEN 0 
            ELSE da.TotalDiscount / da.GrossSales END AS DiscountPct
        , current_timestamp() AS LoadDate 
    FROM main.silver.sales_salesorderheader soh 
    LEFT JOIN detailAgg da ON 
        soh.SalesOrderID = da.SalesOrderID;
END;
