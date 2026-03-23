CREATE PROCEDURE gold.sp_gold_sales_factheader 
AS 
BEGIN
    SET NOCOUNT ON; 
    
    IF OBJECT_ID('gold.factSalesOrder') IS NOT NULL DROP TABLE gold.factSalesOrder; 
    
    CREATE TABLE gold.factSalesOrder 
    WITH
        (
            DISTRIBUTION = HASH(OrderYear),
            CLUSTERED COLUMNSTORE INDEX
        )
    AS
    WITH detailAgg AS (
        SELECT 
            SalesOrderID
            , SUM(OrderQty) AS TotalItems
            , SUM(LineTotal) AS TotalLineAmount
            , SUM(UnitPrice * OrderQty) AS GrossSales
            , SUM(UnitPrice * OrderQty * UnitPriceDiscount) AS TotalDiscount 
        FROM silver.sales_salesorderdetail 
        GROUP BY SalesOrderID)
    SELECT 
        soh.SalesOrderID
        , soh.OrderDate
        , YEAR(soh.OrderDate) AS OrderYear
        , MONTH(soh.OrderDate) AS OrderMonth
        , DATEPART(QUARTER, soh.OrderDate) AS OrderQuarter
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
        , GETDATE() AS LoadDate 
    FROM silver.sales_salesorderheader soh 
    LEFT JOIN detailAgg da ON 
        soh.SalesOrderID = da.SalesOrderID;
END;
GO