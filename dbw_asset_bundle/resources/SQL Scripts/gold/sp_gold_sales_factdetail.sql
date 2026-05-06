CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_gold_sales_factdetail`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DROP TABLE IF EXISTS main.gold.factSalesOrderDetail;
    
    CREATE OR REPLACE TABLE main.gold.factSalesOrderDetail AS
    SELECT 
        sod.SalesOrderID
        , sod.SalesOrderDetailID
        , sod.ProductID
        , sod.OrderQty
        , sod.UnitPrice
        , sod.UnitPriceDiscount
        -- Derived metrics
        , sod.OrderQty * sod.UnitPrice AS ExtendedPrice
        , sod.OrderQty * sod.UnitPrice * sod.UnitPriceDiscount AS DiscountAmount
        , sod.LineTotal AS NetLineAmount
        -- Cost + margin 
        , p.StandardCost * sod.OrderQty AS CostAmount
        , sod.LineTotal - (p.StandardCost * sod.OrderQty) AS MarginAmount
        , current_timestamp() AS LoadDate 
    FROM main.silver.sales_salesorderdetail sod 
    LEFT JOIN main.silver.production_product p ON 
        sod.ProductID = p.ProductID;
END;
