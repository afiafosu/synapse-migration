CREATE PROCEDURE gold.sp_gold_sales_factdetail 
AS 
BEGIN 
    SET NOCOUNT ON; 
    
    IF OBJECT_ID('gold.factSalesOrderDetail') IS NOT NULL DROP TABLE gold.factSalesOrderDetail; 
    
    CREATE TABLE gold.factSalesOrderDetail 
    WITH
        (
            DISTRIBUTION = HASH(SalesOrderID),
            CLUSTERED COLUMNSTORE INDEX
        )
    AS
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
        , GETDATE() AS LoadDate 
    FROM silver.sales_salesorderdetail sod 
    LEFT JOIN silver.production_product p ON 
        sod.ProductID = p.ProductID;
END;
GO