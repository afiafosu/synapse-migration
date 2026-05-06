CREATE OR REPLACE VIEW `main`.`gold`.`vwSalesOrderDetail`
AS 
	SELECT 
        d.*
        , p.Name AS ProductName
        , p.ProductNumber 
    FROM main.gold.factSalesOrderDetail d 
    LEFT JOIN main.gold.dimsalesproduct p ON 
        d.ProductID = p.ProductID;
