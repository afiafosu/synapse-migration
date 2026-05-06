CREATE OR REPLACE VIEW `main`.`gold`.`vwSalesOrder`
AS 
	SELECT 
        f.*
        , c.FirstName
        , c.LastName
        , t.Name AS TerritoryName 
    FROM main.gold.factSalesOrder f 
    LEFT JOIN main.gold.dimsalescustomer c ON 
        f.CustomerID = c.CustomerID 
    LEFT JOIN main.gold.dimsalesterritory t ON 
        f.TerritoryID = t.TerritoryID;
