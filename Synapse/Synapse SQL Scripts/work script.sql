SELECT COUNT(SalesOrderID), MIN(SalesOrderID), MAX(SalesOrderID) FROM [silver].[sales_salesorderheader]

WITH CTE AS (
    SELECT SalesOrderID, COUNT(SalesOrderID) AS [RowCount] FROM [silver].[sales_salesorderheader]
    GROUP BY SalesOrderID
    HAVING COUNT(SalesOrderID) > 1)
SELECT COUNT(*) FROM CTE

SELECT * FROM [silver].[sales_salesorderheader]
WHERE SalesOrderID = 43696

SELECT DISTINCT * INTO [silver].[sales_salesorderheader_fix] FROM [silver].[sales_salesorderheader]

--31464/2= 15732.5 -- MIN 43,659  MID 59391  MAX 75123

SELECT COUNT(SalesOrderID), MIN(SalesOrderID), MAX(SalesOrderID) FROM [silver].[sales_salesorderheader_fix]

SELECT COUNT(SalesOrderID) FROM [silver].[sales_salesorderheader_fix]
WHERE [SalesOrderID] BETWEEN 43659 AND 59391

SELECT COUNT(SalesOrderID) FROM [silver].[sales_salesorderheader_fix]
WHERE [SalesOrderID] > 59391

SELECT * INTO [silver].[salesorderheader] FROM [silver].[sales_salesorderheader_fix]
WHERE [SalesOrderID] BETWEEN 43659 AND 59391

INSERT INTO [silver].[salesorderheader]
SELECT *  FROM [silver].[sales_salesorderheader_fix]
WHERE [SalesOrderID] > 59391

SELECT COUNT(*) FROM [silver].[salesorderheader]