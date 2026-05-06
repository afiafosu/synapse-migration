CREATE OR REPLACE TABLE `main`.`gold`.`stg_sales_part1`
( 
	`SalesOrderID` INT,
	`SalesOrderDetailID` INT,
	`OrderDate` TIMESTAMP  ,
	`CustomerID` INT,
	`TerritoryID` INT,
	`ProductID` INT,
	`OrderQty` INT,
	`UnitPrice` DOUBLE,
	`UnitPriceDiscount` DOUBLE,
	`LineTotal` DOUBLE,
	`OrderYear` INT,
	`OrderMonth` INT,
	`NetRevenue` DOUBLE
);
