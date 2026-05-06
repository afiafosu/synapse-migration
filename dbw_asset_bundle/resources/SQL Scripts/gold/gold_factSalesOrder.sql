CREATE OR REPLACE TABLE `main`.`gold`.`factSalesOrder`
( 
	`SalesOrderID` INT,
	`OrderDate` TIMESTAMP  ,
	`OrderYear` INT,
	`OrderMonth` INT,
	`OrderQuarter` INT,
	`CustomerID` INT,
	`SalesPersonID` INT,
	`TerritoryID` INT,
	`TotalItems` INT,
	`GrossSales` DOUBLE,
	`TotalDiscount` DOUBLE,
	`TotalLineAmount` DOUBLE,
	`SubTotal` DOUBLE,
	`TaxAmt` DOUBLE,
	`Freight` DOUBLE,
	`TotalDue` DOUBLE,
	`DiscountPct` DOUBLE,
	`LoadDate` TIMESTAMP  NOT NULL
);
