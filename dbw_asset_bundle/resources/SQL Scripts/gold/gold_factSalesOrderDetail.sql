CREATE OR REPLACE TABLE `main`.`gold`.`factSalesOrderDetail`
( 
	`SalesOrderID` INT,
	`SalesOrderDetailID` INT,
	`ProductID` INT,
	`OrderQty` INT,
	`UnitPrice` DOUBLE,
	`UnitPriceDiscount` DOUBLE,
	`ExtendedPrice` DOUBLE,
	`DiscountAmount` DOUBLE,
	`NetLineAmount` DOUBLE,
	`CostAmount`  DECIMAL(30,4)  ,
	`MarginAmount` DOUBLE,
	`LoadDate` TIMESTAMP  NOT NULL
);
