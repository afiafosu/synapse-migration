CREATE OR REPLACE TABLE `main`.`gold`.`ProductKPIs`
( 
	`ProductID` INT,
	`ProductName`  STRING,
	`TotalOrders` INT,
	`TotalQuantitySold` INT,
	`TotalRevenue` DOUBLE,
	`AvgUnitPrice` DOUBLE,
	`MaxLineTotal` DOUBLE,
	`MinLineTotal` DOUBLE,
	`RowHash` binary,
	`LoadTS` TIMESTAMP  NOT NULL,
	`IsCurrent` int  NOT NULL
);
