CREATE OR REPLACE TABLE `main`.`gold`.`ProductRevenueKPIs`
( 
	`ProductID` INT,
	`ProductName`  STRING,
	`NumTransactions` INT,
	`TotalRevenue`  DECIMAL(38,4)  ,
	`AvgUnitPrice`  DECIMAL(38,6)  ,
	`MaxUnitPrice`  DECIMAL(19,4)  ,
	`MinUnitPrice`  DECIMAL(19,4)  ,
	`RowHash` binary,
	`LoadTS` TIMESTAMP  NOT NULL,
	`IsCurrent` int  NOT NULL
);
