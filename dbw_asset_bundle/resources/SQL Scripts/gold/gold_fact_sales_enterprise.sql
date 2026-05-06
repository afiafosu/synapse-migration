CREATE OR REPLACE TABLE `main`.`gold`.`fact_sales_enterprise`
( 
	`ProductID` INT,
	`OrderYear` INT,
	`OrderMonth` INT,
	`TotalQty` INT,
	`TotalRevenue` DOUBLE,
	`RunningRevenue` DOUBLE,
	`RevenueRank` INT,
	`LoadDate` TIMESTAMP  
);
