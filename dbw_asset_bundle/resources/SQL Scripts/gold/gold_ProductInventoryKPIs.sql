CREATE OR REPLACE TABLE `main`.`gold`.`ProductInventoryKPIs`
( 
	`ProductID` INT,
	`ProductName`  STRING,
	`TotalQuantity` INT,
	`AvgQuantityPerLocation` INT,
	`MinQuantity` INT,
	`MaxQuantity` INT,
	`RowHash` binary,
	`LoadTS` TIMESTAMP  NOT NULL,
	`IsCurrent` int  NOT NULL
);
