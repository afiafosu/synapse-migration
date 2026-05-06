CREATE OR REPLACE TABLE `main`.`gold`.`ProductCostHistoryRollup`
( 
	`ProductID` INT,
	`ProductName`  STRING,
	`NumCostChanges` INT,
	`MinCost`  DECIMAL(19,4)  ,
	`MaxCost`  DECIMAL(19,4)  ,
	`AvgCost`  DECIMAL(38,6)  ,
	`RowHash` binary,
	`LoadTS` TIMESTAMP  NOT NULL,
	`IsCurrent` int  NOT NULL
);
