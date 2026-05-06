CREATE OR REPLACE TABLE `main`.`gold`.`DimProduct`
( 
	`ProductID` INT,
	`Name`  STRING,
	`ProductNumber`  STRING,
	`ProductModelID` INT,
	`Color`  STRING,
	`Class`  STRING,
	`Style`  STRING,
	`Size`  STRING,
	`SizeUnitMeasureCode`  STRING,
	`WeightUnitMeasureCode`  STRING,
	`StandardCost` decimal(19,4),
	`ListPrice` decimal(19,4),
	`Weight` decimal(18,3),
	`MakeFlag` BOOLEAN,
	`FinishedGoodsFlag` BOOLEAN,
	`DaysToManufacture` INT,
	`ReorderPoint` INT,
	`SafetyStockLevel` INT,
	`ModifiedDate` TIMESTAMP  ,
	`LoadTS` TIMESTAMP  NOT NULL
);
