CREATE OR REPLACE TABLE `main`.`gold`.`dimsalesproduct`
( 
	`ProductID` INT,
	`Name`  STRING,
	`ProductNumber`  STRING,
	`Color`  STRING  NOT NULL,
	`StandardCost`  DECIMAL(19,4)  ,
	`ListPrice`  DECIMAL(19,4)  ,
	`Size`  STRING  NOT NULL,
	`Weight`  DECIMAL(8,2)  NOT NULL,
	`Subcategory`  STRING,
	`Category`  STRING,
	`LoadDate` TIMESTAMP  NOT NULL
);
