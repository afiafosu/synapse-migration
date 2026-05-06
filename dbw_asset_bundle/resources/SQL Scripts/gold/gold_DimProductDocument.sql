CREATE OR REPLACE TABLE `main`.`gold`.`DimProductDocument`
( 
	`ProductID` INT,
	`ProductName`  STRING,
	`DocumentNode`  STRING,
	`DocumentTitle`  STRING,
	`FileName`  STRING,
	`FileExtension`  STRING,
	`Revision`  STRING,
	`RowHash` binary,
	`LoadTS` TIMESTAMP  NOT NULL,
	`IsCurrent` int  NOT NULL
);
