CREATE OR REPLACE TABLE `main`.`config`.`tbl_Entities`
( 
	`IDEntity` int  NOT NULL,
	`EntityName`  STRING  NOT NULL,
	`Environment`  STRING,
	`ADLSaccount`  STRING,
	`ContainerName`  STRING,
	`SourcePath`  STRING,
	`FileExtension`  STRING,
	`TargetPath`  STRING,
	`Notebook`  STRING  NOT NULL,
	`IsActive` tinyint  NOT NULL,
	`InsertedDate` TIMESTAMP  NOT NULL,
	`ModifiedDate` TIMESTAMP  NOT NULL
);
