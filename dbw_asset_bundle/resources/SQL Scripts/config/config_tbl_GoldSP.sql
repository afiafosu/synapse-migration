CREATE OR REPLACE TABLE `main`.`config`.`tbl_GoldSP`
( 
	`ID_GoldSP` int  NOT NULL,
	`ProcessName`  STRING  NOT NULL,
	`SPName`  STRING  NOT NULL,
	`IsActive` tinyint  NOT NULL,
	`InsertedDate` TIMESTAMP  NOT NULL,
	`ModifiedDate` TIMESTAMP  NOT NULL
);
