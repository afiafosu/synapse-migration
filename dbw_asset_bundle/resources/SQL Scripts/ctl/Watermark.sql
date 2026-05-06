CREATE OR REPLACE TABLE `main`.`ctl`.`Watermark`
( 
	`EntityName` STRING  NOT NULL,
	`LastLoadTS` TIMESTAMP  ,
	`LastFullRefreshTS` TIMESTAMP  ,
	`UpdatedAt` TIMESTAMP  NOT NULL
);
