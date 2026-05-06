CREATE OR REPLACE TABLE `main`.`gold`.`etl_execution_log`
( 
	`ExecutionID` BIGINT,
	`EntityName`  STRING,
	`StartTime` TIMESTAMP  ,
	`EndTime` TIMESTAMP  ,
	`RowsProcessed` BIGINT,
	`Status`  STRING,
	`ErrorMessage`  STRING
);
