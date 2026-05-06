CREATE OR REPLACE TABLE `main`.`config`.`tbl_Log`
( 
	`ID_Log` int  NOT NULL,
	`PipelineName`  STRING  NOT NULL,
	`PipelineRunID`  STRING  NOT NULL,
	`PipelineWorkspace`  STRING  NOT NULL,
	`StartTime` TIMESTAMP  NOT NULL,
	`EndTime` TIMESTAMP  ,
	`DurationSeconds` INT,
	`Status`  STRING  NOT NULL
);
