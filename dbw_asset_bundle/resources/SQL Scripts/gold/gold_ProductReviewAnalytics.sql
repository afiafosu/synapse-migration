CREATE OR REPLACE TABLE `main`.`gold`.`ProductReviewAnalytics`
( 
	`ProductID` INT,
	`ProductName`  STRING,
	`NumReviews` INT,
	`AvgRating` INT,
	`NumPositiveReviews` INT,
	`NumNegativeReviews` INT,
	`RowHash` binary,
	`LoadTS` TIMESTAMP  NOT NULL,
	`IsCurrent` int  NOT NULL
);
