CREATE OR REPLACE TABLE `main`.`gold`.`FactVendorSpendMonthlyAgg`
( 
	`VendorBusinessEntityID` INT,
	`MonthStartDate` TIMESTAMP  ,
	`VendorAccountNumber`  STRING,
	`VendorName`  STRING,
	`VendorCreditRating` INT,
	`PreferredVendorStatus` BOOLEAN,
	`VendorActiveFlag` BOOLEAN,
	`PurchasingWebServiceURL`  STRING,
	`VendorModifiedDate` TIMESTAMP  ,
	`TotalSpend` decimal(38,4),
	`POCount` INT,
	`LineCount` INT,
	`AvgLineAmount` decimal(38,6),
	`MinLineAmount` decimal(19,4),
	`MaxLineAmount` decimal(19,4),
	`LoadDate` TIMESTAMP  NOT NULL
);
