CREATE OR REPLACE TABLE `main`.`gold`.`dimsalescustomer`
( 
	`CustomerID` INT,
	`FirstName`  STRING,
	`LastName`  STRING,
	`preferred_email`  STRING,
	`AddressLine1`  STRING  NOT NULL,
	`City`  STRING  NOT NULL,
	`StateProvince`  STRING  NOT NULL,
	`CountryRegion`  STRING  NOT NULL,
	`LoadDate` TIMESTAMP  NOT NULL
);
