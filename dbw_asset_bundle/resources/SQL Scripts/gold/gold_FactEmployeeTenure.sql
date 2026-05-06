CREATE OR REPLACE TABLE `main`.`gold`.`FactEmployeeTenure`
( 
	`BusinessEntityID` INT,
	`TenureStartDate` TIMESTAMP  ,
	`AsOfDate` TIMESTAMP  ,
	`TenureDays` INT,
	`TenureYears`  DECIMAL(27,10)  ,
	`IsActive` int  NOT NULL,
	`DepartmentID` INT,
	`DepartmentName`  STRING,
	`DepartmentGroupName`  STRING,
	`ShiftID` INT,
	`ShiftName`  STRING,
	`LoadDate` TIMESTAMP  NOT NULL
);
