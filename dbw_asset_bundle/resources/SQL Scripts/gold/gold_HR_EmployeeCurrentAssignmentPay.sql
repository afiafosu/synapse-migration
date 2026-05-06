CREATE OR REPLACE TABLE `main`.`gold`.`HR_EmployeeCurrentAssignmentPay`
( 
	`BusinessEntityID` INT,
	`DepartmentID` INT,
	`DepartmentName`  STRING,
	`DepartmentGroupName`  STRING,
	`ShiftID` INT,
	`ShiftName`  STRING,
	`ShiftStartTimeText`  STRING,
	`ShiftEndTimeText`  STRING,
	`DeptStartDate` TIMESTAMP  ,
	`DeptEndDate` TIMESTAMP  ,
	`PayRate` decimal(19,4),
	`PayFrequency` tinyint  ,
	`RateChangeDate` TIMESTAMP  ,
	`LoadRunId` STRING,
	`LoadDate` TIMESTAMP  NOT NULL
);
