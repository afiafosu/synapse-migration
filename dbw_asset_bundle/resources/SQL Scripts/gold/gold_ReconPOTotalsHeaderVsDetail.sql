CREATE OR REPLACE TABLE `main`.`gold`.`ReconPOTotalsHeaderVsDetail`
( 
	`PurchaseOrderID` INT,
	`HeaderSubTotal` decimal(19,4),
	`HeaderTaxAmt` decimal(19,4),
	`HeaderFreight` decimal(19,4),
	`HeaderTotalDue` decimal(19,4),
	`DetailSubTotal` decimal(38,4),
	`ExpectedTotalDue` decimal(19,4),
	`Diff_SubTotal` decimal(19,4),
	`Diff_TotalDue` decimal(19,4),
	`IsSubTotalMatch` int  NOT NULL,
	`IsTotalDueMatch` int  NOT NULL,
	`ReconStatus`  STRING  NOT NULL,
	`LoadDate` TIMESTAMP  NOT NULL
);
