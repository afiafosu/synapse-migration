CREATE OR REPLACE VIEW `main`.`ref`.`vw_Product`
AS 
	SELECT
		CAST(ProductID             AS int)            AS ProductID,
		CAST(Name                  AS STRING)  AS Name,
		CAST(ProductNumber         AS STRING)   AS ProductNumber,
		CAST(ProductModelID        AS int)            AS ProductModelID,
		CAST(Color                 AS STRING)   AS Color,
		CAST(`Class`               AS STRING)    AS `Class`,
		CAST(`Style`               AS STRING)    AS `Style`,
		CAST(`Size`                AS STRING)   AS `Size`,
		CAST(SizeUnitMeasureCode   AS STRING)    AS SizeUnitMeasureCode,
		CAST(WeightUnitMeasureCode AS STRING)    AS WeightUnitMeasureCode,
		CAST(StandardCost          AS decimal(19,4))  AS StandardCost,
		CAST(ListPrice             AS decimal(19,4))  AS ListPrice,
		CAST(Weight                AS decimal(18,3))  AS Weight,
		CAST(MakeFlag              AS BOOLEAN)            AS MakeFlag,
		CAST(FinishedGoodsFlag     AS BOOLEAN)            AS FinishedGoodsFlag,
		CAST(DaysToManufacture     AS int)            AS DaysToManufacture,
		CAST(ReorderPoint          AS int)            AS ReorderPoint,
		CAST(SafetyStockLevel      AS int)            AS SafetyStockLevel,
		CAST(ModifiedDate          AS timestamp)   AS ModifiedDate
	FROM main.silver.production_product;
