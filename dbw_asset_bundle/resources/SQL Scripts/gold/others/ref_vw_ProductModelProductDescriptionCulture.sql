CREATE OR REPLACE VIEW `main`.`ref`.`vw_ProductModelProductDescriptionCulture`
AS 
	SELECT
		CAST(ProductModelID       AS int)          AS ProductModelID,
		CAST(ProductDescriptionID AS int)          AS ProductDescriptionID,
		CAST(CultureID            AS STRING)  AS CultureID,
		CAST(ModifiedDate         AS timestamp) AS ModifiedDate
	FROM main.silver.production_productmodelproductdescriptionculture;
