CREATE OR REPLACE VIEW `main`.`ref`.`vw_ProductDocument`
AS 
	SELECT
		CAST(ProductID    AS int)           AS ProductID,
		CAST(DocumentNode AS STRING) AS DocumentNode,
		CAST(ModifiedDate AS timestamp)  AS ModifiedDate
	FROM main.silver.production_productdocument;
