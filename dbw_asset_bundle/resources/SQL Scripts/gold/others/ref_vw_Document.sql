CREATE OR REPLACE VIEW `main`.`ref`.`vw_Document`
AS 
	SELECT
		CAST(DocumentNode    AS STRING) AS DocumentNode,
		CAST(Title           AS STRING) AS Title,
		CAST(`Owner`         AS int)           AS `Owner`,
		CASE WHEN TRY_CAST(FolderFlag AS int) IN (1) THEN CAST(1 AS BOOLEAN) ELSE CAST(0 AS BOOLEAN) END AS FolderFlag,
		CAST(FileName        AS STRING)  AS FileName,
		CAST(FileExtension   AS STRING)   AS FileExtension,
		CAST(Revision        AS STRING)   AS Revision,
		CAST(ChangeNumber    AS int)            AS ChangeNumber,
		CAST(`Status`        AS smallint)       AS `Status`,
		CAST(DocumentSummary AS STRING)  AS DocumentSummary,
		CAST(rowguid         AS STRING)   AS rowguid,
		CAST(ModifiedDate    AS timestamp)   AS ModifiedDate
	FROM main.silver.production_document;
