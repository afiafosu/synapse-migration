CREATE OR REPLACE PROCEDURE `main`.`config`.`GetMetadata`(
IN V_environment STRING)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    SELECT 
          ADLSaccount
        , ContainerName AS containerName
        , SourcePath AS sourceFolder
        , TargetPath AS targetFolder
        , Environment AS targetEnv
        , EntityName AS entityName
        , FileExtension AS fileExtension
        , Notebook AS Notebook
        , CASE WHEN FileExtension IS NULL THEN 0 ELSE 1 END AS NeedParameters
    FROM main.config.tbl_Entities
    WHERE Environment = V_environment
    AND IsActive = 1
    ORDER BY IDEntity ASC;
END;
