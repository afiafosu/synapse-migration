CREATE PROCEDURE [config].[GetMetadata]
    @environment VARCHAR(64)
AS
BEGIN
	SELECT 
		  [ADLSaccount]
		, [ContainerName] AS [containerName]
		, [SourcePath] AS [sourceFolder]
		, [TargetPath] AS [targetFolder]
		, [Environment] AS [targetEnv]
		, [EntityName] AS [entityName]
		, [FileExtension] AS [fileExtension]
		, [Notebook] AS [Notebook]
		, CASE WHEN [FileExtension] IS NULL THEN 0 ELSE 1 END AS [NeedParameters]
	FROM [config].[tbl_Entities]
	WHERE [Environment] = @environment
	AND [IsActive] = 1
	ORDER BY [IDEntity] ASC
END
GO