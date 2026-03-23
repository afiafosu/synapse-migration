CREATE PROCEDURE [config].[GetMetadataGold]
AS
BEGIN
	SELECT 
		    [ProcessName]
          , [SPName]
	FROM [config].[tbl_GoldSP]
	WHERE [IsActive] = 1
	ORDER BY [ID_GoldSP] ASC
END
GO