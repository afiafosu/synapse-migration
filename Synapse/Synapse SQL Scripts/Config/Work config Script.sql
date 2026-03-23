--SELECT * FROM [config].[tbl_entities] ORDER BY [IDEntity] ASC


--UPDATE [config].[tbl_entities] SET [IsActive] = 1 

--UPDATE [config].[tbl_entities] SET [IsActive] = 0 WHERE [IDEntity] IN (27,25)

--EXEC [config].[GetMetadata] 'silver'

--SELECT * FROM [config].[tbl_GoldSP] ORDER BY [ID_GoldSP];

--EXEC [config].[GetMetadataGold];

--EXEC [gold].[sp_gold_sales_orchestrator];

SELECT * FROM [config].[tbl_Log]

--EXEC [config].[LogStartExec] '02_pl_gold', '4b40eea5-160f-45ff-84c7-daf433b5c2b3', 'Unknown';

--EXEC [config].[LogEndExec] 3, 'Failed'; -- Succeeded, Failed