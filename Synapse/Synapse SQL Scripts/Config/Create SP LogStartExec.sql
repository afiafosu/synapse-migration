CREATE PROCEDURE [config].[LogStartExec]
    @PipelineName VARCHAR(256),
    @PipelineRunID VARCHAR(256),
    @PipelineWorkspace VARCHAR(256)
AS
BEGIN 
    DECLARE @IDLog INT;
    DECLARE @NowDate DATETIME2;

    SET @IDLog = (SELECT ISNULL(MAX(ID_Log), 0) AS ID_Log FROM [config].[tbl_Log]) + 1;
    SET @NowDate = GETDATE();

    INSERT INTO [config].[tbl_Log] ([ID_Log], [PipelineName], [PipelineRunID], [PipelineWorkspace], [StartTime], [Status]) VALUES
    (@IDLog, @PipelineName, @PipelineRunID, @PipelineWorkspace, @NowDate, 'Executing');

    SELECT @IDLog AS [ID_Log];
END;
GO