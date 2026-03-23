CREATE PROCEDURE [config].[LogEndExec]
    @IDLog INT,
    @Status VARCHAR(256)
AS
BEGIN
    DECLARE @NowDate DATETIME2;
    SET @NowDate = GETDATE();

    UPDATE [config].[tbl_Log]
    SET   [EndTime] = @NowDate
        , [DurationSeconds] = DATEDIFF(SECOND, StartTime, @NowDate)
        , [Status] = @Status
    WHERE [ID_Log] = @IDLog;
END;
GO