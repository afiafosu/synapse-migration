CREATE OR REPLACE PROCEDURE `main`.`config`.`LogStartExec`(
	IN V_PipelineName STRING,
	IN V_PipelineRunID STRING,
	IN V_PipelineWorkspace STRING
	OUT V_IDLog INT)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
BEGIN
    DECLARE VARIABLE V_NowDate timestamp;
    SET V_IDLog = (SELECT COALESCE(MAX(ID_Log), 0) AS ID_Log FROM `main`.`config`.`tbl_Log`) + 1;
    SET V_NowDate = current_timestamp();
    INSERT INTO `main`.`config`.`tbl_Log` (`ID_Log`, `PipelineName`, `PipelineRunID`, `PipelineWorkspace`, `StartTime`, `Status`) VALUES
    (V_IDLog, V_PipelineName, V_PipelineRunID, V_PipelineWorkspace, V_NowDate, 'Executing');
 

END;
