CREATE OR REPLACE PROCEDURE `main`.`config`.`LogEndExec`(
	IN V_IDLog int,
	IN V_Status STRING)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN
    DECLARE VARIABLE V_NowDate timestamp;
    
    SET V_NowDate = current_timestamp();
    
    UPDATE `main`.`config`.`tbl_Log`
    SET   `EndTime` = V_NowDate
        , `DurationSeconds` = DATEDIFF(SECOND, V_NowDate, StartTime)
        , `Status` = V_Status
    WHERE `ID_Log` = V_IDLog;
END;
