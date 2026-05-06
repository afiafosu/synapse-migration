CREATE OR REPLACE PROCEDURE `main`.`config`.`GetMetadataGold`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  SELECT 
    ProcessName,
    SPName
  FROM main.config.tbl_GoldSP
  WHERE IsActive = 1
  ORDER BY ID_GoldSP ASC;
END;
