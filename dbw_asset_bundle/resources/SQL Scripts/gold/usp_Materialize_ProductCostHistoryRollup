CREATE OR REPLACE PROCEDURE `main`.`gold`.`usp_Materialize_ProductCostHistoryRollup`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE VARIABLE V_lastLoad timestamp;
    DECLARE VARIABLE V_sql STRING;
    IF NOT EXISTS (SELECT 1 FROM main.ctl.Watermark WHERE EntityName = 'ProductCostHistoryRollup') THEN
        INSERT INTO main.ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'ProductCostHistoryRollup', NULL, current_timestamp();
    END IF;
    SET V_lastLoad = (SELECT LastLoadTS FROM main.ctl.Watermark WHERE EntityName = 'ProductCostHistoryRollup' LIMIT 1);
    DROP TABLE IF EXISTS main.gold.ProductCostHistoryRollup;
    SET V_sql = '
    CREATE OR REPLACE TABLE main.gold.ProductCostHistoryRollup AS
    SELECT
        pch.ProductID,
        p.Name AS ProductName,
        COUNT(*) AS NumCostChanges,
        MIN(pch.StandardCost) AS MinCost,
        MAX(pch.StandardCost) AS MaxCost,
        AVG(pch.StandardCost) AS AvgCost,
        SHA2(CONCAT(CAST(pch.ProductID AS STRING),CAST(MIN(pch.StandardCost) AS STRING),CAST(MAX(pch.StandardCost) AS STRING)), 256) AS RowHash,
        current_timestamp() AS LoadTS,
        1 AS IsCurrent
    FROM main.silver.production_productcosthistory pch
    LEFT JOIN main.silver.production_product p
        ON p.ProductID = pch.ProductID' ||
    CASE WHEN V_lastLoad IS NULL THEN ''
        ELSE ' WHERE p.ModifiedDate > ' || CHR(39) || CAST(V_lastLoad AS STRING) || CHR(39) || ' OR pch.ModifiedDate > ' || CHR(39) || CAST(V_lastLoad AS STRING) || CHR(39)
    END || '
    GROUP BY pch.ProductID, p.Name;';
    EXECUTE IMMEDIATE V_sql;
    UPDATE main.ctl.Watermark SET LastLoadTS = current_timestamp(), UpdatedAt = current_timestamp() WHERE EntityName = 'ProductCostHistoryRollup';
END;
