CREATE OR REPLACE PROCEDURE `main`.`gold`.`usp_Materialize_ProductInventoryKPIs`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE VARIABLE V_lastLoad timestamp;
    DECLARE VARIABLE V_sql STRING;
    IF NOT EXISTS (SELECT 1 FROM main.ctl.Watermark WHERE EntityName = 'ProductInventoryKPIs') THEN
        INSERT INTO main.ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'ProductInventoryKPIs', NULL, current_timestamp();
    END IF;
    SET V_lastLoad = (SELECT LastLoadTS FROM main.ctl.Watermark WHERE EntityName = 'ProductInventoryKPIs' LIMIT 1);
    DROP TABLE IF EXISTS main.gold.ProductInventoryKPIs;
    SET V_sql = '
    CREATE OR REPLACE TABLE main.gold.ProductInventoryKPIs AS
    SELECT
        pi.ProductID,
        p.Name AS ProductName,
        SUM(pi.Quantity) AS TotalQuantity,
        AVG(pi.Quantity) AS AvgQuantityPerLocation,
        MIN(pi.Quantity) AS MinQuantity,
        MAX(pi.Quantity) AS MaxQuantity,
        SHA2(CONCAT(CAST(pi.ProductID AS STRING),CAST(SUM(pi.Quantity) AS STRING)), 256) AS RowHash,
        current_timestamp() AS LoadTS,
        1 AS IsCurrent
    FROM main.silver.production_productinventory pi
    LEFT JOIN main.silver.production_product p
      ON p.ProductID = pi.ProductID' ||
    CASE WHEN V_lastLoad IS NULL THEN ''
        ELSE ' WHERE p.ModifiedDate > ' || CHR(39) || CAST(V_lastLoad AS STRING) || CHR(39) || ' OR pi.ModifiedDate > ' || CHR(39) || CAST(V_lastLoad AS STRING) || CHR(39)
    END || '
    GROUP BY pi.ProductID, p.Name;';
    EXECUTE IMMEDIATE V_sql;
    UPDATE main.ctl.Watermark SET LastLoadTS = current_timestamp(), UpdatedAt = current_timestamp() WHERE EntityName = 'ProductInventoryKPIs';
END;
