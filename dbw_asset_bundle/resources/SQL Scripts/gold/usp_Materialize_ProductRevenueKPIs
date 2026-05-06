CREATE OR REPLACE PROCEDURE `main`.`gold`.`usp_Materialize_ProductRevenueKPIs`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE VARIABLE V_lastLoad timestamp;
    DECLARE VARIABLE V_sql STRING;
    IF NOT EXISTS (SELECT 1 FROM main.ctl.Watermark WHERE EntityName = 'ProductRevenueKPIs') THEN
        INSERT INTO main.ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'ProductRevenueKPIs', NULL, current_timestamp();
    END IF;
    SET V_lastLoad = (SELECT LastLoadTS FROM main.ctl.Watermark WHERE EntityName = 'ProductRevenueKPIs' LIMIT 1);
    DROP TABLE IF EXISTS main.gold.ProductRevenueKPIs;
    SET V_sql = '
    CREATE OR REPLACE TABLE main.gold.ProductRevenueKPIs AS
    SELECT
        th.ProductID,
        p.Name AS ProductName,
        COUNT(DISTINCT th.TransactionID) AS NumTransactions,
        SUM(th.Quantity * th.ActualCost) AS TotalRevenue,
        AVG(th.ActualCost) AS AvgUnitPrice,
        MAX(th.ActualCost) AS MaxUnitPrice,
        MIN(th.ActualCost) AS MinUnitPrice,
        SHA2(CONCAT(CAST(th.ProductID AS STRING),CAST(SUM(th.Quantity * th.ActualCost) AS STRING)), 256) AS RowHash,
        current_timestamp() AS LoadTS,
        1 AS IsCurrent
    FROM main.silver.production_TransactionHistory th
    LEFT JOIN main.silver.production_Product p
        ON p.ProductID = th.ProductID' ||
    CASE WHEN V_lastLoad IS NULL THEN ''
        ELSE ' WHERE p.ModifiedDate > ' || CHR(39) || CAST(V_lastLoad AS STRING) || CHR(39) || ' OR th.ModifiedDate > ' || CHR(39) || CAST(V_lastLoad AS STRING) || CHR(39)
    END || '
    GROUP BY th.ProductID, p.Name;';
    EXECUTE IMMEDIATE V_sql;
    UPDATE main.ctl.Watermark SET LastLoadTS = current_timestamp(), UpdatedAt = current_timestamp() WHERE EntityName = 'ProductRevenueKPIs';
END;
