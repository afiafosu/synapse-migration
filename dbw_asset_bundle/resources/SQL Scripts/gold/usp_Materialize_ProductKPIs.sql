CREATE OR REPLACE PROCEDURE `main`.`gold`.`usp_Materialize_ProductKPIs`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE VARIABLE V_lastLoad timestamp;
    DECLARE VARIABLE V_sql STRING;
    IF NOT EXISTS (SELECT 1 FROM main.ctl.Watermark WHERE EntityName = 'ProductKPIs') THEN
        INSERT INTO main.ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'ProductKPIs', NULL, current_timestamp();
    END IF;
    SET V_lastLoad = (SELECT LastLoadTS FROM main.ctl.Watermark WHERE EntityName = 'ProductKPIs' LIMIT 1);
    DROP TABLE IF EXISTS main.gold.ProductKPIs;
    SET V_sql = '
    CREATE OR REPLACE TABLE main.gold.ProductKPIs AS
    SELECT
        p.ProductID,
        p.Name AS ProductName,
        COUNT(DISTINCT sod.SalesOrderID) AS TotalOrders,
        SUM(sod.OrderQty) AS TotalQuantitySold,
        SUM(sod.LineTotal) AS TotalRevenue,
        AVG(sod.UnitPrice) AS AvgUnitPrice,
        MAX(sod.LineTotal) AS MaxLineTotal,
        MIN(sod.LineTotal) AS MinLineTotal,
        SHA2(CONCAT(CAST(p.ProductID AS STRING),CAST(SUM(sod.LineTotal) AS STRING),CAST(COUNT(DISTINCT sod.SalesOrderID) AS STRING)), 256) AS RowHash,
        current_timestamp() AS LoadTS,
        1 AS IsCurrent
    FROM main.silver.production_product p
    LEFT JOIN main.silver.sales_salesorderdetail sod
        ON sod.ProductID = p.ProductID' ||
    CASE WHEN V_lastLoad IS NULL THEN ''
        ELSE ' WHERE p.ModifiedDate > ' || CHR(39) || CAST(V_lastLoad AS STRING) || CHR(39) || ' OR sod.ModifiedDate > ' || CHR(39) || CAST(V_lastLoad AS STRING) || CHR(39)
    END || '
    GROUP BY p.ProductID, p.Name;';
    EXECUTE IMMEDIATE V_sql;
    UPDATE main.ctl.Watermark SET LastLoadTS = current_timestamp(), UpdatedAt = current_timestamp() WHERE EntityName = 'ProductKPIs';
END;
