CREATE OR REPLACE PROCEDURE `main`.`gold`.`usp_Materialize_ProductReviewAnalytics`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE VARIABLE V_lastLoad timestamp;
    DECLARE VARIABLE V_sql STRING;
    IF NOT EXISTS (SELECT 1 FROM main.ctl.Watermark WHERE EntityName = 'ProductReviewAnalytics') THEN
        INSERT INTO main.ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'ProductReviewAnalytics', NULL, current_timestamp();
    END IF;
    SET V_lastLoad = (SELECT LastLoadTS FROM main.ctl.Watermark WHERE EntityName = 'ProductReviewAnalytics' LIMIT 1);
    DROP TABLE IF EXISTS main.gold.ProductReviewAnalytics;
    SET V_sql = '
    CREATE OR REPLACE TABLE main.gold.ProductReviewAnalytics AS
    SELECT
        pr.ProductID,
        p.Name AS ProductName,
        COUNT(pr.ProductReviewID) AS NumReviews,
        AVG(pr.Rating) AS AvgRating,
        SUM(CASE WHEN pr.Rating >= 4 THEN 1 ELSE 0 END) AS NumPositiveReviews,
        SUM(CASE WHEN pr.Rating <= 2 THEN 1 ELSE 0 END) AS NumNegativeReviews,
        SHA2(CONCAT(CAST(pr.ProductID AS STRING),CAST(COUNT(pr.ProductReviewID) AS STRING),CAST(AVG(pr.Rating) AS STRING)), 256) AS RowHash,
        current_timestamp() AS LoadTS,
        1 AS IsCurrent
    FROM main.silver.production_ProductReview pr
    LEFT JOIN main.silver.production_Product p
      ON p.ProductID = pr.ProductID' ||
    CASE WHEN V_lastLoad IS NULL THEN ''
        ELSE ' WHERE p.ModifiedDate > ' || CHR(39) || CAST(V_lastLoad AS STRING) || CHR(39) || ' OR pr.ModifiedDate > ' || CHR(39) || CAST(V_lastLoad AS STRING) || CHR(39)
    END || '
    GROUP BY pr.ProductID, p.Name;';
    EXECUTE IMMEDIATE V_sql;
    UPDATE main.ctl.Watermark SET LastLoadTS = current_timestamp(), UpdatedAt = current_timestamp() WHERE EntityName = 'ProductReviewAnalytics';
END;
