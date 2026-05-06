CREATE OR REPLACE PROCEDURE `main`.`gold`.`usp_Load_DimProductWithDocument`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE VARIABLE V_lastLoad timestamp;
    DECLARE VARIABLE V_sql STRING ;
    
    -- =================================================
    -- 1) Watermark setup
    -- =================================================
    IF NOT EXISTS (SELECT 1 FROM main.ctl.Watermark WHERE EntityName = 'DimProductDocument') THEN
        INSERT INTO main.ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'DimProductDocument', NULL, current_timestamp();
    END IF;
    
    SET V_lastLoad = (SELECT LastLoadTS FROM main.ctl.Watermark WHERE EntityName = 'DimProductDocument' LIMIT 1);
    
    -- =================================================
    -- 2) Drop table if exists
    -- =================================================
    DROP TABLE IF EXISTS main.gold.DimProductDocument;
    
    -- =================================================
    -- 3) CTAS with join and SCD2 hash
    -- =================================================
    SET V_sql = '
    CREATE OR REPLACE TABLE main.gold.DimProductDocument AS
    SELECT
        p.ProductID,
        p.Name AS ProductName,
        d.DocumentNode,
        d.Title AS DocumentTitle,
        d.FileName,
        d.FileExtension,
        d.Revision,
        SHA2(
        CONCAT(
            CAST(p.ProductID AS STRING),
            p.Name,
            d.DocumentNode,
            d.Title,
            d.FileName), 256) AS RowHash,
        current_timestamp() AS LoadTS,
        1 AS IsCurrent
    FROM main.ref.vw_Product p
    LEFT JOIN main.ref.vw_ProductDocument pd
        ON pd.ProductID = p.ProductID
    LEFT JOIN main.ref.vw_Document d
        ON d.DocumentNode = pd.DocumentNode' ||
    CASE 
        WHEN V_lastLoad IS NULL THEN ';'
        ELSE ' WHERE p.ModifiedDate > ' || CHR(39) || CAST(V_lastLoad AS STRING) || CHR(39) || ' OR d.ModifiedDate > ' || CHR(39) || CAST(V_lastLoad AS STRING) || CHR(39) || ';'
    END;
    
    EXECUTE IMMEDIATE V_sql;
    
    -- =================================================
    -- 4) Update Watermark
    -- =================================================
    UPDATE main.ctl.Watermark
    SET LastLoadTS = current_timestamp(),
        UpdatedAt = current_timestamp()
    WHERE EntityName = 'DimProductDocument';
END;
