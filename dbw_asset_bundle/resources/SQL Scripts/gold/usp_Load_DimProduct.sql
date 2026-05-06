CREATE OR REPLACE PROCEDURE `main`.`gold`.`usp_Load_DimProduct`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE VARIABLE V_lastLoad timestamp;
    DECLARE VARIABLE V_sql STRING ;
    
    -- Watermark init (Dedicated pool: avoid VALUES with functions)
    IF NOT EXISTS (SELECT 1 FROM main.ctl.Watermark WHERE EntityName = 'DimProduct') THEN
        INSERT INTO main.ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'DimProduct', NULL, current_timestamp();
    END IF;
    
    SET V_lastLoad = (SELECT LastLoadTS FROM main.ctl.Watermark WHERE EntityName = 'DimProduct' LIMIT 1);
    
    -- Recreate table (CTAS)
    DROP TABLE IF EXISTS main.gold.DimProduct;
    
    SET V_sql = 'CREATE OR REPLACE TABLE main.gold.DimProduct AS
    SELECT
        p.ProductID,
        p.Name,
        p.ProductNumber,
        p.ProductModelID,
        p.Color,
        p.`Class`,
        p.`Style`,
        p.`Size`,
        p.SizeUnitMeasureCode,
        p.WeightUnitMeasureCode,
        p.StandardCost,
        p.ListPrice,
        p.Weight,
        p.MakeFlag,
        p.FinishedGoodsFlag,
        p.DaysToManufacture,
        p.ReorderPoint,
        p.SafetyStockLevel,
        p.ModifiedDate,
    current_timestamp() AS LoadTS
    FROM main.ref.vw_Product p' ||
    CASE 
        WHEN V_lastLoad IS NULL THEN ';'
        ELSE ' WHERE p.ModifiedDate > ' || CHR(39) || CAST(V_lastLoad AS STRING) || CHR(39) || ';'
    END;
    
    EXECUTE IMMEDIATE V_sql;
    
    -- Update watermark
    UPDATE main.ctl.Watermark
    SET LastLoadTS = current_timestamp(),
        UpdatedAt  = current_timestamp()
    WHERE EntityName = 'DimProduct';
END;
