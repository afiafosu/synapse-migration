CREATE OR REPLACE PROCEDURE `main`.`gold`.`usp_Materialize_Product360`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE VARIABLE V_lastLoad timestamp;
    DECLARE VARIABLE V_sql STRING ;
    
    -- ===============================================
    -- Watermark setup
    -- ===============================================
    IF NOT EXISTS (SELECT 1 FROM main.ctl.Watermark WHERE EntityName = 'Product360') THEN
        INSERT INTO main.ctl.Watermark (EntityName, LastLoadTS, UpdatedAt)
        SELECT 'Product360', NULL, current_timestamp();
    END IF;
    
    SET V_lastLoad = (SELECT LastLoadTS FROM main.ctl.Watermark WHERE EntityName = 'Product360' LIMIT 1);
    
    -- ===============================================
    -- Drop table if exists
    -- ===============================================
    DROP TABLE IF EXISTS main.gold.Product360;
    
    -- ===============================================
    -- CTAS: build full product 360
    -- ===============================================
    SET V_sql = '
    CREATE OR REPLACE TABLE main.gold.Product360 AS
    SELECT
        p.ProductID,
        p.Name AS ProductName,
        p.ProductNumber,
        p.StandardCost,
        p.ListPrice,
        SUM(pi.Quantity) AS TotalInventory,
        COUNT(DISTINCT th.TransactionID) AS NumTransactions,
        SUM(th.Quantity * th.ActualCost) AS TotalRevenue,
        AVG(th.ActualCost) AS AvgUnitPrice,
        COUNT(DISTINCT pr.ProductReviewID) AS NumReviews,
        AVG(pr.Rating) AS AvgRating,
        COUNT(DISTINCT wo.WorkOrderID) AS NumWorkOrders,
        MIN(pch.StandardCost) AS MinCostHistory,
        MAX(pch.StandardCost) AS MaxCostHistory,
        dbo.fn_SCD_Hash_SHA256(
            CONCAT(
                CAST(p.ProductID AS STRING),
                CAST(SUM(pi.Quantity) AS STRING),
                CAST(SUM(th.Quantity * th.Actualcost) AS STRING),
                CAST(AVG(th.Actualcost) AS STRING)
            )
        ) AS RowHash,
        current_timestamp() AS LoadTS,
        1 AS IsCurrent
    FROM main.silver.production_Product p
    LEFT JOIN main.silver.production_ProductInventory pi
        ON pi.ProductID = p.ProductID' ||
    CASE 
        WHEN V_lastLoad IS NULL THEN ''
        ELSE ' AND pi.ModifiedDate > ''' || CAST(V_lastLoad AS STRING) || ''''
    END || '
    LEFT JOIN main.silver.production_TransactionHistory th
        ON th.ProductID = p.ProductID' ||
    CASE 
        WHEN V_lastLoad IS NULL THEN ''
        ELSE ' AND th.ModifiedDate > ''' || CAST(V_lastLoad AS STRING) || ''''
    END || '
    LEFT JOIN main.silver.production_ProductReview pr
        ON pr.ProductID = p.ProductID' ||
    CASE 
        WHEN V_lastLoad IS NULL THEN ''
        ELSE ' AND pr.ModifiedDate > ''' || CAST(V_lastLoad AS STRING) || ''''
    END || '
    LEFT JOIN main.silver.production_WorkOrder wo
        ON wo.ProductID = p.ProductID' ||
    CASE 
        WHEN V_lastLoad IS NULL THEN ''
        ELSE ' AND wo.ModifiedDate > ''' || CAST(V_lastLoad AS STRING) || ''''
    END || '
    LEFT JOIN main.silver.production_ProductCostHistory pch
        ON pch.ProductID = p.ProductID' ||
    CASE 
        WHEN V_lastLoad IS NULL THEN ''
        ELSE ' AND pch.ModifiedDate > ''' || CAST(V_lastLoad AS STRING) || ''''
    END || '
    GROUP BY p.ProductID, p.Name, p.ProductNumber, p.StandardCost, p.ListPrice;';
    
    EXECUTE IMMEDIATE V_sql;
-- ===============================================
    -- Update Watermark
    -- ===============================================

    UPDATE main.ctl.Watermark
    SET LastLoadTS = current_timestamp(),
        UpdatedAt = current_timestamp()
    WHERE EntityName = 'Product360';
END;
