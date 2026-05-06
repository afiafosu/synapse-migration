CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_build_enterprise_gold_layer`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE VARIABLE V_ExecutionID BIGINT ;
    DECLARE VARIABLE V_ErrorMessage STRING;
    DECLARE VARIABLE V_EntityName STRING ;
    DECLARE VARIABLE V_CurrentLoadDate timestamp ;
    DECLARE VARIABLE V_LastWatermark timestamp;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET V_ErrorMessage = MESSAGE_TEXT;
        
        UPDATE main.gold.etl_execution_log
        SET EndTime = current_timestamp(),
            Status = 'FAILED',
            ErrorMessage = V_ErrorMessage
        WHERE ExecutionID = V_ExecutionID;
        
        SIGNAL SQLSTATE '45000';
    END;
    
    ------------------------------------------------------------
    -- VARIABLES
    ------------------------------------------------------------
    
    SET V_EntityName = 'AdventureWorks_Enterprise';
    SET V_CurrentLoadDate = current_timestamp();
    SET V_ExecutionID = ABS(CHECKSUM(UUID()));

    SET V_LastWatermark = (SELECT LastLoadTS FROM main.ctl.Watermark WHERE EntityName = V_EntityName LIMIT 1);
    IF V_LastWatermark IS NULL THEN
        SET V_LastWatermark = '1900-01-01';
    END IF;
    
    -- Create gold schema if not exists
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS `main`.`gold` MANAGED LOCATION ''abfss://fabmigation1@synpasetofabric.dfs.core.windows.net/'';';
    
    -- ETL execution log
    DROP TABLE IF EXISTS main.gold.stg_sales_part1;
    
    CREATE OR REPLACE TABLE main.gold.stg_sales_part1 AS
    SELECT
        h.SalesOrderID,
        d.SalesOrderDetailID,
        h.OrderDate,
        h.CustomerID,
        h.TerritoryID,
        d.ProductID,
        d.OrderQty,
        d.UnitPrice,
        d.UnitPriceDiscount,
        d.LineTotal,
        YEAR(h.OrderDate) AS OrderYear,
        MONTH(h.OrderDate) AS OrderMonth,
        (d.LineTotal - (d.LineTotal * d.UnitPriceDiscount)) AS NetRevenue
    FROM main.silver.sales_salesorderheader h
    INNER JOIN main.silver.sales_salesorderdetail d ON h.SalesOrderID = d.SalesOrderID
    WHERE h.ModifiedDate >= V_LastWatermark OR d.ModifiedDate >= V_LastWatermark;
    
    ------------------------------------------------------------
    -- STAGING SALES PART 2: COMPUTE NET REVENUE PER PRODUCT
    ------------------------------------------------------------
    
    DROP TABLE IF EXISTS main.gold.stg_sales_part2;
    
    CREATE OR REPLACE TABLE main.gold.stg_sales_part2 AS
    SELECT
        ProductID,
        OrderYear,
        OrderMonth,
        SUM(OrderQty) AS TotalQty,
        SUM(NetRevenue) AS TotalRevenue
    FROM main.gold.stg_sales_part1
    GROUP BY ProductID, OrderYear, OrderMonth;
    ------------------------------------------------------------
    -- STAGING SALES PART 3: RUNNING REVENUE AND RANK
    ------------------------------------------------------------
    
    DROP TABLE IF EXISTS main.gold.stg_sales_part3;
    
    CREATE OR REPLACE TABLE main.gold.stg_sales_part3 AS
    SELECT
        ProductID,
        OrderYear,
        OrderMonth,
        TotalQty,
        TotalRevenue,
        SUM(TotalRevenue) OVER (PARTITION BY ProductID ORDER BY OrderYear, OrderMonth ROWS UNBOUNDED PRECEDING) AS RunningRevenue,
        RANK() OVER (PARTITION BY OrderYear ORDER BY TotalRevenue DESC) AS RevenueRank
    FROM main.gold.stg_sales_part2;
    
    ------------------------------------------------------------
    -- CREATE FACT SALES TABLE IF NOT EXISTS
    ------------------------------------------------------------
    
    DROP TABLE IF EXISTS main.gold.stg_customer_r;
    
    CREATE OR REPLACE TABLE main.gold.stg_customer_r AS
    SELECT
        CustomerID,
        MAX(OrderDate) AS LastOrderDate,
        DATEDIFF(MAX(OrderDate), V_CurrentLoadDate) AS RecencyDays
    FROM main.gold.stg_sales_part1
    GROUP BY CustomerID;
    
    DROP TABLE IF EXISTS main.gold.stg_customer_f;
    
    CREATE OR REPLACE TABLE main.gold.stg_customer_f AS
    SELECT
        CustomerID,
        COUNT(DISTINCT SalesOrderID) AS Frequency
    FROM main.gold.stg_sales_part1
    GROUP BY CustomerID;
    
    DROP TABLE IF EXISTS main.gold.stg_customer_m;
    
    CREATE OR REPLACE TABLE main.gold.stg_customer_m AS
    SELECT
        CustomerID,
        SUM(NetRevenue) AS MonetaryValue
    FROM main.gold.stg_sales_part3
    GROUP BY CustomerID;
    
    ------------------------------------------------------------
    -- MERGE RFM INTO SINGLE TABLE
    ------------------------------------------------------------
    
    DROP TABLE IF EXISTS main.gold.stg_customer_rfm;
    
    CREATE OR REPLACE TABLE main.gold.stg_customer_rfm AS
    SELECT
        r.CustomerID,
        r.LastOrderDate,
        r.RecencyDays,
        f.Frequency,
        m.MonetaryValue,
        NTILE(5) OVER (ORDER BY r.RecencyDays ASC) AS RecencyScore,
        NTILE(5) OVER (ORDER BY f.Frequency DESC) AS FrequencyScore,
        NTILE(5) OVER (ORDER BY m.MonetaryValue DESC) AS MonetaryScore
    FROM main.gold.stg_customer_r r
    INNER JOIN main.gold.stg_customer_f f ON r.CustomerID = f.CustomerID
    INNER JOIN main.gold.stg_customer_m m ON r.CustomerID = m.CustomerID;
    
    ------------------------------------------------------------
    -- SECTION 5: CUSTOMER SEGMENTATION
    ------------------------------------------------------------
    
    DROP TABLE IF EXISTS main.gold.stg_customer_segment;
    
    CREATE OR REPLACE TABLE main.gold.stg_customer_segment AS
    SELECT *,
        CASE 
            WHEN RecencyScore >=4 AND FrequencyScore >=4 AND MonetaryScore >=4 THEN 'CHAMPIONS'
            WHEN RecencyScore >=3 AND FrequencyScore >=3 THEN 'LOYAL'
            WHEN RecencyScore <=2 AND FrequencyScore >=4 THEN 'AT_RISK'
            WHEN RecencyScore <=2 AND FrequencyScore <=2 THEN 'HIBERNATING'
            ELSE 'POTENTIAL'
        END AS CustomerSegment
    FROM main.gold.stg_customer_rfm;
    
    ------------------------------------------------------------
    -- SECTION 6: TERRITORY PERFORMANCE
    ------------------------------------------------------------
    
    DROP TABLE IF EXISTS main.gold.stg_territory_performance;
    
    CREATE OR REPLACE TABLE main.gold.stg_territory_performance AS
    SELECT TerritoryID, OrderYear,
           SUM(NetRevenue) AS TerritoryRevenue,
           SUM(TotalQty) AS TerritoryQuantity,
           COUNT(DISTINCT CustomerID) AS ActiveCustomers,
           SUM(SUM(NetRevenue)) OVER (PARTITION BY TerritoryID ORDER BY OrderYear ROWS UNBOUNDED PRECEDING) AS RunningTerritoryRevenue
    FROM main.gold.stg_sales_part3
    GROUP BY TerritoryID, OrderYear;
    
    ------------------------------------------------------------
    -- SECTION 7: PRODUCT PROFITABILITY
    ------------------------------------------------------------
    
    DROP TABLE IF EXISTS main.gold.stg_product_profitability;
    
    CREATE OR REPLACE TABLE main.gold.stg_product_profitability AS
    SELECT s.ProductID,
           SUM(s.NetRevenue) AS TotalRevenue,
           COALESCE(p.TotalProductionCost, 0) AS TotalProductionCost,
           SUM(s.NetRevenue) - COALESCE(p.TotalProductionCost, 0) AS GrossMargin,
           CASE WHEN SUM(s.NetRevenue)=0 THEN 0 ELSE (SUM(s.NetRevenue) - COALESCE(p.TotalProductionCost, 0))/SUM(s.NetRevenue) END AS MarginPercentage
    FROM main.gold.stg_sales_part3 s
    LEFT JOIN main.silver.production_transactionhistory p ON s.ProductID = p.ProductID
    GROUP BY s.ProductID, p.TotalProductionCost;
    
    ------------------------------------------------------------
    -- SECTION 8: INVENTORY TURNOVER
    ------------------------------------------------------------
    
    DROP TABLE IF EXISTS main.gold.stg_inventory_turnover;
    
    CREATE OR REPLACE TABLE main.gold.stg_inventory_turnover AS
    SELECT i.ProductID,
           SUM(i.Quantity) AS CurrentStock,
           CASE WHEN SUM(i.Quantity)=0 THEN NULL ELSE SUM(s.NetRevenue)/SUM(i.Quantity) END AS InventoryTurnoverRatio
    FROM main.silver.production_productinventory i
    LEFT JOIN main.gold.stg_product_profitability s ON i.ProductID = s.ProductID
    GROUP BY i.ProductID;
    
    ------------------------------------------------------------
    -- SECTION 9: SCRAP ANALYSIS (DAMAGE, DEFECTIVE, EXPIRED)
    ------------------------------------------------------------
    
    DROP TABLE IF EXISTS main.gold.stg_scrap_damaged;
    
    CREATE OR REPLACE TABLE main.gold.stg_scrap_damaged AS
    SELECT w.ProductID, SUM(w.ScrappedQty) AS DamagedQty
    FROM main.silver.production_workorder w
    INNER JOIN main.silver.production_scrapreason sr ON w.ScrapReasonID = sr.ScrapReasonID
    WHERE sr.Name = 'Damaged'
    GROUP BY w.ProductID;
    
    DROP TABLE IF EXISTS main.gold.stg_scrap_defective;
    
    CREATE OR REPLACE TABLE main.gold.stg_scrap_defective AS
    SELECT w.ProductID, SUM(w.ScrappedQty) AS DefectiveQty
    FROM main.silver.production_workorder w
    INNER JOIN main.silver.production_scrapreason sr ON w.ScrapReasonID = sr.ScrapReasonID
    WHERE sr.Name = 'Defective'
    GROUP BY w.ProductID;
    
    DROP TABLE IF EXISTS main.gold.stg_scrap_expired;
    
    CREATE OR REPLACE TABLE main.gold.stg_scrap_expired AS
    SELECT w.ProductID, SUM(w.ScrappedQty) AS ExpiredQty
    FROM main.silver.production_workorder w
    INNER JOIN main.silver.production_scrapreason sr ON w.ScrapReasonID = sr.ScrapReasonID
    WHERE sr.Name = 'Expired'
    GROUP BY w.ProductID;
    
    ------------------------------------------------------------
    -- MERGE SCRAP TABLES INTO SINGLE PIVOT
    ------------------------------------------------------------
    
    DROP TABLE IF EXISTS main.gold.stg_scrap_pivot;
    
    CREATE OR REPLACE TABLE main.gold.stg_scrap_pivot AS
    SELECT COALESCE(d.ProductID, e.ProductID, f.ProductID) AS ProductID,
           COALESCE(d.DamagedQty, 0) AS Damaged,
           COALESCE(e.DefectiveQty, 0) AS Defective,
           COALESCE(f.ExpiredQty, 0) AS Expired
    FROM main.gold.stg_scrap_damaged d
    FULL OUTER JOIN main.gold.stg_scrap_defective e ON d.ProductID = e.ProductID
    FULL OUTER JOIN main.gold.stg_scrap_expired f ON COALESCE(d.ProductID, e.ProductID) = f.ProductID;
    
    /******************************************************************************************
    -- Part 3: MERGE FACT, KPI, Watermark, ETL Log Update, Cleanup, Error Handling
    -- Synapse Dedicated SQL Pool Safe
    -- Lines: ~180 functional
    ******************************************************************************************/
    
    ------------------------------------------------------------
    -- SECTION 10: SALES GROUPING (simulate GROUPING SETS with UNION ALL)
    ------------------------------------------------------------

    DROP TABLE IF EXISTS main.gold.stg_sales_groupings;
    
    CREATE OR REPLACE TABLE main.gold.stg_sales_groupings AS
    SELECT ProductID, CategoryName, SubCategoryName, SUM(NetRevenue) AS Revenue, SUM(TotalQty) AS Quantity
    FROM main.gold.stg_sales_part3
    GROUP BY ProductID, CategoryName, SubCategoryName
    UNION ALL
    SELECT NULL, CategoryName, NULL, SUM(NetRevenue), SUM(TotalQty)
    FROM main.gold.stg_sales_part3
    GROUP BY CategoryName
    UNION ALL
    SELECT NULL, NULL, NULL, SUM(NetRevenue), SUM(TotalQty)
    FROM main.gold.stg_sales_part3;
    
    ------------------------------------------------------------
    -- SECTION 11: MERGE INTO FACT SALES ENTERPRISE
    ------------------------------------------------------------
    
    MERGE INTO main.gold.fact_sales_enterprise AS T
    USING (SELECT ProductID, OrderYear, OrderMonth, TotalQty, TotalRevenue, RunningRevenue, RevenueRank, V_CurrentLoadDate AS LoadDate
    FROM main.gold.stg_sales_part3) AS S
    ON T.ProductID = S.ProductID AND T.OrderYear = S.OrderYear AND T.OrderMonth = S.OrderMonth
    WHEN MATCHED THEN UPDATE SET TotalQty = S.TotalQty, TotalRevenue = S.TotalRevenue,
    RunningRevenue = S.RunningRevenue, RevenueRank = S.RevenueRank, LoadDate = S.LoadDate
    WHEN NOT MATCHED THEN INSERT (ProductID, OrderYear, OrderMonth, TotalQty, TotalRevenue, RunningRevenue, RevenueRank, LoadDate)
    VALUES (S.ProductID, S.OrderYear, S.OrderMonth, S.TotalQty, S.TotalRevenue, S.RunningRevenue, S.RevenueRank, S.LoadDate);
    
    ------------------------------------------------------------
    -- SECTION 12: KPI CONSOLIDATION
    ------------------------------------------------------------
    
    DROP TABLE IF EXISTS main.gold.fact_enterprise_kpi;
    
    CREATE OR REPLACE TABLE main.gold.fact_enterprise_kpi AS
    SELECT s.ProductID, s.TotalRevenue, s.TotalQty, p.MarginPercentage, i.InventoryTurnoverRatio,
           sp.Damaged + sp.Defective + sp.Expired AS TotalScrapQty,
           CASE WHEN p.MarginPercentage > 0.4 THEN 'HIGH_MARGIN'
                WHEN p.MarginPercentage > 0.2 THEN 'MEDIUM_MARGIN'
                ELSE 'LOW_MARGIN' END AS MarginCategory,
           CASE WHEN i.InventoryTurnoverRatio > 10 THEN 'FAST_MOVING'
                WHEN i.InventoryTurnoverRatio > 3 THEN 'NORMAL'
                ELSE 'SLOW_MOVING' END AS InventoryCategory,
           V_CurrentLoadDate AS LoadDate
    FROM main.gold.stg_product_profitability p
    LEFT JOIN main.gold.stg_inventory_turnover i ON p.ProductID = i.ProductID
    LEFT JOIN main.gold.stg_scrap_pivot sp ON p.ProductID = sp.ProductID
    LEFT JOIN main.gold.stg_sales_part3 s ON p.ProductID = s.ProductID;
    
    ------------------------------------------------------------
    -- SECTION 13: UPDATE WATERMARK
    ------------------------------------------------------------
    
    MERGE INTO main.ctl.Watermark AS T
    USING (SELECT V_EntityName AS EntityName) AS S
    ON T.EntityName = S.EntityName
    WHEN MATCHED THEN UPDATE SET LastLoadTS = V_CurrentLoadDate, UpdatedAt = V_CurrentLoadDate
    WHEN NOT MATCHED THEN INSERT (EntityName, LastLoadTS, LastFullRefreshTS, UpdatedAt)
    VALUES (V_EntityName, V_CurrentLoadDate, NULL, V_CurrentLoadDate);
    
    ------------------------------------------------------------
    -- SECTION 14: UPDATE ETL EXECUTION LOG
    ------------------------------------------------------------
    
    UPDATE main.gold.etl_execution_log
    SET EndTime = current_timestamp(),
        RowsProcessed = (SELECT COUNT(*) FROM gold.fact_sales_enterprise),
        Status = 'COMPLETED'
    WHERE ExecutionID = V_ExecutionID;
    
    ------------------------------------------------------------
    -- SECTION 15: CLEANUP STAGING TABLES
    ------------------------------------------------------------
    
    DROP TABLE IF EXISTS main.gold.stg_sales_part1;
    DROP TABLE IF EXISTS main.gold.stg_sales_part2;
    DROP TABLE IF EXISTS main.gold.stg_sales_part3;
    DROP TABLE IF EXISTS main.gold.stg_customer_r;
    DROP TABLE IF EXISTS main.gold.stg_customer_f;
    DROP TABLE IF EXISTS main.gold.stg_customer_m;
    DROP TABLE IF EXISTS main.gold.stg_customer_rfm;
    DROP TABLE IF EXISTS main.gold.stg_customer_segment;
    DROP TABLE IF EXISTS main.gold.stg_territory_performance;
    DROP TABLE IF EXISTS main.gold.stg_product_profitability;
    DROP TABLE IF EXISTS main.gold.stg_inventory_turnover;
    DROP TABLE IF EXISTS main.gold.stg_scrap_damaged;
    DROP TABLE IF EXISTS main.gold.stg_scrap_defective;
    DROP TABLE IF EXISTS main.gold.stg_scrap_expired;
    DROP TABLE IF EXISTS main.gold.stg_scrap_pivot;
    DROP TABLE IF EXISTS main.gold.stg_sales_groupings;
END;
