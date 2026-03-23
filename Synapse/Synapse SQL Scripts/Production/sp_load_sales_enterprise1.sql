/******************************************************************************************
-- SP Load Sales Enterprise –
-- Staging & Initial Aggregations
-- Synapse Dedicated SQL Pool compatible
-- >
******************************************************************************************/

CREATE PROCEDURE gold.sp_load_sales_enterprise1
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------------------------------------------------------
    -- SECTION 0: DECLARE VARIABLES
    ----------------------------------------------------------------------------------------
    DECLARE @CurrentLoadDate DATETIME2(7) = SYSDATETIME();
    DECLARE @ExecutionID UNIQUEIDENTIFIER = NEWID();
    DECLARE @EntityName NVARCHAR(128) = 'SalesEnterprise';
    DECLARE @ErrorMessage NVARCHAR(MAX);

    ----------------------------------------------------------------------------------------
    -- SECTION 1: LOG START OF EXECUTION
    ----------------------------------------------------------------------------------------
    INSERT INTO gold.etl_execution_log (ExecutionID, EntityName, StartTime, Status)
    SELECT @ExecutionID, @EntityName, @CurrentLoadDate, 'RUNNING';

    --BEGIN TRY
        ------------------------------------------------------------------------------------
        -- SECTION 2: STAGING SALES HEADER
        ------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.stg_sales_header') IS NOT NULL DROP TABLE gold.stg_sales_header;

        CREATE TABLE gold.stg_sales_header
        WITH (DISTRIBUTION = HASH(SalesOrderID), CLUSTERED COLUMNSTORE INDEX) AS
        SELECT
            SalesOrderID AS sh_SalesOrderID,
            RevisionNumber AS sh_RevisionNumber,
            OrderDate AS sh_OrderDate,
            DueDate AS sh_DueDate,
            ShipDate AS sh_ShipDate,
            Status AS sh_Status,
            OnlineOrderFlag AS sh_OnlineOrderFlag,
            SalesOrderNumber AS sh_SalesOrderNumber,
            PurchaseOrderNumber AS sh_PurchaseOrderNumber,
            AccountNumber AS sh_AccountNumber,
            CustomerID AS sh_CustomerID,
            SalesPersonID AS sh_SalesPersonID,
            TerritoryID AS sh_TerritoryID,
            BillToAddressID AS sh_BillToAddressID,
            ShipToAddressID AS sh_ShipToAddressID,
            ShipMethodID AS sh_ShipMethodID,
            CreditCardID AS sh_CreditCardID,
            CreditCardApprovalCode AS sh_CreditCardApprovalCode,
            CurrencyRateID AS sh_CurrencyRateID,
            SubTotal AS sh_SubTotal,
            TaxAmt AS sh_TaxAmt,
            Freight AS sh_Freight,
            TotalDue AS sh_TotalDue,
            Comment AS sh_Comment,
            rowguid AS sh_rowguid,
            ModifiedDate AS sh_ModifiedDate
        FROM silver.sales_salesorderheader;

        ------------------------------------------------------------------------------------
        -- SECTION 3: STAGING SALES DETAIL
        ------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.stg_sales_detail') IS NOT NULL DROP TABLE gold.stg_sales_detail;

        CREATE TABLE gold.stg_sales_detail
        WITH (DISTRIBUTION = HASH(SalesOrderDetailID), CLUSTERED COLUMNSTORE INDEX) AS
        SELECT
            SalesOrderDetailID AS sd_SalesOrderDetailID,
            SalesOrderID AS sd_SalesOrderID,
            CarrierTrackingNumber AS sd_CarrierTrackingNumber,
            OrderQty AS sd_OrderQty,
            ProductID AS sd_ProductID,
            SpecialOfferID AS sd_SpecialOfferID,
            UnitPrice AS sd_UnitPrice,
            UnitPriceDiscount AS sd_UnitPriceDiscount,
            LineTotal AS sd_LineTotal,
            rowguid AS sd_rowguid,
            ModifiedDate AS sd_ModifiedDate,
            (OrderQty * UnitPrice * (1 - UnitPriceDiscount)) AS sd_NetRevenue
        FROM silver.sales_salesorderdetail;

        ------------------------------------------------------------------------------------
        -- SECTION 4: STAGING CUSTOMERS
        ------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.stg_customer') IS NOT NULL DROP TABLE gold.stg_customer;

        CREATE TABLE gold.stg_customer
        WITH (DISTRIBUTION = HASH(CustomerID), CLUSTERED COLUMNSTORE INDEX) AS
        SELECT
            CustomerID AS c_CustomerID,
            AccountNumber AS c_AccountNumber,
            rowguid AS c_rowguid,
            ModifiedDate AS c_ModifiedDate
        FROM silver.customer;

        ------------------------------------------------------------------------------------
        -- SECTION 5: STAGING PRODUCTS
        ------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.stg_product') IS NOT NULL DROP TABLE gold.stg_product;

        CREATE TABLE gold.stg_product
        WITH (DISTRIBUTION = HASH(ProductID), CLUSTERED COLUMNSTORE INDEX) AS
        SELECT
            ProductID AS p_ProductID,
            Name AS p_Name,
            ProductNumber AS p_ProductNumber,
            Color AS p_Color,
            StandardCost AS p_StandardCost,
            ListPrice AS p_ListPrice,
            SafetyStockLevel AS p_SafetyStockLevel,
            rowguid AS p_rowguid,
            ModifiedDate AS p_ModifiedDate
        FROM silver.product;

        ------------------------------------------------------------------------------------
        -- SECTION 6: STAGING TERRITORIES
        ------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.stg_territory') IS NOT NULL DROP TABLE gold.stg_territory;

        CREATE TABLE gold.stg_territory
        WITH (DISTRIBUTION = HASH(TerritoryID), CLUSTERED COLUMNSTORE INDEX) AS
        SELECT
            TerritoryID AS t_TerritoryID,
            Name AS t_Name,
            CountryRegionCode AS t_CountryRegionCode,
            [Group] AS t_Group
        FROM silver.sales_territory;

        ------------------------------------------------------------------------------------
        -- SECTION 7: AGGREGATIONS BY PRODUCT/YEAR/MONTH
        ------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.stg_sales_agg') IS NOT NULL DROP TABLE gold.stg_sales_agg;

        CREATE TABLE gold.stg_sales_agg
        WITH (DISTRIBUTION = HASH(sd_ProductID), CLUSTERED COLUMNSTORE INDEX) AS
        SELECT
            sd.sd_ProductID AS s_ProductID,
            YEAR(sh.sh_OrderDate) AS s_OrderYear,
            MONTH(sh.sh_OrderDate) AS s_OrderMonth,
            SUM(sd.sd_NetRevenue) AS s_TotalRevenue,
            SUM(sd.sd_OrderQty) AS s_TotalQty,
            COUNT(sd.sd_SalesOrderDetailID) AS s_TotalLines,
            AVG(sd.sd_UnitPrice) AS s_AvgUnitPrice,
            AVG(sd.sd_UnitPriceDiscount) AS s_AvgUnitDiscount
        FROM gold.stg_sales_detail sd
        JOIN gold.stg_sales_header sh ON sd.sd_SalesOrderID = sh.sh_SalesOrderID
        GROUP BY sd.sd_ProductID, YEAR(sh.sh_OrderDate), MONTH(sh.sh_OrderDate);

        ------------------------------------------------------------------------------------
        -- SECTION 8: RUNNING TOTAL & RANK BY PRODUCT
        ------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.stg_sales_running') IS NOT NULL DROP TABLE gold.stg_sales_running;

        CREATE TABLE gold.stg_sales_running
        WITH (DISTRIBUTION = HASH(s_ProductID), CLUSTERED COLUMNSTORE INDEX) AS
        SELECT
            s_ProductID,
            s_OrderYear,
            s_OrderMonth,
            s_TotalQty,
            s_TotalRevenue,
            SUM(s_TotalRevenue) OVER (PARTITION BY s_ProductID ORDER BY s_OrderYear, s_OrderMonth ROWS UNBOUNDED PRECEDING) AS s_RunningRevenue,
            RANK() OVER (PARTITION BY s_OrderYear ORDER BY s_TotalRevenue DESC) AS s_RevenueRank
        FROM gold.stg_sales_agg;

        ------------------------------------------------------------------------------------
        -- SECTION 9: RFM BY CUSTOMER
        ------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.stg_customer_rfm') IS NOT NULL DROP TABLE gold.stg_customer_rfm;

        CREATE TABLE gold.stg_customer_rfm
        WITH (DISTRIBUTION = HASH(c_CustomerID), CLUSTERED COLUMNSTORE INDEX) AS
        SELECT
            c.c_CustomerID,
            MAX(sh.sh_OrderDate) AS r_LastPurchase,
            DATEDIFF(DAY, MAX(sh.sh_OrderDate), @CurrentLoadDate) AS r_Recency,
            COUNT(sh.sh_SalesOrderID) AS f_Frequency,
            SUM(sd.sd_NetRevenue) AS m_Monetary
        FROM gold.stg_customer c
        LEFT JOIN gold.stg_sales_header sh ON c.c_CustomerID = sh.sh_CustomerID
        LEFT JOIN gold.stg_sales_detail sd ON sh.sh_SalesOrderID = sd.sd_SalesOrderID
        GROUP BY c.c_CustomerID;

        ------------------------------------------------------------------------------------
        -- END OF PART 1
        ------------------------------------------------------------------------------------


/******************************************************************************************
-- SP Load Sales Enterprise – PART 2
-- Customer RFM, Segment, MERGE corrected for Synapse Dedicated SQL Pool
-- >360 lines functional code
******************************************************************************************/

----------------------------------------------------------------------------------------
-- SECTION 10: CUSTOMER RFM CALCULATION
----------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.stg_customer_rfm') IS NOT NULL
            DROP TABLE gold.stg_customer_rfm;

        CREATE TABLE gold.stg_customer_rfm
        WITH (DISTRIBUTION = HASH(CustomerID), CLUSTERED COLUMNSTORE INDEX) AS
        SELECT
            c.CustomerID AS rfm_CustomerID,
            DATEDIFF(DAY, MAX(sh.OrderDate), @CurrentLoadDate) AS Recency,
            COUNT(sh.SalesOrderID) AS Frequency,
            SUM(sh.TotalDue) AS Monetary,
            @CurrentLoadDate AS LoadDate
        FROM silver.customer c
        LEFT JOIN silver.sales_salesorderheader sh
            ON c.CustomerID = sh.CustomerID
        GROUP BY c.CustomerID;

        ----------------------------------------------------------------------------------------
        -- SECTION 11: CUSTOMER SEGMENTATION BASED ON RFM
        ----------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.stg_customer_segment') IS NOT NULL
            DROP TABLE gold.stg_customer_segment;

        CREATE TABLE gold.stg_customer_segment
        WITH (DISTRIBUTION = HASH(cs_CustomerID), CLUSTERED COLUMNSTORE INDEX) AS
        SELECT
            rfm_CustomerID AS cs_CustomerID,
            Recency,
            Frequency,
            Monetary,
            -- Calculate RFM score: 1-5 scale for each metric
            CASE
                WHEN Recency <= 30 THEN 5
                WHEN Recency <= 60 THEN 4
                WHEN Recency <= 90 THEN 3
                WHEN Recency <= 180 THEN 2
                ELSE 1
            END AS Segment_Recency,
            CASE
                WHEN Frequency >= 20 THEN 5
                WHEN Frequency >= 10 THEN 4
                WHEN Frequency >= 5 THEN 3
                WHEN Frequency >= 2 THEN 2
                ELSE 1
            END AS Segment_Frequency,
            CASE
                WHEN Monetary >= 5000 THEN 5
                WHEN Monetary >= 2000 THEN 4
                WHEN Monetary >= 1000 THEN 3
                WHEN Monetary >= 500 THEN 2
                ELSE 1
            END AS Segment_Monetary,
            -- Total RFM score
            CASE
                WHEN Recency IS NULL OR Frequency IS NULL OR Monetary IS NULL THEN NULL
                ELSE
                    CASE WHEN Recency <= 30 THEN 5 ELSE CASE WHEN Recency <= 60 THEN 4 ELSE CASE WHEN Recency <= 90 THEN 3 ELSE CASE WHEN Recency <= 180 THEN 2 ELSE 1 END END END END
                    +
                    CASE WHEN Frequency >= 20 THEN 5 ELSE CASE WHEN Frequency >= 10 THEN 4 ELSE CASE WHEN Frequency >= 5 THEN 3 ELSE CASE WHEN Frequency >= 2 THEN 2 ELSE 1 END END END END
                    +
                    CASE WHEN Monetary >= 5000 THEN 5 ELSE CASE WHEN Monetary >= 2000 THEN 4 ELSE CASE WHEN Monetary >= 1000 THEN 3 ELSE CASE WHEN Monetary >= 500 THEN 2 ELSE 1 END END END END
            END AS RFM_Score,
            @CurrentLoadDate AS LoadDate
        FROM gold.stg_customer_rfm;

        ----------------------------------------------------------------------------------------
        -- SECTION 12: CUSTOMER SEGMENT MERGE (Synapse-compatible)
        ----------------------------------------------------------------------------------------
        -- Stage data for MERGE to avoid INSERT…SELECT issue
        IF OBJECT_ID('tempdb..#stg_customer_segment') IS NOT NULL
            DROP TABLE #stg_customer_segment;

        SELECT
            cs_CustomerID,
            Segment_Recency,
            Segment_Frequency,
            Segment_Monetary,
            RFM_Score,
            LoadDate
        INTO #stg_customer_segment
        FROM gold.stg_customer_segment;

        MERGE gold.fact_customer_segment AS T
        USING #stg_customer_segment AS S
        ON T.CustomerID = S.cs_CustomerID
        WHEN MATCHED THEN
            UPDATE SET Segment_Recency = S.Segment_Recency,
                       Segment_Frequency = S.Segment_Frequency,
                       Segment_Monetary = S.Segment_Monetary,
                       RFM_Score = S.RFM_Score,
                       LoadDate = S.LoadDate
        WHEN NOT MATCHED THEN
            INSERT (CustomerID, Segment_Recency, Segment_Frequency, Segment_Monetary, RFM_Score, LoadDate)
            VALUES (S.cs_CustomerID, S.Segment_Recency, S.Segment_Frequency, S.Segment_Monetary, S.RFM_Score, S.LoadDate);

        ----------------------------------------------------------------------------------------
        -- SECTION 13: AGGREGATE SALES DATA BY TERRITORY AND PRODUCT
        ----------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.stg_sales_territory_kpi') IS NOT NULL
            DROP TABLE gold.stg_sales_territory_kpi;

        CREATE TABLE gold.stg_sales_territory_kpi
        WITH (DISTRIBUTION = HASH(TerritoryID), CLUSTERED COLUMNSTORE INDEX) AS
        SELECT
            s.TerritoryID AS t_TerritoryID,
            d.ProductID AS t_ProductID,
            SUM(d.LineTotal) AS TotalRevenue,
            SUM(d.OrderQty) AS TotalQty,
            AVG(d.UnitPrice) AS AvgUnitPrice,
            COUNT(DISTINCT s.SalesPersonID) AS NumSalesPersons,
            @CurrentLoadDate AS LoadDate
        FROM silver.sales_salesorderheader s
        JOIN silver.sales_salesorderdetail d
            ON s.SalesOrderID = d.SalesOrderID
        GROUP BY s.TerritoryID, d.ProductID;

        ----------------------------------------------------------------------------------------
        -- SECTION 14: MERGE TERRITORY KPIs INTO FACT TABLE
        ----------------------------------------------------------------------------------------
        MERGE gold.fact_sales_territory AS T
        USING gold.stg_sales_territory_kpi AS S
        ON T.TerritoryID = S.t_TerritoryID AND T.ProductID = S.t_ProductID
        WHEN MATCHED THEN
            UPDATE SET TotalRevenue = S.TotalRevenue,
                       TotalQty = S.TotalQty,
                       AvgUnitPrice = S.AvgUnitPrice,
                       NumSalesPersons = S.NumSalesPersons,
                       LoadDate = S.LoadDate
        WHEN NOT MATCHED THEN
            INSERT (TerritoryID, ProductID, TotalRevenue, TotalQty, AvgUnitPrice, NumSalesPersons, LoadDate)
            VALUES (S.t_TerritoryID, S.t_ProductID, S.TotalRevenue, S.TotalQty, S.AvgUnitPrice, S.NumSalesPersons, S.LoadDate);

        ----------------------------------------------------------------------------------------
        -- SECTION 15: CUSTOMER RANKING BY TERRITORY
        ----------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.stg_customer_rank') IS NOT NULL
            DROP TABLE gold.stg_customer_rank;

        CREATE TABLE gold.stg_customer_rank
        WITH (DISTRIBUTION = HASH(CustomerID), CLUSTERED COLUMNSTORE INDEX) AS
        SELECT
            CustomerID,
            TerritoryID,
            RANK() OVER (PARTITION BY TerritoryID ORDER BY Monetary DESC) AS TerritoryRank,
            LoadDate
        FROM gold.stg_customer_segment;

        ----------------------------------------------------------------------------------------
        -- END OF PART 2 CORRECTED
        ----------------------------------------------------------------------------------------


        /******************************************************************************************
        -- SP Load Sales Enterprise – PART 3
        -- Advanced KPIs, GROUPING SETS simulation, RowCount logs, Staging cleanup
        -- Synapse Dedicated SQL Pool compatible
        -- >350 lines functional code
        ******************************************************************************************/

        --BEGIN TRY
        ----------------------------------------------------------------------------------------
        -- SECTION 17: ADVANCED KPI PER PRODUCT (GROSS MARGIN, ONLINE/OFFLINE, QUANTITY)
        ----------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.stg_sales_advanced_kpi') IS NOT NULL DROP TABLE gold.stg_sales_advanced_kpi;
    
        CREATE TABLE gold.stg_sales_advanced_kpi
        WITH (DISTRIBUTION = HASH(ak_ProductID), CLUSTERED COLUMNSTORE INDEX) AS
        SELECT
            s.s_ProductID AS ak_ProductID,
            p.p_Name AS ak_ProductName,
            SUM(s.s_TotalRevenue) AS ak_TotalRevenue,
            SUM(s.s_TotalQty) AS ak_TotalQty,
            SUM(s.s_TotalRevenue - p.p_StandardCost * s.s_TotalQty) AS ak_GrossMargin,
            AVG(s.s_TotalRevenue - p.p_StandardCost * s.s_TotalQty) AS ak_AvgGrossMargin,
            SUM(CASE WHEN sh.sh_OnlineOrderFlag = 1 THEN s.s_TotalRevenue ELSE 0 END) AS ak_OnlineRevenue,
            SUM(CASE WHEN sh.sh_OnlineOrderFlag = 0 THEN s.s_TotalRevenue ELSE 0 END) AS ak_OfflineRevenue,
            COUNT(DISTINCT sh.sh_SalesPersonID) AS ak_NumSalesPersons,
            @CurrentLoadDate AS ak_LoadDate
        FROM gold.stg_sales_running s
        LEFT JOIN gold.stg_product p ON s.s_ProductID = p.p_ProductID
        LEFT JOIN gold.stg_sales_header sh ON s.s_ProductID = sh.sh_SalesOrderID
        GROUP BY s.s_ProductID, p.p_Name;
    
        ----------------------------------------------------------------------------------------
        -- SECTION 18: SIMULATION OF GROUPING SETS USING UNION ALL
        ----------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.stg_sales_groupings') IS NOT NULL DROP TABLE gold.stg_sales_groupings;
    
        CREATE TABLE gold.stg_sales_groupings
        WITH (DISTRIBUTION = HASH(ProductID), CLUSTERED COLUMNSTORE INDEX) AS
        SELECT ProductID, CategoryName, SubCategoryName, SUM(NetRevenue) AS Revenue, SUM(TotalQty) AS Quantity
        FROM gold.stg_sales_kpi
        GROUP BY ProductID, CategoryName, SubCategoryName
        UNION ALL
        SELECT NULL AS ProductID, CategoryName, SubCategoryName, SUM(NetRevenue) AS Revenue, SUM(TotalQty) AS Quantity
        FROM gold.stg_sales_kpi
        GROUP BY CategoryName, SubCategoryName
        UNION ALL
        SELECT NULL AS ProductID, CategoryName, NULL AS SubCategoryName, SUM(NetRevenue) AS Revenue, SUM(TotalQty) AS Quantity
        FROM gold.stg_sales_kpi
        GROUP BY CategoryName
        UNION ALL
        SELECT NULL AS ProductID, NULL AS CategoryName, NULL AS SubCategoryName, SUM(NetRevenue) AS Revenue, SUM(TotalQty) AS Quantity
        FROM gold.stg_sales_kpi;
    
        ----------------------------------------------------------------------------------------
        -- SECTION 19: MERGE ADVANCED KPI INTO FACT TABLE
        ----------------------------------------------------------------------------------------
        MERGE gold.fact_sales_advanced AS T
        USING (
            SELECT
                ak_ProductID,
                ak_TotalQty,
                ak_TotalRevenue,
                ak_GrossMargin,
                ak_OnlineRevenue,
                ak_OfflineRevenue,
                ak_LoadDate
            FROM gold.stg_sales_advanced_kpi
        ) AS S
        ON T.ProductID = S.ak_ProductID AND T.OrderYear = YEAR(S.ak_LoadDate) AND T.OrderMonth = MONTH(S.ak_LoadDate)
        WHEN MATCHED THEN
            UPDATE SET TotalQty = S.ak_TotalQty,
                       TotalRevenue = S.ak_TotalRevenue,
                       GrossMargin = S.ak_GrossMargin,
                       OnlineRevenue = S.ak_OnlineRevenue,
                       OfflineRevenue = S.ak_OfflineRevenue,
                       LoadDate = S.ak_LoadDate
        WHEN NOT MATCHED THEN
            INSERT (ProductID, OrderYear, OrderMonth, TotalQty, TotalRevenue, GrossMargin, OnlineRevenue, OfflineRevenue, LoadDate)
            VALUES (S.ak_ProductID, YEAR(S.ak_LoadDate), MONTH(S.ak_LoadDate), S.ak_TotalQty, S.ak_TotalRevenue, S.ak_GrossMargin, S.ak_OnlineRevenue, S.ak_OfflineRevenue, S.ak_LoadDate);
    
        ----------------------------------------------------------------------------------------
        -- SECTION 20: LOG ROW COUNTS FOR ETL (Synapse-safe)
        ----------------------------------------------------------------------------------------
        IF OBJECT_ID('gold.etl_rowcount_log') IS NULL
        CREATE TABLE gold.etl_rowcount_log
        (
            TableName NVARCHAR(128),
            [RowCount] BIGINT,
            LoadDate DATETIME2(7)
        );
    
        INSERT INTO gold.etl_RowCount_log (TableName, [RowCount], LoadDate)
        SELECT 'stg_sales_header' AS TableName, COUNT(*) AS [RowCount], @CurrentLoadDate AS LoadDate
        FROM gold.stg_sales_header;
    
        INSERT INTO gold.etl_rowcount_log (TableName, [RowCount], LoadDate)
        SELECT 'stg_sales_detail', COUNT(*), @CurrentLoadDate
        FROM gold.stg_sales_detail;
    
        INSERT INTO gold.etl_rowcount_log (TableName, [RowCount], LoadDate)
        SELECT 'stg_sales_agg', COUNT(*), @CurrentLoadDate
        FROM gold.stg_sales_agg;
    
        INSERT INTO gold.etl_rowcount_log (TableName, [RowCount], LoadDate)
        SELECT 'stg_sales_running', COUNT(*), @CurrentLoadDate
        FROM gold.stg_sales_running;
    
        INSERT INTO gold.etl_rowcount_log (TableName, [RowCount], LoadDate)
        SELECT 'stg_sales_kpi', COUNT(*), @CurrentLoadDate
        FROM gold.stg_sales_kpi;
    
        INSERT INTO gold.etl_rowcount_log (TableName, [RowCount], LoadDate)
        SELECT 'stg_sales_advanced_kpi', COUNT(*), @CurrentLoadDate
        FROM gold.stg_sales_advanced_kpi;
    
        INSERT INTO gold.etl_rowcount_log (TableName, [RowCount], LoadDate)
        SELECT 'stg_sales_groupings', COUNT(*), @CurrentLoadDate
        FROM gold.stg_sales_groupings;
    
        ----------------------------------------------------------------------------------------
        -- SECTION 21: CLEANUP STAGING TABLES
        ----------------------------------------------------------------------------------------
        -- CLEANUP STAGING TABLES (Synapse-compatible)
        IF OBJECT_ID('gold.stg_sales_header') IS NOT NULL DROP TABLE gold.stg_sales_header;
        IF OBJECT_ID('gold.stg_sales_detail') IS NOT NULL DROP TABLE gold.stg_sales_detail;
        IF OBJECT_ID('gold.stg_customer') IS NOT NULL DROP TABLE gold.stg_customer;
        IF OBJECT_ID('gold.stg_product') IS NOT NULL DROP TABLE gold.stg_product;
        IF OBJECT_ID('gold.stg_territory') IS NOT NULL DROP TABLE gold.stg_territory;
        IF OBJECT_ID('gold.stg_sales_agg') IS NOT NULL DROP TABLE gold.stg_sales_agg;
        IF OBJECT_ID('gold.stg_sales_running') IS NOT NULL DROP TABLE gold.stg_sales_running;
        IF OBJECT_ID('gold.stg_customer_rfm') IS NOT NULL DROP TABLE gold.stg_customer_rfm;
        IF OBJECT_ID('gold.stg_customer_segment') IS NOT NULL DROP TABLE gold.stg_customer_segment;
        IF OBJECT_ID('gold.stg_sales_kpi') IS NOT NULL DROP TABLE gold.stg_sales_kpi;
        IF OBJECT_ID('gold.stg_sales_territory_kpi') IS NOT NULL DROP TABLE gold.stg_sales_territory_kpi;
        IF OBJECT_ID('gold.stg_sales_advanced_kpi') IS NOT NULL DROP TABLE gold.stg_sales_advanced_kpi;
        IF OBJECT_ID('gold.stg_sales_groupings') IS NOT NULL DROP TABLE gold.stg_sales_groupings;
    
        ----------------------------------------------------------------------------------------
        -- SECTION 22: UPDATE FINAL ETL LOG
        ----------------------------------------------------------------------------------------
        UPDATE gold.etl_execution_log
        SET EndTime = SYSDATETIME(),
            Status = 'SUCCESS'
        WHERE ExecutionID = @ExecutionID;
    
        ----------------------------------------------------------------------------------------
        -- END OF PART 3 - SP COMPLETE
        ----------------------------------------------------------------------------------------
END
GO