CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_load_sales_enterprise`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    ----------------------------------------------------------------------------------------
    -- SECTION 0: DECLARE VARIABLES
    ----------------------------------------------------------------------------------------
    DECLARE VARIABLE V_CurrentLoadDate timestamp ;
    DECLARE VARIABLE V_ExecutionID STRING ;
    DECLARE VARIABLE V_EntityName STRING ;
    DECLARE VARIABLE V_ErrorMessage STRING;
    
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
    
    ----------------------------------------------------------------------------------------
    -- SECTION 1: LOG START OF EXECUTION
    ---------------------------------------------------------------------------------------- 
    SET V_CurrentLoadDate = current_timestamp();
    SET V_ExecutionID = UUID();
    SET V_EntityName = 'SalesEnterprise'; 
    
    INSERT INTO main.gold.etl_execution_log (ExecutionID, EntityName, StartTime, Status)
    VALUES (V_ExecutionID, V_EntityName, V_CurrentLoadDate, 'RUNNING');
    
    ------------------------------------------------------------------------------------
    -- SECTION 2: STAGING SALES HEADER
    ------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS main.gold.stg_sales_header;
    
    CREATE OR REPLACE TABLE main.gold.stg_sales_header AS
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
    FROM main.silver.sales_salesorderheader;
    
    ------------------------------------------------------------------------------------
    -- SECTION 3: STAGING SALES DETAIL
    ------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS main.gold.stg_sales_detail;
    
    CREATE OR REPLACE TABLE main.gold.stg_sales_detail AS
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
    FROM main.silver.sales_salesorderdetail;
    
    ------------------------------------------------------------------------------------
    -- SECTION 4: STAGING CUSTOMERS
    ------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS main.gold.stg_customer;
    
    CREATE OR REPLACE TABLE main.gold.stg_customer AS
    SELECT
        CustomerID AS c_CustomerID,
        AccountNumber AS c_AccountNumber,
        rowguid AS c_rowguid,
        ModifiedDate AS c_ModifiedDate
    FROM main.silver.customer;
    
    ------------------------------------------------------------------------------------
    -- SECTION 5: STAGING PRODUCTS
    ------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS main.gold.stg_product;
    
    CREATE OR REPLACE TABLE main.gold.stg_product AS
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
    FROM main.silver.product;
    
    ------------------------------------------------------------------------------------
    -- SECTION 6: STAGING TERRITORIES
    ------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS main.gold.stg_territory;
    
    CREATE OR REPLACE TABLE main.gold.stg_territory AS
    SELECT
        TerritoryID AS t_TerritoryID,
        Name AS t_Name,
        CountryRegionCode AS t_CountryRegionCode,
        `Group` AS t_Group
    FROM main.silver.sales_territory;
    
    ------------------------------------------------------------------------------------
    -- SECTION 7: AGGREGATIONS BY PRODUCT/YEAR/MONTH
    ------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS main.gold.stg_sales_agg;
    
    CREATE OR REPLACE TABLE main.gold.stg_sales_agg AS
    SELECT
        sd.sd_ProductID AS s_ProductID,
        YEAR(sh.sh_OrderDate) AS s_OrderYear,
        MONTH(sh.sh_OrderDate) AS s_OrderMonth,
        SUM(sd.sd_NetRevenue) AS s_TotalRevenue,
        SUM(sd.sd_OrderQty) AS s_TotalQty,
        COUNT(sd.sd_SalesOrderDetailID) AS s_TotalLines,
        AVG(sd.sd_UnitPrice) AS s_AvgUnitPrice,
        AVG(sd.sd_UnitPriceDiscount) AS s_AvgUnitDiscount
    FROM main.gold.stg_sales_detail sd
    JOIN main.gold.stg_sales_header sh ON sd.sd_SalesOrderID = sh.sh_SalesOrderID
    GROUP BY sd.sd_ProductID, YEAR(sh.sh_OrderDate), MONTH(sh.sh_OrderDate);
    
    ------------------------------------------------------------------------------------
    -- SECTION 8: RUNNING TOTAL & RANK BY PRODUCT
    ------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS main.gold.stg_sales_running;
    
    CREATE OR REPLACE TABLE main.gold.stg_sales_running AS
    SELECT
        s_ProductID AS s_ProductID,
        s_OrderYear AS s_OrderYear,
        s_OrderMonth AS s_OrderMonth,
        s_TotalQty AS s_TotalQty,
        s_TotalRevenue AS s_TotalRevenue,
        SUM(s_TotalRevenue) OVER (PARTITION BY s_ProductID ORDER BY s_OrderYear, s_OrderMonth ROWS UNBOUNDED PRECEDING) AS s_RunningRevenue,
        RANK() OVER (PARTITION BY s_OrderYear ORDER BY s_TotalRevenue DESC) AS s_RevenueRank
    FROM main.gold.stg_sales_agg;
    
    ------------------------------------------------------------------------------------
    -- SECTION 9: RFM BY CUSTOMER
    ------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS main.gold.stg_customer_rfm;
    CREATE OR REPLACE TABLE main.gold.stg_customer_rfm AS
    SELECT
        c.c_CustomerID AS c_CustomerID,
        MAX(sh.sh_OrderDate) AS r_LastPurchase,
        DATEDIFF(MAX(sh.sh_OrderDate), V_CurrentLoadDate) AS r_Recency,
        COUNT(sh.sh_SalesOrderID) AS f_Frequency,
        SUM(sd.sd_NetRevenue) AS m_Monetary
    FROM main.gold.stg_customer c
    LEFT JOIN main.gold.stg_sales_header sh ON c.c_CustomerID = sh.sh_CustomerID
    LEFT JOIN main.gold.stg_sales_detail sd ON sh.sh_SalesOrderID = sd.sd_SalesOrderID
    GROUP BY c.c_CustomerID;
    
    ------------------------------------------------------------------------------------
    -- END OF PART 1
    ------------------------------------------------------------------------------------
END;
