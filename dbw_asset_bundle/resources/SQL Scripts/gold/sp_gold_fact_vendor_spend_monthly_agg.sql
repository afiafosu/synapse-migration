CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_gold_fact_vendor_spend_monthly_agg`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    -- EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS `main`.`gold` MANAGED LOCATION ''abfss://fabmigation1@synpasetofabric.dfs.core.windows.net/'';';
    
    DROP TABLE IF EXISTS main.gold.FactVendorSpendMonthlyAgg;
    /*
      MonthStartDate: primer día del mes (PDW-friendly)
      DATEADD(month, DATEDIFF(month, 0, OrderDate), 0)
    */
    
    CREATE OR REPLACE TABLE main.gold.FactVendorSpendMonthlyAgg AS
    WITH H AS(
        SELECT
            CAST(PurchaseOrderID AS int) AS PurchaseOrderID,
            CAST(VendorID AS int)        AS VendorID,
            CAST(OrderDate AS date)      AS OrderDate
        FROM main.silver.purchasing_purchaseorderheader),
        -- Si tu tabla está en silver: cambia a silver.purchasing_purchaseorderheader
    D AS(
        SELECT
            CAST(PurchaseOrderID AS int) AS PurchaseOrderID,
            CAST(
                COALESCE(LineTotal, (OrderQty * UnitPrice))
                AS decimal(19,4)
            ) AS LineAmount
        FROM main.silver.purchasing_purchaseorderdetail),
        -- Si tu tabla está en silver: cambia a silver.purchasing_purchaseorderdetail
    L AS(
        SELECT
            h.VendorID,
            DATE_TRUNC('MONTH', h.OrderDate) AS MonthStartDate,
            h.PurchaseOrderID,
            d.LineAmount
        FROM H h
        JOIN D d
          ON d.PurchaseOrderID = h.PurchaseOrderID)
    SELECT
        /* Vendor ID en header (AdventureWorks) */
        CAST(l.VendorID AS int) AS VendorBusinessEntityID,
        l.MonthStartDate,
        
        /* Vendor attributes desde silver.purchasing_vendor (degradan a NULL si no hay match) */
        MAX(v.AccountNumber)           AS VendorAccountNumber,
        MAX(v.`Name`)                  AS VendorName,
        MAX(v.CreditRating)            AS VendorCreditRating,
        CAST(MAX(CAST(v.PreferredVendorStatus AS int)) AS BOOLEAN) AS PreferredVendorStatus,
        CAST(MAX(CAST(v.ActiveFlag AS int)) AS BOOLEAN)            AS VendorActiveFlag,
        MAX(v.PurchasingWebServiceURL) AS PurchasingWebServiceURL,
        MAX(v.ModifiedDate)            AS VendorModifiedDate,

        /* Métricas */
        SUM(l.LineAmount)                 AS TotalSpend,
        COUNT(DISTINCT l.PurchaseOrderID) AS POCount,
        COUNT(1)                          AS LineCount,
        AVG(l.LineAmount)                 AS AvgLineAmount,
        MIN(l.LineAmount)                 AS MinLineAmount,
        MAX(l.LineAmount)                 AS MaxLineAmount,

        current_timestamp()                      AS LoadDate
    FROM L l
    LEFT JOIN main.silver.purchasing_vendor v
      ON v.BusinessEntityID = l.VendorID
    GROUP BY
        l.VendorID,
        l.MonthStartDate;
END;
