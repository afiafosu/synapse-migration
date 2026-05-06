CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_gold_customer_sales_360`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    -- Tabla destino (full refresh)
    DROP TABLE IF EXISTS main.gold.CustomerSales360;
    
    /*
      Sources (ajusta si tus objetos tienen otro nombre):
        - gold.factSalesOrder          (header: 1 fila por orden)
        - gold.factSalesOrderDetail    (detail: 1 fila por línea)
        - gold.dimsalescustomer        (dim para atributos de customer)
    */
    
    CREATE OR REPLACE TABLE main.gold.CustomerSales360 AS
    WITH H AS (
        SELECT
            CAST(SalesOrderID AS bigint)        AS SalesOrderID,
            CAST(CustomerID   AS bigint)        AS CustomerID,
            CAST(OrderDate    AS date)          AS OrderDate,
            CAST(SubTotal     AS decimal(19,4)) AS SubTotal,
            CAST(TaxAmt       AS decimal(19,4)) AS TaxAmt,
            CAST(Freight      AS decimal(19,4)) AS Freight,
            CAST(TotalDue     AS decimal(19,4)) AS TotalDue
        FROM main.gold.factSalesOrder),
    D AS (
        SELECT
            CAST(SalesOrderID AS bigint)        AS SalesOrderID,
            CAST(ProductID    AS bigint)        AS ProductID,
            CAST(OrderQty     AS decimal(19,4)) AS OrderQty,
            CAST(
                COALESCE(NetLineAmount, (OrderQty * UnitPrice))
                AS decimal(19,4)
            ) AS LineAmount
        FROM main.gold.factSalesOrderDetail),
    AggDetail AS (
        SELECT
            SalesOrderID,
            SUM(OrderQty)              AS TotalUnits,
            COUNT(1)                   AS LineCount,
            COUNT(DISTINCT ProductID)  AS DistinctProducts,
            SUM(LineAmount)            AS DetailAmount
        FROM D
        GROUP BY SalesOrderID),
    AggCustomer AS (
        SELECT
            h.CustomerID,

            MIN(h.OrderDate) AS FirstOrderDate,
            MAX(h.OrderDate) AS LastOrderDate,

            COUNT(DISTINCT h.SalesOrderID) AS OrderCount,

            SUM(h.SubTotal) AS TotalSubTotal,
            SUM(h.TaxAmt)   AS TotalTaxAmt,
            SUM(h.Freight)  AS TotalFreight,
            SUM(h.TotalDue) AS TotalRevenue,

            SUM(COALESCE(ad.TotalUnits,0))       AS TotalUnits,
            SUM(COALESCE(ad.LineCount,0))        AS TotalLines,
            SUM(COALESCE(ad.DistinctProducts,0)) AS SumDistinctProductsPerOrder,
            SUM(COALESCE(ad.DetailAmount,0))     AS TotalDetailAmount
        FROM H h
        LEFT JOIN AggDetail ad
          ON ad.SalesOrderID = h.SalesOrderID
        GROUP BY h.CustomerID)
    SELECT
        ac.CustomerID,

        /* ======= Atributos desde gold.dimsalescustomer (según tu foto) ======= */
        MAX(dc.FirstName)       AS FirstName,
        MAX(dc.LastName)        AS LastName,
        MAX(dc.preferred_email) AS preferred_email,
        MAX(dc.AddressLine1)    AS AddressLine1,
        MAX(dc.City)            AS City,
        MAX(dc.StateProvince)   AS StateProvince,
        MAX(dc.CountryRegion)   AS CountryRegion,

        /* Nombre derivado (útil para reporting) */
        MAX(
            LTRIM(RTRIM(
                COALESCE(dc.FirstName, '') ||
                CASE
                    WHEN dc.FirstName IS NOT NULL AND dc.LastName IS NOT NULL THEN ' '
                    ELSE ''
                END ||
                COALESCE(dc.LastName, '')
            ))
        ) AS CustomerName,

        ac.FirstOrderDate,
        ac.LastOrderDate,
        ac.OrderCount,

        ac.TotalSubTotal,
        ac.TotalTaxAmt,
        ac.TotalFreight,
        ac.TotalRevenue,

        ac.TotalUnits,
        ac.TotalLines,

        -- KPI's derivados
        CASE WHEN ac.OrderCount = 0 THEN NULL
             ELSE CAST(ac.TotalRevenue AS decimal(19,4)) / CAST(ac.OrderCount AS decimal(19,4))
        END AS AvgOrderValue,

        CASE WHEN ac.TotalLines = 0 THEN NULL
             ELSE CAST(ac.TotalDetailAmount AS decimal(19,4)) / CAST(ac.TotalLines AS decimal(19,4))
        END AS AvgLineAmount,

        CASE WHEN ac.OrderCount = 0 THEN NULL
             ELSE CAST(ac.SumDistinctProductsPerOrder AS decimal(19,4)) / CAST(ac.OrderCount AS decimal(19,4))
        END AS AvgDistinctProductsPerOrder,

        -- "Active" heurístico: compró en los últimos 365 días (ajusta ventana si quieres)
        CASE
            WHEN ac.LastOrderDate IS NULL THEN CAST(0 AS BOOLEAN)
            WHEN DATEDIFF(ac.LastOrderDate, CAST(current_timestamp() AS date)) <= 365 THEN CAST(1 AS BOOLEAN)
            ELSE CAST(0 AS BOOLEAN)
        END AS IsActiveLast365Days,

        current_timestamp() AS LoadDate
    FROM AggCustomer ac
    LEFT JOIN main.gold.dimsalescustomer dc
      ON dc.CustomerID = ac.CustomerID
    GROUP BY
        ac.CustomerID,
        ac.FirstOrderDate,
        ac.LastOrderDate,
        ac.OrderCount,
        ac.TotalSubTotal,
        ac.TotalTaxAmt,
        ac.TotalFreight,
        ac.TotalRevenue,
        ac.TotalUnits,
        ac.TotalLines,
        ac.SumDistinctProductsPerOrder,
        ac.TotalDetailAmount;
END;
