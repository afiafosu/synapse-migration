CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_gold_recon_po_totals_header_vs_detail`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    -- Drop tabla gold si existe
    DROP TABLE IF EXISTS main.gold.ReconPOTotalsHeaderVsDetail;
    
    /*  ============================
        CTAS: Recon por PurchaseOrderID
        ============================ */
    
    CREATE OR REPLACE TABLE main.gold.ReconPOTotalsHeaderVsDetail AS 
    WITH H AS(
        SELECT
            CAST(PurchaseOrderID AS int) AS PurchaseOrderID,
            CAST(SubTotal AS decimal(19,4)) AS HeaderSubTotal,
            CAST(TaxAmt  AS decimal(19,4)) AS HeaderTaxAmt,
            CAST(Freight AS decimal(19,4)) AS HeaderFreight,
            CAST(TotalDue AS decimal(19,4)) AS HeaderTotalDue
        FROM main.silver.purchasing_purchaseorderheader),
    D AS(
        SELECT
            CAST(PurchaseOrderID AS int) AS PurchaseOrderID,
            SUM(
                CAST(
                    COALESCE(LineTotal, (OrderQty * UnitPrice))
                    AS decimal(19,4)
                )
            ) AS DetailSubTotal
        FROM main.silver.purchasing_purchaseorderdetail
        GROUP BY PurchaseOrderID)
    SELECT
        COALESCE(H.PurchaseOrderID, D.PurchaseOrderID) AS PurchaseOrderID,

        H.HeaderSubTotal,
        H.HeaderTaxAmt,
        H.HeaderFreight,
        H.HeaderTotalDue,

        D.DetailSubTotal,

        /* Expected total due based on detail + header charges */
        CAST(
            COALESCE(D.DetailSubTotal, 0)
            + COALESCE(H.HeaderTaxAmt, 0)
            + COALESCE(H.HeaderFreight, 0)
            AS decimal(19,4)
        ) AS ExpectedTotalDue,

        /* Differences */
        CAST(COALESCE(H.HeaderSubTotal, 0) - COALESCE(D.DetailSubTotal, 0) AS decimal(19,4)) AS Diff_SubTotal,
        CAST(COALESCE(H.HeaderTotalDue, 0) - (COALESCE(D.DetailSubTotal, 0) + COALESCE(H.HeaderTaxAmt, 0) + COALESCE(H.HeaderFreight, 0))
             AS decimal(19,4)) AS Diff_TotalDue,

        /* Tolerance flags (0.01) */
        CASE
            WHEN H.PurchaseOrderID IS NULL THEN 0
            WHEN D.PurchaseOrderID IS NULL THEN 0
            WHEN ABS(COALESCE(H.HeaderSubTotal, 0) - COALESCE(D.DetailSubTotal, 0)) <= CAST(0.01 AS decimal(19,4)) THEN 1
            ELSE 0
        END AS IsSubTotalMatch,

        CASE
            WHEN H.PurchaseOrderID IS NULL THEN 0
            WHEN D.PurchaseOrderID IS NULL THEN 0
            WHEN ABS(
                COALESCE(H.HeaderTotalDue, 0) -
                (COALESCE(D.DetailSubTotal, 0) + COALESCE(H.HeaderTaxAmt, 0) + COALESCE(H.HeaderFreight, 0))
            ) <= CAST(0.01 AS decimal(19,4))
            THEN 1
            ELSE 0
        END AS IsTotalDueMatch,

        /* Recon status */
        CASE
            WHEN H.PurchaseOrderID IS NULL THEN 'MISSING_HEADER'
            WHEN D.PurchaseOrderID IS NULL THEN 'MISSING_DETAIL'
            WHEN ABS(COALESCE(H.HeaderSubTotal, 0) - COALESCE(D.DetailSubTotal, 0)) <= CAST(0.01 AS decimal(19,4))
             AND ABS(COALESCE(H.HeaderTotalDue, 0) -
                     (COALESCE(D.DetailSubTotal, 0) + COALESCE(H.HeaderTaxAmt, 0) + COALESCE(H.HeaderFreight, 0))
                    ) <= CAST(0.01 AS decimal(19,4))
            THEN 'OK'
            ELSE 'MISMATCH'
        END AS ReconStatus,

        current_timestamp() AS LoadDate
    FROM H
    FULL OUTER JOIN D
      ON H.PurchaseOrderID = D.PurchaseOrderID;
END;
