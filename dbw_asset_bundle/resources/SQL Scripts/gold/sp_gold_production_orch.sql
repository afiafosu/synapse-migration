CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_gold_production_orch`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    call main.gold.usp_Load_DimProduct();
    call main.gold.usp_Load_DimProductWithDocument();
    call main.gold.usp_Materialize_ProductKPIs();
    call main.gold.usp_Materialize_ProductInventoryKPIs();
    call main.gold.usp_Materialize_ProductCostHistoryRollup();
    call main.gold.usp_Materialize_ProductRevenueKPIs();
    call main.gold.usp_Materialize_ProductReviewAnalytics();
END;
