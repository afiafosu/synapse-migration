-- =====================================================
-- execute_all_gold_sps.sql 
-- Master script to execute all Production Gold Layer SPs
-- Executes in order: Simple → Moderate → Complex → Very Complex
-- Updates watermarks automatically
-- =====================================================
CREATE PROCEDURE gold.sp_gold_production_orch
AS 
BEGIN 
    SET NOCOUNT ON;

    EXEC gold.usp_Load_DimProduct;
    EXEC gold.usp_Load_DimProductWithDocument;
    EXEC gold.usp_Materialize_ProductKPIs;
    EXEC gold.usp_Materialize_ProductInventoryKPIs;
    EXEC gold.usp_Materialize_ProductCostHistoryRollup;
    EXEC gold.usp_Materialize_ProductRevenueKPIs;
    EXEC gold.usp_Materialize_ProductReviewAnalytics;
END; 
GO
