CREATE PROCEDURE gold.sp_gold_sales_orchestrator 
AS 
BEGIN 
    SET NOCOUNT ON;

    EXEC gold.sp_gold_sales_dimensions;

    EXEC gold.sp_gold_sales_factheader;

    EXEC gold.sp_gold_sales_factdetail;

    EXEC gold.sp_gold_sales_views;
END; 
GO