CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_gold_sales_orchestrator`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    call main.gold.sp_gold_sales_dimensions();
    call main.gold.sp_gold_sales_factheader();
    call main.gold.sp_gold_sales_factdetail();
    call main.gold.sp_gold_sales_views();
END;
