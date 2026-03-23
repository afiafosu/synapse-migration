DECLARE @NowDate DATETIME2;
SET @NowDate = GETDATE();

INSERT INTO [config].[tbl_GoldSP] VALUES (1, 'Person Gold', '[gold].[sp_refresh_all_gold]', 1, @NowDate, @NowDate);

INSERT INTO [config].[tbl_GoldSP] VALUES (2, 'Sales Gold', '[gold].[sp_gold_sales_orchestrator]', 1, @NowDate, @NowDate);

INSERT INTO [config].[tbl_GoldSP] VALUES (3, 'HR Gold', '[gold].[sp_gold_dim_shift_upsert]', 1, @NowDate, @NowDate);

INSERT INTO [config].[tbl_GoldSP] VALUES (4, 'HR Gold', '[gold].[sp_gold_fact_employee_tenure]', 1, @NowDate, @NowDate);

INSERT INTO [config].[tbl_GoldSP] VALUES (5, 'HR Gold', '[gold].[sp_gold_hr_employee_current_assignment_pay]', 1, @NowDate, @NowDate);

INSERT INTO [config].[tbl_GoldSP] VALUES (6, 'Purchasing Gold', '[gold].[sp_gold_fact_vendor_spend_monthly_agg]', 1, @NowDate, @NowDate);

INSERT INTO [config].[tbl_GoldSP] VALUES (7, 'Purchasing Gold', '[gold].[sp_gold_recon_po_totals_header_vs_detail]', 1, @NowDate, @NowDate);

INSERT INTO [config].[tbl_GoldSP] VALUES (8, 'Sales 2 Gold', '[gold].[sp_gold_customer_sales_360]', 1, @NowDate, @NowDate);

INSERT INTO [config].[tbl_GoldSP] VALUES (9, 'Product Gold', '[gold].[sp_gold_production_orch]', 1, @NowDate, @NowDate);
