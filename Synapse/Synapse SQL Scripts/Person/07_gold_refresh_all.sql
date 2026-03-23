-- =========================
-- 04_gold_refresh_all.sql
-- =========================
IF OBJECT_ID('gold.sp_refresh_all_gold', 'P') IS NOT NULL
    DROP PROCEDURE gold.sp_refresh_all_gold;
GO
CREATE PROCEDURE gold.sp_refresh_all_gold
AS
BEGIN
  SET NOCOUNT ON;

  EXEC gold.sp_materialize_person_360;
  EXEC gold.sp_materialize_contact_rollup;
  EXEC gold.sp_materialize_location_dim;
  EXEC gold.sp_materialize_person_contactability;
  EXEC gold.sp_materialize_person_kpis;

  SELECT
    name                   AS gold_table,
    create_date            AS rebound_at_utc,
    SCHEMA_NAME(schema_id) AS [schema]
  FROM sys.tables
  WHERE SCHEMA_NAME(schema_id) = 'gold'
  ORDER BY create_date DESC;
END
GO