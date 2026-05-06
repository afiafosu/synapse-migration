CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_refresh_all_gold`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    call main.gold.sp_materialize_person_360();
    call main.gold.sp_materialize_contact_rollup();
    call main.gold.sp_materialize_location_dim();
    call main.gold.sp_materialize_person_contactability();
    call main.gold.sp_materialize_person_kpis();
    
    /*SELECT
        name                   AS gold_table,
        create_date            AS rebound_at_utc,
        SCHEMA_NAME(schema_id) AS `schema`
    FROM sys.tables
    WHERE SCHEMA_NAME(schema_id) = 'gold'
    ORDER BY create_date DESC;*/
END;
