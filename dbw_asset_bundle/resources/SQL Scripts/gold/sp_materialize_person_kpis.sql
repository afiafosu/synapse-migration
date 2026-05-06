CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_materialize_person_kpis`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE VARIABLE V_sql STRING ;
    
    DROP TABLE IF EXISTS main.gold.person_kpis;
    
    SET V_sql = '
    CREATE OR REPLACE TABLE main.gold.person_kpis AS
    WITH base AS (
        SELECT * FROM main.gold.person_360),
    email_breakdown AS (
        SELECT 
            "email_domain_category:"|| COALESCE(email_domain_category, "UNKNOWN") AS metric,
            COUNT(*) AS value
        FROM base
        GROUP BY email_domain_category),
    phone_breakdown AS (
        SELECT 
            "phone_type:"|| COALESCE(preferred_phone_type, "UNKNOWN") AS metric,
            COUNT(*) AS value
        FROM base
        GROUP BY preferred_phone_type),
    role_breakdown AS (
        SELECT 
            "contact_role:"|| COALESCE(contact_type_name, "UNKNOWN") AS metric,
            COUNT(*) AS value
        FROM main.gold.person_contact_rollup
        GROUP BY contact_type_name),
    totals AS (
        SELECT 
            "total_persons" AS metric,
            COUNT(DISTINCT business_entity_id) AS value
        FROM base   )
    SELECT * FROM totals
    UNION ALL SELECT * FROM email_breakdown
    UNION ALL SELECT * FROM phone_breakdown
    UNION ALL SELECT * FROM role_breakdown;';
    
    EXECUTE IMMEDIATE V_sql;
END;
