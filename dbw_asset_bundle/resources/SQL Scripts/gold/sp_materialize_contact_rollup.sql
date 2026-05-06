CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_materialize_contact_rollup`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE VARIABLE V_sql STRING ;
    
    DROP TABLE IF EXISTS main.gold.person_contact_rollup;
    
    SET V_sql = '
    CREATE OR REPLACE TABLE main.gold.person_contact_rollup AS
    SELECT
        cr.business_entity_id,
        cr.contact_person_id,
        cr.contact_type_name,
        cp.FirstName AS contact_first_name,
        cp.LastName  AS contact_last_name,
        LTRIM(RTRIM(CONCAT(cp.FirstName, " ", cp.LastName))) AS contact_display_name
    FROM main.silver.person_contactrole AS cr
    LEFT JOIN main.silver.dim_person_scd2 AS cp
        ON cp.business_entity_id = cr.contact_person_id
        AND cp.is_current = TRUE;';
    
    EXECUTE IMMEDIATE V_sql;
END;
