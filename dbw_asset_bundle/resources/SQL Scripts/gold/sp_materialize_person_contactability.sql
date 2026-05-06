CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_materialize_person_contactability`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE VARIABLE V_sql STRING ;
    
    DROP TABLE IF EXISTS main.gold.person_contactability;
    
    SET V_sql = '
    CREATE OR REPLACE TABLE main.gold.person_contactability AS
    SELECT
        p.business_entity_id,
        p.preferred_email, p.email_domain, p.email_domain_category,
        p.preferred_phone_e164, p.preferred_phone_type,
        a.has_password, a.password_hash_length, a.password_salt_length,
        CASE WHEN p.preferred_email IS NOT NULL THEN 1 ELSE 0 END AS has_email,
        CASE WHEN p.preferred_phone_e164 IS NOT NULL THEN 1 ELSE 0 END AS has_phone
    FROM main.gold.person_360 p
    LEFT JOIN main.silver.person_auth_metadata a
        ON a.business_entity_id = p.business_entity_id;';
    
    EXECUTE IMMEDIATE V_sql;
END;
