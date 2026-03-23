IF OBJECT_ID('gold.sp_materialize_person_contactability', 'P') IS NOT NULL
    DROP PROCEDURE gold.sp_materialize_person_contactability;
GO
CREATE PROCEDURE gold.sp_materialize_person_contactability
AS
BEGIN
  SET NOCOUNT ON;

  IF OBJECT_ID('gold.person_contactability') IS NOT NULL DROP TABLE gold.person_contactability;

  DECLARE @sql NVARCHAR(MAX) = N'
  CREATE TABLE gold.person_contactability
    WITH
    (
        DISTRIBUTION = HASH(business_entity_id),
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    SELECT
    p.business_entity_id,
    p.preferred_email, p.email_domain, p.email_domain_category,
    p.preferred_phone_e164, p.preferred_phone_type,
    a.has_password, a.password_hash_length, a.password_salt_length,
    CASE WHEN p.preferred_email IS NOT NULL THEN 1 ELSE 0 END AS has_email,
    CASE WHEN p.preferred_phone_e164 IS NOT NULL THEN 1 ELSE 0 END AS has_phone
    FROM gold.person_360 p
    LEFT JOIN silver.person_auth_metadata a
    ON a.business_entity_id = p.business_entity_id;';
  EXEC sp_executesql @sql;
END
GO