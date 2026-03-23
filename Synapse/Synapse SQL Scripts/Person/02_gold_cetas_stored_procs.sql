-- =========================
-- 03_gold_cetas_stored_procs.sql
-- =========================

IF OBJECT_ID('gold.sp_materialize_person_360', 'P') IS NOT NULL
    DROP PROCEDURE gold.sp_materialize_person_360;
GO

CREATE PROCEDURE gold.sp_materialize_person_360
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('gold.person_360') IS NOT NULL 
        DROP TABLE gold.person_360;

    DECLARE @sql NVARCHAR(MAX) = N'
    CREATE TABLE gold.person_360
    WITH
    (
        DISTRIBUTION = HASH(business_entity_id),
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    SELECT
        p.business_entity_id,
        p.PersonType, p.NameStyle, p.Title, p.FirstName, p.MiddleName, p.LastName, p.Suffix, p.EmailPromotion,
        e.preferred_email, e.email_domain, e.email_domain_category,
        ph.preferred_phone_e164, ph.preferred_phone_type,
        ca.canonical_address_sk, da.AddressLine1, da.AddressLine2, da.City,
        da.StateProvinceCode, da.PostalCode, da.CountryRegionCode, da.CountryName,

        LTRIM(RTRIM(CONCAT(
            COALESCE(p.Title + '' '', ''''),
            p.FirstName,
            COALESCE('' '' + p.MiddleName, ''''),
            '' '',
            p.LastName,
            COALESCE('' '' + p.Suffix, '''')
        ))) AS full_name,

        CASE WHEN e.preferred_email IS NOT NULL THEN 1 ELSE 0 END AS has_email,
        CASE WHEN ph.preferred_phone_e164 IS NOT NULL THEN 1 ELSE 0 END AS has_phone

    FROM silver.dim_person_scd2 p
    LEFT JOIN (
        SELECT BusinessEntityID, email AS preferred_email, domain AS email_domain, domain_category AS email_domain_category
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY BusinessEntityID ORDER BY CAST(is_primary AS INT) DESC, ModifiedDate DESC, email_id ASC) AS rn
            FROM silver.person_email
        ) x WHERE rn = 1
    ) e ON e.BusinessEntityID = p.business_entity_id
    LEFT JOIN (
        SELECT BusinessEntityID,
               phone_e164 AS preferred_phone_e164,
               phone_type_name AS preferred_phone_type
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY BusinessEntityID
                       ORDER BY CASE WHEN LOWER(COALESCE(phone_type_name, '''')) LIKE ''%mobile%'' THEN 1 ELSE 2 END,
                                ModifiedDate DESC
                   ) AS rn
            FROM silver.person_phone
        ) y WHERE rn = 1
    ) ph ON ph.BusinessEntityID = p.business_entity_id
    LEFT JOIN silver.person_canonical_address ca ON ca.BusinessEntityID = p.business_entity_id
    LEFT JOIN silver.dim_address da ON da.address_sk = ca.canonical_address_sk
    WHERE p.is_current = 1;
    ';

    EXEC sys.sp_executesql @sql;
END
GO

