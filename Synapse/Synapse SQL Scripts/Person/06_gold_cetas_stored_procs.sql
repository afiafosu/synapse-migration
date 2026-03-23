IF OBJECT_ID('gold.sp_materialize_person_kpis', 'P') IS NOT NULL
    DROP PROCEDURE gold.sp_materialize_person_kpis;
GO

CREATE PROCEDURE gold.sp_materialize_person_kpis
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('gold.person_kpis') IS NOT NULL
        DROP TABLE gold.person_kpis;

    DECLARE @sql NVARCHAR(MAX) = N'
    CREATE TABLE gold.person_kpis
    WITH
    (
        DISTRIBUTION = ROUND_ROBIN,
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    WITH base AS (
        SELECT * FROM gold.person_360
    ),
    email_breakdown AS (
        SELECT ''email_domain_category:'' + COALESCE(email_domain_category, ''UNKNOWN'') AS metric,
               COUNT(*) AS value
        FROM base
        GROUP BY email_domain_category
    ),
    phone_breakdown AS (
        SELECT ''phone_type:'' + COALESCE(preferred_phone_type, ''UNKNOWN'') AS metric,
               COUNT(*) AS value
        FROM base
        GROUP BY preferred_phone_type
    ),
    role_breakdown AS (
        SELECT ''contact_role:'' + COALESCE(contact_type_name, ''UNKNOWN'') AS metric,
               COUNT(*) AS value
        FROM gold.person_contact_rollup
        GROUP BY contact_type_name
    ),
    totals AS (
        SELECT ''total_persons'' AS metric,
               COUNT(DISTINCT business_entity_id) AS value
        FROM base
    )
    SELECT * FROM totals
    UNION ALL SELECT * FROM email_breakdown
    UNION ALL SELECT * FROM phone_breakdown
    UNION ALL SELECT * FROM role_breakdown;';

    EXEC sys.sp_executesql @sql;
END
GO