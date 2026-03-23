IF OBJECT_ID('gold.sp_materialize_contact_rollup', 'P') IS NOT NULL
    DROP PROCEDURE gold.sp_materialize_contact_rollup;
GO

CREATE PROCEDURE gold.sp_materialize_contact_rollup
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('gold.person_contact_rollup') IS NOT NULL
        DROP TABLE gold.person_contact_rollup;

    DECLARE @sql nvarchar(MAX) = N'
    CREATE TABLE gold.person_contact_rollup
    WITH
    (
        DISTRIBUTION = HASH(business_entity_id),
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    SELECT
        cr.business_entity_id,
        cr.contact_person_id,
        cr.contact_type_name,
        cp.FirstName AS contact_first_name,
        cp.LastName  AS contact_last_name,
        LTRIM(RTRIM(CONCAT(cp.FirstName, '' '', cp.LastName))) AS contact_display_name
    FROM silver.person_contactrole AS cr
    LEFT JOIN silver.dim_person_scd2 AS cp
      ON cp.business_entity_id = cr.contact_person_id
     AND cp.is_current = 1;';

    EXEC sys.sp_executesql @sql;
END
GO