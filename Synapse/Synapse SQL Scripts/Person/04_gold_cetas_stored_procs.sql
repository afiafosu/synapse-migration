IF OBJECT_ID('gold.sp_materialize_location_dim', 'P') IS NOT NULL
    DROP PROCEDURE gold.sp_materialize_location_dim;
GO
CREATE PROCEDURE gold.sp_materialize_location_dim
AS
BEGIN
  SET NOCOUNT ON;

  IF OBJECT_ID('gold.location_dim') IS NOT NULL DROP TABLE gold.location_dim;

  DECLARE @sql NVARCHAR(MAX) = N'
  CREATE TABLE gold.location_dim
    WITH
    (
        DISTRIBUTION = REPLICATE,
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    SELECT
    address_sk,
    AddressLine1 AS address_line1,
    AddressLine2 AS address_line2,
    City,
    StateProvinceCode AS state_province,
    PostalCode,
    CountryRegionCode AS country_code,
    CountryName AS country_name
    FROM silver.dim_address;
    ';
  EXEC sp_executesql @sql;
END
GO