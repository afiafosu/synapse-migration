CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_materialize_location_dim`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    DECLARE VARIABLE V_sql STRING ;
    
    DROP TABLE IF EXISTS main.gold.location_dim;
    
    SET V_sql = '
    CREATE OR REPLACE TABLE main.gold.location_dim AS
    SELECT
        address_sk,
        AddressLine1 AS address_line1,
        AddressLine2 AS address_line2,
        City,
        StateProvinceCode AS state_province,
        PostalCode,
        CountryRegionCode AS country_code,
        CountryName AS country_name
    FROM main.silver.dim_address;';
    
    EXECUTE IMMEDIATE V_sql;
END;
