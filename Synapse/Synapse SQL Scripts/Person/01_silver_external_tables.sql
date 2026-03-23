-- =========================
-- 01_silver_external_tables.sql
-- =========================
-- Drop existing (safe re-run)
IF OBJECT_ID('silver.dim_person_scd2') IS NOT NULL DROP EXTERNAL TABLE silver.dim_person_scd2;
IF OBJECT_ID('silver.person_contactrole') IS NOT NULL DROP EXTERNAL TABLE silver.person_contactrole;
IF OBJECT_ID('silver.person_auth_metadata') IS NOT NULL DROP EXTERNAL TABLE silver.person_auth_metadata;
IF OBJECT_ID('silver.person_email') IS NOT NULL DROP EXTERNAL TABLE silver.person_email;
IF OBJECT_ID('silver.person_phone') IS NOT NULL DROP EXTERNAL TABLE silver.person_phone;
IF OBJECT_ID('silver.person_address') IS NOT NULL DROP EXTERNAL TABLE silver.person_address;
IF OBJECT_ID('silver.dim_address') IS NOT NULL DROP EXTERNAL TABLE silver.dim_address;
IF OBJECT_ID('silver.person_canonical_address') IS NOT NULL DROP EXTERNAL TABLE silver.person_canonical_address;
GO

CREATE EXTERNAL TABLE silver.dim_person_scd2 (
	[person_sk] nvarchar(4000),
	[business_entity_id] int,
	[PersonType] nvarchar(4000),
	[NameStyle] bit,
	[Title] nvarchar(4000),
	[FirstName] nvarchar(4000),
	[MiddleName] nvarchar(4000),
	[LastName] nvarchar(4000),
	[Suffix] nvarchar(4000),
	[AdditionalContactInfo] nvarchar(4000),
	[EmailPromotion] int,
	[rowguid] nvarchar(4000),
	[ModifiedDate] datetime2(7),
	[t2_hashdiff] nvarchar(4000),
	[version_no] int,
	[effective_start_date] datetime2(7),
	[effective_end_date] datetime2(7),
	[is_current] bit
	)
	WITH (
	LOCATION = 'silver/adventureworks/dim_person_scd2',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.person_address (
	[person_address_sk] nvarchar(4000),
	[BusinessEntityID] int,
	[address_sk] nvarchar(4000),
	[address_role] nvarchar(4000),
	[ModifiedDate] datetime2(7),
	[ingestion_timestamp] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/person_address',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.person_contactrole (
	[business_entity_id] int,
	[contact_person_id] int,
	[contact_type_name] nvarchar(4000),
	[ModifiedDate] datetime2(7),
	[person_contactrole_sk] nvarchar(4000)
	)
	WITH (
	LOCATION = 'silver/adventureworks/person_contactrole',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.person_auth_metadata (
	[business_entity_id] int,
	[password_hash_length] int,
	[password_salt_length] int,
	[ModifiedDate] datetime2(7),
	[has_password] bit,
	[policy_violation] bit,
	[person_auth_metadata_sk] nvarchar(4000)
	)
	WITH (
	LOCATION = 'silver/adventureworks/person_auth_metadata',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.person_canonical_address (
	[BusinessEntityID] int,
	[canonical_address_sk] nvarchar(4000),
	[canonical_role] nvarchar(4000),
	[ModifiedDate] datetime2(7),
	[ingestion_timestamp] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/person_canonical_address',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.person_email (
	[person_email_sk] nvarchar(4000),
	[BusinessEntityID] int,
	[email_id] int,
	[email] nvarchar(4000),
	[domain] nvarchar(4000),
	[domain_category] nvarchar(4000),
	[is_primary] bit,
	[ModifiedDate] datetime2(7),
	[record_source] nvarchar(4000),
	[source_system] nvarchar(4000),
	[ingestion_timestamp] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_person_email',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.person_phone (
	[person_phone_sk] nvarchar(4000),
	[BusinessEntityID] int,
	[PhoneNumberTypeID] int,
	[phone_type_name] nvarchar(4000),
	[PhoneNumber] nvarchar(4000),
	[phone_e164] nvarchar(4000),
	[ModifiedDate] datetime2(7),
	[record_source] nvarchar(4000),
	[source_system] nvarchar(4000),
	[ingestion_timestamp] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_person_phone',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.dim_address (
	[address_sk] nvarchar(4000),
	[address_nk] nvarchar(4000),
	[AddressID] bigint,
	[AddressLine1] nvarchar(4000),
	[AddressLine2] nvarchar(4000),
	[City] nvarchar(4000),
	[StateProvinceCode] nvarchar(4000),
	[StateProvinceName] nvarchar(4000),
	[PostalCode] nvarchar(4000),
	[CountryRegionCode] nvarchar(4000),
	[CountryName] nvarchar(4000),
	[ModifiedDate] bigint,
	[ingestion_timestamp] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/dim_address',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO