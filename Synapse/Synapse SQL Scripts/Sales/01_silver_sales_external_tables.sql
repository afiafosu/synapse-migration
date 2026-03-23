IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
	WITH ( FORMAT_TYPE = PARQUET)
GO



CREATE EXTERNAL TABLE silver.sales_countryregioncurrency (
	[CountryRegionCode] nvarchar(4000),
	[CurrencyCode] nvarchar(4000),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_countryregioncurrency',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_creditcard (
	[CreditCardID] bigint,
	[CardType] nvarchar(4000),
	[CardNumber] nvarchar(4000),
	[ExpMonth] bigint,
	[ExpYear] bigint,
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_creditcard',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO
CREATE EXTERNAL TABLE silver.sales_currency (
	[CurrencyCode] nvarchar(4000),
	[Name] nvarchar(4000),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_currency',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO
CREATE EXTERNAL TABLE silver.sales_currencyrate (
	[CurrencyRateID] int,
	[CurrencyRateDate] datetime2(7),
	[FromCurrencyCode] nvarchar(4000),
	[ToCurrencyCode] nvarchar(4000),
	[AverageRate] float,
	[EndOfDayRate] float,
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_currencyrate',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO
CREATE EXTERNAL TABLE silver.sales_customer (
	[CustomerID] int,
	[PersonID] nvarchar(4000),
	[StoreID] int,
	[TerritoryID] int,
	[AccountNumber] nvarchar(4000),
	[rowguid] nvarchar(4000),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_customer',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_personcreditcard (
	[BusinessEntityID] int,
	[CreditCardID] int,
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_personcreditcard',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_salesorderdetail (
	[SalesOrderID] int,
	[SalesOrderDetailID] int,
	[CarrierTrackingNumber] nvarchar(4000),
	[OrderQty] int,
	[ProductID] int,
	[SpecialOfferID] int,
	[UnitPrice] float,
	[UnitPriceDiscount] float,
	[LineTotal] float,
	[rowguid] nvarchar(4000),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_salesorderdetail',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_salesorderheader (
	[SalesOrderID] int,
	[RevisionNumber] int,
	[OrderDate] datetime2(7),
	[DueDate] datetime2(7),
	[ShipDate] datetime2(7),
	[Status] int,
	[OnlineOrderFlag] bit,
	[SalesOrderNumber] nvarchar(4000),
	[PurchaseOrderNumber] nvarchar(4000),
	[AccountNumber] nvarchar(4000),
	[CustomerID] int,
	[SalesPersonID] int,
	[TerritoryID] int,
	[BillToAddressID] int,
	[ShipToAddressID] int,
	[ShipMethodID] int,
	[CreditCardID] int,
	[CreditCardApprovalCode] nvarchar(4000),
	[CurrencyRateID] int,
	[SubTotal] float,
	[TaxAmt] float,
	[Freight] float,
	[TotalDue] float,
	[Comment] nvarchar(4000),
	[rowguid] nvarchar(4000),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_salesorderheader',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_salesorderheadersalesreason (
	[SalesOrderID] int,
	[SalesReasonID] int,
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_salesorderheadersalesreason',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_salesperson (
	[BusinessEntityID] int,
	[TerritoryID] int,
	[SalesQuota] float,
	[Bonus] float,
	[CommissionPct] float,
	[SalesYTD] float,
	[SalesLastYear] float,
	[rowguid] nvarchar(4000),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_salesperson',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_salespersonquotahistory (
	[BusinessEntityID] int,
	[QuotaDate] datetime2(7),
	[SalesQuota] float,
	[rowguid] nvarchar(4000),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_salespersonquotahistory',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_salesreason (
	[SalesReasonID] int,
	[Name] nvarchar(4000),
	[ReasonType] nvarchar(4000),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_salesreason',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_salestaxrate (
	[SalesTaxRateID] int,
	[StateProvinceID] int,
	[TaxType] int,
	[TaxRate] float,
	[Name] nvarchar(4000),
	[rowguid] nvarchar(4000),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_salestaxrate',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_salesterritory (
	[TerritoryID] int,
	[Name] nvarchar(4000),
	[CountryRegionCode] nvarchar(4000),
	[Group] nvarchar(4000),
	[SalesYTD] float,
	[SalesLastYear] float,
	[CostYTD] float,
	[CostLastYear] float,
	[rowguid] nvarchar(4000),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_salesterritory',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_salesterritoryhistory (
	[BusinessEntityID] int,
	[TerritoryID] int,
	[StartDate] datetime2(7),
	[EndDate] datetime2(7),
	[rowguid] nvarchar(4000),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_salesterritoryhistory',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_shoppingcartitem (
	[ShoppingCartItemID] int,
	[ShoppingCartID] nvarchar(4000),
	[Quantity] int,
	[ProductID] int,
	[DateCreated] datetime2(7),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_shoppingcartitem',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_specialoffer (
	[SpecialOfferID] int,
	[Description] nvarchar(4000),
	[DiscountPct] float,
	[Type] nvarchar(4000),
	[Category] nvarchar(4000),
	[StartDate] datetime2(7),
	[EndDate] datetime2(7),
	[MinQty] int,
	[MaxQty] int,
	[rowguid] nvarchar(4000),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_specialoffer',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_specialofferproduct (
	[SpecialOfferID] int,
	[ProductID] int,
	[rowguid] nvarchar(4000),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_specialofferproduct',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.sales_store (
	[BusinessEntityID] int,
	[Name] nvarchar(4000),
	[SalesPersonID] int,
	[Demographics] nvarchar(4000),
	[rowguid] nvarchar(4000),
	[ModifiedDate] datetime2(7)
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_sales_store',
	DATA_SOURCE = [fabmigration_fabmigration_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO
