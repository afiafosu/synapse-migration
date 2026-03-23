SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'fabmigation1_synpasetofabric_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [fabmigation1_synpasetofabric_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://fabmigation1@synpasetofabric.dfs.core.windows.net' 
	)
GO

-- Drop existing (safe re-run)
IF OBJECT_ID('silver.production_document') IS NOT NULL DROP EXTERNAL TABLE silver.production_document;
IF OBJECT_ID('silver.production_productphoto') IS NOT NULL DROP EXTERNAL TABLE silver.production_productphoto;
IF OBJECT_ID('silver.production_productdocument') IS NOT NULL DROP EXTERNAL TABLE silver.production_productdocument;
IF OBJECT_ID('silver.production_productmodelproductdescriptionculture') IS NOT NULL DROP EXTERNAL TABLE silver.production_productmodelproductdescriptionculture;
IF OBJECT_ID('silver.production_product') IS NOT NULL DROP EXTERNAL TABLE silver.production_product;
IF OBJECT_ID('silver.production_productcategory') IS NOT NULL DROP EXTERNAL TABLE silver.production_productcategory;
IF OBJECT_ID('silver.production_billofmaterials') IS NOT NULL DROP EXTERNAL TABLE silver.production_billofmaterials;
IF OBJECT_ID('silver.production_culture') IS NOT NULL DROP EXTERNAL TABLE silver.production_culture;
IF OBJECT_ID('silver.production_location') IS NOT NULL DROP EXTERNAL TABLE silver.production_location;
IF OBJECT_ID('silver.production_productcosthistory') IS NOT NULL DROP EXTERNAL TABLE silver.production_productcosthistory;
IF OBJECT_ID('silver.production_ProductDescription') IS NOT NULL DROP EXTERNAL TABLE silver.production_ProductDescription;
IF OBJECT_ID('silver.production_productinventory') IS NOT NULL DROP EXTERNAL TABLE  silver.production_productinventory;
IF OBJECT_ID('silver.production_productlistpricehistory') IS NOT NULL DROP EXTERNAL TABLE silver.production_productlistpricehistory;

IF OBJECT_ID('silver.production_productmodelillustration') IS NOT NULL DROP EXTERNAL TABLE silver.production_productmodelillustration;
IF OBJECT_ID('silver.production_productproductphoto') IS NOT NULL DROP EXTERNAL TABLE silver.production_productproductphoto;
IF OBJECT_ID('silver.production_productreview') IS NOT NULL DROP EXTERNAL TABLE silver.production_productreview;
IF OBJECT_ID('silver.production_productsubcategory') IS NOT NULL DROP EXTERNAL TABLE silver.production_productsubcategory;
IF OBJECT_ID('silver.production_scrapreason') IS NOT NULL DROP EXTERNAL TABLE silver.production_scrapreason;
IF OBJECT_ID('silver.production_transactionhistory') IS NOT NULL DROP EXTERNAL TABLE silver.production_transactionhistory;
IF OBJECT_ID('silver.production_transactionhistoryarchive') IS NOT NULL DROP EXTERNAL TABLE silver.production_transactionhistoryarchive;
IF OBJECT_ID('silver.production_unitmeasure') IS NOT NULL DROP EXTERNAL TABLE silver.production_unitmeasure;
IF OBJECT_ID('silver.production_workorder') IS NOT NULL DROP EXTERNAL TABLE silver.production_workorder;
IF OBJECT_ID('silver.production_workorderrouting') IS NOT NULL DROP EXTERNAL TABLE silver.production_workorderrouting;

CREATE EXTERNAL TABLE silver.production_document (
	[DocumentNode] nvarchar(4000),
	[Title] nvarchar(4000),
	[Owner] int,
	[FolderFlag] int,
	[FileName] nvarchar(4000),
	[FileExtension] nvarchar(4000),
	[Revision] nvarchar(4000),
	[ChangeNumber] int,
	[Status] int,
	[DocumentSummary] date,
	[Document] varbinary(8000),
	[rowguid] nvarchar(4000),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_document',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_productphoto (
	[ProductPhotoID] int,
	[ThumbNailPhoto] varbinary(8000),
	[ThumbnailPhotoFileName] nvarchar(4000),
	[LargePhoto] varbinary(8000),
	[LargePhotoFileName] nvarchar(4000),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_productphoto',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_productdocument (
	[ProductID] int,
	[DocumentNode] nvarchar(4000),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_productdocument',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_productmodelproductdescriptionculture (
	[ProductModelID] int,
	[ProductDescriptionID] int,
	[CultureID] nvarchar(4000),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_productmodelproductdescriptionculture',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_product (
	[ProductID] int,
	[Name] nvarchar(4000),
	[ProductNumber] nvarchar(4000),
	[MakeFlag] bit,
	[FinishedGoodsFlag] bit,
	[Color] nvarchar(4000),
	[SafetyStockLevel] int,
	[ReorderPoint] int,
	[StandardCost] numeric(19,4),
	[ListPrice] numeric(19,4),
	[Size] nvarchar(4000),
	[SizeUnitMeasureCode] nvarchar(4000),
	[WeightUnitMeasureCode] nvarchar(4000),
	[Weight] numeric(8,2),
	[DaysToManufacture] int,
	[ProductLine] nvarchar(4000),
	[Class] nvarchar(4000),
	[Style] nvarchar(4000),
	[ProductSubcategoryID] int,
	[ProductModelID] int,
	[SellStartDate] date,
	[SellEndDate] date,
	[DiscontinuedDate] date,
	[rowguid] nvarchar(4000),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_Production_Product',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE [silver].[production_productcategory]
( 
	[ProductCategoryID] [int]  NULL,
	[Name] [nvarchar](4000)  NULL,
	[rowguid] [nvarchar](4000)  NULL,
	[ModifiedDate] [date]  NULL,
	[ingest_date] [date]  NULL
)
WITH (DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net], LOCATION = N'silver/adventureworks/silver_production_productcategory', FILE_FORMAT = [SynapseParquetFormat], REJECT_TYPE = VALUE, REJECT_VALUE = 0 )
GO

CREATE EXTERNAL TABLE silver.production_billofmaterials (
	[BillOfMaterialsID] int,
	[ProductAssemblyID] int,
	[ComponentID] int,
	[StartDate] date,
	[EndDate] date,
	[UnitMeasureCode] nvarchar(4000),
	[BOMLevel] int,
	[PerAssemblyQty] numeric(8,2),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_billofmaterials',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_culture (
	[CultureID] nvarchar(4000),
	[Name] nvarchar(4000),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_culture',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_location (
	[LocationID] int,
	[Name] nvarchar(4000),
	[CostRate] numeric(10,4),
	[Availability] numeric(8,2),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_location/**',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_productcosthistory (
	[ProductID] int,
	[StartDate] date,
	[EndDate] date,
	[StandardCost] numeric(19,4),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_productcosthistory',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_ProductDescription (
	[ProductDescriptionID] int,
	[Description] nvarchar(4000),
	[rowguid] nvarchar(4000),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_productdescription',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_productinventory (
	[ProductID] int,
	[LocationID] int,
	[Shelf] nvarchar(4000),
	[Bin] int,
	[Quantity] int,
	[rowguid] nvarchar(4000),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_productinventory',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_productlistpricehistory (
	[ProductID] int,
	[StartDate] date,
	[EndDate] date,
	[ListPrice] numeric(19,4),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_productlistpricehistory',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_productmodelillustration (
	[ProductModelID] int,
	[IllustrationID] int,
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_productmodelillustration',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_productproductphoto (
	[ProductID] int,
	[ProductPhotoID] int,
	[Primary] bit,
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_productproductphoto',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO
CREATE EXTERNAL TABLE silver.production_productreview (
	[ProductReviewID] int,
	[ProductID] int,
	[ReviewerName] nvarchar(4000),
	[ReviewDate] date,
	[EmailAddress] nvarchar(4000),
	[Rating] int,
	[Comments] date,
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_productreview',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_productsubcategory (
	[ProductSubcategoryID] int,
	[ProductCategoryID] int,
	[Name] nvarchar(4000),
	[rowguid] nvarchar(4000),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_productsubcategory',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_scrapreason (
	[ScrapReasonID] int,
	[Name] nvarchar(4000),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_scrapreason',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_transactionhistory (
	[TransactionID] int,
	[ProductID] int,
	[ReferenceOrderID] int,
	[ReferenceOrderLineID] int,
	[TransactionDate] date,
	[TransactionType] nvarchar(4000),
	[Quantity] int,
	[ActualCost] numeric(19,4),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_transactionhistory',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_transactionhistoryarchive (
	[TransactionID] int,
	[ProductID] int,
	[ReferenceOrderID] int,
	[ReferenceOrderLineID] int,
	[TransactionDate] date,
	[TransactionType] nvarchar(4000),
	[Quantity] int,
	[ActualCost] numeric(19,4),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_transactionhistoryarchive',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_unitmeasure (
	[UnitMeasureCode] nvarchar(4000),
	[Name] nvarchar(4000),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_unitmeasure',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_workorder (
	[WorkOrderID] int,
	[ProductID] int,
	[OrderQty] int,
	[StockedQty] int,
	[ScrappedQty] int,
	[StartDate] date,
	[EndDate] date,
	[DueDate] date,
	[ScrapReasonID] int,
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_workorder',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE silver.production_workorderrouting (
	[WorkOrderID] int,
	[ProductID] int,
	[OperationSequence] int,
	[LocationID] int,
	[ScheduledStartDate] date,
	[ScheduledEndDate] date,
	[ActualStartDate] date,
	[ActualEndDate] date,
	[ActualResourceHrs] numeric(9,4),
	[PlannedCost] numeric(19,4),
	[ActualCost] numeric(19,4),
	[ModifiedDate] date,
	[ingest_date] date
	)
	WITH (
	LOCATION = 'silver/adventureworks/silver_production_workorderrouting',
	DATA_SOURCE = [fabmigation1_synpasetofabric_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO
