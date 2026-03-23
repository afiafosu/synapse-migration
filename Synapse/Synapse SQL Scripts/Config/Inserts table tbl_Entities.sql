DECLARE @NowDate DATETIME2;
SET @NowDate = GETDATE();

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath],[Notebook],[InsertedDate],[ModifiedDate]) VALUES
(1,  'Sales.CountryRegionCurrency',	      'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(2,  'Sales.CreditCard',			      'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks',  'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(3,  'Sales.Currency',				      'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(4,  'Sales.CurrencyRate',			      'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(5,  'Sales.Customer',				      'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(6,  'Sales.PersonCreditCard',            'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(7,  'Sales.SalesOrderDetail',		      'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(8,  'Sales.SalesOrderHeader',		      'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(9,  'Sales.SalesOrderHeaderSalesReason', 'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(10, 'Sales.SalesPerson',	    	      'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(11, 'Sales.SalesPersonQuotaHistory',     'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(12, 'Sales.SalesReason',                 'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(13, 'Sales.SalesTaxRate',                'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(14, 'Sales.SalesTerritory',              'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(15, 'Sales.SalesTerritoryHistory',       'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(16, 'Sales.ShoppingCartItem',            'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(17, 'Sales.SpecialOffer',                'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(18, 'Sales.SpecialOfferProduct',         'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath], [Notebook], [InsertedDate], [ModifiedDate]) VALUES
(19, 'Sales.Store',                       'silver', 'fabmigration', 'fabmigration', 'bronze/adventureworks2', 'parquet', 'silver/adventureworks', '01_SilverCleanEntity',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath],[Notebook],[InsertedDate],[ModifiedDate]) VALUES
(20, 'HumanResources', 					  'silver', NULL,			NULL,			NULL,					  NULL,		 NULL,					  'nb_hr_bronze_to_silver',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath],[Notebook],[InsertedDate],[ModifiedDate]) VALUES
(21, 'Purchasing', 						  'silver', NULL,			NULL,			NULL,					  NULL,		 NULL, 					  'nb_purchasing_bronze_to_silver', @NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath],[Notebook],[InsertedDate],[ModifiedDate]) VALUES
(22, 'Production1',						  'silver', NULL,			NULL,			NULL,					  NULL,		 NULL,					  'nb_ProdTables', 					@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath],[Notebook],[InsertedDate],[ModifiedDate]) VALUES
(23, 'Production2', 					  'silver', NULL,			NULL,			NULL,					  NULL,		 NULL,					  'nb_Production_med', 				@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath],[Notebook],[InsertedDate],[ModifiedDate]) VALUES
(24, 'PersonAddress',					  'silver', NULL,			NULL,			NULL,					  NULL,		 NULL,					  'silver_person_address',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath],[Notebook],[InsertedDate],[ModifiedDate]) VALUES
(25, 'PersonContactRoles',				  'silver', NULL,			NULL,			NULL,					  NULL,		 NULL,					  'silver_person_contact_roles',	@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath],[Notebook],[InsertedDate],[ModifiedDate]) VALUES
(26, 'PersonEmail',						  'silver', NULL,			NULL,			NULL,					  NULL,		 NULL,					  'silver_person_email',			@NowDate, @NowDate);

INSERT INTO [config].[tbl_Entities] ([IDEntity],[EntityName],[Environment],[ADLSaccount],[ContainerName],[SourcePath],[FileExtension],[TargetPath],[Notebook],[InsertedDate],[ModifiedDate]) VALUES
(27, 'PersonPhone',						  'silver', NULL,			NULL,			NULL,					  NULL,		 NULL,					  'silver_person_phone',			@NowDate, @NowDate);
