CREATE SCHEMA [config];
GO

CREATE TABLE [config].[tbl_Entities](
    [IDEntity] INT IDENTITY(1,1) NOT NULL,
    [EntityName] VARCHAR(256) NOT NULL,
    [Environment] VARCHAR(64),
    [ADLSaccount] VARCHAR(256),
    [ContainerName] VARCHAR(256),
    [SourcePath] VARCHAR(256),
    [FileExtension] VARCHAR(64),
    [TargetPath] VARCHAR(256),
    [Notebook] VARCHAR(256) NOT NULL,
    [IsActive] TINYINT NOT NULL DEFAULT(1),
    [InsertedDate] DATETIME2 NOT NULL,
    [ModifiedDate] DATETIME2 NOT NULL,
    CONSTRAINT PK_tbl_Entities PRIMARY KEY NONCLUSTERED ([IDEntity]) NOT ENFORCED
)
WITH
(
    DISTRIBUTION = HASH ([Notebook]),
    CLUSTERED COLUMNSTORE INDEX
);

GO

CREATE TABLE [config].[tbl_GoldSP]
(
    [ID_GoldSP] int NOT NULL,
    [ProcessName] VARCHAR(256) NOT NULL,
    [SPName] VARCHAR(256) NOT NULL,
    [IsActive] TINYINT NOT NULL DEFAULT(1),
    [InsertedDate] DATETIME2 NOT NULL,
    [ModifiedDate] DATETIME2 NOT NULL,
    CONSTRAINT PK_tbl_GoldSP PRIMARY KEY NONCLUSTERED ([ID_GoldSP]) NOT ENFORCED
)
WITH
(
    DISTRIBUTION = HASH (ID_GoldSP),
    CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE TABLE [config].[tbl_Log]
(
    [ID_Log] INT NOT NULL,
    [PipelineName] VARCHAR(256) NOT NULL,
    [PipelineRunID] VARCHAR(256) NOT NULL,
    [PipelineWorkspace] VARCHAR(256) NOT NULL,
    [StartTime] DATETIME2 NOT NULL,
    [EndTime] DATETIME2,
    [DurationSeconds] INT,
    [Status] VARCHAR(256) NOT NULL,
    CONSTRAINT PK_tbl_Log PRIMARY KEY NONCLUSTERED ([ID_Log]) NOT ENFORCED
)
WITH
(
    DISTRIBUTION = HASH (PipelineName),
    CLUSTERED COLUMNSTORE INDEX
)
GO