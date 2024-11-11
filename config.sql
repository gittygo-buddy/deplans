

/****** Object:  Table [loader].[Config]    Script Date: 11/11/2024 4:28:40 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [loader].[Config](
	[ConfigId] [int] IDENTITY(1,1) NOT NULL,
	[SourceSystemId] [int] NULL,
	[IntegrationType] [varchar](255) NULL,
	[Incremental] [bit] NULL,
	[WatermarkField] [varchar](255) NULL,
	[WatermarkType] [varchar](255) NULL,
	[WatermarkValue] [varchar](max) NULL,
	[QueryOverride] [nvarchar](max) NULL,
	[ServerName] [nvarchar](256) NULL,
	[DatabaseName] [nvarchar](256) NULL,
	[TableSchema] [nvarchar](256) NULL,
	[TableName] [nvarchar](256) NULL,
	[PrimaryKey] [varchar](max) NULL,
	[ContainerName] [varchar](255) NOT NULL,
	[FolderName] [varchar](255) NOT NULL,
	[FileNameOverride] [varchar](255) NULL,
	[LoadDw] [bit] NOT NULL,
	[StoredProcedure] [varchar](255) NULL,
	[TimeZone] [varchar](255) NULL,
	[ConfigGroup] [varchar](255) NULL,
	[ConfigActive] [bit] NOT NULL,
	[ConfigOptions] [nvarchar](max) NULL,
	[AuditActive] [bit] NOT NULL,
	[CreatedDateTime] [datetime] NOT NULL,
	[LastExtractedDateTime] [datetime] NULL,
	[StartDate] [date] NOT NULL,
	[EndDate] [date] NOT NULL,
	[SchemaShift] [varchar](255) NOT NULL,
	[BronzeOperationMode] [nchar](10) NULL,
	[SilverOperationMode] [nchar](10) NULL,
	[GoldOperationMode] [nchar](10) NULL,
 CONSTRAINT [PK__Config__ConfigId] PRIMARY KEY CLUSTERED 
(
	[ConfigId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY],
 CONSTRAINT [UC_Config__SourceSystemId_DatabaseName_SchemaName_TableName] UNIQUE NONCLUSTERED 
(
	[SourceSystemId] ASC,
	[DatabaseName] ASC,
	[TableSchema] ASC,
	[TableName] ASC,
	[FileNameOverride] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [loader].[Config] ADD  CONSTRAINT [DF_Config_LoadDw]  DEFAULT ((0)) FOR [LoadDw]
GO

ALTER TABLE [loader].[Config] ADD  CONSTRAINT [DF_Config_AuditActive]  DEFAULT ((1)) FOR [AuditActive]
GO

ALTER TABLE [loader].[Config] ADD  CONSTRAINT [DF_ConfigStartDate]  DEFAULT ('1900-01-01') FOR [StartDate]
GO

ALTER TABLE [loader].[Config] ADD  CONSTRAINT [DF_ConfigEndDate]  DEFAULT ('2500-01-01') FOR [EndDate]
GO

ALTER TABLE [loader].[Config] ADD  CONSTRAINT [DF_ConfigSchemaShift]  DEFAULT ('Off') FOR [SchemaShift]
GO


