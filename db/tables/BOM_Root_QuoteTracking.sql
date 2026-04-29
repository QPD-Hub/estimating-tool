SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF COL_LENGTH('dbo.BOM_Root', 'JobBossQuoteNumber') IS NULL
BEGIN
    ALTER TABLE dbo.BOM_Root
    ADD JobBossQuoteNumber NVARCHAR(50) NULL;
END
GO

IF COL_LENGTH('dbo.BOM_Root', 'EstimatingStatus') IS NULL
BEGIN
    ALTER TABLE dbo.BOM_Root
    ADD EstimatingStatus NVARCHAR(40) NOT NULL
        CONSTRAINT DF_BOM_Root_EstimatingStatus DEFAULT ('RAW');
END
GO

IF COL_LENGTH('dbo.BOM_Root', 'QuoteCreatedAt') IS NULL
BEGIN
    ALTER TABLE dbo.BOM_Root
    ADD QuoteCreatedAt DATETIME2(0) NULL;
END
GO

IF COL_LENGTH('dbo.BOM_Root', 'QuoteCreatedBy') IS NULL
BEGIN
    ALTER TABLE dbo.BOM_Root
    ADD QuoteCreatedBy NVARCHAR(100) NULL;
END
GO

