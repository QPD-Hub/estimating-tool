USE [HILLSBORO_Audit]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[BOM_Row](
    [BomRowId] [bigint] IDENTITY(1,1) NOT NULL,
    [RowGuid] [uniqueidentifier] NOT NULL,
    [BomIntakeId] [bigint] NOT NULL,
    [BomRootId] [bigint] NOT NULL,
    [SourceRowNumber] [int] NOT NULL,
    [SourceRowSequence] [int] NOT NULL,
     NULL,
     NULL,
     NULL,
     NULL,
    [BomLevel] [int] NULL,
     NULL,
     NULL,
    [Quantity] [decimal](18, 6) NULL,
     NULL,
     NULL,
     NULL,
     NULL,
     NULL,
    [LeadTimeDays] [decimal](18, 2) NULL,
    [Cost] [decimal](18, 4) NULL,
    [ParentBomRowId] [bigint] NULL,
     NULL,
    [IsLevel0] [bit] NOT NULL,
     NOT NULL,
     NULL,
     NOT NULL,
     NOT NULL,
 CONSTRAINT [PK_BOM_Row] PRIMARY KEY CLUSTERED
(
    [BomRowId] ASC
),
 CONSTRAINT [UQ_BOM_Row_Intake_SourceRowNumber] UNIQUE NONCLUSTERED
(
    [BomIntakeId] ASC,
    [SourceRowNumber] ASC
),
 CONSTRAINT [UQ_BOM_Row_Root_SourceRowSequence] UNIQUE NONCLUSTERED
(
    [BomRootId] ASC,
    [SourceRowSequence] ASC
),
 CONSTRAINT [UQ_BOM_Row_RowGuid] UNIQUE NONCLUSTERED
(
    [RowGuid] ASC
)
)
GO

ALTER TABLE [dbo].[BOM_Row] ADD CONSTRAINT [DF_BOM_Row_RowGuid] DEFAULT (newsequentialid()) FOR [RowGuid]
GO

ALTER TABLE [dbo].[BOM_Row] ADD CONSTRAINT [DF_BOM_Row_IsLevel0] DEFAULT ((0)) FOR [IsLevel0]
GO

ALTER TABLE [dbo].[BOM_Row] ADD CONSTRAINT [DF_BOM_Row_RowStatus] DEFAULT ('raw') FOR [RowStatus]
GO

ALTER TABLE [dbo].[BOM_Row] ADD CONSTRAINT [DF_BOM_Row_CreatedAt] DEFAULT (sysdatetime()) FOR [CreatedAt]
GO

ALTER TABLE [dbo].[BOM_Row] ADD CONSTRAINT [DF_BOM_Row_ModifiedAt] DEFAULT (sysdatetime()) FOR [ModifiedAt]
GO

ALTER TABLE [dbo].[BOM_Row] WITH CHECK ADD CONSTRAINT [FK_BOM_Row_BOM_Intake]
FOREIGN KEY([BomIntakeId]) REFERENCES [dbo].[BOM_Intake] ([BomIntakeId])
GO

ALTER TABLE [dbo].[BOM_Row] CHECK CONSTRAINT [FK_BOM_Row_BOM_Intake]
GO

ALTER TABLE [dbo].[BOM_Row] WITH CHECK ADD CONSTRAINT [FK_BOM_Row_BOM_Root]
FOREIGN KEY([BomRootId]) REFERENCES [dbo].[BOM_Root] ([BomRootId])
GO

ALTER TABLE [dbo].[BOM_Row] CHECK CONSTRAINT [FK_BOM_Row_BOM_Root]
GO

ALTER TABLE [dbo].[BOM_Row] WITH CHECK ADD CONSTRAINT [FK_BOM_Row_ParentBomRow]
FOREIGN KEY([ParentBomRowId]) REFERENCES [dbo].[BOM_Row] ([BomRowId])
GO

ALTER TABLE [dbo].[BOM_Row] CHECK CONSTRAINT [FK_BOM_Row_ParentBomRow]
GO