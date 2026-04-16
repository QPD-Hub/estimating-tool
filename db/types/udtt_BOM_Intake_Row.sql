USE [HILLSBORO_Audit]
GO

CREATE TYPE [dbo].[udtt_BOM_Intake_Row] AS TABLE(
     NOT NULL,
    [RowSequence] [int] NOT NULL,
    [SourceRowNumber] [int] NOT NULL,
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
     NULL
)
GO