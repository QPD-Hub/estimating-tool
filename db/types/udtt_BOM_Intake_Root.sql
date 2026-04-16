USE [HILLSBORO_Audit]
GO

CREATE TYPE [dbo].[udtt_BOM_Intake_Root] AS TABLE(
     NOT NULL,
    [RootSequence] [int] NOT NULL,
    [SourceRowNumber] [int] NOT NULL,
     NOT NULL,
     NOT NULL,
     NOT NULL,
     NULL,
     NULL,
    [RootQuantity] [decimal](18, 6) NULL,
     NULL,
     NULL,
     NULL,
     NULL
)
GO