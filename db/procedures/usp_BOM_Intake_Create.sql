USE [HILLSBORO_Audit]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[usp_BOM_Intake_Create]
    @CustomerName       NVARCHAR(200),
    @QuoteNumber        NVARCHAR(50) = NULL,
    @SourceFileName     NVARCHAR(260) = NULL,
    @SourceFilePath     NVARCHAR(500) = NULL,
    @SourceSheetName    NVARCHAR(128) = NULL,
    @SourceType         NVARCHAR(50) = NULL,
    @UploadedBy         NVARCHAR(100) = NULL,
    @ParserVersion      NVARCHAR(50) = NULL,
    @IntakeNotes        NVARCHAR(MAX) = NULL,
    @BomIntakeId        BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CustomerNameNorm NVARCHAR(200) = UPPER(LTRIM(RTRIM(@CustomerName)));

    IF @CustomerNameNorm IS NULL OR @CustomerNameNorm = ''
    BEGIN
        THROW 50001, 'CustomerName is required.', 1;
    END;

    INSERT INTO dbo.BOM_Intake
    (
        CustomerName,
        QuoteNumber,
        SourceFileName,
        SourceFilePath,
        SourceSheetName,
        SourceType,
        IntakeStatus,
        UploadedBy,
        ParserVersion,
        IntakeNotes
    )
    VALUES
    (
        @CustomerNameNorm,
        NULLIF(LTRIM(RTRIM(@QuoteNumber)), ''),
        NULLIF(LTRIM(RTRIM(@SourceFileName)), ''),
        NULLIF(LTRIM(RTRIM(@SourceFilePath)), ''),
        NULLIF(LTRIM(RTRIM(@SourceSheetName)), ''),
        NULLIF(LTRIM(RTRIM(@SourceType)), ''),
        'raw',
        NULLIF(LTRIM(RTRIM(@UploadedBy)), ''),
        NULLIF(LTRIM(RTRIM(@ParserVersion)), ''),
        @IntakeNotes
    );

    SET @BomIntakeId = SCOPE_IDENTITY();

    SELECT
        BomIntakeId   = @BomIntakeId,
        IntakeStatus  = 'raw';
END
GO