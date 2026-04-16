USE [HILLSBORO_Audit]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[usp_BOM_Intake_ProcessStandardized]
    @BomIntakeId     BIGINT,
    @DetectedBy      NVARCHAR(100) = NULL,
    @Roots           dbo.udtt_BOM_Intake_Root READONLY,
    @Rows            dbo.udtt_BOM_Intake_Row READONLY
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @AcceptedRootCount INT = 0,
        @DuplicateRejectedCount INT = 0,
        @DetectedRootCount INT = 0,
        @FinalIntakeStatus NVARCHAR(30);

    IF NOT EXISTS (
        SELECT 1
        FROM dbo.BOM_Intake
        WHERE BomIntakeId = @BomIntakeId
    )
    BEGIN
        THROW 50010, 'BomIntakeId was not found.', 1;
    END;

    IF NOT EXISTS (SELECT 1 FROM @Roots)
    BEGIN
        UPDATE dbo.BOM_Intake
        SET IntakeStatus = 'failed',
            ModifiedAt = SYSDATETIME()
        WHERE BomIntakeId = @BomIntakeId;

        THROW 50011, 'At least one root candidate is required.', 1;
    END;

    IF EXISTS (
        SELECT RootClientId
        FROM @Roots
        GROUP BY RootClientId
        HAVING COUNT(*) > 1
    )
    BEGIN
        UPDATE dbo.BOM_Intake
        SET IntakeStatus = 'failed',
            ModifiedAt = SYSDATETIME()
        WHERE BomIntakeId = @BomIntakeId;

        THROW 50012, 'Duplicate RootClientId values were found in @Roots.', 1;
    END;

    IF EXISTS (
        SELECT 1
        FROM @Rows r
        LEFT JOIN @Roots rt
            ON rt.RootClientId = r.RootClientId
        WHERE rt.RootClientId IS NULL
    )
    BEGIN
        UPDATE dbo.BOM_Intake
        SET IntakeStatus = 'failed',
            ModifiedAt = SYSDATETIME()
        WHERE BomIntakeId = @BomIntakeId;

        THROW 50013, 'One or more rows reference a RootClientId not present in @Roots.', 1;
    END;

    IF EXISTS (
        SELECT RootClientId, RowSequence
        FROM @Rows
        GROUP BY RootClientId, RowSequence
        HAVING COUNT(*) > 1
    )
    BEGIN
        UPDATE dbo.BOM_Intake
        SET IntakeStatus = 'failed',
            ModifiedAt = SYSDATETIME()
        WHERE BomIntakeId = @BomIntakeId;

        THROW 50014, 'Duplicate RowSequence values were found within a root.', 1;
    END;

    BEGIN TRY
        BEGIN TRAN;

        CREATE TABLE #StageRoots
        (
            RootClientId                NVARCHAR(50)  NOT NULL PRIMARY KEY,
            RootSequence                INT           NOT NULL,
            SourceRowNumber             INT           NOT NULL,
            CustomerName                NVARCHAR(200) NOT NULL,
            Level0PartNumber            NVARCHAR(100) NOT NULL,
            Revision                    NVARCHAR(50)  NOT NULL,
            NormalizedCustomerName      NVARCHAR(200) NOT NULL,
            NormalizedPartNumber        NVARCHAR(100) NOT NULL,
            NormalizedRevision          NVARCHAR(50)  NOT NULL,
            RootDescription             NVARCHAR(500) NULL,
            RootItemNumber              NVARCHAR(50)  NULL,
            RootQuantity                DECIMAL(18,6) NULL,
            RootUOM                     NVARCHAR(25)  NULL,
            RootMakeBuy                 NVARCHAR(20)  NULL,
            RootMFR                     NVARCHAR(100) NULL,
            RootMFRNumber               NVARCHAR(100) NULL,
            ExistingBomRootId           BIGINT        NULL,
            DecisionStatus              NVARCHAR(30)  NULL,
            DecisionReason              NVARCHAR(1000) NULL,
            InternalDuplicateRank       INT           NULL
        );

        INSERT INTO #StageRoots
        (
            RootClientId,
            RootSequence,
            SourceRowNumber,
            CustomerName,
            Level0PartNumber,
            Revision,
            NormalizedCustomerName,
            NormalizedPartNumber,
            NormalizedRevision,
            RootDescription,
            RootItemNumber,
            RootQuantity,
            RootUOM,
            RootMakeBuy,
            RootMFR,
            RootMFRNumber,
            InternalDuplicateRank
        )
        SELECT
            r.RootClientId,
            r.RootSequence,
            r.SourceRowNumber,
            UPPER(LTRIM(RTRIM(r.CustomerName))),
            UPPER(LTRIM(RTRIM(r.Level0PartNumber))),
            UPPER(LTRIM(RTRIM(r.Revision))),
            UPPER(LTRIM(RTRIM(r.CustomerName))),
            UPPER(LTRIM(RTRIM(r.Level0PartNumber))),
            UPPER(LTRIM(RTRIM(r.Revision))),
            NULLIF(LTRIM(RTRIM(r.RootDescription)), ''),
            NULLIF(LTRIM(RTRIM(r.RootItemNumber)), ''),
            r.RootQuantity,
            NULLIF(LTRIM(RTRIM(r.RootUOM)), ''),
            NULLIF(LTRIM(RTRIM(r.RootMakeBuy)), ''),
            NULLIF(LTRIM(RTRIM(r.RootMFR)), ''),
            NULLIF(LTRIM(RTRIM(r.RootMFRNumber)), ''),
            ROW_NUMBER() OVER (
                PARTITION BY
                    UPPER(LTRIM(RTRIM(r.CustomerName))),
                    UPPER(LTRIM(RTRIM(r.Level0PartNumber))),
                    UPPER(LTRIM(RTRIM(r.Revision)))
                ORDER BY r.RootSequence
            )
        FROM @Roots r;

        IF EXISTS (
            SELECT 1
            FROM #StageRoots
            WHERE NormalizedCustomerName = ''
               OR NormalizedPartNumber = ''
               OR NormalizedRevision = ''
        )
        BEGIN
            THROW 50015, 'CustomerName, Level0PartNumber, and Revision are required for every root.', 1;
        END;

        UPDATE sr
        SET sr.ExistingBomRootId = br.BomRootId
        FROM #StageRoots sr
        INNER JOIN dbo.BOM_Root br
            ON br.CustomerName = sr.NormalizedCustomerName
           AND br.Level0PartNumber = sr.NormalizedPartNumber
           AND br.Revision = sr.NormalizedRevision;

        UPDATE sr
        SET
            DecisionStatus =
                CASE
                    WHEN sr.ExistingBomRootId IS NOT NULL THEN 'duplicate_rejected'
                    WHEN sr.InternalDuplicateRank > 1 THEN 'duplicate_rejected'
                    ELSE 'accepted'
                END,
            DecisionReason =
                CASE
                    WHEN sr.ExistingBomRootId IS NOT NULL THEN 'Rejected because Customer + Level0PartNumber + Revision already exists.'
                    WHEN sr.InternalDuplicateRank > 1 THEN 'Rejected because the same Customer + Level0PartNumber + Revision appears multiple times in the current upload.'
                    ELSE 'Accepted as new canonical BOM root.'
                END
        FROM #StageRoots sr;

        CREATE TABLE #AcceptedRootMap
        (
            RootClientId NVARCHAR(50) NOT NULL PRIMARY KEY,
            BomRootId    BIGINT NOT NULL
        );

        MERGE dbo.BOM_Root AS target
        USING
        (
            SELECT
                src.RootClientId,
                @BomIntakeId AS BomIntakeId,
                src.NormalizedCustomerName AS CustomerName,
                src.NormalizedPartNumber AS Level0PartNumber,
                src.NormalizedRevision AS Revision,
                src.NormalizedCustomerName,
                src.NormalizedPartNumber,
                src.NormalizedRevision,
                src.RootDescription,
                src.RootItemNumber,
                src.RootQuantity,
                src.RootUOM,
                src.RootMakeBuy,
                src.RootMFR,
                src.RootMFRNumber,
                CAST('raw' AS NVARCHAR(30)) AS RootStatus
            FROM #StageRoots src
            WHERE src.DecisionStatus = 'accepted'
        ) AS src
            ON 1 = 0
        WHEN NOT MATCHED THEN
            INSERT
            (
                BomIntakeId,
                CustomerName,
                Level0PartNumber,
                Revision,
                NormalizedCustomerName,
                NormalizedPartNumber,
                NormalizedRevision,
                RootDescription,
                RootItemNumber,
                RootQuantity,
                RootUOM,
                RootMakeBuy,
                RootMFR,
                RootMFRNumber,
                RootStatus
            )
            VALUES
            (
                src.BomIntakeId,
                src.CustomerName,
                src.Level0PartNumber,
                src.Revision,
                src.NormalizedCustomerName,
                src.NormalizedPartNumber,
                src.NormalizedRevision,
                src.RootDescription,
                src.RootItemNumber,
                src.RootQuantity,
                src.RootUOM,
                src.RootMakeBuy,
                src.RootMFR,
                src.RootMFRNumber,
                src.RootStatus
            )
        OUTPUT
            src.RootClientId,
            inserted.BomRootId
        INTO #AcceptedRootMap (RootClientId, BomRootId);

        INSERT INTO dbo.BOM_Intake_Root_Result
        (
            BomIntakeId,
            BomRootId,
            ExistingBomRootId,
            RootClientId,
            RootSequence,
            SourceRowNumber,
            CustomerName,
            Level0PartNumber,
            Revision,
            NormalizedCustomerName,
            NormalizedPartNumber,
            NormalizedRevision,
            RootDescription,
            RootItemNumber,
            RootQuantity,
            RootUOM,
            DecisionStatus,
            DecisionReason,
            DetectedBy
        )
        SELECT
            @BomIntakeId,
            arm.BomRootId,
            CASE WHEN sr.DecisionStatus = 'duplicate_rejected' THEN sr.ExistingBomRootId END,
            sr.RootClientId,
            sr.RootSequence,
            sr.SourceRowNumber,
            sr.CustomerName,
            sr.Level0PartNumber,
            sr.Revision,
            sr.NormalizedCustomerName,
            sr.NormalizedPartNumber,
            sr.NormalizedRevision,
            sr.RootDescription,
            sr.RootItemNumber,
            sr.RootQuantity,
            sr.RootUOM,
            sr.DecisionStatus,
            sr.DecisionReason,
            NULLIF(LTRIM(RTRIM(@DetectedBy)), '')
        FROM #StageRoots sr
        LEFT JOIN #AcceptedRootMap arm
            ON arm.RootClientId = sr.RootClientId;

        INSERT INTO dbo.BOM_Row
        (
            BomIntakeId,
            BomRootId,
            SourceRowNumber,
            SourceRowSequence,
            OriginalValue,
            ParentPart,
            PartNumber,
            IndentedPartNumber,
            BomLevel,
            Description,
            Revision,
            Quantity,
            UOM,
            ItemNumber,
            MakeBuy,
            MFR,
            MFRNumber,
            LeadTimeDays,
            Cost,
            IsLevel0,
            RowStatus,
            ValidationMessage
        )
        SELECT
            @BomIntakeId,
            arm.BomRootId,
            r.SourceRowNumber,
            r.RowSequence,
            r.OriginalValue,
            r.ParentPart,
            r.PartNumber,
            r.IndentedPartNumber,
            r.BomLevel,
            r.Description,
            r.Revision,
            r.Quantity,
            r.UOM,
            r.ItemNumber,
            r.MakeBuy,
            r.MFR,
            r.MFRNumber,
            r.LeadTimeDays,
            r.Cost,
            CASE WHEN r.RowSequence = 1 OR r.BomLevel = 0 THEN 1 ELSE 0 END,
            'raw',
            r.ValidationMessage
        FROM @Rows r
        INNER JOIN #AcceptedRootMap arm
            ON arm.RootClientId = r.RootClientId;

        SELECT
            @DetectedRootCount = COUNT(*),
            @AcceptedRootCount = SUM(CASE WHEN DecisionStatus = 'accepted' THEN 1 ELSE 0 END),
            @DuplicateRejectedCount = SUM(CASE WHEN DecisionStatus = 'duplicate_rejected' THEN 1 ELSE 0 END)
        FROM #StageRoots;

        SET @FinalIntakeStatus =
            CASE
                WHEN @AcceptedRootCount > 0 AND @DuplicateRejectedCount = 0 THEN 'processed'
                WHEN @AcceptedRootCount > 0 AND @DuplicateRejectedCount > 0 THEN 'processed_with_duplicates'
                WHEN @AcceptedRootCount = 0 AND @DuplicateRejectedCount > 0 THEN 'processed_with_duplicates'
                ELSE 'failed'
            END;

        UPDATE dbo.BOM_Intake
        SET IntakeStatus = @FinalIntakeStatus,
            ModifiedAt = SYSDATETIME()
        WHERE BomIntakeId = @BomIntakeId;

        COMMIT;

        SELECT
            BomIntakeId = @BomIntakeId,
            DetectedRootCount = @DetectedRootCount,
            AcceptedRootCount = @AcceptedRootCount,
            DuplicateRejectedCount = @DuplicateRejectedCount,
            FinalIntakeStatus = @FinalIntakeStatus;

        SELECT
            sr.RootClientId,
            sr.RootSequence,
            sr.CustomerName,
            sr.Level0PartNumber,
            sr.Revision,
            sr.DecisionStatus,
            sr.DecisionReason,
            arm.BomRootId,
            sr.ExistingBomRootId
        FROM #StageRoots sr
        LEFT JOIN #AcceptedRootMap arm
            ON arm.RootClientId = sr.RootClientId
        ORDER BY sr.RootSequence;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK;

        UPDATE dbo.BOM_Intake
        SET IntakeStatus = 'failed',
            ModifiedAt = SYSDATETIME()
        WHERE BomIntakeId = @BomIntakeId;

        THROW;
    END CATCH
END
GO
