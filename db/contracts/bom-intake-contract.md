# BOM Intake Contract

## Overview

This document defines the strict SQL-bound contract for BOM intake.

SQL Server is the source of truth. The application may validate and transport data, but it must only send fields that are explicitly defined by the stored procedures and table-valued types below.

## Source-of-truth files

```text
/db/procedures/usp_BOM_Intake_Create.sql
/db/procedures/usp_BOM_Intake_ProcessStandardized.sql
/db/types/udtt_BOM_Intake_Root.sql
/db/types/udtt_BOM_Intake_Row.sql
/db/tables/BOM_Row.sql
```

## Stored procedures

### `dbo.usp_BOM_Intake_Create`

Creates the intake header and returns `BomIntakeId`.

| Name | Type | Required | Notes |
| --- | --- | --- | --- |
| `CustomerName` | `NVARCHAR(200)` | Yes | Trimmed and uppercased in SQL |
| `QuoteNumber` | `NVARCHAR(50)` | No | |
| `QuotedBy` | `NVARCHAR(100)` | No | |
| `ContactName` | `NVARCHAR(200)` | No | |
| `QuoteDueDate` | `DATE` | No | |
| `SourceFileName` | `NVARCHAR(260)` | No | |
| `SourceFilePath` | `NVARCHAR(500)` | No | |
| `SourceSheetName` | `NVARCHAR(128)` | No | |
| `SourceType` | `NVARCHAR(50)` | No | |
| `UploadedBy` | `NVARCHAR(100)` | No | |
| `ParserVersion` | `NVARCHAR(50)` | No | |
| `IntakeNotes` | `NVARCHAR(MAX)` | No | |

Output:

| Name | Type |
| --- | --- |
| `BomIntakeId` | `BIGINT` |

### `dbo.usp_BOM_Intake_ProcessStandardized`

Processes a standardized BOM payload using scalar parameters plus TVPs.

| Name | Type | Required |
| --- | --- | --- |
| `BomIntakeId` | `BIGINT` | Yes |
| `DetectedBy` | `NVARCHAR(100)` | No |
| `Roots` | `dbo.udtt_BOM_Intake_Root` | Yes |
| `Rows` | `dbo.udtt_BOM_Intake_Row` | Yes |

## TVP contracts

### `dbo.udtt_BOM_Intake_Root`

| Column | Type | Required |
| --- | --- | --- |
| `RootClientId` | `NVARCHAR(50)` | Yes |
| `RootSequence` | `INT` | Yes |
| `SourceRowNumber` | `INT` | Yes |
| `CustomerName` | `NVARCHAR(200)` | Yes |
| `Level0PartNumber` | `NVARCHAR(100)` | Yes |
| `Revision` | `NVARCHAR(50)` | Yes |
| `RootDescription` | `NVARCHAR(500)` | No |
| `RootItemNumber` | `NVARCHAR(50)` | No |
| `RootQuantity` | `DECIMAL(18,6)` | No |
| `RootUOM` | `NVARCHAR(25)` | No |
| `RootMakeBuy` | `NVARCHAR(20)` | No |
| `RootMFR` | `NVARCHAR(100)` | No |
| `RootMFRNumber` | `NVARCHAR(100)` | No |

### `dbo.udtt_BOM_Intake_Row`

| Column | Type | Required |
| --- | --- | --- |
| `RootClientId` | `NVARCHAR(50)` | Yes |
| `RowSequence` | `INT` | Yes |
| `SourceRowNumber` | `INT` | Yes |
| `OriginalValue` | `NVARCHAR(100)` | No |
| `ParentPart` | `NVARCHAR(100)` | No |
| `PartNumber` | `NVARCHAR(100)` | No |
| `IndentedPartNumber` | `NVARCHAR(200)` | No |
| `BomLevel` | `INT` | No |
| `Description` | `NVARCHAR(500)` | No |
| `Revision` | `NVARCHAR(50)` | No |
| `Quantity` | `DECIMAL(18,6)` | No |
| `UOM` | `NVARCHAR(25)` | No |
| `ItemNumber` | `NVARCHAR(50)` | No |
| `MakeBuy` | `NVARCHAR(20)` | No |
| `MFR` | `NVARCHAR(100)` | No |
| `MFRNumber` | `NVARCHAR(100)` | No |
| `LeadTimeDays` | `DECIMAL(18,2)` | No |
| `Cost` | `DECIMAL(18,4)` | No |
| `ValidationMessage` | `NVARCHAR(1000)` | No |

## Application-owned fields

The application may only send:

- `dbo.usp_BOM_Intake_Create` scalar parameters
- `dbo.usp_BOM_Intake_ProcessStandardized` scalar parameters
- `dbo.udtt_BOM_Intake_Root` row columns
- `dbo.udtt_BOM_Intake_Row` row columns

Unknown or extra fields must be rejected before execution.

## SQL-owned fields

These fields are derived or assigned by SQL and must never be sent by the application:

- `IsLevel0`
- `BomRootId`
- `ExistingBomRootId`
- `RowGuid`
- `ParentBomRowId`
- `RowPath`
- `RowStatus`
- `CreatedAt`
- `ModifiedAt`
- `NormalizedCustomerName`
- `NormalizedPartNumber`
- `NormalizedRevision`
- `DecisionStatus`
- `DecisionReason`
- `InternalDuplicateRank`
- `BomIntakeId` inside TVP rows

`IsLevel0` is computed inside SQL during insert into `dbo.BOM_Row`. It is not part of `dbo.udtt_BOM_Intake_Row`.

## Relationship and validation rules

- `@Roots` must contain at least one row.
- `RootClientId` must be unique within `@Roots`.
- `CustomerName`, `Level0PartNumber`, and `Revision` are required for every root.
- Every row in `@Rows` must reference a `RootClientId` present in `@Roots`.
- `RowSequence` must be unique within each root.
- Only accepted roots are inserted into `dbo.BOM_Root`.
- Rows are inserted only for accepted roots through the accepted-root mapping.

## Accepted root mapping

SQL stages roots into `#StageRoots`, determines which roots are accepted, then writes accepted roots into `dbo.BOM_Root`.

To preserve `RootClientId -> BomRootId` mapping legally, `dbo.usp_BOM_Intake_ProcessStandardized` uses:

- `MERGE dbo.BOM_Root AS target`
- `USING (...) AS src`
- `ON 1 = 0`
- `WHEN NOT MATCHED THEN INSERT ...`
- `OUTPUT src.RootClientId, inserted.BomRootId INTO #AcceptedRootMap`

This avoids the invalid `INSERT ... SELECT ... OUTPUT src.RootClientId, inserted.BomRootId` pattern while still preserving the mapping required for child row inserts.

## Dry run

The app supports dry-run preview mode. Dry-run must:

- build the exact SQL-bound payload
- avoid executing stored procedures
- write preview JSON to `/tmp/bom_intake_payload_preview.json`

## Example preview payload

```json
{
  "createProc": {
    "procedure": "dbo.usp_BOM_Intake_Create",
    "params": {
      "CustomerName": "ACME",
      "QuoteNumber": "Q-100",
      "QuotedBy": "estimator",
      "ContactName": "Alice Smith",
      "QuoteDueDate": "2026-05-01",
      "SourceFileName": "customer-bom.xlsx",
      "SourceFilePath": "/tmp/customer-bom.xlsx",
      "SourceSheetName": "BOM",
      "SourceType": "standardized_upload",
      "UploadedBy": "estimator",
      "ParserVersion": "v1",
      "IntakeNotes": "fixture preview"
    }
  },
  "processStandardizedProc": {
    "procedure": "dbo.usp_BOM_Intake_ProcessStandardized",
    "params": {
      "BomIntakeId": null,
      "DetectedBy": "estimator"
    },
    "roots": [
      {
        "RootClientId": "R1",
        "RootSequence": 1,
        "SourceRowNumber": 1,
        "CustomerName": "ACME",
        "Level0PartNumber": "ABC-1000",
        "Revision": "1",
        "RootDescription": "TOP",
        "RootItemNumber": "10",
        "RootQuantity": 1,
        "RootUOM": "EA",
        "RootMakeBuy": "MAKE",
        "RootMFR": null,
        "RootMFRNumber": null
      }
    ],
    "rows": [
      {
        "RootClientId": "R1",
        "RowSequence": 1,
        "SourceRowNumber": 1,
        "OriginalValue": null,
        "ParentPart": null,
        "PartNumber": "ABC-1000",
        "IndentedPartNumber": "ABC-1000",
        "BomLevel": 0,
        "Description": "TOP",
        "Revision": "1",
        "Quantity": 1,
        "UOM": "EA",
        "ItemNumber": "10",
        "MakeBuy": "MAKE",
        "MFR": null,
        "MFRNumber": null,
        "LeadTimeDays": null,
        "Cost": null,
        "ValidationMessage": null
      }
    ]
  }
}
```
