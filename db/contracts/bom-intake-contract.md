\# BOM Intake Contract



\## Overview



This document defines the \*\*strict contract\*\* between the application layer and SQL Server for BOM intake processing.



The SQL stored procedures and table-valued types are the \*\*single source of truth\*\*.



The application MUST conform exactly to this contract. No fields may be inferred or invented.



\---



\## Stored Procedures



\### 1. dbo.usp\_BOM\_Intake\_Create



Creates a new BOM intake record and returns a `BomIntakeId`.



\#### Parameters



| Name              | Type            | Required | Notes |

|------------------|----------------|----------|------|

| CustomerName     | NVARCHAR(200)  | Yes      | Trimmed + uppercased in SQL |

| QuoteNumber      | NVARCHAR(50)   | No       | |

| SourceFileName   | NVARCHAR(260)  | No       | |

| SourceFilePath   | NVARCHAR(500)  | No       | |

| SourceSheetName  | NVARCHAR(128)  | No       | |

| SourceType       | NVARCHAR(50)   | No       | |

| UploadedBy       | NVARCHAR(100)  | No       | |

| ParserVersion    | NVARCHAR(50)   | No       | |

| IntakeNotes      | NVARCHAR(MAX)  | No       | |



\#### Output



| Name          | Type   |

|---------------|--------|

| BomIntakeId   | BIGINT |



\---



\### 2. dbo.usp\_BOM\_Intake\_ProcessStandardized



Processes a standardized BOM payload using structured TVPs.



\#### Parameters



| Name         | Type                           | Required |

|--------------|--------------------------------|----------|

| BomIntakeId  | BIGINT                         | Yes      |

| DetectedBy   | NVARCHAR(100)                  | No       |

| Roots        | dbo.udtt\_BOM\_Intake\_Root       | Yes      |

| Rows         | dbo.udtt\_BOM\_Intake\_Row        | Yes      |



\---



\## Table-Valued Parameter Contracts



\### dbo.udtt\_BOM\_Intake\_Root



Defines the \*\*Level 0 (root assemblies)\*\*.



| Column             | Type             | Required |

|--------------------|------------------|----------|

| RootClientId       | NVARCHAR(50)     | Yes      |

| RootSequence       | INT              | Yes      |

| SourceRowNumber    | INT              | Yes      |

| CustomerName       | NVARCHAR(200)    | Yes      |

| Level0PartNumber   | NVARCHAR(100)    | Yes      |

| Revision           | NVARCHAR(50)     | Yes      |

| RootDescription    | NVARCHAR(500)    | No       |

| RootItemNumber     | NVARCHAR(50)     | No       |

| RootQuantity       | DECIMAL(18,6)    | No       |

| RootUOM            | NVARCHAR(25)     | No       |

| RootMakeBuy        | NVARCHAR(20)     | No       |

| RootMFR            | NVARCHAR(100)    | No       |

| RootMFRNumber      | NVARCHAR(100)    | No       |



\---



\### dbo.udtt\_BOM\_Intake\_Row



Defines all BOM rows (including children).



| Column               | Type              | Required |

|----------------------|-------------------|----------|

| RootClientId         | NVARCHAR(50)      | Yes      |

| RowSequence          | INT               | Yes      |

| SourceRowNumber      | INT               | Yes      |

| OriginalValue        | NVARCHAR(100)     | No       |

| ParentPart           | NVARCHAR(100)     | No       |

| PartNumber           | NVARCHAR(100)     | No       |

| IndentedPartNumber   | NVARCHAR(200)     | No       |

| BomLevel             | INT               | No       |

| Description          | NVARCHAR(500)     | No       |

| Revision             | NVARCHAR(50)      | No       |

| Quantity             | DECIMAL(18,6)     | No       |

| UOM                  | NVARCHAR(25)      | No       |

| ItemNumber           | NVARCHAR(50)      | No       |

| MakeBuy              | NVARCHAR(20)      | No       |

| MFR                  | NVARCHAR(100)     | No       |

| MFRNumber            | NVARCHAR(100)     | No       |

| LeadTimeDays         | DECIMAL(18,2)     | No       |

| Cost                 | DECIMAL(18,4)     | No       |

| ValidationMessage    | NVARCHAR(1000)    | No       |



\---



\## Critical Rules



\### 1. No Extra Fields Allowed



The application MUST NOT send any columns not defined in the TVPs.



Invalid example:

\- `IsLevel0`

\- `BomRootId`

\- `RowGuid`



\---



\### 2. SQL Owns Derived Fields



The following fields are computed inside SQL and must NEVER be supplied by the application:



\- IsLevel0

\- BomRootId

\- RowGuid

\- ParentBomRowId

\- RowPath

\- RowStatus

\- CreatedAt

\- ModifiedAt

\- NormalizedCustomerName

\- NormalizedPartNumber

\- NormalizedRevision

\- DecisionStatus

\- DecisionReason

\- ExistingBomRootId



\---



\### 3. Root ↔ Row Relationship



\- Every row MUST reference a valid `RootClientId`

\- `RootClientId` must be unique in `@Roots`

\- `RowSequence` must be unique per root



\---



\### 4. Required Business Rules Enforced by SQL



SQL will reject the payload if:



\- No roots are provided

\- Duplicate `RootClientId` exists

\- Rows reference unknown roots

\- Duplicate `RowSequence` exists per root

\- Required root fields are blank:

&#x20; - CustomerName

&#x20; - Level0PartNumber

&#x20; - Revision



\---



\## Example Payload (Conceptual)



\### Roots



```json

\[

&#x20; {

&#x20;   "RootClientId": "R1",

&#x20;   "RootSequence": 1,

&#x20;   "SourceRowNumber": 1,

&#x20;   "CustomerName": "ACME",

&#x20;   "Level0PartNumber": "ABC123",

&#x20;   "Revision": "A"

&#x20; }

]



