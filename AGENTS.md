\# AGENTS.md



\## Project

Estimating Tool



This repository is for the estimating application only. Keep all work focused on the estimating workflow, document intake, normalization, retrieval, and related infrastructure.



\## How to work in this repo

When making changes in this repository:



1\. Preserve and extend the existing app shape unless explicitly told to refactor.

2\. Prefer incremental changes over broad rewrites.

3\. Keep business logic out of the UI layer.

4\. Keep environment-specific values in config/env, not hardcoded in code.

5\. Keep file-handling logic traceable and safe:

&#x20;  - never silently overwrite files

&#x20;  - fail clearly on duplicate filenames unless the requested feature says otherwise

&#x20;  - prevent path traversal and unsafe path handling

6\. Favor simple, maintainable Python using the current stack unless explicitly told to introduce a framework or dependency.

7\. Do not invent unrelated abstractions.

8\. Make changes that are easy to extend into later workflow stages.



\## Core system principles

1\. The system owns the process.

2\. SQL is the source of truth.

3\. Separate working vs published data.

4\. Track workflow state, not “correctness.”

5\. Everything must be visible and traceable.



\## Current app intent

The app is a document intake and normalization layer for estimating.



The system receives customer document packages and transforms them into a standardized internal structure so documents can later be classified, recalled, and used downstream.



The system is not just a folder copier. It should evolve toward:

\- package intake

\- document expansion/unzip/flatten

\- classification

\- metadata extraction

\- normalization

\- retrieval by document type / part / package



\## Current storage intent

Environment variables control all input/output roots.



Current relevant roots:

\- `DOC\_AUTOMATION\_DROP\_ROOT`

\- `DOC\_WORK\_ROOT`



At this stage, automation and working currently receive the same processed file set unless explicitly changed by a later prompt.



Do not hardcode filesystem roots or environment-specific paths.



\## Direction for upcoming work

Customer document packages are standardized by customer, but standards differ between customers.



The intended architecture is:



\*\*Customer -> Processing Profile -> Rules Pipeline\*\*



Do not hardcode the long-term design as large `if customer == "AMAT"` branches throughout the app.



Instead, future work should move toward:

\- customer selection in the UI

\- resolving a processing profile

\- applying profile-driven rules for classification, extraction, renaming, and routing



\## Preferred future model

The selected customer should eventually map to a package processing profile.



Examples of profile behavior:

\- detect files by pattern, extension, and content

\- identify candidate BOMs, drawings, specs, emails, etc.

\- extract metadata from BOMs and other files

\- normalize filenames

\- place files into standardized internal locations

\- store metadata so the app can later answer requests like:

&#x20; - "open drawing for part XYZ"

&#x20; - "show BOM for top-level part ABC"

&#x20; - "find package docs for customer N"



\## Implementation guidance for customer-specific logic

When adding customer-specific handling:



1\. Prefer a profile/resolver pattern.

2\. Keep reusable pipeline stages shared across customers.

3\. Keep customer-specific matching rules data-driven where practical.

4\. Put nontrivial parsing logic in code, not in the UI.

5\. Avoid scattering customer-specific logic across multiple unrelated files.



\## What should be config-driven vs code-driven

Config/data-driven candidates:

\- customer list

\- default profile mapping

\- filename match patterns

\- document type hints

\- rename templates

\- required-document expectations

\- routing rules



Code-driven candidates:

\- Excel parsing

\- BOM extraction

\- archive extraction

\- file flattening

\- PDF/document parsing

\- higher-confidence classification logic



\## UI guidance

Prefer:

\- customer selection from a controlled list rather than arbitrary free text

\- clear validation

\- visible processing results

\- preserving entered values on validation failure



When adding new UI inputs, keep the interface simple and operationally focused.



\## File processing guidance

When working on package handling:

\- support zip extraction where requested

\- flatten package subfolders when requested

\- treat filenames and paths safely

\- preserve traceability between original source and processed outputs

\- keep result structures rich enough for future metadata/audit storage



\## Database direction

Future work should assume the need for SQL-backed entities such as:

\- Customers

\- ProcessingProfiles

\- DocumentPackages

\- PackageFiles / Documents



Do not hardwire the system around folder browsing alone. Folder layout is an output. The real model is document metadata plus workflow state.



\## Change style

When implementing features:

\- follow existing naming and structure unless a prompt asks for refactoring

\- update related success/error messages when behavior changes

\- keep comments minimal and useful

\- call out assumptions in the final summary

\- prefer small cohesive helper functions over large handlers



\## Near-term roadmap

Priority order:

1\. customer/profile-aware intake

2\. file classification

3\. metadata extraction

4\. standardized naming/routing

5\. SQL-backed metadata/audit

6\. retrieval actions by document type / part / package



\## If instructions are ambiguous

Choose the option that:

\- preserves traceability

\- keeps the system config-driven

\- avoids hardcoded environment assumptions

\- supports future profile-based processing

