from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

from src.services.bom_intake_payload import (
    BomIntakeMetadata,
    BomIntakePayload,
    StandardizedBomRow,
    build_bom_intake_payload,
)
from src.services.bom_package_locator import BomPackageLocator
from src.services.bom_spreadsheet_parser import BomSpreadsheetParser
from src.services.bom_standardizer import BomStandardizer


DEFAULT_PARSER_VERSION = "bom-parser-v1"
DEFAULT_PREVIEW_PATH = Path("/tmp/bom_intake_payload_preview.json")


@dataclass(frozen=True)
class BomPayloadBuildInput:
    customer_name: str
    uploaded_by: str
    source_file_name: str
    source_file_path: str | None = None
    quote_number: str | None = None
    quoted_by: str | None = None
    contact_name: str | None = None
    quote_due_date: str | None = None
    source_sheet_name: str | None = None
    source_type: str | None = None
    parser_version: str | None = None
    intake_notes: str | None = None


class BomPayloadBuilder:
    def build(
        self,
        *,
        metadata: BomPayloadBuildInput,
        standardized_rows: list[StandardizedBomRow],
    ) -> BomIntakePayload:
        return build_bom_intake_payload(
            metadata=BomIntakeMetadata(
                customer_name=metadata.customer_name,
                quote_number=metadata.quote_number,
                quoted_by=metadata.quoted_by,
                contact_name=metadata.contact_name,
                quote_due_date=metadata.quote_due_date,
                source_file_name=metadata.source_file_name,
                source_file_path=metadata.source_file_path,
                source_sheet_name=metadata.source_sheet_name,
                source_type=metadata.source_type,
                uploaded_by=metadata.uploaded_by,
                parser_version=metadata.parser_version,
                intake_notes=metadata.intake_notes,
            ),
            standardized_rows=standardized_rows,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BOM intake payload preview JSON.")
    parser.add_argument("--source", required=True, help="Path to a spreadsheet or zip package.")
    parser.add_argument("--customer", required=True, help="Customer name.")
    parser.add_argument("--uploaded-by", required=True, help="UploadedBy value.")
    parser.add_argument("--quote-number", help="Optional quote number.")
    parser.add_argument("--intake-notes", help="Optional intake notes.")
    parser.add_argument("--preview-path", default=str(DEFAULT_PREVIEW_PATH))
    args = parser.parse_args()

    source_path = Path(args.source)
    content = source_path.read_bytes()

    locator = BomPackageLocator()
    parser_service = BomSpreadsheetParser()
    standardizer = BomStandardizer()
    payload_builder = BomPayloadBuilder()

    located = locator.locate(
        filename=source_path.name,
        content=content,
        source_file_path=str(source_path),
    )
    parsed = parser_service.parse(filename=located.filename, content=located.content)
    standardized = standardizer.standardize(parsed)
    payload = payload_builder.build(
        metadata=BomPayloadBuildInput(
            customer_name=args.customer,
            uploaded_by=args.uploaded_by,
            quote_number=args.quote_number,
            source_file_name=located.filename,
            source_file_path=located.source_file_path or str(source_path),
            source_sheet_name=parsed.sheet_name,
            source_type=located.source_type,
            parser_version=DEFAULT_PARSER_VERSION,
            intake_notes=args.intake_notes,
        ),
        standardized_rows=standardized.rows,
    )

    preview_path = Path(args.preview_path)
    preview_path.parent.mkdir(parents=True, exist_ok=True)
    preview_path.write_text(json.dumps(payload.to_preview_dict(), indent=2) + "\n", encoding="utf-8")
    print(preview_path)


if __name__ == "__main__":
    main()
