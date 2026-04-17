from __future__ import annotations

import base64
import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from src.services.bom_payload_builder import (
    DEFAULT_PARSER_VERSION,
    BomPayloadBuildInput,
    BomPayloadBuilder,
)
from src.services.bom_package_locator import BomPackageLocator, BomPackageLocatorError
from src.services.bom_intake_db import BomIntakeDbService
from src.services.bom_intake_payload import (
    BomIntakeMetadata,
    BomIntakePayload,
    BomIntakePayloadError,
    StandardizedBomRow,
    build_bom_intake_payload,
)
from src.services.bom_spreadsheet_parser import (
    BomSpreadsheetParser,
    BomSpreadsheetParserError,
)
from src.services.bom_standardizer import BomStandardizer, BomStandardizerError

logger = logging.getLogger(__name__)

DEFAULT_PREVIEW_PATH = Path("/tmp/bom_intake_payload_preview.json")

HEADER_REQUEST_FIELDS = {
    "customer_name",
    "quote_number",
    "source_file_name",
    "source_file_path",
    "source_sheet_name",
    "source_type",
    "uploaded_by",
    "parser_version",
    "intake_notes",
}

STANDARDIZED_ROW_REQUEST_FIELDS = {
    "source_row_number",
    "original_value",
    "parent_part",
    "part_number",
    "indented_part_number",
    "bom_level",
    "description",
    "revision",
    "quantity",
    "uom",
    "item_number",
    "make_buy",
    "mfr",
    "mfr_number",
    "lead_time_days",
    "cost",
    "validation_message",
}

UPLOAD_REQUEST_FIELDS = {
    "source_file_path",
    "filename",
    "content_base64",
    "content",
}


class BomIntakeServiceError(ValueError):
    pass


class BomIntakeRequestError(BomIntakeServiceError):
    def __init__(
        self,
        message: str,
        *,
        diagnostics: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.diagnostics = diagnostics


@dataclass(frozen=True)
class BomIntakePreview:
    selected_file_name: str
    detected_worksheet: str
    detected_source_type: str
    source_file_path: str | None
    root_count: int
    row_count: int
    standardized_rows: list[StandardizedBomRow]
    payload: BomIntakePayload
    diagnostics: dict[str, object] | None = None

    def to_dict(self) -> dict[str, object]:
        preview_payload = self.payload.to_preview_dict()
        response = {
            "selectedFileName": self.selected_file_name,
            "detectedWorksheet": self.detected_worksheet,
            "detectedSourceType": self.detected_source_type,
            "sourceFilePath": self.source_file_path,
            "rootCount": self.root_count,
            "rowCount": self.row_count,
            "standardizedRows": [
                {
                    "source_row_number": row["source_row_number"],
                    "original_value": row["original_value"],
                    "parent_part": row["parent_part"],
                    "part_number": row["part_number"],
                    "indented_part_number": row["indented_part_number"],
                    "bom_level": row["bom_level"],
                    "description": row["description"],
                    "revision": row["revision"],
                    "quantity": row["quantity"],
                    "uom": row["uom"],
                    "item_number": row["item_number"],
                    "make_buy": row["make_buy"],
                    "mfr": row["mfr"],
                    "mfr_number": row["mfr_number"],
                    "lead_time_days": row["lead_time_days"],
                    "cost": row["cost"],
                    "validation_message": row["validation_message"],
                }
                for row in (asdict(standardized_row) for standardized_row in self.standardized_rows)
            ],
            "createProc": preview_payload["createProc"],
            "processStandardizedProc": preview_payload["processStandardizedProc"],
            "createProcParams": preview_payload["createProc"]["params"],
            "processProcParams": preview_payload["processStandardizedProc"]["params"],
            "rootsTvpRows": preview_payload["processStandardizedProc"]["roots"],
            "bomRowsTvpRows": preview_payload["processStandardizedProc"]["rows"],
        }
        if self.diagnostics is not None:
            response["diagnostics"] = self.diagnostics
        return response


class BomIntakeService:
    def __init__(
        self,
        db_service: BomIntakeDbService,
        *,
        package_locator: BomPackageLocator | None = None,
        spreadsheet_parser: BomSpreadsheetParser | None = None,
        standardizer: BomStandardizer | None = None,
        payload_builder: BomPayloadBuilder | None = None,
    ) -> None:
        self._db_service = db_service
        self._package_locator = package_locator or BomPackageLocator()
        self._spreadsheet_parser = spreadsheet_parser or BomSpreadsheetParser()
        self._standardizer = standardizer or BomStandardizer()
        self._payload_builder = payload_builder or BomPayloadBuilder()

    def process_standardized_upload(
        self,
        header_data: Mapping[str, object],
        standardized_rows_data: Sequence[Mapping[str, object]],
        *,
        dry_run: bool = False,
        preview_path: Path = DEFAULT_PREVIEW_PATH,
    ) -> dict[str, object]:
        payload = self.build_standardized_payload(
            header_data=header_data,
            standardized_rows_data=standardized_rows_data,
        )

        if dry_run:
            # Dry-run previews the exact SQL-bound payload the app would send to SQL Server.
            preview = payload.to_preview_dict()
            self._write_preview_json(preview_path, preview)
            return {
                "DryRun": True,
                "PreviewPath": str(preview_path),
                "Payload": preview,
            }

        logger.info(
            "Dispatching standardized BOM upload for customer '%s' with %s roots and %s rows.",
            payload.create_input.CustomerName,
            len(payload.roots),
            len(payload.rows),
        )

        return self._db_service.create_and_process_intake(payload=payload)

    def process_uploaded_bom(
        self,
        *,
        header_data: Mapping[str, object],
        upload_data: Mapping[str, object],
        dry_run: bool = False,
        preview_path: Path = DEFAULT_PREVIEW_PATH,
    ) -> dict[str, object]:
        preview = self.preview_uploaded_bom(
            header_data=header_data,
            upload_data=upload_data,
        )
        payload = preview.payload

        if dry_run:
            preview_payload = payload.to_preview_dict()
            self._write_preview_json(preview_path, preview_payload)
            return {
                "DryRun": True,
                "PreviewPath": str(preview_path),
                "Payload": preview_payload,
            }

        logger.info(
            "Dispatching uploaded BOM for customer '%s' with %s roots and %s rows.",
            payload.create_input.CustomerName,
            len(payload.roots),
            len(payload.rows),
        )
        return self._db_service.create_and_process_intake(payload=payload)

    def preview_uploaded_bom(
        self,
        *,
        header_data: Mapping[str, object],
        upload_data: Mapping[str, object],
    ) -> BomIntakePreview:
        if not isinstance(header_data, Mapping):
            raise BomIntakeRequestError("Request field 'header' must be an object.")
        if not isinstance(upload_data, Mapping):
            raise BomIntakeRequestError("Request field 'upload' must be an object.")

        metadata = self._build_metadata_for_upload(header_data, upload_data)
        upload_file = self._build_upload_file(upload_data)

        try:
            located = self._package_locator.locate(
                filename=upload_file["filename"],
                content=upload_file["content"],
                source_file_path=upload_file["source_file_path"],
            )
            parsed = self._spreadsheet_parser.parse(
                filename=located.filename,
                content=located.content,
            )
            standardized = self._standardizer.standardize(parsed)
            payload = self._payload_builder.build(
                metadata=BomPayloadBuildInput(
                    customer_name=metadata.customer_name,
                    uploaded_by=metadata.uploaded_by,
                    quote_number=metadata.quote_number,
                    source_file_name=metadata.source_file_name or located.filename,
                    source_file_path=metadata.source_file_path
                    or located.source_file_path,
                    source_sheet_name=metadata.source_sheet_name or parsed.sheet_name,
                    source_type=metadata.source_type or located.source_type,
                    parser_version=metadata.parser_version or DEFAULT_PARSER_VERSION,
                    intake_notes=metadata.intake_notes,
                ),
                standardized_rows=standardized.rows,
            )
        except (
            BomPackageLocatorError,
            BomSpreadsheetParserError,
            BomStandardizerError,
            BomIntakePayloadError,
        ) as exc:
            raise BomIntakeRequestError(
                str(exc),
                diagnostics=self._preview_diagnostics(
                    metadata=metadata,
                    package_diagnostics=getattr(
                        locals().get("located", None),
                        "diagnostics",
                        None,
                    )
                    or getattr(exc, "diagnostics", None),
                    parse_diagnostics=getattr(
                        locals().get("parsed", None),
                        "diagnostics",
                        None,
                    )
                    or getattr(exc, "diagnostics", None),
                ),
            ) from exc

        return BomIntakePreview(
            selected_file_name=located.filename,
            detected_worksheet=parsed.sheet_name,
            detected_source_type=located.source_type,
            source_file_path=located.source_file_path,
            root_count=len(payload.roots),
            row_count=len(payload.rows),
            standardized_rows=standardized.rows,
            payload=payload,
            diagnostics=self._preview_diagnostics(
                metadata=metadata,
                package_diagnostics=located.diagnostics,
                parse_diagnostics=parsed.diagnostics,
            ),
        )

    def _preview_diagnostics(
        self,
        *,
        metadata: BomIntakeMetadata,
        package_diagnostics,
        parse_diagnostics,
    ) -> dict[str, object]:
        diagnostics: dict[str, object] = {
            "selectedSourceFileName": metadata.source_file_name,
            "selectedArchiveMemberName": None,
            "candidateSpreadsheets": [],
            "archiveSelection": None,
            "selectedWorksheetName": None,
            "worksheetNames": [],
            "firstRowsPreview": [],
            "headerRowCandidates": [],
            "worksheets": [],
        }

        if package_diagnostics is not None:
            package_dict = package_diagnostics.to_dict()
            diagnostics["selectedSourceFileName"] = package_dict["sourceFileName"]
            diagnostics["selectedArchiveMemberName"] = package_dict["selectedArchiveMemberName"]
            diagnostics["candidateSpreadsheets"] = package_dict["candidateSpreadsheets"]
            diagnostics["archiveSelection"] = {
                "selectedSpreadsheetFilename": package_dict["selectedSpreadsheetFilename"],
                "selectionReason": package_dict["selectionReason"],
            }

        if parse_diagnostics is not None:
            parse_dict = parse_diagnostics.to_dict()
            diagnostics["selectedWorksheetName"] = parse_dict["selectedWorksheetName"]
            diagnostics["worksheetNames"] = parse_dict["worksheetNames"]
            diagnostics["firstRowsPreview"] = parse_dict["firstRowsPreview"]
            diagnostics["headerRowCandidates"] = parse_dict["headerRowCandidates"]
            diagnostics["worksheets"] = parse_dict["worksheets"]

        return diagnostics

    def build_standardized_payload(
        self,
        *,
        header_data: Mapping[str, object],
        standardized_rows_data: Sequence[Mapping[str, object]],
    ) -> BomIntakePayload:
        if not isinstance(header_data, Mapping):
            raise BomIntakeRequestError("Request field 'header' must be an object.")
        if not isinstance(standardized_rows_data, Sequence) or isinstance(
            standardized_rows_data,
            (str, bytes),
        ):
            raise BomIntakeRequestError(
                "Request field 'standardizedBomRows' must be an array."
            )
        if not standardized_rows_data:
            raise BomIntakeRequestError(
                "Request field 'standardizedBomRows' must contain at least one row."
            )

        metadata = self._build_metadata(header_data)
        standardized_rows = self._build_standardized_rows(standardized_rows_data)

        try:
            return build_bom_intake_payload(
                metadata=metadata,
                standardized_rows=standardized_rows,
            )
        except BomIntakePayloadError as exc:
            raise BomIntakeRequestError(str(exc)) from exc

    def build_uploaded_payload(
        self,
        *,
        header_data: Mapping[str, object],
        upload_data: Mapping[str, object],
    ) -> BomIntakePayload:
        return self.preview_uploaded_bom(
            header_data=header_data,
            upload_data=upload_data,
        ).payload

    def _build_metadata(self, header_data: Mapping[str, object]) -> BomIntakeMetadata:
        _reject_extra_fields(header_data, HEADER_REQUEST_FIELDS, "header")
        try:
            return BomIntakeMetadata(
                customer_name=_required_string(header_data, "customer_name"),
                source_file_name=_required_string(header_data, "source_file_name"),
                uploaded_by=_required_string(header_data, "uploaded_by"),
                quote_number=_optional_string(header_data, "quote_number"),
                source_file_path=_optional_string(header_data, "source_file_path"),
                source_sheet_name=_optional_string(header_data, "source_sheet_name"),
                source_type=_optional_string(header_data, "source_type"),
                parser_version=_optional_string(header_data, "parser_version"),
                intake_notes=_optional_string(header_data, "intake_notes"),
            )
        except (KeyError, TypeError) as exc:
            raise BomIntakeRequestError(f"Invalid header payload: {exc}") from exc

    def _build_metadata_for_upload(
        self,
        header_data: Mapping[str, object],
        upload_data: Mapping[str, object],
    ) -> BomIntakeMetadata:
        _reject_extra_fields(header_data, HEADER_REQUEST_FIELDS, "header")
        _reject_extra_fields(upload_data, UPLOAD_REQUEST_FIELDS, "upload")

        upload_filename = _optional_string(upload_data, "filename")
        upload_source_path = _optional_string(upload_data, "source_file_path")
        inferred_source_filename = upload_filename
        if inferred_source_filename is None and upload_source_path:
            inferred_source_filename = Path(upload_source_path).name

        source_file_name = _optional_string(header_data, "source_file_name") or inferred_source_filename
        if not source_file_name:
            raise BomIntakeRequestError(
                "header.source_file_name is required when upload filename cannot be inferred."
            )

        try:
            return BomIntakeMetadata(
                customer_name=_required_string(header_data, "customer_name"),
                source_file_name=source_file_name,
                uploaded_by=_required_string(header_data, "uploaded_by"),
                quote_number=_optional_string(header_data, "quote_number"),
                source_file_path=_optional_string(header_data, "source_file_path")
                or upload_source_path,
                source_sheet_name=_optional_string(header_data, "source_sheet_name"),
                source_type=_optional_string(header_data, "source_type"),
                parser_version=_optional_string(header_data, "parser_version"),
                intake_notes=_optional_string(header_data, "intake_notes"),
            )
        except (KeyError, TypeError) as exc:
            raise BomIntakeRequestError(f"Invalid header payload: {exc}") from exc

    def _build_standardized_rows(
        self,
        standardized_rows_data: Sequence[Mapping[str, object]],
    ) -> list[StandardizedBomRow]:
        standardized_rows: list[StandardizedBomRow] = []

        for index, row_data in enumerate(standardized_rows_data, start=1):
            if not isinstance(row_data, Mapping):
                raise BomIntakeRequestError(
                    f"standardizedBomRows[{index - 1}] must be an object."
                )
            _reject_extra_fields(
                row_data,
                STANDARDIZED_ROW_REQUEST_FIELDS,
                f"standardizedBomRows[{index - 1}]",
            )

            try:
                standardized_rows.append(
                    StandardizedBomRow(
                        source_row_number=_required_int(row_data, "source_row_number"),
                        original_value=_optional_string(row_data, "original_value"),
                        parent_part=_optional_string(row_data, "parent_part"),
                        part_number=_required_string(row_data, "part_number"),
                        indented_part_number=_required_string(
                            row_data,
                            "indented_part_number",
                        ),
                        bom_level=_required_int(row_data, "bom_level"),
                        description=_optional_string(row_data, "description"),
                        revision=_optional_string(row_data, "revision"),
                        quantity=_optional_number(row_data, "quantity"),
                        uom=_optional_string(row_data, "uom"),
                        item_number=_optional_string(row_data, "item_number"),
                        make_buy=_optional_string(row_data, "make_buy"),
                        mfr=_optional_string(row_data, "mfr"),
                        mfr_number=_optional_string(row_data, "mfr_number"),
                        lead_time_days=_optional_number(row_data, "lead_time_days"),
                        cost=_optional_number(row_data, "cost"),
                        validation_message=_optional_string(
                            row_data,
                            "validation_message",
                        ),
                    )
                )
            except (KeyError, TypeError, BomIntakePayloadError) as exc:
                raise BomIntakeRequestError(
                    f"Invalid standardized BOM row at index {index - 1}: {exc}"
                ) from exc

        return standardized_rows

    def _write_preview_json(self, preview_path: Path, payload: dict[str, object]) -> None:
        preview_path.parent.mkdir(parents=True, exist_ok=True)
        preview_path.write_text(
            json.dumps(payload, indent=2) + "\n",
            encoding="utf-8",
        )

    def _build_upload_file(
        self,
        upload_data: Mapping[str, object],
    ) -> dict[str, object]:
        source_file_path = _optional_string(upload_data, "source_file_path")
        filename = _optional_string(upload_data, "filename")
        content_base64 = _optional_string(upload_data, "content_base64")
        content = upload_data.get("content")

        provided_sources = [
            source_name
            for source_name, value in (
                ("source_file_path", source_file_path),
                ("content_base64", content_base64),
                ("content", content),
            )
            if value is not None
        ]

        if len(provided_sources) > 1:
            raise BomIntakeRequestError(
                "upload must provide exactly one of source_file_path, content_base64, or content."
            )
        if not provided_sources:
            raise BomIntakeRequestError(
                "upload must include source_file_path, content_base64, or content."
            )

        if source_file_path:
            source_path = Path(source_file_path)
            if not source_path.is_file():
                raise BomIntakeRequestError(
                    f"upload.source_file_path does not exist: {source_file_path}"
                )
            return {
                "filename": filename or source_path.name,
                "content": source_path.read_bytes(),
                "source_file_path": str(source_path),
            }

        if content is not None:
            if not isinstance(content, bytes):
                raise BomIntakeRequestError("upload.content must be raw bytes.")
            if filename is None:
                raise BomIntakeRequestError(
                    "upload.filename is required when using content."
                )
            return {
                "filename": filename,
                "content": content,
                "source_file_path": source_file_path,
            }

        if filename is None:
            raise BomIntakeRequestError(
                "upload.filename is required when using content_base64."
            )

        try:
            content = base64.b64decode(content_base64, validate=True)
        except ValueError as exc:
            raise BomIntakeRequestError(
                "upload.content_base64 must be valid base64."
            ) from exc

        return {
            "filename": filename,
            "content": content,
            "source_file_path": source_file_path,
        }


def _reject_extra_fields(
    payload: Mapping[str, object],
    allowed_fields: set[str],
    context: str,
) -> None:
    unknown_fields = sorted(set(payload.keys()) - allowed_fields)
    if unknown_fields:
        raise BomIntakeRequestError(
            f"{context} contains unknown fields: {', '.join(unknown_fields)}."
        )


def _required_string(payload: Mapping[str, object], field_name: str) -> str:
    value = payload[field_name]
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string.")
    return value


def _optional_string(payload: Mapping[str, object], field_name: str) -> str | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string when provided.")
    return value


def _required_int(payload: Mapping[str, object], field_name: str) -> int:
    value = payload[field_name]
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer.")
    return value


def _optional_number(payload: Mapping[str, object], field_name: str) -> int | float | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a number when provided.")
    return value
