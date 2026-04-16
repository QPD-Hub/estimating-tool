from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from pathlib import Path

from src.services.bom_intake_db import BomIntakeDbService
from src.services.bom_intake_payload import (
    BomIntakeMetadata,
    BomIntakePayload,
    BomIntakePayloadError,
    StandardizedBomRow,
    build_bom_intake_payload,
)

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


class BomIntakeServiceError(ValueError):
    pass


class BomIntakeRequestError(BomIntakeServiceError):
    pass


class BomIntakeService:
    def __init__(self, db_service: BomIntakeDbService) -> None:
        self._db_service = db_service

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
            # Dry-run previews the exact contract-bound JSON the app would send to SQL.
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
                        description=_required_string(row_data, "description"),
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
