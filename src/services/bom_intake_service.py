from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence

from src.services.bom_intake_db import BomIntakeDbService
from src.services.bom_intake_payload import (
    BomIntakeMetadata,
    BomIntakePayloadError,
    StandardizedBomRow,
    build_bom_intake_payload,
)

logger = logging.getLogger(__name__)


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
    ) -> dict[str, object]:
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

        payload = build_bom_intake_payload(
            metadata=metadata,
            standardized_rows=standardized_rows,
        )
        sql_payload = payload.to_sql_payload()

        logger.info(
            "Dispatching standardized BOM upload for customer '%s' with %s roots and %s rows.",
            metadata.customer_name,
            len(sql_payload["RootCandidates"]),
            len(sql_payload["BomRows"]),
        )

        return self._db_service.create_and_process_intake(
            header=sql_payload["Header"],
            root_candidates=sql_payload["RootCandidates"],
            bom_rows=sql_payload["BomRows"],
            detected_by=metadata.uploaded_by,
        )

    def _build_metadata(self, header_data: Mapping[str, object]) -> BomIntakeMetadata:
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
        except BomIntakePayloadError as exc:
            raise BomIntakeRequestError(str(exc)) from exc

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
                        quantity=row_data.get("quantity"),
                        uom=_optional_string(row_data, "uom"),
                        item_number=_optional_string(row_data, "item_number"),
                        make_buy=_optional_string(row_data, "make_buy"),
                        mfr=_optional_string(row_data, "mfr"),
                        mfr_number=_optional_string(row_data, "mfr_number"),
                        lead_time_days=_optional_int(row_data, "lead_time_days"),
                        cost=row_data.get("cost"),
                        validation_message=_optional_string(
                            row_data,
                            "validation_message",
                        ),
                        is_level_0=_optional_bool(row_data, "is_level_0"),
                    )
                )
            except (KeyError, TypeError) as exc:
                raise BomIntakeRequestError(
                    f"Invalid standardized BOM row at index {index - 1}: {exc}"
                ) from exc
            except BomIntakePayloadError as exc:
                raise BomIntakeRequestError(str(exc)) from exc

        return standardized_rows


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


def _optional_int(payload: Mapping[str, object], field_name: str) -> int | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer when provided.")
    return value


def _optional_bool(payload: Mapping[str, object], field_name: str) -> bool | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise TypeError(f"{field_name} must be a boolean when provided.")
    return value
