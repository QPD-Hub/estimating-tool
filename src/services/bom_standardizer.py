from __future__ import annotations

from dataclasses import dataclass

from src.services.bom_intake_payload import StandardizedBomRow
from src.services.bom_spreadsheet_parser import ParsedBomSpreadsheet


class BomStandardizerError(ValueError):
    pass


@dataclass(frozen=True)
class StandardizedBomDocument:
    sheet_name: str
    rows: list[StandardizedBomRow]


class BomStandardizer:
    def standardize(self, parsed_spreadsheet: ParsedBomSpreadsheet) -> StandardizedBomDocument:
        standardized_rows: list[StandardizedBomRow] = []
        level_stack: dict[int, str] = {}

        for parsed_row in parsed_spreadsheet.rows:
            values = parsed_row.values
            bom_level = _coerce_required_int(values.get("bom_level"), "bom_level", parsed_row.source_row_number)
            part_number = _coerce_required_text(
                values.get("part_number") or values.get("indented_part_number"),
                "part_number",
                parsed_row.source_row_number,
            )
            indented_part_number = _coerce_optional_text(
                values.get("indented_part_number")
            ) or part_number
            description = _coerce_optional_text(values.get("description"))
            explicit_parent = _coerce_optional_text(values.get("parent_part"))
            parent_part = explicit_parent
            if parent_part is None and bom_level > 0:
                parent_part = _find_parent_part(level_stack, bom_level)

            original_value = _coerce_optional_text(values.get("original_value"))
            if original_value is None:
                original_value = _coerce_optional_text(
                    values.get("part_number") or values.get("indented_part_number")
                )

            standardized_rows.append(
                StandardizedBomRow(
                    source_row_number=parsed_row.source_row_number,
                    original_value=original_value,
                    parent_part=parent_part,
                    part_number=part_number,
                    indented_part_number=indented_part_number,
                    bom_level=bom_level,
                    description=description,
                    revision=_coerce_optional_text(values.get("revision")),
                    quantity=_coerce_optional_number(values.get("quantity"), "quantity", parsed_row.source_row_number),
                    uom=_coerce_optional_text(values.get("uom")),
                    item_number=_coerce_optional_text(values.get("item_number")),
                    make_buy=_coerce_optional_text(values.get("make_buy")),
                    mfr=_coerce_optional_text(values.get("mfr")),
                    mfr_number=_coerce_optional_text(values.get("mfr_number")),
                    lead_time_days=_coerce_optional_number(
                        values.get("lead_time_days"),
                        "lead_time_days",
                        parsed_row.source_row_number,
                    ),
                    cost=_coerce_optional_number(values.get("cost"), "cost", parsed_row.source_row_number),
                )
            )

            level_stack[bom_level] = part_number
            for level in tuple(level_stack):
                if level > bom_level:
                    del level_stack[level]

        return StandardizedBomDocument(
            sheet_name=parsed_spreadsheet.sheet_name,
            rows=standardized_rows,
        )


def _find_parent_part(level_stack: dict[int, str], bom_level: int) -> str | None:
    for level in range(bom_level - 1, -1, -1):
        if level in level_stack:
            return level_stack[level]
    return None


def _coerce_required_text(value: object, field_name: str, source_row_number: int) -> str:
    normalized = _coerce_optional_text(value)
    if normalized is None:
        raise BomStandardizerError(
            f"Row {source_row_number} is missing required field '{field_name}'."
        )
    return normalized


def _coerce_optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _coerce_required_int(value: object, field_name: str, source_row_number: int) -> int:
    normalized = _coerce_optional_number(value, field_name, source_row_number)
    if normalized is None:
        raise BomStandardizerError(
            f"Row {source_row_number} is missing required field '{field_name}'."
        )
    if isinstance(normalized, float):
        if not normalized.is_integer():
            raise BomStandardizerError(
                f"Row {source_row_number} field '{field_name}' must be an integer."
            )
        return int(normalized)
    return int(normalized)


def _coerce_optional_number(
    value: object,
    field_name: str,
    source_row_number: int,
) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise BomStandardizerError(
            f"Row {source_row_number} field '{field_name}' must be numeric."
        )
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else value

    text = str(value).strip()
    if not text:
        return None

    try:
        numeric_value = float(text.replace(",", ""))
    except ValueError as exc:
        raise BomStandardizerError(
            f"Row {source_row_number} field '{field_name}' must be numeric."
        ) from exc

    return int(numeric_value) if numeric_value.is_integer() else numeric_value
