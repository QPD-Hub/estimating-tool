from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO

from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException

REQUIRED_BOM_HEADERS = (
    "ORIGINAL",
    "Parent Part",
    "Part Number",
    "Indented Part Number",
    "Revision",
    "Description",
    "Quantity",
    "UOM",
    "Level",
    "Find No",
    "Release Status",
    "Critical Part",
    "Procurement Type",
    "Bulk Material",
)


class BomWorkbookError(ValueError):
    pass


@dataclass(frozen=True)
class BomIdentity:
    part_number: str
    revision: str

    @property
    def filename(self) -> str:
        return f"{self.part_number}_{self.revision}_BOM.xlsx"


def extract_bom_identity(workbook_content: bytes) -> BomIdentity:
    try:
        workbook = load_workbook(
            filename=BytesIO(workbook_content),
            read_only=True,
            data_only=True,
        )
    except (InvalidFileException, OSError, ValueError, KeyError, EOFError) as exc:
        raise BomWorkbookError("Workbook could not be read.") from exc
    except Exception as exc:
        raise BomWorkbookError("Workbook could not be read.") from exc

    try:
        if "BOM" not in workbook.sheetnames:
            raise BomWorkbookError("Worksheet 'BOM' is required.")

        worksheet = workbook["BOM"]
        header_map, header_row_index = _find_header_map(worksheet.iter_rows(values_only=True))

        for row in worksheet.iter_rows(
            min_row=header_row_index + 1,
            values_only=True,
        ):
            level_value = row[header_map["Level"]]
            if not _is_level_zero(level_value):
                continue

            part_number = _stringify_cell(row[header_map["Part Number"]])
            revision = _stringify_cell(row[header_map["Revision"]])

            if not part_number:
                raise BomWorkbookError(
                    "Level 0 row is missing a value for 'Part Number'."
                )
            if not revision:
                raise BomWorkbookError("Level 0 row is missing a value for 'Revision'.")

            return BomIdentity(part_number=part_number, revision=revision)

        raise BomWorkbookError("No level 0 row exists on worksheet 'BOM'.")
    finally:
        workbook.close()


def _find_header_map(rows) -> tuple[dict[str, int], int]:
    for index, row in enumerate(rows, start=1):
        header_map = {
            _stringify_cell(value): column_index
            for column_index, value in enumerate(row)
            if _stringify_cell(value)
        }
        missing_headers = [
            header for header in REQUIRED_BOM_HEADERS if header not in header_map
        ]
        if not missing_headers:
            return header_map, index

    raise BomWorkbookError(
        "Worksheet 'BOM' is missing one or more required columns: "
        f"{', '.join(REQUIRED_BOM_HEADERS)}"
    )


def _is_level_zero(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return value == 0
    if isinstance(value, str):
        return value.strip() == "0"
    return False


def _stringify_cell(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()
