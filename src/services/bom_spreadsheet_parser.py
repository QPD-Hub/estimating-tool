from __future__ import annotations

import re
from dataclasses import dataclass
from io import BytesIO
from typing import Any
from xml.etree import ElementTree
from zipfile import BadZipFile, ZipFile


CANONICAL_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "original_value": ("original", "original value"),
    "parent_part": ("parent part", "parent assembly", "parent"),
    "part_number": ("part number", "part no", "part #", "pn"),
    "indented_part_number": (
        "indented part number",
        "indented part no",
        "indented part",
        "indented pn",
    ),
    "bom_level": ("level", "bom level", "indent level", "lvl"),
    "description": ("description", "part description", "desc"),
    "revision": ("revision", "rev"),
    "quantity": ("quantity", "qty"),
    "uom": ("uom", "unit", "unit of measure"),
    "item_number": ("item number", "item", "find no", "find number"),
    "make_buy": ("make buy", "make/buy", "procurement type", "buy make"),
    "mfr": ("mfr", "manufacturer"),
    "mfr_number": (
        "mfr number",
        "manufacturer part number",
        "manufacturer number",
        "mpn",
    ),
    "lead_time_days": ("lead time days", "lead time", "lt days"),
    "cost": ("cost", "unit cost", "material cost"),
}

WORKSHEET_NAME_KEYWORDS = ("bom", "bill", "parts", "assembly")


class BomSpreadsheetParserError(ValueError):
    pass


@dataclass(frozen=True)
class ParsedBomColumn:
    canonical_field: str
    header_name: str
    column_index: int


@dataclass(frozen=True)
class ParsedBomRow:
    source_row_number: int
    values: dict[str, object]


@dataclass(frozen=True)
class ParsedBomSpreadsheet:
    sheet_name: str
    header_row_number: int
    columns: list[ParsedBomColumn]
    rows: list[ParsedBomRow]


class BomSpreadsheetParser:
    def parse(
        self,
        *,
        filename: str,
        content: bytes,
    ) -> ParsedBomSpreadsheet:
        suffix = _suffix(filename)
        if suffix == ".xlsx":
            return self._parse_xlsx(content)
        if suffix == ".xls":
            return self._parse_xls(content)
        raise BomSpreadsheetParserError(
            f"Unsupported spreadsheet type '{suffix or filename}'."
        )

    def _parse_xlsx(self, content: bytes) -> ParsedBomSpreadsheet:
        try:
            worksheets = _load_xlsx_worksheets(content)
        except (BadZipFile, OSError, ValueError, KeyError, EOFError, ElementTree.ParseError) as exc:
            raise BomSpreadsheetParserError("Workbook could not be read.") from exc
        except Exception as exc:
            raise BomSpreadsheetParserError("Workbook could not be read.") from exc

        return _select_and_parse_sheet(worksheets)

    def _parse_xls(self, content: bytes) -> ParsedBomSpreadsheet:
        try:
            import xlrd
        except ModuleNotFoundError as exc:
            raise BomSpreadsheetParserError(
                "xlrd is required to read .xls BOM spreadsheets."
            ) from exc

        try:
            workbook = xlrd.open_workbook(file_contents=content)
        except (xlrd.XLRDError, OSError, ValueError, EOFError) as exc:
            raise BomSpreadsheetParserError("Workbook could not be read.") from exc
        except Exception as exc:
            raise BomSpreadsheetParserError("Workbook could not be read.") from exc

        worksheets: list[tuple[str, list[tuple[object, ...]]]] = []
        for index in range(workbook.nsheets):
            worksheet = workbook.sheet_by_index(index)
            rows = [
                tuple(_normalize_xls_value(value) for value in worksheet.row_values(row_index))
                for row_index in range(worksheet.nrows)
            ]
            worksheets.append((worksheet.name, rows))

        return _select_and_parse_sheet(worksheets)


def _select_and_parse_sheet(
    worksheets: list[tuple[str, list[tuple[object, ...]]]],
) -> ParsedBomSpreadsheet:
    best_candidate: tuple[int, int, str, int, dict[str, int], list[tuple[object, ...]]] | None = None

    for sheet_name, rows in worksheets:
        if not rows:
            continue

        header_row_number, column_map, score = _detect_header_row(rows)
        name_score = sum(
            3 for keyword in WORKSHEET_NAME_KEYWORDS if keyword in sheet_name.lower()
        )
        candidate = (score + name_score, name_score, sheet_name, header_row_number, column_map, rows)

        if best_candidate is None or candidate[:2] > best_candidate[:2]:
            best_candidate = candidate

    if best_candidate is None or best_candidate[0] <= 0:
        raise BomSpreadsheetParserError("No BOM worksheet/header could be detected.")

    _score, _name_score, sheet_name, header_row_number, column_map, rows = best_candidate
    parsed_columns = [
        ParsedBomColumn(
            canonical_field=canonical_field,
            header_name=_stringify_cell(rows[header_row_number - 1][column_index]),
            column_index=column_index,
        )
        for canonical_field, column_index in sorted(column_map.items(), key=lambda item: item[1])
    ]
    parsed_rows: list[ParsedBomRow] = []

    for row_number in range(header_row_number + 1, len(rows) + 1):
        row = rows[row_number - 1]
        values = {
            canonical_field: row[column_index] if column_index < len(row) else None
            for canonical_field, column_index in column_map.items()
        }
        if not any(_stringify_cell(value) for value in values.values()):
            continue
        parsed_rows.append(
            ParsedBomRow(
                source_row_number=row_number,
                values=values,
            )
        )

    if not parsed_rows:
        raise BomSpreadsheetParserError(
            f"Worksheet '{sheet_name}' did not contain any BOM data rows."
        )

    return ParsedBomSpreadsheet(
        sheet_name=sheet_name,
        header_row_number=header_row_number,
        columns=parsed_columns,
        rows=parsed_rows,
    )


def _detect_header_row(
    rows: list[tuple[object, ...]],
) -> tuple[int, dict[str, int], int]:
    best: tuple[int, dict[str, int], int] | None = None

    for row_number, row in enumerate(rows[:25], start=1):
        header_map: dict[str, int] = {}
        score = 0

        for column_index, value in enumerate(row):
            normalized_value = _normalize_header_name(value)
            if not normalized_value:
                continue

            for canonical_field, aliases in CANONICAL_FIELD_ALIASES.items():
                if normalized_value in aliases and canonical_field not in header_map:
                    header_map[canonical_field] = column_index
                    score += 2 if canonical_field in {"part_number", "bom_level", "description"} else 1
                    break

        if "bom_level" not in header_map:
            continue
        if "description" not in header_map:
            continue
        if "part_number" not in header_map and "indented_part_number" not in header_map:
            continue

        if best is None or score > best[2]:
            best = (row_number, header_map, score)

    if best is None:
        raise BomSpreadsheetParserError("No BOM header row could be detected.")

    return best


def _normalize_header_name(value: object) -> str:
    raw_value = _stringify_cell(value).lower()
    raw_value = re.sub(r"[^a-z0-9]+", " ", raw_value)
    return " ".join(raw_value.split())


def _normalize_xls_value(value: object) -> object:
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _stringify_cell(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _suffix(filename: str) -> str:
    return ("." + filename.rsplit(".", 1)[-1].lower()) if "." in filename else ""


def _load_xlsx_worksheets(content: bytes) -> list[tuple[str, list[tuple[object, ...]]]]:
    namespace = {
        "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
        "pkgrel": "http://schemas.openxmlformats.org/package/2006/relationships",
    }

    worksheets: list[tuple[str, list[tuple[object, ...]]]] = []
    with ZipFile(BytesIO(content)) as archive:
        shared_strings = _load_shared_strings(archive)
        workbook_xml = ElementTree.fromstring(archive.read("xl/workbook.xml"))
        rels_xml = ElementTree.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        relationship_targets = {
            rel.attrib["Id"]: rel.attrib["Target"]
            for rel in rels_xml.findall("pkgrel:Relationship", namespace)
        }

        for sheet in workbook_xml.findall("main:sheets/main:sheet", namespace):
            relationship_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            if relationship_id is None or relationship_id not in relationship_targets:
                continue
            target = relationship_targets[relationship_id].lstrip("/")
            if not target.startswith("xl/"):
                target = f"xl/{target}"
            sheet_rows = _load_xlsx_sheet_rows(
                archive.read(target),
                shared_strings=shared_strings,
            )
            worksheets.append((sheet.attrib.get("name", "Sheet"), sheet_rows))

    return worksheets


def _load_shared_strings(archive: ZipFile) -> list[str]:
    try:
        shared_strings_xml = archive.read("xl/sharedStrings.xml")
    except KeyError:
        return []

    namespace = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    root = ElementTree.fromstring(shared_strings_xml)
    return [
        "".join(text_node.text or "" for text_node in item.findall(".//main:t", namespace))
        for item in root.findall("main:si", namespace)
    ]


def _load_xlsx_sheet_rows(sheet_xml: bytes, *, shared_strings: list[str]) -> list[tuple[object, ...]]:
    namespace = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    root = ElementTree.fromstring(sheet_xml)
    parsed_rows: list[tuple[object, ...]] = []

    for row in root.findall("main:sheetData/main:row", namespace):
        parsed_cells: dict[int, object] = {}
        max_index = -1

        for cell in row.findall("main:c", namespace):
            reference = cell.attrib.get("r", "")
            column_index = _column_reference_to_index(reference)
            max_index = max(max_index, column_index)
            cell_type = cell.attrib.get("t")
            parsed_cells[column_index] = _parse_xlsx_cell_value(
                cell,
                cell_type=cell_type,
                shared_strings=shared_strings,
                namespace=namespace,
            )

        if max_index < 0:
            parsed_rows.append(tuple())
            continue

        parsed_rows.append(
            tuple(parsed_cells.get(index) for index in range(max_index + 1))
        )

    return parsed_rows


def _parse_xlsx_cell_value(
    cell: ElementTree.Element,
    *,
    cell_type: str | None,
    shared_strings: list[str],
    namespace: dict[str, str],
) -> object:
    if cell_type == "inlineStr":
        return "".join(text_node.text or "" for text_node in cell.findall(".//main:t", namespace))

    value_node = cell.find("main:v", namespace)
    if value_node is None or value_node.text is None:
        return None

    raw_value = value_node.text
    if cell_type == "s":
        try:
            return shared_strings[int(raw_value)]
        except (ValueError, IndexError):
            return raw_value

    if cell_type == "b":
        return raw_value == "1"

    try:
        numeric_value = float(raw_value)
    except ValueError:
        return raw_value

    return int(numeric_value) if numeric_value.is_integer() else numeric_value


def _column_reference_to_index(reference: str) -> int:
    letters = "".join(character for character in reference if character.isalpha()).upper()
    index = 0
    for character in letters:
        index = index * 26 + (ord(character) - ord("A") + 1)
    return max(index - 1, 0)
