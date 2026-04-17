import json
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

from src.contracts.bom_intake import ROOT_TVP_FIELDS, ROW_TVP_FIELDS
from src.services.bom_intake_service import BomIntakeRequestError, BomIntakeService
from src.services.bom_package_locator import BomPackageLocator, BomPackageLocatorError
from src.services.bom_payload_builder import BomPayloadBuildInput, BomPayloadBuilder
from src.services.bom_spreadsheet_parser import BomSpreadsheetParser, BomSpreadsheetParserError
from src.services.bom_standardizer import BomStandardizer


FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "bom_parser"


class FakeDbService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create_and_process_intake(self, *, payload):
        self.calls.append({"payload": payload})
        return {
            "Summary": {
                "BomIntakeId": 222,
                "DetectedRootCount": len(payload.roots),
                "AcceptedRootCount": len(payload.roots),
                "DuplicateRejectedCount": 0,
                "FinalIntakeStatus": "processed",
            },
            "RootResults": [],
        }


class BomUploadPipelineTests(unittest.TestCase):
    def test_locator_prefers_xlsx_candidate_inside_package(self) -> None:
        locator = BomPackageLocator()
        xlsx_content = b"xlsx-content"
        xls_content = b"xls-content"

        with tempfile.TemporaryDirectory() as temp_dir:
            package_path = Path(temp_dir) / "package.zip"
            with ZipFile(package_path, "w") as archive:
                archive.writestr("notes/readme.txt", "ignore")
                archive.writestr("docs/parts-list.xls", xls_content)
                archive.writestr("docs/customer-bom.xlsx", xlsx_content)

            located = locator.locate(
                filename=package_path.name,
                content=package_path.read_bytes(),
                source_file_path=str(package_path),
            )

        self.assertEqual(located.filename, "customer-bom.xlsx")
        self.assertEqual(located.content, xlsx_content)
        self.assertEqual(located.source_type, "archive_upload")
        self.assertIn("docs/customer-bom.xlsx", located.source_file_path)
        self.assertIsNotNone(located.diagnostics)
        self.assertEqual(
            located.diagnostics.selected_archive_member_name,
            "docs/customer-bom.xlsx",
        )
        self.assertEqual(
            [candidate.member_name for candidate in located.diagnostics.candidate_spreadsheets],
            ["docs/customer-bom.xlsx", "docs/parts-list.xls"],
        )

    def test_locator_fails_clearly_when_no_spreadsheet_exists(self) -> None:
        locator = BomPackageLocator()

        with tempfile.TemporaryDirectory() as temp_dir:
            package_path = Path(temp_dir) / "package.zip"
            with ZipFile(package_path, "w") as archive:
                archive.writestr("docs/readme.txt", "ignore")

            with self.assertRaises(BomPackageLocatorError):
                locator.locate(
                    filename=package_path.name,
                    content=package_path.read_bytes(),
                    source_file_path=str(package_path),
                )

    def test_parser_and_standardizer_emit_canonical_rows(self) -> None:
        parser_service = BomSpreadsheetParser()
        standardizer = BomStandardizer()

        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = _materialize_fixture_workbook(
                "single_root_workbook.json",
                Path(temp_dir),
            )
            parsed = parser_service.parse(
                filename=workbook_path.name,
                content=workbook_path.read_bytes(),
            )
            standardized = standardizer.standardize(parsed)

        self.assertEqual(parsed.sheet_name, "BOM")
        self.assertEqual(parsed.header_row_number, 3)
        self.assertEqual(len(standardized.rows), 3)
        self.assertEqual(standardized.rows[0].source_row_number, 4)
        self.assertEqual(standardized.rows[0].part_number, "ASM-1000")
        self.assertEqual(standardized.rows[1].parent_part, "ASM-1000")
        self.assertEqual(standardized.rows[2].parent_part, "COMP-200")
        self.assertIsNone(standardized.rows[1].revision)
        self.assertEqual(standardized.rows[1].quantity, 2)
        self.assertEqual(standardized.rows[1].cost, 1.25)

    def test_payload_builder_splits_multi_root_rows(self) -> None:
        parser_service = BomSpreadsheetParser()
        standardizer = BomStandardizer()
        payload_builder = BomPayloadBuilder()

        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = _materialize_fixture_workbook(
                "multi_root_workbook.json",
                Path(temp_dir),
            )
            parsed = parser_service.parse(
                filename=workbook_path.name,
                content=workbook_path.read_bytes(),
            )
            standardized = standardizer.standardize(parsed)
            payload = payload_builder.build(
                metadata=BomPayloadBuildInput(
                    customer_name="ACME",
                    uploaded_by="estimator",
                    source_file_name=workbook_path.name,
                    source_file_path=str(workbook_path),
                    source_sheet_name=parsed.sheet_name,
                    source_type="spreadsheet_upload",
                    parser_version="bom-parser-v1",
                ),
                standardized_rows=standardized.rows,
            )

        preview = payload.to_preview_dict()
        roots = preview["processStandardizedProc"]["roots"]
        rows = preview["processStandardizedProc"]["rows"]

        self.assertEqual(len(roots), 2)
        self.assertEqual([root["RootClientId"] for root in roots], ["R1", "R2"])
        self.assertEqual([root["RootSequence"] for root in roots], [1, 2])
        self.assertEqual(
            [(row["RootClientId"], row["RowSequence"]) for row in rows],
            [("R1", 1), ("R1", 2), ("R2", 1), ("R2", 2)],
        )

    def test_sql_bound_payload_contains_only_contract_fields(self) -> None:
        parser_service = BomSpreadsheetParser()
        standardizer = BomStandardizer()
        payload_builder = BomPayloadBuilder()

        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = _materialize_fixture_workbook(
                "single_root_workbook.json",
                Path(temp_dir),
            )
            parsed = parser_service.parse(
                filename=workbook_path.name,
                content=workbook_path.read_bytes(),
            )
            standardized = standardizer.standardize(parsed)
            payload = payload_builder.build(
                metadata=BomPayloadBuildInput(
                    customer_name="ACME",
                    uploaded_by="estimator",
                    source_file_name=workbook_path.name,
                    source_file_path=str(workbook_path),
                    source_sheet_name=parsed.sheet_name,
                    source_type="spreadsheet_upload",
                    parser_version="bom-parser-v1",
                ),
                standardized_rows=standardized.rows,
            )

        preview = payload.to_preview_dict()
        self.assertEqual(
            tuple(preview["processStandardizedProc"]["roots"][0].keys()),
            ROOT_TVP_FIELDS,
        )
        self.assertEqual(
            tuple(preview["processStandardizedProc"]["rows"][0].keys()),
            ROW_TVP_FIELDS,
        )

    def test_partial_standardized_customer_headers_preview_and_payload_build(self) -> None:
        parser_service = BomSpreadsheetParser()
        standardizer = BomStandardizer()
        payload_builder = BomPayloadBuilder()

        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = _materialize_fixture_workbook(
                "partial_standardized_customer_workbook.json",
                Path(temp_dir),
            )
            parsed = parser_service.parse(
                filename=workbook_path.name,
                content=workbook_path.read_bytes(),
            )
            standardized = standardizer.standardize(parsed)
            payload = payload_builder.build(
                metadata=BomPayloadBuildInput(
                    customer_name="ACME",
                    uploaded_by="estimator",
                    source_file_name=workbook_path.name,
                    source_file_path=str(workbook_path),
                    source_sheet_name=parsed.sheet_name,
                    source_type="spreadsheet_upload",
                    parser_version="bom-parser-v1",
                ),
                standardized_rows=standardized.rows,
            )

        preview = payload.to_preview_dict()

        self.assertEqual(parsed.sheet_name, "Customer Export")
        self.assertEqual(parsed.header_row_number, 2)
        self.assertEqual(
            [column.header_name for column in parsed.columns],
            [
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
                "Procurement Type",
            ],
        )
        self.assertEqual(len(standardized.rows), 3)
        self.assertEqual(standardized.rows[0].source_row_number, 3)
        self.assertEqual(standardized.rows[1].part_number, "COMP-200")
        self.assertEqual(standardized.rows[1].indented_part_number, "COMP-200")
        self.assertEqual(standardized.rows[1].parent_part, "ASM-1000")
        self.assertEqual(standardized.rows[1].make_buy, "BUY")
        self.assertEqual(preview["processStandardizedProc"]["rows"][1]["PartNumber"], "COMP-200")
        self.assertEqual(preview["processStandardizedProc"]["rows"][1]["ParentPart"], "ASM-1000")
        self.assertEqual(preview["processStandardizedProc"]["rows"][1]["MakeBuy"], "BUY")

    def test_parser_accepts_minimum_required_bom_columns(self) -> None:
        parser_service = BomSpreadsheetParser()
        standardizer = BomStandardizer()

        with tempfile.TemporaryDirectory() as temp_dir:
            target_path = _write_inline_workbook(
                Path(temp_dir) / "minimum-bom.xlsx",
                [
                    {
                        "name": "BOM",
                        "rows": [
                            ["Part Number", "Revision", "Quantity", "Level"],
                            ["ASM-1000", "A", 1, 0],
                            ["COMP-200", "", 2, 1],
                        ],
                    }
                ],
            )
            parsed = parser_service.parse(
                filename=target_path.name,
                content=target_path.read_bytes(),
            )
            standardized = standardizer.standardize(parsed)

        self.assertEqual(parsed.sheet_name, "BOM")
        self.assertEqual([column.header_name for column in parsed.columns], ["Part Number", "Revision", "Quantity", "Level"])
        self.assertEqual(len(standardized.rows), 2)
        self.assertIsNone(standardized.rows[0].description)
        self.assertIsNone(standardized.rows[0].uom)
        self.assertEqual(standardized.rows[1].parent_part, "ASM-1000")
        self.assertIsNone(standardized.rows[1].item_number)
        self.assertEqual(standardized.rows[1].part_number, "COMP-200")

    def test_parser_accepts_required_header_variants(self) -> None:
        parser_service = BomSpreadsheetParser()

        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = _write_inline_workbook(
                Path(temp_dir) / "header-variants.xlsx",
                [
                    {
                        "name": "Customer Export",
                        "rows": [
                            ["Part #", "REV", "QTY", "Lvl"],
                            ["ASM-1000", "A", 1, 0],
                        ],
                    }
                ],
            )
            parsed = parser_service.parse(
                filename=workbook_path.name,
                content=workbook_path.read_bytes(),
            )

        self.assertEqual(
            [column.canonical_field for column in parsed.columns],
            ["part_number", "revision", "quantity", "bom_level"],
        )

    def test_parser_missing_required_columns_lists_only_missing_minimum_fields(self) -> None:
        parser_service = BomSpreadsheetParser()

        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = _write_inline_workbook(
                Path(temp_dir) / "missing-required.xlsx",
                [
                    {
                        "name": "BOM",
                        "rows": [
                            ["Part Number", "Revision", "Description", "Level"],
                            ["ASM-1000", "A", "Assembly", 0],
                        ],
                    }
                ],
            )
            with self.assertRaises(BomSpreadsheetParserError) as context:
                parser_service.parse(
                    filename=workbook_path.name,
                    content=workbook_path.read_bytes(),
                )

        self.assertEqual(
            str(context.exception),
            "Worksheet 'BOM' is missing required columns: Quantity",
        )

    def test_service_preview_keeps_missing_optional_fields_blank(self) -> None:
        db_service = FakeDbService()
        service = BomIntakeService(db_service=db_service)

        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = _write_inline_workbook(
                Path(temp_dir) / "minimum-preview.xlsx",
                [
                    {
                        "name": "BOM",
                        "rows": [
                            ["PartNumber", "Revision", "Qty", "Level"],
                            ["ASM-1000", "A", 1, 0],
                            ["COMP-200", "", 2, 1],
                        ],
                    }
                ],
            )
            preview = service.preview_uploaded_bom(
                header_data={
                    "customer_name": "ACME",
                    "uploaded_by": "estimator",
                    "source_file_name": workbook_path.name,
                },
                upload_data={
                    "filename": workbook_path.name,
                    "content": workbook_path.read_bytes(),
                },
            )

        preview_dict = preview.to_dict()
        self.assertEqual(preview.root_count, 1)
        self.assertEqual(preview.row_count, 2)
        self.assertIsNone(preview.standardized_rows[0].description)
        self.assertIsNone(preview_dict["bomRowsTvpRows"][0]["Description"])
        self.assertIsNone(preview_dict["bomRowsTvpRows"][0]["UOM"])
        self.assertEqual(preview_dict["bomRowsTvpRows"][1]["ParentPart"], "ASM-1000")
        self.assertIsNone(preview_dict["bomRowsTvpRows"][1]["ItemNumber"])

    def test_service_dry_run_builds_payload_from_upload_path(self) -> None:
        db_service = FakeDbService()
        service = BomIntakeService(db_service=db_service)

        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = _materialize_fixture_workbook(
                "single_root_workbook.json",
                Path(temp_dir),
            )
            preview_path = Path(temp_dir) / "preview.json"
            result = service.process_uploaded_bom(
                header_data={
                    "customer_name": "ACME",
                    "uploaded_by": "estimator",
                    "source_file_name": workbook_path.name,
                },
                upload_data={
                    "source_file_path": str(workbook_path),
                },
                dry_run=True,
                preview_path=preview_path,
            )

            preview = json.loads(preview_path.read_text(encoding="utf-8"))

        self.assertTrue(result["DryRun"])
        self.assertEqual(result["Payload"], preview)
        self.assertEqual(preview["createProc"]["params"]["SourceSheetName"], "BOM")
        self.assertEqual(preview["createProc"]["params"]["SourceType"], "spreadsheet_upload")
        self.assertEqual(preview["processStandardizedProc"]["params"]["BomIntakeId"], None)
        self.assertEqual(db_service.calls, [])

    def test_service_preview_builds_from_raw_upload_bytes(self) -> None:
        db_service = FakeDbService()
        service = BomIntakeService(db_service=db_service)

        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = _materialize_fixture_workbook(
                "single_root_workbook.json",
                Path(temp_dir),
            )
            preview = service.preview_uploaded_bom(
                header_data={
                    "customer_name": "ACME",
                    "uploaded_by": "estimator",
                    "source_file_name": workbook_path.name,
                },
                upload_data={
                    "filename": workbook_path.name,
                    "content": workbook_path.read_bytes(),
                },
            )

        self.assertEqual(preview.selected_file_name, workbook_path.name)
        self.assertEqual(preview.detected_worksheet, "BOM")
        self.assertEqual(preview.detected_source_type, "spreadsheet_upload")
        self.assertEqual(preview.root_count, 1)
        self.assertEqual(preview.row_count, 3)
        self.assertEqual(db_service.calls, [])

    def test_service_preview_accepts_partial_standardized_customer_headers(self) -> None:
        db_service = FakeDbService()
        service = BomIntakeService(db_service=db_service)

        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = _materialize_fixture_workbook(
                "partial_standardized_customer_workbook.json",
                Path(temp_dir),
            )
            preview = service.preview_uploaded_bom(
                header_data={
                    "customer_name": "ACME",
                    "uploaded_by": "estimator",
                    "source_file_name": workbook_path.name,
                },
                upload_data={
                    "filename": workbook_path.name,
                    "content": workbook_path.read_bytes(),
                },
            )

        preview_dict = preview.to_dict()

        self.assertEqual(preview.detected_worksheet, "Customer Export")
        self.assertEqual(preview.root_count, 1)
        self.assertEqual(preview.row_count, 3)
        self.assertEqual(preview.standardized_rows[1].source_row_number, 4)
        self.assertEqual(preview.standardized_rows[1].part_number, "COMP-200")
        self.assertEqual(preview_dict["bomRowsTvpRows"][1]["PartNumber"], "COMP-200")
        self.assertEqual(preview_dict["bomRowsTvpRows"][1]["IndentedPartNumber"], "COMP-200")
        self.assertEqual(db_service.calls, [])

    def test_service_preview_zip_diagnostics_show_selected_spreadsheet(self) -> None:
        db_service = FakeDbService()
        service = BomIntakeService(db_service=db_service)

        with tempfile.TemporaryDirectory() as temp_dir:
            bom_workbook = _materialize_fixture_workbook(
                "partial_standardized_customer_workbook.json",
                Path(temp_dir),
            )
            non_bom_workbook = _materialize_fixture_workbook(
                "non_bom_workbook.json",
                Path(temp_dir),
            )
            package_path = Path(temp_dir) / "mixed-package.zip"
            with ZipFile(package_path, "w") as archive:
                archive.writestr("docs/readme.txt", "ignore")
                archive.writestr(
                    "docs/reference.xlsx",
                    non_bom_workbook.read_bytes(),
                )
                archive.writestr(
                    "docs/customer-bom.xlsx",
                    bom_workbook.read_bytes(),
                )

            preview = service.preview_uploaded_bom(
                header_data={
                    "customer_name": "ACME",
                    "uploaded_by": "estimator",
                    "source_file_name": package_path.name,
                },
                upload_data={
                    "filename": package_path.name,
                    "content": package_path.read_bytes(),
                },
            )

        diagnostics = preview.to_dict()["diagnostics"]
        self.assertEqual(diagnostics["selectedArchiveMemberName"], "docs/customer-bom.xlsx")
        self.assertEqual(
            [candidate["memberName"] for candidate in diagnostics["candidateSpreadsheets"]],
            ["docs/customer-bom.xlsx", "docs/reference.xlsx"],
        )
        self.assertTrue(diagnostics["candidateSpreadsheets"][0]["selected"])
        self.assertIn("highest-ranked spreadsheet candidate", diagnostics["archiveSelection"]["selectionReason"])
        self.assertEqual(preview.detected_worksheet, "Customer Export")

    def test_service_preview_failure_returns_header_diagnostics(self) -> None:
        db_service = FakeDbService()
        service = BomIntakeService(db_service=db_service)

        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = _materialize_fixture_workbook(
                "non_bom_workbook.json",
                Path(temp_dir),
            )
            with self.assertRaises(BomIntakeRequestError) as context:
                service.preview_uploaded_bom(
                    header_data={
                        "customer_name": "ACME",
                        "uploaded_by": "estimator",
                        "source_file_name": workbook_path.name,
                    },
                    upload_data={
                        "filename": workbook_path.name,
                        "content": workbook_path.read_bytes(),
                    },
                )

        diagnostics = context.exception.diagnostics
        self.assertEqual(str(context.exception), "No BOM worksheet/header could be detected.")
        self.assertIsNotNone(diagnostics)
        self.assertEqual(diagnostics["selectedSourceFileName"], workbook_path.name)
        self.assertEqual(diagnostics["worksheetNames"], ["Reference"])
        self.assertEqual(diagnostics["selectedWorksheetName"], "Reference")
        self.assertGreater(len(diagnostics["firstRowsPreview"]), 0)
        self.assertEqual(diagnostics["headerRowCandidates"], [])


def _materialize_fixture_workbook(fixture_name: str, target_dir: Path) -> Path:
    fixture = json.loads((FIXTURE_ROOT / fixture_name).read_text(encoding="utf-8"))
    workbook_path = target_dir / fixture_name.replace(".json", ".xlsx")
    _write_minimal_xlsx(workbook_path, fixture["sheets"])
    return workbook_path


def _write_inline_workbook(workbook_path: Path, sheets: list[dict[str, object]]) -> Path:
    _write_minimal_xlsx(workbook_path, sheets)
    return workbook_path


def _write_minimal_xlsx(workbook_path: Path, sheets: list[dict[str, object]]) -> None:
    workbook_xml = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>',
    ]
    workbook_rels_xml = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">',
    ]
    content_types_xml = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">',
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
        '<Default Extension="xml" ContentType="application/xml"/>',
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
    ]

    workbook_xml_parts: list[tuple[str, str]] = []
    for index, sheet in enumerate(sheets, start=1):
        sheet_name = sheet["name"]
        workbook_xml.append(
            f'<sheet name="{sheet_name}" sheetId="{index}" r:id="rId{index}"/>'
        )
        workbook_rels_xml.append(
            f'<Relationship Id="rId{index}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            f'Target="worksheets/sheet{index}.xml"/>'
        )
        content_types_xml.append(
            f'<Override PartName="/xl/worksheets/sheet{index}.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        )
        workbook_xml_parts.append(
            (f"xl/worksheets/sheet{index}.xml", _build_sheet_xml(sheet["rows"]))
        )

    workbook_xml.append("</sheets></workbook>")
    workbook_rels_xml.append("</Relationships>")
    content_types_xml.append("</Types>")

    root_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        "</Relationships>"
    )

    with ZipFile(workbook_path, "w") as archive:
        archive.writestr("[Content_Types].xml", "".join(content_types_xml))
        archive.writestr("_rels/.rels", root_rels_xml)
        archive.writestr("xl/workbook.xml", "".join(workbook_xml))
        archive.writestr("xl/_rels/workbook.xml.rels", "".join(workbook_rels_xml))
        for path, xml_content in workbook_xml_parts:
            archive.writestr(path, xml_content)


def _build_sheet_xml(rows: list[list[object]]) -> str:
    xml_parts = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>',
    ]

    for row_index, row in enumerate(rows, start=1):
        xml_parts.append(f'<row r="{row_index}">')
        for column_index, value in enumerate(row, start=1):
            if value is None:
                continue
            cell_reference = f"{_column_name(column_index)}{row_index}"
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                xml_parts.append(f'<c r="{cell_reference}"><v>{value}</v></c>')
                continue
            escaped = (
                str(value)
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )
            xml_parts.append(
                f'<c r="{cell_reference}" t="inlineStr"><is><t>{escaped}</t></is></c>'
            )
        xml_parts.append("</row>")

    xml_parts.append("</sheetData></worksheet>")
    return "".join(xml_parts)


def _column_name(index: int) -> str:
    value = index
    characters: list[str] = []
    while value:
        value, remainder = divmod(value - 1, 26)
        characters.append(chr(ord("A") + remainder))
    return "".join(reversed(characters))


if __name__ == "__main__":
    unittest.main()
