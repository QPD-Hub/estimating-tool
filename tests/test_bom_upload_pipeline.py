import json
import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

from src.contracts.bom_intake import ROOT_TVP_FIELDS, ROW_TVP_FIELDS
from src.services.bom_intake_service import BomIntakeService
from src.services.bom_package_locator import BomPackageLocator, BomPackageLocatorError
from src.services.bom_payload_builder import BomPayloadBuildInput, BomPayloadBuilder
from src.services.bom_spreadsheet_parser import BomSpreadsheetParser
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


def _materialize_fixture_workbook(fixture_name: str, target_dir: Path) -> Path:
    fixture = json.loads((FIXTURE_ROOT / fixture_name).read_text(encoding="utf-8"))
    workbook_path = target_dir / fixture_name.replace(".json", ".xlsx")
    _write_minimal_xlsx(workbook_path, fixture["sheets"])
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
