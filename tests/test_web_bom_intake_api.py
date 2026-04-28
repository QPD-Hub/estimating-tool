import io
import base64
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path

openpyxl_module = types.ModuleType("openpyxl")
openpyxl_module.load_workbook = lambda *args, **kwargs: None
openpyxl_utils_module = types.ModuleType("openpyxl.utils")
openpyxl_utils_exceptions_module = types.ModuleType("openpyxl.utils.exceptions")
openpyxl_utils_exceptions_module.InvalidFileException = ValueError
xlrd_module = types.ModuleType("xlrd")
xlrd_module.open_workbook = lambda *args, **kwargs: None
xlrd_module.XLRDError = ValueError
xlrd_module.biffh = types.SimpleNamespace(XLRDError=ValueError)

sys.modules.setdefault("openpyxl", openpyxl_module)
sys.modules.setdefault("openpyxl.utils", openpyxl_utils_module)
sys.modules.setdefault("openpyxl.utils.exceptions", openpyxl_utils_exceptions_module)
sys.modules.setdefault("xlrd", xlrd_module)

from src.config import AppConfig
from src.services.bom_intake_service import BomIntakeRequestError, BomIntakeService
from src.services.doc_package_intake_service import DocPackageIntakeResult
from src.services.document_intake_service import (
    DocumentIntakeResult,
)
from src.web import ViewState, create_app, render_page


class FakeBomIntakeService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def preview_uploaded_bom(self, *, header_data, upload_data):
        self.calls.append(
            {
                "header_data": header_data,
                "upload_data": upload_data,
                "mode": "preview",
            }
        )
        return types.SimpleNamespace(
            to_dict=lambda: {
                "selectedFileName": "bom.xlsx",
                "detectedWorksheet": "BOM",
                "detectedSourceType": "spreadsheet_upload",
                "sourceFilePath": None,
                "diagnostics": {
                    "selectedSourceFileName": "bom.xlsx",
                    "selectedArchiveMemberName": None,
                    "candidateSpreadsheets": [],
                    "archiveSelection": None,
                    "selectedWorksheetName": "BOM",
                    "worksheetNames": ["BOM"],
                    "firstRowsPreview": [["Part Number", "Description", "Level"]],
                    "headerRowCandidates": [{"rowNumber": 1, "score": 6}],
                    "worksheets": [],
                },
                "rootCount": 1,
                "rowCount": 2,
                "standardizedRows": [
                    {
                        "source_row_number": 4,
                        "original_value": "ABC-1000",
                        "parent_part": None,
                        "part_number": "ABC-1000",
                        "indented_part_number": "ABC-1000",
                        "bom_level": 0,
                        "description": "TOP",
                        "revision": "1",
                        "quantity": 1,
                        "uom": "EA",
                        "item_number": "10",
                        "make_buy": "MAKE",
                        "mfr": None,
                        "mfr_number": None,
                        "lead_time_days": None,
                        "cost": None,
                        "validation_message": None,
                    }
                ],
                "createProcParams": {"CustomerName": "ACME"},
                "processProcParams": {"BomIntakeId": None, "DetectedBy": "estimator"},
                "rootsTvpRows": [{"RootClientId": "R1"}],
                "bomRowsTvpRows": [{"RootClientId": "R1", "RowSequence": 1}],
                "createProc": {"params": {"CustomerName": "ACME"}},
                "processStandardizedProc": {
                    "params": {"BomIntakeId": None, "DetectedBy": "estimator"},
                    "roots": [{"RootClientId": "R1"}],
                    "rows": [{"RootClientId": "R1", "RowSequence": 1}],
                },
            }
        )

    def process_standardized_upload(self, header_data, standardized_rows_data, *, dry_run=False):
        self.calls.append(
            {
                "header_data": header_data,
                "standardized_rows_data": standardized_rows_data,
                "dry_run": dry_run,
            }
        )
        if dry_run:
            return {
                "DryRun": True,
                "PreviewPath": "/tmp/bom_intake_payload_preview.json",
                "Payload": {"createProc": {"params": {"CustomerName": "ACME"}}},
            }
        return {
            "Summary": {
                "BomIntakeId": 321,
                "DetectedRootCount": 1,
                "AcceptedRootCount": 1,
                "DuplicateRejectedCount": 0,
                "FinalIntakeStatus": "processed",
            },
            "RootResults": [],
        }

    def process_uploaded_bom(self, *, header_data, upload_data, dry_run=False):
        self.calls.append(
            {
                "header_data": header_data,
                "upload_data": upload_data,
                "dry_run": dry_run,
            }
        )
        return {
            "DryRun": dry_run,
            "PreviewPath": "/tmp/bom_intake_payload_preview.json" if dry_run else None,
            "Payload": {
                "createProc": {
                    "params": {
                        "CustomerName": "ACME",
                        "SourceFileName": "bom.xlsx",
                    }
                }
            },
        } if dry_run else {
            "Summary": {
                "BomIntakeId": 654,
                "DetectedRootCount": 1,
                "AcceptedRootCount": 1,
                "DuplicateRejectedCount": 0,
                "FinalIntakeStatus": "processed",
            },
            "RootResults": [],
        }


class FailingPreviewBomIntakeService(FakeBomIntakeService):
    def preview_uploaded_bom(self, *, header_data, upload_data):
        raise BomIntakeRequestError(
            "No BOM header row could be detected.",
            diagnostics={
                "selectedSourceFileName": "package.zip",
                "selectedArchiveMemberName": "docs/reference.xlsx",
                "candidateSpreadsheets": [
                    {
                        "filename": "reference.xlsx",
                        "memberName": "docs/reference.xlsx",
                        "score": 5,
                        "reasons": ["prefers .xlsx"],
                        "selected": True,
                    },
                    {
                        "filename": "notes.xlsx",
                        "memberName": "docs/notes.xlsx",
                        "score": 5,
                        "reasons": ["prefers .xlsx"],
                        "selected": False,
                    },
                ],
                "archiveSelection": {
                    "selectedSpreadsheetFilename": "reference.xlsx",
                    "selectionReason": "Selected highest-ranked spreadsheet candidate (score=5; prefers .xlsx).",
                },
                "selectedWorksheetName": "Sheet1",
                "worksheetNames": ["Sheet1", "Summary"],
                "firstRowsPreview": [
                    ["Header A", "Header B"],
                    ["1", "2"],
                ],
                "headerRowCandidates": [
                    {
                        "rowNumber": 2,
                        "score": 1,
                        "normalizedHeaders": ["header a", "header b"],
                        "matchedFields": {"description": 1},
                    }
                ],
                "worksheets": [],
            },
        )


class FakeDbService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create_and_process_intake(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "Summary": {
                "BomIntakeId": 999,
                "DetectedRootCount": 0,
                "AcceptedRootCount": 0,
                "DuplicateRejectedCount": 0,
                "FinalIntakeStatus": "processed",
            },
            "RootResults": [],
        }


class FakeDocPackageIntakeService:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[dict[str, object]] = []

    def intake_package(
        self,
        *,
        customer_name,
        rfq_number,
        uploaded_by,
        uploaded_files,
        intake_notes=None,
    ):
        self.calls.append(
            {
                "customer_name": customer_name,
                "rfq_number": rfq_number,
                "uploaded_by": uploaded_by,
                "uploaded_files": list(uploaded_files),
                "intake_notes": intake_notes,
            }
        )
        if self.fail:
            raise ValueError("forced failure")

        document_result = DocumentIntakeResult(
            customer_name=customer_name,
            rfq_number=rfq_number,
            sanitized_customer_folder_name="ACME",
            sanitized_rfq_folder_name="RFQ-Q-100",
            automation_path=Path("/tmp/automation/ACME/RFQ-Q-100/package"),
            working_path=Path("/tmp/work/ACME/RFQ-Q-100/package"),
            uploaded_files_count=len(list(uploaded_files)),
            processed_files=["bom.xlsx", "drawing.pdf"],
            extension_summary={".pdf": 1, ".xlsx": 1},
        )
        return DocPackageIntakeResult(
            customer_name=customer_name,
            rfq_number=rfq_number,
            uploaded_by=uploaded_by,
            uploaded_files_count=len(list(uploaded_files)),
            selected_bom_file_name="bom.xlsx",
            document_result=document_result,
            bom_preview=types.SimpleNamespace(
                detected_worksheet="BOM",
                detected_source_type="spreadsheet_upload",
            ),
            bom_result={
                "Summary": {
                    "BomIntakeId": 987,
                    "DetectedRootCount": 1,
                    "AcceptedRootCount": 1,
                    "DuplicateRejectedCount": 0,
                    "FinalIntakeStatus": "processed",
                },
                "RootResults": [],
            },
            detected_roots=[],
            intake_notes=intake_notes,
        )


def _request_payload() -> dict[str, object]:
    return {
        "header": {
            "customer_name": "ACME",
            "source_file_name": "bom.xlsx",
            "uploaded_by": "estimator",
        },
        "standardizedBomRows": [
            {
                "source_row_number": 1,
                "original_value": None,
                "parent_part": None,
                "part_number": "ABC-1000",
                "indented_part_number": "ABC-1000",
                "bom_level": 0,
                "description": "TOP",
                "revision": "1",
                "quantity": 1,
                "uom": "EA",
                "item_number": "10",
                "make_buy": "MAKE",
                "mfr": None,
                "mfr_number": None,
                "lead_time_days": None,
                "cost": None,
                "validation_message": None,
            }
        ],
    }


class WebBomIntakeApiTests(unittest.TestCase):
    def test_render_page_includes_doc_package_overview_extension_counts(self) -> None:
        config = AppConfig(
            app_env="test",
            automation_drop_root=Path("/tmp/automation"),
            work_root=Path("/tmp/work"),
            port=8000,
        )
        result = DocumentIntakeResult(
            customer_name="ACME",
            rfq_number="Q-100",
            sanitized_customer_folder_name="ACME",
            sanitized_rfq_folder_name="RFQ-Q-100",
            automation_path=Path("/tmp/automation/ACME/RFQ-Q-100/package"),
            working_path=Path("/tmp/work/ACME/RFQ-Q-100/package"),
            uploaded_files_count=3,
            processed_files=[
                "README",
                "bom.xlsx",
                "drawing.pdf",
                "notes.pdf",
            ],
            extension_summary={
                ".pdf": 2,
                ".xlsx": 1,
                "[no extension]": 1,
            },
        )

        page = render_page(
            config,
            ViewState(
                customer="ACME",
                rfq_number="Q-100",
                uploaded_by="estimator",
                message="Processed 4 file(s) for ACME / RFQ-Q-100 into both configured roots.",
                result=result,
            ),
        )

        self.assertIn("Processed Document Overview", page)
        self.assertIn("Processed file counts grouped by lowercase extension.", page)
        self.assertIn("Processed Filenames", page)
        self.assertIn("<span>.pdf</span><strong>2</strong>", page)
        self.assertIn("<span>.xlsx</span><strong>1</strong>", page)
        self.assertIn("<span>[no extension]</span><strong>1</strong>", page)
        self.assertIn("Automation destination", page)
        self.assertIn("Working destination", page)
        self.assertIn("Doc Package Intake", page)

    def test_api_happy_path_returns_summary(self) -> None:
        fake_service = FakeBomIntakeService()
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app(
                AppConfig(
                    app_env="test",
                    automation_drop_root=Path(temp_dir) / "automation",
                    work_root=Path(temp_dir) / "work",
                    port=8000,
                ),
                bom_intake_service_override=fake_service,
            )

            status, headers, body = _invoke_json(
                app,
                "/api/dev/bom-intake",
                _request_payload(),
            )

        self.assertEqual(status, "200 OK")
        self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")
        payload = json.loads(body)
        self.assertEqual(payload["summary"]["bomIntakeId"], 321)
        self.assertFalse(fake_service.calls[0]["dry_run"])

    def test_api_dry_run_returns_preview_payload(self) -> None:
        fake_service = FakeBomIntakeService()
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app(
                AppConfig(
                    app_env="test",
                    automation_drop_root=Path(temp_dir) / "automation",
                    work_root=Path(temp_dir) / "work",
                    port=8000,
                ),
                bom_intake_service_override=fake_service,
            )

            status, _headers, body = _invoke_json(
                app,
                "/api/dev/bom-intake",
                {**_request_payload(), "dryRun": True},
            )

        self.assertEqual(status, "200 OK")
        payload = json.loads(body)
        self.assertTrue(payload["dryRun"])
        self.assertEqual(payload["previewPath"], "/tmp/bom_intake_payload_preview.json")
        self.assertTrue(fake_service.calls[0]["dry_run"])

    def test_api_rejects_malformed_request_before_service_call(self) -> None:
        fake_db = FakeDbService()
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app(
                AppConfig(
                    app_env="test",
                    automation_drop_root=Path(temp_dir) / "automation",
                    work_root=Path(temp_dir) / "work",
                    port=8000,
                ),
                bom_intake_service_override=BomIntakeService(db_service=fake_db),
            )

            status, _headers, body = _invoke_json(
                app,
                "/api/dev/bom-intake",
                {
                    "header": "bad-shape",
                    "standardizedBomRows": [],
                },
            )

        self.assertEqual(status, "400 Bad Request")
        self.assertEqual(fake_db.calls, [])
        self.assertIn("header", json.loads(body)["error"])

    def test_api_routes_upload_payload_to_uploaded_bom_flow(self) -> None:
        fake_service = FakeBomIntakeService()
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app(
                AppConfig(
                    app_env="test",
                    automation_drop_root=Path(temp_dir) / "automation",
                    work_root=Path(temp_dir) / "work",
                    port=8000,
                ),
                bom_intake_service_override=fake_service,
            )

            status, _headers, body = _invoke_json(
                app,
                "/api/dev/bom-intake",
                {
                    "header": {
                        "customer_name": "ACME",
                        "uploaded_by": "estimator",
                        "source_file_name": "bom.xlsx",
                    },
                    "upload": {
                        "filename": "bom.xlsx",
                        "content_base64": base64.b64encode(b"fake").decode("ascii"),
                    },
                    "dryRun": True,
                },
            )

        self.assertEqual(status, "200 OK")
        payload = json.loads(body)
        self.assertTrue(payload["dryRun"])
        self.assertIn("upload_data", fake_service.calls[0])

    def test_multipart_preview_endpoint_returns_rich_preview(self) -> None:
        fake_service = FakeBomIntakeService()
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app(
                AppConfig(
                    app_env="test",
                    automation_drop_root=Path(temp_dir) / "automation",
                    work_root=Path(temp_dir) / "work",
                    port=8000,
                ),
                bom_intake_service_override=fake_service,
            )

            status, headers, body = _invoke_multipart(
                app,
                "/api/dev/bom-intake/preview",
                fields={
                    "customer_name": "ACME",
                    "uploaded_by": "estimator",
                    "quote_number": "Q-100",
                    "intake_notes": "preview this",
                },
                file_field_name="bom_file",
                filename="bom.xlsx",
                content=b"fake workbook bytes",
            )

        self.assertEqual(status, "200 OK")
        self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")
        payload = json.loads(body)
        self.assertEqual(payload["selectedFileName"], "bom.xlsx")
        self.assertEqual(payload["detectedWorksheet"], "BOM")
        self.assertEqual(payload["rootCount"], 1)
        self.assertEqual(fake_service.calls[0]["mode"], "preview")
        self.assertEqual(fake_service.calls[0]["upload_data"]["filename"], "bom.xlsx")
        self.assertEqual(fake_service.calls[0]["upload_data"]["content"], b"fake workbook bytes")

    def test_multipart_process_endpoint_returns_summary(self) -> None:
        fake_service = FakeBomIntakeService()
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app(
                AppConfig(
                    app_env="test",
                    automation_drop_root=Path(temp_dir) / "automation",
                    work_root=Path(temp_dir) / "work",
                    port=8000,
                ),
                bom_intake_service_override=fake_service,
            )

            status, _headers, body = _invoke_multipart(
                app,
                "/api/dev/bom-intake/process",
                fields={
                    "customer_name": "ACME",
                    "uploaded_by": "estimator",
                },
                file_field_name="bom_file",
                filename="bom.zip",
                content=b"zip bytes",
            )

        self.assertEqual(status, "200 OK")
        payload = json.loads(body)
        self.assertEqual(payload["summary"]["bomIntakeId"], 654)
        self.assertIn("upload_data", fake_service.calls[0])

    def test_multipart_preview_endpoint_returns_diagnostics_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app(
                AppConfig(
                    app_env="test",
                    automation_drop_root=Path(temp_dir) / "automation",
                    work_root=Path(temp_dir) / "work",
                    port=8000,
                ),
                bom_intake_service_override=FailingPreviewBomIntakeService(),
            )

            status, headers, body = _invoke_multipart(
                app,
                "/api/dev/bom-intake/preview",
                fields={
                    "customer_name": "ACME",
                    "uploaded_by": "estimator",
                },
                file_field_name="bom_file",
                filename="package.zip",
                content=b"zip bytes",
            )

        self.assertEqual(status, "400 Bad Request")
        self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")
        payload = json.loads(body)
        self.assertEqual(payload["error"], "No BOM header row could be detected.")
        self.assertEqual(payload["diagnostics"]["selectedArchiveMemberName"], "docs/reference.xlsx")
        self.assertEqual(payload["diagnostics"]["selectedWorksheetName"], "Sheet1")
        self.assertEqual(len(payload["diagnostics"]["candidateSpreadsheets"]), 2)

    def test_root_page_renders_doc_package_upload_ui(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app(
                AppConfig(
                    app_env="test",
                    automation_drop_root=Path(temp_dir) / "automation",
                    work_root=Path(temp_dir) / "work",
                    port=8000,
                ),
                bom_intake_service_override=FakeBomIntakeService(),
            )

            status, headers, body = _invoke_get(app, "/")

        self.assertEqual(status, "200 OK")
        self.assertEqual(headers["Content-Type"], "text/html; charset=utf-8")
        self.assertIn("Doc Package Intake", body)
        self.assertIn('name="uploaded_by"', body)
        self.assertIn('name="documents"', body)
        self.assertIn("Process Doc Package", body)

    def test_root_post_routes_package_to_doc_package_intake_service(self) -> None:
        fake_package_service = FakeDocPackageIntakeService()
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app(
                AppConfig(
                    app_env="test",
                    automation_drop_root=Path(temp_dir) / "automation",
                    work_root=Path(temp_dir) / "work",
                    port=8000,
                ),
                bom_intake_service_override=FakeBomIntakeService(),
                doc_package_intake_service_override=fake_package_service,
            )

            status, headers, body = _invoke_multipart(
                app,
                "/",
                fields={
                    "customer": "ACME",
                    "rfq_number": "Q-100",
                    "uploaded_by": "estimator",
                    "quote_number": "Q-100",
                    "intake_notes": "same workflow",
                },
                file_field_name="documents",
                filename="bom.xlsx",
                content=b"fake workbook bytes",
            )

        self.assertEqual(status, "200 OK")
        self.assertEqual(headers["Content-Type"], "text/html; charset=utf-8")
        self.assertEqual(fake_package_service.calls[0]["customer_name"], "ACME")
        self.assertEqual(fake_package_service.calls[0]["uploaded_by"], "estimator")
        self.assertIn("Doc Package Intake Complete", body)
        self.assertIn("BOM Intake Overview", body)
        self.assertIn("bom.xlsx", body)


def _invoke_json(app, path: str, payload: dict[str, object]):
    raw_body = json.dumps(payload).encode("utf-8")
    captured: dict[str, object] = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = dict(headers)

    body = b"".join(
        app(
            {
                "REQUEST_METHOD": "POST",
                "PATH_INFO": path,
                "CONTENT_LENGTH": str(len(raw_body)),
                "CONTENT_TYPE": "application/json",
                "wsgi.input": io.BytesIO(raw_body),
            },
            start_response,
        )
    )
    return captured["status"], captured["headers"], body.decode("utf-8")


def _invoke_multipart(
    app,
    path: str,
    *,
    fields: dict[str, str],
    file_field_name: str,
    filename: str,
    content: bytes,
):
    boundary = "----WebKitFormBoundaryTest123456"
    chunks: list[bytes] = []

    for name, value in fields.items():
        chunks.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"),
                value.encode("utf-8"),
                b"\r\n",
            ]
        )

    chunks.extend(
        [
            f"--{boundary}\r\n".encode("utf-8"),
            (
                f'Content-Disposition: form-data; name="{file_field_name}"; '
                f'filename="{filename}"\r\n'
            ).encode("utf-8"),
            b"Content-Type: application/octet-stream\r\n\r\n",
            content,
            b"\r\n",
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
    )
    raw_body = b"".join(chunks)
    captured: dict[str, object] = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = dict(headers)

    body = b"".join(
        app(
            {
                "REQUEST_METHOD": "POST",
                "PATH_INFO": path,
                "CONTENT_LENGTH": str(len(raw_body)),
                "CONTENT_TYPE": f"multipart/form-data; boundary={boundary}",
                "wsgi.input": io.BytesIO(raw_body),
            },
            start_response,
        )
    )
    return captured["status"], captured["headers"], body.decode("utf-8")


def _invoke_get(app, path: str):
    captured: dict[str, object] = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = dict(headers)

    body = b"".join(
        app(
            {
                "REQUEST_METHOD": "GET",
                "PATH_INFO": path,
                "CONTENT_LENGTH": "0",
                "wsgi.input": io.BytesIO(b""),
            },
            start_response,
        )
    )
    return captured["status"], captured["headers"], body.decode("utf-8")


if __name__ == "__main__":
    unittest.main()
