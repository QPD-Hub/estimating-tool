import io
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
from src.services.bom_intake_service import BomIntakeService
from src.web import create_app


class FakeBomIntakeService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

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


if __name__ == "__main__":
    unittest.main()
