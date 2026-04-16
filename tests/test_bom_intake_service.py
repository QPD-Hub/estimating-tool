import json
import tempfile
import unittest
from pathlib import Path

from src.services.bom_intake_service import BomIntakeRequestError, BomIntakeService


class FakeDbService:
    def __init__(self, result: dict[str, object] | None = None) -> None:
        self.result = result or {
            "Summary": {
                "BomIntakeId": 101,
                "DetectedRootCount": 1,
                "AcceptedRootCount": 1,
                "DuplicateRejectedCount": 0,
                "FinalIntakeStatus": "processed",
            },
            "RootResults": [],
        }
        self.calls: list[dict[str, object]] = []

    def create_and_process_intake(self, *, payload):
        self.calls.append({"payload": payload})
        return self.result


def _header() -> dict[str, object]:
    return {
        "customer_name": "ACME",
        "source_file_name": "bom.xlsx",
        "source_file_path": "/tmp/bom.xlsx",
        "source_sheet_name": "BOM",
        "uploaded_by": "estimator",
        "source_type": "standardized_upload",
    }


def _standardized_request_rows() -> list[dict[str, object]]:
    return [
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
        },
        {
            "source_row_number": 2,
            "original_value": None,
            "parent_part": "ABC-1000",
            "part_number": "COMP-200",
            "indented_part_number": "COMP-200",
            "bom_level": 1,
            "description": "COMPONENT",
            "revision": "",
            "quantity": 2,
            "uom": "EA",
            "item_number": "20",
            "make_buy": "BUY",
            "mfr": None,
            "mfr_number": None,
            "lead_time_days": None,
            "cost": None,
            "validation_message": None,
        },
    ]


class BomIntakeServiceTests(unittest.TestCase):
    def test_orchestrates_payload_build_and_db_call(self) -> None:
        db_service = FakeDbService()
        service = BomIntakeService(db_service=db_service)

        result = service.process_standardized_upload(
            header_data=_header(),
            standardized_rows_data=_standardized_request_rows(),
        )

        self.assertEqual(result["Summary"]["BomIntakeId"], 101)
        self.assertEqual(len(db_service.calls), 1)
        payload = db_service.calls[0]["payload"]
        self.assertEqual(payload.create_input.CustomerName, "ACME")
        self.assertEqual(len(payload.roots), 1)
        self.assertEqual(len(payload.rows), 2)
        self.assertEqual(payload.detected_by, "estimator")

    def test_rejects_extra_request_fields_before_db_call(self) -> None:
        db_service = FakeDbService()
        service = BomIntakeService(db_service=db_service)

        with self.assertRaises(BomIntakeRequestError):
            service.process_standardized_upload(
                header_data={**_header(), "unexpected": "value"},
                standardized_rows_data=_standardized_request_rows(),
            )

        with self.assertRaises(BomIntakeRequestError):
            service.process_standardized_upload(
                header_data=_header(),
                standardized_rows_data=[
                    {**_standardized_request_rows()[0], "is_level_0": True}
                ],
            )

        self.assertEqual(db_service.calls, [])

    def test_dry_run_writes_preview_json_without_db_call(self) -> None:
        db_service = FakeDbService()
        service = BomIntakeService(db_service=db_service)

        with tempfile.TemporaryDirectory() as temp_dir:
            preview_path = Path(temp_dir) / "payload-preview.json"
            result = service.process_standardized_upload(
                header_data=_header(),
                standardized_rows_data=_standardized_request_rows(),
                dry_run=True,
                preview_path=preview_path,
            )
            written_payload = json.loads(preview_path.read_text())

        self.assertTrue(result["DryRun"])
        self.assertEqual(result["PreviewPath"], str(preview_path))
        self.assertEqual(written_payload, result["Payload"])
        self.assertEqual(db_service.calls, [])
        self.assertEqual(
            tuple(written_payload["processStandardizedProc"]["params"].keys()),
            ("BomIntakeId", "DetectedBy"),
        )
        self.assertIsNone(
            written_payload["processStandardizedProc"]["params"]["BomIntakeId"]
        )


if __name__ == "__main__":
    unittest.main()
