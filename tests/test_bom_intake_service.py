import unittest

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

    def create_and_process_intake(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


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
            "is_level_0": True,
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
            "is_level_0": False,
            "validation_message": None,
        },
    ]


class BomIntakeServiceTests(unittest.TestCase):
    def test_orchestrates_payload_build_and_db_call(self) -> None:
        db_service = FakeDbService()
        service = BomIntakeService(db_service=db_service)

        result = service.process_standardized_upload(
            header_data={
                "customer_name": "ACME",
                "source_file_name": "bom.xlsx",
                "uploaded_by": "estimator",
                "source_type": "standardized_upload",
            },
            standardized_rows_data=_standardized_request_rows(),
        )

        self.assertEqual(result["Summary"]["BomIntakeId"], 101)
        self.assertEqual(len(db_service.calls), 1)
        self.assertEqual(db_service.calls[0]["header"]["CustomerName"], "ACME")
        self.assertEqual(len(db_service.calls[0]["root_candidates"]), 1)
        self.assertEqual(len(db_service.calls[0]["bom_rows"]), 2)
        self.assertEqual(db_service.calls[0]["detected_by"], "estimator")

    def test_rejects_malformed_request_before_db_call(self) -> None:
        db_service = FakeDbService()
        service = BomIntakeService(db_service=db_service)

        with self.assertRaises(BomIntakeRequestError):
            service.process_standardized_upload(
                header_data={"customer_name": "ACME"},
                standardized_rows_data=_standardized_request_rows(),
            )

        self.assertEqual(db_service.calls, [])

    def test_passes_all_duplicate_result_through(self) -> None:
        db_service = FakeDbService(
            result={
                "Summary": {
                    "BomIntakeId": 202,
                    "DetectedRootCount": 2,
                    "AcceptedRootCount": 0,
                    "DuplicateRejectedCount": 2,
                    "FinalIntakeStatus": "duplicates_rejected",
                },
                "RootResults": [
                    {
                        "RootClientId": "R1",
                        "DecisionStatus": "duplicate_rejected",
                    },
                    {
                        "RootClientId": "R2",
                        "DecisionStatus": "duplicate_rejected",
                    },
                ],
            }
        )
        service = BomIntakeService(db_service=db_service)

        result = service.process_standardized_upload(
            header_data={
                "customer_name": "ACME",
                "source_file_name": "bom.xlsx",
                "uploaded_by": "estimator",
            },
            standardized_rows_data=[
                _standardized_request_rows()[0],
                {
                    **_standardized_request_rows()[0],
                    "source_row_number": 10,
                    "part_number": "XYZ-9000",
                    "indented_part_number": "XYZ-9000",
                    "revision": "A",
                    "description": "SECOND ROOT",
                },
            ],
        )

        self.assertEqual(result["Summary"]["AcceptedRootCount"], 0)
        self.assertEqual(result["Summary"]["DuplicateRejectedCount"], 2)
        self.assertEqual(result["Summary"]["FinalIntakeStatus"], "duplicates_rejected")


if __name__ == "__main__":
    unittest.main()
