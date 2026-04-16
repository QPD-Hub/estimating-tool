import json
import unittest
from pathlib import Path

from src.contracts.bom_intake import (
    CREATE_PROC_SCALAR_FIELDS,
    PROCESS_PROC_SCALAR_FIELDS,
    ROOT_TVP_FIELDS,
    ROW_TVP_FIELDS,
    BomIntakeContractError,
    BomIntakeRow,
    ProcessStandardizedBomIntakeInput,
)
from src.services.bom_intake_payload import (
    BomIntakeMetadata,
    BomIntakePayloadError,
    StandardizedBomRow,
    build_bom_intake_payload,
)


FIXTURE_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "bom_intake"
    / "standardized_payload_example.json"
)

DB_OWNED_FIELDS = {
    "IsLevel0",
    "BomRootId",
    "RowGuid",
    "ParentBomRowId",
    "RowPath",
    "RowStatus",
    "CreatedAt",
    "ModifiedAt",
    "NormalizedCustomerName",
    "NormalizedPartNumber",
    "NormalizedRevision",
    "DecisionStatus",
    "DecisionReason",
    "ExistingBomRootId",
    "InternalDuplicateRank",
}


def _metadata() -> BomIntakeMetadata:
    return BomIntakeMetadata(
        customer_name=" ACME ",
        quote_number=" Q-100 ",
        source_file_name=" customer-bom.xlsx ",
        source_file_path=" /tmp/customer-bom.xlsx ",
        source_sheet_name=" BOM ",
        source_type=" standardized_upload ",
        uploaded_by=" estimator ",
        parser_version=" v1 ",
        intake_notes=" fixture preview ",
    )


def _standardized_rows() -> list[StandardizedBomRow]:
    return [
        StandardizedBomRow(
            source_row_number=1,
            original_value=None,
            parent_part=None,
            part_number="ABC-1000",
            indented_part_number="ABC-1000",
            bom_level=0,
            description="TOP",
            revision="1",
            quantity=1,
            uom="EA",
            item_number="10",
            make_buy="MAKE",
            mfr=None,
            mfr_number=None,
            lead_time_days=None,
            cost=None,
        ),
        StandardizedBomRow(
            source_row_number=2,
            original_value=None,
            parent_part="ABC-1000",
            part_number="COMP-200",
            indented_part_number="COMP-200",
            bom_level=1,
            description="COMPONENT",
            revision="",
            quantity=2,
            uom="EA",
            item_number="20",
            make_buy="BUY",
            mfr="MCMASTER",
            mfr_number="91234A123",
            lead_time_days=7,
            cost=1.25,
        ),
    ]


class BuildBomIntakePayloadTests(unittest.TestCase):
    def test_matches_fixture_preview_shape(self) -> None:
        payload = build_bom_intake_payload(_metadata(), _standardized_rows())

        self.assertEqual(payload.to_preview_dict(), json.loads(FIXTURE_PATH.read_text()))

    def test_contract_field_names_match_sql_contract(self) -> None:
        payload = build_bom_intake_payload(_metadata(), _standardized_rows())
        preview = payload.to_preview_dict()

        self.assertEqual(
            tuple(preview["createProc"]["params"].keys()),
            CREATE_PROC_SCALAR_FIELDS,
        )
        self.assertEqual(PROCESS_PROC_SCALAR_FIELDS, ("BomIntakeId", "DetectedBy"))
        self.assertEqual(
            tuple(preview["processStandardizedProc"]["roots"][0].keys()),
            ROOT_TVP_FIELDS,
        )
        self.assertEqual(
            tuple(preview["processStandardizedProc"]["rows"][0].keys()),
            ROW_TVP_FIELDS,
        )

    def test_preview_payload_excludes_db_owned_fields(self) -> None:
        payload = build_bom_intake_payload(_metadata(), _standardized_rows())
        preview = payload.to_preview_dict()

        preview_keys = set(preview["createProc"]["params"])
        preview_keys.update(preview["processStandardizedProc"]["params"])
        for root in preview["processStandardizedProc"]["roots"]:
            preview_keys.update(root)
        for row in preview["processStandardizedProc"]["rows"]:
            preview_keys.update(row)

        self.assertTrue(DB_OWNED_FIELDS.isdisjoint(preview_keys))
        self.assertNotIn(
            "IsLevel0",
            preview["processStandardizedProc"]["rows"][0],
        )

    def test_contract_models_reject_extra_sql_bound_fields(self) -> None:
        with self.assertRaises(BomIntakeContractError):
            ProcessStandardizedBomIntakeInput.from_dict(
                {
                    "BomIntakeId": 123,
                    "DetectedBy": "estimator",
                    "Unexpected": "value",
                },
                context="Process params",
            )

        with self.assertRaises(BomIntakeContractError):
            BomIntakeRow.from_dict(
                {
                    "RootClientId": "R1",
                    "RowSequence": 1,
                    "SourceRowNumber": 1,
                    "OriginalValue": None,
                    "ParentPart": None,
                    "PartNumber": "ABC-1000",
                    "IndentedPartNumber": "ABC-1000",
                    "BomLevel": 0,
                    "Description": "TOP",
                    "Revision": "1",
                    "Quantity": 1,
                    "UOM": "EA",
                    "ItemNumber": "10",
                    "MakeBuy": "MAKE",
                    "MFR": None,
                    "MFRNumber": None,
                    "LeadTimeDays": None,
                    "Cost": None,
                    "ValidationMessage": None,
                    "IsLevel0": True,
                },
                context="Row payload",
            )

    def test_rejects_rows_before_first_root(self) -> None:
        with self.assertRaises(BomIntakePayloadError):
            build_bom_intake_payload(
                _metadata(),
                [
                    StandardizedBomRow(
                        source_row_number=2,
                        original_value=None,
                        parent_part="ABC-1000",
                        part_number="COMP-200",
                        indented_part_number="COMP-200",
                        bom_level=1,
                        description="COMPONENT",
                        revision="",
                        quantity=2,
                        uom="EA",
                        item_number="20",
                        make_buy="BUY",
                        mfr=None,
                        mfr_number=None,
                        lead_time_days=None,
                        cost=None,
                    )
                ],
            )


if __name__ == "__main__":
    unittest.main()
