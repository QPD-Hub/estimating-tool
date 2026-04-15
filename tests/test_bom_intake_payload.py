import unittest

from src.services.bom_intake_payload import (
    BomIntakeMetadata,
    BomIntakePayload,
    BomIntakePayloadError,
    BomRootCandidate,
    BomUploadRow,
    StandardizedBomRow,
    build_bom_intake_payload,
)


class BuildBomIntakePayloadTests(unittest.TestCase):
    def test_builds_roots_and_rows_from_level_zero_boundaries(self) -> None:
        metadata = BomIntakeMetadata(
            customer_name=" ACME ",
            source_file_name=" customer-bom.xlsx ",
            uploaded_by=" estimators@example.com ",
            quote_number=" Q-100 ",
        )

        payload = build_bom_intake_payload(
            metadata,
            standardized_rows=[
                StandardizedBomRow(
                    source_row_number=1,
                    original_value=None,
                    parent_part=None,
                    part_number="ABC-1000",
                    indented_part_number="ABC-1000",
                    bom_level=0,
                    description="TOP ASSEMBLY",
                    revision=" 1 ",
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
                    indented_part_number="  COMP-200",
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
                StandardizedBomRow(
                    source_row_number=10,
                    original_value=None,
                    parent_part=None,
                    part_number="XYZ-9000",
                    indented_part_number="XYZ-9000",
                    bom_level=0,
                    description="SECOND ROOT",
                    revision="A",
                    quantity=1,
                    uom="EA",
                    item_number="10",
                    make_buy="MAKE",
                    mfr=None,
                    mfr_number=None,
                    lead_time_days=None,
                    cost=None,
                ),
            ],
        )

        self.assertEqual(payload.metadata.customer_name, "ACME")
        self.assertEqual(len(payload.root_candidates), 2)
        self.assertEqual(payload.root_candidates[0].root_client_id, "R1")
        self.assertEqual(payload.root_candidates[1].root_client_id, "R2")
        self.assertEqual(payload.root_candidates[0].revision, "1")
        self.assertEqual(payload.bom_rows[0].row_sequence, 1)
        self.assertEqual(payload.bom_rows[1].row_sequence, 2)
        self.assertEqual(payload.bom_rows[2].row_sequence, 1)

        sql_payload = payload.to_sql_payload()
        self.assertEqual(sql_payload["Header"]["CustomerName"], "ACME")
        self.assertEqual(sql_payload["RootCandidates"][0]["Level0PartNumber"], "ABC-1000")
        self.assertEqual(sql_payload["BomRows"][1]["Revision"], "")

    def test_rejects_rows_before_first_level_zero_root(self) -> None:
        metadata = BomIntakeMetadata(
            customer_name="ACME",
            source_file_name="customer-bom.xlsx",
            uploaded_by="estimator",
        )

        with self.assertRaises(BomIntakePayloadError):
            build_bom_intake_payload(
                metadata,
                standardized_rows=[
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


class BomIntakePayloadValidationTests(unittest.TestCase):
    def test_rejects_duplicate_row_sequence_within_root(self) -> None:
        with self.assertRaises(BomIntakePayloadError):
            BomIntakePayload(
                metadata=BomIntakeMetadata(
                    customer_name="ACME",
                    source_file_name="customer-bom.xlsx",
                    uploaded_by="estimator",
                ),
                root_candidates=[
                    BomRootCandidate(
                        root_client_id="R1",
                        root_sequence=1,
                        source_row_number=1,
                        customer_name="ACME",
                        level_0_part_number="ABC-1000",
                        revision="1",
                    )
                ],
                bom_rows=[
                    BomUploadRow(
                        root_client_id="R1",
                        row_sequence=1,
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
                    BomUploadRow(
                        root_client_id="R1",
                        row_sequence=1,
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
                    ),
                ],
            )


if __name__ == "__main__":
    unittest.main()
