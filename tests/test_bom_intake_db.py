import unittest

from src.config import SqlServerConfig
from src.services.bom_intake_db import BomIntakeDbError, BomIntakeDbService
from src.services.bom_intake_payload import BomIntakeMetadata, StandardizedBomRow, build_bom_intake_payload


class FakeCursor:
    def __init__(self, executions: list[dict[str, object]]) -> None:
        self._executions = executions
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self._current_sets: list[list[dict[str, object]]] = []
        self._set_index = 0
        self.description = None

    def execute(self, sql: str, params: tuple[object, ...] = ()) -> "FakeCursor":
        self.executed.append((sql, params))
        execution = self._executions.pop(0)
        self._current_sets = execution["result_sets"]
        self._set_index = 0
        self._sync_description()
        return self

    def fetchall(self):
        return [tuple(row.values()) for row in self._current_sets[self._set_index]]

    def nextset(self):
        self._set_index += 1
        if self._set_index >= len(self._current_sets):
            self.description = None
            return False
        self._sync_description()
        return True

    def close(self) -> None:
        return None

    def _sync_description(self) -> None:
        if not self._current_sets or not self._current_sets[self._set_index]:
            self.description = None
            return
        row = self._current_sets[self._set_index][0]
        self.description = [(column, None, None, None, None, None, None) for column in row]


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


def _payload():
    return build_bom_intake_payload(
        BomIntakeMetadata(
            customer_name="ACME",
            quote_number="Q-1",
            source_file_name="bom.xlsx",
            source_file_path="/tmp/bom.xlsx",
            source_sheet_name="BOM",
            source_type="standardized_upload",
            uploaded_by="estimator",
            parser_version="v1",
            intake_notes="notes",
        ),
        [
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
                mfr=None,
                mfr_number=None,
                lead_time_days=None,
                cost=None,
            ),
        ],
    )


class BomIntakeDbServiceTests(unittest.TestCase):
    def test_create_and_process_maps_payload_into_sql_calls(self) -> None:
        fake_cursor = FakeCursor(
            executions=[
                {"result_sets": [[{"BomIntakeId": 321}]]},
                {
                    "result_sets": [
                        [
                            {
                                "BomIntakeId": 321,
                                "DetectedRootCount": 1,
                                "AcceptedRootCount": 1,
                                "DuplicateRejectedCount": 0,
                                "FinalIntakeStatus": "processed",
                            }
                        ],
                        [
                            {
                                "RootClientId": "R1",
                                "RootSequence": 1,
                                "CustomerName": "ACME",
                                "Level0PartNumber": "ABC-1000",
                                "Revision": "1",
                                "DecisionStatus": "accepted",
                                "DecisionReason": "Inserted",
                                "BomRootId": 11,
                                "ExistingBomRootId": None,
                            }
                        ],
                    ]
                },
            ]
        )
        fake_connection = FakeConnection(fake_cursor)
        service = BomIntakeDbService(
            sql_config=SqlServerConfig(
                host="sql-host",
                username="app_user",
                password="secret",
            ),
            connect=lambda **_kwargs: fake_connection,
        )

        result = service.create_and_process_intake(payload=_payload())

        create_sql, create_params = fake_cursor.executed[0]
        process_sql, process_params = fake_cursor.executed[1]

        self.assertIn("EXEC dbo.usp_BOM_Intake_Create", create_sql)
        self.assertEqual(create_params[0], "ACME")
        self.assertIn("DECLARE @Roots dbo.udtt_BOM_Intake_Root;", process_sql)
        self.assertIn("DECLARE @Rows dbo.udtt_BOM_Intake_Row;", process_sql)
        self.assertIn("EXEC dbo.usp_BOM_Intake_ProcessStandardized", process_sql)
        self.assertNotIn("IsLevel0", process_sql)
        self.assertEqual(process_params[0], "R1")
        self.assertEqual(process_params[-2], 321)
        self.assertEqual(process_params[-1], "estimator")
        self.assertTrue(fake_connection.committed)
        self.assertTrue(fake_connection.closed)
        self.assertEqual(result["Summary"]["DuplicateRejectedCount"], 0)
        self.assertEqual(len(result["RootResults"]), 1)

    def test_create_intake_prefers_explicit_output_select_over_proc_result_sets(self) -> None:
        fake_cursor = FakeCursor(
            executions=[
                {
                    "result_sets": [
                        [{"Status": "created"}],
                        [{"BomIntakeId": 999, "Message": "intermediate"}],
                        [{"BomIntakeId": 321}],
                    ]
                }
            ]
        )
        service = BomIntakeDbService(
            sql_config=SqlServerConfig(
                host="sql-host",
                username="app_user",
                password="secret",
            ),
            connect=lambda **_kwargs: None,
        )

        bom_intake_id = service._create_intake(
            fake_cursor,
            _payload().create_input.to_dict(),
        )

        create_sql, _create_params = fake_cursor.executed[0]

        self.assertIn("DECLARE @BomIntakeId BIGINT;", create_sql)
        self.assertIn("@BomIntakeId = @BomIntakeId OUTPUT", create_sql)
        self.assertIn("SELECT @BomIntakeId AS BomIntakeId;", create_sql)
        self.assertEqual(bom_intake_id, 321)

    def test_rejects_header_shape_that_does_not_match_sql_contract(self) -> None:
        service = BomIntakeDbService(
            sql_config=SqlServerConfig(
                host="sql-host",
                username="app_user",
                password="secret",
            ),
            connect=lambda **_kwargs: None,
        )

        with self.assertRaises(BomIntakeDbError):
            service._create_intake(
                FakeCursor(executions=[]),
                {"CustomerName": "ACME", "Unexpected": "value"},
            )

    def test_rejects_sql_owned_fields_in_row_payload(self) -> None:
        with self.assertRaises(BomIntakeDbError):
            service = BomIntakeDbService(
                sql_config=SqlServerConfig(
                    host="sql-host",
                    username="app_user",
                    password="secret",
                ),
                connect=lambda **_kwargs: None,
            )
            service._build_process_standardized_command(
                process_params={"BomIntakeId": 321, "DetectedBy": "estimator"},
                root_candidates=[
                    {
                        "RootClientId": "R1",
                        "RootSequence": 1,
                        "SourceRowNumber": 1,
                        "CustomerName": "ACME",
                        "Level0PartNumber": "ABC-1000",
                        "Revision": "1",
                        "RootDescription": "TOP",
                        "RootItemNumber": "10",
                        "RootQuantity": 1,
                        "RootUOM": "EA",
                        "RootMakeBuy": "MAKE",
                        "RootMFR": None,
                        "RootMFRNumber": None,
                    }
                ],
                bom_rows=[
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
                    }
                ],
            )


if __name__ == "__main__":
    unittest.main()
