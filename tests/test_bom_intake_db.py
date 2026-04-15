import unittest

from src.config import SqlServerConfig
from src.services.bom_intake_db import BomIntakeDbService


class FakeCursor:
    def __init__(self, executions: list[dict[str, object]]) -> None:
        self._executions = executions
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self._current_sets: list[list[dict[str, object]]] = []
        self._set_index = 0
        self.description = None

    def execute(self, sql: str, *params: object):
        self.executed.append((sql, params))
        execution = self._executions.pop(0)
        self._current_sets = execution["result_sets"]
        self._set_index = 0
        self._sync_description()
        return self

    def fetchall(self):
        return [
            tuple(row.values()) for row in self._current_sets[self._set_index]
        ]

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self) -> FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


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
                                "DetectedRootCount": 2,
                                "AcceptedRootCount": 1,
                                "DuplicateRejectedCount": 1,
                                "FinalIntakeStatus": "processed_with_duplicates",
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
                            },
                            {
                                "RootClientId": "R2",
                                "RootSequence": 2,
                                "CustomerName": "ACME",
                                "Level0PartNumber": "XYZ-9000",
                                "Revision": "A",
                                "DecisionStatus": "duplicate_rejected",
                                "DecisionReason": "Already exists",
                                "BomRootId": None,
                                "ExistingBomRootId": 22,
                            },
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
            connect=lambda *_args, **_kwargs: fake_connection,
        )

        result = service.create_and_process_intake(
            header={
                "CustomerName": "ACME",
                "QuoteNumber": "Q-1",
                "SourceFileName": "bom.xlsx",
                "SourceFilePath": "/tmp/bom.xlsx",
                "SourceSheetName": "BOM",
                "SourceType": "standardized_upload",
                "UploadedBy": "estimator",
                "ParserVersion": "v1",
                "IntakeNotes": "notes",
            },
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
                },
                {
                    "RootClientId": "R2",
                    "RootSequence": 2,
                    "SourceRowNumber": 10,
                    "CustomerName": "ACME",
                    "Level0PartNumber": "XYZ-9000",
                    "Revision": "A",
                    "RootDescription": "SECOND",
                    "RootItemNumber": "10",
                    "RootQuantity": 1,
                    "RootUOM": "EA",
                    "RootMakeBuy": "MAKE",
                    "RootMFR": None,
                    "RootMFRNumber": None,
                },
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
                    "IsLevel0": True,
                    "ValidationMessage": None,
                }
            ],
            detected_by="estimator",
        )

        create_sql, create_params = fake_cursor.executed[0]
        process_sql, process_params = fake_cursor.executed[1]

        self.assertIn("EXEC dbo.usp_BOM_Intake_Create", create_sql)
        self.assertEqual(create_params[0], "ACME")
        self.assertIn("DECLARE @Roots dbo.udtt_BOM_Intake_Root;", process_sql)
        self.assertIn("DECLARE @Rows dbo.udtt_BOM_Intake_Row;", process_sql)
        self.assertIn("EXEC dbo.usp_BOM_Intake_ProcessStandardized", process_sql)
        self.assertEqual(process_params[0], "R1")
        self.assertEqual(process_params[-2], 321)
        self.assertEqual(process_params[-1], "estimator")
        self.assertTrue(fake_connection.committed)
        self.assertEqual(result["Summary"]["DuplicateRejectedCount"], 1)
        self.assertEqual(len(result["RootResults"]), 2)


if __name__ == "__main__":
    unittest.main()
