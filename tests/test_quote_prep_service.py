import unittest

from src.services.quote_prep_service import QuotePrepRequestError, QuotePrepService


class QuotePrepServiceValidationTests(unittest.TestCase):
    def test_save_rejects_duplicate_qty_breaks(self) -> None:
        service = QuotePrepService.__new__(QuotePrepService)
        with self.assertRaises(QuotePrepRequestError):
            service._normalize_save_item(
                {
                    "bomRootId": 123,
                    "includeInQuote": True,
                    "quoteQtyBreaks": "1,5,5",
                }
            )

    def test_save_sets_empty_breaks_when_excluded(self) -> None:
        service = QuotePrepService.__new__(QuotePrepService)
        result = service._normalize_save_item(
            {
                "bomRootId": 123,
                "includeInQuote": False,
                "quoteQtyBreaks": "1,5,10",
            }
        )
        self.assertEqual(result["quoteQtyBreaks"], "")

    def test_save_submits_jobboss_request_and_returns_request_id(self) -> None:
        class FakeCursor:
            def __init__(self) -> None:
                self.last_jobboss_params = None
                self._result = None

            def execute(self, sql, params):
                sql_text = str(sql)
                if "usp_BOM_Root_SaveQuotePrep" in sql_text:
                    self._result = None
                    return
                if "FROM dbo.BOM_Intake" in sql_text:
                    self._result = {
                        "BomIntakeId": 987,
                        "IntakeGuid": "11111111-1111-1111-1111-111111111111",
                        "CustomerName": "ACME",
                        "ContactName": "Alice",
                        "QuoteNumber": "Q-100",
                        "QuoteDueDate": "2026-05-01",
                        "QuotedBy": "estimator",
                        "UploadedBy": "uploader",
                    }
                    return
                if "FROM dbo.BOM_Root" in sql_text:
                    self._result = [
                        {
                            "BomRootId": 1,
                            "Level0PartNumber": "ASM-1000",
                            "RootDescription": "Top Assembly",
                            "Revision": "A",
                            "QuoteQtyBreaks": "1,5,10",
                        }
                    ]
                    return
                if "FROM HILLSBORO.dbo.Contact AS c" in sql_text:
                    self._result = {"ContactId": "18"}
                    return
                if "usp_JobBossRequest_Create" in sql_text:
                    self.last_jobboss_params = params
                    self._result = {"JobBossRequestId": 321}
                    return
                raise AssertionError("Unexpected SQL executed.")

            def fetchone(self):
                if isinstance(self._result, dict):
                    return self._result
                return None

            def fetchall(self):
                if isinstance(self._result, list):
                    return self._result
                return []

            def close(self):
                return None

        class FakeConnection:
            def __init__(self) -> None:
                self.cursor_obj = FakeCursor()

            def cursor(self, as_dict=False):
                self.as_dict = as_dict
                return self.cursor_obj

            def close(self):
                return None

        fake_connection = FakeConnection()
        service = QuotePrepService.__new__(QuotePrepService)
        service._connect = lambda **kwargs: fake_connection
        service._connection_kwargs = lambda: {}

        with self.assertLogs("src.services.quote_prep_service", level="INFO") as captured_logs:
            result = service.save_quote_prep(
                987,
                [{"bomRootId": 1, "includeInQuote": True, "quoteQtyBreaks": "1,5,10"}],
            )

        self.assertEqual(result["saved"], True)
        self.assertEqual(result["jobBossRequestId"], 321)
        self.assertIsNotNone(fake_connection.cursor_obj.last_jobboss_params)
        request_xml = fake_connection.cursor_obj.last_jobboss_params[9]
        self.assertIn('Session="{SESSION_ID}"', request_xml)
        self.assertIn("<QuoteAddRq>", request_xml)
        self.assertIn(
            "<QuoteAdd><ID></ID><Reference>Q-100</Reference><QuotedBy>estimator</QuotedBy><DueDate>2026-05-01</DueDate><Status>Active</Status></QuoteAdd>",
            request_xml,
        )
        self.assertIn(
            '<QuoteSetUpCustomerInfo><CustomerRef ID="ACME" /><OverrideCreditLimit>false</OverrideCreditLimit><CountactRef ID="18" /></QuoteSetUpCustomerInfo>',
            request_xml,
        )
        self.assertNotIn("<CustomerRef>ACME</CustomerRef>", request_xml)
        self.assertNotIn("<ContactRef", request_xml)
        self.assertLess(
            request_xml.index('<CustomerRef ID="ACME" />'),
            request_xml.index("<OverrideCreditLimit>false</OverrideCreditLimit>"),
        )
        self.assertLess(
            request_xml.index("<OverrideCreditLimit>false</OverrideCreditLimit>"),
            request_xml.index('<CountactRef ID="18" />'),
        )
        self.assertLess(
            request_xml.index("</QuoteSetUpCustomerInfo>"),
            request_xml.index("<QuoteLineItemAdd>"),
        )
        self.assertIn("<LineItemID>001</LineItemID>", request_xml)
        self.assertEqual(fake_connection.cursor_obj.last_jobboss_params[11], "uploader")
        self.assertTrue(
            any("JobBOSS QuoteAddRq preview XML:" in message for message in captured_logs.output)
        )


if __name__ == "__main__":
    unittest.main()
