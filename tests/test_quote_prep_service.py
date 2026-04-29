import unittest
import xml.etree.ElementTree as ET

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
        root = ET.fromstring(request_xml)
        request_node = root.find("JBXMLRequest")
        self.assertIsNotNone(request_node)
        quote_add_rq = request_node.find("QuoteAddRq")
        self.assertIsNotNone(quote_add_rq)

        quote_add = quote_add_rq.find("QuoteAdd")
        self.assertIsNotNone(quote_add)
        self.assertEqual(
            [child.tag for child in quote_add],
            ["ID", "Reference", "QuotedBy", "DueDate", "Status"],
        )
        self.assertEqual(quote_add.findtext("ID"), "")
        self.assertEqual(quote_add.findtext("Reference"), "Q-100")
        self.assertEqual(quote_add.findtext("QuotedBy"), "estimator")
        self.assertEqual(quote_add.findtext("DueDate"), "2026-05-01")
        self.assertEqual(quote_add.findtext("Status"), "Active")

        quote_setup = quote_add_rq.find("QuoteSetUpCustomerInfo")
        self.assertIsNotNone(quote_setup)
        self.assertEqual(
            [child.tag for child in quote_setup],
            ["CustomerRef", "OverrideCreditLimit", "CountactRef"],
        )
        self.assertEqual(quote_setup.find("CustomerRef").attrib.get("ID"), "ACME")
        self.assertEqual(quote_setup.findtext("OverrideCreditLimit"), "false")
        self.assertEqual(quote_setup.find("CountactRef").attrib.get("ID"), "18")

        line_add = quote_add_rq.find("QuoteLineItemAdd")
        self.assertIsNotNone(line_add)
        self.assertEqual(
            [child.tag for child in line_add],
            [
                "LineItemID",
                "LineNumber",
                "PartNumber",
                "PartDescription",
                "PartRevision",
                "UsePartMaster",
            ],
        )
        self.assertEqual(line_add.findtext("LineItemID"), "001")
        self.assertEqual(line_add.findtext("LineNumber"), "001")
        self.assertEqual(line_add.findtext("PartNumber"), "ASM-1000")
        self.assertEqual(line_add.findtext("PartDescription"), "Top Assembly")
        self.assertEqual(line_add.findtext("PartRevision"), "A")
        self.assertEqual(line_add.findtext("UsePartMaster"), "false")
        self.assertIsNone(line_add.find("QuotedBy"))

        qty_add = quote_add_rq.find("QuoteQuantityAdd")
        self.assertIsNotNone(qty_add)
        self.assertEqual([child.tag for child in qty_add], ["LineItemID", "QuotedQuantity"])
        self.assertEqual(qty_add.findtext("LineItemID"), "001")
        self.assertEqual(qty_add.findtext("QuotedQuantity"), "1")

        self.assertEqual(fake_connection.cursor_obj.last_jobboss_params[11], "uploader")
        self.assertTrue(
            any("JobBOSS QuoteAddRq preview XML:" in message for message in captured_logs.output)
        )

    def test_quote_builder_omits_blank_optional_elements(self) -> None:
        service = QuotePrepService.__new__(QuotePrepService)
        xml_text = service._build_quote_add_xml(
            intake_row={
                "QuoteNumber": "Q-200",
                "QuotedBy": " ",
                "QuoteDueDate": None,
                "CustomerName": "ACME",
            },
            quote_lines=[
                {
                    "lineItemId": "001",
                    "lineNumber": "001",
                    "partNumber": "0241-75453",
                    "description": "",
                    "revision": "",
                    "quantities": [1],
                }
            ],
            contact_ref_id=None,
        )

        root = ET.fromstring(xml_text)
        quote_add_rq = root.find("./JBXMLRequest/QuoteAddRq")
        self.assertIsNotNone(quote_add_rq)

        quote_add = quote_add_rq.find("QuoteAdd")
        self.assertEqual([child.tag for child in quote_add], ["ID", "Reference", "Status"])

        quote_setup = quote_add_rq.find("QuoteSetUpCustomerInfo")
        self.assertEqual([child.tag for child in quote_setup], ["CustomerRef", "OverrideCreditLimit"])

        line_add = quote_add_rq.find("QuoteLineItemAdd")
        self.assertEqual(
            [child.tag for child in line_add],
            ["LineItemID", "LineNumber", "PartNumber", "UsePartMaster"],
        )


if __name__ == "__main__":
    unittest.main()
