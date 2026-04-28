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


if __name__ == "__main__":
    unittest.main()
