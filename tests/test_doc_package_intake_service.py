import sys
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

from src.services.bom_intake_service import BomIntakeRequestError
from src.services.doc_package_intake_service import (
    DocPackageIntakeError,
    DocPackageIntakeService,
)
from src.services.document_intake_service import DocumentIntakeResult, UploadedFile


class FakeDocumentIntakeService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def intake_documents(self, customer_name, part_number, uploaded_files):
        self.calls.append(
            {
                "customer_name": customer_name,
                "part_number": part_number,
                "uploaded_files": list(uploaded_files),
            }
        )
        return DocumentIntakeResult(
            customer_name=customer_name,
            part_number=part_number,
            sanitized_customer_folder_name="ACME",
            sanitized_part_folder_name="PART-100",
            automation_path=Path("/tmp/automation/ACME/PART-100"),
            working_path=Path("/tmp/work/ACME/PART-100"),
            uploaded_files_count=len(list(uploaded_files)),
            processed_files=["bom.xlsx", "drawing.pdf"],
            extension_summary={".pdf": 1, ".xlsx": 1},
        )


class FakeBomIntakeService:
    def __init__(self) -> None:
        self.preview_calls: list[dict[str, object]] = []
        self.process_calls: list[dict[str, object]] = []

    def preview_uploaded_bom(self, *, header_data, upload_data):
        self.preview_calls.append(
            {
                "header_data": header_data,
                "upload_data": upload_data,
            }
        )
        if "notes" in upload_data["filename"]:
            raise BomIntakeRequestError("No BOM header row could be detected.")
        return _FakePreview()

    def process_uploaded_bom(self, *, header_data, upload_data, dry_run=False):
        self.process_calls.append(
            {
                "header_data": header_data,
                "upload_data": upload_data,
                "dry_run": dry_run,
            }
        )
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


class DocPackageIntakeServiceTests(unittest.TestCase):
    def test_intake_package_uses_same_upload_set_for_docs_and_bom(self) -> None:
        document_service = FakeDocumentIntakeService()
        bom_service = FakeBomIntakeService()
        service = DocPackageIntakeService(document_service, bom_service)

        result = service.intake_package(
            customer_name="ACME",
            part_number="PART-100",
            uploaded_by="estimator",
            quote_number="Q-100",
            intake_notes="package intake",
            uploaded_files=[
                UploadedFile(filename="notes.xlsx", content=b"not a bom"),
                UploadedFile(filename="bom.xlsx", content=b"bom workbook"),
                UploadedFile(filename="drawing.pdf", content=b"pdf"),
            ],
        )

        self.assertEqual(result.selected_bom_file_name, "bom.xlsx")
        self.assertEqual(document_service.calls[0]["customer_name"], "ACME")
        self.assertEqual(document_service.calls[0]["part_number"], "PART-100")
        self.assertEqual(bom_service.process_calls[0]["upload_data"]["filename"], "bom.xlsx")
        self.assertEqual(bom_service.process_calls[0]["header_data"]["customer_name"], "ACME")
        self.assertEqual(result.bom_result["Summary"]["BomIntakeId"], 321)

    def test_intake_package_requires_bom_candidate_in_uploaded_documents(self) -> None:
        service = DocPackageIntakeService(
            FakeDocumentIntakeService(),
            FakeBomIntakeService(),
        )

        with self.assertRaisesRegex(
            DocPackageIntakeError,
            "Upload at least one BOM workbook or zip package",
        ):
            service.intake_package(
                customer_name="ACME",
                part_number="PART-100",
                uploaded_by="estimator",
                uploaded_files=[UploadedFile(filename="drawing.pdf", content=b"pdf")],
            )


class _FakePreview:
    detected_worksheet = "BOM"
    detected_source_type = "spreadsheet_upload"
