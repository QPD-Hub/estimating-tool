import io
import sys
import tempfile
import types
import unittest
import zipfile
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

from src.services.document_intake_service import DocumentIntakeError, DocumentIntakeService, UploadedFile


class DocumentIntakeServiceTests(unittest.TestCase):
    def test_intake_documents_writes_matching_outputs_to_both_roots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            service = DocumentIntakeService(
                automation_drop_root=temp_path / "automation",
                work_root=temp_path / "work",
            )

            result = service.intake_documents(
                customer_name="ACME / West",
                rfq_number="100:01",
                uploaded_files=[
                    UploadedFile(filename="drawing.pdf", content=b"pdf"),
                    UploadedFile(
                        filename="package.zip",
                        content=_build_zip_bytes(
                            {
                                "nested/spec.pdf": b"spec",
                                "__MACOSX/._ignored": b"junk",
                                "nested/README": b"readme",
                            }
                        ),
                    ),
                ],
            )

            self.assertEqual(result.sanitized_customer_folder_name, "ACME West")
            self.assertEqual(result.sanitized_rfq_folder_name, "RFQ-100 01")
            self.assertEqual(result.uploaded_files_count, 2)
            self.assertEqual(
                result.processed_files,
                ["drawing.pdf", "README", "spec.pdf"],
            )
            self.assertEqual(
                result.extension_summary,
                {".pdf": 2, "[no extension]": 1},
            )

            for filename in result.processed_files:
                automation_file = result.automation_path / filename
                working_file = result.working_path / filename
                self.assertTrue(automation_file.exists())
                self.assertTrue(working_file.exists())
                self.assertEqual(automation_file.read_bytes(), working_file.read_bytes())

    def test_intake_documents_resolves_collisions_once_for_both_roots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            automation_path = temp_path / "automation" / "ACME" / "RFQ-100" / "package"
            working_path = temp_path / "work" / "ACME" / "RFQ-100" / "package"
            automation_path.mkdir(parents=True)
            working_path.mkdir(parents=True)
            (automation_path / "drawing.pdf").write_bytes(b"existing-automation")
            (working_path / "drawing_1.pdf").write_bytes(b"existing-work")

            service = DocumentIntakeService(
                automation_drop_root=temp_path / "automation",
                work_root=temp_path / "work",
            )

            result = service.intake_documents(
                customer_name="ACME",
                rfq_number="100",
                uploaded_files=[
                    UploadedFile(filename="drawing.pdf", content=b"new"),
                ],
            )

            self.assertEqual(result.processed_files, ["drawing_2.pdf"])
            self.assertEqual(
                (automation_path / "drawing_2.pdf").read_bytes(),
                b"new",
            )
            self.assertEqual(
                (working_path / "drawing_2.pdf").read_bytes(),
                b"new",
            )

    def test_intake_documents_requires_rfq_number(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = DocumentIntakeService(
                automation_drop_root=Path(temp_dir) / "automation",
                work_root=Path(temp_dir) / "work",
            )

            with self.assertRaisesRegex(DocumentIntakeError, "RFQ Number is required."):
                service.intake_documents(
                    customer_name="ACME",
                    rfq_number="",
                    uploaded_files=[UploadedFile(filename="drawing.pdf", content=b"pdf")],
                )


def _build_zip_bytes(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return buffer.getvalue()
