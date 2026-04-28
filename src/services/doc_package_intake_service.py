from __future__ import annotations

from dataclasses import dataclass

from src.services.bom_intake_service import (
    BomIntakePreview,
    BomIntakeRequestError,
    BomIntakeService,
)
from src.services.bom_package_locator import PREFERRED_KEYWORDS, SPREADSHEET_SUFFIXES
from src.services.document_intake_service import (
    DocumentIntakeError,
    DocumentIntakeResult,
    DocumentIntakeService,
    UploadedFile,
)

BOM_UPLOAD_SUFFIXES = SPREADSHEET_SUFFIXES + (".zip",)


class DocPackageIntakeError(Exception):
    def __init__(
        self,
        message: str,
        *,
        document_result: DocumentIntakeResult | None = None,
        diagnostics: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.document_result = document_result
        self.diagnostics = diagnostics


@dataclass(frozen=True)
class DocPackageIntakeResult:
    customer_name: str
    rfq_number: str
    uploaded_by: str
    uploaded_files_count: int
    selected_bom_file_name: str
    document_result: DocumentIntakeResult
    bom_preview: BomIntakePreview
    bom_result: dict[str, object]
    detected_roots: list[dict[str, str | None]]
    intake_notes: str | None = None


class DocPackageIntakeService:
    def __init__(
        self,
        document_intake_service: DocumentIntakeService,
        bom_intake_service: BomIntakeService,
    ) -> None:
        self._document_intake_service = document_intake_service
        self._bom_intake_service = bom_intake_service

    def intake_package(
        self,
        *,
        customer_name: str,
        rfq_number: str,
        uploaded_by: str,
        uploaded_files: list[UploadedFile],
        intake_notes: str | None = None,
    ) -> DocPackageIntakeResult:
        normalized_uploaded_by = uploaded_by.strip()
        if not normalized_uploaded_by:
            raise DocPackageIntakeError("Uploaded By is required.")

        if not uploaded_files:
            raise DocPackageIntakeError("At least one file is required.")

        bom_upload, bom_preview = self._select_bom_upload(
            customer_name=customer_name,
            uploaded_by=normalized_uploaded_by,
            rfq_number=rfq_number,
            intake_notes=intake_notes,
            uploaded_files=uploaded_files,
        )

        try:
            document_result = self._document_intake_service.intake_documents(
                customer_name=customer_name,
                rfq_number=rfq_number,
                uploaded_files=uploaded_files,
            )
        except DocumentIntakeError as exc:
            raise DocPackageIntakeError(str(exc)) from exc

        try:
            bom_result = self._bom_intake_service.process_uploaded_bom(
                header_data=self._build_bom_header(
                    customer_name=customer_name,
                    uploaded_by=normalized_uploaded_by,
                    rfq_number=rfq_number,
                    intake_notes=intake_notes,
                    bom_file_name=bom_upload.filename,
                ),
                upload_data={
                    "filename": bom_upload.filename,
                    "content": bom_upload.content,
                },
            )
        except Exception as exc:
            raise DocPackageIntakeError(
                f"Document files were processed, but BOM intake failed: {exc}",
                document_result=document_result,
            ) from exc

        detected_roots = _extract_detected_roots(bom_result)
        return DocPackageIntakeResult(
            customer_name=customer_name.strip(),
            rfq_number=rfq_number.strip(),
            uploaded_by=normalized_uploaded_by,
            uploaded_files_count=len(uploaded_files),
            selected_bom_file_name=bom_upload.filename,
            document_result=document_result,
            bom_preview=bom_preview,
            bom_result=bom_result,
            detected_roots=detected_roots,
            intake_notes=intake_notes.strip() if intake_notes and intake_notes.strip() else None,
        )

    def preview_package_bom(
        self,
        *,
        customer_name: str,
        uploaded_by: str,
        uploaded_files: list[UploadedFile],
        rfq_number: str,
        intake_notes: str | None = None,
    ) -> BomIntakePreview:
        normalized_uploaded_by = uploaded_by.strip()
        if not normalized_uploaded_by:
            raise DocPackageIntakeError("Uploaded By is required.")
        if not uploaded_files:
            raise DocPackageIntakeError("At least one file is required.")

        _, preview = self._select_bom_upload(
            customer_name=customer_name,
            uploaded_by=normalized_uploaded_by,
            rfq_number=rfq_number,
            intake_notes=intake_notes,
            uploaded_files=uploaded_files,
        )
        return preview

    def _select_bom_upload(
        self,
        *,
        customer_name: str,
        uploaded_by: str,
        rfq_number: str,
        intake_notes: str | None,
        uploaded_files: list[UploadedFile],
    ) -> tuple[UploadedFile, BomIntakePreview]:
        bom_candidates = sorted(
            (
                uploaded_file
                for uploaded_file in uploaded_files
                if uploaded_file.filename.lower().endswith(BOM_UPLOAD_SUFFIXES)
            ),
            key=lambda uploaded_file: (
                -_score_bom_upload_candidate(uploaded_file.filename),
                uploaded_file.filename.casefold(),
            ),
        )

        if not bom_candidates:
            raise DocPackageIntakeError(
                "Upload at least one BOM workbook or zip package as part of the document package."
            )

        last_error: BomIntakeRequestError | None = None
        for bom_candidate in bom_candidates:
            try:
                preview = self._bom_intake_service.preview_uploaded_bom(
                    header_data=self._build_bom_header(
                        customer_name=customer_name,
                        uploaded_by=uploaded_by,
                        rfq_number=rfq_number,
                        intake_notes=intake_notes,
                        bom_file_name=bom_candidate.filename,
                    ),
                    upload_data={
                        "filename": bom_candidate.filename,
                        "content": bom_candidate.content,
                    },
                )
                return bom_candidate, preview
            except BomIntakeRequestError as exc:
                last_error = exc

        if last_error is not None:
            raise DocPackageIntakeError(
                str(last_error),
                diagnostics=last_error.diagnostics,
            ) from last_error

        raise DocPackageIntakeError(
            "No BOM workbook could be resolved from the uploaded document package."
        )

    @staticmethod
    def _build_bom_header(
        *,
        customer_name: str,
        uploaded_by: str,
        rfq_number: str,
        intake_notes: str | None,
        bom_file_name: str,
    ) -> dict[str, object]:
        return {
            "customer_name": customer_name.strip(),
            "uploaded_by": uploaded_by.strip(),
            "quote_number": rfq_number.strip() if rfq_number and rfq_number.strip() else None,
            "intake_notes": intake_notes.strip() if intake_notes and intake_notes.strip() else None,
            "source_file_name": bom_file_name,
        }


def _extract_detected_roots(bom_result: dict[str, object]) -> list[dict[str, str | None]]:
    root_results = bom_result.get("RootResults")
    if not isinstance(root_results, list):
        return []
    detected_roots: list[dict[str, str | None]] = []
    for root in root_results:
        if not isinstance(root, dict):
            continue
        detected_roots.append(
            {
                "part_number": _to_optional_text(root.get("Level0PartNumber")),
                "revision": _to_optional_text(root.get("Revision")),
                "decisionStatus": _to_optional_text(root.get("DecisionStatus")),
            }
        )
    return detected_roots


def _to_optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _score_bom_upload_candidate(filename: str) -> int:
    normalized_filename = filename.strip().lower()
    score = 0

    for keyword in PREFERRED_KEYWORDS:
        if keyword in normalized_filename:
            score += 10

    if normalized_filename.endswith(".xlsx"):
        score += 5
    elif normalized_filename.endswith(".xls"):
        score += 1

    return score
