from __future__ import annotations

import logging
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Iterable
from zipfile import BadZipFile, ZipFile

from src.services.bom_workbook import BomWorkbookError, extract_bom_identity
from src.utils.path_safety import (
    PathValidationError,
    sanitize_customer_folder_name,
    sanitize_part_folder_name,
    sanitize_processed_filename,
    validate_upload_filename,
)

logger = logging.getLogger(__name__)


class DocumentIntakeError(Exception):
    pass


@dataclass(frozen=True)
class UploadedFile:
    filename: str
    content: bytes


@dataclass(frozen=True)
class DocumentIntakeResult:
    customer_name: str
    rfq_number: str
    sanitized_customer_folder_name: str
    sanitized_rfq_folder_name: str
    automation_path: Path
    working_path: Path
    uploaded_files_count: int
    processed_files: list[str] = field(default_factory=list)
    extension_summary: dict[str, int] = field(default_factory=dict)


class DocumentIntakeService:
    def __init__(self, automation_drop_root: Path, work_root: Path) -> None:
        self._automation_drop_root = automation_drop_root
        self._work_root = work_root

    def intake_documents(
        self,
        customer_name: str,
        rfq_number: str,
        uploaded_files: Iterable[UploadedFile],
    ) -> DocumentIntakeResult:
        normalized_customer_name = customer_name.strip()
        if not normalized_customer_name:
            raise DocumentIntakeError("Customer is required.")

        normalized_rfq_number = rfq_number.strip()
        if not normalized_rfq_number:
            raise DocumentIntakeError("RFQ Number is required.")

        files = list(uploaded_files)
        if not files:
            raise DocumentIntakeError("At least one file is required.")

        try:
            sanitized_customer_folder_name = sanitize_customer_folder_name(
                normalized_customer_name
            )
            sanitized_rfq_folder_name = sanitize_part_folder_name(
                f"RFQ-{normalized_rfq_number}"
            )
            processed_files = self._build_processed_files(files)
        except PathValidationError as exc:
            raise DocumentIntakeError(str(exc)) from exc

        automation_path = (
            self._automation_drop_root
            / sanitized_customer_folder_name
            / sanitized_rfq_folder_name
            / "package"
        )
        working_path = (
            self._work_root
            / sanitized_customer_folder_name
            / sanitized_rfq_folder_name
            / "package"
        )

        written_paths: list[Path] = []
        created_dirs: list[Path] = []

        try:
            self._ensure_directory(automation_path, created_dirs)
            self._ensure_directory(working_path, created_dirs)

            final_processed_files = self._resolve_mirrored_filenames(
                processed_files,
                automation_path,
                working_path,
            )
            for processed_file in final_processed_files:
                self._write_mirrored_file(
                    processed_file.content,
                    processed_file.filename,
                    automation_path,
                    working_path,
                    written_paths,
                )
        except Exception as exc:
            logger.exception(
                "Document intake processing failed for customer '%s' RFQ '%s'.",
                normalized_customer_name,
                normalized_rfq_number,
            )
            self._cleanup_written_files(written_paths)
            self._cleanup_created_directories(created_dirs)
            raise DocumentIntakeError(
                "Unable to process uploaded documents. No existing files were overwritten."
            ) from exc

        processed_filenames = sorted(
            (processed_file.filename for processed_file in final_processed_files),
            key=lambda value: (value.casefold(), value),
        )
        return DocumentIntakeResult(
            customer_name=normalized_customer_name,
            rfq_number=normalized_rfq_number,
            sanitized_customer_folder_name=sanitized_customer_folder_name,
            sanitized_rfq_folder_name=sanitized_rfq_folder_name,
            automation_path=automation_path,
            working_path=working_path,
            uploaded_files_count=len(files),
            processed_files=processed_filenames,
            extension_summary=self._summarize_extensions(processed_filenames),
        )

    def _build_processed_files(
        self, uploaded_files: list[UploadedFile]
    ) -> list[_ProcessedFile]:
        processed_files: list[_ProcessedFile] = []

        for uploaded_file in uploaded_files:
            validate_upload_filename(uploaded_file.filename)

            if self._is_zip_upload(uploaded_file.filename):
                processed_files.extend(self._extract_zip_file(uploaded_file))
                continue

            processed_files.append(
                _ProcessedFile(
                    filename=self._resolve_processed_filename(
                        uploaded_file.filename,
                        uploaded_file.content,
                    ),
                    content=uploaded_file.content,
                )
            )

        if not processed_files:
            raise DocumentIntakeError("No processable files were found in the upload.")

        return processed_files

    def _extract_zip_file(self, uploaded_file: UploadedFile) -> list[_ProcessedFile]:
        try:
            archive = ZipFile(BytesIO(uploaded_file.content))
        except BadZipFile as exc:
            raise DocumentIntakeError(
                f"Uploaded zip file is invalid or corrupt: {uploaded_file.filename}"
            ) from exc

        extracted_files: list[_ProcessedFile] = []

        with archive:
            for member in archive.infolist():
                if member.is_dir() or self._should_ignore_zip_member(member.filename):
                    continue

                flattened_name = self._flatten_zip_member_name(member.filename)
                if not flattened_name:
                    continue

                try:
                    validate_upload_filename(flattened_name)
                    file_content = archive.read(member)
                except PathValidationError as exc:
                    raise DocumentIntakeError(str(exc)) from exc
                except BadZipFile as exc:
                    raise DocumentIntakeError(
                        f"Uploaded zip file is invalid or corrupt: {uploaded_file.filename}"
                    ) from exc

                extracted_files.append(
                    _ProcessedFile(
                        filename=self._resolve_processed_filename(
                            flattened_name,
                            file_content,
                        ),
                        content=file_content,
                    )
                )

        if not extracted_files:
            raise DocumentIntakeError(
                f"Uploaded zip file contains no files after flattening: {uploaded_file.filename}"
            )

        return extracted_files

    def _resolve_processed_filename(self, filename: str, content: bytes) -> str:
        if not self._is_bom_workbook_candidate(filename):
            return sanitize_processed_filename(filename)

        try:
            bom_identity = extract_bom_identity(filename, content)
        except BomWorkbookError as exc:
            raise DocumentIntakeError(
                f"Unable to derive BOM filename from workbook '{filename}': {exc}"
            ) from exc

        return sanitize_processed_filename(bom_identity.filename)

    def _resolve_mirrored_filenames(
        self,
        processed_files: list[_ProcessedFile],
        automation_path: Path,
        working_path: Path,
    ) -> list[_ProcessedFile]:
        reserved_names: set[str] = set()
        final_processed_files: list[_ProcessedFile] = []

        for processed_file in processed_files:
            resolved_name = self._resolve_available_filename(
                processed_file.filename,
                automation_path,
                working_path,
                reserved_names,
            )
            reserved_names.add(resolved_name.casefold())
            final_processed_files.append(
                _ProcessedFile(filename=resolved_name, content=processed_file.content)
            )

        return final_processed_files

    def _resolve_available_filename(
        self,
        filename: str,
        automation_path: Path,
        working_path: Path,
        reserved_names: set[str],
    ) -> str:
        candidate_path = Path(filename)
        stem = candidate_path.stem or candidate_path.name
        suffix = candidate_path.suffix
        candidate_name = candidate_path.name
        suffix_index = 0

        while True:
            candidate_key = candidate_name.casefold()
            if candidate_key not in reserved_names and not self._exists_in_either_destination(
                candidate_name,
                automation_path,
                working_path,
            ):
                return candidate_name

            suffix_index += 1
            candidate_name = sanitize_processed_filename(
                f"{stem}_{suffix_index}{suffix}"
            )

    @staticmethod
    def _exists_in_either_destination(
        filename: str,
        automation_path: Path,
        working_path: Path,
    ) -> bool:
        return (automation_path / filename).exists() or (working_path / filename).exists()

    def _write_mirrored_file(
        self,
        content: bytes,
        filename: str,
        automation_path: Path,
        working_path: Path,
        written_paths: list[Path],
    ) -> None:
        automation_destination = automation_path / filename
        working_destination = working_path / filename

        self._copy_to_destination(content, automation_destination)
        written_paths.append(automation_destination)

        self._copy_to_destination(content, working_destination)
        written_paths.append(working_destination)

    @staticmethod
    def _is_zip_upload(filename: str) -> bool:
        return filename.lower().endswith(".zip")

    @staticmethod
    def _is_bom_workbook_candidate(filename: str) -> bool:
        normalized_filename = filename.lower()
        return (
            normalized_filename.endswith((".xlsx", ".xls"))
            and "bom" in normalized_filename
        )

    @staticmethod
    def _should_ignore_zip_member(member_name: str) -> bool:
        normalized_name = member_name.replace("\\", "/").strip("/")
        if not normalized_name:
            return True

        parts = [part for part in normalized_name.split("/") if part]
        return any(part == "__MACOSX" for part in parts)

    @staticmethod
    def _flatten_zip_member_name(member_name: str) -> str:
        normalized_name = member_name.replace("\\", "/").strip("/")
        return normalized_name.rsplit("/", 1)[-1].strip()

    @staticmethod
    def _copy_to_destination(content: bytes, destination: Path) -> None:
        with destination.open("xb") as target_file:
            target_file.write(content)

    @staticmethod
    def _ensure_directory(path: Path, created_dirs: list[Path]) -> None:
        paths_to_create: list[Path] = []
        current_path = path

        while not current_path.exists():
            paths_to_create.append(current_path)
            parent = current_path.parent
            if parent == current_path:
                break
            current_path = parent

        for directory in reversed(paths_to_create):
            directory.mkdir(exist_ok=True)
            created_dirs.append(directory)

    @staticmethod
    def _cleanup_written_files(written_paths: list[Path]) -> None:
        for path in reversed(written_paths):
            try:
                if path.exists():
                    path.unlink()
            except OSError:
                logger.exception("Failed to remove partially copied file '%s'.", path)

    @staticmethod
    def _cleanup_created_directories(created_dirs: list[Path]) -> None:
        for path in reversed(created_dirs):
            try:
                path.rmdir()
            except OSError:
                continue

    @staticmethod
    def _summarize_extensions(processed_filenames: list[str]) -> dict[str, int]:
        counts_by_extension: dict[str, int] = {}

        for filename in processed_filenames:
            suffix = Path(filename).suffix.lower()
            extension = suffix if suffix else "[no extension]"
            counts_by_extension[extension] = counts_by_extension.get(extension, 0) + 1

        return dict(sorted(counts_by_extension.items()))


@dataclass(frozen=True)
class _ProcessedFile:
    filename: str
    content: bytes
