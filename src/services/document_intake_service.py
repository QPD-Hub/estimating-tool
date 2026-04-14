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
    sanitize_processed_filename,
    sanitize_top_level_part_folder_name,
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
class ProcessedFileResult:
    filename: str
    size_bytes: int


@dataclass(frozen=True)
class PartDestinationResult:
    part_name: str
    sanitized_part_folder_name: str
    automation_path: Path
    working_path: Path


@dataclass(frozen=True)
class FailedFileResult:
    filename: str
    reason: str


@dataclass(frozen=True)
class DocumentIntakeResult:
    customer_name: str
    sanitized_customer_folder_name: str
    top_level_parts: list[str]
    automation_customer_path: Path
    working_customer_path: Path
    part_destinations: list[PartDestinationResult] = field(default_factory=list)
    processed_files: list[ProcessedFileResult] = field(default_factory=list)
    copied_file_count: int = 0
    failed_files: list[FailedFileResult] = field(default_factory=list)


class DocumentIntakeService:
    def __init__(self, automation_drop_root: Path, work_root: Path) -> None:
        self._automation_drop_root = automation_drop_root
        self._work_root = work_root

    def intake_documents(
        self,
        customer_name: str,
        top_level_parts: Iterable[str],
        uploaded_files: Iterable[UploadedFile],
    ) -> DocumentIntakeResult:
        normalized_customer_name = customer_name.strip()
        if not normalized_customer_name:
            raise DocumentIntakeError("Customer is required.")

        normalized_parts = [part.strip() for part in top_level_parts if part.strip()]
        if not normalized_parts:
            raise DocumentIntakeError("At least one Top Level Part is required.")

        files = list(uploaded_files)
        if not files:
            raise DocumentIntakeError("At least one file is required.")

        try:
            customer_folder_name = sanitize_customer_folder_name(normalized_customer_name)
            sanitized_parts = self._sanitize_top_level_parts(normalized_parts)
            processed_files = self._build_processed_files(files)
        except PathValidationError as exc:
            raise DocumentIntakeError(str(exc)) from exc

        automation_customer_path = self._automation_drop_root / customer_folder_name
        working_customer_path = self._work_root / customer_folder_name
        part_destinations = [
            PartDestinationResult(
                part_name=part_name,
                sanitized_part_folder_name=sanitized_part_name,
                automation_path=automation_customer_path / sanitized_part_name,
                working_path=working_customer_path / sanitized_part_name,
            )
            for part_name, sanitized_part_name in zip(normalized_parts, sanitized_parts)
        ]

        self._validate_destinations(processed_files, part_destinations)

        written_paths: list[Path] = []
        created_dirs: list[Path] = []

        try:
            for destination in part_destinations:
                self._ensure_directory(destination.automation_path, created_dirs)
                self._ensure_directory(destination.working_path, created_dirs)

                for processed_file in processed_files:
                    automation_destination = destination.automation_path / processed_file.filename
                    working_destination = destination.working_path / processed_file.filename

                    self._copy_to_destination(
                        processed_file.content,
                        automation_destination,
                    )
                    written_paths.append(automation_destination)

                    self._copy_to_destination(
                        processed_file.content,
                        working_destination,
                    )
                    written_paths.append(working_destination)
        except Exception as exc:
            logger.exception(
                "Document intake copy failed for customer '%s'.",
                normalized_customer_name,
            )
            self._cleanup_written_files(written_paths)
            self._cleanup_created_directories(created_dirs)
            raise DocumentIntakeError(
                "Unable to copy processed files. No files were overwritten."
            ) from exc

        return DocumentIntakeResult(
            customer_name=normalized_customer_name,
            sanitized_customer_folder_name=customer_folder_name,
            top_level_parts=sanitized_parts,
            automation_customer_path=automation_customer_path,
            working_customer_path=working_customer_path,
            part_destinations=part_destinations,
            processed_files=[
                ProcessedFileResult(
                    filename=processed_file.filename,
                    size_bytes=len(processed_file.content),
                )
                for processed_file in processed_files
            ],
            copied_file_count=len(processed_files) * len(part_destinations) * 2,
            failed_files=[],
        )

    def _sanitize_top_level_parts(self, top_level_parts: list[str]) -> list[str]:
        sanitized_parts: list[str] = []
        seen_parts: set[str] = set()

        for part_name in top_level_parts:
            sanitized_part_name = sanitize_top_level_part_folder_name(part_name)
            if sanitized_part_name in seen_parts:
                raise DocumentIntakeError(
                    f"Duplicate Top Level Part detected after sanitization: {sanitized_part_name}"
                )
            seen_parts.add(sanitized_part_name)
            sanitized_parts.append(sanitized_part_name)

        return sanitized_parts

    def _build_processed_files(
        self, uploaded_files: list[UploadedFile]
    ) -> list[_ProcessedFile]:
        processed_files: list[_ProcessedFile] = []
        seen_filenames: set[str] = set()

        for uploaded_file in uploaded_files:
            if self._is_zip_upload(uploaded_file.filename):
                extracted_files = self._extract_zip_file(uploaded_file)
                for extracted_file in extracted_files:
                    self._append_processed_file(
                        processed_files,
                        seen_filenames,
                        extracted_file.filename,
                        extracted_file.content,
                    )
                continue

            self._append_processed_file(
                processed_files,
                seen_filenames,
                uploaded_file.filename,
                uploaded_file.content,
            )

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
                if member.is_dir():
                    continue

                flattened_name = self._flatten_zip_member_name(member.filename)
                if not flattened_name:
                    continue

                try:
                    file_content = archive.read(member)
                except BadZipFile as exc:
                    raise DocumentIntakeError(
                        f"Uploaded zip file is invalid or corrupt: {uploaded_file.filename}"
                    ) from exc

                extracted_files.append(
                    _ProcessedFile(filename=flattened_name, content=file_content)
                )

        if not extracted_files:
            raise DocumentIntakeError(
                f"Uploaded zip file contains no files after flattening: {uploaded_file.filename}"
            )

        return extracted_files

    def _append_processed_file(
        self,
        processed_files: list[_ProcessedFile],
        seen_filenames: set[str],
        filename: str,
        content: bytes,
    ) -> None:
        validate_upload_filename(filename)
        processed_filename = self._resolve_processed_filename(filename, content)

        if processed_filename in seen_filenames:
            raise DocumentIntakeError(
                "Duplicate processed filename detected after flattening: "
                f"{processed_filename}"
            )

        seen_filenames.add(processed_filename)
        processed_files.append(
            _ProcessedFile(filename=processed_filename, content=content)
        )

    def _resolve_processed_filename(self, filename: str, content: bytes) -> str:
        if not self._is_bom_workbook_candidate(filename):
            return filename

        try:
            bom_identity = extract_bom_identity(content)
        except BomWorkbookError as exc:
            raise DocumentIntakeError(
                f"Unable to derive BOM filename from workbook '{filename}': {exc}"
            ) from exc

        return sanitize_processed_filename(bom_identity.filename)

    def _validate_destinations(
        self,
        processed_files: list[_ProcessedFile],
        part_destinations: list[PartDestinationResult],
    ) -> None:
        for destination in part_destinations:
            for processed_file in processed_files:
                automation_destination = destination.automation_path / processed_file.filename
                working_destination = destination.working_path / processed_file.filename

                if automation_destination.exists():
                    raise DocumentIntakeError(
                        "File already exists in automation destination: "
                        f"{automation_destination}"
                    )

                if working_destination.exists():
                    raise DocumentIntakeError(
                        f"File already exists in working destination: {working_destination}"
                    )

    @staticmethod
    def _is_zip_upload(filename: str) -> bool:
        return filename.lower().endswith(".zip")

    @staticmethod
    def _is_bom_workbook_candidate(filename: str) -> bool:
        normalized_filename = filename.lower()
        return normalized_filename.endswith(".xlsx") and "bom" in normalized_filename

    @staticmethod
    def _flatten_zip_member_name(member_name: str) -> str:
        normalized_name = member_name.replace("\\", "/")
        flattened_name = normalized_name.rsplit("/", 1)[-1].strip()
        return flattened_name

    @staticmethod
    def _copy_to_destination(content: bytes, destination: Path) -> None:
        with destination.open("xb") as target_file:
            target_file.write(content)

    @staticmethod
    def _ensure_directory(path: Path, created_dirs: list[Path]) -> None:
        if path.exists():
            return

        path.mkdir(parents=True, exist_ok=True)
        created_dirs.append(path)

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


@dataclass(frozen=True)
class _ProcessedFile:
    filename: str
    content: bytes
