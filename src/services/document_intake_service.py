from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from src.utils.path_safety import (
    PathValidationError,
    sanitize_customer_folder_name,
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
class CopiedFileResult:
    filename: str
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
    automation_path: Path
    working_path: Path
    copied_files: list[CopiedFileResult] = field(default_factory=list)
    failed_files: list[FailedFileResult] = field(default_factory=list)


class DocumentIntakeService:
    def __init__(self, automation_drop_root: Path, work_root: Path) -> None:
        self._automation_drop_root = automation_drop_root
        self._work_root = work_root

    def intake_documents(
        self, customer_name: str, uploaded_files: Iterable[UploadedFile]
    ) -> DocumentIntakeResult:
        normalized_customer_name = customer_name.strip()
        if not normalized_customer_name:
            raise DocumentIntakeError("Customer is required.")

        files = list(uploaded_files)
        if not files:
            raise DocumentIntakeError("At least one file is required.")

        try:
            customer_folder_name = sanitize_customer_folder_name(normalized_customer_name)
        except PathValidationError as exc:
            raise DocumentIntakeError(str(exc)) from exc

        automation_path = self._automation_drop_root / customer_folder_name
        working_path = self._work_root / customer_folder_name

        self._validate_files(files, automation_path, working_path)

        written_paths: list[Path] = []
        copied_files: list[CopiedFileResult] = []

        try:
            automation_path.mkdir(parents=True, exist_ok=True)
            working_path.mkdir(parents=True, exist_ok=True)

            for file in files:
                automation_destination = automation_path / file.filename
                working_destination = working_path / file.filename

                self._copy_to_destination(file.content, automation_destination)
                written_paths.append(automation_destination)

                self._copy_to_destination(file.content, working_destination)
                written_paths.append(working_destination)

                copied_files.append(
                    CopiedFileResult(
                        filename=file.filename,
                        automation_path=automation_destination,
                        working_path=working_destination,
                    )
                )
        except Exception as exc:
            logger.exception(
                "Document intake copy failed for customer '%s'.",
                normalized_customer_name,
            )
            self._cleanup_written_files(written_paths)
            raise DocumentIntakeError(
                "Unable to copy uploaded files. No files were overwritten."
            ) from exc

        return DocumentIntakeResult(
            customer_name=normalized_customer_name,
            sanitized_customer_folder_name=customer_folder_name,
            automation_path=automation_path,
            working_path=working_path,
            copied_files=copied_files,
            failed_files=[],
        )

    def _validate_files(
        self, files: list[UploadedFile], automation_path: Path, working_path: Path
    ) -> None:
        seen_filenames: set[str] = set()

        for file in files:
            try:
                validate_upload_filename(file.filename)
            except PathValidationError as exc:
                raise DocumentIntakeError(str(exc)) from exc

            if file.filename in seen_filenames:
                raise DocumentIntakeError(
                    f"Duplicate uploaded filename detected: {file.filename}"
                )

            seen_filenames.add(file.filename)

            automation_destination = automation_path / file.filename
            working_destination = working_path / file.filename

            if automation_destination.exists() or working_destination.exists():
                raise DocumentIntakeError(
                    f"File already exists for customer folder: {file.filename}"
                )

    @staticmethod
    def _copy_to_destination(content: bytes, destination: Path) -> None:
        with destination.open("xb") as target_file:
            target_file.write(content)

    @staticmethod
    def _cleanup_written_files(written_paths: list[Path]) -> None:
        for path in reversed(written_paths):
            try:
                if path.exists():
                    path.unlink()
            except OSError:
                logger.exception("Failed to remove partially copied file '%s'.", path)
