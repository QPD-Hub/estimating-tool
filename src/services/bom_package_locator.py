from __future__ import annotations

import logging
from dataclasses import dataclass
from io import BytesIO
from pathlib import PurePosixPath
from zipfile import BadZipFile, ZipFile


SPREADSHEET_SUFFIXES = (".xlsx", ".xls")
PREFERRED_KEYWORDS = ("bom", "bill", "parts", "assembly")

logger = logging.getLogger(__name__)


class BomPackageLocatorError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        diagnostics: BomPackageSelectionDiagnostics | None = None,
    ) -> None:
        super().__init__(message)
        self.diagnostics = diagnostics


@dataclass(frozen=True)
class ArchiveSpreadsheetCandidateDiagnostic:
    filename: str
    member_name: str
    score: int
    reasons: list[str]
    selected: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "filename": self.filename,
            "memberName": self.member_name,
            "score": self.score,
            "reasons": self.reasons,
            "selected": self.selected,
        }


@dataclass(frozen=True)
class BomPackageSelectionDiagnostics:
    source_file_name: str
    selected_archive_member_name: str | None
    selected_spreadsheet_filename: str | None
    selection_reason: str | None
    candidate_spreadsheets: list[ArchiveSpreadsheetCandidateDiagnostic]

    def to_dict(self) -> dict[str, object]:
        return {
            "sourceFileName": self.source_file_name,
            "selectedArchiveMemberName": self.selected_archive_member_name,
            "selectedSpreadsheetFilename": self.selected_spreadsheet_filename,
            "selectionReason": self.selection_reason,
            "candidateSpreadsheets": [
                candidate.to_dict() for candidate in self.candidate_spreadsheets
            ],
        }


@dataclass(frozen=True)
class LocatedBomSpreadsheet:
    filename: str
    content: bytes
    source_type: str
    source_file_path: str | None = None
    archive_member_name: str | None = None
    diagnostics: BomPackageSelectionDiagnostics | None = None


class BomPackageLocator:
    def locate(
        self,
        *,
        filename: str,
        content: bytes,
        source_file_path: str | None = None,
    ) -> LocatedBomSpreadsheet:
        normalized_filename = filename.strip()
        if not normalized_filename:
            raise BomPackageLocatorError("Uploaded filename is required.")

        suffix = _suffix(normalized_filename)
        if suffix in SPREADSHEET_SUFFIXES:
            return LocatedBomSpreadsheet(
                filename=normalized_filename,
                content=content,
                source_type="spreadsheet_upload",
                source_file_path=source_file_path,
                diagnostics=BomPackageSelectionDiagnostics(
                    source_file_name=normalized_filename,
                    selected_archive_member_name=None,
                    selected_spreadsheet_filename=normalized_filename,
                    selection_reason="Uploaded file is already a spreadsheet.",
                    candidate_spreadsheets=[],
                ),
            )

        try:
            archive = ZipFile(BytesIO(content))
        except BadZipFile as exc:
            raise BomPackageLocatorError(
                "No BOM spreadsheet was found. Upload a .xlsx, .xls, or zip package."
            ) from exc

        candidates: list[_ArchiveSpreadsheetCandidate] = []

        with archive:
            for member in archive.infolist():
                if member.is_dir():
                    continue

                member_name = member.filename.replace("\\", "/").strip()
                if not _is_safe_member_name(member_name):
                    continue

                flattened_name = PurePosixPath(member_name).name
                if _suffix(flattened_name) not in SPREADSHEET_SUFFIXES:
                    continue

                candidates.append(
                    _ArchiveSpreadsheetCandidate(
                        filename=flattened_name,
                        member_name=member_name,
                        content=archive.read(member),
                        score=_candidate_score(flattened_name),
                    )
                )

        if not candidates:
            raise BomPackageLocatorError(
                "No spreadsheet (.xlsx or .xls) was found in the uploaded package."
            )

        sorted_candidates = sorted(
            candidates,
            key=lambda candidate: (-candidate.score, candidate.filename.lower(), candidate.member_name.lower()),
        )
        selected = sorted_candidates[0]
        selected_reason = _selection_reason(selected)
        diagnostics = BomPackageSelectionDiagnostics(
            source_file_name=normalized_filename,
            selected_archive_member_name=selected.member_name,
            selected_spreadsheet_filename=selected.filename,
            selection_reason=selected_reason,
            candidate_spreadsheets=[
                ArchiveSpreadsheetCandidateDiagnostic(
                    filename=candidate.filename,
                    member_name=candidate.member_name,
                    score=candidate.score,
                    reasons=_candidate_reasons(candidate.filename),
                    selected=candidate.member_name == selected.member_name,
                )
                for candidate in sorted_candidates
            ],
        )
        logger.info(
            "Selected archive member '%s' for BOM parsing from '%s' (%s).",
            selected.member_name,
            normalized_filename,
            selected_reason,
        )

        located_source_file_path = source_file_path
        if source_file_path:
            located_source_file_path = f"{source_file_path}!{selected.member_name}"

        return LocatedBomSpreadsheet(
            filename=selected.filename,
            content=selected.content,
            source_type="archive_upload",
            source_file_path=located_source_file_path,
            archive_member_name=selected.member_name,
            diagnostics=diagnostics,
        )


@dataclass(frozen=True)
class _ArchiveSpreadsheetCandidate:
    filename: str
    member_name: str
    content: bytes
    score: int


def _candidate_score(filename: str) -> int:
    normalized = filename.lower()
    score = 0

    for keyword in PREFERRED_KEYWORDS:
        if keyword in normalized:
            score += 10

    if normalized.endswith(".xlsx"):
        score += 5
    elif normalized.endswith(".xls"):
        score += 1

    return score


def _candidate_reasons(filename: str) -> list[str]:
    normalized = filename.lower()
    reasons: list[str] = []

    for keyword in PREFERRED_KEYWORDS:
        if keyword in normalized:
            reasons.append(f"name contains '{keyword}'")

    if normalized.endswith(".xlsx"):
        reasons.append("prefers .xlsx")
    elif normalized.endswith(".xls"):
        reasons.append("uses .xls")

    return reasons or ["spreadsheet extension only"]


def _selection_reason(candidate: _ArchiveSpreadsheetCandidate) -> str:
    reasons = ", ".join(_candidate_reasons(candidate.filename))
    return f"Selected highest-ranked spreadsheet candidate (score={candidate.score}; {reasons})."


def _is_safe_member_name(member_name: str) -> bool:
    path = PurePosixPath(member_name)
    if path.is_absolute():
        return False
    return all(part not in ("", ".", "..") for part in path.parts)


def _suffix(filename: str) -> str:
    return ("." + filename.rsplit(".", 1)[-1].lower()) if "." in filename else ""
