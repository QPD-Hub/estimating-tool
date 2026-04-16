from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import PurePosixPath
from zipfile import BadZipFile, ZipFile


SPREADSHEET_SUFFIXES = (".xlsx", ".xls")
PREFERRED_KEYWORDS = ("bom", "bill", "parts", "assembly")


class BomPackageLocatorError(ValueError):
    pass


@dataclass(frozen=True)
class LocatedBomSpreadsheet:
    filename: str
    content: bytes
    source_type: str
    source_file_path: str | None = None
    archive_member_name: str | None = None


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

        selected = sorted(
            candidates,
            key=lambda candidate: (-candidate.score, candidate.filename.lower()),
        )[0]

        located_source_file_path = source_file_path
        if source_file_path:
            located_source_file_path = f"{source_file_path}!{selected.member_name}"

        return LocatedBomSpreadsheet(
            filename=selected.filename,
            content=selected.content,
            source_type="archive_upload",
            source_file_path=located_source_file_path,
            archive_member_name=selected.member_name,
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


def _is_safe_member_name(member_name: str) -> bool:
    path = PurePosixPath(member_name)
    if path.is_absolute():
        return False
    return all(part not in ("", ".", "..") for part in path.parts)


def _suffix(filename: str) -> str:
    return ("." + filename.rsplit(".", 1)[-1].lower()) if "." in filename else ""
