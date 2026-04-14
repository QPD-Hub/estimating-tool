from __future__ import annotations

import re

WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}

INVALID_PATH_CHARS_PATTERN = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


class PathValidationError(ValueError):
    pass


def sanitize_customer_folder_name(customer_name: str) -> str:
    sanitized = INVALID_PATH_CHARS_PATTERN.sub(" ", customer_name.strip())
    sanitized = re.sub(r"\s+", " ", sanitized).strip(" .")

    if not sanitized:
        raise PathValidationError(
            "Customer name is invalid after removing illegal path characters."
        )

    if sanitized.upper() in WINDOWS_RESERVED_NAMES:
        raise PathValidationError(
            f"Customer folder name is not allowed on the filesystem: {sanitized}"
        )

    return sanitized


def validate_upload_filename(filename: str) -> None:
    if not filename or not filename.strip():
        raise PathValidationError("One of the uploaded files is missing a filename.")

    if filename in {".", ".."}:
        raise PathValidationError(f"Invalid uploaded filename: {filename}")

    if "/" in filename or "\\" in filename:
        raise PathValidationError(
            f"Uploaded filename contains path separators and cannot be copied as-is: {filename}"
        )

    if INVALID_PATH_CHARS_PATTERN.search(filename):
        raise PathValidationError(
            f"Uploaded filename contains illegal filesystem characters: {filename}"
        )

    trimmed = filename.strip()
    if trimmed != filename:
        raise PathValidationError(
            f"Uploaded filename has leading or trailing whitespace and cannot be copied as-is: {filename}"
        )

    if trimmed.rstrip(" .") != trimmed:
        raise PathValidationError(
            f"Uploaded filename has a trailing space or period and cannot be copied as-is: {filename}"
        )

    stem = trimmed.rsplit(".", 1)[0]
    if stem.upper() in WINDOWS_RESERVED_NAMES:
        raise PathValidationError(
            f"Uploaded filename is reserved by the filesystem and cannot be copied as-is: {filename}"
        )
