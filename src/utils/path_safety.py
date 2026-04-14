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
    return _sanitize_folder_name(
        customer_name,
        empty_message="Customer name is invalid after removing illegal path characters.",
        reserved_message_prefix="Customer folder name is not allowed on the filesystem",
    )


def sanitize_top_level_part_folder_name(part_name: str) -> str:
    return _sanitize_folder_name(
        part_name,
        empty_message="Top Level Part name is invalid after removing illegal path characters.",
        reserved_message_prefix="Top Level Part folder name is not allowed on the filesystem",
    )


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


def sanitize_processed_filename(filename: str) -> str:
    if not filename or not filename.strip():
        raise PathValidationError("Processed filename is invalid after sanitization.")

    sanitized = INVALID_PATH_CHARS_PATTERN.sub(" ", filename.strip())
    sanitized = re.sub(r"\s+", " ", sanitized).strip(" .")

    if not sanitized:
        raise PathValidationError("Processed filename is invalid after sanitization.")

    if "/" in sanitized or "\\" in sanitized:
        raise PathValidationError(
            f"Processed filename contains path separators after sanitization: {filename}"
        )

    stem = sanitized.rsplit(".", 1)[0]
    if stem.upper() in WINDOWS_RESERVED_NAMES:
        raise PathValidationError(
            f"Processed filename is reserved by the filesystem: {sanitized}"
        )

    return sanitized


def _sanitize_folder_name(
    value: str, empty_message: str, reserved_message_prefix: str
) -> str:
    sanitized = INVALID_PATH_CHARS_PATTERN.sub(" ", value.strip())
    sanitized = re.sub(r"\s+", " ", sanitized).strip(" .")

    if not sanitized:
        raise PathValidationError(empty_message)

    if sanitized.upper() in WINDOWS_RESERVED_NAMES:
        raise PathValidationError(
            f"{reserved_message_prefix}: {sanitized}"
        )

    return sanitized
