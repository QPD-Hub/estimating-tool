from __future__ import annotations

import cgi
import html
import json
import logging
from dataclasses import dataclass, field
from http import HTTPStatus
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs

from src.config import AppConfig, SqlServerConfig, SqlServerConfigError
from src.services.bom_intake_db import (
    BomIntakeDbConnectionError,
    BomIntakeDbError,
    BomIntakeDbProcedureError,
    BomIntakeDbService,
)
from src.services.bom_intake_payload import BomIntakePayloadError
from src.services.bom_intake_service import (
    BomIntakePreview,
    BomIntakeRequestError,
    BomIntakeService,
)
from src.services.document_intake_service import (
    DocumentIntakeError,
    DocumentIntakeResult,
    DocumentIntakeService,
    UploadedFile,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

BOM_UPLOAD_ALLOWED_SUFFIXES = (".xlsx", ".xls", ".zip")


@dataclass(frozen=True)
class ViewState:
    customer: str = ""
    top_level_parts: list[str] = field(default_factory=lambda: [""])
    message: str = ""
    error: str = ""
    result: DocumentIntakeResult | None = None


def create_app(
    config: AppConfig,
    bom_intake_service_override: BomIntakeService | None = None,
) -> Callable:
    service = DocumentIntakeService(
        automation_drop_root=config.automation_drop_root,
        work_root=config.work_root,
    )
    bom_intake_service: BomIntakeService | None = bom_intake_service_override

    def app(environ, start_response):
        nonlocal bom_intake_service
        method = environ.get("REQUEST_METHOD", "GET").upper()
        path = environ.get("PATH_INFO", "/")

        if path == "/" and method == "GET":
            return _respond_html(start_response, render_page(config, ViewState()))
        if path == "/" and method == "POST":
            return _handle_upload(environ, start_response, config, service)
        if path == "/api/dev/bom-intake" and method == "POST":
            if bom_intake_service is None:
                bom_intake_service = _build_bom_intake_service()
            return _handle_bom_intake_api(environ, start_response, bom_intake_service)
        if path == "/api/dev/bom-intake/preview" and method == "POST":
            if bom_intake_service is None:
                bom_intake_service = _build_bom_intake_service()
            return _handle_bom_upload_preview_api(
                environ,
                start_response,
                bom_intake_service,
            )
        if path == "/api/dev/bom-intake/process" and method == "POST":
            if bom_intake_service is None:
                bom_intake_service = _build_bom_intake_service()
            return _handle_bom_upload_process_api(
                environ,
                start_response,
                bom_intake_service,
            )

        start_response(
            f"{HTTPStatus.NOT_FOUND.value} {HTTPStatus.NOT_FOUND.phrase}",
            [("Content-Type", "text/plain; charset=utf-8")],
        )
        return [b"Not found"]

    return app


def _handle_upload(environ, start_response, config, service: DocumentIntakeService):
    customer = ""
    top_level_parts = [""]
    try:
        form = _parse_form_request(environ)
        customer = form.getfirst("customer", "")
        top_level_parts = form.getlist("top_level_parts") or [""]
        file_fields = form["documents"] if "documents" in form else []
        if not isinstance(file_fields, list):
            file_fields = [file_fields]

        uploaded_files = []
        for field in file_fields:
            if not getattr(field, "filename", ""):
                continue
            uploaded_files.append(
                UploadedFile(
                    filename=field.filename,
                    content=field.file.read(),
                )
            )

        result = service.intake_documents(customer, top_level_parts, uploaded_files)
        message = (
            f"Processed {len(result.processed_files)} file(s) into "
            f"{len(result.part_destinations)} top-level part folder(s) for "
            f"{result.customer_name}."
        )
        return _respond_html(
            start_response,
            render_page(
                config,
                ViewState(
                    customer=result.customer_name,
                    top_level_parts=result.top_level_parts,
                    message=message,
                    result=result,
                ),
            ),
        )
    except DocumentIntakeError as exc:
        logger.warning("Document intake validation failed: %s", exc)
        return _respond_html(
            start_response,
            render_page(
                config,
                ViewState(
                    customer=customer,
                    top_level_parts=top_level_parts,
                    error=str(exc),
                ),
            ),
            status=HTTPStatus.BAD_REQUEST,
        )
    except Exception:
        logger.exception("Unexpected document intake error.")
        return _respond_html(
            start_response,
            render_page(
                config,
                ViewState(
                    customer=customer,
                    top_level_parts=top_level_parts,
                    error="Unexpected server error while processing the upload.",
                ),
            ),
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )


def _respond_html(start_response, page: str, status: HTTPStatus = HTTPStatus.OK):
    body = page.encode("utf-8")
    start_response(
        f"{status.value} {status.phrase}",
        [
            ("Content-Type", "text/html; charset=utf-8"),
            ("Content-Length", str(len(body))),
        ],
    )
    return [body]


def _respond_json(
    start_response,
    payload: dict[str, object],
    status: HTTPStatus = HTTPStatus.OK,
):
    body = json.dumps(payload).encode("utf-8")
    start_response(
        f"{status.value} {status.phrase}",
        [
            ("Content-Type", "application/json; charset=utf-8"),
            ("Content-Length", str(len(body))),
        ],
    )
    return [body]


def _build_bom_intake_service() -> BomIntakeService:
    sql_config = SqlServerConfig.load()
    db_service = BomIntakeDbService(sql_config=sql_config)
    return BomIntakeService(db_service=db_service)


def _handle_bom_intake_api(
    environ,
    start_response,
    service: BomIntakeService,
):
    try:
        request_body = _parse_json_request(environ)
        header = request_body.get("header")
        standardized_bom_rows = request_body.get("standardizedBomRows")
        upload = request_body.get("upload")
        dry_run = _resolve_bom_intake_dry_run(environ, request_body)

        if standardized_bom_rows is not None and upload is not None:
            raise BomIntakeRequestError(
                "Provide either standardizedBomRows or upload, not both."
            )
        if upload is not None:
            result = service.process_uploaded_bom(
                header_data=header,
                upload_data=upload,
                dry_run=dry_run,
            )
        else:
            result = service.process_standardized_upload(
                header_data=header,
                standardized_rows_data=standardized_bom_rows,
                dry_run=dry_run,
            )
        return _respond_json(
            start_response,
            _serialize_bom_intake_result(result),
            status=HTTPStatus.OK,
        )
    except ValueError as exc:
        return _handle_bom_request_value_error(start_response, exc)
    except BomIntakeDbConnectionError as exc:
        return _respond_json(
            start_response,
            {"error": str(exc)},
            status=HTTPStatus.BAD_GATEWAY,
        )
    except (BomIntakeDbProcedureError, BomIntakeDbError) as exc:
        return _respond_json(
            start_response,
            {"error": str(exc)},
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )
    except Exception:
        logger.exception("Unexpected BOM intake API error.")
        return _respond_json(
            start_response,
            {"error": "Unexpected server error while processing BOM intake."},
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )


def _handle_bom_upload_preview_api(
    environ,
    start_response,
    service: BomIntakeService,
):
    try:
        form = _parse_form_request(environ)
        header_data, upload_data = _build_bom_upload_request(form)
        preview = service.preview_uploaded_bom(
            header_data=header_data,
            upload_data=upload_data,
        )
        return _respond_json(
            start_response,
            _serialize_bom_preview(preview),
            status=HTTPStatus.OK,
        )
    except ValueError as exc:
        return _handle_bom_request_value_error(start_response, exc)
    except Exception:
        logger.exception("Unexpected BOM preview API error.")
        return _respond_json(
            start_response,
            {"error": "Unexpected server error while previewing BOM intake."},
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )


def _handle_bom_upload_process_api(
    environ,
    start_response,
    service: BomIntakeService,
):
    try:
        form = _parse_form_request(environ)
        header_data, upload_data = _build_bom_upload_request(form)
        result = service.process_uploaded_bom(
            header_data=header_data,
            upload_data=upload_data,
        )
        return _respond_json(
            start_response,
            _serialize_bom_intake_result(result),
            status=HTTPStatus.OK,
        )
    except ValueError as exc:
        return _handle_bom_request_value_error(start_response, exc)
    except BomIntakeDbConnectionError as exc:
        return _respond_json(
            start_response,
            {"error": str(exc)},
            status=HTTPStatus.BAD_GATEWAY,
        )
    except (BomIntakeDbProcedureError, BomIntakeDbError) as exc:
        return _respond_json(
            start_response,
            {"error": str(exc)},
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )
    except Exception:
        logger.exception("Unexpected BOM process API error.")
        return _respond_json(
            start_response,
            {"error": "Unexpected server error while processing BOM intake."},
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )


def _handle_bom_request_value_error(start_response, exc: ValueError):
    if isinstance(
        exc,
        (
            BomIntakeRequestError,
            BomIntakePayloadError,
        ),
    ):
        logger.warning("BOM intake request validation failed: %s", exc)
        payload: dict[str, object] = {"error": str(exc)}
        diagnostics = getattr(exc, "diagnostics", None)
        if diagnostics is not None:
            payload["diagnostics"] = diagnostics
        return _respond_json(
            start_response,
            payload,
            status=HTTPStatus.BAD_REQUEST,
        )
    if isinstance(exc, SqlServerConfigError):
        logger.error("BOM intake SQL configuration failed: %s", exc)
        return _respond_json(
            start_response,
            {"error": str(exc)},
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )
    logger.warning("BOM intake request parsing failed: %s", exc)
    return _respond_json(
        start_response,
        {"error": str(exc)},
        status=HTTPStatus.BAD_REQUEST,
    )


def _parse_json_request(environ) -> dict[str, object]:
    try:
        content_length = int(environ.get("CONTENT_LENGTH", "0") or "0")
    except ValueError as exc:
        raise ValueError("Invalid Content-Length header.") from exc

    body = environ["wsgi.input"].read(content_length) if content_length > 0 else b""
    if not body:
        raise ValueError("Request body is required.")

    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Request body must be valid JSON.") from exc

    if not isinstance(payload, dict):
        raise ValueError("Request body must be a JSON object.")
    return payload


def _parse_form_request(environ) -> cgi.FieldStorage:
    return cgi.FieldStorage(
        fp=environ["wsgi.input"],
        environ=environ,
        keep_blank_values=True,
    )


def _build_bom_upload_request(
    form: cgi.FieldStorage,
) -> tuple[dict[str, object], dict[str, object]]:
    upload_field = form["bom_file"] if "bom_file" in form else None
    if upload_field is None or not getattr(upload_field, "filename", ""):
        raise BomIntakeRequestError("A BOM spreadsheet or zip package is required.")

    filename = upload_field.filename.strip()
    if not filename:
        raise BomIntakeRequestError("Uploaded filename is required.")
    if not _is_allowed_bom_upload(filename):
        raise BomIntakeRequestError(
            "Upload a .xlsx, .xls, or .zip BOM file."
        )

    return (
        {
            "customer_name": _form_value(form, "customer_name", "customerName"),
            "uploaded_by": _form_value(form, "uploaded_by", "uploadedBy"),
            "quote_number": _optional_form_value(
                form,
                "quote_number",
                "quoteNumber",
            ),
            "intake_notes": _optional_form_value(
                form,
                "intake_notes",
                "intakeNotes",
            ),
            "parser_version": _optional_form_value(
                form,
                "parser_version",
                "parserVersion",
            ),
            "source_file_name": _optional_form_value(
                form,
                "source_file_name",
                "sourceFileName",
            )
            or filename,
            "source_file_path": _optional_form_value(
                form,
                "source_file_path",
                "sourceFilePath",
            ),
            "source_sheet_name": _optional_form_value(
                form,
                "source_sheet_name",
                "sourceSheetName",
            ),
            "source_type": _optional_form_value(
                form,
                "source_type",
                "sourceType",
            ),
        },
        {
            "filename": filename,
            "content_base64": None,
            "source_file_path": None,
            "content": upload_field.file.read(),
        },
    )


def _serialize_bom_intake_result(result: dict[str, object]) -> dict[str, object]:
    if result.get("DryRun") is True:
        return {
            "dryRun": True,
            "previewPath": result.get("PreviewPath"),
            "payload": result.get("Payload"),
        }

    summary = result.get("Summary", {})
    root_results = result.get("RootResults", [])
    if not isinstance(summary, dict):
        summary = {}
    if not isinstance(root_results, list):
        root_results = []

    return {
        "summary": {
            "bomIntakeId": summary.get("BomIntakeId"),
            "detectedRootCount": summary.get("DetectedRootCount"),
            "acceptedRootCount": summary.get("AcceptedRootCount"),
            "duplicateRejectedCount": summary.get("DuplicateRejectedCount"),
            "finalIntakeStatus": summary.get("FinalIntakeStatus"),
        },
        "rootResults": [
            {
                "rootClientId": root_result.get("RootClientId"),
                "rootSequence": root_result.get("RootSequence"),
                "customerName": root_result.get("CustomerName"),
                "level0PartNumber": root_result.get("Level0PartNumber"),
                "revision": root_result.get("Revision"),
                "decisionStatus": root_result.get("DecisionStatus"),
                "decisionReason": root_result.get("DecisionReason"),
                "bomRootId": root_result.get("BomRootId"),
                "existingBomRootId": root_result.get("ExistingBomRootId"),
            }
            for root_result in root_results
        ],
    }


def _serialize_bom_preview(preview: BomIntakePreview) -> dict[str, object]:
    return preview.to_dict()


def _resolve_bom_intake_dry_run(environ, request_body: dict[str, object]) -> bool:
    query_values = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
    if "dry_run" in query_values:
        return _parse_bool_value(query_values["dry_run"][-1], "dry_run")
    if "dryRun" in request_body:
        return _parse_bool_value(request_body["dryRun"], "dryRun")
    return False


def _parse_bool_value(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"{field_name} must be a boolean value.")


def _form_value(form: cgi.FieldStorage, *names: str) -> str:
    value = _optional_form_value(form, *names)
    if value is None:
        field_name = names[0]
        raise BomIntakeRequestError(f"{field_name} is required.")
    return value


def _optional_form_value(form: cgi.FieldStorage, *names: str) -> str | None:
    for name in names:
        if name not in form:
            continue
        value = form.getfirst(name)
        if value is None:
            continue
        stripped = value.strip()
        return stripped if stripped else None
    return None


def _is_allowed_bom_upload(filename: str) -> bool:
    return Path(filename).suffix.lower() in BOM_UPLOAD_ALLOWED_SUFFIXES


def _group_processed_files_by_extension(
    processed_files,
) -> list[tuple[str, int]]:
    counts_by_extension: dict[str, int] = {}

    for processed_file in processed_files:
        suffix = Path(processed_file.filename).suffix.lower()
        extension = suffix if suffix else "No extension"
        counts_by_extension[extension] = counts_by_extension.get(extension, 0) + 1

    return sorted(counts_by_extension.items(), key=lambda item: (-item[1], item[0]))


def render_page(config: AppConfig, view_state: ViewState) -> str:
    app_env = html.escape(config.app_env)

    processed_document_overview_html = ""
    if view_state.error:
        processed_document_overview_html = (
            '<section class="stack" id="processed-document-overview" aria-live="polite">'
            '<div class="callout error">'
            "<h3>Processed Document Overview</h3>"
            f"<p>{html.escape(view_state.error)}</p>"
            "</div>"
            "</section>"
        )
    elif view_state.result:
        result = view_state.result
        processed_files_by_extension = _group_processed_files_by_extension(
            result.processed_files
        )
        created_parts_html = "".join(
            f"<li>{html.escape(destination.sanitized_part_folder_name)}</li>"
            for destination in result.part_destinations
        )
        processed_files_by_extension_html = "".join(
            "<li>"
            f"<span>{html.escape(extension)}</span>"
            f"<strong>{count}</strong>"
            "</li>"
            for extension, count in processed_files_by_extension
        )
        processed_document_overview_html = (
            '<section class="stack" id="processed-document-overview" aria-live="polite">'
            '<div class="callout success">'
            "<h3>Processed Document Overview</h3>"
            f"<p>{html.escape(view_state.message)}</p>"
            "<dl class=\"summary-grid\">"
            f"<div><dt>Customer</dt><dd>{html.escape(result.customer_name)}</dd></div>"
            f"<div><dt>Customer folder</dt><dd>{html.escape(result.sanitized_customer_folder_name)}</dd></div>"
            f"<div><dt>Top-level parts</dt><dd>{len(result.part_destinations)}</dd></div>"
            f"<div><dt>Processed files</dt><dd>{len(result.processed_files)}</dd></div>"
            f"<div><dt>Automation path</dt><dd>{html.escape(str(result.automation_customer_path))}</dd></div>"
            f"<div><dt>Working path</dt><dd>{html.escape(str(result.working_customer_path))}</dd></div>"
            "</dl>"
            "<div class=\"stack\">"
            "<div class=\"overview-card\">"
            "<h4>Doc Package Overview</h4>"
            "<p class=\"section-note\">Processed file counts grouped by extension.</p>"
            "<ul class=\"count-list\">"
            f"{processed_files_by_extension_html}</ul>"
            "</div>"
            "<div><h4>Part Folders Created</h4><ul>"
            f"{created_parts_html}</ul></div>"
            "</div>"
            "</div>"
            "</section>"
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BOM Intake</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f1e8;
      --panel: #fffdf9;
      --panel-alt: #f3ece1;
      --ink: #1f1b16;
      --muted: #6a645c;
      --accent: #0d5c63;
      --accent-strong: #08444a;
      --accent-soft: rgba(13, 92, 99, 0.12);
      --border: #d7cfc3;
      --border-strong: #b9aea0;
      --error-bg: #fce8e6;
      --error-ink: #8a1c12;
      --success-bg: #e4f3eb;
      --success-ink: #1f5c39;
      --shadow: 0 20px 40px rgba(37, 30, 22, 0.08);
      --mono: "Consolas", "SFMono-Regular", monospace;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background:
        radial-gradient(circle at top left, rgba(13, 92, 99, 0.10), transparent 24rem),
        linear-gradient(180deg, #fbf7f1 0%, var(--bg) 100%);
      color: var(--ink);
      font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
    }}
    main {{
      width: min(74rem, calc(100vw - 2rem));
      margin: 2rem auto 4rem;
      display: grid;
      gap: 1.25rem;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 1rem;
      box-shadow: var(--shadow);
      padding: 1.5rem;
    }}
    .hero {{
      display: grid;
      gap: 0.85rem;
    }}
    .eyebrow {{
      display: inline-flex;
      width: fit-content;
      padding: 0.3rem 0.7rem;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent-strong);
      font-size: 0.82rem;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }}
    h1, h2, h3, h4 {{
      margin: 0;
      line-height: 1.1;
    }}
    h1 {{
      font-size: clamp(2rem, 4vw, 3.2rem);
    }}
    p {{
      margin: 0;
      color: var(--muted);
      line-height: 1.5;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 1rem;
      align-items: start;
    }}
    .stack {{
      display: grid;
      gap: 1rem;
    }}
    form {{
      display: grid;
      gap: 1rem;
    }}
    .form-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 1rem;
    }}
    label {{
      display: grid;
      gap: 0.4rem;
      font-weight: 600;
    }}
    input[type="text"],
    input[type="file"],
    textarea {{
      width: 100%;
      padding: 0.8rem 0.9rem;
      border: 1px solid var(--border);
      border-radius: 0.8rem;
      background: white;
      color: var(--ink);
      font: inherit;
    }}
    textarea {{
      min-height: 6rem;
      resize: vertical;
    }}
    input:focus,
    textarea:focus {{
      outline: 2px solid rgba(13, 92, 99, 0.16);
      border-color: var(--accent);
    }}
    .actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.75rem;
      align-items: center;
    }}
    button {{
      appearance: none;
      border: 0;
      border-radius: 999px;
      padding: 0.9rem 1.2rem;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      background: var(--accent);
      color: white;
    }}
    button.secondary {{
      background: var(--panel-alt);
      color: var(--accent-strong);
      border: 1px solid var(--border);
    }}
    button.ghost {{
      background: transparent;
      color: var(--accent-strong);
      border: 1px solid var(--border);
    }}
    button:disabled {{
      cursor: wait;
      opacity: 0.7;
    }}
    .subtle {{
      font-size: 0.95rem;
    }}
    .callout {{
      border-radius: 0.9rem;
      padding: 1rem 1.1rem;
      border: 1px solid transparent;
    }}
    .callout.success {{
      background: var(--success-bg);
      color: var(--success-ink);
      border-color: rgba(31, 92, 57, 0.18);
    }}
    .callout.error {{
      background: var(--error-bg);
      color: var(--error-ink);
      border-color: rgba(138, 28, 18, 0.18);
    }}
    .callout p {{
      color: inherit;
      margin-top: 0.35rem;
    }}
    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 0.75rem 1rem;
      margin-top: 1rem;
    }}
    .summary-grid dt {{
      font-weight: 700;
      font-size: 0.9rem;
      color: var(--muted);
    }}
    .summary-grid dd {{
      margin: 0.15rem 0 0;
      word-break: break-word;
    }}
    .json-viewer {{
      width: 100%;
      min-height: 14rem;
      font-family: var(--mono);
      font-size: 0.86rem;
      background: #f8f5ef;
    }}
    .table-wrap {{
      overflow: auto;
      border: 1px solid var(--border);
      border-radius: 0.85rem;
      background: white;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 52rem;
    }}
    th, td {{
      padding: 0.7rem 0.8rem;
      border-bottom: 1px solid #ece4d7;
      text-align: left;
      vertical-align: top;
      font-size: 0.94rem;
    }}
    th {{
      position: sticky;
      top: 0;
      background: #faf5ee;
      color: var(--accent-strong);
    }}
    details {{
      border: 1px solid var(--border);
      border-radius: 0.85rem;
      background: #fff;
      padding: 0.85rem 0.95rem;
    }}
    details summary {{
      cursor: pointer;
      font-weight: 700;
      color: var(--accent-strong);
    }}
    details + details {{
      margin-top: 0.75rem;
    }}
    .status-line {{
      min-height: 1.4rem;
      color: var(--muted);
      font-size: 0.95rem;
    }}
    .hidden {{
      display: none !important;
    }}
    .count-list {{
      list-style: none;
      padding: 0;
      margin: 0;
      display: grid;
      gap: 0.45rem;
    }}
    .count-list li {{
      display: flex;
      justify-content: space-between;
      gap: 1rem;
    }}
    .overview-card {{
      padding: 0.9rem 1rem;
      border: 1px solid var(--border);
      border-radius: 0.85rem;
      background: rgba(255, 255, 255, 0.6);
    }}
    .overview-card h4 {{
      margin: 0 0 0.35rem;
    }}
    .section-note {{
      margin: 0 0 0.75rem;
      color: var(--muted);
      font-size: 0.95rem;
    }}
    .section-header {{
      display: flex;
      justify-content: space-between;
      gap: 1rem;
      align-items: center;
      margin-bottom: 0.75rem;
    }}
    @media (max-width: 900px) {{
      .grid,
      .form-grid,
      .summary-grid {{
        grid-template-columns: 1fr;
      }}
    }}
    @media (max-width: 640px) {{
      main {{
        width: min(100vw - 1rem, 100%);
        margin-top: 0.5rem;
      }}
      .panel {{
        padding: 1rem;
      }}
      .actions,
      .section-header {{
        flex-direction: column;
        align-items: stretch;
      }}
      button {{
        width: 100%;
      }}
      table {{
        min-width: 42rem;
      }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="panel hero">
      <span class="eyebrow">Environment: {app_env}</span>
      <h1>BOM Upload And Intake</h1>
      <p>Upload a BOM workbook or package, preview the standardized SQL-bound payload, then execute intake processing without leaving the app. The UI runs the existing locator, parser, standardizer, payload builder, and stored procedure flow.</p>
    </section>

    <section class="panel">
      <div class="section-header">
        <div class="stack">
          <h2>BOM Intake Workflow</h2>
          <p class="subtle">Accepted uploads: `.xlsx`, `.xls`, `.zip`. Preview does not write to SQL. Process runs the create + standardized intake procedures.</p>
        </div>
      </div>

      <form id="bom-intake-form" enctype="multipart/form-data" novalidate>
        <div class="form-grid">
          <label for="customer_name">
            Customer Name
            <input id="customer_name" name="customer_name" type="text" required>
          </label>
          <label for="uploaded_by">
            Uploaded By
            <input id="uploaded_by" name="uploaded_by" type="text" required>
          </label>
          <label for="quote_number">
            Quote Number
            <input id="quote_number" name="quote_number" type="text">
          </label>
          <label for="bom_file">
            BOM File
            <input id="bom_file" name="bom_file" type="file" accept=".xlsx,.xls,.zip" required>
          </label>
        </div>

        <label for="intake_notes">
          Intake Notes
          <textarea id="intake_notes" name="intake_notes" placeholder="Optional notes for the BOM intake record"></textarea>
        </label>

        <div class="actions">
          <button type="button" id="preview-button">Preview Payload</button>
          <button type="button" class="secondary" id="process-button">Process Intake</button>
          <span class="status-line" id="bom-status" aria-live="polite"></span>
        </div>
      </form>

      <section id="bom-error" class="callout error hidden" aria-live="polite"></section>

      <section id="bom-debug" class="stack hidden" aria-live="polite">
        <div class="callout">
          <h3>Preview Diagnostics</h3>
          <p>This temporary debug panel shows package selection, worksheet selection, and header detection details from the current preview attempt.</p>
        </div>
        <dl class="summary-grid" id="debug-summary"></dl>
        <details open>
          <summary>Candidate Spreadsheets</summary>
          <textarea id="debug-candidate-spreadsheets-json" class="json-viewer" readonly></textarea>
        </details>
        <details>
          <summary>Worksheet Diagnostics</summary>
          <textarea id="debug-worksheets-json" class="json-viewer" readonly></textarea>
        </details>
        <details>
          <summary>Header Candidate Rows</summary>
          <textarea id="debug-header-candidates-json" class="json-viewer" readonly></textarea>
        </details>
        <details>
          <summary>First Rows Preview</summary>
          <textarea id="debug-first-rows-json" class="json-viewer" readonly></textarea>
        </details>
        <details id="debug-error-details" class="hidden">
          <summary>Diagnostic Error Details</summary>
          <textarea id="debug-error-json" class="json-viewer" readonly></textarea>
        </details>
      </section>

      <section id="bom-preview" class="stack hidden" aria-live="polite">
        <div class="callout success">
          <h3>Preview Ready</h3>
          <p>The payload preview below reflects the exact SQL-bound roots and rows the app will send during intake processing.</p>
        </div>

        <dl class="summary-grid" id="preview-summary"></dl>

        <div class="table-wrap">
          <table id="standardized-rows-table">
            <thead>
              <tr>
                <th>Row</th>
                <th>Level</th>
                <th>Part Number</th>
                <th>Parent Part</th>
                <th>Description</th>
                <th>Revision</th>
                <th>Quantity</th>
                <th>UOM</th>
                <th>Item</th>
                <th>Make/Buy</th>
                <th>Validation</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>

        <details open>
          <summary>Standardized Rows JSON</summary>
          <textarea id="standardized-rows-json" class="json-viewer" readonly></textarea>
        </details>
        <details>
          <summary>Create Proc Params</summary>
          <textarea id="create-proc-json" class="json-viewer" readonly></textarea>
        </details>
        <details>
          <summary>Process Proc Params</summary>
          <textarea id="process-proc-json" class="json-viewer" readonly></textarea>
        </details>
        <details>
          <summary>Roots TVP Rows</summary>
          <textarea id="roots-json" class="json-viewer" readonly></textarea>
        </details>
        <details>
          <summary>BOM Rows TVP Rows</summary>
          <textarea id="bom-rows-json" class="json-viewer" readonly></textarea>
        </details>
      </section>

      <section id="bom-process-result" class="stack hidden" aria-live="polite">
        <div class="callout success">
          <h3>Intake Processed</h3>
          <p>The intake summary below reflects the SQL execution result for the uploaded BOM.</p>
        </div>
        <dl class="summary-grid" id="process-summary"></dl>
        <div class="table-wrap">
          <table id="root-results-table">
            <thead>
              <tr>
                <th>Root</th>
                <th>Sequence</th>
                <th>Customer</th>
                <th>Level 0 Part</th>
                <th>Revision</th>
                <th>Decision</th>
                <th>Reason</th>
                <th>BomRootId</th>
                <th>ExistingBomRootId</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </section>
      {processed_document_overview_html}
    </section>
  </main>
  <script>
    const bomForm = document.getElementById("bom-intake-form");
    const previewButton = document.getElementById("preview-button");
    const processButton = document.getElementById("process-button");
    const bomStatus = document.getElementById("bom-status");
    const bomError = document.getElementById("bom-error");
    const bomDebug = document.getElementById("bom-debug");
    const bomPreview = document.getElementById("bom-preview");
    const bomProcessResult = document.getElementById("bom-process-result");
    const previewSummary = document.getElementById("preview-summary");
    const debugSummary = document.getElementById("debug-summary");
    const processSummary = document.getElementById("process-summary");
    const standardizedRowsTableBody = document.querySelector("#standardized-rows-table tbody");
    const rootResultsTableBody = document.querySelector("#root-results-table tbody");
    const standardizedRowsJson = document.getElementById("standardized-rows-json");
    const createProcJson = document.getElementById("create-proc-json");
    const processProcJson = document.getElementById("process-proc-json");
    const rootsJson = document.getElementById("roots-json");
    const bomRowsJson = document.getElementById("bom-rows-json");
    const debugCandidateSpreadsheetsJson = document.getElementById("debug-candidate-spreadsheets-json");
    const debugWorksheetsJson = document.getElementById("debug-worksheets-json");
    const debugHeaderCandidatesJson = document.getElementById("debug-header-candidates-json");
    const debugFirstRowsJson = document.getElementById("debug-first-rows-json");
    const debugErrorDetails = document.getElementById("debug-error-details");
    const debugErrorJson = document.getElementById("debug-error-json");

    function setBusy(isBusy, message) {{
      previewButton.disabled = isBusy;
      processButton.disabled = isBusy;
      previewButton.textContent = isBusy && message === "preview" ? "Previewing..." : "Preview Payload";
      processButton.textContent = isBusy && message === "process" ? "Processing..." : "Process Intake";
      bomStatus.textContent = isBusy
        ? message === "preview"
          ? "Running locator, parser, standardizer, and payload preview..."
          : "Running BOM intake create and process procedures..."
        : "";
    }}

    function showBomError(message) {{
      bomError.textContent = message;
      bomError.classList.remove("hidden");
    }}

    function hideBomError() {{
      bomError.textContent = "";
      bomError.classList.add("hidden");
    }}

    function clearProcessResult() {{
      bomProcessResult.classList.add("hidden");
      processSummary.innerHTML = "";
      rootResultsTableBody.innerHTML = "";
    }}

    function clearPreviewDebug() {{
      bomPreview.classList.add("hidden");
      bomDebug.classList.add("hidden");
      previewSummary.innerHTML = "";
      debugSummary.innerHTML = "";
      standardizedRowsTableBody.innerHTML = "";
      standardizedRowsJson.value = "";
      createProcJson.value = "";
      processProcJson.value = "";
      rootsJson.value = "";
      bomRowsJson.value = "";
      debugCandidateSpreadsheetsJson.value = "";
      debugWorksheetsJson.value = "";
      debugHeaderCandidatesJson.value = "";
      debugFirstRowsJson.value = "";
      debugErrorJson.value = "";
      debugErrorDetails.classList.add("hidden");
    }}

    function validateBomForm() {{
      const customer = bomForm.customer_name.value.trim();
      const uploadedBy = bomForm.uploaded_by.value.trim();
      const file = bomForm.bom_file.files[0];

      if (!customer) {{
        throw new Error("Customer Name is required.");
      }}
      if (!uploadedBy) {{
        throw new Error("Uploaded By is required.");
      }}
      if (!file) {{
        throw new Error("A BOM spreadsheet or zip package is required.");
      }}

      const lowerName = file.name.toLowerCase();
      if (![".xlsx", ".xls", ".zip"].some((suffix) => lowerName.endsWith(suffix))) {{
        throw new Error("Upload a .xlsx, .xls, or .zip BOM file.");
      }}
    }}

    function renderSummaryList(container, items) {{
      container.innerHTML = items.map(([label, value]) => `
        <div>
          <dt>${{escapeHtml(label)}}</dt>
          <dd>${{escapeHtml(value ?? "")}}</dd>
        </div>
      `).join("");
    }}

    function renderPreview(preview) {{
      renderSummaryList(previewSummary, [
        ["Selected File Name", preview.selectedFileName],
        ["Detected Worksheet", preview.detectedWorksheet],
        ["Detected Source Type", preview.detectedSourceType],
        ["Source File Path", preview.sourceFilePath || ""],
        ["Root Count", String(preview.rootCount ?? "")],
        ["Row Count", String(preview.rowCount ?? "")]
      ]);

      standardizedRowsTableBody.innerHTML = (preview.standardizedRows || []).map((row) => `
        <tr>
          <td>${{escapeHtml(row.source_row_number)}}</td>
          <td>${{escapeHtml(row.bom_level)}}</td>
          <td>${{escapeHtml(row.part_number)}}</td>
          <td>${{escapeHtml(row.parent_part || "")}}</td>
          <td>${{escapeHtml(row.description)}}</td>
          <td>${{escapeHtml(row.revision || "")}}</td>
          <td>${{escapeHtml(row.quantity ?? "")}}</td>
          <td>${{escapeHtml(row.uom || "")}}</td>
          <td>${{escapeHtml(row.item_number || "")}}</td>
          <td>${{escapeHtml(row.make_buy || "")}}</td>
          <td>${{escapeHtml(row.validation_message || "")}}</td>
        </tr>
      `).join("");

      standardizedRowsJson.value = formatJson(preview.standardizedRows || []);
      createProcJson.value = formatJson(preview.createProcParams || {{}});
      processProcJson.value = formatJson(preview.processProcParams || {{}});
      rootsJson.value = formatJson(preview.rootsTvpRows || []);
      bomRowsJson.value = formatJson(preview.bomRowsTvpRows || []);

      bomPreview.classList.remove("hidden");
      renderDiagnostics(preview.diagnostics || null);
    }}

    function renderProcessResult(result) {{
      const summary = result.summary || {{}};
      renderSummaryList(processSummary, [
        ["BomIntakeId", String(summary.bomIntakeId ?? "")],
        ["DetectedRootCount", String(summary.detectedRootCount ?? "")],
        ["AcceptedRootCount", String(summary.acceptedRootCount ?? "")],
        ["DuplicateRejectedCount", String(summary.duplicateRejectedCount ?? "")],
        ["FinalIntakeStatus", summary.finalIntakeStatus || ""]
      ]);

      rootResultsTableBody.innerHTML = (result.rootResults || []).map((row) => `
        <tr>
          <td>${{escapeHtml(row.rootClientId || "")}}</td>
          <td>${{escapeHtml(row.rootSequence ?? "")}}</td>
          <td>${{escapeHtml(row.customerName || "")}}</td>
          <td>${{escapeHtml(row.level0PartNumber || "")}}</td>
          <td>${{escapeHtml(row.revision || "")}}</td>
          <td>${{escapeHtml(row.decisionStatus || "")}}</td>
          <td>${{escapeHtml(row.decisionReason || "")}}</td>
          <td>${{escapeHtml(row.bomRootId ?? "")}}</td>
          <td>${{escapeHtml(row.existingBomRootId ?? "")}}</td>
        </tr>
      `).join("");

      bomProcessResult.classList.remove("hidden");
    }}

    function formatJson(value) {{
      return JSON.stringify(value, null, 2);
    }}

    function renderDiagnostics(diagnostics, errorMessage = "") {{
      if (!diagnostics) {{
        bomDebug.classList.add("hidden");
        debugSummary.innerHTML = "";
        debugCandidateSpreadsheetsJson.value = "";
        debugWorksheetsJson.value = "";
        debugHeaderCandidatesJson.value = "";
        debugFirstRowsJson.value = "";
        debugErrorJson.value = "";
        debugErrorDetails.classList.add("hidden");
        return;
      }}

      renderSummaryList(debugSummary, [
        ["Selected File", diagnostics.selectedSourceFileName || ""],
        ["Selected Archive Member", diagnostics.selectedArchiveMemberName || ""],
        ["Selected Worksheet", diagnostics.selectedWorksheetName || ""],
        ["Worksheet Names", (diagnostics.worksheetNames || []).join(", ")],
        ["Archive Selection Reason", diagnostics.archiveSelection?.selectionReason || ""]
      ]);

      debugCandidateSpreadsheetsJson.value = formatJson(diagnostics.candidateSpreadsheets || []);
      debugWorksheetsJson.value = formatJson(diagnostics.worksheets || []);
      debugHeaderCandidatesJson.value = formatJson(diagnostics.headerRowCandidates || []);
      debugFirstRowsJson.value = formatJson(diagnostics.firstRowsPreview || []);

      if (errorMessage) {{
        debugErrorJson.value = formatJson({{
          error: errorMessage,
          diagnostics
        }});
        debugErrorDetails.classList.remove("hidden");
      }} else {{
        debugErrorJson.value = "";
        debugErrorDetails.classList.add("hidden");
      }}

      bomDebug.classList.remove("hidden");
    }}

    function escapeHtml(value) {{
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }}

    async function runBomAction(action) {{
      hideBomError();
      clearPreviewDebug();
      if (action === "preview") {{
        clearProcessResult();
      }}

      try {{
        validateBomForm();
      }} catch (error) {{
        showBomError(error.message);
        return;
      }}

      const formData = new FormData(bomForm);
      setBusy(true, action);

      try {{
        const response = await fetch(
          action === "preview" ? "/api/dev/bom-intake/preview" : "/api/dev/bom-intake/process",
          {{
            method: "POST",
            body: formData
          }}
        );
        const payload = await response.json();

        if (!response.ok) {{
          const error = new Error(payload.error || "Request failed.");
          error.diagnostics = payload.diagnostics || null;
          throw error;
        }}

        if (action === "preview") {{
          renderPreview(payload);
        }} else {{
          renderProcessResult(payload);
        }}
      }} catch (error) {{
        showBomError(error.message || "Unexpected request failure.");
        renderDiagnostics(error.diagnostics || null, error.message || "Unexpected request failure.");
      }} finally {{
        setBusy(false, action);
      }}
    }}

    previewButton.addEventListener("click", () => {{
      runBomAction("preview");
    }});

    processButton.addEventListener("click", () => {{
      runBomAction("process");
    }});
  </script>
</body>
</html>"""
