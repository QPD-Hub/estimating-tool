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
        method = environ.get("REQUEST_METHOD", "GET").upper()
        path = environ.get("PATH_INFO", "/")

        if path == "/" and method == "GET":
            return _respond_html(start_response, render_page(config, ViewState()))
        if path == "/" and method == "POST":
            return _handle_upload(environ, start_response, config, service)
        if path == "/api/dev/bom-intake" and method == "POST":
            nonlocal bom_intake_service
            if bom_intake_service is None:
                bom_intake_service = _build_bom_intake_service()
            return _handle_bom_intake_api(environ, start_response, bom_intake_service)

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
        form = cgi.FieldStorage(
            fp=environ["wsgi.input"],
            environ=environ,
            keep_blank_values=True,
        )
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
        if isinstance(
            exc,
            (
                BomIntakeRequestError,
                BomIntakePayloadError,
            ),
        ):
            logger.warning("BOM intake request validation failed: %s", exc)
            return _respond_json(
                start_response,
                {"error": str(exc)},
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


def _group_processed_files_by_type(
    processed_files: list[ProcessedFileResult],
) -> list[tuple[str, int]]:
    counts_by_type: dict[str, int] = {}

    for processed_file in processed_files:
        suffix = Path(processed_file.filename).suffix.lower()
        file_type = suffix if suffix else "No extension"
        counts_by_type[file_type] = counts_by_type.get(file_type, 0) + 1

    return sorted(counts_by_type.items(), key=lambda item: (-item[1], item[0]))


def render_page(config: AppConfig, view_state: ViewState) -> str:
    customer_value = html.escape(view_state.customer)
    app_env = html.escape(config.app_env)
    part_values = view_state.top_level_parts or [""]

    parts_inputs_html = "".join(
        (
            '<input name="top_level_parts" type="text" '
            f'value="{html.escape(part_value)}" '
            'placeholder="Enter a top-level part" required>'
        )
        for part_value in part_values
    )

    result_html = ""
    if view_state.error:
        result_html = (
            '<section class="result error" aria-live="polite">'
            f"<h2>Upload failed</h2><p>{html.escape(view_state.error)}</p>"
            "</section>"
        )
    elif view_state.result:
        result = view_state.result
        created_parts_html = "".join(
            "<li>"
            f"{html.escape(destination.sanitized_part_folder_name)}"
            "</li>"
            for destination in result.part_destinations
        )
        processed_files_by_type_html = "".join(
            "<li>"
            f"<span>{html.escape(file_type)}</span>"
            f"<strong>{count}</strong>"
            "</li>"
            for file_type, count in _group_processed_files_by_type(result.processed_files)
        )
        result_html = (
            '<section class="result success" aria-live="polite">'
            "<h2>Upload complete</h2>"
            f"<p>{html.escape(view_state.message)}</p>"
            "<dl>"
            f"<div><dt>Customer</dt><dd>{html.escape(result.customer_name)}</dd></div>"
            f"<div><dt>Customer folder</dt><dd>{html.escape(result.sanitized_customer_folder_name)}</dd></div>"
            f"<div><dt>Top-level parts</dt><dd>{len(result.part_destinations)}</dd></div>"
            f"<div><dt>Processed files</dt><dd>{len(result.processed_files)}</dd></div>"
            f"<div><dt>Automation customer path</dt><dd>{html.escape(str(result.automation_customer_path))}</dd></div>"
            f"<div><dt>Working customer path</dt><dd>{html.escape(str(result.working_customer_path))}</dd></div>"
            "</dl>"
            "<div class=\"result-list\">"
            "<h3>Processed files by type</h3>"
            f"<ul>{processed_files_by_type_html}</ul>"
            "</div>"
            "<div class=\"part-list\">"
            "<h3>Part folders created</h3>"
            f"<ul>{created_parts_html}</ul>"
            "</div>"
            "</section>"
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Document Handoff</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f4efe8;
      --panel: #fffdf9;
      --ink: #1d1d1b;
      --muted: #5c5a55;
      --accent: #0d5c63;
      --accent-strong: #084c52;
      --border: #d7cfc3;
      --error-bg: #fce8e6;
      --error-ink: #8a1c12;
      --success-bg: #e4f3eb;
      --success-ink: #1f5c39;
      --shadow: 0 18px 40px rgba(42, 35, 28, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(13, 92, 99, 0.10), transparent 28rem),
        linear-gradient(180deg, #f8f4ee 0%, var(--bg) 100%);
      color: var(--ink);
    }}
    main {{
      max-width: 44rem;
      margin: 4rem auto;
      padding: 0 1.25rem;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 1rem;
      box-shadow: var(--shadow);
      padding: 2rem;
    }}
    .eyebrow {{
      display: inline-block;
      margin-bottom: 0.75rem;
      padding: 0.25rem 0.6rem;
      border-radius: 999px;
      background: rgba(13, 92, 99, 0.10);
      color: var(--accent-strong);
      font-size: 0.85rem;
      font-weight: 600;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }}
    h1 {{
      margin: 0 0 0.5rem;
      font-size: clamp(2rem, 4vw, 2.75rem);
      line-height: 1.05;
    }}
    p {{
      color: var(--muted);
      line-height: 1.5;
    }}
    form {{
      display: grid;
      gap: 1rem;
      margin-top: 1.5rem;
    }}
    label {{
      display: grid;
      gap: 0.45rem;
      font-weight: 600;
    }}
    .field-group {{
      display: grid;
      gap: 0.65rem;
    }}
    .field-group-header {{
      display: flex;
      justify-content: space-between;
      gap: 1rem;
      align-items: center;
    }}
    .field-group-header span {{
      font-weight: 600;
    }}
    .part-inputs {{
      display: grid;
      gap: 0.65rem;
    }}
    input[type="text"],
    input[type="file"] {{
      width: 100%;
      padding: 0.85rem 0.95rem;
      border: 1px solid var(--border);
      border-radius: 0.8rem;
      background: #ffffff;
      color: var(--ink);
      font: inherit;
    }}
    button {{
      width: fit-content;
      min-width: 10rem;
      padding: 0.9rem 1.2rem;
      border: 0;
      border-radius: 999px;
      background: var(--accent);
      color: white;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }}
    button.secondary {{
      min-width: 0;
      padding: 0.65rem 1rem;
      background: rgba(13, 92, 99, 0.12);
      color: var(--accent-strong);
    }}
    button:disabled {{
      opacity: 0.7;
      cursor: wait;
    }}
    .hint {{
      margin: 0;
      font-size: 0.95rem;
    }}
    .result {{
      margin-top: 1.5rem;
      padding: 1rem 1.1rem;
      border-radius: 0.85rem;
      border: 1px solid transparent;
    }}
    .result.success {{
      background: var(--success-bg);
      color: var(--success-ink);
      border-color: rgba(31, 92, 57, 0.18);
    }}
    .result.error {{
      background: var(--error-bg);
      color: var(--error-ink);
      border-color: rgba(138, 28, 18, 0.18);
    }}
    .result h2,
    .result h3 {{
      margin: 0 0 0.6rem;
      font-size: 1.1rem;
    }}
    .result p {{
      margin: 0 0 0.75rem;
      color: inherit;
    }}
    dl {{
      margin: 0;
      display: grid;
      gap: 0.65rem;
    }}
    dt {{
      font-weight: 700;
    }}
    dd {{
      margin: 0.1rem 0 0;
      word-break: break-word;
    }}
    .part-list {{
      margin-top: 1rem;
    }}
    .result-list {{
      margin-top: 1rem;
    }}
    .result-list ul,
    .part-list ul {{
      margin: 0;
      padding-left: 1.25rem;
    }}
    .result-list li {{
      display: flex;
      justify-content: space-between;
      gap: 1rem;
    }}
    @media (max-width: 640px) {{
      main {{ margin: 2rem auto; }}
      .panel {{ padding: 1.25rem; }}
      button {{ width: 100%; }}
      .field-group-header {{ flex-direction: column; align-items: stretch; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="panel">
      <div class="eyebrow">Environment: {app_env}</div>
      <h1>Phase 1 Document Handoff</h1>
      <p>Upload the incoming customer documents. Zip files are unpacked, folder structure is flattened, and the processed file set is copied into each selected top-level part under both configured roots.</p>
      <form method="post" enctype="multipart/form-data" id="handoff-form" novalidate>
        <label for="customer">
          Customer
          <input id="customer" name="customer" type="text" value="{customer_value}" required>
        </label>
        <div class="field-group">
          <div class="field-group-header">
            <span>Top Level Parts</span>
            <button type="button" class="secondary" id="add-part-button">+ Add Part</button>
          </div>
          <div class="part-inputs" id="part-inputs">
            {parts_inputs_html}
          </div>
        </div>
        <label for="documents">
          Documents
          <input id="documents" name="documents" type="file" multiple required>
        </label>
        <p class="hint" id="client-message" aria-live="polite"></p>
        <button type="submit" id="submit-button">Process Documents</button>
      </form>
      {result_html}
    </section>
  </main>
  <script>
    const form = document.getElementById("handoff-form");
    const submitButton = document.getElementById("submit-button");
    const customerInput = document.getElementById("customer");
    const documentsInput = document.getElementById("documents");
    const partInputs = document.getElementById("part-inputs");
    const addPartButton = document.getElementById("add-part-button");
    const clientMessage = document.getElementById("client-message");

    function createPartInput() {{
      const input = document.createElement("input");
      input.name = "top_level_parts";
      input.type = "text";
      input.placeholder = "Enter a top-level part";
      input.required = true;
      return input;
    }}

    addPartButton.addEventListener("click", () => {{
      partInputs.appendChild(createPartInput());
    }});

    form.addEventListener("submit", (event) => {{
      const customer = customerInput.value.trim();
      const fileCount = documentsInput.files.length;
      const partValues = Array.from(
        partInputs.querySelectorAll('input[name="top_level_parts"]')
      ).map((input) => input.value.trim());

      if (!customer) {{
        event.preventDefault();
        clientMessage.textContent = "Customer is required.";
        customerInput.focus();
        return;
      }}

      if (!partValues.some((value) => value)) {{
        event.preventDefault();
        clientMessage.textContent = "Enter at least one Top Level Part.";
        const firstPartInput = partInputs.querySelector('input[name="top_level_parts"]');
        if (firstPartInput) {{
          firstPartInput.focus();
        }}
        return;
      }}

      if (fileCount === 0) {{
        event.preventDefault();
        clientMessage.textContent = "Select at least one file.";
        documentsInput.focus();
        return;
      }}

      submitButton.disabled = true;
      submitButton.textContent = "Processing...";
      clientMessage.textContent = "Uploading and processing files...";
    }});
  </script>
</body>
</html>"""
