from __future__ import annotations

import cgi
import html
import logging
from dataclasses import dataclass
from http import HTTPStatus
from typing import Callable

from src.config import AppConfig
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
    message: str = ""
    error: str = ""
    result: DocumentIntakeResult | None = None


def create_app(config: AppConfig) -> Callable:
    service = DocumentIntakeService(
        automation_drop_root=config.automation_drop_root,
        work_root=config.work_root,
    )

    def app(environ, start_response):
        method = environ.get("REQUEST_METHOD", "GET").upper()
        path = environ.get("PATH_INFO", "/")

        if path == "/" and method == "GET":
            return _respond_html(start_response, render_page(config, ViewState()))
        if path == "/" and method == "POST":
            return _handle_upload(environ, start_response, config, service)

        start_response(
            f"{HTTPStatus.NOT_FOUND.value} {HTTPStatus.NOT_FOUND.phrase}",
            [("Content-Type", "text/plain; charset=utf-8")],
        )
        return [b"Not found"]

    return app


def _handle_upload(environ, start_response, config, service: DocumentIntakeService):
    customer = ""
    try:
        form = cgi.FieldStorage(
            fp=environ["wsgi.input"],
            environ=environ,
            keep_blank_values=True,
        )
        customer = form.getfirst("customer", "")
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

        result = service.intake_documents(customer, uploaded_files)
        message = (
            f"Copied {len(result.copied_files)} file(s) for "
            f"{result.customer_name}."
        )
        return _respond_html(
            start_response,
            render_page(
                config,
                ViewState(
                    customer=result.customer_name,
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
                ViewState(customer=customer, error=str(exc)),
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


def render_page(config: AppConfig, view_state: ViewState) -> str:
    customer_value = html.escape(view_state.customer)
    app_env = html.escape(config.app_env)

    result_html = ""
    if view_state.error:
        result_html = (
            '<section class="result error" aria-live="polite">'
            f"<h2>Upload failed</h2><p>{html.escape(view_state.error)}</p>"
            "</section>"
        )
    elif view_state.result:
        result = view_state.result
        copied_count = len(result.copied_files)
        result_html = (
            '<section class="result success" aria-live="polite">'
            "<h2>Upload complete</h2>"
            f"<p>{html.escape(view_state.message)}</p>"
            "<dl>"
            f"<div><dt>Customer</dt><dd>{html.escape(result.customer_name)}</dd></div>"
            f"<div><dt>Customer folder</dt><dd>{html.escape(result.sanitized_customer_folder_name)}</dd></div>"
            f"<div><dt>Files copied</dt><dd>{copied_count}</dd></div>"
            f"<div><dt>Automation destination</dt><dd>{html.escape(str(result.automation_path))}</dd></div>"
            f"<div><dt>Working destination</dt><dd>{html.escape(str(result.working_path))}</dd></div>"
            "</dl>"
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
    .result h2 {{
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
    @media (max-width: 640px) {{
      main {{ margin: 2rem auto; }}
      .panel {{ padding: 1.25rem; }}
      button {{ width: 100%; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="panel">
      <div class="eyebrow">Environment: {app_env}</div>
      <h1>Phase 1 Document Handoff</h1>
      <p>Upload the incoming customer documents. Files are copied as-is into both configured roots for downstream processing.</p>
      <form method="post" enctype="multipart/form-data" id="handoff-form" novalidate>
        <label for="customer">
          Customer
          <input id="customer" name="customer" type="text" value="{customer_value}" required>
        </label>
        <label for="documents">
          Documents
          <input id="documents" name="documents" type="file" multiple required>
        </label>
        <p class="hint" id="client-message" aria-live="polite"></p>
        <button type="submit" id="submit-button">Copy Documents</button>
      </form>
      {result_html}
    </section>
  </main>
  <script>
    const form = document.getElementById("handoff-form");
    const submitButton = document.getElementById("submit-button");
    const customerInput = document.getElementById("customer");
    const documentsInput = document.getElementById("documents");
    const clientMessage = document.getElementById("client-message");

    form.addEventListener("submit", (event) => {{
      const customer = customerInput.value.trim();
      const fileCount = documentsInput.files.length;

      if (!customer) {{
        event.preventDefault();
        clientMessage.textContent = "Customer is required.";
        customerInput.focus();
        return;
      }}

      if (fileCount === 0) {{
        event.preventDefault();
        clientMessage.textContent = "Select at least one file.";
        documentsInput.focus();
        return;
      }}

      submitButton.disabled = true;
      submitButton.textContent = "Copying...";
      clientMessage.textContent = "Uploading and copying files...";
    }});
  </script>
</body>
</html>"""
