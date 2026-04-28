from __future__ import annotations

import cgi
import html
import json
import logging
from dataclasses import dataclass
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
    _load_pymssql_connect,
)
from src.services.bom_intake_payload import BomIntakePayloadError
from src.services.bom_intake_service import (
    BomIntakePreview,
    BomIntakeRequestError,
    BomIntakeService,
)
from src.services.document_intake_service import (
    DocumentIntakeResult,
    DocumentIntakeService,
    UploadedFile,
)
from src.services.doc_package_intake_service import (
    DocPackageIntakeError,
    DocPackageIntakeResult,
    DocPackageIntakeService,
)
from src.services.quote_prep_service import (
    QuotePrepDbError,
    QuotePrepRequestError,
    QuotePrepService,
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
    rfq_number: str = ""
    uploaded_by: str = ""
    quoted_by: str = ""
    contact_name: str = ""
    quote_due_date: str = ""
    intake_notes: str = ""
    message: str = ""
    error: str = ""
    result: DocumentIntakeResult | None = None
    package_result: DocPackageIntakeResult | None = None
    diagnostics: dict[str, object] | None = None


class LookupService:
    def __init__(
        self,
        sql_config: SqlServerConfig,
        connect: Callable[..., object] | None = None,
    ) -> None:
        self._sql_config = sql_config
        self._connect = connect or _load_pymssql_connect()

    def _connection_kwargs(self) -> dict[str, object]:
        return {
            "server": self._sql_config.host,
            "user": self._sql_config.username,
            "password": self._sql_config.password,
            "database": self._sql_config.database,
            "port": self._sql_config.port,
            "timeout": self._sql_config.timeout,
            "login_timeout": self._sql_config.timeout,
            "autocommit": True,
        }

    def list_customers(self, search: str | None) -> list[str]:
        normalized_search = self._normalize_lookup_value(search)
        return self._query_names(
            """
SELECT TOP (50) Customer
FROM HILLSBORO.dbo.Customer
WHERE Status = 'Active'
  AND Customer IS NOT NULL
  AND LTRIM(RTRIM(Customer)) <> ''
  AND (%s IS NULL OR Customer LIKE '%%' + %s + '%%')
ORDER BY Customer;
""",
            (normalized_search, normalized_search),
        )

    def list_contacts(self, customer: str | None, search: str | None) -> list[str]:
        normalized_customer = self._normalize_lookup_value(customer)
        if normalized_customer is None:
            return []
        normalized_search = self._normalize_lookup_value(search)
        return self._query_names(
            """
SELECT TOP (50) Contact_Name
FROM HILLSBORO.dbo.Contact
WHERE Contact_Name IS NOT NULL
  AND LTRIM(RTRIM(Contact_Name)) <> ''
  AND Customer IS NOT NULL
  AND LTRIM(RTRIM(Customer)) <> ''
  AND LOWER(LTRIM(RTRIM(Customer))) = LOWER(%s)
  AND (%s IS NULL OR Contact_Name LIKE '%%' + %s + '%%')
GROUP BY Contact_Name
ORDER BY Contact_Name;
""",
            (normalized_customer, normalized_search, normalized_search),
        )

    def contact_belongs_to_customer(
        self,
        contact_name: str | None,
        customer: str | None,
    ) -> bool:
        normalized_contact_name = self._normalize_lookup_value(contact_name)
        normalized_customer = self._normalize_lookup_value(customer)
        if normalized_contact_name is None or normalized_customer is None:
            return False
        connection = self._connect(**self._connection_kwargs())
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
SELECT TOP (1) 1
FROM HILLSBORO.dbo.Contact
WHERE Contact_Name IS NOT NULL
  AND Customer IS NOT NULL
  AND LOWER(LTRIM(RTRIM(Contact_Name))) = LOWER(%s)
  AND LOWER(LTRIM(RTRIM(Customer))) = LOWER(%s);
""",
                (normalized_contact_name, normalized_customer),
            )
            return cursor.fetchone() is not None
        finally:
            cursor.close()
            connection.close()

    def _query_names(self, sql: str, params: tuple[object, ...]) -> list[str]:
        connection = self._connect(**self._connection_kwargs())
        cursor = connection.cursor()
        try:
            cursor.execute(sql, params)
            return [str(row[0]).strip() for row in cursor.fetchall() if row and str(row[0]).strip()]
        finally:
            cursor.close()
            connection.close()

    @staticmethod
    def _normalize_lookup_value(value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


def create_app(
    config: AppConfig,
    bom_intake_service_override: BomIntakeService | None = None,
    doc_package_intake_service_override: DocPackageIntakeService | None = None,
    lookup_service_override: LookupService | None = None,
    quote_prep_service_override: QuotePrepService | None = None,
) -> Callable:
    document_service = DocumentIntakeService(
        automation_drop_root=config.automation_drop_root,
        work_root=config.work_root,
    )
    bom_intake_service: BomIntakeService | None = bom_intake_service_override
    doc_package_intake_service: DocPackageIntakeService | None = (
        doc_package_intake_service_override
    )
    lookup_service: LookupService | None = lookup_service_override
    quote_prep_service: QuotePrepService | None = quote_prep_service_override

    def app(environ, start_response):
        nonlocal bom_intake_service, doc_package_intake_service, lookup_service, quote_prep_service
        method = environ.get("REQUEST_METHOD", "GET").upper()
        path = environ.get("PATH_INFO", "/")

        if path == "/" and method == "GET":
            return _respond_html(start_response, render_page(config, ViewState()))
        if path == "/" and method == "POST":
            if lookup_service is None:
                lookup_service = LookupService(sql_config=SqlServerConfig.load())
            if doc_package_intake_service is None:
                if bom_intake_service is None:
                    bom_intake_service = _build_bom_intake_service()
                doc_package_intake_service = DocPackageIntakeService(
                    document_intake_service=document_service,
                    bom_intake_service=bom_intake_service,
                )
            return _handle_upload(
                environ,
                start_response,
                config,
                doc_package_intake_service,
                lookup_service,
            )
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
        if path == "/api/lookups/customers" and method == "GET":
            if lookup_service is None:
                lookup_service = LookupService(sql_config=SqlServerConfig.load())
            return _handle_lookup_customers(environ, start_response, lookup_service)
        if path == "/api/lookups/contacts" and method == "GET":
            if lookup_service is None:
                lookup_service = LookupService(sql_config=SqlServerConfig.load())
            return _handle_lookup_contacts(environ, start_response, lookup_service)
        if path == "/api/quote-prep/candidates" and method == "GET":
            if quote_prep_service is None:
                quote_prep_service = QuotePrepService(sql_config=SqlServerConfig.load())
            return _handle_quote_prep_candidates(environ, start_response, quote_prep_service)
        if path == "/api/quote-prep/save" and method == "POST":
            if quote_prep_service is None:
                quote_prep_service = QuotePrepService(sql_config=SqlServerConfig.load())
            return _handle_quote_prep_save(environ, start_response, quote_prep_service)

        start_response(
            f"{HTTPStatus.NOT_FOUND.value} {HTTPStatus.NOT_FOUND.phrase}",
            [("Content-Type", "text/plain; charset=utf-8")],
        )
        return [b"Not found"]

    return app


def _handle_upload(
    environ,
    start_response,
    config,
    service: DocPackageIntakeService,
    lookup_service: LookupService,
):
    customer = ""
    rfq_number = ""
    uploaded_by = ""
    quoted_by = ""
    contact_name = ""
    quote_due_date = ""
    intake_notes = ""
    try:
        form = _parse_form_request(environ)
        customer = form.getfirst("customer", "")
        rfq_number = form.getfirst("rfq_number", "")
        uploaded_by = form.getfirst("uploaded_by", "")
        quoted_by = form.getfirst("quoted_by", "")
        contact_name = form.getfirst("contact_name", "")
        quote_due_date = form.getfirst("quote_due_date", "")
        intake_notes = form.getfirst("intake_notes", "")
        normalized_customer = customer.strip()
        normalized_contact = contact_name.strip()
        if normalized_contact and not normalized_customer:
            raise DocPackageIntakeError("Customer is required when Contact is provided.")
        if normalized_contact and not lookup_service.contact_belongs_to_customer(
            contact_name=normalized_contact,
            customer=normalized_customer,
        ):
            raise DocPackageIntakeError(
                "Selected Contact does not belong to the entered Customer."
            )
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

        result = service.intake_package(
            customer_name=customer,
            rfq_number=rfq_number,
            uploaded_by=uploaded_by,
            quoted_by=quoted_by,
            contact_name=contact_name,
            quote_due_date=quote_due_date,
            intake_notes=intake_notes,
            uploaded_files=uploaded_files,
        )
        message = (
            f"Processed {len(result.document_result.processed_files)} file(s) and "
            f"completed BOM intake for {result.customer_name} / RFQ-{result.rfq_number}."
        )
        return _respond_html(
            start_response,
            render_page(
                config,
                ViewState(
                    customer=result.customer_name,
                    rfq_number=result.rfq_number,
                    uploaded_by=result.uploaded_by,
                    quoted_by=result.quoted_by,
                    contact_name=result.contact_name or "",
                    quote_due_date=result.quote_due_date or "",
                    intake_notes=result.intake_notes or "",
                    message=message,
                    result=result.document_result,
                    package_result=result,
                ),
            ),
        )
    except DocPackageIntakeError as exc:
        logger.warning("Doc package intake validation failed: %s", exc)
        return _respond_html(
            start_response,
            render_page(
                config,
                ViewState(
                    customer=customer,
                    rfq_number=rfq_number,
                    uploaded_by=uploaded_by,
                    quoted_by=quoted_by,
                    contact_name=contact_name,
                    quote_due_date=quote_due_date,
                    intake_notes=intake_notes,
                    error=str(exc),
                    result=exc.document_result,
                    diagnostics=exc.diagnostics,
                ),
            ),
            status=HTTPStatus.BAD_REQUEST,
        )
    except Exception:
        logger.exception("Unexpected doc package intake error.")
        return _respond_html(
            start_response,
            render_page(
                config,
                ViewState(
                    customer=customer,
                    rfq_number=rfq_number,
                    uploaded_by=uploaded_by,
                    quoted_by=quoted_by,
                    contact_name=contact_name,
                    quote_due_date=quote_due_date,
                    intake_notes=intake_notes,
                    error="Unexpected server error while processing the document package.",
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


def _handle_lookup_customers(environ, start_response, lookup_service: LookupService):
    try:
        search = _query_value(environ, "search")
        return _respond_json(
            start_response,
            {"items": lookup_service.list_customers(search)},
            status=HTTPStatus.OK,
        )
    except Exception:
        logger.exception("Customer lookup failed.")
        return _respond_json(
            start_response,
            {"error": "Unexpected server error while loading customers."},
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )


def _handle_lookup_contacts(environ, start_response, lookup_service: LookupService):
    try:
        search = _query_value(environ, "search")
        customer = _query_value(environ, "customer")
        return _respond_json(
            start_response,
            {"items": lookup_service.list_contacts(customer, search)},
            status=HTTPStatus.OK,
        )
    except Exception:
        logger.exception("Contact lookup failed.")
        return _respond_json(
            start_response,
            {"error": "Unexpected server error while loading contacts."},
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )


def _handle_quote_prep_candidates(
    environ,
    start_response,
    quote_prep_service: QuotePrepService,
):
    try:
        bom_intake_id = _required_positive_int_query_value(environ, "bom_intake_id")
        return _respond_json(
            start_response,
            {
                "bomIntakeId": bom_intake_id,
                "items": quote_prep_service.get_quote_prep_candidates(bom_intake_id),
            },
            status=HTTPStatus.OK,
        )
    except (ValueError, QuotePrepRequestError) as exc:
        return _respond_json(
            start_response,
            {"error": str(exc)},
            status=HTTPStatus.BAD_REQUEST,
        )
    except QuotePrepDbError as exc:
        logger.exception("Quote prep candidate load failed.")
        return _respond_json(
            start_response,
            {"error": str(exc)},
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )
    except Exception:
        logger.exception("Unexpected quote prep candidate error.")
        return _respond_json(
            start_response,
            {"error": "Unexpected server error while loading quote prep candidates."},
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )


def _handle_quote_prep_save(
    environ,
    start_response,
    quote_prep_service: QuotePrepService,
):
    try:
        request_body = _parse_json_request(environ)
        bom_intake_id_raw = request_body.get("bomIntakeId")
        items = request_body.get("items")
        if not isinstance(bom_intake_id_raw, int) or bom_intake_id_raw <= 0:
            raise ValueError("bomIntakeId must be a positive integer.")
        if not isinstance(items, list):
            raise ValueError("items must be an array.")
        quote_prep_service.save_quote_prep(bom_intake_id_raw, items)
        return _respond_json(start_response, {"saved": True}, status=HTTPStatus.OK)
    except (ValueError, QuotePrepRequestError) as exc:
        return _respond_json(
            start_response,
            {"error": str(exc)},
            status=HTTPStatus.BAD_REQUEST,
        )
    except QuotePrepDbError as exc:
        logger.exception("Quote prep save failed.")
        return _respond_json(
            start_response,
            {"error": str(exc)},
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )
    except Exception:
        logger.exception("Unexpected quote prep save error.")
        return _respond_json(
            start_response,
            {"error": "Unexpected server error while saving quote prep decisions."},
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
        )


def _query_value(environ, name: str) -> str | None:
    query_values = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
    values = query_values.get(name)
    if not values:
        return None
    value = values[-1].strip()
    return value or None


def _required_positive_int_query_value(environ, name: str) -> int:
    value = _query_value(environ, name)
    if value is None:
        raise ValueError(f"{name} is required.")
    if not value.isdigit():
        raise ValueError(f"{name} must be a positive integer.")
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be a positive integer.")
    return parsed


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
    extension_summary: dict[str, int],
) -> list[tuple[str, int]]:
    return sorted(extension_summary.items(), key=lambda item: item[0])


def render_page(config: AppConfig, view_state: ViewState) -> str:
    app_env = html.escape(config.app_env)
    processed_document_overview_html = ""
    quote_prep_button_html = ""
    quote_prep_modal_html = ""
    if view_state.result:
        result = view_state.result
        processed_files_by_extension = _group_processed_files_by_extension(
            result.extension_summary
        )
        visible_filenames = result.processed_files[:5]
        hidden_filenames = result.processed_files[5:]
        processed_files_visible_html = "".join(
            f"<li><code>{html.escape(filename)}</code></li>"
            for filename in visible_filenames
        )
        processed_files_hidden_html = "".join(
            f"<li><code>{html.escape(filename)}</code></li>"
            for filename in hidden_filenames
        )
        processed_files_toggle_html = (
            "<div class=\"actions\">"
            "<button type=\"button\" class=\"secondary\" id=\"toggle-processed-filenames\">Show all filenames</button>"
            "</div>"
            "<ul class=\"count-list hidden\" id=\"processed-filenames-hidden\">"
            f"{processed_files_hidden_html}</ul>"
            if hidden_filenames
            else ""
        )
        processed_files_by_extension_html = "".join(
            "<li>"
            f"<span>{html.escape(extension)}</span>"
            f"<strong>{count}</strong>"
            "</li>"
            for extension, count in processed_files_by_extension
        )
        processed_document_overview_html = (
            "<div class=\"stack\">"
            "<div class=\"overview-card\">"
            "<h4>Processed Document Overview</h4>"
            "<dl class=\"summary-grid\">"
            f"<div><dt>Customer</dt><dd>{html.escape(result.customer_name)}</dd></div>"
            f"<div><dt>RFQ Number</dt><dd>{html.escape(result.rfq_number)}</dd></div>"
            f"<div><dt>Customer folder</dt><dd>{html.escape(result.sanitized_customer_folder_name)}</dd></div>"
            f"<div><dt>RFQ folder</dt><dd>{html.escape(result.sanitized_rfq_folder_name)}</dd></div>"
            f"<div><dt>Uploaded files</dt><dd>{result.uploaded_files_count}</dd></div>"
            f"<div><dt>Processed files</dt><dd>{len(result.processed_files)}</dd></div>"
            f"<div><dt>Automation destination</dt><dd>{html.escape(str(result.automation_path))}</dd></div>"
            f"<div><dt>Working destination</dt><dd>{html.escape(str(result.working_path))}</dd></div>"
            "</dl>"
            "</div>"
            "<div class=\"overview-card\">"
            "<h4>Processed Filenames</h4>"
            "<p class=\"section-note\">Final flattened filenames written to both configured roots for this request.</p>"
            "<ul class=\"count-list\">"
            f"{processed_files_visible_html}</ul>"
            f"{processed_files_toggle_html}"
            "</div>"
            "<div class=\"overview-card\">"
            "<h4>Extension Counts</h4>"
            "<p class=\"section-note\">Processed file counts grouped by lowercase extension.</p>"
            "<ul class=\"count-list\">"
            f"{processed_files_by_extension_html}</ul>"
            "</div>"
            "</div>"
        )

    bom_result_html = ""
    if view_state.package_result:
        bom_result = _serialize_bom_intake_result(view_state.package_result.bom_result)
        summary = bom_result["summary"]
        bom_intake_id = summary.get("bomIntakeId")
        if isinstance(bom_intake_id, int) and bom_intake_id > 0:
            quote_prep_button_html = (
                f"<button type=\"button\" class=\"secondary\" id=\"open-quote-prep-modal\" data-bom-intake-id=\"{bom_intake_id}\">"
                "Create JobBoss Quote"
                "</button>"
            )
        root_results = bom_result["rootResults"]
        root_results_html = "".join(
            "<tr>"
            f"<td>{html.escape(str(root_result.get('rootClientId') or ''))}</td>"
            f"<td>{html.escape(str(root_result.get('rootSequence') or ''))}</td>"
            f"<td>{html.escape(str(root_result.get('customerName') or ''))}</td>"
            f"<td>{html.escape(str(root_result.get('level0PartNumber') or ''))}</td>"
            f"<td>{html.escape(str(root_result.get('revision') or ''))}</td>"
            f"<td>{html.escape(str(root_result.get('decisionStatus') or ''))}</td>"
            f"<td>{html.escape(str(root_result.get('decisionReason') or ''))}</td>"
            f"<td>{html.escape(str(root_result.get('bomRootId') or ''))}</td>"
            f"<td>{html.escape(str(root_result.get('existingBomRootId') or ''))}</td>"
            "</tr>"
            for root_result in root_results
        )
        bom_result_html = (
            "<div class=\"overview-card\">"
            "<h4>BOM Intake Overview</h4>"
            "<dl class=\"summary-grid\">"
            f"<div><dt>Selected BOM file</dt><dd>{html.escape(view_state.package_result.selected_bom_file_name)}</dd></div>"
            f"<div><dt>Detected worksheet</dt><dd>{html.escape(view_state.package_result.bom_preview.detected_worksheet)}</dd></div>"
            f"<div><dt>Detected source type</dt><dd>{html.escape(view_state.package_result.bom_preview.detected_source_type)}</dd></div>"
            f"<div><dt>BomIntakeId</dt><dd>{html.escape(str(summary.get('bomIntakeId') or ''))}</dd></div>"
            f"<div><dt>DetectedRootCount</dt><dd>{html.escape(str(summary.get('detectedRootCount') or ''))}</dd></div>"
            f"<div><dt>AcceptedRootCount</dt><dd>{html.escape(str(summary.get('acceptedRootCount') or ''))}</dd></div>"
            f"<div><dt>DuplicateRejectedCount</dt><dd>{html.escape(str(summary.get('duplicateRejectedCount') or ''))}</dd></div>"
            f"<div><dt>FinalIntakeStatus</dt><dd>{html.escape(str(summary.get('finalIntakeStatus') or ''))}</dd></div>"
            "</dl>"
            "</div>"
            + (
                "<div class=\"overview-card\">"
                "<h4>Detected Top-Level Parts</h4>"
                "<ul class=\"count-list\">"
                + "".join(
                    "<li>"
                    f"<code>{html.escape(str(root.get('part_number') or ''))}</code> "
                    f"(Rev: {html.escape(str(root.get('revision') or ''))}, "
                    f"Decision: {html.escape(str(root.get('decisionStatus') or ''))})"
                    "</li>"
                    for root in view_state.package_result.detected_roots
                )
                + "</ul>"
                "</div>"
                if view_state.package_result.detected_roots
                else ""
            )
            + (
                "<div class=\"overview-card\">"
                "<h4>BOM Root Results</h4>"
                "<div class=\"table-wrap\">"
                "<table>"
                "<thead><tr><th>Root</th><th>Sequence</th><th>Customer</th><th>Level 0 Part</th><th>Revision</th><th>Decision</th><th>Reason</th><th>BomRootId</th><th>ExistingBomRootId</th></tr></thead>"
                f"<tbody>{root_results_html}</tbody>"
                "</table>"
                "</div>"
                "</div>"
                if root_results
                else ""
            )
        )

    diagnostics_html = ""
    if view_state.diagnostics:
        diagnostics_html = (
            "<details>"
            "<summary>Preview Diagnostics</summary>"
            f"<textarea class=\"json-viewer\" readonly>{html.escape(json.dumps(view_state.diagnostics, indent=2))}</textarea>"
            "</details>"
        )

    callout_html = ""
    if view_state.error:
        callout_html = (
            '<section class="stack" id="processed-document-overview" aria-live="polite">'
            '<div class="callout error">'
            "<h3>Doc Package Intake Error</h3>"
            f"<p>{html.escape(view_state.error)}</p>"
            "</div>"
            f"{diagnostics_html}"
            "</section>"
        )
    elif view_state.package_result:
        callout_html = (
            '<section class="stack" id="processed-document-overview" aria-live="polite">'
            '<div class="callout success">'
            "<h3>Doc Package Intake Complete</h3>"
            f"<p>{html.escape(view_state.message)}</p>"
            "</div>"
            f"{processed_document_overview_html}"
            f"{bom_result_html}"
            "</section>"
        )
    elif view_state.result:
        callout_html = (
            '<section class="stack" id="processed-document-overview" aria-live="polite">'
            '<div class="callout success">'
            "<h3>Processed Document Overview</h3>"
            f"<p>{html.escape(view_state.message)}</p>"
            "</div>"
            f"{processed_document_overview_html}"
            "</section>"
        )

    quote_prep_modal_html = """
    <div class="modal-overlay hidden" id="quote-prep-overlay" role="dialog" aria-modal="true" aria-labelledby="quote-prep-title">
      <section class="modal-panel">
        <div class="section-header">
          <h3 id="quote-prep-title">Quote Prep</h3>
          <button type="button" class="ghost" id="close-quote-prep-modal">Close</button>
        </div>
        <p class="section-note">Choose which level-0 parts to include and define quote quantity breaks.</p>
        <div id="quote-prep-error" class="callout error hidden"></div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Include</th>
                <th>Part Number</th>
                <th>Description</th>
                <th>Revision</th>
                <th>Drawing / Item</th>
                <th>Quote Qty Breaks</th>
              </tr>
            </thead>
            <tbody id="quote-prep-rows"></tbody>
          </table>
        </div>
        <div class="actions">
          <button type="button" id="save-quote-prep">Save Quote Prep</button>
          <span class="status-line" id="quote-prep-status"></span>
        </div>
      </section>
    </div>
    """

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Doc Package Intake</title>
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
    .hero-top {{
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 0.75rem;
    }}
    .link-button {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 0.5rem 0.8rem;
      border-radius: 0.5rem;
      border: 1px solid var(--accent);
      background: var(--panel-alt);
      color: var(--accent-strong);
      font-weight: 600;
      text-decoration: none;
      white-space: nowrap;
    }}
    .link-button:hover {{
      background: var(--accent-soft);
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
    .modal-overlay {{
      position: fixed;
      inset: 0;
      background: rgba(18, 24, 27, 0.45);
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 1rem;
      z-index: 50;
    }}
    .modal-panel {{
      width: min(64rem, 96vw);
      max-height: 82vh;
      overflow: auto;
      background: var(--panel);
      border: 1px solid var(--border-strong);
      border-radius: 1rem;
      box-shadow: var(--shadow);
      padding: 1rem;
    }}
    .qty-editor {{
      display: grid;
      gap: 0.5rem;
    }}
    .qty-list {{
      display: flex;
      gap: 0.35rem;
      flex-wrap: wrap;
      margin: 0;
      padding: 0;
      list-style: none;
    }}
    .qty-list li {{
      display: inline-flex;
      align-items: center;
      gap: 0.3rem;
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 0.25rem 0.55rem;
      background: white;
    }}
    .qty-add-row {{
      display: flex;
      gap: 0.4rem;
      align-items: center;
    }}
    .qty-add-row input {{
      width: 7rem;
      padding: 0.45rem 0.5rem;
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
      <div class="hero-top">
        <span class="eyebrow">Environment: {app_env}</span>
        <a class="link-button" href="http://development.qpd.lan:8094/" target="_blank" rel="noopener noreferrer">Bom Formatter</a>
      </div>
      <h1>Doc Package Intake</h1>
      <p>Upload a customer document package once. The intake flow flattens and mirrors the processed files into both configured roots, then resolves and processes the BOM intake from the same uploaded package.</p>
    </section>

    <section class="panel">
      <div class="section-header">
        <div class="stack">
          <h2>Doc Package Intake</h2>
          <p class="subtle">Include the full customer package here. The intake flow writes the processed document set to both configured roots and runs BOM intake from the same uploaded files.</p>
        </div>
      </div>

      <form method="post" enctype="multipart/form-data" novalidate>
        <div class="form-grid">
          <label for="customer">
            Customer
            <input id="customer" name="customer" type="text" list="customer_suggestions" required value="{html.escape(view_state.customer)}">
            <datalist id="customer_suggestions"></datalist>
          </label>
          <label for="rfq_number">
            RFQ Number
            <input id="rfq_number" name="rfq_number" type="text" required value="{html.escape(view_state.rfq_number)}">
          </label>
          <label for="uploaded_by">
            Uploaded By
            <input id="uploaded_by" name="uploaded_by" type="text" required value="{html.escape(view_state.uploaded_by)}">
          </label>
          <label for="quoted_by">
            Quoted By
            <input id="quoted_by" name="quoted_by" type="text" required value="{html.escape(view_state.quoted_by)}">
          </label>
          <label for="contact_name">
            Contact
            <input id="contact_name" name="contact_name" type="text" list="contact_suggestions" value="{html.escape(view_state.contact_name)}"{" disabled" if not view_state.customer.strip() else ""}>
            <datalist id="contact_suggestions"></datalist>
          </label>
          <label for="quote_due_date">
            Due Date
            <input id="quote_due_date" name="quote_due_date" type="date" value="{html.escape(view_state.quote_due_date)}">
          </label>
          <label for="documents">
            Package Files
            <input id="documents" name="documents" type="file" multiple required>
          </label>
        </div>

        <label for="intake_notes">
          Intake Notes
          <textarea id="intake_notes" name="intake_notes" placeholder="Optional notes for the intake record">{html.escape(view_state.intake_notes)}</textarea>
        </label>

        <div class="actions">
          <button type="submit">Process Doc Package</button>
          {quote_prep_button_html}
          <span class="status-line">Processing flattens zip contents, mirrors the processed outputs into both configured roots, and runs BOM intake from the same package.</span>
        </div>
      </form>

      {callout_html}
    </section>
    {quote_prep_modal_html}
  </main>
  <script>
    (function() {{
      function renderDatalistOptions(datalist, items) {{
        datalist.innerHTML = items
          .map((item) => "<option value=\\"" + String(item).replaceAll("\\"", "&quot;") + "\\"></option>")
          .join("");
      }}

      function hookLookup(inputId, datalistId, endpoint) {{
        const input = document.getElementById(inputId);
        const datalist = document.getElementById(datalistId);
        if (!input || !datalist) return;
        let token = 0;
        input.addEventListener("input", async function() {{
          const search = input.value.trim();
          token += 1;
          const requestToken = token;
          try {{
            const response = await fetch(endpoint + "?search=" + encodeURIComponent(search));
            if (!response.ok || requestToken !== token) return;
            const payload = await response.json();
            if (!payload || !Array.isArray(payload.items)) return;
            renderDatalistOptions(datalist, payload.items);
          }} catch (_err) {{
          }}
        }});
      }}

      function hookCustomerFilteredContactLookup(customerId, contactId, datalistId, endpoint) {{
        const customerInput = document.getElementById(customerId);
        const contactInput = document.getElementById(contactId);
        const datalist = document.getElementById(datalistId);
        if (!customerInput || !contactInput || !datalist) return;
        let token = 0;

        function syncContactEnabledState() {{
          const hasCustomer = customerInput.value.trim().length > 0;
          contactInput.disabled = !hasCustomer;
          if (!hasCustomer) {{
            contactInput.value = "";
            renderDatalistOptions(datalist, []);
          }}
        }}

        async function loadContacts() {{
          const customer = customerInput.value.trim();
          if (!customer) {{
            renderDatalistOptions(datalist, []);
            return;
          }}
          const search = contactInput.value.trim();
          token += 1;
          const requestToken = token;
          try {{
            const response = await fetch(
              endpoint +
              "?customer=" + encodeURIComponent(customer) +
              "&search=" + encodeURIComponent(search)
            );
            if (!response.ok || requestToken !== token) return;
            const payload = await response.json();
            if (!payload || !Array.isArray(payload.items)) return;
            renderDatalistOptions(datalist, payload.items);
          }} catch (_err) {{
          }}
        }}

        customerInput.addEventListener("input", function() {{
          contactInput.value = "";
          renderDatalistOptions(datalist, []);
          syncContactEnabledState();
          if (contactInput.disabled) return;
          void loadContacts();
        }});

        contactInput.addEventListener("input", function() {{
          if (contactInput.disabled) return;
          void loadContacts();
        }});

        syncContactEnabledState();
      }}

      hookLookup("customer", "customer_suggestions", "/api/lookups/customers");
      hookCustomerFilteredContactLookup("customer", "contact_name", "contact_suggestions", "/api/lookups/contacts");

      const toggleProcessedButton = document.getElementById("toggle-processed-filenames");
      const hiddenFilenames = document.getElementById("processed-filenames-hidden");
      if (toggleProcessedButton && hiddenFilenames) {{
        toggleProcessedButton.addEventListener("click", function() {{
          const isHidden = hiddenFilenames.classList.contains("hidden");
          hiddenFilenames.classList.toggle("hidden", !isHidden);
          toggleProcessedButton.textContent = isHidden ? "Collapse filenames" : "Show all filenames";
        }});
      }}

      const openQuotePrepButton = document.getElementById("open-quote-prep-modal");
      const overlay = document.getElementById("quote-prep-overlay");
      const closeQuotePrepButton = document.getElementById("close-quote-prep-modal");
      const saveQuotePrepButton = document.getElementById("save-quote-prep");
      const rowsEl = document.getElementById("quote-prep-rows");
      const statusEl = document.getElementById("quote-prep-status");
      const errorEl = document.getElementById("quote-prep-error");
      let quotePrepRows = [];
      let activeBomIntakeId = null;

      function showQuotePrepError(message) {{
        if (!errorEl) return;
        if (!message) {{
          errorEl.classList.add("hidden");
          errorEl.textContent = "";
          return;
        }}
        errorEl.classList.remove("hidden");
        errorEl.textContent = message;
      }}

      function parseQty(value) {{
        const parsed = Number(value);
        if (!Number.isInteger(parsed) || parsed <= 0) return null;
        return parsed;
      }}

      function qtyValuesToString(qtys) {{
        return qtys.join(",");
      }}

      function renderQuotePrepRows() {{
        if (!rowsEl) return;
        rowsEl.innerHTML = "";
        quotePrepRows.forEach((row, index) => {{
          const tr = document.createElement("tr");
          tr.innerHTML =
            "<td><input type=\\"checkbox\\" " + (row.includeInQuote ? "checked" : "") + " data-idx=\\"" + index + "\\" data-act=\\"toggle-include\\"></td>" +
            "<td><code>" + row.partNumber + "</code></td>" +
            "<td>" + row.description + "</td>" +
            "<td>" + row.revision + "</td>" +
            "<td>" + row.drawingOrItem + "</td>" +
            "<td>" +
              "<div class=\\"qty-editor\\">" +
                "<ul class=\\"qty-list\\" id=\\"qty-list-" + index + "\\"></ul>" +
                "<div class=\\"qty-add-row\\">" +
                  "<input type=\\"number\\" min=\\"1\\" step=\\"1\\" id=\\"qty-input-" + index + "\\" " + (row.includeInQuote ? "" : "disabled") + ">" +
                  "<button type=\\"button\\" class=\\"ghost\\" data-idx=\\"" + index + "\\" data-act=\\"add-qty\\" " + (row.includeInQuote ? "" : "disabled") + ">Add</button>" +
                "</div>" +
              "</div>" +
            "</td>";
          rowsEl.appendChild(tr);

          const qtyListEl = document.getElementById("qty-list-" + index);
          if (qtyListEl) {{
            row.qtys.forEach((qty, qtyIndex) => {{
              const li = document.createElement("li");
              li.innerHTML =
                "<span>" + qty + "</span>" +
                "<button type=\\"button\\" class=\\"ghost\\" data-idx=\\"" + index + "\\" data-qty-idx=\\"" + qtyIndex + "\\" data-act=\\"remove-qty\\" " + (row.includeInQuote ? "" : "disabled") + ">x</button>";
              qtyListEl.appendChild(li);
            }});
          }}
        }});
      }}

      if (rowsEl) {{
        rowsEl.addEventListener("click", function(event) {{
          const target = event.target;
          if (!(target instanceof HTMLElement)) return;
          const action = target.getAttribute("data-act");
          if (!action) return;
          const idx = Number(target.getAttribute("data-idx"));
          const row = quotePrepRows[idx];
          if (!row) return;

          if (action === "add-qty") {{
            const input = document.getElementById("qty-input-" + idx);
            if (!(input instanceof HTMLInputElement)) return;
            const parsed = parseQty(input.value.trim());
            if (parsed === null) {{
              showQuotePrepError("Qty breaks must be positive whole numbers.");
              return;
            }}
            if (row.qtys.includes(parsed)) {{
              showQuotePrepError("Qty breaks cannot contain duplicate values.");
              return;
            }}
            row.qtys.push(parsed);
            input.value = "";
            showQuotePrepError("");
            renderQuotePrepRows();
            return;
          }}

          if (action === "remove-qty") {{
            const qtyIdx = Number(target.getAttribute("data-qty-idx"));
            row.qtys.splice(qtyIdx, 1);
            renderQuotePrepRows();
          }}
        }});

        rowsEl.addEventListener("change", function(event) {{
          const target = event.target;
          if (!(target instanceof HTMLElement)) return;
          if (target.getAttribute("data-act") !== "toggle-include") return;
          const idx = Number(target.getAttribute("data-idx"));
          const row = quotePrepRows[idx];
          if (!row || !(target instanceof HTMLInputElement)) return;
          row.includeInQuote = target.checked;
          if (row.includeInQuote && row.qtys.length === 0) {{
            row.qtys = [1];
          }}
          renderQuotePrepRows();
        }});
      }}

      async function loadQuotePrepCandidates() {{
        if (!activeBomIntakeId || !rowsEl) return;
        showQuotePrepError("");
        statusEl.textContent = "Loading quote prep candidates...";
        try {{
          const response = await fetch("/api/quote-prep/candidates?bom_intake_id=" + encodeURIComponent(String(activeBomIntakeId)));
          const payload = await response.json();
          if (!response.ok) {{
            throw new Error(payload && payload.error ? payload.error : "Unable to load candidates.");
          }}
          quotePrepRows = (payload.items || []).map((item) => {{
            const qtySource = (item.quoteQtyBreaks || "1").trim();
            const qtys = qtySource
              ? qtySource.split(",").map((v) => Number(v.trim())).filter((v) => Number.isInteger(v) && v > 0)
              : [];
            return {{
              bomRootId: item.bomRootId,
              includeInQuote: Boolean(item.includeInQuote),
              partNumber: String(item.partNumber || ""),
              description: String(item.description || ""),
              revision: String(item.revision || ""),
              drawingOrItem: String(item.drawingOrItem || ""),
              qtys: qtys.length > 0 ? qtys : [1],
            }};
          }});
          renderQuotePrepRows();
          statusEl.textContent = "Loaded " + quotePrepRows.length + " candidate part(s).";
        }} catch (err) {{
          const message = err instanceof Error ? err.message : "Unable to load candidates.";
          showQuotePrepError(message);
          statusEl.textContent = "";
        }}
      }}

      async function saveQuotePrep() {{
        if (!activeBomIntakeId) return;
        showQuotePrepError("");
        const items = [];
        for (const row of quotePrepRows) {{
          if (!row.includeInQuote) {{
            items.push({{
              bomRootId: row.bomRootId,
              includeInQuote: false,
              quoteQtyBreaks: "",
            }});
            continue;
          }}
          if (!Array.isArray(row.qtys) || row.qtys.length === 0) {{
            showQuotePrepError("Each included row requires at least one qty break.");
            return;
          }}
          const seen = new Set();
          for (const qty of row.qtys) {{
            if (!Number.isInteger(qty) || qty <= 0) {{
              showQuotePrepError("Qty breaks must be positive whole numbers.");
              return;
            }}
            if (seen.has(qty)) {{
              showQuotePrepError("Qty breaks cannot contain duplicate values.");
              return;
            }}
            seen.add(qty);
          }}
          items.push({{
            bomRootId: row.bomRootId,
            includeInQuote: true,
            quoteQtyBreaks: qtyValuesToString(row.qtys),
          }});
        }}

        statusEl.textContent = "Saving quote prep decisions...";
        try {{
          const response = await fetch("/api/quote-prep/save", {{
            method: "POST",
            headers: {{ "Content-Type": "application/json" }},
            body: JSON.stringify({{
              bomIntakeId: activeBomIntakeId,
              items: items,
            }}),
          }});
          const payload = await response.json();
          if (!response.ok) {{
            throw new Error(payload && payload.error ? payload.error : "Unable to save quote prep decisions.");
          }}
          statusEl.textContent = "Quote prep saved.";
        }} catch (err) {{
          const message = err instanceof Error ? err.message : "Unable to save quote prep decisions.";
          showQuotePrepError(message);
          statusEl.textContent = "";
        }}
      }}

      function closeQuotePrepModal() {{
        if (!overlay) return;
        overlay.classList.add("hidden");
      }}

      if (openQuotePrepButton && overlay) {{
        openQuotePrepButton.addEventListener("click", function() {{
          const bomIntakeIdValue = openQuotePrepButton.getAttribute("data-bom-intake-id");
          if (!bomIntakeIdValue) return;
          activeBomIntakeId = Number(bomIntakeIdValue);
          overlay.classList.remove("hidden");
          void loadQuotePrepCandidates();
        }});
      }}
      if (closeQuotePrepButton) {{
        closeQuotePrepButton.addEventListener("click", closeQuotePrepModal);
      }}
      if (overlay) {{
        overlay.addEventListener("click", function(event) {{
          if (event.target === overlay) closeQuotePrepModal();
        }});
      }}
      if (saveQuotePrepButton) {{
        saveQuotePrepButton.addEventListener("click", function() {{
          void saveQuotePrep();
        }});
      }}
    }})();
  </script>
</body>
</html>"""
