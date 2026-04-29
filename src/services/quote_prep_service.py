from __future__ import annotations

import hashlib
import json
import logging
import xml.etree.ElementTree as ET
from datetime import date, datetime
from typing import Any, Callable
from uuid import uuid4
from xml.sax.saxutils import escape as xml_escape

from src.config import SqlServerConfig
from src.services.bom_intake_db import _load_pymssql_connect

logger = logging.getLogger(__name__)


class QuotePrepError(ValueError):
    pass


class QuotePrepRequestError(QuotePrepError):
    pass


class QuotePrepDbError(QuotePrepError):
    pass


class QuotePrepService:
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

    def get_quote_prep_candidates(self, bom_intake_id: int) -> list[dict[str, object]]:
        connection = self._connect(**self._connection_kwargs())
        cursor = connection.cursor(as_dict=True)
        try:
            cursor.execute(
                """
EXEC dbo.usp_BOM_Root_GetQuotePrepCandidates
    @BomIntakeId = %s;
""",
                (bom_intake_id,),
            )
            rows = cursor.fetchall() or []
            return [self._serialize_candidate_row(row) for row in rows if isinstance(row, dict)]
        except Exception as exc:
            raise QuotePrepDbError("Failed to load quote prep candidates.") from exc
        finally:
            cursor.close()
            connection.close()

    def save_quote_prep(self, bom_intake_id: int, items: list[dict[str, object]]) -> dict[str, object]:
        payload_rows = [self._normalize_save_item(item) for item in items]
        payload_json = json.dumps(payload_rows, separators=(",", ":"))

        connection = self._connect(**self._connection_kwargs())
        cursor = connection.cursor(as_dict=True)
        try:
            cursor.execute(
                """
EXEC dbo.usp_BOM_Root_SaveQuotePrep
    @BomIntakeId = %s,
    @QuotePrepJson = %s;
""",
                (bom_intake_id, payload_json),
            )
            bridge_request = self._build_and_submit_jobboss_quote_request(
                cursor=cursor,
                bom_intake_id=bom_intake_id,
            )
            return bridge_request
        except Exception as exc:
            raise QuotePrepDbError("Failed to save quote prep decisions.") from exc
        finally:
            cursor.close()
            connection.close()

    def get_jobboss_request_status(self, jobboss_request_id: int) -> dict[str, object]:
        if jobboss_request_id <= 0:
            raise QuotePrepRequestError("jobBossRequestId must be a positive integer.")

        connection = self._connect(**self._connection_kwargs())
        cursor = connection.cursor(as_dict=True)
        try:
            cursor.execute(
                """
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
  AND TABLE_NAME = 'JobBossRequest';
"""
            )
            columns = {
                str(row.get("COLUMN_NAME"))
                for row in (cursor.fetchall() or [])
                if isinstance(row, dict) and row.get("COLUMN_NAME")
            }
            if "JobBossRequestId" not in columns:
                raise QuotePrepDbError("JobBossRequest table is not available.")

            status_col = _first_existing_column(
                columns, "RequestStatus", "Status", "BridgeStatus"
            )
            error_col = _first_existing_column(
                columns, "LastErrorMessage", "ErrorMessage", "LastError", "Error"
            )
            response_xml_col = _first_existing_column(
                columns, "ResponseXml", "ResponseXML", "BridgeResponseXml"
            )

            selected_columns = ["JobBossRequestId"]
            if status_col:
                selected_columns.append(status_col)
            if error_col:
                selected_columns.append(error_col)
            if response_xml_col:
                selected_columns.append(response_xml_col)

            select_list = ", ".join(f"[{name}]" for name in selected_columns)
            cursor.execute(
                f"""
SELECT {select_list}
FROM dbo.JobBossRequest
WHERE JobBossRequestId = %s;
""",
                (jobboss_request_id,),
            )
            row = cursor.fetchone()
            if not isinstance(row, dict):
                raise QuotePrepRequestError("JobBossRequestId was not found.")

            raw_status = _optional_text(row.get(status_col)) if status_col else None
            normalized_status = _normalize_bridge_status(raw_status)
            last_error = _optional_text(row.get(error_col)) if error_col else None
            response_xml = _optional_text(row.get(response_xml_col)) if response_xml_col else None
            quote_id = _extract_quote_id_from_response_xml(response_xml)
            if normalized_status == "Success" and not quote_id:
                quote_id = None

            return {
                "jobBossRequestId": int(row.get("JobBossRequestId") or jobboss_request_id),
                "status": normalized_status,
                "lastError": last_error,
                "jobBossQuoteId": quote_id,
            }
        except QuotePrepError:
            raise
        except Exception as exc:
            raise QuotePrepDbError("Failed to load JobBOSS bridge request status.") from exc
        finally:
            cursor.close()
            connection.close()

    def _build_and_submit_jobboss_quote_request(
        self,
        *,
        cursor: Any,
        bom_intake_id: int,
    ) -> dict[str, object]:
        intake_row, included_roots = self._load_jobboss_request_source_data(
            cursor=cursor,
            bom_intake_id=bom_intake_id,
        )
        quote_lines = self._build_quote_lines(included_roots)
        request_xml = self._build_quote_add_xml(intake_row, quote_lines)
        logger.info("JobBOSS QuoteAddRq preview XML: %s", request_xml)
        payload_json = self._build_payload_json(
            bom_intake_id=bom_intake_id,
            intake_row=intake_row,
            quote_lines=quote_lines,
        )
        request_id = self._create_jobboss_request(
            cursor=cursor,
            bom_intake_id=bom_intake_id,
            intake_row=intake_row,
            quote_lines=quote_lines,
            request_xml=request_xml,
            payload_json=payload_json,
        )
        return {"saved": True, "jobBossRequestId": request_id}

    def _load_jobboss_request_source_data(
        self,
        *,
        cursor: Any,
        bom_intake_id: int,
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        cursor.execute(
            """
SELECT
    bi.BomIntakeId,
    bi.IntakeGuid,
    bi.CustomerName,
    bi.ContactName,
    bi.QuoteNumber,
    bi.QuoteDueDate,
    bi.QuotedBy,
    bi.UploadedBy
FROM dbo.BOM_Intake AS bi
WHERE bi.BomIntakeId = %s;
""",
            (bom_intake_id,),
        )
        intake_row = cursor.fetchone()
        if not isinstance(intake_row, dict):
            raise QuotePrepRequestError("BOM intake record was not found.")

        cursor.execute(
            """
SELECT
    br.BomRootId,
    br.Level0PartNumber,
    br.RootDescription,
    br.Revision,
    br.QuoteQtyBreaks
FROM dbo.BOM_Root AS br
WHERE br.BomIntakeId = %s
  AND br.IncludeInQuote = 1
ORDER BY br.BomRootId ASC;
""",
            (bom_intake_id,),
        )
        included_roots = cursor.fetchall() or []
        if not included_roots:
            raise QuotePrepRequestError("No BOM root lines are included for quote creation.")

        return intake_row, [row for row in included_roots if isinstance(row, dict)]

    def _build_quote_lines(self, included_roots: list[dict[str, object]]) -> list[dict[str, object]]:
        quote_lines: list[dict[str, object]] = []
        for index, root in enumerate(included_roots, start=1):
            line_token = f"{index:03d}"
            qty_breaks = _optional_text(root.get("QuoteQtyBreaks")) or "1"
            quantities = _parse_quote_quantities(qty_breaks)
            quote_lines.append(
                {
                    "bomRootId": root.get("BomRootId"),
                    "lineItemId": line_token,
                    "lineNumber": line_token,
                    "partNumber": _optional_text(root.get("Level0PartNumber")) or "",
                    "description": _optional_text(root.get("RootDescription")) or "",
                    "revision": _optional_text(root.get("Revision")) or "",
                    "quoteQtyBreaks": qty_breaks,
                    "quantities": quantities,
                }
            )
        return quote_lines

    def _build_quote_add_xml(
        self,
        intake_row: dict[str, object],
        quote_lines: list[dict[str, object]],
    ) -> str:
        quote_add_fields: list[str] = []
        _append_xml_tag(quote_add_fields, "ID", "")
        _append_xml_tag(quote_add_fields, "Reference", _optional_text(intake_row.get("QuoteNumber")))
        _append_xml_tag(quote_add_fields, "QuotedBy", _optional_text(intake_row.get("QuotedBy")))
        _append_xml_tag(quote_add_fields, "DueDate", _as_iso_date(intake_row.get("QuoteDueDate")))
        _append_xml_tag(quote_add_fields, "Status", "Active")

        quote_customer_fields: list[str] = []
        _append_xml_tag(quote_customer_fields, "CustomerRef", _optional_text(intake_row.get("CustomerName")))
        _append_xml_tag(quote_customer_fields, "ContactRef", _optional_text(intake_row.get("ContactName")))

        line_xml_parts: list[str] = []
        quoted_by = _optional_text(intake_row.get("QuotedBy"))
        for line in quote_lines:
            line_fields: list[str] = []
            _append_xml_tag(line_fields, "LineItemID", line["lineItemId"])
            _append_xml_tag(line_fields, "LineNumber", line["lineNumber"])
            _append_xml_tag(line_fields, "PartNumber", line["partNumber"])
            _append_xml_tag(line_fields, "PartDescription", line["description"])
            _append_xml_tag(line_fields, "PartRevision", line["revision"])
            _append_xml_tag(line_fields, "QuotedBy", quoted_by)
            line_xml_parts.append(f"<QuoteLineItemAdd>{''.join(line_fields)}</QuoteLineItemAdd>")

            for qty in line["quantities"]:
                qty_fields: list[str] = []
                _append_xml_tag(qty_fields, "LineItemID", line["lineItemId"])
                _append_xml_tag(qty_fields, "QuotedQuantity", str(qty))
                line_xml_parts.append(f"<QuoteQuantityAdd>{''.join(qty_fields)}</QuoteQuantityAdd>")

        return (
            "<JBXML>"
            '<JBXMLRequest Session="{SESSION_ID}">'
            "<QuoteAddRq>"
            f"<QuoteAdd>{''.join(quote_add_fields)}</QuoteAdd>"
            f"<QuoteSetUpCustomerInfo>{''.join(quote_customer_fields)}</QuoteSetUpCustomerInfo>"
            f"{''.join(line_xml_parts)}"
            "</QuoteAddRq>"
            "</JBXMLRequest>"
            "</JBXML>"
        )

    def _build_payload_json(
        self,
        *,
        bom_intake_id: int,
        intake_row: dict[str, object],
        quote_lines: list[dict[str, object]],
    ) -> str:
        payload = {
            "bomIntakeId": bom_intake_id,
            "intakeGuid": _optional_text(intake_row.get("IntakeGuid")) or "",
            "customerName": _optional_text(intake_row.get("CustomerName")) or "",
            "contactName": _optional_text(intake_row.get("ContactName")) or "",
            "rfqNumber": _optional_text(intake_row.get("QuoteNumber")) or "",
            "quoteDueDate": _as_iso_date(intake_row.get("QuoteDueDate")) or "",
            "quotedBy": _optional_text(intake_row.get("QuotedBy")) or "",
            "lines": quote_lines,
        }
        return json.dumps(payload, separators=(",", ":"))

    def _create_jobboss_request(
        self,
        *,
        cursor: Any,
        bom_intake_id: int,
        intake_row: dict[str, object],
        quote_lines: list[dict[str, object]],
        request_xml: str,
        payload_json: str,
    ) -> int:
        lines_hash = _hash_included_lines(quote_lines)
        requested_by = (
            _optional_text(intake_row.get("QuotedBy"))
            or _optional_text(intake_row.get("UploadedBy"))
            or "system"
        )
        correlation_id = str(uuid4())
        idempotency_key = f"QuoteAdd:BOM_Intake:{bom_intake_id}:QuotePrep:{lines_hash}"

        cursor.execute(
            """
SET NOCOUNT ON;
DECLARE @JobBossRequestId BIGINT;
EXEC dbo.usp_JobBossRequest_Create
    @SourceEntityType = %s,
    @SourceEntityId = %s,
    @CorrelationId = %s,
    @IdempotencyKey = %s,
    @Destination = %s,
    @RequestMode = %s,
    @ActionName = %s,
    @PayloadStorageMode = %s,
    @PayloadJson = %s,
    @RequestXml = %s,
    @SourceApp = %s,
    @RequestedBy = %s,
    @Priority = %s,
    @HelperAssignmentMode = %s,
    @SchemaVersion = %s,
    @MaxRetryCount = %s,
    @JobBossRequestId = @JobBossRequestId OUTPUT;
SELECT @JobBossRequestId AS JobBossRequestId;
""",
            (
                "BOM_Intake",
                str(bom_intake_id),
                correlation_id,
                idempotency_key,
                "JobBOSS",
                "Single",
                "QuoteAdd",
                "JsonAndXml",
                payload_json,
                request_xml,
                "EstimatingTool",
                requested_by,
                0,
                "Auto",
                1,
                5,
            ),
        )
        result = cursor.fetchone()
        if not isinstance(result, dict) or not result.get("JobBossRequestId"):
            raise QuotePrepDbError("Bridge request did not return JobBossRequestId.")
        return int(result["JobBossRequestId"])

    def _serialize_candidate_row(self, row: dict[str, object]) -> dict[str, object]:
        return {
            "bomRootId": row.get("BomRootId"),
            "includeInQuote": bool(row.get("IncludeInQuote", True)),
            "partNumber": _optional_text(row.get("Level0PartNumber")) or "",
            "description": _optional_text(row.get("RootDescription")) or "",
            "revision": _optional_text(row.get("Revision")) or "",
            "drawingOrItem": (
                _optional_text(row.get("DrawingNumber"))
                or _optional_text(row.get("Drawing"))
                or _optional_text(row.get("RootItemNumber"))
                or _optional_text(row.get("ItemNumber"))
                or ""
            ),
            "quoteQtyBreaks": _optional_text(row.get("QuoteQtyBreaks")) or "1",
        }

    def _normalize_save_item(self, item: dict[str, object]) -> dict[str, object]:
        if not isinstance(item, dict):
            raise QuotePrepRequestError("Each quote prep item must be an object.")

        bom_root_id = item.get("bomRootId")
        if not isinstance(bom_root_id, int) or bom_root_id <= 0:
            raise QuotePrepRequestError("bomRootId must be a positive integer.")

        include_in_quote = item.get("includeInQuote")
        if not isinstance(include_in_quote, bool):
            raise QuotePrepRequestError("includeInQuote must be true or false.")

        quote_qty_breaks_raw = item.get("quoteQtyBreaks")
        if quote_qty_breaks_raw is None:
            quote_qty_breaks_raw = ""
        if not isinstance(quote_qty_breaks_raw, str):
            raise QuotePrepRequestError("quoteQtyBreaks must be a string.")

        if not include_in_quote:
            normalized_qty_breaks = ""
        else:
            normalized_qty_breaks = _normalize_quote_qty_breaks(quote_qty_breaks_raw)

        return {
            "bomRootId": bom_root_id,
            "includeInQuote": include_in_quote,
            "quoteQtyBreaks": normalized_qty_breaks,
        }


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_existing_column(columns: set[str], *candidates: str) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _normalize_bridge_status(value: str | None) -> str:
    if value is None:
        return "Queued"
    normalized = value.strip().lower()
    if normalized in {"queued", "pending", "ready", "new"}:
        return "Queued"
    if normalized in {"running", "in_progress", "processing", "working"}:
        return "Running"
    if normalized in {"success", "succeeded", "completed", "done"}:
        return "Success"
    if normalized in {"failed", "error", "dead", "aborted"}:
        return "Failed"
    return value.strip() or "Queued"


def _extract_quote_id_from_response_xml(response_xml: str | None) -> str | None:
    if not response_xml:
        return None
    try:
        root = ET.fromstring(response_xml)
    except ET.ParseError:
        return None

    for element in root.iter():
        if _local_name(element.tag) == "QuoteAddRs":
            for child in element.iter():
                if _local_name(child.tag) == "ID":
                    value = _optional_text(child.text)
                    if value:
                        return value
    for element in root.iter():
        if _local_name(element.tag) == "QuoteRet":
            for child in element.iter():
                if _local_name(child.tag) == "ID":
                    value = _optional_text(child.text)
                    if value:
                        return value
    return None


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _normalize_quote_qty_breaks(value: str) -> str:
    parts = [part.strip() for part in value.split(",")]
    if not parts or any(part == "" for part in parts):
        raise QuotePrepRequestError("quoteQtyBreaks cannot contain blank values.")

    normalized: list[str] = []
    seen: set[int] = set()
    for part in parts:
        if not part.isdigit():
            raise QuotePrepRequestError("quoteQtyBreaks must contain positive integers.")
        qty = int(part)
        if qty <= 0:
            raise QuotePrepRequestError("quoteQtyBreaks must contain positive integers.")
        if qty in seen:
            raise QuotePrepRequestError("quoteQtyBreaks cannot contain duplicate values.")
        seen.add(qty)
        normalized.append(str(qty))

    return ",".join(normalized)


def _parse_quote_quantities(value: str) -> list[int]:
    normalized = _normalize_quote_qty_breaks(value)
    return [int(part) for part in normalized.split(",")]


def _append_xml_tag(parts: list[str], tag: str, value: str | None) -> None:
    if value is None:
        return
    parts.append(f"<{tag}>{xml_escape(value)}</{tag}>")


def _as_iso_date(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = _optional_text(value)
    return text or None


def _hash_included_lines(lines: list[dict[str, object]]) -> str:
    hash_input = [
        {
            "bomRootId": line.get("bomRootId"),
            "partNumber": line.get("partNumber"),
            "revision": line.get("revision"),
            "quoteQtyBreaks": line.get("quoteQtyBreaks"),
        }
        for line in lines
    ]
    payload = json.dumps(hash_input, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
