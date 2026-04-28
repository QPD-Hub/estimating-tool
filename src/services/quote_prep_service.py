from __future__ import annotations

import json
from typing import Any, Callable

from src.config import SqlServerConfig
from src.services.bom_intake_db import _load_pymssql_connect


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

    def save_quote_prep(self, bom_intake_id: int, items: list[dict[str, object]]) -> None:
        payload_rows = [self._normalize_save_item(item) for item in items]
        payload_json = json.dumps(payload_rows, separators=(",", ":"))

        connection = self._connect(**self._connection_kwargs())
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
EXEC dbo.usp_BOM_Root_SaveQuotePrep
    @BomIntakeId = %s,
    @QuotePrepJson = %s;
""",
                (bom_intake_id, payload_json),
            )
        except Exception as exc:
            raise QuotePrepDbError("Failed to save quote prep decisions.") from exc
        finally:
            cursor.close()
            connection.close()

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
