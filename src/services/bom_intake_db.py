from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Any

from src.config import SqlServerConfig, SqlServerConfigError

logger = logging.getLogger(__name__)

ROOT_COLUMNS = (
    "RootClientId",
    "RootSequence",
    "SourceRowNumber",
    "CustomerName",
    "Level0PartNumber",
    "Revision",
    "RootDescription",
    "RootItemNumber",
    "RootQuantity",
    "RootUOM",
    "RootMakeBuy",
    "RootMFR",
    "RootMFRNumber",
)

ROW_COLUMNS = (
    "RootClientId",
    "RowSequence",
    "SourceRowNumber",
    "OriginalValue",
    "ParentPart",
    "PartNumber",
    "IndentedPartNumber",
    "BomLevel",
    "Description",
    "Revision",
    "Quantity",
    "UOM",
    "ItemNumber",
    "MakeBuy",
    "MFR",
    "MFRNumber",
    "LeadTimeDays",
    "Cost",
    "IsLevel0",
    "ValidationMessage",
)

CREATE_HEADER_COLUMNS = (
    "CustomerName",
    "QuoteNumber",
    "SourceFileName",
    "SourceFilePath",
    "SourceSheetName",
    "SourceType",
    "UploadedBy",
    "ParserVersion",
    "IntakeNotes",
)


class BomIntakeDbError(RuntimeError):
    pass


class BomIntakeDbConnectionError(BomIntakeDbError):
    pass


class BomIntakeDbProcedureError(BomIntakeDbError):
    pass


class BomIntakeDbService:
    def __init__(
        self,
        sql_config: SqlServerConfig,
        connect: Callable[..., Any] | None = None,
    ) -> None:
        self._sql_config = sql_config
        self._connect = connect or _load_pymssql_connect()

    def build_connection_kwargs(self) -> dict[str, object]:
        return {
            "server": self._sql_config.host,
            "user": self._sql_config.username,
            "password": self._sql_config.password,
            "database": self._sql_config.database,
            "port": self._sql_config.port,
            "timeout": self._sql_config.timeout,
            "login_timeout": self._sql_config.timeout,
            "autocommit": False,
        }

    def create_and_process_intake(
        self,
        *,
        header: dict[str, object],
        root_candidates: list[dict[str, object]],
        bom_rows: list[dict[str, object]],
        detected_by: str,
    ) -> dict[str, object]:
        if not detected_by.strip():
            raise BomIntakeDbError("DetectedBy is required for BOM intake processing.")

        connection_kwargs = self.build_connection_kwargs()
        logger.info(
            "Starting BOM intake create call for customer '%s' and source file '%s'.",
            header.get("CustomerName"),
            header.get("SourceFileName"),
        )

        try:
            connection = self._connect(**connection_kwargs)
        except Exception as exc:
            logger.exception(
                "SQL connection failed for host '%s', database '%s'.",
                self._sql_config.host,
                self._sql_config.database,
            )
            raise BomIntakeDbConnectionError(
                "Unable to connect to SQL Server for BOM intake."
            ) from exc

        cursor = connection.cursor()
        try:
            bom_intake_id = self._create_intake(cursor, header)
            result = self._process_standardized_payload(
                cursor=cursor,
                bom_intake_id=bom_intake_id,
                detected_by=detected_by,
                root_candidates=root_candidates,
                bom_rows=bom_rows,
            )
            connection.commit()
            return result
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    def _create_intake(self, cursor: Any, header: dict[str, object]) -> int:
        sql = """
EXEC dbo.usp_BOM_Intake_Create
    @CustomerName = %s,
    @QuoteNumber = %s,
    @SourceFileName = %s,
    @SourceFilePath = %s,
    @SourceSheetName = %s,
    @SourceType = %s,
    @UploadedBy = %s,
    @ParserVersion = %s,
    @IntakeNotes = %s;
"""
        params = tuple(header.get(column) for column in CREATE_HEADER_COLUMNS)

        try:
            cursor.execute(sql, params)
            result_sets = _fetch_result_sets(cursor)
        except Exception as exc:
            logger.exception("BOM intake create call failed.")
            raise BomIntakeDbProcedureError(
                "dbo.usp_BOM_Intake_Create failed."
            ) from exc

        bom_intake_id = _extract_bom_intake_id(result_sets)
        if bom_intake_id is None:
            raise BomIntakeDbProcedureError(
                "dbo.usp_BOM_Intake_Create did not return a BomIntakeId."
            )

        logger.info("BOM intake create completed with BomIntakeId=%s.", bom_intake_id)
        return bom_intake_id

    def _process_standardized_payload(
        self,
        *,
        cursor: Any,
        bom_intake_id: int,
        detected_by: str,
        root_candidates: list[dict[str, object]],
        bom_rows: list[dict[str, object]],
    ) -> dict[str, object]:
        sql, params = self._build_process_standardized_command(
            bom_intake_id=bom_intake_id,
            detected_by=detected_by,
            root_candidates=root_candidates,
            bom_rows=bom_rows,
        )

        logger.info(
            "Starting BOM intake process call for BomIntakeId=%s with %s roots and %s rows.",
            bom_intake_id,
            len(root_candidates),
            len(bom_rows),
        )

        try:
            cursor.execute(sql, params)
            result_sets = _fetch_result_sets(cursor)
        except Exception as exc:
            logger.exception(
                "BOM intake process call failed for BomIntakeId=%s.",
                bom_intake_id,
            )
            raise BomIntakeDbProcedureError(
                "dbo.usp_BOM_Intake_ProcessStandardized failed."
            ) from exc

        summary, root_results = _extract_process_results(result_sets)
        if summary is None:
            raise BomIntakeDbProcedureError(
                "dbo.usp_BOM_Intake_ProcessStandardized did not return a summary result set."
            )

        summary.setdefault("BomIntakeId", bom_intake_id)
        summary.setdefault("DetectedRootCount", len(root_candidates))
        summary.setdefault(
            "AcceptedRootCount",
            sum(1 for root in root_results if root.get("DecisionStatus") == "accepted"),
        )
        summary.setdefault(
            "DuplicateRejectedCount",
            sum(
                1
                for root in root_results
                if root.get("DecisionStatus") == "duplicate_rejected"
            ),
        )
        summary.setdefault(
            "FinalIntakeStatus",
            "processed" if summary["AcceptedRootCount"] else "duplicates_rejected",
        )

        logger.info(
            "BOM intake process completed for BomIntakeId=%s: accepted=%s duplicates=%s status=%s.",
            summary.get("BomIntakeId"),
            summary.get("AcceptedRootCount"),
            summary.get("DuplicateRejectedCount"),
            summary.get("FinalIntakeStatus"),
        )

        return {
            "Summary": summary,
            "RootResults": root_results,
        }

    def _build_process_standardized_command(
        self,
        *,
        bom_intake_id: int,
        detected_by: str,
        root_candidates: Sequence[dict[str, object]],
        bom_rows: Sequence[dict[str, object]],
    ) -> tuple[str, tuple[object, ...]]:
        root_insert_sql, root_params = _build_table_insert(
            variable_name="@Roots",
            columns=ROOT_COLUMNS,
            rows=root_candidates,
        )
        row_insert_sql, row_params = _build_table_insert(
            variable_name="@Rows",
            columns=ROW_COLUMNS,
            rows=bom_rows,
        )

        sql = f"""
SET NOCOUNT ON;
-- pymssql cannot bind SQL Server TVPs directly, so populate table-typed
-- variables inside the batch and pass those into the existing procedure.
DECLARE @Roots dbo.udtt_BOM_Intake_Root;
DECLARE @Rows dbo.udtt_BOM_Intake_Row;
{root_insert_sql}
{row_insert_sql}
EXEC dbo.usp_BOM_Intake_ProcessStandardized
    @BomIntakeId = %s,
    @DetectedBy = %s,
    @Roots = @Roots,
    @Rows = @Rows;
"""

        return sql, tuple(root_params + row_params + [bom_intake_id, detected_by])


def _build_table_insert(
    *,
    variable_name: str,
    columns: Sequence[str],
    rows: Sequence[dict[str, object]],
) -> tuple[str, list[object]]:
    params: list[object] = []
    value_groups: list[str] = []

    for row in rows:
        value_groups.append("(" + ", ".join("%s" for _ in columns) + ")")
        params.extend(row.get(column) for column in columns)

    if not value_groups:
        return "", params

    insert_sql = (
        f"INSERT INTO {variable_name} ({', '.join(columns)}) VALUES\n    "
        + ",\n    ".join(value_groups)
        + ";"
    )
    return insert_sql, params


def _fetch_result_sets(cursor: Any) -> list[list[dict[str, object]]]:
    result_sets: list[list[dict[str, object]]] = []

    while True:
        if cursor.description:
            columns = [column[0] for column in cursor.description]
            result_sets.append(
                [dict(zip(columns, row)) for row in cursor.fetchall()]
            )

        if not cursor.nextset():
            break

    return result_sets


def _extract_bom_intake_id(
    result_sets: Sequence[Sequence[dict[str, object]]],
) -> int | None:
    for result_set in result_sets:
        for row in result_set:
            if "BomIntakeId" in row and row["BomIntakeId"] is not None:
                return int(row["BomIntakeId"])
            if row:
                first_value = next(iter(row.values()))
                if first_value is not None:
                    try:
                        return int(first_value)
                    except (TypeError, ValueError):
                        continue
    return None


def _extract_process_results(
    result_sets: Sequence[Sequence[dict[str, object]]],
) -> tuple[dict[str, object] | None, list[dict[str, object]]]:
    summary: dict[str, object] | None = None
    root_results: list[dict[str, object]] = []

    for result_set in result_sets:
        if not result_set:
            continue

        first_row = result_set[0]
        if "RootClientId" in first_row:
            root_results = list(result_set)
            continue

        if (
            "BomIntakeId" in first_row
            or "FinalIntakeStatus" in first_row
            or "AcceptedRootCount" in first_row
        ):
            summary = dict(first_row)

    return summary, root_results


def _load_pymssql_connect() -> Callable[..., Any]:
    try:
        import pymssql  # type: ignore
    except ImportError as exc:
        raise SqlServerConfigError(
            "pymssql is required for SQL Server BOM intake connectivity."
        ) from exc

    return pymssql.connect
