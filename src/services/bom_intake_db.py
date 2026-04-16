from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Any

from src.config import SqlServerConfig, SqlServerConfigError
from src.contracts.bom_intake import (
    CREATE_PROC_SCALAR_FIELDS,
    PROCESS_PROC_SCALAR_FIELDS,
    ROOT_TVP_FIELDS,
    ROW_TVP_FIELDS,
    validate_sql_bound_row_dict,
)
from src.services.bom_intake_payload import BomIntakePayload

logger = logging.getLogger(__name__)


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
        payload: BomIntakePayload,
    ) -> dict[str, object]:
        if not payload.detected_by.strip():
            raise BomIntakeDbError("DetectedBy is required for BOM intake processing.")

        header = payload.create_input.to_dict()

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
            process_payload = payload.process_payload(bom_intake_id).to_dict()
            result = self._process_standardized_payload(
                cursor=cursor,
                process_payload=process_payload,
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
        _validate_sql_payload_shape(
            payload=header,
            expected_columns=CREATE_PROC_SCALAR_FIELDS,
            context="Create header payload",
        )
        sql = """
SET NOCOUNT ON;
DECLARE @BomIntakeId BIGINT;
EXEC dbo.usp_BOM_Intake_Create
    @CustomerName = %s,
    @QuoteNumber = %s,
    @SourceFileName = %s,
    @SourceFilePath = %s,
    @SourceSheetName = %s,
    @SourceType = %s,
    @UploadedBy = %s,
    @ParserVersion = %s,
    @IntakeNotes = %s,
    @BomIntakeId = @BomIntakeId OUTPUT;
SELECT @BomIntakeId AS BomIntakeId;
"""
        params = tuple(header.get(column) for column in CREATE_PROC_SCALAR_FIELDS)

        try:
            cursor.execute(sql, params)
            result_sets = _fetch_result_sets(cursor)
        except Exception as exc:
            logger.exception("BOM intake create call failed.")
            raise BomIntakeDbProcedureError(
                "dbo.usp_BOM_Intake_Create failed."
            ) from exc

        bom_intake_id = _extract_bom_intake_id(
            result_sets,
            prefer_explicit_bom_intake_id_result=True,
        )
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
        process_payload: dict[str, object],
    ) -> dict[str, object]:
        process_params = process_payload["params"]
        root_candidates = process_payload["roots"]
        bom_rows = process_payload["rows"]
        sql, params = self._build_process_standardized_command(
            process_params=process_params,
            root_candidates=root_candidates,
            bom_rows=bom_rows,
        )

        logger.info(
            "Starting BOM intake process call for BomIntakeId=%s with %s roots and %s rows.",
            process_params["BomIntakeId"],
            len(root_candidates),
            len(bom_rows),
        )

        try:
            cursor.execute(sql, params)
            result_sets = _fetch_result_sets(cursor)
        except Exception as exc:
            logger.exception(
                "BOM intake process call failed for BomIntakeId=%s.",
                process_params["BomIntakeId"],
            )
            raise BomIntakeDbProcedureError(
                "dbo.usp_BOM_Intake_ProcessStandardized failed."
            ) from exc

        summary, root_results = _extract_process_results(result_sets)
        if summary is None:
            raise BomIntakeDbProcedureError(
                "dbo.usp_BOM_Intake_ProcessStandardized did not return a summary result set."
            )

        bom_intake_id = process_params["BomIntakeId"]
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
        process_params: dict[str, object],
        root_candidates: Sequence[dict[str, object]],
        bom_rows: Sequence[dict[str, object]],
    ) -> tuple[str, tuple[object, ...]]:
        root_insert_sql, root_params = _build_table_insert(
            variable_name="@Roots",
            columns=ROOT_TVP_FIELDS,
            rows=root_candidates,
        )
        row_insert_sql, row_params = _build_table_insert(
            variable_name="@Rows",
            columns=ROW_TVP_FIELDS,
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

        _validate_sql_payload_shape(
            payload=process_params,
            expected_columns=PROCESS_PROC_SCALAR_FIELDS,
            context="Process procedure params",
        )
        return sql, tuple(
            root_params
            + row_params
            + [process_params["BomIntakeId"], process_params["DetectedBy"]]
        )


def _build_table_insert(
    *,
    variable_name: str,
    columns: Sequence[str],
    rows: Sequence[dict[str, object]],
) -> tuple[str, list[object]]:
    params: list[object] = []
    value_groups: list[str] = []

    for row in rows:
        _validate_sql_payload_shape(
            payload=row,
            expected_columns=columns,
            context=f"{variable_name} row",
        )
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
    *,
    prefer_explicit_bom_intake_id_result: bool = False,
) -> int | None:
    if prefer_explicit_bom_intake_id_result:
        for result_set in reversed(result_sets):
            if len(result_set) != 1:
                continue

            row = result_set[0]
            if set(row.keys()) != {"BomIntakeId"}:
                continue

            bom_intake_id = row.get("BomIntakeId")
            if bom_intake_id is not None:
                return int(bom_intake_id)

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


def _validate_sql_payload_shape(
    *,
    payload: dict[str, object],
    expected_columns: Sequence[str],
    context: str,
) -> None:
    try:
        validate_sql_bound_row_dict(
            payload,
            field_names=tuple(expected_columns),
            context=context,
        )
    except ValueError as exc:
        raise BomIntakeDbError(str(exc)) from exc


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
