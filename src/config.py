from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


@dataclass(frozen=True)
class AppConfig:
    app_env: str
    automation_drop_root: Path
    work_root: Path
    port: int

    @classmethod
    def load(cls) -> "AppConfig":
        project_root = Path(__file__).resolve().parent.parent
        _load_dotenv(project_root / ".env")

        automation_drop_root = os.getenv("DOC_AUTOMATION_DROP_ROOT", "").strip()
        work_root = os.getenv("DOC_WORK_ROOT", "").strip()
        port = int(os.getenv("PORT", "8000"))

        if not automation_drop_root:
            raise ValueError("DOC_AUTOMATION_DROP_ROOT is required.")
        if not work_root:
            raise ValueError("DOC_WORK_ROOT is required.")

        return cls(
            app_env=os.getenv("APP_ENV", "development").strip() or "development",
            automation_drop_root=Path(automation_drop_root),
            work_root=Path(work_root),
            port=port,
        )


class SqlServerConfigError(ValueError):
    pass


@dataclass(frozen=True)
class SqlServerConfig:
    host: str
    username: str
    password: str
    port: int = 1433
    database: str = "HILLSBORO_Audit"
    driver: str = "ODBC Driver 18 for SQL Server"
    encrypt: str = "yes"
    trust_server_certificate: str = "yes"
    timeout: int = 30

    @classmethod
    def load(cls) -> "SqlServerConfig":
        project_root = Path(__file__).resolve().parent.parent
        _load_dotenv(project_root / ".env")

        host = _get_trimmed_env("SQL_SERVER_HOST")
        username = _get_trimmed_env("SQL_SERVER_USERNAME")
        password = _get_trimmed_env("SQL_SERVER_PASSWORD")

        missing_fields = [
            env_name
            for env_name, value in (
                ("SQL_SERVER_HOST", host),
                ("SQL_SERVER_USERNAME", username),
                ("SQL_SERVER_PASSWORD", password),
            )
            if not value
        ]
        if missing_fields:
            raise SqlServerConfigError(
                "Missing required SQL Server environment variables: "
                + ", ".join(missing_fields)
            )

        try:
            port = int(_get_trimmed_env("SQL_SERVER_PORT", "1433") or "1433")
        except ValueError as exc:
            raise SqlServerConfigError("SQL_SERVER_PORT must be an integer.") from exc

        try:
            timeout = int(_get_trimmed_env("SQL_SERVER_TIMEOUT", "30") or "30")
        except ValueError as exc:
            raise SqlServerConfigError("SQL_SERVER_TIMEOUT must be an integer.") from exc

        return cls(
            host=host,
            username=username,
            password=password,
            port=port,
            database=_get_trimmed_env("SQL_SERVER_DATABASE", "HILLSBORO_Audit")
            or "HILLSBORO_Audit",
            driver=_get_trimmed_env(
                "SQL_SERVER_DRIVER",
                "ODBC Driver 18 for SQL Server",
            )
            or "ODBC Driver 18 for SQL Server",
            encrypt=_get_trimmed_env("SQL_SERVER_ENCRYPT", "yes") or "yes",
            trust_server_certificate=(
                _get_trimmed_env("SQL_SERVER_TRUST_SERVER_CERTIFICATE", "yes")
                or "yes"
            ),
            timeout=timeout,
        )


def _get_trimmed_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        return ""
    return value.strip()
