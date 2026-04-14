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
