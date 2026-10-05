from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import yaml
from airflow.sdk import Variable


def get_project_config() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[1] / "config" / "media_config.yaml"
    with path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def get_channels() -> list[str]:
    path = Path(__file__).resolve().parents[1] / "config" / "channels.yaml"
    with path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)["channels"]


def get_environment() -> str:
    # Airflow Variable demonstrates runtime configuration.
    # Fallback keeps local parsing possible before the Variable is created.
    try:
        return Variable.get("media_environment")
    except Exception:
        return "dev"


def ensure_dirs(config: dict[str, Any]) -> None:
    for key in ("incoming_dir", "processed_dir", "report_dir"):
        os.makedirs(config[key], exist_ok=True)


def write_json(path: str, payload: Any) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def read_json(path: str) -> Any:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)
