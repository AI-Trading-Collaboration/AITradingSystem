from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any


def write_json_artifact(
    path: Path,
    payload: Mapping[str, Any],
    *,
    sort_keys: bool = True,
    default: Callable[[Any], Any] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=sort_keys,
            default=default,
        ),
        encoding="utf-8",
    )


def write_markdown_artifact(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def json_block(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def load_json_document(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_json_document_or_empty(path: Path) -> Any:
    if not path.exists():
        return {}
    return load_json_document(path)


def load_json_document_or_missing_status(
    path: Path,
    *,
    path_key: str = "missing_path",
) -> dict[str, Any]:
    if not path.exists():
        return {"status": "MISSING", path_key: str(path)}
    return load_json_document(path)


def load_json_document_or_missing_path(path: Path) -> dict[str, Any]:
    return load_json_document_or_missing_status(path, path_key="path")


def load_json_document_or_missing_flag(path: Path) -> Any:
    if not path.exists():
        return {"_missing": True, "_path": str(path)}
    return load_json_document(path)


def load_json_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Required source artifact not found: {path}")
    document = load_json_document(path)
    if not isinstance(document, dict):
        raise ValueError(f"Source artifact must be a JSON object: {path}")
    return document
