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
