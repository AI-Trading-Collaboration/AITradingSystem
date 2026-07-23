from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class StrictJsonContractError(ValueError):
    """Raised when JSON bytes are syntactically or semantically ambiguous."""


def load_strict_json_text(value: str, *, label: str = "json") -> object:
    try:
        return json.loads(
            value,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_nonstandard_json_constant,
        )
    except (json.JSONDecodeError, StrictJsonContractError) as exc:
        raise StrictJsonContractError(f"{label}: {exc}") from exc


def load_strict_json_path(path: Path) -> object:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise StrictJsonContractError(f"{path}: {exc}") from exc
    return load_strict_json_text(text, label=str(path))


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise StrictJsonContractError(f"duplicate JSON key: {key}")
        payload[key] = value
    return payload


def _reject_nonstandard_json_constant(value: str) -> None:
    raise StrictJsonContractError(f"non-standard JSON constant: {value}")


__all__ = [
    "StrictJsonContractError",
    "load_strict_json_path",
    "load_strict_json_text",
]
