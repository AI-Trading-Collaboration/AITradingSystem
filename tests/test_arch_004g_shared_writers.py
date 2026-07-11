from __future__ import annotations

import json
from pathlib import Path

from ai_trading_system import cache_catalog, data_refresh_audit, data_source_fallback_policy
from ai_trading_system.platform.artifacts import (
    write_json_atomic_without_trailing_newline,
    write_text_atomic,
)


def test_g1_2_canonical_json_writer_preserves_exact_legacy_bytes(tmp_path: Path) -> None:
    payload = {"z": "中文", "a": {"value": 1}, "items": [3, 2, 1]}
    expected = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode(
        "utf-8"
    )
    path = tmp_path / "writer" / "artifact.json"
    result = write_json_atomic_without_trailing_newline(path, payload)

    assert path.read_bytes() == expected
    assert not path.read_bytes().endswith(b"\n")
    assert result.path == path
    assert result.atomic is True
    assert not tuple(path.parent.glob(f".{path.name}.*.tmp"))


def test_g1_2_canonical_text_writer_preserves_exact_legacy_bytes(tmp_path: Path) -> None:
    content = "第一行\nsecond line\n"
    path = tmp_path / "writer" / "artifact.md"
    result = write_text_atomic(path, content)

    assert path.read_bytes() == content.encode("utf-8")
    assert result.path == path
    assert result.atomic is True
    assert not tuple(path.parent.glob(f".{path.name}.*.tmp"))


def test_g1_2_private_writer_wrappers_are_removed() -> None:
    modules = (cache_catalog, data_refresh_audit, data_source_fallback_policy)

    assert all(not hasattr(module, "_write_json") for module in modules)
    assert all(not hasattr(module, "_write_text") for module in modules)
