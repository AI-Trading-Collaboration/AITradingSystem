from __future__ import annotations

import json
from pathlib import Path

import pytest

from ai_trading_system import cache_catalog, data_refresh_audit, data_source_fallback_policy
from ai_trading_system.platform.artifacts import (
    write_json_atomic,
    write_json_atomic_without_trailing_newline,
    write_text_atomic,
)
from ai_trading_system.trading_engine import (
    data_freshness_summary,
    notification_delivery_audit_summary,
    parameter_governance_daily_digest,
    parameter_governance_summary,
    pipeline_health_summary,
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


def test_g1_3a_summary_json_preserves_insertion_order_newline_and_oserror(
    tmp_path: Path,
) -> None:
    payload = {"z": "中文", "a": {"value": 1}}
    expected = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    path = tmp_path / "summary" / "artifact.json"

    result = write_json_atomic(path, payload, sort_keys=False)

    assert path.read_bytes() == expected
    assert path.read_bytes().endswith(b"\n")
    assert result.path == path
    assert result.atomic is True
    blocking_parent = tmp_path / "not-a-directory"
    blocking_parent.write_text("block", encoding="utf-8")
    with pytest.raises(OSError):
        write_json_atomic(blocking_parent / "artifact.json", payload, sort_keys=False)


def test_g1_3a_summary_private_writer_helpers_are_removed() -> None:
    modules = (
        data_freshness_summary,
        pipeline_health_summary,
        parameter_governance_summary,
        parameter_governance_daily_digest,
        notification_delivery_audit_summary,
    )

    assert all(not hasattr(module, "_write_json") for module in modules)
    assert all(not hasattr(module, "_write_text") for module in modules)
