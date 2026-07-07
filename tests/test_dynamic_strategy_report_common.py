from __future__ import annotations

import json
from pathlib import Path

from ai_trading_system.dynamic_strategy_report_common import (
    load_text_document_or_missing_flag,
    write_json_artifact,
    write_markdown_artifact,
    write_section_json_artifact,
)


def test_write_json_artifact_creates_parent_and_preserves_format(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "nested" / "artifact.json"

    write_json_artifact(output_path, {"z": "最后", "a": 1})

    assert output_path.exists()
    assert json.loads(output_path.read_text(encoding="utf-8")) == {"a": 1, "z": "最后"}
    assert output_path.read_text(encoding="utf-8") == '{\n  "a": 1,\n  "z": "最后"\n}'


def test_write_markdown_artifact_creates_parent(tmp_path: Path) -> None:
    output_path = tmp_path / "nested" / "artifact.md"

    write_markdown_artifact(output_path, "# 标题\n")

    assert output_path.read_text(encoding="utf-8") == "# 标题\n"


def test_write_section_json_artifact_preserves_standard_envelope(tmp_path: Path) -> None:
    output_path = tmp_path / "nested" / "section.json"

    write_section_json_artifact(
        output_path,
        "section_report",
        "section_report.v1",
        {"status": "READY", "section": {"b": 2}},
        "section",
        task_id="TRADING-TEST",
    )

    assert json.loads(output_path.read_text(encoding="utf-8")) == {
        "broker_action": "none",
        "production_effect": "none",
        "report_type": "section_report",
        "schema_version": "section_report.v1",
        "section": {"b": 2},
        "status": "READY",
        "task_id": "TRADING-TEST",
    }


def test_load_text_document_or_missing_flag_preserves_missing_shape(
    tmp_path: Path,
) -> None:
    missing_path = tmp_path / "missing.md"

    assert load_text_document_or_missing_flag(missing_path) == {
        "_missing": True,
        "_path": str(missing_path),
        "text": "",
    }

    existing_path = tmp_path / "present.md"
    existing_path.write_text("hello", encoding="utf-8")

    assert load_text_document_or_missing_flag(existing_path) == {
        "_path": str(existing_path),
        "text": "hello",
    }
