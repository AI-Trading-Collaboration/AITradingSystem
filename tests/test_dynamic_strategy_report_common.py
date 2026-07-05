from __future__ import annotations

import json
from pathlib import Path

from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
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
