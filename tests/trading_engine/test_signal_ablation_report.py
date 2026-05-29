from __future__ import annotations

import json
from pathlib import Path

from ai_trading_system.trading_engine.signal_ablation import (
    render_signal_ablation_markdown,
    run_signal_ablation,
    signal_ablation_payload_date,
    validate_signal_ablation_payload,
    write_signal_ablation_report_alias,
)
from ai_trading_system.trading_engine.signal_snapshots import run_signal_snapshot_build
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture
from trading_engine.test_signal_ablation import _write_signal_ablation_config


def test_signal_ablation_markdown_and_report_alias(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    config_path = _write_signal_ablation_config(tmp_path, fixture["config_path"])

    run = run_signal_ablation(as_of=fixture["as_of"], config_path=config_path)
    markdown = render_signal_ablation_markdown(run.payload)
    reports_dir = tmp_path / "outputs" / "reports"
    alias_json, alias_md = write_signal_ablation_report_alias(
        run.payload,
        reports_dir,
        run.as_of,
    )

    assert validate_signal_ablation_payload(run.payload) == []
    assert "# Signal Ablation Summary" in markdown
    assert "## Implementation Diagnostics" in markdown
    assert "## Signal Usage Diagnostics" in markdown
    assert "## Score Impact Diagnostics" in markdown
    assert "## Portfolio Impact Diagnostics" in markdown
    assert "## Threshold Diagnostics" in markdown
    assert "## Why No Promotion-credit Signals?" in markdown
    assert "## 3. Signal Contribution Ranking" in markdown
    assert "## 6. Proxy and Fallback Signal Warnings" in markdown
    assert alias_json.exists()
    assert alias_md.exists()
    alias_payload = json.loads(alias_json.read_text(encoding="utf-8"))
    assert alias_payload["report_type"] == "signal_ablation_report"
    assert alias_payload["source_report_type"] == "signal_ablation"
    assert signal_ablation_payload_date(run.payload, run.json_path) == run.as_of
