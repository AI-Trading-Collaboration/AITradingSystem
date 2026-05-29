from __future__ import annotations

import json
from pathlib import Path

from ai_trading_system.trading_engine.signal_calibration import (
    render_signal_calibration_markdown,
    run_signal_calibration,
    signal_calibration_payload_date,
    validate_signal_calibration_payload,
    write_signal_calibration_report_alias,
)
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture
from trading_engine.test_signal_calibration import _write_signal_calibration_config


def test_signal_calibration_markdown_and_report_alias(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=20, min_history_days=8)
    config_path = _write_signal_calibration_config(tmp_path, fixture["config_path"])

    run = run_signal_calibration(
        as_of=fixture["as_of"],
        profile_names=("baseline_v0_1",),
        config_path=config_path,
    )
    markdown = render_signal_calibration_markdown(run.payload)
    reports_dir = tmp_path / "outputs" / "reports"
    alias_json, alias_md = write_signal_calibration_report_alias(
        run.payload,
        reports_dir,
        run.as_of,
    )

    assert validate_signal_calibration_payload(run.payload) == []
    assert "# Signal Calibration Summary" in markdown
    assert "## 3. Profile Ranking" in markdown
    assert "## 4. Signal Distribution Diagnostics" in markdown
    assert "## 5. Signal Correlation Diagnostics" in markdown
    assert "## 10. Promotion Eligibility Impact" in markdown
    assert alias_json.exists()
    assert alias_md.exists()
    alias_payload = json.loads(alias_json.read_text(encoding="utf-8"))
    assert alias_payload["report_type"] == "signal_calibration_report"
    assert alias_payload["source_report_type"] == "signal_calibration"
    assert signal_calibration_payload_date(run.payload, run.json_path) == run.as_of
