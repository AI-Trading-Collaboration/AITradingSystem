from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    run_backtest_input_diagnostics,
)
from ai_trading_system.trading_engine.portfolio_sensitivity import (
    load_portfolio_sensitivity_payload,
    portfolio_sensitivity_payload_date,
    run_portfolio_sensitivity,
    validate_portfolio_sensitivity_payload,
    write_portfolio_sensitivity_report_alias,
)
from ai_trading_system.trading_engine.signal_snapshots import run_signal_snapshot_build
from trading_engine.test_portfolio_sensitivity import _write_portfolio_sensitivity_config
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture


def test_portfolio_sensitivity_report_alias_reads_source_summary(
    tmp_path: Path,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=80, min_history_days=20)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    run_backtest_input_diagnostics(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )
    config_path = _write_portfolio_sensitivity_config(tmp_path, fixture["config_path"])
    run = run_portfolio_sensitivity(
        as_of=fixture["as_of"],
        profile_names=("baseline_v0_1", "lower_rebalance_threshold"),
        config_path=config_path,
    )

    source_payload = load_portfolio_sensitivity_payload(run.json_path)
    report_date = portfolio_sensitivity_payload_date(source_payload, run.json_path)
    alias_json, alias_markdown = write_portfolio_sensitivity_report_alias(
        source_payload,
        tmp_path / "outputs" / "reports",
        report_date,
    )

    assert validate_portfolio_sensitivity_payload(source_payload) == []
    assert report_date == fixture["as_of"]
    assert alias_json.exists()
    assert alias_markdown.exists()
    alias_payload = json.loads(alias_json.read_text(encoding="utf-8"))
    assert alias_payload["report_type"] == "portfolio_sensitivity_report"
    assert alias_payload["source_report_type"] == "portfolio_sensitivity"
    markdown = alias_markdown.read_text(encoding="utf-8")
    assert "# Portfolio Sensitivity Summary" in markdown
    assert "## 6. Target-to-Actual-Weight Transmission" in markdown
    assert "## 11. Promotion Eligibility Impact" in markdown
    assert "auto_promotion" in markdown
