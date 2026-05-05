from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.backtest.gate_attribution import (
    build_gate_event_attribution_report,
    render_gate_event_attribution_report,
)
from ai_trading_system.cli import app


def test_gate_event_attribution_estimates_gate_tradeoff(tmp_path: Path) -> None:
    daily_path = _write_backtest_daily(tmp_path)
    coverage_path = _write_input_coverage(tmp_path)

    report = build_gate_event_attribution_report(
        backtest_daily_path=daily_path,
        input_coverage_path=coverage_path,
        as_of=date(2026, 5, 6),
        left_tail_threshold=-0.03,
    )
    markdown = render_gate_event_attribution_report(report)
    risk_gate = next(row for row in report.gate_rows if row.gate_id == "risk_events")

    assert report.status == "PASS_WITH_LIMITATIONS"
    assert risk_gate.trigger_count == 2
    assert risk_gate.average_position_reduction == pytest.approx(0.25)
    assert risk_gate.avoided_drawdown == pytest.approx(0.01)
    assert risk_gate.missed_upside == pytest.approx(0.005)
    assert risk_gate.net_effect == pytest.approx(0.005)
    assert report.event_summary.risk_event_occurrence_count == 1
    assert report.event_summary.score_eligible_count == 1
    assert "Event / LLM Attribution Readiness" in markdown
    assert "production_effect：none" in markdown


def test_gate_event_attribution_handles_missing_event_coverage(tmp_path: Path) -> None:
    daily_path = _write_backtest_daily(tmp_path)

    report = build_gate_event_attribution_report(
        backtest_daily_path=daily_path,
        input_coverage_path=None,
        as_of=date(2026, 5, 6),
    )
    codes = {issue.code for issue in report.issues}

    assert report.passed
    assert report.event_summary.label_availability == "coverage_missing"
    assert "event_label_metrics_limited" in codes


def test_backtest_gate_attribution_cli_writes_report(tmp_path: Path) -> None:
    daily_path = _write_backtest_daily(tmp_path)
    coverage_path = _write_input_coverage(tmp_path)
    output_path = tmp_path / "gate_event_attribution.md"

    result = CliRunner().invoke(
        app,
        [
            "backtest-gate-attribution",
            "--backtest-daily-path",
            str(daily_path),
            "--input-coverage-path",
            str(coverage_path),
            "--output-path",
            str(output_path),
            "--as-of",
            "2026-05-06",
        ],
    )

    assert result.exit_code == 0
    assert "Gate/event 归因状态：PASS_WITH_LIMITATIONS" in result.output
    assert output_path.exists()
    assert "`risk_events`" in output_path.read_text(encoding="utf-8")


def _write_backtest_daily(tmp_path: Path) -> Path:
    path = tmp_path / "backtest_daily_2026-04-01_2026-04-04.csv"
    pd.DataFrame(
        [
            {
                "signal_date": "2026-04-01",
                "asset_return": -0.04,
                "model_target_exposure": 0.50,
                "risk_events_gate_cap": 0.25,
                "risk_events_gate_triggered": True,
                "valuation_gate_cap": 1.00,
                "valuation_gate_triggered": False,
            },
            {
                "signal_date": "2026-04-02",
                "asset_return": 0.02,
                "model_target_exposure": 0.50,
                "risk_events_gate_cap": 0.25,
                "risk_events_gate_triggered": True,
                "valuation_gate_cap": 0.40,
                "valuation_gate_triggered": True,
            },
            {
                "signal_date": "2026-04-03",
                "asset_return": -0.01,
                "model_target_exposure": 0.50,
                "risk_events_gate_cap": 1.00,
                "risk_events_gate_triggered": False,
                "valuation_gate_cap": 1.00,
                "valuation_gate_triggered": False,
            },
        ]
    ).to_csv(path, index=False)
    return path


def _write_input_coverage(tmp_path: Path) -> Path:
    path = tmp_path / "backtest_input_coverage_2026-04-01_2026-04-04.csv"
    pd.DataFrame(
        [
            {
                "record_type": "risk_event_evidence_url",
                "month": "2026-04",
                "event_id": "ai_chip_export_control_upgrade",
                "occurrence_id": "occ_001",
                "status": "confirmed_high",
                "score_eligible": True,
                "source_type": "primary_source",
                "source_url": "https://example.gov/event",
            },
            {
                "record_type": "risk_event_source_type",
                "month": "2026-04",
                "source_type": "primary_source",
                "count": 1,
            },
        ]
    ).to_csv(path, index=False)
    return path
