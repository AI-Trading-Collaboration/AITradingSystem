from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    run_backtest_input_diagnostics,
)
from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.signal_snapshots import run_signal_snapshot_build
from trading_engine.test_shadow_parameter_backtest import (
    _write_dashboard_metadata,
    _write_shadow_backtest_fixture,
)


def test_dashboard_reads_signal_snapshot_summary(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    run_backtest_input_diagnostics(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        generated_at=datetime(
            fixture["as_of"].year,
            fixture["as_of"].month,
            fixture["as_of"].day,
            tzinfo=UTC,
        ),
    )
    metadata_path = _write_dashboard_metadata(tmp_path, fixture["as_of"])

    report = build_daily_task_dashboard_report(
        as_of=fixture["as_of"],
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["backtest_data_quality"]
    assert card["signal_snapshot_status"] == "LIMITED"
    assert card["backtest_mode"] == "full_signal_backtest_limited"
    assert card["real_signals_count"] == 2
    assert card["fallback_signals_count"] >= 3
    assert card["missing_signals_count"] == 0
    assert "Full signal limited" in html
    assert "Signal Snapshot" in html


def test_reader_brief_displays_full_signal_limited_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
    )
    snapshot_path = _write_decision_snapshot(tmp_path, fixture["as_of"])

    payload = build_reader_brief_payload(
        as_of=fixture["as_of"],
        reports_dir=tmp_path,
        decision_snapshot_path=snapshot_path,
    )

    review = payload["parameter_shadow_review"]
    assert review["backtest_mode"] == "full_signal_backtest_limited"
    assert review["promotion_eligibility"] == "Watch-only"
    assert review["signal_snapshot_status"] == "LIMITED"
    assert review["real_signals_count"] == 2
    assert review["missing_signals_count"] == 0
    assert "full-signal-limited shadow backtests" in review["data_quality_summary"]


def _write_decision_snapshot(tmp_path: Path, as_of: date) -> Path:
    path = tmp_path / f"decision_snapshot_{as_of.isoformat()}.json"
    path.write_text(
        json.dumps(
            {
                "snapshot_id": f"decision_snapshot:{as_of.isoformat()}",
                "signal_date": as_of.isoformat(),
                "market_regime": {"regime_id": "ai_after_chatgpt"},
                "scores": {"overall_score": 70, "confidence_score": 60, "components": []},
                "positions": {
                    "final_risk_asset_ai_band": {"min_position": 0.2, "max_position": 0.4},
                    "position_gates": [],
                },
                "quality": {"market_data_status": "PASS"},
                "manual_review": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path
