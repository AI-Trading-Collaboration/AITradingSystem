from __future__ import annotations

from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from ai_trading_system.trading_engine.signal_calibration import run_signal_calibration
from trading_engine.test_shadow_parameter_backtest import (
    _write_dashboard_metadata,
    _write_shadow_backtest_fixture,
)
from trading_engine.test_signal_calibration import _write_signal_calibration_config
from trading_engine.test_signal_snapshot_dashboard import _write_decision_snapshot


def test_dashboard_reads_latest_signal_calibration_summary(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=20, min_history_days=8)
    config_path = _write_signal_calibration_config(tmp_path, fixture["config_path"])
    run_signal_calibration(
        as_of=fixture["as_of"],
        profile_names=("baseline_v0_1", "trend_long_bias"),
        config_path=config_path,
    )
    metadata_path = _write_dashboard_metadata(tmp_path, fixture["as_of"])

    report = build_daily_task_dashboard_report(
        as_of=fixture["as_of"],
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["signal_calibration_summary"]
    assert card["exists"] is True
    assert card["status"] == "LIMITED"
    assert card["profiles_tested"] == 2
    assert card["best_profile"] in {"baseline_v0_1", "trend_long_bias"}
    assert card["current_profile"] == "baseline_v0_1"
    assert card["can_support_candidate_promotion"] is False
    assert card["production_effect"] == "none"
    assert "Signal Calibration Summary" in html
    assert "best profile" in html
    assert "Neutral compression warning" in html


def test_reader_brief_displays_signal_calibration_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=20, min_history_days=8)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    config_path = _write_signal_calibration_config(tmp_path, fixture["config_path"])
    run_signal_calibration(
        as_of=fixture["as_of"],
        profile_names=("baseline_v0_1",),
        config_path=config_path,
    )
    snapshot_path = _write_decision_snapshot(tmp_path, fixture["as_of"])

    payload = build_reader_brief_payload(
        as_of=fixture["as_of"],
        reports_dir=tmp_path,
        decision_snapshot_path=snapshot_path,
    )

    review = payload["parameter_shadow_review"]
    assert review["signal_calibration_status"] == "LIMITED"
    assert "Signal calibration" in review["signal_calibration_summary"]
    assert review["signal_calibration_best_profile"] == "baseline_v0_1"
    assert review["signal_calibration_profiles_tested"] == 1
    assert review["signal_calibration_promotion_credit_signal_count"] == 0
