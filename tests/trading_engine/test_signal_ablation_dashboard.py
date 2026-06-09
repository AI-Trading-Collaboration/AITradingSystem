from __future__ import annotations

from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from ai_trading_system.trading_engine.signal_ablation import run_signal_ablation
from ai_trading_system.trading_engine.signal_snapshots import run_signal_snapshot_build
from trading_engine.test_shadow_parameter_backtest import (
    _write_dashboard_metadata,
    _write_shadow_backtest_fixture,
)
from trading_engine.test_signal_ablation import _write_signal_ablation_config
from trading_engine.test_signal_snapshot_dashboard import _write_decision_snapshot


def test_dashboard_reads_latest_signal_ablation_summary(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    config_path = _write_signal_ablation_config(tmp_path, fixture["config_path"])
    run_signal_ablation(as_of=fixture["as_of"], config_path=config_path)
    metadata_path = _write_dashboard_metadata(tmp_path, fixture["as_of"])

    report = build_daily_task_dashboard_report(
        as_of=fixture["as_of"],
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["signal_ablation_summary"]
    assert card["exists"] is True
    assert card["status"] == "LIMITED"
    assert card["backtest_mode"] == "full_signal_backtest_limited"
    assert card["fallback_signals_count"] >= 3
    assert card["can_support_candidate_promotion"] is False
    assert card["real_signals_used_in_score"] is True
    assert "No promotion-credit signals" in card["no_promotion_credit_reason"]
    assert card["production_effect"] == "none"
    assert "Signal Ablation Summary" in html
    assert "promotion credit signals" in html
    assert "No-promotion-credit reason" in html


def test_reader_brief_displays_signal_ablation_summary(
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
    config_path = _write_signal_ablation_config(tmp_path, fixture["config_path"])
    run_signal_ablation(as_of=fixture["as_of"], config_path=config_path)
    snapshot_path = _write_decision_snapshot(tmp_path, fixture["as_of"])

    payload = build_reader_brief_payload(
        as_of=fixture["as_of"],
        reports_dir=tmp_path,
        decision_snapshot_path=snapshot_path,
    )

    review = payload["parameter_shadow_review"]
    assert review["signal_ablation_status"] == "LIMITED"
    assert "Signal ablation" in review["signal_ablation_summary"]
    assert "promotion" in review["signal_ablation_summary"]
    assert "No promotion-credit signals" in review["signal_ablation_no_promotion_credit_reason"]
    assert isinstance(review["signal_ablation_negative_signals"], list)
