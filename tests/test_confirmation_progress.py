from __future__ import annotations

from pathlib import Path

from dynamic_v3_confirmation_cycle_helpers import progress_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    confirmation_progress_report_payload,
    validate_confirmation_progress_artifact,
)


def test_confirmation_progress_keeps_insufficient_samples_not_ready(tmp_path: Path) -> None:
    fixture = progress_fixture(tmp_path)
    progress = fixture["progress"]
    rows = {
        row["target_id"]: row
        for row in progress["target_progress"]
    }

    limited = rows["limited_adjustment_vs_no_trade"]
    assert limited["available_forward_events"] == 2
    assert limited["available_by_window"] == {"1": 2, "5": 0, "10": 0, "20": 0}
    assert limited["progress_status"] == "IN_PROGRESS"
    assert "not_enough_forward_events" in limited["blocking_reasons"]
    assert "missing_5_10_20d_windows" in limited["blocking_reasons"]

    defensive = rows["defensive_limited_adjustment_drawdown"]
    assert defensive["progress_status"] == "INSUFFICIENT_EVENTS"
    assert "missing_pressure_regime_forward_events" in defensive["blocking_reasons"]

    summary = progress["target_progress_summary"]
    assert summary["ready_for_evaluation_count"] == 0
    assert summary["summary_recommendation"] == "continue_forward_tracking"

    payload = confirmation_progress_report_payload(
        progress_id=progress["progress_id"],
        output_dir=fixture["progress_dir"],
    )
    assert payload["progress_id"] == progress["progress_id"]

    validation = validate_confirmation_progress_artifact(
        progress_id=progress["progress_id"],
        output_dir=fixture["progress_dir"],
    )
    assert validation["status"] == "PASS"
