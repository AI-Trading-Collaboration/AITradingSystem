from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from dynamic_v3_confirmation_cycle_helpers import cycle_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    build_confirmation_dashboard,
    build_rule_review_queue,
    validate_confirmation_dashboard_artifact,
)


def test_confirmation_dashboard_summarizes_target_and_pressure_progress(
    tmp_path: Path,
) -> None:
    fixture = cycle_fixture(tmp_path)
    queue = build_rule_review_queue(
        cycle_id=fixture["cycle"]["cycle_id"],
        output_dir=tmp_path / "rule_review_queue",
        cycle_dir=fixture["cycle_dir"],
        journal_path=fixture["journal_path"],
        generated_at=datetime(2026, 6, 10, 4, tzinfo=UTC),
    )

    dashboard = build_confirmation_dashboard(
        week_ending=date(2026, 6, 14),
        progress_id=fixture["progress"]["progress_id"],
        evaluation_id=fixture["evaluation"]["evaluation_id"],
        cycle_id=fixture["cycle"]["cycle_id"],
        queue_id=queue["queue_id"],
        output_dir=tmp_path / "confirmation_dashboard",
        progress_dir=fixture["progress_dir"],
        evaluation_dir=fixture["evaluation_dir"],
        rule_cycle_dir=fixture["cycle_dir"],
        queue_dir=tmp_path / "rule_review_queue",
        pressure_tag_dir=tmp_path / "pressure_regime_tag",
        generated_at=datetime(2026, 6, 10, 5, tzinfo=UTC),
    )

    targets = {row["target_id"]: row for row in dashboard["target_status_table"]["targets"]}
    pressure = dashboard["pressure_sample_dashboard"]
    summary = dashboard["confirmation_dashboard_summary"]
    assert summary["targets_total"] == 1
    assert summary["ready_for_evaluation"] == 0
    assert targets["limited_adjustment_vs_no_trade"]["progress_pct"] == 0.0
    assert pressure["defensive_validation_status"] == "INSUFFICIENT_PRESSURE_EVENTS"
    assert pressure["pressure_samples"]["tech_drawdown"] == 0
    assert summary["policy_change_allowed"] is False
    assert (
        validate_confirmation_dashboard_artifact(
            dashboard_id=dashboard["dashboard_id"],
            output_dir=tmp_path / "confirmation_dashboard",
        )["status"]
        == "PASS"
    )
