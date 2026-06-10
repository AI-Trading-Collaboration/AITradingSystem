from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_confirmation_cycle_helpers import register_targets_fixture, write_progress_sources
from dynamic_v3_outcome_loop_helpers import build_ready_outcome_update_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    run_confirmation_cycle_weekly,
    validate_confirmation_cycle_weekly_artifact,
)


def test_confirmation_cycle_weekly_dry_run_skips_outcome_update(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    registry_fixture = register_targets_fixture(tmp_path / "confirmation")
    write_progress_sources(registry_fixture)
    outcome_root = tmp_path / "outcome"
    outcome_root.mkdir()
    outcome_fixture = build_ready_outcome_update_fixture(outcome_root, monkeypatch)

    result = run_confirmation_cycle_weekly(
        week_ending=date(2026, 6, 10),
        execute_ready_updates=False,
        registry_id=registry_fixture["registry"]["registry_id"],
        output_dir=tmp_path / "weekly",
        outcome_due_dir=tmp_path / "weekly_due",
        outcome_update_review_dir=tmp_path / "weekly_update_review",
        outcome_update_dir=tmp_path / "weekly_update",
        rolling_refresh_dir=tmp_path / "weekly_refresh",
        evidence_trend_dir=tmp_path / "weekly_trend",
        forward_decision_dir=tmp_path / "weekly_decision",
        registry_dir=registry_fixture["registry_dir"],
        progress_dir=tmp_path / "weekly_progress",
        evaluation_dir=tmp_path / "weekly_evaluation",
        rule_cycle_dir=tmp_path / "weekly_rule_cycle",
        queue_dir=tmp_path / "weekly_queue",
        dashboard_dir=tmp_path / "weekly_dashboard",
        pressure_tag_dir=tmp_path / "pressure_tag",
        advisory_outcome_dir=outcome_fixture["outcome"]["outcome_dir"].parent,
        limited_vs_notrade_dir=registry_fixture["limited_dir"],
        consensus_risk_dir=registry_fixture["consensus_dir"],
        prices_path=outcome_fixture["prices_path"],
        rates_path=outcome_fixture["rates_path"],
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )

    steps = {row["step"]: row for row in result["weekly_cycle_steps"]["steps"]}
    summary = result["weekly_cycle_summary"]
    assert result["manifest"]["dry_run"] is True
    assert steps["outcome_update"]["status"] == "SKIPPED"
    assert steps["outcome_update"]["reason"] == "execute_ready_updates_false"
    assert summary["updated_windows"] == 0
    assert summary["ready_for_evaluation"] == 0
    assert summary["rule_review_recommendation"] == "continue_tracking"
    assert (
        validate_confirmation_cycle_weekly_artifact(
            weekly_cycle_id=result["weekly_cycle_id"],
            output_dir=tmp_path / "weekly",
        )["status"]
        == "PASS"
    )
