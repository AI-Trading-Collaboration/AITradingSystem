from __future__ import annotations

from pathlib import Path

from dynamic_v3_pressure_validation_helpers import (
    run_defensive_rule_review_fixture,
    write_weekly_cycle_fixture,
)

from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    run_weekly_ops_decision_update,
    validate_weekly_ops_decision_update_artifact,
)


def test_weekly_ops_decision_update_keeps_weekly_tracking_non_production(
    tmp_path: Path,
) -> None:
    fixture = run_defensive_rule_review_fixture(tmp_path)
    weekly = write_weekly_cycle_fixture(tmp_path)
    update_dir = tmp_path / "weekly_ops_decision_update"

    result = run_weekly_ops_decision_update(
        weekly_cycle_id=weekly["weekly_cycle_id"],
        pressure_backfill_id=fixture["pressure_backfill"]["pressure_backfill_id"],
        defensive_review_id=fixture["defensive_rule_review"]["review_id"],
        weekly_cycle_dir=weekly["weekly_cycle_dir"],
        backfill_dir=fixture["pressure_backfill_dir"],
        defensive_review_dir=fixture["defensive_rule_review_dir"],
        pressure_tag_dir=fixture["pressure_tag_dir"],
        output_dir=update_dir,
    )

    matrix = result["updated_weekly_decision_matrix"]
    actions = result["weekly_ops_next_actions"]["next_actions"]
    assert matrix["weekly_recommendation"] == "continue_tracking"
    assert matrix["defensive_validation_relevant_outcomes_before"] == 0
    assert matrix["defensive_validation_relevant_outcomes_after"] == 2
    assert matrix["rule_approval_allowed"] is False
    assert matrix["policy_change_allowed"] is False
    assert matrix["broker_action_allowed"] is False
    assert {row["action"] for row in actions} >= {
        "continue_pressure_sample_collection",
        "do_not_approve_defensive_rule",
    }
    assert (
        validate_weekly_ops_decision_update_artifact(
            decision_update_id=result["decision_update_id"],
            output_dir=update_dir,
        )["status"]
        == "PASS"
    )
