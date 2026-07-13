from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import run_selection_review_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_data_warning_repair_plan_keeps_manual_repair_boundary(tmp_path) -> None:
    fixture = run_selection_review_fixture(tmp_path)
    selection = fixture["selection"]
    impact = system_target.run_data_warning_impact_review(
        backfill_id=fixture["backfill"]["backfill_id"],
        selection_review_id=selection["selection_review_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        selection_review_dir=tmp_path / "system_target_selection_review",
        output_dir=tmp_path / "data_warning_impact",
        generated_at=datetime(2026, 1, 7, 8, tzinfo=UTC),
    )

    repair = system_target.run_data_warning_repair_plan(
        impact_id=impact["impact_id"],
        data_warning_impact_dir=tmp_path / "data_warning_impact",
        output_dir=tmp_path / "data_warning_repair_plan",
        generated_at=datetime(2026, 1, 7, 9, tzinfo=UTC),
    )

    actions = repair["warning_repair_actions"]
    matrix = repair["warning_blocking_matrix"]

    assert actions
    assert matrix["schema_version"] == 1
    assert matrix["report_type"] == "etf_dynamic_v3_data_warning_blocking_matrix"
    assert matrix["status"] == "PASS"
    assert matrix["production_effect"] == "none"
    assert actions[0]["warning_id"] == "pass_with_warnings_detail_unavailable"
    assert actions[0]["recommended_repair_action"] == "manual_review"
    assert actions[0]["auto_repair_allowed"] is False
    assert matrix["overall_data_warning_status"] == "REVIEW_REQUIRED"
    assert matrix["hardening_allowed_after_repair"] == "UNKNOWN"
    assert matrix["warnings"][0]["blocks_hardening"] is True
    assert matrix["warnings"][0]["blocks_research"] is False
    assert repair["manifest"]["auto_repair_executed"] is False

    validation = system_target.validate_data_warning_repair_plan_artifact(
        repair_plan_id=repair["repair_plan_id"],
        output_dir=tmp_path / "data_warning_repair_plan",
    )
    assert validation["status"] == "PASS"
