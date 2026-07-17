from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_promotion_threshold_sensitivity_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_followup as followup
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


@with_artifact_validation_session
def test_promotion_threshold_sensitivity_keeps_relaxed_scenarios_diagnostic(tmp_path) -> None:
    fixture = run_promotion_threshold_sensitivity_fixture(
        tmp_path,
        compact_test_matrix=True,
    )
    sensitivity = fixture["sensitivity"]
    relaxed = [
        row for row in sensitivity["threshold_scenarios"] if row["scenario"] != "base_threshold"
    ]

    assert sensitivity["manifest"]["status"] == "PASS"
    assert relaxed
    assert all(row["recommended"] is False for row in relaxed)
    assert (
        sensitivity["threshold_candidate_impact"]["policy_effect"]
        == "diagnostic_only_no_gate_change"
    )
    assert sensitivity["manifest"]["followup_policy_version"] == "weight_search_followup_v1"
    assert (
        sensitivity["manifest"]["v3_matrix_id"]
        == fixture["targeted_v3"]["v3_matrix_id"]
    )
    assert (
        sensitivity["manifest"]["source_scorecard_id"]
        == fixture["scorecard"]["scorecard_id"]
    )
    assert (
        sensitivity["manifest"]["source_near_miss_id"]
        == fixture["near_miss"]["near_miss_id"]
    )
    assert (
        sensitivity["manifest"]["promotion_threshold_sensitivity_input_snapshot_path"]
    )
    assert followup.SENSITIVITY_INPUT_SCHEMA.endswith(".v2")

    validation = weight_search.validate_promotion_threshold_sensitivity_artifact(
        sensitivity_id=sensitivity["sensitivity_id"],
        output_dir=tmp_path / "promotion_threshold_sensitivity",
    )
    assert validation["status"] == "PASS"
