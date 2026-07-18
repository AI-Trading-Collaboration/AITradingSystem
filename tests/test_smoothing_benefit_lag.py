from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_review_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


@with_artifact_validation_session
def test_smoothing_benefit_lag_outputs_tradeoff_matrix(tmp_path) -> None:
    fixture = run_smoothed_review_chain_fixture(tmp_path)

    result = system_target.run_smoothing_benefit_lag_drilldown(
        smoothed_backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        comparison_id=fixture["comparison"]["comparison_id"],
        backfill_dir=tmp_path / "smoothed_backfill",
        comparison_dir=tmp_path / "smoothed_comparison",
        output_dir=tmp_path / "smoothing_benefit_lag",
    )

    benefit_rows = result["smoothing_benefit_summary"]["methods"]
    lag_rows = result["lag_cost_summary"]["methods"]
    tradeoff_rows = result["benefit_lag_tradeoff_matrix"]["methods"]
    assert {row["method"] for row in benefit_rows} == set(system_target.SMOOTHED_METHOD_TO_VARIANT)
    assert {row["method"] for row in lag_rows} == set(system_target.SMOOTHED_METHOD_TO_VARIANT)
    primary = next(
        row for row in tradeoff_rows if row["method"] == "smooth_weights_3d_limited_adjustment"
    )
    assert primary["benefit_status"] in {"STRONG", "MODERATE", "WEAK", "INSUFFICIENT_DATA"}
    assert primary["lag_cost_status"] in {"LOW", "MEDIUM", "HIGH", "INSUFFICIENT_DATA"}
    assert primary["tradeoff_status"] in {"FAVORABLE", "MIXED", "UNFAVORABLE", "INSUFFICIENT_DATA"}
    assert primary["recommendation"] in {
        "continue_observation",
        "promote_for_review",
        "reject",
        "needs_forward_confirmation",
    }

    validation = system_target.validate_smoothing_benefit_lag_artifact(
        drilldown_id=result["drilldown_id"],
        output_dir=tmp_path / "smoothing_benefit_lag",
    )
    assert validation["status"] == "PASS"
