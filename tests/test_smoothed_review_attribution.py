from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_review_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


@with_artifact_validation_session
def test_smoothed_review_attribution_explains_continue_observation(tmp_path) -> None:
    fixture = run_smoothed_review_chain_fixture(tmp_path)

    result = system_target.run_smoothed_review_attribution(
        review_id=fixture["review"]["review_id"],
        comparison_id=fixture["comparison"]["comparison_id"],
        backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        review_dir=tmp_path / "smoothed_review",
        comparison_dir=tmp_path / "smoothed_comparison",
        backfill_dir=tmp_path / "smoothed_backfill",
        output_dir=tmp_path / "smoothed_review_attribution",
    )

    breakdown = result["smoothed_decision_reason_breakdown"]
    assert breakdown["decision"] in {"CONTINUE_OBSERVATION", "PROMOTE_TO_RECOMMENDED_RESEARCH"}
    assert breakdown["confidence"] in {"LOW", "MEDIUM"}
    assert breakdown["supporting_reasons"]
    assert breakdown["blocking_reasons"]
    assert "forward confirmation target events have not matured" in breakdown["why_not_promote"]
    assert breakdown["why_not_reject"]
    assert breakdown["broker_action_allowed"] is False

    matrix = result["smoothed_metric_support_matrix"]
    statuses = {
        key
        for row in matrix["methods"]
        for key in row["statuses"]
    }
    assert statuses >= {
        "rolling_consistency",
        "turnover",
        "weight_jump",
        "lag_cost",
    }
    assert breakdown["recommended_method"] is None

    validation = system_target.validate_smoothed_review_attribution_artifact(
        attribution_id=result["attribution_id"],
        output_dir=tmp_path / "smoothed_review_attribution",
    )
    assert validation["status"] == "PASS"
