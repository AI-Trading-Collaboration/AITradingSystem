from __future__ import annotations

from pathlib import Path

from high_intensity_forward_outcome_review_fixtures import (
    build_high_intensity_forward_outcome_review_fixture,
)

from ai_trading_system.high_intensity_risk_cap_forward_outcome_review import (
    build_high_intensity_cluster_outcome_review_matrix,
    build_high_intensity_false_warning_review,
    build_high_intensity_horizon_outcome_review_matrix,
    build_high_intensity_missed_upside_review,
    build_high_intensity_partial_coverage_caveat_report,
    load_high_intensity_forward_outcome_review_inputs,
)
from ai_trading_system.post_2085_research_common import records


def test_false_warning_missed_upside_reviews_keep_partial_caveat(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_forward_outcome_review_fixture(tmp_path)
    loaded = load_high_intensity_forward_outcome_review_inputs(
        partial_readiness_dir=fixture["partial_readiness_dir"],
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
    )
    cluster_review = build_high_intensity_cluster_outcome_review_matrix(
        cluster_matrix=records(loaded["outcome_binder"]["cluster_matrix"].get("rows")),
        event_logger_clusters=records(loaded["event_logger"]["cluster_registry"].get("rows")),
    )
    horizon_review = build_high_intensity_horizon_outcome_review_matrix(
        cluster_matrix=records(loaded["outcome_binder"]["cluster_matrix"].get("rows")),
    )

    false_review = build_high_intensity_false_warning_review(
        cluster_review=cluster_review,
        horizon_review=horizon_review,
    )
    missed_review = build_high_intensity_missed_upside_review(
        cluster_review=cluster_review,
        horizon_review=horizon_review,
    )
    caveat = build_high_intensity_partial_coverage_caveat_report(
        partial_summary=loaded["partial_readiness"]["summary"],
        not_due_impact_report=loaded["partial_readiness"]["not_due_impact_report"],
    )

    assert false_review["false_warning_cluster_rate"] == 0.383333
    assert false_review["false_warning_label"] == "FALSE_WARNING_MODERATE"
    assert missed_review["missed_upside_cluster_rate"] == 0.166667
    assert missed_review["missed_upside_label"] == "MISSED_UPSIDE_ACCEPTABLE"
    assert caveat["partial_coverage_caveat_required"] is True
    assert caveat["caveat_label"] == "PARTIAL_COVERAGE_LOW_IMPACT"
