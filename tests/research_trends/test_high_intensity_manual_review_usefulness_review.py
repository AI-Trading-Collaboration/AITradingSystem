from __future__ import annotations

from pathlib import Path

from high_intensity_forward_outcome_review_fixtures import (
    build_high_intensity_forward_outcome_review_fixture,
)

from ai_trading_system.high_intensity_risk_cap_forward_outcome_review import (
    build_high_intensity_cluster_outcome_review_matrix,
    build_high_intensity_manual_review_usefulness_review,
    load_high_intensity_forward_outcome_review_inputs,
)
from ai_trading_system.post_2085_research_common import records


def test_manual_review_usefulness_proxy_and_safety(tmp_path: Path) -> None:
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

    review = build_high_intensity_manual_review_usefulness_review(
        cluster_review=cluster_review,
        source_manual_review_report=loaded["outcome_binder"]["manual_review_report"],
        event_logger_event_count=len(records(loaded["event_logger"]["event_log"].get("rows"))),
    )

    assert review["manual_review_usefulness_proxy"] == 0.733333
    assert review["manual_review_usefulness_label"] == "MANUAL_REVIEW_CONTEXT_USEFUL_PROXY"
    assert review["manual_review_context_recommendation"] == "KEEP_MANUAL_REVIEW_CONTEXT_ONLY"
    assert "reduce_position_instruction" not in review
    assert review["broker_action"] == "none"
