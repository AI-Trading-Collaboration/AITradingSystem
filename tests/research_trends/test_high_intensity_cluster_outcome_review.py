from __future__ import annotations

from pathlib import Path

from high_intensity_forward_outcome_review_fixtures import (
    build_high_intensity_forward_outcome_review_fixture,
)

from ai_trading_system.high_intensity_risk_cap_forward_outcome_review import (
    build_high_intensity_cluster_outcome_review_matrix,
    load_high_intensity_forward_outcome_review_inputs,
)
from ai_trading_system.post_2085_research_common import records


def test_cluster_outcome_review_matrix_uses_cluster_primary_sample(
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

    matrix = build_high_intensity_cluster_outcome_review_matrix(
        cluster_matrix=records(loaded["outcome_binder"]["cluster_matrix"].get("rows")),
        event_logger_clusters=records(loaded["event_logger"]["cluster_registry"].get("rows")),
    )

    assert len(matrix) == 60
    assert {row["event_cluster_id"] for row in matrix}
    assert any(row["cluster_evidence_label"] == "DOWNSIDE_CAPTURE_SUPPORTIVE" for row in matrix)
    assert any(row["cluster_evidence_label"] == "FALSE_WARNING_DOMINANT" for row in matrix)
    assert any(row["cluster_evidence_label"] == "PARTIAL_HORIZON_INCOMPLETE" for row in matrix)
    assert all("trigger_day_level_usage" not in row for row in matrix)
    assert all(row["promotion_allowed"] is False for row in matrix)
