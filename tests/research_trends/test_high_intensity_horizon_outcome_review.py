from __future__ import annotations

from pathlib import Path

from high_intensity_forward_outcome_review_fixtures import (
    build_high_intensity_forward_outcome_review_fixture,
)

from ai_trading_system.high_intensity_risk_cap_forward_outcome_review import (
    build_high_intensity_horizon_outcome_review_matrix,
    load_high_intensity_forward_outcome_review_inputs,
)
from ai_trading_system.post_2085_research_common import records


def test_horizon_outcome_review_rates_and_labels(tmp_path: Path) -> None:
    fixture = build_high_intensity_forward_outcome_review_fixture(tmp_path)
    loaded = load_high_intensity_forward_outcome_review_inputs(
        partial_readiness_dir=fixture["partial_readiness_dir"],
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
    )

    matrix = build_high_intensity_horizon_outcome_review_matrix(
        cluster_matrix=records(loaded["outcome_binder"]["cluster_matrix"].get("rows")),
    )
    by_horizon = {row["horizon"]: row for row in matrix}

    assert set(by_horizon) == {"1d", "5d", "10d", "20d"}
    assert by_horizon["1d"]["coverage_ratio"] == 1.0
    assert by_horizon["20d"]["coverage_ratio"] == 0.9
    assert by_horizon["1d"]["downside_capture_cluster_count"] == 21
    assert by_horizon["1d"]["false_warning_cluster_count"] == 23
    assert by_horizon["1d"]["missed_upside_cluster_count"] == 10
    assert by_horizon["1d"]["horizon_evidence_label"] == "HORIZON_SUPPORTS_WARNING_VALUE"
    assert by_horizon["20d"]["horizon_evidence_label"] != "HORIZON_INCONCLUSIVE"
