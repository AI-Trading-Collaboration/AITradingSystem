from __future__ import annotations

from pathlib import Path

from high_intensity_forward_outcome_review_fixtures import (
    build_high_intensity_forward_outcome_review_fixture,
    sample_cluster_review_row,
)

from ai_trading_system.high_intensity_risk_cap_forward_outcome_review import (
    build_high_intensity_cluster_outcome_review_matrix,
    build_high_intensity_continue_refine_archive_decision_matrix,
    build_high_intensity_downside_capture_review,
    build_high_intensity_horizon_outcome_review_matrix,
    build_high_intensity_manual_review_usefulness_review,
    load_high_intensity_forward_outcome_review_inputs,
)
from ai_trading_system.post_2085_research_common import records


def test_downside_capture_review_materiality(tmp_path: Path) -> None:
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

    review = build_high_intensity_downside_capture_review(
        cluster_review=cluster_review,
        horizon_review=horizon_review,
    )

    assert review["downside_capture_cluster_rate"] == 0.35
    assert review["stress_detected_cluster_rate"] == 0.35
    assert review["worst_forward_max_drawdown_after_warning"] == -0.06
    assert review["downside_capture_label"] == "DOWNSIDE_CAPTURE_MODERATE"


def test_downside_absent_can_route_to_archive() -> None:
    cluster_review = [
        sample_cluster_review_row(
            event_cluster_id=f"hicl_{idx}",
            false_warning_candidate_any_horizon=True,
            missed_upside_candidate_any_horizon=True,
            cluster_evidence_label="MIXED_OUTCOME",
        )
        for idx in range(10)
    ]
    horizon_review = [
        {
            "horizon": horizon,
            "false_warning_cluster_count": 10,
            "missed_upside_cluster_count": 10,
            "downside_capture_cluster_count": 0,
        }
        for horizon in ("1d", "5d", "10d", "20d")
    ]
    downside = build_high_intensity_downside_capture_review(
        cluster_review=cluster_review,
        horizon_review=horizon_review,
    )
    manual = build_high_intensity_manual_review_usefulness_review(
        cluster_review=cluster_review,
        source_manual_review_report={},
        event_logger_event_count=10,
    )
    decision = build_high_intensity_continue_refine_archive_decision_matrix(
        source_data_quality={"validate_data_status": "PASS_WITH_WARNINGS"},
        partial_caveat={
            "coverage_caveat_materiality": "LOW",
            "caveat_label": "PARTIAL_COVERAGE_LOW_IMPACT",
        },
        false_warning_review={"false_warning_label": "FALSE_WARNING_HIGH"},
        missed_upside_review={"missed_upside_label": "MISSED_UPSIDE_HIGH"},
        downside_capture_review=downside,
        manual_review=manual,
        monthly_review={"monthly_concentration_effect_label": "CONCENTRATION_LOW_IMPACT"},
        selected_rule_assessment={"rule_outcome_label": "RULE_NO_DOWNSIDE_CAPTURE"},
    )

    assert downside["downside_capture_label"] == "DOWNSIDE_CAPTURE_ABSENT"
    assert decision["overall_recommendation"] == "ARCHIVE_HIGH_INTENSITY_RISK_CAP_LINE"
