from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_partial_outcome_readiness_review import (
    build_high_intensity_wait_vs_review_decision_matrix,
)


def test_wait_vs_review_proceeds_with_high_partial_coverage() -> None:
    decision = build_high_intensity_wait_vs_review_decision_matrix(
        sufficiency_report={
            "coverage_ratio": 0.9625,
            "not_due_outcome_count": 9,
            "blocked_outcome_count": 0,
        },
        not_due_distribution={"not_due_by_horizon": {"10d": 3, "20d": 6}},
        not_due_impact_report={"critical_clusters_with_not_due": 0},
    )

    assert decision["decision"] == "PROCEED_TO_FORWARD_OUTCOME_REVIEW_WITH_CAVEAT"
    assert decision["next_task_recommendation"].endswith(
        "Forward_Outcome_Review_With_Partial_Coverage_Caveat"
    )
    assert decision["decision_rationale"]


def test_wait_vs_review_waits_on_critical_cluster() -> None:
    decision = build_high_intensity_wait_vs_review_decision_matrix(
        sufficiency_report={
            "coverage_ratio": 0.9625,
            "not_due_outcome_count": 9,
            "blocked_outcome_count": 0,
        },
        not_due_distribution={"not_due_by_horizon": {"10d": 3, "20d": 6}},
        not_due_impact_report={"critical_clusters_with_not_due": 1},
    )

    assert decision["decision"] == "WAIT_FOR_NOT_DUE_HORIZONS"


def test_wait_vs_review_routes_data_gap_to_remediation() -> None:
    decision = build_high_intensity_wait_vs_review_decision_matrix(
        sufficiency_report={
            "coverage_ratio": 0.95,
            "not_due_outcome_count": 0,
            "blocked_outcome_count": 1,
        },
        not_due_distribution={"not_due_by_horizon": {}},
        not_due_impact_report={"critical_clusters_with_not_due": 0},
    )

    assert decision["decision"] == "DATA_REMEDIATION_REQUIRED"


def test_wait_vs_review_allows_partial_only_between_90_and_95() -> None:
    decision = build_high_intensity_wait_vs_review_decision_matrix(
        sufficiency_report={
            "coverage_ratio": 0.92,
            "not_due_outcome_count": 19,
            "blocked_outcome_count": 0,
        },
        not_due_distribution={"not_due_by_horizon": {"20d": 19}},
        not_due_impact_report={"critical_clusters_with_not_due": 0},
    )

    assert decision["decision"] == "PARTIAL_OUTCOME_REVIEW_ONLY"
