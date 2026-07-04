from __future__ import annotations

from datetime import date
from pathlib import Path

from high_intensity_partial_outcome_readiness_fixtures import (
    build_high_intensity_partial_outcome_readiness_fixture,
)

from ai_trading_system.high_intensity_risk_cap_partial_outcome_readiness_review import (
    build_high_intensity_cluster_readiness_matrix,
    build_high_intensity_not_due_asset_horizon_distribution,
    build_high_intensity_not_due_cluster_impact_report,
    build_high_intensity_not_due_horizon_matrix,
    load_high_intensity_partial_outcome_readiness_inputs,
)


def test_not_due_horizon_impact_is_recent_and_noncritical(tmp_path: Path) -> None:
    fixture = build_high_intensity_partial_outcome_readiness_fixture(tmp_path)
    loaded = load_high_intensity_partial_outcome_readiness_inputs(**fixture)
    source_as_of = date(2026, 6, 29)
    cluster_readiness = build_high_intensity_cluster_readiness_matrix(
        cluster_matrix=loaded["outcome_binder"]["cluster_matrix"]["rows"],
        event_logger_clusters=loaded["event_logger"]["cluster_registry"]["rows"],
        source_as_of=source_as_of,
    )
    not_due = build_high_intensity_not_due_horizon_matrix(
        event_matrix=loaded["outcome_binder"]["event_matrix"]["rows"],
        cluster_readiness=cluster_readiness,
        source_as_of=source_as_of,
    )
    impact = build_high_intensity_not_due_cluster_impact_report(
        cluster_readiness=cluster_readiness,
        not_due_matrix=not_due,
    )
    distribution = build_high_intensity_not_due_asset_horizon_distribution(
        not_due_matrix=not_due,
        source_as_of=source_as_of,
    )

    assert len(not_due) == 9
    assert impact["clusters_with_not_due_outcomes"] == 6
    assert impact["critical_clusters_with_not_due"] == 0
    assert impact["not_due_cluster_impact_label"] == "NOT_DUE_IMPACT_MODERATE"
    assert distribution["not_due_by_horizon"] == {"10d": 3, "20d": 6}
    assert distribution["not_due_concentration_label"] == "NOT_DUE_RECENT_20D_CONCENTRATION"


def test_not_due_critical_cluster_marks_high_impact(tmp_path: Path) -> None:
    fixture = build_high_intensity_partial_outcome_readiness_fixture(tmp_path)
    loaded = load_high_intensity_partial_outcome_readiness_inputs(**fixture)
    logger_clusters = loaded["event_logger"]["cluster_registry"]["rows"]
    for row in logger_clusters:
        if row["event_cluster_id"] == "hicl_057":
            row["cluster_active_days"] = 5
    cluster_readiness = build_high_intensity_cluster_readiness_matrix(
        cluster_matrix=loaded["outcome_binder"]["cluster_matrix"]["rows"],
        event_logger_clusters=logger_clusters,
        source_as_of=date(2026, 6, 29),
    )
    not_due = build_high_intensity_not_due_horizon_matrix(
        event_matrix=loaded["outcome_binder"]["event_matrix"]["rows"],
        cluster_readiness=cluster_readiness,
        source_as_of=date(2026, 6, 29),
    )
    impact = build_high_intensity_not_due_cluster_impact_report(
        cluster_readiness=cluster_readiness,
        not_due_matrix=not_due,
    )

    assert any(row["is_critical_cluster"] for row in not_due)
    assert impact["critical_clusters_with_not_due"] == 1
    assert impact["not_due_cluster_impact_label"] == "NOT_DUE_IMPACT_HIGH"
