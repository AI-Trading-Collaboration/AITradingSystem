from __future__ import annotations

from datetime import date
from pathlib import Path

from high_intensity_partial_outcome_readiness_fixtures import (
    build_high_intensity_partial_outcome_readiness_fixture,
)

from ai_trading_system.high_intensity_risk_cap_partial_outcome_readiness_review import (
    build_high_intensity_cluster_readiness_matrix,
    build_high_intensity_horizon_readiness_matrix,
    load_high_intensity_partial_outcome_readiness_inputs,
)


def test_horizon_and_cluster_readiness_statuses(tmp_path: Path) -> None:
    fixture = build_high_intensity_partial_outcome_readiness_fixture(tmp_path)
    loaded = load_high_intensity_partial_outcome_readiness_inputs(**fixture)
    cluster_rows = loaded["outcome_binder"]["cluster_matrix"]["rows"]

    horizon = build_high_intensity_horizon_readiness_matrix(cluster_matrix=cluster_rows)
    by_horizon = {row["horizon"]: row for row in horizon}
    assert by_horizon["1d"]["horizon_readiness_status"] == "HORIZON_READY"
    assert by_horizon["5d"]["horizon_readiness_status"] == "HORIZON_READY"
    assert by_horizon["10d"]["horizon_readiness_status"] == ("HORIZON_READY_WITH_NOT_DUE_CAVEAT")
    assert by_horizon["20d"]["horizon_readiness_status"] == "HORIZON_PARTIAL"

    cluster = build_high_intensity_cluster_readiness_matrix(
        cluster_matrix=cluster_rows,
        event_logger_clusters=loaded["event_logger"]["cluster_registry"]["rows"],
        source_as_of=date(2026, 6, 29),
    )
    statuses = [row["cluster_readiness_status"] for row in cluster]
    assert statuses.count("CLUSTER_FULLY_READY") == 54
    assert statuses.count("CLUSTER_READY_WITH_20D_NOT_DUE") == 3
    assert statuses.count("CLUSTER_PARTIAL_NOT_DUE") == 3
    assert {row["cluster_importance_label"] for row in cluster if row["not_due_horizon_count"]} == {
        "RECENT_INCOMPLETE"
    }
