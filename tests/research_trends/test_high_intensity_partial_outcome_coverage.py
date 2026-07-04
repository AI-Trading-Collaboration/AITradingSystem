from __future__ import annotations

from datetime import date
from pathlib import Path

from high_intensity_partial_outcome_readiness_fixtures import (
    build_high_intensity_partial_outcome_readiness_fixture,
)

from ai_trading_system.high_intensity_risk_cap_partial_outcome_readiness_review import (
    build_high_intensity_cluster_readiness_matrix,
    build_high_intensity_not_due_horizon_matrix,
    build_high_intensity_partial_outcome_coverage_matrix,
    load_high_intensity_partial_outcome_readiness_inputs,
)


def test_partial_outcome_coverage_matrix_matches_231_of_240(tmp_path: Path) -> None:
    fixture = build_high_intensity_partial_outcome_readiness_fixture(tmp_path)
    loaded = load_high_intensity_partial_outcome_readiness_inputs(**fixture)
    event_rows = loaded["outcome_binder"]["event_matrix"]["rows"]
    cluster_rows = loaded["outcome_binder"]["cluster_matrix"]["rows"]
    cluster_readiness = build_high_intensity_cluster_readiness_matrix(
        cluster_matrix=cluster_rows,
        event_logger_clusters=loaded["event_logger"]["cluster_registry"]["rows"],
        source_as_of=date(2026, 6, 29),
    )
    not_due = build_high_intensity_not_due_horizon_matrix(
        event_matrix=event_rows,
        cluster_readiness=cluster_readiness,
        source_as_of=date(2026, 6, 29),
    )

    coverage = build_high_intensity_partial_outcome_coverage_matrix(
        event_matrix=event_rows,
        cluster_matrix=cluster_rows,
        not_due_matrix=not_due,
    )

    cluster_total = [row for row in coverage if row["analysis_level"] == "cluster"]
    assert sum(row["expected_outcome_count"] for row in cluster_total) == 240
    assert sum(row["bound_outcome_count"] for row in cluster_total) == 231
    assert sum(row["not_due_outcome_count"] for row in cluster_total) == 9
    ten_day = next(row for row in cluster_total if row["horizon"] == "10d")
    twenty_day = next(row for row in cluster_total if row["horizon"] == "20d")
    assert ten_day["coverage_ratio"] == 0.95
    assert twenty_day["coverage_ratio"] == 0.9
    assert twenty_day["coverage_status"] == "PARTIAL_COVERAGE_ACCEPTABLE"


def test_partial_outcome_coverage_blocks_data_gaps(tmp_path: Path) -> None:
    fixture = build_high_intensity_partial_outcome_readiness_fixture(tmp_path)
    loaded = load_high_intensity_partial_outcome_readiness_inputs(**fixture)
    event_rows = loaded["outcome_binder"]["event_matrix"]["rows"]
    cluster_rows = loaded["outcome_binder"]["cluster_matrix"]["rows"]
    cluster_rows[0]["cluster_outcome_binding_status"] = "OUTCOME_BLOCKED_MARKET_DATA"
    coverage = build_high_intensity_partial_outcome_coverage_matrix(
        event_matrix=event_rows,
        cluster_matrix=cluster_rows,
    )

    one_day = next(
        row for row in coverage if row["analysis_level"] == "cluster" and row["horizon"] == "1d"
    )
    assert one_day["blocked_outcome_count"] == 1
    assert one_day["coverage_status"] == "COVERAGE_BLOCKED"
