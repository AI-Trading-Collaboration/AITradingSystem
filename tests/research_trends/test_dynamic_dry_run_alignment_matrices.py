from __future__ import annotations

from dynamic_dry_run_readiness_fixtures import dry_run_wrapper_row

from ai_trading_system.dynamic_target_baseline_dry_run_readiness import (
    build_dynamic_dry_run_market_data_alignment_matrix,
    build_dynamic_dry_run_risk_cap_alignment_matrix,
    build_dynamic_dry_run_timestamp_alignment_matrix,
)


def test_timestamp_alignment_matrix_is_ready_with_pit_warnings() -> None:
    matrix = build_dynamic_dry_run_timestamp_alignment_matrix([dry_run_wrapper_row()])

    assert {row["check_id"] for row in matrix} >= {
        "as_of_to_decision_latency",
        "known_at_policy",
        "timestamp_derivation_mode",
    }
    assert not any(row["blocking"] for row in matrix)
    assert any(row["alignment_status"] == "READY_WITH_WARNINGS" for row in matrix)


def test_risk_cap_alignment_matrix_carries_2332_recheck_warning() -> None:
    matrix = build_dynamic_dry_run_risk_cap_alignment_matrix(
        wrapper_rows=[dry_run_wrapper_row()],
        risk_cap_alignment={
            "alignment_readiness_status": "TIMESTAMP_ALIGNMENT_READY_WITH_WARNINGS",
            "risk_cap_trigger_series_available": True,
            "overlap_record_count": 1,
            "date_overlap_start": "2023-01-06",
            "date_overlap_end": "2023-01-06",
            "decision_timestamp_alignment_status": "READY_WITH_PIT_CAVEAT",
            "validity_window_alignment_status": "READY_WITH_WARNINGS",
            "rebalance_timing_alignment_status": "READY_WITH_WARNINGS",
            "asset_overlap_status": "PARTIAL_OR_REQUIRES_2331_RECHECK",
            "horizon_overlap_status": "REQUIRES_2331_RECHECK",
        },
    )

    assert not any(row["blocking"] for row in matrix)
    assert any(row["alignment_status"] == "READY_WITH_WARNINGS" for row in matrix)


def test_market_data_alignment_warns_on_missing_asset_and_date_trim() -> None:
    rows = [
        dry_run_wrapper_row(row_date="2022-12-01", target_asset="QQQ"),
        dry_run_wrapper_row(row_date="2023-01-06", target_asset="QQQ"),
    ]
    matrix = build_dynamic_dry_run_market_data_alignment_matrix(
        wrapper_rows=rows,
        source_binding={
            "market_data_binding": {
                "target_assets": ["QQQ", "SPY"],
                "coverage_start": "2023-01-06",
                "coverage_end": "2026-06-18",
                "data_quality_status": "PASS_WITH_WARNINGS",
            }
        },
        static_dry_run={"summary": {}, "data_quality_report": {"data_quality_status": "PASS"}},
    )

    by_check = {row["check_id"]: row for row in matrix}
    assert by_check["market_asset_coverage"]["blocking"] is False
    assert by_check["market_date_overlap"]["alignment_status"] == "READY_WITH_WARNINGS"

    missing = build_dynamic_dry_run_market_data_alignment_matrix(
        wrapper_rows=[dry_run_wrapper_row(target_asset="TQQQ")],
        source_binding={
            "market_data_binding": {
                "target_assets": ["QQQ"],
                "coverage_start": "2023-01-06",
                "coverage_end": "2026-06-18",
            }
        },
        static_dry_run={"summary": {}, "data_quality_report": {}},
    )
    missing_asset = next(row for row in missing if row["check_id"] == "market_asset_coverage")
    assert missing_asset["blocking"] is False
    assert missing_asset["alignment_status"] == "READY_WITH_WARNINGS"
