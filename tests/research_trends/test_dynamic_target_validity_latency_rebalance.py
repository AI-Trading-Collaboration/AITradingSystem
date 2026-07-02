from __future__ import annotations

from dynamic_target_timestamp_remediation_fixtures import wrapper_row

from ai_trading_system.dynamic_target_baseline_timestamp_remediation import (
    build_dynamic_target_known_at_semantics_report,
    build_dynamic_target_latency_policy_report,
    build_dynamic_target_rebalance_timing_report,
    build_dynamic_target_timestamp_derivation_matrix,
    build_dynamic_target_timestamp_remediation_policy,
    build_dynamic_target_validity_window_remediation_report,
)


def test_validity_latency_and_rebalance_reports_use_research_only_policies() -> None:
    rows = [
        wrapper_row(row_date="2023-01-06"),
        wrapper_row(row_date="2023-01-09"),
    ]
    derivation = build_dynamic_target_timestamp_derivation_matrix(
        wrapper_rows=rows,
        policy=build_dynamic_target_timestamp_remediation_policy(),
    )
    known_at = build_dynamic_target_known_at_semantics_report(
        wrapper_rows=rows,
        derivation_rows=derivation,
        selected_source=rows[0],
    )

    validity = build_dynamic_target_validity_window_remediation_report(
        wrapper_rows=rows,
        derivation_rows=derivation,
        selected_source=rows[0],
    )
    latency = build_dynamic_target_latency_policy_report(
        selected_source=rows[0],
        known_at_report=known_at,
    )
    rebalance = build_dynamic_target_rebalance_timing_report(
        wrapper_rows=rows,
        derivation_rows=derivation,
        selected_source=rows[0],
    )

    assert validity["validity_policy"] == "NEXT_DECISION_UNTIL_REPLACED"
    assert latency["latency_policy"] == "NEXT_TRADING_DAY_DECISION"
    assert rebalance["rebalance_policy"] == "DAILY_DECISION_REBALANCE"


def test_empty_inputs_block_validity_latency_and_rebalance() -> None:
    known_at = build_dynamic_target_known_at_semantics_report(
        wrapper_rows=[],
        derivation_rows=[],
        selected_source={},
    )

    validity = build_dynamic_target_validity_window_remediation_report(
        wrapper_rows=[],
        derivation_rows=[],
        selected_source={},
    )
    latency = build_dynamic_target_latency_policy_report(
        selected_source={},
        known_at_report=known_at,
    )
    rebalance = build_dynamic_target_rebalance_timing_report(
        wrapper_rows=[],
        derivation_rows=[],
        selected_source={},
    )

    assert validity["validity_policy"] == "BLOCKED_VALIDITY_UNKNOWN"
    assert latency["latency_policy"] == "BLOCKED_LATENCY_UNKNOWN"
    assert rebalance["rebalance_policy"] == "BLOCKED_REBALANCE_UNKNOWN"
