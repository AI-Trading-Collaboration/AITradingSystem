from __future__ import annotations

from dynamic_target_timestamp_remediation_fixtures import wrapper_row

from ai_trading_system.dynamic_target_baseline_timestamp_remediation import (
    build_dynamic_target_known_at_semantics_report,
    build_dynamic_target_timestamp_derivation_matrix,
    build_dynamic_target_timestamp_pit_caveat_report,
    build_dynamic_target_timestamp_remediated_wrapper_artifact,
    build_dynamic_target_timestamp_remediation_policy,
    build_dynamic_target_timestamp_wrapper_validation_summary,
)


def test_timestamp_remediated_wrapper_preserves_identity_and_safety() -> None:
    rows = [wrapper_row(row_date="2023-01-06")]
    derivation = build_dynamic_target_timestamp_derivation_matrix(
        wrapper_rows=rows,
        policy=build_dynamic_target_timestamp_remediation_policy(),
    )

    remediated = build_dynamic_target_timestamp_remediated_wrapper_artifact(
        wrapper_rows=rows,
        derivation_rows=derivation,
    )

    assert remediated[0]["baseline_id"] == "baseline"
    assert remediated[0]["source_id"] == "source"
    assert remediated[0]["source_hash"] == "hash"
    assert remediated[0]["decision_timestamp"] == "2023-01-09T00:00:00Z"
    assert remediated[0]["target_exposure_role"] == (
        "research_baseline_field_only_not_trading_instruction"
    )
    assert remediated[0]["promotion_allowed"] is False
    assert remediated[0]["broker_action"] == "none"


def test_wrapper_validation_and_pit_caveat_are_generated() -> None:
    rows = [wrapper_row(row_date="2023-01-06")]
    derivation = build_dynamic_target_timestamp_derivation_matrix(
        wrapper_rows=rows,
        policy=build_dynamic_target_timestamp_remediation_policy(),
    )
    remediated = build_dynamic_target_timestamp_remediated_wrapper_artifact(
        wrapper_rows=rows,
        derivation_rows=derivation,
    )
    known_at = build_dynamic_target_known_at_semantics_report(
        wrapper_rows=rows,
        derivation_rows=derivation,
        selected_source=rows[0],
    )

    validation = build_dynamic_target_timestamp_wrapper_validation_summary(
        remediated_rows=remediated,
        derivation_rows=derivation,
        known_at_report=known_at,
    )
    caveat = build_dynamic_target_timestamp_pit_caveat_report(
        wrapper_rows=rows,
        derivation_rows=derivation,
        known_at_report=known_at,
        selected_source=rows[0],
    )

    assert validation["wrapper_generated"] is True
    assert validation["validation_status"] == "PASS_WITH_WARNINGS"
    assert caveat["pit_approximation_ready"] is True
    assert "broker_action" in caveat["blocked_usage"]
