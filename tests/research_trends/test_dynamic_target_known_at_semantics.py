from __future__ import annotations

from dynamic_target_timestamp_remediation_fixtures import wrapper_row

from ai_trading_system.dynamic_target_baseline_timestamp_remediation import (
    build_dynamic_target_known_at_semantics_report,
    build_dynamic_target_timestamp_derivation_matrix,
    build_dynamic_target_timestamp_remediation_policy,
)


def test_native_known_at_semantics_ready() -> None:
    rows = [wrapper_row(strict_native=True)]
    derivation = build_dynamic_target_timestamp_derivation_matrix(
        wrapper_rows=rows,
        policy=build_dynamic_target_timestamp_remediation_policy(),
    )

    report = build_dynamic_target_known_at_semantics_report(
        wrapper_rows=rows,
        derivation_rows=derivation,
        selected_source=rows[0],
    )

    assert report["known_at_policy"] == "NATIVE_KNOWN_AT"
    assert report["strict_pit_ready"] is True


def test_next_session_known_at_policy_has_pit_caveat() -> None:
    rows = [wrapper_row()]
    derivation = build_dynamic_target_timestamp_derivation_matrix(
        wrapper_rows=rows,
        policy=build_dynamic_target_timestamp_remediation_policy(),
    )

    report = build_dynamic_target_known_at_semantics_report(
        wrapper_rows=rows,
        derivation_rows=derivation,
        selected_source=rows[0],
    )

    assert report["known_at_policy"] == "NEXT_SESSION_DECISION_POLICY"
    assert report["strict_pit_ready"] is False
    assert report["pit_approximation_ready"] is True
    assert report["known_at_caveats"]


def test_unknown_known_at_blocks_without_wrapper() -> None:
    report = build_dynamic_target_known_at_semantics_report(
        wrapper_rows=[],
        derivation_rows=[],
        selected_source={},
    )

    assert report["known_at_policy"] == "UNKNOWN_BLOCKED"
    assert report["research_only_ready"] is False
