from __future__ import annotations

from dynamic_target_timestamp_remediation_fixtures import wrapper_row

from ai_trading_system.dynamic_target_baseline_timestamp_remediation import (
    build_dynamic_target_timestamp_derivation_matrix,
    build_dynamic_target_timestamp_remediation_policy,
)


def test_native_timestamp_copy_is_preserved() -> None:
    rows = [wrapper_row(strict_native=True)]

    matrix = build_dynamic_target_timestamp_derivation_matrix(
        wrapper_rows=rows,
        policy=build_dynamic_target_timestamp_remediation_policy(),
    )

    assert matrix[0]["decision_timestamp_derivation_mode"] == "native_timestamp_copy"
    assert matrix[0]["remediation_status"] == "REMEDIATED_STRICT_NATIVE"
    assert matrix[0]["pit_caveat_required"] is False


def test_decision_timestamp_uses_next_trading_day_policy() -> None:
    rows = [wrapper_row(row_date="2023-01-06", missing_decision=True)]

    matrix = build_dynamic_target_timestamp_derivation_matrix(
        wrapper_rows=rows,
        policy=build_dynamic_target_timestamp_remediation_policy(),
    )

    assert matrix[0]["decision_timestamp_remediated"] == "2023-01-09T00:00:00Z"
    assert matrix[0]["decision_timestamp_derivation_mode"] == "deterministic_latency_policy"
    assert matrix[0]["pit_caveat_required"] is True


def test_unresolved_timestamp_is_marked_blocked() -> None:
    row = wrapper_row(missing_decision=True)
    row["date"] = ""
    row["as_of_timestamp"] = ""
    row["valid_from"] = ""

    matrix = build_dynamic_target_timestamp_derivation_matrix(
        wrapper_rows=[row],
        policy=build_dynamic_target_timestamp_remediation_policy(),
    )

    assert matrix[0]["remediation_status"] == "BLOCKED_TIMESTAMP_UNRESOLVED"
