from __future__ import annotations

from dynamic_target_timestamp_remediation_fixtures import wrapper_row

from ai_trading_system.dynamic_target_baseline_timestamp_remediation import (
    build_dynamic_target_timestamp_gap_matrix,
)


def test_timestamp_gap_matrix_counts_missing_fields() -> None:
    rows = [
        wrapper_row(missing_decision=True, missing_validity=True),
        wrapper_row(target_asset="TQQQ", row_date="2023-01-09"),
    ]

    gap = build_dynamic_target_timestamp_gap_matrix(rows)[0]

    assert gap["missing_decision_timestamp_count"] == 1
    assert gap["missing_valid_from_count"] == 1
    assert gap["missing_valid_until_count"] == 1
    assert gap["timestamp_gap_severity"] == "HIGH"
    assert gap["strict_pit_blocked"] is True
    assert gap["pit_approximation_possible"] is True


def test_timestamp_gap_matrix_blocks_unresolved_decision_timestamp() -> None:
    row = wrapper_row(missing_decision=True)
    row["date"] = ""
    row["as_of_timestamp"] = ""

    gap = build_dynamic_target_timestamp_gap_matrix([row])[0]

    assert gap["timestamp_gap_severity"] == "BLOCKING"
    assert gap["remediation_possible"] is False
    assert "decision_timestamp" in gap["blocking_fields"]
