from __future__ import annotations

from datetime import UTC, datetime

from dynamic_target_source_remediation_fixtures import pit_row, source_row

from ai_trading_system.dynamic_target_baseline_source_remediation import (
    build_dynamic_target_baseline_schema_contract,
    build_dynamic_target_gap_to_schema_matrix,
)


def test_gap_to_schema_matrix_marks_missing_target_exposure_blocking() -> None:
    source = source_row(target_exposure=False)
    contract = build_dynamic_target_baseline_schema_contract(datetime.now(UTC))

    rows = build_dynamic_target_gap_to_schema_matrix(
        inventory_rows=[source],
        pit_rows=[pit_row()],
        source_gap_rows=[],
        schema_contract=contract,
    )

    target_gap = next(row for row in rows if row["required_field"] == "target_exposure")
    assert target_gap["gap_severity"] == "BLOCKING"
    assert target_gap["blocking_for_wrapper"] is True
    timestamp_gap = next(row for row in rows if row["required_field"] == "as_of_timestamp")
    assert timestamp_gap["gap_severity"] in {"WARNING", "BLOCKING"}


def test_gap_to_schema_matrix_validity_and_source_hash_rules() -> None:
    source = source_row(validity=False, source_hash="")
    contract = build_dynamic_target_baseline_schema_contract(datetime.now(UTC))

    rows = build_dynamic_target_gap_to_schema_matrix(
        inventory_rows=[source],
        pit_rows=[pit_row()],
        source_gap_rows=[],
        schema_contract=contract,
    )

    assert next(row for row in rows if row["required_field"] == "valid_until")[
        "fallback_allowed"
    ] is True
    assert next(row for row in rows if row["required_field"] == "source_artifact_hash")[
        "gap_severity"
    ] == "BLOCKING"
