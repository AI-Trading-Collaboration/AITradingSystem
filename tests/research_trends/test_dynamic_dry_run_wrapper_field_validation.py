from __future__ import annotations

from dynamic_dry_run_readiness_fixtures import dry_run_wrapper_row

from ai_trading_system.dynamic_target_baseline_dry_run_readiness import (
    build_dynamic_dry_run_wrapper_field_validation_matrix,
)


def test_wrapper_field_validation_carries_timestamp_caveat() -> None:
    rows = [dry_run_wrapper_row()]

    matrix = build_dynamic_dry_run_wrapper_field_validation_matrix(rows)
    by_field = {row["field_name"]: row for row in matrix}

    assert by_field["baseline_id"]["validation_status"] == "PASS"
    assert by_field["decision_timestamp"]["validation_status"] == "PASS_WITH_WARNINGS"
    assert by_field["known_at_policy"]["validation_status"] == "PASS_WITH_WARNINGS"
    assert by_field["target_exposure"]["blocking"] is False
    assert by_field["broker_action"]["validation_status"] == "PASS"


def test_wrapper_field_validation_blocks_missing_target_exposure() -> None:
    rows = [dry_run_wrapper_row(missing_target_exposure=True)]

    matrix = build_dynamic_dry_run_wrapper_field_validation_matrix(rows)
    target_exposure = next(row for row in matrix if row["field_name"] == "target_exposure")

    assert target_exposure["validation_status"] == "FAIL"
    assert target_exposure["blocking"] is True
    assert target_exposure["missing_count"] == 1
