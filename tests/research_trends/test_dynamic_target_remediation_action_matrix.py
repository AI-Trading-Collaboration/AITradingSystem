from __future__ import annotations

from datetime import UTC, datetime

from dynamic_target_source_remediation_fixtures import alignment_row, pit_row, source_row

from ai_trading_system.dynamic_target_baseline_source_remediation import (
    build_dynamic_target_baseline_schema_contract,
    build_dynamic_target_gap_to_schema_matrix,
    build_dynamic_target_remediation_action_matrix,
)


def _actions(source: dict, pit: dict | None = None) -> list[dict]:
    contract = build_dynamic_target_baseline_schema_contract(datetime.now(UTC))
    gaps = build_dynamic_target_gap_to_schema_matrix(
        inventory_rows=[source],
        pit_rows=[pit or pit_row(source["source_id"])],
        source_gap_rows=[],
        schema_contract=contract,
    )
    return build_dynamic_target_remediation_action_matrix(
        inventory_rows=[source],
        pit_rows=[pit or pit_row(source["source_id"])],
        gap_rows=gaps,
        risk_alignment_rows=[alignment_row(source["source_id"])],
        market_alignment_rows=[alignment_row(source["source_id"])],
    )


def test_remediable_source_generates_wrapper_action() -> None:
    row = _actions(source_row(timestamps=True, validity=True), pit_row(strict=True))[0]

    assert row["wrapper_allowed"] is True
    assert row["remediation_action"] in {
        "GENERATE_BASELINE_WRAPPER",
        "GENERATE_SCHEMA_ADAPTER_THEN_WRAPPER",
        "REQUIRE_REGISTRY_BINDING",
    }


def test_pit_caveat_source_generates_caveat_wrapper_action() -> None:
    row = _actions(source_row())[0]

    assert row["remediation_status"] == "REMEDIATION_READY_WITH_PIT_CAVEAT"
    assert row["pit_caveat_required"] is True


def test_unsafe_or_unknown_source_is_blocked() -> None:
    row = _actions(
        source_row(source_type="unknown_candidate_source", target_exposure=False)
    )[0]

    assert row["wrapper_allowed"] is False
    assert row["source_generation_required"] is True


def test_missing_registry_reference_requests_registry_binding() -> None:
    row = _actions(
        source_row(timestamps=True, validity=True, registry=False),
        pit_row(strict=True),
    )[0]

    assert row["remediation_action"] == "REQUIRE_REGISTRY_BINDING"
