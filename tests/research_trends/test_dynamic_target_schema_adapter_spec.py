from __future__ import annotations

from datetime import UTC, datetime

from dynamic_target_source_remediation_fixtures import alignment_row, pit_row, source_row

from ai_trading_system.dynamic_target_baseline_source_remediation import (
    build_dynamic_target_baseline_schema_contract,
    build_dynamic_target_gap_to_schema_matrix,
    build_dynamic_target_remediation_action_matrix,
    build_dynamic_target_schema_adapter_spec,
)


def test_schema_adapter_spec_maps_source_to_baseline_fields() -> None:
    source = source_row()
    contract = build_dynamic_target_baseline_schema_contract(datetime.now(UTC))
    gaps = build_dynamic_target_gap_to_schema_matrix(
        inventory_rows=[source],
        pit_rows=[pit_row()],
        source_gap_rows=[],
        schema_contract=contract,
    )
    actions = build_dynamic_target_remediation_action_matrix(
        inventory_rows=[source],
        pit_rows=[pit_row()],
        gap_rows=gaps,
        risk_alignment_rows=[alignment_row()],
        market_alignment_rows=[alignment_row()],
    )

    rows = build_dynamic_target_schema_adapter_spec(
        inventory_rows=[source],
        action_rows=actions,
        gap_rows=gaps,
    )

    assert rows
    assert any(row["target_baseline_field"] == "target_exposure" for row in rows)
    assert any(row["fallback_rule"] for row in rows)
    assert any(row["pit_caveat"] for row in rows)
    assert not any(row["target_baseline_field"] == "target_weight" for row in rows)
