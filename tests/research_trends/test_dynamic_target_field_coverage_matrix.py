from __future__ import annotations

from ai_trading_system.dynamic_target_baseline_preparation import (
    REQUIRED_FIELDS,
    build_dynamic_target_field_coverage_matrix,
)


def test_field_coverage_lists_required_fields_and_blocking_policy() -> None:
    rows = build_dynamic_target_field_coverage_matrix(
        inventory_rows=[
            {
                "source_id": "candidate",
                "source_available": True,
                "field_coverage": {
                    "date": True,
                    "target_asset": True,
                    "target_exposure": False,
                    "as_of_timestamp": False,
                    "decision_timestamp": False,
                },
            }
        ],
        selected_source_id="candidate",
    )

    by_field = {row["field_name"]: row for row in rows}
    assert set(by_field) == set(REQUIRED_FIELDS)
    assert by_field["target_exposure"]["blocking_if_missing"] is True
    assert by_field["target_exposure"]["available"] is False
    assert by_field["as_of_timestamp"]["blocking_if_missing"] is True
    assert by_field["decision_timestamp"]["blocking_if_missing"] is True
    assert by_field["valid_until"]["fallback_allowed"] is True
