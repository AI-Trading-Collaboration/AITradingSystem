from __future__ import annotations

from ai_trading_system.dynamic_target_baseline_preparation import (
    build_dynamic_target_pit_replayability_audit,
    build_dynamic_target_source_gap_matrix,
)


def test_missing_source_generates_blocking_gap() -> None:
    inventory = [
        {
            "source_id": "missing",
            "source_available": False,
            "source_type": "dynamic_strategy_target_exposure",
            "field_coverage": {},
            "target_assets_supported": [],
            "horizons_supported": [],
            "record_count": 0,
        }
    ]
    pit_rows = build_dynamic_target_pit_replayability_audit(inventory)

    gaps = build_dynamic_target_source_gap_matrix(
        inventory_rows=inventory,
        pit_rows=pit_rows,
        required_assets=["QQQ", "SPY", "SMH"],
    )

    assert gaps[0]["gap_category"] == "missing_source"
    assert gaps[0]["gap_severity"] == "BLOCKING"
    assert gaps[0]["2329_blocking"] is True


def test_missing_timestamp_and_exposure_are_blocking() -> None:
    inventory = [
        {
            "source_id": "schema_gap",
            "source_available": True,
            "source_type": "dynamic_strategy_target_exposure",
            "field_coverage": {"date": True, "target_asset": True},
            "target_assets_supported": ["QQQ", "SPY", "SMH"],
            "horizons_supported": ["10d"],
            "history_start": "2023-01-06",
            "history_end": "2023-01-06",
            "record_count": 3,
            "source_hash": "abc",
        }
    ]
    pit_rows = build_dynamic_target_pit_replayability_audit(inventory)

    gaps = build_dynamic_target_source_gap_matrix(
        inventory_rows=inventory,
        pit_rows=pit_rows,
        required_assets=["QQQ", "SPY", "SMH"],
    )

    by_category = {row["gap_category"]: row for row in gaps}
    assert by_category["missing_exposure_fields"]["2329_blocking"] is True
    assert by_category["missing_timestamp_fields"]["2329_blocking"] is True
    assert by_category["missing_registry_reference"]["gap_severity"] == "WARNING"


def test_unknown_schema_is_blocking() -> None:
    inventory = [
        {
            "source_id": "unknown",
            "source_available": True,
            "source_type": "unknown_candidate_source",
            "field_coverage": {"date": True, "target_asset": True, "target_exposure": True},
            "target_assets_supported": ["QQQ", "SPY", "SMH"],
            "horizons_supported": ["10d"],
            "history_start": "2023-01-06",
            "history_end": "2023-01-06",
            "record_count": 3,
            "source_hash": "abc",
        }
    ]
    pit_rows = build_dynamic_target_pit_replayability_audit(inventory)

    gaps = build_dynamic_target_source_gap_matrix(
        inventory_rows=inventory,
        pit_rows=pit_rows,
        required_assets=["QQQ", "SPY", "SMH"],
    )

    assert any(row["gap_category"] == "schema_incompatible" for row in gaps)
