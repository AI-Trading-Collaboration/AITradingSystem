from __future__ import annotations

from pathlib import Path

from dynamic_target_baseline_preparation_fixtures import write_ready_dynamic_source

from ai_trading_system.dynamic_target_baseline_preparation import (
    build_dynamic_target_source_inventory,
)


def test_inventory_discovers_ready_dynamic_source(tmp_path: Path) -> None:
    root = tmp_path / "candidates"
    root.mkdir()
    write_ready_dynamic_source(root)

    rows = build_dynamic_target_source_inventory(
        candidate_roots=[root],
        target_assets=["QQQ", "SPY", "SMH"],
    )

    ready = next(row for row in rows if row["source_available"])
    assert ready["source_type"] == "dynamic_strategy_target_exposure"
    assert ready["source_hash"]
    assert ready["artifact_role"] == "dynamic_strategy_target_exposure"
    assert ready["target_assets_supported"] == ["QQQ", "SMH", "SPY"]
    assert set(ready["timestamp_fields_available"]) == {
        "as_of_timestamp",
        "decision_timestamp",
    }
    assert ready["promotion_allowed"] is False
    assert ready["broker_action"] == "none"


def test_inventory_emits_missing_dynamic_source_row(tmp_path: Path) -> None:
    root = tmp_path / "empty"
    root.mkdir()

    rows = build_dynamic_target_source_inventory(
        candidate_roots=[root],
        target_assets=["QQQ", "SPY", "SMH"],
    )

    assert rows == [
        {
            **rows[0],
            "source_id": "dynamic_strategy_target_exposure_missing",
            "source_available": False,
            "source_type": "dynamic_strategy_target_exposure",
        }
    ]
