from __future__ import annotations

from datetime import date

from ai_trading_system.high_intensity_risk_cap_forward_observe_event_logger import (
    NEXT_OUTCOME_BINDER_TASK,
    build_high_intensity_outcome_collection_schedule,
    build_high_intensity_pending_outcome_registry,
)


def test_pending_outcome_registry_creates_required_horizons_without_binding() -> None:
    event_log = [
        {
            "event_id": "evt_1",
            "event_cluster_id": "cluster_1",
            "event_date": "2023-01-03",
            "target_asset": "QQQ",
            "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
        }
    ]

    registry = build_high_intensity_pending_outcome_registry(event_log=event_log)
    schedule = build_high_intensity_outcome_collection_schedule(
        pending_registry=registry,
        as_of_date=date(2023, 2, 1),
    )

    assert {row["horizon"] for row in registry} == {"1d", "5d", "10d", "20d"}
    assert all(row["outcome_status"] == "OUTCOME_PENDING" for row in registry)
    assert all(row["outcome_binding_executed"] is False for row in registry)
    assert all(row["next_binding_task"] == NEXT_OUTCOME_BINDER_TASK for row in registry)
    assert "forward_return" in registry[0]["required_metrics"]
    assert {row["schedule_status"] for row in schedule} == {"HISTORICAL_DUE"}
