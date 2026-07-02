from __future__ import annotations

from ai_trading_system.dynamic_target_baseline_preparation import (
    build_dynamic_target_pit_replayability_audit,
)


def test_pit_audit_labels_strict_replayable_current_and_blocked() -> None:
    inventory = [
        {
            "source_id": "strict",
            "source_available": True,
            "source_type": "dynamic_strategy_target_exposure",
            "field_coverage": {
                "as_of_timestamp": True,
                "decision_timestamp": True,
                "valid_from": True,
                "valid_until": True,
            },
            "history_start": "2023-01-06",
            "history_end": "2023-01-06",
            "record_count": 1,
            "source_hash": "abc",
            "registry_reference_available": True,
        },
        {
            "source_id": "replayable",
            "source_available": True,
            "source_type": "dynamic_strategy_target_exposure",
            "field_coverage": {},
            "history_start": "2023-01-06",
            "history_end": "2023-01-06",
            "record_count": 1,
            "source_hash": "abc",
        },
        {
            "source_id": "current",
            "source_available": True,
            "source_type": "dynamic_strategy_target_exposure",
            "field_coverage": {"target_exposure": True},
            "history_start": "",
            "history_end": "",
            "record_count": 1,
            "source_hash": "abc",
        },
        {"source_id": "blocked", "source_available": False, "field_coverage": {}},
    ]

    rows = build_dynamic_target_pit_replayability_audit(inventory)
    by_source = {row["source_id"]: row for row in rows}

    assert by_source["strict"]["pit_status"] == "STRICT_PIT_READY"
    assert by_source["replayable"]["pit_status"] == "REPLAYABLE_BUT_NOT_STRICT_PIT"
    assert by_source["current"]["pit_status"] == "CURRENT_ARTIFACT_ONLY"
    assert by_source["blocked"]["pit_status"] == "BLOCKED"
    assert by_source["replayable"]["known_at_semantics"] != "strict_known_at"
