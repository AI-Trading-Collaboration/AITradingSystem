from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_forward_observe_event_logger import (
    build_high_intensity_manual_review_event_queue,
)


def test_manual_review_queue_exposes_observation_context_only() -> None:
    event_log = [
        {
            "event_id": "evt_1",
            "event_cluster_id": "cluster_1",
            "event_date": "2023-01-03",
            "target_asset": "QQQ",
            "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
            "high_intensity_reason": "fixture",
        }
    ]
    cluster_registry = [
        {
            "event_cluster_id": "cluster_1",
            "cluster_active_days": 2,
            "trigger_day_count": 2,
            "monthly_event_count": 4,
        }
    ]

    queue = build_high_intensity_manual_review_event_queue(
        event_log=event_log,
        cluster_registry=cluster_registry,
        monthly_report={"monthly_concentration_status": "PASS_WITH_WARNINGS"},
    )

    assert len(queue) == 1
    assert queue[0]["manual_review_observation_flag"] is True
    assert queue[0]["position_instruction_generated"] is False
    assert "target_weight" in queue[0]["forbidden_outputs"]
    assert queue[0]["promotion_allowed"] is False
    assert queue[0]["paper_shadow_allowed"] is False
    assert queue[0]["production_allowed"] is False
    assert queue[0]["broker_action"] == "none"
