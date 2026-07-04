from __future__ import annotations

from high_intensity_event_logger_fixtures import sample_selected_rule

from ai_trading_system.high_intensity_risk_cap_forward_observe_event_logger import (
    build_high_intensity_observe_event_log,
)


def _trigger_day(date: str) -> dict[str, object]:
    return {
        "trigger_day_id": f"td_{date}",
        "date": date,
        "target_asset": "QQQ",
        "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
        "selected_rule_version": "v1",
        "risk_cap_intensity": "high",
        "risk_cap_score": 1.1,
        "high_intensity_reason": "fixture",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def test_observe_event_log_deduplicates_cluster_to_primary_event() -> None:
    clusters, event_log, trigger_rows = build_high_intensity_observe_event_log(
        trigger_day_rows=[_trigger_day("2023-01-03"), _trigger_day("2023-01-04")],
        selected_rule=sample_selected_rule(),
    )
    clusters_again, event_log_again, _ = build_high_intensity_observe_event_log(
        trigger_day_rows=[_trigger_day("2023-01-03"), _trigger_day("2023-01-04")],
        selected_rule=sample_selected_rule(),
    )

    assert len(clusters) == 1
    assert len(event_log) == 1
    assert len(trigger_rows) == 2
    assert event_log[0]["event_id"] == event_log_again[0]["event_id"]
    assert clusters[0]["event_cluster_id"] == clusters_again[0]["event_cluster_id"]
    assert event_log[0]["event_status"] == "OBSERVE_PENDING"
    assert event_log[0]["manual_review_observation_flag"] is True
    assert event_log[0]["promotion_allowed"] is False
    assert event_log[0]["broker_action"] == "none"
