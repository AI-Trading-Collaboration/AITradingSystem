from __future__ import annotations

from copy import deepcopy

from high_intensity_scheduler_dry_run_fixtures import (
    sample_selected_rule,
    sample_trigger_source_rows,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_dry_run import (
    build_high_intensity_scheduler_event_detection_job_dry_run_result,
)


def _inputs() -> dict[str, object]:
    return {
        "runtime_plan": {"summary": {"selected_rule_hash": "rule_hash"}},
        "dynamic_dry_run": {
            "summary": {"known_at_policy": "NEXT_SESSION_DECISION_POLICY"},
            "pit_boundary": {"pit_approximation_ready": True},
        },
    }


def test_scheduler_event_detection_detects_composite_rule() -> None:
    rows = build_high_intensity_scheduler_event_detection_job_dry_run_result(
        trigger_source_rows=sample_trigger_source_rows(),
        selected_rule=sample_selected_rule(),
        inputs=_inputs(),
    )

    assert rows[0]["scheduler_cycle_id"] == "hisch_historical_replay_scheduler_cycle"
    assert rows[0]["cycle_mode"] == "historical_replay_scheduler_cycle"
    assert rows[0]["detection_status"] == "DETECTED"
    assert rows[0]["high_intensity_triggered"] is True
    assert "target_weight" not in rows[0]


def test_scheduler_event_detection_ignores_false_trigger() -> None:
    rows = build_high_intensity_scheduler_event_detection_job_dry_run_result(
        trigger_source_rows=sample_trigger_source_rows(),
        selected_rule=sample_selected_rule(),
        inputs=_inputs(),
    )

    assert rows[1]["detection_status"] == "NOT_DETECTED"
    assert rows[1]["high_intensity_reason"] == "risk_cap_triggered=false"


def test_scheduler_event_detection_ignores_inactive_scope() -> None:
    source = deepcopy(sample_trigger_source_rows())
    source[0]["scope_active"] = False

    rows = build_high_intensity_scheduler_event_detection_job_dry_run_result(
        trigger_source_rows=source,
        selected_rule=sample_selected_rule(),
        inputs=_inputs(),
    )

    assert rows[0]["detection_status"] == "NOT_DETECTED"
    assert rows[0]["high_intensity_reason"] == "scope_active=false"


def test_scheduler_event_detection_ignores_non_defensive_direction() -> None:
    source = deepcopy(sample_trigger_source_rows())
    source[0]["signal_direction"] = "risk_on"

    rows = build_high_intensity_scheduler_event_detection_job_dry_run_result(
        trigger_source_rows=source,
        selected_rule=sample_selected_rule(),
        inputs=_inputs(),
    )

    assert rows[0]["detection_status"] == "NOT_DETECTED"
    assert rows[0]["high_intensity_reason"] == "signal_direction_not_defensive"


def test_scheduler_event_detection_blocks_missing_known_at_timestamp() -> None:
    source = deepcopy(sample_trigger_source_rows())
    source[0]["decision_timestamp"] = ""
    source[0]["risk_cap_decision_timestamp"] = ""

    rows = build_high_intensity_scheduler_event_detection_job_dry_run_result(
        trigger_source_rows=source,
        selected_rule=sample_selected_rule(),
        inputs=_inputs(),
    )

    assert rows[0]["detection_status"] == "BLOCKED_INPUT_INVALID"
    assert "MISSING_KNOWN_AT_TIMESTAMP" in rows[0]["blocked_reason"]
