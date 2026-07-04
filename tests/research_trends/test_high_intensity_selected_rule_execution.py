from __future__ import annotations

import pytest
from high_intensity_event_logger_fixtures import (
    sample_selected_rule,
    sample_trigger_source_rows,
)

from ai_trading_system.high_intensity_risk_cap_forward_observe_event_logger import (
    HighIntensityEventLoggerError,
    selected_rule_matches,
    validate_selected_rule_input_fields,
)


def test_selected_rule_execution_requires_composite_conditions() -> None:
    rule = sample_selected_rule(threshold=1.0)
    base = sample_trigger_source_rows()[0]

    assert selected_rule_matches(base, rule) is True
    assert selected_rule_matches({**base, "risk_cap_triggered": False}, rule) is False
    assert selected_rule_matches({**base, "scope_active": False}, rule) is False
    assert selected_rule_matches({**base, "risk_cap_score": 0.99}, rule) is False
    assert selected_rule_matches({**base, "signal_direction": "none"}, rule) is False
    assert selected_rule_matches({**base, "signal_direction": "risk_on"}, rule) is False


def test_selected_rule_input_field_coverage_allows_controlled_pit_derivations() -> None:
    coverage = validate_selected_rule_input_fields(
        trigger_source_rows=sample_trigger_source_rows(),
        selected_rule=sample_selected_rule(),
        dynamic_dry_run={
            "summary": {
                "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
                "pit_policy": "PIT_APPROXIMATION_READY",
            },
            "pit_boundary": {"known_at_policy": "NEXT_SESSION_DECISION_POLICY"},
        },
    )
    by_field = {row["field"]: row for row in coverage}

    assert by_field["as_of_timestamp"]["status"] == "PRESENT_DERIVED"
    assert by_field["known_at_policy"]["status"] == "PRESENT_DERIVED"
    assert by_field["pit_policy"]["status"] == "PRESENT_DERIVED"


def test_selected_rule_input_field_coverage_blocks_missing_base_fields() -> None:
    rows = sample_trigger_source_rows()
    rows[0].pop("decision_timestamp")
    rows[1].pop("decision_timestamp")

    with pytest.raises(
        HighIntensityEventLoggerError,
        match="BLOCKED_MISSING_SELECTED_RULE_INPUT_FIELDS",
    ):
        validate_selected_rule_input_fields(
            trigger_source_rows=rows,
            selected_rule=sample_selected_rule(),
            dynamic_dry_run={"summary": {}, "pit_boundary": {}},
        )
