from __future__ import annotations

from pathlib import Path

import pytest
from high_intensity_threshold_selection_fixtures import sample_selected_candidate

from ai_trading_system.high_intensity_risk_cap_threshold_selection import (
    HighIntensityThresholdSelectionError,
    build_high_intensity_selected_trigger_contract,
    build_high_intensity_selected_trigger_rule,
    build_high_intensity_trigger_density_guardrail,
)


def test_selected_trigger_contract_contains_hash_and_fields(tmp_path: Path) -> None:
    candidate = sample_selected_candidate()
    guardrail = build_high_intensity_trigger_density_guardrail(
        selected_candidate=candidate,
        selected_event_rows=[{"date": "2023-01-03"}],
    )
    rule_path = tmp_path / "selected_rule.json"
    rule = build_high_intensity_selected_trigger_rule(
        selected_candidate=candidate,
        guardrail=guardrail,
        output_path=rule_path,
    )

    contract = build_high_intensity_selected_trigger_contract(
        selected_rule=rule,
        selected_rule_path=rule_path,
    )

    assert contract["selected_rule_path"] == str(rule_path)
    assert contract["selected_rule_hash"]
    assert "risk_cap_triggered" in contract["required_input_fields"]
    assert "event_id" in contract["output_event_fields"]
    assert contract["safety_fields"]["promotion_allowed"] is False
    assert contract["safety_fields"]["broker_action"] == "none"


def test_selected_trigger_contract_fails_without_selected_rule(tmp_path: Path) -> None:
    with pytest.raises(HighIntensityThresholdSelectionError, match="selected rule"):
        build_high_intensity_selected_trigger_contract(
            selected_rule={},
            selected_rule_path=tmp_path / "missing.json",
        )
