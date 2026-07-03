from __future__ import annotations

from pathlib import Path

from high_intensity_threshold_selection_fixtures import (
    build_high_intensity_threshold_selection_fixture,
    sample_selected_candidate,
)

from ai_trading_system.high_intensity_risk_cap_threshold_selection import (
    build_high_intensity_event_logger_input_contract,
    build_high_intensity_selected_trigger_rule,
    build_high_intensity_trigger_density_guardrail,
    load_trading_2334_forward_observe_plan_outputs,
)


def test_event_logger_input_contract_references_upstream_contracts(
    tmp_path: Path,
) -> None:
    fixture = build_high_intensity_threshold_selection_fixture(tmp_path)
    forward_plan = load_trading_2334_forward_observe_plan_outputs(
        fixture["forward_observe_plan_dir"]
    )
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

    contract = build_high_intensity_event_logger_input_contract(
        selected_rule=rule,
        selected_rule_path=rule_path,
        forward_observe_plan=forward_plan,
    )

    assert contract["event_logger_contract_id"]
    assert contract["source_event_schema"]["hash"]
    assert contract["evidence_contract"]["hash"]
    assert contract["actual_path_outcome_contract"]["hash"]
    assert contract["manual_review_boundary"]["hash"]
    assert contract["runtime_observe_allowed"] is True
    assert contract["paper_shadow_allowed"] is False
    assert contract["broker_action"] == "none"
