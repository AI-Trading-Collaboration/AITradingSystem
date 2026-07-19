from __future__ import annotations

import json
from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_formal_research_method_contract_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_formal_research_method_contract_builds_and_validates(tmp_path: Path, monkeypatch) -> None:
    fixture = run_formal_research_method_contract_fixture(tmp_path, monkeypatch)
    contract_result = fixture["formal_research_method_contract"]
    validation = readiness.validate_formal_research_method_contract_artifact(
        contract_id=contract_result["contract_id"],
        output_dir=tmp_path / "formal_research_method_contract",
    )
    assert validation["status"] == "PASS"
    contract = contract_result["formal_research_method_contract"]
    decision = contract_result["formal_research_method_decision"]
    assert decision["formal_research_method_status"] == "NOT_READY"
    assert decision["promotion_state"] == "NEEDS_MORE_EVIDENCE"
    assert "validated_dated_filtered_outcomes_missing" in decision["blocking_reasons"]
    assert decision["paper_shadow_eligibility"] == "NOT_ELIGIBLE"
    assert decision["next_required_action"] == "collect_missing_research_evidence"
    assert contract["paper_shadow_eligibility"]["eligible"] is False
    assert len(contract["objective_gates"]) >= 9
    assert set(readiness.FORMAL_RESEARCH_PROMOTION_STATES).issubset(
        set(contract["promotion_states"])
    )
    validation_path = (
        contract_result["contract_dir"] / "formal_research_method_contract_validation.json"
    )
    assert validation_path.exists()
    assert_research_safe(contract_result["manifest"])
    assert "blocking_issues" in contract_result["reader_brief_section"]
    snapshot = contract_result["input_snapshot"]
    assert snapshot["schema_version"] == "formal_research_method_contract_input_snapshot.v2"
    assert len(snapshot["sources"]) == 10
    gates = {row["gate_id"]: row for row in contract["objective_gates"]}
    assert gates["confirmation_completed_observations"]["observed_status"] == 0
    assert gates["confirmation_completed_observations"]["passed"] is False
    assert gates["owner_approval"]["observed_status"] == "NOT_OBSERVED"
    assert contract["method_boundary"]["positive_state_requires_all_observed_gates"] is True

    decision_path = contract_result["contract_dir"] / "formal_research_method_decision.json"
    tampered = json.loads(decision_path.read_text(encoding="utf-8"))
    tampered["promotion_state"] = "FORMAL_RESEARCH_READY"
    decision_path.write_text(json.dumps(tampered, sort_keys=True) + "\n", encoding="utf-8")
    assert readiness.validate_formal_research_method_contract_artifact(
        contract_id=contract_result["contract_id"],
        output_dir=tmp_path / "formal_research_method_contract",
        write_output=False,
    )["status"] == "FAIL"
