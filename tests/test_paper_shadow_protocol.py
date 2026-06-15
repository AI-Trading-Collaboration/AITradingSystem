from __future__ import annotations

from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_formal_research_method_contract_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_paper_shadow_protocol_builds_and_validates(tmp_path: Path) -> None:
    fixture = run_formal_research_method_contract_fixture(tmp_path)
    contract_result = fixture["formal_research_method_contract"]
    protocol_result = readiness.build_paper_shadow_protocol(
        contract_id=contract_result["contract_id"],
        contract_dir=tmp_path / "formal_research_method_contract",
        output_dir=tmp_path / "paper_shadow_protocol",
    )

    protocol = protocol_result["paper_shadow_protocol"]
    validation = protocol_result["paper_shadow_protocol_validation"]
    report_payload = readiness.paper_shadow_protocol_report_payload(
        protocol_id=protocol_result["protocol_id"],
        output_dir=tmp_path / "paper_shadow_protocol",
    )

    assert protocol["protocol_status"] == "PROTOCOL_READY"
    assert protocol["eligibility_status"] == "ELIGIBLE"
    assert protocol["blocking_reasons"] == []
    assert protocol["next_required_action"] == "start_daily_paper_shadow_runner_design"
    assert (
        protocol["required_observation_period"]["minimum_trading_days"]
        == readiness.PAPER_SHADOW_REQUIRED_OBSERVATION_DAYS
    )
    assert {row["field"] for row in protocol["daily_review_fields"]} == set(
        readiness.PAPER_SHADOW_DAILY_REVIEW_FIELDS
    )
    assert {row["exit_condition"] for row in protocol["exit_conditions"]} == set(
        readiness.PAPER_SHADOW_EXIT_CONDITIONS
    )
    assert validation["status"] == "PASS"
    assert report_payload["paper_shadow_protocol"]["protocol_status"] == "PROTOCOL_READY"
    assert "paper_shadow_protocol_status" in protocol_result["reader_brief_section"]
    assert_research_safe(protocol_result["manifest"])
    assert protocol["broker_order_system_consumable"] is False
