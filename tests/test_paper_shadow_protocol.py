from __future__ import annotations

import json
from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_formal_research_method_contract_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_paper_shadow_protocol_builds_and_validates(tmp_path: Path, monkeypatch) -> None:
    fixture = run_formal_research_method_contract_fixture(tmp_path, monkeypatch)
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

    assert protocol["protocol_status"] == "PROTOCOL_BLOCKED"
    assert protocol["eligibility_status"] == "BLOCKED"
    assert set(protocol["blocking_reasons"]) >= {
        "formal_contract_ready",
        "paper_shadow_eligible_for_protocol_design",
    }
    assert protocol["next_required_action"] == "return_to_research_contract_review"
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
    assert report_payload["paper_shadow_protocol"]["protocol_status"] == "PROTOCOL_BLOCKED"
    assert "paper_shadow_protocol_status" in protocol_result["reader_brief_section"]
    assert_research_safe(protocol_result["manifest"])
    assert protocol["broker_order_system_consumable"] is False

    snapshot_path = (
        tmp_path
        / "paper_shadow_protocol"
        / protocol_result["protocol_id"]
        / "paper_shadow_protocol_input_snapshot.json"
    )
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot["source_contract_id"] = "tampered-contract"
    snapshot_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tampered_validation = readiness.validate_paper_shadow_protocol_artifact(
        protocol_id=protocol_result["protocol_id"],
        output_dir=tmp_path / "paper_shadow_protocol",
        write_output=False,
    )
    failed = {
        row["check_id"]
        for row in tampered_validation["checks"]
        if row.get("passed") is not True
    }
    assert tampered_validation["status"] == "FAIL"
    assert "input_snapshot_sha256_matches" in failed


def test_paper_shadow_protocol_validation_rejects_live_contract_tamper(
    tmp_path: Path, monkeypatch
) -> None:
    fixture = run_formal_research_method_contract_fixture(tmp_path, monkeypatch)
    contract_result = fixture["formal_research_method_contract"]
    protocol_result = readiness.build_paper_shadow_protocol(
        contract_id=contract_result["contract_id"],
        contract_dir=tmp_path / "formal_research_method_contract",
        output_dir=tmp_path / "paper_shadow_protocol",
    )
    contract_path = (
        tmp_path
        / "formal_research_method_contract"
        / contract_result["contract_id"]
        / "formal_research_method_contract.json"
    )
    payload = json.loads(contract_path.read_text(encoding="utf-8"))
    payload["candidate"] = "tampered_candidate"
    contract_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    validation = readiness.validate_paper_shadow_protocol_artifact(
        protocol_id=protocol_result["protocol_id"],
        output_dir=tmp_path / "paper_shadow_protocol",
        write_output=False,
    )

    assert validation["status"] == "FAIL"
    failed = {
        row["check_id"]
        for row in validation["checks"]
        if row.get("passed") is not True
    }
    assert "source_contract_snapshot_matches_live" in failed
    assert "source_contract_validation_pass" in failed
