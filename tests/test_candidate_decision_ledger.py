from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_paper_shadow_protocol_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness


def test_candidate_decision_ledger_records_and_validates(tmp_path: Path, monkeypatch) -> None:
    fixture = run_paper_shadow_protocol_fixture(tmp_path, monkeypatch)
    contract = fixture["formal_research_method_contract"]
    protocol = fixture["paper_shadow_protocol"]
    first = _record_candidate_decision(tmp_path, fixture, contract, protocol, 21)
    second = _record_candidate_decision(tmp_path, fixture, contract, protocol, 22)

    validation = readiness.validate_candidate_decision_ledger_artifact(
        ledger_run_id=second["ledger_run_id"],
        output_dir=tmp_path / "candidate_decision_ledger",
    )
    report_payload = readiness.candidate_decision_ledger_report_payload(
        ledger_run_id=second["ledger_run_id"],
        output_dir=tmp_path / "candidate_decision_ledger",
    )

    record = second["candidate_decision_record"]
    ledger_rows = report_payload["candidate_decision_ledger_snapshot"]
    canonical_rows = readiness._read_jsonl(
        tmp_path / "candidate_decision_ledger" / "candidate_decision_ledger.jsonl"
    )

    assert validation["status"] == "PASS"
    assert report_payload["candidate_decision_record"]["record_id"] == record["record_id"]
    assert record["candidate"] == readiness.TOP_FILTERED_CANDIDATE
    assert record["evidence_status"] == "INSUFFICIENT_DATA"
    assert record["stress_result"] == "INSUFFICIENT_DATA"
    assert record["mismatch_result"] == "INSUFFICIENT_DATA"
    assert record["flip_result"] == "INSUFFICIENT_DATA"
    assert record["rotation_result"] == "INSUFFICIENT_DATA"
    assert record["ab_result"] == "INSUFFICIENT_DATA"
    assert record["confirmation_count"] == 0
    assert record["owner_action"] is None
    assert record["owner_decision_status"] == "NOT_OBSERVED"
    assert record["system_recommended_action"] == "COLLECT_VALIDATED_DATED_FILTERED_OUTCOMES"
    assert record["final_decision"] == "COLLECT_DATED_EVIDENCE"
    assert record["next_required_action"] == "collect_missing_research_evidence"
    assert record["eb5_protocol_status"] == "UNVALIDATED_EB5_PROTOCOL_IGNORED"
    assert record["ledger_sequence"] == 2
    assert record["previous_record_hash"] == first["candidate_decision_record"]["record_hash"]
    assert {row["record_id"] for row in ledger_rows} == {
        first["record_id"],
        second["record_id"],
    }
    assert [row["record_id"] for row in canonical_rows] == [
        first["record_id"],
        second["record_id"],
    ]
    assert "candidate_decision_ledger_status" in second["reader_brief_section"]
    assert_research_safe(second["manifest"])
    assert second["manifest"]["append_only_ledger"] is True
    assert second["input_snapshot"]["schema_version"] == (
        "candidate_decision_ledger_input_snapshot.v2"
    )

    canonical_path = tmp_path / "candidate_decision_ledger" / "candidate_decision_ledger.jsonl"
    tampered_rows = list(canonical_rows)
    tampered_rows[0] = {**tampered_rows[0], "final_decision": "FORMALIZE"}
    canonical_path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in tampered_rows),
        encoding="utf-8",
    )
    assert readiness.validate_candidate_decision_ledger_artifact(
        ledger_run_id=second["ledger_run_id"],
        output_dir=tmp_path / "candidate_decision_ledger",
        write_output=False,
    )["status"] == "FAIL"


def _record_candidate_decision(
    tmp_path: Path,
    fixture: dict[str, object],
    contract: dict[str, object],
    protocol: dict[str, object],
    day: int,
) -> dict[str, object]:
    return readiness.record_candidate_decision_ledger(
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        evidence_id=fixture["filtered_candidate_evidence"]["evidence_id"],
        stress_backfill_id=fixture["filtered_candidate_stress_backfill"]["stress_backfill_id"],
        mismatch_reduction_id=fixture["drawdown_mismatch_reduction"]["reduction_id"],
        flip_reduction_id=fixture["flip_rotation_reduction"]["flip_reduction_id"],
        ab_review_id=fixture["filtered_candidate_ab_review"]["ab_review_id"],
        confirmation_id=fixture["signal_gate_confirmation"]["confirmation_id"],
        owner_review_id=fixture["owner_filtered_candidate_review"]["owner_review_id"],
        next_decision_id=fixture["filtered_next_decision"]["decision_id"],
        contract_id=contract["contract_id"],
        protocol_id=protocol["protocol_id"],
        evidence_dir=tmp_path / "filtered_candidate_evidence",
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        mismatch_reduction_dir=tmp_path / "drawdown_mismatch_reduction",
        flip_reduction_dir=tmp_path / "flip_rotation_reduction",
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        confirmation_dir=tmp_path / "signal_gate_confirmation",
        owner_review_dir=tmp_path / "owner_filtered_candidate_review",
        next_decision_dir=tmp_path / "filtered_next_decision",
        contract_dir=tmp_path / "formal_research_method_contract",
        protocol_dir=tmp_path / "paper_shadow_protocol",
        output_dir=tmp_path / "candidate_decision_ledger",
        generated_at=datetime(2024, 4, day, tzinfo=UTC),
    )
