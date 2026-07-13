from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from dynamic_v3_confirmation_cycle_helpers import cycle_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    DynamicV3ConfirmationCycleError,
    create_rule_owner_decision,
    list_rule_owner_decisions,
    record_rule_owner_decision,
    rule_owner_decision_report_payload,
    validate_rule_owner_decision_artifact,
)


@pytest.fixture(scope="module")
def owner_decision_cycle(tmp_path_factory: pytest.TempPathFactory) -> dict[str, object]:
    fixture = cycle_fixture(tmp_path_factory.mktemp("rule-owner-decision"))
    yield fixture
    fixture["_monkeypatch"].undo()


def test_rule_owner_decision_records_one_append_only_manual_choice(
    owner_decision_cycle: dict[str, object],
) -> None:
    fixture = owner_decision_cycle
    journal_path = _journal_path(fixture, "happy")
    before_config = fixture["position_config_path"].read_text(encoding="utf-8")
    created = _create(fixture, journal_path)
    decision_id = created["decision_id"]
    snapshot = created["source_snapshot"]

    assert snapshot["schema_version"] == "rule_owner_decision_source_snapshot.v2"
    assert snapshot["cycle_validation"]["status"] == "PASS"
    assert snapshot["cycle_bundle"]["schema_version"] == "content_commitment_bundle.v1"
    assert snapshot["decision_scope_reason"] == "ALL_CYCLE_TARGETS_NO_READY_OWNER_ACTION"
    assert snapshot["target_ids"] == ["limited_adjustment_vs_no_trade"]
    assert "approve_manual_policy_review" not in snapshot["allowed_owner_decisions"]
    assert created["record"]["owner_decision"] == "pending"
    assert created["record"]["event_count"] == 1

    initial_validation = validate_rule_owner_decision_artifact(
        decision_id=decision_id,
        journal_path=journal_path,
    )
    assert initial_validation["status"] == "PASS"

    recorded = record_rule_owner_decision(
        decision_id=decision_id,
        decision="continue_tracking",
        notes="Continue forward evidence accumulation.",
        journal_path=journal_path,
        generated_at=datetime(2026, 8, 1, 2, tzinfo=UTC),
    )
    assert recorded["record"]["owner_decision"] == "continue_tracking"
    assert recorded["record"]["event_count"] == 2
    assert recorded["record"]["policy_change_allowed"] is False
    assert fixture["position_config_path"].read_text(encoding="utf-8") == before_config

    events = _read_jsonl(journal_path)
    assert [event["event_type"] for event in events] == [
        "DECISION_CREATED",
        "DECISION_RECORDED",
    ]
    assert events[1]["previous_event_sha256"] == events[0]["event_sha256"]
    listing = list_rule_owner_decisions(journal_path=journal_path)
    assert listing["decision_count"] == 1
    assert listing["event_count"] == 2
    assert listing["pending_count"] == 0

    report = rule_owner_decision_report_payload(
        decision_id=decision_id,
        journal_path=journal_path,
    )
    assert report["owner_decision"] == "continue_tracking"
    assert Path(report["rule_owner_decision_report_path"]).parent.name == decision_id
    assert (
        validate_rule_owner_decision_artifact(
            decision_id=decision_id,
            journal_path=journal_path,
        )["status"]
        == "PASS"
    )


def test_rule_owner_decision_rejects_naive_cutoff_before_output(
    owner_decision_cycle: dict[str, object],
) -> None:
    fixture = owner_decision_cycle
    journal_path = _journal_path(fixture, "naive")

    with pytest.raises(DynamicV3ConfirmationCycleError, match="timezone-aware"):
        create_rule_owner_decision(
            cycle_id=fixture["cycle"]["cycle_id"],
            cycle_dir=fixture["cycle_dir"],
            journal_path=journal_path,
            generated_at=datetime(2026, 8, 1, 1),
        )

    assert not journal_path.exists()


def test_rule_owner_decision_rejects_duplicate_cycle_before_output(
    owner_decision_cycle: dict[str, object],
) -> None:
    fixture = owner_decision_cycle
    journal_path = _journal_path(fixture, "duplicate")
    _create(fixture, journal_path)
    original = journal_path.read_bytes()

    with pytest.raises(DynamicV3ConfirmationCycleError, match="already exists for cycle"):
        _create(fixture, journal_path, hour=2)

    assert journal_path.read_bytes() == original


def test_rule_owner_decision_enforces_evidence_eligibility_and_single_finalization(
    owner_decision_cycle: dict[str, object],
) -> None:
    fixture = owner_decision_cycle
    journal_path = _journal_path(fixture, "eligibility")
    created = _create(fixture, journal_path)
    decision_id = created["decision_id"]
    original = journal_path.read_bytes()

    with pytest.raises(DynamicV3ConfirmationCycleError, match="not eligible"):
        record_rule_owner_decision(
            decision_id=decision_id,
            decision="approve_manual_policy_review",
            journal_path=journal_path,
            generated_at=datetime(2026, 8, 1, 2, tzinfo=UTC),
        )
    with pytest.raises(DynamicV3ConfirmationCycleError, match="strictly later"):
        record_rule_owner_decision(
            decision_id=decision_id,
            decision="continue_tracking",
            journal_path=journal_path,
            generated_at=datetime(2026, 8, 1, 1, tzinfo=UTC),
        )
    assert journal_path.read_bytes() == original

    record_rule_owner_decision(
        decision_id=decision_id,
        decision="continue_tracking",
        journal_path=journal_path,
        generated_at=datetime(2026, 8, 1, 2, tzinfo=UTC),
    )
    finalized = journal_path.read_bytes()
    with pytest.raises(DynamicV3ConfirmationCycleError, match="already finalized"):
        record_rule_owner_decision(
            decision_id=decision_id,
            decision="defer",
            journal_path=journal_path,
            generated_at=datetime(2026, 8, 1, 3, tzinfo=UTC),
        )
    assert journal_path.read_bytes() == finalized


def test_rule_owner_decision_validator_rejects_every_materialized_view_tamper(
    owner_decision_cycle: dict[str, object],
) -> None:
    fixture = owner_decision_cycle
    journal_path = _journal_path(fixture, "views")
    created = _create(fixture, journal_path)
    decision_id = created["decision_id"]
    decision_dir = journal_path.parent / decision_id
    paths = (
        decision_dir / "rule_owner_decision_source_snapshot.json",
        decision_dir / "rule_owner_decision_record.json",
        decision_dir / "rule_owner_decision_manifest.json",
        decision_dir / "rule_owner_decision_report.md",
        journal_path.parent / "rule_owner_decision_report.md",
    )

    for path in paths:
        original = path.read_bytes()
        path.write_bytes(original + b"\n")
        assert (
            validate_rule_owner_decision_artifact(
                decision_id=decision_id,
                journal_path=journal_path,
            )["status"]
            == "FAIL"
        ), path.name
        path.write_bytes(original)

    assert (
        validate_rule_owner_decision_artifact(
            decision_id=decision_id,
            journal_path=journal_path,
        )["status"]
        == "PASS"
    )


def test_rule_owner_decision_rejects_live_cycle_drift_before_record(
    owner_decision_cycle: dict[str, object],
) -> None:
    fixture = owner_decision_cycle
    journal_path = _journal_path(fixture, "source-drift")
    created = _create(fixture, journal_path)
    decision_id = created["decision_id"]
    cycle_manifest = fixture["cycle"]["cycle_dir"] / "rule_review_cycle_manifest.json"
    original_manifest = cycle_manifest.read_bytes()
    original_journal = journal_path.read_bytes()
    try:
        cycle_manifest.write_bytes(original_manifest + b"\n")
        with pytest.raises(DynamicV3ConfirmationCycleError, match="validation failed"):
            record_rule_owner_decision(
                decision_id=decision_id,
                decision="continue_tracking",
                journal_path=journal_path,
                generated_at=datetime(2026, 8, 1, 2, tzinfo=UTC),
            )
        assert journal_path.read_bytes() == original_journal
        assert (
            validate_rule_owner_decision_artifact(
                decision_id=decision_id,
                journal_path=journal_path,
            )["status"]
            == "FAIL"
        )
    finally:
        cycle_manifest.write_bytes(original_manifest)

    assert (
        validate_rule_owner_decision_artifact(
            decision_id=decision_id,
            journal_path=journal_path,
        )["status"]
        == "PASS"
    )


def test_rule_owner_decision_rejects_broken_event_chain(
    owner_decision_cycle: dict[str, object],
) -> None:
    fixture = owner_decision_cycle
    journal_path = _journal_path(fixture, "chain")
    created = _create(fixture, journal_path)
    decision_id = created["decision_id"]
    original = journal_path.read_bytes()
    events = _read_jsonl(journal_path)
    events[0]["event_sha256"] = "0" * 64
    _write_jsonl(journal_path, events)

    assert (
        validate_rule_owner_decision_artifact(
            decision_id=decision_id,
            journal_path=journal_path,
        )["status"]
        == "FAIL"
    )
    with pytest.raises(DynamicV3ConfirmationCycleError, match="chain invalid"):
        record_rule_owner_decision(
            decision_id=decision_id,
            decision="continue_tracking",
            journal_path=journal_path,
            generated_at=datetime(2026, 8, 1, 2, tzinfo=UTC),
        )
    journal_path.write_bytes(original)


def test_rule_owner_decision_legacy_journal_is_read_only_and_cannot_pass_validation(
    owner_decision_cycle: dict[str, object],
) -> None:
    fixture = owner_decision_cycle
    journal_path = _journal_path(fixture, "legacy")
    legacy = {
        "schema_version": 1,
        "decision_id": "legacy-decision",
        "cycle_id": fixture["cycle"]["cycle_id"],
        "owner_decision": "continue_tracking",
        "policy_change_allowed": False,
        "auto_apply": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
    }
    _write_jsonl(journal_path, [legacy])
    original = journal_path.read_bytes()

    listing = list_rule_owner_decisions(journal_path=journal_path)
    assert listing["status"] == "PASS_WITH_WARNINGS"
    assert listing["legacy_unsnapshotted"] is True
    assert (
        validate_rule_owner_decision_artifact(
            decision_id="legacy-decision",
            journal_path=journal_path,
        )["status"]
        == "FAIL"
    )
    with pytest.raises(DynamicV3ConfirmationCycleError, match="read-only"):
        _create(fixture, journal_path, hour=2)
    assert journal_path.read_bytes() == original


def _journal_path(fixture: dict[str, object], scenario: str) -> Path:
    return fixture["journal_path"].parent / scenario / "rule_owner_decision_journal.jsonl"


def _create(
    fixture: dict[str, object],
    journal_path: Path,
    *,
    hour: int = 1,
) -> dict[str, object]:
    return create_rule_owner_decision(
        cycle_id=fixture["cycle"]["cycle_id"],
        cycle_dir=fixture["cycle_dir"],
        journal_path=journal_path,
        generated_at=datetime(2026, 8, 1, hour, tzinfo=UTC),
    )


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
