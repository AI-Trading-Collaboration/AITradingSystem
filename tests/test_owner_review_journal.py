from __future__ import annotations

import json
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path

import pytest
from dynamic_v3_position_readiness_helpers import (
    position_advisory_config,
    shadow_shortlist_fixture,
)

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DynamicV3ParameterResearchError,
    create_owner_review,
    owner_review_summary,
    record_owner_review_decision,
    run_position_advisory_daily,
    run_shadow_shortlist_monitor,
    validate_owner_review_artifact,
)


def test_owner_review_create_and_record_monitor_decision(tmp_path: Path) -> None:
    daily = _daily_advisory(tmp_path)
    review = create_owner_review(
        daily_advisory_id=daily["daily_advisory_id"],
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        output_dir=tmp_path / "owner_review_journal",
    )
    events_path = tmp_path / "owner_review_journal" / "owner_review_events.jsonl"
    created_event = _read_jsonl(events_path)[0]
    updated = record_owner_review_decision(
        review_id=review["review_id"],
        decision="monitor",
        output_dir=tmp_path / "owner_review_journal",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
    )

    assert review["review"]["owner_decision"] == "pending"
    assert updated["review"]["owner_decision"] == "monitor"
    assert updated["review"]["broker_action_taken"] is False
    events = _read_jsonl(events_path)
    assert len(events) == 2
    assert events[0] == created_event
    assert events[1]["event_sequence"] == 2
    assert events[1]["previous_event_checksum"] == events[0]["event_checksum"]
    assert len(_read_jsonl(tmp_path / "owner_review_journal" / "owner_review_journal.jsonl")) == 1
    assert (
        validate_owner_review_artifact(
            review_id=review["review_id"],
            output_dir=tmp_path / "owner_review_journal",
        )["status"]
        == "PASS"
    )
    with pytest.raises(DynamicV3ParameterResearchError, match="already has a final decision"):
        record_owner_review_decision(
            review_id=review["review_id"],
            decision="no_trade",
            output_dir=tmp_path / "owner_review_journal",
            daily_advisory_dir=tmp_path / "position_advisory_daily",
        )
    assert _read_jsonl(events_path) == events


def test_owner_review_paper_action_stays_paper_only(tmp_path: Path) -> None:
    daily = _daily_advisory(tmp_path)
    review = create_owner_review(
        daily_advisory_id=daily["daily_advisory_id"],
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        output_dir=tmp_path / "owner_review_journal",
    )
    updated = record_owner_review_decision(
        review_id=review["review_id"],
        decision="paper_adjustment",
        output_dir=tmp_path / "owner_review_journal",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
    )

    assert updated["review"]["paper_action"]["enabled"] is True
    assert updated["review"]["broker_action_taken"] is False
    paper_path = tmp_path / "owner_review_journal" / "paper_action_log.jsonl"
    assert paper_path.exists()
    paper = _read_jsonl(paper_path)[0]
    assert paper["decision_event_id"] == updated["review"]["last_event_id"]
    assert paper["source_daily_position_deltas_checksum"]
    assert paper["broker_action_allowed"] is False
    assert paper["order_ticket_generated"] is False
    assert (
        validate_owner_review_artifact(
            review_id=review["review_id"],
            output_dir=tmp_path / "owner_review_journal",
        )["status"]
        == "PASS"
    )


def test_owner_review_rejects_invalid_or_changed_daily_source_before_write(
    tmp_path: Path,
) -> None:
    daily = _daily_advisory(tmp_path)
    daily_dir = Path(daily["daily_advisory_dir"])
    actions_path = daily_dir / "daily_advisory_actions.json"
    actions = json.loads(actions_path.read_text(encoding="utf-8"))
    actions["recommended_action"] = "tampered"
    actions_path.write_text(json.dumps(actions), encoding="utf-8")
    with pytest.raises(DynamicV3ParameterResearchError, match="validation must PASS"):
        create_owner_review(
            daily_advisory_id=str(daily["daily_advisory_id"]),
            daily_advisory_dir=tmp_path / "position_advisory_daily",
            output_dir=tmp_path / "owner_review_journal",
        )
    assert not (tmp_path / "owner_review_journal").exists()

    clean_root = tmp_path / "clean"
    clean_daily = _daily_advisory(clean_root)
    review = create_owner_review(
        daily_advisory_id=str(clean_daily["daily_advisory_id"]),
        daily_advisory_dir=clean_root / "position_advisory_daily",
        output_dir=tmp_path / "owner_review_journal",
    )
    clean_actions_path = Path(clean_daily["daily_advisory_dir"]) / "daily_advisory_actions.json"
    clean_actions = json.loads(clean_actions_path.read_text(encoding="utf-8"))
    clean_actions["recommended_action"] = "tampered"
    clean_actions_path.write_text(json.dumps(clean_actions), encoding="utf-8")
    with pytest.raises(DynamicV3ParameterResearchError, match="validation must PASS"):
        record_owner_review_decision(
            review_id=review["review_id"],
            decision="monitor",
            output_dir=tmp_path / "owner_review_journal",
            daily_advisory_dir=clean_root / "position_advisory_daily",
        )
    assert len(
        _read_jsonl(tmp_path / "owner_review_journal" / "owner_review_events.jsonl")
    ) == 1


def test_owner_review_rejects_sensitive_notes_and_duplicate_daily_review(
    tmp_path: Path,
) -> None:
    daily = _daily_advisory(tmp_path)
    review = create_owner_review(
        daily_advisory_id=str(daily["daily_advisory_id"]),
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        output_dir=tmp_path / "owner_review_journal",
    )
    with pytest.raises(DynamicV3ParameterResearchError, match="sensitive account data"):
        record_owner_review_decision(
            review_id=review["review_id"],
            decision="monitor",
            manual_notes="account number 123456",
            output_dir=tmp_path / "owner_review_journal",
            daily_advisory_dir=tmp_path / "position_advisory_daily",
        )
    with pytest.raises(DynamicV3ParameterResearchError, match="already exists"):
        create_owner_review(
            daily_advisory_id=str(daily["daily_advisory_id"]),
            daily_advisory_dir=tmp_path / "position_advisory_daily",
            output_dir=tmp_path / "owner_review_journal",
        )
    assert len(
        _read_jsonl(tmp_path / "owner_review_journal" / "owner_review_events.jsonl")
    ) == 1


@pytest.mark.parametrize(
    "tamper_target",
    ["event", "journal", "report", "paper_action", "paper_event_rehashed"],
)
def test_owner_review_validator_detects_chain_view_and_output_tampering(
    tmp_path: Path,
    tamper_target: str,
) -> None:
    daily = _daily_advisory(tmp_path)
    review = create_owner_review(
        daily_advisory_id=str(daily["daily_advisory_id"]),
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        output_dir=tmp_path / "owner_review_journal",
    )
    decision = (
        "paper_adjustment"
        if tamper_target in {"paper_action", "paper_event_rehashed"}
        else "monitor"
    )
    record_owner_review_decision(
        review_id=review["review_id"],
        decision=decision,
        output_dir=tmp_path / "owner_review_journal",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
    )
    journal_root = tmp_path / "owner_review_journal"
    if tamper_target == "event":
        events_path = journal_root / "owner_review_events.jsonl"
        events = _read_jsonl(events_path)
        events[-1]["manual_notes"] = "tampered"
        _write_jsonl(events_path, events)
    elif tamper_target == "journal":
        journal_path = journal_root / "owner_review_journal.jsonl"
        records = _read_jsonl(journal_path)
        records[0]["owner_decision"] = "tampered"
        _write_jsonl(journal_path, records)
    elif tamper_target == "report":
        (journal_root / "owner_review_report.md").write_text("tampered\n", encoding="utf-8")
    elif tamper_target == "paper_action":
        paper_path = journal_root / "paper_action_log.jsonl"
        paper = _read_jsonl(paper_path)
        paper[0]["proposed_deltas"] = {"tampered": {"QQQ": 1.0}}
        _write_jsonl(paper_path, paper)
    else:
        events_path = journal_root / "owner_review_events.jsonl"
        events = _read_jsonl(events_path)
        events[-1]["paper_action_record"]["proposed_deltas"] = {  # type: ignore[index]
            "tampered": {"QQQ": 1.0}
        }
        event_without_checksum = dict(events[-1])
        event_without_checksum.pop("event_checksum")
        events[-1]["event_checksum"] = sha256(
            json.dumps(
                event_without_checksum,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        _write_jsonl(events_path, events)
    validation = validate_owner_review_artifact(
        review_id=review["review_id"],
        output_dir=journal_root,
    )
    assert validation["status"] == "FAIL"
    failed = _failed_check_ids(validation)
    expected = {
        "event": "event_chain_valid",
        "journal": "materialized_journal_matches_event_replay",
        "report": "report_matches_event_replay",
        "paper_action": "paper_action_matches_decision_event",
        "paper_event_rehashed": "paper_action_matches_decision_event",
    }
    assert expected[tamper_target] in failed


def test_owner_review_latest_remains_latest_created_when_older_review_is_decided(
    tmp_path: Path,
) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first_daily = _daily_advisory(first_root)
    second_daily = _daily_advisory(second_root)
    journal_root = tmp_path / "owner_review_journal"
    first = create_owner_review(
        daily_advisory_id=str(first_daily["daily_advisory_id"]),
        daily_advisory_dir=first_root / "position_advisory_daily",
        output_dir=journal_root,
        generated_at=datetime(2026, 6, 7, 10, tzinfo=UTC),
    )
    second = create_owner_review(
        daily_advisory_id=str(second_daily["daily_advisory_id"]),
        daily_advisory_dir=second_root / "position_advisory_daily",
        output_dir=journal_root,
        generated_at=datetime(2026, 6, 7, 11, tzinfo=UTC),
    )
    record_owner_review_decision(
        review_id=first["review_id"],
        decision="monitor",
        output_dir=journal_root,
        daily_advisory_dir=first_root / "position_advisory_daily",
        generated_at=datetime(2026, 6, 7, 12, tzinfo=UTC),
    )
    latest = json.loads((journal_root / "latest_owner_review.json").read_text(encoding="utf-8"))
    assert latest["review_id"] == second["review_id"]
    assert owner_review_summary(output_dir=journal_root)["latest_review_id"] == second["review_id"]


def test_owner_review_legacy_journal_is_read_only_and_explicitly_unlinked(
    tmp_path: Path,
) -> None:
    journal_root = tmp_path / "owner_review_journal"
    journal_root.mkdir(parents=True)
    _write_jsonl(
        journal_root / "owner_review_journal.jsonl",
        [
            {
                "review_id": "legacy-review",
                "daily_advisory_id": "legacy-daily",
                "owner_decision": "pending",
                "recommended_action": "manual_review",
                "broker_action_allowed": False,
                "broker_action_taken": False,
            }
        ],
    )
    summary = owner_review_summary(output_dir=journal_root)
    assert summary["status"] == "PASS_WITH_WARNINGS"
    assert summary["event_chain_status"] == "LEGACY_UNCHAINED"

    daily_root = tmp_path / "new"
    daily = _daily_advisory(daily_root)
    with pytest.raises(DynamicV3ParameterResearchError, match="explicit event-chain migration"):
        create_owner_review(
            daily_advisory_id=str(daily["daily_advisory_id"]),
            daily_advisory_dir=daily_root / "position_advisory_daily",
            output_dir=journal_root,
        )


def _daily_advisory(tmp_path: Path) -> dict[str, object]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    fixture = shadow_shortlist_fixture(tmp_path)
    monitor = run_shadow_shortlist_monitor(
        shadow_shortlist_id=fixture["shadow"]["shadow_shortlist_id"],
        as_of=date(2026, 6, 7),
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "shadow_monitor_runs",
    )
    return run_position_advisory_daily(
        shadow_monitor_run_id=monitor["monitor_run_id"],
        config_path=position_advisory_config(tmp_path),
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "position_advisory_daily",
    )


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )


def _failed_check_ids(payload: dict[str, object]) -> set[str]:
    return {
        str(check["check_id"])
        for check in payload["checks"]  # type: ignore[index]
        if check["passed"] is False  # type: ignore[index]
    }
