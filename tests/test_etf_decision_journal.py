from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.decision_journal import (
    DECISION_JOURNAL_SCHEMA_VERSION,
    DecisionJournalError,
    add_decision_entry,
    build_decision_entry,
    build_decision_entry_from_weekly_review,
    build_decision_journal_report,
    empty_decision_journal,
    load_decision_journal,
    remove_decision_entry,
    update_decision_entry,
    validate_decision_entry,
    validate_decision_entry_links,
    validate_decision_journal_schema,
    write_decision_journal,
)
from ai_trading_system.reports.reader_brief import _etf_decision_journal_summary


def test_decision_journal_schema_accepts_safe_entry(tmp_path: Path) -> None:
    entry = _entry(tmp_path)
    journal = empty_decision_journal(datetime(2026, 6, 2, tzinfo=UTC))
    journal["entries"] = [entry]

    assert journal["schema_version"] == DECISION_JOURNAL_SCHEMA_VERSION
    assert validate_decision_journal_schema(journal) == []
    output_path = write_decision_journal(journal, tmp_path / "journal.json")
    assert output_path.exists()


def test_decision_entry_rejects_unknown_decision_status(tmp_path: Path) -> None:
    entry = _entry(tmp_path)
    entry["decision_status"] = "place_order"

    with pytest.raises(DecisionJournalError, match="unsupported decision_status"):
        validate_decision_entry(entry)


def test_decision_entry_rejects_disallowed_action_in_human_decision(tmp_path: Path) -> None:
    entry = _entry(tmp_path)
    entry["decision_status"] = "continue_observation"
    entry["human_decision"] = "place_order"

    with pytest.raises(DecisionJournalError, match="disallowed action"):
        validate_decision_entry(entry)


def test_decision_entry_rejects_missing_safety_field(tmp_path: Path) -> None:
    entry = _entry(tmp_path)
    entry["broker_action"] = "paper_order"

    with pytest.raises(DecisionJournalError, match="broker_action"):
        validate_decision_entry(entry)


def test_decision_entry_rejects_confidence_out_of_bounds(tmp_path: Path) -> None:
    entry = _entry(tmp_path)
    entry["confidence"] = 1.5

    with pytest.raises(DecisionJournalError, match="confidence"):
        validate_decision_entry(entry)


def test_decision_entry_links_to_weekly_review_action(tmp_path: Path) -> None:
    weekly_path = _weekly_review_path(tmp_path)
    entry = build_decision_entry_from_weekly_review(
        weekly_review_path=weekly_path,
        action_item_id="weekly-action-1",
        human_decision="continue observation",
        decision_status="continue_observation",
        rationale="Need one more forward window.",
        confidence=0.6,
        follow_up_task="Review forward dashboard next week.",
        linked_candidate="shadow_base_ai_growth",
        created_at=datetime(2026, 6, 2, tzinfo=UTC),
        decision_id="decision-linked-1",
    )

    assert entry["review_id"] == "etf-weekly-review-2026-06-01"
    assert entry["source_section"] == "shadow_candidate_review"
    assert entry["source_action_type"] == "review_candidate"
    assert validate_decision_entry_links(entry) == []


def test_decision_journal_add_update_remove_preserves_audit(tmp_path: Path) -> None:
    weekly_path = _weekly_review_path(tmp_path)
    entry = build_decision_entry_from_weekly_review(
        weekly_review_path=weekly_path,
        action_item_id="weekly-action-1",
        human_decision="mark watch",
        decision_status="mark_watch",
        rationale="Weak forward performance.",
        confidence=0.8,
        follow_up_task="Check next weekly review.",
        linked_candidate="shadow_base_ai_growth",
        created_at=datetime(2026, 6, 2, tzinfo=UTC),
        decision_id="decision-linked-2",
    )
    journal = add_decision_entry(empty_decision_journal(), entry)
    journal = update_decision_entry(
        journal,
        decision_id="decision-linked-2",
        updates={"decision_status": "defer_decision", "confidence": 0.5},
        updated_at=datetime(2026, 6, 3, tzinfo=UTC),
    )
    assert journal["entries"][0]["decision_status"] == "defer_decision"
    assert len(journal["entries"][0]["audit_trail"]) == 2

    journal = remove_decision_entry(
        journal,
        decision_id="decision-linked-2",
        reason="superseded by newer weekly review",
        removed_at=datetime(2026, 6, 4, tzinfo=UTC),
    )
    assert journal["entries"] == []
    assert journal["removed_entries"][0]["removal_reason"] == "superseded by newer weekly review"


def test_decision_journal_cli_add_update_list_remove(tmp_path: Path) -> None:
    weekly_path = _weekly_review_path(tmp_path)
    journal_path = tmp_path / "journal.json"
    runner = CliRunner()

    result = runner.invoke(
        etf_app,
        [
            "decision-journal",
            "add",
            "--weekly-review-path",
            str(weekly_path),
            "--action-item-id",
            "weekly-action-1",
            "--human-decision",
            "continue observation",
            "--decision-status",
            "continue_observation",
            "--rationale",
            "Forward window is not mature enough.",
            "--confidence",
            "0.7",
            "--follow-up-task",
            "Review next week.",
            "--linked-candidate",
            "shadow_base_ai_growth",
            "--journal-path",
            str(journal_path),
        ],
    )

    assert result.exit_code == 0, result.output
    journal = load_decision_journal(journal_path)
    decision_id = journal["entries"][0]["decision_id"]
    assert journal["entries"][0]["production_effect"] == "none"

    update = runner.invoke(
        etf_app,
        [
            "decision-journal",
            "update",
            "--decision-id",
            decision_id,
            "--journal-path",
            str(journal_path),
            "--decision-status",
            "mark_watch",
            "--rationale",
            "Manual review marked as watch.",
        ],
    )
    assert update.exit_code == 0, update.output
    assert load_decision_journal(journal_path)["entries"][0]["decision_status"] == "mark_watch"

    listed = runner.invoke(
        etf_app,
        ["decision-journal", "list", "--journal-path", str(journal_path), "--json"],
    )
    assert listed.exit_code == 0, listed.output
    assert decision_id in listed.output

    removed = runner.invoke(
        etf_app,
        [
            "decision-journal",
            "remove",
            "--decision-id",
            decision_id,
            "--reason",
            "test cleanup",
            "--journal-path",
            str(journal_path),
        ],
    )
    assert removed.exit_code == 0, removed.output
    assert load_decision_journal(journal_path)["entries"] == []


def test_decision_journal_cli_report_analytics_proposal_validate(tmp_path: Path) -> None:
    weekly_path = _weekly_review_path(tmp_path)
    journal_path = tmp_path / "journal.json"
    entry = build_decision_entry_from_weekly_review(
        weekly_review_path=weekly_path,
        action_item_id="weekly-action-1",
        human_decision="continue observation",
        decision_status="continue_observation",
        rationale="Forward window is not mature enough.",
        confidence=0.7,
        follow_up_task="Review next week.",
        linked_candidate="shadow_base_ai_growth",
        created_at=datetime(2026, 6, 2, tzinfo=UTC),
        decision_id="decision-report-1",
    )
    journal = add_decision_entry(empty_decision_journal(), entry)
    write_decision_journal(journal, journal_path)
    output_dir = tmp_path / "reports"
    proposal_dir = tmp_path / "proposals"
    validation_dir = tmp_path / "validation"
    runner = CliRunner()

    report = runner.invoke(
        etf_app,
        [
            "decision-journal",
            "report",
            "--as-of",
            "2026-06-02",
            "--journal-path",
            str(journal_path),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0, report.output
    assert (output_dir / "decision_journal_2026-06-02.json").exists()
    assert (output_dir / "decision_journal_2026-06-02.md").exists()
    assert (output_dir / "decision_journal_2026-06-02.html").exists()

    analytics = runner.invoke(
        etf_app,
        [
            "decision-journal",
            "analytics",
            "--as-of",
            "2026-06-02",
            "--journal-path",
            str(journal_path),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert analytics.exit_code == 0, analytics.output
    analytics_payload = json.loads(
        (output_dir / "decision_journal_analytics_2026-06-02.json").read_text(encoding="utf-8")
    )
    assert analytics_payload["decision_status_counts"]["continue_observation"] == 1

    proposal = runner.invoke(
        etf_app,
        [
            "decision-journal",
            "propose-state-updates",
            "--as-of",
            "2026-06-02",
            "--journal-path",
            str(journal_path),
            "--output-dir",
            str(proposal_dir),
        ],
    )
    assert proposal.exit_code == 0, proposal.output
    proposal_payload = json.loads(
        (proposal_dir / "decision_state_update_proposals_2026-06-02.json").read_text(
            encoding="utf-8"
        )
    )
    assert proposal_payload["state_mutation_performed"] is False
    assert proposal_payload["proposals"][0]["proposed_state_action"] == "continue_observation"

    validation = runner.invoke(
        etf_app,
        [
            "decision-journal",
            "validate",
            "--journal-path",
            str(journal_path),
            "--output-dir",
            str(validation_dir),
        ],
    )
    assert validation.exit_code == 0, validation.output
    validation_payload = json.loads(next(validation_dir.glob("*.json")).read_text(encoding="utf-8"))
    assert validation_payload["status"] == "PASS"


def test_reader_brief_decision_journal_summary_reads_latest_report(tmp_path: Path) -> None:
    weekly_path = _weekly_review_path(tmp_path)
    entry = build_decision_entry_from_weekly_review(
        weekly_review_path=weekly_path,
        action_item_id="weekly-action-1",
        human_decision="continue observation",
        decision_status="continue_observation",
        rationale="Forward window is not mature enough.",
        confidence=0.7,
        follow_up_task="Review next week.",
        linked_candidate="shadow_base_ai_growth",
        created_at=datetime(2026, 6, 2, tzinfo=UTC),
        decision_id="decision-reader-1",
    )
    journal = add_decision_entry(empty_decision_journal(), entry)
    report_payload = build_decision_journal_report(
        journal,
        as_of=date(2026, 6, 2),
        journal_path=tmp_path / "journal.json",
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )
    report_path = tmp_path / "decision_journal_2026-06-02.json"
    report_path.write_text(json.dumps(report_payload, indent=2, sort_keys=True), encoding="utf-8")

    summary = _etf_decision_journal_summary(
        {
            "reports": [
                {
                    "report_id": "etf_decision_journal_report",
                    "latest_artifact_path": str(report_path),
                }
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["entry_count"] == 1
    assert summary["follow_up_task_count"] == 1
    assert "observe_only=true" in summary["safety_status"]


def _entry(tmp_path: Path) -> dict[str, object]:
    return build_decision_entry(
        review_id="etf-weekly-review-2026-06-01",
        decision_id="decision-unit-1",
        review_date=date(2026, 6, 1),
        source_weekly_review=tmp_path / "weekly_review_2026-06-01.json",
        action_item_id="weekly-action-1",
        human_decision="continue observation",
        decision_status="continue_observation",
        rationale="Forward window is not mature enough.",
        confidence=0.7,
        follow_up_task="Review again next week.",
        linked_candidate="shadow_base_ai_growth",
        linked_report=tmp_path / "weekly_review_2026-06-01.md",
        created_at=datetime(2026, 6, 2, tzinfo=UTC),
    )


def _weekly_review_path(tmp_path: Path) -> Path:
    path = tmp_path / "weekly_review_2026-06-01.json"
    payload = {
        "schema_version": "etf_weekly_review_v1",
        "report_type": "etf_weekly_review",
        "review_id": "etf-weekly-review-2026-06-01",
        "review_end_date": "2026-06-01",
        "requested_date_range": {"start": "2026-05-26", "end": "2026-06-01"},
        "manual_review_actions": [
            {
                "action_id": "weekly-action-1",
                "action_type": "review_candidate",
                "priority": "medium",
                "source_module": "etf_forward_dashboard",
                "evidence": [
                    {
                        "source_module": "etf_forward_dashboard",
                        "source_report_path": "forward_dashboard_2026-06-01.json",
                        "source_metric": "candidate_summary_table",
                        "time_window": "2026-06-01",
                        "reason_code": "watch",
                    }
                ],
                "recommended_reason": "Review shadow candidate.",
                "requires_manual_review": True,
                "status": "open",
                "created_at": "2026-06-02T00:00:00+00:00",
            }
        ],
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path
