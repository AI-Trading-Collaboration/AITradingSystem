from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ai_trading_system.etf_portfolio.decision_journal import (
    DECISION_JOURNAL_SCHEMA_VERSION,
    DecisionJournalError,
    build_decision_entry,
    empty_decision_journal,
    validate_decision_entry,
    validate_decision_journal_schema,
    write_decision_journal,
)


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
