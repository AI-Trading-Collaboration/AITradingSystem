"""DEVX-016 S3: the run journal is append-only, hash-chained and fails closed when tampered."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture.publication_journal import (
    PublicationJournal,
    PublicationJournalError,
    PublicationRunLock,
)

T0 = datetime(2026, 10, 7, 0, 0, tzinfo=UTC)


def _journal(tmp_path: Path) -> PublicationJournal:
    return PublicationJournal(tmp_path / "run" / "journal.jsonl")


def _lines(journal: PublicationJournal) -> list[str]:
    return [line for line in journal.path.read_text(encoding="utf-8").split("\n") if line]


def test_an_absent_journal_replays_empty_and_pass(tmp_path: Path) -> None:
    replay = _journal(tmp_path).replay()
    assert replay.status == "PASS" and replay.entries == () and replay.head_sha256 is None


def test_entries_chain_by_hash_and_expose_the_last_status_and_detail(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    first = journal.append(step_id="P10.prep_acquire", status="STARTED", now=T0)
    second = journal.append(
        step_id="P10.prep_acquire",
        status="DONE",
        detail={"transaction": "t1", "paths": ("a", "b"), "nested": {"k": ("x",)}},
        now=T0,
    )
    third = journal.append(
        step_id="F20.formal_acquire", status="FAILED", detail={"exit": 2}, now=T0
    )
    assert (first.sequence, second.sequence, third.sequence) == (1, 2, 3)
    assert first.previous_sha256 is None and second.previous_sha256 == first.entry_sha256
    assert third.previous_sha256 == second.entry_sha256
    replay = journal.replay()
    assert replay.status == "PASS" and replay.head_sha256 == third.entry_sha256
    assert replay.last_status("P10.prep_acquire") == "DONE"
    assert replay.last_status("F20.formal_acquire") == "FAILED"
    assert replay.last_status("never.ran") is None
    assert replay.last_detail("P10.prep_acquire") == {
        "transaction": "t1",
        "paths": ["a", "b"],
        "nested": {"k": ["x"]},
    }
    assert replay.last_detail("F20.formal_acquire") is None  # no DONE entry for that step


def test_a_changed_entry_a_removed_line_and_a_reordering_all_fail_replay(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    for index in range(4):
        journal.append(step_id=f"S{index}", status="DONE", detail={"i": index}, now=T0)
    original = _lines(journal)

    def write(lines: list[str]) -> None:
        journal.path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="")

    changed = json.loads(original[1])
    changed["detail"]["i"] = 99
    write([original[0], json.dumps(changed), *original[2:]])
    assert journal.replay().status == "FAIL"
    assert any(issue.startswith("JOURNAL_ENTRY_HASH") for issue in journal.replay().issues)

    write([original[0], *original[2:]])  # middle entry removed
    assert any(issue.startswith("JOURNAL_SEQUENCE") for issue in journal.replay().issues)

    write([original[0], original[2], original[1], original[3]])  # reordered
    assert journal.replay().status == "FAIL"

    write(original)
    assert journal.replay().status == "PASS"


def test_a_failed_journal_refuses_further_appends(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    journal.append(step_id="S0", status="DONE", now=T0)
    journal.path.write_text(_lines(journal)[0] + "\n{broken\n", encoding="utf-8", newline="")
    with pytest.raises(PublicationJournalError) as raised:
        journal.append(step_id="S1", status="DONE", now=T0)
    assert raised.value.code == "PUBLICATION_RUN_JOURNAL_INVALID"


def test_a_truncated_tail_is_a_valid_shorter_history_so_resume_must_reprobe_reality(
    tmp_path: Path,
) -> None:
    """Documented limit: the chain cannot see a dropped TAIL. Steps with external effects are
    therefore idempotent probes (is main already the candidate? is origin already there?)."""
    journal = _journal(tmp_path)
    for index in range(3):
        journal.append(step_id=f"S{index}", status="DONE", now=T0)
    journal.path.write_text("\n".join(_lines(journal)[:2]) + "\n", encoding="utf-8", newline="")
    replay = journal.replay()
    assert replay.status == "PASS" and len(replay.entries) == 2


@pytest.mark.parametrize(
    "step_id,status,code",
    [
        ("bad id", "DONE", "PUBLICATION_RUN_STEP_ID_INVALID"),
        ("", "DONE", "PUBLICATION_RUN_STEP_ID_INVALID"),
        ("S1", "WINNING", "PUBLICATION_RUN_STATUS_INVALID"),
    ],
)
def test_step_ids_and_statuses_are_validated(
    tmp_path: Path, step_id: str, status: str, code: str
) -> None:
    with pytest.raises(PublicationJournalError) as raised:
        _journal(tmp_path).append(step_id=step_id, status=status, now=T0)
    assert raised.value.code == code


def test_the_run_lock_admits_one_orchestrator_and_never_takes_over_silently(tmp_path: Path) -> None:
    lock = PublicationRunLock(tmp_path / "run" / "run.lock")
    lock.acquire(owner="orchestrator-a", now=T0)
    with pytest.raises(PublicationJournalError) as raised:
        PublicationRunLock(lock.path).acquire(owner="orchestrator-b", now=T0)
    assert raised.value.code == "PUBLICATION_RUN_LOCKED"
    assert "2026-10-07T00:00:00+00:00" in raised.value.message  # when it was taken
    PublicationRunLock(lock.path).acquire(owner="orchestrator-b", takeover=True, now=T0)
    held = json.loads(lock.path.read_text(encoding="utf-8"))
    assert held["owner"] == "orchestrator-b"
    assert held["production_effect"] == "none" and held["broker_action"] == "none"
    lock.release()
    lock.release()  # idempotent
    assert not lock.path.exists()
