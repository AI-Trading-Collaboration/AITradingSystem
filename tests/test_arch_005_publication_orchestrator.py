"""DEVX-016 S3: the run engine sequences steps, resumes without repeating effects, gates push."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.platform.architecture.publication_journal import (
    PublicationJournal,
    PublicationJournalError,
)
from ai_trading_system.platform.architecture.publication_orchestrator import (
    OUTCOME_AWAITING_AUTHORIZATION,
    OUTCOME_COMPLETE,
    OUTCOME_NEEDS_REVIEW,
    OUTCOME_PAUSED,
    OUTCOME_STOPPED_FAILED,
    PublicationRunEngine,
    Step,
    StepAwaitingAuthorization,
    StepContext,
    StepFailed,
)

T0 = datetime(2026, 10, 7, 0, 0, tzinfo=UTC)


class Clock:
    """A fake monotonic clock: each step reads its own elapsed seconds from `advance`."""

    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def _engine(
    tmp_path: Path, steps: list[Step], clock: Clock | None = None
) -> tuple[PublicationRunEngine, PublicationJournal, StepContext]:
    journal = PublicationJournal(tmp_path / "run" / "journal.jsonl")
    context = StepContext(run_id="run-1")
    engine = PublicationRunEngine(
        steps=steps,
        journal=journal,
        context=context,
        monotonic=clock or Clock(),
        now=lambda: T0,
    )
    return engine, journal, context


def _recording_step(
    log: list[str], step_id: str, *, detail: Mapping[str, Any] | None = None, **kwargs: Any
) -> Step:
    def execute(context: StepContext) -> Mapping[str, Any]:
        log.append(step_id)
        return dict(detail or {})

    return Step(step_id=step_id, title=step_id, execute=execute, **kwargs)


def test_steps_run_in_order_and_facts_flow_forward(tmp_path: Path) -> None:
    log: list[str] = []

    def second(context: StepContext) -> Mapping[str, Any]:
        log.append("s2")
        return {"saw": context.fact("s1", "value")}

    steps = [
        _recording_step(log, "s1", detail={"value": 41}),
        Step(step_id="s2", title="second", execute=second),
        _recording_step(log, "s3"),
    ]
    engine, journal, _ = _engine(tmp_path, steps)
    summary = engine.run()
    assert log == ["s1", "s2", "s3"] and summary.outcome == OUTCOME_COMPLETE
    assert journal.replay().last_detail("s2")["saw"] == 41  # type: ignore[index]
    assert [row.status for row in summary.rows] == ["DONE", "DONE", "DONE"]
    again = engine.run()
    assert log == ["s1", "s2", "s3"] and again.outcome == OUTCOME_COMPLETE  # nothing re-executed
    assert summary.to_dict()["production_effect"] == "none"


def test_a_failed_step_stops_the_run_until_an_explicit_retry(tmp_path: Path) -> None:
    log: list[str] = []
    attempts = {"count": 0}

    def flaky(context: StepContext) -> Mapping[str, Any]:
        attempts["count"] += 1
        log.append("flaky")
        if attempts["count"] == 1:
            raise StepFailed("TEST_BOOM", "first attempt fails", {"exit_code": 2})
        return {"ok": True}

    steps = [
        _recording_step(log, "s1"),
        Step(step_id="flaky", title="flaky", execute=flaky),
        _recording_step(log, "s3"),
    ]
    engine, journal, _ = _engine(tmp_path, steps)
    summary = engine.run()
    assert summary.outcome == OUTCOME_STOPPED_FAILED and summary.stopped_step == "flaky"
    assert journal.replay().last_detail("flaky", status="FAILED")["code"] == "TEST_BOOM"  # type: ignore[index]
    assert log == ["s1", "flaky"]
    assert engine.run().outcome == OUTCOME_STOPPED_FAILED and log == ["s1", "flaky"]  # no retry
    assert engine.status().outcome == OUTCOME_STOPPED_FAILED
    retried = engine.run(retry="flaky")
    assert retried.outcome == OUTCOME_COMPLETE and log == ["s1", "flaky", "flaky", "s3"]


def _seed_started(journal: PublicationJournal, step_id: str) -> None:
    journal.append(step_id=step_id, status="STARTED", now=T0)


def test_a_crashed_read_only_step_is_executed_again(tmp_path: Path) -> None:
    log: list[str] = []
    engine, journal, _ = _engine(tmp_path, [_recording_step(log, "readonly")])
    _seed_started(journal, "readonly")
    assert engine.run().outcome == OUTCOME_COMPLETE and log == ["readonly"]


def test_a_crashed_mutating_step_is_probed_and_never_blindly_repeated(tmp_path: Path) -> None:
    log: list[str] = []
    probes: list[str] = []

    def proves_done(context: StepContext) -> Mapping[str, Any] | None:
        probes.append("probe")
        return {"main": "abc"}

    steps = [_recording_step(log, "ff_main", mutating=True, already_done=proves_done)]
    engine, journal, _ = _engine(tmp_path, steps)
    _seed_started(journal, "ff_main")
    summary = engine.run()
    assert summary.outcome == OUTCOME_COMPLETE and log == [] and probes == ["probe"]
    detail = journal.replay().last_detail("ff_main")
    assert detail is not None and detail["_recovered"] is True and detail["main"] == "abc"

    log2: list[str] = []
    unproven = [
        _recording_step(log2, "push", mutating=True, already_done=lambda context: None),
        _recording_step(log2, "after"),
    ]
    engine2, journal2, _ = _engine(tmp_path / "other", unproven)
    _seed_started(journal2, "push")
    stopped = engine2.run()
    assert stopped.outcome == OUTCOME_NEEDS_REVIEW and stopped.stopped_step == "push"
    assert log2 == []  # neither the push nor anything after it ran
    assert engine2.run().outcome == OUTCOME_NEEDS_REVIEW and log2 == []


def test_a_mutating_step_without_a_probe_stops_for_review_after_a_crash(tmp_path: Path) -> None:
    log: list[str] = []
    engine, journal, _ = _engine(tmp_path, [_recording_step(log, "push", mutating=True)])
    _seed_started(journal, "push")
    assert engine.run().outcome == OUTCOME_NEEDS_REVIEW and log == []


def test_an_explicit_retry_of_a_mutating_step_probes_first(tmp_path: Path) -> None:
    log: list[str] = []

    def execute(context: StepContext) -> Mapping[str, Any]:
        log.append("exec")
        raise StepFailed("TEST_BOOM", "fails")

    state = {"done": False}
    step = Step(
        step_id="m",
        title="m",
        execute=execute,
        mutating=True,
        already_done=lambda context: {"x": 1} if state["done"] else None,
    )
    engine, _, _ = _engine(tmp_path, [step])
    assert engine.run().outcome == OUTCOME_STOPPED_FAILED and log == ["exec"]
    state["done"] = True  # reality shows the effect happened after all
    assert engine.run(retry="m").outcome == OUTCOME_COMPLETE and log == ["exec"]


def test_the_authorization_gate_stops_everything_after_it_until_a_record_exists(
    tmp_path: Path,
) -> None:
    log: list[str] = []
    state = {"authorized": False}

    def gate(context: StepContext) -> Mapping[str, Any]:
        if not state["authorized"]:
            raise StepAwaitingAuthorization("no owner authorization record", {"candidate": "abc"})
        return {"authorized_candidate": "abc"}

    steps = [
        _recording_step(log, "before"),
        Step(step_id="gate", title="owner authorization", execute=gate),
        _recording_step(log, "push", mutating=True),
    ]
    engine, journal, _ = _engine(tmp_path, steps)
    summary = engine.run()
    assert summary.outcome == OUTCOME_AWAITING_AUTHORIZATION and log == ["before"]
    assert journal.replay().last_status("gate") == "AWAITING_AUTHORIZATION"
    assert engine.run().outcome == OUTCOME_AWAITING_AUTHORIZATION and log == ["before"]
    assert engine.status().outcome == OUTCOME_AWAITING_AUTHORIZATION
    state["authorized"] = True
    assert engine.run().outcome == OUTCOME_COMPLETE and log == ["before", "push"]


def test_slow_steps_are_flagged_against_their_baseline_and_never_fail_the_run(
    tmp_path: Path,
) -> None:
    clock = Clock()

    def timed(seconds: float) -> Callable[[StepContext], Mapping[str, Any]]:
        def execute(context: StepContext) -> Mapping[str, Any]:
            clock.advance(seconds)
            return {}

        return execute

    steps = [
        Step(step_id="fast", title="fast", execute=timed(100), baseline_seconds=100),
        Step(step_id="edge", title="edge", execute=timed(125), baseline_seconds=100),  # == 1.25x
        Step(step_id="slow", title="slow", execute=timed(751), baseline_seconds=500),
        Step(step_id="nobaseline", title="n", execute=timed(9999)),
    ]
    engine, _, _ = _engine(tmp_path, steps, clock)
    summary = engine.run()
    assert summary.outcome == OUTCOME_COMPLETE
    assert summary.slow_steps == ("slow",)
    slow_row = next(row for row in summary.rows if row.step_id == "slow")
    assert slow_row.elapsed_seconds == 751 and slow_row.baseline_seconds == 500 and slow_row.slow


def test_until_pauses_after_the_named_step_and_unknown_names_are_refused(tmp_path: Path) -> None:
    log: list[str] = []
    steps = [_recording_step(log, "a"), _recording_step(log, "b"), _recording_step(log, "c")]
    engine, _, _ = _engine(tmp_path, steps)
    paused = engine.run(until="b")
    assert paused.outcome == OUTCOME_PAUSED and paused.stopped_step == "b" and log == ["a", "b"]
    assert engine.run().outcome == OUTCOME_COMPLETE and log == ["a", "b", "c"]
    with pytest.raises(PublicationJournalError) as raised:
        engine.run(until="nope")
    assert raised.value.code == "PUBLICATION_RUN_STEP_UNKNOWN"
    with pytest.raises(PublicationJournalError) as raised:
        engine.run(retry="nope")
    assert raised.value.code == "PUBLICATION_RUN_STEP_UNKNOWN"


def test_duplicate_step_ids_and_a_tampered_journal_are_refused(tmp_path: Path) -> None:
    log: list[str] = []
    with pytest.raises(PublicationJournalError) as raised:
        _engine(tmp_path, [_recording_step(log, "a"), _recording_step(log, "a")])
    assert raised.value.code == "PUBLICATION_RUN_STEP_DUPLICATE"

    engine, journal, _ = _engine(
        tmp_path / "t", [_recording_step(log, "a"), _recording_step(log, "b")]
    )
    engine.run(until="a")
    lines = journal.path.read_text(encoding="utf-8").split("\n")
    journal.path.write_text(
        lines[0].replace('"step_id":"a"', '"step_id":"z"') + "\n", encoding="utf-8"
    )
    with pytest.raises(PublicationJournalError) as raised:
        engine.run()
    assert raised.value.code == "PUBLICATION_RUN_JOURNAL_INVALID"
