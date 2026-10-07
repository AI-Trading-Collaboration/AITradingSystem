"""DEVX-016 S3: the publication run engine (steps, resume, authorization gate, duration check).

The engine only sequences steps and records what each one did in the hash-chained run journal. It
never decides anything about the repository itself: every step is a thin wrapper over an existing
reviewed command (fence, generators, validation driver, local publication, push), and the fence
transaction plus its lease remain the authority for every external effect.

Resume semantics (the property the single-command flow exists for):
- a DONE step is never executed again;
- a step left STARTED by a crash is re-executed only when it is read-only; a step with an external
  effect is first PROBED against reality (`already_done`) and is recorded DONE only if the probe
  proves the effect, otherwise the run stops as NEEDS_REVIEW instead of repeating it;
- a FAILED step stops the run until the operator asks for an explicit retry;
- the authorization gate is a step: without a matching owner authorization record the run stops at
  AWAITING_AUTHORIZATION and nothing after it can run.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ai_trading_system.platform.architecture.publication_journal import (
    JournalReplay,
    PublicationJournal,
    PublicationJournalError,
)

# Reporting threshold only: a step slower than baseline x this factor is flagged SLOW_STEP so the
# operator sees the excess and analyses it (owner instruction of 2026-10-07); it never fails a run.
DEFAULT_SLOW_FACTOR = 1.25

OUTCOME_COMPLETE = "COMPLETE"
OUTCOME_STOPPED_FAILED = "STOPPED_FAILED"
OUTCOME_AWAITING_AUTHORIZATION = "AWAITING_AUTHORIZATION"
OUTCOME_NEEDS_REVIEW = "NEEDS_REVIEW"
OUTCOME_PAUSED = "PAUSED_AT_REQUESTED_STEP"


class StepFailed(RuntimeError):
    def __init__(self, code: str, message: str, detail: Mapping[str, Any] | None = None) -> None:
        self.code = code
        self.message = message
        self.detail = dict(detail or {})
        super().__init__(f"{code}: {message}")


class StepAwaitingAuthorization(RuntimeError):
    def __init__(self, message: str, detail: Mapping[str, Any] | None = None) -> None:
        self.message = message
        self.detail = dict(detail or {})
        super().__init__(message)


@dataclass
class StepContext:
    run_id: str
    facts: dict[str, Any] = field(default_factory=dict)
    services: dict[str, Any] = field(default_factory=dict)

    def fact(self, step_id: str, key: str) -> Any:
        try:
            return self.facts[step_id][key]
        except KeyError as exc:
            raise StepFailed("PUBLICATION_RUN_FACT_MISSING", f"{step_id}.{key}") from exc


@dataclass(frozen=True)
class Step:
    step_id: str
    title: str
    execute: Callable[[StepContext], Mapping[str, Any]]
    baseline_seconds: float | None = None
    mutating: bool = False
    # For a mutating step: return the facts when reality already shows the effect, else None.
    already_done: Callable[[StepContext], Mapping[str, Any] | None] | None = None


@dataclass(frozen=True)
class StepRow:
    step_id: str
    title: str
    status: str
    elapsed_seconds: float | None
    baseline_seconds: float | None
    slow: bool
    detail: Mapping[str, Any]


@dataclass(frozen=True)
class RunSummary:
    outcome: str
    rows: tuple[StepRow, ...]
    stopped_step: str | None
    next_action: str
    journal_head_sha256: str | None

    @property
    def slow_steps(self) -> tuple[str, ...]:
        return tuple(row.step_id for row in self.rows if row.slow)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "devx_016_publication_run_summary.v1",
            "outcome": self.outcome,
            "stopped_step": self.stopped_step,
            "next_action": self.next_action,
            "journal_head_sha256": self.journal_head_sha256,
            "slow_steps": list(self.slow_steps),
            "steps": [
                {
                    "step_id": row.step_id,
                    "title": row.title,
                    "status": row.status,
                    "elapsed_seconds": row.elapsed_seconds,
                    "baseline_seconds": row.baseline_seconds,
                    "slow": row.slow,
                }
                for row in self.rows
            ],
            "production_effect": "none",
            "broker_action": "none",
        }


class PublicationRunEngine:
    def __init__(
        self,
        *,
        steps: Sequence[Step],
        journal: PublicationJournal,
        context: StepContext,
        monotonic: Callable[[], float],
        now: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
        slow_factor: float = DEFAULT_SLOW_FACTOR,
    ) -> None:
        ids = [step.step_id for step in steps]
        if len(set(ids)) != len(ids):
            raise PublicationJournalError("PUBLICATION_RUN_STEP_DUPLICATE", ",".join(ids))
        self.steps = tuple(steps)
        self.journal = journal
        self.context = context
        self.monotonic = monotonic
        self.now = now
        self.slow_factor = slow_factor

    def status(self) -> RunSummary:
        replay = self._replay()
        return self._summary(replay, outcome=None, stopped=None)

    def run(self, *, until: str | None = None, retry: str | None = None) -> RunSummary:
        """Execute from the first step that is not DONE. `until` stops after that step is DONE;
        `retry` names a FAILED step the operator explicitly wants executed again."""
        known = {step.step_id for step in self.steps}
        for name in (until, retry):
            if name is not None and name not in known:
                raise PublicationJournalError("PUBLICATION_RUN_STEP_UNKNOWN", name)
        for step in self.steps:
            replay = self._replay()
            self._load_facts(replay)
            last = replay.last_status(step.step_id)
            if last == "DONE":
                if until == step.step_id:
                    return self._summary(self._replay(), OUTCOME_PAUSED, step.step_id)
                continue
            if last == "NEEDS_REVIEW":
                return self._summary(replay, OUTCOME_NEEDS_REVIEW, step.step_id)
            if last == "FAILED" and retry != step.step_id:
                return self._summary(replay, OUTCOME_STOPPED_FAILED, step.step_id)
            outcome = self._execute(step, interrupted=last in {"STARTED", "FAILED"})
            if outcome is not None:
                return self._summary(self._replay(), outcome, step.step_id)
            if until == step.step_id:
                return self._summary(self._replay(), OUTCOME_PAUSED, step.step_id)
        return self._summary(self._replay(), OUTCOME_COMPLETE, None)

    def _execute(self, step: Step, *, interrupted: bool) -> str | None:
        if interrupted and step.mutating:
            # A crash (or an explicit retry) may have left an external effect behind: prove it
            # before anything is repeated.
            probe = step.already_done(self.context) if step.already_done else None
            if probe is not None:
                self._record_done(step, probe, elapsed=0.0, recovered=True)
                return None
            if self._replay().last_status(step.step_id) == "STARTED":
                self.journal.append(
                    step_id=step.step_id,
                    status="NEEDS_REVIEW",
                    detail={"reason": "started_and_not_proven_done", "mutating": True},
                    now=self.now(),
                )
                return OUTCOME_NEEDS_REVIEW
        self.journal.append(step_id=step.step_id, status="STARTED", now=self.now())
        started = self.monotonic()
        try:
            detail = step.execute(self.context)
        except StepAwaitingAuthorization as waiting:
            self.journal.append(
                step_id=step.step_id,
                status="AWAITING_AUTHORIZATION",
                detail={"message": waiting.message, **waiting.detail},
                now=self.now(),
            )
            return OUTCOME_AWAITING_AUTHORIZATION
        except StepFailed as failure:
            self.journal.append(
                step_id=step.step_id,
                status="FAILED",
                detail={"code": failure.code, "message": failure.message, **failure.detail},
                now=self.now(),
            )
            return OUTCOME_STOPPED_FAILED
        self._record_done(step, detail, elapsed=self.monotonic() - started, recovered=False)
        return None

    def _record_done(
        self, step: Step, detail: Mapping[str, Any], *, elapsed: float, recovered: bool
    ) -> None:
        baseline = step.baseline_seconds
        slow = bool(baseline is not None and elapsed > baseline * self.slow_factor)
        self.journal.append(
            step_id=step.step_id,
            status="DONE",
            detail={
                **detail,
                "_elapsed_seconds": round(elapsed, 3),
                "_baseline_seconds": baseline,
                "_slow": slow,
                "_recovered": recovered,
                "_recorded_at": self.now().isoformat(),
            },
            now=self.now(),
        )

    def _replay(self) -> JournalReplay:
        replay = self.journal.replay()
        if replay.status != "PASS":
            raise PublicationJournalError(
                "PUBLICATION_RUN_JOURNAL_INVALID", ",".join(replay.issues)
            )
        return replay

    def _load_facts(self, replay: JournalReplay) -> None:
        for step in self.steps:
            detail = replay.last_detail(step.step_id)
            if detail is not None:
                self.context.facts[step.step_id] = dict(detail)

    def _summary(
        self, replay: JournalReplay, outcome: str | None, stopped: str | None
    ) -> RunSummary:
        rows: list[StepRow] = []
        next_step: Step | None = None
        for step in self.steps:
            status = replay.last_status(step.step_id) or "PENDING"
            done_detail = replay.last_detail(step.step_id) or {}
            rows.append(
                StepRow(
                    step_id=step.step_id,
                    title=step.title,
                    status=status,
                    elapsed_seconds=done_detail.get("_elapsed_seconds"),
                    baseline_seconds=step.baseline_seconds,
                    slow=bool(done_detail.get("_slow", False)),
                    detail=done_detail,
                )
            )
            if next_step is None and status != "DONE":
                next_step = step
        if outcome is None:
            outcome = OUTCOME_COMPLETE if next_step is None else _status_outcome(rows, next_step)
            stopped = None if next_step is None else next_step.step_id
        return RunSummary(
            outcome=outcome,
            rows=tuple(rows),
            stopped_step=stopped,
            next_action=_next_action(outcome, stopped, _stopped_detail(replay, stopped)),
            journal_head_sha256=replay.head_sha256,
        )


def _stopped_detail(replay: JournalReplay, stopped: str | None) -> Mapping[str, Any] | None:
    status = replay.last_status(stopped) if stopped else None
    return replay.last_detail(stopped, status=status) if stopped and status else None


def _status_outcome(rows: Sequence[StepRow], next_step: Step) -> str:
    row = next(item for item in rows if item.step_id == next_step.step_id)
    return {
        "FAILED": OUTCOME_STOPPED_FAILED,
        "NEEDS_REVIEW": OUTCOME_NEEDS_REVIEW,
        "AWAITING_AUTHORIZATION": OUTCOME_AWAITING_AUTHORIZATION,
    }.get(row.status, "NOT_STARTED" if row.status == "PENDING" else "INTERRUPTED")


def _next_action(outcome: str, stopped: str | None, detail: Mapping[str, Any] | None) -> str:
    if outcome == OUTCOME_COMPLETE:
        return "无：所有步骤均已 DONE"
    if outcome == OUTCOME_AWAITING_AUTHORIZATION:
        waiting = detail or {}
        message = str(waiting.get("message") or "需要 owner 授权记录")
        command = waiting.get("owner_command")
        owner = f"；请 owner 在自己的终端执行 `{command}`，然后 resume" if command else ""
        return f"{stopped}：{message}{owner}；其后的步骤不可能执行"
    if outcome == OUTCOME_STOPPED_FAILED:
        return f"检查 {stopped} 的证据；只有显式 --retry 该步骤才会继续"
    if outcome == OUTCOME_NEEDS_REVIEW:
        return f"{stopped} 曾被中断且现实无法证明它已完成：请人工核对"
    if outcome == OUTCOME_PAUSED:
        return f"已在 {stopped} 之后暂停；再次运行以继续"
    return f"运行以从 {stopped} 继续"
