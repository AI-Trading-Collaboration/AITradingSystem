from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Final

_SHA256_LENGTH: Final = 64
_TREND_PRODUCER_VERSION: Final = "qqq_underlying_trend_break_v1.synthetic.1"
_EVENT_TAXONOMY: Final = {
    "FEDERAL_RESERVE": frozenset({"FOMC_RATE_DECISION"}),
    "BLS": frozenset({"CPI", "NONFARM_PAYROLLS"}),
    "BEA": frozenset({"PCE_PRICE_INDEX", "GDP_ADVANCE_ESTIMATE"}),
}


class VetoOutcome(StrEnum):
    CLEAR = "CLEAR"
    VETO_ACTIVE = "VETO_ACTIVE"
    INSUFFICIENT = "INSUFFICIENT"
    INVALID = "INVALID"


class TrendState(StrEnum):
    UNKNOWN = "UNKNOWN"
    CLEAR = "CLEAR"
    VETO_ACTIVE = "VETO_ACTIVE"


class RevisionAction(StrEnum):
    UPSERT = "UPSERT"
    RESCHEDULE = "RESCHEDULE"
    CANCEL = "CANCEL"


@dataclass(frozen=True)
class ResearchClock:
    decision_as_of: datetime
    next_action_cutoff: datetime


@dataclass(frozen=True)
class VetoDecision:
    outcome: VetoOutcome
    veto_active: bool | None
    reason_code: str
    metrics: tuple[tuple[str, float], ...] = ()


@dataclass(frozen=True)
class PriceWindowInput:
    ticker: str
    sma200_closes: tuple[float | None, ...]
    drawdown63_closes: tuple[float | None, ...]
    available_at: datetime


@dataclass(frozen=True)
class VolatilityWindowInput:
    qqq_closes_21: tuple[float | None, ...]
    vix_levels_252: tuple[float | None, ...]
    qqq_available_at: datetime
    vix_available_at: datetime


@dataclass(frozen=True)
class EventRevision:
    authority: str
    event_type: str
    stable_event_key: str
    scheduled_for: datetime
    published_at: datetime
    revision_id: str
    revision_action: RevisionAction
    source_identity: str
    source_snapshot_sha256: str


@dataclass(frozen=True)
class EventCoverageReceipt:
    authority: str
    coverage_through: datetime
    published_at: datetime
    source_snapshot_sha256: str


@dataclass(frozen=True)
class ScheduledEventInput:
    revisions: tuple[EventRevision, ...]
    coverage_receipts: tuple[EventCoverageReceipt, ...]


@dataclass(frozen=True)
class TrendCheckpoint:
    state: TrendState
    recovery_streak: int
    producer_version: str
    source_inventory_sha256: str
    state_checkpoint_sha256: str


@dataclass(frozen=True)
class TrendStep:
    decision: VetoDecision
    next_checkpoint: TrendCheckpoint


@dataclass(frozen=True)
class _PriceMetrics:
    close: float
    sma200: float
    drawdown63: Decimal


def _terminal(outcome: VetoOutcome, reason_code: str) -> VetoDecision:
    return VetoDecision(outcome=outcome, veto_active=None, reason_code=reason_code)


def _boolean_decision(
    active: bool,
    *,
    reason_code: str,
    metrics: tuple[tuple[str, float], ...],
) -> VetoDecision:
    return VetoDecision(
        outcome=VetoOutcome.VETO_ACTIVE if active else VetoOutcome.CLEAR,
        veto_active=active,
        reason_code=reason_code,
        metrics=metrics,
    )


def _aware(value: datetime) -> bool:
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() is not None
    )


def _clock_terminal(
    clock: ResearchClock,
    available_at: Iterable[datetime],
) -> VetoDecision | None:
    if not _aware(clock.decision_as_of) or not _aware(clock.next_action_cutoff):
        return _terminal(VetoOutcome.INVALID, "CLOCK_TIMEZONE_REQUIRED")
    if clock.decision_as_of >= clock.next_action_cutoff:
        return _terminal(VetoOutcome.INVALID, "CLOCK_ACTION_CUTOFF_NOT_AFTER_DECISION")
    for timestamp in available_at:
        if not _aware(timestamp):
            return _terminal(VetoOutcome.INVALID, "AVAILABLE_AT_TIMEZONE_REQUIRED")
        if timestamp > clock.decision_as_of:
            return _terminal(VetoOutcome.INSUFFICIENT, "INPUT_NOT_AVAILABLE_AT_DECISION")
    return None


def _valid_positive(value: float | None) -> bool:
    return (
        value is not None
        and not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and float(value) > 0.0
    )


def _qualified_window(
    values: tuple[float | None, ...],
    *,
    expected: int,
    label: str,
) -> tuple[tuple[float, ...] | None, VetoDecision | None]:
    if len(values) < expected or any(item is None for item in values):
        return None, _terminal(VetoOutcome.INSUFFICIENT, f"{label}_WINDOW_INSUFFICIENT")
    if len(values) != expected:
        return None, _terminal(VetoOutcome.INVALID, f"{label}_WINDOW_LENGTH_INVALID")
    if not all(_valid_positive(item) for item in values):
        return None, _terminal(VetoOutcome.INVALID, f"{label}_VALUE_INVALID")
    return tuple(float(item) for item in values if item is not None), None


def _price_metrics(
    inputs: PriceWindowInput,
    *,
    expected_ticker: str,
    clock: ResearchClock,
) -> tuple[_PriceMetrics | None, VetoDecision | None]:
    timing = _clock_terminal(clock, (inputs.available_at,))
    if timing is not None:
        return None, timing
    if inputs.ticker != expected_ticker:
        return None, _terminal(VetoOutcome.INVALID, "PRICE_TICKER_INVALID")
    sma_values, sma_terminal = _qualified_window(
        inputs.sma200_closes, expected=200, label=f"{expected_ticker}_SMA200"
    )
    drawdown_values, drawdown_terminal = _qualified_window(
        inputs.drawdown63_closes, expected=63, label=f"{expected_ticker}_DRAWDOWN63"
    )
    if sma_terminal is not None:
        return None, sma_terminal
    if drawdown_terminal is not None:
        return None, drawdown_terminal
    assert sma_values is not None and drawdown_values is not None
    if sma_values[-1] != drawdown_values[-1]:
        return None, _terminal(VetoOutcome.INVALID, "PRICE_CURRENT_SESSION_MISMATCH")
    close = sma_values[-1]
    reference = max(drawdown_values)
    return (
        _PriceMetrics(
            close=close,
            sma200=statistics.fmean(sma_values),
            drawdown63=Decimal(str(close)) / Decimal(str(reference)) - Decimal(1),
        ),
        None,
    )


def evaluate_broad_market_risk_off(
    inputs: PriceWindowInput,
    *,
    clock: ResearchClock,
) -> VetoDecision:
    metrics, terminal = _price_metrics(inputs, expected_ticker="SPY", clock=clock)
    if terminal is not None:
        return terminal
    assert metrics is not None
    active = metrics.close < metrics.sma200 or metrics.drawdown63 <= Decimal("-0.10")
    return _boolean_decision(
        active,
        reason_code="BROAD_MARKET_EXACT_FORMULA_EVALUATED",
        metrics=(
            ("close", metrics.close),
            ("sma200", metrics.sma200),
            ("drawdown63", float(metrics.drawdown63)),
        ),
    )


def _vix_average_rank_percentile(values: tuple[float, ...]) -> float:
    current = values[-1]
    less = sum(item < current for item in values)
    equal = sum(item == current for item in values)
    average_one_based_rank = less + (equal + 1.0) / 2.0
    return average_one_based_rank / len(values)


def _annualized_rv20(closes: tuple[float, ...]) -> float:
    returns = tuple(
        current / prior - 1.0
        for prior, current in zip(closes[:-1], closes[1:], strict=True)
    )
    return statistics.stdev(returns) * math.sqrt(252.0)


def evaluate_realized_volatility_veto(
    inputs: VolatilityWindowInput,
    *,
    clock: ResearchClock,
) -> VetoDecision:
    timing = _clock_terminal(clock, (inputs.qqq_available_at, inputs.vix_available_at))
    if timing is not None:
        return timing
    qqq, qqq_terminal = _qualified_window(
        inputs.qqq_closes_21, expected=21, label="QQQ_RV20"
    )
    vix, vix_terminal = _qualified_window(
        inputs.vix_levels_252, expected=252, label="VIX_PERCENTILE252"
    )
    if qqq_terminal is not None:
        return qqq_terminal
    if vix_terminal is not None:
        return vix_terminal
    assert qqq is not None and vix is not None
    percentile = _vix_average_rank_percentile(vix)
    annualized_rv = _annualized_rv20(qqq)
    active = percentile >= 0.75 or annualized_rv > 0.25
    return _boolean_decision(
        active,
        reason_code="VOLATILITY_EXACT_FORMULA_EVALUATED",
        metrics=(("vix_percentile252", percentile), ("qqq_annualized_rv20", annualized_rv)),
    )


def _valid_sha(value: str) -> bool:
    return (
        isinstance(value, str)
        and len(value) == _SHA256_LENGTH
        and all(char in "0123456789abcdef" for char in value)
    )


def _event_row_invalid(row: EventRevision) -> bool:
    return (
        row.authority not in _EVENT_TAXONOMY
        or row.event_type not in _EVENT_TAXONOMY.get(row.authority, frozenset())
        or not row.stable_event_key
        or not row.revision_id
        or not row.source_identity
        or not _valid_sha(row.source_snapshot_sha256)
        or not _aware(row.scheduled_for)
        or not _aware(row.published_at)
        or not isinstance(row.revision_action, RevisionAction)
    )


def evaluate_scheduled_event_risk(
    inputs: ScheduledEventInput,
    *,
    clock: ResearchClock,
) -> VetoDecision:
    timing = _clock_terminal(clock, ())
    if timing is not None:
        return timing
    receipts: dict[str, EventCoverageReceipt] = {}
    for receipt in inputs.coverage_receipts:
        if (
            receipt.authority not in _EVENT_TAXONOMY
            or receipt.authority in receipts
            or not _aware(receipt.coverage_through)
            or not _aware(receipt.published_at)
            or not _valid_sha(receipt.source_snapshot_sha256)
        ):
            return _terminal(VetoOutcome.INVALID, "EVENT_COVERAGE_RECEIPT_INVALID")
        if receipt.published_at > clock.decision_as_of:
            return _terminal(VetoOutcome.INSUFFICIENT, "EVENT_COVERAGE_NOT_AVAILABLE")
        receipts[receipt.authority] = receipt
    if set(receipts) != set(_EVENT_TAXONOMY):
        return _terminal(VetoOutcome.INSUFFICIENT, "EVENT_COVERAGE_AUTHORITY_INCOMPLETE")
    if any(
        receipt.coverage_through < clock.next_action_cutoff for receipt in receipts.values()
    ):
        return _terminal(VetoOutcome.INSUFFICIENT, "EVENT_COVERAGE_HORIZON_INSUFFICIENT")

    eligible_by_key: dict[str, list[EventRevision]] = {}
    for row in inputs.revisions:
        if _event_row_invalid(row):
            return _terminal(VetoOutcome.INVALID, "EVENT_REVISION_INVALID")
        if row.published_at <= clock.decision_as_of:
            eligible_by_key.setdefault(row.stable_event_key, []).append(row)
    admitted_count = 0
    for rows in eligible_by_key.values():
        seen_published: set[datetime] = set()
        for row in rows:
            if row.published_at in seen_published:
                return _terminal(VetoOutcome.INVALID, "EVENT_SAME_PUBLISHED_AT_CONFLICT")
            seen_published.add(row.published_at)
        latest = max(rows, key=lambda row: (row.published_at, row.revision_id))
        if latest.revision_action is RevisionAction.CANCEL:
            continue
        if clock.decision_as_of < latest.scheduled_for <= clock.next_action_cutoff:
            admitted_count += 1
    return _boolean_decision(
        admitted_count >= 1,
        reason_code="SCHEDULED_EVENT_EXACT_PIT_FORMULA_EVALUATED",
        metrics=(("admitted_event_count", float(admitted_count)),),
    )


def _checkpoint_valid(checkpoint: TrendCheckpoint) -> bool:
    return (
        isinstance(checkpoint.state, TrendState)
        and checkpoint.recovery_streak in (0, 1)
        and not (checkpoint.state is TrendState.CLEAR and checkpoint.recovery_streak != 0)
        and checkpoint.producer_version == _TREND_PRODUCER_VERSION
        and _valid_sha(checkpoint.source_inventory_sha256)
        and _valid_sha(checkpoint.state_checkpoint_sha256)
    )


def _next_checkpoint(
    *,
    state: TrendState,
    recovery_streak: int,
    source_inventory_sha256: str,
    clock: ResearchClock,
    marker: str,
) -> TrendCheckpoint:
    payload = {
        "decision_as_of": clock.decision_as_of.isoformat(),
        "marker": marker,
        "producer_version": _TREND_PRODUCER_VERSION,
        "recovery_streak": recovery_streak,
        "source_inventory_sha256": source_inventory_sha256,
        "state": state.value,
    }
    checkpoint_sha = hashlib.sha256(
        (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    ).hexdigest()
    return TrendCheckpoint(
        state=state,
        recovery_streak=recovery_streak,
        producer_version=_TREND_PRODUCER_VERSION,
        source_inventory_sha256=source_inventory_sha256,
        state_checkpoint_sha256=checkpoint_sha,
    )


def initial_trend_checkpoint(*, source_inventory_sha256: str) -> TrendCheckpoint:
    if not _valid_sha(source_inventory_sha256):
        raise ValueError("source_inventory_sha256 must be lowercase SHA-256")
    seed = hashlib.sha256(
        f"{_TREND_PRODUCER_VERSION}:{source_inventory_sha256}:UNKNOWN:0\n".encode()
    ).hexdigest()
    return TrendCheckpoint(
        state=TrendState.UNKNOWN,
        recovery_streak=0,
        producer_version=_TREND_PRODUCER_VERSION,
        source_inventory_sha256=source_inventory_sha256,
        state_checkpoint_sha256=seed,
    )


def evaluate_underlying_trend_break(
    inputs: PriceWindowInput,
    *,
    clock: ResearchClock,
    checkpoint: TrendCheckpoint,
) -> TrendStep:
    if not _checkpoint_valid(checkpoint):
        invalid_checkpoint = _next_checkpoint(
            state=TrendState.UNKNOWN,
            recovery_streak=0,
            source_inventory_sha256=(
                checkpoint.source_inventory_sha256
                if _valid_sha(checkpoint.source_inventory_sha256)
                else "0" * _SHA256_LENGTH
            ),
            clock=clock,
            marker="CHECKPOINT_INVALID",
        )
        return TrendStep(
            decision=_terminal(VetoOutcome.INVALID, "TREND_CHECKPOINT_INVALID"),
            next_checkpoint=invalid_checkpoint,
        )
    metrics, terminal = _price_metrics(inputs, expected_ticker="QQQ", clock=clock)
    if terminal is not None:
        reset = _next_checkpoint(
            state=TrendState.UNKNOWN,
            recovery_streak=0,
            source_inventory_sha256=checkpoint.source_inventory_sha256,
            clock=clock,
            marker=terminal.reason_code,
        )
        return TrendStep(decision=terminal, next_checkpoint=reset)
    assert metrics is not None
    entry = metrics.close < metrics.sma200 and metrics.drawdown63 <= Decimal("-0.12")
    recovery = metrics.close >= metrics.sma200
    next_state = checkpoint.state
    next_streak = 0
    if checkpoint.state is TrendState.VETO_ACTIVE:
        if recovery:
            candidate_streak = checkpoint.recovery_streak + 1
            if candidate_streak >= 2:
                next_state = TrendState.CLEAR
            else:
                next_state = TrendState.VETO_ACTIVE
                next_streak = candidate_streak
        else:
            next_state = TrendState.VETO_ACTIVE
    elif checkpoint.state is TrendState.CLEAR:
        next_state = TrendState.VETO_ACTIVE if entry else TrendState.CLEAR
    elif entry:
        next_state = TrendState.VETO_ACTIVE
    elif recovery:
        candidate_streak = checkpoint.recovery_streak + 1
        if candidate_streak >= 2:
            next_state = TrendState.CLEAR
        else:
            next_state = TrendState.UNKNOWN
            next_streak = candidate_streak
    else:
        next_state = TrendState.UNKNOWN

    next_checkpoint = _next_checkpoint(
        state=next_state,
        recovery_streak=next_streak,
        source_inventory_sha256=checkpoint.source_inventory_sha256,
        clock=clock,
        marker=(
            f"close={metrics.close:.17g};sma200={metrics.sma200:.17g};"
            f"drawdown63={metrics.drawdown63}"
        ),
    )
    metric_values = (
        ("close", metrics.close),
        ("sma200", metrics.sma200),
        ("drawdown63", float(metrics.drawdown63)),
        ("recovery_streak", float(next_streak)),
    )
    if next_state is TrendState.UNKNOWN:
        decision = VetoDecision(
            outcome=VetoOutcome.INSUFFICIENT,
            veto_active=None,
            reason_code="TREND_STATE_NOT_REESTABLISHED",
            metrics=metric_values,
        )
    else:
        decision = _boolean_decision(
            next_state is TrendState.VETO_ACTIVE,
            reason_code="TREND_EXACT_STATE_TRANSITION_EVALUATED",
            metrics=metric_values,
        )
    return TrendStep(decision=decision, next_checkpoint=next_checkpoint)


__all__ = [
    "EventCoverageReceipt",
    "EventRevision",
    "PriceWindowInput",
    "ResearchClock",
    "RevisionAction",
    "ScheduledEventInput",
    "TrendCheckpoint",
    "TrendState",
    "TrendStep",
    "VetoDecision",
    "VetoOutcome",
    "VolatilityWindowInput",
    "evaluate_broad_market_risk_off",
    "evaluate_realized_volatility_veto",
    "evaluate_scheduled_event_risk",
    "evaluate_underlying_trend_break",
    "initial_trend_checkpoint",
]
