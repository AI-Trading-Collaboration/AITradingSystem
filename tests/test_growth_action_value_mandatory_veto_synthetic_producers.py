from __future__ import annotations

import copy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_synthetic_producer_contract as contract,
)
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_synthetic_producers as producers,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

EventCoverageReceipt = producers.EventCoverageReceipt
EventRevision = producers.EventRevision
PriceWindowInput = producers.PriceWindowInput
ResearchClock = producers.ResearchClock
RevisionAction = producers.RevisionAction
ScheduledEventInput = producers.ScheduledEventInput
TrendState = producers.TrendState
VetoOutcome = producers.VetoOutcome
VolatilityWindowInput = producers.VolatilityWindowInput
evaluate_broad_market_risk_off = producers.evaluate_broad_market_risk_off
evaluate_realized_volatility_veto = producers.evaluate_realized_volatility_veto
evaluate_scheduled_event_risk = producers.evaluate_scheduled_event_risk
evaluate_underlying_trend_break = producers.evaluate_underlying_trend_break
initial_trend_checkpoint = producers.initial_trend_checkpoint

_CONFIG_PATH = PROJECT_ROOT / (
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_synthetic_producer_contract_v1.yaml"
)
_SHA = "a" * 64


def _clock(day: int = 1) -> ResearchClock:
    decision = datetime(2025, 1, day, 21, 15, tzinfo=UTC)
    return ResearchClock(
        decision_as_of=decision,
        next_action_cutoff=decision + timedelta(hours=20),
    )


def _price_input(
    *,
    ticker: str,
    sma: tuple[float | None, ...] | None = None,
    drawdown: tuple[float | None, ...] | None = None,
    clock: ResearchClock | None = None,
) -> PriceWindowInput:
    selected_clock = clock or _clock()
    return PriceWindowInput(
        ticker=ticker,
        sma200_closes=sma or (100.0,) * 200,
        drawdown63_closes=drawdown or (100.0,) * 63,
        available_at=selected_clock.decision_as_of,
    )


def _receipts(clock: ResearchClock) -> tuple[EventCoverageReceipt, ...]:
    return tuple(
        EventCoverageReceipt(
            authority=authority,
            coverage_through=clock.next_action_cutoff,
            published_at=clock.decision_as_of,
            source_snapshot_sha256=_SHA,
        )
        for authority in ("FEDERAL_RESERVE", "BLS", "BEA")
    )


def _event(
    clock: ResearchClock,
    *,
    revision_id: str = "r1",
    action: RevisionAction = RevisionAction.UPSERT,
    published_delta: timedelta = timedelta(hours=-1),
    scheduled_delta: timedelta = timedelta(hours=1),
) -> EventRevision:
    return EventRevision(
        authority="BLS",
        event_type="CPI",
        stable_event_key="bls-cpi-2025-01",
        scheduled_for=clock.decision_as_of + scheduled_delta,
        published_at=clock.decision_as_of + published_delta,
        revision_id=revision_id,
        revision_action=action,
        source_identity="BLS_OFFICIAL",
        source_snapshot_sha256=_SHA,
    )


def _payload() -> dict[str, Any]:
    return cast(
        dict[str, Any],
        load_strict_yaml_text(_CONFIG_PATH.read_text(encoding="utf-8"), label=str(_CONFIG_PATH)),
    )


def test_contract_replays_s4b_and_separates_synthetic_from_source_admission() -> None:
    loaded = contract.load_mandatory_veto_synthetic_producer_contract()

    assert loaded.terminal == (
        "SYNTHETIC_CALLABLE_CONFORMANCE_READY_4_OF_4_SOURCE_UNADMITTED_0_OF_4"
    )
    assert loaded.freeze_admission.file_sha256 == (
        "ef075527750efd24433eafbd8a2e586104562868f4ce2b666043069fe5368765"
    )
    assert all(
        row.synthetic_callable_conformance_implemented
        for row in loaded.policy.producer_rows
    )
    assert not any(row.producer_contract_admitted for row in loaded.policy.producer_rows)
    assert not any(row.real_source_identity_admitted for row in loaded.policy.producer_rows)
    assert not any(row.series_generation_allowed for row in loaded.policy.producer_rows)


def test_contract_rejects_fabricated_admission_or_execution() -> None:
    payload = _payload()
    admitted = copy.deepcopy(payload)
    admitted["producer_rows"][0]["producer_contract_admitted"] = True
    with pytest.raises(ValidationError):
        contract.MandatoryVetoSyntheticProducerContract.model_validate(admitted)

    executable = copy.deepcopy(payload)
    executable["safety"]["backtest_authorized"] = True
    with pytest.raises(ValidationError):
        contract.MandatoryVetoSyntheticProducerContract.model_validate(executable)


def test_broad_market_exact_boundaries_and_no_short_circuit() -> None:
    clock = _clock()
    clear = evaluate_broad_market_risk_off(_price_input(ticker="SPY", clock=clock), clock=clock)
    assert clear.outcome is VetoOutcome.CLEAR

    drawdown_at_threshold = _price_input(
        ticker="SPY",
        sma=(80.0,) * 199 + (90.0,),
        drawdown=(100.0,) + (90.0,) * 62,
        clock=clock,
    )
    threshold = evaluate_broad_market_risk_off(drawdown_at_threshold, clock=clock)
    assert threshold.outcome is VetoOutcome.VETO_ACTIVE
    assert dict(threshold.metrics)["drawdown63"] == pytest.approx(-0.10)

    incomplete_other_component = _price_input(
        ticker="SPY",
        sma=(100.0,) * 199 + (90.0,),
        drawdown=(100.0,) * 62 + (None,),
        clock=clock,
    )
    no_short_circuit = evaluate_broad_market_risk_off(
        incomplete_other_component, clock=clock
    )
    assert no_short_circuit.outcome is VetoOutcome.INSUFFICIENT


def test_broad_market_rejects_wrong_ticker_window_and_late_data() -> None:
    clock = _clock()
    wrong_ticker = evaluate_broad_market_risk_off(
        _price_input(ticker="QQQ", clock=clock), clock=clock
    )
    assert wrong_ticker.outcome is VetoOutcome.INVALID

    long_window = evaluate_broad_market_risk_off(
        _price_input(ticker="SPY", sma=(100.0,) * 201, clock=clock), clock=clock
    )
    assert long_window.outcome is VetoOutcome.INVALID

    late = replace(
        _price_input(ticker="SPY", clock=clock),
        available_at=clock.decision_as_of + timedelta(seconds=1),
    )
    assert evaluate_broad_market_risk_off(late, clock=clock).outcome is VetoOutcome.INSUFFICIENT


def test_volatility_average_rank_threshold_is_inclusive() -> None:
    clock = _clock()
    below = tuple(float(value) for value in range(1, 189))
    above = tuple(float(value) for value in range(190, 253))
    exact_rank = below + above + (189.0,)
    decision = evaluate_realized_volatility_veto(
        VolatilityWindowInput(
            qqq_closes_21=(100.0,) * 21,
            vix_levels_252=exact_rank,
            qqq_available_at=clock.decision_as_of,
            vix_available_at=clock.decision_as_of,
        ),
        clock=clock,
    )
    assert dict(decision.metrics)["vix_percentile252"] == pytest.approx(0.75)
    assert decision.outcome is VetoOutcome.VETO_ACTIVE


def test_volatility_rv20_and_no_short_circuit() -> None:
    clock = _clock()
    closes = [100.0]
    for position in range(20):
        closes.append(closes[-1] * (1.03 if position % 2 == 0 else 0.97))
    active = evaluate_realized_volatility_veto(
        VolatilityWindowInput(
            qqq_closes_21=tuple(closes),
            vix_levels_252=(20.0,) * 252,
            qqq_available_at=clock.decision_as_of,
            vix_available_at=clock.decision_as_of,
        ),
        clock=clock,
    )
    assert dict(active.metrics)["qqq_annualized_rv20"] > 0.25
    assert active.outcome is VetoOutcome.VETO_ACTIVE

    incomplete_qqq = evaluate_realized_volatility_veto(
        VolatilityWindowInput(
            qqq_closes_21=(100.0,) * 20 + (None,),
            vix_levels_252=tuple(float(value) for value in range(1, 253)),
            qqq_available_at=clock.decision_as_of,
            vix_available_at=clock.decision_as_of,
        ),
        clock=clock,
    )
    assert incomplete_qqq.outcome is VetoOutcome.INSUFFICIENT


def test_event_requires_complete_coverage_before_clear() -> None:
    clock = _clock()
    clear = evaluate_scheduled_event_risk(
        ScheduledEventInput(revisions=(), coverage_receipts=_receipts(clock)), clock=clock
    )
    assert clear.outcome is VetoOutcome.CLEAR

    missing = evaluate_scheduled_event_risk(
        ScheduledEventInput(revisions=(), coverage_receipts=_receipts(clock)[:2]), clock=clock
    )
    assert missing.outcome is VetoOutcome.INSUFFICIENT

    short_receipt = replace(
        _receipts(clock)[0], coverage_through=clock.next_action_cutoff - timedelta(seconds=1)
    )
    short = evaluate_scheduled_event_risk(
        ScheduledEventInput(
            revisions=(), coverage_receipts=(short_receipt,) + _receipts(clock)[1:]
        ),
        clock=clock,
    )
    assert short.outcome is VetoOutcome.INSUFFICIENT


def test_event_revision_reschedule_cancel_and_conflict_are_pit() -> None:
    clock = _clock()
    initial = _event(clock)
    active = evaluate_scheduled_event_risk(
        ScheduledEventInput(revisions=(initial,), coverage_receipts=_receipts(clock)),
        clock=clock,
    )
    assert active.outcome is VetoOutcome.VETO_ACTIVE

    at_action_cutoff = replace(initial, scheduled_for=clock.next_action_cutoff)
    inclusive_cutoff = evaluate_scheduled_event_risk(
        ScheduledEventInput(
            revisions=(at_action_cutoff,), coverage_receipts=_receipts(clock)
        ),
        clock=clock,
    )
    assert inclusive_cutoff.outcome is VetoOutcome.VETO_ACTIVE

    at_decision = replace(initial, scheduled_for=clock.decision_as_of)
    strict_decision_frontier = evaluate_scheduled_event_risk(
        ScheduledEventInput(revisions=(at_decision,), coverage_receipts=_receipts(clock)),
        clock=clock,
    )
    assert strict_decision_frontier.outcome is VetoOutcome.CLEAR

    cancel = _event(
        clock,
        revision_id="r2",
        action=RevisionAction.CANCEL,
        published_delta=timedelta(minutes=-30),
    )
    cancelled = evaluate_scheduled_event_risk(
        ScheduledEventInput(revisions=(initial, cancel), coverage_receipts=_receipts(clock)),
        clock=clock,
    )
    assert cancelled.outcome is VetoOutcome.CLEAR

    after_cutoff = _event(
        clock,
        revision_id="r2",
        action=RevisionAction.RESCHEDULE,
        published_delta=timedelta(minutes=-30),
        scheduled_delta=timedelta(hours=21),
    )
    rescheduled = evaluate_scheduled_event_risk(
        ScheduledEventInput(
            revisions=(initial, after_cutoff), coverage_receipts=_receipts(clock)
        ),
        clock=clock,
    )
    assert rescheduled.outcome is VetoOutcome.CLEAR

    conflict = replace(initial, revision_id="other")
    invalid = evaluate_scheduled_event_risk(
        ScheduledEventInput(revisions=(initial, conflict), coverage_receipts=_receipts(clock)),
        clock=clock,
    )
    assert invalid.outcome is VetoOutcome.INVALID


def test_event_rejects_unfrozen_taxonomy_and_ignores_future_revision() -> None:
    clock = _clock()
    invalid_event = replace(_event(clock), event_type="UNSCHEDULED_INTERVENTION")
    invalid = evaluate_scheduled_event_risk(
        ScheduledEventInput(
            revisions=(invalid_event,), coverage_receipts=_receipts(clock)
        ),
        clock=clock,
    )
    assert invalid.outcome is VetoOutcome.INVALID

    future_cancel = _event(
        clock,
        revision_id="r2",
        action=RevisionAction.CANCEL,
        published_delta=timedelta(minutes=1),
    )
    historical = evaluate_scheduled_event_risk(
        ScheduledEventInput(
            revisions=(_event(clock), future_cancel), coverage_receipts=_receipts(clock)
        ),
        clock=clock,
    )
    assert historical.outcome is VetoOutcome.VETO_ACTIVE


def test_trend_unknown_requires_two_valid_recovery_sessions() -> None:
    checkpoint = initial_trend_checkpoint(source_inventory_sha256=_SHA)
    first_clock = _clock(1)
    first = evaluate_underlying_trend_break(
        _price_input(ticker="QQQ", clock=first_clock),
        clock=first_clock,
        checkpoint=checkpoint,
    )
    assert first.decision.outcome is VetoOutcome.INSUFFICIENT
    assert first.next_checkpoint.state is TrendState.UNKNOWN
    assert first.next_checkpoint.recovery_streak == 1

    second_clock = _clock(2)
    second = evaluate_underlying_trend_break(
        _price_input(ticker="QQQ", clock=second_clock),
        clock=second_clock,
        checkpoint=first.next_checkpoint,
    )
    assert second.decision.outcome is VetoOutcome.CLEAR
    assert second.next_checkpoint.state is TrendState.CLEAR
    assert second.next_checkpoint.recovery_streak == 0


def test_trend_entry_persistence_and_two_session_clear() -> None:
    clock1 = _clock(1)
    checkpoint = initial_trend_checkpoint(source_inventory_sha256=_SHA)
    entry_input = _price_input(
        ticker="QQQ",
        sma=(100.0,) * 199 + (88.0,),
        drawdown=(100.0,) + (88.0,) * 62,
        clock=clock1,
    )
    entry = evaluate_underlying_trend_break(
        entry_input, clock=clock1, checkpoint=checkpoint
    )
    assert entry.decision.outcome is VetoOutcome.VETO_ACTIVE
    assert dict(entry.decision.metrics)["drawdown63"] == pytest.approx(-0.12)

    clock2 = _clock(2)
    persistence_input = _price_input(
        ticker="QQQ",
        sma=(100.0,) * 199 + (95.0,),
        drawdown=(95.0,) * 63,
        clock=clock2,
    )
    persistent = evaluate_underlying_trend_break(
        persistence_input, clock=clock2, checkpoint=entry.next_checkpoint
    )
    assert persistent.decision.outcome is VetoOutcome.VETO_ACTIVE

    clock3 = _clock(3)
    recovery1 = evaluate_underlying_trend_break(
        _price_input(ticker="QQQ", clock=clock3),
        clock=clock3,
        checkpoint=persistent.next_checkpoint,
    )
    assert recovery1.decision.outcome is VetoOutcome.VETO_ACTIVE
    assert recovery1.next_checkpoint.recovery_streak == 1

    clock4 = _clock(4)
    recovery2 = evaluate_underlying_trend_break(
        _price_input(ticker="QQQ", clock=clock4),
        clock=clock4,
        checkpoint=recovery1.next_checkpoint,
    )
    assert recovery2.decision.outcome is VetoOutcome.CLEAR


def test_trend_missing_or_malformed_resets_to_unknown() -> None:
    clock = _clock()
    checkpoint = initial_trend_checkpoint(source_inventory_sha256=_SHA)
    missing = evaluate_underlying_trend_break(
        _price_input(ticker="QQQ", drawdown=(100.0,) * 62 + (None,), clock=clock),
        clock=clock,
        checkpoint=checkpoint,
    )
    assert missing.decision.outcome is VetoOutcome.INSUFFICIENT
    assert missing.next_checkpoint.state is TrendState.UNKNOWN
    assert missing.next_checkpoint.recovery_streak == 0

    malformed = evaluate_underlying_trend_break(
        _price_input(ticker="QQQ", sma=(100.0,) * 199 + (float("nan"),), clock=clock),
        clock=clock,
        checkpoint=checkpoint,
    )
    assert malformed.decision.outcome is VetoOutcome.INVALID
    assert malformed.next_checkpoint.state is TrendState.UNKNOWN

    bad_checkpoint = replace(checkpoint, state_checkpoint_sha256="not-a-sha")
    rejected_checkpoint = evaluate_underlying_trend_break(
        _price_input(ticker="QQQ", clock=clock),
        clock=clock,
        checkpoint=bad_checkpoint,
    )
    assert rejected_checkpoint.decision.outcome is VetoOutcome.INVALID


def test_naive_clock_is_invalid_for_every_producer() -> None:
    naive = ResearchClock(
        decision_as_of=datetime(2025, 1, 1, 21, 15),
        next_action_cutoff=datetime(2025, 1, 2, 17, 15),
    )
    broad = evaluate_broad_market_risk_off(
        _price_input(ticker="SPY", clock=naive), clock=naive
    )
    assert broad.outcome is VetoOutcome.INVALID


def test_initial_checkpoint_rejects_non_sha_inventory() -> None:
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        initial_trend_checkpoint(source_inventory_sha256="synthetic")
