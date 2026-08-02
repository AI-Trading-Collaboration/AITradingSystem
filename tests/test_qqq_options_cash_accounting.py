from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.cash_accounting import (
    DEFAULT_QQQ_OPTIONS_CASH_ACCOUNTING_POLICY_PATH,
    ActiveCashAccountingCriteria,
    QQQOptionCashAccountingContractError,
    QQQOptionCashAccountingRequest,
    QQQOptionCashAccountingResult,
    QQQOptionIntentAccountingInput,
    QQQOptionValuationQuoteInput,
    UnresolvedCashAccountingCriteria,
    build_qqq_option_cash_accounting_input_sha256,
    load_qqq_options_cash_accounting_policy,
    replay_qqq_option_cash_accounting,
)
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    FillEventRecord,
    OrderEventRecord,
    OrderIntentRecord,
    QQQOptionsSafetyBoundary,
    RunManifestRecord,
)
from ai_trading_system.qqq_options_research.minute_execution import (
    QQQOptionExecutionResult,
)

_SHA_A = "a" * 64
_SHA_B = "b" * 64
_SHA_C = "c" * 64
_REPOSITORY_SHA = "e" * 40
_EXECUTION_POLICY_SHA = (
    "8c8823ddcc509e7dfdb81803a6fe7099b1ff44fccefc5a607c2a9abc7875226a"
)
_RUN_ID = "run-20210222-cash-accounting"
_OPTION_SID = "QQQ-20210319-C-325"
_REQUESTED_START = date(2021, 2, 22)
_REQUESTED_END = date(2021, 3, 31)
_MANIFEST_AT = datetime(2021, 2, 22, 13, 0, tzinfo=UTC)
_DEFAULT_SNAPSHOT_AT = datetime(2021, 2, 26, 21, 0, tzinfo=UTC)
_DEFAULT_SESSIONS = tuple(date(2021, 2, day) for day in range(22, 27))


def _sha(value: str | bytes) -> str:
    content = value if isinstance(value, bytes) else value.encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def _safety() -> QQQOptionsSafetyBoundary:
    return QQQOptionsSafetyBoundary(
        research_only=True,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        raw_options_data_export_allowed=False,
        strategy_execution_allowed=False,
        bounded_cloud_pilot_authorized=False,
        production_effect="none",
        broker_action="none",
    )


def _envelope(
    *,
    schema_name: str,
    record_id: str,
    created_at_utc: datetime,
    suffix: str,
    policy_sha256: str = _EXECUTION_POLICY_SHA,
) -> dict[str, Any]:
    return {
        "schema_name": schema_name,
        "schema_version": "1.0.0",
        "run_id": _RUN_ID,
        "record_id": record_id,
        "created_at_utc": created_at_utc,
        "producer_version": "test.cash-accounting.v1",
        "repository_code_sha": _REPOSITORY_SHA,
        "policy_id": "qqq_options_minute_execution_reality_model_v1",
        "policy_version": "1.0.0",
        "policy_sha256": policy_sha256,
        "contract_schema_sha256": QQQ_OPTIONS_CONTRACT_SHA256,
        "source_ids": (f"qqq.options.accounting.synthetic.{suffix}",),
        "source_checksums": (_sha(f"source-{suffix}"),),
        "requested_start": _REQUESTED_START,
        "requested_end": _REQUESTED_END,
        "evaluated_start": _REQUESTED_START,
        "evaluated_end": _REQUESTED_END,
        "storage_timezone": "UTC",
        "exchange_timezone": "America/New_York",
        "dq_status": "PASS",
        "pit_status": "PASS",
        "export_classification": "EXPORT_ALLOWED_DERIVED",
        "lineage_id": f"lineage-{suffix}",
        "safety": _safety(),
    }


def _manifest(*, initial_cash: str = "10000.00") -> RunManifestRecord:
    return RunManifestRecord.seal(
        **_envelope(
            schema_name="run_manifest",
            record_id="run-manifest-cash-accounting",
            created_at_utc=_MANIFEST_AT,
            suffix="manifest",
            policy_sha256=_SHA_A,
        ),
        underlying="QQQ",
        initial_cash_usd=Decimal(initial_cash),
        account_currency="USD",
        account_type="CASH",
        signal_resolution="DAILY",
        execution_resolution="MINUTE",
        signal_artifact_sha256=_SHA_B,
        engine_identity_status="UNKNOWN",
        engine_identity=None,
        evidence_admission_decision="CAPABILITY_OR_LICENSE_BLOCKED",
    )


def _execution_result(
    *,
    suffix: str,
    side: str = "BUY_TO_OPEN",
    contracts: int = 2,
    multiplier: int = 50,
    limit: str = "5.50",
    fill_specs: tuple[tuple[str, int, str], ...] = (("5.30", 1, "0.65"),),
    intent_at: datetime = datetime(2021, 2, 23, 14, 31, tzinfo=UTC),
    reserved_cash: Decimal | None = None,
    rejected: bool = False,
    selection_decision_sha256: str | None = None,
) -> QQQOptionExecutionResult:
    limit_value = Decimal(limit)
    reservation = (
        Decimal("0")
        if side == "SELL_TO_CLOSE"
        else limit_value * multiplier * contracts + Decimal(contracts)
    )
    intent = OrderIntentRecord.seal(
        **_envelope(
            schema_name="order_intent",
            record_id=f"intent-{suffix}",
            created_at_utc=intent_at,
            suffix=f"intent.{suffix}",
        ),
        intent_id=f"intent-{suffix}",
        decision_id=f"selection-{suffix}",
        option_sid=_OPTION_SID,
        side=side,
        contracts=contracts,
        order_type="MARKETABLE_LIMIT",
        limit_price_per_share=limit_value,
        reserved_cash_usd=reservation if reserved_cash is None else reserved_cash,
        not_before_utc=intent_at + timedelta(seconds=30),
    )
    platform_order_id = f"order-{suffix}"
    events: list[OrderEventRecord] = []
    fills: list[FillEventRecord] = []
    events.append(
        OrderEventRecord.seal(
            **_envelope(
                schema_name="order_event",
                record_id=f"event-{suffix}-0",
                created_at_utc=intent_at,
                suffix=f"event.{suffix}.0",
            ),
            platform_order_id=platform_order_id,
            event_sequence=0,
            event_type="CREATED",
            event_at_utc=intent_at,
            side=side,
            order_contracts=contracts,
            filled_contracts_total=0,
            limit_price_per_share=limit_value,
            reason_code=None,
        )
    )
    submitted_at = intent_at + timedelta(minutes=1)
    events.append(
        OrderEventRecord.seal(
            **_envelope(
                schema_name="order_event",
                record_id=f"event-{suffix}-1",
                created_at_utc=submitted_at,
                suffix=f"event.{suffix}.1",
            ),
            platform_order_id=platform_order_id,
            event_sequence=1,
            event_type="SUBMITTED",
            event_at_utc=submitted_at,
            side=side,
            order_contracts=contracts,
            filled_contracts_total=0,
            limit_price_per_share=limit_value,
            reason_code=None,
        )
    )
    if rejected:
        rejected_at = intent_at + timedelta(minutes=2)
        events.append(
            OrderEventRecord.seal(
                **_envelope(
                    schema_name="order_event",
                    record_id=f"event-{suffix}-2",
                    created_at_utc=rejected_at,
                    suffix=f"event.{suffix}.2",
                ),
                platform_order_id=platform_order_id,
                event_sequence=2,
                event_type="REJECTED",
                event_at_utc=rejected_at,
                side=side,
                order_contracts=contracts,
                filled_contracts_total=0,
                limit_price_per_share=limit_value,
                reason_code="VENUE_REJECTED",
            )
        )
    else:
        cumulative = 0
        for index, (fill_price, fill_contracts, fee) in enumerate(fill_specs, start=1):
            fill_at = intent_at + timedelta(minutes=index + 1)
            fill_price_value = Decimal(fill_price)
            cumulative += fill_contracts
            fills.append(
                FillEventRecord.seal(
                    **_envelope(
                        schema_name="fill_event",
                        record_id=f"fill-{suffix}-{index}",
                        created_at_utc=fill_at,
                        suffix=f"fill.{suffix}.{index}",
                    ),
                    platform_order_id=platform_order_id,
                    fill_sequence=index,
                    fill_at_utc=fill_at,
                    quote_end_utc=fill_at - timedelta(seconds=30),
                    side=side,
                    filled_contracts=fill_contracts,
                    fill_price_per_share=fill_price_value,
                    contract_multiplier=multiplier,
                    fee_usd=Decimal(fee),
                    settlement_currency="USD",
                    quote_side="ASK" if side == "BUY_TO_OPEN" else "BID",
                    gross_cash_delta_usd=(
                        (-1 if side == "BUY_TO_OPEN" else 1)
                        * fill_price_value
                        * multiplier
                        * fill_contracts
                    ),
                )
            )
            complete = cumulative == contracts
            event_at = fill_at
            events.append(
                OrderEventRecord.seal(
                    **_envelope(
                        schema_name="order_event",
                        record_id=f"event-{suffix}-{len(events)}",
                        created_at_utc=event_at,
                        suffix=f"event.{suffix}.{len(events)}",
                    ),
                    platform_order_id=platform_order_id,
                    event_sequence=len(events),
                    event_type="FILLED" if complete else "PARTIALLY_FILLED",
                    event_at_utc=event_at,
                    side=side,
                    order_contracts=contracts,
                    filled_contracts_total=cumulative,
                    limit_price_per_share=limit_value,
                    reason_code=None,
                )
            )
        if cumulative < contracts:
            canceled_at = intent_at + timedelta(minutes=len(fill_specs) + 2)
            events.append(
                OrderEventRecord.seal(
                    **_envelope(
                        schema_name="order_event",
                        record_id=f"event-{suffix}-{len(events)}",
                        created_at_utc=canceled_at,
                        suffix=f"event.{suffix}.{len(events)}",
                    ),
                    platform_order_id=platform_order_id,
                    event_sequence=len(events),
                    event_type="CANCELED",
                    event_at_utc=canceled_at,
                    side=side,
                    order_contracts=contracts,
                    filled_contracts_total=cumulative,
                    limit_price_per_share=limit_value,
                    reason_code="CANCEL_TIMEOUT",
                )
            )
    filled_contracts = sum(item.filled_contracts for item in fills)
    reason = (
        "VENUE_REJECTED"
        if rejected
        else "NO_FILL_CANCELED"
        if not fills
        else "FILLED"
        if filled_contracts == contracts
        else "PARTIAL_CANCELED"
    )
    return QQQOptionExecutionResult.seal(
        schema_version="qqq_options_minute_execution_result.v1",
        policy_sha256=_EXECUTION_POLICY_SHA,
        selection_policy_sha256=_SHA_A,
        selection_decision_sha256=(
            selection_decision_sha256 or _sha(f"selection-{suffix}")
        ),
        quote_set_sha256=_sha(f"quote-set-{suffix}"),
        execution_authorized=True,
        selection_authorized=True,
        cash_preservation_required=not fills,
        reason_code=reason,
        execution_stage_dq_status="PASS" if fills else "NOT_EVALUATED",
        global_dq_status="PASS" if fills else "NOT_EVALUATED",
        global_pit_status="PASS" if fills else "NOT_EVALUATED",
        accounting_status="NOT_EVALUATED",
        order_intent=intent,
        order_events=tuple(events),
        fill_events=tuple(fills),
        execution_dq_reports=(),
    )


def _default_policy_payload() -> dict[str, Any]:
    path = PROJECT_ROOT / DEFAULT_QQQ_OPTIONS_CASH_ACCOUNTING_POLICY_PATH
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _active_policy_path(
    tmp_path: Path,
    *,
    initial_cash: str = "10000.00",
    premium_budget: str = "5000.00",
    max_contracts: int = 5,
    fee_buffer: str = "1.00",
    settlement_lag: int = 1,
    max_quote_age_ms: int = 120000,
    include_fees: bool = True,
    scenario_role: str = "SYNTHETIC_TEST_ONLY",
    reality_baseline: bool = False,
    name: str = "synthetic-active-accounting-policy.yaml",
) -> Path:
    payload = _default_policy_payload()
    payload.update(
        {
            "status": "OWNER_REVIEWED_ACTIVE",
            "owner": "synthetic_test_fixture_only",
            "owner_decision": "synthetic_test_fixture_only:not_project_authority",
            "accounting_authorized": True,
        }
    )
    payload["criteria"] = {
        "mode": "ACTIVE",
        "scenario_role": scenario_role,
        "dq_caveat": "Synthetic arithmetic fixture only; no investment authority.",
        "approved_initial_cash_usd": initial_cash,
        "premium_budget_usd": premium_budget,
        "max_contracts_per_order": max_contracts,
        "fee_buffer_per_contract_usd": fee_buffer,
        "sell_proceeds_settlement_lag_sessions": settlement_lag,
        "max_valuation_quote_age_ms": max_quote_age_ms,
        "cost_basis_method": "FIFO",
        "include_fees_in_cost_basis": include_fees,
        "cash_quantum_usd": "0.01",
        "rounding_mode": "ROUND_HALF_EVEN",
        "reality_baseline": reality_baseline,
    }
    path = tmp_path / name
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _valuation(
    *,
    option_sid: str = _OPTION_SID,
    snapshot_at: datetime = _DEFAULT_SNAPSHOT_AT,
    bid: str = "6.00",
    ask: str = "6.20",
    age: timedelta = timedelta(minutes=1),
    suffix: str = "325",
) -> QQQOptionValuationQuoteInput:
    return QQQOptionValuationQuoteInput(
        option_sid=option_sid,
        source_id=f"qqq.options.valuation.synthetic.{suffix}",
        source_sha256=_sha(f"valuation-{suffix}-{bid}-{ask}-{age}"),
        quote_end_utc=snapshot_at - age,
        resolution="MINUTE",
        bid_per_share=Decimal(bid),
        ask_per_share=Decimal(ask),
    )


def _request(
    results: tuple[QQQOptionExecutionResult, ...],
    *,
    initial_cash: str = "10000.00",
    snapshot_at: datetime = _DEFAULT_SNAPSHOT_AT,
    as_of_session: date = date(2021, 2, 26),
    sessions: tuple[date, ...] = _DEFAULT_SESSIONS,
    valuations: tuple[QQQOptionValuationQuoteInput, ...] = (),
    multiplier_overrides: dict[str, int] | None = None,
) -> QQQOptionCashAccountingRequest:
    manifest = _manifest(initial_cash=initial_cash)
    result_bytes = tuple(item.canonical_bytes for item in results)
    intent_inputs: list[QQQOptionIntentAccountingInput] = []
    admitted_intent_hashes: set[str] = set()
    for result in results:
        if result.order_intent is None:
            continue
        if result.order_intent.content_sha256 in admitted_intent_hashes:
            continue
        admitted_intent_hashes.add(result.order_intent.content_sha256)
        suffix = result.order_intent.intent_id.removeprefix("intent-")
        fill_multiplier = (
            result.fill_events[0].contract_multiplier
            if result.fill_events
            else 50
        )
        multiplier = (
            multiplier_overrides.get(suffix, fill_multiplier)
            if multiplier_overrides is not None
            else fill_multiplier
        )
        intent_inputs.append(
            QQQOptionIntentAccountingInput(
                intent_content_sha256=result.order_intent.content_sha256,
                contract_multiplier=multiplier,
                source_id=f"qqq.options.multiplier.synthetic.{suffix}",
                source_sha256=_sha(f"multiplier-{suffix}-{multiplier}"),
            )
        )
    return QQQOptionCashAccountingRequest(
        run_manifest_bytes=manifest.canonical_bytes,
        run_manifest_file_sha256=_sha(manifest.canonical_bytes),
        execution_result_bytes=result_bytes,
        execution_result_file_sha256s=tuple(_sha(item) for item in result_bytes),
        intent_accounting_inputs=tuple(intent_inputs),
        snapshot_at_utc=snapshot_at,
        as_of_session=as_of_session,
        exchange_sessions=sessions,
        exchange_calendar_source_id="xnys.reviewed.synthetic",
        exchange_calendar_source_sha256=_sha(
            "|".join(item.isoformat() for item in sessions)
        ),
        valuation_quotes=valuations,
        producer_version="test.cash-accounting.v1",
        lineage_id="cash-accounting-lineage-20210222",
    )


def test_default_policy_is_exact_unresolved_and_unauthorized() -> None:
    loaded = load_qqq_options_cash_accounting_policy()

    assert loaded.policy.status == "OWNER_REVIEW_REQUIRED_BASELINE"
    assert loaded.policy.accounting_authorized is False
    assert isinstance(loaded.policy.criteria, UnresolvedCashAccountingCriteria)
    assert set(loaded.policy.criteria.model_dump().values()) >= {
        "UNKNOWN_REQUIRES_POLICY_REVIEW"
    }
    assert loaded.policy.primary_research_start == date(2021, 2, 22)
    assert loaded.policy.legacy_non_default_start_is_default is False
    assert loaded.policy.safety.margin_allowed is False
    assert loaded.policy.safety.external_order_allowed is False


def test_default_policy_returns_typed_cash_preservation_without_mutation() -> None:
    result = _execution_result(suffix="default", contracts=1)
    request = _request((result,), valuations=(_valuation(),))

    accounting = replay_qqq_option_cash_accounting(request)

    assert accounting.reason_code == "ACCOUNTING_POLICY_REVIEW_REQUIRED"
    assert accounting.accounting_authorized is False
    assert accounting.cash_preservation_required is True
    assert accounting.ledger_entries == ()
    assert accounting.positions == ()
    assert accounting.portfolio_snapshot is None


def test_active_full_buy_uses_actual_multiplier_fee_and_bid_value(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    execution = _execution_result(
        suffix="full-buy",
        contracts=2,
        multiplier=50,
        fill_specs=(("5.30", 1, "0.65"), ("5.20", 1, "0.65")),
    )
    request = _request((execution,), valuations=(_valuation(),))

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "ACCOUNTING_REPLAY_READY"
    assert result.investment_interpretation_allowed is False
    assert result.reality_baseline is False
    assert tuple(item.entry_type for item in result.ledger_entries) == (
        "RESERVATION_CREATED",
        "BUY_FILL_SETTLED",
        "BUY_FILL_SETTLED",
    )
    snapshot = result.portfolio_snapshot
    assert snapshot is not None
    assert snapshot.settled_cash_usd == Decimal("9473.70")
    assert snapshot.reserved_cash_usd == Decimal("0")
    assert snapshot.option_market_value_usd == Decimal("600.00")
    assert snapshot.fees_paid_usd == Decimal("1.30")
    assert snapshot.unrealized_pnl_usd == Decimal("73.70")
    assert result.positions[0].contract_multiplier == 50
    assert result.positions[0].contracts_open == 2


def test_partial_then_cancel_releases_remainder_and_price_improvement(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    execution = _execution_result(
        suffix="partial-cancel",
        contracts=3,
        multiplier=50,
        fill_specs=(("5.20", 1, "0.65"),),
    )
    request = _request((execution,), valuations=(_valuation(),))

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "ACCOUNTING_REPLAY_READY"
    assert tuple(item.entry_type for item in result.ledger_entries) == (
        "RESERVATION_CREATED",
        "BUY_FILL_SETTLED",
        "RESERVATION_RELEASED",
    )
    assert result.ledger_entries[-1].reserved_cash_after_usd == Decimal("0")
    assert result.portfolio_snapshot is not None
    assert result.portfolio_snapshot.settled_cash_usd == Decimal("9739.35")
    assert result.positions[0].contracts_open == 1


def test_mixed_no_fill_result_does_not_create_ledger_mutation(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    no_fill = _execution_result(
        suffix="mixed-no-fill",
        contracts=1,
        fill_specs=(),
    )
    filled = _execution_result(
        suffix="mixed-filled",
        contracts=1,
        intent_at=datetime(2021, 2, 24, 14, 31, tzinfo=UTC),
    )
    request = _request((no_fill, filled), valuations=(_valuation(),))

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "ACCOUNTING_REPLAY_READY"
    assert no_fill.order_intent is not None
    assert all(
        entry.source_record_sha256 != no_fill.order_intent.content_sha256
        for entry in result.ledger_entries
    )


@pytest.mark.parametrize("rejected", [False, True])
def test_no_fill_or_reject_is_typed_no_mutation(
    tmp_path: Path, rejected: bool
) -> None:
    policy_path = _active_policy_path(tmp_path)
    execution = _execution_result(
        suffix=f"no-fill-{rejected}",
        contracts=1,
        fill_specs=(),
        rejected=rejected,
    )
    request = _request((execution,))

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "EXECUTION_BLOCKED_CASH_PRESERVED"
    assert result.cash_preservation_required is True
    assert result.ledger_entries == ()
    assert result.portfolio_snapshot is None


@pytest.mark.parametrize(
    ("policy_updates", "execution_updates", "expected_reason"),
    [
        ({"initial_cash": "100.00", "premium_budget": "100.00"}, {}, "INSUFFICIENT_SETTLED_CASH"),
        ({"premium_budget": "500.00"}, {}, "PREMIUM_BUDGET_EXCEEDED"),
        ({"max_contracts": 1}, {}, "MAX_CONTRACTS_EXCEEDED"),
        (
            {"fee_buffer": "0.50"},
            {
                "fill_specs": (("5.30", 1, "0.65"),),
                "reserved_cash": Decimal("551.00"),
            },
            "FEE_BUFFER_EXCEEDED",
        ),
    ],
)
def test_cash_budget_contract_and_fee_gates_fail_closed(
    tmp_path: Path,
    policy_updates: dict[str, Any],
    execution_updates: dict[str, Any],
    expected_reason: str,
) -> None:
    policy_path = _active_policy_path(tmp_path, **policy_updates)
    initial_cash = str(policy_updates.get("initial_cash", "10000.00"))
    execution = _execution_result(
        suffix=expected_reason.lower(),
        contracts=2,
        **execution_updates,
    )
    request = _request(
        (execution,),
        initial_cash=initial_cash,
        valuations=(_valuation(),),
    )

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == expected_reason
    assert result.cash_preservation_required is True
    assert result.ledger_entries == ()


def test_manifest_initial_cash_must_match_active_policy(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path, initial_cash="9000.00")
    execution = _execution_result(suffix="initial-cash", contracts=1)
    request = _request((execution,), valuations=(_valuation(),))

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "INITIAL_CASH_POLICY_MISMATCH"


def test_reservation_mismatch_fails_closed(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    execution = _execution_result(
        suffix="reservation-mismatch",
        contracts=1,
        reserved_cash=Decimal("1.00"),
    )
    request = _request((execution,), valuations=(_valuation(),))

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "RESERVATION_MISMATCH"
    assert result.ledger_entries == ()


@pytest.mark.parametrize("multiplier", [10, 50, 100, 125])
def test_reservation_property_uses_runtime_multiplier(
    tmp_path: Path, multiplier: int
) -> None:
    policy_path = _active_policy_path(tmp_path)
    contracts = 1
    limit = Decimal("5.50")
    reservation = limit * multiplier * contracts + Decimal(contracts)
    execution = _execution_result(
        suffix=f"multiplier-{multiplier}",
        contracts=contracts,
        multiplier=multiplier,
        reserved_cash=reservation,
    )
    request = _request((execution,), valuations=(_valuation(),))

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "ACCOUNTING_REPLAY_READY"
    assert result.ledger_entries[0].reserved_cash_delta_usd == reservation
    assert result.positions[0].contract_multiplier == multiplier


def test_multiplier_lineage_mismatch_fails_closed(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    execution = _execution_result(suffix="multiplier-drift", contracts=1, multiplier=50)
    request = _request(
        (execution,),
        valuations=(_valuation(),),
        multiplier_overrides={"multiplier-drift": 100},
    )

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "EXECUTION_IDENTITY_INVALID"
    assert result.portfolio_snapshot is None


def test_sell_before_buy_cannot_create_short_option(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    sell = _execution_result(
        suffix="sell-before-buy",
        side="SELL_TO_CLOSE",
        contracts=1,
        limit="5.00",
        fill_specs=(("5.20", 1, "0.65"),),
    )
    request = _request((sell,))

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "SHORT_OPTION_PROHIBITED"
    assert result.ledger_entries == ()


def _round_trip_results() -> tuple[QQQOptionExecutionResult, QQQOptionExecutionResult]:
    buy = _execution_result(
        suffix="roundtrip-buy",
        contracts=1,
        multiplier=100,
        fill_specs=(("5.00", 1, "0.65"),),
        intent_at=datetime(2021, 2, 23, 14, 31, tzinfo=UTC),
    )
    sell = _execution_result(
        suffix="roundtrip-sell",
        side="SELL_TO_CLOSE",
        contracts=1,
        multiplier=100,
        limit="6.50",
        fill_specs=(("7.00", 1, "0.65"),),
        intent_at=datetime(2021, 2, 24, 14, 31, tzinfo=UTC),
    )
    return buy, sell


def test_sell_proceeds_are_unsettled_before_reviewed_due_session(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    request = _request(
        _round_trip_results(),
        snapshot_at=datetime(2021, 2, 24, 21, 0, tzinfo=UTC),
        as_of_session=date(2021, 2, 24),
        sessions=tuple(date(2021, 2, day) for day in range(22, 26)),
    )

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "ACCOUNTING_REPLAY_READY"
    assert result.portfolio_snapshot is not None
    assert result.portfolio_snapshot.settled_cash_usd == Decimal("9499.35")
    assert result.portfolio_snapshot.unsettled_cash_usd == Decimal("699.35")
    assert result.portfolio_snapshot.realized_pnl_usd == Decimal("198.70")
    assert result.positions == ()


def test_sell_proceeds_settle_on_reviewed_due_session(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    request = _request(
        _round_trip_results(),
        snapshot_at=datetime(2021, 2, 25, 21, 0, tzinfo=UTC),
        as_of_session=date(2021, 2, 25),
        sessions=tuple(date(2021, 2, day) for day in range(22, 26)),
    )

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.portfolio_snapshot is not None
    assert result.portfolio_snapshot.settled_cash_usd == Decimal("10198.70")
    assert result.portfolio_snapshot.unsettled_cash_usd == Decimal("0")
    assert result.ledger_entries[-1].entry_type == "SELL_PROCEEDS_SETTLED"


def test_settlement_calendar_must_cover_due_session(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    request = _request(
        _round_trip_results(),
        snapshot_at=datetime(2021, 2, 24, 21, 0, tzinfo=UTC),
        as_of_session=date(2021, 2, 24),
        sessions=tuple(date(2021, 2, day) for day in range(22, 25)),
    )

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "SETTLEMENT_CALENDAR_INVALID"
    assert result.portfolio_snapshot is None


def test_weekend_is_not_a_settlement_session(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    buy = _execution_result(
        suffix="weekend-buy",
        contracts=1,
        multiplier=100,
        fill_specs=(("5.00", 1, "0.65"),),
        intent_at=datetime(2021, 2, 25, 14, 31, tzinfo=UTC),
    )
    sell = _execution_result(
        suffix="weekend-sell",
        side="SELL_TO_CLOSE",
        contracts=1,
        multiplier=100,
        limit="6.50",
        fill_specs=(("7.00", 1, "0.65"),),
        intent_at=datetime(2021, 2, 26, 14, 31, tzinfo=UTC),
    )
    sessions = (
        date(2021, 2, 22),
        date(2021, 2, 23),
        date(2021, 2, 24),
        date(2021, 2, 25),
        date(2021, 2, 26),
        date(2021, 3, 1),
    )
    request = _request(
        (buy, sell),
        snapshot_at=datetime(2021, 3, 1, 21, 0, tzinfo=UTC),
        as_of_session=date(2021, 3, 1),
        sessions=sessions,
    )

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    settlement = result.ledger_entries[-1]
    assert settlement.entry_type == "SELL_PROCEEDS_SETTLED"
    assert settlement.effective_session == date(2021, 3, 1)


def test_fifo_consumes_oldest_lot_and_preserves_remaining_cost(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    first = _execution_result(
        suffix="fifo-first",
        contracts=1,
        multiplier=100,
        fill_specs=(("5.00", 1, "0.65"),),
        intent_at=datetime(2021, 2, 23, 14, 31, tzinfo=UTC),
    )
    second = _execution_result(
        suffix="fifo-second",
        contracts=1,
        multiplier=100,
        limit="6.50",
        fill_specs=(("6.00", 1, "0.65"),),
        intent_at=datetime(2021, 2, 24, 14, 31, tzinfo=UTC),
    )
    sell = _execution_result(
        suffix="fifo-sell",
        side="SELL_TO_CLOSE",
        contracts=1,
        multiplier=100,
        limit="6.50",
        fill_specs=(("7.00", 1, "0.65"),),
        intent_at=datetime(2021, 2, 25, 14, 31, tzinfo=UTC),
    )
    request = _request((first, second, sell), valuations=(_valuation(),))

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.portfolio_snapshot is not None
    assert result.portfolio_snapshot.realized_pnl_usd == Decimal("198.70")
    assert result.positions[0].remaining_cost_basis_usd == Decimal("600.65")
    assert result.positions[0].lots[0].source_fill_sha256 == second.fill_events[0].content_sha256
    assert result.positions[0].unrealized_pnl_usd == Decimal("-0.65")


@pytest.mark.parametrize(
    "valuations",
    [
        (),
        (_valuation(age=timedelta(minutes=3)),),
        (_valuation(age=timedelta(minutes=-1)),),
    ],
)
def test_missing_stale_or_future_bid_valuation_fails_closed(
    tmp_path: Path,
    valuations: tuple[QQQOptionValuationQuoteInput, ...],
) -> None:
    policy_path = _active_policy_path(tmp_path)
    execution = _execution_result(suffix="valuation", contracts=1)
    request = _request((execution,), valuations=valuations)

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "VALUATION_QUOTE_REQUIRED"
    assert result.portfolio_snapshot is None


def test_crossed_valuation_is_rejected_at_request_boundary() -> None:
    with pytest.raises(ValidationError, match="crossed"):
        _valuation(bid="6.30", ask="6.20")


def test_extra_valuation_sid_is_not_silently_admitted(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    execution = _execution_result(suffix="extra-valuation", contracts=1)
    valuations = (
        _valuation(),
        _valuation(option_sid="QQQ-20210319-P-300", suffix="300"),
    )
    request = _request((execution,), valuations=valuations)

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "VALUATION_QUOTE_REQUIRED"


def test_duplicate_result_file_is_rejected_before_replay() -> None:
    execution = _execution_result(suffix="duplicate", contracts=1)

    with pytest.raises(ValidationError, match="identities must be unique"):
        _request((execution, execution), valuations=(_valuation(),))


def test_duplicate_record_across_distinct_results_fails_closed(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    original = _execution_result(suffix="duplicate-record", contracts=1)
    duplicate_payload = original.model_dump(
        exclude={"content_sha256", "selection_decision_sha256"}
    )
    duplicate_payload.update(
        {
            "order_intent": original.order_intent,
            "order_events": original.order_events,
            "fill_events": original.fill_events,
            "execution_dq_reports": original.execution_dq_reports,
        }
    )
    duplicate = QQQOptionExecutionResult.seal(
        **duplicate_payload,
        selection_decision_sha256=_SHA_C,
    )
    request = _request((original, duplicate), valuations=(_valuation(),))

    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "EXECUTION_IDENTITY_INVALID"
    assert result.ledger_entries == ()


def test_result_and_input_identity_are_permutation_invariant(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    first = _execution_result(
        suffix="permutation-first",
        contracts=1,
        fill_specs=(("5.10", 1, "0.65"),),
        intent_at=datetime(2021, 2, 23, 14, 31, tzinfo=UTC),
    )
    second = _execution_result(
        suffix="permutation-second",
        contracts=1,
        fill_specs=(("5.20", 1, "0.65"),),
        intent_at=datetime(2021, 2, 24, 14, 31, tzinfo=UTC),
    )
    forward = _request((first, second), valuations=(_valuation(),))
    reverse = _request((second, first), valuations=(_valuation(),))

    forward_result = replay_qqq_option_cash_accounting(
        forward, policy_path=policy_path
    )
    reverse_result = replay_qqq_option_cash_accounting(
        reverse, policy_path=policy_path
    )

    assert build_qqq_option_cash_accounting_input_sha256(forward) == (
        build_qqq_option_cash_accounting_input_sha256(reverse)
    )
    assert forward_result.canonical_bytes == reverse_result.canonical_bytes


def test_calendar_order_duplicate_weekend_and_asof_mismatch_are_rejected() -> None:
    execution = _execution_result(suffix="calendar", contracts=1)
    invalid_sessions = [
        (date(2021, 2, 23), date(2021, 2, 22)),
        (date(2021, 2, 22), date(2021, 2, 22)),
        (date(2021, 2, 22), date(2021, 2, 27)),
    ]
    for sessions in invalid_sessions:
        with pytest.raises(ValidationError):
            _request(
                (execution,),
                sessions=sessions,
                as_of_session=sessions[-1],
                snapshot_at=datetime.combine(
                    sessions[-1], datetime.min.time(), tzinfo=UTC
                )
                + timedelta(hours=21),
                valuations=(_valuation(),),
            )


def test_policy_extra_and_authority_drift_are_rejected(tmp_path: Path) -> None:
    payload = _default_policy_payload()
    payload["unexpected"] = True
    extra = tmp_path / "extra.yaml"
    extra.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(QQQOptionCashAccountingContractError):
        load_qqq_options_cash_accounting_policy(extra)

    payload = _default_policy_payload()
    payload["shared_contract_sha256"] = _SHA_A
    drift = tmp_path / "drift.yaml"
    drift.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(QQQOptionCashAccountingContractError, match="exact shared contract"):
        load_qqq_options_cash_accounting_policy(drift)


def test_active_policy_cannot_claim_synthetic_reality_baseline(tmp_path: Path) -> None:
    path = _active_policy_path(
        tmp_path,
        scenario_role="SYNTHETIC_TEST_ONLY",
        reality_baseline=True,
    )

    with pytest.raises(QQQOptionCashAccountingContractError, match="reality baseline"):
        load_qqq_options_cash_accounting_policy(path)


def test_result_canonical_replay_rejects_noncanonical_and_semantic_tamper(
    tmp_path: Path,
) -> None:
    policy_path = _active_policy_path(tmp_path)
    execution = _execution_result(suffix="canonical", contracts=1)
    result = replay_qqq_option_cash_accounting(
        _request((execution,), valuations=(_valuation(),)),
        policy_path=policy_path,
    )

    assert QQQOptionCashAccountingResult.from_json_bytes(result.canonical_bytes) == result
    noncanonical = json.dumps(
        result.model_dump(mode="json"), sort_keys=False
    ).encode("utf-8")
    with pytest.raises(QQQOptionCashAccountingContractError, match="NOT_CANONICAL"):
        QQQOptionCashAccountingResult.from_json_bytes(noncanonical)

    payload = result.model_dump(mode="json")
    payload["portfolio_snapshot"]["settled_cash_usd"] = "1.00"
    tampered = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode("utf-8")
    with pytest.raises(QQQOptionCashAccountingContractError):
        QQQOptionCashAccountingResult.from_json_bytes(tampered)


def test_source_checksum_changes_input_and_result_identity(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    execution = _execution_result(suffix="source-hash", contracts=1)
    original = _request((execution,), valuations=(_valuation(),))
    changed = original.model_copy(
        update={"exchange_calendar_source_sha256": _SHA_C}
    )
    changed = QQQOptionCashAccountingRequest.model_validate(changed.model_dump())

    original_result = replay_qqq_option_cash_accounting(
        original, policy_path=policy_path
    )
    changed_result = replay_qqq_option_cash_accounting(
        changed, policy_path=policy_path
    )

    assert original_result.input_sha256 != changed_result.input_sha256
    assert original_result.content_sha256 != changed_result.content_sha256


def test_golden_accounting_identity_is_stable(tmp_path: Path) -> None:
    policy_path = _active_policy_path(tmp_path)
    execution = _execution_result(
        suffix="golden",
        contracts=2,
        multiplier=50,
        fill_specs=(("5.30", 1, "0.65"), ("5.20", 1, "0.65")),
    )
    request = _request((execution,), valuations=(_valuation(),))
    result = replay_qqq_option_cash_accounting(request, policy_path=policy_path)

    assert result.reason_code == "ACCOUNTING_REPLAY_READY"
    assert result.content_sha256 == (
        "096ad627efffce28e961527a243194b883c8b3889e8fdd3400dcc8f0d36eb836"
    )
    assert _sha(result.canonical_bytes) == (
        "f7b38ab361fdf7c24cbcec97581b0c052612c41874b2c0f5552f604ac0652caa"
    )
    assert isinstance(
        load_qqq_options_cash_accounting_policy(policy_path).policy.criteria,
        ActiveCashAccountingCriteria,
    )
