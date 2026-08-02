from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import permutations
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.cash_accounting import (
    DEFAULT_QQQ_OPTIONS_CASH_ACCOUNTING_POLICY_PATH,
    QQQOptionCashAccountingRequest,
    QQQOptionCashAccountingResult,
    QQQOptionIntentAccountingInput,
    QQQOptionValuationQuoteInput,
    load_qqq_options_cash_accounting_policy,
    replay_qqq_option_cash_accounting,
)
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    ContractCandidateSnapshotRecord,
    DQCheckResult,
    DQReportRecord,
    FillEventRecord,
    OrderEventRecord,
    OrderIntentRecord,
    QQQOptionsSafetyBoundary,
    RunManifestRecord,
)
from ai_trading_system.qqq_options_research.minute_execution import (
    QQQOptionExecutionResult,
)
from ai_trading_system.qqq_options_research.position_lifecycle import (
    DEFAULT_QQQ_OPTIONS_POSITION_LIFECYCLE_POLICY_PATH,
    ActivePositionLifecycleCriteria,
    QQQOptionCandidateSnapshotArtifact,
    QQQOptionExecutionResultArtifact,
    QQQOptionLifecycleExternalEvent,
    QQQOptionLifecycleMarketObservation,
    QQQOptionPositionLifecycleContractError,
    QQQOptionPositionLifecycleRequest,
    QQQOptionPositionLifecycleResult,
    UnresolvedPositionLifecycleCriteria,
    build_qqq_option_position_lifecycle_input_sha256,
    load_qqq_options_position_lifecycle_policy,
    replay_qqq_option_position_lifecycle,
)

_REPOSITORY_SHA = "e" * 40
_SELECTION_POLICY_SHA = (
    "bbb51a147e89dd279f35ed005810b7274c1ac2ff302df492c183e2f7f2abad30"
)
_EXECUTION_POLICY_SHA = (
    "8c8823ddcc509e7dfdb81803a6fe7099b1ff44fccefc5a607c2a9abc7875226a"
)
_DQ_POLICY_SHA = (
    "1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358"
)
_RUN_ID = "run-20210222-position-lifecycle"
_OPTION_SID = "QQQ-20210319-C-100"
_REQUESTED_START = date(2021, 2, 22)
_REQUESTED_END = date(2021, 3, 31)
_MANIFEST_AT = datetime(2021, 2, 22, 13, 0, tzinfo=UTC)
_SELECTION_AT = datetime(2021, 3, 1, 14, 30, tzinfo=UTC)
_INTENT_AT = datetime(2021, 3, 1, 15, 0, tzinfo=UTC)
_EXPIRY = date(2021, 3, 19)
_SESSIONS = tuple(
    date(2021, 2, 22) + timedelta(days=index)
    for index in range(38)
    if (date(2021, 2, 22) + timedelta(days=index)).weekday() < 5
)


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
    policy_sha256: str,
    dq_status: str = "PASS",
    pit_status: str = "PASS",
    source_ids: tuple[str, ...] | None = None,
    source_checksums: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    return {
        "schema_name": schema_name,
        "schema_version": "1.0.0",
        "run_id": _RUN_ID,
        "record_id": record_id,
        "created_at_utc": created_at_utc,
        "producer_version": "test.position-lifecycle.v1",
        "repository_code_sha": _REPOSITORY_SHA,
        "policy_id": f"synthetic_{schema_name}_policy_v1",
        "policy_version": "1.0.0",
        "policy_sha256": policy_sha256,
        "contract_schema_sha256": QQQ_OPTIONS_CONTRACT_SHA256,
        "source_ids": source_ids or (f"qqq.options.lifecycle.synthetic.{suffix}",),
        "source_checksums": source_checksums or (_sha(f"source-{suffix}"),),
        "requested_start": _REQUESTED_START,
        "requested_end": _REQUESTED_END,
        "evaluated_start": _REQUESTED_START,
        "evaluated_end": _REQUESTED_END,
        "storage_timezone": "UTC",
        "exchange_timezone": "America/New_York",
        "dq_status": dq_status,
        "pit_status": pit_status,
        "export_classification": "EXPORT_ALLOWED_DERIVED",
        "lineage_id": f"lineage-{suffix}",
        "safety": _safety(),
    }


def _manifest() -> RunManifestRecord:
    return RunManifestRecord.seal(
        **_envelope(
            schema_name="run_manifest",
            record_id="run-manifest-position-lifecycle",
            created_at_utc=_MANIFEST_AT,
            suffix="manifest",
            policy_sha256=_sha("manifest-policy"),
        ),
        underlying="QQQ",
        initial_cash_usd=Decimal("10000.00"),
        account_currency="USD",
        account_type="CASH",
        signal_resolution="DAILY",
        execution_resolution="MINUTE",
        signal_artifact_sha256=_sha("signal-artifact"),
        engine_identity_status="UNKNOWN",
        engine_identity=None,
        evidence_admission_decision="CAPABILITY_OR_LICENSE_BLOCKED",
    )


def _candidate(*, right: str = "CALL") -> ContractCandidateSnapshotRecord:
    return ContractCandidateSnapshotRecord.seal(
        **_envelope(
            schema_name="contract_candidate_snapshot",
            record_id=f"candidate-{right.lower()}",
            created_at_utc=_SELECTION_AT,
            suffix=f"candidate.{right.lower()}",
            policy_sha256=_SELECTION_POLICY_SHA,
        ),
        selection_snapshot_utc=_SELECTION_AT,
        option_sid=_OPTION_SID if right == "CALL" else "QQQ-20210319-P-100",
        right=right,
        expiry=_EXPIRY,
        strike_usd_per_share=Decimal("100.00"),
        contract_multiplier=100,
        dte=18,
        moneyness=Decimal("1.00"),
        prior_day_model_as_of_session=date(2021, 2, 26),
        open_interest_as_of_session=date(2021, 2, 26),
        quote_bid_per_share=Decimal("1.90"),
        quote_ask_per_share=Decimal("2.00"),
        quote_end_utc=_SELECTION_AT - timedelta(seconds=30),
        quote_validity="VALID",
        eligible=True,
        field_export_classification="EXPORT_ALLOWED_DERIVED",
    )


def _execution_result(
    candidate: ContractCandidateSnapshotRecord,
    *,
    contracts: int = 1,
    filled_contracts: int = 1,
    side: str = "BUY_TO_OPEN",
    suffix: str = "open",
    intent_at: datetime = _INTENT_AT,
) -> QQQOptionExecutionResult:
    limit_price = Decimal("2.10") if side == "BUY_TO_OPEN" else Decimal("1.40")
    fill_price = Decimal("2.00") if side == "BUY_TO_OPEN" else Decimal("1.50")
    reservation = (
        limit_price * 100 * contracts + Decimal(contracts)
        if side == "BUY_TO_OPEN"
        else Decimal("0")
    )
    intent = OrderIntentRecord.seal(
        **_envelope(
            schema_name="order_intent",
            record_id=f"intent-lifecycle-{suffix}",
            created_at_utc=intent_at,
            suffix=f"intent.{suffix}",
            policy_sha256=_EXECUTION_POLICY_SHA,
        ),
        intent_id=f"intent-lifecycle-{suffix}",
        decision_id=f"selection-lifecycle-{suffix}",
        option_sid=candidate.option_sid,
        side=side,
        contracts=contracts,
        order_type="MARKETABLE_LIMIT",
        limit_price_per_share=limit_price,
        reserved_cash_usd=reservation,
        not_before_utc=intent_at + timedelta(seconds=30),
    )
    events = [
        OrderEventRecord.seal(
            **_envelope(
                schema_name="order_event",
                record_id=f"event-{suffix}-0",
                created_at_utc=intent_at,
                suffix=f"event.{suffix}.0",
                policy_sha256=_EXECUTION_POLICY_SHA,
            ),
            platform_order_id=f"order-lifecycle-{suffix}",
            event_sequence=0,
            event_type="CREATED",
            event_at_utc=intent_at,
            side=side,
            order_contracts=contracts,
            filled_contracts_total=0,
            limit_price_per_share=limit_price,
            reason_code=None,
        ),
        OrderEventRecord.seal(
            **_envelope(
                schema_name="order_event",
                record_id=f"event-{suffix}-1",
                created_at_utc=intent_at + timedelta(minutes=1),
                suffix=f"event.{suffix}.1",
                policy_sha256=_EXECUTION_POLICY_SHA,
            ),
            platform_order_id=f"order-lifecycle-{suffix}",
            event_sequence=1,
            event_type="SUBMITTED",
            event_at_utc=intent_at + timedelta(minutes=1),
            side=side,
            order_contracts=contracts,
            filled_contracts_total=0,
            limit_price_per_share=limit_price,
            reason_code=None,
        ),
    ]
    fills: list[FillEventRecord] = []
    if filled_contracts:
        fill_at = intent_at + timedelta(minutes=2)
        fills.append(
            FillEventRecord.seal(
                **_envelope(
                    schema_name="fill_event",
                    record_id=f"fill-{suffix}-1",
                    created_at_utc=fill_at,
                    suffix=f"fill.{suffix}.1",
                    policy_sha256=_EXECUTION_POLICY_SHA,
                ),
                platform_order_id=f"order-lifecycle-{suffix}",
                fill_sequence=1,
                fill_at_utc=fill_at,
                quote_end_utc=fill_at - timedelta(seconds=30),
                side=side,
                filled_contracts=filled_contracts,
                fill_price_per_share=fill_price,
                contract_multiplier=100,
                fee_usd=Decimal(str(filled_contracts)),
                settlement_currency="USD",
                quote_side="ASK" if side == "BUY_TO_OPEN" else "BID",
                gross_cash_delta_usd=(
                    (-1 if side == "BUY_TO_OPEN" else 1)
                    * fill_price
                    * 100
                    * filled_contracts
                ),
            )
        )
        events.append(
            OrderEventRecord.seal(
                **_envelope(
                    schema_name="order_event",
                    record_id=f"event-{suffix}-2",
                    created_at_utc=fill_at,
                    suffix=f"event.{suffix}.2",
                    policy_sha256=_EXECUTION_POLICY_SHA,
                ),
                platform_order_id=f"order-lifecycle-{suffix}",
                event_sequence=2,
                event_type="FILLED" if filled_contracts == contracts else "PARTIALLY_FILLED",
                event_at_utc=fill_at,
                side=side,
                order_contracts=contracts,
                filled_contracts_total=filled_contracts,
                limit_price_per_share=limit_price,
                reason_code=None,
            )
        )
    if filled_contracts < contracts:
        canceled_at = intent_at + timedelta(minutes=3)
        events.append(
            OrderEventRecord.seal(
                **_envelope(
                    schema_name="order_event",
                    record_id=f"event-{suffix}-3",
                    created_at_utc=canceled_at,
                    suffix=f"event.{suffix}.3",
                    policy_sha256=_EXECUTION_POLICY_SHA,
                ),
                platform_order_id=f"order-lifecycle-{suffix}",
                event_sequence=len(events),
                event_type="CANCELED",
                event_at_utc=canceled_at,
                side=side,
                order_contracts=contracts,
                filled_contracts_total=filled_contracts,
                limit_price_per_share=limit_price,
                reason_code="CANCEL_TIMEOUT",
            )
        )
    reason = (
        "FILLED"
        if filled_contracts == contracts
        else "PARTIAL_CANCELED"
        if filled_contracts
        else "NO_FILL_CANCELED"
    )
    return QQQOptionExecutionResult.seal(
        schema_version="qqq_options_minute_execution_result.v1",
        policy_sha256=_EXECUTION_POLICY_SHA,
        selection_policy_sha256=_SELECTION_POLICY_SHA,
        selection_decision_sha256=_sha(f"selection-decision-{suffix}"),
        quote_set_sha256=_sha(f"execution-quotes-{suffix}"),
        execution_authorized=True,
        selection_authorized=True,
        cash_preservation_required=not filled_contracts,
        reason_code=reason,
        execution_stage_dq_status="PASS" if filled_contracts else "NOT_EVALUATED",
        global_dq_status="PASS" if filled_contracts else "NOT_EVALUATED",
        global_pit_status="PASS" if filled_contracts else "NOT_EVALUATED",
        accounting_status="NOT_EVALUATED",
        order_intent=intent,
        order_events=tuple(events),
        fill_events=tuple(fills),
        execution_dq_reports=(),
    )


def _default_policy_payload(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load((PROJECT_ROOT / path).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _active_cash_policy_path(tmp_path: Path) -> Path:
    payload = _default_policy_payload(DEFAULT_QQQ_OPTIONS_CASH_ACCOUNTING_POLICY_PATH)
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
        "scenario_role": "SYNTHETIC_TEST_ONLY",
        "dq_caveat": "Synthetic lifecycle arithmetic fixture only.",
        "approved_initial_cash_usd": "10000.00",
        "premium_budget_usd": "5000.00",
        "max_contracts_per_order": 5,
        "fee_buffer_per_contract_usd": "1.00",
        "sell_proceeds_settlement_lag_sessions": 1,
        "max_valuation_quote_age_ms": 120000,
        "cost_basis_method": "FIFO",
        "include_fees_in_cost_basis": True,
        "cash_quantum_usd": "0.01",
        "rounding_mode": "ROUND_HALF_EVEN",
        "reality_baseline": False,
    }
    path = tmp_path / "active-cash-accounting.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _active_lifecycle_policy_path(
    tmp_path: Path,
    *,
    accounting_policy_sha256: str,
) -> Path:
    payload = _default_policy_payload(
        DEFAULT_QQQ_OPTIONS_POSITION_LIFECYCLE_POLICY_PATH
    )
    payload.update(
        {
            "status": "OWNER_REVIEWED_ACTIVE",
            "owner": "synthetic_test_fixture_only",
            "owner_decision": "synthetic_test_fixture_only:not_project_authority",
            "lifecycle_authorized": True,
            "accounting_policy_sha256": accounting_policy_sha256,
        }
    )
    payload["criteria"] = {
        "mode": "ACTIVE",
        "scenario_role": "SYNTHETIC_TEST_ONLY",
        "dq_caveat": "Synthetic lifecycle safety fixture only.",
        "pre_expiry_guard_sessions": 1,
        "max_exit_quote_age_ms": 60000,
        "expiry_settlement_source_policy": "REVIEWED_EVENT_DQ_REPORT",
        "scope_violation_disposition": "INVALIDATE_RUN_WITHOUT_DELIVERY",
        "reality_baseline": False,
    }
    path = tmp_path / "active-position-lifecycle.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _accounting_request(
    manifest: RunManifestRecord,
    execution: QQQOptionExecutionResult | tuple[QQQOptionExecutionResult, ...],
    *,
    evaluation_at: datetime,
    as_of_session: date,
) -> QQQOptionCashAccountingRequest:
    executions = execution if isinstance(execution, tuple) else (execution,)
    result_bytes = tuple(item.canonical_bytes for item in executions)
    intents = tuple(item.order_intent for item in executions)
    assert all(intent is not None for intent in intents)
    admitted_intents = tuple(intent for intent in intents if intent is not None)
    open_quantity = sum(
        fill.filled_contracts
        * (1 if fill.side == "BUY_TO_OPEN" else -1)
        for item in executions
        for fill in item.fill_events
    )
    valuations = ()
    if open_quantity:
        valuations = (
            QQQOptionValuationQuoteInput(
                option_sid=admitted_intents[0].option_sid,
                source_id="qqq.options.valuation.synthetic.lifecycle",
                source_sha256=_sha(f"valuation-{evaluation_at.isoformat()}"),
                quote_end_utc=evaluation_at - timedelta(minutes=1),
                resolution="MINUTE",
                bid_per_share=Decimal("1.50"),
                ask_per_share=Decimal("1.60"),
            ),
        )
    return QQQOptionCashAccountingRequest(
        run_manifest_bytes=manifest.canonical_bytes,
        run_manifest_file_sha256=_sha(manifest.canonical_bytes),
        execution_result_bytes=result_bytes,
        execution_result_file_sha256s=tuple(_sha(item) for item in result_bytes),
        intent_accounting_inputs=tuple(
            QQQOptionIntentAccountingInput(
                intent_content_sha256=intent.content_sha256,
                contract_multiplier=100,
                source_id=f"qqq.options.multiplier.synthetic.{intent.intent_id}",
                source_sha256=_sha(f"multiplier-100-{intent.intent_id}"),
            )
            for intent in admitted_intents
        ),
        snapshot_at_utc=evaluation_at,
        as_of_session=as_of_session,
        exchange_sessions=_SESSIONS,
        exchange_calendar_source_id="xnys.reviewed.synthetic.lifecycle",
        exchange_calendar_source_sha256=_sha(
            "|".join(item.isoformat() for item in _SESSIONS)
        ),
        valuation_quotes=valuations,
        producer_version="test.position-lifecycle.v1",
        lineage_id="cash-accounting-lineage-20210222",
    )


def _dq_report(
    manifest: RunManifestRecord,
    *,
    source_id: str,
    source_sha256: str,
    observed_at: datetime,
    status: str = "PASS",
) -> DQReportRecord:
    return DQReportRecord.seal(
        **_envelope(
            schema_name="dq_report",
            record_id=f"dq-report-{source_id.rsplit('.', maxsplit=1)[-1]}",
            created_at_utc=observed_at,
            suffix=f"dq.{source_id}",
            policy_sha256=_DQ_POLICY_SHA,
            dq_status=status,
            pit_status=status,
            source_ids=(source_id,),
            source_checksums=(source_sha256,),
        ),
        scope="qqq_options_event_dq_pit_identity",
        report_version="1.0",
        generated_at_utc=observed_at,
        checks=(
            DQCheckResult(
                check_id="lifecycle_event_source",
                status=status,
                reason_code=None if status == "PASS" else f"SOURCE_{status}",
                observed_at_utc=observed_at,
            ),
        ),
    )


def _observation(
    manifest: RunManifestRecord,
    *,
    candidate: ContractCandidateSnapshotRecord,
    kind: str,
    observed_at: datetime,
    effective_session: date,
    status: str = "PASS",
    underlying: str = "99.00",
) -> QQQOptionLifecycleMarketObservation:
    source_id = f"qqq.options.lifecycle.observation.{kind.lower()}"
    source_sha = _sha(f"{source_id}-{observed_at.isoformat()}-{status}-{underlying}")
    report = _dq_report(
        manifest,
        source_id=source_id,
        source_sha256=source_sha,
        observed_at=observed_at,
        status=status,
    )
    return QQQOptionLifecycleMarketObservation(
        source_id=source_id,
        source_sha256=source_sha,
        dq_report_bytes=report.canonical_bytes,
        dq_report_file_sha256=_sha(report.canonical_bytes),
        observation_id=f"observation-{kind.lower()}",
        observation_kind=kind,
        option_sid=candidate.option_sid,
        observed_at_utc=observed_at,
        effective_session=effective_session,
        bid_per_share=Decimal("1.00") if kind == "EXIT_QUOTE" else None,
        ask_per_share=Decimal("1.10") if kind == "EXIT_QUOTE" else None,
        underlying_price_usd_per_share=(
            None if kind == "EXIT_QUOTE" else Decimal(underlying)
        ),
    )


def _external_event(
    manifest: RunManifestRecord,
    *,
    candidate: ContractCandidateSnapshotRecord,
    event_type: str,
    occurred_at: datetime,
) -> QQQOptionLifecycleExternalEvent:
    source_id = f"qqq.options.lifecycle.external.{event_type.lower()}"
    source_sha = _sha(f"{source_id}-{occurred_at.isoformat()}")
    report = _dq_report(
        manifest,
        source_id=source_id,
        source_sha256=source_sha,
        observed_at=occurred_at,
    )
    return QQQOptionLifecycleExternalEvent(
        source_id=source_id,
        source_sha256=source_sha,
        dq_report_bytes=report.canonical_bytes,
        dq_report_file_sha256=_sha(report.canonical_bytes),
        event_id=f"external-{event_type.lower()}",
        event_type=event_type,
        option_sid=candidate.option_sid,
        occurred_at_utc=occurred_at,
        effective_session=occurred_at.date(),
        contracts=1 if event_type in {"EXERCISE", "ASSIGNMENT"} else None,
    )


def _fixture(
    tmp_path: Path,
    *,
    evaluation_at: datetime,
    as_of_session: date,
    right: str = "CALL",
    contracts: int = 1,
    filled_contracts: int = 1,
) -> tuple[
    RunManifestRecord,
    ContractCandidateSnapshotRecord,
    QQQOptionExecutionResult,
    QQQOptionCashAccountingResult,
    Path,
]:
    manifest = _manifest()
    candidate = _candidate(right=right)
    execution = _execution_result(
        candidate,
        contracts=contracts,
        filled_contracts=filled_contracts,
    )
    cash_policy_path = _active_cash_policy_path(tmp_path)
    accounting = replay_qqq_option_cash_accounting(
        _accounting_request(
            manifest,
            execution,
            evaluation_at=evaluation_at,
            as_of_session=as_of_session,
        ),
        policy_path=cash_policy_path,
    )
    assert accounting.reason_code == "ACCOUNTING_REPLAY_READY"
    cash_policy_sha = load_qqq_options_cash_accounting_policy(
        cash_policy_path
    ).policy_sha256
    lifecycle_path = _active_lifecycle_policy_path(
        tmp_path,
        accounting_policy_sha256=cash_policy_sha,
    )
    return manifest, candidate, execution, accounting, lifecycle_path


def _request(
    manifest: RunManifestRecord,
    candidate: ContractCandidateSnapshotRecord,
    execution: QQQOptionExecutionResult | tuple[QQQOptionExecutionResult, ...],
    accounting: QQQOptionCashAccountingResult,
    *,
    evaluation_at: datetime,
    as_of_session: date,
    observations: tuple[QQQOptionLifecycleMarketObservation, ...] = (),
    external_events: tuple[QQQOptionLifecycleExternalEvent, ...] = (),
) -> QQQOptionPositionLifecycleRequest:
    executions = execution if isinstance(execution, tuple) else (execution,)
    execution_bytes = tuple(item.canonical_bytes for item in executions)
    candidate_bytes = candidate.canonical_bytes
    return QQQOptionPositionLifecycleRequest(
        run_manifest_bytes=manifest.canonical_bytes,
        run_manifest_file_sha256=_sha(manifest.canonical_bytes),
        cash_accounting_result_bytes=accounting.canonical_bytes,
        cash_accounting_result_file_sha256=_sha(accounting.canonical_bytes),
        execution_artifacts=tuple(
            QQQOptionExecutionResultArtifact(
                content=item,
                file_sha256=_sha(item),
            )
            for item in execution_bytes
        ),
        candidate_artifacts=(
            QQQOptionCandidateSnapshotArtifact(
                content=candidate_bytes,
                file_sha256=_sha(candidate_bytes),
            ),
        ),
        observations=observations,
        external_events=external_events,
        evaluation_at_utc=evaluation_at,
        as_of_session=as_of_session,
        exchange_sessions=_SESSIONS,
        exchange_calendar_source_id="xnys.reviewed.synthetic.lifecycle",
        exchange_calendar_source_sha256=_sha(
            "|".join(item.isoformat() for item in _SESSIONS)
        ),
        producer_version="test.position-lifecycle.v1",
        lineage_id="position-lifecycle-lineage-20210222",
    )


def test_default_policy_is_exact_unresolved_and_unauthorized() -> None:
    loaded = load_qqq_options_position_lifecycle_policy()

    assert loaded.policy.status == "OWNER_REVIEW_REQUIRED_BASELINE"
    assert loaded.policy.lifecycle_authorized is False
    assert isinstance(loaded.policy.criteria, UnresolvedPositionLifecycleCriteria)
    assert loaded.policy.primary_research_start == date(2021, 2, 22)
    assert loaded.policy.legacy_non_default_start_is_default is False
    assert loaded.policy.safety.underlying_share_delivery_allowed is False
    assert loaded.policy.safety.new_order_intent_allowed is False


def test_default_policy_returns_typed_cash_preservation_result(tmp_path: Path) -> None:
    evaluation = datetime(2021, 3, 10, 21, 0, tzinfo=UTC)
    fixture = _fixture(tmp_path, evaluation_at=evaluation, as_of_session=date(2021, 3, 10))
    request = _request(
        *fixture[:4],
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 10),
    )

    result = replay_qqq_option_position_lifecycle(request)

    assert result.reason_code == "LIFECYCLE_POLICY_REVIEW_REQUIRED"
    assert result.lifecycle_authorized is False
    assert result.cash_preservation_required is True
    assert result.run_valid is False
    assert result.lifecycle_events == ()
    assert result.portfolio_snapshot is None
    assert result.new_order_intent_count == result.new_fill_count == 0


@pytest.mark.parametrize(
    ("contracts", "filled_contracts", "terminal_state"),
    ((1, 1, "OPEN"), (2, 1, "OPEN_PARTIAL")),
)
def test_open_fill_replay_uses_shared_events_and_preserves_accounting(
    tmp_path: Path,
    contracts: int,
    filled_contracts: int,
    terminal_state: str,
) -> None:
    evaluation = datetime(2021, 3, 10, 21, 0, tzinfo=UTC)
    manifest, candidate, execution, accounting, lifecycle_path = _fixture(
        tmp_path,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 10),
        contracts=contracts,
        filled_contracts=filled_contracts,
    )
    request = _request(
        manifest,
        candidate,
        execution,
        accounting,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 10),
    )

    result = replay_qqq_option_position_lifecycle(request, policy_path=lifecycle_path)

    assert result.reason_code == "LIFECYCLE_REPLAY_READY"
    assert result.run_valid is True
    assert tuple(event.prior_state for event in result.lifecycle_events) == (
        "FLAT",
        "INTENT_PENDING",
    )
    assert tuple(event.next_state for event in result.lifecycle_events) == (
        "INTENT_PENDING",
        terminal_state,
    )
    assert result.positions[0].terminal_state == terminal_state
    assert result.positions[0].contracts_open == filled_contracts
    assert result.portfolio_snapshot is not None
    assert accounting.portfolio_snapshot is not None
    assert result.portfolio_snapshot.settled_cash_usd == (
        accounting.portfolio_snapshot.settled_cash_usd
    )
    assert result.new_order_intent_count == result.new_fill_count == 0


def test_no_fill_execution_remains_accounting_blocked_without_lifecycle_state(
    tmp_path: Path,
) -> None:
    evaluation = datetime(2021, 3, 10, 21, 0, tzinfo=UTC)
    manifest = _manifest()
    candidate = _candidate()
    execution = _execution_result(candidate, filled_contracts=0)
    cash_policy_path = _active_cash_policy_path(tmp_path)
    accounting = replay_qqq_option_cash_accounting(
        _accounting_request(
            manifest,
            execution,
            evaluation_at=evaluation,
            as_of_session=date(2021, 3, 10),
        ),
        policy_path=cash_policy_path,
    )
    assert accounting.reason_code == "EXECUTION_BLOCKED_CASH_PRESERVED"
    lifecycle_path = _active_lifecycle_policy_path(
        tmp_path,
        accounting_policy_sha256=load_qqq_options_cash_accounting_policy(
            cash_policy_path
        ).policy_sha256,
    )

    result = replay_qqq_option_position_lifecycle(
        _request(
            manifest,
            candidate,
            execution,
            accounting,
            evaluation_at=evaluation,
            as_of_session=date(2021, 3, 10),
        ),
        policy_path=lifecycle_path,
    )

    assert result.reason_code == "ACCOUNTING_REPLAY_BLOCKED_CASH_PRESERVED"
    assert result.lifecycle_events == ()
    assert result.positions == ()
    assert result.portfolio_snapshot is None
    assert result.new_order_intent_count == result.new_fill_count == 0


@pytest.mark.parametrize(
    ("sell_filled", "terminal_state", "contracts_open"),
    ((2, "CLOSED", 0), (1, "EXIT_BLOCKED", 1)),
)
def test_sell_to_close_full_or_partial_replays_existing_execution_only(
    tmp_path: Path,
    sell_filled: int,
    terminal_state: str,
    contracts_open: int,
) -> None:
    evaluation = datetime(2021, 3, 10, 21, 0, tzinfo=UTC)
    manifest = _manifest()
    candidate = _candidate()
    buy = _execution_result(candidate, contracts=2, filled_contracts=2)
    sell = _execution_result(
        candidate,
        contracts=2,
        filled_contracts=sell_filled,
        side="SELL_TO_CLOSE",
        suffix="close",
        intent_at=datetime(2021, 3, 5, 15, 0, tzinfo=UTC),
    )
    cash_policy_path = _active_cash_policy_path(tmp_path)
    accounting = replay_qqq_option_cash_accounting(
        _accounting_request(
            manifest,
            (buy, sell),
            evaluation_at=evaluation,
            as_of_session=date(2021, 3, 10),
        ),
        policy_path=cash_policy_path,
    )
    assert accounting.reason_code == "ACCOUNTING_REPLAY_READY"
    lifecycle_path = _active_lifecycle_policy_path(
        tmp_path,
        accounting_policy_sha256=load_qqq_options_cash_accounting_policy(
            cash_policy_path
        ).policy_sha256,
    )

    result = replay_qqq_option_position_lifecycle(
        _request(
            manifest,
            candidate,
            (buy, sell),
            accounting,
            evaluation_at=evaluation,
            as_of_session=date(2021, 3, 10),
        ),
        policy_path=lifecycle_path,
    )

    assert result.reason_code == "LIFECYCLE_REPLAY_READY"
    assert result.positions[0].terminal_state == terminal_state
    assert result.positions[0].contracts_open == contracts_open
    assert tuple(item.next_state for item in result.lifecycle_events) == (
        "INTENT_PENDING",
        "OPEN",
        "EXIT_PENDING",
        terminal_state,
    )
    assert result.portfolio_snapshot is not None
    assert accounting.portfolio_snapshot is not None
    assert result.portfolio_snapshot.settled_cash_usd == (
        accounting.portfolio_snapshot.settled_cash_usd
    )
    assert result.new_order_intent_count == result.new_fill_count == 0


@pytest.mark.parametrize(
    ("quote_mode", "expected"),
    (
        ("FRESH", "EXIT_PENDING"),
        ("MISSING", "EXIT_BLOCKED"),
        ("STALE", "EXIT_BLOCKED"),
        ("DQ_FAIL", "EXIT_BLOCKED"),
    ),
)
def test_pre_expiry_guard_requires_a_fresh_dq_pass_quote(
    tmp_path: Path,
    quote_mode: str,
    expected: str,
) -> None:
    evaluation = datetime(2021, 3, 18, 20, 0, tzinfo=UTC)
    manifest, candidate, execution, accounting, lifecycle_path = _fixture(
        tmp_path,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 18),
    )
    observations = ()
    if quote_mode != "MISSING":
        observations = (
            _observation(
                manifest,
                candidate=candidate,
                kind="EXIT_QUOTE",
                observed_at=(
                    evaluation - timedelta(minutes=2)
                    if quote_mode == "STALE"
                    else evaluation - timedelta(seconds=30)
                ),
                effective_session=date(2021, 3, 18),
                status="FAIL" if quote_mode == "DQ_FAIL" else "PASS",
            ),
        )
    request = _request(
        manifest,
        candidate,
        execution,
        accounting,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 18),
        observations=observations,
    )

    result = replay_qqq_option_position_lifecycle(request, policy_path=lifecycle_path)

    assert result.reason_code == (
        "PRE_EXPIRY_EXIT_REQUIRED"
        if quote_mode == "FRESH"
        else "PRE_EXPIRY_EXIT_BLOCKED"
    )
    assert result.positions[0].terminal_state == expected
    assert result.new_order_intent_count == result.new_fill_count == 0


def test_otm_expiry_closes_worthless_without_cash_or_underlying_delivery(
    tmp_path: Path,
) -> None:
    evaluation = datetime(2021, 3, 19, 21, 0, tzinfo=UTC)
    manifest, candidate, execution, accounting, lifecycle_path = _fixture(
        tmp_path,
        evaluation_at=evaluation,
        as_of_session=_EXPIRY,
    )
    settlement = _observation(
        manifest,
        candidate=candidate,
        kind="EXPIRY_SETTLEMENT",
        observed_at=evaluation - timedelta(minutes=1),
        effective_session=_EXPIRY,
        underlying="99.00",
    )
    request = _request(
        manifest,
        candidate,
        execution,
        accounting,
        evaluation_at=evaluation,
        as_of_session=_EXPIRY,
        observations=(settlement,),
    )

    result = replay_qqq_option_position_lifecycle(request, policy_path=lifecycle_path)

    assert result.reason_code == "EXPIRY_CLOSED_WORTHLESS"
    assert result.positions[0].terminal_state == "CLOSED"
    assert result.positions[0].contracts_open == 0
    assert result.positions[0].remaining_cost_basis_usd == Decimal("0")
    assert result.portfolio_snapshot is not None
    assert accounting.portfolio_snapshot is not None
    assert result.portfolio_snapshot.settled_cash_usd == (
        accounting.portfolio_snapshot.settled_cash_usd
    )
    assert result.portfolio_snapshot.option_market_value_usd == Decimal("0")
    assert result.portfolio_snapshot.realized_pnl_usd == Decimal("-201")
    assert result.portfolio_snapshot.unrealized_pnl_usd == Decimal("0")
    assert result.new_order_intent_count == result.new_fill_count == 0


@pytest.mark.parametrize(
    ("right", "settlement_price"),
    (("CALL", "101.00"), ("PUT", "99.00")),
)
def test_itm_expiry_invalidates_without_publishing_downstream_snapshot(
    tmp_path: Path,
    right: str,
    settlement_price: str,
) -> None:
    evaluation = datetime(2021, 3, 19, 21, 0, tzinfo=UTC)
    manifest, candidate, execution, accounting, lifecycle_path = _fixture(
        tmp_path,
        evaluation_at=evaluation,
        as_of_session=_EXPIRY,
        right=right,
    )
    settlement = _observation(
        manifest,
        candidate=candidate,
        kind="EXPIRY_SETTLEMENT",
        observed_at=evaluation - timedelta(minutes=1),
        effective_session=_EXPIRY,
        underlying=settlement_price,
    )

    result = replay_qqq_option_position_lifecycle(
        _request(
            manifest,
            candidate,
            execution,
            accounting,
            evaluation_at=evaluation,
            as_of_session=_EXPIRY,
            observations=(settlement,),
        ),
        policy_path=lifecycle_path,
    )

    assert result.reason_code == "EXPIRY_SCOPE_VIOLATION_INVALID_RUN"
    assert result.run_valid is False
    assert result.cash_preservation_required is True
    assert result.positions[0].terminal_state == "INVALID_RUN"
    assert result.portfolio_snapshot is None
    assert result.new_order_intent_count == result.new_fill_count == 0


@pytest.mark.parametrize("status", ("FAIL", "NOT_EVALUATED"))
def test_expiry_dq_fail_or_not_evaluated_never_closes_worthless(
    tmp_path: Path,
    status: str,
) -> None:
    evaluation = datetime(2021, 3, 19, 21, 0, tzinfo=UTC)
    manifest, candidate, execution, accounting, lifecycle_path = _fixture(
        tmp_path,
        evaluation_at=evaluation,
        as_of_session=_EXPIRY,
    )
    settlement = _observation(
        manifest,
        candidate=candidate,
        kind="EXPIRY_SETTLEMENT",
        observed_at=evaluation - timedelta(minutes=1),
        effective_session=_EXPIRY,
        status=status,
    )

    result = replay_qqq_option_position_lifecycle(
        _request(
            manifest,
            candidate,
            execution,
            accounting,
            evaluation_at=evaluation,
            as_of_session=_EXPIRY,
            observations=(settlement,),
        ),
        policy_path=lifecycle_path,
    )

    assert result.reason_code == "UNRESOLVED_EXPIRY_INVALID_RUN"
    assert result.run_valid is False
    assert result.portfolio_snapshot is None
    assert result.lifecycle_stage_dq_status == status


def test_missing_expiry_settlement_is_an_unresolved_invalid_run(
    tmp_path: Path,
) -> None:
    evaluation = datetime(2021, 3, 19, 21, 0, tzinfo=UTC)
    manifest, candidate, execution, accounting, lifecycle_path = _fixture(
        tmp_path,
        evaluation_at=evaluation,
        as_of_session=_EXPIRY,
    )

    result = replay_qqq_option_position_lifecycle(
        _request(
            manifest,
            candidate,
            execution,
            accounting,
            evaluation_at=evaluation,
            as_of_session=_EXPIRY,
        ),
        policy_path=lifecycle_path,
    )

    assert result.reason_code == "UNRESOLVED_EXPIRY_INVALID_RUN"
    assert result.run_valid is False
    assert result.positions[0].terminal_state == "INVALID_RUN"
    assert result.portfolio_snapshot is None


@pytest.mark.parametrize(
    "event_type",
    ("EXERCISE", "ASSIGNMENT", "UNDERLYING_SPLIT", "SPECIAL_DIVIDEND"),
)
def test_unexpected_exercise_assignment_and_corporate_actions_fail_closed(
    tmp_path: Path,
    event_type: str,
) -> None:
    evaluation = datetime(2021, 3, 10, 21, 0, tzinfo=UTC)
    manifest, candidate, execution, accounting, lifecycle_path = _fixture(
        tmp_path,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 10),
    )
    external = _external_event(
        manifest,
        candidate=candidate,
        event_type=event_type,
        occurred_at=evaluation - timedelta(minutes=1),
    )

    result = replay_qqq_option_position_lifecycle(
        _request(
            manifest,
            candidate,
            execution,
            accounting,
            evaluation_at=evaluation,
            as_of_session=date(2021, 3, 10),
            external_events=(external,),
        ),
        policy_path=lifecycle_path,
    )

    assert result.reason_code == "EXTERNAL_SCOPE_VIOLATION_INVALID_RUN"
    assert result.positions[0].terminal_state == "INVALID_RUN"
    assert result.run_valid is False
    assert result.portfolio_snapshot is None


def test_input_permutation_does_not_change_identity_or_replay(tmp_path: Path) -> None:
    evaluation = datetime(2021, 3, 10, 21, 0, tzinfo=UTC)
    manifest, candidate, execution, accounting, lifecycle_path = _fixture(
        tmp_path,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 10),
    )
    observations = (
        _observation(
            manifest,
            candidate=candidate,
            kind="EXIT_QUOTE",
            observed_at=evaluation - timedelta(minutes=2),
            effective_session=date(2021, 3, 10),
        ),
        _observation(
            manifest,
            candidate=candidate,
            kind="EXPIRY_SETTLEMENT",
            observed_at=evaluation - timedelta(minutes=1),
            effective_session=_EXPIRY,
        ),
    )
    identities: set[str] = set()
    outputs: set[str] = set()
    for ordered in permutations(observations):
        request = _request(
            manifest,
            candidate,
            execution,
            accounting,
            evaluation_at=evaluation,
            as_of_session=date(2021, 3, 10),
            observations=tuple(ordered),
        )
        identities.add(build_qqq_option_position_lifecycle_input_sha256(request))
        outputs.add(
            replay_qqq_option_position_lifecycle(
                request,
                policy_path=lifecycle_path,
            ).content_sha256
        )

    assert len(identities) == 1
    assert len(outputs) == 1


def test_identity_or_calendar_drift_returns_typed_no_partial_state(tmp_path: Path) -> None:
    evaluation = datetime(2021, 3, 10, 21, 0, tzinfo=UTC)
    manifest, candidate, execution, accounting, lifecycle_path = _fixture(
        tmp_path,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 10),
    )
    request = _request(
        manifest,
        candidate,
        execution,
        accounting,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 10),
    )
    drifted = request.model_copy(
        update={"exchange_calendar_source_sha256": _sha("different-calendar")},
    )

    result = replay_qqq_option_position_lifecycle(
        drifted,
        policy_path=lifecycle_path,
    )

    assert result.reason_code == "LIFECYCLE_INPUT_INVALID"
    assert result.run_valid is False
    assert result.lifecycle_events == ()
    assert result.positions == ()
    assert result.portfolio_snapshot is None


def test_dq_report_scope_or_source_mismatch_returns_typed_invalid_input(
    tmp_path: Path,
) -> None:
    evaluation = datetime(2021, 3, 18, 20, 0, tzinfo=UTC)
    manifest, candidate, execution, accounting, lifecycle_path = _fixture(
        tmp_path,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 18),
    )
    observation = _observation(
        manifest,
        candidate=candidate,
        kind="EXIT_QUOTE",
        observed_at=evaluation - timedelta(seconds=30),
        effective_session=date(2021, 3, 18),
    )
    drifted = observation.model_copy(update={"source_sha256": _sha("other-source")})
    request = _request(
        manifest,
        candidate,
        execution,
        accounting,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 18),
        observations=(drifted,),
    )

    result = replay_qqq_option_position_lifecycle(
        request,
        policy_path=lifecycle_path,
    )

    assert result.reason_code == "LIFECYCLE_INPUT_INVALID"
    assert result.lifecycle_events == ()
    assert result.portfolio_snapshot is None


def test_canonical_artifact_and_result_tampering_fail_closed(tmp_path: Path) -> None:
    evaluation = datetime(2021, 3, 10, 21, 0, tzinfo=UTC)
    manifest, candidate, execution, accounting, lifecycle_path = _fixture(
        tmp_path,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 10),
    )
    request = _request(
        manifest,
        candidate,
        execution,
        accounting,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 10),
    )

    with pytest.raises(ValidationError, match="hash differs"):
        QQQOptionExecutionResultArtifact(
            content=execution.canonical_bytes,
            file_sha256=_sha("forged"),
        )
    result = replay_qqq_option_position_lifecycle(request, policy_path=lifecycle_path)
    payload = json.loads(result.canonical_bytes)
    payload["run_valid"] = False
    with pytest.raises(QQQOptionPositionLifecycleContractError):
        QQQOptionPositionLifecycleResult.from_json_bytes(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        )


def test_active_lifecycle_preserves_blocked_accounting_without_replay(tmp_path: Path) -> None:
    evaluation = datetime(2021, 3, 10, 21, 0, tzinfo=UTC)
    manifest = _manifest()
    candidate = _candidate()
    execution = _execution_result(candidate)
    accounting = replay_qqq_option_cash_accounting(
        _accounting_request(
            manifest,
            execution,
            evaluation_at=evaluation,
            as_of_session=date(2021, 3, 10),
        )
    )
    default_cash_sha = load_qqq_options_cash_accounting_policy().policy_sha256
    lifecycle_path = _active_lifecycle_policy_path(
        tmp_path,
        accounting_policy_sha256=default_cash_sha,
    )

    result = replay_qqq_option_position_lifecycle(
        _request(
            manifest,
            candidate,
            execution,
            accounting,
            evaluation_at=evaluation,
            as_of_session=date(2021, 3, 10),
        ),
        policy_path=lifecycle_path,
    )

    assert result.reason_code == "ACCOUNTING_REPLAY_BLOCKED_CASH_PRESERVED"
    assert result.lifecycle_authorized is True
    assert result.run_valid is False
    assert result.cash_preservation_required is True
    assert result.lifecycle_events == ()


def test_primary_window_is_not_replaced_by_legacy_start(tmp_path: Path) -> None:
    evaluation = datetime(2021, 3, 10, 21, 0, tzinfo=UTC)
    manifest, candidate, execution, accounting, lifecycle_path = _fixture(
        tmp_path,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 10),
    )
    payload = manifest.model_dump(mode="python")
    payload.pop("content_sha256")
    payload["requested_start"] = date(2022, 12, 1)
    payload["requested_end"] = date(2023, 1, 31)
    payload["evaluated_start"] = date(2022, 12, 1)
    payload["evaluated_end"] = date(2023, 1, 31)
    legacy_manifest = RunManifestRecord.seal(**payload)
    request = _request(
        legacy_manifest,
        candidate,
        execution,
        accounting,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 10),
    )

    result = replay_qqq_option_position_lifecycle(request, policy_path=lifecycle_path)

    assert result.reason_code == "LIFECYCLE_INPUT_INVALID"
    assert result.run_valid is False


def test_active_policy_requires_all_reviewed_criteria(tmp_path: Path) -> None:
    payload = _default_policy_payload(
        DEFAULT_QQQ_OPTIONS_POSITION_LIFECYCLE_POLICY_PATH
    )
    payload.update(
        {
            "status": "OWNER_REVIEWED_ACTIVE",
            "lifecycle_authorized": True,
        }
    )
    path = tmp_path / "invalid-active-lifecycle.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(QQQOptionPositionLifecycleContractError):
        load_qqq_options_position_lifecycle_policy(path)


def test_result_golden_identity_is_stable(tmp_path: Path) -> None:
    evaluation = datetime(2021, 3, 10, 21, 0, tzinfo=UTC)
    manifest, candidate, execution, accounting, lifecycle_path = _fixture(
        tmp_path,
        evaluation_at=evaluation,
        as_of_session=date(2021, 3, 10),
    )
    result = replay_qqq_option_position_lifecycle(
        _request(
            manifest,
            candidate,
            execution,
            accounting,
            evaluation_at=evaluation,
            as_of_session=date(2021, 3, 10),
        ),
        policy_path=lifecycle_path,
    )

    assert result.content_sha256 == (
        "be80ffc536add3622d7d882021f8513e877c975cc6725e696755ace8e1081c46"
    )
    assert QQQOptionPositionLifecycleResult.from_json_bytes(
        result.canonical_bytes
    ) == result
    assert isinstance(
        load_qqq_options_position_lifecycle_policy(lifecycle_path).policy.criteria,
        ActivePositionLifecycleCriteria,
    )
