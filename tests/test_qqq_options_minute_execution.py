from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import permutations
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    DQCheckResult,
    DQReportRecord,
    DQStatus,
    FillEventRecord,
    OrderEventRecord,
    OrderIntentRecord,
    QQQOptionsSafetyBoundary,
    SelectionDecisionRecord,
)
from ai_trading_system.qqq_options_research.deterministic_selection import (
    DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH,
    load_qqq_option_selection_policy,
)
from ai_trading_system.qqq_options_research.dq_pit_identity import (
    QQQOptionsDQObservation,
    load_qqq_options_dq_pit_identity_policy,
)
from ai_trading_system.qqq_options_research.minute_execution import (
    DEFAULT_QQQ_OPTIONS_MINUTE_EXECUTION_POLICY_PATH,
    QQQOptionExecutionQuoteInput,
    QQQOptionExecutionRequest,
    QQQOptionExecutionResult,
    QQQOptionMinuteExecutionContractError,
    build_qqq_option_execution_quote_set_sha256,
    load_qqq_options_minute_execution_policy,
    simulate_qqq_option_minute_execution,
)

_SHA_A = "a" * 64
_SHA_B = "b" * 64
_SHA_C = "c" * 64
_SHA_D = "d" * 64
_REPOSITORY_SHA = "e" * 40
_RUN_ID = "run-20210223-execution"
_REQUESTED_START = date(2021, 2, 22)
_REQUESTED_END = date(2021, 3, 31)
_SELECTION_SESSION = date(2021, 2, 23)
_PRIOR_SESSION = date(2021, 2, 22)
_SIGNAL_AT = datetime(2021, 2, 22, 21, 0, tzinfo=UTC)
_SELECTION_AT = datetime(2021, 2, 23, 14, 31, tzinfo=UTC)
_INTENT_AT = datetime(2021, 2, 23, 14, 31, 30, tzinfo=UTC)
_FIRST_QUOTE_START = datetime(2021, 2, 23, 14, 33, tzinfo=UTC)
_SHARED_POLICY_SHA = "d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349"
_DQ_POLICY_SHA = "1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358"
_ADAPTER_POLICY_SHA = "b9e48f0b53a6259a5bbc9594cbe1929721568d1723d498591ce14b8e3be92616"
_SELECTION_STAGE_CHECKS = {
    "cache_identity",
    "chain_presence",
    "engine_identity",
    "exchange_calendar_identity",
    "fill_forward_ambiguity",
    "local_cache_dq_scope_separation",
    "open_interest_freshness",
    "prior_day_model_freshness",
    "quote_freshness",
    "quote_integrity",
    "signal_selection_chronology",
    "symbol_mapping_identity",
}


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


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


def _identity(
    identity_id: str,
    *,
    assessment: str = "PASS",
    expected_sha: str = _SHA_A,
    observed_sha: str | None = _SHA_A,
) -> dict[str, Any]:
    return {
        "assessment": assessment,
        "expected_id": identity_id,
        "expected_version": "1.0.0",
        "expected_sha256": expected_sha,
        "observed_id": identity_id if observed_sha is not None else None,
        "observed_version": "1.0.0" if observed_sha is not None else None,
        "observed_sha256": observed_sha,
    }


def _default_selection_policy_payload() -> dict[str, Any]:
    path = PROJECT_ROOT / DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _active_selection_policy_path(tmp_path: Path) -> Path:
    payload = _default_selection_policy_payload()
    payload.update(
        {
            "status": "OWNER_REVIEWED_ACTIVE",
            "owner": "synthetic_test_fixture_only",
            "owner_decision": "synthetic_test_fixture_only:not_project_authority",
            "selection_authorized": True,
        }
    )
    payload["criteria"] = {
        "mode": "ACTIVE",
        "min_dte": 10,
        "target_dte": 24,
        "max_dte": 40,
        "max_abs_moneyness_deviation": "0.10",
        "min_abs_delta": "0.20",
        "target_abs_delta": "0.50",
        "max_abs_delta": "0.80",
        "max_quote_age_seconds": 120,
        "max_relative_spread": "0.20",
        "min_open_interest": 10,
        "min_volume": 1,
        "rank_components": [
            "dte_distance",
            "moneyness_distance",
            "delta_distance",
            "relative_spread",
            "negative_open_interest",
            "negative_volume",
            "option_sid",
        ],
    }
    path = tmp_path / "synthetic-active-selection-policy.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _default_execution_policy_payload() -> dict[str, Any]:
    path = PROJECT_ROOT / DEFAULT_QQQ_OPTIONS_MINUTE_EXECUTION_POLICY_PATH
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _active_execution_policy_path(
    tmp_path: Path,
    *,
    selection_policy_sha256: str,
    scenario_role: str = "REALITY_BASELINE",
    slippage: str = "0.05",
    reality_baseline: bool = True,
    limit_buffer: str = "1.00",
    fill_latency_ms: int = 100,
    max_quote_age_ms: int = 1000,
    max_contracts_per_quote: int = 1,
    cancel_after_ms: int = 300000,
    name: str = "synthetic-active-execution-policy.yaml",
) -> Path:
    payload = _default_execution_policy_payload()
    payload.update(
        {
            "status": "OWNER_REVIEWED_ACTIVE",
            "owner": "synthetic_test_fixture_only",
            "owner_decision": "synthetic_test_fixture_only:not_project_authority",
            "execution_authorized": True,
            "selection_policy_sha256": selection_policy_sha256,
        }
    )
    payload["criteria"] = {
        "mode": "ACTIVE",
        "scenario_role": scenario_role,
        "dq_caveat": "Synthetic fixture only; no investment or production authority.",
        "submission_latency_ms": 100,
        "fill_latency_ms": fill_latency_ms,
        "max_quote_age_ms": max_quote_age_ms,
        "marketable_limit_buffer_per_share": limit_buffer,
        "slippage_per_share": slippage,
        "fee_per_contract_usd": "0.65",
        "max_contracts_per_quote": max_contracts_per_quote,
        "cancel_after_ms": cancel_after_ms,
        "reality_baseline": reality_baseline,
    }
    path = tmp_path / name
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _selection_decision(
    selection_policy_path: Path = DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH,
    *,
    selected: bool,
) -> SelectionDecisionRecord:
    loaded = load_qqq_option_selection_policy(selection_policy_path)
    pairs = tuple(
        sorted(
            (
                ("qqq.options.adapter_descriptor", _SHA_A),
                ("qqq.options.selection_candidate_set", _SHA_B),
                ("qqq.options.selection_policy", loaded.policy_sha256),
            )
        )
    )
    return SelectionDecisionRecord.seal(
        schema_name="selection_decision",
        schema_version="1.0.0",
        run_id=_RUN_ID,
        record_id="selection-decision-20210223",
        created_at_utc=_SELECTION_AT + timedelta(seconds=10),
        producer_version="test.execution.selection.v1",
        repository_code_sha=_REPOSITORY_SHA,
        policy_id=loaded.policy.policy_id,
        policy_version=loaded.policy.policy_version,
        policy_sha256=loaded.policy_sha256,
        contract_schema_sha256=QQQ_OPTIONS_CONTRACT_SHA256,
        source_ids=tuple(item[0] for item in pairs),
        source_checksums=tuple(item[1] for item in pairs),
        requested_start=_REQUESTED_START,
        requested_end=_REQUESTED_END,
        evaluated_start=_REQUESTED_START,
        evaluated_end=_REQUESTED_END,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status="NOT_EVALUATED",
        pit_status="NOT_EVALUATED",
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id="selection-lineage-20210223",
        safety=_safety(),
        decision_id="selection-decision-20210223",
        selection_snapshot_utc=_SELECTION_AT,
        selected_option_sid="QQQ-20210319-C-325" if selected else None,
        no_contract_reason=None if selected else "SELECTION_POLICY_REVIEW_REQUIRED",
        candidate_set_sha256=_SHA_B,
        stable_rank_components=("option_sid",),
        rejected_counts=(),
    )


def _dq_checks(
    *,
    overrides: dict[str, tuple[DQStatus, str | None]] | None = None,
) -> tuple[DQCheckResult, ...]:
    policy = load_qqq_options_dq_pit_identity_policy().policy
    missing_reasons = {
        "evidence_identity": "EVIDENCE_MANIFEST_MISSING",
        "order_fill_chronology": "ORDER_FILL_CHRONOLOGY_MISSING",
        "provider_raw_checksum": "PROVIDER_RAW_CHECKSUM_UNAVAILABLE",
    }
    result: list[DQCheckResult] = []
    for check_id in policy.required_check_ids:
        status: DQStatus = "PASS" if check_id in _SELECTION_STAGE_CHECKS else "NOT_EVALUATED"
        reason = None if status == "PASS" else missing_reasons[check_id]
        if overrides and check_id in overrides:
            status, reason = overrides[check_id]
        result.append(
            DQCheckResult(
                check_id=check_id,
                status=status,
                reason_code=reason,
                observed_at_utc=_SELECTION_AT + timedelta(seconds=15),
            )
        )
    return tuple(result)


def _selection_dq_report(
    *,
    overrides: dict[str, tuple[DQStatus, str | None]] | None = None,
    adapter_sha256: str = _SHA_A,
    scope: str = "qqq_options_event_dq_pit_identity",
    generated_at: datetime = _SELECTION_AT + timedelta(seconds=15),
) -> DQReportRecord:
    checks = _dq_checks(overrides=overrides)
    dq_status: DQStatus = (
        "FAIL"
        if any(item.status == "FAIL" for item in checks)
        else "NOT_EVALUATED"
    )
    pit_ids = {
        "exchange_calendar_identity",
        "fill_forward_ambiguity",
        "open_interest_freshness",
        "order_fill_chronology",
        "prior_day_model_freshness",
        "quote_freshness",
        "signal_selection_chronology",
        "symbol_mapping_identity",
    }
    pit_status: DQStatus = (
        "FAIL"
        if any(item.status == "FAIL" for item in checks if item.check_id in pit_ids)
        else "NOT_EVALUATED"
    )
    pairs = tuple(
        sorted(
            (
                ("qqq.options.adapter_descriptor", adapter_sha256),
                ("qqq.options.dq_policy", _DQ_POLICY_SHA),
                ("qqq.options.raw.synthetic", _SHA_D),
            )
        )
    )
    return DQReportRecord.seal(
        schema_name="dq_report",
        schema_version="1.0.0",
        run_id=_RUN_ID,
        record_id="selection-dq-20210223",
        created_at_utc=generated_at,
        producer_version="test.execution.dq.v1",
        repository_code_sha=_REPOSITORY_SHA,
        policy_id="qqq_options_dq_pit_identity_v1",
        policy_version="1.0.0",
        policy_sha256=_DQ_POLICY_SHA,
        contract_schema_sha256=QQQ_OPTIONS_CONTRACT_SHA256,
        source_ids=tuple(item[0] for item in pairs),
        source_checksums=tuple(item[1] for item in pairs),
        requested_start=_REQUESTED_START,
        requested_end=_REQUESTED_END,
        evaluated_start=_REQUESTED_START,
        evaluated_end=_REQUESTED_END,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status=dq_status,
        pit_status=pit_status,
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id="selection-dq-lineage-20210223",
        safety=_safety(),
        scope=scope,
        report_version="1.0.0",
        generated_at_utc=generated_at,
        checks=checks,
    )


def _observation() -> QQQOptionsDQObservation:
    return QQQOptionsDQObservation(
        observed_at_utc=_SELECTION_AT + timedelta(seconds=15),
        chain_present=True,
        candidate_present=True,
        quote_bid_per_share=Decimal("5.00"),
        quote_ask_per_share=Decimal("5.20"),
        quote_end_utc=_SELECTION_AT - timedelta(minutes=1),
        quote_freshness_assessment="PASS",
        selection_session=_SELECTION_SESSION,
        expected_prior_session=_PRIOR_SESSION,
        prior_day_model_as_of_session=_PRIOR_SESSION,
        model_freshness_assessment="PASS",
        open_interest_as_of_session=_PRIOR_SESSION,
        open_interest_freshness_assessment="PASS",
        exchange_calendar_identity=_identity("xnys.calendar"),
        symbol_mapping_identity=_identity("qqq.mapping"),
        signal_as_of_utc=_SIGNAL_AT,
        selection_snapshot_utc=_SELECTION_AT,
        order_intent_utc=None,
        order_submit_utc=None,
        fill_quote_end_utc=None,
        fill_utc=None,
        fill_forward_assessment="PASS",
        cache_key="qc.qqq.options.minute.20210222.20210331",
        prior_cache_identity_sha256=None,
        cache_material={
            "provider": "QuantConnect",
            "dataset": "USOptions",
            "underlying": "QQQ",
            "option_sid": "QQQ-20210319-C-325",
            "resolution": "MINUTE",
            "requested_start": _REQUESTED_START,
            "requested_end": _REQUESTED_END,
            "calendar_identity": _identity("xnys.calendar"),
            "mapping_identity": _identity("qqq.mapping"),
            "normalization_identity": _identity("raw.normalization"),
            "dq_policy_sha256": _DQ_POLICY_SHA,
            "shared_contract_sha256": QQQ_OPTIONS_CONTRACT_SHA256,
            "repository_code_sha": _REPOSITORY_SHA,
            "engine_identity": _identity("lean.engine"),
            "source_checksum_evidence": {
                "availability": "UNAVAILABLE_PROVIDER_DOES_NOT_EXPOSE",
                "sha256": None,
                "export_classification": "QC_ONLY_NOT_EXPORTED",
            },
        },
        engine_identity=_identity("lean.engine"),
        evidence_identity=_identity(
            "qc.bundle",
            assessment="UNKNOWN_REQUIRES_POLICY_REVIEW",
            observed_sha=None,
        ),
        platform_evidence_manifest=None,
        local_cached_data_gate={
            "status": "NOT_APPLICABLE_TO_OPTION_EVENT_SCOPE",
            "scope": "NOT_APPLICABLE_TO_OPTION_EVENT_SCOPE",
            "as_of_utc": None,
            "report_locator": None,
            "report_sha256": None,
        },
    )


def _quote(
    minute_offset: int = 0,
    *,
    suffix: str | None = None,
    bid: str | None = "5.10",
    ask: str | None = "5.30",
    available: int = 1,
    disposition: str = "TRADEABLE",
    rejection_reason: str | None = None,
) -> QQQOptionExecutionQuoteInput:
    start = _FIRST_QUOTE_START + timedelta(minutes=minute_offset)
    quote_suffix = suffix or str(minute_offset)
    return QQQOptionExecutionQuoteInput(
        source_id=f"qqq.options.minute.synthetic.{quote_suffix}",
        source_sha256=_sha(f"quote-{quote_suffix}-{bid}-{ask}-{disposition}"),
        quote_start_utc=start,
        quote_end_utc=start + timedelta(minutes=1),
        resolution="MINUTE",
        disposition=disposition,
        bid_per_share=None if disposition != "TRADEABLE" else Decimal(str(bid)),
        ask_per_share=None if disposition != "TRADEABLE" else Decimal(str(ask)),
        available_contracts=available if disposition == "TRADEABLE" else 0,
        rejection_reason_code=rejection_reason,
    )


def _request(
    *,
    selection_policy_path: Path,
    selected: bool = True,
    side: str = "BUY_TO_OPEN",
    contracts: int = 2,
    quotes: tuple[QQQOptionExecutionQuoteInput, ...] = (),
    dq_report: DQReportRecord | None = None,
) -> QQQOptionExecutionRequest:
    report = dq_report if dq_report is not None else (_selection_dq_report() if selected else None)
    return QQQOptionExecutionRequest(
        selection_decision=_selection_decision(selection_policy_path, selected=selected),
        side=side,
        contracts=contracts,
        contract_multiplier=100,
        reserved_cash_usd=Decimal("2000"),
        selection_quote_bid_per_share=Decimal("5.00") if selected else None,
        selection_quote_ask_per_share=Decimal("5.20") if selected else None,
        selection_quote_end_utc=_SELECTION_AT - timedelta(minutes=1) if selected else None,
        selection_quote_source_id="qqq.options.selection.quote.synthetic" if selected else None,
        selection_quote_source_sha256=_SHA_C if selected else None,
        signal_as_of_utc=_SIGNAL_AT,
        intent_id="execution-intent-20210223",
        platform_order_id="execution-order-20210223",
        intent_at_utc=_INTENT_AT,
        producer_version="test.execution.v1",
        lineage_id="execution-lineage-20210223",
        selection_dq_report_bytes=None if report is None else report.canonical_bytes,
        selection_dq_report_sha256=(
            None if report is None else hashlib.sha256(report.canonical_bytes).hexdigest()
        ),
        dq_observation_template=_observation() if selected else None,
        dq_record_id_prefix="execution-dq-20210223",
        dq_lineage_id="execution-dq-lineage-20210223",
        quotes=quotes,
    )


def _active_paths(
    tmp_path: Path,
    **execution_updates: Any,
) -> tuple[Path, Path]:
    selection_path = _active_selection_policy_path(tmp_path)
    selection_sha = hashlib.sha256(selection_path.read_bytes()).hexdigest()
    execution_path = _active_execution_policy_path(
        tmp_path,
        selection_policy_sha256=selection_sha,
        **execution_updates,
    )
    return selection_path, execution_path


def test_default_policy_is_exact_unresolved_and_execution_unauthorized() -> None:
    loaded = load_qqq_options_minute_execution_policy()

    assert loaded.policy.status == "OWNER_REVIEW_REQUIRED_BASELINE"
    assert loaded.policy.execution_authorized is False
    assert loaded.policy.criteria.mode == "UNRESOLVED"
    assert set(loaded.policy.criteria.model_dump().values()) == {
        "UNRESOLVED",
        "UNKNOWN_REQUIRES_POLICY_REVIEW",
        False,
    }
    assert loaded.policy.primary_research_start == date(2021, 2, 22)
    assert loaded.policy.legacy_non_default_start_is_default is False
    assert loaded.policy_sha256 == hashlib.sha256(loaded.policy_path.read_bytes()).hexdigest()


def test_default_unauthorized_returns_typed_cash_without_order_or_fill() -> None:
    request = _request(
        selection_policy_path=DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH,
        selected=False,
    )

    result = simulate_qqq_option_minute_execution(request)

    assert result.reason_code == "EXECUTION_POLICY_REVIEW_REQUIRED"
    assert result.execution_authorized is False
    assert result.selection_authorized is False
    assert result.cash_preservation_required is True
    assert result.order_intent is None
    assert result.order_events == ()
    assert result.fill_events == ()
    assert result.execution_dq_reports == ()
    assert QQQOptionExecutionResult.from_json_bytes(result.canonical_bytes) == result


def test_active_execution_cannot_activate_blocked_2485_selection(tmp_path: Path) -> None:
    selection = load_qqq_option_selection_policy()
    execution_path = _active_execution_policy_path(
        tmp_path,
        selection_policy_sha256=selection.policy_sha256,
    )
    request = _request(
        selection_policy_path=DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH,
        selected=False,
    )

    result = simulate_qqq_option_minute_execution(
        request,
        policy_path=execution_path,
    )

    assert result.reason_code == "SELECTION_POLICY_REVIEW_REQUIRED"
    assert result.execution_authorized is True
    assert result.selection_authorized is False
    assert result.cash_preservation_required is True
    assert result.order_intent is None
    assert result.order_events == ()
    assert result.fill_events == ()


def test_buy_partial_then_full_uses_ask_limit_fee_and_next_independent_minute(
    tmp_path: Path,
) -> None:
    selection_path, execution_path = _active_paths(tmp_path)
    request = _request(
        selection_policy_path=selection_path,
        quotes=(_quote(0), _quote(1)),
    )

    result = simulate_qqq_option_minute_execution(
        request,
        policy_path=execution_path,
        selection_policy_path=selection_path,
    )

    assert result.reason_code == "FILLED"
    assert result.execution_stage_dq_status == "PASS"
    assert result.global_dq_status == "NOT_EVALUATED"
    assert result.global_pit_status == "PASS"
    assert result.accounting_status == "NOT_EVALUATED"
    assert result.order_intent is not None
    assert result.order_intent.not_before_utc == datetime(
        2021, 2, 23, 14, 32, tzinfo=UTC
    )
    assert result.order_intent.limit_price_per_share == Decimal("6.20")
    assert [item.event_type for item in result.order_events] == [
        "CREATED",
        "SUBMITTED",
        "PARTIALLY_FILLED",
        "FILLED",
    ]
    assert [item.filled_contracts_total for item in result.order_events] == [0, 0, 1, 2]
    assert [item.quote_side for item in result.fill_events] == ["ASK", "ASK"]
    assert [item.fill_price_per_share for item in result.fill_events] == [
        Decimal("5.35"),
        Decimal("5.35"),
    ]
    assert [item.fee_usd for item in result.fill_events] == [
        Decimal("0.65"),
        Decimal("0.65"),
    ]
    assert all(
        fill.quote_end_utc < fill.fill_at_utc for fill in result.fill_events
    )
    assert all(
        fill.fill_price_per_share <= result.order_intent.limit_price_per_share
        for fill in result.fill_events
    )
    assert all(
        FillEventRecord.from_json_bytes(item.canonical_bytes) == item
        for item in result.fill_events
    )
    assert OrderIntentRecord.from_json_bytes(result.order_intent.canonical_bytes) == (
        result.order_intent
    )
    assert all(
        OrderEventRecord.from_json_bytes(item.canonical_bytes) == item
        for item in result.order_events
    )


def test_sell_uses_bid_and_never_fills_below_limit(tmp_path: Path) -> None:
    selection_path, execution_path = _active_paths(tmp_path)
    request = _request(
        selection_policy_path=selection_path,
        side="SELL_TO_CLOSE",
        contracts=1,
        quotes=(_quote(0, bid="5.10", ask="5.30"),),
    )

    result = simulate_qqq_option_minute_execution(
        request,
        policy_path=execution_path,
        selection_policy_path=selection_path,
    )

    assert result.reason_code == "FILLED"
    assert result.order_intent is not None
    fill = result.fill_events[0]
    assert fill.quote_side == "BID"
    assert fill.fill_price_per_share == Decimal("5.05")
    assert fill.fill_price_per_share >= result.order_intent.limit_price_per_share
    assert fill.gross_cash_delta_usd == Decimal("505")


def test_same_bar_quote_is_never_filled_and_times_out(tmp_path: Path) -> None:
    selection_path, execution_path = _active_paths(tmp_path)
    same_bar = QQQOptionExecutionQuoteInput(
        source_id="qqq.options.minute.synthetic.samebar",
        source_sha256=_SHA_D,
        quote_start_utc=datetime(2021, 2, 23, 14, 32, tzinfo=UTC),
        quote_end_utc=datetime(2021, 2, 23, 14, 33, tzinfo=UTC),
        resolution="MINUTE",
        disposition="TRADEABLE",
        bid_per_share=Decimal("5.10"),
        ask_per_share=Decimal("5.30"),
        available_contracts=2,
        rejection_reason_code=None,
    )
    request = _request(
        selection_policy_path=selection_path,
        quotes=(same_bar,),
    )

    result = simulate_qqq_option_minute_execution(
        request,
        policy_path=execution_path,
        selection_policy_path=selection_path,
    )

    assert result.reason_code == "NO_FILL_CANCELED"
    assert result.fill_events == ()
    assert result.order_events[-1].event_type == "CANCELED"
    assert result.order_events[-1].reason_code == "NO_FILL_TIMEOUT"


def test_daily_quote_resolution_is_rejected_before_simulation() -> None:
    with pytest.raises(ValidationError, match="MINUTE"):
        QQQOptionExecutionQuoteInput(
            source_id="qqq.options.daily.synthetic",
            source_sha256=_SHA_A,
            quote_start_utc=_FIRST_QUOTE_START,
            quote_end_utc=_FIRST_QUOTE_START + timedelta(minutes=1),
            resolution="DAILY",
            disposition="TRADEABLE",
            bid_per_share=Decimal("5.10"),
            ask_per_share=Decimal("5.30"),
            available_contracts=1,
            rejection_reason_code=None,
        )


@pytest.mark.parametrize(
    "quotes",
    (
        (_quote(0, disposition="MISSING", bid=None, ask=None, available=0),),
        (_quote(0, bid="5.10", ask="7.50"),),
    ),
)
def test_missing_or_non_marketable_quote_is_typed_no_fill_cancel(
    tmp_path: Path,
    quotes: tuple[QQQOptionExecutionQuoteInput, ...],
) -> None:
    selection_path, execution_path = _active_paths(tmp_path)
    request = _request(selection_policy_path=selection_path, quotes=quotes)

    result = simulate_qqq_option_minute_execution(
        request,
        policy_path=execution_path,
        selection_policy_path=selection_path,
    )

    assert result.reason_code == "NO_FILL_CANCELED"
    assert result.cash_preservation_required is True
    assert result.fill_events == ()
    assert result.order_events[-1].event_type == "CANCELED"
    assert result.execution_stage_dq_status == "NOT_EVALUATED"


def test_stale_execution_quote_is_never_filled(tmp_path: Path) -> None:
    selection_path, execution_path = _active_paths(
        tmp_path,
        fill_latency_ms=1500,
        max_quote_age_ms=1000,
    )
    request = _request(selection_policy_path=selection_path, quotes=(_quote(0),))

    result = simulate_qqq_option_minute_execution(
        request,
        policy_path=execution_path,
        selection_policy_path=selection_path,
    )

    assert result.reason_code == "NO_FILL_CANCELED"
    assert result.fill_events == ()


def test_crossed_execution_quote_rejects_without_fill(tmp_path: Path) -> None:
    selection_path, execution_path = _active_paths(tmp_path)
    request = _request(
        selection_policy_path=selection_path,
        quotes=(_quote(0, bid="5.40", ask="5.30"),),
    )

    result = simulate_qqq_option_minute_execution(
        request,
        policy_path=execution_path,
        selection_policy_path=selection_path,
    )

    assert result.reason_code == "EXECUTION_DQ_REJECTED"
    assert result.fill_events == ()
    assert result.order_events[-1].event_type == "REJECTED"
    assert result.order_events[-1].reason_code == "CROSSED_EXECUTION_QUOTE"


def test_venue_rejection_is_typed_and_carries_no_fill(tmp_path: Path) -> None:
    selection_path, execution_path = _active_paths(tmp_path)
    rejected = _quote(
        0,
        disposition="VENUE_REJECTED",
        bid=None,
        ask=None,
        available=0,
        rejection_reason="VENUE_ORDER_REJECTED",
    )
    request = _request(selection_policy_path=selection_path, quotes=(rejected,))

    result = simulate_qqq_option_minute_execution(
        request,
        policy_path=execution_path,
        selection_policy_path=selection_path,
    )

    assert result.reason_code == "VENUE_REJECTED"
    assert result.fill_events == ()
    assert result.order_events[-1].event_type == "REJECTED"
    assert result.order_events[-1].reason_code == "VENUE_ORDER_REJECTED"


def test_partial_fill_replays_then_cancels_monotonically(tmp_path: Path) -> None:
    selection_path, execution_path = _active_paths(tmp_path)
    request = _request(selection_policy_path=selection_path, quotes=(_quote(0),))

    result = simulate_qqq_option_minute_execution(
        request,
        policy_path=execution_path,
        selection_policy_path=selection_path,
    )
    replay = QQQOptionExecutionResult.from_json_bytes(result.canonical_bytes)

    assert result.reason_code == "PARTIAL_CANCELED"
    assert [item.event_type for item in result.order_events] == [
        "CREATED",
        "SUBMITTED",
        "PARTIALLY_FILLED",
        "CANCELED",
    ]
    assert [item.filled_contracts_total for item in result.order_events] == [0, 0, 1, 1]
    assert replay == result
    assert replay.canonical_bytes == result.canonical_bytes


def test_quote_permutation_does_not_change_quote_set_or_replay(tmp_path: Path) -> None:
    selection_path, execution_path = _active_paths(tmp_path)
    quotes = (_quote(0), _quote(1))
    results = [
        simulate_qqq_option_minute_execution(
            _request(selection_policy_path=selection_path, quotes=tuple(order)),
            policy_path=execution_path,
            selection_policy_path=selection_path,
        )
        for order in permutations(quotes)
    ]

    assert len({item.quote_set_sha256 for item in results}) == 1
    assert len({item.content_sha256 for item in results}) == 1
    assert len({item.canonical_bytes for item in results}) == 1


def test_duplicate_source_identity_fails_closed(tmp_path: Path) -> None:
    selection_path = _active_selection_policy_path(tmp_path)
    quote = _quote(0)

    with pytest.raises(ValidationError, match="source ids must be unique"):
        _request(selection_policy_path=selection_path, quotes=(quote, quote))


@pytest.mark.parametrize(
    ("status", "reason"),
    (("FAIL", "QUOTE_FRESHNESS_FAIL"), ("NOT_EVALUATED", "QUOTE_FRESHNESS_UNKNOWN")),
)
def test_selection_dq_semantic_fail_or_unknown_never_produces_order(
    tmp_path: Path,
    status: DQStatus,
    reason: str,
) -> None:
    selection_path, execution_path = _active_paths(tmp_path)
    report = _selection_dq_report(
        overrides={"quote_freshness": (status, reason)}
    )
    request = _request(
        selection_policy_path=selection_path,
        quotes=(_quote(0),),
        dq_report=report,
    )

    result = simulate_qqq_option_minute_execution(
        request,
        policy_path=execution_path,
        selection_policy_path=selection_path,
    )

    assert result.reason_code == "SELECTION_DQ_NOT_PASS"
    assert result.execution_stage_dq_status == status
    assert result.order_intent is None
    assert result.order_events == ()
    assert result.fill_events == ()


def test_arbitrary_or_tampered_dq_bytes_cannot_pair_with_forged_hash(tmp_path: Path) -> None:
    selection_path = _active_selection_policy_path(tmp_path)
    payload = _request(selection_policy_path=selection_path).model_dump(mode="python")
    payload["selection_dq_report_bytes"] = b"arbitrary bytes"
    payload["selection_dq_report_sha256"] = hashlib.sha256(b"arbitrary bytes").hexdigest()

    with pytest.raises(ValidationError, match="json"):
        QQQOptionExecutionRequest.model_validate(payload, strict=True)


@pytest.mark.parametrize(
    ("report", "error"),
    (
        (_selection_dq_report(scope="wrong_scope"), "DQ_SCOPE_MISMATCH"),
        (
            _selection_dq_report(adapter_sha256=_SHA_B),
            "DQ_ADAPTER_MISMATCH",
        ),
        (
            _selection_dq_report(generated_at=_INTENT_AT + timedelta(seconds=1)),
            "DQ_AS_OF_FUTURE",
        ),
    ),
)
def test_dq_scope_adapter_or_asof_mismatch_fails_closed(
    tmp_path: Path,
    report: DQReportRecord,
    error: str,
) -> None:
    selection_path, execution_path = _active_paths(tmp_path)
    request = _request(
        selection_policy_path=selection_path,
        quotes=(_quote(0),),
        dq_report=report,
    )

    with pytest.raises(QQQOptionMinuteExecutionContractError, match=error):
        simulate_qqq_option_minute_execution(
            request,
            policy_path=execution_path,
            selection_policy_path=selection_path,
        )


def test_selection_policy_hash_lineage_tamper_fails_closed(tmp_path: Path) -> None:
    selection_path, execution_path = _active_paths(tmp_path)
    request = _request(selection_policy_path=selection_path, quotes=(_quote(0),))

    with pytest.raises(
        QQQOptionMinuteExecutionContractError,
        match="SELECTION_POLICY_MISMATCH",
    ):
        simulate_qqq_option_minute_execution(
            request,
            policy_path=execution_path,
            selection_policy_path=DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH,
        )


def test_zero_slippage_only_loads_as_isolation_sensitivity(tmp_path: Path) -> None:
    selection_path = _active_selection_policy_path(tmp_path)
    selection_sha = hashlib.sha256(selection_path.read_bytes()).hexdigest()
    invalid = _active_execution_policy_path(
        tmp_path,
        selection_policy_sha256=selection_sha,
        slippage="0",
        name="invalid-zero-reality.yaml",
    )
    with pytest.raises(QQQOptionMinuteExecutionContractError, match="isolation"):
        load_qqq_options_minute_execution_policy(invalid)

    valid = _active_execution_policy_path(
        tmp_path,
        selection_policy_sha256=selection_sha,
        scenario_role="ISOLATION_SENSITIVITY",
        slippage="0",
        reality_baseline=False,
        name="valid-zero-isolation.yaml",
    )
    loaded = load_qqq_options_minute_execution_policy(valid)
    assert loaded.policy.criteria.scenario_role == "ISOLATION_SENSITIVITY"
    assert loaded.policy.criteria.reality_baseline is False


def test_policy_or_result_extra_and_tamper_are_rejected(tmp_path: Path) -> None:
    payload = _default_execution_policy_payload()
    payload["unexpected"] = True
    invalid_policy = tmp_path / "invalid-extra-policy.yaml"
    invalid_policy.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(QQQOptionMinuteExecutionContractError, match="POLICY_INVALID"):
        load_qqq_options_minute_execution_policy(invalid_policy)

    selection_path, execution_path = _active_paths(tmp_path)
    result = simulate_qqq_option_minute_execution(
        _request(selection_policy_path=selection_path, contracts=1, quotes=(_quote(0),)),
        policy_path=execution_path,
        selection_policy_path=selection_path,
    )
    tampered = result.canonical_bytes.replace(b'"fee_usd": "0.65"', b'"fee_usd": "0.66"')
    with pytest.raises(QQQOptionMinuteExecutionContractError, match="RESULT_INVALID"):
        QQQOptionExecutionResult.from_json_bytes(tampered)


def test_quote_set_hash_binds_source_checksum_and_event_time(tmp_path: Path) -> None:
    selection_path = _active_selection_policy_path(tmp_path)
    baseline = _request(selection_policy_path=selection_path, quotes=(_quote(0),))
    changed_checksum = _request(
        selection_policy_path=selection_path,
        quotes=(
            _quote(0).model_copy(update={"source_sha256": _SHA_B}),
        ),
    )
    changed_time = _request(
        selection_policy_path=selection_path,
        quotes=(_quote(1),),
    )

    identities = {
        build_qqq_option_execution_quote_set_sha256(item)
        for item in (baseline, changed_checksum, changed_time)
    }
    assert len(identities) == 3


def test_golden_execution_identity_is_stable(tmp_path: Path) -> None:
    selection_path, execution_path = _active_paths(tmp_path)
    result = simulate_qqq_option_minute_execution(
        _request(
            selection_policy_path=selection_path,
            contracts=2,
            quotes=(_quote(0), _quote(1)),
        ),
        policy_path=execution_path,
        selection_policy_path=selection_path,
    )

    assert result.content_sha256 == (
        "43fc8916b9c47b118516f25915cc527e669cc9a1671a40402e700a2a3b739f74"
    )
