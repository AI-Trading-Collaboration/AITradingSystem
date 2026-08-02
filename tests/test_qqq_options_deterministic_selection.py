from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
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
    ContractCandidateSnapshotRecord,
    DailySignalRecord,
    DQCheckResult,
    DQReportRecord,
    DQStatus,
    OptionRight,
    QQQOptionsSafetyBoundary,
    SelectionDecisionRecord,
)
from ai_trading_system.qqq_options_research.deterministic_selection import (
    DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH,
    QQQOptionSelectionCandidateInput,
    QQQOptionSelectionContractError,
    QQQOptionSelectionRequest,
    build_qqq_option_selection_candidate_set_sha256,
    load_qqq_option_selection_policy,
    select_qqq_option_contract,
)
from ai_trading_system.qqq_options_research.dq_pit_identity import (
    load_qqq_options_dq_pit_identity_policy,
)
from ai_trading_system.qqq_options_research.qc_project_adapter import (
    QCProjectAdapterDescriptor,
    load_qc_qqq_options_project_adapter_policy,
)

_SHA_A = "a" * 64
_SHA_B = "b" * 64
_SHA_C = "c" * 64
_SHA_D = "d" * 64
_REPOSITORY_SHA = "e" * 40
_RUN_ID = "run-20210222-selection"
_REQUESTED_START = date(2021, 2, 22)
_REQUESTED_END = date(2021, 3, 31)
_SELECTION_SESSION = date(2021, 2, 23)
_PRIOR_SESSION = date(2021, 2, 22)
_SELECTION = datetime(2021, 2, 23, 14, 32, tzinfo=UTC)
_DQ_AT = datetime(2021, 2, 23, 14, 33, tzinfo=UTC)
_CREATED = datetime(2021, 2, 23, 14, 34, tzinfo=UTC)
_SHARED_POLICY_SHA = "d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349"
_DQ_POLICY_SHA = "1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358"
_SIGNAL_POLICY_SHA = "cf9d6ba3044bdf1d601de1ae7fe6f82fa3e26cc7811dc50160d24dfc902259e9"
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


def _shared_safety() -> QQQOptionsSafetyBoundary:
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


def _daily_signal(direction: str = "LONG_CALL") -> DailySignalRecord:
    return DailySignalRecord.seal(
        schema_name="daily_signal",
        schema_version="1.0.0",
        run_id=_RUN_ID,
        record_id=f"signal-{direction.lower()}",
        created_at_utc=datetime(2021, 2, 23, 1, 0, tzinfo=UTC),
        producer_version="test.selection.v1",
        repository_code_sha=_REPOSITORY_SHA,
        policy_id="qqq_options_signal_export_v1",
        policy_version="1.0.0",
        policy_sha256=_SIGNAL_POLICY_SHA,
        contract_schema_sha256=QQQ_OPTIONS_CONTRACT_SHA256,
        source_ids=("qqq.signal.normalized",),
        source_checksums=(_SHA_A,),
        requested_start=_REQUESTED_START,
        requested_end=_REQUESTED_END,
        evaluated_start=_REQUESTED_START,
        evaluated_end=_REQUESTED_END,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status="PASS",
        pit_status="PASS",
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id="signal-lineage-20210222",
        safety=_shared_safety(),
        signal_session=date(2021, 2, 22),
        signal_as_of_utc=datetime(2021, 2, 22, 21, 0, tzinfo=UTC),
        generated_at_utc=datetime(2021, 2, 23, 1, 0, tzinfo=UTC),
        earliest_effective_session=_SELECTION_SESSION,
        signal=direction,
        signal_source_sha256=_SHA_B,
    )


def _adapter_descriptor() -> QCProjectAdapterDescriptor:
    adapter = load_qc_qqq_options_project_adapter_policy().policy
    return QCProjectAdapterDescriptor.seal(
        schema_version="qc_qqq_options_project_adapter_descriptor.v1",
        run_id=_RUN_ID,
        created_at_utc=datetime(2021, 2, 23, 1, 0, tzinfo=UTC),
        repository_code_sha=_REPOSITORY_SHA,
        adapter_policy_id=adapter.policy_id,
        adapter_policy_version=adapter.policy_version,
        adapter_policy_sha256=_ADAPTER_POLICY_SHA,
        signal_export_policy_sha256=_SIGNAL_POLICY_SHA,
        shared_contract_sha256=QQQ_OPTIONS_CONTRACT_SHA256,
        shared_policy_sha256=_SHARED_POLICY_SHA,
        dq_pit_policy_sha256=_DQ_POLICY_SHA,
        signal_package_receipt_sha256=_SHA_A,
        signal_package_receipt_content_sha256=_SHA_B,
        signal_index_sha256=_SHA_C,
        signal_index_content_sha256=_SHA_D,
        run_manifest_sha256=_sha("manifest"),
        daily_signal_count=27,
        requested_start=_REQUESTED_START,
        requested_end=_REQUESTED_END,
        evaluated_start=_REQUESTED_START,
        evaluated_end=_REQUESTED_END,
        capability_receipt_id="capability-receipt-test",
        capability_receipt_sha256=_sha("capability-receipt"),
        capability_policy_sha256=_sha("capability-policy"),
        capability_evidence_sha256=_sha("capability-evidence"),
        capability_decision="CAPABILITY_OR_LICENSE_BLOCKED",
        capability_blocking_reason_codes=("LICENSE_UNKNOWN",),
        capability_bounded_pilot_preparation_allowed=False,
        option_event_dq_status="NOT_EVALUATED",
        option_event_pit_status="NOT_EVALUATED",
        input_admission_status="UNKNOWN_REQUIRES_PLATFORM_EVIDENCE",
        subscription=adapter.subscription,
        project_file_boundary=adapter.project_file_boundary,
        engine_identity=adapter.engine_identity,
        result_mappings=adapter.result_mappings,
        cloud_run_authorized=False,
        decision=adapter.decision,
        safety=adapter.safety,
    )


def _dq_checks(
    *, overrides: dict[str, tuple[DQStatus, str | None]] | None = None
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
                observed_at_utc=_DQ_AT,
            )
        )
    return tuple(result)


def _dq_report(
    *,
    descriptor: QCProjectAdapterDescriptor,
    source_id: str,
    source_sha256: str,
    overrides: dict[str, tuple[DQStatus, str | None]] | None = None,
    descriptor_sha256: str | None = None,
) -> DQReportRecord:
    pairs = tuple(
        sorted(
            (
                (
                    "qqq.options.adapter_descriptor",
                    descriptor_sha256 or descriptor.canonical_sha256,
                ),
                (source_id, source_sha256),
                ("qqq.options.dq_policy", _DQ_POLICY_SHA),
            )
        )
    )
    checks = _dq_checks(overrides=overrides)
    dq_status = "FAIL" if any(item.status == "FAIL" for item in checks) else "NOT_EVALUATED"
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
    pit_status = (
        "FAIL"
        if any(item.status == "FAIL" for item in checks if item.check_id in pit_ids)
        else "NOT_EVALUATED"
    )
    return DQReportRecord.seal(
        schema_name="dq_report",
        schema_version="1.0.0",
        run_id=_RUN_ID,
        record_id=f"dq-{source_id.rsplit('.', maxsplit=1)[-1]}",
        created_at_utc=_CREATED,
        producer_version="test.selection.v1",
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
        lineage_id="dq-lineage-20210223",
        safety=_shared_safety(),
        scope="qqq_options_event_dq_pit_identity",
        report_version="1.0.0",
        generated_at_utc=_DQ_AT,
        checks=checks,
    )


def _candidate(
    suffix: str,
    *,
    descriptor: QCProjectAdapterDescriptor | None = None,
    right: OptionRight = "CALL",
    expiry: date = date(2021, 3, 19),
    strike: str = "325",
    delta: str = "0.50",
    bid: str = "5.00",
    ask: str = "5.20",
    open_interest: int = 100,
    volume: int = 20,
    model_session: date = _PRIOR_SESSION,
    oi_session: date = _PRIOR_SESSION,
    volume_session: date = _PRIOR_SESSION,
    quote_end: datetime = datetime(2021, 2, 23, 14, 31, tzinfo=UTC),
    dq_overrides: dict[str, tuple[DQStatus, str | None]] | None = None,
    dq_descriptor_sha256: str | None = None,
) -> QQQOptionSelectionCandidateInput:
    descriptor = descriptor or _adapter_descriptor()
    source_id = f"qqq.options.raw.{suffix}"
    source_sha256 = _sha(f"candidate-{suffix}")
    report = _dq_report(
        descriptor=descriptor,
        source_id=source_id,
        source_sha256=source_sha256,
        overrides=dq_overrides,
        descriptor_sha256=dq_descriptor_sha256,
    )
    return QQQOptionSelectionCandidateInput(
        option_sid=f"QQQ-20210319-{right[0]}-{strike}-{suffix}",
        right=right,
        expiry=expiry,
        strike_usd_per_share=Decimal(strike),
        contract_multiplier=100,
        underlying_price_usd_per_share=Decimal("325"),
        model_delta=Decimal(delta),
        prior_day_model_as_of_session=model_session,
        open_interest=open_interest,
        open_interest_as_of_session=oi_session,
        volume=volume,
        volume_as_of_session=volume_session,
        quote_bid_per_share=Decimal(bid),
        quote_ask_per_share=Decimal(ask),
        quote_end_utc=quote_end,
        source_id=source_id,
        source_sha256=source_sha256,
        dq_report_bytes=report.canonical_bytes,
        dq_report_sha256=hashlib.sha256(report.canonical_bytes).hexdigest(),
        field_export_classification="QC_ONLY_NOT_EXPORTED",
    )


def _request(
    *,
    signal: str = "LONG_CALL",
    candidates: tuple[QQQOptionSelectionCandidateInput, ...] = (),
    descriptor: QCProjectAdapterDescriptor | None = None,
) -> QQQOptionSelectionRequest:
    return QQQOptionSelectionRequest(
        adapter_descriptor=descriptor or _adapter_descriptor(),
        daily_signal=_daily_signal(signal),
        selection_session=_SELECTION_SESSION,
        expected_prior_session=_PRIOR_SESSION,
        selection_snapshot_utc=_SELECTION,
        decision_id="selection-decision-20210223",
        created_at_utc=_CREATED,
        producer_version="test.selection.v1",
        lineage_id="selection-lineage-20210223",
        candidates=candidates,
    )


def _default_policy_payload() -> dict[str, Any]:
    path = PROJECT_ROOT / DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _active_policy_path(tmp_path: Path, **criteria_updates: object) -> Path:
    payload = _default_policy_payload()
    payload.update(
        {
            "status": "OWNER_REVIEWED_ACTIVE",
            "owner": "synthetic_test_fixture_only",
            "owner_decision": "synthetic_test_fixture_only:not_project_authority",
            "selection_authorized": True,
        }
    )
    criteria: dict[str, object] = {
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
    criteria.update(criteria_updates)
    payload["criteria"] = criteria
    path = tmp_path / "synthetic-active-selection-policy.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def test_default_policy_is_exact_unresolved_and_cannot_select() -> None:
    loaded = load_qqq_option_selection_policy()

    assert loaded.policy.status == "OWNER_REVIEW_REQUIRED_BASELINE"
    assert loaded.policy.selection_authorized is False
    assert loaded.policy.criteria.mode == "UNRESOLVED"
    assert set(loaded.policy.criteria.model_dump().values()) == {
        "UNRESOLVED",
        "UNKNOWN_REQUIRES_POLICY_REVIEW",
    }
    assert loaded.policy_sha256 == hashlib.sha256(loaded.policy_path.read_bytes()).hexdigest()


def test_unresolved_default_emits_typed_cash_decision_without_candidates() -> None:
    result = select_qqq_option_contract(_request())

    assert result.selection_authorized is False
    assert result.cash_preservation_required is True
    assert result.candidate_snapshots == ()
    assert result.decision.selected_option_sid is None
    assert result.decision.no_contract_reason == "SELECTION_POLICY_REVIEW_REQUIRED"
    assert result.decision.dq_status == "NOT_EVALUATED"
    assert result.decision.pit_status == "NOT_EVALUATED"
    assert result.decision.stable_rank_components == ("option_sid",)
    assert SelectionDecisionRecord.from_json_bytes(result.decision.canonical_bytes) == (
        result.decision
    )


def test_flat_signal_has_precedence_and_preserves_cash() -> None:
    result = select_qqq_option_contract(_request(signal="FLAT"))

    assert result.decision.no_contract_reason == "FLAT_SIGNAL_CASH"
    assert result.cash_preservation_required is True


def test_active_synthetic_policy_selects_deterministically_across_permutations(
    tmp_path: Path,
) -> None:
    descriptor = _adapter_descriptor()
    candidates = (
        _candidate("a", descriptor=descriptor, strike="325", delta="0.50"),
        _candidate("b", descriptor=descriptor, strike="330", delta="0.45"),
        _candidate("c", descriptor=descriptor, strike="320", delta="0.60"),
    )
    policy_path = _active_policy_path(tmp_path)
    results = [
        select_qqq_option_contract(
            _request(candidates=tuple(order), descriptor=descriptor),
            policy_path=policy_path,
        )
        for order in permutations(candidates)
    ]

    assert {item.decision.selected_option_sid for item in results} == {candidates[0].option_sid}
    assert len({item.candidate_set_sha256 for item in results}) == 1
    assert len({item.canonical_bytes for item in results}) == 1
    assert all(item.decision.dq_status == "NOT_EVALUATED" for item in results)


def test_final_option_sid_tie_break_is_stable(tmp_path: Path) -> None:
    descriptor = _adapter_descriptor()
    first = _candidate("z", descriptor=descriptor)
    second = _candidate("a", descriptor=descriptor)
    policy_path = _active_policy_path(tmp_path)

    result = select_qqq_option_contract(
        _request(candidates=(first, second), descriptor=descriptor),
        policy_path=policy_path,
    )

    assert result.decision.selected_option_sid == min(first.option_sid, second.option_sid)


def test_long_put_never_selects_call(tmp_path: Path) -> None:
    descriptor = _adapter_descriptor()
    call = _candidate("call", descriptor=descriptor, right="CALL", delta="0.50")
    put = _candidate("put", descriptor=descriptor, right="PUT", delta="-0.50")
    policy_path = _active_policy_path(tmp_path)

    result = select_qqq_option_contract(
        _request(signal="LONG_PUT", candidates=(call, put), descriptor=descriptor),
        policy_path=policy_path,
    )

    assert result.decision.selected_option_sid == put.option_sid
    rejected = {item.reason_code: item.count for item in result.decision.rejected_counts}
    assert rejected["WRONG_OPTION_RIGHT"] == 1


def test_no_eligible_contract_does_not_widen_policy(tmp_path: Path) -> None:
    descriptor = _adapter_descriptor()
    candidate = _candidate("illiquid", descriptor=descriptor, open_interest=0, volume=0)
    policy_path = _active_policy_path(tmp_path)

    result = select_qqq_option_contract(
        _request(candidates=(candidate,), descriptor=descriptor),
        policy_path=policy_path,
    )

    assert result.decision.selected_option_sid is None
    assert result.decision.no_contract_reason == "NO_ELIGIBLE_CONTRACT_CASH"
    assert result.cash_preservation_required is True
    assert result.candidate_snapshots[0].eligible is False
    reasons = {item.reason_code for item in result.decision.rejected_counts}
    assert {"OPEN_INTEREST_BELOW_MIN", "VOLUME_BELOW_MIN"} <= reasons


@pytest.mark.parametrize(
    ("check_id", "reason_code"),
    (
        ("quote_freshness", "QUOTE_FRESHNESS_NOT_PASS"),
        ("open_interest_freshness", "OPEN_INTEREST_FRESHNESS_NOT_PASS"),
        ("engine_identity", "ENGINE_IDENTITY_NOT_PASS"),
    ),
)
def test_selection_stage_unknown_never_becomes_eligible(
    tmp_path: Path, check_id: str, reason_code: str
) -> None:
    descriptor = _adapter_descriptor()
    candidate = _candidate(
        "unknown",
        descriptor=descriptor,
        dq_overrides={check_id: ("NOT_EVALUATED", f"{check_id.upper()}_UNKNOWN")},
    )
    policy_path = _active_policy_path(tmp_path)

    result = select_qqq_option_contract(
        _request(candidates=(candidate,), descriptor=descriptor),
        policy_path=policy_path,
    )

    assert result.decision.selected_option_sid is None
    assert reason_code in {item.reason_code for item in result.decision.rejected_counts}


@pytest.mark.parametrize(
    ("field", "reason_code"),
    (
        ("prior_day_model_as_of_session", "MODEL_SESSION_NOT_EXACT_PRIOR"),
        ("open_interest_as_of_session", "OI_SESSION_NOT_EXACT_PRIOR"),
        ("volume_as_of_session", "VOLUME_SESSION_NOT_EXACT_PRIOR"),
    ),
)
def test_wrong_prior_session_is_typed_rejection(
    tmp_path: Path, field: str, reason_code: str
) -> None:
    descriptor = _adapter_descriptor()
    wrong_session = date(2021, 2, 19)
    if field == "prior_day_model_as_of_session":
        candidate = _candidate("wrong-session", descriptor=descriptor, model_session=wrong_session)
    elif field == "open_interest_as_of_session":
        candidate = _candidate("wrong-session", descriptor=descriptor, oi_session=wrong_session)
    else:
        candidate = _candidate("wrong-session", descriptor=descriptor, volume_session=wrong_session)
    policy_path = _active_policy_path(tmp_path)

    result = select_qqq_option_contract(
        _request(candidates=(candidate,), descriptor=descriptor),
        policy_path=policy_path,
    )

    assert reason_code in {item.reason_code for item in result.decision.rejected_counts}


def test_stale_quote_is_rejected_without_same_bar_fallback(tmp_path: Path) -> None:
    descriptor = _adapter_descriptor()
    candidate = _candidate(
        "stale",
        descriptor=descriptor,
        quote_end=datetime(2021, 2, 23, 14, 20, tzinfo=UTC),
    )
    policy_path = _active_policy_path(tmp_path)

    result = select_qqq_option_contract(
        _request(candidates=(candidate,), descriptor=descriptor),
        policy_path=policy_path,
    )

    assert result.decision.selected_option_sid is None
    assert result.candidate_snapshots[0].quote_validity == "STALE"
    assert "QUOTE_TOO_OLD" in {item.reason_code for item in result.decision.rejected_counts}


@pytest.mark.parametrize(
    ("case", "reason_code"),
    (
        ("dte_below", "DTE_BELOW_MIN"),
        ("dte_above", "DTE_ABOVE_MAX"),
        ("moneyness", "MONEYNESS_OUTSIDE_POLICY"),
        ("delta_below", "DELTA_BELOW_MIN"),
        ("delta_above", "DELTA_ABOVE_MAX"),
        ("spread", "SPREAD_TOO_WIDE"),
    ),
)
def test_each_numeric_gate_is_explicit_and_fail_closed(
    tmp_path: Path, case: str, reason_code: str
) -> None:
    descriptor = _adapter_descriptor()
    if case == "dte_below":
        candidate = _candidate(case, descriptor=descriptor, expiry=date(2021, 3, 1))
    elif case == "dte_above":
        candidate = _candidate(case, descriptor=descriptor, expiry=date(2021, 4, 30))
    elif case == "moneyness":
        candidate = _candidate(case, descriptor=descriptor, strike="400")
    elif case == "delta_below":
        candidate = _candidate(case, descriptor=descriptor, delta="0.10")
    elif case == "delta_above":
        candidate = _candidate(case, descriptor=descriptor, delta="0.90")
    else:
        candidate = _candidate(case, descriptor=descriptor, bid="1.00", ask="2.00")
    result = select_qqq_option_contract(
        _request(candidates=(candidate,), descriptor=descriptor),
        policy_path=_active_policy_path(tmp_path),
    )

    assert result.decision.selected_option_sid is None
    assert reason_code in {item.reason_code for item in result.decision.rejected_counts}


def test_crossed_quote_and_duplicate_sid_fail_closed() -> None:
    with pytest.raises(ValidationError, match="crossed"):
        _candidate("crossed", bid="5.20", ask="5.10")

    descriptor = _adapter_descriptor()
    candidate = _candidate("duplicate", descriptor=descriptor)
    with pytest.raises(ValidationError, match="unique"):
        _request(candidates=(candidate, candidate), descriptor=descriptor)


def test_future_quote_and_naive_selection_time_fail_closed() -> None:
    descriptor = _adapter_descriptor()
    future = _candidate(
        "future",
        descriptor=descriptor,
        quote_end=datetime(2021, 2, 23, 14, 33, tzinfo=UTC),
    )
    with pytest.raises(ValidationError, match="future"):
        _request(candidates=(future,), descriptor=descriptor)

    payload = _request().model_dump()
    payload["selection_snapshot_utc"] = datetime(2021, 2, 23, 14, 32)
    with pytest.raises(ValidationError, match="timezone-aware"):
        QQQOptionSelectionRequest(**payload)


def test_dq_report_bytes_hash_canonical_and_source_binding_are_strict(
    tmp_path: Path,
) -> None:
    descriptor = _adapter_descriptor()
    baseline = _candidate("tamper", descriptor=descriptor)
    tampered = baseline.dq_report_bytes.replace(b"NOT_EVALUATED", b"FAIL", 1)
    payload = baseline.model_dump()
    payload["dq_report_bytes"] = tampered
    payload["dq_report_sha256"] = hashlib.sha256(tampered).hexdigest()
    with pytest.raises(ValidationError):
        QQQOptionSelectionCandidateInput(**payload)

    wrong_source = _candidate(
        "wrong-source",
        descriptor=descriptor,
        dq_descriptor_sha256=_SHA_A,
    )
    with pytest.raises(QQQOptionSelectionContractError, match="DQ_SOURCE_MISMATCH"):
        select_qqq_option_contract(
            _request(candidates=(wrong_source,), descriptor=descriptor),
            policy_path=_active_policy_path(tmp_path),
        )


def test_model_copy_cannot_bypass_descriptor_content_hash() -> None:
    descriptor = _adapter_descriptor()
    forged = descriptor.model_copy(update={"repository_code_sha": "f" * 40})
    with pytest.raises(
        (ValidationError, QQQOptionSelectionContractError),
        match="content hash|canonical|code ids",
    ):
        _request(descriptor=forged)


def test_candidate_set_hash_binds_policy_descriptor_dq_and_raw_source(
    tmp_path: Path,
) -> None:
    descriptor = _adapter_descriptor()
    first = _candidate("hash-a", descriptor=descriptor)
    second = _candidate("hash-b", descriptor=descriptor)
    request = _request(candidates=(first, second), descriptor=descriptor)
    policy_path = _active_policy_path(tmp_path)
    baseline = build_qqq_option_selection_candidate_set_sha256(request, policy_path=policy_path)

    changed = _candidate("hash-b", descriptor=descriptor, volume=21)
    changed_request = _request(candidates=(first, changed), descriptor=descriptor)

    assert baseline == build_qqq_option_selection_candidate_set_sha256(
        _request(candidates=(second, first), descriptor=descriptor),
        policy_path=policy_path,
    )
    assert baseline != build_qqq_option_selection_candidate_set_sha256(
        changed_request, policy_path=policy_path
    )


def test_policy_loader_rejects_unresolved_numeric_active_unknown_hash_and_extra(
    tmp_path: Path,
) -> None:
    mutations: list[dict[str, Any]] = []

    unresolved_numeric = _default_policy_payload()
    unresolved_numeric["criteria"]["min_dte"] = 1
    mutations.append(unresolved_numeric)

    active_unknown = _default_policy_payload()
    active_unknown["status"] = "OWNER_REVIEWED_ACTIVE"
    active_unknown["selection_authorized"] = True
    mutations.append(active_unknown)

    wrong_hash = _default_policy_payload()
    wrong_hash["adapter_policy_sha256"] = _SHA_A
    mutations.append(wrong_hash)

    extra = _default_policy_payload()
    extra["temporary_fallback"] = True
    mutations.append(extra)

    for index, payload in enumerate(mutations):
        path = tmp_path / f"invalid-{index}.yaml"
        path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        with pytest.raises(
            QQQOptionSelectionContractError,
            match="QQQ_OPTION_SELECTION_POLICY_INVALID",
        ):
            load_qqq_option_selection_policy(path)


def test_active_policy_requires_complete_rank_and_option_sid_last(tmp_path: Path) -> None:
    missing_path = _active_policy_path(
        tmp_path,
        rank_components=["dte_distance", "option_sid"],
    )
    with pytest.raises(QQQOptionSelectionContractError, match="complete"):
        load_qqq_option_selection_policy(missing_path)

    payload = _default_policy_payload()
    active_path = _active_policy_path(tmp_path)
    active = yaml.safe_load(active_path.read_text(encoding="utf-8"))
    rank = active["criteria"]["rank_components"]
    active["criteria"]["rank_components"] = [rank[-1], *rank[:-1]]
    payload.update(active)
    not_last = tmp_path / "option-sid-not-last.yaml"
    not_last.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(QQQOptionSelectionContractError, match="final stable"):
        load_qqq_option_selection_policy(not_last)


def test_active_policy_rejects_reversed_ranges(tmp_path: Path) -> None:
    with pytest.raises(QQQOptionSelectionContractError, match="min <= target <= max"):
        load_qqq_option_selection_policy(
            _active_policy_path(tmp_path, min_dte=30, target_dte=20, max_dte=10)
        )


def test_selected_shared_records_replay_exactly(tmp_path: Path) -> None:
    descriptor = _adapter_descriptor()
    candidate = _candidate("replay", descriptor=descriptor)
    result = select_qqq_option_contract(
        _request(candidates=(candidate,), descriptor=descriptor),
        policy_path=_active_policy_path(tmp_path),
    )

    assert (
        ContractCandidateSnapshotRecord.from_json_bytes(
            result.candidate_snapshots[0].canonical_bytes
        )
        == result.candidate_snapshots[0]
    )
    assert SelectionDecisionRecord.from_json_bytes(result.decision.canonical_bytes) == (
        result.decision
    )
    assert result.canonical_bytes == result.canonical_bytes


def test_default_policy_bytes_are_unchanged_by_synthetic_active_fixture(
    tmp_path: Path,
) -> None:
    path = PROJECT_ROOT / DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH
    before = path.read_bytes()
    synthetic = _active_policy_path(tmp_path)

    assert load_qqq_option_selection_policy(synthetic).policy.selection_authorized is True
    assert path.read_bytes() == before
    assert load_qqq_option_selection_policy().policy.selection_authorized is False
