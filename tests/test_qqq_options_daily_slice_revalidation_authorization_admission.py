from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from ai_trading_system.qqq_options_research import (
    daily_slice_revalidation_authorization_admission as admission_v4,
)
from ai_trading_system.qqq_options_research import (
    primary_window_derived_aggregate_collection_evidence_admission as admission_v1,
)

DailySliceOwnerDecisionCandidate = admission_v4.DailySliceOwnerDecisionCandidate
DailySliceRunAttemptConsumptionReceipt = admission_v4.DailySliceRunAttemptConsumptionReceipt
QCQQQOptionsDailySliceAuthorizationAdmissionError = (
    admission_v4.QCQQQOptionsDailySliceAuthorizationAdmissionError
)
admit_qc_qqq_options_daily_slice_owner_authorization = (
    admission_v4.admit_qc_qqq_options_daily_slice_owner_authorization
)
build_qc_qqq_options_daily_slice_external_action_ledger = (
    admission_v4.build_qc_qqq_options_daily_slice_external_action_ledger
)
build_qc_qqq_options_daily_slice_run_attempt_consumption = (
    admission_v4.build_qc_qqq_options_daily_slice_run_attempt_consumption
)
load_qc_qqq_options_daily_slice_revalidation_authorization_admission_policy = (
    admission_v4.load_qc_qqq_options_daily_slice_revalidation_authorization_admission_policy
)
validate_qc_qqq_options_daily_slice_owner_decision_candidate = (
    admission_v4.validate_qc_qqq_options_daily_slice_owner_decision_candidate
)
CollectionActionStatus = admission_v1.CollectionActionStatus
CollectionActionType = admission_v1.CollectionActionType
CollectionExternalAction = admission_v1.CollectionExternalAction
collector_v1 = admission_v4.collector_v1

ROOT = Path(__file__).resolve().parents[1]
REVIEWED_AT = datetime(2026, 8, 15, 1, tzinfo=UTC)
EXPIRY = "2026-08-21T00:00:00Z"
BACKTEST_ID = "trading2521backtest"
STATISTIC_VALUES: dict[str, int | float] = {
    "delta_max": 0.8,
    "delta_min": -0.8,
    "dte_days_max": 45,
    "dte_days_min": 1,
    "moneyness_ratio_max": 1.2,
    "moneyness_ratio_min": 0.8,
    "open_interest_max": 1000,
    "open_interest_min_nonzero": 1,
    "candidate_count": 20,
    "deterministic_tie_count": 2,
    "relative_spread_max": 0.5,
    "relative_spread_min": 0.01,
    "volume_max": 500,
    "volume_min_nonzero": 1,
    "ask_price_max": 20,
    "ask_price_min": 0.05,
    "missing_quote_count": 4,
    "one_sided_quote_count": 6,
    "two_sided_quote_count": 20,
}

def _owner_token(*, overrides: dict[str, str] | None = None) -> bytes:
    loaded = load_qc_qqq_options_daily_slice_revalidation_authorization_admission_policy(
        project_root=ROOT
    )
    policy = loaded.policy
    fields = (
        ("ordinary_pushed_main_sha", policy.predecessor_ordinary_pushed_main_sha),
        ("registration_base_repository_code_sha", policy.registration_base_repository_code_sha),
        ("revalidation_policy_file_sha256", policy.revalidation_policy_file_sha256),
        ("revalidation_policy_canonical_sha256", policy.revalidation_policy_canonical_sha256),
        (
            "revalidation_package_manifest_file_sha256",
            policy.revalidation_package_manifest_file_sha256,
        ),
        (
            "revalidation_package_manifest_content_sha256",
            policy.revalidation_package_manifest_content_sha256,
        ),
        ("proposal_content_sha256", policy.proposal_content_sha256),
        ("run_scope_content_sha256", policy.run_scope_content_sha256),
        ("corrected_project_code_lf_sha256", policy.corrected_project_code_lf_sha256),
        ("predecessor_failed_backtest_id", policy.predecessor_failed_backtest_id),
        (
            "predecessor_failed_result_file_sha256",
            policy.predecessor_failed_result_file_sha256,
        ),
        ("target_project_id", str(policy.target_project_id)),
        ("requested_range", "2021-02-22..2025-12-02"),
        ("expected_session_count", "1202"),
        ("maximum_project_mutations", "1"),
        ("maximum_cloud_backtests", "1"),
        ("maximum_orders", "0"),
        ("maximum_fills", "0"),
        ("collector", policy.collector_id),
        ("independent_reviewer", policy.independent_reviewer_id),
        ("authorization_expires_at_utc", EXPIRY),
        ("authorization_single_use", "true"),
        ("authorization_invalidates_after_first_run_attempt", "true"),
    )
    replacements = overrides or {}
    lines = (policy.expected_owner_decision_token,) + tuple(
        f"{key}:{replacements.get(key, value)}" for key, value in fields
    )
    return ("\n".join(lines) + "\n").encode()


def _admitted():
    return admit_qc_qqq_options_daily_slice_owner_authorization(
        admission_id="trading-2521-v4-admission",
        admitted_at_utc=REVIEWED_AT,
        owner_decision_bytes=_owner_token(),
        owner_decision_source="PROJECT_OWNER_CURRENT_CODEX_DIALOG",
        project_root=ROOT,
    )


def _actions(*, run_status: CollectionActionStatus = CollectionActionStatus.COMPLETED):
    code_hash = _admitted().collector_authorization.project_code_lf_sha256
    return (
        CollectionExternalAction(
            action_id="trading-2521-login",
            ordinal=1,
            action_type=CollectionActionType.QUANTCONNECT_LOGIN,
            occurred_at_utc=datetime(2026, 8, 15, 1, 1, tzinfo=UTC),
            status=CollectionActionStatus.COMPLETED,
            target_project_id=34808569,
        ),
        CollectionExternalAction(
            action_id="trading-2521-project-mutation",
            ordinal=2,
            action_type=CollectionActionType.MODIFY_EXISTING_DEDICATED_PROJECT_ONCE,
            occurred_at_utc=datetime(2026, 8, 15, 1, 2, tzinfo=UTC),
            status=CollectionActionStatus.COMPLETED,
            target_project_id=34808569,
            project_code_lf_sha256=code_hash,
        ),
        CollectionExternalAction(
            action_id="trading-2521-cloud-run",
            ordinal=3,
            action_type=CollectionActionType.RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST,
            occurred_at_utc=datetime(2026, 8, 15, 1, 3, tzinfo=UTC),
            status=run_status,
            target_project_id=34808569,
            project_code_lf_sha256=code_hash,
            backtest_id=BACKTEST_ID,
            failure_reason_code=(
                "PLATFORM_RUN_FAILED" if run_status is CollectionActionStatus.FAILED else None
            ),
        ),
    )


def _runtime_identity(proposal: Any) -> str:
    return (
        "schema=qc_qqq_options_derived_aggregate_collector_runtime.v1"
        f"|scope={proposal.run_scope.content_sha256}"
        f"|repository={proposal.run_scope.repository_code_sha}"
        f"|policy_file={proposal.collector_policy_file_sha256}"
        f"|policy_canonical={proposal.collector_policy_canonical_sha256}"
        f"|transport={proposal.transport_map_sha256}"
    )


def _result_bytes() -> bytes:
    admission = load_qc_qqq_options_daily_slice_revalidation_authorization_admission_policy(
        project_root=ROOT
    )
    proposal = admission.revalidation_package.proposal
    collector = collector_v1.load_qc_qqq_options_primary_window_derived_aggregate_collector_policy(
        project_root=ROOT
    )
    transport = collector.policy.transport
    zone = ZoneInfo(transport.algorithm_time_zone)
    series: dict[str, Any] = {}
    for series_mapping in transport.expected_series:
        values: list[list[int | float]] = []
        for session_id in proposal.run_scope.session_ids:
            for statistic in series_mapping.mappings:
                point_time = datetime.combine(
                    session_id,
                    time(
                        transport.point_local_hour,
                        transport.point_local_minute,
                        statistic.ordinal_second,
                    ),
                    tzinfo=zone,
                )
                values.append(
                    [int(point_time.timestamp()), STATISTIC_VALUES[statistic.statistic_id]]
                )
        series[series_mapping.series_id] = {
            "name": series_mapping.series_id,
            "unit": series_mapping.unit_id,
            "seriesType": 1,
            "values": values,
        }
    scope = proposal.run_scope
    payload = {
        "algorithmConfiguration": {
            "startDate": f"{scope.requested_start.isoformat()}T00:00:00Z",
            "endDate": f"{scope.requested_end.isoformat()}T23:59:59Z",
        },
        "charts": {
            "TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1": {
                "name": "TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1",
                "chartType": 0,
                "series": series,
            }
        },
        "orders": {},
        "runtimeStatistics": {
            "TRADING2512_IDENTITY": _runtime_identity(proposal),
            "TRADING2512_TERMINAL": (
                f"status=COMPLETE|observed_sessions={len(scope.session_ids)}"
                "|invalid_sessions=0|orders=0|fills=0|portfolio_invested=false"
                "|raw_rows=false|log_data=false|object_store=false"
            ),
            "Holdings": "$0.00",
            "Unrealized": "$0.00",
            "Volume": "$0.00",
        },
        "state": {
            "RuntimeError": "",
            "OrderCount": "0",
            "Hostname": f"BACKTESTING-1-{BACKTEST_ID}",
            "Status": "Completed",
        },
        "statistics": {"Total Orders": "0", "Total Fees": "$0.00"},
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()


def _complete_actions(result_bytes: bytes) -> tuple[CollectionExternalAction, ...]:
    admitted = _admitted()
    code_hash = admitted.collector_authorization.project_code_lf_sha256
    return _actions() + (
        CollectionExternalAction(
            action_id="trading-2521-result-download",
            ordinal=4,
            action_type=CollectionActionType.EXPORT_SAFE_MANUAL_DOWNLOAD_RESULTS_COLLECTION,
            occurred_at_utc=datetime(2026, 8, 15, 1, 4, tzinfo=UTC),
            status=CollectionActionStatus.COMPLETED,
            target_project_id=34808569,
            project_code_lf_sha256=code_hash,
            backtest_id=BACKTEST_ID,
            result_file_sha256=hashlib.sha256(result_bytes).hexdigest(),
        ),
    )


def test_policy_exactly_binds_2520_revalidation_authority() -> None:
    loaded = load_qc_qqq_options_daily_slice_revalidation_authorization_admission_policy(
        project_root=ROOT
    )
    policy = loaded.policy
    package = loaded.revalidation_package
    assert policy.predecessor_ordinary_pushed_main_sha == (
        "2dc9171ad5f56fc0a9c31b5d388c7d37eb499b8b"
    )
    assert policy.revalidation_policy_file_sha256 == (
        "f9f859568e34c836a2453b175dc283cbdeec7a009887f6f868beccaabd14f35c"
    )
    assert package.manifest.content_sha256 == policy.revalidation_package_manifest_content_sha256
    assert package.proposal.content_sha256 == policy.proposal_content_sha256
    assert package.run_scope.content_sha256 == policy.run_scope_content_sha256
    assert package.manifest.project_code_lf_sha256 == policy.corrected_project_code_lf_sha256
    assert len(package.run_scope.session_ids) == 1202
    assert policy.safety.owner_token_observed is False
    assert policy.safety.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"


def test_exact_current_dialog_v4_token_admits_without_consuming() -> None:
    candidate = validate_qc_qqq_options_daily_slice_owner_decision_candidate(
        owner_decision_bytes=_owner_token(),
        reviewed_at_utc=REVIEWED_AT,
        project_root=ROOT,
    )
    assert candidate.decision == "OWNER_V4_AUTHORIZATION_REVIEWED_NOT_CONSUMED"
    assert candidate.authorization_consumed is False
    admitted = _admitted()
    receipt = admitted.daily_slice_admission_receipt
    assert receipt.decision == "OWNER_V4_AUTHORIZATION_ADMITTED_UNUSED"
    assert receipt.authorization_consumed is False
    assert receipt.external_action_performed is False
    assert admitted.collector_authorization.maximum_orders == 0


@pytest.mark.parametrize(
    "raw",
    (
        b"arbitrary self-reported PASS\n",
        _owner_token().replace(b"TRADING-2520", b"TRADING-2518", 1),
        _owner_token().replace(b"\n", b"\r\n"),
        _owner_token() + b"\n",
    ),
)
def test_missing_old_local_or_noncanonical_token_fails_closed(raw: bytes) -> None:
    with pytest.raises(QCQQQOptionsDailySliceAuthorizationAdmissionError):
        validate_qc_qqq_options_daily_slice_owner_decision_candidate(
            owner_decision_bytes=raw,
            reviewed_at_utc=REVIEWED_AT,
            project_root=ROOT,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("ordinary_pushed_main_sha", "0" * 40),
        ("revalidation_package_manifest_content_sha256", "0" * 64),
        ("corrected_project_code_lf_sha256", "0" * 64),
        ("target_project_id", "1"),
        ("requested_range", "2022-12-01..2025-12-02"),
        ("expected_session_count", "1201"),
        ("maximum_cloud_backtests", "2"),
        ("maximum_orders", "1"),
        ("independent_reviewer", "caller"),
        ("authorization_single_use", "false"),
    ),
)
def test_authority_scope_and_cap_mismatch_fail_closed(field: str, value: str) -> None:
    with pytest.raises(QCQQQOptionsDailySliceAuthorizationAdmissionError):
        validate_qc_qqq_options_daily_slice_owner_decision_candidate(
            owner_decision_bytes=_owner_token(overrides={field: value}),
            reviewed_at_utc=REVIEWED_AT,
            project_root=ROOT,
        )


def test_expired_token_or_wrong_source_fails_before_action() -> None:
    with pytest.raises(QCQQQOptionsDailySliceAuthorizationAdmissionError):
        validate_qc_qqq_options_daily_slice_owner_decision_candidate(
            owner_decision_bytes=_owner_token(),
            reviewed_at_utc=datetime(2026, 8, 21, 0, 0, 1, tzinfo=UTC),
            project_root=ROOT,
        )
    with pytest.raises(QCQQQOptionsDailySliceAuthorizationAdmissionError):
        admit_qc_qqq_options_daily_slice_owner_authorization(
            admission_id="trading-2521-wrong-source",
            admitted_at_utc=REVIEWED_AT,
            owner_decision_bytes=_owner_token(),
            owner_decision_source="LOCAL_DRY_RUN",
            project_root=ROOT,
        )


def test_action_input_permutation_replays_to_one_ledger_identity() -> None:
    admitted = _admitted()
    actions = _actions()
    forward = build_qc_qqq_options_daily_slice_external_action_ledger(
        ledger_id="trading-2521-ledger",
        sealed_at_utc=datetime(2026, 8, 15, 1, 4, tzinfo=UTC),
        admitted_authorization=admitted,
        actions=actions,
    )
    reverse = build_qc_qqq_options_daily_slice_external_action_ledger(
        ledger_id="trading-2521-ledger",
        sealed_at_utc=datetime(2026, 8, 15, 1, 4, tzinfo=UTC),
        admitted_authorization=admitted,
        actions=tuple(reversed(actions)),
    )
    assert forward.canonical_bytes == reverse.canonical_bytes
    assert forward.lifecycle_status == "INCOMPLETE"


@pytest.mark.parametrize(
    "run_status", (CollectionActionStatus.COMPLETED, CollectionActionStatus.FAILED)
)
def test_first_cloud_run_attempt_consumes_even_when_run_fails(
    run_status: CollectionActionStatus,
) -> None:
    admitted = _admitted()
    ledger = build_qc_qqq_options_daily_slice_external_action_ledger(
        ledger_id=f"trading-2521-{run_status.value.lower()}-ledger",
        sealed_at_utc=datetime(2026, 8, 15, 1, 4, tzinfo=UTC),
        admitted_authorization=admitted,
        actions=_actions(run_status=run_status),
    )
    receipt = build_qc_qqq_options_daily_slice_run_attempt_consumption(
        consumption_id=f"trading-2521-{run_status.value.lower()}-consumption",
        recorded_at_utc=datetime(2026, 8, 15, 1, 5, tzinfo=UTC),
        admitted_authorization=admitted,
        external_action_ledger=ledger,
    )
    assert receipt.authorization_consumed is True
    assert receipt.authorization_invalidated_for_further_cloud_runs is True
    assert receipt.run_status is run_status
    assert receipt.orders == receipt.fills == 0
    assert receipt.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"


def test_prior_consumption_blocks_second_cloud_run() -> None:
    admitted = _admitted()
    ledger = build_qc_qqq_options_daily_slice_external_action_ledger(
        ledger_id="trading-2521-first-ledger",
        sealed_at_utc=datetime(2026, 8, 15, 1, 4, tzinfo=UTC),
        admitted_authorization=admitted,
        actions=_actions(),
    )
    first = build_qc_qqq_options_daily_slice_run_attempt_consumption(
        consumption_id="trading-2521-first-consumption",
        recorded_at_utc=datetime(2026, 8, 15, 1, 5, tzinfo=UTC),
        admitted_authorization=admitted,
        external_action_ledger=ledger,
    )
    with pytest.raises(
        QCQQQOptionsDailySliceAuthorizationAdmissionError,
        match="OWNER_V4_AUTHORIZATION_ALREADY_CONSUMED",
    ):
        build_qc_qqq_options_daily_slice_run_attempt_consumption(
            consumption_id="trading-2521-second-consumption",
            recorded_at_utc=datetime(2026, 8, 15, 1, 6, tzinfo=UTC),
            admitted_authorization=admitted,
            external_action_ledger=ledger,
            prior_consumption_receipts=(first,),
        )


def test_complete_result_uses_canonical_parser_and_stays_dq_not_evaluated() -> None:
    admitted = _admitted()
    partial_ledger = build_qc_qqq_options_daily_slice_external_action_ledger(
        ledger_id="trading-2521-result-run-ledger",
        sealed_at_utc=datetime(2026, 8, 15, 1, 3, 30, tzinfo=UTC),
        admitted_authorization=admitted,
        actions=_actions(),
    )
    run_consumption = build_qc_qqq_options_daily_slice_run_attempt_consumption(
        consumption_id="trading-2521-result-run-consumption",
        recorded_at_utc=datetime(2026, 8, 15, 1, 3, 40, tzinfo=UTC),
        admitted_authorization=admitted,
        external_action_ledger=partial_ledger,
    )
    result_bytes = _result_bytes()
    bundle = admission_v4.build_qc_qqq_options_daily_slice_parsed_result_admission(
        result_admission_id="trading-2521-result-admission",
        admitted_at_utc=datetime(2026, 8, 15, 1, 5, tzinfo=UTC),
        admitted_authorization=admitted,
        run_attempt_consumption=run_consumption,
        actions=_complete_actions(result_bytes),
        backtest_id=BACKTEST_ID,
        result_bytes=result_bytes,
        reviewed_project_code_lf_sha256=(
            admitted.collector_authorization.project_code_lf_sha256
        ),
        project_root=ROOT,
    )
    receipt = bundle.result_admission
    assert bundle.external_action_ledger.lifecycle_status == "COMPLETE"
    assert len(bundle.collector_evidence.session_ids) == 1202
    assert receipt.evidence_status == "RESULT_PARSED_DQ_NOT_EVALUATED"
    assert receipt.local_derived_aggregate_dq_status == "NOT_EVALUATED"
    assert receipt.local_derived_aggregate_pit_status == "NOT_EVALUATED"
    assert receipt.option_event_dq_status == "NOT_EVALUATED"
    assert receipt.orders == receipt.fills == 0
    assert receipt.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"


def test_result_missing_download_or_identity_mismatch_fails_closed() -> None:
    admitted = _admitted()
    partial_ledger = build_qc_qqq_options_daily_slice_external_action_ledger(
        ledger_id="trading-2521-negative-run-ledger",
        sealed_at_utc=datetime(2026, 8, 15, 1, 3, 30, tzinfo=UTC),
        admitted_authorization=admitted,
        actions=_actions(),
    )
    run_consumption = build_qc_qqq_options_daily_slice_run_attempt_consumption(
        consumption_id="trading-2521-negative-run-consumption",
        recorded_at_utc=datetime(2026, 8, 15, 1, 3, 40, tzinfo=UTC),
        admitted_authorization=admitted,
        external_action_ledger=partial_ledger,
    )
    with pytest.raises(
        QCQQQOptionsDailySliceAuthorizationAdmissionError,
        match="DAILY_SLICE_RESULT_LIFECYCLE_INCOMPLETE",
    ):
        admission_v4.build_qc_qqq_options_daily_slice_parsed_result_admission(
            result_admission_id="trading-2521-missing-result",
            admitted_at_utc=datetime(2026, 8, 15, 1, 5, tzinfo=UTC),
            admitted_authorization=admitted,
            run_attempt_consumption=run_consumption,
            actions=_actions(),
            backtest_id=BACKTEST_ID,
            result_bytes=b"{}",
            reviewed_project_code_lf_sha256=(
                admitted.collector_authorization.project_code_lf_sha256
            ),
            project_root=ROOT,
        )


def test_canonical_records_reject_tamper_and_noncanonical_bytes() -> None:
    candidate = validate_qc_qqq_options_daily_slice_owner_decision_candidate(
        owner_decision_bytes=_owner_token(),
        reviewed_at_utc=REVIEWED_AT,
        project_root=ROOT,
    )
    assert DailySliceOwnerDecisionCandidate.from_json_bytes(candidate.canonical_bytes) == candidate
    payload = json.loads(candidate.canonical_bytes)
    payload["selection_authorized"] = True
    tampered = json.dumps(payload, sort_keys=True, indent=2).encode() + b"\n"
    with pytest.raises(QCQQQOptionsDailySliceAuthorizationAdmissionError):
        DailySliceOwnerDecisionCandidate.from_json_bytes(tampered)

    admitted = _admitted()
    ledger = build_qc_qqq_options_daily_slice_external_action_ledger(
        ledger_id="trading-2521-canonical-ledger",
        sealed_at_utc=datetime(2026, 8, 15, 1, 4, tzinfo=UTC),
        admitted_authorization=admitted,
        actions=_actions(),
    )
    receipt = build_qc_qqq_options_daily_slice_run_attempt_consumption(
        consumption_id="trading-2521-canonical-consumption",
        recorded_at_utc=datetime(2026, 8, 15, 1, 5, tzinfo=UTC),
        admitted_authorization=admitted,
        external_action_ledger=ledger,
    )
    replay = DailySliceRunAttemptConsumptionReceipt.from_json_bytes(receipt.canonical_bytes)
    assert replay == receipt
