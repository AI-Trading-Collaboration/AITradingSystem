from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    DQCheckResult,
    DQReportRecord,
    QQQOptionsSafetyBoundary,
)
from ai_trading_system.qqq_options_research.primary_window_derived_aggregate_collection_evidence_admission import (  # noqa: E501
    CollectionActionStatus,
    CollectionActionType,
    CollectionExternalAction,
    QCQQQOptionsCollectionEvidenceAdmissionBundle,
    QCQQQOptionsCollectionEvidenceAdmissionError,
    _validate_dq_report,
    admit_qc_qqq_options_primary_window_collection_owner_authorization,
    build_qc_qqq_options_collection_external_action_ledger,
    build_qc_qqq_options_primary_window_collection_evidence_admission,
    load_qc_qqq_options_collection_evidence_admission_policy,
)
from ai_trading_system.qqq_options_research.primary_window_export_safe_derived_aggregate_collector import (  # noqa: E501
    load_qc_qqq_options_primary_window_derived_aggregate_collector_policy,
)
from ai_trading_system.qqq_options_research.primary_window_policy_calibration import (
    load_qqq_options_primary_window_calibration_policy,
)

ROOT = Path(__file__).resolve().parents[1]
IMPLEMENTATION_SHA = "3ffc405c65dd6572d6e81f5cbc94fe2eecfe7701"
BACKTEST_ID = "trading2514backtest"
AUTH_AT = datetime(2026, 8, 12, 13, 0, tzinfo=UTC)
ACTION_START = datetime(2026, 8, 12, 14, 0, tzinfo=UTC)
DQ_AT = datetime(2026, 8, 12, 17, 30, tzinfo=UTC)
ADMISSION_AT = datetime(2026, 8, 12, 18, 0, tzinfo=UTC)
DQ_POLICY_SHA = "1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358"

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


def _owner_token() -> bytes:
    loaded = load_qc_qqq_options_collection_evidence_admission_policy(project_root=ROOT)
    policy = loaded.policy
    proposal = loaded.proposal_package.proposal
    fields = (
        ("ordinary_pushed_main_sha", policy.ordinary_pushed_main_sha),
        ("repository_code_sha", proposal.run_scope.repository_code_sha),
        ("proposal_content_sha256", policy.proposal_content_sha256),
        ("run_scope_content_sha256", policy.run_scope_content_sha256),
        ("project_code_lf_sha256", policy.project_code_lf_sha256),
        ("proposal_policy_file_sha256", policy.proposal_policy_file_sha256),
        ("proposal_policy_canonical_sha256", policy.proposal_policy_canonical_sha256),
        ("collector_policy_file_sha256", policy.collector_policy_file_sha256),
        ("collector_policy_canonical_sha256", policy.collector_policy_canonical_sha256),
        ("transport_map_sha256", policy.transport_map_sha256),
        ("target_project_id", str(policy.target_project_id)),
        ("requested_range", f"{policy.requested_start}..{policy.requested_end}"),
        ("expected_session_count", str(policy.expected_session_count)),
        ("maximum_project_mutations", "1"),
        ("maximum_cloud_backtests", "1"),
        ("maximum_orders", "0"),
        ("maximum_fills", "0"),
        ("collector", policy.collector_id),
        ("independent_reviewer", policy.independent_reviewer_id),
        ("authorization_expires_at_utc", "2026-08-19T00:00:00Z"),
        ("authorization_single_use", "true"),
        ("authorization_invalidates_after_evidence_collection", "true"),
    )
    lines = (policy.expected_owner_decision_token,) + tuple(
        f"{key}:{value}" for key, value in fields
    )
    return ("\n".join(lines) + "\n").encode()


def _runtime_identity(proposal: Any) -> str:
    return (
        "schema=qc_qqq_options_derived_aggregate_collector_runtime.v1"
        f"|scope={proposal.run_scope.content_sha256}"
        f"|repository={proposal.run_scope.repository_code_sha}"
        f"|policy_file={proposal.collector_policy_file_sha256}"
        f"|policy_canonical={proposal.collector_policy_canonical_sha256}"
        f"|transport={proposal.transport_map_sha256}"
    )


def _result_payload() -> dict[str, Any]:
    admission = load_qc_qqq_options_collection_evidence_admission_policy(project_root=ROOT)
    proposal = admission.proposal_package.proposal
    collector = load_qc_qqq_options_primary_window_derived_aggregate_collector_policy(
        project_root=ROOT
    )
    zone = ZoneInfo(collector.policy.transport.algorithm_time_zone)
    series: dict[str, Any] = {}
    for series_mapping in collector.policy.transport.expected_series:
        values: list[list[int | float]] = []
        for session_id in proposal.run_scope.session_ids:
            for statistic in series_mapping.mappings:
                point_time = datetime.combine(
                    session_id,
                    time(
                        collector.policy.transport.point_local_hour,
                        collector.policy.transport.point_local_minute,
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
    return {
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
                "status=COMPLETE"
                f"|observed_sessions={len(scope.session_ids)}"
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


def _result_bytes(payload: dict[str, Any] | None = None, *, sorted_keys: bool = False) -> bytes:
    return json.dumps(
        payload or _result_payload(),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=sorted_keys,
    ).encode()


def _actions(result_bytes: bytes) -> tuple[CollectionExternalAction, ...]:
    policy = load_qc_qqq_options_collection_evidence_admission_policy(project_root=ROOT).policy
    shared = {
        "target_project_id": 34_808_569,
        "project_code_lf_sha256": policy.project_code_lf_sha256,
        "status": CollectionActionStatus.COMPLETED,
    }
    return (
        CollectionExternalAction(
            action_id="trading-2514-action-login",
            ordinal=1,
            action_type=CollectionActionType.QUANTCONNECT_LOGIN,
            occurred_at_utc=ACTION_START,
            target_project_id=34_808_569,
            status=CollectionActionStatus.COMPLETED,
        ),
        CollectionExternalAction(
            action_id="trading-2514-action-project",
            ordinal=2,
            action_type=CollectionActionType.MODIFY_EXISTING_DEDICATED_PROJECT_ONCE,
            occurred_at_utc=ACTION_START + timedelta(minutes=10),
            **shared,
        ),
        CollectionExternalAction(
            action_id="trading-2514-action-run",
            ordinal=3,
            action_type=CollectionActionType.RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST,
            occurred_at_utc=ACTION_START + timedelta(minutes=20),
            backtest_id=BACKTEST_ID,
            **shared,
        ),
        CollectionExternalAction(
            action_id="trading-2514-action-download",
            ordinal=4,
            action_type=CollectionActionType.EXPORT_SAFE_MANUAL_DOWNLOAD_RESULTS_COLLECTION,
            occurred_at_utc=ACTION_START + timedelta(minutes=30),
            backtest_id=BACKTEST_ID,
            result_file_sha256=hashlib.sha256(result_bytes).hexdigest(),
            **shared,
        ),
    )


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


def _dq_report(
    *,
    result_file_sha256: str,
    dq_status: str = "PASS",
    pit_status: str = "PASS",
    generated_at: datetime = DQ_AT,
) -> DQReportRecord:
    loaded = load_qc_qqq_options_collection_evidence_admission_policy(project_root=ROOT)
    proposal = loaded.proposal_package.proposal
    calibration = load_qqq_options_primary_window_calibration_policy(project_root=ROOT)
    check_ids = calibration.policy.dq_contract.required_check_ids
    checks = tuple(
        DQCheckResult(
            check_id=check_id,
            status=(dq_status if index == 0 and dq_status != "PASS" else "PASS"),
            reason_code=(
                "DERIVED_SOURCE_DQ_NOT_PASS" if index == 0 and dq_status != "PASS" else None
            ),
            observed_at_utc=generated_at,
        )
        for index, check_id in enumerate(check_ids)
    )
    scope = proposal.run_scope
    return DQReportRecord.seal(
        schema_name="dq_report",
        schema_version="1.0.0",
        run_id="dq-trading-2514-derived-source",
        record_id="dq-trading-2514-derived-source-record",
        created_at_utc=generated_at,
        producer_version="trading-2514-test-v1",
        repository_code_sha=scope.repository_code_sha,
        policy_id=calibration.policy.dq_contract.policy_id,
        policy_version=calibration.policy.dq_contract.policy_version,
        policy_sha256=DQ_POLICY_SHA,
        contract_schema_sha256=QQQ_OPTIONS_CONTRACT_SHA256,
        source_ids=(loaded.policy.dq_handoff.dataset_id,),
        source_checksums=(result_file_sha256,),
        requested_start=scope.requested_start,
        requested_end=scope.requested_end,
        evaluated_start=scope.evaluated_start,
        evaluated_end=scope.evaluated_end,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status=dq_status,
        pit_status=pit_status,
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id="dq-trading-2514-derived-source-lineage",
        safety=_safety(),
        scope=calibration.policy.dq_contract.scope,
        report_version=calibration.policy.dq_contract.report_version,
        generated_at_utc=generated_at,
        checks=checks,
    )


def _build(
    evidence_root: Path,
    *,
    result_bytes: bytes | None = None,
    dq_report: DQReportRecord | None = None,
    prior: tuple[Any, ...] = (),
) -> QCQQQOptionsCollectionEvidenceAdmissionBundle:
    result = result_bytes or _result_bytes()
    dq = dq_report or _dq_report(result_file_sha256=hashlib.sha256(result).hexdigest())
    relative = "dq/trading_2514_report.json"
    path = evidence_root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(dq.canonical_bytes)
    policy = load_qc_qqq_options_collection_evidence_admission_policy(project_root=ROOT).policy
    return build_qc_qqq_options_primary_window_collection_evidence_admission(
        evidence_admission_id="trading-2514-evidence-admission",
        authorization_admission_id="trading-2514-authorization-admission",
        authorization_consumption_id="trading-2514-authorization-consumption",
        action_ledger_id="trading-2514-action-ledger",
        authorization_admitted_at_utc=AUTH_AT,
        admitted_at_utc=ADMISSION_AT,
        implementation_repository_code_sha=IMPLEMENTATION_SHA,
        owner_decision_bytes=_owner_token(),
        actions=_actions(result),
        backtest_id=BACKTEST_ID,
        result_bytes=result,
        dq_report_path=relative,
        dq_report_bytes=dq.canonical_bytes,
        reviewed_project_code_lf_sha256=policy.project_code_lf_sha256,
        prior_consumption_receipts=prior,
        project_root=ROOT,
        evidence_root=evidence_root,
    )


@pytest.fixture(scope="module")
def positive_bundle(
    tmp_path_factory: pytest.TempPathFactory,
) -> QCQQQOptionsCollectionEvidenceAdmissionBundle:
    return _build(tmp_path_factory.mktemp("trading2514_positive"))


def test_policy_binds_exact_2513_2512_and_dq_authority() -> None:
    loaded = load_qc_qqq_options_collection_evidence_admission_policy(project_root=ROOT)
    policy = loaded.policy
    assert policy.ordinary_pushed_main_sha == "f6505359ab6697c4c54bc42807026f34685d97a8"
    assert policy.proposal_content_sha256 == loaded.proposal_package.proposal.content_sha256
    assert policy.run_scope_content_sha256 == loaded.proposal_package.run_scope.content_sha256
    assert policy.expected_session_count == 1202
    assert policy.requested_start.isoformat() == "2021-02-22"
    assert policy.safety.authorization_status == "OWNER_AUTHORIZATION_NOT_PROVIDED"
    assert policy.safety.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert policy.safety.owner_policy_value_count == 0


def test_missing_arbitrary_and_reordered_owner_token_fail_closed() -> None:
    with pytest.raises(
        QCQQQOptionsCollectionEvidenceAdmissionError,
        match="OWNER_AUTHORIZATION_NOT_PROVIDED",
    ):
        admit_qc_qqq_options_primary_window_collection_owner_authorization(
            admission_id="trading-2514-auth",
            admitted_at_utc=AUTH_AT,
            owner_decision_bytes=b"arbitrary self-reported PASS\n",
            project_root=ROOT,
        )
    lines = _owner_token().decode().splitlines()
    lines[1], lines[2] = lines[2], lines[1]
    with pytest.raises(
        QCQQQOptionsCollectionEvidenceAdmissionError,
        match="field inventory/order drifted",
    ):
        admit_qc_qqq_options_primary_window_collection_owner_authorization(
            admission_id="trading-2514-auth",
            admitted_at_utc=AUTH_AT,
            owner_decision_bytes=("\n".join(lines) + "\n").encode(),
            project_root=ROOT,
        )


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("expected_session_count", "1201"),
        ("maximum_orders", "1"),
        ("target_project_id", "1"),
        ("requested_range", "2022-12-01..2025-12-02"),
        ("authorization_single_use", "false"),
    ),
)
def test_owner_token_scope_hash_caps_and_primary_window_mismatch_fail(
    field: str, replacement: str
) -> None:
    lines = _owner_token().decode().splitlines()
    lines = [f"{field}:{replacement}" if line.startswith(f"{field}:") else line for line in lines]
    with pytest.raises(
        QCQQQOptionsCollectionEvidenceAdmissionError,
        match="OWNER_AUTHORIZATION_BINDING_MISMATCH",
    ):
        admit_qc_qqq_options_primary_window_collection_owner_authorization(
            admission_id="trading-2514-auth",
            admitted_at_utc=AUTH_AT,
            owner_decision_bytes=("\n".join(lines) + "\n").encode(),
            project_root=ROOT,
        )


def test_owner_token_expiry_is_enforced() -> None:
    with pytest.raises(
        QCQQQOptionsCollectionEvidenceAdmissionError, match="OWNER_AUTHORIZATION_EXPIRED"
    ):
        admit_qc_qqq_options_primary_window_collection_owner_authorization(
            admission_id="trading-2514-auth",
            admitted_at_utc=datetime(2026, 8, 19, 0, 0, 1, tzinfo=UTC),
            owner_decision_bytes=_owner_token(),
            project_root=ROOT,
        )


def test_action_input_permutation_replays_to_one_chronological_identity() -> None:
    loaded, authorization, receipt = (
        admit_qc_qqq_options_primary_window_collection_owner_authorization(
            admission_id="trading-2514-auth",
            admitted_at_utc=AUTH_AT,
            owner_decision_bytes=_owner_token(),
            project_root=ROOT,
        )
    )
    del loaded
    result = _result_bytes()
    forward = build_qc_qqq_options_collection_external_action_ledger(
        ledger_id="trading-2514-ledger",
        sealed_at_utc=ADMISSION_AT,
        authorization=authorization,
        authorization_admission=receipt,
        actions=_actions(result),
    )
    reverse = build_qc_qqq_options_collection_external_action_ledger(
        ledger_id="trading-2514-ledger",
        sealed_at_utc=ADMISSION_AT,
        authorization=authorization,
        authorization_admission=receipt,
        actions=tuple(reversed(_actions(result))),
    )
    assert forward.canonical_bytes == reverse.canonical_bytes
    assert forward.lifecycle_status == "COMPLETE"
    assert forward.scope_status == "PASS"


def test_partial_or_failed_action_lifecycle_never_passes() -> None:
    _, authorization, receipt = admit_qc_qqq_options_primary_window_collection_owner_authorization(
        admission_id="trading-2514-auth",
        admitted_at_utc=AUTH_AT,
        owner_decision_bytes=_owner_token(),
        project_root=ROOT,
    )
    partial = build_qc_qqq_options_collection_external_action_ledger(
        ledger_id="trading-2514-partial-ledger",
        sealed_at_utc=ADMISSION_AT,
        authorization=authorization,
        authorization_admission=receipt,
        actions=_actions(_result_bytes())[:3],
    )
    assert partial.lifecycle_status == "INCOMPLETE"
    assert partial.scope_status == "FAIL"
    assert partial.reason_codes == ("EXTERNAL_ACTION_LIFECYCLE_INCOMPLETE",)


def test_complete_result_and_canonical_dq_pit_build_policy_blocked_handoff(
    positive_bundle: QCQQQOptionsCollectionEvidenceAdmissionBundle,
) -> None:
    bundle = positive_bundle
    receipt = bundle.evidence_admission
    assert bundle.external_action_ledger.lifecycle_status == "COMPLETE"
    assert bundle.collector_evidence.orders == bundle.collector_evidence.fills == 0
    assert len(bundle.collector_evidence.session_ids) == 1202
    assert len(bundle.collector_evidence.observations) == 9 * 1202
    assert receipt.local_derived_aggregate_dq_status == "PASS"
    assert receipt.local_derived_aggregate_pit_status == "PASS"
    assert receipt.option_event_dq_status == "NOT_EVALUATED"
    assert receipt.decision == "EVIDENCE_ADMITTED_DQ_PIT_PASS_POLICY_BLOCKED"
    assert receipt.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert receipt.selection_authorized is False
    assert receipt.owner_policy_value_count == 0
    assert bundle.source_bundle.contains_raw_option_rows is False
    assert bundle.authorization_consumption.authorization_consumed is True


def test_result_object_key_permutation_preserves_payload_semantics_not_file_identity(
    positive_bundle: QCQQQOptionsCollectionEvidenceAdmissionBundle,
    tmp_path: Path,
) -> None:
    permuted = _result_bytes(_result_payload(), sorted_keys=True)
    result_sha = hashlib.sha256(permuted).hexdigest()
    second = _build(
        tmp_path,
        result_bytes=permuted,
        dq_report=_dq_report(result_file_sha256=result_sha),
    )
    assert (
        second.collector_evidence.result_payload_sha256
        == positive_bundle.collector_evidence.result_payload_sha256
    )
    assert (
        second.collector_evidence.result_file_sha256
        != positive_bundle.collector_evidence.result_file_sha256
    )
    assert second.source_bundle.source_checksum == result_sha


@pytest.mark.parametrize(("dq_status", "pit_status"), (("FAIL", "PASS"), ("PASS", "NOT_EVALUATED")))
def test_semantic_dq_or_pit_non_pass_never_admits(
    positive_bundle: QCQQQOptionsCollectionEvidenceAdmissionBundle,
    tmp_path: Path,
    dq_status: str,
    pit_status: str,
) -> None:
    report = _dq_report(
        result_file_sha256=positive_bundle.collector_evidence.result_file_sha256,
        dq_status=dq_status,
        pit_status=pit_status,
    )
    path = tmp_path / "report.json"
    path.write_bytes(report.canonical_bytes)
    with pytest.raises(
        QCQQQOptionsCollectionEvidenceAdmissionError, match="COLLECTION_DQ_PIT_REJECTED"
    ):
        _validate_dq_report(
            raw=report.canonical_bytes,
            dq_report_path="report.json",
            evidence_root=tmp_path,
            collector_evidence=positive_bundle.collector_evidence,
            admitted_at_utc=ADMISSION_AT,
            project_root=ROOT,
            policy=positive_bundle.policy_load.policy,
        )


def test_dq_scope_asof_and_checksum_mismatch_fail_closed(
    positive_bundle: QCQQQOptionsCollectionEvidenceAdmissionBundle,
    tmp_path: Path,
) -> None:
    base = _dq_report(result_file_sha256=positive_bundle.collector_evidence.result_file_sha256)
    mutations = (
        {"source_checksums": ("f" * 64,)},
        {
            "created_at_utc": ADMISSION_AT + timedelta(seconds=1),
            "generated_at_utc": ADMISSION_AT + timedelta(seconds=1),
        },
        {
            "requested_start": base.requested_start + timedelta(days=1),
            "evaluated_start": base.evaluated_start + timedelta(days=1),
        },
    )
    for index, mutation in enumerate(mutations):
        report = DQReportRecord.seal(
            **{
                **base.model_dump(mode="python", exclude={"content_sha256"}),
                **mutation,
            }
        )
        relative = f"report-{index}.json"
        (tmp_path / relative).write_bytes(report.canonical_bytes)
        with pytest.raises(
            QCQQQOptionsCollectionEvidenceAdmissionError,
            match="COLLECTION_DQ_PIT_REJECTED",
        ):
            _validate_dq_report(
                raw=report.canonical_bytes,
                dq_report_path=relative,
                evidence_root=tmp_path,
                collector_evidence=positive_bundle.collector_evidence,
                admitted_at_utc=ADMISSION_AT,
                project_root=ROOT,
                policy=positive_bundle.policy_load.policy,
            )


def test_noncanonical_unknown_dq_taxonomy_is_rejected(
    positive_bundle: QCQQQOptionsCollectionEvidenceAdmissionBundle,
    tmp_path: Path,
) -> None:
    report = _dq_report(result_file_sha256=positive_bundle.collector_evidence.result_file_sha256)
    unknown = report.canonical_bytes.replace(b'"pit_status": "PASS"', b'"pit_status": "UNKNOWN"')
    path = tmp_path / "unknown.json"
    path.write_bytes(unknown)
    with pytest.raises(
        QCQQQOptionsCollectionEvidenceAdmissionError,
        match="COLLECTION_DQ_PIT_REJECTED",
    ):
        _validate_dq_report(
            raw=unknown,
            dq_report_path="unknown.json",
            evidence_root=tmp_path,
            collector_evidence=positive_bundle.collector_evidence,
            admitted_at_utc=ADMISSION_AT,
            project_root=ROOT,
            policy=positive_bundle.policy_load.policy,
        )


def test_single_use_authorization_replay_is_rejected_before_result_reuse(
    positive_bundle: QCQQQOptionsCollectionEvidenceAdmissionBundle,
    tmp_path: Path,
) -> None:
    with pytest.raises(
        QCQQQOptionsCollectionEvidenceAdmissionError,
        match="OWNER_AUTHORIZATION_ALREADY_CONSUMED",
    ):
        _build(tmp_path, prior=(positive_bundle.authorization_consumption,))


def test_new_sealed_records_require_canonical_bytes_and_valid_hash(
    positive_bundle: QCQQQOptionsCollectionEvidenceAdmissionBundle,
) -> None:
    receipt = positive_bundle.evidence_admission
    assert type(receipt).from_json_bytes(receipt.canonical_bytes) == receipt
    noncanonical = json.dumps(receipt.model_dump(mode="json"), sort_keys=False).encode()
    with pytest.raises(
        QCQQQOptionsCollectionEvidenceAdmissionError,
        match="COLLECTION_ADMISSION_RECORD_INVALID",
    ):
        type(receipt).from_json_bytes(noncanonical)
    mutated = copy.deepcopy(receipt.model_dump(mode="json"))
    mutated["decision"] = "EVIDENCE_NOT_ADMITTED_POLICY_BLOCKED"
    with pytest.raises(
        QCQQQOptionsCollectionEvidenceAdmissionError,
        match="COLLECTION_ADMISSION_RECORD_INVALID",
    ):
        type(receipt).from_json_bytes(
            (json.dumps(mutated, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
        )
