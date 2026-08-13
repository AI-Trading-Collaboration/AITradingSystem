from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.qqq_options_research import (
    primary_window_derived_aggregate_collection_evidence_admission as admission_v1,
)
from ai_trading_system.qqq_options_research.refresh_authorization_admission import (
    QCQQQOptionsRefreshAuthorizationAdmissionError,
    RefreshAuthorizationLifecycleState,
    RefreshAuthorizationRunAttemptConsumptionReceipt,
    RefreshOwnerAuthorizationAdmissionReceipt,
    admit_qc_qqq_options_refresh_owner_authorization,
    build_qc_qqq_options_refresh_authorization_not_provided_state,
    build_qc_qqq_options_refresh_external_action_ledger,
    build_qc_qqq_options_refresh_run_attempt_consumption,
    load_qc_qqq_options_refresh_authorization_admission_policy,
)

ROOT = Path(__file__).resolve().parents[1]
AUTH_AT = datetime(2026, 8, 13, 4, 0, tzinfo=UTC)
EXPIRY = datetime(2026, 8, 18, 0, 0, tzinfo=UTC)
BACKTEST_ID = "trading2517backtest001"


def _owner_token(
    *,
    token: str | None = None,
    overrides: dict[str, str] | None = None,
) -> bytes:
    loaded = load_qc_qqq_options_refresh_authorization_admission_policy(
        project_root=ROOT
    )
    policy = loaded.policy
    refresh_load = loaded.refresh_package.policy_load
    upstream = refresh_load.policy.upstream_authority
    fields = (
        ("ordinary_pushed_main_sha", policy.predecessor_ordinary_pushed_main_sha),
        ("refresh_policy_file_sha256", policy.refresh_policy_file_sha256),
        ("refresh_policy_canonical_sha256", policy.refresh_policy_canonical_sha256),
        (
            "refresh_package_manifest_file_sha256",
            policy.refresh_package_manifest_file_sha256,
        ),
        (
            "refresh_package_manifest_content_sha256",
            policy.refresh_package_manifest_content_sha256,
        ),
        ("proposal_content_sha256", policy.proposal_content_sha256),
        ("run_scope_content_sha256", policy.run_scope_content_sha256),
        ("project_code_lf_sha256", policy.project_code_lf_sha256),
        (
            "proposal_policy_file_sha256",
            upstream.proposal_policy_file_sha256,
        ),
        (
            "proposal_policy_canonical_sha256",
            upstream.proposal_policy_canonical_sha256,
        ),
        ("collector_policy_file_sha256", policy.collector_policy_file_sha256),
        (
            "collector_policy_canonical_sha256",
            policy.collector_policy_canonical_sha256,
        ),
        ("transport_map_sha256", policy.transport_map_sha256),
        (
            "admission_policy_file_sha256",
            policy.legacy_admission_policy_file_sha256,
        ),
        (
            "admission_policy_canonical_sha256",
            policy.legacy_admission_policy_canonical_sha256,
        ),
        ("target_project_id", str(policy.target_project_id)),
        (
            "requested_range",
            f"{policy.requested_start.isoformat()}..{policy.requested_end.isoformat()}",
        ),
        ("expected_session_count", str(policy.expected_session_count)),
        ("maximum_project_mutations", str(policy.maximum_project_mutations)),
        ("maximum_cloud_backtests", str(policy.maximum_cloud_backtests)),
        ("maximum_orders", "0"),
        ("maximum_fills", "0"),
        ("collector", policy.collector_id),
        ("independent_reviewer", policy.independent_reviewer_id),
        ("authorization_expires_at_utc", EXPIRY.strftime("%Y-%m-%dT%H:%M:%SZ")),
        ("authorization_single_use", "true"),
        ("authorization_invalidates_after_evidence_collection", "true"),
    )
    changes = overrides or {}
    lines = [token or policy.expected_owner_decision_token]
    lines.extend(f"{key}:{changes.get(key, value)}" for key, value in fields)
    return ("\n".join(lines) + "\n").encode()


def _admit() -> Any:
    return admit_qc_qqq_options_refresh_owner_authorization(
        admission_id="trading-2517-admission",
        admitted_at_utc=AUTH_AT,
        owner_decision_bytes=_owner_token(),
        owner_decision_source="PROJECT_OWNER_CURRENT_CODEX_DIALOG",
        project_root=ROOT,
    )


def _actions(
    *,
    run_status: admission_v1.CollectionActionStatus = (
        admission_v1.CollectionActionStatus.COMPLETED
    ),
    include_download: bool = False,
) -> tuple[admission_v1.CollectionExternalAction, ...]:
    loaded = load_qc_qqq_options_refresh_authorization_admission_policy(
        project_root=ROOT
    )
    code_sha = loaded.policy.project_code_lf_sha256
    actions = [
        admission_v1.CollectionExternalAction(
            action_id="trading-2517-action-login",
            ordinal=1,
            action_type=admission_v1.CollectionActionType.QUANTCONNECT_LOGIN,
            occurred_at_utc=AUTH_AT + timedelta(minutes=5),
            status=admission_v1.CollectionActionStatus.COMPLETED,
            target_project_id=34_808_569,
        ),
        admission_v1.CollectionExternalAction(
            action_id="trading-2517-action-project",
            ordinal=2,
            action_type=(
                admission_v1.CollectionActionType.MODIFY_EXISTING_DEDICATED_PROJECT_ONCE
            ),
            occurred_at_utc=AUTH_AT + timedelta(minutes=10),
            status=admission_v1.CollectionActionStatus.COMPLETED,
            target_project_id=34_808_569,
            project_code_lf_sha256=code_sha,
        ),
        admission_v1.CollectionExternalAction(
            action_id="trading-2517-action-run",
            ordinal=3,
            action_type=(
                admission_v1.CollectionActionType.RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST
            ),
            occurred_at_utc=AUTH_AT + timedelta(minutes=15),
            status=run_status,
            target_project_id=34_808_569,
            project_code_lf_sha256=code_sha,
            backtest_id=BACKTEST_ID,
            failure_reason_code=(
                "QC_PLATFORM_RUN_FAILED"
                if run_status is admission_v1.CollectionActionStatus.FAILED
                else None
            ),
        ),
    ]
    if include_download:
        result_sha = hashlib.sha256(b"synthetic-export-safe-result").hexdigest()
        actions.append(
            admission_v1.CollectionExternalAction(
                action_id="trading-2517-action-download",
                ordinal=4,
                action_type=(
                    admission_v1.CollectionActionType.EXPORT_SAFE_MANUAL_DOWNLOAD_RESULTS_COLLECTION
                ),
                occurred_at_utc=AUTH_AT + timedelta(minutes=20),
                status=admission_v1.CollectionActionStatus.COMPLETED,
                target_project_id=34_808_569,
                project_code_lf_sha256=code_sha,
                backtest_id=BACKTEST_ID,
                result_file_sha256=result_sha,
            )
        )
    return tuple(actions)


def test_policy_binds_2516_refresh_and_2512_2514_collection_authority() -> None:
    loaded = load_qc_qqq_options_refresh_authorization_admission_policy(
        project_root=ROOT
    )
    policy = loaded.policy
    assert (
        policy.predecessor_ordinary_pushed_main_sha
        == "0d1d614e01a040661050329cef48ac7ecab06bda"
    )
    assert policy.proposal_content_sha256 == (
        loaded.legacy_admission.proposal_package.proposal.content_sha256
    )
    assert policy.expected_session_count == 1202
    assert policy.requested_start.isoformat() == "2021-02-22"
    assert policy.safety.authorization_status == "OWNER_REFRESH_TOKEN_NOT_PROVIDED"
    assert policy.safety.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert loaded.refresh_package.manifest.authorization_status == (
        "OWNER_AUTHORIZATION_NOT_PROVIDED"
    )


def test_missing_token_state_is_canonical_cash_preservation() -> None:
    state = build_qc_qqq_options_refresh_authorization_not_provided_state(
        state_id="trading-2517-missing-token",
        observed_at_utc=AUTH_AT,
        project_root=ROOT,
    )
    assert state.authorization_status == "OWNER_REFRESH_TOKEN_NOT_PROVIDED"
    assert state.authorization_consumed is False
    assert state.external_action_performed is False
    assert state.orders == state.fills == 0
    assert state.selection_authorized is False
    assert state.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert RefreshAuthorizationLifecycleState.from_json_bytes(state.canonical_bytes) == state


def test_exact_v2_owner_token_admits_unused_authorization() -> None:
    admitted = _admit()
    candidate = admitted.refresh_candidate
    receipt = admitted.refresh_admission_receipt
    authorization = admitted.collector_authorization
    assert candidate.authorization_consumed is False
    assert candidate.external_action_performed is False
    assert receipt.decision == "OWNER_REFRESH_AUTHORIZATION_ADMITTED_UNUSED"
    assert receipt.owner_decision_source == "PROJECT_OWNER_CURRENT_CODEX_DIALOG"
    assert receipt.collector_authorization_content_sha256 == authorization.content_sha256
    assert authorization.owner_decision_token.endswith("collection_v2")
    assert authorization.maximum_cloud_backtests == 1
    assert authorization.maximum_orders == authorization.maximum_fills == 0
    assert RefreshOwnerAuthorizationAdmissionReceipt.from_json_bytes(
        receipt.canonical_bytes
    ) == receipt


def test_local_dry_run_or_non_owner_source_is_not_an_authorization_fact() -> None:
    with pytest.raises(
        QCQQQOptionsRefreshAuthorizationAdmissionError,
        match="OWNER_REFRESH_AUTHORIZATION_SOURCE_REJECTED",
    ):
        admit_qc_qqq_options_refresh_owner_authorization(
            admission_id="trading-2517-local-dry-run",
            admitted_at_utc=AUTH_AT,
            owner_decision_bytes=_owner_token(),
            owner_decision_source="LOCAL_DRY_RUN",
            project_root=ROOT,
        )


def test_old_v1_token_and_binding_drift_fail_closed() -> None:
    old = (
        "owner_decision:TRADING-2513:2026-08-12:"
        "authorize_single_zero_order_primary_window_derived_aggregate_collection_v1"
    )
    cases = (
        _owner_token(token=old),
        _owner_token(overrides={"ordinary_pushed_main_sha": "f" * 40}),
        _owner_token(overrides={"expected_session_count": "1201"}),
        _owner_token(overrides={"maximum_orders": "1"}),
    )
    for raw in cases:
        with pytest.raises(
            QCQQQOptionsRefreshAuthorizationAdmissionError,
            match="OWNER_REFRESH_AUTHORIZATION_REJECTED",
        ):
            admit_qc_qqq_options_refresh_owner_authorization(
                admission_id="trading-2517-rejected",
                admitted_at_utc=AUTH_AT,
                owner_decision_bytes=raw,
                owner_decision_source="PROJECT_OWNER_CURRENT_CODEX_DIALOG",
                project_root=ROOT,
            )


def test_expired_owner_token_fails_before_any_action() -> None:
    with pytest.raises(
        QCQQQOptionsRefreshAuthorizationAdmissionError,
        match="OWNER_REFRESH_AUTHORIZATION_REJECTED",
    ):
        admit_qc_qqq_options_refresh_owner_authorization(
            admission_id="trading-2517-expired",
            admitted_at_utc=EXPIRY + timedelta(seconds=1),
            owner_decision_bytes=_owner_token(),
            owner_decision_source="PROJECT_OWNER_CURRENT_CODEX_DIALOG",
            project_root=ROOT,
        )


def test_action_input_permutation_replays_to_one_ledger_identity() -> None:
    admitted = _admit()
    actions = _actions(include_download=True)
    forward = build_qc_qqq_options_refresh_external_action_ledger(
        ledger_id="trading-2517-ledger",
        sealed_at_utc=AUTH_AT + timedelta(minutes=25),
        admitted_authorization=admitted,
        actions=actions,
    )
    reverse = build_qc_qqq_options_refresh_external_action_ledger(
        ledger_id="trading-2517-ledger",
        sealed_at_utc=AUTH_AT + timedelta(minutes=25),
        admitted_authorization=admitted,
        actions=tuple(reversed(actions)),
    )
    assert forward.canonical_bytes == reverse.canonical_bytes
    assert forward.lifecycle_status == "COMPLETE"
    assert forward.scope_status == "PASS"


def test_login_without_cloud_run_does_not_consume_authorization() -> None:
    admitted = _admit()
    ledger = build_qc_qqq_options_refresh_external_action_ledger(
        ledger_id="trading-2517-login-only",
        sealed_at_utc=AUTH_AT + timedelta(minutes=6),
        admitted_authorization=admitted,
        actions=_actions()[:1],
    )
    with pytest.raises(
        QCQQQOptionsRefreshAuthorizationAdmissionError,
        match="FIRST_CLOUD_RUN_NOT_ATTEMPTED",
    ):
        build_qc_qqq_options_refresh_run_attempt_consumption(
            consumption_id="trading-2517-no-run-consumption",
            recorded_at_utc=ledger.sealed_at_utc,
            admitted_authorization=admitted,
            external_action_ledger=ledger,
        )


@pytest.mark.parametrize(
    "run_status",
    (
        admission_v1.CollectionActionStatus.COMPLETED,
        admission_v1.CollectionActionStatus.FAILED,
    ),
)
def test_first_cloud_run_attempt_consumes_even_when_platform_run_fails(
    run_status: admission_v1.CollectionActionStatus,
) -> None:
    admitted = _admit()
    ledger = build_qc_qqq_options_refresh_external_action_ledger(
        ledger_id=f"trading-2517-{run_status.value.lower()}-ledger",
        sealed_at_utc=AUTH_AT + timedelta(minutes=16),
        admitted_authorization=admitted,
        actions=_actions(run_status=run_status),
    )
    receipt = build_qc_qqq_options_refresh_run_attempt_consumption(
        consumption_id=f"trading-2517-{run_status.value.lower()}-consumption",
        recorded_at_utc=ledger.sealed_at_utc,
        admitted_authorization=admitted,
        external_action_ledger=ledger,
    )
    assert receipt.authorization_consumed is True
    assert receipt.authorization_invalidated_for_further_cloud_runs is True
    assert receipt.evidence_collection_completed is False
    assert receipt.run_status is run_status
    assert receipt.orders == receipt.fills == 0
    assert RefreshAuthorizationRunAttemptConsumptionReceipt.from_json_bytes(
        receipt.canonical_bytes
    ) == receipt


def test_prior_run_attempt_receipt_blocks_second_cloud_run() -> None:
    admitted = _admit()
    ledger = build_qc_qqq_options_refresh_external_action_ledger(
        ledger_id="trading-2517-first-run-ledger",
        sealed_at_utc=AUTH_AT + timedelta(minutes=16),
        admitted_authorization=admitted,
        actions=_actions(),
    )
    first = build_qc_qqq_options_refresh_run_attempt_consumption(
        consumption_id="trading-2517-first-run-consumption",
        recorded_at_utc=ledger.sealed_at_utc,
        admitted_authorization=admitted,
        external_action_ledger=ledger,
    )
    with pytest.raises(
        QCQQQOptionsRefreshAuthorizationAdmissionError,
        match="OWNER_REFRESH_AUTHORIZATION_ALREADY_CONSUMED",
    ):
        build_qc_qqq_options_refresh_run_attempt_consumption(
            consumption_id="trading-2517-second-run-consumption",
            recorded_at_utc=ledger.sealed_at_utc,
            admitted_authorization=admitted,
            external_action_ledger=ledger,
            prior_consumption_receipts=(first,),
        )


def test_run_receipt_rejects_ledger_authority_or_recording_time_tamper() -> None:
    admitted = _admit()
    ledger = build_qc_qqq_options_refresh_external_action_ledger(
        ledger_id="trading-2517-authority-ledger",
        sealed_at_utc=AUTH_AT + timedelta(minutes=16),
        admitted_authorization=admitted,
        actions=_actions(),
    )
    with pytest.raises(
        QCQQQOptionsRefreshAuthorizationAdmissionError,
        match="REFRESH_RUN_ATTEMPT_SCOPE_REJECTED",
    ):
        build_qc_qqq_options_refresh_run_attempt_consumption(
            consumption_id="trading-2517-too-early",
            recorded_at_utc=ledger.sealed_at_utc - timedelta(seconds=1),
            admitted_authorization=admitted,
            external_action_ledger=ledger,
        )


def test_canonical_records_reject_noncanonical_or_semantic_tamper() -> None:
    receipt = _admit().refresh_admission_receipt
    noncanonical = json.dumps(receipt.model_dump(mode="json"), sort_keys=False).encode()
    with pytest.raises(
        QCQQQOptionsRefreshAuthorizationAdmissionError,
        match="REFRESH_AUTHORIZATION_RECORD_INVALID",
    ):
        RefreshOwnerAuthorizationAdmissionReceipt.from_json_bytes(noncanonical)
    mutated = copy.deepcopy(receipt.model_dump(mode="json"))
    mutated["owner_decision_source"] = "LOCAL_DRY_RUN"
    raw = (json.dumps(mutated, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
    with pytest.raises(
        QCQQQOptionsRefreshAuthorizationAdmissionError,
        match="REFRESH_AUTHORIZATION_RECORD_INVALID",
    ):
        RefreshOwnerAuthorizationAdmissionReceipt.from_json_bytes(raw)
