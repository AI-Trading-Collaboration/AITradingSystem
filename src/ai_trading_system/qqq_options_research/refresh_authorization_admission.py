from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    primary_window_derived_aggregate_collection_evidence_admission as admission_v1,
)
from ai_trading_system.qqq_options_research import (
    primary_window_evidence_lane_authorization_refresh as refresh_v1,
)
from ai_trading_system.qqq_options_research import (
    primary_window_export_safe_derived_aggregate_collector as collector_v1,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_REFRESH_AUTHORIZATION_ADMISSION_POLICY_PATH = Path(
    "config/research/qc_qqq_options_refresh_authorization_admission_v1.yaml"
)

_UNSEALED_SHA256 = "0" * 64
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_ALLOWED_ACTIONS = (
    "QUANTCONNECT_LOGIN",
    "MODIFY_EXISTING_DEDICATED_PROJECT_ONCE",
    "RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST",
    "EXPORT_SAFE_MANUAL_DOWNLOAD_RESULTS_COLLECTION",
)
_PROHIBITED_ACTIONS = (
    "API",
    "BROKER",
    "CLI",
    "HTTP",
    "INVESTMENT_INTERPRETATION",
    "LIVE",
    "OBJECT_STORE",
    "PAPER",
    "PRODUCTION",
    "PURCHASE_OR_SUBSCRIPTION",
    "RAW_OPTIONS_DATA_DOWNLOAD",
    "RAW_OPTION_ROW_LOGGING_OR_EXPORT",
    "SECOND_CLOUD_BACKTEST",
)


class QCQQQOptionsRefreshAuthorizationAdmissionError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        self.detail = detail
        super().__init__(f"{reason_code}: {detail}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _duplicate_key_rejecting_json(raw: bytes) -> object:
    def hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=hook)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("record is not UTF-8 JSON") from exc


def _sha256(value: str, field: str) -> str:
    if not _SHA256.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase SHA-256")
    return value


def _git_sha(value: str, field: str) -> str:
    if not _GIT_SHA.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase full Git SHA")
    return value


def _identifier(value: str, field: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{field} must be a stable identifier")
    return value


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    if value.utcoffset().total_seconds() != 0:
        raise ValueError(f"{field} must use UTC")
    return value.astimezone(UTC)


def _relative_path(value: str, field: str) -> str:
    path = Path(value)
    if path.is_absolute() or path.drive or ".." in path.parts or path.as_posix() != value:
        raise ValueError(f"{field} must be a normalized repository-relative path")
    return value


def _bound_file(path: Path, *, root: Path, field: str) -> Path:
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes project root") from exc
    if not resolved.is_file() or candidate.is_symlink() or resolved.is_symlink():
        raise ValueError(f"{field} must be a regular non-symlink file")
    return resolved


class RefreshAuthorizationAdmissionSafety(_PolicyModel):
    owner_token_observed: Literal[False]
    authorization_status: Literal["OWNER_REFRESH_TOKEN_NOT_PROVIDED"]
    evidence_status: Literal["EVIDENCE_NOT_ADMITTED_POLICY_BLOCKED"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    external_action_performed: Literal[False]
    investment_interpretation_generated: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QCQQQOptionsRefreshAuthorizationAdmissionPolicy(_PolicyModel):
    schema_version: Literal["qc_qqq_options_refresh_authorization_admission_policy.v1"]
    policy_id: str
    policy_version: Literal["1.0.0"]
    policy_status: Literal["ENGINEERING_BASELINE_OWNER_REFRESH_TOKEN_NOT_PROVIDED"]
    task_id: Literal[
        "TRADING-2517_QC_QQQ_OPTIONS_REFRESH_AUTHORIZATION_ADMISSION_AND_BOUNDED_COLLECTION_LIFECYCLE_V1"
    ]
    predecessor_ordinary_pushed_main_sha: str
    refresh_policy_path: str
    refresh_policy_file_sha256: str
    refresh_policy_canonical_sha256: str
    refresh_package_root: str
    refresh_package_manifest_file_sha256: str
    refresh_package_manifest_content_sha256: str
    legacy_admission_policy_path: str
    legacy_admission_policy_file_sha256: str
    legacy_admission_policy_canonical_sha256: str
    proposal_content_sha256: str
    run_scope_content_sha256: str
    project_code_lf_sha256: str
    collector_policy_file_sha256: str
    collector_policy_canonical_sha256: str
    transport_map_sha256: str
    expected_owner_decision_token: str
    target_project_id: Literal[34808569]
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    primary_research_role: Literal["PRIMARY"]
    exchange_calendar: Literal["XNYS"]
    expected_session_count: Literal[1202]
    maximum_project_mutations: Literal[1]
    maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    collector_id: str
    independent_reviewer_id: str
    owner_decision_source: Literal["PROJECT_OWNER_CURRENT_CODEX_DIALOG"]
    result_carrier: Literal["MANUAL_DOWNLOAD_RESULTS_JSON"]
    allowed_actions: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    safety: RefreshAuthorizationAdmissionSafety

    @field_validator("predecessor_ordinary_pushed_main_sha")
    @classmethod
    def _main_sha(cls, value: str) -> str:
        return _git_sha(value, "predecessor_ordinary_pushed_main_sha")

    @field_validator(
        "refresh_policy_file_sha256",
        "refresh_policy_canonical_sha256",
        "refresh_package_manifest_file_sha256",
        "refresh_package_manifest_content_sha256",
        "legacy_admission_policy_file_sha256",
        "legacy_admission_policy_canonical_sha256",
        "proposal_content_sha256",
        "run_scope_content_sha256",
        "project_code_lf_sha256",
        "collector_policy_file_sha256",
        "collector_policy_canonical_sha256",
        "transport_map_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator(
        "refresh_policy_path", "refresh_package_root", "legacy_admission_policy_path"
    )
    @classmethod
    def _paths(cls, value: str, info: ValidationInfo) -> str:
        return _relative_path(value, str(info.field_name))

    @field_validator("policy_id", "collector_id", "independent_reviewer_id")
    @classmethod
    def _ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _scope(self) -> Self:
        expected_token = (
            "owner_decision:TRADING-2516:2026-08-13:"
            "authorize_single_zero_order_primary_window_derived_aggregate_collection_v2"
        )
        if self.expected_owner_decision_token != expected_token:
            raise ValueError("successor Owner decision token drifted")
        if (self.requested_start, self.evaluated_start) != (
            date(2021, 2, 22),
            date(2021, 2, 22),
        ) or (self.requested_end, self.evaluated_end) != (
            date(2025, 12, 2),
            date(2025, 12, 2),
        ):
            raise ValueError("PRIMARY collection range drifted")
        if self.allowed_actions != _ALLOWED_ACTIONS:
            raise ValueError("allowed action inventory/order drifted")
        if self.prohibited_actions != _PROHIBITED_ACTIONS:
            raise ValueError("prohibited action inventory/order drifted")
        return self

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


@dataclass(frozen=True)
class QCQQQOptionsRefreshAuthorizationAdmissionPolicyLoadResult:
    policy: QCQQQOptionsRefreshAuthorizationAdmissionPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str
    refresh_package: refresh_v1.BuiltQCQQQOptionsEvidenceLaneAuthorizationRefreshPackage
    legacy_admission: admission_v1.QCQQQOptionsCollectionEvidenceAdmissionPolicyLoadResult


class _SealedModel(_StrictModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_hash(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self.semantic_payload())).hexdigest()

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @model_validator(mode="after")
    def _verify_content_hash(self) -> Self:
        if self.content_sha256 != _UNSEALED_SHA256:
            expected = self.compute_content_sha256()
            if self.content_sha256 != expected:
                raise ValueError("content_sha256 does not match semantic payload")
        return self

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        draft = cls(content_sha256=_UNSEALED_SHA256, **payload)
        return cls(content_sha256=draft.compute_content_sha256(), **payload)

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            payload = _duplicate_key_rejecting_json(raw)
            if not isinstance(payload, dict):
                raise TypeError("record JSON root must be an object")
            value = cls.model_validate_json(raw)
            if raw != value.canonical_bytes:
                raise ValueError("record is not canonical JSON bytes")
            return value
        except (TypeError, ValueError) as exc:
            raise QCQQQOptionsRefreshAuthorizationAdmissionError(
                "REFRESH_AUTHORIZATION_RECORD_INVALID", str(exc)
            ) from exc


class RefreshAuthorizationLifecycleState(_SealedModel):
    schema_version: Literal["qc_qqq_options_refresh_authorization_lifecycle_state.v1"]
    state_id: str
    observed_at_utc: datetime
    admission_policy_file_sha256: str
    admission_policy_canonical_sha256: str
    authorization_status: Literal["OWNER_REFRESH_TOKEN_NOT_PROVIDED"]
    evidence_status: Literal["EVIDENCE_NOT_ADMITTED_POLICY_BLOCKED"]
    authorization_consumed: Literal[False]
    external_action_performed: Literal[False]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("state_id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _identifier(value, "state_id")

    @field_validator("observed_at_utc")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value, "observed_at_utc")

    @field_validator("admission_policy_file_sha256", "admission_policy_canonical_sha256")
    @classmethod
    def _policy_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class RefreshOwnerAuthorizationAdmissionReceipt(_SealedModel):
    schema_version: Literal["qc_qqq_options_refresh_owner_authorization_admission_receipt.v1"]
    admission_id: str
    admitted_at_utc: datetime
    owner_decision_source: Literal["PROJECT_OWNER_CURRENT_CODEX_DIALOG"]
    admission_policy_file_sha256: str
    admission_policy_canonical_sha256: str
    refresh_candidate_content_sha256: str
    legacy_admission_receipt_content_sha256: str
    collector_authorization_content_sha256: str
    owner_decision_token: str
    owner_decision_file_sha256: str
    owner_decision_content_sha256: str
    ordinary_pushed_main_sha: str
    authorized_at_utc: datetime
    expires_at_utc: datetime
    authorization_single_use: Literal[True]
    authorization_invalidates_after_evidence_collection: Literal[True]
    authorization_consumed: Literal[False]
    decision: Literal["OWNER_REFRESH_AUTHORIZATION_ADMITTED_UNUSED"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    external_action_performed: Literal[False]
    investment_interpretation_generated: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("admission_id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _identifier(value, "admission_id")

    @field_validator("admitted_at_utc", "authorized_at_utc", "expires_at_utc")
    @classmethod
    def _times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc(value, str(info.field_name))

    @field_validator("ordinary_pushed_main_sha")
    @classmethod
    def _main_sha(cls, value: str) -> str:
        return _git_sha(value, "ordinary_pushed_main_sha")

    @field_validator(
        "admission_policy_file_sha256",
        "admission_policy_canonical_sha256",
        "refresh_candidate_content_sha256",
        "legacy_admission_receipt_content_sha256",
        "collector_authorization_content_sha256",
        "owner_decision_file_sha256",
        "owner_decision_content_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class RefreshAuthorizationRunAttemptConsumptionReceipt(_SealedModel):
    schema_version: Literal[
        "qc_qqq_options_refresh_authorization_run_attempt_consumption_receipt.v1"
    ]
    consumption_id: str
    recorded_at_utc: datetime
    first_cloud_run_attempted_at_utc: datetime
    refresh_authorization_admission_content_sha256: str
    legacy_authorization_admission_content_sha256: str
    collector_authorization_content_sha256: str
    external_action_ledger_content_sha256: str
    owner_decision_token: str
    owner_decision_file_sha256: str
    owner_decision_content_sha256: str
    target_project_id: Literal[34808569]
    project_code_lf_sha256: str
    backtest_id: str
    run_action_id: str
    run_status: admission_v1.CollectionActionStatus
    failure_reason_code: str | None
    authorization_single_use: Literal[True]
    authorization_consumed: Literal[True]
    authorization_invalidated_for_further_cloud_runs: Literal[True]
    evidence_collection_completed: Literal[False]
    decision: Literal["AUTHORIZATION_CONSUMED_AT_FIRST_CLOUD_RUN_ATTEMPT"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("consumption_id", "backtest_id", "run_action_id")
    @classmethod
    def _ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("failure_reason_code")
    @classmethod
    def _reason(cls, value: str | None) -> str | None:
        return None if value is None else _identifier(value, "failure_reason_code")

    @field_validator("recorded_at_utc", "first_cloud_run_attempted_at_utc")
    @classmethod
    def _times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc(value, str(info.field_name))

    @field_validator(
        "refresh_authorization_admission_content_sha256",
        "legacy_authorization_admission_content_sha256",
        "collector_authorization_content_sha256",
        "external_action_ledger_content_sha256",
        "owner_decision_file_sha256",
        "owner_decision_content_sha256",
        "project_code_lf_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _status_fields(self) -> Self:
        if self.run_status is admission_v1.CollectionActionStatus.FAILED:
            if self.failure_reason_code is None:
                raise ValueError("failed run attempt requires failure_reason_code")
        elif self.failure_reason_code is not None:
            raise ValueError("completed run attempt cannot carry failure_reason_code")
        if self.recorded_at_utc < self.first_cloud_run_attempted_at_utc:
            raise ValueError("consumption record cannot predate first Cloud run attempt")
        return self


@dataclass(frozen=True)
class AdmittedQCQQQOptionsRefreshAuthorization:
    policy_load: QCQQQOptionsRefreshAuthorizationAdmissionPolicyLoadResult
    refresh_candidate: refresh_v1.QCQQQOptionsAuthorizationRefreshOwnerDecisionCandidate
    collector_authorization: collector_v1.QCQQQOptionsDerivedAggregateCollectorAuthorization
    legacy_admission_receipt: admission_v1.OwnerAuthorizationAdmissionReceipt
    refresh_admission_receipt: RefreshOwnerAuthorizationAdmissionReceipt


@dataclass(frozen=True)
class QCQQQOptionsRefreshCollectionEvidenceAdmissionBundle:
    refresh_policy_load: QCQQQOptionsRefreshAuthorizationAdmissionPolicyLoadResult
    admitted_authorization: AdmittedQCQQQOptionsRefreshAuthorization
    run_attempt_consumption: RefreshAuthorizationRunAttemptConsumptionReceipt
    legacy_evidence_bundle: admission_v1.QCQQQOptionsCollectionEvidenceAdmissionBundle


def load_qc_qqq_options_refresh_authorization_admission_policy(
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_REFRESH_AUTHORIZATION_ADMISSION_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsRefreshAuthorizationAdmissionPolicyLoadResult:
    root = project_root.resolve()
    try:
        path = _bound_file(policy_path, root=root, field="2517 admission policy")
        raw = path.read_bytes()
        payload = safe_load_yaml_path(path)
        if not isinstance(payload, dict):
            raise TypeError("2517 admission policy root must be a mapping")
        policy = QCQQQOptionsRefreshAuthorizationAdmissionPolicy.model_validate(payload)
        refresh_policy_path = _bound_file(
            Path(policy.refresh_policy_path), root=root, field="2516 refresh policy"
        )
        refresh_package_root = (root / policy.refresh_package_root).resolve()
        default_refresh_root = (
            refresh_v1.DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE_LANE_AUTHORIZATION_REFRESH_PACKAGE_ROOT
        )
        if refresh_package_root != (root / default_refresh_root).resolve():
            raise ValueError("2516 refresh package root drifted")
        refresh_package = (
            refresh_v1.load_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
                project_root=root,
            )
        )
        refresh_manifest_path = _bound_file(
            refresh_package_root / "package_manifest.json",
            root=root,
            field="2516 refresh package manifest",
        )
        refresh_load = refresh_package.policy_load
        if (
            refresh_load.policy_path != refresh_policy_path
            or refresh_load.policy_file_sha256 != policy.refresh_policy_file_sha256
            or refresh_load.policy_canonical_sha256
            != policy.refresh_policy_canonical_sha256
            or hashlib.sha256(refresh_manifest_path.read_bytes()).hexdigest()
            != policy.refresh_package_manifest_file_sha256
            or refresh_package.manifest.content_sha256
            != policy.refresh_package_manifest_content_sha256
            or refresh_load.policy.registration_base_repository_code_sha
            != "65b2bc1c88bf98132b7f6d58359ae3f18cea85f9"
            or refresh_load.policy.decision_token != policy.expected_owner_decision_token
        ):
            raise ValueError("2516 refresh authority drifted")
        legacy_policy_path = _bound_file(
            Path(policy.legacy_admission_policy_path),
            root=root,
            field="2514 admission policy",
        )
        legacy = admission_v1.load_qc_qqq_options_collection_evidence_admission_policy(
            policy_path=legacy_policy_path,
            project_root=root,
        )
        proposal = legacy.proposal_package.proposal
        scope = proposal.run_scope
        if (
            legacy.policy_file_sha256 != policy.legacy_admission_policy_file_sha256
            or legacy.policy_canonical_sha256
            != policy.legacy_admission_policy_canonical_sha256
            or proposal.content_sha256 != policy.proposal_content_sha256
            or scope.content_sha256 != policy.run_scope_content_sha256
            or proposal.project_code_lf_sha256 != policy.project_code_lf_sha256
            or proposal.collector_policy_file_sha256
            != policy.collector_policy_file_sha256
            or proposal.collector_policy_canonical_sha256
            != policy.collector_policy_canonical_sha256
            or proposal.transport_map_sha256 != policy.transport_map_sha256
            or scope.target_project_id != policy.target_project_id
            or (scope.requested_start, scope.requested_end)
            != (policy.requested_start, policy.requested_end)
            or (scope.evaluated_start, scope.evaluated_end)
            != (policy.evaluated_start, policy.evaluated_end)
            or len(scope.session_ids) != policy.expected_session_count
            or proposal.allowed_actions != policy.allowed_actions
            or proposal.prohibited_actions != policy.prohibited_actions
        ):
            raise ValueError("2512-2514 collection authority drifted")
    except QCQQQOptionsRefreshAuthorizationAdmissionError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsRefreshAuthorizationAdmissionError(
            "REFRESH_AUTHORIZATION_ADMISSION_POLICY_REJECTED", str(exc)
        ) from exc
    return QCQQQOptionsRefreshAuthorizationAdmissionPolicyLoadResult(
        policy=policy,
        policy_path=path,
        policy_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy_canonical_sha256=policy.canonical_sha256,
        refresh_package=refresh_package,
        legacy_admission=legacy,
    )


def build_qc_qqq_options_refresh_authorization_not_provided_state(
    *,
    state_id: str,
    observed_at_utc: datetime,
    project_root: Path = PROJECT_ROOT,
) -> RefreshAuthorizationLifecycleState:
    loaded = load_qc_qqq_options_refresh_authorization_admission_policy(
        project_root=project_root
    )
    return RefreshAuthorizationLifecycleState.seal(
        schema_version="qc_qqq_options_refresh_authorization_lifecycle_state.v1",
        state_id=state_id,
        observed_at_utc=_utc(observed_at_utc, "observed_at_utc"),
        admission_policy_file_sha256=loaded.policy_file_sha256,
        admission_policy_canonical_sha256=loaded.policy_canonical_sha256,
        authorization_status="OWNER_REFRESH_TOKEN_NOT_PROVIDED",
        evidence_status="EVIDENCE_NOT_ADMITTED_POLICY_BLOCKED",
        authorization_consumed=False,
        external_action_performed=False,
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        orders=0,
        fills=0,
        production_effect="none",
        broker_action="none",
    )


def admit_qc_qqq_options_refresh_owner_authorization(
    *,
    admission_id: str,
    admitted_at_utc: datetime,
    owner_decision_bytes: bytes,
    owner_decision_source: str,
    project_root: Path = PROJECT_ROOT,
) -> AdmittedQCQQQOptionsRefreshAuthorization:
    loaded = load_qc_qqq_options_refresh_authorization_admission_policy(
        project_root=project_root
    )
    policy = loaded.policy
    if owner_decision_source != policy.owner_decision_source:
        raise QCQQQOptionsRefreshAuthorizationAdmissionError(
            "OWNER_REFRESH_AUTHORIZATION_SOURCE_REJECTED",
            "Owner decision must originate from the Project Owner current Codex dialog",
        )
    admitted_at = _utc(admitted_at_utc, "admitted_at_utc")
    try:
        candidate = (
            refresh_v1.validate_qc_qqq_options_authorization_refresh_owner_decision_candidate(
                owner_decision_bytes=owner_decision_bytes,
                expected_ordinary_pushed_main_sha=policy.predecessor_ordinary_pushed_main_sha,
                reviewed_at_utc=admitted_at,
                project_root=project_root,
            )
        )
    except (TypeError, ValueError) as exc:
        raise QCQQQOptionsRefreshAuthorizationAdmissionError(
            "OWNER_REFRESH_AUTHORIZATION_REJECTED", str(exc)
        ) from exc
    proposal = loaded.legacy_admission.proposal_package.proposal
    authorization = collector_v1.QCQQQOptionsDerivedAggregateCollectorAuthorization.seal(
        schema_version="qc_qqq_options_derived_aggregate_collector_authorization.v1",
        owner_decision_token=candidate.owner_decision_token,
        authorized_at_utc=candidate.reviewed_at_utc,
        expires_at_utc=candidate.expires_at_utc,
        authorization_single_use=True,
        authorization_invalidates_after_evidence_collection=True,
        proposal_content_sha256=proposal.content_sha256,
        run_scope_content_sha256=proposal.run_scope.content_sha256,
        repository_code_sha=proposal.run_scope.repository_code_sha,
        target_project_id=policy.target_project_id,
        project_code_lf_sha256=policy.project_code_lf_sha256,
        collector_policy_file_sha256=policy.collector_policy_file_sha256,
        collector_policy_canonical_sha256=policy.collector_policy_canonical_sha256,
        maximum_project_mutations=1,
        maximum_cloud_backtests=1,
        maximum_orders=0,
        maximum_fills=0,
        allowed_actions=policy.allowed_actions,
        prohibited_actions=policy.prohibited_actions,
        collector_id=policy.collector_id,
        independent_reviewer_id=policy.independent_reviewer_id,
    )
    legacy_receipt = admission_v1.OwnerAuthorizationAdmissionReceipt.seal(
        schema_version="qc_qqq_options_owner_authorization_admission_receipt.v1",
        admission_id=admission_id,
        admitted_at_utc=admitted_at,
        admission_policy_file_sha256=loaded.policy_file_sha256,
        admission_policy_canonical_sha256=loaded.policy_canonical_sha256,
        owner_decision_token=candidate.owner_decision_token,
        owner_decision_file_sha256=candidate.owner_decision_file_sha256,
        owner_decision_content_sha256=candidate.owner_decision_content_sha256,
        proposal_content_sha256=authorization.proposal_content_sha256,
        run_scope_content_sha256=authorization.run_scope_content_sha256,
        collector_authorization_content_sha256=authorization.content_sha256,
        authorized_at_utc=authorization.authorized_at_utc,
        expires_at_utc=authorization.expires_at_utc,
        authorization_single_use=True,
        authorization_invalidates_after_evidence_collection=True,
        authorization_consumed=False,
        decision="OWNER_AUTHORIZATION_ADMITTED_UNUSED",
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        external_action_performed=False,
        investment_interpretation_generated=False,
        production_effect="none",
        broker_action="none",
    )
    refresh_receipt = RefreshOwnerAuthorizationAdmissionReceipt.seal(
        schema_version="qc_qqq_options_refresh_owner_authorization_admission_receipt.v1",
        admission_id=admission_id,
        admitted_at_utc=admitted_at,
        owner_decision_source="PROJECT_OWNER_CURRENT_CODEX_DIALOG",
        admission_policy_file_sha256=loaded.policy_file_sha256,
        admission_policy_canonical_sha256=loaded.policy_canonical_sha256,
        refresh_candidate_content_sha256=candidate.content_sha256,
        legacy_admission_receipt_content_sha256=legacy_receipt.content_sha256,
        collector_authorization_content_sha256=authorization.content_sha256,
        owner_decision_token=candidate.owner_decision_token,
        owner_decision_file_sha256=candidate.owner_decision_file_sha256,
        owner_decision_content_sha256=candidate.owner_decision_content_sha256,
        ordinary_pushed_main_sha=candidate.ordinary_pushed_main_sha,
        authorized_at_utc=authorization.authorized_at_utc,
        expires_at_utc=authorization.expires_at_utc,
        authorization_single_use=True,
        authorization_invalidates_after_evidence_collection=True,
        authorization_consumed=False,
        decision="OWNER_REFRESH_AUTHORIZATION_ADMITTED_UNUSED",
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        external_action_performed=False,
        investment_interpretation_generated=False,
        production_effect="none",
        broker_action="none",
    )
    return AdmittedQCQQQOptionsRefreshAuthorization(
        policy_load=loaded,
        refresh_candidate=candidate,
        collector_authorization=authorization,
        legacy_admission_receipt=legacy_receipt,
        refresh_admission_receipt=refresh_receipt,
    )


def build_qc_qqq_options_refresh_external_action_ledger(
    *,
    ledger_id: str,
    sealed_at_utc: datetime,
    admitted_authorization: AdmittedQCQQQOptionsRefreshAuthorization,
    actions: tuple[admission_v1.CollectionExternalAction, ...],
) -> admission_v1.CollectionExternalActionLedger:
    return admission_v1.build_qc_qqq_options_collection_external_action_ledger(
        ledger_id=ledger_id,
        sealed_at_utc=sealed_at_utc,
        authorization=admitted_authorization.collector_authorization,
        authorization_admission=admitted_authorization.legacy_admission_receipt,
        actions=actions,
    )


def build_qc_qqq_options_refresh_run_attempt_consumption(
    *,
    consumption_id: str,
    recorded_at_utc: datetime,
    admitted_authorization: AdmittedQCQQQOptionsRefreshAuthorization,
    external_action_ledger: admission_v1.CollectionExternalActionLedger,
    prior_consumption_receipts: tuple[
        RefreshAuthorizationRunAttemptConsumptionReceipt, ...
    ] = (),
) -> RefreshAuthorizationRunAttemptConsumptionReceipt:
    authorization = admitted_authorization.collector_authorization
    legacy_receipt = admitted_authorization.legacy_admission_receipt
    refresh_receipt = admitted_authorization.refresh_admission_receipt
    recorded_at = _utc(recorded_at_utc, "recorded_at_utc")
    if (
        external_action_ledger.authorization_admission_content_sha256
        != legacy_receipt.content_sha256
        or external_action_ledger.collector_authorization_content_sha256
        != authorization.content_sha256
        or external_action_ledger.target_project_id != authorization.target_project_id
    ):
        raise QCQQQOptionsRefreshAuthorizationAdmissionError(
            "REFRESH_RUN_ATTEMPT_AUTHORITY_MISMATCH",
            "action ledger differs from admitted refresh authorization",
        )
    if any(
        item.owner_decision_token == authorization.owner_decision_token
        or item.collector_authorization_content_sha256 == authorization.content_sha256
        or item.refresh_authorization_admission_content_sha256
        == refresh_receipt.content_sha256
        for item in prior_consumption_receipts
    ):
        raise QCQQQOptionsRefreshAuthorizationAdmissionError(
            "OWNER_REFRESH_AUTHORIZATION_ALREADY_CONSUMED",
            "single-use authorization cannot fund a second Cloud run attempt",
        )
    run_actions = tuple(
        action
        for action in external_action_ledger.actions
        if action.action_type
        is admission_v1.CollectionActionType.RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST
    )
    if len(run_actions) != 1 or external_action_ledger.attempted_cloud_backtests != 1:
        raise QCQQQOptionsRefreshAuthorizationAdmissionError(
            "FIRST_CLOUD_RUN_NOT_ATTEMPTED",
            "consumption requires exactly one recorded first Cloud run attempt",
        )
    run = run_actions[0]
    if (
        run.backtest_id is None
        or run.project_code_lf_sha256 != authorization.project_code_lf_sha256
        or run.target_project_id != authorization.target_project_id
        or run.occurred_at_utc < authorization.authorized_at_utc
        or run.occurred_at_utc > authorization.expires_at_utc
        or recorded_at < external_action_ledger.sealed_at_utc
    ):
        raise QCQQQOptionsRefreshAuthorizationAdmissionError(
            "REFRESH_RUN_ATTEMPT_SCOPE_REJECTED",
            "first Cloud run identity, chronology, or authorization window mismatched",
        )
    return RefreshAuthorizationRunAttemptConsumptionReceipt.seal(
        schema_version="qc_qqq_options_refresh_authorization_run_attempt_consumption_receipt.v1",
        consumption_id=consumption_id,
        recorded_at_utc=recorded_at,
        first_cloud_run_attempted_at_utc=run.occurred_at_utc,
        refresh_authorization_admission_content_sha256=refresh_receipt.content_sha256,
        legacy_authorization_admission_content_sha256=legacy_receipt.content_sha256,
        collector_authorization_content_sha256=authorization.content_sha256,
        external_action_ledger_content_sha256=external_action_ledger.content_sha256,
        owner_decision_token=authorization.owner_decision_token,
        owner_decision_file_sha256=refresh_receipt.owner_decision_file_sha256,
        owner_decision_content_sha256=refresh_receipt.owner_decision_content_sha256,
        target_project_id=authorization.target_project_id,
        project_code_lf_sha256=authorization.project_code_lf_sha256,
        backtest_id=run.backtest_id,
        run_action_id=run.action_id,
        run_status=run.status,
        failure_reason_code=run.failure_reason_code,
        authorization_single_use=True,
        authorization_consumed=True,
        authorization_invalidated_for_further_cloud_runs=True,
        evidence_collection_completed=False,
        decision="AUTHORIZATION_CONSUMED_AT_FIRST_CLOUD_RUN_ATTEMPT",
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        orders=0,
        fills=0,
        production_effect="none",
        broker_action="none",
    )


def build_qc_qqq_options_refresh_collection_evidence_admission(
    *,
    evidence_admission_id: str,
    authorization_consumption_id: str,
    run_attempt_consumption_id: str,
    action_ledger_id: str,
    admitted_at_utc: datetime,
    implementation_repository_code_sha: str,
    admitted_authorization: AdmittedQCQQQOptionsRefreshAuthorization,
    actions: tuple[admission_v1.CollectionExternalAction, ...],
    backtest_id: str,
    result_bytes: bytes,
    dq_report_path: str,
    dq_report_bytes: bytes,
    reviewed_project_code_lf_sha256: str,
    prior_run_attempt_consumption_receipts: tuple[
        RefreshAuthorizationRunAttemptConsumptionReceipt, ...
    ] = (),
    prior_evidence_consumption_receipts: tuple[
        admission_v1.OwnerAuthorizationConsumptionReceipt, ...
    ] = (),
    project_root: Path = PROJECT_ROOT,
    evidence_root: Path | None = None,
) -> QCQQQOptionsRefreshCollectionEvidenceAdmissionBundle:
    admitted_at = _utc(admitted_at_utc, "admitted_at_utc")
    canonical = load_qc_qqq_options_refresh_authorization_admission_policy(
        project_root=project_root
    )
    if admitted_authorization.policy_load != canonical:
        raise QCQQQOptionsRefreshAuthorizationAdmissionError(
            "REFRESH_AUTHORIZATION_ADMISSION_AUTHORITY_MISMATCH",
            "admitted authorization does not match canonical 2517 policy",
        )
    ledger = build_qc_qqq_options_refresh_external_action_ledger(
        ledger_id=action_ledger_id,
        sealed_at_utc=admitted_at,
        admitted_authorization=admitted_authorization,
        actions=actions,
    )
    run_consumption = build_qc_qqq_options_refresh_run_attempt_consumption(
        consumption_id=run_attempt_consumption_id,
        recorded_at_utc=admitted_at,
        admitted_authorization=admitted_authorization,
        external_action_ledger=ledger,
        prior_consumption_receipts=prior_run_attempt_consumption_receipts,
    )
    if ledger.lifecycle_status != "COMPLETE" or ledger.scope_status != "PASS":
        raise QCQQQOptionsRefreshAuthorizationAdmissionError(
            "REFRESH_EVIDENCE_LIFECYCLE_INCOMPLETE",
            f"external action lifecycle is {ledger.lifecycle_status}/{ledger.scope_status}",
        )
    try:
        evidence_builder = (
            admission_v1.build_qc_qqq_options_primary_window_collection_evidence_admission_from_admitted_authorization
        )
        legacy_bundle = evidence_builder(
            evidence_admission_id=evidence_admission_id,
            authorization_consumption_id=authorization_consumption_id,
            action_ledger_id=action_ledger_id,
            admitted_at_utc=admitted_at,
            implementation_repository_code_sha=implementation_repository_code_sha,
            policy_load=canonical.legacy_admission,
            authorization=admitted_authorization.collector_authorization,
            authorization_admission=admitted_authorization.legacy_admission_receipt,
            actions=actions,
            backtest_id=backtest_id,
            result_bytes=result_bytes,
            dq_report_path=dq_report_path,
            dq_report_bytes=dq_report_bytes,
            reviewed_project_code_lf_sha256=reviewed_project_code_lf_sha256,
            prior_consumption_receipts=prior_evidence_consumption_receipts,
            project_root=project_root,
            evidence_root=evidence_root,
        )
    except (TypeError, ValueError) as exc:
        raise QCQQQOptionsRefreshAuthorizationAdmissionError(
            "REFRESH_COLLECTION_EVIDENCE_REJECTED", str(exc)
        ) from exc
    if legacy_bundle.external_action_ledger.content_sha256 != ledger.content_sha256:
        raise QCQQQOptionsRefreshAuthorizationAdmissionError(
            "REFRESH_EVIDENCE_LEDGER_IDENTITY_MISMATCH",
            "evidence admission replay produced a different external-action ledger",
        )
    return QCQQQOptionsRefreshCollectionEvidenceAdmissionBundle(
        refresh_policy_load=canonical,
        admitted_authorization=admitted_authorization,
        run_attempt_consumption=run_consumption,
        legacy_evidence_bundle=legacy_bundle,
    )


__all__ = [
    "DEFAULT_QC_QQQ_OPTIONS_REFRESH_AUTHORIZATION_ADMISSION_POLICY_PATH",
    "AdmittedQCQQQOptionsRefreshAuthorization",
    "QCQQQOptionsRefreshAuthorizationAdmissionError",
    "QCQQQOptionsRefreshAuthorizationAdmissionPolicy",
    "QCQQQOptionsRefreshAuthorizationAdmissionPolicyLoadResult",
    "QCQQQOptionsRefreshCollectionEvidenceAdmissionBundle",
    "RefreshAuthorizationLifecycleState",
    "RefreshAuthorizationRunAttemptConsumptionReceipt",
    "RefreshOwnerAuthorizationAdmissionReceipt",
    "admit_qc_qqq_options_refresh_owner_authorization",
    "build_qc_qqq_options_refresh_authorization_not_provided_state",
    "build_qc_qqq_options_refresh_collection_evidence_admission",
    "build_qc_qqq_options_refresh_external_action_ledger",
    "build_qc_qqq_options_refresh_run_attempt_consumption",
    "load_qc_qqq_options_refresh_authorization_admission_policy",
]
