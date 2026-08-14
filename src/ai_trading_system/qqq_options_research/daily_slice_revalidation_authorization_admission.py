from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Self, cast

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    primary_window_daily_slice_zero_order_revalidation as revalidation_v1,
)
from ai_trading_system.qqq_options_research import (
    primary_window_derived_aggregate_collection_evidence_admission as admission_v1,
)
from ai_trading_system.qqq_options_research import (
    primary_window_export_safe_derived_aggregate_collector as collector_v1,
)
from ai_trading_system.qqq_options_research import refresh_authorization_admission as refresh_v1
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_DAILY_SLICE_REVALIDATION_AUTHORIZATION_ADMISSION_POLICY_PATH = Path(
    "config/research/qc_qqq_options_daily_slice_revalidation_authorization_admission_v1.yaml"
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
_OWNER_TOKEN_FIELD_ORDER = (
    "ordinary_pushed_main_sha",
    "registration_base_repository_code_sha",
    "revalidation_policy_file_sha256",
    "revalidation_policy_canonical_sha256",
    "revalidation_package_manifest_file_sha256",
    "revalidation_package_manifest_content_sha256",
    "proposal_content_sha256",
    "run_scope_content_sha256",
    "corrected_project_code_lf_sha256",
    "predecessor_failed_backtest_id",
    "predecessor_failed_result_file_sha256",
    "target_project_id",
    "requested_range",
    "expected_session_count",
    "maximum_project_mutations",
    "maximum_cloud_backtests",
    "maximum_orders",
    "maximum_fills",
    "collector",
    "independent_reviewer",
    "authorization_expires_at_utc",
    "authorization_single_use",
    "authorization_invalidates_after_first_run_attempt",
)


class QCQQQOptionsDailySliceAuthorizationAdmissionError(ValueError):
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
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None:
        raise ValueError(f"{field} must be timezone-aware")
    if offset.total_seconds() != 0:
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


class DailySliceAuthorizationAdmissionSafety(_PolicyModel):
    owner_token_observed: Literal[False]
    authorization_status: Literal["OWNER_V4_TOKEN_NOT_PROVIDED"]
    evidence_status: Literal["EVIDENCE_NOT_ADMITTED_POLICY_BLOCKED"]
    dq_pit_status: Literal["NOT_EVALUATED"]
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


class QCQQQOptionsDailySliceAuthorizationAdmissionPolicy(_PolicyModel):
    schema_version: Literal[
        "qc_qqq_options_daily_slice_revalidation_authorization_admission_policy.v1"
    ]
    policy_id: Literal[
        "TRADING_2521_QC_QQQ_OPTIONS_DAILY_SLICE_REVALIDATION_AUTHORIZATION_ADMISSION_V1"
    ]
    policy_version: Literal["1.0.0"]
    policy_status: Literal["ENGINEERING_BASELINE_OWNER_V4_TOKEN_NOT_PROVIDED"]
    task_id: Literal[
        "TRADING-2521_QC_QQQ_OPTIONS_DAILY_SLICE_REVALIDATION_AUTHORIZATION_ADMISSION_V1"
    ]
    predecessor_ordinary_pushed_main_sha: str
    registration_base_repository_code_sha: str
    revalidation_policy_path: str
    revalidation_policy_file_sha256: str
    revalidation_policy_canonical_sha256: str
    revalidation_package_root: str
    revalidation_package_manifest_file_sha256: str
    revalidation_package_manifest_content_sha256: str
    proposal_content_sha256: str
    run_scope_content_sha256: str
    corrected_project_code_lf_sha256: str
    predecessor_failed_backtest_id: str
    predecessor_failed_result_file_sha256: str
    collector_policy_file_sha256: str
    collector_policy_canonical_sha256: str
    transport_map_sha256: str
    expected_owner_decision_token: str
    token_decision_date: date
    authorization_expires_after_hours: Literal[168]
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
    safety: DailySliceAuthorizationAdmissionSafety

    @field_validator(
        "predecessor_ordinary_pushed_main_sha", "registration_base_repository_code_sha"
    )
    @classmethod
    def _git_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _git_sha(value, str(info.field_name))

    @field_validator(
        "revalidation_policy_file_sha256",
        "revalidation_policy_canonical_sha256",
        "revalidation_package_manifest_file_sha256",
        "revalidation_package_manifest_content_sha256",
        "proposal_content_sha256",
        "run_scope_content_sha256",
        "corrected_project_code_lf_sha256",
        "predecessor_failed_result_file_sha256",
        "collector_policy_file_sha256",
        "collector_policy_canonical_sha256",
        "transport_map_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("revalidation_policy_path", "revalidation_package_root")
    @classmethod
    def _paths(cls, value: str, info: ValidationInfo) -> str:
        return _relative_path(value, str(info.field_name))

    @field_validator("predecessor_failed_backtest_id", "collector_id", "independent_reviewer_id")
    @classmethod
    def _ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _scope(self) -> Self:
        expected = (
            "owner_decision:TRADING-2520:2026-08-15:"
            "authorize_single_zero_order_primary_window_daily_slice_revalidation_v4"
        )
        if self.expected_owner_decision_token != expected:
            raise ValueError("2520 v4 Owner token identity drifted")
        if self.token_decision_date != date(2026, 8, 15):
            raise ValueError("v4 token decision date drifted")
        if (self.requested_start, self.evaluated_start) != (
            date(2021, 2, 22),
            date(2021, 2, 22),
        ) or (self.requested_end, self.evaluated_end) != (
            date(2025, 12, 2),
            date(2025, 12, 2),
        ):
            raise ValueError("PRIMARY revalidation range drifted")
        if self.allowed_actions != _ALLOWED_ACTIONS:
            raise ValueError("allowed action inventory/order drifted")
        if self.prohibited_actions != _PROHIBITED_ACTIONS:
            raise ValueError("prohibited action inventory/order drifted")
        return self

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


@dataclass(frozen=True)
class QCQQQOptionsDailySliceAuthorizationAdmissionPolicyLoadResult:
    policy: QCQQQOptionsDailySliceAuthorizationAdmissionPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str
    revalidation_package: revalidation_v1.LoadedDailySliceZeroOrderRevalidationPackage


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
            raise QCQQQOptionsDailySliceAuthorizationAdmissionError(
                "DAILY_SLICE_AUTHORIZATION_RECORD_INVALID", str(exc)
            ) from exc


class DailySliceOwnerDecisionCandidate(_SealedModel):
    schema_version: Literal["qc_qqq_options_daily_slice_owner_decision_candidate.v1"]
    owner_decision_token: str
    owner_decision_file_sha256: str
    owner_decision_content_sha256: str
    ordinary_pushed_main_sha: str
    reviewed_at_utc: datetime
    expires_at_utc: datetime
    admission_policy_file_sha256: str
    admission_policy_canonical_sha256: str
    revalidation_package_manifest_content_sha256: str
    proposal_content_sha256: str
    run_scope_content_sha256: str
    project_code_lf_sha256: str
    authorization_single_use: Literal[True]
    authorization_invalidates_after_first_run_attempt: Literal[True]
    authorization_consumed: Literal[False]
    decision: Literal["OWNER_V4_AUTHORIZATION_REVIEWED_NOT_CONSUMED"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    external_action_performed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("ordinary_pushed_main_sha")
    @classmethod
    def _main_sha(cls, value: str) -> str:
        return _git_sha(value, "ordinary_pushed_main_sha")

    @field_validator("reviewed_at_utc", "expires_at_utc")
    @classmethod
    def _times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc(value, str(info.field_name))

    @field_validator(
        "owner_decision_file_sha256",
        "owner_decision_content_sha256",
        "admission_policy_file_sha256",
        "admission_policy_canonical_sha256",
        "revalidation_package_manifest_content_sha256",
        "proposal_content_sha256",
        "run_scope_content_sha256",
        "project_code_lf_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class DailySliceOwnerAuthorizationAdmissionReceipt(_SealedModel):
    schema_version: Literal[
        "qc_qqq_options_daily_slice_owner_authorization_admission_receipt.v1"
    ]
    admission_id: str
    admitted_at_utc: datetime
    owner_decision_source: Literal["PROJECT_OWNER_CURRENT_CODEX_DIALOG"]
    admission_policy_file_sha256: str
    admission_policy_canonical_sha256: str
    owner_candidate_content_sha256: str
    legacy_admission_receipt_content_sha256: str
    collector_authorization_content_sha256: str
    owner_decision_token: str
    owner_decision_file_sha256: str
    owner_decision_content_sha256: str
    ordinary_pushed_main_sha: str
    authorized_at_utc: datetime
    expires_at_utc: datetime
    authorization_single_use: Literal[True]
    authorization_invalidates_after_first_run_attempt: Literal[True]
    authorization_consumed: Literal[False]
    decision: Literal["OWNER_V4_AUTHORIZATION_ADMITTED_UNUSED"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    external_action_performed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("admission_id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _identifier(value, "admission_id")

    @field_validator("ordinary_pushed_main_sha")
    @classmethod
    def _main_sha(cls, value: str) -> str:
        return _git_sha(value, "ordinary_pushed_main_sha")

    @field_validator("admitted_at_utc", "authorized_at_utc", "expires_at_utc")
    @classmethod
    def _times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc(value, str(info.field_name))

    @field_validator(
        "admission_policy_file_sha256",
        "admission_policy_canonical_sha256",
        "owner_candidate_content_sha256",
        "legacy_admission_receipt_content_sha256",
        "collector_authorization_content_sha256",
        "owner_decision_file_sha256",
        "owner_decision_content_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class DailySliceRunAttemptConsumptionReceipt(_SealedModel):
    schema_version: Literal[
        "qc_qqq_options_daily_slice_run_attempt_consumption_receipt.v1"
    ]
    consumption_id: str
    recorded_at_utc: datetime
    daily_slice_authorization_admission_content_sha256: str
    legacy_refresh_consumption_content_sha256: str
    external_action_ledger_content_sha256: str
    owner_decision_token: str
    backtest_id: str
    run_status: admission_v1.CollectionActionStatus
    failure_reason_code: str | None
    authorization_single_use: Literal[True]
    authorization_consumed: Literal[True]
    authorization_invalidated_for_further_cloud_runs: Literal[True]
    evidence_collection_completed: Literal[False]
    decision: Literal["V4_AUTHORIZATION_CONSUMED_AT_FIRST_CLOUD_RUN_ATTEMPT"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("consumption_id", "backtest_id")
    @classmethod
    def _ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("recorded_at_utc")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value, "recorded_at_utc")

    @field_validator("failure_reason_code")
    @classmethod
    def _reason(cls, value: str | None) -> str | None:
        return None if value is None else _identifier(value, "failure_reason_code")

    @field_validator(
        "daily_slice_authorization_admission_content_sha256",
        "legacy_refresh_consumption_content_sha256",
        "external_action_ledger_content_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class DailySliceResultAdmissionReceipt(_SealedModel):
    schema_version: Literal["qc_qqq_options_daily_slice_result_admission_receipt.v1"]
    result_admission_id: str
    admitted_at_utc: datetime
    admission_policy_file_sha256: str
    admission_policy_canonical_sha256: str
    daily_slice_authorization_admission_content_sha256: str
    run_attempt_consumption_content_sha256: str
    external_action_ledger_content_sha256: str
    collector_evidence_content_sha256: str
    collector_evidence_canonical_sha256: str
    result_file_sha256: str
    result_payload_sha256: str
    backtest_id: str
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    session_count: Literal[1202]
    evidence_status: Literal["RESULT_PARSED_DQ_NOT_EVALUATED"]
    local_derived_aggregate_dq_status: Literal["NOT_EVALUATED"]
    local_derived_aggregate_pit_status: Literal["NOT_EVALUATED"]
    option_event_dq_status: Literal["NOT_EVALUATED"]
    option_event_pit_status: Literal["NOT_EVALUATED"]
    decision: Literal["RESULT_PARSED_CANONICALLY_DQ_PIT_GATE_REQUIRED"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    raw_option_rows_exported: Literal[False]
    investment_interpretation_generated: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("result_admission_id", "backtest_id")
    @classmethod
    def _ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("admitted_at_utc")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value, "admitted_at_utc")

    @field_validator(
        "admission_policy_file_sha256",
        "admission_policy_canonical_sha256",
        "daily_slice_authorization_admission_content_sha256",
        "run_attempt_consumption_content_sha256",
        "external_action_ledger_content_sha256",
        "collector_evidence_content_sha256",
        "collector_evidence_canonical_sha256",
        "result_file_sha256",
        "result_payload_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


@dataclass(frozen=True)
class AdmittedQCQQQOptionsDailySliceAuthorization:
    policy_load: QCQQQOptionsDailySliceAuthorizationAdmissionPolicyLoadResult
    owner_candidate: DailySliceOwnerDecisionCandidate
    collector_authorization: collector_v1.QCQQQOptionsDerivedAggregateCollectorAuthorization
    legacy_admission_receipt: admission_v1.OwnerAuthorizationAdmissionReceipt
    legacy_refresh_admission_receipt: refresh_v1.RefreshOwnerAuthorizationAdmissionReceipt
    daily_slice_admission_receipt: DailySliceOwnerAuthorizationAdmissionReceipt


@dataclass(frozen=True)
class QCQQQOptionsDailySliceParsedResultBundle:
    policy_load: QCQQQOptionsDailySliceAuthorizationAdmissionPolicyLoadResult
    admitted_authorization: AdmittedQCQQQOptionsDailySliceAuthorization
    run_attempt_consumption: DailySliceRunAttemptConsumptionReceipt
    external_action_ledger: admission_v1.CollectionExternalActionLedger
    collector_evidence: collector_v1.QCQQQOptionsDerivedAggregateCollectorEvidence
    result_admission: DailySliceResultAdmissionReceipt


def load_qc_qqq_options_daily_slice_revalidation_authorization_admission_policy(
    policy_path: Path = (
        DEFAULT_QC_QQQ_OPTIONS_DAILY_SLICE_REVALIDATION_AUTHORIZATION_ADMISSION_POLICY_PATH
    ),
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDailySliceAuthorizationAdmissionPolicyLoadResult:
    root = project_root.resolve()
    try:
        path = _bound_file(policy_path, root=root, field="admission policy")
        raw = path.read_bytes()
        payload = safe_load_yaml_path(path)
        policy = QCQQQOptionsDailySliceAuthorizationAdmissionPolicy.model_validate(payload)
        package_loader = (
            revalidation_v1.load_qc_qqq_options_primary_window_daily_slice_zero_order_revalidation_package
        )
        package = package_loader(project_root=root)
        manifest_path = package.package_root / "package_manifest.json"
        revalidation_policy = package.policy
        proposal = package.proposal
        scope = package.run_scope
        predecessor = package.policy.policy.predecessor
        if (
            revalidation_policy.policy_path.relative_to(root).as_posix()
            != policy.revalidation_policy_path
            or revalidation_policy.policy_file_sha256 != policy.revalidation_policy_file_sha256
            or revalidation_policy.policy_canonical_sha256
            != policy.revalidation_policy_canonical_sha256
            or package.package_root.relative_to(root).as_posix()
            != policy.revalidation_package_root
            or hashlib.sha256(manifest_path.read_bytes()).hexdigest()
            != policy.revalidation_package_manifest_file_sha256
            or package.manifest.content_sha256
            != policy.revalidation_package_manifest_content_sha256
            or proposal.content_sha256 != policy.proposal_content_sha256
            or scope.content_sha256 != policy.run_scope_content_sha256
            or package.manifest.project_code_lf_sha256
            != policy.corrected_project_code_lf_sha256
            or predecessor.failed_backtest_id != policy.predecessor_failed_backtest_id
            or predecessor.failed_result_file_sha256
            != policy.predecessor_failed_result_file_sha256
            or proposal.collector_policy_file_sha256 != policy.collector_policy_file_sha256
            or proposal.collector_policy_canonical_sha256
            != policy.collector_policy_canonical_sha256
            or proposal.transport_map_sha256 != policy.transport_map_sha256
            or scope.repository_code_sha != policy.registration_base_repository_code_sha
            or scope.target_project_id != policy.target_project_id
            or (scope.requested_start, scope.requested_end)
            != (policy.requested_start, policy.requested_end)
            or (scope.evaluated_start, scope.evaluated_end)
            != (policy.evaluated_start, policy.evaluated_end)
            or len(scope.session_ids) != policy.expected_session_count
            or proposal.allowed_actions != policy.allowed_actions
            or proposal.prohibited_actions != policy.prohibited_actions
        ):
            raise ValueError("2520 daily Slice revalidation authority drifted")
    except QCQQQOptionsDailySliceAuthorizationAdmissionError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsDailySliceAuthorizationAdmissionError(
            "DAILY_SLICE_AUTHORIZATION_ADMISSION_POLICY_REJECTED", str(exc)
        ) from exc
    return QCQQQOptionsDailySliceAuthorizationAdmissionPolicyLoadResult(
        policy=policy,
        policy_path=path,
        policy_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy_canonical_sha256=policy.canonical_sha256,
        revalidation_package=package,
    )


def validate_qc_qqq_options_daily_slice_owner_decision_candidate(
    *,
    owner_decision_bytes: bytes,
    reviewed_at_utc: datetime,
    project_root: Path = PROJECT_ROOT,
) -> DailySliceOwnerDecisionCandidate:
    loaded = load_qc_qqq_options_daily_slice_revalidation_authorization_admission_policy(
        project_root=project_root
    )
    policy = loaded.policy
    reviewed_at = _utc(reviewed_at_utc, "reviewed_at_utc")
    try:
        text = owner_decision_bytes.decode("utf-8")
        if "\r" in text or not text.endswith("\n") or text.endswith("\n\n"):
            raise ValueError("Owner decision must be exact LF text with one final newline")
        lines = text[:-1].split("\n")
        if not lines or lines[0] != policy.expected_owner_decision_token:
            raise ValueError("fresh exact 2520 v4 Owner decision token was not supplied")
        fields: dict[str, str] = {}
        order: list[str] = []
        for line in lines[1:]:
            if line.count(":") < 1:
                raise ValueError("Owner decision line lacks key/value separator")
            key, value = line.split(":", 1)
            if key in fields or not _IDENTIFIER.fullmatch(key):
                raise ValueError(f"invalid or duplicate Owner decision field: {key}")
            if not value or value != value.strip():
                raise ValueError(f"invalid Owner decision value: {key}")
            fields[key] = value
            order.append(key)
        if tuple(order) != _OWNER_TOKEN_FIELD_ORDER:
            raise ValueError("Owner decision field inventory/order drifted")
        expiry_text = fields["authorization_expires_at_utc"]
        expiry = datetime.strptime(expiry_text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        decision_start = datetime.combine(policy.token_decision_date, datetime.min.time(), UTC)
        decision_limit = decision_start + timedelta(hours=policy.authorization_expires_after_hours)
        if not decision_start < expiry <= decision_limit:
            raise ValueError("authorization expiry is outside the reviewed <=168h window")
        if not decision_start <= reviewed_at <= expiry:
            raise ValueError("Owner review as-of is outside the authorization window")
        expected = {
            "ordinary_pushed_main_sha": policy.predecessor_ordinary_pushed_main_sha,
            "registration_base_repository_code_sha": policy.registration_base_repository_code_sha,
            "revalidation_policy_file_sha256": policy.revalidation_policy_file_sha256,
            "revalidation_policy_canonical_sha256": policy.revalidation_policy_canonical_sha256,
            "revalidation_package_manifest_file_sha256": (
                policy.revalidation_package_manifest_file_sha256
            ),
            "revalidation_package_manifest_content_sha256": (
                policy.revalidation_package_manifest_content_sha256
            ),
            "proposal_content_sha256": policy.proposal_content_sha256,
            "run_scope_content_sha256": policy.run_scope_content_sha256,
            "corrected_project_code_lf_sha256": policy.corrected_project_code_lf_sha256,
            "predecessor_failed_backtest_id": policy.predecessor_failed_backtest_id,
            "predecessor_failed_result_file_sha256": policy.predecessor_failed_result_file_sha256,
            "target_project_id": str(policy.target_project_id),
            "requested_range": (
                f"{policy.requested_start.isoformat()}..{policy.requested_end.isoformat()}"
            ),
            "expected_session_count": str(policy.expected_session_count),
            "maximum_project_mutations": str(policy.maximum_project_mutations),
            "maximum_cloud_backtests": str(policy.maximum_cloud_backtests),
            "maximum_orders": "0",
            "maximum_fills": "0",
            "collector": policy.collector_id,
            "independent_reviewer": policy.independent_reviewer_id,
            "authorization_expires_at_utc": expiry_text,
            "authorization_single_use": "true",
            "authorization_invalidates_after_first_run_attempt": "true",
        }
        if fields != expected:
            mismatches = tuple(sorted(key for key in expected if fields.get(key) != expected[key]))
            raise ValueError(f"Owner decision binding mismatch: {mismatches}")
    except (UnicodeDecodeError, OSError, ValueError) as exc:
        raise QCQQQOptionsDailySliceAuthorizationAdmissionError(
            "OWNER_V4_AUTHORIZATION_CANDIDATE_REJECTED", str(exc)
        ) from exc
    semantic_sha = hashlib.sha256(
        _canonical_json_bytes({"owner_decision_token": lines[0], **fields})
    ).hexdigest()
    return DailySliceOwnerDecisionCandidate.seal(
        schema_version="qc_qqq_options_daily_slice_owner_decision_candidate.v1",
        owner_decision_token=policy.expected_owner_decision_token,
        owner_decision_file_sha256=hashlib.sha256(owner_decision_bytes).hexdigest(),
        owner_decision_content_sha256=semantic_sha,
        ordinary_pushed_main_sha=policy.predecessor_ordinary_pushed_main_sha,
        reviewed_at_utc=reviewed_at,
        expires_at_utc=expiry,
        admission_policy_file_sha256=loaded.policy_file_sha256,
        admission_policy_canonical_sha256=loaded.policy_canonical_sha256,
        revalidation_package_manifest_content_sha256=policy.revalidation_package_manifest_content_sha256,
        proposal_content_sha256=policy.proposal_content_sha256,
        run_scope_content_sha256=policy.run_scope_content_sha256,
        project_code_lf_sha256=policy.corrected_project_code_lf_sha256,
        authorization_single_use=True,
        authorization_invalidates_after_first_run_attempt=True,
        authorization_consumed=False,
        decision="OWNER_V4_AUTHORIZATION_REVIEWED_NOT_CONSUMED",
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        external_action_performed=False,
        production_effect="none",
        broker_action="none",
    )


def admit_qc_qqq_options_daily_slice_owner_authorization(
    *,
    admission_id: str,
    admitted_at_utc: datetime,
    owner_decision_bytes: bytes,
    owner_decision_source: str,
    project_root: Path = PROJECT_ROOT,
) -> AdmittedQCQQQOptionsDailySliceAuthorization:
    loaded = load_qc_qqq_options_daily_slice_revalidation_authorization_admission_policy(
        project_root=project_root
    )
    policy = loaded.policy
    if owner_decision_source != policy.owner_decision_source:
        raise QCQQQOptionsDailySliceAuthorizationAdmissionError(
            "OWNER_V4_AUTHORIZATION_SOURCE_REJECTED",
            "Owner decision must originate from the Project Owner current Codex dialog",
        )
    admitted_at = _utc(admitted_at_utc, "admitted_at_utc")
    candidate = validate_qc_qqq_options_daily_slice_owner_decision_candidate(
        owner_decision_bytes=owner_decision_bytes,
        reviewed_at_utc=admitted_at,
        project_root=project_root,
    )
    package = loaded.revalidation_package
    authorization = collector_v1.QCQQQOptionsDerivedAggregateCollectorAuthorization.seal(
        schema_version="qc_qqq_options_derived_aggregate_collector_authorization.v1",
        owner_decision_token=candidate.owner_decision_token,
        authorized_at_utc=candidate.reviewed_at_utc,
        expires_at_utc=candidate.expires_at_utc,
        authorization_single_use=True,
        authorization_invalidates_after_evidence_collection=True,
        proposal_content_sha256=package.proposal.content_sha256,
        run_scope_content_sha256=package.run_scope.content_sha256,
        repository_code_sha=package.run_scope.repository_code_sha,
        target_project_id=policy.target_project_id,
        project_code_lf_sha256=policy.corrected_project_code_lf_sha256,
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
    refresh_receipt = refresh_v1.RefreshOwnerAuthorizationAdmissionReceipt.seal(
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
    daily_receipt = DailySliceOwnerAuthorizationAdmissionReceipt.seal(
        schema_version="qc_qqq_options_daily_slice_owner_authorization_admission_receipt.v1",
        admission_id=admission_id,
        admitted_at_utc=admitted_at,
        owner_decision_source="PROJECT_OWNER_CURRENT_CODEX_DIALOG",
        admission_policy_file_sha256=loaded.policy_file_sha256,
        admission_policy_canonical_sha256=loaded.policy_canonical_sha256,
        owner_candidate_content_sha256=candidate.content_sha256,
        legacy_admission_receipt_content_sha256=legacy_receipt.content_sha256,
        collector_authorization_content_sha256=authorization.content_sha256,
        owner_decision_token=candidate.owner_decision_token,
        owner_decision_file_sha256=candidate.owner_decision_file_sha256,
        owner_decision_content_sha256=candidate.owner_decision_content_sha256,
        ordinary_pushed_main_sha=candidate.ordinary_pushed_main_sha,
        authorized_at_utc=authorization.authorized_at_utc,
        expires_at_utc=authorization.expires_at_utc,
        authorization_single_use=True,
        authorization_invalidates_after_first_run_attempt=True,
        authorization_consumed=False,
        decision="OWNER_V4_AUTHORIZATION_ADMITTED_UNUSED",
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        external_action_performed=False,
        production_effect="none",
        broker_action="none",
    )
    return AdmittedQCQQQOptionsDailySliceAuthorization(
        policy_load=loaded,
        owner_candidate=candidate,
        collector_authorization=authorization,
        legacy_admission_receipt=legacy_receipt,
        legacy_refresh_admission_receipt=refresh_receipt,
        daily_slice_admission_receipt=daily_receipt,
    )


def build_qc_qqq_options_daily_slice_external_action_ledger(
    *,
    ledger_id: str,
    sealed_at_utc: datetime,
    admitted_authorization: AdmittedQCQQQOptionsDailySliceAuthorization,
    actions: tuple[admission_v1.CollectionExternalAction, ...],
) -> admission_v1.CollectionExternalActionLedger:
    return admission_v1.build_qc_qqq_options_collection_external_action_ledger(
        ledger_id=ledger_id,
        sealed_at_utc=sealed_at_utc,
        authorization=admitted_authorization.collector_authorization,
        authorization_admission=admitted_authorization.legacy_admission_receipt,
        actions=actions,
    )


def build_qc_qqq_options_daily_slice_run_attempt_consumption(
    *,
    consumption_id: str,
    recorded_at_utc: datetime,
    admitted_authorization: AdmittedQCQQQOptionsDailySliceAuthorization,
    external_action_ledger: admission_v1.CollectionExternalActionLedger,
    prior_consumption_receipts: tuple[DailySliceRunAttemptConsumptionReceipt, ...] = (),
) -> DailySliceRunAttemptConsumptionReceipt:
    receipt = admitted_authorization.daily_slice_admission_receipt
    if any(
        item.owner_decision_token == receipt.owner_decision_token
        or item.daily_slice_authorization_admission_content_sha256 == receipt.content_sha256
        for item in prior_consumption_receipts
    ):
        raise QCQQQOptionsDailySliceAuthorizationAdmissionError(
            "OWNER_V4_AUTHORIZATION_ALREADY_CONSUMED",
            "single-use v4 authorization cannot fund a second Cloud run attempt",
        )
    adapter = refresh_v1.AdmittedQCQQQOptionsRefreshAuthorization(
        policy_load=cast(Any, admitted_authorization.policy_load),
        refresh_candidate=cast(Any, admitted_authorization.owner_candidate),
        collector_authorization=admitted_authorization.collector_authorization,
        legacy_admission_receipt=admitted_authorization.legacy_admission_receipt,
        refresh_admission_receipt=admitted_authorization.legacy_refresh_admission_receipt,
    )
    try:
        legacy = refresh_v1.build_qc_qqq_options_refresh_run_attempt_consumption(
            consumption_id=f"{consumption_id}:legacy",
            recorded_at_utc=recorded_at_utc,
            admitted_authorization=adapter,
            external_action_ledger=external_action_ledger,
        )
    except (TypeError, ValueError) as exc:
        raise QCQQQOptionsDailySliceAuthorizationAdmissionError(
            "DAILY_SLICE_RUN_ATTEMPT_REJECTED", str(exc)
        ) from exc
    return DailySliceRunAttemptConsumptionReceipt.seal(
        schema_version="qc_qqq_options_daily_slice_run_attempt_consumption_receipt.v1",
        consumption_id=consumption_id,
        recorded_at_utc=_utc(recorded_at_utc, "recorded_at_utc"),
        daily_slice_authorization_admission_content_sha256=receipt.content_sha256,
        legacy_refresh_consumption_content_sha256=legacy.content_sha256,
        external_action_ledger_content_sha256=external_action_ledger.content_sha256,
        owner_decision_token=receipt.owner_decision_token,
        backtest_id=legacy.backtest_id,
        run_status=legacy.run_status,
        failure_reason_code=legacy.failure_reason_code,
        authorization_single_use=True,
        authorization_consumed=True,
        authorization_invalidated_for_further_cloud_runs=True,
        evidence_collection_completed=False,
        decision="V4_AUTHORIZATION_CONSUMED_AT_FIRST_CLOUD_RUN_ATTEMPT",
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        orders=0,
        fills=0,
        production_effect="none",
        broker_action="none",
    )


def build_qc_qqq_options_daily_slice_parsed_result_admission(
    *,
    result_admission_id: str,
    admitted_at_utc: datetime,
    admitted_authorization: AdmittedQCQQQOptionsDailySliceAuthorization,
    run_attempt_consumption: DailySliceRunAttemptConsumptionReceipt,
    actions: tuple[admission_v1.CollectionExternalAction, ...],
    backtest_id: str,
    result_bytes: bytes,
    reviewed_project_code_lf_sha256: str,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDailySliceParsedResultBundle:
    admitted_at = _utc(admitted_at_utc, "admitted_at_utc")
    canonical = load_qc_qqq_options_daily_slice_revalidation_authorization_admission_policy(
        project_root=project_root
    )
    if admitted_authorization.policy_load != canonical:
        raise QCQQQOptionsDailySliceAuthorizationAdmissionError(
            "DAILY_SLICE_AUTHORIZATION_AUTHORITY_MISMATCH",
            "admitted authorization differs from canonical 2521 policy",
        )
    daily_receipt = admitted_authorization.daily_slice_admission_receipt
    if (
        run_attempt_consumption.daily_slice_authorization_admission_content_sha256
        != daily_receipt.content_sha256
        or run_attempt_consumption.owner_decision_token != daily_receipt.owner_decision_token
        or run_attempt_consumption.backtest_id != backtest_id
        or not run_attempt_consumption.authorization_consumed
    ):
        raise QCQQQOptionsDailySliceAuthorizationAdmissionError(
            "DAILY_SLICE_RUN_CONSUMPTION_IDENTITY_MISMATCH",
            "result admission differs from the consumed first Cloud run",
        )
    ledger = build_qc_qqq_options_daily_slice_external_action_ledger(
        ledger_id=f"{result_admission_id}:ledger",
        sealed_at_utc=admitted_at,
        admitted_authorization=admitted_authorization,
        actions=actions,
    )
    if ledger.lifecycle_status != "COMPLETE" or ledger.scope_status != "PASS":
        raise QCQQQOptionsDailySliceAuthorizationAdmissionError(
            "DAILY_SLICE_RESULT_LIFECYCLE_INCOMPLETE",
            f"external action lifecycle is {ledger.lifecycle_status}/{ledger.scope_status}",
        )
    result_sha = hashlib.sha256(result_bytes).hexdigest()
    run_action = ledger.actions[2]
    result_action = ledger.actions[3]
    if (
        run_action.backtest_id != backtest_id
        or result_action.backtest_id != backtest_id
        or result_action.result_file_sha256 != result_sha
        or run_action.backtest_id != run_attempt_consumption.backtest_id
    ):
        raise QCQQQOptionsDailySliceAuthorizationAdmissionError(
            "DAILY_SLICE_RESULT_ACTION_IDENTITY_MISMATCH",
            "run/download actions do not bind the admitted result bytes",
        )
    try:
        evidence = (
            collector_v1.build_qc_qqq_options_primary_window_derived_aggregate_collector_evidence(
                evidence_id=f"{result_admission_id}:collector",
                collected_at_utc=result_action.occurred_at_utc,
                backtest_id=backtest_id,
                result_bytes=result_bytes,
                proposal=canonical.revalidation_package.proposal,
                authorization=admitted_authorization.collector_authorization,
                reviewed_target_project_id=canonical.policy.target_project_id,
                reviewed_project_code_lf_sha256=reviewed_project_code_lf_sha256,
                project_root=project_root,
            )
        )
    except (TypeError, ValueError) as exc:
        raise QCQQQOptionsDailySliceAuthorizationAdmissionError(
            "DAILY_SLICE_RESULT_PARSER_REJECTED", str(exc)
        ) from exc
    receipt = DailySliceResultAdmissionReceipt.seal(
        schema_version="qc_qqq_options_daily_slice_result_admission_receipt.v1",
        result_admission_id=result_admission_id,
        admitted_at_utc=admitted_at,
        admission_policy_file_sha256=canonical.policy_file_sha256,
        admission_policy_canonical_sha256=canonical.policy_canonical_sha256,
        daily_slice_authorization_admission_content_sha256=daily_receipt.content_sha256,
        run_attempt_consumption_content_sha256=run_attempt_consumption.content_sha256,
        external_action_ledger_content_sha256=ledger.content_sha256,
        collector_evidence_content_sha256=evidence.content_sha256,
        collector_evidence_canonical_sha256=evidence.canonical_sha256,
        result_file_sha256=evidence.result_file_sha256,
        result_payload_sha256=evidence.result_payload_sha256,
        backtest_id=evidence.backtest_id,
        requested_start=evidence.requested_start,
        requested_end=evidence.requested_end,
        evaluated_start=evidence.evaluated_start,
        evaluated_end=evidence.evaluated_end,
        session_count=len(evidence.session_ids),
        evidence_status="RESULT_PARSED_DQ_NOT_EVALUATED",
        local_derived_aggregate_dq_status="NOT_EVALUATED",
        local_derived_aggregate_pit_status="NOT_EVALUATED",
        option_event_dq_status="NOT_EVALUATED",
        option_event_pit_status="NOT_EVALUATED",
        decision="RESULT_PARSED_CANONICALLY_DQ_PIT_GATE_REQUIRED",
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        orders=0,
        fills=0,
        raw_option_rows_exported=False,
        investment_interpretation_generated=False,
        production_effect="none",
        broker_action="none",
    )
    return QCQQQOptionsDailySliceParsedResultBundle(
        policy_load=canonical,
        admitted_authorization=admitted_authorization,
        run_attempt_consumption=run_attempt_consumption,
        external_action_ledger=ledger,
        collector_evidence=evidence,
        result_admission=receipt,
    )


__all__ = [
    "DEFAULT_QC_QQQ_OPTIONS_DAILY_SLICE_REVALIDATION_AUTHORIZATION_ADMISSION_POLICY_PATH",
    "AdmittedQCQQQOptionsDailySliceAuthorization",
    "DailySliceOwnerAuthorizationAdmissionReceipt",
    "DailySliceOwnerDecisionCandidate",
    "DailySliceRunAttemptConsumptionReceipt",
    "DailySliceResultAdmissionReceipt",
    "QCQQQOptionsDailySliceAuthorizationAdmissionError",
    "QCQQQOptionsDailySliceAuthorizationAdmissionPolicy",
    "QCQQQOptionsDailySliceAuthorizationAdmissionPolicyLoadResult",
    "QCQQQOptionsDailySliceParsedResultBundle",
    "admit_qc_qqq_options_daily_slice_owner_authorization",
    "build_qc_qqq_options_daily_slice_external_action_ledger",
    "build_qc_qqq_options_daily_slice_parsed_result_admission",
    "build_qc_qqq_options_daily_slice_run_attempt_consumption",
    "load_qc_qqq_options_daily_slice_revalidation_authorization_admission_policy",
    "validate_qc_qqq_options_daily_slice_owner_decision_candidate",
]
