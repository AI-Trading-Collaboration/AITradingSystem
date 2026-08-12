from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    primary_window_derived_aggregate_run_proposal as proposal_v1,
)
from ai_trading_system.qqq_options_research import (
    primary_window_derived_calibration_evidence_generator as generator_v1,
)
from ai_trading_system.qqq_options_research import (
    primary_window_export_safe_derived_aggregate_collector as collector_v1,
)
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    DQReportRecord,
)
from ai_trading_system.qqq_options_research.primary_window_policy_calibration import (
    load_qqq_options_primary_window_calibration_policy,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_COLLECTION_EVIDENCE_ADMISSION_POLICY_PATH = Path(  # noqa: E501
    "config/research/qc_qqq_options_primary_window_derived_aggregate_collection_evidence_admission_v1.yaml"
)

_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")

_OWNER_TOKEN_FIELD_ORDER = (
    "ordinary_pushed_main_sha",
    "repository_code_sha",
    "proposal_content_sha256",
    "run_scope_content_sha256",
    "project_code_lf_sha256",
    "proposal_policy_file_sha256",
    "proposal_policy_canonical_sha256",
    "collector_policy_file_sha256",
    "collector_policy_canonical_sha256",
    "transport_map_sha256",
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
    "authorization_invalidates_after_evidence_collection",
)

_ACTION_ORDER = (
    "QUANTCONNECT_LOGIN",
    "MODIFY_EXISTING_DEDICATED_PROJECT_ONCE",
    "RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST",
    "EXPORT_SAFE_MANUAL_DOWNLOAD_RESULTS_COLLECTION",
)


class QCQQQOptionsCollectionEvidenceAdmissionError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class CollectionActionType(StrEnum):
    QUANTCONNECT_LOGIN = "QUANTCONNECT_LOGIN"
    MODIFY_EXISTING_DEDICATED_PROJECT_ONCE = "MODIFY_EXISTING_DEDICATED_PROJECT_ONCE"
    RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST = "RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST"
    EXPORT_SAFE_MANUAL_DOWNLOAD_RESULTS_COLLECTION = (
        "EXPORT_SAFE_MANUAL_DOWNLOAD_RESULTS_COLLECTION"
    )


class CollectionActionStatus(StrEnum):
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


def _duplicate_key_rejecting_json(raw: bytes) -> object:
    def hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> object:
        raise ValueError(f"non-finite JSON constant is prohibited: {value}")

    try:
        return json.loads(
            raw.decode("utf-8"), object_pairs_hook=hook, parse_constant=reject_constant
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("record is not UTF-8 JSON") from exc


def _required_text(value: str, field: str) -> str:
    if not value or value != value.strip() or not _TEXT_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be non-empty single-line text")
    return value


def _identifier(value: str, field: str) -> str:
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a stable identifier")
    return value


def _sha256(value: str, field: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase SHA-256")
    return value


def _git_sha(value: str, field: str) -> str:
    if not _GIT_SHA_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a 40-character Git SHA")
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
    if not value or path.is_absolute() or path.drive or ".." in path.parts:
        raise ValueError(f"{field} must be a bounded project-relative path")
    if path.as_posix() != value:
        raise ValueError(f"{field} must use normalized forward slashes")
    return value


def _bound_file(path: Path, *, root: Path, field: str, must_exist: bool) -> Path:
    resolved_root = root.resolve()
    candidate = path if path.is_absolute() else resolved_root / path
    try:
        relative = candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes its reviewed root") from exc
    current = resolved_root
    for part in relative.parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise ValueError(f"{field} cannot traverse a symlink")
    if must_exist and (not candidate.is_file() or candidate.is_symlink()):
        raise ValueError(f"{field} must be a non-symlink regular file")
    return candidate


class AdmissionDQHandoff(_PolicyModel):
    calibration_policy_path: str
    calibration_policy_file_sha256: str
    calibration_policy_canonical_sha256: str
    generator_policy_path: str
    generator_policy_file_sha256: str
    generator_policy_canonical_sha256: str
    shared_contract_schema_sha256: str
    provider_id: str
    dataset_id: str
    source_checksum_role: Literal["RESULT_FILE_SHA256"]
    required_dq_status: Literal["PASS"]
    required_pit_status: Literal["PASS"]
    required_check_count: Literal[15]

    @field_validator("calibration_policy_path", "generator_policy_path")
    @classmethod
    def _paths(cls, value: str, info: ValidationInfo) -> str:
        return _relative_path(value, str(info.field_name))

    @field_validator(
        "calibration_policy_file_sha256",
        "calibration_policy_canonical_sha256",
        "generator_policy_file_sha256",
        "generator_policy_canonical_sha256",
        "shared_contract_schema_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("provider_id", "dataset_id")
    @classmethod
    def _ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))


class AdmissionSafety(_PolicyModel):
    owner_token_observed: Literal[False]
    authorization_status: Literal["OWNER_AUTHORIZATION_NOT_PROVIDED"]
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


class QCQQQOptionsCollectionEvidenceAdmissionPolicy(_PolicyModel):
    schema_version: Literal[
        "qc_qqq_options_primary_window_derived_aggregate_collection_evidence_admission_policy.v1"
    ]
    policy_id: str
    policy_version: Literal["1.0.0"]
    policy_status: Literal["ENGINEERING_BASELINE_OWNER_TOKEN_NOT_PROVIDED"]
    task_id: Literal[
        "TRADING-2514_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_COLLECTION_EVIDENCE_ADMISSION_V1"
    ]
    ordinary_pushed_main_sha: str
    proposal_package_root: str
    proposal_package_manifest_file_sha256: str
    proposal_package_manifest_content_sha256: str
    proposal_content_sha256: str
    run_scope_content_sha256: str
    project_code_lf_sha256: str
    proposal_policy_file_sha256: str
    proposal_policy_canonical_sha256: str
    collector_policy_file_sha256: str
    collector_policy_canonical_sha256: str
    transport_map_sha256: str
    expected_owner_decision_token: str
    token_decision_date: date
    authorization_expires_at_utc: datetime
    authorization_single_use: Literal[True]
    authorization_invalidates_after_evidence_collection: Literal[True]
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
    result_carrier: Literal["MANUAL_DOWNLOAD_RESULTS_JSON"]
    allowed_actions: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    dq_handoff: AdmissionDQHandoff
    safety: AdmissionSafety

    @field_validator("ordinary_pushed_main_sha")
    @classmethod
    def _main_sha(cls, value: str) -> str:
        return _git_sha(value, "ordinary_pushed_main_sha")

    @field_validator("proposal_package_root")
    @classmethod
    def _package_root(cls, value: str) -> str:
        return _relative_path(value, "proposal_package_root")

    @field_validator(
        "proposal_package_manifest_file_sha256",
        "proposal_package_manifest_content_sha256",
        "proposal_content_sha256",
        "run_scope_content_sha256",
        "project_code_lf_sha256",
        "proposal_policy_file_sha256",
        "proposal_policy_canonical_sha256",
        "collector_policy_file_sha256",
        "collector_policy_canonical_sha256",
        "transport_map_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("authorization_expires_at_utc")
    @classmethod
    def _expiry(cls, value: datetime) -> datetime:
        return _utc(value, "authorization_expires_at_utc")

    @field_validator("policy_id", "collector_id", "independent_reviewer_id")
    @classmethod
    def _ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("expected_owner_decision_token")
    @classmethod
    def _token(cls, value: str) -> str:
        return _required_text(value, "expected_owner_decision_token")

    @model_validator(mode="after")
    def _scope_and_actions(self) -> Self:
        if (self.requested_start, self.evaluated_start) != (
            date(2021, 2, 22),
            date(2021, 2, 22),
        ):
            raise ValueError("PRIMARY start must remain 2021-02-22")
        if (self.requested_end, self.evaluated_end) != (
            date(2025, 12, 2),
            date(2025, 12, 2),
        ):
            raise ValueError("reviewed evidence collection end must remain 2025-12-02")
        if self.allowed_actions != _ACTION_ORDER:
            raise ValueError("allowed action inventory or order drifted")
        if (
            len(self.prohibited_actions) != len(set(self.prohibited_actions))
            or tuple(sorted(self.prohibited_actions)) != self.prohibited_actions
        ):
            raise ValueError("prohibited action inventory must be unique and sorted")
        if self.authorization_expires_at_utc.date() <= self.token_decision_date:
            raise ValueError("authorization expiry must follow the decision date")
        return self

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


@dataclass(frozen=True)
class QCQQQOptionsCollectionEvidenceAdmissionPolicyLoadResult:
    policy: QCQQQOptionsCollectionEvidenceAdmissionPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str
    proposal_package: proposal_v1.BuiltQCQQQOptionsPrimaryWindowRunProposalPackage


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

    @model_validator(mode="after")
    def _seal(self, info: ValidationInfo) -> Self:
        if (
            info.context
            and info.context.get("allow_unsealed")
            and self.content_sha256 == (_UNSEALED_SHA256)
        ):
            return self
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("semantic content SHA-256 mismatch")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def seal(cls, **payload: object) -> Self:
        try:
            candidate = cls.model_validate(
                {**payload, "content_sha256": _UNSEALED_SHA256},
                context={"allow_unsealed": True},
            )
            return cls.model_validate(
                {**payload, "content_sha256": candidate.compute_content_sha256()}
            )
        except (TypeError, ValueError) as exc:
            raise QCQQQOptionsCollectionEvidenceAdmissionError(
                "COLLECTION_ADMISSION_PAYLOAD_INVALID", str(exc)
            ) from exc

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
            raise QCQQQOptionsCollectionEvidenceAdmissionError(
                "COLLECTION_ADMISSION_RECORD_INVALID", str(exc)
            ) from exc


class OwnerAuthorizationAdmissionReceipt(_SealedModel):
    schema_version: Literal["qc_qqq_options_owner_authorization_admission_receipt.v1"]
    admission_id: str
    admitted_at_utc: datetime
    admission_policy_file_sha256: str
    admission_policy_canonical_sha256: str
    owner_decision_token: str
    owner_decision_file_sha256: str
    owner_decision_content_sha256: str
    proposal_content_sha256: str
    run_scope_content_sha256: str
    collector_authorization_content_sha256: str
    authorized_at_utc: datetime
    expires_at_utc: datetime
    authorization_single_use: Literal[True]
    authorization_invalidates_after_evidence_collection: Literal[True]
    authorization_consumed: Literal[False]
    decision: Literal["OWNER_AUTHORIZATION_ADMITTED_UNUSED"]
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

    @field_validator("owner_decision_token")
    @classmethod
    def _token(cls, value: str) -> str:
        return _required_text(value, "owner_decision_token")

    @field_validator("admitted_at_utc", "authorized_at_utc", "expires_at_utc")
    @classmethod
    def _times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc(value, str(info.field_name))

    @field_validator(
        "admission_policy_file_sha256",
        "admission_policy_canonical_sha256",
        "owner_decision_file_sha256",
        "owner_decision_content_sha256",
        "proposal_content_sha256",
        "run_scope_content_sha256",
        "collector_authorization_content_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class CollectionExternalAction(_StrictModel):
    action_id: str
    ordinal: int = Field(ge=1, le=4)
    action_type: CollectionActionType
    occurred_at_utc: datetime
    status: CollectionActionStatus
    target_project_id: Literal[34808569]
    project_code_lf_sha256: str | None = None
    backtest_id: str | None = None
    result_file_sha256: str | None = None
    failure_reason_code: str | None = None

    @field_validator("action_id")
    @classmethod
    def _action_id(cls, value: str) -> str:
        return _identifier(value, "action_id")

    @field_validator("occurred_at_utc")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value, "occurred_at_utc")

    @field_validator("project_code_lf_sha256", "result_file_sha256")
    @classmethod
    def _optional_hash(cls, value: str | None, info: ValidationInfo) -> str | None:
        return None if value is None else _sha256(value, str(info.field_name))

    @field_validator("backtest_id", "failure_reason_code")
    @classmethod
    def _optional_id(cls, value: str | None, info: ValidationInfo) -> str | None:
        return None if value is None else _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _conditional_fields(self) -> Self:
        expected_type = CollectionActionType(_ACTION_ORDER[self.ordinal - 1])
        if self.action_type is not expected_type:
            raise ValueError("action type differs from reviewed ordinal")
        if self.status is CollectionActionStatus.FAILED:
            if self.failure_reason_code is None:
                raise ValueError("failed action requires failure_reason_code")
        elif self.failure_reason_code is not None:
            raise ValueError("completed action cannot carry failure_reason_code")
        if self.action_type is CollectionActionType.QUANTCONNECT_LOGIN:
            if any(
                value is not None
                for value in (
                    self.project_code_lf_sha256,
                    self.backtest_id,
                    self.result_file_sha256,
                )
            ):
                raise ValueError("login action cannot claim code, backtest, or result identity")
        elif self.action_type is CollectionActionType.MODIFY_EXISTING_DEDICATED_PROJECT_ONCE:
            if self.project_code_lf_sha256 is None or any(
                value is not None for value in (self.backtest_id, self.result_file_sha256)
            ):
                raise ValueError("project mutation requires only reviewed project code identity")
        elif self.action_type is CollectionActionType.RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST:
            if (
                self.project_code_lf_sha256 is None
                or self.backtest_id is None
                or self.result_file_sha256 is not None
            ):
                raise ValueError("cloud run requires code and backtest identity only")
        elif (
            self.project_code_lf_sha256 is None
            or self.backtest_id is None
            or self.result_file_sha256 is None
        ):
            raise ValueError("manual result collection requires code, backtest, and file identity")
        return self


class CollectionExternalActionLedger(_SealedModel):
    schema_version: Literal["qc_qqq_options_collection_external_action_ledger.v1"]
    ledger_id: str
    sealed_at_utc: datetime
    authorization_admission_content_sha256: str
    collector_authorization_content_sha256: str
    target_project_id: Literal[34808569]
    actions: tuple[CollectionExternalAction, ...]
    attempted_project_mutations: int = Field(ge=0, le=1)
    attempted_cloud_backtests: int = Field(ge=0, le=1)
    completed_result_downloads: int = Field(ge=0, le=1)
    lifecycle_status: Literal["COMPLETE", "INCOMPLETE", "FAILED"]
    scope_status: Literal["PASS", "FAIL"]
    reason_codes: tuple[str, ...]
    orders: Literal[0]
    fills: Literal[0]
    raw_option_rows_exported: Literal[False]
    api_used: Literal[False]
    cli_used: Literal[False]
    http_used: Literal[False]
    object_store_used: Literal[False]
    investment_interpretation_generated: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("ledger_id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _identifier(value, "ledger_id")

    @field_validator("sealed_at_utc")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value, "sealed_at_utc")

    @field_validator(
        "authorization_admission_content_sha256",
        "collector_authorization_content_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("reason_codes")
    @classmethod
    def _reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_identifier(item, "reason_code") for item in value)
        if checked != tuple(sorted(set(checked))):
            raise ValueError("reason codes must be unique and sorted")
        return checked

    @model_validator(mode="after")
    def _ledger_identity(self) -> Self:
        ordinals = tuple(item.ordinal for item in self.actions)
        if ordinals != tuple(range(1, len(self.actions) + 1)):
            raise ValueError("action ledger ordinals must be complete and ordered")
        if self.lifecycle_status == "COMPLETE" and (
            len(self.actions) != 4
            or any(item.status is not CollectionActionStatus.COMPLETED for item in self.actions)
            or self.scope_status != "PASS"
            or self.reason_codes
        ):
            raise ValueError("COMPLETE lifecycle must be exact successful four-action PASS")
        if self.lifecycle_status != "COMPLETE" and self.scope_status == "PASS":
            raise ValueError("non-complete lifecycle cannot report scope PASS")
        return self


class OwnerAuthorizationConsumptionReceipt(_SealedModel):
    schema_version: Literal["qc_qqq_options_owner_authorization_consumption_receipt.v1"]
    consumption_id: str
    consumed_at_utc: datetime
    authorization_admission_content_sha256: str
    collector_authorization_content_sha256: str
    external_action_ledger_content_sha256: str
    collector_evidence_content_sha256: str
    owner_decision_token: str
    authorization_single_use: Literal[True]
    authorization_consumed: Literal[True]
    authorization_invalidated_after_evidence_collection: Literal[True]
    decision: Literal["AUTHORIZATION_CONSUMED_AFTER_EVIDENCE_COLLECTION"]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("consumption_id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _identifier(value, "consumption_id")

    @field_validator("owner_decision_token")
    @classmethod
    def _token(cls, value: str) -> str:
        return _required_text(value, "owner_decision_token")

    @field_validator("consumed_at_utc")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value, "consumed_at_utc")

    @field_validator(
        "authorization_admission_content_sha256",
        "collector_authorization_content_sha256",
        "external_action_ledger_content_sha256",
        "collector_evidence_content_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class CollectionEvidenceAdmissionReceipt(_SealedModel):
    schema_version: Literal["qc_qqq_options_collection_evidence_admission_receipt.v1"]
    evidence_admission_id: str
    admitted_at_utc: datetime
    implementation_repository_code_sha: str
    admission_policy_file_sha256: str
    admission_policy_canonical_sha256: str
    authorization_consumption_content_sha256: str
    external_action_ledger_content_sha256: str
    collector_evidence_content_sha256: str
    collector_evidence_canonical_sha256: str
    result_file_sha256: str
    result_payload_sha256: str
    dq_report_file_sha256: str
    dq_report_content_sha256: str
    dq_report_canonical_sha256: str
    dq_record_id: str
    dq_lineage_id: str
    dq_passed_check_ids: tuple[str, ...]
    source_bundle_content_sha256: str
    source_bundle_canonical_sha256: str
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    session_count: Literal[1202]
    supported_slot_count: Literal[9]
    unsupported_slot_count: Literal[9]
    option_event_dq_status: Literal["NOT_EVALUATED"]
    local_derived_aggregate_dq_status: Literal["PASS"]
    local_derived_aggregate_pit_status: Literal["PASS"]
    decision: Literal["EVIDENCE_ADMITTED_DQ_PIT_PASS_POLICY_BLOCKED"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    raw_option_rows_exported: Literal[False]
    external_action_performed: Literal[True]
    investment_interpretation_generated: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("evidence_admission_id", "dq_record_id", "dq_lineage_id")
    @classmethod
    def _ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("admitted_at_utc")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value, "admitted_at_utc")

    @field_validator("implementation_repository_code_sha")
    @classmethod
    def _repository_sha(cls, value: str) -> str:
        return _git_sha(value, "implementation_repository_code_sha")

    @field_validator(
        "admission_policy_file_sha256",
        "admission_policy_canonical_sha256",
        "authorization_consumption_content_sha256",
        "external_action_ledger_content_sha256",
        "collector_evidence_content_sha256",
        "collector_evidence_canonical_sha256",
        "result_file_sha256",
        "result_payload_sha256",
        "dq_report_file_sha256",
        "dq_report_content_sha256",
        "dq_report_canonical_sha256",
        "source_bundle_content_sha256",
        "source_bundle_canonical_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("dq_passed_check_ids")
    @classmethod
    def _checks(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_identifier(item, "dq_check_id") for item in value)
        if len(checked) != 15 or len(checked) != len(set(checked)):
            raise ValueError("DQ receipt must bind exact 15 unique checks")
        return checked

    @model_validator(mode="after")
    def _scope(self) -> Self:
        if (self.requested_start, self.evaluated_start) != (
            date(2021, 2, 22),
            date(2021, 2, 22),
        ) or (self.requested_end, self.evaluated_end) != (
            date(2025, 12, 2),
            date(2025, 12, 2),
        ):
            raise ValueError("evidence receipt differs from exact PRIMARY range")
        return self


@dataclass(frozen=True)
class QCQQQOptionsCollectionEvidenceAdmissionBundle:
    policy_load: QCQQQOptionsCollectionEvidenceAdmissionPolicyLoadResult
    collector_authorization: collector_v1.QCQQQOptionsDerivedAggregateCollectorAuthorization
    authorization_admission: OwnerAuthorizationAdmissionReceipt
    external_action_ledger: CollectionExternalActionLedger
    collector_evidence: collector_v1.QCQQQOptionsDerivedAggregateCollectorEvidence
    authorization_consumption: OwnerAuthorizationConsumptionReceipt
    dq_report: DQReportRecord
    source_bundle: generator_v1.QQQOptionsPrimaryWindowDerivedObservationBundle
    evidence_admission: CollectionEvidenceAdmissionReceipt


def load_qc_qqq_options_collection_evidence_admission_policy(
    policy_path: Path = (
        DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_COLLECTION_EVIDENCE_ADMISSION_POLICY_PATH
    ),
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsCollectionEvidenceAdmissionPolicyLoadResult:
    root = project_root.resolve()
    try:
        path = _bound_file(policy_path, root=root, field="admission policy", must_exist=True)
        raw = path.read_bytes()
        payload = safe_load_yaml_path(path)
        policy = QCQQQOptionsCollectionEvidenceAdmissionPolicy.model_validate(payload)
        package = proposal_v1.load_qc_qqq_options_primary_window_run_proposal_package(
            package_root=root / policy.proposal_package_root,
            project_root=root,
        )
        manifest = package.manifest
        proposal = package.proposal
        scope = package.run_scope
        if (
            manifest.canonical_sha256 != policy.proposal_package_manifest_file_sha256
            or manifest.content_sha256 != policy.proposal_package_manifest_content_sha256
            or proposal.content_sha256 != policy.proposal_content_sha256
            or scope.content_sha256 != policy.run_scope_content_sha256
            or proposal.project_code_lf_sha256 != policy.project_code_lf_sha256
            or package.policy_load.policy_file_sha256 != policy.proposal_policy_file_sha256
            or package.policy_load.policy_canonical_sha256
            != policy.proposal_policy_canonical_sha256
            or proposal.collector_policy_file_sha256 != policy.collector_policy_file_sha256
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
            raise ValueError("2513 proposal authority differs from admission policy")
        calibration = load_qqq_options_primary_window_calibration_policy(project_root=root)
        dq = policy.dq_handoff
        if (
            calibration.policy_path.relative_to(root).as_posix() != dq.calibration_policy_path
            or calibration.policy_file_sha256 != dq.calibration_policy_file_sha256
            or calibration.policy_canonical_sha256 != dq.calibration_policy_canonical_sha256
            or QQQ_OPTIONS_CONTRACT_SHA256 != dq.shared_contract_schema_sha256
        ):
            raise ValueError("2510 DQ authority differs from admission policy")
        generator_path = _bound_file(
            Path(dq.generator_policy_path), root=root, field="generator policy", must_exist=True
        )
        generator_raw = generator_path.read_bytes()
        if hashlib.sha256(generator_raw).hexdigest() != dq.generator_policy_file_sha256:
            raise ValueError("2511 generator policy file identity differs")
        generator_payload = safe_load_yaml_path(generator_path)
        if hashlib.sha256(_canonical_json_bytes(generator_payload)).hexdigest() != (
            dq.generator_policy_canonical_sha256
        ):
            raise ValueError("2511 generator policy canonical identity differs")
    except QCQQQOptionsCollectionEvidenceAdmissionError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "COLLECTION_ADMISSION_POLICY_REJECTED", str(exc)
        ) from exc
    return QCQQQOptionsCollectionEvidenceAdmissionPolicyLoadResult(
        policy=policy,
        policy_path=path,
        policy_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy_canonical_sha256=policy.canonical_sha256,
        proposal_package=package,
    )


def _parse_owner_decision(
    raw: bytes,
    *,
    policy: QCQQQOptionsCollectionEvidenceAdmissionPolicy,
    proposal: collector_v1.QCQQQOptionsDerivedAggregateCollectorProposal,
) -> tuple[dict[str, str], str]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "OWNER_AUTHORIZATION_INVALID", "Owner decision is not UTF-8"
        ) from exc
    if "\r" in text or not text.endswith("\n") or text.endswith("\n\n"):
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "OWNER_AUTHORIZATION_INVALID",
            "Owner decision must be exact LF text with one final newline",
        )
    lines = text[:-1].split("\n")
    if not lines or lines[0] != policy.expected_owner_decision_token:
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "OWNER_AUTHORIZATION_NOT_PROVIDED", "exact 2513 Owner decision token was not supplied"
        )
    fields: dict[str, str] = {}
    observed_order: list[str] = []
    for line in lines[1:]:
        if line.count(":") < 1:
            raise QCQQQOptionsCollectionEvidenceAdmissionError(
                "OWNER_AUTHORIZATION_INVALID", "Owner decision line lacks key/value separator"
            )
        key, value = line.split(":", 1)
        if key in fields:
            raise QCQQQOptionsCollectionEvidenceAdmissionError(
                "OWNER_AUTHORIZATION_INVALID", f"duplicate Owner decision field: {key}"
            )
        if not _IDENTIFIER_PATTERN.fullmatch(key) or not value or value != value.strip():
            raise QCQQQOptionsCollectionEvidenceAdmissionError(
                "OWNER_AUTHORIZATION_INVALID", f"invalid Owner decision field: {key}"
            )
        fields[key] = value
        observed_order.append(key)
    if tuple(observed_order) != _OWNER_TOKEN_FIELD_ORDER:
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "OWNER_AUTHORIZATION_INVALID", "Owner decision field inventory/order drifted"
        )
    expected = {
        "ordinary_pushed_main_sha": policy.ordinary_pushed_main_sha,
        "repository_code_sha": proposal.run_scope.repository_code_sha,
        "proposal_content_sha256": policy.proposal_content_sha256,
        "run_scope_content_sha256": policy.run_scope_content_sha256,
        "project_code_lf_sha256": policy.project_code_lf_sha256,
        "proposal_policy_file_sha256": policy.proposal_policy_file_sha256,
        "proposal_policy_canonical_sha256": policy.proposal_policy_canonical_sha256,
        "collector_policy_file_sha256": policy.collector_policy_file_sha256,
        "collector_policy_canonical_sha256": policy.collector_policy_canonical_sha256,
        "transport_map_sha256": policy.transport_map_sha256,
        "target_project_id": str(policy.target_project_id),
        "requested_range": (
            f"{policy.requested_start.isoformat()}..{policy.requested_end.isoformat()}"
        ),
        "expected_session_count": str(policy.expected_session_count),
        "maximum_project_mutations": str(policy.maximum_project_mutations),
        "maximum_cloud_backtests": str(policy.maximum_cloud_backtests),
        "maximum_orders": str(policy.maximum_orders),
        "maximum_fills": str(policy.maximum_fills),
        "collector": policy.collector_id,
        "independent_reviewer": policy.independent_reviewer_id,
        "authorization_expires_at_utc": policy.authorization_expires_at_utc.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "authorization_single_use": "true",
        "authorization_invalidates_after_evidence_collection": "true",
    }
    if fields != expected:
        mismatches = tuple(sorted(key for key in expected if fields.get(key) != expected[key]))
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "OWNER_AUTHORIZATION_BINDING_MISMATCH",
            f"Owner decision fields differ from exact proposal: {mismatches}",
        )
    semantic_sha = hashlib.sha256(
        _canonical_json_bytes({"owner_decision_token": lines[0], **fields})
    ).hexdigest()
    return fields, semantic_sha


def admit_qc_qqq_options_primary_window_collection_owner_authorization(
    *,
    admission_id: str,
    admitted_at_utc: datetime,
    owner_decision_bytes: bytes,
    project_root: Path = PROJECT_ROOT,
) -> tuple[
    QCQQQOptionsCollectionEvidenceAdmissionPolicyLoadResult,
    collector_v1.QCQQQOptionsDerivedAggregateCollectorAuthorization,
    OwnerAuthorizationAdmissionReceipt,
]:
    loaded = load_qc_qqq_options_collection_evidence_admission_policy(project_root=project_root)
    policy = loaded.policy
    proposal = loaded.proposal_package.proposal
    admitted_at = _utc(admitted_at_utc, "admitted_at_utc")
    if admitted_at.date() < policy.token_decision_date:
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "OWNER_AUTHORIZATION_AS_OF_MISMATCH", "admission predates Owner decision date"
        )
    if admitted_at > policy.authorization_expires_at_utc:
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "OWNER_AUTHORIZATION_EXPIRED", "Owner authorization expired before admission"
        )
    _, semantic_sha = _parse_owner_decision(owner_decision_bytes, policy=policy, proposal=proposal)
    try:
        authorization = collector_v1.QCQQQOptionsDerivedAggregateCollectorAuthorization.seal(
            schema_version="qc_qqq_options_derived_aggregate_collector_authorization.v1",
            owner_decision_token=policy.expected_owner_decision_token,
            authorized_at_utc=admitted_at,
            expires_at_utc=policy.authorization_expires_at_utc,
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
    except (TypeError, ValueError) as exc:
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "OWNER_AUTHORIZATION_INVALID", str(exc)
        ) from exc
    receipt = OwnerAuthorizationAdmissionReceipt.seal(
        schema_version="qc_qqq_options_owner_authorization_admission_receipt.v1",
        admission_id=admission_id,
        admitted_at_utc=admitted_at,
        admission_policy_file_sha256=loaded.policy_file_sha256,
        admission_policy_canonical_sha256=loaded.policy_canonical_sha256,
        owner_decision_token=policy.expected_owner_decision_token,
        owner_decision_file_sha256=hashlib.sha256(owner_decision_bytes).hexdigest(),
        owner_decision_content_sha256=semantic_sha,
        proposal_content_sha256=proposal.content_sha256,
        run_scope_content_sha256=proposal.run_scope.content_sha256,
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
    return loaded, authorization, receipt


def build_qc_qqq_options_collection_external_action_ledger(
    *,
    ledger_id: str,
    sealed_at_utc: datetime,
    authorization: collector_v1.QCQQQOptionsDerivedAggregateCollectorAuthorization,
    authorization_admission: OwnerAuthorizationAdmissionReceipt,
    actions: tuple[CollectionExternalAction, ...],
) -> CollectionExternalActionLedger:
    sealed_at = _utc(sealed_at_utc, "sealed_at_utc")
    ordered = tuple(sorted(actions, key=lambda item: (item.ordinal, item.action_id)))
    if not ordered:
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "EXTERNAL_ACTION_LEDGER_EMPTY", "an evidence lifecycle requires recorded actions"
        )
    if len({item.ordinal for item in ordered}) != len(ordered):
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "EXTERNAL_ACTION_SCOPE_VIOLATION", "duplicate action ordinal"
        )
    if authorization_admission.collector_authorization_content_sha256 != (
        authorization.content_sha256
    ):
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "EXTERNAL_ACTION_AUTHORITY_MISMATCH", "authorization receipt differs from authorization"
        )
    previous_time = authorization.authorized_at_utc
    reasons: set[str] = set()
    for action in ordered:
        if action.occurred_at_utc < previous_time:
            reasons.add("ACTION_CHRONOLOGY_INVALID")
        previous_time = action.occurred_at_utc
        if action.occurred_at_utc > authorization.expires_at_utc:
            reasons.add("ACTION_AFTER_AUTHORIZATION_EXPIRY")
        if action.occurred_at_utc > sealed_at:
            reasons.add("ACTION_AFTER_LEDGER_SEAL")
        if action.target_project_id != authorization.target_project_id:
            reasons.add("ACTION_PROJECT_ID_MISMATCH")
        if action.project_code_lf_sha256 is not None and action.project_code_lf_sha256 != (
            authorization.project_code_lf_sha256
        ):
            reasons.add("ACTION_PROJECT_CODE_MISMATCH")
        if action.status is CollectionActionStatus.FAILED:
            reasons.add(action.failure_reason_code or "ACTION_FAILED")
    mutation_count = sum(
        item.action_type is CollectionActionType.MODIFY_EXISTING_DEDICATED_PROJECT_ONCE
        for item in ordered
    )
    run_count = sum(
        item.action_type is CollectionActionType.RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST
        for item in ordered
    )
    download_count = sum(
        item.action_type is CollectionActionType.EXPORT_SAFE_MANUAL_DOWNLOAD_RESULTS_COLLECTION
        and item.status is CollectionActionStatus.COMPLETED
        for item in ordered
    )
    if mutation_count > authorization.maximum_project_mutations:
        reasons.add("PROJECT_MUTATION_CAP_EXCEEDED")
    if run_count > authorization.maximum_cloud_backtests:
        reasons.add("CLOUD_BACKTEST_CAP_EXCEEDED")
    complete = (
        tuple(item.action_type.value for item in ordered) == _ACTION_ORDER
        and all(item.status is CollectionActionStatus.COMPLETED for item in ordered)
        and not reasons
    )
    if any(item.status is CollectionActionStatus.FAILED for item in ordered) or reasons:
        lifecycle_status = "FAILED"
    elif complete:
        lifecycle_status = "COMPLETE"
    else:
        lifecycle_status = "INCOMPLETE"
        reasons.add("EXTERNAL_ACTION_LIFECYCLE_INCOMPLETE")
    return CollectionExternalActionLedger.seal(
        schema_version="qc_qqq_options_collection_external_action_ledger.v1",
        ledger_id=ledger_id,
        sealed_at_utc=sealed_at,
        authorization_admission_content_sha256=authorization_admission.content_sha256,
        collector_authorization_content_sha256=authorization.content_sha256,
        target_project_id=authorization.target_project_id,
        actions=ordered,
        attempted_project_mutations=mutation_count,
        attempted_cloud_backtests=run_count,
        completed_result_downloads=download_count,
        lifecycle_status=lifecycle_status,
        scope_status="PASS" if complete else "FAIL",
        reason_codes=tuple(sorted(reasons)),
        orders=0,
        fills=0,
        raw_option_rows_exported=False,
        api_used=False,
        cli_used=False,
        http_used=False,
        object_store_used=False,
        investment_interpretation_generated=False,
        production_effect="none",
        broker_action="none",
    )


def _validate_dq_report(
    *,
    raw: bytes,
    dq_report_path: str,
    evidence_root: Path,
    collector_evidence: collector_v1.QCQQQOptionsDerivedAggregateCollectorEvidence,
    admitted_at_utc: datetime,
    project_root: Path,
    policy: QCQQQOptionsCollectionEvidenceAdmissionPolicy,
) -> tuple[DQReportRecord, str, str]:
    try:
        path = _bound_file(
            Path(dq_report_path), root=evidence_root, field="DQ report", must_exist=True
        )
        if path.read_bytes() != raw:
            raise ValueError("DQ report bytes differ from the reviewed path")
        report = DQReportRecord.from_json_bytes(raw)
        calibration = load_qqq_options_primary_window_calibration_policy(project_root=project_root)
        contract = calibration.policy.dq_contract
        if (
            report.scope != contract.scope
            or report.report_version != contract.report_version
            or report.policy_id != contract.policy_id
            or report.policy_version != contract.policy_version
            or report.policy_sha256 != contract.policy_sha256
            or report.contract_schema_sha256 != QQQ_OPTIONS_CONTRACT_SHA256
            or report.repository_code_sha != collector_evidence.repository_code_sha
        ):
            raise ValueError("DQ report authority identity mismatch")
        evidence_range = (
            collector_evidence.requested_start,
            collector_evidence.requested_end,
            collector_evidence.evaluated_start,
            collector_evidence.evaluated_end,
        )
        if (
            report.requested_start,
            report.requested_end,
            report.evaluated_start,
            report.evaluated_end,
        ) != evidence_range:
            raise ValueError("DQ report scope/range differs from collector evidence")
        if report.generated_at_utc < collector_evidence.collected_at_utc:
            raise ValueError("DQ report predates result collection")
        if report.generated_at_utc > admitted_at_utc or report.created_at_utc > admitted_at_utc:
            raise ValueError("DQ report is later than evidence admission")
        if (
            report.dq_status != policy.dq_handoff.required_dq_status
            or report.pit_status != policy.dq_handoff.required_pit_status
        ):
            raise ValueError("DQ/PIT status is not PASS")
        check_ids = tuple(item.check_id for item in report.checks)
        if (
            check_ids != contract.required_check_ids
            or len(check_ids) != policy.dq_handoff.required_check_count
            or any(item.status != "PASS" for item in report.checks)
        ):
            raise ValueError("DQ report is not exact canonical 15-check PASS")
        source_pair = (
            policy.dq_handoff.dataset_id,
            collector_evidence.result_file_sha256,
        )
        if source_pair not in tuple(zip(report.source_ids, report.source_checksums, strict=True)):
            raise ValueError("DQ report source identity/checksum differs from result artifact")
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "COLLECTION_DQ_PIT_REJECTED", str(exc)
        ) from exc
    return report, hashlib.sha256(raw).hexdigest(), report.content_sha256


def build_qc_qqq_options_primary_window_collection_evidence_admission(
    *,
    evidence_admission_id: str,
    authorization_admission_id: str,
    authorization_consumption_id: str,
    action_ledger_id: str,
    authorization_admitted_at_utc: datetime,
    admitted_at_utc: datetime,
    implementation_repository_code_sha: str,
    owner_decision_bytes: bytes,
    actions: tuple[CollectionExternalAction, ...],
    backtest_id: str,
    result_bytes: bytes,
    dq_report_path: str,
    dq_report_bytes: bytes,
    reviewed_project_code_lf_sha256: str,
    prior_consumption_receipts: tuple[OwnerAuthorizationConsumptionReceipt, ...] = (),
    project_root: Path = PROJECT_ROOT,
    evidence_root: Path | None = None,
) -> QCQQQOptionsCollectionEvidenceAdmissionBundle:
    admitted_at = _utc(admitted_at_utc, "admitted_at_utc")
    authorization_admitted_at = _utc(authorization_admitted_at_utc, "authorization_admitted_at_utc")
    if authorization_admitted_at > admitted_at:
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "OWNER_AUTHORIZATION_AS_OF_MISMATCH",
            "authorization admission cannot follow evidence admission",
        )
    implementation_sha = _git_sha(
        implementation_repository_code_sha, "implementation_repository_code_sha"
    )
    loaded, authorization, authorization_receipt = (
        admit_qc_qqq_options_primary_window_collection_owner_authorization(
            admission_id=authorization_admission_id,
            admitted_at_utc=authorization_admitted_at,
            owner_decision_bytes=owner_decision_bytes,
            project_root=project_root,
        )
    )
    if any(
        item.owner_decision_token == authorization.owner_decision_token
        or item.collector_authorization_content_sha256 == authorization.content_sha256
        for item in prior_consumption_receipts
    ):
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "OWNER_AUTHORIZATION_ALREADY_CONSUMED", "single-use authorization cannot be replayed"
        )
    ledger = build_qc_qqq_options_collection_external_action_ledger(
        ledger_id=action_ledger_id,
        sealed_at_utc=admitted_at,
        authorization=authorization,
        authorization_admission=authorization_receipt,
        actions=actions,
    )
    if ledger.lifecycle_status != "COMPLETE" or ledger.scope_status != "PASS":
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "EXTERNAL_ACTION_SCOPE_VIOLATION",
            f"external action lifecycle is {ledger.lifecycle_status}/{ledger.scope_status}",
        )
    result_sha = hashlib.sha256(result_bytes).hexdigest()
    final_action = ledger.actions[-1]
    if final_action.backtest_id != backtest_id or final_action.result_file_sha256 != result_sha:
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "RESULT_ACTION_IDENTITY_MISMATCH",
            "manual result action does not bind the admitted backtest/result bytes",
        )
    try:
        collector_evidence = (
            collector_v1.build_qc_qqq_options_primary_window_derived_aggregate_collector_evidence(
                evidence_id=f"{evidence_admission_id}:collector",
                collected_at_utc=final_action.occurred_at_utc,
                backtest_id=backtest_id,
                result_bytes=result_bytes,
                proposal=loaded.proposal_package.proposal,
                authorization=authorization,
                reviewed_target_project_id=loaded.policy.target_project_id,
                reviewed_project_code_lf_sha256=reviewed_project_code_lf_sha256,
                project_root=project_root,
            )
        )
    except (TypeError, ValueError) as exc:
        raise QCQQQOptionsCollectionEvidenceAdmissionError(
            "COLLECTOR_RESULT_ADMISSION_REJECTED", str(exc)
        ) from exc
    consumption = OwnerAuthorizationConsumptionReceipt.seal(
        schema_version="qc_qqq_options_owner_authorization_consumption_receipt.v1",
        consumption_id=authorization_consumption_id,
        consumed_at_utc=admitted_at,
        authorization_admission_content_sha256=authorization_receipt.content_sha256,
        collector_authorization_content_sha256=authorization.content_sha256,
        external_action_ledger_content_sha256=ledger.content_sha256,
        collector_evidence_content_sha256=collector_evidence.content_sha256,
        owner_decision_token=authorization.owner_decision_token,
        authorization_single_use=True,
        authorization_consumed=True,
        authorization_invalidated_after_evidence_collection=True,
        decision="AUTHORIZATION_CONSUMED_AFTER_EVIDENCE_COLLECTION",
        production_effect="none",
        broker_action="none",
    )
    root = (evidence_root or project_root).resolve()
    dq_report, dq_file_sha, dq_content_sha = _validate_dq_report(
        raw=dq_report_bytes,
        dq_report_path=dq_report_path,
        evidence_root=root,
        collector_evidence=collector_evidence,
        admitted_at_utc=admitted_at,
        project_root=project_root,
        policy=loaded.policy,
    )
    source_bundle = generator_v1.build_qqq_options_primary_window_derived_observation_bundle(
        bundle_id=f"{evidence_admission_id}:source-bundle",
        created_at_utc=admitted_at,
        repository_code_sha=collector_evidence.repository_code_sha,
        requested_start=collector_evidence.requested_start,
        requested_end=collector_evidence.requested_end,
        evaluated_start=collector_evidence.evaluated_start,
        evaluated_end=collector_evidence.evaluated_end,
        provider_id=loaded.policy.dq_handoff.provider_id,
        dataset_id=loaded.policy.dq_handoff.dataset_id,
        source_checksum=collector_evidence.result_file_sha256,
        dq_report_path=dq_report_path,
        dq_report_file_sha256=dq_file_sha,
        dq_report_content_sha256=dq_content_sha,
        observations=collector_evidence.observations,
    )
    receipt = CollectionEvidenceAdmissionReceipt.seal(
        schema_version="qc_qqq_options_collection_evidence_admission_receipt.v1",
        evidence_admission_id=evidence_admission_id,
        admitted_at_utc=admitted_at,
        implementation_repository_code_sha=implementation_sha,
        admission_policy_file_sha256=loaded.policy_file_sha256,
        admission_policy_canonical_sha256=loaded.policy_canonical_sha256,
        authorization_consumption_content_sha256=consumption.content_sha256,
        external_action_ledger_content_sha256=ledger.content_sha256,
        collector_evidence_content_sha256=collector_evidence.content_sha256,
        collector_evidence_canonical_sha256=collector_evidence.canonical_sha256,
        result_file_sha256=collector_evidence.result_file_sha256,
        result_payload_sha256=collector_evidence.result_payload_sha256,
        dq_report_file_sha256=dq_file_sha,
        dq_report_content_sha256=dq_content_sha,
        dq_report_canonical_sha256=hashlib.sha256(dq_report.canonical_bytes).hexdigest(),
        dq_record_id=dq_report.record_id,
        dq_lineage_id=dq_report.lineage_id,
        dq_passed_check_ids=tuple(item.check_id for item in dq_report.checks),
        source_bundle_content_sha256=source_bundle.content_sha256,
        source_bundle_canonical_sha256=source_bundle.canonical_sha256,
        requested_start=collector_evidence.requested_start,
        requested_end=collector_evidence.requested_end,
        evaluated_start=collector_evidence.evaluated_start,
        evaluated_end=collector_evidence.evaluated_end,
        session_count=len(collector_evidence.session_ids),
        supported_slot_count=len(collector_evidence.supported_slot_ids),
        unsupported_slot_count=len(collector_evidence.unsupported_slots),
        option_event_dq_status="NOT_EVALUATED",
        local_derived_aggregate_dq_status="PASS",
        local_derived_aggregate_pit_status="PASS",
        decision="EVIDENCE_ADMITTED_DQ_PIT_PASS_POLICY_BLOCKED",
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        orders=0,
        fills=0,
        raw_option_rows_exported=False,
        external_action_performed=True,
        investment_interpretation_generated=False,
        production_effect="none",
        broker_action="none",
    )
    return QCQQQOptionsCollectionEvidenceAdmissionBundle(
        policy_load=loaded,
        collector_authorization=authorization,
        authorization_admission=authorization_receipt,
        external_action_ledger=ledger,
        collector_evidence=collector_evidence,
        authorization_consumption=consumption,
        dq_report=dq_report,
        source_bundle=source_bundle,
        evidence_admission=receipt,
    )


__all__ = [
    "DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_COLLECTION_EVIDENCE_ADMISSION_POLICY_PATH",
    "AdmissionDQHandoff",
    "AdmissionSafety",
    "CollectionActionStatus",
    "CollectionActionType",
    "CollectionEvidenceAdmissionReceipt",
    "CollectionExternalAction",
    "CollectionExternalActionLedger",
    "OwnerAuthorizationAdmissionReceipt",
    "OwnerAuthorizationConsumptionReceipt",
    "QCQQQOptionsCollectionEvidenceAdmissionBundle",
    "QCQQQOptionsCollectionEvidenceAdmissionError",
    "QCQQQOptionsCollectionEvidenceAdmissionPolicy",
    "QCQQQOptionsCollectionEvidenceAdmissionPolicyLoadResult",
    "admit_qc_qqq_options_primary_window_collection_owner_authorization",
    "build_qc_qqq_options_collection_external_action_ledger",
    "build_qc_qqq_options_primary_window_collection_evidence_admission",
    "load_qc_qqq_options_collection_evidence_admission_policy",
]
