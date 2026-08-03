from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, ClassVar, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.cross_layer_validation import (
    build_qqq_options_cloud_smoke_checklist,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_POLICY_PATH = Path(
    "config/research/qc_qqq_options_bounded_free_cloud_pilot_v1.yaml"
)

_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_NOT_GRANTED_TOKEN = "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"

_AUTHORITY_IDS: tuple[str, ...] = tuple(
    f"TRADING-{task}-{kind}"
    for task in range(2480, 2491)
    for kind in ("MODULE", "POLICY")
) + (
    "TRADING-2491-GOLDEN",
    "TRADING-2491-MODULE",
    "TRADING-2491-POLICY",
)
_SCOPE_FIELD_IDS: tuple[str, ...] = (
    "CLOUD_COMPUTE_BUDGET",
    "FEE_POLICY",
    "MAXIMUM_CONTRACT_QUANTITY",
    "MAXIMUM_ORDER_COUNT",
    "MAXIMUM_RUNTIME",
    "PARTIAL_FILL_POLICY",
    "REALITY_LATENCY_POLICY",
    "REALITY_SLIPPAGE_POLICY",
    "RECONCILIATION_TOLERANCE",
    "REQUESTED_END",
    "REQUESTED_START",
    "RESOURCE_TELEMETRY_LIMIT",
)
_EVIDENCE_ROLE_IDS: tuple[str, ...] = (
    "CAPABILITY_RECEIPT",
    "CROSS_LAYER_CHECKLIST",
    "ENGINE_PROJECT_IDENTITY",
    "LOCAL_RECONCILIATION",
    "MANUAL_EVIDENCE_BUNDLE",
    "OPTION_EVENT_DQ_PIT",
    "PILOT_RUN_RECEIPT",
    "RESOURCE_TELEMETRY",
    "REVIEWER_ATTESTATION",
    "RUN_RANGE",
)
_READINESS_ITEM_IDS: tuple[str, ...] = (
    "CAPABILITY_RECEIPT_CONFIRMED",
    "CROSS_LAYER_CHECKLIST_FACT_DERIVED",
    "ENGINE_PROJECT_IDENTITY_BOUND",
    "EXTERNAL_OWNER_TOKEN_GRANTED",
    "LOCAL_RECONCILIATION_EXPLAINED",
    "MANUAL_BUNDLE_COMPLETE",
    "NO_PROHIBITED_ACTION_CONFIRMED",
    "OPTION_EVENT_DQ_PIT_REVIEWED",
    "PILOT_SCOPE_OWNER_REVIEWED",
    "RESOURCE_TELEMETRY_COMPLETE",
    "RUN_RANGE_CONFIRMED",
    "TWO_PERSON_ATTESTATION_COMPLETE",
)
_BLOCKING_REASON_CODES: tuple[str, ...] = (
    "OWNER_AUTHORIZATION_NOT_GRANTED",
    "OWNER_REVIEWED_PILOT_SCOPE_NOT_GRANTED",
)
_ALLOWED_DISPOSITIONS: tuple[str, ...] = (
    "BOUNDED_PILOT_ACCEPTED_FOR_RANGE_EXPANSION",
    "PILOT_NO_GO_LICENSE_OR_EVIDENCE",
    "PILOT_REQUIRES_PAID_TIER",
)


class QQQOptionsBoundedCloudPilotContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _required_text(value: str, field_name: str) -> str:
    if not value or value.strip() != value or any(ord(char) < 32 for char in value):
        raise ValueError(f"{field_name} must be normalized non-empty text")
    return value


def _identifier(value: str, field_name: str) -> str:
    value = _required_text(value, field_name)
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be a portable identifier")
    return value


def _sha256(value: str, field_name: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be lowercase SHA-256")
    return value


def _git_sha(value: str, field_name: str) -> str:
    if not _GIT_SHA_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be lowercase 40-character Git SHA")
    return value


def _utc(value: datetime, field_name: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    normalized = value.astimezone(UTC)
    if normalized > datetime.now(UTC):
        raise ValueError(f"{field_name} cannot be in the future")
    return normalized


def _canonical_json_bytes(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _canonical_sha256(payload: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _lf_sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


class _SealedModel(_StrictModel):
    content_sha256: str
    _HASH_FIELD: ClassVar[str] = "content_sha256"

    @field_validator("content_sha256")
    @classmethod
    def _validate_content_hash(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    def _semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={self._HASH_FIELD})

    def _expected_content_sha256(self) -> str:
        return _canonical_sha256(self._semantic_payload())

    @model_validator(mode="after")
    def _validate_seal(self) -> Self:
        if self.content_sha256 not in {_UNSEALED_SHA256, self._expected_content_sha256()}:
            raise ValueError("content_sha256 does not match canonical semantic payload")
        return self

    @classmethod
    def seal(cls, **payload: object) -> Self:
        candidate = cls.model_validate(
            {**payload, cls._HASH_FIELD: _UNSEALED_SHA256},
            strict=True,
        )
        return candidate.model_copy(
            update={cls._HASH_FIELD: candidate._expected_content_sha256()}
        )

    def canonical_bytes(self) -> bytes:
        if self.content_sha256 == _UNSEALED_SHA256:
            raise QQQOptionsBoundedCloudPilotContractError(
                "BOUNDED_CLOUD_PILOT_RECORD_UNSEALED",
                self.__class__.__name__,
            )
        return _canonical_json_bytes(self.model_dump(mode="json"))

    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            value = cls.model_validate_json(content, strict=True)
        except ValueError as exc:
            raise QQQOptionsBoundedCloudPilotContractError(
                "BOUNDED_CLOUD_PILOT_RECORD_INVALID",
                f"{cls.__name__}: {exc}",
            ) from exc
        if value.content_sha256 == _UNSEALED_SHA256:
            raise QQQOptionsBoundedCloudPilotContractError(
                "BOUNDED_CLOUD_PILOT_RECORD_UNSEALED",
                cls.__name__,
            )
        if value.canonical_bytes() != content:
            raise QQQOptionsBoundedCloudPilotContractError(
                "BOUNDED_CLOUD_PILOT_RECORD_NONCANONICAL",
                cls.__name__,
            )
        return value


class QQQOptionsBoundedPilotAuthorityBinding(_PolicyModel):
    authority_id: str
    path: str
    sha256: str

    @field_validator("authority_id")
    @classmethod
    def _validate_authority_id(cls, value: str) -> str:
        return _identifier(value, "authority_id")

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        value = _required_text(value, "path")
        path = Path(value)
        if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
            raise ValueError("authority path must be portable and repository-relative")
        return value

    @field_validator("sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "sha256")


class QQQOptionsBoundedPilotScopeFieldPolicy(_PolicyModel):
    field_id: str
    status: Literal["UNKNOWN_REQUIRES_OWNER_REVIEW"]
    owner: str
    exit_condition: str

    @field_validator("field_id")
    @classmethod
    def _validate_field_id(cls, value: str) -> str:
        return _identifier(value, "field_id")

    @field_validator("owner", "exit_condition")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))


class QQQOptionsBoundedPilotEvidenceRequirement(_PolicyModel):
    evidence_role: str
    source_task: str
    current_status: Literal["NOT_EVALUATED_NO_AUTHORIZED_PILOT"]
    required_for_disposition: Literal[True]

    @field_validator("evidence_role", "source_task")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))


class QQQOptionsBoundedPilotReadinessItemPolicy(_PolicyModel):
    item_id: str
    source_task: str
    owner: str
    current_status: Literal["BLOCKED"]
    evidence_status: Literal["NOT_EVALUATED"]

    @field_validator("item_id", "source_task")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("owner")
    @classmethod
    def _validate_owner(cls, value: str) -> str:
        return _required_text(value, "owner")


class QQQOptionsBoundedPilotSafety(_PolicyModel):
    research_only: Literal[True]
    external_platform_action_allowed: Literal[False]
    cloud_run_authorized: Literal[False]
    api_allowed: Literal[False]
    cli_allowed: Literal[False]
    remote_http_allowed: Literal[False]
    object_store_allowed: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    synthetic_pass_may_authorize_pilot: Literal[False]
    caller_token_may_authorize_pilot: Literal[False]
    investment_interpretation_allowed: Literal[False]
    range_expansion_allowed: Literal[False]
    paper_shadow_allowed: Literal[False]
    production_allowed: Literal[False]
    broker_action_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QQQOptionsBoundedCloudPilotPolicy(_PolicyModel):
    schema_version: Literal["qc_qqq_options_bounded_cloud_pilot_policy.v1"]
    policy_id: Literal["qc_qqq_options_bounded_free_cloud_pilot_v1"]
    policy_version: str
    status: Literal["BLOCKED_OWNER_INPUT"]
    owner: str
    owner_authorization_token: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    pilot_role: Literal["BOUNDED_PLATFORM_SMOKE_NOT_RESEARCH_CONCLUSION"]
    primary_research_start: date
    legacy_non_default_start: date
    legacy_non_default_start_is_default: Literal[False]
    authority_bindings: tuple[QQQOptionsBoundedPilotAuthorityBinding, ...]
    scope_fields: tuple[QQQOptionsBoundedPilotScopeFieldPolicy, ...]
    required_evidence: tuple[QQQOptionsBoundedPilotEvidenceRequirement, ...]
    readiness_items: tuple[QQQOptionsBoundedPilotReadinessItemPolicy, ...]
    blocking_reason_codes: tuple[str, ...]
    allowed_final_dispositions: tuple[str, ...]
    safety: QQQOptionsBoundedPilotSafety
    decision: Literal["PILOT_PREREGISTRATION_READY_OWNER_BLOCKED"]

    @field_validator(
        "policy_version",
        "owner",
        "rationale",
        "intended_effect",
        "validation_plan",
        "review_condition",
        "expiry_condition",
    )
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_inventory(self) -> Self:
        if self.primary_research_start != date(2021, 2, 22):
            raise ValueError("primary research start must remain 2021-02-22")
        if self.legacy_non_default_start != date(2022, 12, 1):
            raise ValueError("legacy non-default marker must remain 2022-12-01")
        authority_ids = tuple(item.authority_id for item in self.authority_bindings)
        if authority_ids != _AUTHORITY_IDS or len(authority_ids) != len(set(authority_ids)):
            raise ValueError("authority inventory must remain complete, sorted, and unique")
        scope_ids = tuple(item.field_id for item in self.scope_fields)
        if scope_ids != _SCOPE_FIELD_IDS:
            raise ValueError("pilot scope field inventory must remain exact and sorted")
        evidence_ids = tuple(item.evidence_role for item in self.required_evidence)
        if evidence_ids != _EVIDENCE_ROLE_IDS:
            raise ValueError("required evidence inventory must remain exact and sorted")
        readiness_ids = tuple(item.item_id for item in self.readiness_items)
        if readiness_ids != _READINESS_ITEM_IDS:
            raise ValueError("readiness inventory must remain exact and sorted")
        if self.blocking_reason_codes != _BLOCKING_REASON_CODES:
            raise ValueError("blocking reasons must remain exact")
        if self.allowed_final_dispositions != _ALLOWED_DISPOSITIONS:
            raise ValueError("final disposition inventory must remain exact")
        return self


@dataclass(frozen=True)
class QQQOptionsBoundedCloudPilotPolicyLoadResult:
    policy: QQQOptionsBoundedCloudPilotPolicy
    policy_path: Path
    policy_sha256: str
    authority_set_sha256: str


class QQQOptionsBoundedPilotScopeFieldState(_SealedModel):
    field_id: str
    status: Literal["UNKNOWN_REQUIRES_OWNER_REVIEW"]
    value: Literal["NOT_GRANTED"]
    owner: str
    exit_condition: str

    @field_validator("field_id")
    @classmethod
    def _validate_field_id(cls, value: str) -> str:
        return _identifier(value, "field_id")

    @field_validator("owner", "exit_condition")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))


class QQQOptionsBoundedPilotReadinessItemState(_SealedModel):
    item_id: str
    source_task: str
    owner: str
    status: Literal["BLOCKED"]
    evidence_status: Literal["NOT_EVALUATED"]

    @field_validator("item_id", "source_task")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("owner")
    @classmethod
    def _validate_owner(cls, value: str) -> str:
        return _required_text(value, "owner")


class QQQOptionsBoundedCloudPilotPreregistration(_SealedModel):
    schema_version: Literal["qc_qqq_options_bounded_cloud_pilot_preregistration.v1"]
    preregistration_id: str
    created_at_utc: datetime
    repository_code_sha: str
    policy_sha256: str
    authority_set_sha256: str
    cross_layer_checklist_sha256: str
    owner_authorization_token: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    pilot_role: Literal["BOUNDED_PLATFORM_SMOKE_NOT_RESEARCH_CONCLUSION"]
    primary_research_start: date
    legacy_non_default_start: date
    legacy_non_default_start_is_default: Literal[False]
    scope_fields: tuple[QQQOptionsBoundedPilotScopeFieldState, ...]
    readiness_items: tuple[QQQOptionsBoundedPilotReadinessItemState, ...]
    required_evidence_roles: tuple[str, ...]
    blocking_reason_codes: tuple[str, ...]
    status: Literal["BLOCKED_OWNER_AUTHORIZATION_AND_SCOPE"]
    final_disposition_status: Literal["NOT_EVALUATED_NO_AUTHORIZED_PILOT"]
    cash_preservation_required: Literal[True]
    order_creation_allowed: Literal[False]
    fill_creation_allowed: Literal[False]
    external_action_executed: Literal[False]
    pilot_authorized: Literal[False]
    range_expansion_allowed: Literal[False]
    safety: QQQOptionsBoundedPilotSafety

    @field_validator("preregistration_id")
    @classmethod
    def _validate_id(cls, value: str) -> str:
        return _identifier(value, "preregistration_id")

    @field_validator("created_at_utc")
    @classmethod
    def _validate_created_at(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_code_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "policy_sha256",
        "authority_set_sha256",
        "cross_layer_checklist_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_blocked_inventory(self) -> Self:
        if tuple(item.field_id for item in self.scope_fields) != _SCOPE_FIELD_IDS:
            raise ValueError("preregistration scope inventory mismatch")
        if tuple(item.item_id for item in self.readiness_items) != _READINESS_ITEM_IDS:
            raise ValueError("preregistration readiness inventory mismatch")
        if self.required_evidence_roles != _EVIDENCE_ROLE_IDS:
            raise ValueError("preregistration evidence inventory mismatch")
        if self.blocking_reason_codes != _BLOCKING_REASON_CODES:
            raise ValueError("preregistration blocker inventory mismatch")
        if self.primary_research_start != date(2021, 2, 22):
            raise ValueError("preregistration primary research start mismatch")
        if self.legacy_non_default_start != date(2022, 12, 1):
            raise ValueError("preregistration legacy marker mismatch")
        return self


class QQQOptionsBoundedCloudPilotReadinessReport(_SealedModel):
    schema_version: Literal["qc_qqq_options_bounded_cloud_pilot_readiness_report.v1"]
    report_id: str
    evaluated_at_utc: datetime
    preregistration_sha256: str
    policy_sha256: str
    readiness_items: tuple[QQQOptionsBoundedPilotReadinessItemState, ...]
    blocking_reason_codes: tuple[str, ...]
    status: Literal["BLOCKED_OWNER_AUTHORIZATION_AND_SCOPE"]
    external_evidence_status: Literal["NOT_EVALUATED_NO_AUTHORIZED_PILOT"]
    cash_preservation_required: Literal[True]
    order_count: Literal[0]
    fill_count: Literal[0]
    pilot_authorized: Literal[False]
    external_action_executed: Literal[False]
    range_expansion_allowed: Literal[False]

    @field_validator("report_id")
    @classmethod
    def _validate_id(cls, value: str) -> str:
        return _identifier(value, "report_id")

    @field_validator("evaluated_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "evaluated_at_utc")

    @field_validator("preregistration_sha256", "policy_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_inventory(self) -> Self:
        if tuple(item.item_id for item in self.readiness_items) != _READINESS_ITEM_IDS:
            raise ValueError("readiness report inventory mismatch")
        if self.blocking_reason_codes != _BLOCKING_REASON_CODES:
            raise ValueError("readiness report blockers mismatch")
        return self


def load_qc_qqq_options_bounded_cloud_pilot_policy(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsBoundedCloudPilotPolicyLoadResult:
    resolved = path if path.is_absolute() else project_root / path
    try:
        _assert_regular_non_symlink_file(resolved, root=None)
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = QQQOptionsBoundedCloudPilotPolicy.model_validate(payload, strict=False)
        _verify_authority_bindings(policy, project_root=project_root)
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsBoundedCloudPilotContractError(
            "BOUNDED_CLOUD_PILOT_POLICY_INVALID",
            f"{resolved}: {exc}",
        ) from exc
    authority_set_sha256 = _canonical_sha256(
        [item.model_dump(mode="json") for item in policy.authority_bindings]
    )
    return QQQOptionsBoundedCloudPilotPolicyLoadResult(
        policy=policy,
        policy_path=resolved,
        policy_sha256=_lf_sha256_path(resolved),
        authority_set_sha256=authority_set_sha256,
    )


def build_qc_qqq_options_bounded_cloud_pilot_preregistration(
    *,
    preregistration_id: str,
    created_at_utc: datetime,
    repository_code_sha: str,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsBoundedCloudPilotPreregistration:
    loaded = load_qc_qqq_options_bounded_cloud_pilot_policy(
        policy_path,
        project_root=project_root,
    )
    checklist = build_qqq_options_cloud_smoke_checklist(
        checklist_id=f"{preregistration_id}-inherited-checklist",
        created_at_utc=created_at_utc,
        project_root=project_root,
    )
    scope_fields = tuple(
        QQQOptionsBoundedPilotScopeFieldState.seal(
            field_id=item.field_id,
            status=item.status,
            value="NOT_GRANTED",
            owner=item.owner,
            exit_condition=item.exit_condition,
        )
        for item in loaded.policy.scope_fields
    )
    readiness_items = tuple(
        QQQOptionsBoundedPilotReadinessItemState.seal(
            item_id=item.item_id,
            source_task=item.source_task,
            owner=item.owner,
            status=item.current_status,
            evidence_status=item.evidence_status,
        )
        for item in loaded.policy.readiness_items
    )
    return QQQOptionsBoundedCloudPilotPreregistration.seal(
        schema_version="qc_qqq_options_bounded_cloud_pilot_preregistration.v1",
        preregistration_id=preregistration_id,
        created_at_utc=created_at_utc,
        repository_code_sha=repository_code_sha,
        policy_sha256=loaded.policy_sha256,
        authority_set_sha256=loaded.authority_set_sha256,
        cross_layer_checklist_sha256=checklist.canonical_sha256(),
        owner_authorization_token=_NOT_GRANTED_TOKEN,
        pilot_role=loaded.policy.pilot_role,
        primary_research_start=loaded.policy.primary_research_start,
        legacy_non_default_start=loaded.policy.legacy_non_default_start,
        legacy_non_default_start_is_default=False,
        scope_fields=scope_fields,
        readiness_items=readiness_items,
        required_evidence_roles=tuple(
            item.evidence_role for item in loaded.policy.required_evidence
        ),
        blocking_reason_codes=loaded.policy.blocking_reason_codes,
        status="BLOCKED_OWNER_AUTHORIZATION_AND_SCOPE",
        final_disposition_status="NOT_EVALUATED_NO_AUTHORIZED_PILOT",
        cash_preservation_required=True,
        order_creation_allowed=False,
        fill_creation_allowed=False,
        external_action_executed=False,
        pilot_authorized=False,
        range_expansion_allowed=False,
        safety=loaded.policy.safety,
    )


def evaluate_qc_qqq_options_bounded_cloud_pilot_readiness(
    *,
    report_id: str,
    evaluated_at_utc: datetime,
    preregistration_bytes: bytes,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsBoundedCloudPilotReadinessReport:
    preregistration = QQQOptionsBoundedCloudPilotPreregistration.from_json_bytes(
        preregistration_bytes
    )
    loaded = load_qc_qqq_options_bounded_cloud_pilot_policy(
        policy_path,
        project_root=project_root,
    )
    if preregistration.policy_sha256 != loaded.policy_sha256:
        raise QQQOptionsBoundedCloudPilotContractError(
            "BOUNDED_CLOUD_PILOT_POLICY_BINDING_MISMATCH",
            "preregistration policy hash differs from current policy",
        )
    if preregistration.authority_set_sha256 != loaded.authority_set_sha256:
        raise QQQOptionsBoundedCloudPilotContractError(
            "BOUNDED_CLOUD_PILOT_AUTHORITY_BINDING_MISMATCH",
            "preregistration authority set differs from current policy",
        )
    return QQQOptionsBoundedCloudPilotReadinessReport.seal(
        schema_version="qc_qqq_options_bounded_cloud_pilot_readiness_report.v1",
        report_id=report_id,
        evaluated_at_utc=evaluated_at_utc,
        preregistration_sha256=preregistration.canonical_sha256(),
        policy_sha256=loaded.policy_sha256,
        readiness_items=preregistration.readiness_items,
        blocking_reason_codes=preregistration.blocking_reason_codes,
        status="BLOCKED_OWNER_AUTHORIZATION_AND_SCOPE",
        external_evidence_status="NOT_EVALUATED_NO_AUTHORIZED_PILOT",
        cash_preservation_required=True,
        order_count=0,
        fill_count=0,
        pilot_authorized=False,
        external_action_executed=False,
        range_expansion_allowed=False,
    )


def _assert_regular_non_symlink_file(path: Path, *, root: Path | None) -> None:
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"required regular non-symlink file missing: {path}")
    if root is None:
        return
    resolved_root = root.resolve()
    resolved_path = path.resolve()
    if not resolved_path.is_relative_to(resolved_root):
        raise ValueError(f"authority path escapes project root: {path}")
    current = resolved_root
    for part in path.relative_to(root).parts:
        current /= part
        if current.is_symlink():
            raise ValueError(f"authority path contains symlink: {path}")


def _verify_authority_bindings(
    policy: QQQOptionsBoundedCloudPilotPolicy,
    *,
    project_root: Path,
) -> None:
    for binding in policy.authority_bindings:
        path = project_root / binding.path
        _assert_regular_non_symlink_file(path, root=project_root)
        if _lf_sha256_path(path) != binding.sha256:
            raise ValueError(f"authority hash drifted: {binding.authority_id}")


__all__ = [
    "DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_POLICY_PATH",
    "QQQOptionsBoundedCloudPilotContractError",
    "QQQOptionsBoundedCloudPilotPolicy",
    "QQQOptionsBoundedCloudPilotPolicyLoadResult",
    "QQQOptionsBoundedCloudPilotPreregistration",
    "QQQOptionsBoundedCloudPilotReadinessReport",
    "QQQOptionsBoundedPilotAuthorityBinding",
    "QQQOptionsBoundedPilotEvidenceRequirement",
    "QQQOptionsBoundedPilotReadinessItemPolicy",
    "QQQOptionsBoundedPilotReadinessItemState",
    "QQQOptionsBoundedPilotSafety",
    "QQQOptionsBoundedPilotScopeFieldPolicy",
    "QQQOptionsBoundedPilotScopeFieldState",
    "build_qc_qqq_options_bounded_cloud_pilot_preregistration",
    "evaluate_qc_qqq_options_bounded_cloud_pilot_readiness",
    "load_qc_qqq_options_bounded_cloud_pilot_policy",
]
