from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import sha256_path
from ai_trading_system.qqq_options_research.bounded_cloud_pilot_platform_action import (
    QCBoundedCloudPilotExecutionEvidenceRecord,
    QCBoundedCloudPilotIndependentReviewRecord,
    QCBoundedCloudPilotIndependentReviewRequestRecord,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_POLICY_PATH = Path(
    "config/research/qc_qqq_options_owner_stage_gate_signoff_v1.yaml"
)
DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_PROPOSAL_PATH = Path(
    "inputs/external_validation/qc_qqq_options_owner_stage_gate_proposal_20260806.json"
)
DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_OWNER_ATTESTATION_PATH = Path(
    "inputs/external_validation/qc_qqq_options_owner_stage_gate_owner_attestation_20260806.json"
)
OWNER_STAGE_GATE_DECISION_ID = (
    "owner_decision:TRADING-2493:2026-08-06:accept_no_go_keep_blocked_stage_gate_v1"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")


class OwnerStageGateDecision(StrEnum):
    GO = "GO"
    CONDITIONAL_GO = "CONDITIONAL_GO"
    NO_GO = "NO_GO"


class OwnerStageGateAxis(StrEnum):
    PLATFORM_CAPABILITY = "PLATFORM_CAPABILITY"
    TECHNICAL_CORRECTNESS = "TECHNICAL_CORRECTNESS"
    LICENSE_EXPORT = "LICENSE_EXPORT"
    DQ_PIT = "DQ_PIT"
    RESOURCE_BUDGET = "RESOURCE_BUDGET"
    SHARED_RECONCILIATION = "SHARED_RECONCILIATION"
    RANGE_EXPANSION = "RANGE_EXPANSION"
    PAID_TIER_UPGRADE = "PAID_TIER_UPGRADE"


EXPECTED_AXIS_DECISIONS: tuple[tuple[OwnerStageGateAxis, OwnerStageGateDecision], ...] = (
    (OwnerStageGateAxis.PLATFORM_CAPABILITY, OwnerStageGateDecision.CONDITIONAL_GO),
    (OwnerStageGateAxis.TECHNICAL_CORRECTNESS, OwnerStageGateDecision.CONDITIONAL_GO),
    (OwnerStageGateAxis.LICENSE_EXPORT, OwnerStageGateDecision.NO_GO),
    (OwnerStageGateAxis.DQ_PIT, OwnerStageGateDecision.NO_GO),
    (OwnerStageGateAxis.RESOURCE_BUDGET, OwnerStageGateDecision.NO_GO),
    (OwnerStageGateAxis.SHARED_RECONCILIATION, OwnerStageGateDecision.NO_GO),
    (OwnerStageGateAxis.RANGE_EXPANSION, OwnerStageGateDecision.NO_GO),
    (OwnerStageGateAxis.PAID_TIER_UPGRADE, OwnerStageGateDecision.NO_GO),
)
EXPECTED_UNKNOWN_IDS: tuple[str, ...] = (
    "FREE_TIER_HISTORICAL_ENTITLEMENT",
    "TRADING_2489_MANUAL_BUNDLE_COLLECTION",
    "TRADING_2490_RECONCILIATION_POLICY",
    "RESOURCE_CAP_CALIBRATION",
    "PRIMARY_RESEARCH_WINDOW_VIABILITY",
)
EXPECTED_AUTHORITY_IDS: tuple[str, ...] = (
    "TRADING_2492_PLATFORM_ACTION_MODULE",
    "TRADING_2492_PLATFORM_AUTHORIZATION_POLICY",
    "TRADING_2492_EXECUTION_EVIDENCE",
    "TRADING_2492_REVIEW_REQUEST",
    "TRADING_2492_OWNER_REVIEW",
)


class QCQQQOptionsOwnerStageGateContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, use_enum_values=False)


def _required_text(value: str, field: str) -> str:
    if not value or value != value.strip() or not _TEXT_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be non-empty normalized text")
    return value


def _identifier(value: str, field: str) -> str:
    checked = _required_text(value, field)
    if not _IDENTIFIER_PATTERN.fullmatch(checked):
        raise ValueError(f"{field} must be a portable identifier")
    return checked


def _sha256(value: str, field: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be lowercase SHA-256")
    return value


def _git_sha(value: str, field: str) -> str:
    if not _GIT_SHA_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase 40-character Git SHA")
    return value


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _canonical_sha256(payload: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


class _SealedModel(_StrictModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _validate_content_sha256(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        semantic = cls.model_validate({**payload, "content_sha256": "0" * 64}).semantic_payload()
        return cls.model_validate({**payload, "content_sha256": _canonical_sha256(semantic)})

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            decoded = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("record is not valid UTF-8 JSON") from exc
        if not isinstance(decoded, dict):
            raise ValueError("record JSON root must be an object")
        record = cls.model_validate(decoded)
        if record.canonical_bytes != raw:
            raise ValueError("record bytes are not canonical")
        if record.content_sha256 != _canonical_sha256(record.semantic_payload()):
            raise ValueError("record semantic content SHA-256 mismatch")
        return record


class QCQQQOptionsOwnerStageGateAuthorityBinding(_PolicyModel):
    authority_id: str
    relative_path: str
    sha256: str
    schema_version: str

    @field_validator("authority_id", "schema_version")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("relative_path")
    @classmethod
    def _validate_relative_path(cls, value: str) -> str:
        checked = _required_text(value, "relative_path")
        path = Path(checked)
        if path.is_absolute() or ".." in path.parts or path.as_posix() != checked:
            raise ValueError("relative_path must be a normalized project-relative path")
        return checked

    @field_validator("sha256")
    @classmethod
    def _validate_sha(cls, value: str) -> str:
        return _sha256(value, "sha256")


class QCQQQOptionsOwnerStageGateAxisPolicy(_PolicyModel):
    axis_id: OwnerStageGateAxis
    decision: OwnerStageGateDecision
    reason_code: str
    summary: str
    evidence_refs: tuple[str, ...]

    @field_validator("reason_code")
    @classmethod
    def _validate_reason_code(cls, value: str) -> str:
        return _identifier(value, "reason_code")

    @field_validator("summary")
    @classmethod
    def _validate_summary(cls, value: str) -> str:
        return _required_text(value, "summary")

    @field_validator("evidence_refs")
    @classmethod
    def _validate_evidence_refs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(set(value)) != len(value):
            raise ValueError("evidence_refs must be non-empty and unique")
        return tuple(_identifier(item, "evidence_ref") for item in value)


class QCQQQOptionsOwnerStageGateUnknownPolicy(_PolicyModel):
    unknown_id: str
    owner: str
    exit_condition: str

    @field_validator("unknown_id")
    @classmethod
    def _validate_unknown_id(cls, value: str) -> str:
        return _identifier(value, "unknown_id")

    @field_validator("owner", "exit_condition")
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))


class QCQQQOptionsOwnerStageGateSafety(_PolicyModel):
    range_expansion_allowed: Literal[False]
    further_cloud_action_authorized: Literal[False]
    paid_tier_upgrade_authorized: Literal[False]
    investment_interpretation_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_allowed: Literal[False]
    short_options_allowed: Literal[False]
    roll_allowed: Literal[False]
    multi_leg_allowed: Literal[False]
    leaps_allowed: Literal[False]
    wheel_allowed: Literal[False]
    broker_action: Literal["none"]


class QCQQQOptionsOwnerStageGatePolicy(_PolicyModel):
    schema_version: Literal["qc_qqq_options_owner_stage_gate_policy.v1"]
    policy_id: str
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_REVIEW_REQUIRED"]
    effective_date: date
    predecessor_task_id: Literal["TRADING-2492_QC_QQQ_OPTIONS_BOUNDED_FREE_CLOUD_PILOT_V1"]
    owner_decision_id: Literal[
        "owner_decision:TRADING-2493:2026-08-06:accept_no_go_keep_blocked_stage_gate_v1"
    ]
    collector_id: Literal["codex_pilot_coordinator"]
    signer_id: Literal["project_owner"]
    independent_reviewer_id: Literal["project_owner"]
    authority_bindings: tuple[QCQQQOptionsOwnerStageGateAuthorityBinding, ...]
    axis_policies: tuple[QCQQQOptionsOwnerStageGateAxisPolicy, ...]
    unknown_policies: tuple[QCQQQOptionsOwnerStageGateUnknownPolicy, ...]
    aggregate_recommendation: Literal["NO_GO_KEEP_BLOCKED"]
    safety: QCQQQOptionsOwnerStageGateSafety

    @field_validator("policy_id")
    @classmethod
    def _validate_policy_id(cls, value: str) -> str:
        return _identifier(value, "policy_id")

    @model_validator(mode="after")
    def _validate_inventory(self) -> Self:
        authority_ids = tuple(item.authority_id for item in self.authority_bindings)
        if authority_ids != EXPECTED_AUTHORITY_IDS or len(set(authority_ids)) != len(authority_ids):
            raise ValueError("authority binding inventory must be exact and ordered")
        axis_pairs = tuple((item.axis_id, item.decision) for item in self.axis_policies)
        if axis_pairs != EXPECTED_AXIS_DECISIONS:
            raise ValueError("axis policy inventory or decision drifted")
        unknown_ids = tuple(item.unknown_id for item in self.unknown_policies)
        if unknown_ids != EXPECTED_UNKNOWN_IDS:
            raise ValueError("unknown policy inventory must be exact and ordered")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))


@dataclass(frozen=True)
class QCQQQOptionsOwnerStageGatePolicyLoadResult:
    policy: QCQQQOptionsOwnerStageGatePolicy
    policy_path: Path
    policy_sha256: str
    policy_canonical_sha256: str
    authority_set_sha256: str


class QCQQQOptionsOwnerStageGateAxisDecision(_StrictModel):
    axis_id: OwnerStageGateAxis
    decision: OwnerStageGateDecision
    reason_code: str
    summary: str
    evidence_refs: tuple[str, ...]
    observed_facts: tuple[str, ...]

    @field_validator("reason_code")
    @classmethod
    def _validate_reason_code(cls, value: str) -> str:
        return _identifier(value, "reason_code")

    @field_validator("summary")
    @classmethod
    def _validate_summary(cls, value: str) -> str:
        return _required_text(value, "summary")

    @field_validator("evidence_refs", "observed_facts")
    @classmethod
    def _validate_tuples(cls, value: tuple[str, ...], info: Any) -> tuple[str, ...]:
        if not value or len(set(value)) != len(value):
            raise ValueError(f"{info.field_name} must be non-empty and unique")
        return tuple(_required_text(item, str(info.field_name)) for item in value)


class QCQQQOptionsOwnerStageGateUnknown(_StrictModel):
    unknown_id: str
    status: Literal["UNKNOWN_BLOCKS_GO"]
    owner: str
    exit_condition: str

    @field_validator("unknown_id")
    @classmethod
    def _validate_unknown_id(cls, value: str) -> str:
        return _identifier(value, "unknown_id")

    @field_validator("owner", "exit_condition")
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))


class QCQQQOptionsOwnerStageGateProposalRecord(_SealedModel):
    schema_version: Literal["qc_qqq_options_owner_stage_gate_proposal.v1"]
    record_id: str
    created_at_utc: datetime
    repository_code_sha: str
    policy_id: str
    policy_version: Literal["1.0.0"]
    policy_file_sha256: str
    policy_canonical_sha256: str
    authority_set_sha256: str
    predecessor_owner_review_file_sha256: Literal[
        "3857b5fe52725ff1dfd7d101dda6e27ff3c2b1d89e28505d3a3d52c2bb9c1913"
    ]
    predecessor_execution_evidence_file_sha256: Literal[
        "2e57bfec7119daa05f89e1a48d8e06d7ca5fda6b38846e8f3d985c3ccdc6293c"
    ]
    predecessor_review_request_file_sha256: Literal[
        "94d7aef27daab59fa5dcacf82e993086bdda57fa177520d6d370f90a75d1794f"
    ]
    result_artifact_sha256: Literal[
        "fdd11ab6ce0791cc3ebd952269f670ba65a1b9747e663628ae462b52ff166ead"
    ]
    project_id: Literal["34808569"]
    backtest_id: Literal["6e70793600035ddc3d7f856319a352db"]
    source_disposition: Literal["PILOT_NO_GO_LICENSE_OR_EVIDENCE"]
    source_authorization_state: Literal["INVALIDATED_AFTER_EVIDENCE_COLLECTION_AND_SCOPE_VIOLATION"]
    confirmed_processed_data_points: Literal[734127]
    confirmed_reviewed_cap: Literal[250000]
    confirmed_scope_violation: Literal[True]
    confirmed_no_raw_option_rows: Literal[True]
    confirmed_shared_2489_2490_blocked: Literal[True]
    option_event_dq_status: Literal["PASS_PLATFORM_LOG_ONLY"]
    option_event_pit_status: Literal["PASS_PLATFORM_LOG_ONLY"]
    axis_decisions: tuple[QCQQQOptionsOwnerStageGateAxisDecision, ...]
    unknowns: tuple[QCQQQOptionsOwnerStageGateUnknown, ...]
    aggregate_recommendation: Literal["NO_GO_KEEP_BLOCKED"]
    owner_signoff_status: Literal["PENDING_OWNER_SIGNATURE"]
    owner_signoff_completed: Literal[False]
    terminal_stage_gate_issued: Literal[False]
    safety: QCQQQOptionsOwnerStageGateSafety

    @field_validator("record_id", "policy_id")
    @classmethod
    def _validate_identifier(cls, value: str, info: Any) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("created_at_utc")
    @classmethod
    def _validate_created_at(cls, value: datetime) -> datetime:
        checked = _utc(value, "created_at_utc")
        if checked.date() < date(2026, 8, 6):
            raise ValueError("proposal predates the stage-gate task")
        return checked

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "policy_file_sha256",
        "policy_canonical_sha256",
        "authority_set_sha256",
    )
    @classmethod
    def _validate_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_proposal(self) -> Self:
        axis_pairs = tuple((item.axis_id, item.decision) for item in self.axis_decisions)
        if axis_pairs != EXPECTED_AXIS_DECISIONS:
            raise ValueError("proposal axis decisions drifted")
        unknown_ids = tuple(item.unknown_id for item in self.unknowns)
        if unknown_ids != EXPECTED_UNKNOWN_IDS:
            raise ValueError("proposal unknown inventory drifted")
        if self.confirmed_processed_data_points <= self.confirmed_reviewed_cap:
            raise ValueError("proposal must preserve the observed resource cap breach")
        return self


class QCQQQOptionsOwnerStageGateOwnerAttestationRecord(_SealedModel):
    schema_version: Literal["qc_qqq_options_owner_stage_gate_owner_attestation.v1"]
    record_id: str
    owner_decision_id: Literal[
        "owner_decision:TRADING-2493:2026-08-06:accept_no_go_keep_blocked_stage_gate_v1"
    ]
    decision_date: date
    proposal_path: Literal[
        "inputs/external_validation/qc_qqq_options_owner_stage_gate_proposal_20260806.json"
    ]
    proposal_file_sha256: str
    proposal_content_sha256: str
    policy_file_sha256: str
    policy_canonical_sha256: str
    signer_id: Literal["project_owner"]
    independent_reviewer_id: Literal["project_owner"]
    accepted_axis_decisions: tuple[str, ...]
    accepted_aggregate_recommendation: Literal["NO_GO_KEEP_BLOCKED"]
    confirmed_scope_violation: Literal[True]
    confirmed_shared_2489_2490_blocked: Literal[True]
    confirmed_no_range_expansion: Literal[True]
    confirmed_no_further_cloud_action: Literal[True]
    confirmed_no_paid_upgrade_authorization: Literal[True]
    confirmed_no_investment_interpretation: Literal[True]
    confirmed_no_external_action: Literal[True]

    @field_validator("record_id")
    @classmethod
    def _validate_record_id(cls, value: str) -> str:
        return _identifier(value, "record_id")

    @field_validator(
        "proposal_file_sha256",
        "proposal_content_sha256",
        "policy_file_sha256",
        "policy_canonical_sha256",
    )
    @classmethod
    def _validate_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_attestation(self) -> Self:
        if self.decision_date != date(2026, 8, 6):
            raise ValueError("Owner decision date drifted")
        expected = tuple(
            f"{axis.value}={decision.value}" for axis, decision in EXPECTED_AXIS_DECISIONS
        )
        if self.accepted_axis_decisions != expected:
            raise ValueError("Owner attestation axis decisions drifted")
        return self


class QCQQQOptionsOwnerStageGateSignoffRecord(_SealedModel):
    schema_version: Literal["qc_qqq_options_owner_stage_gate_signoff.v1"]
    record_id: str
    issued_at_utc: datetime
    repository_code_sha: str
    owner_decision_id: Literal[
        "owner_decision:TRADING-2493:2026-08-06:accept_no_go_keep_blocked_stage_gate_v1"
    ]
    proposal_file_sha256: str
    proposal_content_sha256: str
    owner_attestation_file_sha256: str
    owner_attestation_content_sha256: str
    policy_file_sha256: str
    policy_canonical_sha256: str
    project_id: Literal["34808569"]
    backtest_id: Literal["6e70793600035ddc3d7f856319a352db"]
    axis_decisions: tuple[QCQQQOptionsOwnerStageGateAxisDecision, ...]
    unknowns: tuple[QCQQQOptionsOwnerStageGateUnknown, ...]
    aggregate_decision: Literal["NO_GO_KEEP_BLOCKED"]
    signoff_status: Literal["SIGNED_NO_GO"]
    source_pilot_disposition: Literal["PILOT_NO_GO_LICENSE_OR_EVIDENCE"]
    safety: QCQQQOptionsOwnerStageGateSafety

    @field_validator("record_id")
    @classmethod
    def _validate_record_id(cls, value: str) -> str:
        return _identifier(value, "record_id")

    @field_validator("issued_at_utc")
    @classmethod
    def _validate_issued_at(cls, value: datetime) -> datetime:
        return _utc(value, "issued_at_utc")

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "proposal_file_sha256",
        "proposal_content_sha256",
        "owner_attestation_file_sha256",
        "owner_attestation_content_sha256",
        "policy_file_sha256",
        "policy_canonical_sha256",
    )
    @classmethod
    def _validate_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_signoff(self) -> Self:
        axis_pairs = tuple((item.axis_id, item.decision) for item in self.axis_decisions)
        if axis_pairs != EXPECTED_AXIS_DECISIONS:
            raise ValueError("signed axis decisions drifted")
        if tuple(item.unknown_id for item in self.unknowns) != EXPECTED_UNKNOWN_IDS:
            raise ValueError("signed unknown inventory drifted")
        return self


@dataclass(frozen=True)
class _PredecessorFacts:
    evidence: QCBoundedCloudPilotExecutionEvidenceRecord
    review_request: QCBoundedCloudPilotIndependentReviewRequestRecord
    owner_review: QCBoundedCloudPilotIndependentReviewRecord
    evidence_sha256: str
    review_request_sha256: str
    owner_review_sha256: str


def _require_bound_regular_file(path: Path, *, project_root: Path, field: str) -> Path:
    root = project_root.resolve()
    candidate = path if path.is_absolute() else root / path
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes the project root") from exc
    if ".." in relative.parts:
        raise ValueError(f"{field} escapes the project root")
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"{field} cannot use a symlink")
    if not candidate.is_file():
        raise ValueError(f"{field} must be a regular file")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} resolves outside the project root") from exc
    if not resolved.is_file():
        raise ValueError(f"{field} must be a regular file")
    return resolved


def _binding_map(
    policy: QCQQQOptionsOwnerStageGatePolicy,
) -> dict[str, QCQQQOptionsOwnerStageGateAuthorityBinding]:
    return {binding.authority_id: binding for binding in policy.authority_bindings}


def load_qc_qqq_options_owner_stage_gate_policy(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsOwnerStageGatePolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _require_bound_regular_file(
            path, project_root=root, field="stage-gate policy"
        )
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("stage-gate policy root must be a mapping")
        policy = QCQQQOptionsOwnerStageGatePolicy.model_validate(payload)
        authority_payload: list[dict[str, str]] = []
        for binding in policy.authority_bindings:
            bound_path = _require_bound_regular_file(
                Path(binding.relative_path),
                project_root=root,
                field=f"authority {binding.authority_id}",
            )
            actual = sha256_path(bound_path)
            if actual != binding.sha256:
                raise ValueError(
                    f"authority {binding.authority_id} SHA-256 mismatch: expected "
                    f"{binding.sha256}, observed {actual}"
                )
            authority_payload.append(binding.model_dump(mode="json"))
    except QCQQQOptionsOwnerStageGateContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsOwnerStageGateContractError(
            "QC_QQQ_OPTIONS_OWNER_STAGE_GATE_POLICY_INVALID", str(exc)
        ) from exc
    return QCQQQOptionsOwnerStageGatePolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_sha256=sha256_path(policy_path),
        policy_canonical_sha256=policy.canonical_sha256,
        authority_set_sha256=_canonical_sha256(authority_payload),
    )


def _load_predecessor_facts(
    *,
    loaded: QCQQQOptionsOwnerStageGatePolicyLoadResult,
    project_root: Path,
) -> _PredecessorFacts:
    bindings = _binding_map(loaded.policy)

    def read(authority_id: str) -> tuple[bytes, str]:
        binding = bindings[authority_id]
        path = _require_bound_regular_file(
            Path(binding.relative_path), project_root=project_root, field=authority_id
        )
        raw = path.read_bytes()
        return raw, hashlib.sha256(raw).hexdigest()

    evidence_raw, evidence_sha = read("TRADING_2492_EXECUTION_EVIDENCE")
    request_raw, request_sha = read("TRADING_2492_REVIEW_REQUEST")
    review_raw, review_sha = read("TRADING_2492_OWNER_REVIEW")
    try:
        evidence = QCBoundedCloudPilotExecutionEvidenceRecord.from_json_bytes(evidence_raw)
        request = QCBoundedCloudPilotIndependentReviewRequestRecord.from_json_bytes(request_raw)
        review = QCBoundedCloudPilotIndependentReviewRecord.from_json_bytes(review_raw)
    except ValueError as exc:
        raise QCQQQOptionsOwnerStageGateContractError(
            "QC_QQQ_OPTIONS_OWNER_STAGE_GATE_PREDECESSOR_INVALID", str(exc)
        ) from exc
    if request.evidence_record_sha256 != evidence_sha:
        raise QCQQQOptionsOwnerStageGateContractError(
            "QC_QQQ_OPTIONS_OWNER_STAGE_GATE_LINEAGE_MISMATCH",
            "review request does not bind execution evidence",
        )
    if review.evidence_record_sha256 != evidence_sha or review.review_request_sha256 != request_sha:
        raise QCQQQOptionsOwnerStageGateContractError(
            "QC_QQQ_OPTIONS_OWNER_STAGE_GATE_LINEAGE_MISMATCH",
            "Owner review does not bind predecessor records",
        )
    if not (
        evidence.result_artifact_sha256
        == request.result_artifact_sha256
        == review.result_artifact_sha256
    ):
        raise QCQQQOptionsOwnerStageGateContractError(
            "QC_QQQ_OPTIONS_OWNER_STAGE_GATE_RESULT_IDENTITY_MISMATCH",
            "result artifact identity drifted",
        )
    if not (
        evidence.project_id == request.project_id == review.project_id
        and evidence.backtest_id == request.backtest_id == review.backtest_id
    ):
        raise QCQQQOptionsOwnerStageGateContractError(
            "QC_QQQ_OPTIONS_OWNER_STAGE_GATE_RUN_IDENTITY_MISMATCH",
            "project or backtest identity drifted",
        )
    return _PredecessorFacts(
        evidence=evidence,
        review_request=request,
        owner_review=review,
        evidence_sha256=evidence_sha,
        review_request_sha256=request_sha,
        owner_review_sha256=review_sha,
    )


def _observed_facts(axis: OwnerStageGateAxis, facts: _PredecessorFacts) -> tuple[str, ...]:
    evidence = facts.evidence
    review = facts.owner_review
    values: dict[OwnerStageGateAxis, tuple[str, ...]] = {
        OwnerStageGateAxis.PLATFORM_CAPABILITY: (
            f"account_tier={evidence.account_tier}",
            f"cloud_compute={evidence.cloud_compute}",
            "one_simulated_long_call_fill_observed=true",
            "historical_entitlement_complete=unknown",
        ),
        OwnerStageGateAxis.TECHNICAL_CORRECTNESS: (
            "intent_submit_fill_independent_minutes=true",
            "one_order_one_fill=true",
            f"source_disposition={review.disposition}",
        ),
        OwnerStageGateAxis.LICENSE_EXPORT: (
            f"prior_capability_admission={evidence.prior_capability_admission}",
            f"raw_options_rows_present={str(evidence.raw_options_rows_present).lower()}",
            "license_entitlement_for_range_expansion=unknown",
        ),
        OwnerStageGateAxis.DQ_PIT: (
            f"option_event_dq_status={evidence.option_event_dq_status}",
            f"option_event_pit_status={evidence.option_event_pit_status}",
            "shared_lifecycle_dq_pit_complete=false",
        ),
        OwnerStageGateAxis.RESOURCE_BUDGET: (
            f"processed_data_points={evidence.processed_data_points}",
            f"reviewed_cap={evidence.maximum_processed_data_points}",
            "scope_violation=PROCESSED_DATA_POINTS",
        ),
        OwnerStageGateAxis.SHARED_RECONCILIATION: (
            f"shared_2489={evidence.shared_2489_bundle_status}",
            f"shared_2490={evidence.shared_2490_reconciliation_status}",
        ),
        OwnerStageGateAxis.RANGE_EXPANSION: (
            f"range_expansion_allowed={str(review.range_expansion_allowed).lower()}",
            f"further_cloud_action_authorized={str(review.further_cloud_action_authorized).lower()}",
        ),
        OwnerStageGateAxis.PAID_TIER_UPGRADE: (
            f"source_disposition={review.disposition}",
            "paid_tier_value_of_information_reviewed=false",
            "paid_tier_upgrade_authorized=false",
        ),
    }
    return values[axis]


def build_qc_qqq_options_owner_stage_gate_proposal(
    *,
    record_id: str,
    created_at_utc: datetime,
    repository_code_sha: str,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsOwnerStageGateProposalRecord:
    loaded = load_qc_qqq_options_owner_stage_gate_policy(policy_path, project_root=project_root)
    facts = _load_predecessor_facts(loaded=loaded, project_root=project_root.resolve())
    evidence = facts.evidence
    review = facts.owner_review
    if not (
        review.confirmed_scope_violation
        and review.confirmed_processed_data_points > review.confirmed_reviewed_cap
        and review.confirmed_shared_2489_2490_blocked
        and review.disposition == "PILOT_NO_GO_LICENSE_OR_EVIDENCE"
        and not review.range_expansion_allowed
        and not review.further_cloud_action_authorized
    ):
        raise QCQQQOptionsOwnerStageGateContractError(
            "QC_QQQ_OPTIONS_OWNER_STAGE_GATE_NO_GO_FACTS_REQUIRED",
            "predecessor review does not preserve the terminal NO-GO facts",
        )
    axes = tuple(
        QCQQQOptionsOwnerStageGateAxisDecision(
            axis_id=policy_axis.axis_id,
            decision=policy_axis.decision,
            reason_code=policy_axis.reason_code,
            summary=policy_axis.summary,
            evidence_refs=policy_axis.evidence_refs,
            observed_facts=_observed_facts(policy_axis.axis_id, facts),
        )
        for policy_axis in loaded.policy.axis_policies
    )
    unknowns = tuple(
        QCQQQOptionsOwnerStageGateUnknown(
            unknown_id=item.unknown_id,
            status="UNKNOWN_BLOCKS_GO",
            owner=item.owner,
            exit_condition=item.exit_condition,
        )
        for item in loaded.policy.unknown_policies
    )
    return QCQQQOptionsOwnerStageGateProposalRecord.seal(
        schema_version="qc_qqq_options_owner_stage_gate_proposal.v1",
        record_id=record_id,
        created_at_utc=created_at_utc,
        repository_code_sha=repository_code_sha,
        policy_id=loaded.policy.policy_id,
        policy_version=loaded.policy.policy_version,
        policy_file_sha256=loaded.policy_sha256,
        policy_canonical_sha256=loaded.policy_canonical_sha256,
        authority_set_sha256=loaded.authority_set_sha256,
        predecessor_owner_review_file_sha256=facts.owner_review_sha256,
        predecessor_execution_evidence_file_sha256=facts.evidence_sha256,
        predecessor_review_request_file_sha256=facts.review_request_sha256,
        result_artifact_sha256=review.result_artifact_sha256,
        project_id=review.project_id,
        backtest_id=review.backtest_id,
        source_disposition=review.disposition,
        source_authorization_state=review.authorization_state,
        confirmed_processed_data_points=review.confirmed_processed_data_points,
        confirmed_reviewed_cap=review.confirmed_reviewed_cap,
        confirmed_scope_violation=review.confirmed_scope_violation,
        confirmed_no_raw_option_rows=review.confirmed_no_raw_option_rows,
        confirmed_shared_2489_2490_blocked=review.confirmed_shared_2489_2490_blocked,
        option_event_dq_status=evidence.option_event_dq_status,
        option_event_pit_status=evidence.option_event_pit_status,
        axis_decisions=axes,
        unknowns=unknowns,
        aggregate_recommendation="NO_GO_KEEP_BLOCKED",
        owner_signoff_status="PENDING_OWNER_SIGNATURE",
        owner_signoff_completed=False,
        terminal_stage_gate_issued=False,
        safety=loaded.policy.safety,
    )


def build_qc_qqq_options_owner_stage_gate_signoff(
    *,
    record_id: str,
    issued_at_utc: datetime,
    repository_code_sha: str,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_POLICY_PATH,
    proposal_path: Path = DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_PROPOSAL_PATH,
    owner_attestation_path: Path = (DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_OWNER_ATTESTATION_PATH),
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsOwnerStageGateSignoffRecord:
    root = project_root.resolve()
    loaded = load_qc_qqq_options_owner_stage_gate_policy(policy_path, project_root=root)
    try:
        proposal_file = _require_bound_regular_file(
            proposal_path, project_root=root, field="stage-gate proposal"
        )
        attestation_file = _require_bound_regular_file(
            owner_attestation_path, project_root=root, field="Owner stage-gate attestation"
        )
        proposal_raw = proposal_file.read_bytes()
        attestation_raw = attestation_file.read_bytes()
        proposal = QCQQQOptionsOwnerStageGateProposalRecord.from_json_bytes(proposal_raw)
        attestation = QCQQQOptionsOwnerStageGateOwnerAttestationRecord.from_json_bytes(
            attestation_raw
        )
    except (OSError, ValueError) as exc:
        raise QCQQQOptionsOwnerStageGateContractError(
            "QC_QQQ_OPTIONS_OWNER_STAGE_GATE_ATTESTATION_REQUIRED", str(exc)
        ) from exc
    proposal_sha = hashlib.sha256(proposal_raw).hexdigest()
    attestation_sha = hashlib.sha256(attestation_raw).hexdigest()
    if not (
        attestation.proposal_file_sha256 == proposal_sha
        and attestation.proposal_content_sha256 == proposal.content_sha256
        and attestation.policy_file_sha256 == loaded.policy_sha256
        and attestation.policy_canonical_sha256 == loaded.policy_canonical_sha256
        and proposal.policy_file_sha256 == loaded.policy_sha256
        and proposal.policy_canonical_sha256 == loaded.policy_canonical_sha256
    ):
        raise QCQQQOptionsOwnerStageGateContractError(
            "QC_QQQ_OPTIONS_OWNER_STAGE_GATE_ATTESTATION_BINDING_MISMATCH",
            "Owner attestation does not exact-bind proposal and policy",
        )
    return QCQQQOptionsOwnerStageGateSignoffRecord.seal(
        schema_version="qc_qqq_options_owner_stage_gate_signoff.v1",
        record_id=record_id,
        issued_at_utc=issued_at_utc,
        repository_code_sha=repository_code_sha,
        owner_decision_id=attestation.owner_decision_id,
        proposal_file_sha256=proposal_sha,
        proposal_content_sha256=proposal.content_sha256,
        owner_attestation_file_sha256=attestation_sha,
        owner_attestation_content_sha256=attestation.content_sha256,
        policy_file_sha256=loaded.policy_sha256,
        policy_canonical_sha256=loaded.policy_canonical_sha256,
        project_id=proposal.project_id,
        backtest_id=proposal.backtest_id,
        axis_decisions=proposal.axis_decisions,
        unknowns=proposal.unknowns,
        aggregate_decision=attestation.accepted_aggregate_recommendation,
        signoff_status="SIGNED_NO_GO",
        source_pilot_disposition=proposal.source_disposition,
        safety=proposal.safety,
    )
