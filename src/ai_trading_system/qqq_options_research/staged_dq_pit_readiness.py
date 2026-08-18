from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    DQCheckResult,
    DQStatus,
    QQQOptionsContractError,
    QQQOptionsSafetyBoundary,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTIONS_STAGED_READINESS_POLICY_PATH = Path(
    "config/research/qqq_options_staged_dq_pit_readiness_v1.yaml"
)

ReadinessStage = Literal["DATA_RESEARCH", "SHADOW_SELECTION", "EXECUTION"]
ReadinessStatus = Literal["READY", "NOT_READY", "BLOCKED"]
SourceEvidenceRoute = Literal[
    "PROVIDER_RAW_CHECKSUM",
    "PLATFORM_ATTESTED_DERIVED",
    "UNSATISFIED",
]

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT_SHA_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")

_REGISTRATION_BASE_REPOSITORY_CODE_SHA = "a887aee4e0d0cfe396a9f7e6994a46afa0c9fe44"
_BASE_DQ_POLICY_FILE_SHA256 = "1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358"
_BASE_DQ_EVALUATOR_FILE_SHA256 = "baf9baffe1bc441342b9acfcca0010d3a40482bc63b8f5f16629d8b29326ef07"

_REQUIRED_CHECK_IDS = (
    "cache_identity",
    "chain_presence",
    "engine_identity",
    "evidence_identity",
    "exchange_calendar_identity",
    "fill_forward_ambiguity",
    "local_cache_dq_scope_separation",
    "open_interest_freshness",
    "order_fill_chronology",
    "prior_day_model_freshness",
    "provider_raw_checksum",
    "quote_freshness",
    "quote_integrity",
    "signal_selection_chronology",
    "symbol_mapping_identity",
)
_STAGE_ORDER: tuple[ReadinessStage, ...] = (
    "DATA_RESEARCH",
    "SHADOW_SELECTION",
    "EXECUTION",
)
_STAGE_REQUIRED_CHECKS: dict[ReadinessStage, tuple[str, ...]] = {
    "DATA_RESEARCH": (
        "cache_identity",
        "chain_presence",
        "engine_identity",
        "evidence_identity",
        "exchange_calendar_identity",
        "local_cache_dq_scope_separation",
        "open_interest_freshness",
        "prior_day_model_freshness",
        "provider_raw_checksum",
        "quote_freshness",
        "quote_integrity",
        "symbol_mapping_identity",
    ),
    "SHADOW_SELECTION": ("signal_selection_chronology",),
    "EXECUTION": ("fill_forward_ambiguity", "order_fill_chronology"),
}


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _required_text(value: str, field: str) -> str:
    if not value or value != value.strip() or any(ord(char) < 32 for char in value):
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


def _git_object_sha(value: str, field: str) -> str:
    if not _GIT_OBJECT_SHA_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase Git object SHA")
    return value


def _utc(value: datetime, field: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None:
        raise ValueError(f"{field} must be timezone-aware")
    if offset.total_seconds() != 0:
        raise ValueError(f"{field} must use UTC offset")
    return value.astimezone(UTC)


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


def _content_sha256(payload: dict[str, Any]) -> str:
    body = dict(payload)
    body.pop("content_sha256", None)
    return hashlib.sha256(_canonical_json_bytes(body)).hexdigest()


class UnknownNumericThresholds(_StrictModel):
    max_quote_age_seconds: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    max_relative_spread: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    min_open_interest: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    min_volume: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]


class QQQOptionsStagedReadinessPolicy(_StrictModel):
    schema_version: Literal["qqq_options_staged_dq_pit_readiness_policy.v1"]
    policy_id: Literal["qqq_options_staged_dq_pit_readiness_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["REVIEWED_SERIAL_CONTRACT_WAVE"]
    owner: str
    owner_decision: str
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    registration_base_repository_code_sha: str
    base_dq_policy_path: Literal["config/research/qqq_options_dq_pit_identity_v1.yaml"]
    base_dq_policy_file_sha256: str
    base_dq_evaluator_path: Literal["src/ai_trading_system/qqq_options_research/dq_pit_identity.py"]
    base_dq_evaluator_file_sha256: str
    shared_contract_sha256: str
    required_check_ids: tuple[str, ...]
    stage_order: tuple[ReadinessStage, ...]
    stage_required_checks: dict[ReadinessStage, tuple[str, ...]]
    source_evidence_routes: tuple[
        Literal["PROVIDER_RAW_CHECKSUM", "PLATFORM_ATTESTED_DERIVED"], ...
    ]
    alternate_route_trigger_reason: Literal["PROVIDER_RAW_CHECKSUM_UNAVAILABLE"]
    alternate_route_provider_state: Literal["UNAVAILABLE_PROVIDER_DOES_NOT_EXPOSE"]
    alternate_route_export_classification: Literal["EXPORT_ALLOWED_DERIVED"]
    alternate_route_license_state: Literal["CONFIRMED_EXPORT_SAFE_DERIVED"]
    numeric_thresholds: UnknownNumericThresholds
    unknown_can_pass: Literal[False]
    current_window_session_exclusion_allowed: Literal[False]
    external_action_authorized: Literal[False]
    safety: QQQOptionsSafetyBoundary

    @field_validator(
        "owner",
        "owner_decision",
        "rationale",
        "intended_effect",
        "validation_plan",
        "review_condition",
        "expiry_condition",
    )
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator(
        "base_dq_policy_file_sha256",
        "base_dq_evaluator_file_sha256",
        "shared_contract_sha256",
    )
    @classmethod
    def _validate_hash(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("registration_base_repository_code_sha")
    @classmethod
    def _validate_git_sha(cls, value: str) -> str:
        return _git_object_sha(value, "registration_base_repository_code_sha")

    @model_validator(mode="after")
    def _validate_authority(self) -> Self:
        if self.registration_base_repository_code_sha != _REGISTRATION_BASE_REPOSITORY_CODE_SHA:
            raise ValueError("registration base differs from the reviewed contract wave")
        if self.base_dq_policy_file_sha256 != _BASE_DQ_POLICY_FILE_SHA256:
            raise ValueError("base DQ policy hash differs from the reviewed 2482 bytes")
        if self.base_dq_evaluator_file_sha256 != _BASE_DQ_EVALUATOR_FILE_SHA256:
            raise ValueError("base DQ evaluator hash differs from the reviewed 2482 bytes")
        if self.shared_contract_sha256 != QQQ_OPTIONS_CONTRACT_SHA256:
            raise ValueError("staged readiness must bind the frozen 2481 contract")
        if self.required_check_ids != _REQUIRED_CHECK_IDS:
            raise ValueError("required checks differ from the frozen 2482 set")
        if self.stage_order != _STAGE_ORDER:
            raise ValueError("stage order differs from the reviewed contract wave")
        if self.stage_required_checks != _STAGE_REQUIRED_CHECKS:
            raise ValueError("stage check applicability differs from the reviewed wave")
        flattened = tuple(
            check_id for stage in self.stage_order for check_id in self.stage_required_checks[stage]
        )
        if len(flattened) != len(set(flattened)) or set(flattened) != set(self.required_check_ids):
            raise ValueError("stage checks must partition all 2482 checks exactly once")
        if self.source_evidence_routes != (
            "PROVIDER_RAW_CHECKSUM",
            "PLATFORM_ATTESTED_DERIVED",
        ):
            raise ValueError("source evidence routes differ from the reviewed wave")
        return self


class QQQOptionsStagedReadinessPolicyLoadResult(_StrictModel):
    policy: QQQOptionsStagedReadinessPolicy
    policy_path: Path
    policy_sha256: str

    @field_validator("policy_sha256")
    @classmethod
    def _validate_policy_hash(cls, value: str) -> str:
        return _sha256(value, "policy_sha256")


class ReadinessCheckEvidence(_StrictModel):
    check_id: str
    status: DQStatus
    reason_code: str | None

    @field_validator("check_id")
    @classmethod
    def _validate_check_id(cls, value: str) -> str:
        return _identifier(value, "check_id")

    @field_validator("reason_code")
    @classmethod
    def _validate_reason(cls, value: str | None) -> str | None:
        return None if value is None else _identifier(value, "reason_code")

    @model_validator(mode="after")
    def _validate_disposition(self) -> Self:
        if self.status == "PASS" and self.reason_code is not None:
            raise ValueError("passing readiness evidence cannot carry a reason")
        if self.status != "PASS" and self.reason_code is None:
            raise ValueError("non-passing readiness evidence requires a reason")
        return self


class PlatformAttestedDerivedEvidence(_StrictModel):
    schema_version: Literal["platform_attested_derived_evidence.v1"]
    provider: Literal["QuantConnect"]
    provider_checksum_availability: Literal["UNAVAILABLE_PROVIDER_DOES_NOT_EXPOSE"]
    provider_raw_checksum_claimed: Literal[False]
    derived_evidence_export_classification: Literal["EXPORT_ALLOWED_DERIVED"]
    raw_option_field_classification: Literal["QC_ONLY_NOT_EXPORTED", "EXPORT_PROHIBITED"]
    raw_option_rows_included: Literal[False]
    account_or_broker_identifiers_included: Literal[False]
    platform_identity_status: Literal["CONFIRMED"]
    tier_status: Literal["CONFIRMED"]
    engine_identity_status: Literal["CONFIRMED"]
    evidence_manifest_status: Literal["CONFIRMED"]
    platform_tier: str
    engine_id: str
    bundle_id: str
    project_id: int = Field(gt=0)
    backtest_id: str
    repository_code_sha: str
    shared_contract_sha256: str
    base_dq_policy_file_sha256: str
    base_dq_evaluator_file_sha256: str
    source_report_content_sha256: str
    derived_evidence_content_sha256: str
    evidence_manifest_content_sha256: str
    requested_start: date
    requested_end: date
    expected_session_count: int = Field(gt=0)
    observed_session_count: int = Field(gt=0)
    deterministic_replay_status: Literal["PASS"]
    license_state: Literal["CONFIRMED_EXPORT_SAFE_DERIVED"]
    attested_at_utc: datetime

    @field_validator("platform_tier", "engine_id", "bundle_id", "backtest_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator(
        "shared_contract_sha256",
        "base_dq_policy_file_sha256",
        "base_dq_evaluator_file_sha256",
        "source_report_content_sha256",
        "derived_evidence_content_sha256",
        "evidence_manifest_content_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_object_sha(value, "repository_code_sha")

    @field_validator("attested_at_utc")
    @classmethod
    def _validate_attested_at(cls, value: datetime) -> datetime:
        return _utc(value, "attested_at_utc")

    @model_validator(mode="after")
    def _validate_scope(self) -> Self:
        if self.requested_start > self.requested_end:
            raise ValueError("requested range is reversed")
        if self.observed_session_count != self.expected_session_count:
            raise ValueError("derived evidence session count is incomplete")
        return self


class StageReadinessDecision(_StrictModel):
    stage: ReadinessStage
    status: ReadinessStatus
    required_check_ids: tuple[str, ...]
    satisfied_check_ids: tuple[str, ...]
    failed_check_ids: tuple[str, ...]
    not_evaluated_check_ids: tuple[str, ...]
    reason_codes: tuple[str, ...]

    @model_validator(mode="after")
    def _validate_sets(self) -> Self:
        fields = (
            self.required_check_ids,
            self.satisfied_check_ids,
            self.failed_check_ids,
            self.not_evaluated_check_ids,
            self.reason_codes,
        )
        if any(items != tuple(sorted(items)) or len(items) != len(set(items)) for items in fields):
            raise ValueError("readiness decision tuples must be sorted and unique")
        classified = (
            set(self.satisfied_check_ids)
            | set(self.failed_check_ids)
            | set(self.not_evaluated_check_ids)
        )
        if classified != set(self.required_check_ids):
            raise ValueError("every required stage check must be classified exactly once")
        classified_sets = (
            set(self.satisfied_check_ids),
            set(self.failed_check_ids),
            set(self.not_evaluated_check_ids),
        )
        if any(
            left & right
            for index, left in enumerate(classified_sets)
            for right in classified_sets[index + 1 :]
        ):
            raise ValueError("stage check classifications overlap")
        if self.status == "READY" and (
            self.failed_check_ids or self.not_evaluated_check_ids or self.reason_codes
        ):
            raise ValueError("ready stage cannot carry blockers")
        if self.status != "READY" and not self.reason_codes:
            raise ValueError("non-ready stage requires reasons")
        return self


class QQQOptionsStagedReadinessDecision(_StrictModel):
    schema_version: Literal["qqq_options_staged_dq_pit_readiness_decision.v1"]
    policy_id: Literal["qqq_options_staged_dq_pit_readiness_v1"]
    policy_version: Literal["1.0.0"]
    policy_sha256: str
    source_report_content_sha256: str
    source_evidence_route: SourceEvidenceRoute
    derived_evidence: PlatformAttestedDerivedEvidence | None
    evaluated_at_utc: datetime
    source_checks: tuple[ReadinessCheckEvidence, ...]
    stages: tuple[StageReadinessDecision, ...]
    external_action_authorized: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    content_sha256: str

    @field_validator("policy_sha256", "source_report_content_sha256", "content_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("evaluated_at_utc")
    @classmethod
    def _validate_evaluated_at(cls, value: datetime) -> datetime:
        return _utc(value, "evaluated_at_utc")

    @model_validator(mode="after")
    def _validate_decision(self) -> Self:
        if tuple(check.check_id for check in self.source_checks) != _REQUIRED_CHECK_IDS:
            raise ValueError("source checks differ from the frozen 2482 set")
        if tuple(stage.stage for stage in self.stages) != _STAGE_ORDER:
            raise ValueError("readiness decisions must follow canonical stage order")
        cumulative: list[str] = []
        for stage in self.stages:
            cumulative.extend(_STAGE_REQUIRED_CHECKS[stage.stage])
            if stage.required_check_ids != tuple(sorted(cumulative)):
                raise ValueError("stage decision check applicability drifted")
        raw_checksum = next(
            check for check in self.source_checks if check.check_id == "provider_raw_checksum"
        )
        if self.source_evidence_route == "PROVIDER_RAW_CHECKSUM":
            if raw_checksum.status != "PASS" or self.derived_evidence is not None:
                raise ValueError("provider checksum route does not match source evidence")
        elif self.source_evidence_route == "PLATFORM_ATTESTED_DERIVED":
            if (
                raw_checksum.status != "NOT_EVALUATED"
                or raw_checksum.reason_code != "PROVIDER_RAW_CHECKSUM_UNAVAILABLE"
                or self.derived_evidence is None
                or not _derived_evidence_matches_frozen_authority(
                    self.derived_evidence,
                    self.source_report_content_sha256,
                    self.evaluated_at_utc,
                )
            ):
                raise ValueError("platform-attested route does not match frozen authority")
        elif raw_checksum.status == "PASS":
            raise ValueError("passing provider checksum cannot use an unsatisfied route")
        statuses = tuple(stage.status for stage in self.stages)
        for index, status in enumerate(statuses[:-1]):
            if status != "READY" and any(later == "READY" for later in statuses[index + 1 :]):
                raise ValueError("readiness cannot recover after an earlier stage is not ready")
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("staged readiness content SHA-256 mismatch")
        return self

    def compute_content_sha256(self) -> str:
        return _content_sha256(self.model_dump(mode="json"))

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        body = dict(payload)
        draft = cls.model_construct(**body, content_sha256="0" * 64)
        body["content_sha256"] = _content_sha256(draft.model_dump(mode="json"))
        return cls(**body)

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            payload = json.loads(content)
            if not isinstance(payload, dict):
                raise TypeError("decision root must be a mapping")
            decision = cls.model_validate(payload, strict=False)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise QQQOptionsContractError("QQQ_OPTIONS_STAGED_READINESS_INVALID", str(exc)) from exc
        if content != decision.canonical_bytes:
            raise QQQOptionsContractError(
                "QQQ_OPTIONS_STAGED_READINESS_NOT_CANONICAL",
                "decision bytes do not match canonical JSON encoding",
            )
        return decision


def load_qqq_options_staged_readiness_policy(
    path: Path = DEFAULT_QQQ_OPTIONS_STAGED_READINESS_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsStagedReadinessPolicyLoadResult:
    resolved = path if path.is_absolute() else project_root / path
    try:
        content = resolved.read_bytes()
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = QQQOptionsStagedReadinessPolicy.model_validate(payload, strict=False)
        _require_file_hash(
            project_root / policy.base_dq_policy_path,
            policy.base_dq_policy_file_sha256,
            "base DQ policy",
        )
        _require_file_hash(
            project_root / policy.base_dq_evaluator_path,
            policy.base_dq_evaluator_file_sha256,
            "base DQ evaluator",
        )
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_STAGED_READINESS_POLICY_INVALID", f"{resolved}: {exc}"
        ) from exc
    return QQQOptionsStagedReadinessPolicyLoadResult(
        policy=policy,
        policy_path=resolved,
        policy_sha256=hashlib.sha256(content).hexdigest(),
    )


def _require_file_hash(path: Path, expected: str, label: str) -> None:
    observed = hashlib.sha256(path.read_bytes()).hexdigest()
    if observed != expected:
        raise ValueError(f"{label} SHA-256 mismatch: {observed} != {expected}")


def readiness_checks_from_dq_results(
    checks: tuple[DQCheckResult, ...],
) -> tuple[ReadinessCheckEvidence, ...]:
    return tuple(
        ReadinessCheckEvidence(
            check_id=check.check_id,
            status=check.status,
            reason_code=check.reason_code,
        )
        for check in checks
    )


def evaluate_qqq_options_staged_readiness(
    *,
    source_report_content_sha256: str,
    checks: tuple[ReadinessCheckEvidence, ...],
    evaluated_at_utc: datetime,
    derived_evidence: PlatformAttestedDerivedEvidence | None = None,
    policy_path: Path = DEFAULT_QQQ_OPTIONS_STAGED_READINESS_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsStagedReadinessDecision:
    loaded = load_qqq_options_staged_readiness_policy(policy_path, project_root=project_root)
    _sha256(source_report_content_sha256, "source_report_content_sha256")
    evaluated_at = _utc(evaluated_at_utc, "evaluated_at_utc")
    if tuple(check.check_id for check in checks) != loaded.policy.required_check_ids:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_STAGED_READINESS_CHECK_SET_DRIFT",
            "checks must appear exactly once in canonical 2482 order",
        )

    by_id = {check.check_id: check for check in checks}
    raw_checksum = by_id["provider_raw_checksum"]
    source_route: SourceEvidenceRoute = "UNSATISFIED"
    alternate_satisfied = False
    if raw_checksum.status == "PASS":
        if derived_evidence is not None:
            raise QQQOptionsContractError(
                "QQQ_OPTIONS_STAGED_READINESS_AMBIGUOUS_SOURCE_ROUTE",
                "provider checksum PASS cannot also supply alternate derived evidence",
            )
        source_route = "PROVIDER_RAW_CHECKSUM"
    elif (
        raw_checksum.status == "NOT_EVALUATED"
        and raw_checksum.reason_code == loaded.policy.alternate_route_trigger_reason
        and derived_evidence is not None
        and _derived_evidence_matches(
            derived_evidence,
            loaded.policy,
            source_report_content_sha256,
            evaluated_at,
        )
    ):
        source_route = "PLATFORM_ATTESTED_DERIVED"
        alternate_satisfied = True

    stage_decisions: list[StageReadinessDecision] = []
    cumulative: list[str] = []
    predecessor_ready = True
    for stage in loaded.policy.stage_order:
        cumulative.extend(loaded.policy.stage_required_checks[stage])
        required = tuple(sorted(cumulative))
        satisfied: list[str] = []
        failed: list[str] = []
        not_evaluated: list[str] = []
        reasons: set[str] = set()
        for check_id in required:
            check = by_id[check_id]
            if check_id == "provider_raw_checksum" and alternate_satisfied:
                satisfied.append(check_id)
            elif check.status == "PASS":
                satisfied.append(check_id)
            elif check.status == "FAIL":
                failed.append(check_id)
                reasons.add(check.reason_code or "CHECK_FAILED_WITHOUT_REASON")
            else:
                not_evaluated.append(check_id)
                reasons.add(check.reason_code or "CHECK_NOT_EVALUATED_WITHOUT_REASON")

        if not predecessor_ready:
            status: ReadinessStatus = "BLOCKED"
            reasons.add("PREDECESSOR_STAGE_NOT_READY")
        elif failed:
            status = "BLOCKED"
        elif not_evaluated:
            status = "NOT_READY"
        else:
            status = "READY"
        predecessor_ready = status == "READY"
        stage_decisions.append(
            StageReadinessDecision(
                stage=stage,
                status=status,
                required_check_ids=required,
                satisfied_check_ids=tuple(sorted(satisfied)),
                failed_check_ids=tuple(sorted(failed)),
                not_evaluated_check_ids=tuple(sorted(not_evaluated)),
                reason_codes=tuple(sorted(reasons)),
            )
        )

    return QQQOptionsStagedReadinessDecision.seal(
        schema_version="qqq_options_staged_dq_pit_readiness_decision.v1",
        policy_id=loaded.policy.policy_id,
        policy_version=loaded.policy.policy_version,
        policy_sha256=loaded.policy_sha256,
        source_report_content_sha256=source_report_content_sha256,
        source_evidence_route=source_route,
        derived_evidence=derived_evidence,
        evaluated_at_utc=evaluated_at,
        source_checks=checks,
        stages=tuple(stage_decisions),
        external_action_authorized=False,
        production_effect="none",
        broker_action="none",
    )


def _derived_evidence_matches(
    evidence: PlatformAttestedDerivedEvidence,
    policy: QQQOptionsStagedReadinessPolicy,
    source_report_content_sha256: str,
    evaluated_at_utc: datetime,
) -> bool:
    return (
        evidence.provider_checksum_availability == policy.alternate_route_provider_state
        and evidence.derived_evidence_export_classification
        == policy.alternate_route_export_classification
        and evidence.license_state == policy.alternate_route_license_state
        and evidence.shared_contract_sha256 == policy.shared_contract_sha256
        and evidence.base_dq_policy_file_sha256 == policy.base_dq_policy_file_sha256
        and evidence.base_dq_evaluator_file_sha256 == policy.base_dq_evaluator_file_sha256
        and evidence.source_report_content_sha256 == source_report_content_sha256
        and evidence.attested_at_utc <= evaluated_at_utc
    )


def _derived_evidence_matches_frozen_authority(
    evidence: PlatformAttestedDerivedEvidence,
    source_report_content_sha256: str,
    evaluated_at_utc: datetime,
) -> bool:
    return (
        evidence.provider_checksum_availability == "UNAVAILABLE_PROVIDER_DOES_NOT_EXPOSE"
        and evidence.derived_evidence_export_classification == "EXPORT_ALLOWED_DERIVED"
        and evidence.license_state == "CONFIRMED_EXPORT_SAFE_DERIVED"
        and evidence.shared_contract_sha256 == QQQ_OPTIONS_CONTRACT_SHA256
        and evidence.base_dq_policy_file_sha256 == _BASE_DQ_POLICY_FILE_SHA256
        and evidence.base_dq_evaluator_file_sha256 == _BASE_DQ_EVALUATOR_FILE_SHA256
        and evidence.source_report_content_sha256 == source_report_content_sha256
        and evidence.attested_at_utc <= evaluated_at_utc
    )


__all__ = [
    "DEFAULT_QQQ_OPTIONS_STAGED_READINESS_POLICY_PATH",
    "PlatformAttestedDerivedEvidence",
    "QQQOptionsStagedReadinessDecision",
    "QQQOptionsStagedReadinessPolicy",
    "QQQOptionsStagedReadinessPolicyLoadResult",
    "ReadinessCheckEvidence",
    "StageReadinessDecision",
    "evaluate_qqq_options_staged_readiness",
    "load_qqq_options_staged_readiness_policy",
    "readiness_checks_from_dq_results",
]
