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
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    CanonicalDecimal,
    DQCheckResult,
    DQReportRecord,
    DQStatus,
    PITStatus,
    PlatformEvidenceManifestRecord,
    QQQOptionsContractError,
    QQQOptionsRecordEnvelope,
    QQQOptionsSafetyBoundary,
)
from ai_trading_system.qqq_options_research.policy import (
    load_qqq_options_shared_contract_policy,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTIONS_DQ_PIT_IDENTITY_POLICY_PATH = Path(
    "config/research/qqq_options_dq_pit_identity_v1.yaml"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT_SHA_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")

AssessmentStatus = Literal["PASS", "FAIL", "UNKNOWN_REQUIRES_POLICY_REVIEW"]
ChecksumAvailability = Literal[
    "AVAILABLE",
    "UNKNOWN",
    "UNAVAILABLE_PROVIDER_DOES_NOT_EXPOSE",
]
LocalCacheDQStatus = Literal[
    "PASS",
    "FAIL",
    "NOT_EVALUATED",
    "NOT_APPLICABLE_TO_OPTION_EVENT_SCOPE",
]

_REQUIRED_CHECK_IDS = tuple(
    sorted(
        (
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
    )
)

_REASON_CODES = tuple(
    sorted(
        (
            "CACHE_CONTRACT_IDENTITY_MISMATCH",
            "CACHE_IDENTITY_COLLISION",
            "CACHE_POLICY_IDENTITY_MISMATCH",
            "CANDIDATE_MISSING",
            "CHAIN_MISSING",
            "ENGINE_IDENTITY_ASSESSMENT_FAIL",
            "ENGINE_IDENTITY_MISMATCH",
            "ENGINE_IDENTITY_UNKNOWN",
            "EVIDENCE_IDENTITY_ASSESSMENT_FAIL",
            "EVIDENCE_IDENTITY_MISMATCH",
            "EVIDENCE_IDENTITY_UNKNOWN",
            "EVIDENCE_MANIFEST_IDENTITY_MISMATCH",
            "EVIDENCE_MANIFEST_MISSING",
            "EVIDENCE_MANIFEST_NOT_CONFIRMED",
            "EXCHANGE_CALENDAR_IDENTITY_ASSESSMENT_FAIL",
            "EXCHANGE_CALENDAR_IDENTITY_MISMATCH",
            "EXCHANGE_CALENDAR_IDENTITY_UNKNOWN",
            "FILL_FORWARD_AMBIGUITY",
            "FILL_FORWARD_STATUS_UNKNOWN",
            "MODEL_AS_OF_MISSING",
            "MODEL_FRESHNESS_FAIL",
            "MODEL_FRESHNESS_UNKNOWN",
            "MODEL_SESSION_NOT_EXACT_PRIOR",
            "OI_AS_OF_MISSING",
            "OI_FRESHNESS_FAIL",
            "OI_FRESHNESS_UNKNOWN",
            "OI_SESSION_NOT_EXACT_PRIOR",
            "ORDER_FILL_CHRONOLOGY_INVALID",
            "ORDER_FILL_CHRONOLOGY_MISSING",
            "PROVIDER_RAW_CHECKSUM_UNAVAILABLE",
            "PROVIDER_RAW_CHECKSUM_UNKNOWN",
            "QUOTE_AFTER_SELECTION",
            "QUOTE_CROSSED",
            "QUOTE_FRESHNESS_FAIL",
            "QUOTE_FRESHNESS_UNKNOWN",
            "QUOTE_MISSING",
            "QUOTE_NEGATIVE_BID",
            "QUOTE_SINGLE_SIDED",
            "QUOTE_TIMESTAMP_MISSING",
            "QUOTE_ZERO_ASK",
            "SIGNAL_SELECTION_CHRONOLOGY_INVALID",
            "SIGNAL_SELECTION_CHRONOLOGY_MISSING",
            "SYMBOL_MAPPING_IDENTITY_ASSESSMENT_FAIL",
            "SYMBOL_MAPPING_IDENTITY_MISMATCH",
            "SYMBOL_MAPPING_IDENTITY_UNKNOWN",
        )
    )
)

_CHRONOLOGY_FIELDS = (
    "signal_as_of_utc",
    "selection_snapshot_utc",
    "order_intent_utc",
    "order_submit_utc",
    "fill_quote_end_utc",
    "fill_utc",
)

_CACHE_IDENTITY_COMPONENTS = tuple(
    sorted(
        (
            "calendar_identity",
            "dataset",
            "dq_policy_sha256",
            "engine_identity",
            "mapping_identity",
            "normalization_identity",
            "option_sid",
            "provider",
            "repository_code_sha",
            "requested_end",
            "requested_start",
            "resolution",
            "shared_contract_sha256",
            "source_checksum_evidence",
            "underlying",
        )
    )
)


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
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    if value.utcoffset().total_seconds() != 0:
        raise ValueError(f"{field} must use UTC offset")
    return value.astimezone(UTC)


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
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


class IdentityBinding(_StrictModel):
    assessment: AssessmentStatus
    expected_id: str
    expected_version: str
    expected_sha256: str
    observed_id: str | None
    observed_version: str | None
    observed_sha256: str | None

    @field_validator("expected_id", "expected_version")
    @classmethod
    def _validate_expected_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("observed_id", "observed_version")
    @classmethod
    def _validate_observed_ids(
        cls, value: str | None, info: ValidationInfo
    ) -> str | None:
        return None if value is None else _identifier(value, str(info.field_name))

    @field_validator("expected_sha256")
    @classmethod
    def _validate_expected_hash(cls, value: str) -> str:
        return _sha256(value, "expected_sha256")

    @field_validator("observed_sha256")
    @classmethod
    def _validate_observed_hash(cls, value: str | None) -> str | None:
        return None if value is None else _sha256(value, "observed_sha256")


class LocalCachedDataGateDeclaration(_StrictModel):
    status: LocalCacheDQStatus
    scope: Literal["CACHED_MARKET_MACRO", "NOT_APPLICABLE_TO_OPTION_EVENT_SCOPE"]
    as_of_utc: datetime | None
    report_locator: str | None
    report_sha256: str | None

    @field_validator("as_of_utc")
    @classmethod
    def _validate_as_of(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _utc(value, "as_of_utc")

    @field_validator("report_locator")
    @classmethod
    def _validate_locator(cls, value: str | None) -> str | None:
        return None if value is None else _required_text(value, "report_locator")

    @field_validator("report_sha256")
    @classmethod
    def _validate_report_hash(cls, value: str | None) -> str | None:
        return None if value is None else _sha256(value, "report_sha256")

    @model_validator(mode="after")
    def _validate_declaration(self) -> Self:
        evidence = (self.as_of_utc, self.report_locator, self.report_sha256)
        if self.status in {"PASS", "FAIL"}:
            if self.scope != "CACHED_MARKET_MACRO" or any(item is None for item in evidence):
                raise ValueError("evaluated local cache DQ requires scoped report evidence")
        elif self.status == "NOT_APPLICABLE_TO_OPTION_EVENT_SCOPE":
            if self.scope != "NOT_APPLICABLE_TO_OPTION_EVENT_SCOPE":
                raise ValueError(
                    "not-applicable local DQ must declare option-event scope separation"
                )
        return self


class SourceChecksumEvidence(_StrictModel):
    availability: ChecksumAvailability
    sha256: str | None
    export_classification: Literal["EXPORT_PROHIBITED", "QC_ONLY_NOT_EXPORTED"]

    @field_validator("sha256")
    @classmethod
    def _validate_hash(cls, value: str | None) -> str | None:
        return None if value is None else _sha256(value, "sha256")

    @model_validator(mode="after")
    def _validate_availability(self) -> Self:
        if self.availability == "AVAILABLE" and self.sha256 is None:
            raise ValueError("available provider checksum requires SHA-256")
        if self.availability != "AVAILABLE" and self.sha256 is not None:
            raise ValueError("unavailable or unknown provider checksum cannot be fabricated")
        return self


class QQQOptionsCacheIdentityMaterial(_StrictModel):
    provider: str
    dataset: str
    underlying: Literal["QQQ"]
    option_sid: str
    resolution: Literal["MINUTE", "DAILY"]
    requested_start: date
    requested_end: date
    calendar_identity: IdentityBinding
    mapping_identity: IdentityBinding
    normalization_identity: IdentityBinding
    dq_policy_sha256: str
    shared_contract_sha256: str
    repository_code_sha: str
    engine_identity: IdentityBinding
    source_checksum_evidence: SourceChecksumEvidence

    @field_validator("provider", "dataset", "option_sid")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("dq_policy_sha256", "shared_contract_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_code_hash(cls, value: str) -> str:
        return _git_object_sha(value, "repository_code_sha")

    @model_validator(mode="after")
    def _validate_range(self) -> Self:
        if self.requested_start > self.requested_end:
            raise ValueError("cache requested range is reversed")
        return self


class QQQOptionsCacheIdentityReceipt(_StrictModel):
    schema_version: Literal["qqq_options_cache_identity_receipt.v1"]
    cache_key: str
    material: QQQOptionsCacheIdentityMaterial
    identity_sha256: str

    @field_validator("cache_key")
    @classmethod
    def _validate_cache_key(cls, value: str) -> str:
        return _identifier(value, "cache_key")

    @field_validator("identity_sha256")
    @classmethod
    def _validate_identity_hash(cls, value: str) -> str:
        return _sha256(value, "identity_sha256")

    def material_payload(self) -> dict[str, Any]:
        return {
            "cache_key": self.cache_key,
            "material": self.material.model_dump(mode="json"),
            "schema_version": self.schema_version,
        }

    def compute_identity_sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self.material_payload())).hexdigest()

    @model_validator(mode="after")
    def _validate_identity(self) -> Self:
        if self.identity_sha256 != self.compute_identity_sha256():
            raise ValueError("cache identity SHA-256 does not match canonical material")
        return self

    @classmethod
    def seal(
        cls, *, cache_key: str, material: QQQOptionsCacheIdentityMaterial
    ) -> Self:
        payload = {
            "schema_version": "qqq_options_cache_identity_receipt.v1",
            "cache_key": cache_key,
            "material": material,
        }
        provisional = cls.model_construct(**payload, identity_sha256="0" * 64)
        return cls(**payload, identity_sha256=provisional.compute_identity_sha256())

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            receipt = cls.model_validate_json(content)
        except ValueError as exc:
            raise QQQOptionsContractError(
                "QQQ_OPTIONS_CACHE_IDENTITY_INVALID", str(exc)
            ) from exc
        if content != receipt.canonical_bytes:
            raise QQQOptionsContractError(
                "QQQ_OPTIONS_CACHE_IDENTITY_NOT_CANONICAL",
                "cache identity bytes do not match canonical JSON encoding",
            )
        return receipt


class QQQOptionsDQObservation(_StrictModel):
    observed_at_utc: datetime
    chain_present: bool
    candidate_present: bool
    quote_bid_per_share: CanonicalDecimal | None
    quote_ask_per_share: CanonicalDecimal | None
    quote_end_utc: datetime | None
    quote_freshness_assessment: AssessmentStatus
    selection_session: date
    expected_prior_session: date
    prior_day_model_as_of_session: date | None
    model_freshness_assessment: AssessmentStatus
    open_interest_as_of_session: date | None
    open_interest_freshness_assessment: AssessmentStatus
    exchange_calendar_identity: IdentityBinding
    symbol_mapping_identity: IdentityBinding
    signal_as_of_utc: datetime | None
    selection_snapshot_utc: datetime | None
    order_intent_utc: datetime | None
    order_submit_utc: datetime | None
    fill_quote_end_utc: datetime | None
    fill_utc: datetime | None
    fill_forward_assessment: AssessmentStatus
    cache_key: str
    prior_cache_identity_sha256: str | None
    cache_material: QQQOptionsCacheIdentityMaterial
    engine_identity: IdentityBinding
    evidence_identity: IdentityBinding
    platform_evidence_manifest: PlatformEvidenceManifestRecord | None
    local_cached_data_gate: LocalCachedDataGateDeclaration

    @field_validator(
        "observed_at_utc",
        "quote_end_utc",
        "signal_as_of_utc",
        "selection_snapshot_utc",
        "order_intent_utc",
        "order_submit_utc",
        "fill_quote_end_utc",
        "fill_utc",
    )
    @classmethod
    def _validate_times(
        cls, value: datetime | None, info: ValidationInfo
    ) -> datetime | None:
        return None if value is None else _utc(value, str(info.field_name))

    @field_validator("cache_key")
    @classmethod
    def _validate_cache_key(cls, value: str) -> str:
        return _identifier(value, "cache_key")

    @field_validator("prior_cache_identity_sha256")
    @classmethod
    def _validate_prior_hash(cls, value: str | None) -> str | None:
        return None if value is None else _sha256(value, "prior_cache_identity_sha256")

    @model_validator(mode="after")
    def _validate_sessions(self) -> Self:
        if self.expected_prior_session >= self.selection_session:
            raise ValueError("expected prior session must precede selection session")
        if self.quote_end_utc is not None and self.quote_end_utc > self.observed_at_utc:
            raise ValueError("quote observation cannot be in the future of the DQ observation")
        return self


class NumericThresholdPolicy(_StrictModel):
    max_quote_age_seconds: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    max_relative_spread: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    min_open_interest: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    min_volume: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]


class QQQOptionsDQIdentityPolicy(_StrictModel):
    schema_version: Literal["qqq_options_dq_pit_identity_policy.v1"]
    policy_id: Literal["qqq_options_dq_pit_identity_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["REVIEWED_BASELINE_FAIL_CLOSED"]
    owner: str
    owner_decision: str
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    shared_contract_sha256: str
    shared_policy_sha256: str
    required_check_ids: tuple[str, ...]
    reason_codes: tuple[str, ...]
    chronology_fields: tuple[str, ...]
    chronology_expression: Literal[
        "signal_as_of_utc < selection_snapshot_utc < order_intent_utc "
        "<= order_submit_utc < fill_quote_end_utc <= fill_utc"
    ]
    cache_identity_components: tuple[str, ...]
    source_checksum_availability_states: tuple[ChecksumAvailability, ...]
    numeric_thresholds: NumericThresholdPolicy
    daily_freshness_rule: Literal["EXACT_PRIOR_EXCHANGE_SESSION"]
    local_cache_dq_substitution_allowed: Literal[False]
    unknown_can_pass: Literal[False]
    raw_field_classifications: tuple[
        Literal["EXPORT_PROHIBITED", "QC_ONLY_NOT_EXPORTED"], ...
    ]
    license_unknown_state: Literal["UNKNOWN_REQUIRES_LICENSE_REVIEW"]
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

    @field_validator("shared_contract_sha256", "shared_policy_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_frozen_policy(self) -> Self:
        shared = load_qqq_options_shared_contract_policy()
        if self.shared_contract_sha256 != QQQ_OPTIONS_CONTRACT_SHA256:
            raise ValueError("2482 policy must bind the reviewed 2481 contract hash")
        if self.shared_policy_sha256 != shared.policy_sha256:
            raise ValueError("2482 policy must bind the exact 2481 policy bytes")
        if self.required_check_ids != _REQUIRED_CHECK_IDS:
            raise ValueError("required checks differ from the reviewed 2482 freeze")
        if self.reason_codes != _REASON_CODES:
            raise ValueError("reason codes differ from the reviewed 2482 freeze")
        if self.chronology_fields != _CHRONOLOGY_FIELDS:
            raise ValueError("chronology fields differ from the reviewed 2482 freeze")
        if self.cache_identity_components != _CACHE_IDENTITY_COMPONENTS:
            raise ValueError("cache identity components differ from the reviewed freeze")
        if self.source_checksum_availability_states != (
            "AVAILABLE",
            "UNKNOWN",
            "UNAVAILABLE_PROVIDER_DOES_NOT_EXPOSE",
        ):
            raise ValueError("source checksum availability states drifted")
        if self.raw_field_classifications != (
            "EXPORT_PROHIBITED",
            "QC_ONLY_NOT_EXPORTED",
        ):
            raise ValueError("raw option fields must remain prohibited or QC-only")
        return self


@dataclass(frozen=True)
class QQQOptionsDQIdentityPolicyLoadResult:
    policy: QQQOptionsDQIdentityPolicy
    policy_path: Path
    policy_sha256: str


@dataclass(frozen=True)
class QQQOptionsDQIdentityEvaluation:
    policy_sha256: str
    cache_identity: QQQOptionsCacheIdentityReceipt
    local_cached_data_gate: LocalCachedDataGateDeclaration
    report: DQReportRecord


def load_qqq_options_dq_pit_identity_policy(
    path: Path = DEFAULT_QQQ_OPTIONS_DQ_PIT_IDENTITY_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsDQIdentityPolicyLoadResult:
    resolved = path if path.is_absolute() else project_root / path
    try:
        content = resolved.read_bytes()
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = QQQOptionsDQIdentityPolicy.model_validate(payload, strict=False)
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_DQ_PIT_POLICY_INVALID", f"{resolved}: {exc}"
        ) from exc
    return QQQOptionsDQIdentityPolicyLoadResult(
        policy=policy,
        policy_path=resolved,
        policy_sha256=hashlib.sha256(content).hexdigest(),
    )


def build_qqq_options_cache_identity(
    *, cache_key: str, material: QQQOptionsCacheIdentityMaterial
) -> QQQOptionsCacheIdentityReceipt:
    return QQQOptionsCacheIdentityReceipt.seal(cache_key=cache_key, material=material)


def _check(status: DQStatus, check_id: str, reason_code: str | None, at: datetime) -> DQCheckResult:
    return DQCheckResult(
        check_id=check_id,
        status=status,
        reason_code=reason_code,
        observed_at_utc=at,
    )


def _identity_check(
    check_id: str, binding: IdentityBinding, reason_prefix: str, at: datetime
) -> DQCheckResult:
    if binding.assessment == "UNKNOWN_REQUIRES_POLICY_REVIEW":
        return _check("NOT_EVALUATED", check_id, f"{reason_prefix}_UNKNOWN", at)
    if binding.assessment == "FAIL":
        return _check("FAIL", check_id, f"{reason_prefix}_ASSESSMENT_FAIL", at)
    expected = (binding.expected_id, binding.expected_version, binding.expected_sha256)
    observed = (binding.observed_id, binding.observed_version, binding.observed_sha256)
    if expected != observed:
        return _check("FAIL", check_id, f"{reason_prefix}_MISMATCH", at)
    return _check("PASS", check_id, None, at)


def _aggregate_status(checks: tuple[DQCheckResult, ...]) -> DQStatus:
    if any(check.status == "FAIL" for check in checks):
        return "FAIL"
    if any(check.status == "NOT_EVALUATED" for check in checks):
        return "NOT_EVALUATED"
    return "PASS"


def _quote_integrity_check(observation: QQQOptionsDQObservation) -> DQCheckResult:
    at = observation.observed_at_utc
    bid = observation.quote_bid_per_share
    ask = observation.quote_ask_per_share
    if bid is None and ask is None:
        return _check("FAIL", "quote_integrity", "QUOTE_MISSING", at)
    if (bid is None) != (ask is None):
        return _check("FAIL", "quote_integrity", "QUOTE_SINGLE_SIDED", at)
    if observation.quote_end_utc is None:
        return _check("FAIL", "quote_integrity", "QUOTE_TIMESTAMP_MISSING", at)
    assert bid is not None and ask is not None
    if bid < 0:
        return _check("FAIL", "quote_integrity", "QUOTE_NEGATIVE_BID", at)
    if ask <= 0:
        return _check("FAIL", "quote_integrity", "QUOTE_ZERO_ASK", at)
    if ask < bid:
        return _check("FAIL", "quote_integrity", "QUOTE_CROSSED", at)
    if (
        observation.selection_snapshot_utc is not None
        and observation.quote_end_utc > observation.selection_snapshot_utc
    ):
        return _check("FAIL", "quote_integrity", "QUOTE_AFTER_SELECTION", at)
    return _check("PASS", "quote_integrity", None, at)


def _assessment_check(
    *,
    check_id: str,
    assessment: AssessmentStatus,
    fail_reason: str,
    unknown_reason: str,
    at: datetime,
) -> DQCheckResult:
    if assessment == "FAIL":
        return _check("FAIL", check_id, fail_reason, at)
    if assessment == "UNKNOWN_REQUIRES_POLICY_REVIEW":
        return _check("NOT_EVALUATED", check_id, unknown_reason, at)
    return _check("PASS", check_id, None, at)


def evaluate_qqq_options_dq_pit_identity(
    *,
    source_record: QQQOptionsRecordEnvelope,
    observation: QQQOptionsDQObservation,
    record_id: str,
    created_at_utc: datetime,
    producer_version: str,
    lineage_id: str,
    policy_path: Path = DEFAULT_QQQ_OPTIONS_DQ_PIT_IDENTITY_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsDQIdentityEvaluation:
    created_at_utc = _utc(created_at_utc, "created_at_utc")
    loaded = load_qqq_options_dq_pit_identity_policy(
        policy_path, project_root=project_root
    )
    policy = loaded.policy
    cache_identity = build_qqq_options_cache_identity(
        cache_key=observation.cache_key, material=observation.cache_material
    )
    at = observation.observed_at_utc

    checks: list[DQCheckResult] = []
    checks.append(_check("PASS", "local_cache_dq_scope_separation", None, at))

    if not observation.chain_present:
        checks.append(_check("FAIL", "chain_presence", "CHAIN_MISSING", at))
    elif not observation.candidate_present:
        checks.append(_check("FAIL", "chain_presence", "CANDIDATE_MISSING", at))
    else:
        checks.append(_check("PASS", "chain_presence", None, at))

    checks.append(_quote_integrity_check(observation))
    checks.append(
        _assessment_check(
            check_id="quote_freshness",
            assessment=observation.quote_freshness_assessment,
            fail_reason="QUOTE_FRESHNESS_FAIL",
            unknown_reason="QUOTE_FRESHNESS_UNKNOWN",
            at=at,
        )
    )

    if observation.prior_day_model_as_of_session is None:
        model_check = _check("FAIL", "prior_day_model_freshness", "MODEL_AS_OF_MISSING", at)
    elif observation.prior_day_model_as_of_session != observation.expected_prior_session:
        model_check = _check(
            "FAIL", "prior_day_model_freshness", "MODEL_SESSION_NOT_EXACT_PRIOR", at
        )
    else:
        model_check = _assessment_check(
            check_id="prior_day_model_freshness",
            assessment=observation.model_freshness_assessment,
            fail_reason="MODEL_FRESHNESS_FAIL",
            unknown_reason="MODEL_FRESHNESS_UNKNOWN",
            at=at,
        )
    checks.append(model_check)

    if observation.open_interest_as_of_session is None:
        oi_check = _check("FAIL", "open_interest_freshness", "OI_AS_OF_MISSING", at)
    elif observation.open_interest_as_of_session != observation.expected_prior_session:
        oi_check = _check(
            "FAIL", "open_interest_freshness", "OI_SESSION_NOT_EXACT_PRIOR", at
        )
    else:
        oi_check = _assessment_check(
            check_id="open_interest_freshness",
            assessment=observation.open_interest_freshness_assessment,
            fail_reason="OI_FRESHNESS_FAIL",
            unknown_reason="OI_FRESHNESS_UNKNOWN",
            at=at,
        )
    checks.append(oi_check)

    checks.append(
        _identity_check(
            "exchange_calendar_identity",
            observation.exchange_calendar_identity,
            "EXCHANGE_CALENDAR_IDENTITY",
            at,
        )
    )
    checks.append(
        _identity_check(
            "symbol_mapping_identity",
            observation.symbol_mapping_identity,
            "SYMBOL_MAPPING_IDENTITY",
            at,
        )
    )

    if observation.signal_as_of_utc is None or observation.selection_snapshot_utc is None:
        signal_check = _check(
            "NOT_EVALUATED",
            "signal_selection_chronology",
            "SIGNAL_SELECTION_CHRONOLOGY_MISSING",
            at,
        )
    elif not observation.signal_as_of_utc < observation.selection_snapshot_utc:
        signal_check = _check(
            "FAIL",
            "signal_selection_chronology",
            "SIGNAL_SELECTION_CHRONOLOGY_INVALID",
            at,
        )
    else:
        signal_check = _check("PASS", "signal_selection_chronology", None, at)
    checks.append(signal_check)

    order_times = (
        observation.selection_snapshot_utc,
        observation.order_intent_utc,
        observation.order_submit_utc,
        observation.fill_quote_end_utc,
        observation.fill_utc,
    )
    if any(value is None for value in order_times):
        order_check = _check(
            "NOT_EVALUATED",
            "order_fill_chronology",
            "ORDER_FILL_CHRONOLOGY_MISSING",
            at,
        )
    else:
        selection, intent, submit, fill_quote, fill = order_times
        assert selection is not None
        assert intent is not None
        assert submit is not None
        assert fill_quote is not None
        assert fill is not None
        if not (selection < intent <= submit < fill_quote <= fill):
            order_check = _check(
                "FAIL",
                "order_fill_chronology",
                "ORDER_FILL_CHRONOLOGY_INVALID",
                at,
            )
        else:
            order_check = _check("PASS", "order_fill_chronology", None, at)
    checks.append(order_check)

    checks.append(
        _assessment_check(
            check_id="fill_forward_ambiguity",
            assessment=observation.fill_forward_assessment,
            fail_reason="FILL_FORWARD_AMBIGUITY",
            unknown_reason="FILL_FORWARD_STATUS_UNKNOWN",
            at=at,
        )
    )

    if observation.cache_material.shared_contract_sha256 != policy.shared_contract_sha256:
        cache_check = _check(
            "FAIL", "cache_identity", "CACHE_CONTRACT_IDENTITY_MISMATCH", at
        )
    elif observation.cache_material.dq_policy_sha256 != loaded.policy_sha256:
        cache_check = _check(
            "FAIL", "cache_identity", "CACHE_POLICY_IDENTITY_MISMATCH", at
        )
    elif (
        observation.prior_cache_identity_sha256 is not None
        and observation.prior_cache_identity_sha256 != cache_identity.identity_sha256
    ):
        cache_check = _check("FAIL", "cache_identity", "CACHE_IDENTITY_COLLISION", at)
    else:
        cache_check = _check("PASS", "cache_identity", None, at)
    checks.append(cache_check)

    checks.append(
        _identity_check(
            "engine_identity", observation.engine_identity, "ENGINE_IDENTITY", at
        )
    )

    evidence_binding_check = _identity_check(
        "evidence_identity", observation.evidence_identity, "EVIDENCE_IDENTITY", at
    )
    manifest = observation.platform_evidence_manifest
    if evidence_binding_check.status == "PASS":
        if manifest is None:
            evidence_binding_check = _check(
                "NOT_EVALUATED", "evidence_identity", "EVIDENCE_MANIFEST_MISSING", at
            )
        elif manifest.content_sha256 != observation.evidence_identity.observed_sha256:
            evidence_binding_check = _check(
                "FAIL",
                "evidence_identity",
                "EVIDENCE_MANIFEST_IDENTITY_MISMATCH",
                at,
            )
        elif manifest.tier_status != "CONFIRMED" or manifest.engine_identity_status != "CONFIRMED":
            evidence_binding_check = _check(
                "NOT_EVALUATED",
                "evidence_identity",
                "EVIDENCE_MANIFEST_NOT_CONFIRMED",
                at,
            )
    checks.append(evidence_binding_check)

    checksum = observation.cache_material.source_checksum_evidence
    if checksum.availability == "AVAILABLE":
        raw_checksum_check = _check("PASS", "provider_raw_checksum", None, at)
    elif checksum.availability == "UNAVAILABLE_PROVIDER_DOES_NOT_EXPOSE":
        raw_checksum_check = _check(
            "NOT_EVALUATED",
            "provider_raw_checksum",
            "PROVIDER_RAW_CHECKSUM_UNAVAILABLE",
            at,
        )
    else:
        raw_checksum_check = _check(
            "NOT_EVALUATED",
            "provider_raw_checksum",
            "PROVIDER_RAW_CHECKSUM_UNKNOWN",
            at,
        )
    checks.append(raw_checksum_check)

    sorted_checks = tuple(sorted(checks, key=lambda item: item.check_id))
    if tuple(check.check_id for check in sorted_checks) != policy.required_check_ids:
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_DQ_CHECK_SET_DRIFT",
            "evaluator checks differ from the reviewed policy",
        )

    dq_status = _aggregate_status(sorted_checks)
    pit_check_ids = {
        "exchange_calendar_identity",
        "fill_forward_ambiguity",
        "open_interest_freshness",
        "order_fill_chronology",
        "prior_day_model_freshness",
        "quote_freshness",
        "signal_selection_chronology",
        "symbol_mapping_identity",
    }
    pit_status: PITStatus = _aggregate_status(
        tuple(check for check in sorted_checks if check.check_id in pit_check_ids)
    )

    source_pairs = list(zip(source_record.source_ids, source_record.source_checksums, strict=True))
    source_pairs.extend(
        (
            ("qqq.options.cache_identity", cache_identity.identity_sha256),
            ("qqq.options.dq_policy", loaded.policy_sha256),
        )
    )
    if manifest is not None:
        source_pairs.append(("qqq.options.platform_evidence", manifest.content_sha256))
    if checksum.availability == "AVAILABLE":
        assert checksum.sha256 is not None
        source_pairs.append(("qqq.options.provider_raw", checksum.sha256))
    source_pairs = sorted(source_pairs)
    if len({source_id for source_id, _ in source_pairs}) != len(source_pairs):
        raise QQQOptionsContractError(
            "QQQ_OPTIONS_DQ_SOURCE_ID_COLLISION",
            "DQ report source ids must remain unique",
        )

    report = DQReportRecord.seal(
        schema_name="dq_report",
        schema_version="1.0.0",
        run_id=source_record.run_id,
        record_id=record_id,
        created_at_utc=created_at_utc,
        producer_version=producer_version,
        repository_code_sha=source_record.repository_code_sha,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=loaded.policy_sha256,
        contract_schema_sha256=policy.shared_contract_sha256,
        source_ids=tuple(source_id for source_id, _ in source_pairs),
        source_checksums=tuple(checksum_value for _, checksum_value in source_pairs),
        requested_start=source_record.requested_start,
        requested_end=source_record.requested_end,
        evaluated_start=source_record.evaluated_start,
        evaluated_end=source_record.evaluated_end,
        storage_timezone=source_record.storage_timezone,
        exchange_timezone=source_record.exchange_timezone,
        dq_status=dq_status,
        pit_status=pit_status,
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id=lineage_id,
        safety=source_record.safety,
        scope="qqq_options_event_dq_pit_identity",
        report_version="1.0.0",
        generated_at_utc=at,
        checks=sorted_checks,
    )
    return QQQOptionsDQIdentityEvaluation(
        policy_sha256=loaded.policy_sha256,
        cache_identity=cache_identity,
        local_cached_data_gate=observation.local_cached_data_gate,
        report=report,
    )


__all__ = [
    "AssessmentStatus",
    "ChecksumAvailability",
    "DEFAULT_QQQ_OPTIONS_DQ_PIT_IDENTITY_POLICY_PATH",
    "IdentityBinding",
    "LocalCacheDQStatus",
    "LocalCachedDataGateDeclaration",
    "NumericThresholdPolicy",
    "QQQOptionsCacheIdentityMaterial",
    "QQQOptionsCacheIdentityReceipt",
    "QQQOptionsDQIdentityEvaluation",
    "QQQOptionsDQIdentityPolicy",
    "QQQOptionsDQIdentityPolicyLoadResult",
    "QQQOptionsDQObservation",
    "SourceChecksumEvidence",
    "build_qqq_options_cache_identity",
    "evaluate_qqq_options_dq_pit_identity",
    "load_qqq_options_dq_pit_identity_policy",
]
