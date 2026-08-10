from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import sha256_path
from ai_trading_system.qqq_options_research.owner_decision_manifest import (
    OwnerDecisionAction,
    OwnerDecisionCanonicalGroup,
    OwnerDecisionGroupChoice,
    OwnerDecisionNotApplicableRationale,
    OwnerDecisionSlotChoice,
    OwnerReviewedPolicyValue,
    QQQOptionsOwnerDecisionManifest,
    QQQOptionsOwnerDecisionManifestContractError,
    QQQOptionsOwnerDecisionResolutionResult,
    build_qqq_options_owner_decision_manifest,
    load_qqq_options_owner_decision_manifest_policy,
    resolve_qqq_options_owner_decision_manifest,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTIONS_OWNER_POLICY_ADOPTION_CONTRACT_PATH = Path(
    "config/research/qqq_options_owner_policy_adoption_contract_v1.yaml"
)

_PRIMARY_RESEARCH_START = date(2021, 2, 22)
_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")
_OWNER_DECISION_PATTERN = re.compile(
    r"^owner_decision:TRADING-2502:(\d{4}-\d{2}-\d{2}):"
    r"review_qqq_options_backtest_policy_decision_pack_v1$"
)

_EXPECTED_AMENDMENT_IDS = (
    "SPLIT_LIFE_CLOSE_HOLD_ROLL",
    "SPLIT_LIFE_EXERCISE_ASSIGNMENT",
    "SPLIT_ACC_IDENTITY_ROUNDING",
    "SPLIT_ACC_SETTLEMENT_COST_BASIS",
    "EXPLICIT_QUOTE_OBSERVATION_IDENTITIES",
    "ADD_TERMINAL_VALUATION_RESULT_INCLUSION_DEPENDENCY",
    "ADD_LIFE_POSITION_STATE_TRANSITION",
    "ADD_EXE_EXECUTION_OBSERVATION_SOURCE",
    "ADD_ACC_CASH_CARRY_BENCHMARK",
    "ADD_ACC_METRIC_BENCHMARK_IDENTITY",
    "ADD_ACC_RESEARCH_MULTIPLICITY_CONTROL",
)


class OwnerDecisionAttestationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class SlotCatalogAmendmentDisposition(StrEnum):
    OWNER_REVIEW_REQUIRED = "OWNER_REVIEW_REQUIRED"
    ACCEPTED_FOR_VERSIONED_SUCCESSOR = "ACCEPTED_FOR_VERSIONED_SUCCESSOR"
    REJECTED_WITH_RATIONALE = "REJECTED_WITH_RATIONALE"


class PolicyAdoptionSlotState(StrEnum):
    UNRESOLVED_BLOCKED = "UNRESOLVED_BLOCKED"
    OWNER_REVIEWED_VALUE_BOUND = "OWNER_REVIEWED_VALUE_BOUND"
    EVIDENCE_CALIBRATION_REQUIRED = "EVIDENCE_CALIBRATION_REQUIRED"
    SENSITIVITY_ONLY_NOT_REALITY_BASELINE = "SENSITIVITY_ONLY_NOT_REALITY_BASELINE"
    NOT_APPLICABLE_WITH_REVIEWED_RATIONALE = "NOT_APPLICABLE_WITH_REVIEWED_RATIONALE"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


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
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None:
        raise ValueError(f"{field} must be timezone-aware")
    if offset.total_seconds() != 0:
        raise ValueError(f"{field} must use UTC offset")
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


def _duplicate_key_rejecting_json(raw: bytes) -> object:
    def pairs_hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> object:
        raise ValueError(f"non-finite JSON constant is prohibited: {value}")

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("record is not valid UTF-8") from exc
    try:
        return json.loads(
            text,
            object_pairs_hook=pairs_hook,
            parse_constant=reject_constant,
        )
    except json.JSONDecodeError as exc:
        raise ValueError("record is not valid JSON") from exc


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
    return resolved


class _SealedContractModel(_StrictModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _validate_content_sha256(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _canonical_sha256(self.semantic_payload())

    @model_validator(mode="after")
    def _validate_content_seal(self) -> Self:
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
    def _normalize_seal_payload(cls, payload: dict[str, Any]) -> dict[str, Any]:
        return payload

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        if "content_sha256" in payload:
            raise OwnerDecisionAttestationError(
                "OWNER_ATTESTATION_PAYLOAD_MISMATCH",
                "seal computes content_sha256 and rejects caller-supplied values",
            )
        normalized = cls._normalize_seal_payload(dict(payload))
        provisional = cls.model_construct(
            **normalized,
            content_sha256=_UNSEALED_SHA256,
        )
        return cls.model_validate(
            {
                **normalized,
                "content_sha256": provisional.compute_content_sha256(),
            }
        )

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            decoded = _duplicate_key_rejecting_json(raw)
            if not isinstance(decoded, dict):
                raise ValueError("record JSON root must be an object")
            record = cls.model_validate_json(raw)
        except (TypeError, ValueError) as exc:
            raise OwnerDecisionAttestationError(
                "OWNER_ATTESTATION_PAYLOAD_MISMATCH",
                str(exc),
            ) from exc
        if record.canonical_bytes != raw:
            raise OwnerDecisionAttestationError(
                "OWNER_ATTESTATION_NOT_CANONICAL",
                "record bytes do not match canonical UTF-8/LF JSON",
            )
        return record


class SlotCatalogAmendmentResolution(_StrictModel):
    amendment_id: str
    disposition: SlotCatalogAmendmentDisposition
    rationale: str
    successor_catalog_version: str | None

    @field_validator("amendment_id")
    @classmethod
    def _validate_amendment_id(cls, value: str) -> str:
        return _identifier(value, "amendment_id")

    @field_validator("rationale")
    @classmethod
    def _validate_rationale(cls, value: str) -> str:
        return _required_text(value, "rationale")

    @field_validator("successor_catalog_version")
    @classmethod
    def _validate_successor_version(cls, value: str | None) -> str | None:
        return None if value is None else _identifier(value, "successor_catalog_version")

    @model_validator(mode="after")
    def _validate_disposition(self) -> Self:
        accepted = (
            self.disposition is SlotCatalogAmendmentDisposition.ACCEPTED_FOR_VERSIONED_SUCCESSOR
        )
        if accepted != (self.successor_catalog_version is not None):
            raise ValueError("successor_catalog_version is required only for an accepted amendment")
        return self


class OwnerPolicyAdoptionUpstreamAuthority(_PolicyModel):
    pack_requirement_path: str
    pack_requirement_lf_sha256: str
    authority_set_sha256: str
    manifest_policy_path: str
    manifest_policy_file_sha256: str
    manifest_policy_canonical_sha256: str
    slot_catalog_sha256: str

    @field_validator("pack_requirement_path", "manifest_policy_path")
    @classmethod
    def _validate_paths(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator(
        "pack_requirement_lf_sha256",
        "authority_set_sha256",
        "manifest_policy_file_sha256",
        "manifest_policy_canonical_sha256",
        "slot_catalog_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class SlotCatalogAmendmentPolicy(_PolicyModel):
    amendment_id: str
    affected_slot_ids: tuple[str, ...]
    proposal: str

    @field_validator("amendment_id")
    @classmethod
    def _validate_id(cls, value: str) -> str:
        return _identifier(value, "amendment_id")

    @field_validator("affected_slot_ids")
    @classmethod
    def _validate_slots(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(set(value)) != len(value):
            raise ValueError("affected_slot_ids must be non-empty and unique")
        return tuple(_identifier(item, "affected_slot_id") for item in value)

    @field_validator("proposal")
    @classmethod
    def _validate_proposal(cls, value: str) -> str:
        return _required_text(value, "proposal")


class OwnerPolicyAdoptionSafety(_PolicyModel):
    maximum_adoption_status: Literal["VALID_POLICY_ADOPTION_CONTRACT_ONLY"]
    dq_pit_status: Literal["NOT_EVALUATED_BY_THIS_CONTRACT"]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    investment_interpretation_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QQQOptionsOwnerPolicyAdoptionContractPolicy(_PolicyModel):
    schema_version: Literal["qqq_options_owner_policy_adoption_contract_policy.v1"]
    policy_id: Literal["qqq_options_owner_policy_adoption_contract"]
    policy_version: Literal["1.0.0"]
    policy_status: Literal["CONTRACT_ONLY_BLOCKED_OWNER_INPUT"]
    expected_owner_id: Literal["project_owner"]
    attestation_schema_version: Literal["qqq_options_owner_decision_attestation.v1"]
    primary_research_start: date
    frozen_slot_count: Literal[28]
    upstream_authority: OwnerPolicyAdoptionUpstreamAuthority
    slot_catalog_amendments: tuple[SlotCatalogAmendmentPolicy, ...]
    safety: OwnerPolicyAdoptionSafety

    @model_validator(mode="after")
    def _validate_exact_policy(self) -> Self:
        if self.primary_research_start != _PRIMARY_RESEARCH_START:
            raise ValueError("primary research start must remain 2021-02-22")
        amendment_ids = tuple(item.amendment_id for item in self.slot_catalog_amendments)
        if amendment_ids != _EXPECTED_AMENDMENT_IDS:
            raise ValueError("slot catalog amendment inventory or order drifted")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))


@dataclass(frozen=True)
class QQQOptionsOwnerPolicyAdoptionContractPolicyLoadResult:
    policy: QQQOptionsOwnerPolicyAdoptionContractPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str


class OwnerDecisionAttestationRecord(_SealedContractModel):
    schema_version: Literal["qqq_options_owner_decision_attestation.v1"]
    record_id: str
    created_at_utc: datetime
    repository_code_sha: str
    owner_decision_id: str
    decision_date: date
    owner_id: str
    independent_reviewer_id: str
    pack_requirement_lf_sha256: str
    authority_set_sha256: str
    manifest_policy_file_sha256: str
    manifest_policy_canonical_sha256: str
    slot_catalog_sha256: str
    group_choices: tuple[OwnerDecisionGroupChoice, ...]
    slot_choices: tuple[OwnerDecisionSlotChoice, ...]
    owner_policy_values: tuple[OwnerReviewedPolicyValue, ...]
    not_applicable_rationales: tuple[OwnerDecisionNotApplicableRationale, ...]
    amendment_resolutions: tuple[SlotCatalogAmendmentResolution, ...]
    confirmed_no_engine_activation: Literal[True]
    confirmed_no_external_action: Literal[True]

    @field_validator("record_id", "owner_id", "independent_reviewer_id")
    @classmethod
    def _validate_identifiers(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("created_at_utc")
    @classmethod
    def _validate_created_at(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator("owner_decision_id")
    @classmethod
    def _validate_owner_decision_id(cls, value: str) -> str:
        return _required_text(value, "owner_decision_id")

    @field_validator(
        "pack_requirement_lf_sha256",
        "authority_set_sha256",
        "manifest_policy_file_sha256",
        "manifest_policy_canonical_sha256",
        "slot_catalog_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @classmethod
    def _normalize_seal_payload(cls, payload: dict[str, Any]) -> dict[str, Any]:
        tuple_sorters = {
            "group_choices": lambda item: item.canonical_group.value,
            "slot_choices": lambda item: item.slot_id,
            "owner_policy_values": lambda item: item.slot_id,
            "not_applicable_rationales": lambda item: item.slot_id,
            "amendment_resolutions": lambda item: item.amendment_id,
        }
        for field, key in tuple_sorters.items():
            if field in payload:
                payload[field] = tuple(sorted(payload[field], key=key))
        return payload

    @model_validator(mode="after")
    def _validate_exact_inventory(self) -> Self:
        token_match = _OWNER_DECISION_PATTERN.fullmatch(self.owner_decision_id)
        if token_match is None:
            raise ValueError("owner_decision_id does not match the 2502 review token")
        if date.fromisoformat(token_match.group(1)) != self.decision_date:
            raise ValueError("Owner decision token and decision_date differ")
        inventories = {
            "group_choices": tuple(item.canonical_group.value for item in self.group_choices),
            "slot_choices": tuple(item.slot_id for item in self.slot_choices),
            "owner_policy_values": tuple(item.slot_id for item in self.owner_policy_values),
            "not_applicable_rationales": tuple(
                item.slot_id for item in self.not_applicable_rationales
            ),
            "amendment_resolutions": tuple(
                item.amendment_id for item in self.amendment_resolutions
            ),
        }
        for field, values in inventories.items():
            if len(values) != len(set(values)):
                raise ValueError(f"{field} contains duplicate identities")
            if values != tuple(sorted(values)):
                raise ValueError(f"{field} must use canonical identity order")
        return self


@dataclass(frozen=True)
class OwnerDecisionAttestationLoadResult:
    attestation: OwnerDecisionAttestationRecord
    attestation_bytes: bytes
    raw_byte_sha256: str
    semantic_content_sha256: str
    manifest: QQQOptionsOwnerDecisionManifest
    manifest_resolution: QQQOptionsOwnerDecisionResolutionResult
    adoption_policy_file_sha256: str
    adoption_policy_canonical_sha256: str


class PolicyAdoptionSlotPlan(_StrictModel):
    slot_id: str
    canonical_group: OwnerDecisionCanonicalGroup
    owner_action: OwnerDecisionAction
    adoption_state: PolicyAdoptionSlotState
    owner_policy_value_sha256: str | None
    not_applicable_rationale_sha256: str | None

    @field_validator("slot_id")
    @classmethod
    def _validate_slot_id(cls, value: str) -> str:
        return _identifier(value, "slot_id")

    @field_validator("owner_policy_value_sha256", "not_applicable_rationale_sha256")
    @classmethod
    def _validate_optional_hash(
        cls,
        value: str | None,
        info: ValidationInfo,
    ) -> str | None:
        return None if value is None else _sha256(value, str(info.field_name))


class QQQOptionsPolicyAdoptionPlan(_SealedContractModel):
    schema_version: Literal["qqq_options_policy_adoption_plan.v1"]
    repository_code_sha: str
    attestation_raw_byte_sha256: str
    attestation_content_sha256: str
    attestation_canonical_sha256: str
    manifest_content_sha256: str
    manifest_canonical_sha256: str
    manifest_resolution_content_sha256: str
    adoption_policy_file_sha256: str
    adoption_policy_canonical_sha256: str
    slot_catalog_sha256: str
    frozen_slot_count: Literal[28]
    research_window_role: Literal["PRIMARY"]
    requested_start: date
    evaluated_start: date
    slot_plans: tuple[PolicyAdoptionSlotPlan, ...]
    amendment_resolutions: tuple[SlotCatalogAmendmentResolution, ...]
    maximum_adoption_status: Literal["VALID_POLICY_ADOPTION_CONTRACT_ONLY"]
    executable_policy_authorized: Literal[False]
    dq_pit_status: Literal["NOT_EVALUATED_BY_THIS_CONTRACT"]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    investment_interpretation_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "attestation_raw_byte_sha256",
        "attestation_content_sha256",
        "attestation_canonical_sha256",
        "manifest_content_sha256",
        "manifest_canonical_sha256",
        "manifest_resolution_content_sha256",
        "adoption_policy_file_sha256",
        "adoption_policy_canonical_sha256",
        "slot_catalog_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_exact_plan(self) -> Self:
        if self.requested_start != _PRIMARY_RESEARCH_START:
            raise ValueError("requested_start must remain 2021-02-22")
        if self.evaluated_start != _PRIMARY_RESEARCH_START:
            raise ValueError("evaluated_start must remain 2021-02-22")
        ids = tuple(item.slot_id for item in self.slot_plans)
        if len(ids) != 28 or len(set(ids)) != 28:
            raise ValueError("slot_plans must contain exactly 28 unique slots")
        if ids != tuple(sorted(ids)):
            raise ValueError("slot_plans must use canonical identity order")
        amendment_ids = tuple(item.amendment_id for item in self.amendment_resolutions)
        if amendment_ids != tuple(sorted(_EXPECTED_AMENDMENT_IDS)):
            raise ValueError("amendment resolution inventory drifted")
        return self


class QQQOptionsPolicyAdoptionResolution(_SealedContractModel):
    schema_version: Literal["qqq_options_policy_adoption_resolution.v1"]
    validation_status: Literal["VALID_POLICY_ADOPTION_CONTRACT_ONLY"]
    repository_code_sha: str
    plan_content_sha256: str
    plan_canonical_sha256: str
    attestation_raw_byte_sha256: str
    adoption_policy_file_sha256: str
    adoption_policy_canonical_sha256: str
    frozen_slot_count: Literal[28]
    executable_policy_authorized: Literal[False]
    owner_input_blocker_cleared: Literal[False]
    dq_pit_status: Literal["NOT_EVALUATED_BY_THIS_CONTRACT"]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    investment_interpretation_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "plan_content_sha256",
        "plan_canonical_sha256",
        "attestation_raw_byte_sha256",
        "adoption_policy_file_sha256",
        "adoption_policy_canonical_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


def load_qqq_options_owner_policy_adoption_contract(
    path: Path = DEFAULT_QQQ_OPTIONS_OWNER_POLICY_ADOPTION_CONTRACT_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsOwnerPolicyAdoptionContractPolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _require_bound_regular_file(
            path,
            project_root=root,
            field="Owner policy adoption contract",
        )
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("Owner policy adoption contract root must be a mapping")
        policy = QQQOptionsOwnerPolicyAdoptionContractPolicy.model_validate(payload)
        upstream = load_qqq_options_owner_decision_manifest_policy(
            Path(policy.upstream_authority.manifest_policy_path),
            project_root=root,
        )
        declared = policy.upstream_authority
        mismatches = {
            "manifest_policy_file_sha256": (
                declared.manifest_policy_file_sha256,
                upstream.policy_file_sha256,
            ),
            "manifest_policy_canonical_sha256": (
                declared.manifest_policy_canonical_sha256,
                upstream.policy_canonical_sha256,
            ),
            "slot_catalog_sha256": (
                declared.slot_catalog_sha256,
                upstream.slot_catalog_sha256,
            ),
            "pack_requirement_path": (
                declared.pack_requirement_path,
                upstream.policy.pack_requirement_path,
            ),
            "pack_requirement_lf_sha256": (
                declared.pack_requirement_lf_sha256,
                upstream.policy.pack_requirement_lf_sha256,
            ),
            "authority_set_sha256": (
                declared.authority_set_sha256,
                upstream.policy.authority_set_sha256,
            ),
        }
        drifted = [field for field, (actual, expected) in mismatches.items() if actual != expected]
        if drifted:
            raise ValueError(f"upstream authority binding mismatch: {drifted}")
        upstream_slot_ids = {slot.slot_id for slot in upstream.policy.slots}
        unknown = {
            slot_id
            for amendment in policy.slot_catalog_amendments
            for slot_id in amendment.affected_slot_ids
            if slot_id not in upstream_slot_ids
        }
        if unknown:
            raise ValueError(f"amendments reference unknown v1 slots: {sorted(unknown)}")
    except OwnerDecisionAttestationError:
        raise
    except (OSError, TypeError, ValueError, QQQOptionsOwnerDecisionManifestContractError) as exc:
        raise OwnerDecisionAttestationError(
            "AUTHORITY_BINDING_MISMATCH",
            str(exc),
        ) from exc
    return QQQOptionsOwnerPolicyAdoptionContractPolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_file_sha256=sha256_path(policy_path),
        policy_canonical_sha256=policy.canonical_sha256,
    )


def _map_upstream_error(exc: QQQOptionsOwnerDecisionManifestContractError) -> str:
    if "G2" in exc.code:
        return "G2_METADATA_INCOMPLETE"
    if "G5" in exc.code:
        return "G5_RATIONALE_INCOMPLETE"
    if any(token in exc.code for token in ("SLOT", "GROUP", "INVENTORY")):
        return "SLOT_INVENTORY_INVALID"
    if "BINDING" in exc.code or "POLICY" in exc.code:
        return "AUTHORITY_BINDING_MISMATCH"
    return "OWNER_ATTESTATION_PAYLOAD_MISMATCH"


def load_owner_decision_attestation(
    raw: bytes | None,
    *,
    expected_repository_code_sha: str,
    project_root: Path = PROJECT_ROOT,
) -> OwnerDecisionAttestationLoadResult:
    if raw is None or not raw:
        raise OwnerDecisionAttestationError(
            "OWNER_ATTESTATION_MISSING",
            "canonical Owner attestation bytes are required",
        )
    expected_sha = _git_sha(
        expected_repository_code_sha,
        "expected_repository_code_sha",
    )
    attestation = OwnerDecisionAttestationRecord.from_json_bytes(raw)
    loaded = load_qqq_options_owner_policy_adoption_contract(project_root=project_root)
    upstream = loaded.policy.upstream_authority
    binding_pairs = {
        "repository_code_sha": (attestation.repository_code_sha, expected_sha),
        "owner_id": (attestation.owner_id, loaded.policy.expected_owner_id),
        "independent_reviewer_id": (
            attestation.independent_reviewer_id,
            loaded.policy.expected_owner_id,
        ),
        "pack_requirement_lf_sha256": (
            attestation.pack_requirement_lf_sha256,
            upstream.pack_requirement_lf_sha256,
        ),
        "authority_set_sha256": (
            attestation.authority_set_sha256,
            upstream.authority_set_sha256,
        ),
        "manifest_policy_file_sha256": (
            attestation.manifest_policy_file_sha256,
            upstream.manifest_policy_file_sha256,
        ),
        "manifest_policy_canonical_sha256": (
            attestation.manifest_policy_canonical_sha256,
            upstream.manifest_policy_canonical_sha256,
        ),
        "slot_catalog_sha256": (
            attestation.slot_catalog_sha256,
            upstream.slot_catalog_sha256,
        ),
    }
    drifted = [field for field, (actual, expected) in binding_pairs.items() if actual != expected]
    if drifted:
        code = (
            "OWNER_IDENTITY_NOT_BOUND"
            if {"owner_id", "independent_reviewer_id"}.intersection(drifted)
            else "AUTHORITY_BINDING_MISMATCH"
        )
        raise OwnerDecisionAttestationError(
            code,
            f"attestation binding mismatch: {drifted}",
        )
    observed_amendments = tuple(item.amendment_id for item in attestation.amendment_resolutions)
    if observed_amendments != tuple(sorted(_EXPECTED_AMENDMENT_IDS)):
        raise OwnerDecisionAttestationError(
            "CATALOG_AMENDMENT_REQUIRED",
            "attestation must resolve the exact versioned amendment proposal inventory",
        )
    try:
        manifest = build_qqq_options_owner_decision_manifest(
            record_id=f"{attestation.record_id}.manifest",
            created_at_utc=attestation.created_at_utc,
            repository_code_sha=attestation.repository_code_sha,
            owner_decision_id=attestation.owner_decision_id,
            decision_date=attestation.decision_date,
            independent_reviewer_id=attestation.independent_reviewer_id,
            group_choices=attestation.group_choices,
            slot_choices=attestation.slot_choices,
            owner_policy_values=attestation.owner_policy_values,
            not_applicable_rationales=attestation.not_applicable_rationales,
            project_root=project_root,
        )
        manifest_resolution = resolve_qqq_options_owner_decision_manifest(
            manifest.canonical_bytes,
            expected_repository_code_sha=expected_sha,
            project_root=project_root,
        )
    except QQQOptionsOwnerDecisionManifestContractError as exc:
        raise OwnerDecisionAttestationError(
            _map_upstream_error(exc),
            exc.message,
        ) from exc
    return OwnerDecisionAttestationLoadResult(
        attestation=attestation,
        attestation_bytes=bytes(raw),
        raw_byte_sha256=hashlib.sha256(raw).hexdigest(),
        semantic_content_sha256=attestation.content_sha256,
        manifest=manifest,
        manifest_resolution=manifest_resolution,
        adoption_policy_file_sha256=loaded.policy_file_sha256,
        adoption_policy_canonical_sha256=loaded.policy_canonical_sha256,
    )


_STATE_BY_ACTION = {
    OwnerDecisionAction.G1: PolicyAdoptionSlotState.UNRESOLVED_BLOCKED,
    OwnerDecisionAction.G2: PolicyAdoptionSlotState.OWNER_REVIEWED_VALUE_BOUND,
    OwnerDecisionAction.G3: PolicyAdoptionSlotState.EVIDENCE_CALIBRATION_REQUIRED,
    OwnerDecisionAction.G4: (PolicyAdoptionSlotState.SENSITIVITY_ONLY_NOT_REALITY_BASELINE),
    OwnerDecisionAction.G5: (PolicyAdoptionSlotState.NOT_APPLICABLE_WITH_REVIEWED_RATIONALE),
}


def _model_sha256(model: BaseModel) -> str:
    return _canonical_sha256(model.model_dump(mode="json"))


def build_policy_adoption_plan(
    admission: OwnerDecisionAttestationLoadResult,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsPolicyAdoptionPlan:
    verified = load_owner_decision_attestation(
        admission.attestation_bytes,
        expected_repository_code_sha=admission.attestation.repository_code_sha,
        project_root=project_root,
    )
    if verified.raw_byte_sha256 != admission.raw_byte_sha256:
        raise OwnerDecisionAttestationError(
            "OWNER_ATTESTATION_PAYLOAD_MISMATCH",
            "admission raw byte identity does not match replay",
        )
    loaded = load_qqq_options_owner_policy_adoption_contract(project_root=project_root)
    manifest = verified.manifest
    actions = {item.slot_id: item for item in manifest.materialized_decisions}
    values = {item.slot_id: item for item in manifest.owner_policy_values}
    rationales = {item.slot_id: item for item in manifest.not_applicable_rationales}
    slot_plans = tuple(
        PolicyAdoptionSlotPlan(
            slot_id=slot_id,
            canonical_group=actions[slot_id].canonical_group,
            owner_action=actions[slot_id].action,
            adoption_state=_STATE_BY_ACTION[actions[slot_id].action],
            owner_policy_value_sha256=(
                _model_sha256(values[slot_id]) if slot_id in values else None
            ),
            not_applicable_rationale_sha256=(
                _model_sha256(rationales[slot_id]) if slot_id in rationales else None
            ),
        )
        for slot_id in sorted(actions)
    )
    return QQQOptionsPolicyAdoptionPlan.seal(
        schema_version="qqq_options_policy_adoption_plan.v1",
        repository_code_sha=manifest.repository_code_sha,
        attestation_raw_byte_sha256=verified.raw_byte_sha256,
        attestation_content_sha256=verified.attestation.content_sha256,
        attestation_canonical_sha256=verified.attestation.canonical_sha256,
        manifest_content_sha256=manifest.content_sha256,
        manifest_canonical_sha256=manifest.canonical_sha256,
        manifest_resolution_content_sha256=(verified.manifest_resolution.content_sha256),
        adoption_policy_file_sha256=loaded.policy_file_sha256,
        adoption_policy_canonical_sha256=loaded.policy_canonical_sha256,
        slot_catalog_sha256=manifest.slot_catalog_sha256,
        frozen_slot_count=28,
        research_window_role="PRIMARY",
        requested_start=_PRIMARY_RESEARCH_START,
        evaluated_start=_PRIMARY_RESEARCH_START,
        slot_plans=slot_plans,
        amendment_resolutions=verified.attestation.amendment_resolutions,
        maximum_adoption_status="VALID_POLICY_ADOPTION_CONTRACT_ONLY",
        executable_policy_authorized=False,
        dq_pit_status="NOT_EVALUATED_BY_THIS_CONTRACT",
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        orders=0,
        fills=0,
        investment_interpretation_allowed=False,
        paper_allowed=False,
        live_allowed=False,
        broker_allowed=False,
        production_effect="none",
        broker_action="none",
    )


def resolve_policy_adoption_plan(
    plan_bytes: bytes,
    *,
    expected_repository_code_sha: str,
    expected_attestation_raw_byte_sha256: str,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsPolicyAdoptionResolution:
    expected_sha = _git_sha(
        expected_repository_code_sha,
        "expected_repository_code_sha",
    )
    expected_attestation_hash = _sha256(
        expected_attestation_raw_byte_sha256,
        "expected_attestation_raw_byte_sha256",
    )
    plan = QQQOptionsPolicyAdoptionPlan.from_json_bytes(plan_bytes)
    loaded = load_qqq_options_owner_policy_adoption_contract(project_root=project_root)
    mismatches = {
        "repository_code_sha": (plan.repository_code_sha, expected_sha),
        "attestation_raw_byte_sha256": (
            plan.attestation_raw_byte_sha256,
            expected_attestation_hash,
        ),
        "adoption_policy_file_sha256": (
            plan.adoption_policy_file_sha256,
            loaded.policy_file_sha256,
        ),
        "adoption_policy_canonical_sha256": (
            plan.adoption_policy_canonical_sha256,
            loaded.policy_canonical_sha256,
        ),
        "slot_catalog_sha256": (
            plan.slot_catalog_sha256,
            loaded.policy.upstream_authority.slot_catalog_sha256,
        ),
    }
    drifted = [field for field, (actual, expected) in mismatches.items() if actual != expected]
    if drifted:
        raise OwnerDecisionAttestationError(
            "AUTHORITY_BINDING_MISMATCH",
            f"policy adoption plan binding mismatch: {drifted}",
        )
    return QQQOptionsPolicyAdoptionResolution.seal(
        schema_version="qqq_options_policy_adoption_resolution.v1",
        validation_status="VALID_POLICY_ADOPTION_CONTRACT_ONLY",
        repository_code_sha=plan.repository_code_sha,
        plan_content_sha256=plan.content_sha256,
        plan_canonical_sha256=plan.canonical_sha256,
        attestation_raw_byte_sha256=plan.attestation_raw_byte_sha256,
        adoption_policy_file_sha256=loaded.policy_file_sha256,
        adoption_policy_canonical_sha256=loaded.policy_canonical_sha256,
        frozen_slot_count=28,
        executable_policy_authorized=False,
        owner_input_blocker_cleared=False,
        dq_pit_status="NOT_EVALUATED_BY_THIS_CONTRACT",
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        orders=0,
        fills=0,
        investment_interpretation_allowed=False,
        paper_allowed=False,
        live_allowed=False,
        broker_allowed=False,
        production_effect="none",
        broker_action="none",
    )
