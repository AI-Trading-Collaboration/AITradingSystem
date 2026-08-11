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
    OwnerDecisionEvidenceClass,
    load_qqq_options_owner_decision_manifest_policy,
)
from ai_trading_system.qqq_options_research.owner_policy_adoption_contract import (
    OwnerDecisionAttestationError,
    OwnerDecisionAttestationRecord,
    SlotCatalogAmendmentDisposition,
    load_owner_decision_attestation,
    load_qqq_options_owner_policy_adoption_contract,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_V2_POLICY_PATH = Path(
    "config/research/qqq_options_owner_decision_manifest_v2.yaml"
)
DEFAULT_QQQ_OPTIONS_OWNER_DECISION_ATTESTATION_PATH = Path(
    "inputs/external_validation/qqq_options_owner_decision_attestation_20260811.json"
)

_PRIMARY_RESEARCH_START = date(2021, 2, 22)
_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")

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
_EXPECTED_REVIEW_FINDINGS = (
    "SPLIT_EXE_CANCEL_REJECT_NO_FILL",
    "SPLIT_ACC_DQ_PIT_REPRO_INVARIANT_FROM_OWNER_POLICY",
)


class QQQOptionsOwnerDecisionManifestV2ContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class CatalogMutationKind(StrEnum):
    UNCHANGED = "UNCHANGED"
    SPLIT_SUCCESSOR = "SPLIT_SUCCESSOR"
    ADDED_AXIS = "ADDED_AXIS"


class SuccessorPolicyState(StrEnum):
    INHERITED_BLOCKED_ACTION = "INHERITED_BLOCKED_ACTION"
    OWNER_ACTION_UNRESOLVED = "OWNER_ACTION_UNRESOLVED"


class ReviewFindingDisposition(StrEnum):
    OWNER_REVIEW_REQUIRED_NOT_IN_ATTESTED_INVENTORY = (
        "OWNER_REVIEW_REQUIRED_NOT_IN_ATTESTED_INVENTORY"
    )


class EvidenceDQStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    UNKNOWN = "UNKNOWN"
    NOT_EVALUATED = "NOT_EVALUATED"


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
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field} must use a timezone-aware UTC value")
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
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs_hook,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("record is not canonical UTF-8 JSON") from exc


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


class _SealedModel(_StrictModel):
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
            raise QQQOptionsOwnerDecisionManifestV2ContractError(
                "V2_PAYLOAD_MISMATCH",
                "seal computes content_sha256 and rejects caller-supplied values",
            )
        normalized = cls._normalize_seal_payload(dict(payload))
        provisional = cls.model_construct(**normalized, content_sha256=_UNSEALED_SHA256)
        return cls.model_validate(
            {**normalized, "content_sha256": provisional.compute_content_sha256()}
        )

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            decoded = _duplicate_key_rejecting_json(raw)
            if not isinstance(decoded, dict):
                raise ValueError("record JSON root must be an object")
            record = cls.model_validate_json(raw)
        except (TypeError, ValueError) as exc:
            raise QQQOptionsOwnerDecisionManifestV2ContractError(
                "V2_PAYLOAD_MISMATCH", str(exc)
            ) from exc
        if record.canonical_bytes != raw:
            raise QQQOptionsOwnerDecisionManifestV2ContractError(
                "V2_RECORD_NOT_CANONICAL",
                "record bytes do not match canonical UTF-8/LF JSON",
            )
        return record


class V2PredecessorAuthority(_PolicyModel):
    reviewed_main_sha: str
    manifest_v1_policy_path: str
    manifest_v1_policy_file_sha256: str
    manifest_v1_policy_canonical_sha256: str
    slot_catalog_v1_sha256: str
    adoption_policy_path: str
    adoption_policy_file_sha256: str
    adoption_policy_canonical_sha256: str
    attestation_path: str
    attestation_raw_sha256: str
    attestation_content_sha256: str
    attestation_canonical_sha256: str

    @field_validator("reviewed_main_sha")
    @classmethod
    def _validate_reviewed_main(cls, value: str) -> str:
        return _git_sha(value, "reviewed_main_sha")

    @field_validator("manifest_v1_policy_path", "adoption_policy_path", "attestation_path")
    @classmethod
    def _validate_paths(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator(
        "manifest_v1_policy_file_sha256",
        "manifest_v1_policy_canonical_sha256",
        "slot_catalog_v1_sha256",
        "adoption_policy_file_sha256",
        "adoption_policy_canonical_sha256",
        "attestation_raw_sha256",
        "attestation_content_sha256",
        "attestation_canonical_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class V2SplitRule(_PolicyModel):
    amendment_id: str
    source_slot_id: str
    successor_slot_ids: tuple[str, str]

    @field_validator("amendment_id", "source_slot_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("successor_slot_ids")
    @classmethod
    def _validate_successors(cls, value: tuple[str, str]) -> tuple[str, str]:
        checked = tuple(_identifier(item, "successor_slot_id") for item in value)
        if len(set(checked)) != 2:
            raise ValueError("split successors must be unique")
        return checked[0], checked[1]


class V2AddedAxisRule(_PolicyModel):
    amendment_id: str
    slot_id: str
    canonical_group: OwnerDecisionCanonicalGroup
    evidence_class: OwnerDecisionEvidenceClass
    requires: tuple[str, ...]
    blocks: tuple[str, ...]

    @field_validator("amendment_id", "slot_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("requires", "blocks")
    @classmethod
    def _validate_inventory(
        cls, value: tuple[str, ...], info: ValidationInfo
    ) -> tuple[str, ...]:
        if not value or len(value) != len(set(value)):
            raise ValueError(f"{info.field_name} must be non-empty and unique")
        return tuple(_identifier(item, str(info.field_name)) for item in value)


class V2QuoteObservationIdentityRule(_PolicyModel):
    slot_id: str
    observation_identity: Literal[
        "SELECTION_QUOTE_OBSERVATION", "EXECUTION_QUOTE_OBSERVATION"
    ]

    @field_validator("slot_id")
    @classmethod
    def _validate_slot_id(cls, value: str) -> str:
        return _identifier(value, "slot_id")


class V2DependencyAmendment(_PolicyModel):
    upstream_slot_id: str
    downstream_slot_id: str

    @field_validator("upstream_slot_id", "downstream_slot_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))


class V2ReviewFinding(_PolicyModel):
    finding_id: str
    affected_slot_ids: tuple[str, ...]
    disposition: ReviewFindingDisposition
    rationale: str

    @field_validator("finding_id")
    @classmethod
    def _validate_finding_id(cls, value: str) -> str:
        return _identifier(value, "finding_id")

    @field_validator("affected_slot_ids")
    @classmethod
    def _validate_slots(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(value) != len(set(value)):
            raise ValueError("affected_slot_ids must be non-empty and unique")
        return tuple(_identifier(item, "affected_slot_id") for item in value)

    @field_validator("rationale")
    @classmethod
    def _validate_rationale(cls, value: str) -> str:
        return _required_text(value, "rationale")


class V2EvidenceReferenceContract(_PolicyModel):
    schema_version: Literal["qqq_options_owner_policy_evidence_reference.v1"]
    required_fields: tuple[
        Literal[
            "relative_path",
            "schema_version",
            "file_sha256",
            "content_sha256",
            "requested_start",
            "requested_end",
            "evaluated_start",
            "evaluated_end",
            "as_of_session",
            "dq_status",
        ],
        ...,
    ]
    pass_required_for_admission: Literal[True]
    unknown_never_passes: Literal[True]


class V2Safety(_PolicyModel):
    maximum_status: Literal["VALID_VERSIONED_SUCCESSOR_CONTRACT_ONLY"]
    executable_policy_authorized: Literal[False]
    dq_pit_status: Literal["NOT_EVALUATED_BY_THIS_CONTRACT"]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    external_action_authorized: Literal[False]
    investment_interpretation_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QQQOptionsOwnerDecisionManifestV2Policy(_PolicyModel):
    schema_version: Literal["qqq_options_owner_decision_manifest_policy.v2"]
    policy_id: Literal["qqq_options_owner_decision_manifest_v2"]
    policy_version: Literal["2.0.0"]
    policy_status: Literal["STRUCTURAL_SUCCESSOR_CONTRACT_ONLY"]
    task_id: Literal[
        "TRADING-2509_QQQ_OPTIONS_OWNER_DECISION_SLOT_CATALOG_V2_AMENDMENT_CONTRACT_V1"
    ]
    primary_research_start: date
    primary_research_role: Literal["PRIMARY"]
    predecessor: V2PredecessorAuthority
    accepted_amendment_ids: tuple[str, ...]
    unchanged_slot_ids: tuple[str, ...]
    split_rules: tuple[V2SplitRule, ...]
    added_axis_rules: tuple[V2AddedAxisRule, ...]
    quote_observation_identity_rules: tuple[V2QuoteObservationIdentityRule, ...]
    dependency_amendments: tuple[V2DependencyAmendment, ...]
    review_findings: tuple[V2ReviewFinding, ...]
    evidence_reference_contract: V2EvidenceReferenceContract
    safety: V2Safety

    @field_validator("accepted_amendment_ids", "unchanged_slot_ids")
    @classmethod
    def _validate_ordered_ids(
        cls, value: tuple[str, ...], info: ValidationInfo
    ) -> tuple[str, ...]:
        checked = tuple(_identifier(item, str(info.field_name)) for item in value)
        if checked != tuple(sorted(checked)) or len(checked) != len(set(checked)):
            raise ValueError(f"{info.field_name} must be unique and sorted")
        return checked

    @model_validator(mode="after")
    def _validate_exact_contract(self) -> Self:
        if self.primary_research_start != _PRIMARY_RESEARCH_START:
            raise ValueError("primary research start must remain 2021-02-22")
        if self.accepted_amendment_ids != tuple(sorted(_EXPECTED_AMENDMENT_IDS)):
            raise ValueError("accepted amendment inventory drifted")
        split_amendments = tuple(rule.amendment_id for rule in self.split_rules)
        if split_amendments != tuple(sorted(split_amendments)):
            raise ValueError("split_rules must use amendment identity order")
        added_amendments = tuple(rule.amendment_id for rule in self.added_axis_rules)
        if added_amendments != tuple(sorted(added_amendments)):
            raise ValueError("added_axis_rules must use amendment identity order")
        finding_ids = tuple(item.finding_id for item in self.review_findings)
        if finding_ids != _EXPECTED_REVIEW_FINDINGS:
            raise ValueError("review finding inventory or order drifted")
        if len(self.quote_observation_identity_rules) != 2:
            raise ValueError("selection/execution quote identities must both be explicit")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))


@dataclass(frozen=True)
class QQQOptionsOwnerDecisionManifestV2PolicyLoadResult:
    policy: QQQOptionsOwnerDecisionManifestV2Policy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str
    attestation: OwnerDecisionAttestationRecord
    attestation_bytes: bytes


class OwnerPolicyEvidenceReference(_StrictModel):
    relative_path: str
    schema_version: str
    file_sha256: str
    content_sha256: str
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    as_of_session: date
    dq_status: EvidenceDQStatus

    @field_validator("relative_path", "schema_version")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("file_sha256", "content_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_dates_and_dq(self) -> Self:
        if self.requested_start > self.requested_end:
            raise ValueError("requested range is inverted")
        if self.evaluated_start > self.evaluated_end:
            raise ValueError("evaluated range is inverted")
        if self.requested_start != _PRIMARY_RESEARCH_START:
            raise ValueError("primary evidence requested_start must remain 2021-02-22")
        if self.evaluated_start != _PRIMARY_RESEARCH_START:
            raise ValueError("primary evidence evaluated_start must remain 2021-02-22")
        if self.dq_status is not EvidenceDQStatus.PASS:
            raise ValueError("only canonical DQ PASS evidence can be admitted")
        return self


class V2SlotMigrationEntry(_StrictModel):
    successor_slot_id: str
    canonical_group: OwnerDecisionCanonicalGroup
    mutation_kind: CatalogMutationKind
    source_slot_ids: tuple[str, ...]
    inherited_owner_action: OwnerDecisionAction | None
    successor_policy_state: SuccessorPolicyState
    evidence_class: OwnerDecisionEvidenceClass
    quote_observation_identity: str | None
    requires: tuple[str, ...]
    blocks: tuple[str, ...]

    @field_validator("successor_slot_id")
    @classmethod
    def _validate_slot_id(cls, value: str) -> str:
        return _identifier(value, "successor_slot_id")

    @field_validator("source_slot_ids", "requires", "blocks")
    @classmethod
    def _validate_ids(
        cls, value: tuple[str, ...], info: ValidationInfo
    ) -> tuple[str, ...]:
        if info.field_name != "source_slot_ids" and not value:
            raise ValueError(f"{info.field_name} must be non-empty")
        if len(value) != len(set(value)):
            raise ValueError(f"{info.field_name} must be unique")
        return tuple(_identifier(item, str(info.field_name)) for item in value)

    @field_validator("quote_observation_identity")
    @classmethod
    def _validate_observation_identity(cls, value: str | None) -> str | None:
        return None if value is None else _identifier(value, "quote_observation_identity")

    @model_validator(mode="after")
    def _validate_state(self) -> Self:
        added = self.mutation_kind is CatalogMutationKind.ADDED_AXIS
        if added != (self.inherited_owner_action is None):
            raise ValueError("only added axes may have no inherited Owner action")
        expected_state = (
            SuccessorPolicyState.OWNER_ACTION_UNRESOLVED
            if added
            else SuccessorPolicyState.INHERITED_BLOCKED_ACTION
        )
        if self.successor_policy_state is not expected_state:
            raise ValueError("successor policy state is inconsistent with mutation kind")
        return self


class QQQOptionsOwnerDecisionCatalogV2MigrationReceipt(_SealedModel):
    schema_version: Literal["qqq_options_owner_decision_catalog_migration_receipt.v2"]
    record_id: str
    issued_at_utc: datetime
    implementation_repository_code_sha: str
    reviewed_main_sha: str
    predecessor_catalog_version: Literal["1.0.0"]
    successor_catalog_version: Literal["2.0.0"]
    policy_file_sha256: str
    policy_canonical_sha256: str
    predecessor_slot_catalog_sha256: str
    attestation_raw_sha256: str
    attestation_content_sha256: str
    attestation_canonical_sha256: str
    accepted_amendment_ids: tuple[str, ...]
    successor_slots: tuple[V2SlotMigrationEntry, ...]
    review_findings: tuple[V2ReviewFinding, ...]
    policy_evidence: tuple[OwnerPolicyEvidenceReference, ...]
    primary_research_start: date
    primary_research_role: Literal["PRIMARY"]
    validation_status: Literal["VALID_VERSIONED_SUCCESSOR_CONTRACT_ONLY"]
    executable_policy_authorized: Literal[False]
    dq_pit_status: Literal["NOT_EVALUATED_BY_THIS_CONTRACT"]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    external_action_authorized: Literal[False]
    investment_interpretation_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("record_id")
    @classmethod
    def _validate_record_id(cls, value: str) -> str:
        return _identifier(value, "record_id")

    @field_validator("issued_at_utc")
    @classmethod
    def _validate_issued_at(cls, value: datetime) -> datetime:
        return _utc(value, "issued_at_utc")

    @field_validator("implementation_repository_code_sha", "reviewed_main_sha")
    @classmethod
    def _validate_git_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _git_sha(value, str(info.field_name))

    @field_validator(
        "policy_file_sha256",
        "policy_canonical_sha256",
        "predecessor_slot_catalog_sha256",
        "attestation_raw_sha256",
        "attestation_content_sha256",
        "attestation_canonical_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @classmethod
    def _normalize_seal_payload(cls, payload: dict[str, Any]) -> dict[str, Any]:
        if "successor_slots" in payload:
            payload["successor_slots"] = tuple(
                sorted(payload["successor_slots"], key=lambda item: item.successor_slot_id)
            )
        if "policy_evidence" in payload:
            payload["policy_evidence"] = tuple(
                sorted(payload["policy_evidence"], key=lambda item: item.relative_path)
            )
        return payload

    @model_validator(mode="after")
    def _validate_exact_receipt(self) -> Self:
        if self.accepted_amendment_ids != tuple(sorted(_EXPECTED_AMENDMENT_IDS)):
            raise ValueError("accepted amendment inventory drifted")
        slot_ids = tuple(item.successor_slot_id for item in self.successor_slots)
        if len(slot_ids) != 37 or len(set(slot_ids)) != 37:
            raise ValueError("v2 successor catalog must contain exactly 37 unique slots")
        if slot_ids != tuple(sorted(slot_ids)):
            raise ValueError("successor_slots must use canonical identity order")
        if self.policy_evidence:
            raise ValueError("Owner supplied no G2 policy values or evidence in this decision")
        if self.primary_research_start != _PRIMARY_RESEARCH_START:
            raise ValueError("primary research start must remain 2021-02-22")
        return self


class QQQOptionsOwnerDecisionCatalogV2Resolution(_SealedModel):
    schema_version: Literal["qqq_options_owner_decision_catalog_resolution.v2"]
    validation_status: Literal["VALID_VERSIONED_SUCCESSOR_CONTRACT_ONLY"]
    implementation_repository_code_sha: str
    receipt_content_sha256: str
    receipt_canonical_sha256: str
    policy_file_sha256: str
    policy_canonical_sha256: str
    attestation_raw_sha256: str
    successor_slot_count: Literal[37]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    dq_pit_status: Literal["NOT_EVALUATED_BY_THIS_CONTRACT"]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    external_action_authorized: Literal[False]
    investment_interpretation_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("implementation_repository_code_sha")
    @classmethod
    def _validate_git_sha(cls, value: str) -> str:
        return _git_sha(value, "implementation_repository_code_sha")

    @field_validator(
        "receipt_content_sha256",
        "receipt_canonical_sha256",
        "policy_file_sha256",
        "policy_canonical_sha256",
        "attestation_raw_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


def _assert_acyclic(entries: tuple[V2SlotMigrationEntry, ...]) -> None:
    slot_ids = {entry.successor_slot_id for entry in entries}
    graph = {
        entry.successor_slot_id: tuple(
            requirement for requirement in entry.requires if requirement in slot_ids
        )
        for entry in entries
    }
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visiting:
            raise ValueError(f"v2 slot dependency cycle detected at {node}")
        if node in visited:
            return
        visiting.add(node)
        for upstream in graph[node]:
            visit(upstream)
        visiting.remove(node)
        visited.add(node)

    for slot_id in sorted(slot_ids):
        visit(slot_id)


def load_qqq_options_owner_decision_manifest_v2_policy(
    path: Path = DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_V2_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsOwnerDecisionManifestV2PolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _require_bound_regular_file(
            path, project_root=root, field="Owner decision manifest v2 policy"
        )
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("Owner decision manifest v2 policy root must be a mapping")
        policy = QQQOptionsOwnerDecisionManifestV2Policy.model_validate(payload)
        predecessor = policy.predecessor
        v1 = load_qqq_options_owner_decision_manifest_policy(
            Path(predecessor.manifest_v1_policy_path), project_root=root
        )
        adoption = load_qqq_options_owner_policy_adoption_contract(
            Path(predecessor.adoption_policy_path), project_root=root
        )
        attestation_file = _require_bound_regular_file(
            Path(predecessor.attestation_path),
            project_root=root,
            field="Owner decision attestation",
        )
        attestation_bytes = attestation_file.read_bytes()
        admission = load_owner_decision_attestation(
            attestation_bytes,
            expected_repository_code_sha=predecessor.reviewed_main_sha,
            project_root=root,
        )
        actual = {
            "manifest_v1_policy_file_sha256": v1.policy_file_sha256,
            "manifest_v1_policy_canonical_sha256": v1.policy_canonical_sha256,
            "slot_catalog_v1_sha256": v1.slot_catalog_sha256,
            "adoption_policy_file_sha256": adoption.policy_file_sha256,
            "adoption_policy_canonical_sha256": adoption.policy_canonical_sha256,
            "attestation_raw_sha256": admission.raw_byte_sha256,
            "attestation_content_sha256": admission.attestation.content_sha256,
            "attestation_canonical_sha256": admission.attestation.canonical_sha256,
        }
        drifted = [
            field
            for field, observed in actual.items()
            if observed != getattr(predecessor, field)
        ]
        if drifted:
            raise ValueError(f"v2 predecessor authority binding mismatch: {drifted}")
        source_slots = {slot.slot_id for slot in v1.policy.slots}
        split_sources = {rule.source_slot_id for rule in policy.split_rules}
        if not split_sources.issubset(source_slots):
            raise ValueError("split rule references an unknown v1 slot")
        if tuple(sorted(source_slots - split_sources)) != policy.unchanged_slot_ids:
            raise ValueError("unchanged v1 slot inventory drifted")
        accepted = {
            item.amendment_id
            for item in admission.attestation.amendment_resolutions
            if item.disposition
            is SlotCatalogAmendmentDisposition.ACCEPTED_FOR_VERSIONED_SUCCESSOR
            and item.successor_catalog_version == "2.0.0"
        }
        if accepted != set(_EXPECTED_AMENDMENT_IDS):
            raise ValueError("attestation does not accept the exact v2 amendment inventory")
    except QQQOptionsOwnerDecisionManifestV2ContractError:
        raise
    except (OSError, TypeError, ValueError, OwnerDecisionAttestationError) as exc:
        raise QQQOptionsOwnerDecisionManifestV2ContractError(
            "V2_AUTHORITY_BINDING_MISMATCH", str(exc)
        ) from exc
    return QQQOptionsOwnerDecisionManifestV2PolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_file_sha256=sha256_path(policy_path),
        policy_canonical_sha256=policy.canonical_sha256,
        attestation=admission.attestation,
        attestation_bytes=attestation_bytes,
    )


def build_qqq_options_owner_decision_catalog_v2_migration_receipt(
    *,
    record_id: str,
    issued_at_utc: datetime,
    implementation_repository_code_sha: str,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsOwnerDecisionCatalogV2MigrationReceipt:
    implementation_sha = _git_sha(
        implementation_repository_code_sha, "implementation_repository_code_sha"
    )
    loaded = load_qqq_options_owner_decision_manifest_v2_policy(project_root=project_root)
    predecessor = loaded.policy.predecessor
    admission = load_owner_decision_attestation(
        loaded.attestation_bytes,
        expected_repository_code_sha=predecessor.reviewed_main_sha,
        project_root=project_root,
    )
    v1 = load_qqq_options_owner_decision_manifest_policy(
        Path(predecessor.manifest_v1_policy_path), project_root=project_root
    )
    source_by_id = {slot.slot_id: slot for slot in v1.policy.slots}
    action_by_id = {
        item.slot_id: item.action for item in admission.manifest.materialized_decisions
    }
    observation_ids = {
        rule.slot_id: rule.observation_identity
        for rule in loaded.policy.quote_observation_identity_rules
    }
    dependency_additions: dict[str, tuple[str, ...]] = {}
    for edge in loaded.policy.dependency_amendments:
        dependency_additions.setdefault(edge.downstream_slot_id, ())
        dependency_additions[edge.downstream_slot_id] += (edge.upstream_slot_id,)

    entries: list[V2SlotMigrationEntry] = []
    for slot_id in loaded.policy.unchanged_slot_ids:
        source = source_by_id[slot_id]
        requires = tuple(source.requires) + dependency_additions.get(slot_id, ())
        entries.append(
            V2SlotMigrationEntry(
                successor_slot_id=slot_id,
                canonical_group=source.canonical_group,
                mutation_kind=CatalogMutationKind.UNCHANGED,
                source_slot_ids=(slot_id,),
                inherited_owner_action=action_by_id[slot_id],
                successor_policy_state=SuccessorPolicyState.INHERITED_BLOCKED_ACTION,
                evidence_class=source.evidence_class,
                quote_observation_identity=observation_ids.get(slot_id),
                requires=tuple(dict.fromkeys(requires)),
                blocks=source.blocks,
            )
        )
    for split_rule in loaded.policy.split_rules:
        source = source_by_id[split_rule.source_slot_id]
        for successor_id in split_rule.successor_slot_ids:
            entries.append(
                V2SlotMigrationEntry(
                    successor_slot_id=successor_id,
                    canonical_group=source.canonical_group,
                    mutation_kind=CatalogMutationKind.SPLIT_SUCCESSOR,
                    source_slot_ids=(split_rule.source_slot_id,),
                    inherited_owner_action=action_by_id[split_rule.source_slot_id],
                    successor_policy_state=SuccessorPolicyState.INHERITED_BLOCKED_ACTION,
                    evidence_class=source.evidence_class,
                    quote_observation_identity=None,
                    requires=source.requires,
                    blocks=source.blocks,
                )
            )
    for added_rule in loaded.policy.added_axis_rules:
        entries.append(
            V2SlotMigrationEntry(
                successor_slot_id=added_rule.slot_id,
                canonical_group=added_rule.canonical_group,
                mutation_kind=CatalogMutationKind.ADDED_AXIS,
                source_slot_ids=(),
                inherited_owner_action=None,
                successor_policy_state=SuccessorPolicyState.OWNER_ACTION_UNRESOLVED,
                evidence_class=added_rule.evidence_class,
                quote_observation_identity=None,
                requires=added_rule.requires,
                blocks=added_rule.blocks,
            )
        )
    canonical_entries = tuple(sorted(entries, key=lambda item: item.successor_slot_id))
    _assert_acyclic(canonical_entries)
    return QQQOptionsOwnerDecisionCatalogV2MigrationReceipt.seal(
        schema_version="qqq_options_owner_decision_catalog_migration_receipt.v2",
        record_id=record_id,
        issued_at_utc=issued_at_utc,
        implementation_repository_code_sha=implementation_sha,
        reviewed_main_sha=predecessor.reviewed_main_sha,
        predecessor_catalog_version="1.0.0",
        successor_catalog_version="2.0.0",
        policy_file_sha256=loaded.policy_file_sha256,
        policy_canonical_sha256=loaded.policy_canonical_sha256,
        predecessor_slot_catalog_sha256=predecessor.slot_catalog_v1_sha256,
        attestation_raw_sha256=predecessor.attestation_raw_sha256,
        attestation_content_sha256=predecessor.attestation_content_sha256,
        attestation_canonical_sha256=predecessor.attestation_canonical_sha256,
        accepted_amendment_ids=loaded.policy.accepted_amendment_ids,
        successor_slots=canonical_entries,
        review_findings=loaded.policy.review_findings,
        policy_evidence=(),
        primary_research_start=_PRIMARY_RESEARCH_START,
        primary_research_role="PRIMARY",
        validation_status="VALID_VERSIONED_SUCCESSOR_CONTRACT_ONLY",
        executable_policy_authorized=False,
        dq_pit_status="NOT_EVALUATED_BY_THIS_CONTRACT",
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        orders=0,
        fills=0,
        external_action_authorized=False,
        investment_interpretation_allowed=False,
        paper_allowed=False,
        live_allowed=False,
        broker_allowed=False,
        production_effect="none",
        broker_action="none",
    )


def resolve_qqq_options_owner_decision_catalog_v2_migration_receipt(
    raw: bytes,
    *,
    expected_implementation_repository_code_sha: str,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsOwnerDecisionCatalogV2Resolution:
    expected_sha = _git_sha(
        expected_implementation_repository_code_sha,
        "expected_implementation_repository_code_sha",
    )
    receipt = QQQOptionsOwnerDecisionCatalogV2MigrationReceipt.from_json_bytes(raw)
    if receipt.implementation_repository_code_sha != expected_sha:
        raise QQQOptionsOwnerDecisionManifestV2ContractError(
            "V2_AUTHORITY_BINDING_MISMATCH",
            "migration receipt repository identity does not match the expected tree",
        )
    rebuilt = build_qqq_options_owner_decision_catalog_v2_migration_receipt(
        record_id=receipt.record_id,
        issued_at_utc=receipt.issued_at_utc,
        implementation_repository_code_sha=expected_sha,
        project_root=project_root,
    )
    if rebuilt.canonical_bytes != raw:
        raise QQQOptionsOwnerDecisionManifestV2ContractError(
            "V2_MIGRATION_REPLAY_MISMATCH",
            "migration receipt does not match deterministic replay",
        )
    loaded = load_qqq_options_owner_decision_manifest_v2_policy(project_root=project_root)
    return QQQOptionsOwnerDecisionCatalogV2Resolution.seal(
        schema_version="qqq_options_owner_decision_catalog_resolution.v2",
        validation_status="VALID_VERSIONED_SUCCESSOR_CONTRACT_ONLY",
        implementation_repository_code_sha=expected_sha,
        receipt_content_sha256=receipt.content_sha256,
        receipt_canonical_sha256=receipt.canonical_sha256,
        policy_file_sha256=loaded.policy_file_sha256,
        policy_canonical_sha256=loaded.policy_canonical_sha256,
        attestation_raw_sha256=loaded.policy.predecessor.attestation_raw_sha256,
        successor_slot_count=37,
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        dq_pit_status="NOT_EVALUATED_BY_THIS_CONTRACT",
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        orders=0,
        fills=0,
        external_action_authorized=False,
        investment_interpretation_allowed=False,
        production_effect="none",
        broker_action="none",
    )
