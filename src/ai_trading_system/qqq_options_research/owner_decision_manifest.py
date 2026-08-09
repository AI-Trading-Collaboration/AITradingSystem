from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self, TypeAlias

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import sha256_path
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_POLICY_PATH = Path(
    "config/research/qqq_options_owner_decision_manifest_v1.yaml"
)

_PACK_REQUIREMENT_PATH = Path(
    "docs/requirements/"
    "TRADING-2502_QQQ_Options_Owner_Reviewed_Backtest_Policy_Decision_Pack_V1.md"
)
_PACK_REQUIREMENT_LF_SHA256 = (
    "afdcb44f44032fee958d4f6b1e8e4b56c1edb2faefa44026e16aff7153968588"
)
_AUTHORITY_SET_SHA256 = (
    "1702d50c135204f1d92405cfaf4da7c3a06dae0bb09f2095d68ea388390e687c"
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


class OwnerDecisionCanonicalGroup(StrEnum):
    SELECTION = "selection"
    EXECUTION = "execution"
    ACCOUNTING = "accounting"
    LIFECYCLE = "lifecycle"
    ACCEPTANCE = "acceptance"


class OwnerDecisionAction(StrEnum):
    G1 = "G1"
    G2 = "G2"
    G3 = "G3"
    G4 = "G4"
    G5 = "G5"


class OwnerDecisionGroupMode(StrEnum):
    G1 = "G1"
    G3 = "G3"
    G4 = "G4"
    PER_SLOT = "PER_SLOT"


class OwnerDecisionValueKind(StrEnum):
    RANGE_RULE = "RANGE_RULE"
    MODEL_RANGE_RULE = "MODEL_RANGE_RULE"
    LIMIT_RULE = "LIMIT_RULE"
    FLOOR_RULE = "FLOOR_RULE"
    FRESHNESS_RULE = "FRESHNESS_RULE"
    ORDERED_RULE = "ORDERED_RULE"
    MODEL_RULE = "MODEL_RULE"
    DURATION_RULE = "DURATION_RULE"
    FRACTION_RULE = "FRACTION_RULE"
    DISPOSITION_RULE = "DISPOSITION_RULE"
    SCHEDULE_RULE = "SCHEDULE_RULE"
    MONEY_RULE = "MONEY_RULE"
    EXPOSURE_RULE = "EXPOSURE_RULE"
    RESERVATION_RULE = "RESERVATION_RULE"
    ROUNDING_RULE = "ROUNDING_RULE"
    SETTLEMENT_RULE = "SETTLEMENT_RULE"
    EXIT_RULE = "EXIT_RULE"
    VALUATION_RULE = "VALUATION_RULE"
    INCLUSION_RULE = "INCLUSION_RULE"
    COVERAGE_RULE = "COVERAGE_RULE"
    GATE_RULE = "GATE_RULE"


class OwnerDecisionEvidenceClass(StrEnum):
    DERIVED_MARKET_AGGREGATES = "DERIVED_MARKET_AGGREGATES"
    EXECUTION_SENSITIVITY = "EXECUTION_SENSITIVITY"
    ACCOUNTING_AUTHORITY = "ACCOUNTING_AUTHORITY"
    LIFECYCLE_EVENT_EVIDENCE = "LIFECYCLE_EVENT_EVIDENCE"
    ACCEPTANCE_VALIDATION_EVIDENCE = "ACCEPTANCE_VALIDATION_EVIDENCE"


class OwnerDecisionResolutionStatus(StrEnum):
    VALID_CONTRACT_ONLY_OWNER_DECISION = "VALID_CONTRACT_ONLY_OWNER_DECISION"


JSONScalar: TypeAlias = str | int | float | bool
DependencyStatus: TypeAlias = Literal[
    "NOT_EVALUATED_BY_THIS_CONTRACT",
    "BLOCKED_BY_UNRESOLVED_UPSTREAM",
    "FORMALLY_DECIDED_NOT_ENGINE_AUTHORIZED",
]

_EXPECTED_GROUPS = (
    OwnerDecisionCanonicalGroup.SELECTION,
    OwnerDecisionCanonicalGroup.EXECUTION,
    OwnerDecisionCanonicalGroup.ACCOUNTING,
    OwnerDecisionCanonicalGroup.LIFECYCLE,
    OwnerDecisionCanonicalGroup.ACCEPTANCE,
)
_EXPECTED_SLOT_GROUPS: tuple[tuple[str, OwnerDecisionCanonicalGroup], ...] = (
    ("SEL_DTE_WINDOW", OwnerDecisionCanonicalGroup.SELECTION),
    ("SEL_MONEYNESS_RANGE", OwnerDecisionCanonicalGroup.SELECTION),
    ("SEL_DELTA_SOURCE_RANGE", OwnerDecisionCanonicalGroup.SELECTION),
    ("SEL_SPREAD_LIMIT", OwnerDecisionCanonicalGroup.SELECTION),
    ("SEL_OPEN_INTEREST_FLOOR", OwnerDecisionCanonicalGroup.SELECTION),
    ("SEL_VOLUME_FLOOR", OwnerDecisionCanonicalGroup.SELECTION),
    ("SEL_QUOTE_FRESHNESS", OwnerDecisionCanonicalGroup.SELECTION),
    ("SEL_RANK_PRIORITY", OwnerDecisionCanonicalGroup.SELECTION),
    ("EXE_MARKETABLE_LIMIT", OwnerDecisionCanonicalGroup.EXECUTION),
    ("EXE_SLIPPAGE", OwnerDecisionCanonicalGroup.EXECUTION),
    ("EXE_LATENCY", OwnerDecisionCanonicalGroup.EXECUTION),
    ("EXE_PARTIAL_FILL", OwnerDecisionCanonicalGroup.EXECUTION),
    ("EXE_CANCEL_REJECT_NO_FILL", OwnerDecisionCanonicalGroup.EXECUTION),
    ("EXE_QUOTE_DISPOSITION", OwnerDecisionCanonicalGroup.EXECUTION),
    ("ACC_FEE_SCHEDULE", OwnerDecisionCanonicalGroup.ACCOUNTING),
    ("ACC_INITIAL_CASH", OwnerDecisionCanonicalGroup.ACCOUNTING),
    ("ACC_SIZING_EXPOSURE", OwnerDecisionCanonicalGroup.ACCOUNTING),
    ("ACC_CASH_RESERVATION", OwnerDecisionCanonicalGroup.ACCOUNTING),
    ("ACC_IDENTITY_ROUNDING", OwnerDecisionCanonicalGroup.ACCOUNTING),
    ("ACC_SETTLEMENT_COST_BASIS", OwnerDecisionCanonicalGroup.ACCOUNTING),
    ("LIFE_EXPIRY_EXIT_GUARD", OwnerDecisionCanonicalGroup.LIFECYCLE),
    ("LIFE_EXERCISE_ASSIGNMENT", OwnerDecisionCanonicalGroup.LIFECYCLE),
    ("LIFE_CLOSE_HOLD_ROLL", OwnerDecisionCanonicalGroup.LIFECYCLE),
    ("LIFE_TERMINAL_VALUATION", OwnerDecisionCanonicalGroup.LIFECYCLE),
    ("ACC_RESULT_INCLUSION", OwnerDecisionCanonicalGroup.ACCEPTANCE),
    ("ACC_SAMPLE_COVERAGE", OwnerDecisionCanonicalGroup.ACCEPTANCE),
    ("ACC_DQ_PIT_REPRO", OwnerDecisionCanonicalGroup.ACCEPTANCE),
    ("ACC_INVESTMENT_PROMOTION", OwnerDecisionCanonicalGroup.ACCEPTANCE),
)
_EXPECTED_ACTION_SEMANTICS = (
    (OwnerDecisionAction.G1, "KEEP_UNRESOLVED_BLOCKED", False, False),
    (OwnerDecisionAction.G2, "OWNER_SUPPLIED_REVIEWED_POLICY", True, False),
    (OwnerDecisionAction.G3, "EVIDENCE_CALIBRATION_REQUIRED", False, False),
    (OwnerDecisionAction.G4, "SENSITIVITY_ONLY_NOT_REALITY_BASELINE", False, False),
    (OwnerDecisionAction.G5, "NOT_APPLICABLE_WITH_REVIEWED_RATIONALE", False, True),
)
_EXPECTED_DEPENDENCY_EDGES = (
    ("DQ_PIT_AUTHORITY", OwnerDecisionCanonicalGroup.SELECTION),
    ("selection", OwnerDecisionCanonicalGroup.EXECUTION),
    ("execution", OwnerDecisionCanonicalGroup.ACCOUNTING),
    ("accounting", OwnerDecisionCanonicalGroup.LIFECYCLE),
    ("DQ_PIT_AUTHORITY", OwnerDecisionCanonicalGroup.ACCEPTANCE),
    ("accounting", OwnerDecisionCanonicalGroup.ACCEPTANCE),
    ("lifecycle", OwnerDecisionCanonicalGroup.ACCEPTANCE),
)


class QQQOptionsOwnerDecisionManifestContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


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


def _lf_sha256(raw: bytes) -> str:
    text = raw.decode("utf-8-sig").replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


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
        return json.loads(text, object_pairs_hook=pairs_hook, parse_constant=reject_constant)
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
    def seal(cls, **payload: Any) -> Self:
        if "content_sha256" in payload:
            raise QQQOptionsOwnerDecisionManifestContractError(
                "OWNER_DECISION_HASH_CALLER_SUPPLIED",
                "seal computes content_sha256 and rejects caller-supplied values",
            )
        provisional = cls.model_construct(**payload, content_sha256=_UNSEALED_SHA256)
        return cls.model_validate(
            {**payload, "content_sha256": provisional.compute_content_sha256()}
        )

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            decoded = _duplicate_key_rejecting_json(raw)
            if not isinstance(decoded, dict):
                raise ValueError("record JSON root must be an object")
            record = cls.model_validate_json(raw)
        except (TypeError, ValueError) as exc:
            raise QQQOptionsOwnerDecisionManifestContractError(
                "OWNER_DECISION_RECORD_INVALID", str(exc)
            ) from exc
        if record.canonical_bytes != raw:
            raise QQQOptionsOwnerDecisionManifestContractError(
                "OWNER_DECISION_RECORD_NOT_CANONICAL",
                "record bytes do not match canonical UTF-8/LF JSON",
            )
        return record


class OwnerDecisionValueSchemaPolicy(_PolicyModel):
    schema_id: str
    value_kind: OwnerDecisionValueKind
    required_payload_fields: tuple[str, ...]

    @field_validator("schema_id")
    @classmethod
    def _validate_schema_id(cls, value: str) -> str:
        return _identifier(value, "schema_id")

    @field_validator("required_payload_fields")
    @classmethod
    def _validate_payload_fields(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(set(value)) != len(value):
            raise ValueError("required payload fields must be non-empty and unique")
        return tuple(_identifier(item, "required_payload_field") for item in value)


class OwnerDecisionSlotPolicy(_PolicyModel):
    slot_id: str
    canonical_group: OwnerDecisionCanonicalGroup
    evidence_class: OwnerDecisionEvidenceClass
    requires: tuple[str, ...]
    blocks: tuple[str, ...]
    value_schema: OwnerDecisionValueSchemaPolicy

    @field_validator("slot_id")
    @classmethod
    def _validate_slot_id(cls, value: str) -> str:
        return _identifier(value, "slot_id")

    @field_validator("requires", "blocks")
    @classmethod
    def _validate_inventory(cls, value: tuple[str, ...], info: ValidationInfo) -> tuple[str, ...]:
        if not value or len(set(value)) != len(value):
            raise ValueError(f"{info.field_name} must be non-empty and unique")
        return tuple(_identifier(item, str(info.field_name)) for item in value)


class OwnerDecisionActionSemanticPolicy(_PolicyModel):
    action: OwnerDecisionAction
    semantic: str
    owner_value_required: bool
    reviewed_rationale_required: bool

    @field_validator("semantic")
    @classmethod
    def _validate_semantic(cls, value: str) -> str:
        return _identifier(value, "semantic")


class OwnerDecisionDependencyEdgePolicy(_PolicyModel):
    upstream_gate_id: str
    downstream_group: OwnerDecisionCanonicalGroup

    @field_validator("upstream_gate_id")
    @classmethod
    def _validate_upstream(cls, value: str) -> str:
        return _identifier(value, "upstream_gate_id")


class OwnerDecisionCorporateActionHardStop(_PolicyModel):
    authority_id: Literal["TRADING-2488_CORPORATE_ACTION_HARD_STOP"]
    status: Literal["HARD_STOP_NOT_A_DECISION_SLOT"]
    bypass_allowed: Literal[False]
    inherited_policy_sha256: Literal[
        "1798b6696e0f31571f9242a4276a06530fb951d15f250a2ef6756ac547037582"
    ]


class OwnerDecisionManifestSafety(_PolicyModel):
    engine_activation_authorized: Literal[False]
    selection_authorized: Literal[False]
    order_intent_authorized: Literal[False]
    fill_authorized: Literal[False]
    cloud_run_authorized: Literal[False]
    external_action_authorized: Literal[False]
    raw_options_export_authorized: Literal[False]
    investment_interpretation_authorized: Literal[False]
    paper_authorized: Literal[False]
    live_authorized: Literal[False]
    broker_authorized: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QQQOptionsOwnerDecisionManifestPolicy(_PolicyModel):
    schema_version: Literal["qqq_options_owner_decision_manifest_policy.v1"]
    policy_id: Literal["qqq_options_owner_decision_manifest_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_DECISION_REQUIRED_CONTRACT_ONLY"]
    owner: Literal["project_owner"]
    predecessor_task_id: Literal[
        "TRADING-2502_QQQ_OPTIONS_OWNER_REVIEWED_BACKTEST_POLICY_DECISION_PACK_V1"
    ]
    pack_requirement_path: Literal[
        "docs/requirements/"
        "TRADING-2502_QQQ_Options_Owner_Reviewed_Backtest_Policy_Decision_Pack_V1.md"
    ]
    pack_requirement_lf_sha256: Literal[
        "afdcb44f44032fee958d4f6b1e8e4b56c1edb2faefa44026e16aff7153968588"
    ]
    authority_set_sha256: Literal[
        "1702d50c135204f1d92405cfaf4da7c3a06dae0bb09f2095d68ea388390e687c"
    ]
    primary_research_start: date
    primary_research_role: Literal["PRIMARY"]
    group_order: tuple[OwnerDecisionCanonicalGroup, ...]
    allowed_group_modes: tuple[OwnerDecisionGroupMode, ...]
    action_semantics: tuple[OwnerDecisionActionSemanticPolicy, ...]
    dependency_edges: tuple[OwnerDecisionDependencyEdgePolicy, ...]
    corporate_action_hard_stop: OwnerDecisionCorporateActionHardStop
    slots: tuple[OwnerDecisionSlotPolicy, ...]
    safety: OwnerDecisionManifestSafety

    @model_validator(mode="after")
    def _validate_exact_contract(self) -> Self:
        if self.primary_research_start != _PRIMARY_RESEARCH_START:
            raise ValueError("primary research start must remain 2021-02-22")
        if self.group_order != _EXPECTED_GROUPS:
            raise ValueError("canonical group order drifted")
        expected_modes = (
            OwnerDecisionGroupMode.G1,
            OwnerDecisionGroupMode.G3,
            OwnerDecisionGroupMode.G4,
            OwnerDecisionGroupMode.PER_SLOT,
        )
        if self.allowed_group_modes != expected_modes:
            raise ValueError("allowed group modes drifted")
        semantics = tuple(
            (
                item.action,
                item.semantic,
                item.owner_value_required,
                item.reviewed_rationale_required,
            )
            for item in self.action_semantics
        )
        if semantics != _EXPECTED_ACTION_SEMANTICS:
            raise ValueError("G1-G5 semantics differ from TRADING-2502 authority")
        slot_groups = tuple((item.slot_id, item.canonical_group) for item in self.slots)
        if slot_groups != _EXPECTED_SLOT_GROUPS:
            raise ValueError("28-slot catalog or canonical group mapping drifted")
        edges = tuple(
            (item.upstream_gate_id, item.downstream_group)
            for item in self.dependency_edges
        )
        if edges != _EXPECTED_DEPENDENCY_EDGES:
            raise ValueError("cross-layer dependency DAG drifted")
        if any(
            slot.slot_id == self.corporate_action_hard_stop.authority_id
            for slot in self.slots
        ):
            raise ValueError("corporate-action hard stop cannot be a decision slot")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))

    @property
    def slot_catalog_sha256(self) -> str:
        return _canonical_sha256(
            [slot.model_dump(mode="json") for slot in self.slots]
        )


@dataclass(frozen=True)
class QQQOptionsOwnerDecisionManifestPolicyLoadResult:
    policy: QQQOptionsOwnerDecisionManifestPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str
    slot_catalog_sha256: str


class OwnerDecisionGroupChoice(_StrictModel):
    canonical_group: OwnerDecisionCanonicalGroup
    mode: OwnerDecisionGroupMode


class OwnerDecisionSlotChoice(_StrictModel):
    slot_id: str
    action: OwnerDecisionAction

    @field_validator("slot_id")
    @classmethod
    def _validate_slot_id(cls, value: str) -> str:
        return _identifier(value, "slot_id")


class OwnerDecisionMaterializedSlot(_StrictModel):
    slot_id: str
    canonical_group: OwnerDecisionCanonicalGroup
    action: OwnerDecisionAction

    @field_validator("slot_id")
    @classmethod
    def _validate_slot_id(cls, value: str) -> str:
        return _identifier(value, "slot_id")


class OwnerReviewedPolicyValue(_StrictModel):
    slot_id: str
    value_schema_id: str
    value_kind: OwnerDecisionValueKind
    payload: dict[str, JSONScalar]
    owner: str
    policy_id: str
    policy_version: str
    policy_status: Literal["OWNER_REVIEWED"]
    rationale: str
    intended_effect: str
    evidence_refs: tuple[str, ...]
    reviewed_at_utc: datetime
    review_condition: str
    expires_at_utc: datetime | None
    reviewed_no_expiry_rationale: str | None

    @field_validator("slot_id", "value_schema_id", "policy_id", "policy_version")
    @classmethod
    def _validate_identifiers(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("owner", "rationale", "intended_effect", "review_condition")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("reviewed_no_expiry_rationale")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        return None if value is None else _required_text(value, "reviewed_no_expiry_rationale")

    @field_validator("evidence_refs")
    @classmethod
    def _validate_evidence_refs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(set(value)) != len(value) or value != tuple(sorted(value)):
            raise ValueError("evidence_refs must be non-empty, unique, and sorted")
        return tuple(_identifier(item, "evidence_ref") for item in value)

    @field_validator("reviewed_at_utc", "expires_at_utc")
    @classmethod
    def _validate_times(
        cls, value: datetime | None, info: ValidationInfo
    ) -> datetime | None:
        return None if value is None else _utc(value, str(info.field_name))

    @field_validator("payload")
    @classmethod
    def _validate_payload(cls, value: dict[str, JSONScalar]) -> dict[str, JSONScalar]:
        if not value:
            raise ValueError("payload must be non-empty")
        for key, item in value.items():
            _identifier(key, "payload field")
            if isinstance(item, str):
                _required_text(item, f"payload.{key}")
            elif isinstance(item, float) and not math.isfinite(item):
                raise ValueError(f"payload.{key} must be finite")
        return value

    @model_validator(mode="after")
    def _validate_review_window(self) -> Self:
        if (self.expires_at_utc is None) == (self.reviewed_no_expiry_rationale is None):
            raise ValueError(
                "exactly one of expires_at_utc or reviewed_no_expiry_rationale is required"
            )
        if self.expires_at_utc is not None and self.expires_at_utc <= self.reviewed_at_utc:
            raise ValueError("expires_at_utc must be later than reviewed_at_utc")
        return self


class OwnerDecisionNotApplicableRationale(_StrictModel):
    slot_id: str
    rationale: str
    impact_scope: str

    @field_validator("slot_id")
    @classmethod
    def _validate_slot_id(cls, value: str) -> str:
        return _identifier(value, "slot_id")

    @field_validator("rationale", "impact_scope")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))


class QQQOptionsOwnerDecisionManifest(_SealedModel):
    schema_version: Literal["qqq_options_owner_decision_manifest.v1"]
    record_id: str
    created_at_utc: datetime
    repository_code_sha: str
    owner_decision_id: str
    decision_date: date
    independent_reviewer_id: str
    pack_requirement_lf_sha256: Literal[
        "afdcb44f44032fee958d4f6b1e8e4b56c1edb2faefa44026e16aff7153968588"
    ]
    authority_set_sha256: Literal[
        "1702d50c135204f1d92405cfaf4da7c3a06dae0bb09f2095d68ea388390e687c"
    ]
    manifest_policy_file_sha256: str
    manifest_policy_canonical_sha256: str
    slot_catalog_sha256: str
    research_window_role: Literal["PRIMARY"]
    requested_start: date
    evaluated_start: date
    group_choices: tuple[OwnerDecisionGroupChoice, ...]
    materialized_decisions: tuple[OwnerDecisionMaterializedSlot, ...]
    owner_policy_values: tuple[OwnerReviewedPolicyValue, ...]
    not_applicable_rationales: tuple[OwnerDecisionNotApplicableRationale, ...]
    confirmed_no_engine_activation: Literal[True]
    confirmed_no_external_action: Literal[True]

    @field_validator("record_id", "independent_reviewer_id")
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
        "manifest_policy_file_sha256",
        "manifest_policy_canonical_sha256",
        "slot_catalog_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_manifest_contract(self) -> Self:
        match = _OWNER_DECISION_PATTERN.fullmatch(self.owner_decision_id)
        if match is None or date.fromisoformat(match.group(1)) != self.decision_date:
            raise ValueError("Owner decision token and decision_date do not match")
        if self.created_at_utc.date() < self.decision_date:
            raise ValueError("manifest cannot predate the Owner decision")
        if self.requested_start != _PRIMARY_RESEARCH_START:
            raise ValueError("requested_start must remain 2021-02-22")
        if self.evaluated_start != _PRIMARY_RESEARCH_START:
            raise ValueError("evaluated_start must remain 2021-02-22")
        if tuple(item.canonical_group for item in self.group_choices) != _EXPECTED_GROUPS:
            raise ValueError("group choices must use exact canonical order")
        materialized = tuple(
            (item.slot_id, item.canonical_group) for item in self.materialized_decisions
        )
        if materialized != _EXPECTED_SLOT_GROUPS:
            raise ValueError(
                "materialized decisions must contain the exact ordered 28-slot catalog"
            )
        modes = {item.canonical_group: item.mode for item in self.group_choices}
        for group in _EXPECTED_GROUPS:
            group_actions = tuple(
                item.action
                for item in self.materialized_decisions
                if item.canonical_group is group
            )
            mode = modes[group]
            if mode is not OwnerDecisionGroupMode.PER_SLOT:
                expected_action = OwnerDecisionAction(mode.value)
                if any(action is not expected_action for action in group_actions):
                    raise ValueError("group-level mode does not match materialized actions")
        slot_order = [slot_id for slot_id, _ in _EXPECTED_SLOT_GROUPS]
        value_ids = tuple(item.slot_id for item in self.owner_policy_values)
        g2_ids = tuple(
            item.slot_id
            for item in self.materialized_decisions
            if item.action is OwnerDecisionAction.G2
        )
        if value_ids != g2_ids or list(value_ids) != sorted(value_ids, key=slot_order.index):
            raise ValueError("G2 slots must have exactly one ordered typed Owner value")
        rationale_ids = tuple(item.slot_id for item in self.not_applicable_rationales)
        g5_ids = tuple(
            item.slot_id
            for item in self.materialized_decisions
            if item.action is OwnerDecisionAction.G5
        )
        if rationale_ids != g5_ids or list(rationale_ids) != sorted(
            rationale_ids, key=slot_order.index
        ):
            raise ValueError("G5 slots must have exactly one ordered rationale and impact")
        return self


class OwnerDecisionDependencyAudit(_StrictModel):
    upstream_gate_id: str
    downstream_group: OwnerDecisionCanonicalGroup
    status: DependencyStatus

    @field_validator("upstream_gate_id")
    @classmethod
    def _validate_upstream(cls, value: str) -> str:
        return _identifier(value, "upstream_gate_id")


class QQQOptionsOwnerDecisionResolutionResult(_SealedModel):
    schema_version: Literal["qqq_options_owner_decision_resolution.v1"]
    validation_status: Literal["VALID_CONTRACT_ONLY_OWNER_DECISION"]
    manifest_content_sha256: str
    manifest_canonical_sha256: str
    manifest_policy_file_sha256: str
    slot_catalog_sha256: str
    reviewed_policy_slot_ids: tuple[str, ...]
    unresolved_slot_ids: tuple[str, ...]
    calibration_required_slot_ids: tuple[str, ...]
    sensitivity_only_slot_ids: tuple[str, ...]
    not_applicable_slot_ids: tuple[str, ...]
    dependency_audit: tuple[OwnerDecisionDependencyAudit, ...]
    corporate_action_status: Literal["HARD_STOP_NOT_A_DECISION_SLOT"]
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

    @field_validator(
        "manifest_content_sha256",
        "manifest_canonical_sha256",
        "manifest_policy_file_sha256",
        "slot_catalog_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


def load_qqq_options_owner_decision_manifest_policy(
    path: Path = DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsOwnerDecisionManifestPolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _require_bound_regular_file(
            path, project_root=root, field="Owner decision manifest policy"
        )
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("Owner decision manifest policy root must be a mapping")
        policy = QQQOptionsOwnerDecisionManifestPolicy.model_validate(payload)
        pack_path = _require_bound_regular_file(
            Path(policy.pack_requirement_path),
            project_root=root,
            field="TRADING-2502 decision pack",
        )
        pack_hash = _lf_sha256(pack_path.read_bytes())
        if pack_hash != policy.pack_requirement_lf_sha256:
            raise ValueError(
                "TRADING-2502 decision pack LF SHA-256 mismatch: "
                f"expected {policy.pack_requirement_lf_sha256}, observed {pack_hash}"
            )
    except QQQOptionsOwnerDecisionManifestContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionsOwnerDecisionManifestContractError(
            "OWNER_DECISION_MANIFEST_POLICY_INVALID", str(exc)
        ) from exc
    return QQQOptionsOwnerDecisionManifestPolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_file_sha256=sha256_path(policy_path),
        policy_canonical_sha256=policy.canonical_sha256,
        slot_catalog_sha256=policy.slot_catalog_sha256,
    )


def _unique_by_slot_id(items: tuple[Any, ...], *, field: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in items:
        if item.slot_id in result:
            raise QQQOptionsOwnerDecisionManifestContractError(
                "OWNER_DECISION_DUPLICATE_SLOT", f"{field} repeats {item.slot_id}"
            )
        result[item.slot_id] = item
    return result


def build_qqq_options_owner_decision_manifest(
    *,
    record_id: str,
    created_at_utc: datetime,
    repository_code_sha: str,
    owner_decision_id: str,
    decision_date: date,
    independent_reviewer_id: str,
    group_choices: tuple[OwnerDecisionGroupChoice, ...],
    slot_choices: tuple[OwnerDecisionSlotChoice, ...],
    owner_policy_values: tuple[OwnerReviewedPolicyValue, ...],
    not_applicable_rationales: tuple[OwnerDecisionNotApplicableRationale, ...],
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsOwnerDecisionManifest:
    loaded = load_qqq_options_owner_decision_manifest_policy(project_root=project_root)
    policy = loaded.policy
    group_map: dict[OwnerDecisionCanonicalGroup, OwnerDecisionGroupChoice] = {}
    for choice in group_choices:
        if choice.canonical_group in group_map:
            raise QQQOptionsOwnerDecisionManifestContractError(
                "OWNER_DECISION_DUPLICATE_GROUP",
                f"group choice repeats {choice.canonical_group.value}",
            )
        group_map[choice.canonical_group] = choice
    if set(group_map) != set(_EXPECTED_GROUPS):
        raise QQQOptionsOwnerDecisionManifestContractError(
            "OWNER_DECISION_GROUP_INVENTORY_INVALID",
            "group choices must contain each canonical group exactly once",
        )

    catalog_by_id = {slot.slot_id: slot for slot in policy.slots}
    choices_by_id = _unique_by_slot_id(slot_choices, field="slot_choices")
    unknown_choices = set(choices_by_id) - set(catalog_by_id)
    if unknown_choices:
        raise QQQOptionsOwnerDecisionManifestContractError(
            "OWNER_DECISION_UNKNOWN_SLOT",
            f"slot choices contain unknown ids: {sorted(unknown_choices)}",
        )

    materialized: list[OwnerDecisionMaterializedSlot] = []
    consumed: set[str] = set()
    for group in policy.group_order:
        group_slots = tuple(slot for slot in policy.slots if slot.canonical_group is group)
        choice = group_map[group]
        supplied_for_group = {
            slot_id
            for slot_id in choices_by_id
            if catalog_by_id[slot_id].canonical_group is group
        }
        if choice.mode is OwnerDecisionGroupMode.PER_SLOT:
            expected = {slot.slot_id for slot in group_slots}
            if supplied_for_group != expected:
                missing = sorted(expected - supplied_for_group)
                extra = sorted(supplied_for_group - expected)
                raise QQQOptionsOwnerDecisionManifestContractError(
                    "OWNER_DECISION_PER_SLOT_NOT_TOTAL",
                    f"{group.value} PER_SLOT missing={missing} extra={extra}",
                )
            for slot in group_slots:
                action = choices_by_id[slot.slot_id].action
                materialized.append(
                    OwnerDecisionMaterializedSlot(
                        slot_id=slot.slot_id,
                        canonical_group=group,
                        action=action,
                    )
                )
                consumed.add(slot.slot_id)
        else:
            if supplied_for_group:
                raise QQQOptionsOwnerDecisionManifestContractError(
                    "OWNER_DECISION_GROUP_OVERRIDE_CONFLICT",
                    f"{group.value} group-level choice cannot have slot overrides",
                )
            action = OwnerDecisionAction(choice.mode.value)
            materialized.extend(
                OwnerDecisionMaterializedSlot(
                    slot_id=slot.slot_id,
                    canonical_group=group,
                    action=action,
                )
                for slot in group_slots
            )
    if consumed != set(choices_by_id):
        raise QQQOptionsOwnerDecisionManifestContractError(
            "OWNER_DECISION_UNCONSUMED_SLOT",
            "one or more slot decisions were not consumed by PER_SLOT groups",
        )

    values_by_id = _unique_by_slot_id(owner_policy_values, field="owner_policy_values")
    rationales_by_id = _unique_by_slot_id(
        not_applicable_rationales, field="not_applicable_rationales"
    )
    actions = {item.slot_id: item.action for item in materialized}
    g2_ids = {slot_id for slot_id, action in actions.items() if action is OwnerDecisionAction.G2}
    g5_ids = {slot_id for slot_id, action in actions.items() if action is OwnerDecisionAction.G5}
    if set(values_by_id) != g2_ids:
        raise QQQOptionsOwnerDecisionManifestContractError(
            "OWNER_DECISION_G2_VALUE_INVENTORY_INVALID",
            f"G2 values expected={sorted(g2_ids)} observed={sorted(values_by_id)}",
        )
    if set(rationales_by_id) != g5_ids:
        raise QQQOptionsOwnerDecisionManifestContractError(
            "OWNER_DECISION_G5_RATIONALE_INVENTORY_INVALID",
            f"G5 rationale expected={sorted(g5_ids)} observed={sorted(rationales_by_id)}",
        )
    for slot_id, value in values_by_id.items():
        schema = catalog_by_id[slot_id].value_schema
        if value.value_schema_id != schema.schema_id or value.value_kind is not schema.value_kind:
            raise QQQOptionsOwnerDecisionManifestContractError(
                "OWNER_DECISION_G2_SCHEMA_MISMATCH",
                f"{slot_id} value schema or kind differs from the canonical catalog",
            )
        if set(value.payload) != set(schema.required_payload_fields):
            raise QQQOptionsOwnerDecisionManifestContractError(
                "OWNER_DECISION_G2_PAYLOAD_FIELDS_MISMATCH",
                f"{slot_id} requires payload fields {schema.required_payload_fields}",
            )

    slot_order = [slot.slot_id for slot in policy.slots]
    ordered_groups = tuple(group_map[group] for group in policy.group_order)
    ordered_values = tuple(
        values_by_id[slot_id] for slot_id in slot_order if slot_id in values_by_id
    )
    ordered_rationales = tuple(
        rationales_by_id[slot_id] for slot_id in slot_order if slot_id in rationales_by_id
    )
    try:
        return QQQOptionsOwnerDecisionManifest.seal(
            schema_version="qqq_options_owner_decision_manifest.v1",
            record_id=record_id,
            created_at_utc=created_at_utc,
            repository_code_sha=repository_code_sha,
            owner_decision_id=owner_decision_id,
            decision_date=decision_date,
            independent_reviewer_id=independent_reviewer_id,
            pack_requirement_lf_sha256=policy.pack_requirement_lf_sha256,
            authority_set_sha256=policy.authority_set_sha256,
            manifest_policy_file_sha256=loaded.policy_file_sha256,
            manifest_policy_canonical_sha256=loaded.policy_canonical_sha256,
            slot_catalog_sha256=loaded.slot_catalog_sha256,
            research_window_role="PRIMARY",
            requested_start=_PRIMARY_RESEARCH_START,
            evaluated_start=_PRIMARY_RESEARCH_START,
            group_choices=ordered_groups,
            materialized_decisions=tuple(materialized),
            owner_policy_values=ordered_values,
            not_applicable_rationales=ordered_rationales,
            confirmed_no_engine_activation=True,
            confirmed_no_external_action=True,
        )
    except ValueError as exc:
        raise QQQOptionsOwnerDecisionManifestContractError(
            "OWNER_DECISION_MANIFEST_INVALID", str(exc)
        ) from exc


def _group_formally_decided(
    group: OwnerDecisionCanonicalGroup,
    manifest: QQQOptionsOwnerDecisionManifest,
) -> bool:
    return all(
        item.action in {OwnerDecisionAction.G2, OwnerDecisionAction.G5}
        for item in manifest.materialized_decisions
        if item.canonical_group is group
    )


def resolve_qqq_options_owner_decision_manifest(
    manifest_bytes: bytes,
    *,
    expected_repository_code_sha: str,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsOwnerDecisionResolutionResult:
    expected_sha = _git_sha(expected_repository_code_sha, "expected_repository_code_sha")
    manifest = QQQOptionsOwnerDecisionManifest.from_json_bytes(manifest_bytes)
    loaded = load_qqq_options_owner_decision_manifest_policy(project_root=project_root)
    mismatches = {
        "repository_code_sha": (manifest.repository_code_sha, expected_sha),
        "manifest_policy_file_sha256": (
            manifest.manifest_policy_file_sha256,
            loaded.policy_file_sha256,
        ),
        "manifest_policy_canonical_sha256": (
            manifest.manifest_policy_canonical_sha256,
            loaded.policy_canonical_sha256,
        ),
        "slot_catalog_sha256": (
            manifest.slot_catalog_sha256,
            loaded.slot_catalog_sha256,
        ),
    }
    drifted = [field for field, (actual, expected) in mismatches.items() if actual != expected]
    if drifted:
        raise QQQOptionsOwnerDecisionManifestContractError(
            "OWNER_DECISION_MANIFEST_BINDING_MISMATCH",
            f"manifest binding mismatch: {drifted}",
        )

    actions = {item.slot_id: item.action for item in manifest.materialized_decisions}
    dependency_audit: list[OwnerDecisionDependencyAudit] = []
    for edge in loaded.policy.dependency_edges:
        if edge.upstream_gate_id == "DQ_PIT_AUTHORITY":
            status: DependencyStatus = "NOT_EVALUATED_BY_THIS_CONTRACT"
        else:
            upstream_group = OwnerDecisionCanonicalGroup(edge.upstream_gate_id)
            status = (
                "FORMALLY_DECIDED_NOT_ENGINE_AUTHORIZED"
                if _group_formally_decided(upstream_group, manifest)
                else "BLOCKED_BY_UNRESOLVED_UPSTREAM"
            )
        dependency_audit.append(
            OwnerDecisionDependencyAudit(
                upstream_gate_id=edge.upstream_gate_id,
                downstream_group=edge.downstream_group,
                status=status,
            )
        )

    def ids_for(action: OwnerDecisionAction) -> tuple[str, ...]:
        return tuple(
            slot.slot_id
            for slot in loaded.policy.slots
            if actions[slot.slot_id] is action
        )

    return QQQOptionsOwnerDecisionResolutionResult.seal(
        schema_version="qqq_options_owner_decision_resolution.v1",
        validation_status=OwnerDecisionResolutionStatus.VALID_CONTRACT_ONLY_OWNER_DECISION.value,
        manifest_content_sha256=manifest.content_sha256,
        manifest_canonical_sha256=manifest.canonical_sha256,
        manifest_policy_file_sha256=loaded.policy_file_sha256,
        slot_catalog_sha256=loaded.slot_catalog_sha256,
        reviewed_policy_slot_ids=ids_for(OwnerDecisionAction.G2),
        unresolved_slot_ids=ids_for(OwnerDecisionAction.G1),
        calibration_required_slot_ids=ids_for(OwnerDecisionAction.G3),
        sensitivity_only_slot_ids=ids_for(OwnerDecisionAction.G4),
        not_applicable_slot_ids=ids_for(OwnerDecisionAction.G5),
        dependency_audit=tuple(dependency_audit),
        corporate_action_status="HARD_STOP_NOT_A_DECISION_SLOT",
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
