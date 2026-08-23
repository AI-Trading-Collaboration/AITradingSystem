from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.strategy_growth_action_value_preregistration import MandatoryAxis
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_THRESHOLD_DECISION_PACK_PATH = Path(
    "config/research/strategy_growth_action_value_threshold_decision_pack_v1.yaml"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_BINDING_AUTHORITY_ORDER = (
    "PROJECT_ENGINEERING_RULES",
    "TRADING_2540_PREREGISTRATION_POLICY",
    "TRADING_2540_REQUIREMENT",
    "TRADING_2541_RECOVERY_TERMINAL",
)
_SOURCE_INVENTORY = (
    (
        "ACTION_VALUE_SCORE_POLICY_V2",
        "config/research/action_value_score_policy_v2.yaml",
        "WRONG_SCOPE",
    ),
    (
        "DEFENSIVE_LANE_ACTION_VALUE_POLICY",
        "config/research/defensive_lane_action_value_policy.yaml",
        "RETIRED_FAMILY",
    ),
    (
        "FIRST_LAYER_THRESHOLD_POLICY_V2",
        "config/research/first_layer_threshold_policy_v2.yaml",
        "RETIRED_FAMILY",
    ),
    (
        "PROMOTION_GATE_THRESHOLDS",
        "config/research/promotion_gate_thresholds.yaml",
        "WRONG_SCOPE",
    ),
    (
        "THRESHOLD_REGISTRY_V1",
        "config/research/threshold_registry.yaml",
        "UNCALIBRATED_INVENTORY",
    ),
    (
        "TRANSACTION_COST_MODEL_V1",
        "config/research/transaction_cost_model.yaml",
        "PARTIAL_INPUT_ONLY",
    ),
    (
        "QQQ_OPTIONS_DQ_PIT_IDENTITY_V1",
        "config/research/qqq_options_dq_pit_identity_v1.yaml",
        "PARTIAL_INPUT_ONLY",
    ),
    (
        "QQQ_OPTIONS_STAGED_DQ_PIT_READINESS_V1",
        "config/research/qqq_options_staged_dq_pit_readiness_v1.yaml",
        "PARTIAL_INPUT_ONLY",
    ),
)
_CALIBRATION_OPTION_ORDER = (
    "OWNER_ECONOMIC_MATERIALITY",
    "TRAIN_WINDOW_ONLY_DISTRIBUTIONAL",
    "HYBRID_PRECOMMITTED",
    "CANONICAL_STRICT_DQ_PIT",
    "FIXED_PRIMARY_WINDOW_STABILITY",
)
_AXIS_ORDER = tuple(MandatoryAxis)
_OWNER_QUESTION_ORDER = (
    "SOURCE_ASSIGNMENT",
    "EXACT_VALUE_SHEET",
    "REVIEW_CONDITION",
)


class StrategyGrowthActionValueThresholdDecisionPackError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class AuthorityDisposition(StrEnum):
    ADMISSIBLE = "ADMISSIBLE"
    PARTIAL_INPUT_ONLY = "PARTIAL_INPUT_ONLY"
    WRONG_SCOPE = "WRONG_SCOPE"
    RETIRED_FAMILY = "RETIRED_FAMILY"
    UNCALIBRATED_INVENTORY = "UNCALIBRATED_INVENTORY"


class ThresholdDirection(StrEnum):
    MINIMUM = "MINIMUM"
    MAXIMUM = "MAXIMUM"
    EXACT_CATEGORICAL = "EXACT_CATEGORICAL"
    COMPOSITE_ALL = "COMPOSITE_ALL"


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

    def reject_constant(value: str) -> object:
        raise ValueError(f"non-finite JSON constant is prohibited: {value}")

    try:
        return json.loads(
            raw.decode("utf-8"), object_pairs_hook=hook, parse_constant=reject_constant
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("record is not UTF-8 JSON") from exc


def _identifier(value: str, field: str) -> str:
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a stable identifier")
    return value


def _sha256(value: str, field: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase SHA-256")
    return value


def _relative_path(value: str, field: str) -> str:
    path = Path(value)
    if not value or path.is_absolute() or path.drive or ".." in path.parts:
        raise ValueError(f"{field} must be a bounded project-relative path")
    if path.as_posix() != value:
        raise ValueError(f"{field} must use normalized forward slashes")
    return value


def _bound_file(path: Path, *, root: Path, field: str) -> Path:
    resolved_root = root.resolve()
    candidate = path if path.is_absolute() else resolved_root / path
    try:
        relative = candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes its reviewed root") from exc
    current = resolved_root
    for part in relative.parts:
        current /= part
        if current.exists() and current.is_symlink():
            raise ValueError(f"{field} cannot traverse a symlink")
    if not candidate.is_file() or candidate.is_symlink():
        raise ValueError(f"{field} must be a non-symlink regular file")
    return candidate


def _unique(values: tuple[str, ...], field: str) -> tuple[str, ...]:
    if not values or len(set(values)) != len(values):
        raise ValueError(f"{field} must be non-empty and unique")
    return values


class PolicyMetadata(_PolicyModel):
    owner: str
    rationale: str
    intended_effect: str
    validation_evidence: str
    review_condition: str

    @field_validator(
        "owner", "rationale", "intended_effect", "validation_evidence", "review_condition"
    )
    @classmethod
    def validate_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("policy metadata text must not be empty")
        return value


class ScopeBinding(_PolicyModel):
    predecessor_policy_id: Literal["strategy_growth_action_value_preregistration_v1"]
    hypothesis_id: Literal["BASELINE_BOUNDED_QQQ_GROWTH_OVERLAY_NON_BETA_ACTION_VALUE_V1"]
    baseline_id: Literal["equal_risk_qqq_sgov"]
    comparator_ids: tuple[Literal["equal_risk_qqq_sgov", "exposure_matched_no_signal"], ...]
    action_universe: tuple[Literal["QQQ", "SGOV"], ...]
    primary_window_start: date
    primary_window_end: date
    selected_data_lane: Literal["QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE"]
    uses_leverage_etf: Literal[False]
    uses_options: Literal[False]
    borrowed_leverage_allowed: Literal[False]
    threshold_after_result_allowed: Literal[False]

    @model_validator(mode="after")
    def validate_scope(self) -> Self:
        if self.comparator_ids != ("equal_risk_qqq_sgov", "exposure_matched_no_signal"):
            raise ValueError("comparator inventory drifted")
        if self.action_universe != ("QQQ", "SGOV"):
            raise ValueError("action universe drifted")
        if self.primary_window_start != date(2021, 2, 22):
            raise ValueError("primary window start drifted")
        if self.primary_window_end != date(2025, 12, 2):
            raise ValueError("primary window end drifted")
        return self


class DecisionTiming(_PolicyModel):
    state: Literal["PRE_EMPIRICAL_OWNER_DECISION_REQUIRED"]
    new_dq_result_visible: Literal[False]
    new_strategy_result_visible: Literal[False]
    holdout_result_visible: Literal[False]
    threshold_value_selected: Literal[False]


class FileAuthorityBinding(_PolicyModel):
    authority_id: str
    path: str
    file_sha256: str

    @field_validator("authority_id")
    @classmethod
    def validate_authority_id(cls, value: str) -> str:
        return _identifier(value, "authority_id")

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return _relative_path(value, "path")

    @field_validator("file_sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        return _sha256(value, "file_sha256")


class ThresholdSourceInventoryItem(FileAuthorityBinding):
    disposition: AuthorityDisposition
    applicable_axes: tuple[MandatoryAxis, ...]
    reusable_roles: tuple[str, ...]
    prohibited_roles: tuple[str, ...]
    rationale: str

    @field_validator("applicable_axes")
    @classmethod
    def validate_axes(cls, value: tuple[MandatoryAxis, ...]) -> tuple[MandatoryAxis, ...]:
        if not value or len(set(value)) != len(value):
            raise ValueError("applicable_axes must be non-empty and unique")
        return value

    @field_validator("reusable_roles", "prohibited_roles")
    @classmethod
    def validate_roles(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("authority roles must be unique")
        return tuple(_identifier(item, "authority_role") for item in value)

    @field_validator("rationale")
    @classmethod
    def validate_rationale(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("authority rationale must not be empty")
        return value


class CalibrationSourceOption(_PolicyModel):
    option_id: str
    source_kind: str
    allowed_inputs: tuple[str, ...]
    prohibited_inputs: tuple[str, ...]
    risk: str

    @field_validator("option_id", "source_kind")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        return _identifier(value, "calibration option identifier")

    @field_validator("allowed_inputs", "prohibited_inputs")
    @classmethod
    def validate_inputs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_identifier(item, "calibration input") for item in _unique(value, "inputs"))

    @field_validator("risk")
    @classmethod
    def validate_risk(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("calibration risk must not be empty")
        return value


class AxisGap(_PolicyModel):
    axis_id: MandatoryAxis
    threshold_id: str
    proposed_unit: str
    proposed_direction: ThresholdDirection
    source_authority_ids: tuple[str, ...]
    current_gap: str
    calibration_option_ids: tuple[str, ...]
    recommended_option_id: str
    owner_value_fields: tuple[str, ...]
    owner_value_state: Literal["NOT_PROVIDED"]
    rationale: str
    intended_effect: str
    pass_rule_code: str
    fail_rule_code: str
    insufficient_rule_code: str
    invalid_rule_code: str
    review_condition: str

    @field_validator(
        "threshold_id",
        "proposed_unit",
        "current_gap",
        "recommended_option_id",
        "pass_rule_code",
        "fail_rule_code",
        "insufficient_rule_code",
        "invalid_rule_code",
    )
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        return _identifier(value, "axis identifier field")

    @field_validator("source_authority_ids", "calibration_option_ids", "owner_value_fields")
    @classmethod
    def validate_identifier_tuple(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_identifier(item, "axis reference") for item in _unique(value, "axis refs"))

    @field_validator("rationale", "intended_effect", "review_condition")
    @classmethod
    def validate_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("axis explanatory text must not be empty")
        return value


class OwnerQuestion(_PolicyModel):
    question_id: str
    prompt: str
    allowed_choices: tuple[str, ...]
    recommended_choice: str
    required_response_fields: tuple[str, ...]

    @field_validator("question_id", "recommended_choice")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        return _identifier(value, "owner question identifier")

    @field_validator("allowed_choices", "required_response_fields")
    @classmethod
    def validate_identifier_tuple(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(
            _identifier(item, "owner response identifier")
            for item in _unique(value, "owner refs")
        )

    @field_validator("prompt")
    @classmethod
    def validate_prompt(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("owner prompt must not be empty")
        return value

    @model_validator(mode="after")
    def validate_recommendation(self) -> Self:
        if self.recommended_choice not in self.allowed_choices:
            raise ValueError("recommended owner choice is not allowed")
        return self


class OwnerResponseContract(_PolicyModel):
    response_schema: Literal["strategy_growth_action_value_threshold_owner_decision.v1"]
    source_assignment_choice: Literal["OWNER_CHOICE_REQUIRED"]
    review_condition_choice: Literal["OWNER_CHOICE_REQUIRED"]
    axis_value_token: Literal["OWNER_VALUE_REQUIRED"]
    complete_axis_set_required: Literal[True]
    partial_response_allowed: Literal[False]
    decision_before_result_required: Literal[True]


class DecisionPackTerminal(_PolicyModel):
    status: Literal["BLOCKED_OWNER_INPUT"]
    next_action: Literal["PROJECT_OWNER_EXACT_THRESHOLD_SOURCE_VALUE_AND_REVIEW_DECISION"]
    threshold_bundle_frozen: Literal[False]
    dq_successor_authorized: Literal[False]
    empirical_successor_authorized: Literal[False]


class DecisionPackSafety(_PolicyModel):
    empirical_research_authorized: Literal[False]
    dq_run_authorized: Literal[False]
    cache_mutation_authorized: Literal[False]
    backtest_authorized: Literal[False]
    holdout_access_authorized: Literal[False]
    external_action_authorized: Literal[False]
    investment_conclusion_authorized: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class StrategyGrowthActionValueThresholdDecisionPack(_PolicyModel):
    schema_version: Literal["strategy_growth_action_value_threshold_decision_pack.v1"]
    pack_id: Literal["strategy_growth_action_value_threshold_decision_pack_v1"]
    pack_version: Literal["1.0.0"]
    pack_status: Literal["OWNER_DECISION_REQUIRED"]
    task_id: Literal[
        "TRADING-2542_GROWTH_ACTION_VALUE_THRESHOLD_POLICY_DECISION_PACK_AND_FREEZE_V1"
    ]
    policy_metadata: PolicyMetadata
    scope_binding: ScopeBinding
    decision_timing: DecisionTiming
    binding_authorities: tuple[FileAuthorityBinding, ...]
    threshold_source_inventory: tuple[ThresholdSourceInventoryItem, ...]
    calibration_source_options: tuple[CalibrationSourceOption, ...]
    axis_gap_matrix: tuple[AxisGap, ...]
    owner_questions: tuple[OwnerQuestion, ...]
    owner_response_contract: OwnerResponseContract
    terminal: DecisionPackTerminal
    safety: DecisionPackSafety

    @model_validator(mode="after")
    def validate_inventory(self) -> Self:
        binding_ids = tuple(item.authority_id for item in self.binding_authorities)
        if binding_ids != _BINDING_AUTHORITY_ORDER:
            raise ValueError("binding authority inventory drifted")

        actual_sources = tuple(
            (item.authority_id, item.path, item.disposition.value)
            for item in self.threshold_source_inventory
        )
        if actual_sources != _SOURCE_INVENTORY:
            raise ValueError("threshold source inventory or disposition drifted")

        option_ids = tuple(item.option_id for item in self.calibration_source_options)
        if option_ids != _CALIBRATION_OPTION_ORDER:
            raise ValueError("calibration option inventory drifted")

        axis_ids = tuple(item.axis_id for item in self.axis_gap_matrix)
        if axis_ids != _AXIS_ORDER:
            raise ValueError("axis gap matrix must contain all eight axes in canonical order")
        threshold_ids = tuple(item.threshold_id for item in self.axis_gap_matrix)
        if len(set(threshold_ids)) != len(threshold_ids):
            raise ValueError("axis threshold ids must be unique")

        source_by_id = {item.authority_id: item for item in self.threshold_source_inventory}
        option_id_set = set(option_ids)
        owner_fields: list[str] = []
        for axis in self.axis_gap_matrix:
            if any(source_id not in source_by_id for source_id in axis.source_authority_ids):
                raise ValueError(f"unknown threshold source for axis {axis.axis_id}")
            if any(
                axis.axis_id not in source_by_id[source_id].applicable_axes
                for source_id in axis.source_authority_ids
            ):
                raise ValueError(f"threshold source scope mismatch for axis {axis.axis_id}")
            if any(option_id not in option_id_set for option_id in axis.calibration_option_ids):
                raise ValueError(f"unknown calibration option for axis {axis.axis_id}")
            if axis.recommended_option_id not in axis.calibration_option_ids:
                raise ValueError(f"recommended option missing for axis {axis.axis_id}")
            owner_fields.extend(axis.owner_value_fields)
        if len(set(owner_fields)) != len(owner_fields):
            raise ValueError("owner value fields must be globally unique")

        question_ids = tuple(item.question_id for item in self.owner_questions)
        if question_ids != _OWNER_QUESTION_ORDER:
            raise ValueError("owner question inventory drifted")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            payload = _duplicate_key_rejecting_json(raw)
            if not isinstance(payload, dict):
                raise TypeError("decision pack JSON root must be an object")
            pack = cls.model_validate_json(raw)
            if raw != pack.canonical_bytes:
                raise ValueError("decision pack is not canonical JSON bytes")
            return pack
        except (TypeError, ValueError) as exc:
            raise StrategyGrowthActionValueThresholdDecisionPackError(
                "GROWTH_THRESHOLD_DECISION_PACK_RECORD_INVALID", str(exc)
            ) from exc


class AuthorityObservation(_PolicyModel):
    authority_id: str
    path: str
    file_sha256: str
    identity_verified: Literal[True]


@dataclass(frozen=True)
class StrategyGrowthActionValueThresholdDecisionPackLoadResult:
    pack: StrategyGrowthActionValueThresholdDecisionPack
    pack_path: Path
    pack_file_sha256: str
    pack_canonical_sha256: str
    authority_observations: tuple[AuthorityObservation, ...]
    authority_set_sha256: str


def _reconcile_predecessor(
    *,
    pack: StrategyGrowthActionValueThresholdDecisionPack,
    authority_paths: dict[str, Path],
) -> None:
    predecessor_path = authority_paths["TRADING_2540_PREREGISTRATION_POLICY"]
    predecessor = load_strict_yaml_text(
        predecessor_path.read_text(encoding="utf-8"), label=str(predecessor_path)
    )
    hypothesis = predecessor["hypothesis"]
    data_lane = predecessor["data_lane"]
    scope = pack.scope_binding
    comparisons = {
        "policy_id": (predecessor["policy_id"], scope.predecessor_policy_id),
        "primary_window_start": (
            str(predecessor["primary_research_start"]),
            scope.primary_window_start.isoformat(),
        ),
        "hypothesis_id": (hypothesis["hypothesis_id"], scope.hypothesis_id),
        "baseline_id": (hypothesis["baseline_id"], scope.baseline_id),
        "comparator_ids": (tuple(hypothesis["comparator_ids"]), scope.comparator_ids),
        "action_universe": (tuple(hypothesis["action_universe"]), scope.action_universe),
        "selected_data_lane": (
            data_lane["selected_data_lane"],
            scope.selected_data_lane,
        ),
        "uses_leverage_etf": (hypothesis["uses_leverage_etf"], scope.uses_leverage_etf),
        "uses_options": (hypothesis["uses_options"], scope.uses_options),
        "borrowed_leverage_allowed": (
            hypothesis["borrowed_leverage_allowed"],
            scope.borrowed_leverage_allowed,
        ),
        "threshold_after_result_allowed": (
            predecessor["threshold_policy"]["threshold_after_result_allowed"],
            scope.threshold_after_result_allowed,
        ),
    }
    for field, (actual, expected) in comparisons.items():
        if actual != expected:
            raise ValueError(f"predecessor scope mismatch: {field}")

    predecessor_axes = {item["axis_id"]: item for item in predecessor["mandatory_axes"]}
    for axis in pack.axis_gap_matrix:
        predecessor_axis = predecessor_axes.get(axis.axis_id.value)
        if predecessor_axis is None:
            raise ValueError(f"predecessor axis missing: {axis.axis_id}")
        for field in (
            "pass_rule_code",
            "fail_rule_code",
            "insufficient_rule_code",
            "invalid_rule_code",
        ):
            if predecessor_axis[field] != getattr(axis, field):
                raise ValueError(f"predecessor rule mismatch: {axis.axis_id}:{field}")

    recovery_text = authority_paths["TRADING_2541_RECOVERY_TERMINAL"].read_text(
        encoding="utf-8"
    )
    for snippet in ("2021-02-22..2025-12-02", "1202/1202", "dq_pit_promoted=false"):
        if snippet not in recovery_text:
            raise ValueError(f"recovery authority snippet missing: {snippet}")


def load_strategy_growth_action_value_threshold_decision_pack(
    *,
    pack_path: Path = DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_THRESHOLD_DECISION_PACK_PATH,
    project_root: Path = PROJECT_ROOT,
) -> StrategyGrowthActionValueThresholdDecisionPackLoadResult:
    try:
        path = _bound_file(pack_path, root=project_root, field="pack_path")
        raw = path.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(pack_path))
        pack = StrategyGrowthActionValueThresholdDecisionPack.model_validate(payload)

        observations: list[AuthorityObservation] = []
        authority_paths: dict[str, Path] = {}
        for binding in (*pack.binding_authorities, *pack.threshold_source_inventory):
            authority_path = _bound_file(
                Path(binding.path), root=project_root, field=f"authority:{binding.authority_id}"
            )
            actual_sha256 = hashlib.sha256(authority_path.read_bytes()).hexdigest()
            if actual_sha256 != binding.file_sha256:
                raise ValueError(f"authority file SHA-256 mismatch: {binding.authority_id}")
            authority_paths[binding.authority_id] = authority_path
            observations.append(
                AuthorityObservation(
                    authority_id=binding.authority_id,
                    path=binding.path,
                    file_sha256=actual_sha256,
                    identity_verified=True,
                )
            )
        _reconcile_predecessor(pack=pack, authority_paths=authority_paths)
        observation_tuple = tuple(observations)
        authority_set_sha256 = hashlib.sha256(
            _canonical_json_bytes([item.model_dump(mode="json") for item in observation_tuple])
        ).hexdigest()
    except StrategyGrowthActionValueThresholdDecisionPackError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise StrategyGrowthActionValueThresholdDecisionPackError(
            "GROWTH_THRESHOLD_DECISION_PACK_REJECTED", str(exc)
        ) from exc
    return StrategyGrowthActionValueThresholdDecisionPackLoadResult(
        pack=pack,
        pack_path=path,
        pack_file_sha256=hashlib.sha256(raw).hexdigest(),
        pack_canonical_sha256=pack.canonical_sha256,
        authority_observations=observation_tuple,
        authority_set_sha256=authority_set_sha256,
    )


__all__ = [
    "DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_THRESHOLD_DECISION_PACK_PATH",
    "AuthorityDisposition",
    "AuthorityObservation",
    "AxisGap",
    "CalibrationSourceOption",
    "DecisionPackSafety",
    "DecisionPackTerminal",
    "DecisionTiming",
    "FileAuthorityBinding",
    "OwnerQuestion",
    "OwnerResponseContract",
    "PolicyMetadata",
    "ScopeBinding",
    "StrategyGrowthActionValueThresholdDecisionPack",
    "StrategyGrowthActionValueThresholdDecisionPackError",
    "StrategyGrowthActionValueThresholdDecisionPackLoadResult",
    "ThresholdDirection",
    "ThresholdSourceInventoryItem",
    "load_strategy_growth_action_value_threshold_decision_pack",
]
