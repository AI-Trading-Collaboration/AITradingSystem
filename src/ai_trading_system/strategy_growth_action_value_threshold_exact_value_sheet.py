from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Literal, Self, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.strategy_growth_action_value_threshold_decision_pack import (
    DecisionPackSafety,
    PolicyMetadata,
    ScopeBinding,
    StrategyGrowthActionValueThresholdDecisionPackLoadResult,
    ThresholdDirection,
    load_strategy_growth_action_value_threshold_decision_pack,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_THRESHOLD_EXACT_VALUE_SHEET_PATH = Path(
    "config/research/strategy_growth_action_value_threshold_exact_value_sheet_v1.yaml"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_AXIS_ORDER = (
    "NON_BETA_ACTION_VALUE",
    "NET_OF_COST_RETURN",
    "ACTUAL_PATH_DRAWDOWN_REGRESSION",
    "FALSE_RISK_OFF_COST",
    "CANONICAL_DQ_PIT",
    "SAMPLE_AND_WINDOW_DEPENDENCE",
    "ACTUAL_PATH_TURNOVER",
    "LEVERAGE_BETA_ATTRIBUTION",
)
_WINDOW_SLICES = (
    ("PRIMARY_WINDOW_FULL", date(2021, 2, 22), date(2025, 12, 2), "PRIMARY_AGGREGATE"),
    ("PRIMARY_2021_PARTIAL", date(2021, 2, 22), date(2021, 12, 31), "CALENDAR_STABILITY"),
    ("RATE_HIKE_BEAR_2022", date(2022, 1, 3), date(2022, 12, 30), "MANDATORY_STRESS"),
    ("RECOVERY_2023", date(2023, 1, 3), date(2023, 12, 29), "CALENDAR_STABILITY"),
    ("AI_RALLY_2024", date(2024, 1, 2), date(2024, 12, 31), "CALENDAR_STABILITY"),
    ("PRIMARY_2025_TO_END", date(2025, 1, 2), date(2025, 12, 2), "CALENDAR_STABILITY"),
)
_PERIOD_SLICE_IDS = tuple(item[0] for item in _WINDOW_SLICES[1:])
_ALL_SLICE_IDS = tuple(item[0] for item in _WINDOW_SLICES)


class StrategyGrowthActionValueThresholdExactValueSheetError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class _DraftModel(BaseModel):
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


def _non_empty(value: str) -> str:
    if not value.strip():
        raise ValueError("explanatory text must not be empty")
    return value


class OwnerInstruction(_DraftModel):
    decision_id: Literal[
        "owner_decision:TRADING-2542:2026-08-23:adopt_recommended_sources_and_draft_complete_exact_value_sheet_before_freeze_v1"
    ]
    source_assignment_choice: Literal["APPROVE_RECOMMENDED_PER_AXIS"]
    review_condition_choice: Literal[
        "LOCK_V1_FOR_ONE_PRIMARY_WINDOW_EVALUATION_NEW_VERSION_FOR_CHANGE"
    ]
    exact_value_approval_state: Literal["NOT_PROVIDED"]
    complete_axis_set_review_required: Literal[True]
    partial_approval_can_freeze: Literal[False]


class DecisionPackBinding(_DraftModel):
    path: Literal[
        "config/research/strategy_growth_action_value_threshold_decision_pack_v1.yaml"
    ]
    file_sha256: str
    canonical_sha256: str

    @field_validator("file_sha256", "canonical_sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if not _SHA256_PATTERN.fullmatch(value):
            raise ValueError("decision-pack binding must be a lowercase SHA-256")
        return value


class DraftDecisionTiming(_DraftModel):
    state: Literal["PRE_EMPIRICAL_DRAFT_FOR_OWNER_REVIEW"]
    new_dq_result_visible: Literal[False]
    new_strategy_result_visible: Literal[False]
    holdout_result_visible: Literal[False]
    draft_values_visible: Literal[True]
    exact_owner_approval_visible: Literal[False]
    threshold_bundle_frozen: Literal[False]


class MeasurementPolicy(_DraftModel):
    annualization_sessions: Literal[252]
    return_measurement_basis: Literal["ANNUALIZED_GEOMETRIC_RETURN_DELTA"]
    drawdown_measurement_basis: Literal[
        "CANDIDATE_MINUS_COMPARATOR_ABSOLUTE_MAX_DRAWDOWN"
    ]
    false_risk_off_cost_basis: Literal[
        "CANDIDATE_MINUS_BASELINE_MEAN_EVENT_MISSED_EXCESS_RETURN"
    ]
    turnover_measurement_basis: Literal[
        "ONE_WAY_TRADED_NOTIONAL_OVER_AVERAGE_NAV_ANNUALIZED"
    ]
    beta_measurement_basis: Literal[
        "DAILY_RETURN_OLS_BETA_WITH_INTERCEPT_252_SESSION_ANNUALIZATION"
    ]
    cost_model_policy_id: Literal["transaction_cost_model_v1"]


class WindowSlice(_DraftModel):
    slice_id: str
    start: date
    end: date
    role: Literal["PRIMARY_AGGREGATE", "CALENDAR_STABILITY", "MANDATORY_STRESS"]

    @model_validator(mode="after")
    def validate_dates(self) -> Self:
        if self.start > self.end:
            raise ValueError("window slice start must not exceed end")
        return self


class NumericDqPolicyDraft(_DraftModel):
    policy_ref: Literal[
        "strategy_growth_action_value_qqq_options_numeric_dq_thresholds_draft_v1"
    ]
    status: Literal["DRAFT_FOR_OWNER_REVIEW"]
    applicable_stage: Literal["DATA_RESEARCH"]
    max_quote_age_seconds: int = Field(ge=0, le=3600)
    max_relative_spread: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    min_open_interest: int = Field(ge=0)
    min_volume: int = Field(ge=0)
    exact_source_date_required: Literal[True]
    unknown_can_pass: Literal[False]
    rationale: str
    known_risk: str

    @field_validator("rationale", "known_risk")
    @classmethod
    def validate_text(cls, value: str) -> str:
        return _non_empty(value)


class AttributionConfidenceRule(_DraftModel):
    method: Literal["MOVING_BLOCK_BOOTSTRAP"]
    block_length_sessions: int = Field(ge=2)
    resamples: int = Field(ge=1000)
    one_sided_confidence_level: Decimal = Field(
        gt=Decimal("0.5"), lt=Decimal("1")
    )
    lower_bound_rule: Literal["STRICTLY_GREATER_THAN_ZERO"]
    random_seed: int = Field(ge=0)


class FalseRiskOffEventDefinition(_DraftModel):
    horizon_exchange_sessions: int = Field(ge=1)
    qqq_minus_sgov_forward_return_min: Decimal = Field(gt=Decimal("0"), lt=Decimal("1"))
    qqq_forward_max_drawdown_floor: Decimal = Field(gt=Decimal("-1"), le=Decimal("0"))
    adjacent_signal_cooldown_sessions: int = Field(ge=1)
    activation_rule: Literal["BASELINE_DEFENSIVE_VETO_ACTIVE_AT_SIGNAL_TIME"]
    regression_basis: Literal[
        "CANDIDATE_MINUS_BASELINE_MEAN_MISSED_QQQ_MINUS_SGOV_RETURN"
    ]


class _AxisDraftBase(_DraftModel):
    source_selection_state: Literal["OWNER_APPROVED_RECOMMENDED_SOURCE"]
    owner_review_state: Literal["PENDING_OWNER_APPROVAL"]
    unit: str
    direction: ThresholdDirection
    rationale: str
    known_risk: str

    @field_validator("unit", "rationale", "known_risk")
    @classmethod
    def validate_text(cls, value: str) -> str:
        return _non_empty(value)


class NonBetaActionValueDraft(_AxisDraftBase):
    axis_id: Literal["NON_BETA_ACTION_VALUE"]
    threshold_id: Literal["growth_action_value.non_beta_increment_min"]
    recommended_option_id: Literal["HYBRID_PRECOMMITTED"]
    direction: Literal[ThresholdDirection.MINIMUM]
    minimum_non_beta_return_delta: Decimal = Field(gt=Decimal("0"), lt=Decimal("1"))
    attribution_confidence_rule: AttributionConfidenceRule


class NetOfCostReturnDraft(_AxisDraftBase):
    axis_id: Literal["NET_OF_COST_RETURN"]
    threshold_id: Literal["growth_action_value.net_of_cost_return_min"]
    recommended_option_id: Literal["HYBRID_PRECOMMITTED"]
    direction: Literal[ThresholdDirection.MINIMUM]
    minimum_net_of_cost_return_delta: Decimal = Field(gt=Decimal("0"), lt=Decimal("1"))
    cost_reconciliation_tolerance: Decimal = Field(ge=Decimal("0"), lt=Decimal("0.01"))


class ActualPathDrawdownRegressionDraft(_AxisDraftBase):
    axis_id: Literal["ACTUAL_PATH_DRAWDOWN_REGRESSION"]
    threshold_id: Literal["growth_action_value.actual_path_drawdown_regression_max"]
    recommended_option_id: Literal["HYBRID_PRECOMMITTED"]
    direction: Literal[ThresholdDirection.MAXIMUM]
    maximum_actual_path_drawdown_regression: Decimal = Field(
        ge=Decimal("0"), lt=Decimal("1")
    )
    mandatory_stress_slice_set: tuple[str, ...]


class FalseRiskOffCostDraft(_AxisDraftBase):
    axis_id: Literal["FALSE_RISK_OFF_COST"]
    threshold_id: Literal["growth_action_value.false_risk_off_cost_regression_max"]
    recommended_option_id: Literal["HYBRID_PRECOMMITTED"]
    direction: Literal[ThresholdDirection.MAXIMUM]
    maximum_false_risk_off_cost_regression: Decimal = Field(
        ge=Decimal("0"), lt=Decimal("1")
    )
    false_risk_off_event_definition: FalseRiskOffEventDefinition


class CanonicalDqPitDraft(_AxisDraftBase):
    axis_id: Literal["CANONICAL_DQ_PIT"]
    threshold_id: Literal["growth_action_value.canonical_dq_pit_required_status"]
    recommended_option_id: Literal["CANONICAL_STRICT_DQ_PIT"]
    direction: Literal[ThresholdDirection.EXACT_CATEGORICAL]
    required_data_research_gate_status: Literal["PASS"]
    numeric_dq_threshold_policy_ref: Literal[
        "strategy_growth_action_value_qqq_options_numeric_dq_thresholds_draft_v1"
    ]


class SampleAndWindowDependenceDraft(_AxisDraftBase):
    axis_id: Literal["SAMPLE_AND_WINDOW_DEPENDENCE"]
    threshold_id: Literal["growth_action_value.primary_window_stability_gate"]
    recommended_option_id: Literal["FIXED_PRIMARY_WINDOW_STABILITY"]
    direction: Literal[ThresholdDirection.COMPOSITE_ALL]
    minimum_independent_action_count: int = Field(ge=1)
    minimum_independent_action_count_per_slice: int = Field(ge=1)
    independence_gap_exchange_sessions: int = Field(ge=1)
    maximum_single_regime_contribution_share: Decimal = Field(
        gt=Decimal("0"), lt=Decimal("1")
    )
    contribution_share_basis: Literal["ABSOLUTE_NET_NON_BETA_ACTION_VALUE"]
    mandatory_window_slices: tuple[str, ...]


class ActualPathTurnoverDraft(_AxisDraftBase):
    axis_id: Literal["ACTUAL_PATH_TURNOVER"]
    threshold_id: Literal["growth_action_value.actual_path_turnover_max"]
    recommended_option_id: Literal["HYBRID_PRECOMMITTED"]
    direction: Literal[ThresholdDirection.COMPOSITE_ALL]
    maximum_annualized_actual_path_turnover: Decimal = Field(gt=Decimal("0"))
    maximum_cost_drag_share: Decimal = Field(ge=Decimal("0"), lt=Decimal("1"))


class LeverageBetaAttributionDraft(_AxisDraftBase):
    axis_id: Literal["LEVERAGE_BETA_ATTRIBUTION"]
    threshold_id: Literal["growth_action_value.realized_beta_increment_max"]
    recommended_option_id: Literal["HYBRID_PRECOMMITTED"]
    direction: Literal[ThresholdDirection.COMPOSITE_ALL]
    maximum_realized_beta_increment: Decimal = Field(ge=Decimal("0"), lt=Decimal("1"))
    exposure_match_tolerance: Decimal = Field(ge=Decimal("0"), lt=Decimal("1"))
    leverage_etf_allowed: Literal[False]
    options_position_allowed: Literal[False]
    borrowed_leverage_allowed: Literal[False]


AxisValueDraft: TypeAlias = Annotated[
    NonBetaActionValueDraft
    | NetOfCostReturnDraft
    | ActualPathDrawdownRegressionDraft
    | FalseRiskOffCostDraft
    | CanonicalDqPitDraft
    | SampleAndWindowDependenceDraft
    | ActualPathTurnoverDraft
    | LeverageBetaAttributionDraft,
    Field(discriminator="axis_id"),
]


class OwnerReviewContract(_DraftModel):
    required_review_order: tuple[str, ...]
    allowed_axis_decisions: tuple[
        Literal["APPROVE_EXACTLY_AS_DRAFTED", "REJECT_AND_REQUEST_NEW_VERSION"], ...
    ]
    partial_review_state_allowed: Literal[True]
    partial_review_can_freeze: Literal[False]
    all_axes_approval_required_for_freeze: Literal[True]
    approval_must_precede_any_new_result: Literal[True]

    @model_validator(mode="after")
    def validate_contract(self) -> Self:
        if self.required_review_order != _AXIS_ORDER:
            raise ValueError("owner review order must cover all eight axes")
        if self.allowed_axis_decisions != (
            "APPROVE_EXACTLY_AS_DRAFTED",
            "REJECT_AND_REQUEST_NEW_VERSION",
        ):
            raise ValueError("owner axis decision inventory drifted")
        return self


class DraftTerminal(_DraftModel):
    status: Literal["BLOCKED_OWNER_REVIEW"]
    next_action: Literal["PROJECT_OWNER_REVIEW_COMPLETE_EXACT_VALUE_SHEET"]
    threshold_bundle_frozen: Literal[False]
    dq_successor_authorized: Literal[False]
    empirical_successor_authorized: Literal[False]


class StrategyGrowthActionValueThresholdExactValueSheet(_DraftModel):
    schema_version: Literal[
        "strategy_growth_action_value_threshold_exact_value_sheet.v1"
    ]
    sheet_id: Literal["strategy_growth_action_value_threshold_exact_value_sheet_v1"]
    sheet_version: Literal["1.0.0-draft.1"]
    sheet_status: Literal["DRAFT_FOR_OWNER_REVIEW"]
    task_id: Literal[
        "TRADING-2542_GROWTH_ACTION_VALUE_THRESHOLD_POLICY_DECISION_PACK_AND_FREEZE_V1"
    ]
    owner_instruction: OwnerInstruction
    policy_metadata: PolicyMetadata
    decision_pack_binding: DecisionPackBinding
    scope_binding: ScopeBinding
    decision_timing: DraftDecisionTiming
    measurement_policy: MeasurementPolicy
    window_slice_catalog: tuple[WindowSlice, ...]
    numeric_dq_policy_draft: NumericDqPolicyDraft
    axis_values: tuple[AxisValueDraft, ...]
    owner_review_contract: OwnerReviewContract
    terminal: DraftTerminal
    safety: DecisionPackSafety

    @model_validator(mode="after")
    def validate_complete_draft(self) -> Self:
        actual_slices = tuple(
            (item.slice_id, item.start, item.end, item.role)
            for item in self.window_slice_catalog
        )
        if actual_slices != _WINDOW_SLICES:
            raise ValueError("window slice catalog drifted")

        axis_ids = tuple(item.axis_id for item in self.axis_values)
        if axis_ids != _AXIS_ORDER:
            raise ValueError("exact value sheet must contain all eight axes in canonical order")

        drawdown = self.axis_values[2]
        sample = self.axis_values[5]
        dq_axis = self.axis_values[4]
        non_beta = self.axis_values[0]
        net = self.axis_values[1]
        leverage = self.axis_values[7]
        if not isinstance(drawdown, ActualPathDrawdownRegressionDraft):
            raise TypeError("drawdown axis type drifted")
        if drawdown.mandatory_stress_slice_set != _ALL_SLICE_IDS:
            raise ValueError("mandatory stress slice set drifted")
        if not isinstance(sample, SampleAndWindowDependenceDraft):
            raise TypeError("sample axis type drifted")
        if sample.mandatory_window_slices != _PERIOD_SLICE_IDS:
            raise ValueError("mandatory window slice set drifted")
        if (
            sample.minimum_independent_action_count_per_slice
            * len(sample.mandatory_window_slices)
            > sample.minimum_independent_action_count
        ):
            raise ValueError("per-slice action floors exceed the full-window action floor")
        if not isinstance(dq_axis, CanonicalDqPitDraft):
            raise TypeError("DQ axis type drifted")
        if (
            dq_axis.numeric_dq_threshold_policy_ref
            != self.numeric_dq_policy_draft.policy_ref
        ):
            raise ValueError("numeric DQ policy reference drifted")
        if not isinstance(non_beta, NonBetaActionValueDraft) or not isinstance(
            net, NetOfCostReturnDraft
        ):
            raise TypeError("return axis type drifted")
        if net.minimum_net_of_cost_return_delta > non_beta.minimum_non_beta_return_delta:
            raise ValueError("net return floor cannot exceed the non-beta gross floor")
        if not isinstance(leverage, LeverageBetaAttributionDraft):
            raise TypeError("leverage axis type drifted")
        if leverage.exposure_match_tolerance > leverage.maximum_realized_beta_increment:
            raise ValueError("exposure match tolerance cannot exceed maximum beta increment")
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
                raise TypeError("exact value sheet JSON root must be an object")
            sheet = cls.model_validate_json(raw)
            if raw != sheet.canonical_bytes:
                raise ValueError("exact value sheet is not canonical JSON bytes")
            return sheet
        except (TypeError, ValueError) as exc:
            raise StrategyGrowthActionValueThresholdExactValueSheetError(
                "GROWTH_THRESHOLD_EXACT_VALUE_SHEET_RECORD_INVALID", str(exc)
            ) from exc


@dataclass(frozen=True)
class StrategyGrowthActionValueThresholdExactValueSheetLoadResult:
    sheet: StrategyGrowthActionValueThresholdExactValueSheet
    sheet_path: Path
    sheet_file_sha256: str
    sheet_canonical_sha256: str
    decision_pack: StrategyGrowthActionValueThresholdDecisionPackLoadResult


def _reconcile_decision_pack(
    *,
    sheet: StrategyGrowthActionValueThresholdExactValueSheet,
    pack_result: StrategyGrowthActionValueThresholdDecisionPackLoadResult,
) -> None:
    binding = sheet.decision_pack_binding
    if pack_result.pack_file_sha256 != binding.file_sha256:
        raise ValueError("decision-pack file SHA-256 mismatch")
    if pack_result.pack_canonical_sha256 != binding.canonical_sha256:
        raise ValueError("decision-pack canonical SHA-256 mismatch")
    if sheet.scope_binding != pack_result.pack.scope_binding:
        raise ValueError("decision-pack scope binding mismatch")

    pack_axes = {item.axis_id.value: item for item in pack_result.pack.axis_gap_matrix}
    for draft in sheet.axis_values:
        source_axis = pack_axes[draft.axis_id]
        if draft.threshold_id != source_axis.threshold_id:
            raise ValueError(f"threshold id mismatch: {draft.axis_id}")
        if draft.recommended_option_id != source_axis.recommended_option_id:
            raise ValueError(f"recommended source mismatch: {draft.axis_id}")
        if draft.direction != source_axis.proposed_direction:
            raise ValueError(f"threshold direction mismatch: {draft.axis_id}")
        draft_fields = draft.model_fields_set
        if any(field not in draft_fields for field in source_axis.owner_value_fields):
            raise ValueError(f"owner value field missing: {draft.axis_id}")


def load_strategy_growth_action_value_threshold_exact_value_sheet(
    *,
    sheet_path: Path = DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_THRESHOLD_EXACT_VALUE_SHEET_PATH,
    project_root: Path = PROJECT_ROOT,
) -> StrategyGrowthActionValueThresholdExactValueSheetLoadResult:
    try:
        path = _bound_file(sheet_path, root=project_root, field="sheet_path")
        raw = path.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(sheet_path))
        sheet = StrategyGrowthActionValueThresholdExactValueSheet.model_validate(payload)

        pack_path = _bound_file(
            Path(sheet.decision_pack_binding.path),
            root=project_root,
            field="decision_pack_path",
        )
        pack_result = load_strategy_growth_action_value_threshold_decision_pack(
            pack_path=pack_path,
            project_root=project_root,
        )
        _reconcile_decision_pack(sheet=sheet, pack_result=pack_result)
    except StrategyGrowthActionValueThresholdExactValueSheetError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise StrategyGrowthActionValueThresholdExactValueSheetError(
            "GROWTH_THRESHOLD_EXACT_VALUE_SHEET_REJECTED", str(exc)
        ) from exc
    return StrategyGrowthActionValueThresholdExactValueSheetLoadResult(
        sheet=sheet,
        sheet_path=path,
        sheet_file_sha256=hashlib.sha256(raw).hexdigest(),
        sheet_canonical_sha256=sheet.canonical_sha256,
        decision_pack=pack_result,
    )


__all__ = [
    "DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_THRESHOLD_EXACT_VALUE_SHEET_PATH",
    "ActualPathDrawdownRegressionDraft",
    "ActualPathTurnoverDraft",
    "AttributionConfidenceRule",
    "CanonicalDqPitDraft",
    "DraftDecisionTiming",
    "DraftTerminal",
    "FalseRiskOffCostDraft",
    "FalseRiskOffEventDefinition",
    "LeverageBetaAttributionDraft",
    "MeasurementPolicy",
    "NetOfCostReturnDraft",
    "NonBetaActionValueDraft",
    "NumericDqPolicyDraft",
    "OwnerInstruction",
    "OwnerReviewContract",
    "SampleAndWindowDependenceDraft",
    "StrategyGrowthActionValueThresholdExactValueSheet",
    "StrategyGrowthActionValueThresholdExactValueSheetError",
    "StrategyGrowthActionValueThresholdExactValueSheetLoadResult",
    "WindowSlice",
    "load_strategy_growth_action_value_threshold_exact_value_sheet",
]
