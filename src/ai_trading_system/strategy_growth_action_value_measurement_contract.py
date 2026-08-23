from __future__ import annotations

import hashlib
import json
import math
import random
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Literal, Self, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.strategy_growth_action_value_threshold_decision_pack import (
    DecisionPackSafety,
    ScopeBinding,
)
from ai_trading_system.strategy_growth_action_value_threshold_exact_value_sheet import (
    StrategyGrowthActionValueThresholdExactValueSheetLoadResult,
    load_strategy_growth_action_value_threshold_exact_value_sheet,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_EXPOSURE_MATCHED_NO_SIGNAL_COMPARATOR_CONTRACT_PATH = Path(
    "config/research/exposure_matched_no_signal_comparator_contract_v1.yaml"
)
DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_MEASUREMENT_CONTRACT_PATH = Path(
    "config/research/strategy_growth_action_value_threshold_exact_value_sheet_v2.yaml"
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
_ALL_SLICE_IDS = tuple(item[0] for item in _WINDOW_SLICES)
_PERIOD_SLICE_IDS = _ALL_SLICE_IDS[1:]


class StrategyGrowthActionValueMeasurementContractError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class _StrictModel(BaseModel):
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


class _CanonicalModel(_StrictModel):
    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        payload = _duplicate_key_rejecting_json(raw)
        if not isinstance(payload, dict):
            raise ValueError("canonical record root must be an object")
        record = cls.model_validate(payload)
        if raw != record.canonical_bytes:
            raise ValueError("record is not canonical JSON bytes")
        return record


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


class DecimalBounds(_StrictModel):
    minimum: Decimal
    maximum: Decimal

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if self.minimum != Decimal("0.0000") or self.maximum != Decimal("1.0000"):
            raise ValueError("candidate exposure bounds must remain [0, 1]")
        return self


class ComparatorCalendarContract(_StrictModel):
    calendar: Literal["QQQ_EXCHANGE_SESSIONS"]
    requested_start: Literal["2021-02-22"]
    requested_end: Literal["2025-12-02"]
    join_rule: Literal["EXACT_COMMON_SESSION_INTERSECTION"]
    duplicate_session_outcome: Literal["INVALID"]
    missing_session_outcome: Literal["INVALID"]
    non_finite_return_outcome: Literal["INVALID"]
    return_less_than_or_equal_to_minus_one_outcome: Literal["INVALID"]


class ComparatorReturnContract(_StrictModel):
    source_field: Literal["DAILY_TOTAL_RETURN"]
    price_only_return_allowed: Literal[False]
    corporate_action_adjustment: Literal["PROVIDER_TOTAL_RETURN_SERIES"]
    qqq_factor_field: Literal["QQQ_DAILY_TOTAL_RETURN"]
    sgov_field: Literal["SGOV_DAILY_TOTAL_RETURN"]


class ComparatorConstructionContract(_StrictModel):
    candidate_exposure_field: Literal["ACTUAL_PRE_TRADE_QQQ_NOTIONAL_OVER_OPENING_NAV"]
    candidate_exposure_bounds: DecimalBounds
    fixed_qqq_weight_formula: Literal[
        "ARITHMETIC_MEAN_OF_CANDIDATE_OPENING_QQQ_EXPOSURE_ON_COMMON_SESSIONS"
    ]
    fixed_sgov_weight_formula: Literal["ONE_MINUS_FIXED_QQQ_WEIGHT"]
    weight_application_timing: Literal["PRIOR_SESSION_CLOSE_FOR_NEXT_SESSION_TOTAL_RETURN"]
    rebalance_frequency: Literal["EACH_COMMON_SESSION_TO_FIXED_WEIGHTS"]
    same_session_opposite_trades_can_net: Literal[False]
    return_outcome_can_select_weight: Literal[False]
    growth_signal_value_or_timestamp_read_allowed: Literal[False]
    candidate_return_read_allowed_for_construction: Literal[False]
    baseline_return_read_allowed_for_construction: Literal[False]
    cost_model_policy_id: Literal["transaction_cost_model_v1"]
    cost_treatment: Literal["DEDUCT_MODELED_REBALANCE_COST_FROM_COMPARATOR_DAILY_GROSS_RETURN"]


class ComparatorExposureMatchContract(_StrictModel):
    metric: Literal["ABSOLUTE_REALIZED_DAILY_OLS_BETA_DIFFERENCE"]
    factor: Literal["QQQ_DAILY_TOTAL_RETURN"]
    intercept: Literal[True]
    beta_annualized: Literal[False]
    minimum_common_sessions: Literal[252]
    maximum_mismatch: Decimal
    comparison: Literal["ABS_BETA_CANDIDATE_MINUS_BETA_COMPARATOR"]
    mismatch_above_limit_outcome: Literal["FAIL"]
    undefined_beta_outcome: Literal["INSUFFICIENT"]

    @field_validator("maximum_mismatch")
    @classmethod
    def validate_mismatch(cls, value: Decimal) -> Decimal:
        if value != Decimal("0.0100"):
            raise ValueError("exposure match tolerance drifted")
        return value


class ComparatorIdentityContract(_StrictModel):
    version_change_required_for_any_semantic_change: Literal[True]
    exact_file_sha256_binding_required: Literal[True]
    canonical_sha256_binding_required: Literal[True]
    parameter_search_allowed: Literal[False]
    result_conditioned_rewrite_allowed: Literal[False]


class ComparatorUniverse(_StrictModel):
    allowed_assets: tuple[Literal["QQQ", "SGOV"], ...]
    leverage_etf_allowed: Literal[False]
    options_position_allowed: Literal[False]
    borrowed_leverage_allowed: Literal[False]

    @field_validator("allowed_assets")
    @classmethod
    def validate_assets(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != ("QQQ", "SGOV"):
            raise ValueError("comparator universe must be exactly QQQ/SGOV")
        return value


class ExposureMatchedNoSignalComparatorContract(_CanonicalModel):
    schema_version: Literal["exposure_matched_no_signal_comparator_contract.v1"]
    comparator_id: Literal["exposure_matched_no_signal"]
    contract_version: Literal["1.0.0-draft.1"]
    contract_status: Literal["DRAFT_FOR_OWNER_REVIEW"]
    task_id: Literal[
        "TRADING-2542A_GROWTH_ACTION_VALUE_EXACT_MEASUREMENT_AND_JOINT_DECISION_CONTRACT_V1"
    ]
    owner_decision: Literal[
        "owner_decision:TRADING-2542A:2026-08-23:adopt_gpt_pro_review_and_request_measurement_complete_v2_draft_v1"
    ]
    role: Literal["RESEARCH_ATTRIBUTION_ONLY_NOT_TRADABLE"]
    objective: str
    universe: ComparatorUniverse
    calendar_contract: ComparatorCalendarContract
    return_contract: ComparatorReturnContract
    construction_contract: ComparatorConstructionContract
    exposure_match_contract: ComparatorExposureMatchContract
    identity_contract: ComparatorIdentityContract
    safety: DecisionPackSafety

    @field_validator("objective")
    @classmethod
    def validate_objective(cls, value: str) -> str:
        return _non_empty(value)


@dataclass(frozen=True)
class ExposureMatchedNoSignalComparatorContractLoadResult:
    contract: ExposureMatchedNoSignalComparatorContract
    contract_path: Path
    contract_file_sha256: str
    contract_canonical_sha256: str


class OwnerInstruction(_StrictModel):
    decision_id: Literal[
        "owner_decision:TRADING-2542A:2026-08-23:adopt_gpt_pro_review_and_request_measurement_complete_v2_draft_v1"
    ]
    adopted_review_conclusion: Literal["REQUEST_NEW_VERSION_BEFORE_ANY_FREEZE"]
    exact_value_approval_state: Literal["NOT_PROVIDED"]
    complete_axis_set_review_required: Literal[True]
    partial_approval_can_freeze: Literal[False]


class ReviewEvidence(_StrictModel):
    reviewed_repository_commit: Literal["b70fe3963988241b187bc0d30bbc422eed2b2160"]
    conversation_url: Literal["https://chatgpt.com/c/6a8a90ac-2e40-83e8-9ce6-6fc1cfb4dfdd"]
    model_evidence: Literal["UI_PRO_AND_MODEL_SELF_REPORT_GPT_5_6_PRO_ROUTE_UNVERIFIED"]
    backend_route_attested: Literal[False]
    advisory_reconciled_by_project_owner: Literal[True]


class ShaBinding(_StrictModel):
    path: str
    file_sha256: str
    canonical_sha256: str

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return _non_empty(value)

    @field_validator("file_sha256", "canonical_sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if not _SHA256_PATTERN.fullmatch(value):
            raise ValueError("binding hash must be a lowercase SHA-256")
        return value


class PredecessorBinding(ShaBinding):
    disposition: Literal["REJECTED_FOR_FREEZE_RETAINED_IMMUTABLE"]


class ComparatorBinding(ShaBinding):
    comparator_id: Literal["exposure_matched_no_signal"]


class DecisionTiming(_StrictModel):
    state: Literal["PRE_EMPIRICAL_DRAFT_FOR_OWNER_REVIEW"]
    new_dq_result_visible: Literal[False]
    new_strategy_result_visible: Literal[False]
    holdout_result_visible: Literal[False]
    exact_owner_approval_visible: Literal[False]
    threshold_bundle_frozen: Literal[False]


class CommonSeriesContract(_StrictModel):
    calendar: Literal["QQQ_EXCHANGE_SESSIONS"]
    join_rule: Literal["EXACT_COMMON_SESSION_INTERSECTION"]
    return_field: Literal["DAILY_TOTAL_RETURN"]
    annualization_sessions: Literal[252]
    annualized_geometric_return_formula: Literal[
        "PRODUCT_ONE_PLUS_DAILY_RETURN_POWER_252_OVER_N_MINUS_ONE"
    ]
    return_delta_formula: Literal["ANNUALIZED_CANDIDATE_MINUS_ANNUALIZED_COMPARATOR"]
    duplicate_session_outcome: Literal["INVALID"]
    missing_required_session_outcome: Literal["INVALID"]
    non_finite_value_outcome: Literal["INVALID"]
    return_less_than_or_equal_to_minus_one_outcome: Literal["INVALID"]
    rounding_before_threshold_comparison_allowed: Literal[False]


class WindowSlice(_StrictModel):
    slice_id: str
    start: date
    end: date
    role: Literal["PRIMARY_AGGREGATE", "CALENDAR_STABILITY", "MANDATORY_STRESS"]


class OutcomeContract(_StrictModel):
    pass_rule: str
    fail_rule: str
    insufficient_rule: str
    invalid_rule: str

    @field_validator("pass_rule", "fail_rule", "insufficient_rule", "invalid_rule")
    @classmethod
    def validate_rule(cls, value: str) -> str:
        return _non_empty(value)


class AxisBase(_StrictModel):
    predecessor_disposition: Literal[
        "REJECT_AND_REQUEST_NEW_VERSION", "INSUFFICIENT_EVIDENCE_TO_APPROVE"
    ]
    owner_review_state: Literal["PENDING_OWNER_APPROVAL"]
    threshold_id: str
    unit: str
    direction: Literal["MINIMUM", "MAXIMUM", "EXACT_CATEGORICAL", "COMPOSITE_ALL"]
    outcome_contract: OutcomeContract

    @field_validator("threshold_id", "unit")
    @classmethod
    def validate_text(cls, value: str) -> str:
        return _non_empty(value)


class BootstrapContract(_StrictModel):
    method: Literal["CIRCULAR_MOVING_BLOCK_BOOTSTRAP"]
    paired_common_session_returns: Literal[True]
    block_length_sessions: Literal[20]
    resamples: Literal[10000]
    one_sided_confidence_level: Decimal
    lower_quantile: Decimal
    quantile_rule: Literal["NEAREST_RANK_CEILING_P_TIMES_N"]
    lower_bound_rule: Literal["STRICTLY_GREATER_THAN_ZERO"]
    random_seed: Literal[2542]

    @model_validator(mode="after")
    def validate_confidence(self) -> Self:
        if self.one_sided_confidence_level != Decimal("0.95"):
            raise ValueError("one-sided confidence must be 0.95")
        if self.lower_quantile != Decimal("0.05"):
            raise ValueError("bootstrap lower quantile must be 0.05")
        return self


class NonBetaActionValueAxis(AxisBase):
    axis_id: Literal["NON_BETA_ACTION_VALUE"]
    minimum_non_beta_return_delta: Decimal
    bootstrap: BootstrapContract


class NetOfCostReturnAxis(AxisBase):
    axis_id: Literal["NET_OF_COST_RETURN"]
    minimum_net_of_cost_return_delta: Decimal
    cost_reconciliation_tolerance: Decimal
    cost_model_policy_id: Literal["transaction_cost_model_v1"]
    candidate_net_daily_return_formula: Literal[
        "CANDIDATE_GROSS_DAILY_RETURN_MINUS_MODELED_DAILY_COST_RETURN"
    ]
    annualized_cost_drag_formula: Literal["ANNUALIZED_GROSS_RETURN_MINUS_ANNUALIZED_NET_RETURN"]


class DrawdownAxis(AxisBase):
    axis_id: Literal["ACTUAL_PATH_DRAWDOWN_REGRESSION"]
    maximum_actual_path_drawdown_regression: Decimal
    nav_reset_rule: Literal["RESET_NAV_TO_ONE_AT_FIRST_COMMON_SESSION_OF_EACH_SLICE"]
    max_drawdown_formula: Literal["MIN_NAV_DIVIDED_BY_RUNNING_PEAK_MINUS_ONE"]
    regression_formula: Literal["ABS_CANDIDATE_MAX_DRAWDOWN_MINUS_ABS_COMPARATOR_MAX_DRAWDOWN"]
    mandatory_slice_set: tuple[str, ...]


class FalseRiskOffEventContract(_StrictModel):
    anchor_rule: Literal["BASELINE_DEFENSIVE_VETO_INACTIVE_TO_ACTIVE_TRANSITION"]
    forward_horizon_exchange_sessions: Literal[20]
    forward_window_start_offset_sessions: Literal[1]
    right_censored_anchor_rule: Literal[
        "EXCLUDE_FROM_NUMERIC_STATISTIC_AND_COUNT_AS_RIGHT_CENSORED"
    ]
    merge_distance_exchange_sessions: Literal[20]
    merged_episode_anchor_rule: Literal["KEEP_EARLIEST_ANCHOR"]
    qqq_minus_sgov_forward_compounded_return_min: Decimal
    qqq_forward_path_max_drawdown_floor: Decimal
    path_daily_missed_return_formula: Literal[
        "ONE_MINUS_OPENING_QQQ_WEIGHT_TIMES_QQQ_MINUS_SGOV_DAILY_TOTAL_RETURN"
    ]
    path_event_cost_formula: Literal["PRODUCT_ONE_PLUS_DAILY_MISSED_RETURN_MINUS_ONE"]
    event_regression_formula: Literal["CANDIDATE_EVENT_COST_MINUS_BASELINE_EVENT_COST"]
    aggregate_formula: Literal["ARITHMETIC_MEAN_OF_QUALIFYING_EVENT_REGRESSIONS"]


class FalseRiskOffCostAxis(AxisBase):
    axis_id: Literal["FALSE_RISK_OFF_COST"]
    maximum_false_risk_off_cost_regression: Decimal
    minimum_independent_qualifying_event_count: Literal[10]
    event_contract: FalseRiskOffEventContract


class NumericDqIntentDraft(_StrictModel):
    status: Literal["OWNER_INTENT_ONLY_NOT_EXECUTABLE_AUTHORITY"]
    max_quote_age_seconds: Literal[120]
    max_relative_spread: Decimal
    min_open_interest: Literal[10]
    min_volume: Literal[1]
    exact_source_date_required: Literal[True]
    unknown_can_pass: Literal[False]


class CanonicalDqPitAxis(AxisBase):
    axis_id: Literal["CANONICAL_DQ_PIT"]
    required_data_research_gate_status: Literal["PASS"]
    operational_authority_state: Literal["UNAVAILABLE_PENDING_INDEPENDENT_SERIAL_DQ_CONTRACT"]
    numeric_intent_draft: NumericDqIntentDraft
    required_serial_contract_fields: tuple[str, ...]


class EpisodeContract(_StrictModel):
    anchor_rule: Literal["GROWTH_ACTION_INACTIVE_TO_ACTIVE_TRANSITION"]
    merge_distance_exchange_sessions: Literal[20]
    merged_episode_anchor_rule: Literal["KEEP_EARLIEST_ANCHOR"]
    slice_assignment_rule: Literal["EARLIEST_ANCHOR_SESSION_SLICE"]
    cross_slice_double_count_allowed: Literal[False]
    episode_value_formula: Literal[
        "COMPOUNDED_CANDIDATE_NET_RETURN_MINUS_COMPOUNDED_COMPARATOR_NET_RETURN"
    ]
    contribution_numerator: Literal["SUM_ABS_EPISODE_VALUE_WITH_ANCHOR_IN_SLICE"]
    contribution_denominator: Literal["SUM_ABS_EPISODE_VALUE_ALL_EPISODES"]
    nonpositive_denominator_outcome: Literal["INSUFFICIENT"]


class SampleAndWindowDependenceAxis(AxisBase):
    axis_id: Literal["SAMPLE_AND_WINDOW_DEPENDENCE"]
    minimum_independent_action_count: Literal[30]
    minimum_independent_action_count_per_slice: Literal[5]
    independence_gap_exchange_sessions: Literal[20]
    maximum_single_regime_contribution_share: Decimal
    episode_contract: EpisodeContract
    mandatory_window_slices: tuple[str, ...]


class ActualPathTurnoverAxis(AxisBase):
    axis_id: Literal["ACTUAL_PATH_TURNOVER"]
    maximum_annualized_actual_path_turnover: Decimal
    maximum_cost_drag_share: Decimal
    session_turnover_formula: Literal["SUM_ABS_FILL_NOTIONAL_DIVIDED_BY_OPENING_NAV"]
    half_turnover_multiplier_allowed: Literal[False]
    same_session_opposite_fills_can_net: Literal[False]
    annualized_turnover_formula: Literal[
        "SUM_SESSION_TURNOVER_TIMES_252_DIVIDED_BY_COMMON_SESSION_COUNT"
    ]
    annualized_cost_drag_formula: Literal[
        "ANNUALIZED_CANDIDATE_GROSS_RETURN_MINUS_ANNUALIZED_CANDIDATE_NET_RETURN"
    ]
    gross_non_beta_edge_formula: Literal[
        "ANNUALIZED_CANDIDATE_GROSS_RETURN_MINUS_ANNUALIZED_COMPARATOR_GROSS_RETURN"
    ]
    cost_drag_share_formula: Literal["ANNUALIZED_COST_DRAG_DIVIDED_BY_GROSS_NON_BETA_EDGE"]
    nonpositive_cost_drag_denominator_outcome: Literal["FAIL"]


class BetaContract(_StrictModel):
    method: Literal["DAILY_OLS_SLOPE_WITH_INTERCEPT"]
    factor: Literal["QQQ_DAILY_TOTAL_RETURN"]
    annualized: Literal[False]
    common_session_rule: Literal["EXACT_COMMON_SESSION_INTERSECTION"]
    minimum_common_sessions: Literal[252]
    candidate_beta_formula: Literal["COVARIANCE_CANDIDATE_QQQ_DIVIDED_BY_VARIANCE_QQQ"]
    comparator_beta_formula: Literal["COVARIANCE_COMPARATOR_QQQ_DIVIDED_BY_VARIANCE_QQQ"]
    increment_formula: Literal["CANDIDATE_BETA_MINUS_COMPARATOR_BETA"]
    zero_factor_variance_outcome: Literal["INSUFFICIENT"]


class LeverageBetaAttributionAxis(AxisBase):
    axis_id: Literal["LEVERAGE_BETA_ATTRIBUTION"]
    maximum_realized_beta_increment: Decimal
    exposure_match_tolerance: Decimal
    beta_contract: BetaContract
    leverage_etf_allowed: Literal[False]
    options_position_allowed: Literal[False]
    borrowed_leverage_allowed: Literal[False]


AxisContract: TypeAlias = Annotated[
    NonBetaActionValueAxis
    | NetOfCostReturnAxis
    | DrawdownAxis
    | FalseRiskOffCostAxis
    | CanonicalDqPitAxis
    | SampleAndWindowDependenceAxis
    | ActualPathTurnoverAxis
    | LeverageBetaAttributionAxis,
    Field(discriminator="axis_id"),
]


class JointTerminalContract(_StrictModel):
    required_axis_order: tuple[str, ...]
    precedence: tuple[Literal["INVALID", "FAIL", "INSUFFICIENT", "PASS"], ...]
    invalid_terminal: Literal["GLOBAL_INVALID"]
    fail_terminal: Literal["GLOBAL_FAIL"]
    insufficient_terminal: Literal["GLOBAL_INSUFFICIENT"]
    pass_terminal: Literal["GLOBAL_PASS"]
    weighted_compensation_allowed: Literal[False]
    majority_vote_allowed: Literal[False]
    seven_of_eight_allowed: Literal[False]

    @model_validator(mode="after")
    def validate_joint_rule(self) -> Self:
        if self.required_axis_order != _AXIS_ORDER:
            raise ValueError("joint terminal axis order drifted")
        if self.precedence != ("INVALID", "FAIL", "INSUFFICIENT", "PASS"):
            raise ValueError("joint terminal precedence drifted")
        return self


class OwnerReviewContract(_StrictModel):
    required_review_order: tuple[str, ...]
    allowed_axis_decisions: tuple[
        Literal["APPROVE_EXACTLY_AS_DRAFTED", "REJECT_AND_REQUEST_NEW_VERSION"], ...
    ]
    all_axes_approval_required_for_freeze: Literal[True]
    approval_must_precede_any_new_result: Literal[True]
    any_change_requires_new_version: Literal[True]

    @model_validator(mode="after")
    def validate_review_rule(self) -> Self:
        if self.required_review_order != _AXIS_ORDER:
            raise ValueError("owner review order drifted")
        if self.allowed_axis_decisions != (
            "APPROVE_EXACTLY_AS_DRAFTED",
            "REJECT_AND_REQUEST_NEW_VERSION",
        ):
            raise ValueError("owner decision inventory drifted")
        return self


class DraftTerminal(_StrictModel):
    status: Literal["BLOCKED_OWNER_REVIEW_AND_DQ_AUTHORITY"]
    next_action: Literal["PROJECT_OWNER_REVIEW_COMPLETE_V2_THEN_INDEPENDENT_DQ_CONTRACT"]
    threshold_bundle_frozen: Literal[False]
    dq_successor_authorized: Literal[False]
    empirical_successor_authorized: Literal[False]


class StrategyGrowthActionValueMeasurementContract(_CanonicalModel):
    schema_version: Literal["strategy_growth_action_value_threshold_exact_value_sheet.v2"]
    sheet_id: Literal["strategy_growth_action_value_threshold_exact_value_sheet_v2"]
    sheet_version: Literal["2.0.0-draft.1"]
    sheet_status: Literal["DRAFT_FOR_OWNER_REVIEW"]
    task_id: Literal[
        "TRADING-2542A_GROWTH_ACTION_VALUE_EXACT_MEASUREMENT_AND_JOINT_DECISION_CONTRACT_V1"
    ]
    owner_instruction: OwnerInstruction
    review_evidence: ReviewEvidence
    predecessor_binding: PredecessorBinding
    comparator_binding: ComparatorBinding
    scope_binding: ScopeBinding
    decision_timing: DecisionTiming
    common_series_contract: CommonSeriesContract
    window_slice_catalog: tuple[WindowSlice, ...]
    axis_contracts: tuple[AxisContract, ...]
    joint_terminal_contract: JointTerminalContract
    owner_review_contract: OwnerReviewContract
    terminal: DraftTerminal
    safety: DecisionPackSafety

    @model_validator(mode="after")
    def validate_complete_contract(self) -> Self:
        actual_slices = tuple(
            (item.slice_id, item.start, item.end, item.role) for item in self.window_slice_catalog
        )
        if actual_slices != _WINDOW_SLICES:
            raise ValueError("window slice catalog drifted")
        if tuple(item.axis_id for item in self.axis_contracts) != _AXIS_ORDER:
            raise ValueError("measurement contract must contain all axes in order")

        non_beta, net, drawdown, false_risk, dq, sample, turnover, beta = self.axis_contracts
        if not isinstance(non_beta, NonBetaActionValueAxis):
            raise TypeError("non-beta axis type drifted")
        if non_beta.minimum_non_beta_return_delta != Decimal("0.0100"):
            raise ValueError("non-beta economic floor drifted")
        if not isinstance(net, NetOfCostReturnAxis):
            raise TypeError("net-of-cost axis type drifted")
        if net.minimum_net_of_cost_return_delta != Decimal("0.0075"):
            raise ValueError("net-of-cost floor drifted")
        if net.cost_reconciliation_tolerance != Decimal("0.0001"):
            raise ValueError("cost reconciliation tolerance drifted")
        if not isinstance(drawdown, DrawdownAxis):
            raise TypeError("drawdown axis type drifted")
        if drawdown.maximum_actual_path_drawdown_regression != Decimal("0.0200"):
            raise ValueError("drawdown regression limit drifted")
        if drawdown.mandatory_slice_set != _ALL_SLICE_IDS:
            raise ValueError("drawdown slice set drifted")
        if not isinstance(false_risk, FalseRiskOffCostAxis):
            raise TypeError("false-risk-off axis type drifted")
        if false_risk.maximum_false_risk_off_cost_regression != Decimal("0.0025"):
            raise ValueError("false-risk-off regression limit drifted")
        if not isinstance(dq, CanonicalDqPitAxis):
            raise TypeError("DQ axis type drifted")
        if dq.predecessor_disposition != "INSUFFICIENT_EVIDENCE_TO_APPROVE":
            raise ValueError("DQ predecessor disposition drifted")
        if dq.numeric_intent_draft.max_relative_spread != Decimal("0.20"):
            raise ValueError("DQ numeric intent drifted")
        if len(dq.required_serial_contract_fields) != 6:
            raise ValueError("DQ serial contract inventory incomplete")
        if not isinstance(sample, SampleAndWindowDependenceAxis):
            raise TypeError("sample axis type drifted")
        if sample.maximum_single_regime_contribution_share != Decimal("0.50"):
            raise ValueError("regime contribution share drifted")
        if sample.mandatory_window_slices != _PERIOD_SLICE_IDS:
            raise ValueError("sample slice set drifted")
        if not isinstance(turnover, ActualPathTurnoverAxis):
            raise TypeError("turnover axis type drifted")
        if turnover.maximum_annualized_actual_path_turnover != Decimal("1.00"):
            raise ValueError("turnover limit drifted")
        if turnover.maximum_cost_drag_share != Decimal("0.25"):
            raise ValueError("cost-drag share limit drifted")
        if not isinstance(beta, LeverageBetaAttributionAxis):
            raise TypeError("beta axis type drifted")
        if beta.maximum_realized_beta_increment != Decimal("0.0200"):
            raise ValueError("beta increment limit drifted")
        if beta.exposure_match_tolerance != Decimal("0.0100"):
            raise ValueError("exposure match tolerance drifted")
        return self


@dataclass(frozen=True)
class StrategyGrowthActionValueMeasurementContractLoadResult:
    contract: StrategyGrowthActionValueMeasurementContract
    contract_path: Path
    contract_file_sha256: str
    contract_canonical_sha256: str
    predecessor: StrategyGrowthActionValueThresholdExactValueSheetLoadResult
    comparator: ExposureMatchedNoSignalComparatorContractLoadResult


def load_exposure_matched_no_signal_comparator_contract(
    *,
    contract_path: Path = DEFAULT_EXPOSURE_MATCHED_NO_SIGNAL_COMPARATOR_CONTRACT_PATH,
    project_root: Path = PROJECT_ROOT,
) -> ExposureMatchedNoSignalComparatorContractLoadResult:
    try:
        path = _bound_file(contract_path, root=project_root, field="contract_path")
        raw = path.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(contract_path))
        contract = ExposureMatchedNoSignalComparatorContract.model_validate(payload)
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise StrategyGrowthActionValueMeasurementContractError(
            "EXPOSURE_MATCHED_NO_SIGNAL_COMPARATOR_CONTRACT_REJECTED", str(exc)
        ) from exc
    return ExposureMatchedNoSignalComparatorContractLoadResult(
        contract=contract,
        contract_path=path,
        contract_file_sha256=hashlib.sha256(raw).hexdigest(),
        contract_canonical_sha256=contract.canonical_sha256,
    )


def load_strategy_growth_action_value_measurement_contract(
    *,
    contract_path: Path = DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_MEASUREMENT_CONTRACT_PATH,
    project_root: Path = PROJECT_ROOT,
) -> StrategyGrowthActionValueMeasurementContractLoadResult:
    try:
        path = _bound_file(contract_path, root=project_root, field="contract_path")
        raw = path.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(contract_path))
        contract = StrategyGrowthActionValueMeasurementContract.model_validate(payload)
        predecessor = load_strategy_growth_action_value_threshold_exact_value_sheet(
            sheet_path=Path(contract.predecessor_binding.path), project_root=project_root
        )
        comparator = load_exposure_matched_no_signal_comparator_contract(
            contract_path=Path(contract.comparator_binding.path), project_root=project_root
        )
        if predecessor.sheet_file_sha256 != contract.predecessor_binding.file_sha256:
            raise ValueError("predecessor file SHA-256 mismatch")
        if predecessor.sheet_canonical_sha256 != contract.predecessor_binding.canonical_sha256:
            raise ValueError("predecessor canonical SHA-256 mismatch")
        if comparator.contract_file_sha256 != contract.comparator_binding.file_sha256:
            raise ValueError("comparator file SHA-256 mismatch")
        if comparator.contract_canonical_sha256 != contract.comparator_binding.canonical_sha256:
            raise ValueError("comparator canonical SHA-256 mismatch")
        if comparator.contract.comparator_id != contract.comparator_binding.comparator_id:
            raise ValueError("comparator id mismatch")
    except StrategyGrowthActionValueMeasurementContractError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise StrategyGrowthActionValueMeasurementContractError(
            "GROWTH_ACTION_VALUE_MEASUREMENT_CONTRACT_REJECTED", str(exc)
        ) from exc
    return StrategyGrowthActionValueMeasurementContractLoadResult(
        contract=contract,
        contract_path=path,
        contract_file_sha256=hashlib.sha256(raw).hexdigest(),
        contract_canonical_sha256=contract.canonical_sha256,
        predecessor=predecessor,
        comparator=comparator,
    )


def _validated_returns(values: Sequence[float], *, label: str) -> tuple[float, ...]:
    result = tuple(float(value) for value in values)
    if not result:
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_SAMPLE_INSUFFICIENT", f"{label} is empty"
        )
    for value in result:
        if not math.isfinite(value) or value <= -1.0:
            raise StrategyGrowthActionValueMeasurementContractError(
                "MEASUREMENT_RETURN_DOMAIN_INVALID",
                f"{label} contains a non-finite return or return <= -1",
            )
    return result


def _paired_returns(
    candidate: Sequence[float], comparator: Sequence[float]
) -> tuple[tuple[float, ...], tuple[float, ...]]:
    candidate_values = _validated_returns(candidate, label="candidate_returns")
    comparator_values = _validated_returns(comparator, label="comparator_returns")
    if len(candidate_values) != len(comparator_values):
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_COMMON_SESSION_IDENTITY_INVALID",
            "paired return series lengths differ",
        )
    return candidate_values, comparator_values


def compounded_return(returns: Sequence[float]) -> float:
    values = _validated_returns(returns, label="returns")
    return math.exp(math.fsum(math.log1p(value) for value in values)) - 1.0


def annualized_geometric_return(
    returns: Sequence[float], *, annualization_sessions: int = 252
) -> float:
    values = _validated_returns(returns, label="returns")
    if annualization_sessions <= 0:
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_ANNUALIZATION_INVALID", "annualization_sessions must be positive"
        )
    return (
        math.exp(
            math.fsum(math.log1p(value) for value in values) * annualization_sessions / len(values)
        )
        - 1.0
    )


def annualized_return_delta(candidate: Sequence[float], comparator: Sequence[float]) -> float:
    candidate_values, comparator_values = _paired_returns(candidate, comparator)
    return annualized_geometric_return(candidate_values) - annualized_geometric_return(
        comparator_values
    )


def nearest_rank_quantile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_SAMPLE_INSUFFICIENT", "quantile sample is empty"
        )
    if not 0.0 < probability <= 1.0:
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_QUANTILE_INVALID", "probability must be in (0, 1]"
        )
    rank = max(1, math.ceil(probability * len(ordered)))
    return ordered[rank - 1]


def circular_moving_block_bootstrap_lower_bound(
    candidate: Sequence[float],
    comparator: Sequence[float],
    *,
    block_length_sessions: int = 20,
    resamples: int = 10000,
    lower_quantile: float = 0.05,
    random_seed: int = 2542,
) -> float:
    candidate_values, comparator_values = _paired_returns(candidate, comparator)
    sample_size = len(candidate_values)
    if block_length_sessions <= 0 or block_length_sessions > sample_size:
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_SAMPLE_INSUFFICIENT",
            "bootstrap block length exceeds the common sample",
        )
    if resamples <= 0:
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_BOOTSTRAP_INVALID", "resamples must be positive"
        )
    generator = random.Random(random_seed)
    replicates: list[float] = []
    for _ in range(resamples):
        indices: list[int] = []
        while len(indices) < sample_size:
            start = generator.randrange(sample_size)
            indices.extend(
                (start + offset) % sample_size for offset in range(block_length_sessions)
            )
        selected = indices[:sample_size]
        replicates.append(
            annualized_return_delta(
                tuple(candidate_values[index] for index in selected),
                tuple(comparator_values[index] for index in selected),
            )
        )
    return nearest_rank_quantile(replicates, lower_quantile)


def max_drawdown(returns: Sequence[float]) -> float:
    values = _validated_returns(returns, label="returns")
    nav = 1.0
    peak = 1.0
    worst = 0.0
    for value in values:
        nav *= 1.0 + value
        peak = max(peak, nav)
        worst = min(worst, nav / peak - 1.0)
    return worst


def drawdown_regression(candidate: Sequence[float], comparator: Sequence[float]) -> float:
    candidate_values, comparator_values = _paired_returns(candidate, comparator)
    return abs(max_drawdown(candidate_values)) - abs(max_drawdown(comparator_values))


def activation_anchors(active: Sequence[bool]) -> tuple[int, ...]:
    values = tuple(bool(value) for value in active)
    return tuple(
        index
        for index, value in enumerate(values)
        if value and (index == 0 or not values[index - 1])
    )


def merge_anchor_indices(
    anchors: Sequence[int], *, merge_distance_sessions: int = 20
) -> tuple[int, ...]:
    if merge_distance_sessions < 0:
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_EPISODE_RULE_INVALID",
            "merge distance cannot be negative",
        )
    ordered = tuple(int(anchor) for anchor in anchors)
    if any(anchor < 0 for anchor in ordered) or any(
        right <= left for left, right in zip(ordered, ordered[1:], strict=False)
    ):
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_EPISODE_IDENTITY_INVALID",
            "anchors must be unique, nonnegative, and strictly increasing",
        )
    merged: list[int] = []
    for anchor in ordered:
        if not merged or anchor - merged[-1] > merge_distance_sessions:
            merged.append(anchor)
    return tuple(merged)


def event_missed_return_cost(
    opening_qqq_weights: Sequence[float],
    qqq_returns: Sequence[float],
    sgov_returns: Sequence[float],
) -> float:
    qqq_values, sgov_values = _paired_returns(qqq_returns, sgov_returns)
    weights = tuple(float(value) for value in opening_qqq_weights)
    if len(weights) != len(qqq_values):
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_COMMON_SESSION_IDENTITY_INVALID",
            "weight and return series lengths differ",
        )
    if any(not math.isfinite(value) or not 0.0 <= value <= 1.0 for value in weights):
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_WEIGHT_DOMAIN_INVALID", "QQQ weights must be finite in [0, 1]"
        )
    daily_cost = tuple(
        (1.0 - weight) * (qqq_return - sgov_return)
        for weight, qqq_return, sgov_return in zip(weights, qqq_values, sgov_values, strict=True)
    )
    return compounded_return(daily_cost)


@dataclass(frozen=True)
class FalseRiskOffEventWindow:
    anchor_index: int
    forward_start_index: int
    forward_end_index_exclusive: int | None
    right_censored: bool
    qualifies: bool | None
    qqq_minus_sgov_forward_return: float | None
    qqq_forward_max_drawdown: float | None


def false_risk_off_event_windows(
    baseline_defensive_veto_active: Sequence[bool],
    qqq_returns: Sequence[float],
    sgov_returns: Sequence[float],
    *,
    horizon_sessions: int = 20,
    merge_distance_sessions: int = 20,
    qqq_minus_sgov_minimum: float = 0.03,
    qqq_drawdown_floor: float = -0.05,
) -> tuple[FalseRiskOffEventWindow, ...]:
    qqq_values, sgov_values = _paired_returns(qqq_returns, sgov_returns)
    active = tuple(bool(value) for value in baseline_defensive_veto_active)
    if len(active) != len(qqq_values):
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_COMMON_SESSION_IDENTITY_INVALID",
            "defensive-veto and return series lengths differ",
        )
    if horizon_sessions <= 0:
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_EVENT_RULE_INVALID", "event horizon must be positive"
        )
    anchors = merge_anchor_indices(
        activation_anchors(active), merge_distance_sessions=merge_distance_sessions
    )
    windows: list[FalseRiskOffEventWindow] = []
    for anchor in anchors:
        start = anchor + 1
        end = start + horizon_sessions
        if end > len(qqq_values):
            windows.append(
                FalseRiskOffEventWindow(
                    anchor_index=anchor,
                    forward_start_index=start,
                    forward_end_index_exclusive=None,
                    right_censored=True,
                    qualifies=None,
                    qqq_minus_sgov_forward_return=None,
                    qqq_forward_max_drawdown=None,
                )
            )
            continue
        qqq_window = qqq_values[start:end]
        sgov_window = sgov_values[start:end]
        forward_spread = compounded_return(qqq_window) - compounded_return(sgov_window)
        forward_drawdown = max_drawdown(qqq_window)
        windows.append(
            FalseRiskOffEventWindow(
                anchor_index=anchor,
                forward_start_index=start,
                forward_end_index_exclusive=end,
                right_censored=False,
                qualifies=(
                    forward_spread >= qqq_minus_sgov_minimum
                    and forward_drawdown >= qqq_drawdown_floor
                ),
                qqq_minus_sgov_forward_return=forward_spread,
                qqq_forward_max_drawdown=forward_drawdown,
            )
        )
    return tuple(windows)


def fixed_exposure_matched_qqq_weight(
    candidate_opening_qqq_exposures: Sequence[float],
) -> float:
    exposures = tuple(float(value) for value in candidate_opening_qqq_exposures)
    if not exposures:
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_SAMPLE_INSUFFICIENT", "candidate exposure series is empty"
        )
    if any(not math.isfinite(value) or not 0.0 <= value <= 1.0 for value in exposures):
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_WEIGHT_DOMAIN_INVALID",
            "candidate opening QQQ exposures must be finite in [0, 1]",
        )
    return math.fsum(exposures) / len(exposures)


def fixed_weight_qqq_sgov_gross_returns(
    fixed_qqq_weight: float,
    qqq_returns: Sequence[float],
    sgov_returns: Sequence[float],
) -> tuple[float, ...]:
    if not math.isfinite(fixed_qqq_weight) or not 0.0 <= fixed_qqq_weight <= 1.0:
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_WEIGHT_DOMAIN_INVALID", "fixed QQQ weight must be in [0, 1]"
        )
    qqq_values, sgov_values = _paired_returns(qqq_returns, sgov_returns)
    return tuple(
        fixed_qqq_weight * qqq_return + (1.0 - fixed_qqq_weight) * sgov_return
        for qqq_return, sgov_return in zip(qqq_values, sgov_values, strict=True)
    )


def annualized_one_way_turnover(
    fill_notionals_by_session: Sequence[Sequence[float]],
    opening_nav_by_session: Sequence[float],
    *,
    annualization_sessions: int = 252,
) -> float:
    fills = tuple(tuple(float(value) for value in session) for session in fill_notionals_by_session)
    navs = tuple(float(value) for value in opening_nav_by_session)
    if not fills or len(fills) != len(navs):
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_COMMON_SESSION_IDENTITY_INVALID",
            "fill and opening NAV session counts differ or are empty",
        )
    if any(not math.isfinite(nav) or nav <= 0.0 for nav in navs):
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_NAV_DOMAIN_INVALID", "opening NAV must be finite and positive"
        )
    session_turnover = []
    for session_fills, nav in zip(fills, navs, strict=True):
        if any(not math.isfinite(value) for value in session_fills):
            raise StrategyGrowthActionValueMeasurementContractError(
                "MEASUREMENT_FILL_DOMAIN_INVALID", "fill notional must be finite"
            )
        session_turnover.append(math.fsum(abs(value) for value in session_fills) / nav)
    return math.fsum(session_turnover) * annualization_sessions / len(session_turnover)


@dataclass(frozen=True)
class CostDragShareResult:
    annualized_cost_drag: float
    gross_non_beta_edge: float
    cost_drag_share: float | None
    denominator_outcome: Literal["VALID", "FAIL_NONPOSITIVE"]


def cost_drag_share(
    candidate_gross_returns: Sequence[float],
    candidate_net_returns: Sequence[float],
    comparator_gross_returns: Sequence[float],
) -> CostDragShareResult:
    gross_values, net_values = _paired_returns(candidate_gross_returns, candidate_net_returns)
    gross_values, comparator_values = _paired_returns(gross_values, comparator_gross_returns)
    annualized_gross = annualized_geometric_return(gross_values)
    annualized_net = annualized_geometric_return(net_values)
    annualized_comparator = annualized_geometric_return(comparator_values)
    drag = annualized_gross - annualized_net
    edge = annualized_gross - annualized_comparator
    if edge <= 0.0:
        return CostDragShareResult(
            annualized_cost_drag=drag,
            gross_non_beta_edge=edge,
            cost_drag_share=None,
            denominator_outcome="FAIL_NONPOSITIVE",
        )
    return CostDragShareResult(
        annualized_cost_drag=drag,
        gross_non_beta_edge=edge,
        cost_drag_share=drag / edge,
        denominator_outcome="VALID",
    )


def daily_ols_beta_with_intercept(
    dependent_returns: Sequence[float],
    qqq_factor_returns: Sequence[float],
    *,
    minimum_common_sessions: int = 252,
) -> float:
    dependent, factor = _paired_returns(dependent_returns, qqq_factor_returns)
    if len(dependent) < minimum_common_sessions:
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_SAMPLE_INSUFFICIENT",
            "beta common-session count is below the frozen floor",
        )
    dependent_mean = math.fsum(dependent) / len(dependent)
    factor_mean = math.fsum(factor) / len(factor)
    covariance_numerator = math.fsum(
        (dependent_value - dependent_mean) * (factor_value - factor_mean)
        for dependent_value, factor_value in zip(dependent, factor, strict=True)
    )
    variance_numerator = math.fsum((factor_value - factor_mean) ** 2 for factor_value in factor)
    if variance_numerator <= 0.0:
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_SAMPLE_INSUFFICIENT", "QQQ factor variance is zero"
        )
    return covariance_numerator / variance_numerator


def contribution_shares(
    episode_values: Sequence[float], episode_slice_ids: Sequence[str]
) -> dict[str, float]:
    values = tuple(float(value) for value in episode_values)
    slices = tuple(str(value) for value in episode_slice_ids)
    if not values or len(values) != len(slices):
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_SAMPLE_INSUFFICIENT",
            "episode value and slice inventories differ or are empty",
        )
    if any(not math.isfinite(value) for value in values):
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_EPISODE_VALUE_INVALID", "episode values must be finite"
        )
    if any(slice_id not in _PERIOD_SLICE_IDS for slice_id in slices):
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_EPISODE_IDENTITY_INVALID", "episode slice is not mandatory"
        )
    denominator = math.fsum(abs(value) for value in values)
    if denominator <= 0.0:
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_SAMPLE_INSUFFICIENT",
            "episode contribution denominator is nonpositive",
        )
    return {
        slice_id: math.fsum(
            abs(value)
            for value, assigned_slice in zip(values, slices, strict=True)
            if assigned_slice == slice_id
        )
        / denominator
        for slice_id in _PERIOD_SLICE_IDS
    }


AxisOutcome = Literal["PASS", "FAIL", "INSUFFICIENT", "INVALID"]
GlobalTerminal = Literal["GLOBAL_PASS", "GLOBAL_FAIL", "GLOBAL_INSUFFICIENT", "GLOBAL_INVALID"]


def aggregate_joint_terminal(
    outcomes: Sequence[tuple[str, AxisOutcome]],
) -> GlobalTerminal:
    values = tuple(outcomes)
    if tuple(axis_id for axis_id, _ in values) != _AXIS_ORDER:
        raise StrategyGrowthActionValueMeasurementContractError(
            "MEASUREMENT_AXIS_OUTCOME_SET_INVALID",
            "axis outcomes must be complete, unique, and in canonical order",
        )
    outcome_values = tuple(outcome for _, outcome in values)
    if any(outcome == "INVALID" for outcome in outcome_values):
        return "GLOBAL_INVALID"
    if any(outcome == "FAIL" for outcome in outcome_values):
        return "GLOBAL_FAIL"
    if any(outcome == "INSUFFICIENT" for outcome in outcome_values):
        return "GLOBAL_INSUFFICIENT"
    if all(outcome == "PASS" for outcome in outcome_values):
        return "GLOBAL_PASS"
    raise StrategyGrowthActionValueMeasurementContractError(
        "MEASUREMENT_AXIS_OUTCOME_INVALID", "unknown axis outcome"
    )


__all__ = [
    "DEFAULT_EXPOSURE_MATCHED_NO_SIGNAL_COMPARATOR_CONTRACT_PATH",
    "DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_MEASUREMENT_CONTRACT_PATH",
    "ActualPathTurnoverAxis",
    "AxisOutcome",
    "CanonicalDqPitAxis",
    "CostDragShareResult",
    "DrawdownAxis",
    "ExposureMatchedNoSignalComparatorContract",
    "ExposureMatchedNoSignalComparatorContractLoadResult",
    "FalseRiskOffCostAxis",
    "FalseRiskOffEventWindow",
    "GlobalTerminal",
    "LeverageBetaAttributionAxis",
    "NetOfCostReturnAxis",
    "NonBetaActionValueAxis",
    "SampleAndWindowDependenceAxis",
    "StrategyGrowthActionValueMeasurementContract",
    "StrategyGrowthActionValueMeasurementContractError",
    "StrategyGrowthActionValueMeasurementContractLoadResult",
    "activation_anchors",
    "aggregate_joint_terminal",
    "annualized_geometric_return",
    "annualized_one_way_turnover",
    "annualized_return_delta",
    "circular_moving_block_bootstrap_lower_bound",
    "compounded_return",
    "contribution_shares",
    "cost_drag_share",
    "daily_ols_beta_with_intercept",
    "drawdown_regression",
    "event_missed_return_cost",
    "false_risk_off_event_windows",
    "fixed_exposure_matched_qqq_weight",
    "fixed_weight_qqq_sgov_gross_returns",
    "load_exposure_matched_no_signal_comparator_contract",
    "load_strategy_growth_action_value_measurement_contract",
    "max_drawdown",
    "merge_anchor_indices",
    "nearest_rank_quantile",
]
