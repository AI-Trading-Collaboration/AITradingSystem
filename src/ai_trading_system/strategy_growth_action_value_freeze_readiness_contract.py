from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Literal, Self, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.strategy_growth_action_value_dq_pit_contract_v2 import (
    StrategyGrowthActionValueDqPitContractV2LoadResult,
    load_strategy_growth_action_value_dq_pit_contract_v2,
)
from ai_trading_system.strategy_growth_action_value_measurement_contract import (
    BetaContract,
    BootstrapContract,
    DecisionTiming,
    JointTerminalContract,
    OutcomeContract,
    OwnerReviewContract,
    StrategyGrowthActionValueMeasurementContractLoadResult,
    WindowSlice,
    compounded_return,
    load_strategy_growth_action_value_measurement_contract,
)
from ai_trading_system.strategy_growth_action_value_threshold_decision_pack import (
    DecisionPackSafety,
    ScopeBinding,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_FREEZE_READINESS_CONTRACT_PATH = Path(
    "config/research/strategy_growth_action_value_threshold_exact_value_sheet_v3.yaml"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SESSION_INVENTORY_SHA256 = "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
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


class StrategyGrowthActionValueFreezeReadinessContractError(ValueError):
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


class _CanonicalModel(_StrictModel):
    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


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


class OwnerInstruction(_StrictModel):
    decision_id: Literal[
        "owner_decision:TRADING-2542C:2026-08-23:continue_pro_review_successor_remediation_v1"
    ]
    adopted_review_disposition: Literal["REQUEST_NEW_VERSION_BEFORE_FREEZE"]
    exact_successor_approval_state: Literal["NOT_PROVIDED"]
    second_independent_review_required: Literal[True]
    no_separate_progress_confirmation_required: Literal[True]


class ReviewEvidence(_StrictModel):
    reviewed_repository_commit: Literal["1ca8ccf95c2a93a1b50164345d3e101a59b50838"]
    conversation_url: Literal["https://chatgpt.com/c/6a8ae448-a5b4-83e8-8d88-d7e6b22e0fc2"]
    visible_product_label: Literal["ChatGPT Pro"]
    visible_model_label: Literal["GPT-5.6 Pro"]
    backend_route_attestation: Literal["CANNOT_VERIFY_EXACT_BACKEND_ROUTE"]
    approved_axis_count: Literal[4]
    rejected_axis_count: Literal[4]
    insufficient_dq_numeric_count: Literal[4]


class ShaBinding(_StrictModel):
    path: str
    file_sha256: str
    canonical_sha256: str

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("binding path cannot be empty")
        return value

    @field_validator("file_sha256", "canonical_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256_PATTERN.fullmatch(value):
            raise ValueError("binding hash must be a lowercase SHA-256")
        return value


class PredecessorBinding(ShaBinding):
    path: Literal[
        "config/research/strategy_growth_action_value_threshold_exact_value_sheet_v2.yaml"
    ]
    file_sha256: Literal["ee1db5f51affe3c76e3b6fd9dc78dd7308b4b4999ed67c2853c116c079b0965d"]
    canonical_sha256: Literal["8da0aa87f463ee886d8195f39338c10af6fed536c1c982c0352c1cf37950fb7d"]
    disposition: Literal["REQUEST_NEW_VERSION_RETAINED_IMMUTABLE"]


class DqSuccessorBinding(ShaBinding):
    path: Literal["config/research/strategy_growth_action_value_canonical_dq_pit_contract_v2.yaml"]
    file_sha256: Literal["c9c74d5da0819f206ae59543dcab34a2f1f920687fd4bf646da49a4eabbbd327"]
    canonical_sha256: Literal["94e99dea15f0c62756f87230a7706d575b24e4c193db7bd4673ef2bb44427843"]
    contract_id: Literal["strategy_growth_action_value_canonical_dq_pit_contract_v2"]
    executable_authority: Literal[False]


class CommonSeriesContractV3(_StrictModel):
    calendar: Literal["QQQ_EXCHANGE_SESSIONS"]
    primary_window_start: Literal["2021-02-22"]
    primary_window_end: Literal["2025-12-02"]
    expected_session_count: Literal[1202]
    expected_session_inventory_lf_sha256: Literal[
        "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
    ]
    exact_expected_session_set_required: Literal[True]
    join_rule: Literal["EXACT_COMMON_SESSION_INTERSECTION_AFTER_INVENTORY_VALIDATION"]
    return_field: Literal["DAILY_TOTAL_RETURN"]
    annualization_sessions: Literal[252]
    annualized_geometric_return_formula: Literal[
        "PRODUCT_ONE_PLUS_DAILY_RETURN_POWER_252_OVER_N_MINUS_ONE"
    ]
    return_delta_formula: Literal["ANNUALIZED_CANDIDATE_MINUS_ANNUALIZED_COMPARATOR"]
    duplicate_missing_or_unexpected_session_outcome: Literal["INVALID"]
    simultaneous_candidate_and_comparator_session_drop_outcome: Literal["INVALID"]
    non_finite_value_outcome: Literal["INVALID"]
    return_less_than_or_equal_to_minus_one_outcome: Literal["INVALID"]
    rounding_before_threshold_comparison_allowed: Literal[False]


class AxisBaseV3(_StrictModel):
    predecessor_disposition: Literal["APPROVE_EXACTLY_AS_DRAFTED", "REJECT_AND_REQUEST_NEW_VERSION"]
    owner_review_state: Literal["PENDING_SUCCESSOR_OWNER_APPROVAL"]
    threshold_id: str
    unit: str
    direction: Literal["MINIMUM", "MAXIMUM", "EXACT_CATEGORICAL", "COMPOSITE_ALL"]
    outcome_contract: OutcomeContract


class NonBetaActionValueAxisV3(AxisBaseV3):
    axis_id: Literal["NON_BETA_ACTION_VALUE"]
    predecessor_disposition: Literal["APPROVE_EXACTLY_AS_DRAFTED"]
    minimum_non_beta_return_delta: Decimal
    minimum_common_sessions: Literal[252]
    complete_primary_inventory_required: Literal[True]
    bootstrap: BootstrapContract


class CostReconciliationContractV3(_StrictModel):
    candidate_residual_formula: Literal[
        "CANDIDATE_GROSS_DAILY_RETURN_MINUS_CANDIDATE_NET_DAILY_RETURN_MINUS_CANDIDATE_MODELED_DAILY_COST_RETURN"
    ]
    comparator_residual_formula: Literal[
        "COMPARATOR_GROSS_DAILY_RETURN_MINUS_COMPARATOR_NET_DAILY_RETURN_MINUS_COMPARATOR_MODELED_DAILY_COST_RETURN"
    ]
    frequency: Literal["EACH_EXACT_COMMON_SESSION"]
    aggregation: Literal["MAX_ABSOLUTE_SESSION_RESIDUAL_ACROSS_BOTH_SERIES"]
    unit: Literal["decimal_daily_return"]
    tolerance: Decimal
    rounding_before_comparison_allowed: Literal[False]
    missing_required_cost_or_return_input_outcome: Literal["INSUFFICIENT"]
    residual_above_tolerance_outcome: Literal["INVALID"]

    @field_validator("tolerance")
    @classmethod
    def validate_tolerance(cls, value: Decimal) -> Decimal:
        if value != Decimal("0.0001"):
            raise ValueError("cost reconciliation tolerance drifted")
        return value


class NetOfCostReturnAxisV3(AxisBaseV3):
    axis_id: Literal["NET_OF_COST_RETURN"]
    predecessor_disposition: Literal["REJECT_AND_REQUEST_NEW_VERSION"]
    minimum_net_of_cost_return_delta: Decimal
    cost_model_policy_id: Literal["transaction_cost_model_v1"]
    candidate_net_daily_return_formula: Literal[
        "CANDIDATE_GROSS_DAILY_RETURN_MINUS_CANDIDATE_MODELED_DAILY_COST_RETURN"
    ]
    comparator_net_daily_return_formula: Literal[
        "COMPARATOR_GROSS_DAILY_RETURN_MINUS_COMPARATOR_MODELED_DAILY_COST_RETURN"
    ]
    annualized_cost_drag_formula: Literal["ANNUALIZED_GROSS_RETURN_MINUS_ANNUALIZED_NET_RETURN"]
    reconciliation_contract: CostReconciliationContractV3


class DrawdownAxisV3(AxisBaseV3):
    axis_id: Literal["ACTUAL_PATH_DRAWDOWN_REGRESSION"]
    predecessor_disposition: Literal["APPROVE_EXACTLY_AS_DRAFTED"]
    maximum_actual_path_drawdown_regression: Decimal
    nav_reset_rule: Literal["RESET_NAV_TO_ONE_AT_FIRST_COMMON_SESSION_OF_EACH_SLICE"]
    max_drawdown_formula: Literal["MIN_NAV_DIVIDED_BY_RUNNING_PEAK_MINUS_ONE"]
    regression_formula: Literal["ABS_CANDIDATE_MAX_DRAWDOWN_MINUS_ABS_COMPARATOR_MAX_DRAWDOWN"]
    mandatory_slice_set: tuple[str, ...]


class FalseRiskOffEventContractV3(_StrictModel):
    anchor_rule: Literal["BASELINE_DEFENSIVE_VETO_INACTIVE_TO_ACTIVE_TRANSITION"]
    first_session_active_rule: Literal["LEFT_CENSORED_NOT_AN_ANCHOR"]
    forward_horizon_exchange_sessions: Literal[20]
    forward_window_start_offset_sessions: Literal[1]
    right_censored_anchor_rule: Literal[
        "EXCLUDE_FROM_NUMERIC_STATISTIC_AND_COUNT_AS_RIGHT_CENSORED"
    ]
    merge_distance_exchange_sessions: Literal[20]
    merge_rule: Literal["TRANSITIVE_ADJACENT_RAW_ANCHOR_CHAIN"]
    merged_episode_anchor_rule: Literal["KEEP_EARLIEST_ANCHOR"]
    qqq_minus_sgov_forward_compounded_return_min: Decimal
    qqq_forward_path_max_drawdown_floor: Decimal
    path_daily_missed_return_formula: Literal[
        "PARENTHESIZED_ONE_MINUS_OPENING_QQQ_WEIGHT_TIMES_PARENTHESIZED_QQQ_MINUS_SGOV_DAILY_TOTAL_RETURN"
    ]
    path_event_cost_formula: Literal["PRODUCT_ONE_PLUS_DAILY_MISSED_RETURN_MINUS_ONE"]
    event_regression_formula: Literal["CANDIDATE_EVENT_COST_MINUS_BASELINE_EVENT_COST"]
    aggregate_formula: Literal["ARITHMETIC_MEAN_OF_QUALIFYING_EVENT_REGRESSIONS"]
    future_path_use: Literal["EX_POST_ATTRIBUTION_ONLY_NOT_DECISION_INPUT"]


class FalseRiskOffCostAxisV3(AxisBaseV3):
    axis_id: Literal["FALSE_RISK_OFF_COST"]
    predecessor_disposition: Literal["REJECT_AND_REQUEST_NEW_VERSION"]
    maximum_false_risk_off_cost_regression: Decimal
    minimum_independent_qualifying_event_count: Literal[10]
    event_contract: FalseRiskOffEventContractV3


class CanonicalDqPitAxisV3(AxisBaseV3):
    axis_id: Literal["CANONICAL_DQ_PIT"]
    predecessor_disposition: Literal["REJECT_AND_REQUEST_NEW_VERSION"]
    required_data_research_gate_status: Literal["PASS"]
    dq_successor_contract_id: Literal["strategy_growth_action_value_canonical_dq_pit_contract_v2"]
    numeric_policy_state: Literal["NON_EXECUTABLE_PILOT_POLICY_PENDING_REVIEW"]
    numeric_review_disposition: Literal["INSUFFICIENT_EVIDENCE_TO_APPROVE"]
    operational_authority_state: Literal[
        "UNAVAILABLE_PENDING_SECOND_INDEPENDENT_REVIEW_AND_OWNER_EXACT_APPROVAL"
    ]


class EpisodeContractV3(_StrictModel):
    anchor_rule: Literal["GROWTH_ACTION_INACTIVE_TO_ACTIVE_TRANSITION"]
    first_session_active_rule: Literal["LEFT_CENSORED_NOT_AN_ANCHOR"]
    episode_start_rule: Literal["INCLUDE_ANCHOR_SESSION"]
    episode_end_rule: Literal["INCLUDE_LAST_ACTIVE_SESSION_BEFORE_DEACTIVATION"]
    right_censored_episode_rule: Literal[
        "EXCLUDE_FROM_NUMERIC_STATISTIC_AND_COUNT_AS_RIGHT_CENSORED"
    ]
    merge_distance_exchange_sessions: Literal[20]
    merge_rule: Literal["TRANSITIVE_ADJACENT_RAW_ANCHOR_CHAIN"]
    merged_episode_anchor_rule: Literal["KEEP_EARLIEST_ANCHOR"]
    merged_episode_end_rule: Literal["LAST_ACTIVE_SESSION_OF_LAST_CHAIN_MEMBER"]
    slice_assignment_rule: Literal["EARLIEST_ANCHOR_SESSION_SLICE"]
    cross_slice_sessions_in_episode_value_allowed: Literal[True]
    cross_slice_double_count_allowed: Literal[False]
    episode_value_formula: Literal[
        "COMPOUNDED_CANDIDATE_NET_RETURN_MINUS_COMPOUNDED_COMPARATOR_NET_RETURN"
    ]
    contribution_numerator: Literal["SUM_ABS_EPISODE_VALUE_WITH_ANCHOR_IN_SLICE"]
    contribution_denominator: Literal["SUM_ABS_EPISODE_VALUE_ALL_EPISODES"]
    nonpositive_denominator_outcome: Literal["INSUFFICIENT"]


class SampleAndWindowDependenceAxisV3(AxisBaseV3):
    axis_id: Literal["SAMPLE_AND_WINDOW_DEPENDENCE"]
    predecessor_disposition: Literal["REJECT_AND_REQUEST_NEW_VERSION"]
    minimum_independent_action_count: Literal[30]
    minimum_independent_action_count_per_slice: Literal[5]
    independence_gap_exchange_sessions: Literal[20]
    maximum_single_regime_contribution_share: Decimal
    episode_contract: EpisodeContractV3
    mandatory_window_slices: tuple[str, ...]


class ActualPathTurnoverAxisV3(AxisBaseV3):
    axis_id: Literal["ACTUAL_PATH_TURNOVER"]
    predecessor_disposition: Literal["APPROVE_EXACTLY_AS_DRAFTED"]
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


class LeverageBetaAttributionAxisV3(AxisBaseV3):
    axis_id: Literal["LEVERAGE_BETA_ATTRIBUTION"]
    predecessor_disposition: Literal["APPROVE_EXACTLY_AS_DRAFTED"]
    maximum_realized_beta_increment: Decimal
    exposure_match_tolerance: Decimal
    beta_contract: BetaContract
    leverage_etf_allowed: Literal[False]
    options_position_allowed: Literal[False]
    borrowed_leverage_allowed: Literal[False]


AxisContractV3: TypeAlias = Annotated[
    NonBetaActionValueAxisV3
    | NetOfCostReturnAxisV3
    | DrawdownAxisV3
    | FalseRiskOffCostAxisV3
    | CanonicalDqPitAxisV3
    | SampleAndWindowDependenceAxisV3
    | ActualPathTurnoverAxisV3
    | LeverageBetaAttributionAxisV3,
    Field(discriminator="axis_id"),
]


class DraftTerminalV3(_StrictModel):
    status: Literal["BLOCKED_SECOND_REVIEW_AND_OWNER_EXACT_APPROVAL"]
    next_action: Literal["SECOND_INDEPENDENT_REVIEW_THEN_OWNER_EXACT_APPROVAL"]
    threshold_bundle_frozen: Literal[False]
    dq_successor_authorized: Literal[False]
    empirical_successor_authorized: Literal[False]


class StrategyGrowthActionValueFreezeReadinessContract(_CanonicalModel):
    schema_version: Literal["strategy_growth_action_value_threshold_exact_value_sheet.v3"]
    sheet_id: Literal["strategy_growth_action_value_threshold_exact_value_sheet_v3"]
    sheet_version: Literal["3.0.0-draft.1"]
    sheet_status: Literal[
        "DRAFT_CORRECTION_COMPLETE_PENDING_SECOND_REVIEW_AND_OWNER_EXACT_APPROVAL"
    ]
    task_id: Literal[
        "TRADING-2542C_GROWTH_ACTION_VALUE_INDEPENDENT_REVIEW_REMEDIATION_AND_FREEZE_READINESS_V1"
    ]
    owner_instruction: OwnerInstruction
    review_evidence: ReviewEvidence
    predecessor_binding: PredecessorBinding
    dq_successor_binding: DqSuccessorBinding
    scope_binding: ScopeBinding
    decision_timing: DecisionTiming
    common_series_contract: CommonSeriesContractV3
    window_slice_catalog: tuple[WindowSlice, ...]
    axis_contracts: tuple[AxisContractV3, ...]
    joint_terminal_contract: JointTerminalContract
    owner_review_contract: OwnerReviewContract
    terminal: DraftTerminalV3
    safety: DecisionPackSafety

    @model_validator(mode="after")
    def validate_complete_contract(self) -> Self:
        slices = tuple(
            (item.slice_id, item.start, item.end, item.role) for item in self.window_slice_catalog
        )
        if slices != _WINDOW_SLICES:
            raise ValueError("window slice catalog drifted")
        if tuple(item.axis_id for item in self.axis_contracts) != _AXIS_ORDER:
            raise ValueError("V3 axis order drifted")
        non_beta, net, drawdown, false_risk, dq, sample, turnover, beta = self.axis_contracts
        if not isinstance(non_beta, NonBetaActionValueAxisV3):
            raise TypeError("non-beta axis type drifted")
        if non_beta.minimum_non_beta_return_delta != Decimal("0.0100"):
            raise ValueError("non-beta floor drifted")
        if not isinstance(net, NetOfCostReturnAxisV3):
            raise TypeError("net axis type drifted")
        if net.minimum_net_of_cost_return_delta != Decimal("0.0075"):
            raise ValueError("net return floor drifted")
        if not isinstance(drawdown, DrawdownAxisV3):
            raise TypeError("drawdown axis type drifted")
        if drawdown.maximum_actual_path_drawdown_regression != Decimal("0.0200"):
            raise ValueError("drawdown threshold drifted")
        if drawdown.mandatory_slice_set != _ALL_SLICE_IDS:
            raise ValueError("drawdown slice set drifted")
        if not isinstance(false_risk, FalseRiskOffCostAxisV3):
            raise TypeError("false-risk axis type drifted")
        if false_risk.maximum_false_risk_off_cost_regression != Decimal("0.0025"):
            raise ValueError("false-risk threshold drifted")
        if not isinstance(dq, CanonicalDqPitAxisV3):
            raise TypeError("DQ axis type drifted")
        if not isinstance(sample, SampleAndWindowDependenceAxisV3):
            raise TypeError("sample axis type drifted")
        if sample.maximum_single_regime_contribution_share != Decimal("0.50"):
            raise ValueError("regime contribution threshold drifted")
        if sample.mandatory_window_slices != _PERIOD_SLICE_IDS:
            raise ValueError("sample slice set drifted")
        if not isinstance(turnover, ActualPathTurnoverAxisV3):
            raise TypeError("turnover axis type drifted")
        if turnover.maximum_annualized_actual_path_turnover != Decimal(
            "1.00"
        ) or turnover.maximum_cost_drag_share != Decimal("0.25"):
            raise ValueError("turnover threshold drifted")
        if not isinstance(beta, LeverageBetaAttributionAxisV3):
            raise TypeError("beta axis type drifted")
        if beta.maximum_realized_beta_increment != Decimal(
            "0.0200"
        ) or beta.exposure_match_tolerance != Decimal("0.0100"):
            raise ValueError("beta threshold drifted")
        return self


@dataclass(frozen=True)
class StrategyGrowthActionValueFreezeReadinessContractLoadResult:
    contract: StrategyGrowthActionValueFreezeReadinessContract
    contract_path: Path
    contract_file_sha256: str
    contract_canonical_sha256: str
    predecessor: StrategyGrowthActionValueMeasurementContractLoadResult
    dq_successor: StrategyGrowthActionValueDqPitContractV2LoadResult


def load_strategy_growth_action_value_freeze_readiness_contract(
    *,
    contract_path: Path = DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_FREEZE_READINESS_CONTRACT_PATH,
    project_root: Path = PROJECT_ROOT,
) -> StrategyGrowthActionValueFreezeReadinessContractLoadResult:
    try:
        path = _bound_file(contract_path, root=project_root, field="contract_path")
        raw = path.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(contract_path))
        contract = StrategyGrowthActionValueFreezeReadinessContract.model_validate(payload)
        predecessor = load_strategy_growth_action_value_measurement_contract(
            contract_path=Path(contract.predecessor_binding.path), project_root=project_root
        )
        dq_successor = load_strategy_growth_action_value_dq_pit_contract_v2(
            contract_path=Path(contract.dq_successor_binding.path), project_root=project_root
        )
        if predecessor.contract_file_sha256 != contract.predecessor_binding.file_sha256:
            raise ValueError("predecessor file SHA-256 mismatch")
        if predecessor.contract_canonical_sha256 != contract.predecessor_binding.canonical_sha256:
            raise ValueError("predecessor canonical SHA-256 mismatch")
        if dq_successor.contract_file_sha256 != contract.dq_successor_binding.file_sha256:
            raise ValueError("DQ successor file SHA-256 mismatch")
        if dq_successor.contract_canonical_sha256 != contract.dq_successor_binding.canonical_sha256:
            raise ValueError("DQ successor canonical SHA-256 mismatch")
        if dq_successor.contract.contract_id != contract.dq_successor_binding.contract_id:
            raise ValueError("DQ successor contract id mismatch")
    except StrategyGrowthActionValueFreezeReadinessContractError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise StrategyGrowthActionValueFreezeReadinessContractError(
            "GROWTH_ACTION_VALUE_FREEZE_READINESS_CONTRACT_REJECTED", str(exc)
        ) from exc
    return StrategyGrowthActionValueFreezeReadinessContractLoadResult(
        contract=contract,
        contract_path=path,
        contract_file_sha256=hashlib.sha256(raw).hexdigest(),
        contract_canonical_sha256=contract.canonical_sha256,
        predecessor=predecessor,
        dq_successor=dq_successor,
    )


def _decimal_series(values: Sequence[Decimal], *, label: str) -> tuple[Decimal, ...]:
    result = tuple(values)
    if not result:
        raise ValueError(f"{label} cannot be empty")
    if any(not isinstance(value, Decimal) or not value.is_finite() for value in result):
        raise ValueError(f"{label} must contain finite Decimal values")
    return result


def maximum_cost_reconciliation_residual(
    *,
    candidate_gross: Sequence[Decimal],
    candidate_net: Sequence[Decimal],
    candidate_modeled_cost: Sequence[Decimal],
    comparator_gross: Sequence[Decimal],
    comparator_net: Sequence[Decimal],
    comparator_modeled_cost: Sequence[Decimal],
) -> Decimal:
    series = tuple(
        _decimal_series(values, label=label)
        for label, values in (
            ("candidate_gross", candidate_gross),
            ("candidate_net", candidate_net),
            ("candidate_modeled_cost", candidate_modeled_cost),
            ("comparator_gross", comparator_gross),
            ("comparator_net", comparator_net),
            ("comparator_modeled_cost", comparator_modeled_cost),
        )
    )
    if len({len(values) for values in series}) != 1:
        raise ValueError("gross, net, and cost series must share exact session length")
    candidate_residuals = (
        abs(gross - net - cost)
        for gross, net, cost in zip(series[0], series[1], series[2], strict=True)
    )
    comparator_residuals = (
        abs(gross - net - cost)
        for gross, net, cost in zip(series[3], series[4], series[5], strict=True)
    )
    return max((*candidate_residuals, *comparator_residuals))


def activation_anchors_without_left_boundary(active: Sequence[bool]) -> tuple[int, ...]:
    values = tuple(active)
    return tuple(
        index for index in range(1, len(values)) if values[index] and not values[index - 1]
    )


def merge_anchor_indices_transitively(
    anchors: Sequence[int], *, maximum_gap: int
) -> tuple[int, ...]:
    if maximum_gap < 1:
        raise ValueError("maximum gap must be positive")
    values = tuple(anchors)
    if any(value < 0 for value in values) or tuple(sorted(set(values))) != values:
        raise ValueError("anchors must be sorted unique nonnegative indices")
    if not values:
        return ()
    kept = [values[0]]
    previous_raw = values[0]
    for anchor in values[1:]:
        if anchor - previous_raw > maximum_gap:
            kept.append(anchor)
        previous_raw = anchor
    return tuple(kept)


def missed_return_for_qqq_weight(
    *, opening_qqq_weight: Decimal, qqq_return: Decimal, sgov_return: Decimal
) -> Decimal:
    values = (opening_qqq_weight, qqq_return, sgov_return)
    if any(not value.is_finite() for value in values):
        raise ValueError("missed-return inputs must be finite")
    if opening_qqq_weight < 0 or opening_qqq_weight > 1:
        raise ValueError("opening QQQ weight must be within [0, 1]")
    return (Decimal(1) - opening_qqq_weight) * (qqq_return - sgov_return)


@dataclass(frozen=True)
class EpisodeInterval:
    anchor_index: int
    start_index: int
    end_index: int


@dataclass(frozen=True)
class EpisodeConstructionResult:
    intervals: tuple[EpisodeInterval, ...]
    left_censored: bool
    right_censored_count: int


def active_episode_intervals(
    active: Sequence[bool], *, merge_distance: int = 20
) -> EpisodeConstructionResult:
    if merge_distance < 1:
        raise ValueError("merge distance must be positive")
    values = tuple(active)
    if not values:
        return EpisodeConstructionResult((), False, 0)
    left_censored = values[0]
    raw: list[EpisodeInterval] = []
    right_censored = 0
    for anchor in activation_anchors_without_left_boundary(values):
        end = anchor
        while end + 1 < len(values) and values[end + 1]:
            end += 1
        if end == len(values) - 1 and values[end]:
            right_censored += 1
        else:
            raw.append(EpisodeInterval(anchor, anchor, end))
    if not raw:
        return EpisodeConstructionResult((), left_censored, right_censored)
    merged: list[EpisodeInterval] = [raw[0]]
    previous_raw_anchor = raw[0].anchor_index
    for interval in raw[1:]:
        if interval.anchor_index - previous_raw_anchor <= merge_distance:
            first = merged[-1]
            merged[-1] = EpisodeInterval(
                first.anchor_index,
                first.start_index,
                interval.end_index,
            )
        else:
            merged.append(interval)
        previous_raw_anchor = interval.anchor_index
    return EpisodeConstructionResult(tuple(merged), left_censored, right_censored)


def episode_return_value(
    candidate_returns: Sequence[float],
    comparator_returns: Sequence[float],
    interval: EpisodeInterval,
) -> float:
    candidate = tuple(candidate_returns)
    comparator = tuple(comparator_returns)
    if len(candidate) != len(comparator):
        raise ValueError("candidate and comparator returns must share exact length")
    if (
        interval.anchor_index != interval.start_index
        or interval.start_index < 0
        or interval.end_index < interval.start_index
        or interval.end_index >= len(candidate)
    ):
        raise ValueError("episode interval is outside the common return series")
    span = slice(interval.start_index, interval.end_index + 1)
    return compounded_return(candidate[span]) - compounded_return(comparator[span])


__all__ = [
    "DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_FREEZE_READINESS_CONTRACT_PATH",
    "EpisodeConstructionResult",
    "EpisodeInterval",
    "StrategyGrowthActionValueFreezeReadinessContract",
    "StrategyGrowthActionValueFreezeReadinessContractError",
    "StrategyGrowthActionValueFreezeReadinessContractLoadResult",
    "activation_anchors_without_left_boundary",
    "active_episode_intervals",
    "episode_return_value",
    "load_strategy_growth_action_value_freeze_readiness_contract",
    "maximum_cost_reconciliation_residual",
    "merge_anchor_indices_transitively",
    "missed_return_for_qqq_weight",
]
