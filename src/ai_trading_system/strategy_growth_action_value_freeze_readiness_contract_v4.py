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
from ai_trading_system.strategy_growth_action_value_dq_pit_contract_v3 import (
    StrategyGrowthActionValueDqPitContractV3LoadResult,
    load_strategy_growth_action_value_dq_pit_contract_v3,
)
from ai_trading_system.strategy_growth_action_value_freeze_readiness_contract import (
    ActualPathTurnoverAxisV3,
    AxisBaseV3,
    CommonSeriesContractV3,
    DrawdownAxisV3,
    EpisodeInterval,
    FalseRiskOffCostAxisV3,
    LeverageBetaAttributionAxisV3,
    NetOfCostReturnAxisV3,
    NonBetaActionValueAxisV3,
    StrategyGrowthActionValueFreezeReadinessContractLoadResult,
    activation_anchors_without_left_boundary,
    load_strategy_growth_action_value_freeze_readiness_contract,
)
from ai_trading_system.strategy_growth_action_value_measurement_contract import (
    DecisionTiming,
    JointTerminalContract,
    OwnerReviewContract,
    WindowSlice,
)
from ai_trading_system.strategy_growth_action_value_threshold_decision_pack import (
    DecisionPackSafety,
    ScopeBinding,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_FREEZE_READINESS_CONTRACT_V4_PATH = Path(
    "config/research/strategy_growth_action_value_threshold_exact_value_sheet_v4.yaml"
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
_PRESERVED_AXIS_INDICES = (0, 1, 2, 3, 6, 7)
_WINDOW_SLICES = (
    ("PRIMARY_WINDOW_FULL", date(2021, 2, 22), date(2025, 12, 2), "PRIMARY_AGGREGATE"),
    ("PRIMARY_2021_PARTIAL", date(2021, 2, 22), date(2021, 12, 31), "CALENDAR_STABILITY"),
    ("RATE_HIKE_BEAR_2022", date(2022, 1, 3), date(2022, 12, 30), "MANDATORY_STRESS"),
    ("RECOVERY_2023", date(2023, 1, 3), date(2023, 12, 29), "CALENDAR_STABILITY"),
    ("AI_RALLY_2024", date(2024, 1, 2), date(2024, 12, 31), "CALENDAR_STABILITY"),
    ("PRIMARY_2025_TO_END", date(2025, 1, 2), date(2025, 12, 2), "CALENDAR_STABILITY"),
)
_PERIOD_SLICE_IDS = tuple(item[0] for item in _WINDOW_SLICES[1:])


class StrategyGrowthActionValueFreezeReadinessContractV4Error(ValueError):
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


class OwnerInstructionV4(_StrictModel):
    decision_id: Literal[
        "owner_decision:TRADING-2542D:2026-08-24:adopt_second_pro_review_and_continue_v1"
    ]
    adopted_review_disposition: Literal[
        "REQUEST_NEW_VERSION_BEFORE_OWNER_FREEZE_DECISION"
    ]
    exact_successor_freeze_approval_state: Literal["NOT_PROVIDED"]
    no_separate_progress_confirmation_required: Literal[True]


class ReviewEvidenceV4(_StrictModel):
    reviewed_repository_commit: Literal["e5266c9aadfba067060b013d83ec26bd4f065604"]
    conversation_url: Literal["https://chatgpt.com/c/6a8b95b1-30dc-83e8-8d49-4b74a696acc1"]
    visible_product_label: Literal["ChatGPT Pro"]
    visible_model_label: Literal["GPT-5.6 Pro"]
    backend_route_attestation: Literal["CANNOT_VERIFY_EXACT_BACKEND_ROUTE"]
    routing_evidence_state: Literal["UI_PRO_AND_SELF_REPORT_PRO_ROUTE_UNVERIFIED"]
    preserved_axis_count: Literal[6]
    successor_axis_count: Literal[2]


class ShaBindingV4(_StrictModel):
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


class PredecessorBindingV4(ShaBindingV4):
    path: Literal[
        "config/research/strategy_growth_action_value_threshold_exact_value_sheet_v3.yaml"
    ]
    file_sha256: Literal["304b5de907bbc0858d2ca1f6786e9e325d5493572561b8e4cff71fa91ff05375"]
    canonical_sha256: Literal["68acb53ce3a2c2656565f24a98fe2de5b700d0ed3b994b9b3b20477f7aa6edb0"]
    disposition: Literal["REQUEST_NEW_VERSION_RETAINED_IMMUTABLE"]


class DqSuccessorBindingV4(ShaBindingV4):
    path: Literal["config/research/strategy_growth_action_value_canonical_dq_pit_contract_v3.yaml"]
    file_sha256: Literal["b84d8d3dbe2dded761e989c623469607c386297e59d61207bb478d3054523c2e"]
    canonical_sha256: Literal["9140e68dce070ca5cd421fe05ab480c9d2d330fd21a7f7c6cff0bda0b00aca8b"]
    contract_id: Literal["strategy_growth_action_value_canonical_dq_pit_contract_v3"]
    executable_authority: Literal[False]


class CanonicalDqPitAxisV4(AxisBaseV3):
    axis_id: Literal["CANONICAL_DQ_PIT"]
    predecessor_disposition: Literal["REJECT_AND_REQUEST_NEW_VERSION"]
    required_data_research_gate_status: Literal["PASS"]
    dq_successor_contract_id: Literal["strategy_growth_action_value_canonical_dq_pit_contract_v3"]
    numeric_policy_state: Literal["NON_EXECUTABLE_PILOT_FREEZE_READY"]
    pilot_review_disposition: Literal["APPROVE_EXACTLY_AS_DRAFTED"]
    executable_evidence_disposition: Literal["INSUFFICIENT_EVIDENCE_TO_APPROVE"]
    operational_authority_state: Literal["UNAVAILABLE_PENDING_OWNER_EXACT_FREEZE_APPROVAL"]


class EpisodeContractV4(_StrictModel):
    anchor_rule: Literal["GROWTH_ACTION_INACTIVE_TO_ACTIVE_TRANSITION"]
    first_session_active_rule: Literal["LEFT_CENSORED_NOT_AN_ANCHOR"]
    episode_start_rule: Literal["INCLUDE_ANCHOR_SESSION"]
    episode_end_rule: Literal["INCLUDE_LAST_ACTIVE_SESSION_BEFORE_DEACTIVATION"]
    right_censored_episode_rule: Literal[
        "EXCLUDE_FROM_NUMERIC_STATISTIC_AND_COUNT_AS_RIGHT_CENSORED"
    ]
    merge_distance_exchange_sessions: Literal[20]
    merge_rule: Literal["TRANSITIVE_ADJACENT_RAW_ANCHOR_CHAIN"]
    right_censor_application_order: Literal[
        "TRANSITIVE_CLUSTER_MERGE_BEFORE_RIGHT_CENSOR_EXCLUSION"
    ]
    connected_right_censored_cluster_rule: Literal[
        "ANY_RIGHT_CENSORED_RAW_MEMBER_EXCLUDES_ENTIRE_CONNECTED_CLUSTER"
    ]
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


class SampleAndWindowDependenceAxisV4(AxisBaseV3):
    axis_id: Literal["SAMPLE_AND_WINDOW_DEPENDENCE"]
    predecessor_disposition: Literal["REJECT_AND_REQUEST_NEW_VERSION"]
    minimum_independent_action_count: Literal[30]
    minimum_independent_action_count_per_slice: Literal[5]
    independence_gap_exchange_sessions: Literal[20]
    maximum_single_regime_contribution_share: Decimal
    episode_contract: EpisodeContractV4
    mandatory_window_slices: tuple[str, ...]


AxisContractV4: TypeAlias = Annotated[
    NonBetaActionValueAxisV3
    | NetOfCostReturnAxisV3
    | DrawdownAxisV3
    | FalseRiskOffCostAxisV3
    | CanonicalDqPitAxisV4
    | SampleAndWindowDependenceAxisV4
    | ActualPathTurnoverAxisV3
    | LeverageBetaAttributionAxisV3,
    Field(discriminator="axis_id"),
]


class DraftTerminalV4(_StrictModel):
    status: Literal["BLOCKED_OWNER_EXACT_FREEZE_APPROVAL"]
    next_action: Literal["OWNER_REVIEW_V4_THEN_SEPARATE_REAL_EVIDENCE_AUTHORIZATION"]
    threshold_bundle_frozen: Literal[False]
    dq_successor_authorized: Literal[False]
    empirical_successor_authorized: Literal[False]


class StrategyGrowthActionValueFreezeReadinessContractV4(_CanonicalModel):
    schema_version: Literal["strategy_growth_action_value_threshold_exact_value_sheet.v4"]
    sheet_id: Literal["strategy_growth_action_value_threshold_exact_value_sheet_v4"]
    sheet_version: Literal["4.0.0-draft.1"]
    sheet_status: Literal["NEW_VERSION_DRAFT_COMPLETE_PENDING_OWNER_FREEZE_DECISION"]
    task_id: Literal[
        "TRADING-2542D_GROWTH_ACTION_VALUE_DQ_PIT_AND_SAMPLE_SEMANTICS_FREEZE_CORRECTION_V1"
    ]
    owner_instruction: OwnerInstructionV4
    review_evidence: ReviewEvidenceV4
    predecessor_binding: PredecessorBindingV4
    dq_successor_binding: DqSuccessorBindingV4
    scope_binding: ScopeBinding
    decision_timing: DecisionTiming
    common_series_contract: CommonSeriesContractV3
    window_slice_catalog: tuple[WindowSlice, ...]
    axis_contracts: tuple[AxisContractV4, ...]
    joint_terminal_contract: JointTerminalContract
    owner_review_contract: OwnerReviewContract
    terminal: DraftTerminalV4
    safety: DecisionPackSafety

    @model_validator(mode="after")
    def validate_complete_contract(self) -> Self:
        slices = tuple(
            (item.slice_id, item.start, item.end, item.role) for item in self.window_slice_catalog
        )
        if slices != _WINDOW_SLICES:
            raise ValueError("window slice catalog drifted")
        if tuple(item.axis_id for item in self.axis_contracts) != _AXIS_ORDER:
            raise ValueError("V4 axis order drifted")
        if self.common_series_contract.expected_session_inventory_lf_sha256 != (
            _SESSION_INVENTORY_SHA256
        ):
            raise ValueError("primary inventory identity drifted")
        non_beta, net, drawdown, false_risk, dq, sample, turnover, beta = self.axis_contracts
        if not isinstance(non_beta, NonBetaActionValueAxisV3) or (
            non_beta.minimum_non_beta_return_delta != Decimal("0.0100")
        ):
            raise ValueError("non-beta axis drifted")
        if not isinstance(net, NetOfCostReturnAxisV3) or (
            net.minimum_net_of_cost_return_delta != Decimal("0.0075")
        ):
            raise ValueError("net-of-cost axis drifted")
        if not isinstance(drawdown, DrawdownAxisV3) or (
            drawdown.maximum_actual_path_drawdown_regression != Decimal("0.0200")
        ):
            raise ValueError("drawdown axis drifted")
        if not isinstance(false_risk, FalseRiskOffCostAxisV3) or (
            false_risk.maximum_false_risk_off_cost_regression != Decimal("0.0025")
        ):
            raise ValueError("false-risk axis drifted")
        if not isinstance(dq, CanonicalDqPitAxisV4):
            raise TypeError("DQ axis type drifted")
        if not isinstance(sample, SampleAndWindowDependenceAxisV4):
            raise TypeError("sample axis type drifted")
        if sample.maximum_single_regime_contribution_share != Decimal("0.50"):
            raise ValueError("regime contribution threshold drifted")
        if sample.mandatory_window_slices != _PERIOD_SLICE_IDS:
            raise ValueError("sample slice set drifted")
        if not isinstance(turnover, ActualPathTurnoverAxisV3) or (
            turnover.maximum_annualized_actual_path_turnover != Decimal("1.00")
            or turnover.maximum_cost_drag_share != Decimal("0.25")
        ):
            raise ValueError("turnover axis drifted")
        if not isinstance(beta, LeverageBetaAttributionAxisV3) or (
            beta.maximum_realized_beta_increment != Decimal("0.0200")
            or beta.exposure_match_tolerance != Decimal("0.0100")
        ):
            raise ValueError("beta axis drifted")
        return self


@dataclass(frozen=True)
class StrategyGrowthActionValueFreezeReadinessContractV4LoadResult:
    contract: StrategyGrowthActionValueFreezeReadinessContractV4
    contract_path: Path
    contract_file_sha256: str
    contract_canonical_sha256: str
    predecessor: StrategyGrowthActionValueFreezeReadinessContractLoadResult
    dq_successor: StrategyGrowthActionValueDqPitContractV3LoadResult


def load_strategy_growth_action_value_freeze_readiness_contract_v4(
    *,
    contract_path: Path = DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_FREEZE_READINESS_CONTRACT_V4_PATH,
    project_root: Path = PROJECT_ROOT,
) -> StrategyGrowthActionValueFreezeReadinessContractV4LoadResult:
    try:
        path = _bound_file(contract_path, root=project_root, field="contract_path")
        raw = path.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(contract_path))
        contract = StrategyGrowthActionValueFreezeReadinessContractV4.model_validate(payload)
        predecessor = load_strategy_growth_action_value_freeze_readiness_contract(
            contract_path=Path(contract.predecessor_binding.path), project_root=project_root
        )
        dq_successor = load_strategy_growth_action_value_dq_pit_contract_v3(
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
        for index in _PRESERVED_AXIS_INDICES:
            current = contract.axis_contracts[index].model_dump(mode="json")
            prior = predecessor.contract.axis_contracts[index].model_dump(mode="json")
            if current != prior:
                raise ValueError(f"preserved axis drifted at index {index}")
    except StrategyGrowthActionValueFreezeReadinessContractV4Error:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise StrategyGrowthActionValueFreezeReadinessContractV4Error(
            "GROWTH_ACTION_VALUE_FREEZE_READINESS_CONTRACT_V4_REJECTED", str(exc)
        ) from exc
    return StrategyGrowthActionValueFreezeReadinessContractV4LoadResult(
        contract=contract,
        contract_path=path,
        contract_file_sha256=hashlib.sha256(raw).hexdigest(),
        contract_canonical_sha256=contract.canonical_sha256,
        predecessor=predecessor,
        dq_successor=dq_successor,
    )


@dataclass(frozen=True)
class CostReconciliationSessionV4:
    session_date: date
    candidate_gross: Decimal
    candidate_net: Decimal
    candidate_modeled_cost: Decimal
    comparator_gross: Decimal
    comparator_net: Decimal
    comparator_modeled_cost: Decimal


def maximum_keyed_cost_reconciliation_residual_v4(
    records: Sequence[CostReconciliationSessionV4], *, expected_sessions: Sequence[date]
) -> Decimal:
    values = tuple(records)
    expected = tuple(expected_sessions)
    if not values or not expected:
        raise ValueError("cost reconciliation sessions cannot be empty")
    if tuple(sorted(set(expected))) != expected:
        raise ValueError("expected sessions must be sorted unique")
    observed = tuple(item.session_date for item in values)
    if len(set(observed)) != len(observed):
        raise ValueError("cost reconciliation session keys must be unique")
    if set(observed) != set(expected):
        raise ValueError("cost reconciliation session key set mismatch")
    residuals: list[Decimal] = []
    for item in sorted(values, key=lambda row: row.session_date):
        operands = (
            item.candidate_gross,
            item.candidate_net,
            item.candidate_modeled_cost,
            item.comparator_gross,
            item.comparator_net,
            item.comparator_modeled_cost,
        )
        if any(not isinstance(value, Decimal) or not value.is_finite() for value in operands):
            raise ValueError("cost reconciliation operands must be finite Decimal values")
        residuals.extend(
            (
                abs(item.candidate_gross - item.candidate_net - item.candidate_modeled_cost),
                abs(
                    item.comparator_gross
                    - item.comparator_net
                    - item.comparator_modeled_cost
                ),
            )
        )
    return max(residuals)


@dataclass(frozen=True)
class EpisodeConstructionResultV4:
    intervals: tuple[EpisodeInterval, ...]
    left_censored: bool
    right_censored_count: int


@dataclass(frozen=True)
class _RawEpisodeV4:
    anchor_index: int
    end_index: int
    right_censored: bool


def active_episode_intervals_v4(
    active: Sequence[bool], *, merge_distance: int = 20
) -> EpisodeConstructionResultV4:
    if merge_distance < 1:
        raise ValueError("merge distance must be positive")
    values = tuple(active)
    if not values:
        return EpisodeConstructionResultV4((), False, 0)
    left_censored = values[0]
    raw: list[_RawEpisodeV4] = []
    for anchor in activation_anchors_without_left_boundary(values):
        end = anchor
        while end + 1 < len(values) and values[end + 1]:
            end += 1
        raw.append(_RawEpisodeV4(anchor, end, end == len(values) - 1))
    if not raw:
        return EpisodeConstructionResultV4((), left_censored, 0)

    clusters: list[list[_RawEpisodeV4]] = [[raw[0]]]
    previous_raw_anchor = raw[0].anchor_index
    for episode in raw[1:]:
        if episode.anchor_index - previous_raw_anchor <= merge_distance:
            clusters[-1].append(episode)
        else:
            clusters.append([episode])
        previous_raw_anchor = episode.anchor_index

    intervals: list[EpisodeInterval] = []
    right_censored_count = 0
    for cluster in clusters:
        if any(item.right_censored for item in cluster):
            right_censored_count += 1
            continue
        intervals.append(
            EpisodeInterval(
                anchor_index=cluster[0].anchor_index,
                start_index=cluster[0].anchor_index,
                end_index=cluster[-1].end_index,
            )
        )
    return EpisodeConstructionResultV4(
        tuple(intervals), left_censored, right_censored_count
    )


__all__ = [
    "CostReconciliationSessionV4",
    "DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_FREEZE_READINESS_CONTRACT_V4_PATH",
    "EpisodeConstructionResultV4",
    "StrategyGrowthActionValueFreezeReadinessContractV4",
    "StrategyGrowthActionValueFreezeReadinessContractV4Error",
    "StrategyGrowthActionValueFreezeReadinessContractV4LoadResult",
    "active_episode_intervals_v4",
    "load_strategy_growth_action_value_freeze_readiness_contract_v4",
    "maximum_keyed_cost_reconciliation_residual_v4",
]
