from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft as predecessor,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_CALCULATION_SEMANTICS_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_calculation_semantics_v1.yaml"
)
DEFAULT_OWNER_FREEZE_DECISION_PACK_DRAFT_V2_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft_v2.yaml"
)

_TASK_ID = "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
_PREDECESSOR_FILE_SHA256 = "4f188c6e10758a32984bb92c3252507636686f97404c4491df014c1d22807479"
_PREDECESSOR_CANONICAL_SHA256 = (
    "c8838a4baef788a6b936e4e098658413e2c563e169f1ec4a5da8ec7318c9e4af"
)
_SEMANTICS_FILE_SHA256 = "813c2eb2bb0d4b4f7673048889b66fa843b739a48405cc2e87272d925dd7b0d0"
_SEMANTICS_CANONICAL_SHA256 = (
    "824ef20a66e4eba3c2841489cae8b03ff3a6cad4f73003469c086d8e09237cf1"
)
_SESSION_INVENTORY_SHA256 = "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_VETO_IDS = (
    "broad_market_risk_off_veto",
    "realized_volatility_veto",
    "scheduled_event_risk_veto",
    "underlying_trend_break_veto",
)
_FORBIDDEN_DEPENDENCY_MARKERS = (
    "selected_call_contract_identity",
    "selected_put_contract_identity",
    "selected_pair_checksum",
    "selected_call_activity",
    "selected_put_activity",
    "option_alpha_state",
    "growth_allowed",
    "growth_active",
    "growth_inactive",
    "candidate_target_weights",
    "candidate_return",
    "v4_result",
    "result_dependent_contributor_universe",
)
_EXPECTED_DESCRIPTIVE_FIELDS = (
    "per_veto_true_count_and_rate",
    "exclusive_true_count_and_rate",
    "pairwise_overlap_and_jaccard",
    "union_blocked_session_count_and_rate",
    "episode_count_median_and_max_duration",
    "trend_recovery_lag",
    "event_only_blocked_sessions",
    "alpha_available_but_gate_blocked_sessions",
    "fail_insufficient_invalid_inventory",
)
_EXPECTED_INPUTS = {
    "broad_market_risk_off_veto": (
        "SPY.exchange_session",
        "SPY.adjusted_close",
        "SPY.available_at",
        "SPY.source_identity",
        "SPY.adjustment_vintage",
        "SPY.source_snapshot_sha256",
    ),
    "realized_volatility_veto": (
        "QQQ.exchange_session",
        "QQQ.adjusted_close",
        "QQQ.available_at",
        "QQQ.source_identity",
        "QQQ.adjustment_vintage",
        "VIX.observation_session",
        "VIX.vix_level",
        "VIX.published_at",
        "VIX.available_at",
        "VIX.revision_id",
        "VIX.source_identity",
        "VIX.source_snapshot_sha256",
    ),
    "scheduled_event_risk_veto": (
        "event_authority",
        "event_type",
        "stable_event_key",
        "scheduled_for",
        "published_at",
        "revision_id",
        "revision_action",
        "source_identity",
        "source_snapshot_sha256",
        "coverage_through",
    ),
    "underlying_trend_break_veto": (
        "QQQ.exchange_session",
        "QQQ.adjusted_close",
        "QQQ.available_at",
        "QQQ.source_identity",
        "QQQ.adjustment_vintage",
        "QQQ.source_snapshot_sha256",
    ),
}
_EXPECTED_COMPARISONS = {
    "broad_market_risk_off_veto": (
        ("SPY_CLOSE_LT_SMA200", "SPY.adjusted_close", "LT", "SPY.SMA200", None),
        (
            "SPY_DRAWDOWN63_LTE_NEGATIVE_0_10",
            "SPY.drawdown63",
            "LTE",
            "CONSTANT_FRACTION",
            -0.10,
        ),
    ),
    "realized_volatility_veto": (
        (
            "VIX_PERCENTILE252_GTE_0_75",
            "VIX.percentile252",
            "GTE",
            "CONSTANT_FRACTION",
            0.75,
        ),
        (
            "QQQ_ANNUALIZED_RV20_GT_0_25",
            "QQQ.annualized_rv20",
            "GT",
            "CONSTANT_FRACTION",
            0.25,
        ),
    ),
    "scheduled_event_risk_veto": (
        (
            "ADMITTED_EVENT_COUNT_GTE_1",
            "admitted_event_count",
            "GTE",
            "CONSTANT_COUNT",
            1.0,
        ),
    ),
    "underlying_trend_break_veto": (
        ("QQQ_CLOSE_LT_SMA200", "QQQ.adjusted_close", "LT", "QQQ.SMA200", None),
        (
            "QQQ_DRAWDOWN63_LTE_NEGATIVE_0_12",
            "QQQ.drawdown63",
            "LTE",
            "CONSTANT_FRACTION",
            -0.12,
        ),
    ),
}


class MandatoryVetoExactSemanticsDraftError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
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
        raise ValueError(f"{field} escapes project root") from exc
    if ".." in relative.parts:
        raise ValueError(f"{field} escapes project root")
    current = resolved_root
    for part in relative.parts:
        current /= part
        if current.exists() and current.is_symlink():
            raise ValueError(f"{field} cannot traverse a symlink")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes project root") from exc
    if not resolved.is_file() or candidate.is_symlink():
        raise ValueError(f"{field} must be a non-symlink regular file")
    return resolved


class SemanticsOwnerScope(_StrictModel):
    decision_ref: Literal[
        "owner_decision:TRADING-2542G:S4A:2026-08-26:"
        "authorize_exact_calculation_time_state_contract_v2"
    ]
    exact_semantics_drafting_authorized: Literal[True]
    generated_bytes_are_owner_frozen: Literal[False]
    producer_implementation_authorized: Literal[False]
    real_data_or_backtest_authorized: Literal[False]
    authorization_state: Literal["EXACT_PREAUTHORIZED"]


class CalendarContract(_StrictModel):
    target_calendar_identity: Literal["QQQ_EXCHANGE_SESSIONS"]
    source_window_axis: Literal["BOUND_SOURCE_EXCHANGE_SESSIONS"]
    next_session_definition: Literal["NEXT_VALID_QQQ_EXCHANGE_SESSION"]
    target_inventory_excludes_warmup: Literal[True]
    source_warmup_inventory_required: Literal[True]
    session_gaps_may_be_compressed: Literal[False]
    same_session_action_allowed: Literal[False]
    cross_date_fallback_allowed: Literal[False]


class ClockContract(_StrictModel):
    timestamp_requirement: Literal["IANA_TIMEZONE_AND_UTC_OFFSET"]
    decision_cutoff: Literal["TARGET_QQQ_SESSION_EXACT_POST_CLOSE_RESEARCH_CUTOFF"]
    availability_predicate: Literal["MAX_REQUIRED_INPUT_AVAILABLE_AT_LTE_DECISION_AS_OF"]
    action_cutoff_predicate: Literal["DECISION_AS_OF_LT_NEXT_SESSION_ACTION_CUTOFF"]
    early_close_and_holiday_calendar_required: Literal[True]


class PriceContract(_StrictModel):
    price_field: Literal["adjusted_close"]
    adjustment_basis_identity_required: Literal[True]
    corporate_action_vintage_required: Literal[True]
    source_snapshot_sha256_required: Literal[True]
    sma_method: Literal["ARITHMETIC_MEAN"]
    rolling_window_includes_current_session: Literal[True]
    full_minimum_observations_required: Literal[True]
    drawdown_reference: Literal["ROLLING_MAX_INCLUSIVE_CURRENT"]
    forward_fill_allowed: Literal[False]
    interpolation_allowed: Literal[False]
    duplicate_session_terminal: Literal["INVALID"]


class TerminalPolicy(_StrictModel):
    missing_terminal: Literal["INSUFFICIENT"]
    malformed_authority_terminal: Literal["INVALID"]
    formula_short_circuit_before_all_components_qualified: Literal[False]
    missing_may_be_interpreted_as_false: Literal[False]
    event_empty_rows_may_prove_false: Literal[False]
    invalid_checkpoint_may_continue: Literal[False]


class SharedStateContract(_StrictModel):
    allowed_states: tuple[Literal["UNKNOWN", "CLEAR", "VETO_ACTIVE"], ...]
    initial_state: Literal["UNKNOWN"]
    known_state_required_before_target_inventory: Literal[True]
    missing_observation_interrupts_recovery: Literal[True]
    missing_observation_next_state: Literal["UNKNOWN"]
    malformed_observation_requires_checkpoint_replay: Literal[True]

    @model_validator(mode="after")
    def validate_state_order(self) -> Self:
        if self.allowed_states != ("UNKNOWN", "CLEAR", "VETO_ACTIVE"):
            raise ValueError("shared state inventory drifted")
        return self


class OverlapContract(_StrictModel):
    orthogonality_claim: Literal[
        "SEMANTIC_AND_INPUT_SEPARATION_ONLY_NOT_EMPIRICAL_INDEPENDENCE"
    ]
    empirical_independence_claim_allowed: Literal[False]
    result_blind_descriptive_evidence_fields: tuple[str, ...]
    returns_or_weights_allowed_in_first_observation: Literal[False]

    @model_validator(mode="after")
    def validate_evidence_fields(self) -> Self:
        if self.result_blind_descriptive_evidence_fields != _EXPECTED_DESCRIPTIVE_FIELDS:
            raise ValueError("result-blind descriptive evidence schema drifted")
        return self


class ContractDraftSafety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    contract_draft_only: Literal[True]
    producer_implementation_allowed: Literal[False]
    series_generation_allowed: Literal[False]
    r1_manifest_generation_allowed: Literal[False]
    cache_read_authorized: Literal[False]
    provider_query_authorized: Literal[False]
    real_dq_authorized: Literal[False]
    backtest_authorized: Literal[False]
    parameter_or_threshold_search_allowed: Literal[False]
    orders_allowed: Literal[False]
    fills_allowed: Literal[False]
    positions_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class MandatoryVetoCalculationSemantics(_CanonicalModel):
    schema_version: Literal["growth_action_value_mandatory_veto_calculation_semantics.v1"]
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_calculation_semantics_v1"
    ]
    policy_version: Literal["1.0.0-draft.1"]
    status: Literal["RESULT_BLIND_EXACT_SEMANTICS_DRAFT_NOT_OWNER_FROZEN"]
    task_id: Literal[
        "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
    ]
    owner_scope: SemanticsOwnerScope
    calendar_contract: CalendarContract
    clock_contract: ClockContract
    price_contract: PriceContract
    terminal_policy: TerminalPolicy
    state_contract: SharedStateContract
    overlap_contract: OverlapContract
    safety: ContractDraftSafety


class ExactBinding(_StrictModel):
    path: str
    file_sha256: str
    canonical_sha256: str
    immutable: Literal[True]

    @field_validator("file_sha256", "canonical_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


class V2OwnerScope(_StrictModel):
    decision_ref: Literal[
        "owner_decision:TRADING-2542G:S4A:2026-08-26:"
        "authorize_exact_calculation_time_state_contract_v2"
    ]
    v2_contract_drafting_authorized: Literal[True]
    v2_bytes_are_owner_frozen: Literal[False]
    producer_implementation_authorized: Literal[False]
    source_admission_authorized: Literal[False]
    real_data_or_backtest_authorized: Literal[False]
    authorization_state: Literal["EXACT_PREAUTHORIZED"]


class TargetInventory(_StrictModel):
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    expected_session_count: Literal[1202]
    target_session_inventory_lf_sha256: str

    @model_validator(mode="after")
    def validate_inventory(self) -> Self:
        if not (
            self.requested_start == self.evaluated_start == date(2021, 2, 22)
            and self.requested_end == self.evaluated_end == date(2025, 12, 2)
        ):
            raise ValueError("V2 research scope drifted")
        if self.target_session_inventory_lf_sha256 != _SESSION_INVENTORY_SHA256:
            raise ValueError("V2 target inventory identity drifted")
        return self


class ReviewPolicy(_StrictModel):
    evidence_role: Literal["RESULT_BLIND_EXACT_SEMANTICS_OWNER_REVIEW_PROPOSAL"]
    calibration_status: Literal["UNVALIDATED_NO_REAL_DATA_OR_BACKTEST"]
    orthogonality_claim: Literal[
        "SEMANTIC_AND_INPUT_SEPARATION_ONLY_NOT_EMPIRICAL_INDEPENDENCE"
    ]
    recommendation_values_may_drive_runtime: Literal[False]
    owner_must_freeze_each_veto_separately: Literal[True]
    partial_owner_freeze_may_generate_series: Literal[False]
    expiry_condition: Literal["BEFORE_ANY_SOURCE_CONTRACT_ADMISSION_OR_SERIES_GENERATION"]


class ProducerContract(_StrictModel):
    producer_id: str
    callable_state: str
    independent_input_universe: tuple[str, ...]
    forbidden_input_classes: tuple[str, ...]


class Comparison(_StrictModel):
    component_id: str
    left_metric: str
    operator: Literal["LT", "LTE", "GT", "GTE"]
    right_operand: str
    right_numeric: float | None


class FormulaContract(_StrictModel):
    formula_category: str
    combination_rule: Literal["OR", "AND", "ANY_EVENT"]
    comparisons: tuple[Comparison, ...]
    full_component_qualification_required_before_evaluation: Literal[True]
    exact_human_rendering: str


class PriceRollingContract(_StrictModel):
    source_window_axis: Literal[
        "BOUND_SPY_EXCHANGE_SESSIONS", "BOUND_QQQ_EXCHANGE_SESSIONS"
    ]
    sma_method: Literal["ARITHMETIC_MEAN"]
    moving_average_sessions: Literal[200]
    sma_min_valid_observations: Literal[200]
    drawdown_sessions: Literal[63]
    drawdown_min_valid_observations: Literal[63]
    window_includes_current_session: Literal[True]
    drawdown_reference: Literal["ROLLING_MAX_INCLUSIVE_CURRENT"]
    forward_fill_allowed: Literal[False]
    interpolation_allowed: Literal[False]
    duplicate_session_terminal: Literal["INVALID"]


class BroadEntryRecoveryContract(_StrictModel):
    state_style: Literal["STATELESS_SESSION_BOOLEAN"]
    entry_confirmation_sessions: Literal[1]
    recovery_rule: Literal["CLEAR_WHEN_BOTH_COMPONENTS_FALSE"]
    extra_hysteresis_allowed: Literal[False]


class VolatilityRollingContract(_StrictModel):
    source_window_axis: Literal["BOUND_QQQ_AND_VIX_EXCHANGE_SESSIONS"]
    vix_percentile_sessions: Literal[252]
    vix_min_valid_observations: Literal[252]
    vix_window_includes_current_observation: Literal[True]
    vix_tie_method: Literal["AVERAGE_RANK"]
    vix_rank_denominator: Literal["VALID_OBSERVATION_COUNT"]
    qqq_close_observations: Literal[21]
    realized_volatility_return_observations: Literal[20]
    return_type: Literal["SIMPLE"]
    pct_change_fill_method: Literal["NONE"]
    standard_deviation_ddof: Literal[1]
    annualization_sessions: Literal[252]
    annualization_scaling: Literal["SQRT"]
    session_gap_compression_allowed: Literal[False]
    forward_fill_allowed: Literal[False]


class VolatilityComponentSemantics(_StrictModel):
    vix_component_role: Literal["IMPLIED_VOLATILITY_STRESS_PROXY"]
    qqq_component_role: Literal["REALIZED_VOLATILITY"]
    whole_veto_may_be_described_as_pure_realized_volatility: Literal[False]


class TimingContract(_StrictModel):
    decision_as_of: Literal[
        "EXACT_POST_CLOSE_RESEARCH_CUTOFF_BOUND_TO_TARGET_QQQ_SESSION"
    ]
    available_at_predicate: Literal[
        "MAX_REQUIRED_INPUT_AVAILABLE_AT_LTE_DECISION_AS_OF",
        "LATEST_ACTIVE_REVISION_PUBLISHED_AT_LTE_DECISION_AS_OF",
    ]
    effective_session: Literal["NEXT_VALID_QQQ_EXCHANGE_SESSION"]
    action_cutoff_predicate: Literal["DECISION_AS_OF_LT_NEXT_SESSION_ACTION_CUTOFF"]
    same_session_action_allowed: Literal[False]
    cross_date_fallback_allowed: Literal[False]


class PITContract(_StrictModel):
    required_source_fields: tuple[str, ...]
    adjusted_close_basis_and_vintage_required: bool
    published_at_required: bool
    revision_identity_required: bool
    no_fill_or_interpolation: Literal[True]
    missing_terminal: Literal["INSUFFICIENT"]
    malformed_authority_terminal: Literal["INVALID"]


class EventTaxonomy(_StrictModel):
    authority: Literal["FEDERAL_RESERVE", "BLS", "BEA"]
    event_types: tuple[str, ...]


class EventContract(_StrictModel):
    admitted_event_taxonomy: tuple[EventTaxonomy, ...]
    pre_event_qqq_sessions: Literal[1]
    post_event_qqq_sessions: Literal[0]
    scheduled_for_timestamp_required: Literal[True]
    mapping_interval: Literal[
        "DECISION_AS_OF_LT_SCHEDULED_FOR_LTE_NEXT_ACTION_SESSION_CLOSE"
    ]
    premarket_and_in_session_event_blocks_next_action_session: Literal[True]
    after_close_event_maps_to_following_action_session: Literal[True]
    source_precedence: Literal["EXACT_OFFICIAL_AUTHORITY_ONLY_NO_CROSS_PROVIDER_FILL"]
    unscheduled_interventions_in_scope: Literal[False]
    event_results_or_weights_allowed: Literal[False]


class RevisionContract(_StrictModel):
    stable_event_key_required: Literal[True]
    revision_id_required: Literal[True]
    published_at_required: Literal[True]
    deterministic_revision_ordering_required: Literal[True]
    reschedule_supersedes_prior_revision: Literal[True]
    cancel_revision_supported: Literal[True]
    same_published_at_conflict_terminal: Literal["INVALID"]
    post_decision_revision_may_rewrite_history: Literal[False]


class CoverageContract(_StrictModel):
    required_authorities: tuple[Literal["FEDERAL_RESERVE", "BLS", "BEA"], ...]
    coverage_receipt_required_to_emit_false: Literal[True]
    coverage_horizon: Literal["NEXT_ACTION_SESSION_CLOSE"]
    empty_rows_may_prove_false: Literal[False]
    parser_taxonomy_must_match_exact_official_identity: Literal[True]

    @model_validator(mode="after")
    def validate_authorities(self) -> Self:
        if self.required_authorities != ("FEDERAL_RESERVE", "BLS", "BEA"):
            raise ValueError("event coverage authority inventory drifted")
        return self


class TrendStateContract(_StrictModel):
    allowed_states: tuple[Literal["UNKNOWN", "CLEAR", "VETO_ACTIVE"], ...]
    initial_state: Literal["UNKNOWN"]
    pre_target_replay_required: Literal[True]
    known_state_required_before_target_inventory: Literal[True]
    entry_confirmation_sessions: Literal[1]
    entry_rule: Literal["CLOSE_LT_SMA200_AND_DRAWDOWN63_LTE_NEGATIVE_0_12"]
    persistence_rule: Literal["REMAIN_ACTIVE_UNTIL_EXACT_RECOVERY"]
    recovery_confirmation_sessions: Literal[2]
    recovery_rule: Literal[
        "TWO_CONSECUTIVE_VALID_QQQ_SESSIONS_CLOSE_GTE_SMA200"
    ]
    recovery_equality: Literal["GTE"]
    entry_drawdown_controls_persistence_or_clear: Literal[False]
    missing_observation_interrupts_recovery: Literal[True]
    missing_observation_next_state: Literal["UNKNOWN"]
    malformed_observation_terminal: Literal["INVALID"]
    replay_from_affected_checkpoint_required: Literal[True]
    checkpoint_identity_fields: tuple[str, ...]

    @model_validator(mode="after")
    def validate_state_contract(self) -> Self:
        if self.allowed_states != ("UNKNOWN", "CLEAR", "VETO_ACTIVE"):
            raise ValueError("trend state inventory drifted")
        if self.checkpoint_identity_fields != (
            "producer_version",
            "source_inventory_sha256",
            "state_checkpoint_sha256",
        ):
            raise ValueError("trend checkpoint identity drifted")
        return self


class _BaseDecision(_StrictModel):
    recommendation_state: Literal["EXACT_SEMANTICS_PROPOSAL_NOT_OWNER_FROZEN"]
    producer_contract: ProducerContract
    formula_contract: FormulaContract
    timing_contract: TimingContract
    pit_contract: PITContract
    provenance_statement: str
    open_evidence_blockers: tuple[str, ...]
    owner_exact_freeze_granted: Literal[False]
    producer_contract_admitted: Literal[False]
    exact_1202_session_inventory_admitted: Literal[False]
    observed_inventory_lf_sha256: None
    series_generation_allowed: Literal[False]


class BroadDecision(_BaseDecision):
    veto_id: Literal["broad_market_risk_off_veto"]
    recommendation_id: Literal[
        "broad_market_risk_off_spy_sma200_drawdown63_v2_exact_semantics_proposal"
    ]
    rolling_contract: PriceRollingContract
    entry_recovery_contract: BroadEntryRecoveryContract


class VolatilityDecision(_BaseDecision):
    veto_id: Literal["realized_volatility_veto"]
    recommendation_id: Literal[
        "realized_volatility_vix252_or_qqq_rv20_v2_exact_semantics_proposal"
    ]
    rolling_contract: VolatilityRollingContract
    component_semantics: VolatilityComponentSemantics


class ScheduledEventDecision(_BaseDecision):
    veto_id: Literal["scheduled_event_risk_veto"]
    recommendation_id: Literal[
        "scheduled_event_official_next_session_any_v2_exact_semantics_proposal"
    ]
    event_contract: EventContract
    revision_contract: RevisionContract
    coverage_contract: CoverageContract


class TrendDecision(_BaseDecision):
    veto_id: Literal["underlying_trend_break_veto"]
    recommendation_id: Literal[
        "qqq_sma200_drawdown63_confirmed_recovery_v2_exact_semantics_proposal"
    ]
    rolling_contract: PriceRollingContract
    state_contract: TrendStateContract


class AggregateState(_StrictModel):
    exact_semantics_objects_ready_for_owner_review: tuple[str, ...]
    owner_frozen_producer_contracts: tuple[()]
    admitted_producer_contracts: tuple[()]
    unresolved_producer_contracts: tuple[str, ...]
    terminal: Literal["OWNER_EXACT_FREEZE_REQUIRED_0_OF_4_ADMITTED"]
    next_legal_action: Literal[
        "OWNER_REVIEW_AND_EXACT_FREEZE_ALL_FOUR_V2_OBJECTS_OR_RETURN_WITH_CHANGES"
    ]
    predecessor_terminal_preserved: Literal[
        "OWNER_EXACT_FREEZE_DECISION_REQUIRED_0_OF_4_ADMITTED"
    ]


class V2Safety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    exact_semantics_contract_draft_only: Literal[True]
    recommendation_values_are_runtime_policy: Literal[False]
    owner_exact_freeze_granted_for_any_veto: Literal[False]
    producer_implementation_allowed: Literal[False]
    source_contract_admission_allowed: Literal[False]
    veto_series_generation_allowed: Literal[False]
    r1_manifest_generation_allowed: Literal[False]
    cache_read_authorized: Literal[False]
    provider_query_authorized: Literal[False]
    real_dq_authorized: Literal[False]
    backtest_authorized: Literal[False]
    parameter_or_threshold_search_allowed: Literal[False]
    constant_false_fill_allowed: Literal[False]
    retained_series_truncation_allowed: Literal[False]
    cross_date_fallback_allowed: Literal[False]
    orders_allowed: Literal[False]
    fills_allowed: Literal[False]
    positions_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class MandatoryVetoOwnerFreezeDecisionPackDraftV2(_CanonicalModel):
    schema_version: Literal[
        "growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft.v2"
    ]
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft_v2"
    ]
    policy_version: Literal["2.0.0-draft.1"]
    status: Literal["OWNER_EXACT_FREEZE_REQUIRED_0_OF_4_ADMITTED"]
    task_id: Literal[
        "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
    ]
    predecessor_binding: ExactBinding
    calculation_semantics_binding: ExactBinding
    owner_scope: V2OwnerScope
    target_inventory: TargetInventory
    review_policy: ReviewPolicy
    decision_rows: tuple[
        BroadDecision,
        VolatilityDecision,
        ScheduledEventDecision,
        TrendDecision,
    ]
    aggregate_state: AggregateState
    safety: V2Safety

    @model_validator(mode="after")
    def validate_contract_surface(self) -> Self:
        if (
            self.predecessor_binding.path
            != "config/research/"
            "qc_qqq_options_growth_action_value_mandatory_veto_"
            "owner_freeze_decision_pack_draft_v1.yaml"
            or (
                self.predecessor_binding.file_sha256,
                self.predecessor_binding.canonical_sha256,
            )
            != (_PREDECESSOR_FILE_SHA256, _PREDECESSOR_CANONICAL_SHA256)
        ):
            raise ValueError("V2 predecessor exact identity drifted")
        if (
            self.calculation_semantics_binding.path
            != "config/research/"
            "qc_qqq_options_growth_action_value_mandatory_veto_calculation_semantics_v1.yaml"
            or (
                self.calculation_semantics_binding.file_sha256,
                self.calculation_semantics_binding.canonical_sha256,
            )
            != (_SEMANTICS_FILE_SHA256, _SEMANTICS_CANONICAL_SHA256)
        ):
            raise ValueError("V2 calculation semantics identity drifted")
        if self.aggregate_state.exact_semantics_objects_ready_for_owner_review != _VETO_IDS:
            raise ValueError("V2 owner-review inventory drifted")
        if self.aggregate_state.unresolved_producer_contracts != _VETO_IDS:
            raise ValueError("V2 unresolved inventory drifted")
        for row in self.decision_rows:
            if row.producer_contract.independent_input_universe != _EXPECTED_INPUTS[row.veto_id]:
                raise ValueError(f"{row.veto_id} input universe drifted")
            comparison_surface = tuple(
                (
                    item.component_id,
                    item.left_metric,
                    item.operator,
                    item.right_operand,
                    item.right_numeric,
                )
                for item in row.formula_contract.comparisons
            )
            if comparison_surface != _EXPECTED_COMPARISONS[row.veto_id]:
                raise ValueError(f"{row.veto_id} formula operator tree drifted")
            if "OWNER_EXACT_FREEZE_NOT_GRANTED" not in row.open_evidence_blockers:
                raise ValueError(f"{row.veto_id} dropped owner-freeze blocker")
            lowered_inputs = tuple(
                item.lower() for item in row.producer_contract.independent_input_universe
            )
            if any(marker in lowered_inputs for marker in _FORBIDDEN_DEPENDENCY_MARKERS):
                raise ValueError(f"{row.veto_id} contains forbidden result dependency")
        broad, volatility, event, trend = self.decision_rows
        if broad.formula_contract.combination_rule != "OR":
            raise ValueError("broad-market combination drifted")
        if broad.rolling_contract.source_window_axis != "BOUND_SPY_EXCHANGE_SESSIONS":
            raise ValueError("broad-market source window drifted")
        if volatility.formula_contract.combination_rule != "OR":
            raise ValueError("volatility combination drifted")
        if event.formula_contract.combination_rule != "ANY_EVENT":
            raise ValueError("event combination drifted")
        taxonomy = tuple(
            (item.authority, item.event_types)
            for item in event.event_contract.admitted_event_taxonomy
        )
        if taxonomy != (
            ("FEDERAL_RESERVE", ("FOMC_RATE_DECISION",)),
            ("BLS", ("CPI", "NONFARM_PAYROLLS")),
            ("BEA", ("PCE_PRICE_INDEX", "GDP_ADVANCE_ESTIMATE")),
        ):
            raise ValueError("event taxonomy drifted")
        if trend.formula_contract.combination_rule != "AND":
            raise ValueError("trend entry combination drifted")
        if trend.rolling_contract.source_window_axis != "BOUND_QQQ_EXCHANGE_SESSIONS":
            raise ValueError("trend source window drifted")
        return self


@dataclass(frozen=True)
class MandatoryVetoCalculationSemanticsLoadResult:
    policy: MandatoryVetoCalculationSemantics
    path: Path
    file_sha256: str
    canonical_sha256: str


@dataclass(frozen=True)
class MandatoryVetoOwnerFreezeDecisionPackDraftV2LoadResult:
    policy: MandatoryVetoOwnerFreezeDecisionPackDraftV2
    path: Path
    file_sha256: str
    canonical_sha256: str
    predecessor: predecessor.MandatoryVetoOwnerFreezeDecisionPackDraftLoadResult
    calculation_semantics: MandatoryVetoCalculationSemanticsLoadResult
    terminal: Literal["OWNER_EXACT_FREEZE_REQUIRED_0_OF_4_ADMITTED"]


def load_mandatory_veto_calculation_semantics(
    *,
    path: Path = DEFAULT_CALCULATION_SEMANTICS_PATH,
    project_root: Path = PROJECT_ROOT,
) -> MandatoryVetoCalculationSemanticsLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="calculation_semantics")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = MandatoryVetoCalculationSemantics.model_validate(payload)
        file_sha256 = hashlib.sha256(raw).hexdigest()
        if (file_sha256, policy.canonical_sha256) != (
            _SEMANTICS_FILE_SHA256,
            _SEMANTICS_CANONICAL_SHA256,
        ):
            raise ValueError("calculation semantics exact identity drifted")
    except MandatoryVetoExactSemanticsDraftError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise MandatoryVetoExactSemanticsDraftError(
            "MANDATORY_VETO_CALCULATION_SEMANTICS_REJECTED", str(exc)
        ) from exc
    return MandatoryVetoCalculationSemanticsLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=file_sha256,
        canonical_sha256=policy.canonical_sha256,
    )


def load_mandatory_veto_owner_freeze_decision_pack_draft_v2(
    *,
    path: Path = DEFAULT_OWNER_FREEZE_DECISION_PACK_DRAFT_V2_PATH,
    project_root: Path = PROJECT_ROOT,
) -> MandatoryVetoOwnerFreezeDecisionPackDraftV2LoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="owner_freeze_decision_pack_v2")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = MandatoryVetoOwnerFreezeDecisionPackDraftV2.model_validate(payload)
        bound_predecessor = _bound_file(
            Path(policy.predecessor_binding.path),
            root=project_root,
            field="predecessor_binding",
        )
        if hashlib.sha256(bound_predecessor.read_bytes()).hexdigest() != (
            policy.predecessor_binding.file_sha256
        ):
            raise ValueError("V2 predecessor bound file SHA-256 mismatch")
        loaded_predecessor = predecessor.load_mandatory_veto_owner_freeze_decision_pack_draft(
            project_root=project_root
        )
        if (loaded_predecessor.file_sha256, loaded_predecessor.canonical_sha256) != (
            policy.predecessor_binding.file_sha256,
            policy.predecessor_binding.canonical_sha256,
        ):
            raise ValueError("V2 predecessor loader identity drifted")
        loaded_semantics = load_mandatory_veto_calculation_semantics(
            path=Path(policy.calculation_semantics_binding.path),
            project_root=project_root,
        )
        if (loaded_semantics.file_sha256, loaded_semantics.canonical_sha256) != (
            policy.calculation_semantics_binding.file_sha256,
            policy.calculation_semantics_binding.canonical_sha256,
        ):
            raise ValueError("V2 calculation semantics loader identity drifted")
    except (
        MandatoryVetoExactSemanticsDraftError,
        predecessor.MandatoryVetoOwnerFreezeDecisionPackDraftError,
    ):
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise MandatoryVetoExactSemanticsDraftError(
            "MANDATORY_VETO_OWNER_FREEZE_DECISION_PACK_DRAFT_V2_REJECTED", str(exc)
        ) from exc
    return MandatoryVetoOwnerFreezeDecisionPackDraftV2LoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        predecessor=loaded_predecessor,
        calculation_semantics=loaded_semantics,
        terminal="OWNER_EXACT_FREEZE_REQUIRED_0_OF_4_ADMITTED",
    )


__all__ = [
    "DEFAULT_CALCULATION_SEMANTICS_PATH",
    "DEFAULT_OWNER_FREEZE_DECISION_PACK_DRAFT_V2_PATH",
    "MandatoryVetoCalculationSemantics",
    "MandatoryVetoCalculationSemanticsLoadResult",
    "MandatoryVetoExactSemanticsDraftError",
    "MandatoryVetoOwnerFreezeDecisionPackDraftV2",
    "MandatoryVetoOwnerFreezeDecisionPackDraftV2LoadResult",
    "load_mandatory_veto_calculation_semantics",
    "load_mandatory_veto_owner_freeze_decision_pack_draft_v2",
]
