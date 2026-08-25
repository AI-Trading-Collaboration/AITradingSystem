from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.strategy_growth_action_value_dq_pit_contract_v3 import (
    load_strategy_growth_action_value_dq_pit_contract_v3,
)
from ai_trading_system.strategy_growth_action_value_freeze_readiness_contract_v4 import (
    load_strategy_growth_action_value_freeze_readiness_contract_v4,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_GROWTH_ACTION_VALUE_REAL_REVIEW_POLICY_PATH = Path(
    "config/research/qc_qqq_options_growth_action_value_real_review_execution_v1.yaml"
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SESSION_SHA256 = "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
_RANK_COMPONENTS = (
    "ABSOLUTE_DTE_DISTANCE",
    "ABSOLUTE_DELTA_DISTANCE",
    "RELATIVE_SPREAD",
    "NEGATIVE_PRIOR_SESSION_OPEN_INTEREST",
    "NEGATIVE_DECISION_AS_OF_CUMULATIVE_VOLUME",
    "STABLE_OPTION_SID",
)
_VETO_TYPES = (
    "risk_off_veto",
    "volatility_veto",
    "event_risk_veto",
    "trend_break_veto",
    "tqqq_veto",
)


class GrowthActionValueRealReviewPolicyError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
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
    current = resolved_root
    for part in relative.parts:
        current /= part
        if current.exists() and current.is_symlink():
            raise ValueError(f"{field} cannot traverse a symlink")
    if not candidate.is_file() or candidate.is_symlink():
        raise ValueError(f"{field} must be a non-symlink regular file")
    return candidate


class OwnerProcessAuthorization(_StrictModel):
    decision_id: Literal[
        "owner_decision:TRADING-2542E:2026-08-25:authorize_policy_pack_drafting_and_continue_review_v1"
    ]
    drafting_authorized: Literal[True]
    exact_value_freeze_state: Literal["PENDING_OWNER_REVIEW"]
    real_run_scope_authorized: Literal[True]
    real_run_dispatch_authorized: Literal[False]
    authorization_consumption_state: Literal["UNCONSUMED_NO_BACKTEST_DISPATCH"]
    rationale: str


class PolicyMetadata(_StrictModel):
    owner: Literal["project_owner_and_strategy_research_governance"]
    prepared_by: Literal["codex_research_coordinator"]
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str


class ImmutableBinding(_StrictModel):
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


class FileRoleBinding(_StrictModel):
    path: str
    file_sha256: str
    accepted_role: str

    @field_validator("file_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


class AuthorityBindings(_StrictModel):
    dq_pit_v3: ImmutableBinding
    exact_sheet_v4: ImmutableBinding
    exposure_matched_comparator: FileRoleBinding
    historical_selection_source: FileRoleBinding
    risk_veto_policy: FileRoleBinding


class ScopeBinding(_StrictModel):
    hypothesis_id: Literal["BASELINE_BOUNDED_QQQ_GROWTH_OVERLAY_NON_BETA_ACTION_VALUE_V1"]
    selected_data_lane: Literal["QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE"]
    baseline_id: Literal["equal_risk_qqq_sgov"]
    comparator_id: Literal["exposure_matched_no_signal"]
    action_universe: tuple[Literal["QQQ", "SGOV"], Literal["QQQ", "SGOV"]]
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    expected_session_count: Literal[1202]
    target_session_inventory_lf_sha256: str
    pre_window_prior_session: date
    random_seed: Literal[2542]
    bootstrap_resamples: Literal[10000]

    @model_validator(mode="after")
    def validate_scope(self) -> Self:
        if self.action_universe != ("QQQ", "SGOV"):
            raise ValueError("action universe order drifted")
        expected = (date(2021, 2, 22), date(2025, 12, 2))
        if (self.requested_start, self.requested_end) != expected:
            raise ValueError("requested range drifted")
        if (self.evaluated_start, self.evaluated_end) != expected:
            raise ValueError("evaluated range drifted")
        if self.pre_window_prior_session != date(2021, 2, 19):
            raise ValueError("pre-window prior drifted")
        if self.target_session_inventory_lf_sha256 != _SESSION_SHA256:
            raise ValueError("session inventory identity drifted")
        return self


class CatalogAndOpenInterestAdapter(_StrictModel):
    source_type: Literal["OptionUniverse"]
    source_date_field: Literal["Time"]
    available_at_field: Literal["EndTime"]
    exact_available_at_rule: Literal["END_TIME_EQUALS_TIME_PLUS_ONE_CALENDAR_DAY"]
    target_session_catalog_rule: Literal["PREVIOUS_QQQ_EXCHANGE_SESSION_SOURCE_ROW"]
    target_session_open_interest_rule: Literal["PREVIOUS_QQQ_EXCHANGE_SESSION_SOURCE_ROW"]
    first_target_source_row: date
    exact_source_date_required: Literal[True]
    cross_date_fallback_allowed: Literal[False]
    current_session_open_interest_allowed: Literal[False]


class QuoteAdapter(_StrictModel):
    source_type: Literal["MINUTE_QUOTE_BAR"]
    bid_field: Literal["Bid.Close"]
    ask_field: Literal["Ask.Close"]
    quote_end_field: Literal["EndTime"]
    available_at_field: Literal["DELIVERY_FRONTIER_UTC"]
    source_date_rule: Literal["QUOTE_BAR_EXCHANGE_DATE_EQUALS_TARGET_SESSION"]
    decision_time_new_york: Literal["15:59:00"]
    last_completed_bar_only: Literal[True]
    max_quote_age_seconds: Literal[120]
    daily_option_universe_time_as_quote_end_allowed: Literal[False]


class VolumeAdapter(_StrictModel):
    source_type: Literal["MINUTE_TRADE_BAR"]
    field: Literal["Volume"]
    aggregation: Literal["SUM_FROM_REGULAR_SESSION_OPEN_THROUGH_DECISION_AS_OF_INCLUSIVE"]
    available_at_field: Literal["DELIVERY_FRONTIER_UTC"]
    source_date_rule: Literal["TRADE_BAR_EXCHANGE_DATE_EQUALS_TARGET_SESSION"]
    end_of_day_or_revised_volume_allowed: Literal[False]


class ProviderAdapter(_StrictModel):
    provider: Literal["QuantConnect_US_Equity_Options_AlgoSeek"]
    storage_timezone: Literal["UTC"]
    exchange_timezone: Literal["America/New_York"]
    catalog_and_open_interest: CatalogAndOpenInterestAdapter
    quote: QuoteAdapter
    volume: VolumeAdapter
    missing_or_timestamp_drift_outcome: Literal["INVALID"]


class ContributorSelection(_StrictModel):
    policy_state: Literal["PROPOSED_OWNER_REVIEW_REQUIRED"]
    option_rights: tuple[Literal["CALL", "PUT"], Literal["CALL", "PUT"]]
    selected_contract_count_per_right: Literal[1]
    total_expected_contributor_count_per_session: Literal[2]
    eligibility_order: tuple[str, ...]
    min_dte: Literal[7]
    target_dte: Literal[14]
    max_dte: Literal[21]
    max_abs_moneyness_deviation: Decimal
    min_abs_prior_day_delta: Decimal
    target_abs_prior_day_delta: Decimal
    max_abs_prior_day_delta: Decimal
    max_quote_age_seconds: Literal[120]
    max_relative_spread: Decimal
    min_prior_session_open_interest: Literal[10]
    min_decision_as_of_cumulative_volume: Literal[1]
    deterministic_rank_components: tuple[str, ...]
    expected_manifest_rule: Literal["SORTED_UNIQUE_SELECTED_CALL_AND_PUT_SID_HASH_ONLY"]
    raw_or_hashed_sid_export_allowed: Literal[False]
    missing_either_right_outcome: Literal["INSUFFICIENT"]
    invalid_row_before_exclusion_outcome: Literal["INVALID"]

    @model_validator(mode="after")
    def validate_selection(self) -> Self:
        if self.option_rights != ("CALL", "PUT"):
            raise ValueError("option rights drifted")
        if self.deterministic_rank_components != _RANK_COMPONENTS:
            raise ValueError("deterministic rank order drifted")
        if not self.min_dte <= self.target_dte <= self.max_dte:
            raise ValueError("DTE bounds invalid")
        if not (
            Decimal("0")
            <= self.min_abs_prior_day_delta
            <= self.target_abs_prior_day_delta
            <= self.max_abs_prior_day_delta
            <= Decimal("1")
        ):
            raise ValueError("delta bounds invalid")
        if self.max_abs_moneyness_deviation != Decimal("0.05"):
            raise ValueError("moneyness proposal drifted")
        if self.max_relative_spread != Decimal("0.20"):
            raise ValueError("spread must inherit DQ/PIT V3")
        return self


class GrowthStateMapping(_StrictModel):
    policy_state: Literal["PROPOSED_OWNER_REVIEW_REQUIRED"]
    source_session_rule: Literal["TARGET_SESSION_MINUTE_DATA_THROUGH_15_59_NEW_YORK"]
    selected_call_activity_formula: str
    selected_put_activity_formula: str
    activation_rule: Literal["CALL_ACTIVITY_STRICTLY_GREATER_THAN_PUT_ACTIVITY"]
    equality_rule: Literal["GROWTH_INACTIVE"]
    active_state: Literal["GROWTH_ACTIVE"]
    inactive_state: Literal["GROWTH_INACTIVE"]
    effective_session_rule: Literal["NEXT_VALID_QQQ_EXCHANGE_SESSION"]
    signal_lag_sessions: Literal[1]
    missing_input_outcome: Literal["INSUFFICIENT"]
    invalid_input_outcome: Literal["INVALID"]
    same_session_weight_application_allowed: Literal[False]


class ResearchWeights(_StrictModel):
    QQQ: Decimal
    SGOV: Decimal

    @model_validator(mode="after")
    def validate_weights(self) -> Self:
        if self.QQQ < 0 or self.SGOV < 0 or self.QQQ + self.SGOV != Decimal("1"):
            raise ValueError("weights must be long-only and sum exactly to one")
        return self


class ActionSizing(_StrictModel):
    policy_state: Literal["PROPOSED_OWNER_REVIEW_REQUIRED"]
    baseline_research_weights: ResearchWeights
    growth_active_research_weights: ResearchWeights
    growth_inactive_research_weights: ResearchWeights
    any_hard_veto_active_research_weights: ResearchWeights
    maximum_qqq_increment_over_baseline: Decimal
    weight_sum_required: Decimal
    long_only: Literal[True]
    uses_leverage_etf: Literal[False]
    uses_options_position: Literal[False]
    borrowed_leverage_allowed: Literal[False]
    effective_timing: Literal["PRIOR_SESSION_SIGNAL_FOR_NEXT_SESSION_OPENING_RESEARCH_WEIGHTS"]
    derived_fill_rule: Literal[
        "DIFFERENCE_BETWEEN_OPENING_PRE_TRADE_AND_TARGET_NOTIONAL_AT_SESSION_OPEN"
    ]
    lean_order_api_allowed: Literal[False]
    official_target_weights: Literal[False]

    @model_validator(mode="after")
    def validate_action(self) -> Self:
        baseline = ResearchWeights(QQQ=Decimal("0.50"), SGOV=Decimal("0.50"))
        growth = ResearchWeights(QQQ=Decimal("0.60"), SGOV=Decimal("0.40"))
        if self.baseline_research_weights != baseline:
            raise ValueError("baseline weights drifted")
        if self.growth_inactive_research_weights != baseline:
            raise ValueError("inactive weights must equal baseline")
        if self.any_hard_veto_active_research_weights != baseline:
            raise ValueError("hard veto must suppress growth increment")
        if self.growth_active_research_weights != growth:
            raise ValueError("growth-active proposal drifted")
        if self.maximum_qqq_increment_over_baseline != Decimal("0.10"):
            raise ValueError("growth increment drifted")
        if self.weight_sum_required != Decimal("1.00"):
            raise ValueError("weight sum policy drifted")
        return self


class DefensiveVetoInput(_StrictModel):
    required: Literal[True]
    taxonomy_policy_id: Literal["risk_on_veto_policy_v1"]
    required_veto_types: tuple[str, ...]
    candidate_growth_allowed_rule: Literal["ALL_REQUIRED_VETOES_EXACTLY_FALSE"]
    missing_veto_interpreted_as_clear: Literal[False]
    missing_or_non_pit_veto_outcome: Literal["INVALID"]
    current_series_admission_state: Literal["BLOCKED_NO_EXACT_1202_SESSION_PIT_DQ_SERIES"]
    required_before_exact_freeze: str

    @model_validator(mode="after")
    def validate_vetoes(self) -> Self:
        if self.required_veto_types != _VETO_TYPES:
            raise ValueError("veto taxonomy drifted")
        return self


class ExternalScope(_StrictModel):
    platform: Literal["QuantConnect"]
    target_clone_project_id: Literal[35444189]
    original_project_id: Literal[34808569]
    maximum_existing_clone_mutations: Literal[1]
    maximum_saves: Literal[1]
    maximum_candidate_builds: Literal[1]
    maximum_zero_order_backtests: Literal[1]
    maximum_retries: Literal[0]
    maximum_new_clones: Literal[0]
    maximum_exact_date_provider_queries: Literal[1]
    exact_date_provider_query_source_date: date
    original_project_mutations: Literal[0]
    raw_option_rows_exported: Literal[0]
    contract_identifiers_exported: Literal[0]
    object_store_writes: Literal[0]
    public_shares: Literal[0]
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]


class Safety(_StrictModel):
    proposal_only: Literal[True]
    policy_values_frozen: Literal[False]
    candidate_generation_allowed: Literal[False]
    manifest_generation_allowed: Literal[False]
    cache_read_authorized: Literal[False]
    provider_query_authorized: Literal[False]
    real_run_dispatch_authorized: Literal[False]
    result_visible_when_values_selected: Literal[False]
    parameter_search_allowed: Literal[False]
    threshold_after_result_allowed: Literal[False]
    raw_option_rows_allowed: Literal[False]
    order_generation_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class GrowthActionValueRealReviewPolicy(_CanonicalModel):
    schema_version: Literal[
        "qc_qqq_options_growth_action_value_real_review_execution_policy.v1"
    ]
    policy_id: Literal["qc_qqq_options_growth_action_value_real_review_execution_v1"]
    policy_version: Literal["1.0.0-draft.1"]
    status: Literal["DRAFT_FOR_OWNER_EXACT_FREEZE"]
    task_id: Literal[
        "TRADING-2542E_GROWTH_ACTION_VALUE_REAL_DQ_AND_LOCKED_BACKTEST_REVIEW_V1"
    ]
    owner_process_authorization: OwnerProcessAuthorization
    policy_metadata: PolicyMetadata
    authority_bindings: AuthorityBindings
    scope_binding: ScopeBinding
    provider_adapter: ProviderAdapter
    contributor_selection: ContributorSelection
    growth_state_mapping: GrowthStateMapping
    action_sizing: ActionSizing
    defensive_veto_input: DefensiveVetoInput
    external_scope: ExternalScope
    safety: Safety

    @model_validator(mode="after")
    def validate_cross_contract(self) -> Self:
        selection = self.contributor_selection
        quote = self.provider_adapter.quote
        if selection.max_quote_age_seconds != quote.max_quote_age_seconds:
            raise ValueError("selection and quote adapter age thresholds differ")
        if self.provider_adapter.catalog_and_open_interest.first_target_source_row != date(
            2021, 2, 19
        ):
            raise ValueError("first target prior row drifted")
        if self.external_scope.exact_date_provider_query_source_date != date(2022, 8, 26):
            raise ValueError("exact-date query target drifted")
        return self


@dataclass(frozen=True)
class GrowthActionValueRealReviewPolicyLoadResult:
    policy: GrowthActionValueRealReviewPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str
    terminal: Literal["DRAFT_READY_FOR_OWNER_REVIEW_WITH_EXPLICIT_VETO_INPUT_BLOCKER"]


def _validate_authority_files(
    policy: GrowthActionValueRealReviewPolicy, *, project_root: Path
) -> None:
    bindings = policy.authority_bindings
    for field, binding in (
        ("dq_pit_v3", bindings.dq_pit_v3),
        ("exact_sheet_v4", bindings.exact_sheet_v4),
        ("exposure_matched_comparator", bindings.exposure_matched_comparator),
        ("historical_selection_source", bindings.historical_selection_source),
        ("risk_veto_policy", bindings.risk_veto_policy),
    ):
        path = _bound_file(Path(binding.path), root=project_root, field=field)
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != binding.file_sha256:
            raise ValueError(f"{field} file SHA-256 mismatch")


def load_growth_action_value_real_review_policy(
    *,
    policy_path: Path = DEFAULT_GROWTH_ACTION_VALUE_REAL_REVIEW_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> GrowthActionValueRealReviewPolicyLoadResult:
    try:
        path = _bound_file(policy_path, root=project_root, field="policy_path")
        raw = path.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(policy_path))
        policy = GrowthActionValueRealReviewPolicy.model_validate(payload)
        _validate_authority_files(policy, project_root=project_root)

        dq = load_strategy_growth_action_value_dq_pit_contract_v3(project_root=project_root)
        dq_binding = policy.authority_bindings.dq_pit_v3
        if dq.contract_file_sha256 != dq_binding.file_sha256:
            raise ValueError("DQ/PIT V3 file identity drifted")
        if dq.contract_canonical_sha256 != dq_binding.canonical_sha256:
            raise ValueError("DQ/PIT V3 canonical identity drifted")
        numeric = dq.contract.numeric_policy
        selection = policy.contributor_selection
        if numeric.max_quote_age_seconds.value != selection.max_quote_age_seconds:
            raise ValueError("quote-age proposal does not inherit DQ/PIT V3")
        if Decimal(numeric.max_relative_spread.value) != selection.max_relative_spread:
            raise ValueError("spread proposal does not inherit DQ/PIT V3")
        if numeric.min_open_interest.value != selection.min_prior_session_open_interest:
            raise ValueError("open-interest proposal does not inherit DQ/PIT V3")
        if numeric.min_volume.value != selection.min_decision_as_of_cumulative_volume:
            raise ValueError("volume proposal does not inherit DQ/PIT V3")

        sheet = load_strategy_growth_action_value_freeze_readiness_contract_v4(
            project_root=project_root
        )
        sheet_binding = policy.authority_bindings.exact_sheet_v4
        if sheet.contract_file_sha256 != sheet_binding.file_sha256:
            raise ValueError("exact sheet V4 file identity drifted")
        if sheet.contract_canonical_sha256 != sheet_binding.canonical_sha256:
            raise ValueError("exact sheet V4 canonical identity drifted")
    except GrowthActionValueRealReviewPolicyError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise GrowthActionValueRealReviewPolicyError(
            "GROWTH_ACTION_VALUE_REAL_REVIEW_POLICY_REJECTED", str(exc)
        ) from exc

    return GrowthActionValueRealReviewPolicyLoadResult(
        policy=policy,
        policy_path=path,
        policy_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy_canonical_sha256=policy.canonical_sha256,
        terminal="DRAFT_READY_FOR_OWNER_REVIEW_WITH_EXPLICIT_VETO_INPUT_BLOCKER",
    )


__all__ = [
    "DEFAULT_GROWTH_ACTION_VALUE_REAL_REVIEW_POLICY_PATH",
    "GrowthActionValueRealReviewPolicy",
    "GrowthActionValueRealReviewPolicyError",
    "GrowthActionValueRealReviewPolicyLoadResult",
    "load_growth_action_value_real_review_policy",
]
