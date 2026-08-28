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
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_EXACT_SIGNAL_IMPLEMENTATION_POLICY_DRAFT_PATH = Path(
    "config/research/qc_qqq_options_exact_signal_implementation_policy_draft_v1.yaml"
)

_TASK_ID = "TRADING-2542I_QQQ_OPTIONS_EXACT_SIGNAL_AND_IMPLEMENTATION_POLICY_DRAFT_V1"
_TERMINAL: Literal["OWNER_EXACT_FREEZE_AND_SIGNAL_PACKAGE_REQUIRED_NO_BACKTEST"] = (
    "OWNER_EXACT_FREEZE_AND_SIGNAL_PACKAGE_REQUIRED_NO_BACKTEST"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_AUTHORITY_PATHS = (
    "config/research/first_layer_composer_v2.yaml",
    "config/research/qqq_options_signal_export_v1.yaml",
    "config/research/qqq_options_deterministic_selection_v1.yaml",
    "config/research/qqq_options_minute_execution_reality_v1.yaml",
    "config/research/qqq_options_cash_premium_settlement_accounting_v1.yaml",
    "config/research/qqq_options_lifecycle_expiry_corporate_action_safety_v1.yaml",
    "config/research/qqq_options_daily_primary_backtest_contract_v1.yaml",
    "config/research/qqq_options_owner_decision_manifest_v2.yaml",
    "inputs/research/qqq_options/"
    "trading_2541_exact_date_subscription_recovery_execution_v3/"
    "export_safe_terminal_evidence.json",
    "config/research/qc_qqq_options_frozen_signal_implementation_retest_contract_v1.yaml",
)
_AUTHORITY_ROLES = (
    "SOURCE_SIGNAL_SEMANTIC_CANDIDATE",
    "SIGNAL_PACKAGE_MECHANICS",
    "SELECTION_MECHANICS",
    "EXECUTION_MECHANICS",
    "ACCOUNTING_MECHANICS",
    "LIFECYCLE_MECHANICS",
    "DAILY_PRIMARY_CONTRACT",
    "OWNER_SLOT_CATALOG_V2",
    "QC_CHAIN_COVERAGE_EVIDENCE",
    "FROZEN_SIGNAL_RETEST_BOUNDARY",
)
_SOURCE_ENUM = ("risk_on", "constructive", "neutral", "defensive", "risk_off")
_MAPPING_ROWS = (
    ("risk_on", "LONG_CALL"),
    ("constructive", "LONG_CALL"),
    ("neutral", "FLAT"),
    ("defensive", "FLAT"),
    ("risk_off", "FLAT"),
)
_RANK_COMPONENTS = (
    "ABS_DELTA_DISTANCE",
    "ABS_DTE_DISTANCE",
    "RELATIVE_SPREAD",
    "OPEN_INTEREST_DESC",
    "EXPIRY",
    "STRIKE",
    "SID",
)
_EXPECTED_SLOT_GROUPS = (
    ("ACC_CASH_CARRY_BENCHMARK", "accounting"),
    ("ACC_CASH_RESERVATION", "accounting"),
    ("ACC_COST_BASIS_CONVENTION", "accounting"),
    ("ACC_DQ_PIT_REPRO", "acceptance"),
    ("ACC_EVENT_IDENTITY_INVARIANT", "accounting"),
    ("ACC_FEE_SCHEDULE", "accounting"),
    ("ACC_INITIAL_CASH", "accounting"),
    ("ACC_INVESTMENT_PROMOTION", "acceptance"),
    ("ACC_METRIC_BENCHMARK_IDENTITY", "acceptance"),
    ("ACC_RESEARCH_MULTIPLICITY_CONTROL", "acceptance"),
    ("ACC_RESULT_INCLUSION", "acceptance"),
    ("ACC_ROUNDING_RECONCILIATION", "accounting"),
    ("ACC_SAMPLE_COVERAGE", "acceptance"),
    ("ACC_SETTLEMENT_TIMING", "accounting"),
    ("ACC_SIZING_EXPOSURE", "accounting"),
    ("EXE_CANCEL_REJECT_NO_FILL", "execution"),
    ("EXE_EXECUTION_OBSERVATION_SOURCE", "execution"),
    ("EXE_LATENCY", "execution"),
    ("EXE_MARKETABLE_LIMIT", "execution"),
    ("EXE_PARTIAL_FILL", "execution"),
    ("EXE_QUOTE_DISPOSITION", "execution"),
    ("EXE_SLIPPAGE", "execution"),
    ("LIFE_ASSIGNMENT_POLICY", "lifecycle"),
    ("LIFE_CLOSE_HOLD_POLICY", "lifecycle"),
    ("LIFE_EXERCISE_POLICY", "lifecycle"),
    ("LIFE_EXPIRY_EXIT_GUARD", "lifecycle"),
    ("LIFE_POSITION_STATE_TRANSITION", "lifecycle"),
    ("LIFE_ROLL_AUTHORIZATION", "lifecycle"),
    ("LIFE_TERMINAL_VALUATION", "lifecycle"),
    ("SEL_DELTA_SOURCE_RANGE", "selection"),
    ("SEL_DTE_WINDOW", "selection"),
    ("SEL_MONEYNESS_RANGE", "selection"),
    ("SEL_OPEN_INTEREST_FLOOR", "selection"),
    ("SEL_QUOTE_FRESHNESS", "selection"),
    ("SEL_RANK_PRIORITY", "selection"),
    ("SEL_SPREAD_LIMIT", "selection"),
    ("SEL_VOLUME_FLOOR", "selection"),
)
_G5_SLOT_IDS = frozenset(
    {"ACC_INVESTMENT_PROMOTION", "EXE_PARTIAL_FILL", "SEL_VOLUME_FLOOR"}
)
_EXPECTED_SLOT_REFS = {
    "ACC_CASH_CARRY_BENCHMARK": "accounting_proposal.idle_cash_carry",
    "ACC_CASH_RESERVATION": "accounting_proposal.reservation_rule",
    "ACC_COST_BASIS_CONVENTION": "accounting_proposal.cost_basis_method",
    "ACC_DQ_PIT_REPRO": "result_admission.exact_session_coverage_required",
    "ACC_EVENT_IDENTITY_INVARIANT": "accounting_proposal.cash_quantum_usd",
    "ACC_FEE_SCHEDULE": "execution_proposal.fee_per_contract_per_side_usd",
    "ACC_INITIAL_CASH": "accounting_proposal.initial_cash_usd",
    "ACC_INVESTMENT_PROMOTION": "result_admission.investment_promotion_allowed",
    "ACC_METRIC_BENCHMARK_IDENTITY": "result_admission.same_signal_paired_comparator_required",
    "ACC_RESEARCH_MULTIPLICITY_CONTROL": "result_admission.preregistered_baseline_count",
    "ACC_RESULT_INCLUSION": "result_admission.invalid_run_in_aggregate_allowed",
    "ACC_ROUNDING_RECONCILIATION": "accounting_proposal.rounding_mode",
    "ACC_SAMPLE_COVERAGE": "research_window.expected_session_count",
    "ACC_SETTLEMENT_TIMING": "accounting_proposal.sell_proceeds_settlement_lag_xnys_sessions",
    "ACC_SIZING_EXPOSURE": "accounting_proposal.premium_budget_fraction_of_pretrade_nav",
    "EXE_CANCEL_REJECT_NO_FILL": "execution_proposal.cancel_after_minutes",
    "EXE_EXECUTION_OBSERVATION_SOURCE": "execution_proposal.entry_order",
    "EXE_LATENCY": "execution_proposal.submit_time_rule",
    "EXE_MARKETABLE_LIMIT": "execution_proposal.adverse_price_adjustment_per_share_usd",
    "EXE_PARTIAL_FILL": "execution_proposal.partial_fill_disposition",
    "EXE_QUOTE_DISPOSITION": "selection_proposal.quote_integrity",
    "EXE_SLIPPAGE": "execution_proposal.adverse_price_adjustment_per_share_usd",
    "LIFE_ASSIGNMENT_POLICY": "lifecycle_proposal.assignment_allowed",
    "LIFE_CLOSE_HOLD_POLICY": "lifecycle_proposal.exit_on_flat_signal",
    "LIFE_EXERCISE_POLICY": "lifecycle_proposal.exercise_allowed",
    "LIFE_EXPIRY_EXIT_GUARD": "lifecycle_proposal.pre_expiry_guard_xnys_sessions",
    "LIFE_POSITION_STATE_TRANSITION": "lifecycle_proposal.max_simultaneous_positions",
    "LIFE_ROLL_AUTHORIZATION": "lifecycle_proposal.atomic_or_same_session_roll_allowed",
    "LIFE_TERMINAL_VALUATION": "lifecycle_proposal.terminal_mark",
    "SEL_DELTA_SOURCE_RANGE": "selection_proposal.target_abs_delta",
    "SEL_DTE_WINDOW": "selection_proposal.target_dte",
    "SEL_MONEYNESS_RANGE": "selection_proposal.moneyness_definition",
    "SEL_OPEN_INTEREST_FLOOR": "selection_proposal.min_prior_session_open_interest",
    "SEL_QUOTE_FRESHNESS": "selection_proposal.max_quote_age_seconds",
    "SEL_RANK_PRIORITY": "selection_proposal.rank_components",
    "SEL_SPREAD_LIMIT": "selection_proposal.max_relative_spread",
    "SEL_VOLUME_FLOOR": "selection_proposal.volume_floor_disposition",
}


class ExactSignalImplementationPolicyDraftError(ValueError):
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


class AuthorityBinding(_StrictModel):
    path: str
    file_sha256: str
    role: str
    immutable: Literal[True]

    @field_validator("file_sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("file_sha256 must be lowercase SHA-256")
        return value


class ResearchWindow(_StrictModel):
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    expected_session_count: int
    calendar: Literal["XNYS"]
    role: Literal["PRIMARY"]

    @model_validator(mode="after")
    def validate_window(self) -> Self:
        if (
            self.requested_start,
            self.requested_end,
            self.evaluated_start,
            self.evaluated_end,
            self.expected_session_count,
        ) != (
            date(2021, 2, 22),
            date(2025, 12, 2),
            date(2021, 2, 22),
            date(2025, 12, 2),
            1202,
        ):
            raise ValueError("primary research window drifted")
        return self


class SourceCandidate(_StrictModel):
    producer_id: Literal["first_layer_composer_v2"]
    source_locator: Literal[
        "outputs/research_trends/models/first_layer_composer_v2_predictions.csv:trend_state"
    ]
    source_policy_status: Literal["pilot_baseline"]
    source_role: Literal["RECOMMENDED_SEMANTIC_SOURCE_NOT_EXACT_PACKAGE_READY"]
    source_enum: tuple[str, ...]
    documented_actual_start: date
    documented_actual_end: date
    raw_row_count: int
    unique_dated_signal_count: int
    primary_start_covered: Literal[False]
    retained_exact_1202_session_package_present: Literal[False]
    poc_rewrap_admissible_as_exact_package: Literal[False]
    required_next_artifact: Literal[
        "REGENERATED_EXACT_ONE_ROW_PER_XNYS_SESSION_WITH_CODE_CONFIG_INPUT_DQ_PIT_IDENTITY"
    ]

    @model_validator(mode="after")
    def validate_source_facts(self) -> Self:
        if self.source_enum != _SOURCE_ENUM:
            raise ValueError("source enum drifted")
        if (
            self.documented_actual_start,
            self.documented_actual_end,
            self.raw_row_count,
            self.unique_dated_signal_count,
        ) != (date(2023, 2, 22), date(2026, 3, 27), 2205, 777):
            raise ValueError("documented source readiness facts drifted")
        return self


class SignalMappingRow(_StrictModel):
    source_state: str
    option_action: Literal["LONG_CALL", "LONG_PUT", "FLAT"]


class SignalMapping(_StrictModel):
    status: Literal["PROPOSED_OWNER_REVIEW_NOT_FROZEN"]
    baseline_actions: tuple[Literal["LONG_CALL", "FLAT"], ...]
    long_put_baseline_allowed: Literal[False]
    long_put_role: Literal["SEPARATE_SENSITIVITY_ONLY"]
    option_or_result_input_allowed: Literal[False]
    effective_session_rule: Literal["NEXT_VALID_XNYS_SESSION"]
    missing_or_unknown_terminal: Literal["INVALID_SOURCE_SIGNAL"]
    rows: tuple[SignalMappingRow, ...]

    @model_validator(mode="after")
    def validate_mapping(self) -> Self:
        if self.baseline_actions != ("LONG_CALL", "FLAT"):
            raise ValueError("baseline action set drifted")
        observed = tuple((row.source_state, row.option_action) for row in self.rows)
        if observed != _MAPPING_ROWS:
            raise ValueError("five-state call-or-flat mapping drifted")
        return self


class SelectionProposal(_StrictModel):
    underlying: Literal["QQQ"]
    option_right: Literal["CALL"]
    long_premium_only: Literal[True]
    single_leg_only: Literal[True]
    min_dte_inclusive: int
    target_dte: int
    max_dte_inclusive: int
    delta_source: Literal["PRIOR_COMPLETED_SESSION_MODEL"]
    min_abs_delta_inclusive: Decimal
    target_abs_delta: Decimal
    max_abs_delta_inclusive: Decimal
    moneyness_definition: Literal["UNDERLYING_PRICE_DIVIDED_BY_STRIKE"]
    min_moneyness_inclusive: Decimal
    max_moneyness_inclusive: Decimal
    max_quote_age_seconds: int
    quote_integrity: Literal["TWO_SIDED_NON_CROSSED_POSITIVE_ASK"]
    spread_definition: Literal["ASK_MINUS_BID_DIVIDED_BY_MID"]
    max_relative_spread: Decimal
    min_prior_session_open_interest: int
    volume_floor_disposition: Literal[
        "PROPOSE_G5_NOT_APPLICABLE_LOOKAHEAD_AND_REDUNDANCY"
    ]
    rank_components: tuple[str, ...]
    no_contract_disposition: Literal[
        "NO_ELIGIBLE_CONTRACT_PRESERVE_CASH_NO_RELAXATION"
    ]

    @model_validator(mode="after")
    def validate_pilot_selection(self) -> Self:
        observed = (
            self.min_dte_inclusive,
            self.target_dte,
            self.max_dte_inclusive,
            self.min_abs_delta_inclusive,
            self.target_abs_delta,
            self.max_abs_delta_inclusive,
            self.min_moneyness_inclusive,
            self.max_moneyness_inclusive,
            self.max_quote_age_seconds,
            self.max_relative_spread,
            self.min_prior_session_open_interest,
        )
        expected = (
            30,
            35,
            45,
            Decimal("0.45"),
            Decimal("0.50"),
            Decimal("0.60"),
            Decimal("0.90"),
            Decimal("1.10"),
            60,
            Decimal("0.20"),
            100,
        )
        if observed != expected or self.rank_components != _RANK_COMPONENTS:
            raise ValueError("pilot selection proposal drifted")
        return self


class ExecutionProposal(_StrictModel):
    selection_time_rule: Literal["AFTER_FIRST_COMPLETE_DPLUS1_MINUTE_BAR"]
    submit_time_rule: Literal["NEXT_INDEPENDENT_MINUTE_AFTER_SELECTION"]
    entry_order: Literal["ASK_SIDE_MARKETABLE_LIMIT"]
    exit_order: Literal["BID_SIDE_MARKETABLE_LIMIT"]
    adverse_price_adjustment_per_share_usd: Decimal
    fee_per_contract_per_side_usd: Decimal
    cancel_after_minutes: int
    same_session_retry_allowed: Literal[False]
    contract_substitution_after_failure_allowed: Literal[False]
    max_contracts_per_order: int
    partial_fill_disposition: Literal[
        "PROPOSE_G5_NOT_APPLICABLE_INTEGER_SINGLE_CONTRACT"
    ]
    daily_close_fill_allowed: Literal[False]
    same_bar_fill_allowed: Literal[False]
    mid_or_last_fill_allowed: Literal[False]
    fill_forward_allowed: Literal[False]

    @model_validator(mode="after")
    def validate_pilot_execution(self) -> Self:
        if (
            self.adverse_price_adjustment_per_share_usd,
            self.fee_per_contract_per_side_usd,
            self.cancel_after_minutes,
            self.max_contracts_per_order,
        ) != (Decimal("0.01"), Decimal("0.65"), 5, 1):
            raise ValueError("pilot execution proposal drifted")
        return self


class AccountingProposal(_StrictModel):
    currency: Literal["USD"]
    account_type: Literal["CASH"]
    initial_cash_usd: Decimal
    premium_budget_fraction_of_pretrade_nav: Decimal
    max_open_contracts: int
    required_platform_multiplier: int
    reservation_rule: Literal[
        "LIMIT_PREMIUM_TIMES_MULTIPLIER_PLUS_ENTRY_FEE_BUFFER"
    ]
    fee_buffer_per_contract_usd: Decimal
    cash_quantum_usd: Decimal
    rounding_mode: Literal["ROUND_HALF_EVEN"]
    cost_basis_method: Literal["FIFO"]
    include_fees_in_cost_basis: Literal[True]
    sell_proceeds_settlement_lag_xnys_sessions: int
    idle_cash_carry: Literal["ZERO_RETURN_BASELINE"]
    cash_carry_comparator: Literal["SEPARATE_SGOV_CARRY_COMPARATOR"]
    negative_settled_cash_allowed: Literal[False]
    margin_allowed: Literal[False]

    @model_validator(mode="after")
    def validate_pilot_accounting(self) -> Self:
        if (
            self.initial_cash_usd,
            self.premium_budget_fraction_of_pretrade_nav,
            self.max_open_contracts,
            self.required_platform_multiplier,
            self.fee_buffer_per_contract_usd,
            self.cash_quantum_usd,
            self.sell_proceeds_settlement_lag_xnys_sessions,
        ) != (
            Decimal("100000.00"),
            Decimal("0.02"),
            1,
            100,
            Decimal("0.65"),
            Decimal("0.01"),
            1,
        ):
            raise ValueError("pilot accounting proposal drifted")
        return self


class LifecycleProposal(_StrictModel):
    max_simultaneous_positions: int
    exit_on_flat_signal: Literal["NEXT_LEGAL_EXECUTION_EVENT"]
    pre_expiry_guard_xnys_sessions: int
    atomic_or_same_session_roll_allowed: Literal[False]
    fresh_next_session_reentry_allowed: Literal[True]
    exercise_allowed: Literal[False]
    assignment_allowed: Literal[False]
    share_delivery_allowed: Literal[False]
    corporate_action_ambiguity_disposition: Literal["INVALID_RUN"]
    terminal_mark: Literal["VALID_BID_LIQUIDATION_VALUE"]
    max_mark_quote_age_seconds: int
    missing_mark_disposition: Literal["INSUFFICIENT_PLATFORM_EVIDENCE"]

    @model_validator(mode="after")
    def validate_pilot_lifecycle(self) -> Self:
        if (
            self.max_simultaneous_positions,
            self.pre_expiry_guard_xnys_sessions,
            self.max_mark_quote_age_seconds,
        ) != (1, 7, 60):
            raise ValueError("pilot lifecycle proposal drifted")
        return self


class ResultAdmission(_StrictModel):
    exact_session_coverage_required: Literal[True]
    no_contract_no_fill_cancel_retained_as_cash_facts: Literal[True]
    invalid_run_in_aggregate_allowed: Literal[False]
    preregistered_baseline_count: int
    sensitivity_result_selection_allowed: Literal[False]
    same_signal_paired_comparator_required: Literal[True]
    underlying_implementation_id: Literal["UNDERLYING_IMPLEMENTATION"]
    optionized_implementation_id: Literal["OPTIONIZED_IMPLEMENTATION"]
    maximum_interpretation: Literal["RESEARCH_COMPARISON_ONLY"]
    investment_promotion_allowed: Literal[False]
    raw_option_rows_allowed: Literal[False]
    local_option_repricing_allowed: Literal[False]

    @model_validator(mode="after")
    def validate_result_admission(self) -> Self:
        if self.preregistered_baseline_count != 1:
            raise ValueError("exactly one preregistered baseline is required")
        return self


class SlotProposal(_StrictModel):
    slot_id: str
    canonical_group: Literal[
        "selection", "execution", "accounting", "lifecycle", "acceptance"
    ]
    proposal_action: Literal["PROPOSE_G2", "PROPOSE_G5_NOT_APPLICABLE"]
    proposal_ref: str
    owner_frozen: Literal[False]


class Safety(_StrictModel):
    draft_authorized: Literal[True]
    owner_exact_freeze: Literal[False]
    exact_signal_package_present: Literal[False]
    executable_policy_authorized: Literal[False]
    manifest_generation_authorized: Literal[False]
    manifest_replay_executed: Literal[False]
    real_dq_authorized: Literal[False]
    qc_backtest_authorized: Literal[False]
    qc_project_mutation_authorized: Literal[False]
    provider_query_authorized: Literal[False]
    raw_option_payload_download_or_export_allowed: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]
    investment_interpretation_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class ExactSignalImplementationPolicyDraft(_CanonicalModel):
    schema_version: Literal["qqq_options_exact_signal_implementation_policy_draft.v1"]
    draft_id: Literal["qc_qqq_options_exact_signal_implementation_policy_draft_v1"]
    draft_version: Literal["1.0.0-draft.1"]
    status: Literal["OWNER_REVIEW_DRAFT_NON_EXECUTABLE"]
    task_id: str
    scope: Literal["NON_EXECUTABLE_DATA_RESEARCH"]
    authority_bindings: tuple[AuthorityBinding, ...]
    research_window: ResearchWindow
    source_candidate: SourceCandidate
    signal_mapping: SignalMapping
    selection_proposal: SelectionProposal
    execution_proposal: ExecutionProposal
    accounting_proposal: AccountingProposal
    lifecycle_proposal: LifecycleProposal
    result_admission: ResultAdmission
    slot_proposals: tuple[SlotProposal, ...]
    safety: Safety
    terminal: Literal["OWNER_EXACT_FREEZE_AND_SIGNAL_PACKAGE_REQUIRED_NO_BACKTEST"]

    @model_validator(mode="after")
    def validate_exact_draft(self) -> Self:
        if self.task_id != _TASK_ID:
            raise ValueError("task identity drifted")
        if tuple(item.path for item in self.authority_bindings) != _AUTHORITY_PATHS:
            raise ValueError("authority path order drifted")
        if tuple(item.role for item in self.authority_bindings) != _AUTHORITY_ROLES:
            raise ValueError("authority role order drifted")
        observed_slots = tuple(
            (item.slot_id, item.canonical_group) for item in self.slot_proposals
        )
        if observed_slots != _EXPECTED_SLOT_GROUPS:
            raise ValueError("37-slot successor inventory or group mapping drifted")
        if len({item.slot_id for item in self.slot_proposals}) != 37:
            raise ValueError("slot inventory must contain exactly 37 unique rows")
        for item in self.slot_proposals:
            expected_action = (
                "PROPOSE_G5_NOT_APPLICABLE"
                if item.slot_id in _G5_SLOT_IDS
                else "PROPOSE_G2"
            )
            if item.proposal_action != expected_action:
                raise ValueError(f"slot action drifted at {item.slot_id}")
            if item.proposal_ref != _EXPECTED_SLOT_REFS[item.slot_id]:
                raise ValueError(f"slot proposal reference drifted at {item.slot_id}")
        return self


@dataclass(frozen=True)
class ExactSignalImplementationPolicyDraftLoadResult:
    draft: ExactSignalImplementationPolicyDraft
    path: Path
    file_sha256: str
    canonical_sha256: str
    terminal: Literal["OWNER_EXACT_FREEZE_AND_SIGNAL_PACKAGE_REQUIRED_NO_BACKTEST"]


def _read_yaml(path: Path) -> dict[str, object]:
    payload = load_strict_yaml_text(path.read_text(encoding="utf-8"), label=str(path))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a mapping")
    return payload


def _require_blocked_predecessors(*, paths_by_role: dict[str, Path]) -> None:
    first_layer = _read_yaml(paths_by_role["SOURCE_SIGNAL_SEMANTIC_CANDIDATE"])
    if first_layer.get("policy_id") != "first_layer_composer_v2":
        raise ValueError("first-layer source policy identity drifted")
    if first_layer.get("status") != "pilot_baseline":
        raise ValueError("first-layer source policy status drifted")
    output_contract = first_layer.get("output_contract")
    if not isinstance(output_contract, dict) or "trend_state" not in output_contract.get(
        "allowed_columns", []
    ):
        raise ValueError("first-layer trend_state output contract missing")
    source_safety = first_layer.get("safety_boundary")
    if not isinstance(source_safety, dict) or source_safety.get("research_only") is not True:
        raise ValueError("first-layer research-only boundary drifted")

    signal = _read_yaml(paths_by_role["SIGNAL_PACKAGE_MECHANICS"])
    if signal.get("etf_signal_mapping_status") != "UNKNOWN_REQUIRES_OWNER_REVIEW":
        raise ValueError("2483 signal mapping predecessor status drifted")
    if signal.get("etf_signal_mapping_allowed") is not False:
        raise ValueError("2483 signal mapping predecessor unexpectedly activated")

    for role, authorization_key in (
        ("SELECTION_MECHANICS", "selection_authorized"),
        ("EXECUTION_MECHANICS", "execution_authorized"),
        ("ACCOUNTING_MECHANICS", "accounting_authorized"),
        ("LIFECYCLE_MECHANICS", "lifecycle_authorized"),
    ):
        payload = _read_yaml(paths_by_role[role])
        if payload.get("status") != "OWNER_REVIEW_REQUIRED_BASELINE":
            raise ValueError(f"{role} predecessor status drifted")
        if payload.get(authorization_key) is not False:
            raise ValueError(f"{role} predecessor unexpectedly activated")

    daily = _read_yaml(paths_by_role["DAILY_PRIMARY_CONTRACT"])
    if daily.get("backtest_execution_authorized") is not False:
        raise ValueError("daily primary backtest unexpectedly activated")

    slot_catalog = _read_yaml(paths_by_role["OWNER_SLOT_CATALOG_V2"])
    slot_safety = slot_catalog.get("safety")
    if not isinstance(slot_safety, dict) or slot_safety.get(
        "executable_policy_authorized"
    ) is not False:
        raise ValueError("owner slot catalog unexpectedly executable")

    evidence = json.loads(
        paths_by_role["QC_CHAIN_COVERAGE_EVIDENCE"].read_text(encoding="utf-8")
    )
    expected_evidence = {
        "technical_validation_state": "PASS",
        "expected_session_count": 1202,
        "observed_session_count": 1202,
        "unresolved_session_count": 0,
        "strategy_engine_status": "NOT_IN_SCOPE_ZERO_ORDER_VALIDATION",
        "orders": 0,
        "fills": 0,
        "portfolio_invested": False,
        "raw_option_rows_exported": False,
    }
    for key, expected in expected_evidence.items():
        if evidence.get(key) != expected:
            raise ValueError(f"QC chain evidence drifted at {key}")

    boundary = _read_yaml(paths_by_role["FROZEN_SIGNAL_RETEST_BOUNDARY"])
    if boundary.get("terminal") != "OWNER_EXACT_POLICY_FREEZE_REQUIRED_NO_BACKTEST":
        raise ValueError("2542H terminal drifted")
    boundary_safety = boundary.get("safety")
    if not isinstance(boundary_safety, dict):
        raise ValueError("2542H safety boundary missing")
    if boundary_safety.get("qc_backtest_authorized") is not False:
        raise ValueError("2542H unexpectedly authorized QC backtest")


def load_exact_signal_implementation_policy_draft(
    *,
    path: Path = DEFAULT_EXACT_SIGNAL_IMPLEMENTATION_POLICY_DRAFT_PATH,
    project_root: Path = PROJECT_ROOT,
) -> ExactSignalImplementationPolicyDraftLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="policy_draft")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        draft = ExactSignalImplementationPolicyDraft.model_validate(payload)
        paths_by_role: dict[str, Path] = {}
        for binding in draft.authority_bindings:
            bound = _bound_file(Path(binding.path), root=project_root, field=binding.role)
            if hashlib.sha256(bound.read_bytes()).hexdigest() != binding.file_sha256:
                raise ValueError(f"{binding.role} file SHA-256 mismatch")
            paths_by_role[binding.role] = bound
        _require_blocked_predecessors(paths_by_role=paths_by_role)
    except ExactSignalImplementationPolicyDraftError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise ExactSignalImplementationPolicyDraftError(
            "EXACT_SIGNAL_IMPLEMENTATION_POLICY_DRAFT_REJECTED", str(exc)
        ) from exc
    return ExactSignalImplementationPolicyDraftLoadResult(
        draft=draft,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=draft.canonical_sha256,
        terminal=_TERMINAL,
    )


__all__ = [
    "DEFAULT_EXACT_SIGNAL_IMPLEMENTATION_POLICY_DRAFT_PATH",
    "ExactSignalImplementationPolicyDraft",
    "ExactSignalImplementationPolicyDraftError",
    "ExactSignalImplementationPolicyDraftLoadResult",
    "load_exact_signal_implementation_policy_draft",
]
