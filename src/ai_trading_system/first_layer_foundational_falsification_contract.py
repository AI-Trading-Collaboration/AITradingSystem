from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_FOUNDATIONAL_FALSIFICATION_POLICY_PATH = Path(
    "config/research/first_layer_composer_v2_foundational_falsification_preregistration_v1.yaml"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_DIAGNOSTIC_IDS = (
    "POLICY_CONSUMPTION_INVENTORY",
    "CALENDAR_YEAR_ATTRIBUTION",
    "CONTIGUOUS_EPISODE_ATTRIBUTION",
    "LEAVE_ONE_CALENDAR_YEAR_OUT",
    "PAIRED_MOVING_BLOCK_BOOTSTRAP",
    "COST_SENSITIVITY",
    "SGOV_CARRY_SENSITIVITY",
    "STATE_TRANSITION_ATTRIBUTION",
    "SELECTION_HISTORY_INVENTORY",
    "SOURCE_REVISION_DIFF",
)
_AUTHORITY_IDS = (
    "TRADING_2550_PREREGISTRATION",
    "TRADING_2550_RESULT_ADMISSION",
    "FIRST_LAYER_COMPOSER_V2",
    "FIRST_LAYER_THRESHOLD_POLICY_V2",
    "ACTION_VALUE_SCORE_POLICY_V2",
    "FIRST_LAYER_OPERATIONAL_FORECAST_SOURCE",
    "UPPER_STATE_LABEL_FEATURE_RESET_SOURCE",
)
_CONSUMPTION_STATUSES = (
    "DECLARED_AND_CONSUMED",
    "DECLARED_NOT_CONSUMED",
    "CODE_ONLY",
    "NOT_APPLICABLE",
)
_CONSUMPTION_ENTRIES = (
    (
        "threshold_selection.positive_score_quantile",
        "FIRST_LAYER_THRESHOLD_POLICY_V2",
        "DECLARED_AND_CONSUMED",
    ),
    (
        "positive_sample_floor",
        "FIRST_LAYER_THRESHOLD_POLICY_V2",
        "DECLARED_AND_CONSUMED",
    ),
    (
        "threshold_selection.negative_score_quantile",
        "FIRST_LAYER_THRESHOLD_POLICY_V2",
        "DECLARED_NOT_CONSUMED",
    ),
    (
        "threshold_selection.min_predicted_share",
        "FIRST_LAYER_THRESHOLD_POLICY_V2",
        "DECLARED_NOT_CONSUMED",
    ),
    (
        "threshold_selection.max_predicted_share",
        "FIRST_LAYER_THRESHOLD_POLICY_V2",
        "DECLARED_NOT_CONSUMED",
    ),
    (
        "score_weights.missed_upside_penalty",
        "ACTION_VALUE_SCORE_POLICY_V2",
        "DECLARED_NOT_CONSUMED",
    ),
    (
        "score_weights.net_of_cost_penalty",
        "ACTION_VALUE_SCORE_POLICY_V2",
        "DECLARED_NOT_CONSUMED",
    ),
    (
        "score_weights.tqqq_penalty",
        "ACTION_VALUE_SCORE_POLICY_V2",
        "DECLARED_NOT_CONSUMED",
    ),
    (
        "tqqq.penalty_per_weight",
        "ACTION_VALUE_SCORE_POLICY_V2",
        "DECLARED_AND_CONSUMED",
    ),
)
_STATE_MAPPING = {
    "constructive": 1.0,
    "defensive": 0.0,
    "neutral": 0.0,
    "risk_off": 0.0,
    "risk_on": 1.0,
}
_YEARS = (2021, 2022, 2023, 2024, 2025)
_BLOCK_LENGTHS = (21, 63)
_COST_GRID = (10.0, 15.0, 20.0)
_REDUCER_PRECEDENCE = ("INVALID", "FAIL", "INSUFFICIENT", "PASS")


class FoundationalFalsificationContractError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


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


def _mapping_fact(payload: object, dotted_path: str) -> object:
    current = payload
    for part in dotted_path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
            continue
        if isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
            continue
        raise ValueError(f"semantic fact path is missing: {dotted_path}")
    return current


def _normalize_semantic_value(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _normalize_semantic_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_semantic_value(item) for item in value]
    return value


class PolicyMetadata(_PolicyModel):
    rationale: str
    intended_effect: str
    validation_plan: tuple[str, ...]
    review_condition: str

    @model_validator(mode="after")
    def _non_empty(self) -> Self:
        if not self.rationale.strip() or not self.intended_effect.strip():
            raise ValueError("policy rationale and intended effect are required")
        if len(self.validation_plan) != 4 or len(set(self.validation_plan)) != 4:
            raise ValueError("validation plan inventory drifted")
        if not self.review_condition.strip():
            raise ValueError("review condition is required")
        return self


class KnownResultBoundary(_PolicyModel):
    result_visibility: Literal["PARTIAL_PREEXISTING"]
    known_before_freeze: tuple[str, ...]
    not_yet_computed: tuple[str, ...]
    historical_window_role: Literal["REUSED_DEVELOPMENT_CONFIRMATION"]
    pristine_oos_claim_allowed: Literal[False]
    post_result_parameter_rescue_allowed: Literal[False]

    @model_validator(mode="after")
    def _inventories(self) -> Self:
        if len(self.known_before_freeze) != 5 or len(set(self.known_before_freeze)) != 5:
            raise ValueError("known-result inventory drifted")
        if len(self.not_yet_computed) != 8 or len(set(self.not_yet_computed)) != 8:
            raise ValueError("result-blind diagnostic inventory drifted")
        return self


class PrimaryIdentity(_PolicyModel):
    producer_id: Literal["first_layer_composer_v2"]
    calendar: Literal["XNYS"]
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    expected_signal_sessions: Literal[1202]
    expected_return_intervals: Literal[1201]
    price_field: Literal["ADJUSTED_CLOSE"]
    return_clock: Literal["EFFECTIVE_SESSION_CLOSE_TO_NEXT_XNYS_SESSION_CLOSE"]
    signal_lag_sessions: Literal[1]
    candidate_id: Literal["FROZEN_SIGNAL_FULLY_FUNDED_QQQ_ZERO_RETURN_CASH"]
    candidate_state_to_qqq_weight: dict[str, float]
    comparator_id: Literal["EXPOSURE_MATCHED_STATIC_QQQ_ZERO_RETURN_CASH"]
    comparator_weight_formula: Literal["LONG_EXPOSURE_RETURN_INTERVAL_COUNT_DIVIDED_BY_1201"]
    primary_idle_cash_asset: Literal["ZERO_RETURN_CASH"]
    primary_one_way_cost_bps: float
    same_cost_formula_for_candidate_and_comparator: Literal[True]
    leverage_allowed: Literal[False]
    short_allowed: Literal[False]
    options_allowed: Literal[False]

    @model_validator(mode="after")
    def _identity(self) -> Self:
        start = date(2021, 2, 22)
        end = date(2025, 12, 2)
        if (
            self.requested_start,
            self.evaluated_start,
            self.requested_end,
            self.evaluated_end,
        ) != (start, start, end, end):
            raise ValueError("primary requested/evaluated window drifted")
        if self.candidate_state_to_qqq_weight != _STATE_MAPPING:
            raise ValueError("candidate state-to-exposure mapping drifted")
        if self.primary_one_way_cost_bps != 5.0:
            raise ValueError("primary one-way cost drifted")
        return self


class DiagnosticDefinition(_PolicyModel):
    diagnostic_id: str
    required: Literal[True]
    verdict_role: Literal[
        "VALIDITY_INPUT",
        "ROBUSTNESS_INPUT",
        "CONCENTRATION_DISCLOSURE",
        "UNCERTAINTY_INPUT",
        "FRAGILITY_DISCLOSURE",
        "INTERPRETATION_DISCLOSURE",
    ]


class CalendarYearContract(_PolicyModel):
    ordered_years: tuple[int, ...]
    partial_years: tuple[int, ...]
    interval_attribution: Literal["LEFT_ENDPOINT_SESSION_YEAR"]
    leave_one_year_rule: Literal["REMOVE_YEAR_INTERVALS_THEN_COMPOUND_REMAINING_IN_ORIGINAL_ORDER"]
    retraining_allowed: Literal[False]
    threshold_refit_allowed: Literal[False]

    @model_validator(mode="after")
    def _years(self) -> Self:
        if self.ordered_years != _YEARS or self.partial_years != (2021, 2025):
            raise ValueError("calendar-year diagnostic inventory drifted")
        return self


class BootstrapContract(_PolicyModel):
    method: Literal["PAIRED_CIRCULAR_MOVING_BLOCK_BOOTSTRAP"]
    input_series: Literal["SAME_SESSION_CANDIDATE_MINUS_COMPARATOR_DAILY_NET_RETURN"]
    block_lengths_sessions: tuple[int, ...]
    random_seed: int
    replicates_per_block_length: int
    interval_percentiles: tuple[float, ...]
    report_nonpositive_probability: Literal[True]
    parameter_search_allowed: Literal[False]

    @model_validator(mode="after")
    def _freeze(self) -> Self:
        if self.block_lengths_sessions != _BLOCK_LENGTHS:
            raise ValueError("bootstrap block lengths drifted")
        if self.random_seed != 2555 or self.replicates_per_block_length != 10000:
            raise ValueError("bootstrap seed or replicate budget drifted")
        if self.interval_percentiles != (2.5, 50.0, 97.5):
            raise ValueError("bootstrap percentile inventory drifted")
        return self


class SensitivityContract(_PolicyModel):
    primary_one_way_cost_bps: float
    diagnostic_one_way_cost_bps: tuple[float, ...]
    cost_application: Literal["FULL_RECOMPUTATION_BOTH_CANDIDATE_AND_COMPARATOR"]
    break_even_output: Literal["DISCRETE_BRACKET_ONLY"]
    cash_carry_asset: Literal["SGOV"]
    cash_carry_role: Literal["DIAGNOSTIC_ONLY"]
    primary_cash_replacement_allowed: Literal[False]

    @model_validator(mode="after")
    def _freeze(self) -> Self:
        if self.primary_one_way_cost_bps != 5.0:
            raise ValueError("sensitivity primary cost drifted")
        if self.diagnostic_one_way_cost_bps != _COST_GRID:
            raise ValueError("diagnostic cost grid drifted")
        return self


class StateTransitionContract(_PolicyModel):
    ordered_states: tuple[str, ...]
    exposure_actions: tuple[str, ...]
    forward_horizons_sessions: tuple[int, ...]
    immature_tail_status: Literal["MISSING"]
    confidence_probability_claim_allowed: Literal[False]

    @model_validator(mode="after")
    def _freeze(self) -> Self:
        if self.ordered_states != (
            "risk_off",
            "defensive",
            "neutral",
            "constructive",
            "risk_on",
        ):
            raise ValueError("state inventory drifted")
        if self.exposure_actions != ("FLAT", "LONG_CALL"):
            raise ValueError("exposure action inventory drifted")
        if self.forward_horizons_sessions != (1, 5, 20):
            raise ValueError("transition horizon inventory drifted")
        return self


ConsumptionStatus = Literal[
    "DECLARED_AND_CONSUMED",
    "DECLARED_NOT_CONSUMED",
    "CODE_ONLY",
    "NOT_APPLICABLE",
]


class PolicyConsumptionEntry(_PolicyModel):
    field_id: str
    authority_id: str
    expected_status: ConsumptionStatus


class PolicyConsumptionInventory(_PolicyModel):
    allowed_statuses: tuple[str, ...]
    entries: tuple[PolicyConsumptionEntry, ...]
    old_model_wiring_change_allowed: Literal[False]

    @model_validator(mode="after")
    def _freeze(self) -> Self:
        if self.allowed_statuses != _CONSUMPTION_STATUSES:
            raise ValueError("policy-consumption status inventory drifted")
        observed = tuple(
            (item.field_id, item.authority_id, item.expected_status) for item in self.entries
        )
        if observed != _CONSUMPTION_ENTRIES:
            raise ValueError("policy-consumption field inventory drifted")
        return self


class ReducerPolicy(_PolicyModel):
    precedence: tuple[Literal["INVALID", "FAIL", "INSUFFICIENT", "PASS"], ...]
    invalid_if_any: tuple[str, ...]
    fail_if_any: tuple[str, ...]
    insufficient_if_any: tuple[str, ...]
    pass_if_all: tuple[str, ...]
    conclusion_by_status: dict[str, str]
    qqq_options_wave_b_by_status: dict[str, str]
    qqq_options_wave_c_by_status: dict[str, str]
    production_allowed_by_status: dict[str, bool]

    @model_validator(mode="after")
    def _freeze(self) -> Self:
        if self.precedence != _REDUCER_PRECEDENCE:
            raise ValueError("foundational reducer precedence drifted")
        inventories = (
            self.invalid_if_any,
            self.fail_if_any,
            self.insufficient_if_any,
            self.pass_if_all,
        )
        if any(not rows or len(rows) != len(set(rows)) for rows in inventories):
            raise ValueError("reducer reason inventories must be non-empty and unique")
        statuses = set(_REDUCER_PRECEDENCE)
        for mapping in (
            self.conclusion_by_status,
            self.qqq_options_wave_b_by_status,
            self.qqq_options_wave_c_by_status,
            self.production_allowed_by_status,
        ):
            if set(mapping) != statuses:
                raise ValueError("reducer status mapping drifted")
        if any(self.production_allowed_by_status.values()):
            raise ValueError("no foundational verdict may authorize production")
        if set(self.qqq_options_wave_c_by_status.values()) != {"NOT_AUTHORIZED"}:
            raise ValueError("Wave C must remain unauthorized")
        if self.qqq_options_wave_b_by_status != {
            "INVALID": "HOLD",
            "FAIL": "STOP",
            "INSUFFICIENT": "HOLD",
            "PASS": "OWNER_REVIEW_REQUIRED",
        }:
            raise ValueError("Wave B status mapping drifted")
        return self


class SuccessorRunMaxima(_PolicyModel):
    canonical_dq_runs: Literal[1]
    manifest_replays: Literal[1]
    local_foundational_runs: Literal[1]
    independent_replays: Literal[1]
    data_downloads: Literal[0]
    cache_mutations: Literal[0]
    quantconnect_actions: Literal[0]
    option_backtests: Literal[0]
    provider_actions: Literal[0]
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]


class SuccessorRunEnvelope(_PolicyModel):
    status: Literal["SPECIFICATION_ONLY_NOT_AUTHORIZED_BY_F0"]
    requires_new_canonical_task: Literal[True]
    requires_exact_f0_main_commit: Literal[True]
    proposed_maxima: SuccessorRunMaxima
    terminal_artifact: Literal["first_layer_composer_v2_foundational_falsification_result.v1"]


class AuthoritySemanticFact(_PolicyModel):
    dotted_path: str
    expected_json: str

    @field_validator("dotted_path")
    @classmethod
    def _fact_path(cls, value: str) -> str:
        if not value or value.startswith(".") or value.endswith(".") or ".." in value:
            raise ValueError("dotted_path must be stable")
        return value

    @field_validator("expected_json")
    @classmethod
    def _expected_json(cls, value: str) -> str:
        parsed = json.loads(value)
        canonical = json.dumps(parsed, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if value != canonical:
            raise ValueError("expected_json must use compact canonical JSON")
        return value


class AuthorityBinding(_PolicyModel):
    authority_id: str
    path: str
    file_sha256: str
    semantic_facts: tuple[AuthoritySemanticFact, ...]

    @field_validator("path")
    @classmethod
    def _path(cls, value: str) -> str:
        return _relative_path(value, "path")

    @field_validator("file_sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, "file_sha256")

    @model_validator(mode="after")
    def _facts(self) -> Self:
        paths = tuple(fact.dotted_path for fact in self.semantic_facts)
        if len(paths) != len(set(paths)):
            raise ValueError("authority semantic fact paths must be unique")
        if self.path.endswith((".yaml", ".yml")) and not self.semantic_facts:
            raise ValueError("YAML authority must bind at least one semantic fact")
        if not self.path.endswith((".yaml", ".yml")) and self.semantic_facts:
            raise ValueError("non-YAML source binding cannot declare YAML semantic facts")
        return self


class ContractSafety(_PolicyModel):
    result_blind_contract_only: Literal[True]
    empirical_diagnostic_access_authorized: Literal[False]
    market_data_read_authorized: Literal[False]
    dq_authorized: Literal[False]
    backtest_authorized: Literal[False]
    parameter_change_allowed: Literal[False]
    threshold_change_allowed: Literal[False]
    comparator_change_allowed: Literal[False]
    quantconnect_authorized: Literal[False]
    provider_authorized: Literal[False]
    options_wave_b_authorized: Literal[False]
    options_wave_c_authorized: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_allowed: Literal[False]
    broker_allowed: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class FoundationalFalsificationPolicy(_PolicyModel):
    schema_version: Literal["first_layer_composer_v2_foundational_falsification_preregistration.v1"]
    policy_id: Literal["first_layer_composer_v2_foundational_falsification_preregistration_v1"]
    policy_version: Literal["1.0.0"]
    policy_status: Literal["OWNER_DIRECTED_RESULT_BLIND_CONTRACT"]
    task_id: Literal["TRADING-2555_FIRST_LAYER_COMPOSER_V2_FOUNDATIONAL_FALSIFICATION_CONTRACT_V1"]
    owner: Literal["project_owner"]
    approval_ref: Literal[
        "owner_instruction:TRADING-2555:2026-09-03:continue_foundational_validation"
    ]
    policy_metadata: PolicyMetadata
    known_result_boundary: KnownResultBoundary
    primary_identity: PrimaryIdentity
    diagnostic_inventory: tuple[DiagnosticDefinition, ...]
    calendar_year_contract: CalendarYearContract
    bootstrap_contract: BootstrapContract
    sensitivity_contract: SensitivityContract
    state_transition_contract: StateTransitionContract
    policy_consumption_inventory: PolicyConsumptionInventory
    reducer: ReducerPolicy
    successor_run_envelope: SuccessorRunEnvelope
    authority_bindings: tuple[AuthorityBinding, ...]
    safety: ContractSafety

    @model_validator(mode="after")
    def _exact_contract(self) -> Self:
        if tuple(row.diagnostic_id for row in self.diagnostic_inventory) != _DIAGNOSTIC_IDS:
            raise ValueError("diagnostic inventory or order drifted")
        if tuple(row.authority_id for row in self.authority_bindings) != _AUTHORITY_IDS:
            raise ValueError("authority inventory or order drifted")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


class AuthorityObservation(_StrictModel):
    authority_id: str
    path: str
    file_sha256: str
    semantic_fact_count: int
    identity_verified: Literal[True]
    semantics_verified: Literal[True]

    @field_validator("file_sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, "file_sha256")


@dataclass(frozen=True)
class FoundationalFalsificationLoadResult:
    policy: FoundationalFalsificationPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str
    authority_observations: tuple[AuthorityObservation, ...]
    authority_set_sha256: str


class ContractActionRequest(_StrictModel):
    read_empirical_diagnostics: bool = False
    read_market_data: bool = False
    run_dq: bool = False
    run_backtest: bool = False
    change_parameter: bool = False
    change_threshold: bool = False
    change_comparator: bool = False
    quantconnect_action: bool = False
    provider_action: bool = False
    options_wave_b: bool = False
    options_wave_c: bool = False
    paper: bool = False
    live: bool = False
    production: bool = False
    broker: bool = False

    @property
    def requested_actions(self) -> tuple[str, ...]:
        return tuple(name for name, enabled in self.model_dump(mode="python").items() if enabled)


def assert_contract_action_allowed(request: ContractActionRequest) -> None:
    if request.requested_actions:
        raise FoundationalFalsificationContractError(
            "FOUNDATIONAL_F0_ACTION_NOT_AUTHORIZED",
            ",".join(request.requested_actions),
        )


class BootstrapInterval(_StrictModel):
    block_length_sessions: Literal[21, 63]
    percentile_2_5: float
    percentile_50: float
    percentile_97_5: float
    probability_excess_less_than_or_equal_to_zero: float

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if not self.percentile_2_5 <= self.percentile_50 <= self.percentile_97_5:
            raise ValueError("bootstrap percentiles must be ordered")
        if not 0.0 <= self.probability_excess_less_than_or_equal_to_zero <= 1.0:
            raise ValueError("bootstrap nonpositive probability must be in [0, 1]")
        return self


class LeaveOneYearOutResult(_StrictModel):
    calendar_year: Literal[2021, 2022, 2023, 2024, 2025]
    paired_excess_percentage_points: float


class FoundationalDiagnosticSummary(_StrictModel):
    identity_issues: tuple[str, ...] = ()
    completed_diagnostic_ids: tuple[str, ...] = ()
    policy_consumption_matches_contract: bool | None = None
    source_revision_status: Literal["MATCHED", "DIFF_REPORTED", "INVALID", "MISSING"] = "MISSING"
    primary_paired_excess_percentage_points: float | None = None
    bootstrap_intervals: tuple[BootstrapInterval, ...] = ()
    leave_one_calendar_year_out: tuple[LeaveOneYearOutResult, ...] = ()

    @model_validator(mode="after")
    def _shape(self) -> Self:
        if len(self.identity_issues) != len(set(self.identity_issues)):
            raise ValueError("identity issues must be unique")
        if any(item not in _DIAGNOSTIC_IDS for item in self.completed_diagnostic_ids):
            raise ValueError("completed diagnostic inventory contains an unknown id")
        if len(self.completed_diagnostic_ids) != len(set(self.completed_diagnostic_ids)):
            raise ValueError("completed diagnostic ids must be unique")
        blocks = tuple(item.block_length_sessions for item in self.bootstrap_intervals)
        if len(blocks) != len(set(blocks)):
            raise ValueError("bootstrap block lengths must be unique")
        years = tuple(item.calendar_year for item in self.leave_one_calendar_year_out)
        if len(years) != len(set(years)):
            raise ValueError("leave-one-year-out years must be unique")
        return self


class FoundationalDecision(_StrictModel):
    status: Literal["INVALID", "FAIL", "INSUFFICIENT", "PASS"]
    conclusion: str
    qqq_options_wave_b: str
    qqq_options_wave_c: Literal["NOT_AUTHORIZED"]
    production_allowed: Literal[False]
    reason_codes: tuple[str, ...]


def reduce_foundational_falsification_status(
    summary: FoundationalDiagnosticSummary,
    *,
    policy: FoundationalFalsificationPolicy,
) -> FoundationalDecision:
    invalid_reasons = list(summary.identity_issues)
    if summary.policy_consumption_matches_contract is False:
        invalid_reasons.append("POLICY_CONSUMPTION_MISMATCH")
    if summary.source_revision_status == "INVALID":
        invalid_reasons.append("SOURCE_REVISION_DIFF_INVALID")
    if invalid_reasons:
        return _decision("INVALID", tuple(invalid_reasons), policy)

    fail_reasons: list[str] = []
    if (
        summary.primary_paired_excess_percentage_points is not None
        and summary.primary_paired_excess_percentage_points <= 0.0
    ):
        fail_reasons.append("PRIMARY_5_BPS_PAIRED_EXCESS_LESS_THAN_OR_EQUAL_TO_ZERO")
    if any(item.percentile_97_5 <= 0.0 for item in summary.bootstrap_intervals):
        fail_reasons.append("ANY_BOOTSTRAP_97_5_PERCENTILE_LESS_THAN_OR_EQUAL_TO_ZERO")
    if fail_reasons:
        return _decision("FAIL", tuple(fail_reasons), policy)

    insufficient_reasons: list[str] = []
    if set(summary.completed_diagnostic_ids) != set(_DIAGNOSTIC_IDS):
        insufficient_reasons.append("REQUIRED_DIAGNOSTIC_INCOMPLETE")
    if summary.policy_consumption_matches_contract is None:
        insufficient_reasons.append("POLICY_CONSUMPTION_INCOMPLETE")
    if summary.source_revision_status == "MISSING":
        insufficient_reasons.append("SOURCE_REVISION_DIFF_INCOMPLETE")
    if summary.primary_paired_excess_percentage_points is None:
        insufficient_reasons.append("PRIMARY_5_BPS_PAIRED_EXCESS_MISSING")
    if set(item.block_length_sessions for item in summary.bootstrap_intervals) != set(
        _BLOCK_LENGTHS
    ):
        insufficient_reasons.append("PAIRED_MOVING_BLOCK_BOOTSTRAP_INCOMPLETE")
    if set(item.calendar_year for item in summary.leave_one_calendar_year_out) != set(_YEARS):
        insufficient_reasons.append("LEAVE_ONE_CALENDAR_YEAR_OUT_INCOMPLETE")
    if any(item.percentile_2_5 <= 0.0 for item in summary.bootstrap_intervals):
        insufficient_reasons.append("ANY_BOOTSTRAP_2_5_PERCENTILE_LESS_THAN_OR_EQUAL_TO_ZERO")
    if any(
        item.paired_excess_percentage_points <= 0.0 for item in summary.leave_one_calendar_year_out
    ):
        insufficient_reasons.append(
            "ANY_LEAVE_ONE_CALENDAR_YEAR_OUT_EXCESS_LESS_THAN_OR_EQUAL_TO_ZERO"
        )
    if insufficient_reasons:
        return _decision("INSUFFICIENT", tuple(dict.fromkeys(insufficient_reasons)), policy)
    return _decision("PASS", ("ALL_FOUNDATIONAL_GATES_PASS",), policy)


def _decision(
    status: Literal["INVALID", "FAIL", "INSUFFICIENT", "PASS"],
    reason_codes: tuple[str, ...],
    policy: FoundationalFalsificationPolicy,
) -> FoundationalDecision:
    return FoundationalDecision(
        status=status,
        conclusion=policy.reducer.conclusion_by_status[status],
        qqq_options_wave_b=policy.reducer.qqq_options_wave_b_by_status[status],
        qqq_options_wave_c="NOT_AUTHORIZED",
        production_allowed=False,
        reason_codes=reason_codes,
    )


def load_foundational_falsification_contract(
    *,
    policy_path: Path = DEFAULT_FOUNDATIONAL_FALSIFICATION_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> FoundationalFalsificationLoadResult:
    try:
        resolved_policy_path = _bound_file(policy_path, root=project_root, field="policy_path")
        raw = resolved_policy_path.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(policy_path))
        policy = FoundationalFalsificationPolicy.model_validate(payload)
        observations: list[AuthorityObservation] = []
        for binding in policy.authority_bindings:
            authority_path = _bound_file(
                Path(binding.path),
                root=project_root,
                field=f"authority:{binding.authority_id}",
            )
            authority_raw = authority_path.read_bytes()
            actual_sha256 = hashlib.sha256(authority_raw).hexdigest()
            if actual_sha256 != binding.file_sha256:
                raise ValueError(f"authority file SHA-256 mismatch: {binding.authority_id}")
            if binding.semantic_facts:
                authority_payload = load_strict_yaml_text(
                    authority_raw.decode("utf-8"), label=binding.path
                )
                for fact in binding.semantic_facts:
                    actual = _normalize_semantic_value(
                        _mapping_fact(authority_payload, fact.dotted_path)
                    )
                    expected = json.loads(fact.expected_json)
                    if actual != expected:
                        raise ValueError(
                            "authority semantic fact mismatch: "
                            f"{binding.authority_id}:{fact.dotted_path}"
                        )
            observations.append(
                AuthorityObservation(
                    authority_id=binding.authority_id,
                    path=binding.path,
                    file_sha256=actual_sha256,
                    semantic_fact_count=len(binding.semantic_facts),
                    identity_verified=True,
                    semantics_verified=True,
                )
            )
        observation_tuple = tuple(observations)
        authority_set_sha256 = hashlib.sha256(
            _canonical_json_bytes([item.model_dump(mode="json") for item in observation_tuple])
        ).hexdigest()
    except FoundationalFalsificationContractError:
        raise
    except (OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise FoundationalFalsificationContractError(
            "FOUNDATIONAL_FALSIFICATION_CONTRACT_REJECTED", str(exc)
        ) from exc
    return FoundationalFalsificationLoadResult(
        policy=policy,
        policy_path=resolved_policy_path,
        policy_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy_canonical_sha256=policy.canonical_sha256,
        authority_observations=observation_tuple,
        authority_set_sha256=authority_set_sha256,
    )


__all__ = [
    "DEFAULT_FOUNDATIONAL_FALSIFICATION_POLICY_PATH",
    "AuthorityBinding",
    "AuthorityObservation",
    "BootstrapInterval",
    "ContractActionRequest",
    "FoundationalDecision",
    "FoundationalDiagnosticSummary",
    "FoundationalFalsificationContractError",
    "FoundationalFalsificationLoadResult",
    "FoundationalFalsificationPolicy",
    "LeaveOneYearOutResult",
    "PolicyConsumptionEntry",
    "assert_contract_action_allowed",
    "load_foundational_falsification_contract",
    "reduce_foundational_falsification_status",
]
