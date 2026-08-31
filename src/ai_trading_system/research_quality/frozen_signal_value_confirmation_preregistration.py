from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_FROZEN_SIGNAL_VALUE_CONFIRMATION_PREREGISTRATION_PATH = Path(
    "config/research/frozen_signal_value_confirmation_preregistration_v1.yaml"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_STATE_MAPPING = {
    "constructive": 1.0,
    "defensive": 0.0,
    "neutral": 0.0,
    "risk_off": 0.0,
    "risk_on": 1.0,
}
_AUTHORITY_ORDER = (
    "EVIDENCE_FIRST_PORTFOLIO",
    "FIRST_LAYER_COMPOSER_V2_POLICY",
    "EXACT_SIGNAL_PACKAGE_ADMISSION",
    "EXACT_SIGNAL_EXECUTION_IDENTITY",
)
_DIAGNOSTIC_ORDER = (
    "QQQ_BUY_AND_HOLD",
    "CALENDAR_YEAR_AND_PRE_2023_POST_2022_SLICES",
)
_VERDICT_ORDER = ("INSUFFICIENT", "REJECT", "RETAIN")
_APPROVAL_AXES = (
    "SIGNAL_TO_EXPOSURE_MAPPING",
    "EXPOSURE_MATCHED_COMPARATOR",
    "FIVE_BPS_ONE_WAY_COST",
    "ZERO_EXCESS_RETURN_THRESHOLD",
    "ZERO_DRAWDOWN_REGRESSION_GUARD",
    "THREE_STATE_REDUCER_AND_STOP_RULES",
)
_EXPECTED_FUTURE_MAXIMA = {
    "manifest_replays": 1,
    "canonical_dq_runs": 1,
    "local_signal_value_confirmations": 1,
    "independent_replays": 1,
    "data_downloads": 0,
    "cache_mutations": 0,
    "quantconnect_actions": 0,
    "option_backtests": 0,
    "external_provider_actions": 0,
    "orders": 0,
    "fills": 0,
    "positions": 0,
}


class FrozenSignalValuePreregistrationError(ValueError):
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


class OwnerReviewPolicy(_PolicyModel):
    decision_state: Literal["OWNER_REVIEW_REQUIRED"]
    result_blind_draft: Literal[True]
    exact_file_and_canonical_sha_approval_required: Literal[True]
    approval_must_cover: tuple[str, ...]
    execution_activation_allowed: Literal[False]

    @model_validator(mode="after")
    def _approval_inventory(self) -> Self:
        if self.approval_must_cover != _APPROVAL_AXES:
            raise ValueError("owner-review approval axis inventory drifted")
        return self


class ResearchQuestionPolicy(_PolicyModel):
    question_id: Literal["SIGNAL_VALUE_FIRST_LAYER_COMPOSER_V2"]
    question_zh: str
    allowed_verdicts: tuple[Literal["RETAIN", "REJECT", "INSUFFICIENT"], ...]
    option_data_used: Literal[False]
    option_strategy_evaluated: Literal[False]
    new_trend_model_created: Literal[False]

    @model_validator(mode="after")
    def _verdict_inventory(self) -> Self:
        if self.allowed_verdicts != ("RETAIN", "REJECT", "INSUFFICIENT"):
            raise ValueError("allowed verdict inventory drifted")
        if not self.question_zh.strip():
            raise ValueError("research question cannot be empty")
        return self


class SignalIdentityPolicy(_PolicyModel):
    package_id: Literal["trading_2542i_operational_forecast_real_v3"]
    package_root: Literal[
        "outputs/qqq_options/signal_packages/trading_2542i_operational_forecast_real_v3"
    ]
    signal_index_sha256: str
    materialization_receipt_sha256: str
    manifest_replay_receipt_sha256: str
    producer_id: Literal["first_layer_composer_v2"]
    state_field: Literal["trend_state"]
    action_field: Literal["signal"]
    calendar: Literal["XNYS"]
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    expected_signal_sessions: Literal[1202]
    expected_return_intervals: Literal[1201]
    allowed_states: tuple[
        Literal["constructive", "defensive", "neutral", "risk_off", "risk_on"], ...
    ]
    expected_actions: tuple[Literal["FLAT", "LONG_CALL"], ...]

    @field_validator(
        "signal_index_sha256",
        "materialization_receipt_sha256",
        "manifest_replay_receipt_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _window_and_inventory(self) -> Self:
        expected_start = date(2021, 2, 22)
        expected_end = date(2025, 12, 2)
        if (
            self.requested_start,
            self.evaluated_start,
            self.requested_end,
            self.evaluated_end,
        ) != (expected_start, expected_start, expected_end, expected_end):
            raise ValueError("signal research window drifted")
        if self.allowed_states != (
            "constructive",
            "defensive",
            "neutral",
            "risk_off",
            "risk_on",
        ):
            raise ValueError("signal state inventory drifted")
        if self.expected_actions != ("FLAT", "LONG_CALL"):
            raise ValueError("normalized signal action inventory drifted")
        return self


class CandidateImplementationPolicy(_PolicyModel):
    implementation_id: Literal["FROZEN_SIGNAL_FULLY_FUNDED_QQQ_ZERO_RETURN_CASH"]
    state_to_target_qqq_weight: dict[str, float]
    residual_asset: Literal["ZERO_RETURN_CASH"]
    leverage_allowed: Literal[False]
    short_allowed: Literal[False]
    options_allowed: Literal[False]
    parameter_search_allowed: Literal[False]
    mapping_change_after_outcome_allowed: Literal[False]

    @model_validator(mode="after")
    def _mapping(self) -> Self:
        if self.state_to_target_qqq_weight != _STATE_MAPPING:
            raise ValueError("signal-to-QQQ exposure mapping drifted")
        return self


class PrimaryComparatorPolicy(_PolicyModel):
    comparator_id: Literal["EXPOSURE_MATCHED_STATIC_QQQ_ZERO_RETURN_CASH"]
    role: Literal["PRIMARY"]
    target_qqq_weight_formula: Literal[
        "LONG_EXPOSURE_RETURN_INTERVAL_COUNT_DIVIDED_BY_1201"
    ]
    formula_inputs_available_before_price_outcomes: Literal[True]
    allocation_rule: Literal[
        "BUY_ONCE_AT_FIRST_EVALUABLE_CLOSE_HOLD_CONSTANT_SHARES_LIQUIDATE_AT_FINAL_CLOSE"
    ]
    residual_asset: Literal["ZERO_RETURN_CASH"]
    outcome_dependent_fit_allowed: Literal[False]
    post_result_comparator_addition_allowed: Literal[False]


class DiagnosticPolicy(_PolicyModel):
    diagnostic_id: str
    verdict_role: Literal["CONTEXT_ONLY", "CONCENTRATION_DISCLOSURE_ONLY"]


class AccountingAndClockPolicy(_PolicyModel):
    initial_capital_usd: Literal[100000]
    price_field: Literal["ADJUSTED_CLOSE"]
    return_clock: Literal["EFFECTIVE_SESSION_CLOSE_TO_NEXT_XNYS_SESSION_CLOSE"]
    signal_lag_sessions: Literal[1]
    effective_session_rule: Literal["NEXT_VALID_US_EQUITY_SESSION"]
    candidate_rebalances_only_on_target_change: Literal[True]
    comparator_rebalances_after_initial_allocation: Literal[False]
    terminal_liquidation_required: Literal[True]
    fractional_shares_allowed: Literal[True]
    negative_cash_allowed: Literal[False]
    missing_price_imputation_allowed: Literal[False]
    fill_forward_allowed: Literal[False]


class CostModelPolicy(_PolicyModel):
    model_id: Literal["FIXED_TRADED_NOTIONAL_BPS"]
    one_way_cost_bps: float
    applies_to: tuple[Literal["INITIAL_ALLOCATION", "TARGET_CHANGE", "TERMINAL_LIQUIDATION"], ...]
    same_formula_for_candidate_and_comparator: Literal[True]
    financing_cost: Literal["ZERO_BECAUSE_BORROWING_PROHIBITED"]
    idle_cash_return: float

    @model_validator(mode="after")
    def _cost_events(self) -> Self:
        if self.one_way_cost_bps != 5.0 or self.idle_cash_return != 0.0:
            raise ValueError("cost amount or idle-cash return drifted")
        if self.applies_to != (
            "INITIAL_ALLOCATION",
            "TARGET_CHANGE",
            "TERMINAL_LIQUIDATION",
        ):
            raise ValueError("cost event inventory drifted")
        return self


class PrimaryMetricPolicy(_PolicyModel):
    metric_id: Literal["NET_TOTAL_RETURN_DIFFERENCE_PERCENTAGE_POINTS"]
    formula: Literal["CANDIDATE_NET_TOTAL_RETURN_MINUS_COMPARATOR_NET_TOTAL_RETURN"]
    retain_threshold_strictly_greater_than: float
    missing_value_status: Literal["INSUFFICIENT"]

    @model_validator(mode="after")
    def _zero_threshold(self) -> Self:
        if self.retain_threshold_strictly_greater_than != 0.0:
            raise ValueError("primary metric RETAIN threshold drifted")
        return self


class FalsificationGuardPolicy(_PolicyModel):
    guard_id: Literal["MAX_DRAWDOWN_MAGNITUDE_NON_REGRESSION"]
    formula: Literal[
        "CANDIDATE_MAX_DRAWDOWN_MAGNITUDE_MINUS_COMPARATOR_MAX_DRAWDOWN_MAGNITUDE"
    ]
    retain_threshold_less_than_or_equal_to: float
    missing_value_status: Literal["INSUFFICIENT"]

    @model_validator(mode="after")
    def _zero_guard(self) -> Self:
        if self.retain_threshold_less_than_or_equal_to != 0.0:
            raise ValueError("drawdown non-regression guard drifted")
        return self


class VerdictReducerPolicy(_PolicyModel):
    precedence: tuple[Literal["INSUFFICIENT", "REJECT", "RETAIN"], ...]
    insufficient_if_any: tuple[str, ...]
    retain_if_all: tuple[str, ...]
    reject_if_any: tuple[str, ...]
    stop_actions: dict[str, str]

    @model_validator(mode="after")
    def _reducer_contract(self) -> Self:
        if self.precedence != _VERDICT_ORDER:
            raise ValueError("verdict precedence drifted")
        if len(self.insufficient_if_any) != len(set(self.insufficient_if_any)):
            raise ValueError("insufficient reasons must be unique")
        if self.retain_if_all != (
            "PRIMARY_METRIC_STRICTLY_GREATER_THAN_ZERO",
            "MAX_DRAWDOWN_MAGNITUDE_DELTA_LESS_THAN_OR_EQUAL_TO_ZERO",
        ):
            raise ValueError("RETAIN rule drifted")
        if self.reject_if_any != (
            "PRIMARY_METRIC_LESS_THAN_OR_EQUAL_TO_ZERO",
            "MAX_DRAWDOWN_MAGNITUDE_DELTA_GREATER_THAN_ZERO",
        ):
            raise ValueError("REJECT rule drifted")
        if self.stop_actions != {
            "RETAIN": "OPEN_OWNER_REVIEW_FOR_CONDITIONAL_OPTIONS_PAIRED_COMPARISON",
            "REJECT": "CLOSE_OPTIONS_IMPLEMENTATION_P0_ROUTE_NO_PARAMETER_RESCUE",
            "INSUFFICIENT": "COLLECT_ONLY_EXPLICITLY_IDENTIFIED_PROSPECTIVE_EVIDENCE",
        }:
            raise ValueError("verdict stop-action mapping drifted")
        return self


class EvidenceRoleBoundaryPolicy(_PolicyModel):
    historical_window_role: Literal["REUSED_DEVELOPMENT_CONFIRMATION"]
    historical_result_can_be_called_pristine_oos: Literal[False]
    prospective_start_rule: Literal["FIRST_XNYS_SESSION_AFTER_FINAL_POLICY_APPROVAL"]
    prospective_sessions_may_only_confirm_frozen_rules: Literal[True]
    historical_period_deletion_allowed: Literal[False]
    post_result_threshold_change_allowed: Literal[False]


class ResultAdmissionPolicy(_PolicyModel):
    exact_policy_identity_required: Literal[True]
    exact_signal_identity_required: Literal[True]
    canonical_dq_pit_pass_required: Literal[True]
    manifest_replay_pass_required: Literal[True]
    independent_replay_pass_required: Literal[True]
    raw_market_payload_export_allowed: Literal[False]
    aggregate_result_only: Literal[True]


class FutureRunMaxima(_PolicyModel):
    manifest_replays: int
    canonical_dq_runs: int
    local_signal_value_confirmations: int
    independent_replays: int
    data_downloads: int
    cache_mutations: int
    quantconnect_actions: int
    option_backtests: int
    external_provider_actions: int
    orders: int
    fills: int
    positions: int

    @model_validator(mode="after")
    def _bounded_request(self) -> Self:
        if self.model_dump(mode="python") != _EXPECTED_FUTURE_MAXIMA:
            raise ValueError("future bounded-run maxima drifted")
        return self


class FutureRunEnvelopePolicy(_PolicyModel):
    status: Literal["SPECIFICATION_ONLY_NOT_AUTHORIZED"]
    activation_requires_new_exact_owner_authorization: Literal[True]
    proposed_maxima_after_activation: FutureRunMaxima
    terminal_artifact: Literal["frozen_signal_value_confirmation_result.v1"]


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
        if not self.semantic_facts:
            raise ValueError("authority must bind at least one semantic fact")
        paths = tuple(fact.dotted_path for fact in self.semantic_facts)
        if len(paths) != len(set(paths)):
            raise ValueError("authority semantic fact paths must be unique")
        return self


class PreregistrationSafety(_PolicyModel):
    policy_or_threshold_finally_approved: Literal[False]
    outcome_access_authorized: Literal[False]
    market_data_read_authorized: Literal[False]
    data_download_authorized: Literal[False]
    dq_authorized: Literal[False]
    local_signal_value_confirmation_authorized: Literal[False]
    backtest_authorized: Literal[False]
    quantconnect_authorized: Literal[False]
    external_provider_authorized: Literal[False]
    raw_option_export_authorized: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_allowed: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class FrozenSignalValueConfirmationPreregistrationPolicy(_PolicyModel):
    schema_version: Literal["frozen_signal_value_confirmation_preregistration_policy.v1"]
    policy_id: Literal["frozen_signal_value_confirmation_preregistration_v1"]
    policy_version: Literal["1.0.0-draft.1"]
    policy_status: Literal["DRAFT_OWNER_REVIEW_REQUIRED"]
    task_id: Literal["TRADING-2550_FROZEN_SIGNAL_VALUE_CONFIRMATION_V1"]
    owner_review: OwnerReviewPolicy
    research_question: ResearchQuestionPolicy
    signal_identity: SignalIdentityPolicy
    candidate_implementation: CandidateImplementationPolicy
    primary_comparator: PrimaryComparatorPolicy
    diagnostics: tuple[DiagnosticPolicy, ...]
    accounting_and_clock: AccountingAndClockPolicy
    cost_model: CostModelPolicy
    primary_metric: PrimaryMetricPolicy
    falsification_guard: FalsificationGuardPolicy
    verdict_reducer: VerdictReducerPolicy
    evidence_role_boundary: EvidenceRoleBoundaryPolicy
    result_admission: ResultAdmissionPolicy
    future_run_envelope: FutureRunEnvelopePolicy
    authorities: tuple[AuthorityBinding, ...]
    safety: PreregistrationSafety

    @model_validator(mode="after")
    def _exact_draft(self) -> Self:
        if tuple(item.diagnostic_id for item in self.diagnostics) != _DIAGNOSTIC_ORDER:
            raise ValueError("diagnostic inventory or order drifted")
        if tuple(item.authority_id for item in self.authorities) != _AUTHORITY_ORDER:
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
class FrozenSignalValuePreregistrationLoadResult:
    policy: FrozenSignalValueConfirmationPreregistrationPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str
    authority_observations: tuple[AuthorityObservation, ...]
    authority_set_sha256: str


class PreregistrationActionRequest(_StrictModel):
    read_market_data: bool = False
    download_data: bool = False
    run_dq: bool = False
    run_signal_value_confirmation: bool = False
    run_backtest: bool = False
    quantconnect_action: bool = False
    external_provider_action: bool = False
    export_raw_options: bool = False
    paper: bool = False
    live: bool = False
    production: bool = False
    broker: bool = False

    @property
    def requested_actions(self) -> tuple[str, ...]:
        return tuple(name for name, enabled in self.model_dump(mode="python").items() if enabled)


def assert_preregistration_action_allowed(request: PreregistrationActionRequest) -> None:
    if request.requested_actions:
        raise FrozenSignalValuePreregistrationError(
            "PREREGISTRATION_ACTION_NOT_AUTHORIZED",
            ",".join(request.requested_actions),
        )


def load_frozen_signal_value_confirmation_preregistration(
    *,
    policy_path: Path = DEFAULT_FROZEN_SIGNAL_VALUE_CONFIRMATION_PREREGISTRATION_PATH,
    project_root: Path = PROJECT_ROOT,
) -> FrozenSignalValuePreregistrationLoadResult:
    try:
        resolved_policy_path = _bound_file(policy_path, root=project_root, field="policy_path")
        raw = resolved_policy_path.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(policy_path))
        policy = FrozenSignalValueConfirmationPreregistrationPolicy.model_validate(payload)
        observations: list[AuthorityObservation] = []
        for binding in policy.authorities:
            authority_path = _bound_file(
                Path(binding.path), root=project_root, field=f"authority:{binding.authority_id}"
            )
            authority_raw = authority_path.read_bytes()
            actual_sha256 = hashlib.sha256(authority_raw).hexdigest()
            if actual_sha256 != binding.file_sha256:
                raise ValueError(f"authority file SHA-256 mismatch: {binding.authority_id}")
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
                        f"authority semantic fact mismatch: {binding.authority_id}:"
                        f"{fact.dotted_path}"
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
    except FrozenSignalValuePreregistrationError:
        raise
    except (OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise FrozenSignalValuePreregistrationError(
            "FROZEN_SIGNAL_VALUE_PREREGISTRATION_REJECTED", str(exc)
        ) from exc
    return FrozenSignalValuePreregistrationLoadResult(
        policy=policy,
        policy_path=resolved_policy_path,
        policy_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy_canonical_sha256=policy.canonical_sha256,
        authority_observations=observation_tuple,
        authority_set_sha256=authority_set_sha256,
    )


__all__ = [
    "DEFAULT_FROZEN_SIGNAL_VALUE_CONFIRMATION_PREREGISTRATION_PATH",
    "AuthorityBinding",
    "AuthorityObservation",
    "AuthoritySemanticFact",
    "FrozenSignalValueConfirmationPreregistrationPolicy",
    "FrozenSignalValuePreregistrationError",
    "FrozenSignalValuePreregistrationLoadResult",
    "PreregistrationActionRequest",
    "assert_preregistration_action_allowed",
    "load_frozen_signal_value_confirmation_preregistration",
]
