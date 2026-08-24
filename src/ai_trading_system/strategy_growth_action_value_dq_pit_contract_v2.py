from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.strategy_growth_action_value_dq_pit_contract import (
    ContractTerminal,
    SessionEvaluation,
    SessionTerminalStatus,
    StrategyGrowthActionValueDqPitContractLoadResult,
    aggregate_window,
    load_strategy_growth_action_value_dq_pit_contract,
    session_inventory_lf_sha256,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_DQ_PIT_CONTRACT_V2_PATH = Path(
    "config/research/strategy_growth_action_value_canonical_dq_pit_contract_v2.yaml"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SESSION_INVENTORY_SHA256 = "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
_SERIAL_FIELDS = (
    "QUOTE_AGE_CLOCK_AND_TIMESTAMP_DIRECTION",
    "RELATIVE_SPREAD_DENOMINATOR_AND_ZERO_DENOMINATOR",
    "CONTRACT_TO_SESSION_AGGREGATION",
    "MISSING_AND_UNKNOWN_TERMINAL_MAPPING",
    "EXACT_SOURCE_DATE_AND_PIT_RULE",
    "GLOBAL_DQ_TERMINAL_ORDER",
)
_IDENTITY_FIELDS = (
    "provider",
    "engine",
    "exchange_calendar",
    "symbol_mapping",
    "normalization",
    "repository_code_sha",
    "source_evidence",
    "aggregate_manifest",
)
_ARTIFACT_TYPES = (
    "authority_receipt",
    "run_manifest",
    "expected_session_inventory",
    "expected_contributor_manifest_by_session",
    "contract_level_derived_check_results",
    "session_level_terminals",
    "window_global_terminal",
    "stop_abort_receipt",
    "deterministic_replay_report",
    "artifact_checksum_catalog",
)


class StrategyGrowthActionValueDqPitContractV2Error(ValueError):
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


class PredecessorBinding(_StrictModel):
    path: Literal["config/research/strategy_growth_action_value_canonical_dq_pit_contract_v1.yaml"]
    file_sha256: Literal["39650c2aba7c4b33804de01f34c52e22f9992d7ded673cc6db506112d72e7d9c"]
    canonical_sha256: Literal["c40296090fc5f84d5142e3b3d24f38c04a5fdc02223a3a329ce3d8e74b22aca1"]
    disposition: Literal["REJECTED_FOR_EXECUTABLE_AUTHORITY_RETAINED_IMMUTABLE"]


class ReviewEvidence(_StrictModel):
    reviewed_repository_commit: Literal["1ca8ccf95c2a93a1b50164345d3e101a59b50838"]
    conversation_url: Literal["https://chatgpt.com/c/6a8ae448-a5b4-83e8-8d88-d7e6b22e0fc2"]
    visible_product_label: Literal["ChatGPT Pro"]
    visible_model_label: Literal["GPT-5.6 Pro"]
    backend_route_attestation: Literal["CANNOT_VERIFY_EXACT_BACKEND_ROUTE"]
    overall_disposition: Literal["REQUEST_NEW_VERSION_BEFORE_FREEZE"]


class ReviewState(_StrictModel):
    project_owner_continuation_instruction: Literal[True]
    owner_exact_approval: Literal["NOT_PROVIDED"]
    second_independent_review: Literal["NOT_PERFORMED"]
    executable_authority: Literal[False]
    freeze_allowed: Literal[False]


class ScopeBinding(_StrictModel):
    hypothesis_id: Literal["BASELINE_BOUNDED_QQQ_GROWTH_OVERLAY_NON_BETA_ACTION_VALUE_V1"]
    selected_data_lane: Literal["QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE"]
    stage: Literal["DATA_RESEARCH"]
    primary_window_start: Literal["2021-02-22"]
    primary_window_end: Literal["2025-12-02"]
    expected_session_count: Literal[1202]
    calendar_id: Literal["QQQ_EXCHANGE_SESSIONS"]
    session_inventory_lf_sha256: Literal[
        "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
    ]


class NumericFieldPolicy(_StrictModel):
    value: Decimal
    unit: Literal["seconds", "ratio", "contracts"]
    direction: Literal["LESS_THAN_OR_EQUAL", "GREATER_THAN_OR_EQUAL"]
    policy_source: str
    rationale: str
    intended_effect: str
    known_risk: str
    stage: Literal["DATA_RESEARCH"]
    execution_liquidity_authority: Literal[False]
    primary_window_result_visible_when_selected: Literal[False]
    review_condition: Literal[
        "REVIEW_AFTER_ONE_PRIMARY_WINDOW_EVALUATION_OR_PROVIDER_SEMANTIC_CHANGE"
    ]
    expiry_condition: Literal["EXPIRES_BEFORE_REUSE_OUTSIDE_BOUND_PRIMARY_WINDOW_OR_STAGE"]
    review_disposition: Literal["INSUFFICIENT_EVIDENCE_TO_APPROVE"]

    @field_validator("policy_source", "rationale", "intended_effect", "known_risk")
    @classmethod
    def validate_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("numeric policy explanation must not be empty")
        return value


class NumericPolicyBundle(_StrictModel):
    state: Literal["NON_EXECUTABLE_PILOT_POLICY_PENDING_REVIEW"]
    executable: Literal[False]
    unknown_can_pass: Literal[False]
    numeric_check_order: tuple[str, ...]
    numeric_failure_collection: Literal["COLLECT_ALL_NUMERIC_FAILURES"]
    max_quote_age_seconds: NumericFieldPolicy
    max_relative_spread: NumericFieldPolicy
    min_open_interest: NumericFieldPolicy
    min_volume: NumericFieldPolicy

    @model_validator(mode="after")
    def validate_bundle(self) -> Self:
        if self.numeric_check_order != (
            "max_quote_age_seconds",
            "max_relative_spread",
            "min_open_interest",
            "min_volume",
        ):
            raise ValueError("numeric check order drifted")
        expected = (
            (
                self.max_quote_age_seconds,
                Decimal(120),
                "seconds",
                "LESS_THAN_OR_EQUAL",
            ),
            (
                self.max_relative_spread,
                Decimal("0.20"),
                "ratio",
                "LESS_THAN_OR_EQUAL",
            ),
            (
                self.min_open_interest,
                Decimal(10),
                "contracts",
                "GREATER_THAN_OR_EQUAL",
            ),
            (
                self.min_volume,
                Decimal(1),
                "contracts",
                "GREATER_THAN_OR_EQUAL",
            ),
        )
        for item, value, unit, direction in expected:
            if item.value != value or item.unit != unit or item.direction != direction:
                raise ValueError("numeric pilot policy drifted")
        return self


class TimestampContract(_StrictModel):
    timezone: Literal["UTC"]
    quote_age_formula: Literal["DECISION_AS_OF_UTC_MINUS_QUOTE_END_UTC_TOTAL_SECONDS"]
    missing_quote_timestamp_outcome: Literal["UNKNOWN"]
    missing_decision_timestamp_outcome: Literal["INVALID"]
    malformed_naive_or_non_utc_timestamp_outcome: Literal["INVALID"]
    future_quote_outcome: Literal["INVALID"]
    quote_end_source_date_rule: Literal["QUOTE_END_UTC_CALENDAR_DATE_EQUALS_QUOTE_SOURCE_DATE"]
    rounding_before_comparison_allowed: Literal[False]
    absolute_value_allowed: Literal[False]


class SpreadContract(_StrictModel):
    formula: Literal["ASK_MINUS_BID_DIVIDED_BY_BID_ASK_MIDPOINT"]
    denominator: Literal["BID_PLUS_ASK_DIVIDED_BY_TWO"]
    both_sides_missing_outcome: Literal["UNKNOWN"]
    single_sided_provider_quote_outcome: Literal["INVALID"]
    non_decimal_nonfinite_crossed_or_zero_denominator_outcome: Literal["INVALID"]
    alternate_denominator_allowed: Literal[False]


class PitContract(_StrictModel):
    source_date_rule: Literal["EXACT_TARGET_EXCHANGE_SESSION_DATE"]
    cross_date_fallback_allowed: Literal[False]
    quote_volume_source_rule: Literal["SAME_TARGET_SESSION"]
    open_interest_source_rule: Literal["EXACT_PRIOR_SESSION_DERIVED_FROM_FROZEN_SESSION_INVENTORY"]
    quote_available_at_rule: Literal["QUOTE_AVAILABLE_AT_UTC_LE_DECISION_AS_OF_UTC"]
    volume_available_at_rule: Literal["VOLUME_AVAILABLE_AT_UTC_LE_DECISION_AS_OF_UTC"]
    open_interest_available_at_rule: Literal["OPEN_INTEREST_AVAILABLE_AT_UTC_LE_DECISION_AS_OF_UTC"]
    volume_semantics: Literal["DECISION_AS_OF_CUMULATIVE_SESSION_VOLUME"]
    end_of_day_or_revised_volume_allowed: Literal[False]
    missing_field_available_at_outcome: Literal["UNKNOWN"]
    future_field_available_at_outcome: Literal["INVALID"]
    expected_identity_fields: tuple[str, ...]
    expected_identity_values_bound_by_run_manifest: Literal[True]
    identity_mismatch_outcome: Literal["INVALID"]

    @field_validator("expected_identity_fields")
    @classmethod
    def validate_identity_fields(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != _IDENTITY_FIELDS:
            raise ValueError("identity field inventory drifted")
        return value


class ContributorManifestContract(_StrictModel):
    expected_manifest_required_per_session: Literal[True]
    expected_contract_ids_sorted_unique: Literal[True]
    duplicate_contract_id_outcome: Literal["INVALID"]
    missing_or_unexpected_contributing_contract_outcome: Literal["INVALID"]
    excluded_row_reason_required: Literal[True]
    excluded_row_invalid_propagates_to_session: Literal[True]
    zero_expected_or_observed_contributors_outcome: Literal["FAIL"]
    result_dependent_manifest_change_allowed: Literal[False]


class RunControlContract(_StrictModel):
    pre_run_stop_terminal: Literal["INSUFFICIENT_EVIDENCE_TO_RUN_DQ"]
    pre_run_invalid_terminal: Literal["PRE_RUN_AUTHORITY_INVALID"]
    identity_or_pit_invalid_action: Literal["HARD_STOP_AND_WRITE_ABORT_RECEIPT"]
    numeric_fail_or_unknown_action: Literal["CONTINUE_FIXED_1202_SESSION_INVENTORY"]
    strategy_evaluation_after_nonpass_allowed: Literal[False]
    maximum_real_runs: Literal[1]


class ArtifactContract(_StrictModel):
    required_artifact_types: tuple[str, ...]
    derived_only: Literal[True]
    raw_option_rows_allowed: Literal[False]
    deterministic_replay_required: Literal[True]
    checksum_catalog_required: Literal[True]

    @field_validator("required_artifact_types")
    @classmethod
    def validate_artifacts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != _ARTIFACT_TYPES:
            raise ValueError("run artifact inventory drifted")
        return value


class SessionAndWindowTerminalContract(_StrictModel):
    session_precedence: tuple[str, ...]
    window_precedence: tuple[str, ...]
    exact_expected_session_set_required: Literal[True]
    exact_1202_of_1202_all_pass_required: Literal[True]
    missing_duplicate_or_unexpected_session_outcome: Literal["GLOBAL_INVALID"]
    majority_vote_allowed: Literal[False]
    pass_rate_tolerance_allowed: Literal[False]
    silent_drop_allowed: Literal[False]

    @model_validator(mode="after")
    def validate_precedence(self) -> Self:
        if self.session_precedence != ("INVALID", "FAIL", "INSUFFICIENT", "PASS"):
            raise ValueError("session terminal precedence drifted")
        if self.window_precedence != (
            "GLOBAL_INVALID",
            "GLOBAL_FAIL",
            "GLOBAL_INSUFFICIENT",
            "GLOBAL_PASS",
        ):
            raise ValueError("window terminal precedence drifted")
        return self


class Safety(_StrictModel):
    dq_run_authorized: Literal[False]
    cache_read_authorized: Literal[False]
    cache_mutation_allowed: Literal[False]
    provider_query_allowed: Literal[False]
    raw_option_rows_allowed: Literal[False]
    empirical_research_authorized: Literal[False]
    backtest_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class StrategyGrowthActionValueDqPitContractV2(_CanonicalModel):
    schema_version: Literal["strategy_growth_action_value_canonical_dq_pit_contract.v2"]
    contract_id: Literal["strategy_growth_action_value_canonical_dq_pit_contract_v2"]
    contract_version: Literal["2.0.0-draft.1"]
    status: Literal["DRAFT_COMPLETE_PENDING_SECOND_REVIEW_AND_OWNER_EXACT_APPROVAL"]
    task_id: Literal[
        "TRADING-2542C_GROWTH_ACTION_VALUE_INDEPENDENT_REVIEW_REMEDIATION_AND_FREEZE_READINESS_V1"
    ]
    predecessor_binding: PredecessorBinding
    review_evidence: ReviewEvidence
    review_state: ReviewState
    scope_binding: ScopeBinding
    required_serial_contract_fields: tuple[str, ...]
    numeric_policy: NumericPolicyBundle
    timestamp_contract: TimestampContract
    spread_contract: SpreadContract
    pit_contract: PitContract
    contributor_manifest_contract: ContributorManifestContract
    run_control_contract: RunControlContract
    artifact_contract: ArtifactContract
    terminal_contract: SessionAndWindowTerminalContract
    safety: Safety

    @model_validator(mode="after")
    def validate_contract(self) -> Self:
        if self.required_serial_contract_fields != _SERIAL_FIELDS:
            raise ValueError("required serial contract fields drifted")
        if self.scope_binding.session_inventory_lf_sha256 != _SESSION_INVENTORY_SHA256:
            raise ValueError("session inventory identity drifted")
        return self


@dataclass(frozen=True)
class StrategyGrowthActionValueDqPitContractV2LoadResult:
    contract: StrategyGrowthActionValueDqPitContractV2
    contract_path: Path
    contract_file_sha256: str
    contract_canonical_sha256: str
    predecessor: StrategyGrowthActionValueDqPitContractLoadResult


def load_strategy_growth_action_value_dq_pit_contract_v2(
    *,
    contract_path: Path = DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_DQ_PIT_CONTRACT_V2_PATH,
    project_root: Path = PROJECT_ROOT,
) -> StrategyGrowthActionValueDqPitContractV2LoadResult:
    try:
        path = _bound_file(contract_path, root=project_root, field="contract_path")
        raw = path.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(contract_path))
        contract = StrategyGrowthActionValueDqPitContractV2.model_validate(payload)
        predecessor = load_strategy_growth_action_value_dq_pit_contract(
            contract_path=Path(contract.predecessor_binding.path), project_root=project_root
        )
        if predecessor.contract_file_sha256 != contract.predecessor_binding.file_sha256:
            raise ValueError("predecessor file SHA-256 mismatch")
        if predecessor.contract_canonical_sha256 != contract.predecessor_binding.canonical_sha256:
            raise ValueError("predecessor canonical SHA-256 mismatch")
    except StrategyGrowthActionValueDqPitContractV2Error:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise StrategyGrowthActionValueDqPitContractV2Error(
            "GROWTH_ACTION_VALUE_DQ_PIT_CONTRACT_V2_REJECTED", str(exc)
        ) from exc
    return StrategyGrowthActionValueDqPitContractV2LoadResult(
        contract=contract,
        contract_path=path,
        contract_file_sha256=hashlib.sha256(raw).hexdigest(),
        contract_canonical_sha256=contract.canonical_sha256,
        predecessor=predecessor,
    )


@dataclass(frozen=True)
class EvidenceIdentity:
    provider: str | None
    engine: str | None
    exchange_calendar: str | None
    symbol_mapping: str | None
    normalization: str | None
    repository_code_sha: str | None
    source_evidence: str | None
    aggregate_manifest: str | None

    def values(self) -> tuple[str | None, ...]:
        return tuple(getattr(self, field) for field in _IDENTITY_FIELDS)


@dataclass(frozen=True)
class ContractObservationV2:
    contract_id: str
    session_date: date
    source_date: date | None
    quote_source_date: date | None
    volume_source_date: date | None
    open_interest_session_date: date | None
    quote_end_utc: datetime | None
    decision_as_of_utc: datetime | None
    quote_available_at_utc: datetime | None
    volume_available_at_utc: datetime | None
    open_interest_available_at_utc: datetime | None
    bid: object
    ask: object
    open_interest: int | None
    volume: int | None
    volume_semantics: str | None
    actual_identity: EvidenceIdentity
    contributing_contract: bool = True
    exclusion_reason: str | None = None
    evidence_scope: Literal["SYNTHETIC_CONTRACT_TEST_ONLY", "REAL_EVIDENCE"] = "REAL_EVIDENCE"


SemanticStatusV2 = Literal["EXCLUDED", "INVALID", "UNKNOWN", "READY_FOR_NUMERIC_CHECK"]


@dataclass(frozen=True)
class SemanticEvaluationV2:
    contract_id: str
    session_date: date
    contributing_contract: bool
    status: SemanticStatusV2
    reasons: tuple[str, ...]
    quote_age_seconds: Decimal | None
    relative_spread: Decimal | None


@dataclass(frozen=True)
class SyntheticNumericThresholdsV2:
    max_quote_age_seconds: Decimal = Decimal(120)
    max_relative_spread: Decimal = Decimal("0.20")
    min_open_interest: int = 10
    min_volume: int = 1
    evidence_scope: Literal["SYNTHETIC_CONTRACT_TEST_ONLY"] = "SYNTHETIC_CONTRACT_TEST_ONLY"


@dataclass(frozen=True)
class ContractEvaluationV2:
    contract_id: str
    session_date: date
    contributing_contract: bool
    status: ContractTerminal
    reasons: tuple[str, ...]
    quote_age_seconds: Decimal | None
    relative_spread: Decimal | None


@dataclass(frozen=True)
class SessionContributorManifest:
    session_date: date
    expected_contract_ids: tuple[str, ...]
    contract_ids_lf_sha256: str


RunControlStatus = Literal["CONTINUE_FIXED_INVENTORY", "HARD_STOP_INVALID"]


def _is_utc_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() == timedelta(0)


def _decimal_seconds(value: timedelta) -> Decimal:
    return (
        Decimal(value.days) * Decimal(86400)
        + Decimal(value.seconds)
        + Decimal(value.microseconds) / Decimal(1_000_000)
    )


def _expected_prior_session(session_date: date, expected_sessions: Sequence[date]) -> date | None:
    sessions = tuple(expected_sessions)
    if tuple(sorted(set(sessions))) != sessions:
        raise ValueError("expected sessions must be sorted and unique")
    try:
        index = sessions.index(session_date)
    except ValueError as exc:
        raise ValueError("observation session is outside frozen inventory") from exc
    return sessions[index - 1] if index > 0 else None


def evaluate_contract_semantics_v2(
    observation: ContractObservationV2,
    *,
    expected_sessions: Sequence[date],
    expected_identity: EvidenceIdentity,
) -> SemanticEvaluationV2:
    if not observation.contract_id.strip():
        return SemanticEvaluationV2(
            observation.contract_id,
            observation.session_date,
            observation.contributing_contract,
            "INVALID",
            ("CONTRACT_ID_MISSING",),
            None,
            None,
        )
    if not observation.contributing_contract:
        if observation.exclusion_reason is None or not observation.exclusion_reason.strip():
            return SemanticEvaluationV2(
                observation.contract_id,
                observation.session_date,
                False,
                "INVALID",
                ("NONCONTRIBUTING_EXCLUSION_REASON_MISSING",),
                None,
                None,
            )
        return SemanticEvaluationV2(
            observation.contract_id,
            observation.session_date,
            False,
            "EXCLUDED",
            ("NONCONTRIBUTING_CONTRACT_EXCLUDED",),
            None,
            None,
        )

    invalid: list[str] = []
    unknown: list[str] = []
    quote_age: Decimal | None = None
    relative_spread: Decimal | None = None

    if observation.decision_as_of_utc is None:
        invalid.append("DECISION_AS_OF_UTC_MISSING")
    elif not _is_utc_aware(observation.decision_as_of_utc):
        invalid.append("DECISION_AS_OF_UTC_NAIVE_OR_NON_UTC")
    if observation.quote_end_utc is None:
        unknown.append("QUOTE_END_UTC_MISSING")
    elif not _is_utc_aware(observation.quote_end_utc):
        invalid.append("QUOTE_END_UTC_NAIVE_OR_NON_UTC")
    if (
        observation.quote_end_utc is not None
        and observation.decision_as_of_utc is not None
        and _is_utc_aware(observation.quote_end_utc)
        and _is_utc_aware(observation.decision_as_of_utc)
    ):
        quote_age = _decimal_seconds(observation.decision_as_of_utc - observation.quote_end_utc)
        if quote_age < 0:
            invalid.append("QUOTE_END_AFTER_DECISION_AS_OF")
        if (
            observation.quote_source_date is not None
            and observation.quote_end_utc.date() != observation.quote_source_date
        ):
            invalid.append("QUOTE_END_SOURCE_DATE_MISMATCH")

    available_fields = (
        ("QUOTE", observation.quote_available_at_utc),
        ("VOLUME", observation.volume_available_at_utc),
        ("OPEN_INTEREST", observation.open_interest_available_at_utc),
    )
    for label, available_at in available_fields:
        if available_at is None:
            unknown.append(f"{label}_AVAILABLE_AT_UTC_MISSING")
        elif not _is_utc_aware(available_at):
            invalid.append(f"{label}_AVAILABLE_AT_UTC_NAIVE_OR_NON_UTC")
        elif (
            observation.decision_as_of_utc is not None
            and _is_utc_aware(observation.decision_as_of_utc)
            and available_at > observation.decision_as_of_utc
        ):
            invalid.append(f"{label}_AVAILABLE_AFTER_DECISION")

    bid_missing = observation.bid is None
    ask_missing = observation.ask is None
    if bid_missing and ask_missing:
        unknown.append("BID_AND_ASK_MISSING")
    elif bid_missing or ask_missing:
        invalid.append("SINGLE_SIDED_PROVIDER_QUOTE")
    elif not isinstance(observation.bid, Decimal) or not isinstance(observation.ask, Decimal):
        invalid.append("BID_OR_ASK_NOT_DECIMAL")
    else:
        bid = observation.bid
        ask = observation.ask
        try:
            if not bid.is_finite() or not ask.is_finite():
                invalid.append("BID_OR_ASK_NONFINITE")
            elif bid < 0 or ask <= 0 or ask < bid:
                invalid.append("QUOTE_DOMAIN_INVALID")
            else:
                midpoint = (bid + ask) / Decimal(2)
                if not midpoint.is_finite() or midpoint <= 0:
                    invalid.append("SPREAD_DENOMINATOR_INVALID")
                else:
                    relative_spread = (ask - bid) / midpoint
                    if not relative_spread.is_finite():
                        invalid.append("RELATIVE_SPREAD_NONFINITE")
        except InvalidOperation:
            invalid.append("QUOTE_DECIMAL_OPERATION_INVALID")

    try:
        prior_session = _expected_prior_session(observation.session_date, expected_sessions)
    except ValueError:
        invalid.append("FROZEN_SESSION_INVENTORY_MISMATCH")
        prior_session = None
    for label, actual, expected in (
        ("SOURCE_DATE", observation.source_date, observation.session_date),
        ("QUOTE_SOURCE_DATE", observation.quote_source_date, observation.session_date),
        ("VOLUME_SOURCE_DATE", observation.volume_source_date, observation.session_date),
    ):
        if actual is None:
            unknown.append(f"{label}_MISSING")
        elif actual != expected:
            invalid.append(f"{label}_MISMATCH")
    if prior_session is None:
        unknown.append("PRIOR_SESSION_UNAVAILABLE_AT_LEFT_BOUNDARY")
    elif observation.open_interest_session_date is None:
        unknown.append("OPEN_INTEREST_SESSION_DATE_MISSING")
    elif observation.open_interest_session_date != prior_session:
        invalid.append("OPEN_INTEREST_SESSION_DATE_MISMATCH")

    if observation.volume_semantics is None:
        unknown.append("VOLUME_SEMANTICS_MISSING")
    elif observation.volume_semantics != "DECISION_AS_OF_CUMULATIVE_SESSION_VOLUME":
        invalid.append("VOLUME_SEMANTICS_LOOKAHEAD_OR_REVISION_INVALID")
    if observation.open_interest is None:
        unknown.append("OPEN_INTEREST_MISSING")
    elif not isinstance(observation.open_interest, int) or observation.open_interest < 0:
        invalid.append("OPEN_INTEREST_DOMAIN_INVALID")
    if observation.volume is None:
        unknown.append("VOLUME_MISSING")
    elif not isinstance(observation.volume, int) or observation.volume < 0:
        invalid.append("VOLUME_DOMAIN_INVALID")

    actual_identity = observation.actual_identity.values()
    expected_identity_values = expected_identity.values()
    if any(value is None or not value.strip() for value in expected_identity_values):
        invalid.append("EXPECTED_IDENTITY_MANIFEST_INCOMPLETE")
    if any(value is None or not value.strip() for value in actual_identity):
        unknown.append("ACTUAL_IDENTITY_FIELD_MISSING")
    elif actual_identity != expected_identity_values:
        invalid.append("EVIDENCE_IDENTITY_MISMATCH")

    if invalid:
        status: SemanticStatusV2 = "INVALID"
        reasons = tuple(dict.fromkeys(invalid + unknown))
    elif unknown:
        status = "UNKNOWN"
        reasons = tuple(dict.fromkeys(unknown))
    else:
        status = "READY_FOR_NUMERIC_CHECK"
        reasons = ("SEMANTIC_CONTRACT_V2_PASS",)
    return SemanticEvaluationV2(
        observation.contract_id,
        observation.session_date,
        True,
        status,
        reasons,
        quote_age,
        relative_spread,
    )


def evaluate_contract_v2(
    contract: StrategyGrowthActionValueDqPitContractV2,
    observation: ContractObservationV2,
    *,
    expected_sessions: Sequence[date],
    expected_identity: EvidenceIdentity,
    synthetic_thresholds: SyntheticNumericThresholdsV2 | None = None,
) -> ContractEvaluationV2:
    semantic = evaluate_contract_semantics_v2(
        observation,
        expected_sessions=expected_sessions,
        expected_identity=expected_identity,
    )
    if semantic.status != "READY_FOR_NUMERIC_CHECK":
        return ContractEvaluationV2(
            semantic.contract_id,
            semantic.session_date,
            semantic.contributing_contract,
            semantic.status,
            semantic.reasons,
            semantic.quote_age_seconds,
            semantic.relative_spread,
        )
    if synthetic_thresholds is None:
        return ContractEvaluationV2(
            semantic.contract_id,
            semantic.session_date,
            True,
            "AUTHORITY_UNAVAILABLE",
            ("NUMERIC_PILOT_POLICY_NOT_EXECUTABLE",),
            semantic.quote_age_seconds,
            semantic.relative_spread,
        )
    if observation.evidence_scope != synthetic_thresholds.evidence_scope:
        return ContractEvaluationV2(
            semantic.contract_id,
            semantic.session_date,
            True,
            "INVALID",
            ("SYNTHETIC_THRESHOLDS_PROHIBITED_FOR_REAL_EVIDENCE",),
            semantic.quote_age_seconds,
            semantic.relative_spread,
        )
    if contract.numeric_policy.executable or contract.review_state.executable_authority:
        raise StrategyGrowthActionValueDqPitContractV2Error(
            "UNREVIEWED_EXECUTABLE_AUTHORITY",
            "draft V2 cannot expose executable authority",
        )
    assert semantic.quote_age_seconds is not None
    assert semantic.relative_spread is not None
    assert observation.open_interest is not None
    assert observation.volume is not None
    misses: list[str] = []
    if semantic.quote_age_seconds > synthetic_thresholds.max_quote_age_seconds:
        misses.append("QUOTE_AGE_ABOVE_MAXIMUM")
    if semantic.relative_spread > synthetic_thresholds.max_relative_spread:
        misses.append("RELATIVE_SPREAD_ABOVE_MAXIMUM")
    if observation.open_interest < synthetic_thresholds.min_open_interest:
        misses.append("OPEN_INTEREST_BELOW_MINIMUM")
    if observation.volume < synthetic_thresholds.min_volume:
        misses.append("VOLUME_BELOW_MINIMUM")
    return ContractEvaluationV2(
        semantic.contract_id,
        semantic.session_date,
        True,
        "FAIL" if misses else "PASS",
        tuple(misses) if misses else ("SYNTHETIC_NUMERIC_CONTRACT_V2_PASS",),
        semantic.quote_age_seconds,
        semantic.relative_spread,
    )


def contributor_manifest_lf_sha256(contract_ids: Sequence[str]) -> str:
    payload = "\n".join(contract_ids) + "\n"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def aggregate_session_v2(
    session_date: date,
    evaluations: Sequence[ContractEvaluationV2],
    *,
    manifest: SessionContributorManifest,
) -> SessionEvaluation:
    values = tuple(evaluations)
    reasons: list[str] = []
    if manifest.session_date != session_date:
        reasons.append("CONTRIBUTOR_MANIFEST_SESSION_MISMATCH")
    expected = manifest.expected_contract_ids
    if not expected:
        reasons.append("ZERO_EXPECTED_CONTRIBUTORS")
    if expected != tuple(sorted(set(expected))):
        reasons.append("EXPECTED_CONTRACT_IDS_NOT_SORTED_UNIQUE")
    if contributor_manifest_lf_sha256(expected) != manifest.contract_ids_lf_sha256:
        reasons.append("CONTRIBUTOR_MANIFEST_SHA256_MISMATCH")
    if any(item.session_date != session_date for item in values):
        reasons.append("CONTRACT_SESSION_DATE_MISMATCH")
    observed_ids = tuple(item.contract_id for item in values)
    if len(set(observed_ids)) != len(observed_ids):
        reasons.append("DUPLICATE_CONTRACT_ID")
    contributing = tuple(item for item in values if item.contributing_contract)
    contributing_ids = tuple(sorted(item.contract_id for item in contributing))
    if contributing_ids != expected:
        reasons.append("CONTRIBUTOR_SET_MISMATCH")
    counts = Counter(item.status for item in values)
    terminal_counts = tuple(sorted(counts.items()))
    excluded_count = sum(item.status == "EXCLUDED" for item in values)
    if reasons or any(item.status == "INVALID" for item in values):
        status: SessionTerminalStatus = "INVALID"
        if not reasons:
            reasons.append("ANY_CONTRACT_INVALID_INCLUDING_EXCLUDED_ROW")
    elif not contributing:
        status = "FAIL"
        reasons.append("ZERO_CONTRIBUTING_CONTRACTS")
    elif any(item.status == "FAIL" for item in contributing):
        status = "FAIL"
        reasons.append("ANY_CONTRIBUTING_CONTRACT_FAIL")
    elif any(item.status in {"UNKNOWN", "AUTHORITY_UNAVAILABLE"} for item in contributing):
        status = "INSUFFICIENT"
        reasons.append("ANY_CONTRIBUTING_CONTRACT_INSUFFICIENT")
    elif all(item.status == "PASS" for item in contributing):
        status = "PASS"
        reasons.append("EXACT_MANIFEST_ALL_CONTRIBUTING_CONTRACTS_PASS")
    else:
        status = "INVALID"
        reasons.append("UNRECOGNIZED_SESSION_TERMINAL_COMBINATION")
    return SessionEvaluation(
        session_date,
        status,
        len(contributing),
        excluded_count,
        terminal_counts,
        tuple(dict.fromkeys(reasons)),
    )


def in_run_control_action(evaluation: ContractEvaluationV2) -> RunControlStatus:
    if evaluation.status == "INVALID":
        return "HARD_STOP_INVALID"
    return "CONTINUE_FIXED_INVENTORY"


def pre_run_authority_state(
    contract: StrategyGrowthActionValueDqPitContractV2,
) -> Literal["INSUFFICIENT_EVIDENCE_TO_RUN_DQ"]:
    if contract.review_state.executable_authority or contract.numeric_policy.executable:
        raise StrategyGrowthActionValueDqPitContractV2Error(
            "UNREVIEWED_EXECUTABLE_AUTHORITY",
            "draft V2 must remain non-executable",
        )
    return "INSUFFICIENT_EVIDENCE_TO_RUN_DQ"


__all__ = [
    "ContractEvaluationV2",
    "ContractObservationV2",
    "DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_DQ_PIT_CONTRACT_V2_PATH",
    "EvidenceIdentity",
    "SemanticEvaluationV2",
    "SessionContributorManifest",
    "StrategyGrowthActionValueDqPitContractV2",
    "StrategyGrowthActionValueDqPitContractV2Error",
    "StrategyGrowthActionValueDqPitContractV2LoadResult",
    "SyntheticNumericThresholdsV2",
    "aggregate_session_v2",
    "aggregate_window",
    "contributor_manifest_lf_sha256",
    "evaluate_contract_semantics_v2",
    "evaluate_contract_v2",
    "in_run_control_action",
    "load_strategy_growth_action_value_dq_pit_contract_v2",
    "pre_run_authority_state",
    "session_inventory_lf_sha256",
]
