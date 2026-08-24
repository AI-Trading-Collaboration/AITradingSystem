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
from ai_trading_system.strategy_growth_action_value_measurement_contract import (
    load_strategy_growth_action_value_measurement_contract,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_DQ_PIT_CONTRACT_PATH = Path(
    "config/research/strategy_growth_action_value_canonical_dq_pit_contract_v1.yaml"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
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
_SOURCE_BINDINGS = (
    (
        "BASE_DQ_IDENTITY",
        "config/research/qqq_options_dq_pit_identity_v1.yaml",
        "1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358",
        "PRESERVE_REVIEWED_FAIL_CLOSED_FACTS",
    ),
    (
        "STAGED_DQ_READINESS",
        "config/research/qqq_options_staged_dq_pit_readiness_v1.yaml",
        "35e0455bc8f7e1b2660ffdbac5b508286a28671ca225872f4a86b7671ac14f2d",
        "PRESERVE_DATA_RESEARCH_STAGE_SEPARATION",
    ),
    (
        "RECOVERED_SESSION_INVENTORY",
        "inputs/research/qqq_options/trading_2541_exact_date_subscription_recovery_v1/"
        "recovery_contract.json",
        "80fe6c16ab0e0c7d7104fc9d87c7a29437119b8d1ace4992285ff8f3ef18a86e",
        "BIND_EXACT_1202_SESSION_INVENTORY_IDENTITY",
    ),
)


class StrategyGrowthActionValueDqPitContractError(ValueError):
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


class OwnerInstruction(_StrictModel):
    decision_id: Literal[
        "owner_decision:TRADING-2542B:2026-08-23:"
        "continue_complete_canonical_dq_pit_contract_draft_v1"
    ]
    scope: Literal["CONTINUE_DRAFT_NOT_EXACT_THRESHOLD_APPROVAL"]


class ReviewState(_StrictModel):
    owner_exact_approval: Literal["NOT_PROVIDED"]
    independent_review: Literal["NOT_PERFORMED"]
    executable_authority: Literal[False]
    freeze_allowed: Literal[False]


class ScopeBinding(_StrictModel):
    hypothesis_id: Literal["BASELINE_BOUNDED_QQQ_GROWTH_OVERLAY_NON_BETA_ACTION_VALUE_V1"]
    selected_data_lane: Literal["QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE"]
    stage: Literal["DATA_RESEARCH"]
    shadow_selection_authorized: Literal[False]
    execution_authorized: Literal[False]
    primary_window_start: Literal["2021-02-22"]
    primary_window_end: Literal["2025-12-02"]
    expected_session_count: int
    calendar_id: Literal["QQQ_EXCHANGE_SESSIONS"]
    session_inventory_lf_sha256: str

    @model_validator(mode="after")
    def validate_scope(self) -> Self:
        if self.expected_session_count != 1202:
            raise ValueError("expected session count drifted")
        if self.session_inventory_lf_sha256 != (
            "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
        ):
            raise ValueError("session inventory identity drifted")
        return self


class SourceAuthorityBinding(_StrictModel):
    role: str
    path: str
    file_sha256: str
    disposition: str

    @field_validator("file_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256_PATTERN.fullmatch(value):
            raise ValueError("source binding hash must be a lowercase SHA-256")
        return value


class ConsumerBinding(_StrictModel):
    path: Literal[
        "config/research/strategy_growth_action_value_threshold_exact_value_sheet_v2.yaml"
    ]
    file_sha256: Literal["ee1db5f51affe3c76e3b6fd9dc78dd7308b4b4999ed67c2853c116c079b0965d"]
    canonical_sha256: Literal["8da0aa87f463ee886d8195f39338c10af6fed536c1c982c0352c1cf37950fb7d"]
    axis_id: Literal["CANONICAL_DQ_PIT"]
    consumer_bytes_mutated: Literal[False]


class NumericValue(_StrictModel):
    value: Decimal
    unit: Literal["seconds", "ratio", "contracts"]
    direction: Literal["LESS_THAN_OR_EQUAL", "GREATER_THAN_OR_EQUAL"]


class NumericValues(_StrictModel):
    max_quote_age_seconds: NumericValue
    max_relative_spread: NumericValue
    min_open_interest: NumericValue
    min_volume: NumericValue


class NumericAuthority(_StrictModel):
    state: Literal["OWNER_INTENT_ONLY_NOT_EXECUTABLE_AUTHORITY"]
    authority_unavailable_outcome: Literal["INSUFFICIENT"]
    owner_review_state: Literal["PENDING_OWNER_APPROVAL"]
    independent_review_state: Literal["NOT_PERFORMED"]
    executable: Literal[False]
    unknown_can_pass: Literal[False]
    values: NumericValues

    @model_validator(mode="after")
    def validate_intent_only_values(self) -> Self:
        expected = (
            (self.values.max_quote_age_seconds, Decimal(120), "seconds", "LESS_THAN_OR_EQUAL"),
            (self.values.max_relative_spread, Decimal("0.20"), "ratio", "LESS_THAN_OR_EQUAL"),
            (self.values.min_open_interest, Decimal(10), "contracts", "GREATER_THAN_OR_EQUAL"),
            (self.values.min_volume, Decimal(1), "contracts", "GREATER_THAN_OR_EQUAL"),
        )
        for item, value, unit, direction in expected:
            if item.value != value or item.unit != unit or item.direction != direction:
                raise ValueError("numeric owner intent drifted")
        return self


class TimestampContract(_StrictModel):
    timezone: Literal["UTC"]
    quote_age_formula: Literal["DECISION_AS_OF_UTC_MINUS_QUOTE_END_UTC_TOTAL_SECONDS"]
    rounding_before_comparison_allowed: Literal[False]
    absolute_value_allowed: Literal[False]
    quote_timestamp_direction: Literal["QUOTE_END_UTC_LE_DECISION_AS_OF_UTC"]
    future_quote_outcome: Literal["INVALID"]
    missing_timestamp_outcome: Literal["UNKNOWN"]
    naive_or_non_utc_timestamp_outcome: Literal["INVALID"]


class SpreadContract(_StrictModel):
    formula: Literal["ASK_MINUS_BID_DIVIDED_BY_BID_ASK_MIDPOINT"]
    denominator: Literal["BID_PLUS_ASK_DIVIDED_BY_TWO"]
    bid_minimum: Decimal
    ask_strictly_positive: Literal[True]
    ask_greater_than_or_equal_to_bid: Literal[True]
    zero_or_nonfinite_denominator_outcome: Literal["INVALID"]
    crossed_or_single_sided_quote_outcome: Literal["INVALID"]
    alternate_denominator_allowed: Literal[False]

    @field_validator("bid_minimum")
    @classmethod
    def validate_bid_minimum(cls, value: Decimal) -> Decimal:
        if value != Decimal(0):
            raise ValueError("bid minimum drifted")
        return value


class PitContract(_StrictModel):
    source_date_rule: Literal["EXACT_TARGET_EXCHANGE_SESSION_DATE"]
    cross_date_fallback_allowed: Literal[False]
    quote_volume_contract_identity_rule: Literal["SAME_SOURCE_SESSION"]
    open_interest_freshness_rule: Literal["EXACT_PRIOR_EXCHANGE_SESSION"]
    availability_rule: Literal["AVAILABLE_AT_UTC_LE_DECISION_AS_OF_UTC"]
    missing_available_at_outcome: Literal["UNKNOWN"]
    future_available_at_outcome: Literal["INVALID"]
    identity_fields: tuple[str, ...]
    identity_mismatch_outcome: Literal["INVALID"]

    @field_validator("identity_fields")
    @classmethod
    def validate_identity_fields(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != _IDENTITY_FIELDS:
            raise ValueError("identity field order drifted")
        return value


class ContractAggregation(_StrictModel):
    contribution_marker: Literal["contributing_contract"]
    contribution_marker_binding: Literal["REVIEWED_AGGREGATE_MANIFEST_EXACT_IDENTITY"]
    result_dependent_marker_change_allowed: Literal[False]
    noncontributing_row_terminal: Literal["EXCLUDED"]
    noncontributing_exclusion_reason_required: Literal[True]
    contributing_identity_or_pit_violation_outcome: Literal["INVALID"]
    contributing_numeric_threshold_miss_outcome: Literal["FAIL"]
    contributing_required_field_missing_outcome: Literal["UNKNOWN"]
    contributing_all_checks_pass_outcome: Literal["PASS"]


class SessionAggregation(_StrictModel):
    minimum_contributing_contract_count: Literal[1]
    zero_contributing_contract_outcome: Literal["FAIL"]
    precedence: tuple[Literal["INVALID", "FAIL", "UNKNOWN", "PASS"], ...]
    unknown_session_mapping: Literal["INSUFFICIENT"]
    contribution_manifest_mismatch_outcome: Literal["INVALID"]
    one_terminal_record_per_requested_session: Literal[True]
    duplicate_or_unexpected_session_outcome: Literal["INVALID"]

    @field_validator("precedence")
    @classmethod
    def validate_precedence(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != ("INVALID", "FAIL", "UNKNOWN", "PASS"):
            raise ValueError("session precedence drifted")
        return value


class WindowTerminal(_StrictModel):
    expected_session_count: Literal[1202]
    exact_expected_session_set_required: Literal[True]
    missing_session_outcome: Literal["GLOBAL_INVALID"]
    precedence: tuple[
        Literal["GLOBAL_INVALID", "GLOBAL_FAIL", "GLOBAL_INSUFFICIENT", "GLOBAL_PASS"], ...
    ]
    invalid_rule: Literal["ANY_SESSION_INVALID_OR_SESSION_SET_IDENTITY_DRIFT"]
    fail_rule: Literal["NO_INVALID_AND_ANY_SESSION_FAIL"]
    insufficient_rule: Literal["NO_INVALID_OR_FAIL_AND_ANY_SESSION_INSUFFICIENT"]
    pass_rule: Literal["EXACT_1202_OF_1202_ALL_SESSION_PASS"]
    majority_vote_allowed: Literal[False]
    pass_rate_tolerance_allowed: Literal[False]
    silent_drop_allowed: Literal[False]

    @field_validator("precedence")
    @classmethod
    def validate_precedence(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        expected = ("GLOBAL_INVALID", "GLOBAL_FAIL", "GLOBAL_INSUFFICIENT", "GLOBAL_PASS")
        if value != expected:
            raise ValueError("global precedence drifted")
        return value


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


class StrategyGrowthActionValueDqPitContract(_CanonicalModel):
    schema_version: Literal["strategy_growth_action_value_canonical_dq_pit_contract.v1"]
    contract_id: Literal["strategy_growth_action_value_canonical_dq_pit_contract_v1"]
    contract_version: Literal["1.0.0-draft.1"]
    status: Literal["DRAFT_COMPLETE_PENDING_OWNER_AND_INDEPENDENT_REVIEW"]
    task_id: Literal["TRADING-2542B_GROWTH_ACTION_VALUE_CANONICAL_DQ_PIT_SERIAL_CONTRACT_V1"]
    registration_base_repository_code_sha: Literal["675b8841890b9c943d9e57ab9e99509426e00fa2"]
    owner_instruction: OwnerInstruction
    review_state: ReviewState
    scope_binding: ScopeBinding
    source_authority_bindings: tuple[SourceAuthorityBinding, ...]
    consumer_binding: ConsumerBinding
    required_serial_contract_fields: tuple[str, ...]
    numeric_authority: NumericAuthority
    timestamp_contract: TimestampContract
    spread_contract: SpreadContract
    pit_contract: PitContract
    contract_aggregation: ContractAggregation
    session_aggregation: SessionAggregation
    window_terminal: WindowTerminal
    safety: Safety

    @model_validator(mode="after")
    def validate_complete_contract(self) -> Self:
        actual_bindings = tuple(
            (item.role, item.path, item.file_sha256, item.disposition)
            for item in self.source_authority_bindings
        )
        if actual_bindings != _SOURCE_BINDINGS:
            raise ValueError("source authority bindings drifted")
        if self.required_serial_contract_fields != _SERIAL_FIELDS:
            raise ValueError("required serial contract field inventory drifted")
        if self.window_terminal.expected_session_count != self.scope_binding.expected_session_count:
            raise ValueError("window and scope session counts disagree")
        return self


@dataclass(frozen=True)
class StrategyGrowthActionValueDqPitContractLoadResult:
    contract: StrategyGrowthActionValueDqPitContract
    contract_path: Path
    contract_file_sha256: str
    contract_canonical_sha256: str


def load_strategy_growth_action_value_dq_pit_contract(
    *,
    contract_path: Path = DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_DQ_PIT_CONTRACT_PATH,
    project_root: Path = PROJECT_ROOT,
) -> StrategyGrowthActionValueDqPitContractLoadResult:
    try:
        path = _bound_file(contract_path, root=project_root, field="contract_path")
        raw = path.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(contract_path))
        contract = StrategyGrowthActionValueDqPitContract.model_validate(payload)
        for binding in contract.source_authority_bindings:
            bound = _bound_file(Path(binding.path), root=project_root, field=binding.role)
            if hashlib.sha256(bound.read_bytes()).hexdigest() != binding.file_sha256:
                raise ValueError(f"{binding.role} file SHA-256 mismatch")
        measurement = load_strategy_growth_action_value_measurement_contract(
            contract_path=Path(contract.consumer_binding.path), project_root=project_root
        )
        if measurement.contract_file_sha256 != contract.consumer_binding.file_sha256:
            raise ValueError("consumer file SHA-256 mismatch")
        if measurement.contract_canonical_sha256 != contract.consumer_binding.canonical_sha256:
            raise ValueError("consumer canonical SHA-256 mismatch")
        recovery_binding = contract.source_authority_bindings[2]
        recovery_path = _bound_file(
            Path(recovery_binding.path), root=project_root, field=recovery_binding.role
        )
        recovery = _duplicate_key_rejecting_json(recovery_path.read_bytes())
        if not isinstance(recovery, dict):
            raise ValueError("recovery contract root must be an object")
        expected_recovery = {
            "requested_range": (
                f"{contract.scope_binding.primary_window_start}.."
                f"{contract.scope_binding.primary_window_end}"
            ),
            "expected_session_count": contract.scope_binding.expected_session_count,
            "session_inventory_lf_sha256": contract.scope_binding.session_inventory_lf_sha256,
            "exchange_calendar": "XNYS",
        }
        for field, expected in expected_recovery.items():
            if recovery.get(field) != expected:
                raise ValueError(f"recovery contract {field} mismatch")
    except StrategyGrowthActionValueDqPitContractError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise StrategyGrowthActionValueDqPitContractError(
            "GROWTH_ACTION_VALUE_DQ_PIT_CONTRACT_REJECTED", str(exc)
        ) from exc
    return StrategyGrowthActionValueDqPitContractLoadResult(
        contract=contract,
        contract_path=path,
        contract_file_sha256=hashlib.sha256(raw).hexdigest(),
        contract_canonical_sha256=contract.canonical_sha256,
    )


IdentityStatus = Literal["EXACT", "MISMATCH", "UNKNOWN"]
EvidenceScope = Literal["SYNTHETIC_CONTRACT_TEST_ONLY", "REAL_EVIDENCE"]
SemanticStatus = Literal["EXCLUDED", "INVALID", "UNKNOWN", "READY_FOR_NUMERIC_CHECK"]
ContractTerminal = Literal[
    "EXCLUDED", "INVALID", "UNKNOWN", "AUTHORITY_UNAVAILABLE", "FAIL", "PASS"
]
SessionTerminalStatus = Literal["INVALID", "FAIL", "INSUFFICIENT", "PASS"]
WindowTerminalStatus = Literal[
    "GLOBAL_INVALID", "GLOBAL_FAIL", "GLOBAL_INSUFFICIENT", "GLOBAL_PASS"
]


@dataclass(frozen=True)
class ContractObservation:
    contract_id: str
    session_date: date
    source_date: date | None
    quote_source_date: date | None
    volume_source_date: date | None
    expected_prior_session_date: date | None
    open_interest_session_date: date | None
    quote_end_utc: datetime | None
    decision_as_of_utc: datetime | None
    available_at_utc: datetime | None
    bid: Decimal | None
    ask: Decimal | None
    open_interest: int | None
    volume: int | None
    contributing_contract: bool = True
    exclusion_reason: str | None = None
    provider_status: IdentityStatus = "UNKNOWN"
    engine_status: IdentityStatus = "UNKNOWN"
    exchange_calendar_status: IdentityStatus = "UNKNOWN"
    symbol_mapping_status: IdentityStatus = "UNKNOWN"
    normalization_status: IdentityStatus = "UNKNOWN"
    repository_code_sha_status: IdentityStatus = "UNKNOWN"
    source_evidence_status: IdentityStatus = "UNKNOWN"
    aggregate_manifest_status: IdentityStatus = "UNKNOWN"
    evidence_scope: EvidenceScope = "REAL_EVIDENCE"


@dataclass(frozen=True)
class SemanticEvaluation:
    contract_id: str
    session_date: date
    contributing_contract: bool
    status: SemanticStatus
    reasons: tuple[str, ...]
    quote_age_seconds: Decimal | None
    relative_spread: Decimal | None


@dataclass(frozen=True)
class SyntheticNumericThresholds:
    max_quote_age_seconds: Decimal
    max_relative_spread: Decimal
    min_open_interest: int
    min_volume: int
    evidence_scope: Literal["SYNTHETIC_CONTRACT_TEST_ONLY"] = "SYNTHETIC_CONTRACT_TEST_ONLY"


@dataclass(frozen=True)
class ContractEvaluation:
    contract_id: str
    session_date: date
    contributing_contract: bool
    status: ContractTerminal
    reasons: tuple[str, ...]
    quote_age_seconds: Decimal | None
    relative_spread: Decimal | None


@dataclass(frozen=True)
class SessionEvaluation:
    session_date: date
    status: SessionTerminalStatus
    contributing_contract_count: int
    excluded_contract_count: int
    terminal_counts: tuple[tuple[str, int], ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class WindowEvaluation:
    status: WindowTerminalStatus
    observed_session_count: int
    expected_session_count: int
    terminal_counts: tuple[tuple[str, int], ...]
    reasons: tuple[str, ...]


def _is_utc_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() == timedelta(0)


def _timedelta_decimal_seconds(value: timedelta) -> Decimal:
    return (
        Decimal(value.days) * Decimal(86400)
        + Decimal(value.seconds)
        + Decimal(value.microseconds) / Decimal(1_000_000)
    )


def _identity_statuses(observation: ContractObservation) -> tuple[IdentityStatus, ...]:
    return (
        observation.provider_status,
        observation.engine_status,
        observation.exchange_calendar_status,
        observation.symbol_mapping_status,
        observation.normalization_status,
        observation.repository_code_sha_status,
        observation.source_evidence_status,
        observation.aggregate_manifest_status,
    )


def evaluate_contract_semantics(observation: ContractObservation) -> SemanticEvaluation:
    if not observation.contributing_contract:
        if observation.exclusion_reason is None or not observation.exclusion_reason.strip():
            return SemanticEvaluation(
                observation.contract_id,
                observation.session_date,
                False,
                "INVALID",
                ("NONCONTRIBUTING_EXCLUSION_REASON_MISSING",),
                None,
                None,
            )
        return SemanticEvaluation(
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

    timestamps = (
        ("QUOTE_END", observation.quote_end_utc),
        ("DECISION_AS_OF", observation.decision_as_of_utc),
        ("AVAILABLE_AT", observation.available_at_utc),
    )
    for label, value in timestamps:
        if value is None:
            unknown.append(f"{label}_UTC_MISSING")
        elif not _is_utc_aware(value):
            invalid.append(f"{label}_UTC_NAIVE_OR_NON_UTC")
    if observation.quote_end_utc is not None and observation.decision_as_of_utc is not None:
        if _is_utc_aware(observation.quote_end_utc) and _is_utc_aware(
            observation.decision_as_of_utc
        ):
            delta = observation.decision_as_of_utc - observation.quote_end_utc
            quote_age = _timedelta_decimal_seconds(delta)
            if quote_age < 0:
                invalid.append("QUOTE_END_AFTER_DECISION_AS_OF")
    if observation.available_at_utc is not None and observation.decision_as_of_utc is not None:
        if (
            _is_utc_aware(observation.available_at_utc)
            and _is_utc_aware(observation.decision_as_of_utc)
            and observation.available_at_utc > observation.decision_as_of_utc
        ):
            invalid.append("AVAILABLE_AT_AFTER_DECISION_AS_OF")

    if observation.bid is None or observation.ask is None:
        unknown.append("BID_OR_ASK_MISSING")
    else:
        try:
            if not observation.bid.is_finite() or not observation.ask.is_finite():
                invalid.append("BID_OR_ASK_NONFINITE")
            elif observation.bid < 0 or observation.ask <= 0 or observation.ask < observation.bid:
                invalid.append("QUOTE_DOMAIN_INVALID")
            else:
                midpoint = (observation.bid + observation.ask) / Decimal(2)
                if not midpoint.is_finite() or midpoint <= 0:
                    invalid.append("SPREAD_DENOMINATOR_INVALID")
                else:
                    relative_spread = (observation.ask - observation.bid) / midpoint
                    if not relative_spread.is_finite():
                        invalid.append("RELATIVE_SPREAD_NONFINITE")
        except InvalidOperation:
            invalid.append("QUOTE_DECIMAL_OPERATION_INVALID")

    date_fields = (
        ("SOURCE_DATE", observation.source_date, observation.session_date),
        ("QUOTE_SOURCE_DATE", observation.quote_source_date, observation.session_date),
        ("VOLUME_SOURCE_DATE", observation.volume_source_date, observation.session_date),
        (
            "OPEN_INTEREST_SESSION_DATE",
            observation.open_interest_session_date,
            observation.expected_prior_session_date,
        ),
    )
    if observation.expected_prior_session_date is None:
        unknown.append("EXPECTED_PRIOR_SESSION_DATE_MISSING")
    for label, actual, expected in date_fields:
        if actual is None:
            unknown.append(f"{label}_MISSING")
        elif expected is not None and actual != expected:
            invalid.append(f"{label}_MISMATCH")

    if observation.open_interest is None:
        unknown.append("OPEN_INTEREST_MISSING")
    elif observation.open_interest < 0:
        invalid.append("OPEN_INTEREST_NEGATIVE")
    if observation.volume is None:
        unknown.append("VOLUME_MISSING")
    elif observation.volume < 0:
        invalid.append("VOLUME_NEGATIVE")

    identities = _identity_statuses(observation)
    if any(status == "MISMATCH" for status in identities):
        invalid.append("IDENTITY_MISMATCH")
    if any(status == "UNKNOWN" for status in identities):
        unknown.append("IDENTITY_UNKNOWN")

    if invalid:
        status: SemanticStatus = "INVALID"
        reasons = tuple(dict.fromkeys(invalid + unknown))
    elif unknown:
        status = "UNKNOWN"
        reasons = tuple(dict.fromkeys(unknown))
    else:
        status = "READY_FOR_NUMERIC_CHECK"
        reasons = ("SEMANTIC_CONTRACT_PASS",)
    return SemanticEvaluation(
        observation.contract_id,
        observation.session_date,
        True,
        status,
        reasons,
        quote_age,
        relative_spread,
    )


def synthetic_thresholds_from_intent(
    contract: StrategyGrowthActionValueDqPitContract,
    *,
    confirmation: str,
) -> SyntheticNumericThresholds:
    if confirmation != "SYNTHETIC_CONTRACT_VALIDATION_ONLY":
        raise StrategyGrowthActionValueDqPitContractError(
            "SYNTHETIC_THRESHOLD_SCOPE_REJECTED",
            "synthetic thresholds require the exact test-only confirmation",
        )
    values = contract.numeric_authority.values
    return SyntheticNumericThresholds(
        max_quote_age_seconds=values.max_quote_age_seconds.value,
        max_relative_spread=values.max_relative_spread.value,
        min_open_interest=int(values.min_open_interest.value),
        min_volume=int(values.min_volume.value),
    )


def evaluate_contract(
    observation: ContractObservation,
    *,
    thresholds: SyntheticNumericThresholds | None = None,
) -> ContractEvaluation:
    semantic = evaluate_contract_semantics(observation)
    if semantic.status != "READY_FOR_NUMERIC_CHECK":
        return ContractEvaluation(
            semantic.contract_id,
            semantic.session_date,
            semantic.contributing_contract,
            semantic.status,
            semantic.reasons,
            semantic.quote_age_seconds,
            semantic.relative_spread,
        )
    if thresholds is None:
        return ContractEvaluation(
            semantic.contract_id,
            semantic.session_date,
            True,
            "AUTHORITY_UNAVAILABLE",
            ("NUMERIC_AUTHORITY_NOT_EXECUTABLE",),
            semantic.quote_age_seconds,
            semantic.relative_spread,
        )
    if observation.evidence_scope != thresholds.evidence_scope:
        return ContractEvaluation(
            semantic.contract_id,
            semantic.session_date,
            True,
            "INVALID",
            ("SYNTHETIC_THRESHOLDS_PROHIBITED_FOR_REAL_EVIDENCE",),
            semantic.quote_age_seconds,
            semantic.relative_spread,
        )
    assert semantic.quote_age_seconds is not None
    assert semantic.relative_spread is not None
    assert observation.open_interest is not None
    assert observation.volume is not None
    misses: list[str] = []
    if semantic.quote_age_seconds > thresholds.max_quote_age_seconds:
        misses.append("QUOTE_AGE_ABOVE_MAXIMUM")
    if semantic.relative_spread > thresholds.max_relative_spread:
        misses.append("RELATIVE_SPREAD_ABOVE_MAXIMUM")
    if observation.open_interest < thresholds.min_open_interest:
        misses.append("OPEN_INTEREST_BELOW_MINIMUM")
    if observation.volume < thresholds.min_volume:
        misses.append("VOLUME_BELOW_MINIMUM")
    return ContractEvaluation(
        semantic.contract_id,
        semantic.session_date,
        True,
        "FAIL" if misses else "PASS",
        tuple(misses) if misses else ("SYNTHETIC_NUMERIC_CONTRACT_PASS",),
        semantic.quote_age_seconds,
        semantic.relative_spread,
    )


def evaluate_authority_state(
    contract: StrategyGrowthActionValueDqPitContract,
) -> Literal["AUTHORITY_UNAVAILABLE"]:
    if contract.review_state.executable_authority or contract.numeric_authority.executable:
        raise StrategyGrowthActionValueDqPitContractError(
            "UNREVIEWED_EXECUTABLE_AUTHORITY",
            "draft v1 must not expose executable authority",
        )
    return "AUTHORITY_UNAVAILABLE"


def aggregate_session(
    session_date: date,
    evaluations: Sequence[ContractEvaluation],
    *,
    contribution_manifest_status: IdentityStatus,
) -> SessionEvaluation:
    values = tuple(evaluations)
    if any(item.session_date != session_date for item in values):
        return SessionEvaluation(
            session_date,
            "INVALID",
            0,
            0,
            (),
            ("CONTRACT_SESSION_DATE_MISMATCH",),
        )
    counts = Counter(item.status for item in values)
    terminal_counts = tuple(sorted(counts.items()))
    contributing = tuple(item for item in values if item.contributing_contract)
    excluded_count = sum(item.status == "EXCLUDED" for item in values)
    if contribution_manifest_status == "MISMATCH":
        status: SessionTerminalStatus = "INVALID"
        reasons = ("CONTRIBUTION_MANIFEST_MISMATCH",)
    elif contribution_manifest_status == "UNKNOWN":
        status = "INSUFFICIENT"
        reasons = ("CONTRIBUTION_MANIFEST_UNKNOWN",)
    elif not contributing:
        status = "FAIL"
        reasons = ("ZERO_CONTRIBUTING_CONTRACTS",)
    elif any(item.status == "INVALID" for item in contributing):
        status = "INVALID"
        reasons = ("ANY_CONTRIBUTING_CONTRACT_INVALID",)
    elif any(item.status == "FAIL" for item in contributing):
        status = "FAIL"
        reasons = ("ANY_CONTRIBUTING_CONTRACT_FAIL",)
    elif any(item.status in {"UNKNOWN", "AUTHORITY_UNAVAILABLE"} for item in contributing):
        status = "INSUFFICIENT"
        reasons = ("ANY_CONTRIBUTING_CONTRACT_INSUFFICIENT",)
    elif all(item.status == "PASS" for item in contributing):
        status = "PASS"
        reasons = ("ALL_CONTRIBUTING_CONTRACTS_PASS",)
    else:
        status = "INVALID"
        reasons = ("UNRECOGNIZED_SESSION_TERMINAL_COMBINATION",)
    return SessionEvaluation(
        session_date,
        status,
        len(contributing),
        excluded_count,
        terminal_counts,
        reasons,
    )


def session_inventory_lf_sha256(expected_sessions: Sequence[date]) -> str:
    payload = "\n".join(item.isoformat() for item in expected_sessions) + "\n"
    return hashlib.sha256(payload.encode("ascii")).hexdigest()


def aggregate_window(
    sessions: Sequence[SessionEvaluation],
    *,
    expected_sessions: Sequence[date],
    expected_session_count: int,
    expected_session_inventory_lf_sha256: str,
) -> WindowEvaluation:
    expected = tuple(expected_sessions)
    values = tuple(sessions)
    reasons: list[str] = []
    if len(expected) != expected_session_count:
        reasons.append("EXPECTED_SESSION_COUNT_MISMATCH")
    if len(set(expected)) != len(expected):
        reasons.append("EXPECTED_SESSION_DUPLICATE")
    if session_inventory_lf_sha256(expected) != expected_session_inventory_lf_sha256:
        reasons.append("EXPECTED_SESSION_INVENTORY_SHA256_MISMATCH")
    observed_dates = tuple(item.session_date for item in values)
    if len(set(observed_dates)) != len(observed_dates):
        reasons.append("OBSERVED_SESSION_DUPLICATE")
    if set(observed_dates) != set(expected):
        reasons.append("OBSERVED_SESSION_SET_MISMATCH")
    counts = Counter(item.status for item in values)
    terminal_counts = tuple(sorted(counts.items()))
    if reasons or any(item.status == "INVALID" for item in values):
        status: WindowTerminalStatus = "GLOBAL_INVALID"
        if not reasons:
            reasons.append("ANY_SESSION_INVALID")
    elif any(item.status == "FAIL" for item in values):
        status = "GLOBAL_FAIL"
        reasons.append("ANY_SESSION_FAIL")
    elif any(item.status == "INSUFFICIENT" for item in values):
        status = "GLOBAL_INSUFFICIENT"
        reasons.append("ANY_SESSION_INSUFFICIENT")
    elif values and all(item.status == "PASS" for item in values):
        status = "GLOBAL_PASS"
        reasons.append("EXACT_EXPECTED_SESSION_SET_ALL_PASS")
    else:
        status = "GLOBAL_INVALID"
        reasons.append("EMPTY_OR_UNRECOGNIZED_WINDOW")
    return WindowEvaluation(
        status,
        len(values),
        expected_session_count,
        terminal_counts,
        tuple(reasons),
    )


__all__ = [
    "DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_DQ_PIT_CONTRACT_PATH",
    "ContractEvaluation",
    "ContractObservation",
    "SessionEvaluation",
    "StrategyGrowthActionValueDqPitContract",
    "StrategyGrowthActionValueDqPitContractError",
    "StrategyGrowthActionValueDqPitContractLoadResult",
    "SyntheticNumericThresholds",
    "WindowEvaluation",
    "aggregate_session",
    "aggregate_window",
    "evaluate_authority_state",
    "evaluate_contract",
    "evaluate_contract_semantics",
    "load_strategy_growth_action_value_dq_pit_contract",
    "session_inventory_lf_sha256",
    "synthetic_thresholds_from_intent",
]
