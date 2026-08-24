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
    WindowEvaluation,
    aggregate_window,
    session_inventory_lf_sha256,
)
from ai_trading_system.strategy_growth_action_value_dq_pit_contract_v2 import (
    ArtifactContract,
    ContractObservationV2,
    EvidenceIdentity,
    NumericFieldPolicy,
    RunControlContract,
    Safety,
    SessionAndWindowTerminalContract,
    SessionContributorManifest,
    SpreadContract,
    StrategyGrowthActionValueDqPitContractV2LoadResult,
    TimestampContract,
    contributor_manifest_lf_sha256,
    load_strategy_growth_action_value_dq_pit_contract_v2,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_DQ_PIT_CONTRACT_V3_PATH = Path(
    "config/research/strategy_growth_action_value_canonical_dq_pit_contract_v3.yaml"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SESSION_INVENTORY_SHA256 = "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
_TARGET_START = date(2021, 2, 22)
_TARGET_END = date(2025, 12, 2)
_PRE_WINDOW_PRIOR = date(2021, 2, 19)
_EXPECTED_SESSION_COUNT = 1202
_CONTRACT_CANONICAL_SHA256 = "e8e180b147e1a88dad3776f886b8eb7398481b1518785b6a2243ae795f4a6ede"
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


class StrategyGrowthActionValueDqPitContractV3Error(ValueError):
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


class PredecessorBindingV3(_StrictModel):
    path: Literal["config/research/strategy_growth_action_value_canonical_dq_pit_contract_v2.yaml"]
    file_sha256: Literal["c9c74d5da0819f206ae59543dcab34a2f1f920687fd4bf646da49a4eabbbd327"]
    canonical_sha256: Literal["94e99dea15f0c62756f87230a7706d575b24e4c193db7bd4673ef2bb44427843"]
    disposition: Literal["REQUEST_NEW_VERSION_RETAINED_IMMUTABLE"]


class ReviewEvidenceV3(_StrictModel):
    reviewed_repository_commit: Literal["e5266c9aadfba067060b013d83ec26bd4f065604"]
    conversation_url: Literal["https://chatgpt.com/c/6a8b95b1-30dc-83e8-8d49-4b74a696acc1"]
    visible_product_label: Literal["ChatGPT Pro"]
    visible_model_label: Literal["GPT-5.6 Pro"]
    backend_route_attestation: Literal["CANNOT_VERIFY_EXACT_BACKEND_ROUTE"]
    routing_evidence_state: Literal["UI_PRO_AND_SELF_REPORT_PRO_ROUTE_UNVERIFIED"]
    overall_disposition: Literal["REQUEST_NEW_VERSION_BEFORE_OWNER_FREEZE_DECISION"]


class ReviewStateV3(_StrictModel):
    project_owner_adopted_review: Literal[True]
    second_independent_review: Literal["PERFORMED_NEW_VERSION_REQUIRED"]
    non_executable_pilot_values_freeze_ready: Literal[True]
    owner_exact_successor_freeze_approval: Literal[
        "APPROVED_EXACTLY_AS_DRAFTED_NON_EXECUTABLE_DATA_RESEARCH"
    ]
    executable_authority: Literal[False]
    real_evidence_authority: Literal[False]
    freeze_allowed: Literal[True]


class ScopeBindingV3(_StrictModel):
    hypothesis_id: Literal["BASELINE_BOUNDED_QQQ_GROWTH_OVERLAY_NON_BETA_ACTION_VALUE_V1"]
    selected_data_lane: Literal["QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE"]
    stage: Literal["DATA_RESEARCH"]
    primary_window_start: Literal["2021-02-22"]
    primary_window_end: Literal["2025-12-02"]
    expected_session_count: Literal[1202]
    calendar_id: Literal["QQQ_EXCHANGE_SESSIONS"]
    target_session_inventory_lf_sha256: Literal[
        "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
    ]
    pre_window_prior_session: Literal["2021-02-19"]
    pre_window_prior_is_target_inventory_member: Literal[False]
    target_inventory_extension_allowed: Literal[False]
    first_target_session_exemption_allowed: Literal[False]


class NumericPolicyBundleV3(_StrictModel):
    state: Literal["OWNER_FROZEN_NON_EXECUTABLE_DATA_RESEARCH"]
    executable: Literal[False]
    pilot_review_disposition: Literal["APPROVE_EXACTLY_AS_DRAFTED"]
    executable_evidence_disposition: Literal["INSUFFICIENT_EVIDENCE_TO_APPROVE"]
    unknown_can_pass: Literal[False]
    numeric_check_order: tuple[str, ...]
    numeric_failure_collection: Literal["COLLECT_ALL_NUMERIC_FAILURES"]
    max_quote_age_seconds: NumericFieldPolicy
    max_relative_spread: NumericFieldPolicy
    min_open_interest: NumericFieldPolicy
    min_volume: NumericFieldPolicy

    @model_validator(mode="after")
    def validate_exact_values(self) -> Self:
        if self.numeric_check_order != (
            "max_quote_age_seconds",
            "max_relative_spread",
            "min_open_interest",
            "min_volume",
        ):
            raise ValueError("numeric check order drifted")
        expected = (
            (self.max_quote_age_seconds, Decimal(120), "seconds", "LESS_THAN_OR_EQUAL"),
            (self.max_relative_spread, Decimal("0.20"), "ratio", "LESS_THAN_OR_EQUAL"),
            (self.min_open_interest, Decimal(10), "contracts", "GREATER_THAN_OR_EQUAL"),
            (self.min_volume, Decimal(1), "contracts", "GREATER_THAN_OR_EQUAL"),
        )
        for item, value, unit, direction in expected:
            if item.value != value or item.unit != unit or item.direction != direction:
                raise ValueError("numeric pilot policy drifted")
        return self


class NumericThresholdBindingV3(_StrictModel):
    source: Literal["LOADED_V3_CONTRACT_NUMERIC_POLICY_ONLY"]
    caller_supplied_thresholds_allowed: Literal[False]
    synthetic_evidence_scope: Literal["SYNTHETIC_CONTRACT_TEST_ONLY"]
    real_evidence_outcome_without_executable_authority: Literal["AUTHORITY_UNAVAILABLE"]
    contract_canonical_sha_required: Literal[True]


class PitContractV3(_StrictModel):
    source_date_rule: Literal["EXACT_TARGET_EXCHANGE_SESSION_DATE"]
    cross_date_fallback_allowed: Literal[False]
    quote_volume_source_rule: Literal["SAME_TARGET_SESSION"]
    open_interest_source_rule: Literal[
        "FIRST_TARGET_USES_EXACT_PRE_WINDOW_PRIOR_THEN_PREVIOUS_TARGET_SESSION"
    ]
    pre_window_prior_session_rule: Literal[
        "EXACT_2021_02_19_SEPARATE_FROM_1202_TARGET_INVENTORY"
    ]
    quote_available_at_rule: Literal["QUOTE_AVAILABLE_AT_UTC_LE_DECISION_AS_OF_UTC"]
    volume_available_at_rule: Literal["VOLUME_AVAILABLE_AT_UTC_LE_DECISION_AS_OF_UTC"]
    open_interest_available_at_rule: Literal[
        "OPEN_INTEREST_AVAILABLE_AT_UTC_LE_DECISION_AS_OF_UTC"
    ]
    volume_semantics: Literal["DECISION_AS_OF_CUMULATIVE_SESSION_VOLUME"]
    end_of_day_or_revised_volume_allowed: Literal[False]
    missing_field_available_at_outcome: Literal["UNKNOWN"]
    future_field_available_at_outcome: Literal["INVALID"]
    expected_identity_fields: tuple[str, ...]
    expected_identity_values_bound_by_run_authority: Literal[True]
    identity_mismatch_outcome: Literal["INVALID"]

    @field_validator("expected_identity_fields")
    @classmethod
    def validate_identity_fields(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != _IDENTITY_FIELDS:
            raise ValueError("identity field inventory drifted")
        return value


class ContributorManifestContractV3(_StrictModel):
    expected_manifest_required_per_session: Literal[True]
    expected_contract_ids_sorted_unique: Literal[True]
    duplicate_contract_id_outcome: Literal["INVALID"]
    missing_or_unexpected_contributing_contract_outcome: Literal["INVALID"]
    excluded_row_reason_required: Literal[True]
    excluded_row_invalid_propagates_to_session: Literal[True]
    validation_before_exclusion_required: Literal[True]
    zero_expected_contributors_outcome: Literal["FAIL"]
    expected_nonempty_zero_observed_outcome: Literal["INVALID"]
    result_dependent_manifest_change_allowed: Literal[False]


class RunAuthorityContractV3(_StrictModel):
    schema_version: Literal["strategy_growth_action_value_dq_pit_run_authority.v3"]
    typed_authority_required: Literal[True]
    canonical_sha256_required: Literal[True]
    loose_sessions_identity_or_manifests_allowed: Literal[False]
    binds_target_sessions: Literal[True]
    binds_pre_window_prior_session: Literal[True]
    binds_expected_identity: Literal[True]
    binds_contributor_manifests_by_session: Literal[True]
    binds_evidence_scope: Literal[True]
    exact_target_and_manifest_session_set_required: Literal[True]


class StrategyGrowthActionValueDqPitContractV3(_CanonicalModel):
    schema_version: Literal["strategy_growth_action_value_canonical_dq_pit_contract.v3"]
    contract_id: Literal["strategy_growth_action_value_canonical_dq_pit_contract_v3"]
    contract_version: Literal["3.0.0"]
    status: Literal["OWNER_FROZEN_NON_EXECUTABLE_DATA_RESEARCH"]
    task_id: Literal[
        "TRADING-2542D_GROWTH_ACTION_VALUE_DQ_PIT_AND_SAMPLE_SEMANTICS_FREEZE_CORRECTION_V1"
    ]
    predecessor_binding: PredecessorBindingV3
    review_evidence: ReviewEvidenceV3
    review_state: ReviewStateV3
    scope_binding: ScopeBindingV3
    required_serial_contract_fields: tuple[str, ...]
    numeric_policy: NumericPolicyBundleV3
    numeric_threshold_binding: NumericThresholdBindingV3
    timestamp_contract: TimestampContract
    spread_contract: SpreadContract
    pit_contract: PitContractV3
    contributor_manifest_contract: ContributorManifestContractV3
    run_authority_contract: RunAuthorityContractV3
    run_control_contract: RunControlContract
    artifact_contract: ArtifactContract
    terminal_contract: SessionAndWindowTerminalContract
    safety: Safety

    @model_validator(mode="after")
    def validate_contract(self) -> Self:
        if self.required_serial_contract_fields != _SERIAL_FIELDS:
            raise ValueError("required serial contract fields drifted")
        if self.scope_binding.target_session_inventory_lf_sha256 != _SESSION_INVENTORY_SHA256:
            raise ValueError("target session inventory identity drifted")
        return self


@dataclass(frozen=True)
class StrategyGrowthActionValueDqPitContractV3LoadResult:
    contract: StrategyGrowthActionValueDqPitContractV3
    contract_path: Path
    contract_file_sha256: str
    contract_canonical_sha256: str
    predecessor: StrategyGrowthActionValueDqPitContractV2LoadResult


def load_strategy_growth_action_value_dq_pit_contract_v3(
    *,
    contract_path: Path = DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_DQ_PIT_CONTRACT_V3_PATH,
    project_root: Path = PROJECT_ROOT,
) -> StrategyGrowthActionValueDqPitContractV3LoadResult:
    try:
        path = _bound_file(contract_path, root=project_root, field="contract_path")
        raw = path.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(contract_path))
        contract = StrategyGrowthActionValueDqPitContractV3.model_validate(payload)
        predecessor = load_strategy_growth_action_value_dq_pit_contract_v2(
            contract_path=Path(contract.predecessor_binding.path), project_root=project_root
        )
        if predecessor.contract_file_sha256 != contract.predecessor_binding.file_sha256:
            raise ValueError("predecessor file SHA-256 mismatch")
        if predecessor.contract_canonical_sha256 != contract.predecessor_binding.canonical_sha256:
            raise ValueError("predecessor canonical SHA-256 mismatch")
    except StrategyGrowthActionValueDqPitContractV3Error:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise StrategyGrowthActionValueDqPitContractV3Error(
            "GROWTH_ACTION_VALUE_DQ_PIT_CONTRACT_V3_REJECTED", str(exc)
        ) from exc
    return StrategyGrowthActionValueDqPitContractV3LoadResult(
        contract=contract,
        contract_path=path,
        contract_file_sha256=hashlib.sha256(raw).hexdigest(),
        contract_canonical_sha256=contract.canonical_sha256,
        predecessor=predecessor,
    )


@dataclass(frozen=True)
class ContractObservationV3(ContractObservationV2):
    pass


SemanticStatusV3 = Literal["EXCLUDED", "INVALID", "UNKNOWN", "READY_FOR_NUMERIC_CHECK"]
EvidenceScopeV3 = Literal["SYNTHETIC_CONTRACT_TEST_ONLY", "REAL_EVIDENCE"]


@dataclass(frozen=True)
class SemanticEvaluationV3:
    contract_id: str
    session_date: date
    contributing_contract: bool
    status: SemanticStatusV3
    reasons: tuple[str, ...]
    quote_age_seconds: Decimal | None
    relative_spread: Decimal | None


@dataclass(frozen=True)
class ContractEvaluationV3:
    contract_id: str
    session_date: date
    contributing_contract: bool
    status: ContractTerminal
    reasons: tuple[str, ...]
    quote_age_seconds: Decimal | None
    relative_spread: Decimal | None


@dataclass(frozen=True)
class RunAuthorityPayloadV3:
    contract_canonical_sha256: str
    target_sessions: tuple[date, ...]
    pre_window_prior_session: date
    expected_identity: EvidenceIdentity
    contributor_manifests: tuple[SessionContributorManifest, ...]
    evidence_scope: EvidenceScopeV3


@dataclass(frozen=True)
class RunAuthorityV3:
    schema_version: Literal["strategy_growth_action_value_dq_pit_run_authority.v3"]
    payload: RunAuthorityPayloadV3
    canonical_sha256: str


def _identity_payload(identity: EvidenceIdentity) -> dict[str, str | None]:
    return {field: getattr(identity, field) for field in _IDENTITY_FIELDS}


def _run_authority_body(payload: RunAuthorityPayloadV3) -> dict[str, object]:
    return {
        "schema_version": "strategy_growth_action_value_dq_pit_run_authority_payload.v3",
        "contract_canonical_sha256": payload.contract_canonical_sha256,
        "target_sessions": [item.isoformat() for item in payload.target_sessions],
        "target_session_inventory_lf_sha256": session_inventory_lf_sha256(
            payload.target_sessions
        ),
        "pre_window_prior_session": payload.pre_window_prior_session.isoformat(),
        "expected_identity": _identity_payload(payload.expected_identity),
        "contributor_manifests": [
            {
                "session_date": item.session_date.isoformat(),
                "expected_contract_ids": list(item.expected_contract_ids),
                "contract_ids_lf_sha256": item.contract_ids_lf_sha256,
            }
            for item in payload.contributor_manifests
        ],
        "evidence_scope": payload.evidence_scope,
    }


def _validate_authority_payload(payload: RunAuthorityPayloadV3) -> None:
    if payload.contract_canonical_sha256 != _CONTRACT_CANONICAL_SHA256:
        raise ValueError("run authority contract canonical SHA-256 mismatch")
    sessions = payload.target_sessions
    if tuple(sorted(set(sessions))) != sessions:
        raise ValueError("target sessions must be sorted unique")
    if (
        len(sessions) != _EXPECTED_SESSION_COUNT
        or not sessions
        or sessions[0] != _TARGET_START
        or sessions[-1] != _TARGET_END
        or session_inventory_lf_sha256(sessions) != _SESSION_INVENTORY_SHA256
    ):
        raise ValueError("exact 1202 target session inventory mismatch")
    if payload.pre_window_prior_session != _PRE_WINDOW_PRIOR:
        raise ValueError("pre-window prior session mismatch")
    if payload.pre_window_prior_session in sessions:
        raise ValueError("pre-window prior cannot enter target inventory")
    identity_values = payload.expected_identity.values()
    if any(value is None or not value.strip() for value in identity_values):
        raise ValueError("expected identity authority is incomplete")
    manifests = payload.contributor_manifests
    manifest_dates = tuple(item.session_date for item in manifests)
    if manifest_dates != sessions:
        raise ValueError("contributor manifest session set or order mismatch")
    for manifest in manifests:
        expected_ids = manifest.expected_contract_ids
        if expected_ids != tuple(sorted(set(expected_ids))):
            raise ValueError("expected contract ids must be sorted unique")
        if any(not item.strip() for item in expected_ids):
            raise ValueError("expected contract id cannot be empty")
        if contributor_manifest_lf_sha256(expected_ids) != manifest.contract_ids_lf_sha256:
            raise ValueError("contributor manifest SHA-256 mismatch")


def build_run_authority_v3(
    contract: StrategyGrowthActionValueDqPitContractV3,
    *,
    target_sessions: Sequence[date],
    pre_window_prior_session: date,
    expected_identity: EvidenceIdentity,
    contributor_manifests: Sequence[SessionContributorManifest],
    evidence_scope: EvidenceScopeV3,
) -> RunAuthorityV3:
    if contract.scope_binding.target_session_inventory_lf_sha256 != _SESSION_INVENTORY_SHA256:
        raise ValueError("contract target inventory authority drifted")
    payload = RunAuthorityPayloadV3(
        contract_canonical_sha256=contract.canonical_sha256,
        target_sessions=tuple(target_sessions),
        pre_window_prior_session=pre_window_prior_session,
        expected_identity=expected_identity,
        contributor_manifests=tuple(contributor_manifests),
        evidence_scope=evidence_scope,
    )
    _validate_authority_payload(payload)
    canonical_sha256 = hashlib.sha256(
        _canonical_json_bytes(_run_authority_body(payload))
    ).hexdigest()
    return RunAuthorityV3(
        schema_version="strategy_growth_action_value_dq_pit_run_authority.v3",
        payload=payload,
        canonical_sha256=canonical_sha256,
    )


def validate_run_authority_v3(authority: RunAuthorityV3) -> None:
    if authority.schema_version != "strategy_growth_action_value_dq_pit_run_authority.v3":
        raise ValueError("run authority schema mismatch")
    _validate_authority_payload(authority.payload)
    expected_sha = hashlib.sha256(
        _canonical_json_bytes(_run_authority_body(authority.payload))
    ).hexdigest()
    if authority.canonical_sha256 != expected_sha or not _SHA256_PATTERN.fullmatch(
        authority.canonical_sha256
    ):
        raise ValueError("run authority canonical SHA-256 mismatch")


def expected_prior_session_v3(session_date: date, authority: RunAuthorityV3) -> date:
    validate_run_authority_v3(authority)
    sessions = authority.payload.target_sessions
    try:
        index = sessions.index(session_date)
    except ValueError as exc:
        raise ValueError("observation session is outside target inventory") from exc
    return authority.payload.pre_window_prior_session if index == 0 else sessions[index - 1]


def _is_utc_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() == timedelta(0)


def _decimal_seconds(value: timedelta) -> Decimal:
    return (
        Decimal(value.days) * Decimal(86400)
        + Decimal(value.seconds)
        + Decimal(value.microseconds) / Decimal(1_000_000)
    )


def evaluate_contract_semantics_v3(
    observation: ContractObservationV3,
    *,
    authority: RunAuthorityV3,
) -> SemanticEvaluationV3:
    try:
        validate_run_authority_v3(authority)
    except ValueError as exc:
        return SemanticEvaluationV3(
            observation.contract_id,
            observation.session_date,
            observation.contributing_contract,
            "INVALID",
            (f"RUN_AUTHORITY_INVALID:{exc}",),
            None,
            None,
        )
    invalid: list[str] = []
    unknown: list[str] = []
    quote_age: Decimal | None = None
    relative_spread: Decimal | None = None

    if not observation.contract_id.strip():
        invalid.append("CONTRACT_ID_MISSING")
    if observation.evidence_scope != authority.payload.evidence_scope:
        invalid.append("EVIDENCE_SCOPE_AUTHORITY_MISMATCH")
    if not observation.contributing_contract and (
        observation.exclusion_reason is None or not observation.exclusion_reason.strip()
    ):
        invalid.append("NONCONTRIBUTING_EXCLUSION_REASON_MISSING")

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

    for label, available_at in (
        ("QUOTE", observation.quote_available_at_utc),
        ("VOLUME", observation.volume_available_at_utc),
        ("OPEN_INTEREST", observation.open_interest_available_at_utc),
    ):
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

    try:
        prior_session = expected_prior_session_v3(observation.session_date, authority)
    except ValueError:
        invalid.append("TARGET_SESSION_INVENTORY_MISMATCH")
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
    if observation.open_interest_session_date is None:
        unknown.append("OPEN_INTEREST_SESSION_DATE_MISSING")
    elif prior_session is not None and observation.open_interest_session_date != prior_session:
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
    expected_identity = authority.payload.expected_identity.values()
    if any(value is None or not value.strip() for value in actual_identity):
        unknown.append("ACTUAL_IDENTITY_FIELD_MISSING")
    elif actual_identity != expected_identity:
        invalid.append("EVIDENCE_IDENTITY_MISMATCH")

    if invalid:
        status: SemanticStatusV3 = "INVALID"
        reasons = tuple(dict.fromkeys((*invalid, *unknown)))
    elif not observation.contributing_contract:
        status = "EXCLUDED"
        reasons = ("NONCONTRIBUTING_CONTRACT_EXCLUDED_AFTER_VALIDATION",)
    elif unknown:
        status = "UNKNOWN"
        reasons = tuple(dict.fromkeys(unknown))
    else:
        status = "READY_FOR_NUMERIC_CHECK"
        reasons = ("SEMANTIC_CONTRACT_V3_PASS",)
    return SemanticEvaluationV3(
        observation.contract_id,
        observation.session_date,
        observation.contributing_contract,
        status,
        reasons,
        quote_age,
        relative_spread,
    )


def evaluate_contract_v3(
    contract: StrategyGrowthActionValueDqPitContractV3,
    observation: ContractObservationV3,
    *,
    authority: RunAuthorityV3,
) -> ContractEvaluationV3:
    semantic = evaluate_contract_semantics_v3(observation, authority=authority)
    if semantic.status != "READY_FOR_NUMERIC_CHECK":
        return ContractEvaluationV3(
            semantic.contract_id,
            semantic.session_date,
            semantic.contributing_contract,
            semantic.status,
            semantic.reasons,
            semantic.quote_age_seconds,
            semantic.relative_spread,
        )
    if authority.payload.evidence_scope != "SYNTHETIC_CONTRACT_TEST_ONLY":
        return ContractEvaluationV3(
            semantic.contract_id,
            semantic.session_date,
            True,
            "AUTHORITY_UNAVAILABLE",
            ("REAL_EVIDENCE_NUMERIC_AUTHORITY_UNAVAILABLE",),
            semantic.quote_age_seconds,
            semantic.relative_spread,
        )
    if contract.numeric_policy.executable or contract.review_state.executable_authority:
        raise StrategyGrowthActionValueDqPitContractV3Error(
            "UNREVIEWED_EXECUTABLE_AUTHORITY", "draft V3 cannot expose executable authority"
        )
    assert semantic.quote_age_seconds is not None
    assert semantic.relative_spread is not None
    assert observation.open_interest is not None
    assert observation.volume is not None
    misses: list[str] = []
    if semantic.quote_age_seconds > contract.numeric_policy.max_quote_age_seconds.value:
        misses.append("QUOTE_AGE_ABOVE_MAXIMUM")
    if semantic.relative_spread > contract.numeric_policy.max_relative_spread.value:
        misses.append("RELATIVE_SPREAD_ABOVE_MAXIMUM")
    if Decimal(observation.open_interest) < contract.numeric_policy.min_open_interest.value:
        misses.append("OPEN_INTEREST_BELOW_MINIMUM")
    if Decimal(observation.volume) < contract.numeric_policy.min_volume.value:
        misses.append("VOLUME_BELOW_MINIMUM")
    return ContractEvaluationV3(
        semantic.contract_id,
        semantic.session_date,
        True,
        "FAIL" if misses else "PASS",
        tuple(misses) if misses else ("SYNTHETIC_NUMERIC_CONTRACT_V3_PASS",),
        semantic.quote_age_seconds,
        semantic.relative_spread,
    )


def _manifest_for_session(
    authority: RunAuthorityV3, session_date: date
) -> SessionContributorManifest:
    for manifest in authority.payload.contributor_manifests:
        if manifest.session_date == session_date:
            return manifest
    raise ValueError("session contributor manifest missing")


def aggregate_session_v3(
    session_date: date,
    evaluations: Sequence[ContractEvaluationV3],
    *,
    authority: RunAuthorityV3,
) -> SessionEvaluation:
    try:
        validate_run_authority_v3(authority)
        manifest = _manifest_for_session(authority, session_date)
    except ValueError as exc:
        return SessionEvaluation(
            session_date,
            "INVALID",
            0,
            0,
            (),
            (f"RUN_AUTHORITY_INVALID:{exc}",),
        )
    values = tuple(evaluations)
    reasons: list[str] = []
    if any(item.session_date != session_date for item in values):
        reasons.append("CONTRACT_SESSION_DATE_MISMATCH")
    observed_ids = tuple(item.contract_id for item in values)
    if len(set(observed_ids)) != len(observed_ids):
        reasons.append("DUPLICATE_CONTRACT_ID")
    contributing = tuple(item for item in values if item.contributing_contract)
    contributing_ids = tuple(sorted(item.contract_id for item in contributing))
    expected = manifest.expected_contract_ids
    if contributing_ids != expected:
        reasons.append("CONTRIBUTOR_SET_MISMATCH")
    if expected and not contributing:
        reasons.append("EXPECTED_NONEMPTY_ZERO_OBSERVED_CONTRIBUTORS")
    counts = Counter(item.status for item in values)
    terminal_counts = tuple(sorted(counts.items()))
    excluded_count = sum(item.status == "EXCLUDED" for item in values)

    if reasons or any(item.status == "INVALID" for item in values):
        status: SessionTerminalStatus = "INVALID"
        if not reasons:
            reasons.append("ANY_CONTRACT_INVALID_INCLUDING_EXCLUDED_ROW")
    elif not expected:
        status = "FAIL"
        reasons.append("ZERO_EXPECTED_CONTRIBUTORS")
    elif any(item.status == "FAIL" for item in contributing):
        status = "FAIL"
        reasons.append("ANY_CONTRIBUTING_CONTRACT_FAIL")
    elif any(item.status in {"UNKNOWN", "AUTHORITY_UNAVAILABLE"} for item in contributing):
        status = "INSUFFICIENT"
        reasons.append("ANY_CONTRIBUTING_CONTRACT_INSUFFICIENT")
    elif contributing and all(item.status == "PASS" for item in contributing):
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


def aggregate_window_v3(
    sessions: Sequence[SessionEvaluation], *, authority: RunAuthorityV3
) -> WindowEvaluation:
    try:
        validate_run_authority_v3(authority)
    except ValueError as exc:
        return WindowEvaluation(
            "GLOBAL_INVALID", len(tuple(sessions)), _EXPECTED_SESSION_COUNT, (),
            (f"RUN_AUTHORITY_INVALID:{exc}",),
        )
    return aggregate_window(
        sessions,
        expected_sessions=authority.payload.target_sessions,
        expected_session_count=_EXPECTED_SESSION_COUNT,
        expected_session_inventory_lf_sha256=_SESSION_INVENTORY_SHA256,
    )


RunControlStatusV3 = Literal["CONTINUE_FIXED_INVENTORY", "HARD_STOP_INVALID"]


def in_run_control_action_v3(evaluation: ContractEvaluationV3) -> RunControlStatusV3:
    return "HARD_STOP_INVALID" if evaluation.status == "INVALID" else "CONTINUE_FIXED_INVENTORY"


def pre_run_authority_state_v3(
    contract: StrategyGrowthActionValueDqPitContractV3,
) -> Literal["INSUFFICIENT_EVIDENCE_TO_RUN_DQ"]:
    if contract.review_state.executable_authority or contract.numeric_policy.executable:
        raise StrategyGrowthActionValueDqPitContractV3Error(
            "UNREVIEWED_EXECUTABLE_AUTHORITY", "draft V3 must remain non-executable"
        )
    return "INSUFFICIENT_EVIDENCE_TO_RUN_DQ"


__all__ = [
    "ContractEvaluationV3",
    "ContractObservationV3",
    "DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_DQ_PIT_CONTRACT_V3_PATH",
    "EvidenceIdentity",
    "RunAuthorityPayloadV3",
    "RunAuthorityV3",
    "SemanticEvaluationV3",
    "SessionContributorManifest",
    "StrategyGrowthActionValueDqPitContractV3",
    "StrategyGrowthActionValueDqPitContractV3Error",
    "StrategyGrowthActionValueDqPitContractV3LoadResult",
    "aggregate_session_v3",
    "aggregate_window_v3",
    "build_run_authority_v3",
    "contributor_manifest_lf_sha256",
    "evaluate_contract_semantics_v3",
    "evaluate_contract_v3",
    "expected_prior_session_v3",
    "in_run_control_action_v3",
    "load_strategy_growth_action_value_dq_pit_contract_v3",
    "pre_run_authority_state_v3",
    "session_inventory_lf_sha256",
    "validate_run_authority_v3",
]
