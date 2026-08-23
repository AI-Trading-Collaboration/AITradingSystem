from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_POLICY_PATH = Path(
    "config/research/strategy_growth_action_value_preregistration_v1.yaml"
)

_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")


class StrategyGrowthActionValuePreregistrationError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class DataEvidenceLane(StrEnum):
    QLD_CANONICAL_FULL_CACHE_DQ = "QLD_CANONICAL_FULL_CACHE_DQ"
    QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE = (
        "QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE"
    )


class PreregistrationStatus(StrEnum):
    PREREGISTRATION_FROZEN_AWAITING_DQ = "PREREGISTRATION_FROZEN_AWAITING_DQ"
    BLOCKED_POLICY_INPUT = "BLOCKED_POLICY_INPUT"


class ThresholdPolicyStatus(StrEnum):
    NOT_PROVIDED = "NOT_PROVIDED"
    REVIEWED = "REVIEWED"


class TerminalOutcome(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    INSUFFICIENT = "INSUFFICIENT"
    INVALID = "INVALID"


class MandatoryAxis(StrEnum):
    NON_BETA_ACTION_VALUE = "NON_BETA_ACTION_VALUE"
    NET_OF_COST_RETURN = "NET_OF_COST_RETURN"
    ACTUAL_PATH_DRAWDOWN_REGRESSION = "ACTUAL_PATH_DRAWDOWN_REGRESSION"
    FALSE_RISK_OFF_COST = "FALSE_RISK_OFF_COST"
    CANONICAL_DQ_PIT = "CANONICAL_DQ_PIT"
    SAMPLE_AND_WINDOW_DEPENDENCE = "SAMPLE_AND_WINDOW_DEPENDENCE"
    ACTUAL_PATH_TURNOVER = "ACTUAL_PATH_TURNOVER"
    LEVERAGE_BETA_ATTRIBUTION = "LEVERAGE_BETA_ATTRIBUTION"


class StopAction(StrEnum):
    DISCARD_ALL_EMPIRICAL_CONCLUSIONS_REQUIRE_NEW_OWNER_AUTHORIZATION = (
        "DISCARD_ALL_EMPIRICAL_CONCLUSIONS_REQUIRE_NEW_OWNER_AUTHORIZATION"
    )
    RETIRE_CURRENT_HYPOTHESIS_VERSION_NO_PARAMETER_RESCUE = (
        "RETIRE_CURRENT_HYPOTHESIS_VERSION_NO_PARAMETER_RESCUE"
    )
    COLLECT_ONLY_PREREGISTERED_MISSING_EVIDENCE = (
        "COLLECT_ONLY_PREREGISTERED_MISSING_EVIDENCE"
    )
    READY_FOR_OWNER_GROWTH_HYPOTHESIS_REVIEW_NOT_PROMOTION = (
        "READY_FOR_OWNER_GROWTH_HYPOTHESIS_REVIEW_NOT_PROMOTION"
    )


_AXIS_ORDER = (
    MandatoryAxis.NON_BETA_ACTION_VALUE,
    MandatoryAxis.NET_OF_COST_RETURN,
    MandatoryAxis.ACTUAL_PATH_DRAWDOWN_REGRESSION,
    MandatoryAxis.FALSE_RISK_OFF_COST,
    MandatoryAxis.CANONICAL_DQ_PIT,
    MandatoryAxis.SAMPLE_AND_WINDOW_DEPENDENCE,
    MandatoryAxis.ACTUAL_PATH_TURNOVER,
    MandatoryAxis.LEVERAGE_BETA_ATTRIBUTION,
)
_OUTCOME_PRIORITY = (
    TerminalOutcome.INVALID,
    TerminalOutcome.FAIL,
    TerminalOutcome.INSUFFICIENT,
    TerminalOutcome.PASS,
)
_STOP_ACTION_ORDER = (
    StopAction.DISCARD_ALL_EMPIRICAL_CONCLUSIONS_REQUIRE_NEW_OWNER_AUTHORIZATION,
    StopAction.RETIRE_CURRENT_HYPOTHESIS_VERSION_NO_PARAMETER_RESCUE,
    StopAction.COLLECT_ONLY_PREREGISTERED_MISSING_EVIDENCE,
    StopAction.READY_FOR_OWNER_GROWTH_HYPOTHESIS_REVIEW_NOT_PROMOTION,
)
_AUTHORITY_ORDER = (
    "PROJECT_ENGINEERING_RULES",
    "TRADING_2515_READINESS_POLICY",
    "TRADING_2515_READINESS_REQUIREMENT",
    "TRADING_2516_QQQ_OPTIONS_LANE_SELECTION",
    "TRADING_2541_EXACT_DATE_RECOVERY_TERMINAL",
    "SIMPLE_BASELINE_EQUAL_RISK_QQQ_SGOV",
    "TWO_LAYER_STRATEGY_BOUNDARY",
    "FIRST_LAYER_CHANNEL_CLOSEOUT",
    "DEFENSIVE_LANE_CLOSEOUT",
    "TWO_LANE_OPTIMIZATION_CLOSEOUT",
    "QLD_ROLE_LIMITED_IMPLEMENTATION_POLICY",
    "TRADING_2458_RETIRED_CANDIDATE_FAMILY",
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _PolicyModel(BaseModel):
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


def _sha256(value: str, field: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase SHA-256")
    return value


def _identifier(value: str, field: str) -> str:
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a stable identifier")
    return value


def _relative_path(value: str, field: str) -> str:
    path = Path(value)
    if not value or path.is_absolute() or path.drive or ".." in path.parts:
        raise ValueError(f"{field} must be a bounded project-relative path")
    if path.as_posix() != value:
        raise ValueError(f"{field} must use normalized forward slashes")
    return value


def _utc(value: datetime, field: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field} must use UTC")
    return value.astimezone(UTC)


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
        if isinstance(current, Mapping):
            if part not in current:
                raise ValueError(f"semantic fact path is missing: {dotted_path}")
            current = current[part]
            continue
        if isinstance(current, list) and part.isdigit():
            index = int(part)
            if index >= len(current):
                raise ValueError(f"semantic fact list index is missing: {dotted_path}")
            current = current[index]
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
    owner: Literal["project_owner_and_strategy_research_governance"]
    rationale: str
    intended_effect: str
    validation_evidence: str
    review_condition: str

    @field_validator("rationale", "intended_effect", "validation_evidence", "review_condition")
    @classmethod
    def _nonempty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("policy metadata text cannot be empty")
        return value


class GrowthHypothesisPolicy(_PolicyModel):
    hypothesis_id: Literal["BASELINE_BOUNDED_QQQ_GROWTH_OVERLAY_NON_BETA_ACTION_VALUE_V1"]
    mechanism_scope: Literal["SINGLE_GROWTH_STATE_BOUNDED_QQQ_SGOV_REALLOCATION"]
    baseline_id: Literal["equal_risk_qqq_sgov"]
    comparator_ids: tuple[
        Literal["equal_risk_qqq_sgov"], Literal["exposure_matched_no_signal"]
    ]
    action_universe: tuple[Literal["QQQ"], Literal["SGOV"]]
    action_count: Literal[1]
    candidate_count: Literal[1]
    uses_leverage_etf: Literal[False]
    uses_options: Literal[False]
    borrowed_leverage_allowed: Literal[False]
    candidate_search_allowed: Literal[False]
    parameter_search_allowed: Literal[False]
    risk_veto_priority: Literal["highest"]
    defense_is_independent_hard_gate: Literal[True]
    growth_can_modify_defensive_policy: Literal[False]
    growth_can_emit_official_weights: Literal[False]


class DataLanePolicy(_PolicyModel):
    selected_data_lane: Literal[
        DataEvidenceLane.QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE
    ]
    selection_status: Literal["OWNER_RETAINED_NOT_EXECUTABLE"]
    qqq_options_lane_selected: Literal[True]
    transport_completeness_status: Literal["RECOVERED_COMPLETE_NOT_DQ_PIT_PROMOTED"]
    expected_session_count: Literal[1202]
    observed_session_count: Literal[1202]
    exact_date_recovery_session_count: Literal[1]
    unresolved_session_count: Literal[0]
    dq_pit_promoted: Literal[False]
    data_lane_execution_authorized: Literal[False]
    cache_mutation_authorized: Literal[False]
    proves_growth_action_value: Literal[False]
    proves_qld_investment_value: Literal[False]


class ThresholdPolicy(_PolicyModel):
    status: Literal[ThresholdPolicyStatus.NOT_PROVIDED]
    reviewed_policy_refs: tuple[str, ...]
    threshold_after_result_allowed: Literal[False]

    @model_validator(mode="after")
    def _unprovided_has_no_refs(self) -> Self:
        if self.reviewed_policy_refs:
            raise ValueError("NOT_PROVIDED threshold policy cannot contain reviewed refs")
        return self


class EvaluationAxisPolicy(_PolicyModel):
    axis_id: MandatoryAxis
    reviewed_policy_refs: tuple[str, ...]
    pass_rule_code: str
    fail_rule_code: str
    insufficient_rule_code: str
    invalid_rule_code: str

    @field_validator(
        "pass_rule_code", "fail_rule_code", "insufficient_rule_code", "invalid_rule_code"
    )
    @classmethod
    def _rule_code(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))


class TerminalStopRule(_PolicyModel):
    outcome: TerminalOutcome
    action: StopAction


class TerminalOutcomePolicy(_PolicyModel):
    aggregation_priority: tuple[TerminalOutcome, ...]
    stop_rules: tuple[TerminalStopRule, ...]

    @model_validator(mode="after")
    def _frozen_order(self) -> Self:
        if self.aggregation_priority != _OUTCOME_PRIORITY:
            raise ValueError("terminal outcome priority drifted")
        if tuple(item.outcome for item in self.stop_rules) != _OUTCOME_PRIORITY:
            raise ValueError("terminal stop outcome order drifted")
        if tuple(item.action for item in self.stop_rules) != _STOP_ACTION_ORDER:
            raise ValueError("terminal stop action mapping drifted")
        return self


class AuthoritySemanticFact(_PolicyModel):
    dotted_path: str
    expected_json: str

    @field_validator("dotted_path")
    @classmethod
    def _path(cls, value: str) -> str:
        if not value or value.startswith(".") or value.endswith(".") or ".." in value:
            raise ValueError("dotted_path must be a stable mapping path")
        return value

    @field_validator("expected_json")
    @classmethod
    def _expected_json(cls, value: str) -> str:
        parsed = json.loads(value)
        canonical = json.dumps(parsed, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if value != canonical:
            raise ValueError("expected_json must use compact canonical JSON")
        return value


class GrowthAuthorityBinding(_PolicyModel):
    authority_id: str
    path: str
    format: Literal["YAML", "TEXT"]
    file_sha256: str
    semantic_facts: tuple[AuthoritySemanticFact, ...]
    required_snippets: tuple[str, ...]

    @field_validator("authority_id")
    @classmethod
    def _authority_id(cls, value: str) -> str:
        return _identifier(value, "authority_id")

    @field_validator("path")
    @classmethod
    def _path(cls, value: str) -> str:
        return _relative_path(value, "path")

    @field_validator("file_sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, "file_sha256")

    @model_validator(mode="after")
    def _format_contract(self) -> Self:
        if self.format == "YAML" and (not self.semantic_facts or self.required_snippets):
            raise ValueError("YAML authority requires facts and forbids text snippets")
        if self.format == "TEXT" and (self.semantic_facts or not self.required_snippets):
            raise ValueError("TEXT authority requires snippets and forbids semantic facts")
        if len(self.semantic_facts) != len({fact.dotted_path for fact in self.semantic_facts}):
            raise ValueError("semantic fact paths must be unique")
        if len(self.required_snippets) != len(set(self.required_snippets)):
            raise ValueError("required snippets must be unique")
        return self


class PreregistrationSafety(_PolicyModel):
    empirical_research_authorized: Literal[False]
    candidate_search_authorized: Literal[False]
    parameter_search_authorized: Literal[False]
    backtest_authorized: Literal[False]
    holdout_access_authorized: Literal[False]
    investment_conclusion_authorized: Literal[False]
    data_lane_execution_authorized: Literal[False]
    cache_mutation_authorized: Literal[False]
    external_action_authorized: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class StrategyGrowthActionValuePreregistrationPolicy(_PolicyModel):
    schema_version: Literal["strategy_growth_action_value_preregistration_policy.v1"]
    policy_id: Literal["strategy_growth_action_value_preregistration_v1"]
    policy_version: Literal["1.0.0"]
    policy_status: Literal["REVIEWED_BASELINE_BLOCKED_POLICY_INPUT"]
    task_id: Literal[
        "TRADING-2540_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_AND_SINGLE_LANE_DECISION_V1"
    ]
    owner_decision: Literal[
        "owner_decision:TRADING-2540:2026-08-23:retain_qqq_options_lane_and_remove_qld_selected_lane_semantics_v1"
    ]
    policy_metadata: PolicyMetadata
    primary_research_start: date
    primary_research_role: Literal["PRIMARY"]
    prohibited_default_start: date
    preregistration_stage: Literal["PREREGISTRATION_ONLY"]
    hypothesis: GrowthHypothesisPolicy
    data_lane: DataLanePolicy
    threshold_policy: ThresholdPolicy
    mandatory_axes: tuple[EvaluationAxisPolicy, ...]
    terminal_outcome_policy: TerminalOutcomePolicy
    authorities: tuple[GrowthAuthorityBinding, ...]
    safety: PreregistrationSafety

    @model_validator(mode="after")
    def _frozen_baseline(self) -> Self:
        if self.primary_research_start != date(2021, 2, 22):
            raise ValueError("PRIMARY research start must remain 2021-02-22")
        if self.prohibited_default_start != date(2022, 12, 1):
            raise ValueError("historical 2022-12-01 boundary must remain explicitly non-default")
        if tuple(item.axis_id for item in self.mandatory_axes) != _AXIS_ORDER:
            raise ValueError("mandatory evaluation axis inventory or order drifted")
        if tuple(item.authority_id for item in self.authorities) != _AUTHORITY_ORDER:
            raise ValueError("growth preregistration authority inventory or order drifted")
        if any(item.reviewed_policy_refs for item in self.mandatory_axes):
            raise ValueError("v1 blocked baseline cannot claim reviewed threshold policy refs")
        return self

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


class AuthorityObservation(_StrictModel):
    authority_id: str
    path: str
    file_sha256: str
    semantic_fact_count: int
    required_snippet_count: int
    identity_verified: Literal[True]
    semantics_verified: Literal[True]

    @field_validator("authority_id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _identifier(value, "authority_id")

    @field_validator("path")
    @classmethod
    def _path(cls, value: str) -> str:
        return _relative_path(value, "path")

    @field_validator("file_sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, "file_sha256")

    @model_validator(mode="after")
    def _counts(self) -> Self:
        if self.semantic_fact_count < 0 or self.required_snippet_count < 0:
            raise ValueError("authority verification counts cannot be negative")
        if self.semantic_fact_count + self.required_snippet_count == 0:
            raise ValueError("authority observation must verify semantics")
        return self


@dataclass(frozen=True)
class StrategyGrowthActionValuePreregistrationPolicyLoadResult:
    policy: StrategyGrowthActionValuePreregistrationPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str
    authority_observations: tuple[AuthorityObservation, ...]
    authority_set_sha256: str


class PreregistrationActionRequest(_StrictModel):
    empirical_research: bool = False
    candidate_search: bool = False
    parameter_search: bool = False
    backtest: bool = False
    holdout_access: bool = False
    qld_data_lane_execution: bool = False
    qqq_options_lane_execution: bool = False
    cache_mutation: bool = False
    external_action: bool = False
    investment_conclusion: bool = False
    paper: bool = False
    live: bool = False
    broker: bool = False
    production: bool = False
    use_leverage_etf: bool = False
    use_options: bool = False
    use_retired_family: bool = False
    threshold_after_result: bool = False

    @property
    def requested_actions(self) -> tuple[str, ...]:
        return tuple(name for name, value in self.model_dump(mode="python").items() if value)


class MandatoryAxisOutcome(_StrictModel):
    axis_id: MandatoryAxis
    outcome: TerminalOutcome
    reason_codes: tuple[str, ...] = ()

    @field_validator("reason_codes")
    @classmethod
    def _reason_codes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("mandatory-axis reason codes must be unique")
        return tuple(_identifier(item, "reason_code") for item in value)


def aggregate_mandatory_axis_outcomes(
    outcomes: tuple[MandatoryAxisOutcome, ...],
) -> TerminalOutcome:
    if len(outcomes) != len(_AXIS_ORDER):
        raise StrategyGrowthActionValuePreregistrationError(
            "MANDATORY_AXIS_OUTCOME_SET_INVALID", "all mandatory axes are required exactly once"
        )
    observed_axes = tuple(item.axis_id for item in outcomes)
    if len(set(observed_axes)) != len(_AXIS_ORDER) or set(observed_axes) != set(_AXIS_ORDER):
        raise StrategyGrowthActionValuePreregistrationError(
            "MANDATORY_AXIS_OUTCOME_SET_INVALID", "axis inventory is missing or duplicated"
        )
    for outcome in _OUTCOME_PRIORITY:
        if any(item.outcome is outcome for item in outcomes):
            return outcome
    raise StrategyGrowthActionValuePreregistrationError(
        "MANDATORY_AXIS_OUTCOME_SET_INVALID", "no terminal outcome was provided"
    )


class StrategyGrowthActionValuePreregistrationDecision(_StrictModel):
    schema_version: Literal["strategy_growth_action_value_preregistration_decision.v1"]
    decision_id: str
    evaluated_at_utc: datetime
    policy_file_sha256: str
    policy_canonical_sha256: str
    authority_set_sha256: str
    authority_observations: tuple[AuthorityObservation, ...]
    preregistration_status: Literal[PreregistrationStatus.BLOCKED_POLICY_INPUT]
    primary_research_start: date
    primary_research_role: Literal["PRIMARY"]
    prohibited_default_start: date
    preregistration_stage: Literal["PREREGISTRATION_ONLY"]
    hypothesis: GrowthHypothesisPolicy
    data_lane: DataLanePolicy
    threshold_policy: ThresholdPolicy
    mandatory_axes: tuple[EvaluationAxisPolicy, ...]
    terminal_outcome_policy: TerminalOutcomePolicy
    downstream_gate: Literal["OWNER_REVIEWED_THRESHOLD_POLICY_REQUIRED"]
    safety: PreregistrationSafety
    content_sha256: str

    @field_validator("decision_id")
    @classmethod
    def _id(cls, value: str) -> str:
        return _identifier(value, "decision_id")

    @field_validator(
        "policy_file_sha256", "policy_canonical_sha256", "authority_set_sha256", "content_sha256"
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("evaluated_at_utc")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value, "evaluated_at_utc")

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self.semantic_payload())).hexdigest()

    @model_validator(mode="after")
    def _seal_and_boundary(self, info: ValidationInfo) -> Self:
        if (
            info.context
            and info.context.get("allow_unsealed")
            and self.content_sha256 == _UNSEALED_SHA256
        ):
            return self
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("semantic content SHA-256 mismatch")
        if tuple(item.authority_id for item in self.authority_observations) != _AUTHORITY_ORDER:
            raise ValueError("decision authority inventory drifted")
        if tuple(item.axis_id for item in self.mandatory_axes) != _AXIS_ORDER:
            raise ValueError("decision mandatory axis inventory drifted")
        if self.threshold_policy.status is not ThresholdPolicyStatus.NOT_PROVIDED:
            raise ValueError("v1 decision cannot claim reviewed threshold policy")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def seal(cls, **payload: object) -> Self:
        try:
            candidate = cls.model_validate(
                {**payload, "content_sha256": _UNSEALED_SHA256},
                context={"allow_unsealed": True},
            )
            return cls.model_validate(
                {**payload, "content_sha256": candidate.compute_content_sha256()}
            )
        except (TypeError, ValueError) as exc:
            raise StrategyGrowthActionValuePreregistrationError(
                "GROWTH_PREREGISTRATION_DECISION_PAYLOAD_INVALID", str(exc)
            ) from exc

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            payload = _duplicate_key_rejecting_json(raw)
            if not isinstance(payload, dict):
                raise TypeError("decision JSON root must be an object")
            decision = cls.model_validate_json(raw)
            if raw != decision.canonical_bytes:
                raise ValueError("decision is not canonical JSON bytes")
            return decision
        except (TypeError, ValueError) as exc:
            raise StrategyGrowthActionValuePreregistrationError(
                "GROWTH_PREREGISTRATION_DECISION_RECORD_INVALID", str(exc)
            ) from exc


def load_strategy_growth_action_value_preregistration_policy(
    *,
    policy_path: Path = DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> StrategyGrowthActionValuePreregistrationPolicyLoadResult:
    try:
        path = _bound_file(policy_path, root=project_root, field="policy_path")
        raw = path.read_bytes()
        policy_payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(policy_path))
        policy = StrategyGrowthActionValuePreregistrationPolicy.model_validate(policy_payload)
        observations: list[AuthorityObservation] = []
        for binding in policy.authorities:
            authority_path = _bound_file(
                Path(binding.path), root=project_root, field=f"authority:{binding.authority_id}"
            )
            authority_raw = authority_path.read_bytes()
            actual_sha256 = hashlib.sha256(authority_raw).hexdigest()
            if actual_sha256 != binding.file_sha256:
                raise ValueError(
                    f"authority file SHA-256 mismatch: {binding.authority_id}"
                )
            authority_text = authority_raw.decode("utf-8")
            if binding.format == "YAML":
                authority_payload = load_strict_yaml_text(
                    authority_text, label=binding.path
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
            else:
                for snippet in binding.required_snippets:
                    if snippet not in authority_text:
                        raise ValueError(
                            f"authority required text is missing: {binding.authority_id}"
                        )
            observations.append(
                AuthorityObservation(
                    authority_id=binding.authority_id,
                    path=binding.path,
                    file_sha256=actual_sha256,
                    semantic_fact_count=len(binding.semantic_facts),
                    required_snippet_count=len(binding.required_snippets),
                    identity_verified=True,
                    semantics_verified=True,
                )
            )
        observation_tuple = tuple(observations)
        authority_set_sha256 = hashlib.sha256(
            _canonical_json_bytes([item.model_dump(mode="json") for item in observation_tuple])
        ).hexdigest()
    except StrategyGrowthActionValuePreregistrationError:
        raise
    except (OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise StrategyGrowthActionValuePreregistrationError(
            "GROWTH_PREREGISTRATION_POLICY_REJECTED", str(exc)
        ) from exc
    return StrategyGrowthActionValuePreregistrationPolicyLoadResult(
        policy=policy,
        policy_path=path,
        policy_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy_canonical_sha256=policy.canonical_sha256,
        authority_observations=observation_tuple,
        authority_set_sha256=authority_set_sha256,
    )


def _prohibited_action_reason(request: PreregistrationActionRequest) -> str:
    if request.threshold_after_result:
        return "THRESHOLD_AFTER_RESULT_PROHIBITED"
    if request.use_retired_family:
        return "RETIRED_FAMILY_REUSE_PROHIBITED"
    if request.use_leverage_etf or request.use_options:
        return "HIDDEN_OR_EXPLICIT_LEVERAGE_PROHIBITED"
    if request.qld_data_lane_execution:
        return "UNSELECTED_DATA_EVIDENCE_LANE_PROHIBITED"
    return "EMPIRICAL_DATA_OR_EXTERNAL_ACTION_NOT_AUTHORIZED"


def build_strategy_growth_action_value_preregistration_decision(
    *,
    decision_id: str,
    evaluated_at_utc: datetime,
    policy_path: Path = DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
    selected_data_lanes: tuple[DataEvidenceLane, ...] | None = None,
    reviewed_threshold_policy_refs: tuple[str, ...] | None = None,
    action_request: PreregistrationActionRequest | None = None,
) -> StrategyGrowthActionValuePreregistrationDecision:
    loaded = load_strategy_growth_action_value_preregistration_policy(
        policy_path=policy_path, project_root=project_root
    )
    policy = loaded.policy
    if selected_data_lanes is not None:
        if len(selected_data_lanes) > 1:
            raise StrategyGrowthActionValuePreregistrationError(
                "MULTIPLE_DATA_EVIDENCE_LANES_PROHIBITED",
                "only one heavy data evidence lane may be selected",
            )
        if not selected_data_lanes:
            raise StrategyGrowthActionValuePreregistrationError(
                "DATA_EVIDENCE_LANE_SELECTION_REQUIRED",
                "the Owner-retained QQQ Options evidence lane must remain explicit",
            )
        if selected_data_lanes != (policy.data_lane.selected_data_lane,):
            raise StrategyGrowthActionValuePreregistrationError(
                "DATA_EVIDENCE_LANE_SELECTION_MISMATCH",
                "caller selection differs from the reviewed Owner decision",
            )
    expected_refs = policy.threshold_policy.reviewed_policy_refs
    if (
        reviewed_threshold_policy_refs is not None
        and reviewed_threshold_policy_refs != expected_refs
    ):
        raise StrategyGrowthActionValuePreregistrationError(
            "THRESHOLD_POLICY_DECLARATION_MISMATCH",
            "caller cannot add threshold policy refs outside a reviewed policy version",
        )
    request = action_request or PreregistrationActionRequest()
    if request.requested_actions:
        raise StrategyGrowthActionValuePreregistrationError(
            _prohibited_action_reason(request), ",".join(request.requested_actions)
        )
    return StrategyGrowthActionValuePreregistrationDecision.seal(
        schema_version="strategy_growth_action_value_preregistration_decision.v1",
        decision_id=decision_id,
        evaluated_at_utc=evaluated_at_utc,
        policy_file_sha256=loaded.policy_file_sha256,
        policy_canonical_sha256=loaded.policy_canonical_sha256,
        authority_set_sha256=loaded.authority_set_sha256,
        authority_observations=loaded.authority_observations,
        preregistration_status=PreregistrationStatus.BLOCKED_POLICY_INPUT,
        primary_research_start=policy.primary_research_start,
        primary_research_role=policy.primary_research_role,
        prohibited_default_start=policy.prohibited_default_start,
        preregistration_stage=policy.preregistration_stage,
        hypothesis=policy.hypothesis,
        data_lane=policy.data_lane,
        threshold_policy=policy.threshold_policy,
        mandatory_axes=policy.mandatory_axes,
        terminal_outcome_policy=policy.terminal_outcome_policy,
        downstream_gate="OWNER_REVIEWED_THRESHOLD_POLICY_REQUIRED",
        safety=policy.safety,
    )


__all__ = [
    "DEFAULT_STRATEGY_GROWTH_ACTION_VALUE_PREREGISTRATION_POLICY_PATH",
    "AuthorityObservation",
    "AuthoritySemanticFact",
    "DataEvidenceLane",
    "EvaluationAxisPolicy",
    "GrowthAuthorityBinding",
    "GrowthHypothesisPolicy",
    "MandatoryAxis",
    "MandatoryAxisOutcome",
    "PreregistrationActionRequest",
    "PreregistrationSafety",
    "PreregistrationStatus",
    "StopAction",
    "StrategyGrowthActionValuePreregistrationDecision",
    "StrategyGrowthActionValuePreregistrationError",
    "StrategyGrowthActionValuePreregistrationPolicy",
    "StrategyGrowthActionValuePreregistrationPolicyLoadResult",
    "TerminalOutcome",
    "TerminalOutcomePolicy",
    "ThresholdPolicyStatus",
    "aggregate_mandatory_axis_outcomes",
    "build_strategy_growth_action_value_preregistration_decision",
    "load_strategy_growth_action_value_preregistration_policy",
]
