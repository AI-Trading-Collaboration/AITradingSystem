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

DEFAULT_STRATEGY_RESEARCH_REOPEN_READINESS_DECISION_POLICY_PATH = Path(
    "config/research/strategy_research_reopen_readiness_decision_v1.yaml"
)

_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_SOURCE_ORDER = (
    "LEGACY_DYNAMIC_V3_SELECTION",
    "CLEAN_S1_RESEARCH",
    "O1_CAPABILITY",
    "O1_BLIND_REENTRY",
    "QQQ_OPTIONS_OWNER_AUTHORIZATION",
    "QQQ_OPTIONS_EVIDENCE",
    "QQQ_OPTIONS_DQ_PIT",
    "DATA_EVIDENCE_LANE_SELECTION",
)
_AUTHORITY_ORDER = (
    "TRADING_2449_DYNAMIC_V3_GATE",
    "TRADING_2451_CLEAN_S1_PREREGISTRATION",
    "TRADING_2463_O1_PREREGISTRATION",
    "TRADING_2467_BLIND_REENTRY_POLICY",
    "TRADING_2510_PRIMARY_WINDOW_CALIBRATION",
    "TRADING_2511_DERIVED_EVIDENCE_GENERATOR",
    "TRADING_2512_DERIVED_AGGREGATE_COLLECTOR",
    "TRADING_2513_EXACT_RUN_PROPOSAL",
    "TRADING_2514_EVIDENCE_ADMISSION",
)
_BLOCKING_REASON_ORDER = (
    "LEGACY_SOURCE_CONTAMINATED",
    "CLEAN_S1_REQUIRES_SEPARATE_OWNER_RUN_AUTHORIZATION",
    "O1_CANONICAL_DQ_AND_OWNER_AUTHORIZATION_REQUIRED",
    "O1_BLIND_REENTRY_INACTIVE_AND_NOT_YET_DUE",
    "SINGLE_DATA_EVIDENCE_LANE_NOT_OWNER_SELECTED",
    "QQQ_OPTIONS_OWNER_TOKEN_NOT_PROVIDED",
    "QQQ_OPTIONS_EVIDENCE_NOT_ADMITTED",
    "QQQ_OPTIONS_DQ_PIT_NOT_EVALUATED",
)


class StrategyResearchReopenReadinessError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class ReopenDecision(StrEnum):
    KEEP_CLOSED = "KEEP_CLOSED"


class PermittedResearchStage(StrEnum):
    KEEP_CLOSED = "KEEP_CLOSED"
    PREREGISTRATION_ONLY = "PREREGISTRATION_ONLY"
    SINGLE_DATA_EVIDENCE_LANE_ONLY = "SINGLE_DATA_EVIDENCE_LANE_ONLY"
    READY_FOR_OWNER_REOPEN_REVIEW = "READY_FOR_OWNER_REOPEN_REVIEW"


class DataEvidenceLane(StrEnum):
    QLD_CANONICAL_FULL_CACHE_DQ = "QLD_CANONICAL_FULL_CACHE_DQ"
    QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE = "QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE"


class ReadinessSourceStatus(StrEnum):
    BLOCKED_CONTAMINATED_LEGACY_SOURCE = "BLOCKED_CONTAMINATED_LEGACY_SOURCE"
    ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN = "ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN"
    NOT_EVALUATED = "NOT_EVALUATED"
    INACTIVE = "INACTIVE"
    OWNER_AUTHORIZATION_NOT_PROVIDED = "OWNER_AUTHORIZATION_NOT_PROVIDED"
    EVIDENCE_NOT_ADMITTED_POLICY_BLOCKED = "EVIDENCE_NOT_ADMITTED_POLICY_BLOCKED"
    NOT_SELECTED = "NOT_SELECTED"


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
        if not isinstance(current, Mapping) or part not in current:
            raise ValueError(f"semantic fact path is missing: {dotted_path}")
        current = current[part]
    return current


def _normalize_semantic_value(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _normalize_semantic_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_semantic_value(item) for item in value]
    return value


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


class ReadinessAuthorityBinding(_PolicyModel):
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


class ReadinessSourceFact(_PolicyModel):
    source_id: str
    status: ReadinessSourceStatus

    @field_validator("source_id")
    @classmethod
    def _source_id(cls, value: str) -> str:
        return _identifier(value, "source_id")


class ReadinessSafety(_PolicyModel):
    empirical_research_authorized: Literal[False]
    candidate_search_authorized: Literal[False]
    parameter_search_authorized: Literal[False]
    backtest_authorized: Literal[False]
    holdout_access_authorized: Literal[False]
    investment_conclusion_authorized: Literal[False]
    data_lane_selected: Literal[False]
    cache_mutation_authorized: Literal[False]
    external_action_authorized: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class StrategyResearchReopenReadinessPolicy(_PolicyModel):
    schema_version: Literal["strategy_research_reopen_readiness_decision_policy.v1"]
    policy_id: str
    policy_version: Literal["1.0.0"]
    policy_status: Literal["REVIEWED_BASELINE_KEEP_CLOSED"]
    task_id: Literal["TRADING-2515_STRATEGY_RESEARCH_REOPEN_READINESS_DECISION_V1"]
    primary_research_start: date
    primary_research_role: Literal["PRIMARY"]
    prohibited_default_start: date
    reopen_decision: Literal[ReopenDecision.KEEP_CLOSED]
    permitted_stage: Literal[PermittedResearchStage.PREREGISTRATION_ONLY]
    selected_data_lane: None
    recommended_data_lane: Literal[DataEvidenceLane.QLD_CANONICAL_FULL_CACHE_DQ]
    recommendation_status: Literal["RECOMMENDATION_ONLY_NOT_SELECTED_OR_AUTHORIZED"]
    source_facts: tuple[ReadinessSourceFact, ...]
    blocking_reason_codes: tuple[str, ...]
    authorities: tuple[ReadinessAuthorityBinding, ...]
    safety: ReadinessSafety

    @field_validator("policy_id")
    @classmethod
    def _policy_id(cls, value: str) -> str:
        return _identifier(value, "policy_id")

    @model_validator(mode="after")
    def _frozen_baseline(self) -> Self:
        if self.primary_research_start != date(2021, 2, 22):
            raise ValueError("PRIMARY research start must remain 2021-02-22")
        if self.prohibited_default_start != date(2022, 12, 1):
            raise ValueError("historical 2022-12-01 boundary must remain explicitly non-default")
        if tuple(item.source_id for item in self.source_facts) != _SOURCE_ORDER:
            raise ValueError("readiness source inventory or order drifted")
        if tuple(item.authority_id for item in self.authorities) != _AUTHORITY_ORDER:
            raise ValueError("authority inventory or order drifted")
        if self.blocking_reason_codes != _BLOCKING_REASON_ORDER:
            raise ValueError("blocking reason taxonomy or order drifted")
        expected_statuses = (
            ReadinessSourceStatus.BLOCKED_CONTAMINATED_LEGACY_SOURCE,
            ReadinessSourceStatus.ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN,
            ReadinessSourceStatus.NOT_EVALUATED,
            ReadinessSourceStatus.INACTIVE,
            ReadinessSourceStatus.OWNER_AUTHORIZATION_NOT_PROVIDED,
            ReadinessSourceStatus.EVIDENCE_NOT_ADMITTED_POLICY_BLOCKED,
            ReadinessSourceStatus.NOT_EVALUATED,
            ReadinessSourceStatus.NOT_SELECTED,
        )
        if tuple(item.status for item in self.source_facts) != expected_statuses:
            raise ValueError("current readiness source facts cannot be promoted or fabricated")
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
class StrategyResearchReopenReadinessPolicyLoadResult:
    policy: StrategyResearchReopenReadinessPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str
    authority_observations: tuple[AuthorityObservation, ...]
    authority_set_sha256: str


class ReadinessActionRequest(_StrictModel):
    empirical_research: bool = False
    candidate_search: bool = False
    parameter_search: bool = False
    backtest: bool = False
    holdout_access: bool = False
    cache_mutation: bool = False
    external_action: bool = False
    investment_conclusion: bool = False
    paper: bool = False
    live: bool = False
    broker: bool = False
    production: bool = False

    @property
    def requested_actions(self) -> tuple[str, ...]:
        return tuple(name for name, value in self.model_dump(mode="python").items() if value)


class StrategyResearchReopenReadinessDecision(_StrictModel):
    schema_version: Literal["strategy_research_reopen_readiness_decision.v1"]
    decision_id: str
    evaluated_at_utc: datetime
    policy_file_sha256: str
    policy_canonical_sha256: str
    authority_set_sha256: str
    authority_observations: tuple[AuthorityObservation, ...]
    primary_research_start: date
    primary_research_role: Literal["PRIMARY"]
    prohibited_default_start: date
    reopen_decision: Literal[ReopenDecision.KEEP_CLOSED]
    permitted_stage: Literal[PermittedResearchStage.PREREGISTRATION_ONLY]
    selected_data_lane: None
    recommended_data_lane: Literal[DataEvidenceLane.QLD_CANONICAL_FULL_CACHE_DQ]
    recommendation_status: Literal["RECOMMENDATION_ONLY_NOT_SELECTED_OR_AUTHORIZED"]
    source_facts: tuple[ReadinessSourceFact, ...]
    blocking_reason_codes: tuple[str, ...]
    empirical_research_authorized: Literal[False]
    candidate_search_authorized: Literal[False]
    parameter_search_authorized: Literal[False]
    backtest_authorized: Literal[False]
    holdout_access_authorized: Literal[False]
    investment_conclusion_authorized: Literal[False]
    data_lane_selected: Literal[False]
    cache_mutation_authorized: Literal[False]
    external_action_authorized: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
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
    def _seal(self, info: ValidationInfo) -> Self:
        if (
            info.context
            and info.context.get("allow_unsealed")
            and self.content_sha256 == _UNSEALED_SHA256
        ):
            return self
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("semantic content SHA-256 mismatch")
        if tuple(item.source_id for item in self.source_facts) != _SOURCE_ORDER:
            raise ValueError("decision source inventory drifted")
        if tuple(item.authority_id for item in self.authority_observations) != _AUTHORITY_ORDER:
            raise ValueError("decision authority inventory drifted")
        if self.blocking_reason_codes != _BLOCKING_REASON_ORDER:
            raise ValueError("decision blocking reason taxonomy drifted")
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
            raise StrategyResearchReopenReadinessError(
                "READINESS_DECISION_PAYLOAD_INVALID", str(exc)
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
            raise StrategyResearchReopenReadinessError(
                "READINESS_DECISION_RECORD_INVALID", str(exc)
            ) from exc


def load_strategy_research_reopen_readiness_policy(
    policy_path: Path = DEFAULT_STRATEGY_RESEARCH_REOPEN_READINESS_DECISION_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> StrategyResearchReopenReadinessPolicyLoadResult:
    root = project_root.resolve()
    try:
        path = _bound_file(policy_path, root=root, field="readiness policy")
        raw = path.read_bytes()
        text = raw.decode("utf-8")
        payload = load_strict_yaml_text(text, label="strategy-research readiness policy")
        policy = StrategyResearchReopenReadinessPolicy.model_validate(payload)
        observations: list[AuthorityObservation] = []
        for binding in policy.authorities:
            authority_path = _bound_file(
                Path(binding.path), root=root, field=f"authority {binding.authority_id}"
            )
            authority_raw = authority_path.read_bytes()
            actual_sha256 = hashlib.sha256(authority_raw).hexdigest()
            if actual_sha256 != binding.file_sha256:
                raise ValueError(f"authority file SHA-256 mismatch: {binding.authority_id}")
            if binding.format == "YAML":
                authority_payload = load_strict_yaml_text(
                    authority_raw.decode("utf-8"), label=binding.authority_id
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
            else:
                authority_text = authority_raw.decode("utf-8")
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
    except StrategyResearchReopenReadinessError:
        raise
    except (OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise StrategyResearchReopenReadinessError("READINESS_POLICY_REJECTED", str(exc)) from exc
    return StrategyResearchReopenReadinessPolicyLoadResult(
        policy=policy,
        policy_path=path,
        policy_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy_canonical_sha256=policy.canonical_sha256,
        authority_observations=observation_tuple,
        authority_set_sha256=authority_set_sha256,
    )


def build_strategy_research_reopen_readiness_decision(
    *,
    decision_id: str,
    evaluated_at_utc: datetime,
    policy_path: Path = DEFAULT_STRATEGY_RESEARCH_REOPEN_READINESS_DECISION_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
    requested_stage: PermittedResearchStage = PermittedResearchStage.PREREGISTRATION_ONLY,
    selected_data_lanes: tuple[DataEvidenceLane, ...] = (),
    source_status_declarations: Mapping[str, ReadinessSourceStatus] | None = None,
    action_request: ReadinessActionRequest | None = None,
) -> StrategyResearchReopenReadinessDecision:
    loaded = load_strategy_research_reopen_readiness_policy(
        policy_path=policy_path, project_root=project_root
    )
    policy = loaded.policy
    if requested_stage is not PermittedResearchStage.PREREGISTRATION_ONLY:
        raise StrategyResearchReopenReadinessError(
            "READINESS_STAGE_NOT_AUTHORIZED",
            f"requested stage {requested_stage} exceeds the reviewed baseline",
        )
    if selected_data_lanes:
        reason = (
            "MULTIPLE_DATA_EVIDENCE_LANES_PROHIBITED"
            if len(selected_data_lanes) > 1
            else "DATA_EVIDENCE_LANE_NOT_OWNER_SELECTED"
        )
        raise StrategyResearchReopenReadinessError(
            reason, "2515 records a recommendation but authorizes no data lane"
        )
    expected_declarations = {item.source_id: item.status for item in policy.source_facts}
    if (
        source_status_declarations is not None
        and dict(source_status_declarations) != expected_declarations
    ):
        raise StrategyResearchReopenReadinessError(
            "READINESS_SOURCE_DECLARATION_MISMATCH",
            "caller declaration differs from authority-derived source facts",
        )
    request = action_request or ReadinessActionRequest()
    if request.requested_actions:
        raise StrategyResearchReopenReadinessError(
            "EMPIRICAL_OR_EXTERNAL_ACTION_NOT_AUTHORIZED",
            ",".join(request.requested_actions),
        )
    safety = policy.safety
    return StrategyResearchReopenReadinessDecision.seal(
        schema_version="strategy_research_reopen_readiness_decision.v1",
        decision_id=decision_id,
        evaluated_at_utc=evaluated_at_utc,
        policy_file_sha256=loaded.policy_file_sha256,
        policy_canonical_sha256=loaded.policy_canonical_sha256,
        authority_set_sha256=loaded.authority_set_sha256,
        authority_observations=loaded.authority_observations,
        primary_research_start=policy.primary_research_start,
        primary_research_role=policy.primary_research_role,
        prohibited_default_start=policy.prohibited_default_start,
        reopen_decision=policy.reopen_decision,
        permitted_stage=policy.permitted_stage,
        selected_data_lane=None,
        recommended_data_lane=policy.recommended_data_lane,
        recommendation_status=policy.recommendation_status,
        source_facts=policy.source_facts,
        blocking_reason_codes=policy.blocking_reason_codes,
        **safety.model_dump(mode="python"),
    )


__all__ = [
    "DEFAULT_STRATEGY_RESEARCH_REOPEN_READINESS_DECISION_POLICY_PATH",
    "AuthorityObservation",
    "AuthoritySemanticFact",
    "DataEvidenceLane",
    "PermittedResearchStage",
    "ReadinessActionRequest",
    "ReadinessAuthorityBinding",
    "ReadinessSafety",
    "ReadinessSourceFact",
    "ReadinessSourceStatus",
    "ReopenDecision",
    "StrategyResearchReopenReadinessDecision",
    "StrategyResearchReopenReadinessError",
    "StrategyResearchReopenReadinessPolicy",
    "StrategyResearchReopenReadinessPolicyLoadResult",
    "build_strategy_research_reopen_readiness_decision",
    "load_strategy_research_reopen_readiness_policy",
]
