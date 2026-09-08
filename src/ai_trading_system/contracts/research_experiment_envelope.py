"""Strict declarations for the bounded S4 synthetic outcome-access gateway.

An envelope hash is identity, not review or permission. The gateway must load the
local authority from its fixed trusted location, replay durable history, verify
the admission and record pending exposure before invoking a synthetic loader.
Owner review and domain inventory remain controlled local trust inputs, not
signatures or proof of out-of-sample status. See
TRADING-2564_S4_Experiment_Envelope_First_Access_V1.md.
"""

from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, fields
from datetime import date, datetime
from typing import ClassVar, NoReturn, Self, cast

from ai_trading_system.contracts.prospective_event_time_evidence import (
    PRIMARY_WINDOW_START,
    canonical_json_bytes,
    parse_utc_datetime,
    strict_json_loads,
    utc_datetime_text,
)

# Protocol boundaries from the reviewed S4 requirement, not investment thresholds.
SYNTHETIC_SCOPE = "SYNTHETIC_ENGINEERING_ONLY"
LOCAL_TIME_EVIDENCE = "LOCAL_RECORDED_ONLY"
DATA_ROLES = frozenset({"DISCOVERY_KNOWN", "TRAIN", "SEEN_VALIDATION", "PROSPECTIVE_UNTOUCHED"})
EXPOSURE_STATES = frozenset({"KNOWN", "PARTIAL", "POSSIBLY_EXPOSED", "UNKNOWN"})
LOOK_MODES = frozenset({"FIRST_ONLY", "REPEAT_DECLARED"})
REQUIRED_POLICY_RULES: dict[str, frozenset[str]] = {
    "hypothesis": frozenset({"statement", "falsification"}),
    "candidate": frozenset({"selection_rule", "parameters_rule"}),
    "comparator": frozenset({"definition", "selection_rule"}),
    "accounting": frozenset(
        {
            "cost_rule",
            "holding_rule",
            "cash_rule",
            "return_attribution_rule",
            "entry_rule",
            "terminal_liquidation_rule",
            "rebalance_rule",
            "self_financing_rule",
            "carry_rule",
        }
    ),
    "main_metric": frozenset({"definition", "aggregation_rule"}),
    "sample": frozenset(
        {
            "unit",
            "eligibility_rule",
            "sufficiency_rule",
            "overlap_rule",
            "missing_rule",
            "maturity_rule",
        }
    ),
    "episode": frozenset({"definition", "boundary_rule", "overlap_rule"}),
    "look": frozenset({"mode", "first_access_rule", "repeat_access_rule"}),
    "stop": frozenset({"stopping_rule", "failure_rule"}),
    "history": frozenset({"inventory_rule", "unknown_history_rule"}),
}
_SHA256 = re.compile(r"[0-9a-f]{64}")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.@-]*")
_RULE = re.compile(r"[a-z][a-z0-9_]*")
_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_RESERVED = frozenset(
    {"con", "prn", "aux", "nul"}
    | {f"{prefix}{number}" for prefix in ("com", "lpt") for number in range(1, 10)}
    | {f"{prefix}{number}" for prefix in ("com", "lpt") for number in "¹²³"}
)


class ExperimentContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _invalid(message: str, code: str = "EXPERIMENT_FIELDS_INVALID") -> NoReturn:
    raise ExperimentContractError(code, message)


def _text(value: object, name: str) -> str:
    if type(value) is not str or not value.strip() or value != value.strip():
        _invalid(f"{name}: nonempty unpadded string required")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        _invalid(f"{name}: control characters forbidden")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise ExperimentContractError("EXPERIMENT_FIELDS_INVALID", name) from exc
    return value


def _identifier(value: object, name: str) -> str:
    text = _text(value, name)
    if _IDENTIFIER.fullmatch(text) is None:
        _invalid(f"{name}: portable identifier required")
    return text


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        _invalid(f"{name}: lowercase SHA-256 required")
    return value


def _choice(value: object, choices: frozenset[str], name: str) -> str:
    text = _text(value, name)
    if text not in choices:
        _invalid(f"{name}: unsupported value")
    return text


def _fixed(value: object, expected: str, name: str) -> None:
    if type(value) is not str or value != expected:
        _invalid(f"{name}: must equal {expected}")


def _relative_path(value: object) -> str:
    text = _text(value, "ledger_relative_path")
    if any(character in '\\:<>"|?*' for character in text) or any(
        not part
        or part in {".", ".."}
        or part != part.strip()
        or part.endswith(".")
        or part.split(".", 1)[0].casefold() in _RESERVED
        for part in text.split("/")
    ):
        _invalid("ledger_relative_path: strict portable relative path required")
    return text


def _date(value: object, name: str) -> date:
    if type(value) is not str or _DATE.fullmatch(value) is None:
        _invalid(f"{name}: exact ISO date required")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ExperimentContractError("EXPERIMENT_FIELDS_INVALID", name) from exc


def _absolute_path(value: object, name: str) -> str:
    text = _text(value, name)
    if text.startswith("/") and not text.startswith("//"):
        remainder = text[1:]
    elif re.match(r"[A-Za-z]:/", text):
        remainder = text[3:]
    else:
        _invalid(f"{name}: canonical absolute path with forward slashes required")
    _relative_path(remainder)
    return text


def _array(value: object, name: str) -> list[object]:
    if type(value) is not list:
        _invalid(f"{name}: JSON array required")
    return cast(list[object], value)


def _tuple(value: object, item_type: type[object], name: str) -> None:
    if type(value) is not tuple or any(type(item) is not item_type for item in value):
        _invalid(f"{name}: immutable tuple of exact {item_type.__name__} values required")


def _sha_set(value: object, name: str) -> None:
    _tuple(value, str, name)
    hashes = cast(tuple[str, ...], value)
    if not hashes or len(set(hashes)) != len(hashes):
        _invalid(f"{name}: nonempty unique hashes required")
    for digest in hashes:
        _sha(digest, name)


class _Record(ABC):
    schema_version: ClassVar[str]

    @abstractmethod
    def to_dict(self) -> dict[str, object]: ...

    @classmethod
    @abstractmethod
    def from_dict(cls, value: object) -> Self: ...

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        return cls.from_dict(strict_json_loads(content))

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    @classmethod
    def _object(cls, value: object) -> dict[str, object]:
        if type(value) is not dict:
            _invalid(f"{cls.__name__}: exact JSON object required")
        result = cast(dict[str, object], value)
        expected = {field.name for field in fields(cls)} | {"schema_version"}  # type: ignore[arg-type]
        if set(result) != expected or any(type(key) is not str for key in result):
            _invalid(f"{cls.__name__}: missing or unknown fields")
        _fixed(result["schema_version"], cls.schema_version, "schema_version")
        return result


@dataclass(frozen=True)
class OutcomeInterval(_Record):
    """Inclusive outcome-date interval; overlap is independent of candidate/revision."""

    schema_version: ClassVar[str] = "research_outcome_interval.v1"
    start: date
    end: date

    def __post_init__(self) -> None:
        if type(self.start) is not date or type(self.end) is not date or self.start > self.end:
            _invalid("interval: ordered exact date values required")

    def overlaps(self, other: OutcomeInterval) -> bool:
        if type(other) is not OutcomeInterval:
            _invalid("overlap: exact OutcomeInterval required")
        return self.start <= other.end and other.start <= self.end

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "start": str(self.start),
            "end": str(self.end),
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        row = cls._object(value)
        return cls(_date(row["start"], "start"), _date(row["end"], "end"))


@dataclass(frozen=True)
class OutcomeExposure(_Record):
    schema_version: ClassVar[str] = "research_prior_outcome_exposure.v1"
    interval: OutcomeInterval
    visibility: str
    reference: str

    def __post_init__(self) -> None:
        if type(self.interval) is not OutcomeInterval:
            _invalid("exposure interval: exact OutcomeInterval required")
        _choice(self.visibility, EXPOSURE_STATES, "visibility")
        _text(self.reference, "reference")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "interval": self.interval.to_dict(),
            "visibility": self.visibility,
            "reference": self.reference,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        row = cls._object(value)
        return cls(
            OutcomeInterval.from_dict(row["interval"]),
            _text(row["visibility"], "visibility"),
            _text(row["reference"], "reference"),
        )


@dataclass(frozen=True)
class ExperimentPolicy(_Record):
    """Frozen substantive rules; approval comes from the independent local authority."""

    schema_version: ClassVar[str] = "research_experiment_policy.v1"
    role: str
    policy_id: str
    version: str
    owner: str
    review_reference: str
    rationale: str
    rules: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        _choice(self.role, frozenset(REQUIRED_POLICY_RULES), "role")
        _identifier(self.policy_id, "policy_id")
        _identifier(self.version, "version")
        for name in ("owner", "review_reference", "rationale"):
            _text(getattr(self, name), name)
        _tuple(self.rules, tuple, "rules")
        keys: set[str] = set()
        for rule in self.rules:
            if len(rule) != 2:
                _invalid("rules: (name, content) pairs required")
            key, content = rule
            if _RULE.fullmatch(_text(key, "rule name")) is None or key in keys:
                _invalid("rules: unique named rules required")
            keys.add(key)
            _text(content, "rule content")
        if not REQUIRED_POLICY_RULES[self.role].issubset(keys):
            _invalid(f"{self.role}: required substantive rules missing")
        if self.role == "look":
            _choice(self.rule("mode"), LOOK_MODES, "look mode")

    def rule(self, name: str) -> str:
        for key, content in self.rules:
            if key == name:
                return content
        _invalid(f"rule absent: {name}")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "role": self.role,
            "policy_id": self.policy_id,
            "version": self.version,
            "owner": self.owner,
            "review_reference": self.review_reference,
            "rationale": self.rationale,
            "rules": dict(self.rules),
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        row = cls._object(value)
        if type(row["rules"]) is not dict:
            _invalid("rules: exact JSON object required")
        rules = cast(dict[object, object], row["rules"])
        return cls(
            _text(row["role"], "role"),
            _text(row["policy_id"], "policy_id"),
            _text(row["version"], "version"),
            _text(row["owner"], "owner"),
            _text(row["review_reference"], "review_reference"),
            _text(row["rationale"], "rationale"),
            tuple(
                (_text(key, "rule name"), _text(item, "rule content"))
                for key, item in sorted(rules.items(), key=lambda pair: str(pair[0]))
            ),
        )


@dataclass(frozen=True)
class ExperimentEnvelope(_Record):
    schema_version: ClassVar[str] = "research_experiment_envelope.v1"
    envelope_id: str
    family_id: str
    hypothesis: str
    candidate_id: str
    candidate_parameters_sha256: str
    implementation_sha256: str
    input_information_set_sha256: str
    domain_id: str
    requested_interval: OutcomeInterval
    evaluated_interval: OutcomeInterval
    data_role: str
    policies: tuple[ExperimentPolicy, ...]
    primary_window_start: date = PRIMARY_WINDOW_START
    scope: str = SYNTHETIC_SCOPE

    def __post_init__(self) -> None:
        for name in ("envelope_id", "family_id", "candidate_id", "domain_id"):
            _identifier(getattr(self, name), name)
        _text(self.hypothesis, "hypothesis")
        for name in (
            "candidate_parameters_sha256",
            "implementation_sha256",
            "input_information_set_sha256",
        ):
            _sha(getattr(self, name), name)
        for interval in (self.requested_interval, self.evaluated_interval):
            if type(interval) is not OutcomeInterval:
                _invalid("envelope intervals: exact OutcomeInterval required")
        if (
            type(self.primary_window_start) is not date
            or self.primary_window_start != PRIMARY_WINDOW_START
        ):
            _invalid("primary_window_start: reviewed 2021-02-22 boundary required")
        if not (
            self.requested_interval.start >= self.primary_window_start
            and self.requested_interval.start <= self.evaluated_interval.start
            and self.evaluated_interval.end <= self.requested_interval.end
        ):
            _invalid("evaluated interval must be inside requested primary-window interval")
        _choice(self.data_role, DATA_ROLES, "data_role")
        _fixed(self.scope, SYNTHETIC_SCOPE, "scope")
        _tuple(self.policies, ExperimentPolicy, "policies")
        if len(self.policies) != len(REQUIRED_POLICY_RULES) or {
            policy.role for policy in self.policies
        } != set(REQUIRED_POLICY_RULES):
            _invalid("policies: each required role must occur exactly once")
        if self.policy("hypothesis").rule("statement") != self.hypothesis:
            _invalid("hypothesis must equal the frozen hypothesis policy statement")

    def policy(self, role: str) -> ExperimentPolicy:
        for policy in self.policies:
            if policy.role == role:
                return policy
        _invalid(f"policy absent: {role}")

    @property
    def look_mode(self) -> str:
        return self.policy("look").rule("mode")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "envelope_id": self.envelope_id,
            "family_id": self.family_id,
            "hypothesis": self.hypothesis,
            "candidate_id": self.candidate_id,
            "candidate_parameters_sha256": self.candidate_parameters_sha256,
            "implementation_sha256": self.implementation_sha256,
            "input_information_set_sha256": self.input_information_set_sha256,
            "domain_id": self.domain_id,
            "requested_interval": self.requested_interval.to_dict(),
            "evaluated_interval": self.evaluated_interval.to_dict(),
            "data_role": self.data_role,
            "policies": [
                policy.to_dict() for policy in sorted(self.policies, key=lambda p: p.role)
            ],
            "primary_window_start": str(self.primary_window_start),
            "scope": self.scope,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        row = cls._object(value)
        return cls(
            envelope_id=_text(row["envelope_id"], "envelope_id"),
            family_id=_text(row["family_id"], "family_id"),
            hypothesis=_text(row["hypothesis"], "hypothesis"),
            candidate_id=_text(row["candidate_id"], "candidate_id"),
            candidate_parameters_sha256=_sha(row["candidate_parameters_sha256"], "parameters"),
            implementation_sha256=_sha(row["implementation_sha256"], "implementation"),
            input_information_set_sha256=_sha(row["input_information_set_sha256"], "input"),
            domain_id=_text(row["domain_id"], "domain_id"),
            requested_interval=OutcomeInterval.from_dict(row["requested_interval"]),
            evaluated_interval=OutcomeInterval.from_dict(row["evaluated_interval"]),
            data_role=_text(row["data_role"], "data_role"),
            policies=tuple(
                ExperimentPolicy.from_dict(item) for item in _array(row["policies"], "policies")
            ),
            primary_window_start=_date(row["primary_window_start"], "primary_window_start"),
            scope=_text(row["scope"], "scope"),
        )


@dataclass(frozen=True)
class LocalExperimentAuthority(_Record):
    """Fixed local inventory/review authority; a caller-provided copy grants nothing.

    domain_definition_sha256 identifies the underlying outcome domain across display
    aliases and data revisions. Genesis is independently bootstrapped and must not
    contain this full authority hash (which would form a cycle). The runtime must
    reject missing ledgers and bind an independent durable checkpoint under S4D.
    """

    schema_version: ClassVar[str] = "local_research_experiment_authority.v1"
    authority_id: str
    version: str
    owner: str
    owner_review_reference: str
    domain_id: str
    domain_definition_sha256: str
    canonical_execution_root: str
    canonical_git_common_dir: str
    ledger_relative_path: str
    genesis_sha256: str
    history_status: str
    prior_exposures: tuple[OutcomeExposure, ...]
    approved_policy_sha256s: tuple[str, ...]
    approved_envelope_sha256s: tuple[str, ...]
    scope: str = SYNTHETIC_SCOPE

    def __post_init__(self) -> None:
        for name in ("authority_id", "version", "domain_id"):
            _identifier(getattr(self, name), name)
        _text(self.owner, "owner")
        _text(self.owner_review_reference, "owner_review_reference")
        _sha(self.domain_definition_sha256, "domain_definition_sha256")
        _sha(self.genesis_sha256, "genesis_sha256")
        _absolute_path(self.canonical_execution_root, "canonical_execution_root")
        _absolute_path(self.canonical_git_common_dir, "canonical_git_common_dir")
        _relative_path(self.ledger_relative_path)
        _choice(self.history_status, frozenset({"KNOWN_COMPLETE", "UNKNOWN"}), "history_status")
        _tuple(self.prior_exposures, OutcomeExposure, "prior_exposures")
        _sha_set(self.approved_policy_sha256s, "approved_policy_sha256s")
        _sha_set(self.approved_envelope_sha256s, "approved_envelope_sha256s")
        _fixed(self.scope, SYNTHETIC_SCOPE, "scope")

    def validate_envelope(self, envelope: ExperimentEnvelope) -> None:
        if type(envelope) is not ExperimentEnvelope:
            _invalid("exact ExperimentEnvelope required")
        if envelope.domain_id != self.domain_id or envelope.scope != self.scope:
            _invalid(
                "envelope domain/scope differs from authority", "EXPERIMENT_AUTHORITY_MISMATCH"
            )
        if envelope.canonical_sha256() not in self.approved_envelope_sha256s:
            _invalid("exact envelope lacks independent review", "EXPERIMENT_ENVELOPE_NOT_APPROVED")
        if any(
            policy.canonical_sha256() not in self.approved_policy_sha256s
            for policy in envelope.policies
        ):
            _invalid(
                "exact policy content lacks independent review", "EXPERIMENT_POLICY_NOT_APPROVED"
            )

    def prior_history_blocks_first_access(self, interval: OutcomeInterval) -> bool:
        if type(interval) is not OutcomeInterval:
            _invalid("exact OutcomeInterval required")
        return self.history_status != "KNOWN_COMPLETE" or any(
            exposure.interval.overlaps(interval) for exposure in self.prior_exposures
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "authority_id": self.authority_id,
            "version": self.version,
            "owner": self.owner,
            "owner_review_reference": self.owner_review_reference,
            "domain_id": self.domain_id,
            "domain_definition_sha256": self.domain_definition_sha256,
            "canonical_execution_root": self.canonical_execution_root,
            "canonical_git_common_dir": self.canonical_git_common_dir,
            "ledger_relative_path": self.ledger_relative_path,
            "genesis_sha256": self.genesis_sha256,
            "history_status": self.history_status,
            "prior_exposures": [item.to_dict() for item in self.prior_exposures],
            "approved_policy_sha256s": sorted(self.approved_policy_sha256s),
            "approved_envelope_sha256s": sorted(self.approved_envelope_sha256s),
            "scope": self.scope,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        row = cls._object(value)
        return cls(
            authority_id=_text(row["authority_id"], "authority_id"),
            version=_text(row["version"], "version"),
            owner=_text(row["owner"], "owner"),
            owner_review_reference=_text(row["owner_review_reference"], "owner_review_reference"),
            domain_id=_text(row["domain_id"], "domain_id"),
            domain_definition_sha256=_sha(
                row["domain_definition_sha256"], "domain_definition_sha256"
            ),
            canonical_execution_root=_absolute_path(
                row["canonical_execution_root"], "execution root"
            ),
            canonical_git_common_dir=_absolute_path(
                row["canonical_git_common_dir"], "Git common dir"
            ),
            ledger_relative_path=_relative_path(row["ledger_relative_path"]),
            genesis_sha256=_sha(row["genesis_sha256"], "genesis_sha256"),
            history_status=_text(row["history_status"], "history_status"),
            prior_exposures=tuple(
                OutcomeExposure.from_dict(item)
                for item in _array(row["prior_exposures"], "prior_exposures")
            ),
            approved_policy_sha256s=tuple(
                _sha(item, "policy hash")
                for item in _array(row["approved_policy_sha256s"], "policies")
            ),
            approved_envelope_sha256s=tuple(
                _sha(item, "envelope hash")
                for item in _array(row["approved_envelope_sha256s"], "envelopes")
            ),
            scope=_text(row["scope"], "scope"),
        )


@dataclass(frozen=True)
class FreezeAdmission(_Record):
    """Independent review binding, not a caller's frozen_at/visibility assertion.

    time_evidence_sha256 binds retained local evidence bytes. The runtime must
    verify those bytes and ordering; local time never establishes external witness
    quality, universal non-exposure, real-research permission or OOS eligibility.
    """

    schema_version: ClassVar[str] = "research_experiment_freeze_admission.v1"
    envelope_sha256: str
    authority_sha256: str
    domain_id: str
    owner_review_reference: str
    reviewed_at: datetime
    time_evidence_sha256: str
    time_evidence_level: str = LOCAL_TIME_EVIDENCE
    scope: str = SYNTHETIC_SCOPE

    def __post_init__(self) -> None:
        for name in ("envelope_sha256", "authority_sha256", "time_evidence_sha256"):
            _sha(getattr(self, name), name)
        _identifier(self.domain_id, "domain_id")
        _text(self.owner_review_reference, "owner_review_reference")
        utc_datetime_text(self.reviewed_at)
        _fixed(self.time_evidence_level, LOCAL_TIME_EVIDENCE, "time_evidence_level")
        _fixed(self.scope, SYNTHETIC_SCOPE, "scope")

    def validate_bindings(
        self, envelope: ExperimentEnvelope, authority: LocalExperimentAuthority
    ) -> None:
        if type(authority) is not LocalExperimentAuthority:
            _invalid("exact LocalExperimentAuthority required")
        authority.validate_envelope(envelope)
        if (
            self.envelope_sha256 != envelope.canonical_sha256()
            or self.authority_sha256 != authority.canonical_sha256()
            or self.domain_id != authority.domain_id
            or self.owner_review_reference != authority.owner_review_reference
            or self.scope != authority.scope
        ):
            _invalid(
                "admission does not bind exact reviewed envelope/authority",
                "EXPERIMENT_ADMISSION_MISMATCH",
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "envelope_sha256": self.envelope_sha256,
            "authority_sha256": self.authority_sha256,
            "domain_id": self.domain_id,
            "owner_review_reference": self.owner_review_reference,
            "reviewed_at": utc_datetime_text(self.reviewed_at),
            "time_evidence_sha256": self.time_evidence_sha256,
            "time_evidence_level": self.time_evidence_level,
            "scope": self.scope,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        row = cls._object(value)
        return cls(
            _sha(row["envelope_sha256"], "envelope_sha256"),
            _sha(row["authority_sha256"], "authority_sha256"),
            _text(row["domain_id"], "domain_id"),
            _text(row["owner_review_reference"], "owner_review_reference"),
            parse_utc_datetime(row["reviewed_at"], "reviewed_at"),
            _sha(row["time_evidence_sha256"], "time_evidence_sha256"),
            _text(row["time_evidence_level"], "time_evidence_level"),
            _text(row["scope"], "scope"),
        )
