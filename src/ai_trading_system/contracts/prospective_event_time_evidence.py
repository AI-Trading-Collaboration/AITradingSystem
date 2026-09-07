"""Pure declarations for the S3a local event-time evidence recorder.

These values do not prove execution, calendar membership, provider availability,
or authorization. Only the separately reviewed recorder can observe and retain
local completion facts. See TRADING-2564_S3a_Prospective_Event_Time_Evidence_V1.md.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import UTC, date, datetime
from typing import ClassVar, NoReturn, Self, cast

# Versioned research-clock and primary-window policy, not tunable thresholds.
TIMING_VERSION = "NEXT_XNYS_CLOSE_FORWARD_V1"
RETURN_CLOCK = "EFFECTIVE_SESSION_CLOSE_TO_NEXT_XNYS_SESSION_CLOSE"
PRIMARY_WINDOW_START = date(2021, 2, 22)
EVIDENCE_ROLE = "SYNTHETIC_OR_UNADOPTED_TEMPORAL_EVIDENCE"

_ROLE = re.compile(r"[a-z][a-z0-9_]*")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_COMMIT = re.compile(r"[0-9a-f]{40}")
_PLAN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.@-]*")
_VERSION_SUFFIX = re.compile(r"(?:[._-][vV][0-9]+(?:\.[0-9]+)*|@[0-9]+(?:\.[0-9]+)*)$")
_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_INSTANT = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-5][0-9])"
)
# Windows device names are protocol/path constraints, not model policy.
_RESERVED_PATH_NAMES = frozenset(
    {"con", "prn", "aux", "nul"}
    | {f"{prefix}{number}" for prefix in ("com", "lpt") for number in range(1, 10)}
    | {f"{prefix}{number}" for prefix in ("com", "lpt") for number in "¹²³"}
)


class TemporalEvidenceError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def _invalid(message: str) -> NoReturn:
    raise TemporalEvidenceError("TEMPORAL_FIELDS_INVALID", message)


def _validate_json(value: object, active: set[int]) -> None:
    kind = type(value)
    if value is None or kind in (bool, int):
        return
    if kind is str:
        cast(str, value).encode("utf-8")
        return
    if kind is float:
        if not math.isfinite(cast(float, value)):
            raise ValueError("non-finite JSON number")
        return
    if kind not in (dict, list):
        raise ValueError("only exact JSON scalar, object and array types are supported")
    identity = id(value)
    if identity in active:
        raise ValueError("cyclic JSON value")
    active.add(identity)
    try:
        if kind is dict:
            mapping = cast(dict[object, object], value)
            for key, item in mapping.items():
                if type(key) is not str:
                    raise ValueError("JSON object keys must be strings")
                _validate_json(key, active)
                _validate_json(item, active)
        else:
            for item in cast(list[object], value):
                _validate_json(item, active)
    finally:
        active.remove(identity)


def canonical_json_bytes(value: object) -> bytes:
    """Encode exact JSON types as sorted, compact UTF-8 without a newline."""

    try:
        _validate_json(value, set())
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise TemporalEvidenceError("TEMPORAL_JSON_INVALID", str(exc)) from exc


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _reject_constant(value: str) -> NoReturn:
    raise ValueError(f"non-finite JSON constant: {value}")


def strict_json_loads(content: bytes) -> object:
    """Decode UTF-8 JSON, rejecting duplicates at every depth and non-finite values."""

    if type(content) is not bytes:
        raise TemporalEvidenceError("TEMPORAL_JSON_INVALID", "immutable bytes required")
    try:
        value: object = json.loads(
            content.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
        _validate_json(value, set())
        return value
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise TemporalEvidenceError("TEMPORAL_JSON_INVALID", str(exc)) from exc


def _utc_datetime(value: object, field: str) -> datetime:
    if type(value) is not datetime:
        raise TemporalEvidenceError("TEMPORAL_TIME_INVALID", f"{field} must be a datetime")
    instant = value
    try:
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise ValueError("timezone-aware datetime required")
        return instant.astimezone(UTC)
    except (ValueError, OverflowError) as exc:
        raise TemporalEvidenceError("TEMPORAL_TIME_INVALID", f"{field}: {exc}") from exc


def parse_utc_datetime(value: object, field: str = "timestamp") -> datetime:
    """Normalize an explicitly offset ISO instant to UTC; never assume a timezone."""

    if type(value) is not str or _INSTANT.fullmatch(value) is None or value.endswith("-00:00"):
        raise TemporalEvidenceError("TEMPORAL_TIME_INVALID", f"{field}: aware ISO time required")
    try:
        return _utc_datetime(datetime.fromisoformat(value), field)
    except (ValueError, OverflowError) as exc:
        if isinstance(exc, TemporalEvidenceError):
            raise
        raise TemporalEvidenceError("TEMPORAL_TIME_INVALID", f"{field}: {exc}") from exc


def utc_datetime_text(value: datetime) -> str:
    return _utc_datetime(value, "timestamp").isoformat()


def _parse_date(value: object, field: str) -> date:
    if type(value) is not str or _DATE.fullmatch(value) is None:
        _invalid(f"{field}: ISO date required")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise TemporalEvidenceError("TEMPORAL_FIELDS_INVALID", f"{field}: {exc}") from exc


def _role(value: object) -> None:
    if type(value) is not str or _ROLE.fullmatch(value) is None:
        _invalid("role must be a lowercase ASCII identifier without a path")


def _hash_and_size(digest: object, size: object) -> None:
    if type(digest) is not str or _SHA256.fullmatch(digest) is None:
        _invalid("sha256 must be 64 lowercase hexadecimal characters")
    if type(size) is not int or size < 0:
        _invalid("size_bytes must be a non-negative integer, not a bool")


def _relative_path(value: object) -> None:
    if type(value) is not str or not value:
        raise TemporalEvidenceError("TEMPORAL_PATH_INVALID", "nonempty relative path required")
    parts = value.split("/")
    if any(
        character in '\\:<>"|?*' or ord(character) < 32 or ord(character) == 127
        for character in value
    ) or any(
        not part
        or part in {".", ".."}
        or part != part.strip()
        or part.endswith(".")
        or part.split(".", 1)[0].casefold() in _RESERVED_PATH_NAMES
        for part in parts
    ):
        raise TemporalEvidenceError(
            "TEMPORAL_PATH_INVALID", "strict portable relative path required"
        )


def _fixed(actual: object, expected: object, field: str) -> None:
    if type(actual) is not type(expected) or actual != expected:
        _invalid(f"{field} differs from its fixed declaration-only contract")


def _object(value: object, expected: set[str]) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        _invalid("exact object fields required")
    return cast(dict[str, object], value)


class _JsonDTO(ABC):
    @abstractmethod
    def to_dict(self) -> dict[str, object]: ...

    @classmethod
    @abstractmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self: ...

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        return cls.from_dict(cast(Mapping[str, object], strict_json_loads(content)))


@dataclass(frozen=True)
class ArtifactBinding(_JsonDTO):
    """Declared payload identity; no path, live-byte check or authority."""

    role: str
    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        _role(self.role)
        _hash_and_size(self.sha256, self.size_bytes)

    def to_dict(self) -> dict[str, object]:
        return {"role": self.role, "sha256": self.sha256, "size_bytes": self.size_bytes}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self:
        raw = _object(payload, {"role", "sha256", "size_bytes"})
        return cls(cast(str, raw["role"]), cast(str, raw["sha256"]), cast(int, raw["size_bytes"]))


@dataclass(frozen=True)
class PayloadMember:
    """Immutable caller bytes. The recording entry validates tuple role uniqueness."""

    role: str
    content: bytes

    def __post_init__(self) -> None:
        _role(self.role)
        if type(self.content) is not bytes:
            _invalid("payload content must be immutable bytes")

    @property
    def binding(self) -> ArtifactBinding:
        return ArtifactBinding(
            self.role, hashlib.sha256(self.content).hexdigest(), len(self.content)
        )


@dataclass(frozen=True)
class RecordingPlan(_JsonDTO):
    """Versioned caller declaration, never an activation or execution capability."""

    schema_version: ClassVar[str] = "prospective_recording_plan.v1"
    plan_id: str
    consumer_family: str
    declared_source_commit: str
    definition_bindings: tuple[ArtifactBinding, ...]
    timing_version: str = TIMING_VERSION
    return_clock: str = RETURN_CLOCK
    primary_window_start: date = PRIMARY_WINDOW_START
    evidence_role: str = EVIDENCE_ROLE
    real_adoption: bool = False
    temporal_evidence_only: bool = True
    observation_authorized: bool = False
    provider_available_at_status: str = "NOT_ESTABLISHED"
    oos_admission_status: str = "NOT_ESTABLISHED"
    production_effect: str = "none"
    broker_action: str = "none"

    def __post_init__(self) -> None:
        if (
            type(self.plan_id) is not str
            or _PLAN_ID.fullmatch(self.plan_id) is None
            or _VERSION_SUFFIX.search(self.plan_id) is None
        ):
            _invalid("plan_id must be a portable identifier with an explicit version suffix")
        if type(self.consumer_family) is not str or self.consumer_family not in {
            "FIVE_CANDIDATE",
            "COMPOSER",
        }:
            _invalid("consumer_family must be FIVE_CANDIDATE or COMPOSER")
        if (
            type(self.declared_source_commit) is not str
            or _COMMIT.fullmatch(self.declared_source_commit) is None
        ):
            _invalid("declared_source_commit must be 40 lowercase hexadecimal characters")
        if (
            type(self.definition_bindings) is not tuple
            or not self.definition_bindings
            or any(type(item) is not ArtifactBinding for item in self.definition_bindings)
            or len({item.role for item in self.definition_bindings})
            != len(self.definition_bindings)
        ):
            _invalid("definition_bindings require a nonempty typed tuple with unique roles")
        object.__setattr__(
            self,
            "definition_bindings",
            tuple(sorted(self.definition_bindings, key=lambda item: item.role)),
        )
        for name, expected in (
            ("timing_version", TIMING_VERSION),
            ("return_clock", RETURN_CLOCK),
            ("primary_window_start", PRIMARY_WINDOW_START),
            ("evidence_role", EVIDENCE_ROLE),
            ("real_adoption", False),
            ("temporal_evidence_only", True),
            ("observation_authorized", False),
            ("provider_available_at_status", "NOT_ESTABLISHED"),
            ("oos_admission_status", "NOT_ESTABLISHED"),
            ("production_effect", "none"),
            ("broker_action", "none"),
        ):
            _fixed(getattr(self, name), expected, name)

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {item.name: getattr(self, item.name) for item in fields(self)}
        payload["schema_version"] = self.schema_version
        payload["primary_window_start"] = self.primary_window_start.isoformat()
        payload["definition_bindings"] = [item.to_dict() for item in self.definition_bindings]
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self:
        raw = _object(payload, {item.name for item in fields(cls)} | {"schema_version"})
        _fixed(raw["schema_version"], cls.schema_version, "schema_version")
        bindings = raw["definition_bindings"]
        if type(bindings) is not list:
            _invalid("definition_bindings JSON array required")
        return cls(
            plan_id=cast(str, raw["plan_id"]),
            consumer_family=cast(str, raw["consumer_family"]),
            declared_source_commit=cast(str, raw["declared_source_commit"]),
            definition_bindings=tuple(
                ArtifactBinding.from_dict(cast(Mapping[str, object], item)) for item in bindings
            ),
            timing_version=cast(str, raw["timing_version"]),
            return_clock=cast(str, raw["return_clock"]),
            primary_window_start=_parse_date(raw["primary_window_start"], "primary_window_start"),
            evidence_role=cast(str, raw["evidence_role"]),
            real_adoption=cast(bool, raw["real_adoption"]),
            temporal_evidence_only=cast(bool, raw["temporal_evidence_only"]),
            observation_authorized=cast(bool, raw["observation_authorized"]),
            provider_available_at_status=cast(str, raw["provider_available_at_status"]),
            oos_admission_status=cast(str, raw["oos_admission_status"]),
            production_effect=cast(str, raw["production_effect"]),
            broker_action=cast(str, raw["broker_action"]),
        )

    @property
    def stream_id(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


@dataclass(frozen=True)
class SessionTiming(_JsonDTO):
    """Declared F/D/E order only; canonical session/close evidence is external."""

    schema_version: ClassVar[str] = "prospective_session_timing.v1"
    feature_session: date
    effective_session: date
    first_return_end_session: date
    feature_close_at: datetime
    effective_close_at: datetime
    first_return_end_close_at: datetime

    def __post_init__(self) -> None:
        for name in ("feature_session", "effective_session", "first_return_end_session"):
            if type(getattr(self, name)) is not date:
                _invalid(f"{name} must be a date, not a datetime or string")
        for name in ("feature_close_at", "effective_close_at", "first_return_end_close_at"):
            object.__setattr__(self, name, _utc_datetime(getattr(self, name), name))
        if not (
            self.feature_session < self.effective_session < self.first_return_end_session
            and self.feature_close_at < self.effective_close_at < self.first_return_end_close_at
        ):
            raise TemporalEvidenceError(
                "TEMPORAL_SESSION_ORDER_INVALID", "strict F/D/E order required"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "feature_session": self.feature_session.isoformat(),
            "effective_session": self.effective_session.isoformat(),
            "first_return_end_session": self.first_return_end_session.isoformat(),
            "feature_close_at": utc_datetime_text(self.feature_close_at),
            "effective_close_at": utc_datetime_text(self.effective_close_at),
            "first_return_end_close_at": utc_datetime_text(self.first_return_end_close_at),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self:
        raw = _object(payload, {item.name for item in fields(cls)} | {"schema_version"})
        _fixed(raw["schema_version"], cls.schema_version, "schema_version")
        return cls(
            feature_session=_parse_date(raw["feature_session"], "feature_session"),
            effective_session=_parse_date(raw["effective_session"], "effective_session"),
            first_return_end_session=_parse_date(
                raw["first_return_end_session"], "first_return_end_session"
            ),
            feature_close_at=parse_utc_datetime(raw["feature_close_at"], "feature_close_at"),
            effective_close_at=parse_utc_datetime(raw["effective_close_at"], "effective_close_at"),
            first_return_end_close_at=parse_utc_datetime(
                raw["first_return_end_close_at"], "first_return_end_close_at"
            ),
        )


@dataclass(frozen=True)
class EventBinding(_JsonDTO):
    """Portable retained-event locator; parsing it does not verify the event."""

    relative_path: str
    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        _relative_path(self.relative_path)
        _hash_and_size(self.sha256, self.size_bytes)

    def to_dict(self) -> dict[str, object]:
        return {
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self:
        raw = _object(payload, {"relative_path", "sha256", "size_bytes"})
        return cls(
            cast(str, raw["relative_path"]), cast(str, raw["sha256"]), cast(int, raw["size_bytes"])
        )
