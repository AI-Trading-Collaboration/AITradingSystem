"""Pure replay of the reviewed S3b trusted-host clock measurement model.

This contract proves neither physical UTC accuracy nor child/source authority.
Calling consumers must independently bind policy bytes, execution and children.
See TRADING-2564_S3b_Clock_Evidence_Correction_V1.md and the fixed policy below.
"""

from __future__ import annotations

import hashlib
import math
import re
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import ClassVar, NoReturn, Self, cast

from ai_trading_system.contracts.prospective_event_time_evidence import (
    TemporalEvidenceError,
    canonical_json_bytes,
    strict_json_loads,
)

POLICY_PATH = "config/research/host_clock_evidence_v1.yaml"
POLICY_SHA256 = "09d4b5bfa286650f4a6e1066c9a0a9fd93ede5007065ee547795d082be22557a"
PROFILE_ID = "WINDOWS_CPYTHON_TIME_NS_QPC_V1"
BOUND_FORMULA_VERSION = "FROZEN_ANCHOR_OUTER_MAX_V1"
# Protocol units and the two sampled endpoints, not drift/strategy tolerances.
NS_PER_SECOND = 1_000_000_000
NS_PER_MICROSECOND = 1_000
COUNTER_QUANTIZATION_ENDPOINT_COUNT = 2
FILETIME_ENCODING_QUANTUM_NS = 100
# CPython's signed 64-bit _PyTime_t representation; this protocol accepts
# nonnegative POSIX instants/counters only and fails before arithmetic overflow.
MAX_CLOCK_NS = (1 << 63) - 1
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_LABEL = re.compile(r"[a-z][a-z0-9_]*")
_VERSION = re.compile(r"(?:0|[1-9][0-9]*)[.](?:0|[1-9][0-9]*)[.](?:0|[1-9][0-9]*)")
_CLOCKS = {
    "TIME_NS": ("GetSystemTimeAsFileTime()", False, True),
    "PERF_COUNTER_NS": ("QueryPerformanceCounter()", True, False),
}


class HostClockEvidenceError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        # A sampler may attach an immutable, explicitly non-admission diagnostic.
        # The pure contract does not manufacture or verify runtime observations.
        self.diagnostic: object | None = None
        super().__init__(f"{code}: {message}")


def _fail(code: str, message: str) -> NoReturn:
    raise HostClockEvidenceError(code, message)


def _fixed(value: object, expected: object, name: str) -> None:
    if type(value) is not type(expected) or value != expected:
        _fail("HOST_CLOCK_FIELDS_INVALID", f"{name}: fixed contract value required")


def _ns(value: object, name: str) -> int:
    if type(value) is not int or value < 0:
        _fail("HOST_CLOCK_FIELDS_INVALID", f"{name}: nonnegative exact integer required")
    if value > MAX_CLOCK_NS:
        _fail("HOST_CLOCK_NS_OVERFLOW", name)
    return value


def _resolution_ns(value: object) -> int:
    # get_clock_info returns a float. Preserve its canonical decimal spelling,
    # then perform the outward conversion with Decimal, never float arithmetic.
    if type(value) is not str:
        _fail("HOST_CLOCK_FIELDS_INVALID", "resolution must be a canonical decimal string")
    try:
        number = float(value)
    except (ValueError, OverflowError) as exc:
        raise HostClockEvidenceError("HOST_CLOCK_FIELDS_INVALID", "invalid resolution") from exc
    if not math.isfinite(number) or number <= 0 or str(number) != value:
        _fail("HOST_CLOCK_FIELDS_INVALID", "positive canonical clock-info resolution required")
    # Decimal construction/as_tuple are exact. All arithmetic below is integer
    # arithmetic, independent of the process's ambient Decimal context.
    decimal_tuple = Decimal(value).as_tuple()
    exponent = decimal_tuple.exponent
    assert type(exponent) is int  # finite canonical float spelling checked above
    coefficient = int("".join(str(digit) for digit in decimal_tuple.digits))
    power = exponent + 9  # seconds to nanoseconds, exactly 10**9
    if power >= 0:
        nanoseconds = coefficient * 10**power
    else:
        denominator = 10 ** (-power)
        nanoseconds = (coefficient + denominator - 1) // denominator
    return _ns(nanoseconds, "reported clock resolution")


def _object(value: object, names: set[str]) -> dict[str, object]:
    if type(value) is not dict or set(value) != names:
        _fail("HOST_CLOCK_FIELDS_INVALID", "exact object fields required")
    return cast(dict[str, object], value)


class _JsonDTO(ABC):
    schema_version: ClassVar[str]

    @abstractmethod
    def to_dict(self) -> dict[str, object]: ...

    @classmethod
    @abstractmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self: ...

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            raw = strict_json_loads(content)
        except TemporalEvidenceError as exc:
            raise HostClockEvidenceError("HOST_CLOCK_JSON_INVALID", exc.message) from exc
        value = cls.from_dict(cast(Mapping[str, object], raw))
        if value.canonical_bytes != content:
            _fail("HOST_CLOCK_FIELDS_INVALID", "canonical retained JSON bytes required")
        return value


def _fields(cls: type[_JsonDTO], payload: object) -> dict[str, object]:
    # Every concrete parser is a frozen dataclass; the abstract codec is not.
    declared = fields(cls)  # type: ignore[arg-type]
    raw = _object(payload, {item.name for item in declared} | {"schema_version"})
    _fixed(raw["schema_version"], cls.schema_version, "schema_version")
    return raw


@dataclass(frozen=True)
class HostClockMetadata(_JsonDTO):
    schema_version: ClassVar[str] = "host_clock_metadata.v1"
    clock_name: str
    implementation: str
    reported_resolution_seconds: str
    monotonic: bool
    adjustable: bool

    def __post_init__(self) -> None:
        if type(self.clock_name) is not str or self.clock_name not in _CLOCKS:
            _fail("HOST_CLOCK_PROVIDER_UNSUPPORTED", "unknown clock name")
        expected = _CLOCKS[self.clock_name]
        actual = (self.implementation, self.monotonic, self.adjustable)
        if any(
            type(left) is not type(right) or left != right
            for left, right in zip(actual, expected, strict=True)
        ):
            _fail("HOST_CLOCK_PROVIDER_UNSUPPORTED", "unknown implementation or clock attributes")
        _resolution_ns(self.reported_resolution_seconds)

    @property
    def resolution_ns(self) -> int:
        return _resolution_ns(self.reported_resolution_seconds)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            **{item.name: getattr(self, item.name) for item in fields(self)},
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self:
        raw = _fields(cls, payload)
        return cls(
            cast(str, raw["clock_name"]),
            cast(str, raw["implementation"]),
            cast(str, raw["reported_resolution_seconds"]),
            cast(bool, raw["monotonic"]),
            cast(bool, raw["adjustable"]),
        )


@dataclass(frozen=True)
class HostClockProvider(_JsonDTO):
    schema_version: ClassVar[str] = "host_clock_provider.v1"
    platform: str
    python_implementation: str
    python_version: str
    utc_clock: HostClockMetadata
    counter_clock: HostClockMetadata
    profile_id: str = PROFILE_ID

    def __post_init__(self) -> None:
        if (
            type(self.platform) is not str
            or self.platform != "win32"
            or type(self.python_implementation) is not str
            or self.python_implementation != "CPython"
        ):
            _fail("HOST_CLOCK_PROVIDER_UNSUPPORTED", "reviewed Windows CPython provider required")
        if type(self.python_version) is not str or _VERSION.fullmatch(self.python_version) is None:
            _fail("HOST_CLOCK_FIELDS_INVALID", "record the exact three-part Python runtime version")
        _fixed(self.profile_id, PROFILE_ID, "profile_id")
        for value, name in ((self.utc_clock, "TIME_NS"), (self.counter_clock, "PERF_COUNTER_NS")):
            if type(value) is not HostClockMetadata or value.clock_name != name:
                _fail("HOST_CLOCK_PROVIDER_UNSUPPORTED", "UTC and counter metadata roles differ")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "platform": self.platform,
            "python_implementation": self.python_implementation,
            "python_version": self.python_version,
            "utc_clock": self.utc_clock.to_dict(),
            "counter_clock": self.counter_clock.to_dict(),
            "profile_id": self.profile_id,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self:
        raw = _fields(cls, payload)
        return cls(
            cast(str, raw["platform"]),
            cast(str, raw["python_implementation"]),
            cast(str, raw["python_version"]),
            HostClockMetadata.from_dict(cast(Mapping[str, object], raw["utc_clock"])),
            HostClockMetadata.from_dict(cast(Mapping[str, object], raw["counter_clock"])),
            cast(str, raw["profile_id"]),
        )


@dataclass(frozen=True)
class ClockSample(_JsonDTO):
    """The counter readings bracket the raw UTC read, in this exact call order."""

    schema_version: ClassVar[str] = "host_clock_sample.v1"
    utc_ns: int
    counter_before_ns: int
    counter_after_ns: int

    def __post_init__(self) -> None:
        for item in fields(self):
            _ns(getattr(self, item.name), item.name)
        if self.counter_after_ns < self.counter_before_ns:
            _fail("HOST_CLOCK_COUNTER_BACKWARD", "sample's outer counter readings reversed")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            **{item.name: getattr(self, item.name) for item in fields(self)},
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self:
        raw = _fields(cls, payload)
        return cls(
            cast(int, raw["utc_ns"]),
            cast(int, raw["counter_before_ns"]),
            cast(int, raw["counter_after_ns"]),
        )


@dataclass(frozen=True)
class HostClockCheckpoint(_JsonDTO):
    """Child-bound provenance is a consumer obligation, never a core assertion."""

    schema_version: ClassVar[str] = "host_clock_checkpoint.v1"
    sequence: int
    label: str
    sample: ClockSample
    inherited_child_bound_ns: int | None
    admission_bound_ns: int

    def __post_init__(self) -> None:
        if type(self.sequence) is not int or self.sequence < 1:
            _fail("HOST_CLOCK_FIELDS_INVALID", "positive exact checkpoint sequence required")
        if type(self.label) is not str or _LABEL.fullmatch(self.label) is None:
            _fail("HOST_CLOCK_FIELDS_INVALID", "lowercase checkpoint label required")
        if type(self.sample) is not ClockSample:
            _fail("HOST_CLOCK_FIELDS_INVALID", "exact ClockSample required")
        _ns(self.admission_bound_ns, "admission_bound_ns")
        if self.inherited_child_bound_ns is not None:
            _ns(self.inherited_child_bound_ns, "inherited_child_bound_ns")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "sequence": self.sequence,
            "label": self.label,
            "sample": self.sample.to_dict(),
            "inherited_child_bound_ns": self.inherited_child_bound_ns,
            "admission_bound_ns": self.admission_bound_ns,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self:
        raw = _fields(cls, payload)
        return cls(
            cast(int, raw["sequence"]),
            cast(str, raw["label"]),
            ClockSample.from_dict(cast(Mapping[str, object], raw["sample"])),
            cast(int | None, raw["inherited_child_bound_ns"]),
            cast(int, raw["admission_bound_ns"]),
        )


def _sample_encoding(sample: ClockSample) -> None:
    # FILETIME stores 100 ns units. This is a wire encoding check, not accuracy.
    if sample.utc_ns % FILETIME_ENCODING_QUANTUM_NS:
        _fail("HOST_CLOCK_SAMPLE_ENCODING_INVALID", "TIME_NS value is not a FILETIME encoding")


def _bound(
    provider: HostClockProvider,
    anchor: ClockSample,
    sample: ClockSample,
    prior_bound_ns: int,
    inherited_child_bound_ns: int | None,
) -> int:
    outer_elapsed = sample.counter_after_ns - anchor.counter_before_ns
    quantization = COUNTER_QUANTIZATION_ENDPOINT_COUNT * provider.counter_clock.resolution_ns
    anchored_bound = _ns(anchor.utc_ns + outer_elapsed + quantization, "anchored admission bound")
    return max(
        sample.utc_ns,
        anchored_bound,
        prior_bound_ns,
        inherited_child_bound_ns if inherited_child_bound_ns is not None else 0,
    )


@dataclass(frozen=True)
class HostClockEvidence(_JsonDTO):
    schema_version: ClassVar[str] = "host_clock_evidence.v1"
    provider: HostClockProvider
    anchor: ClockSample
    checkpoints: tuple[HostClockCheckpoint, ...] = ()
    policy_path: str = POLICY_PATH
    policy_sha256: str = POLICY_SHA256
    bound_formula_version: str = BOUND_FORMULA_VERSION
    host_clock_model: str = "TRUSTED_LOCAL_HOST_MODEL"
    hidden_host_adjustment_proof: str = "NOT_ESTABLISHED"
    absolute_utc_accuracy_proof: str = "NOT_ESTABLISHED"
    child_bound_authority: str = "CONSUMER_MUST_INDEPENDENTLY_VERIFY"

    def __post_init__(self) -> None:
        if type(self.provider) is not HostClockProvider or type(self.anchor) is not ClockSample:
            _fail("HOST_CLOCK_FIELDS_INVALID", "exact provider and anchor types required")
        if type(self.checkpoints) is not tuple or any(
            type(item) is not HostClockCheckpoint for item in self.checkpoints
        ):
            _fail("HOST_CLOCK_FIELDS_INVALID", "immutable typed checkpoint tuple required")
        for name, expected in (
            ("policy_path", POLICY_PATH),
            ("policy_sha256", POLICY_SHA256),
            ("bound_formula_version", BOUND_FORMULA_VERSION),
            ("host_clock_model", "TRUSTED_LOCAL_HOST_MODEL"),
            ("hidden_host_adjustment_proof", "NOT_ESTABLISHED"),
            ("absolute_utc_accuracy_proof", "NOT_ESTABLISHED"),
            ("child_bound_authority", "CONSUMER_MUST_INDEPENDENTLY_VERIFY"),
        ):
            _fixed(getattr(self, name), expected, name)
        replay_admission_bounds(self)

    @property
    def admission_bound_ns(self) -> int:
        return replay_admission_bounds(self)[-1]

    @property
    def latest_sample(self) -> ClockSample:
        return self.checkpoints[-1].sample if self.checkpoints else self.anchor

    def to_dict(self) -> dict[str, object]:
        payload = {item.name: getattr(self, item.name) for item in fields(self)}
        payload.update(
            schema_version=self.schema_version,
            provider=self.provider.to_dict(),
            anchor=self.anchor.to_dict(),
            checkpoints=[item.to_dict() for item in self.checkpoints],
        )
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> Self:
        raw = _fields(cls, payload)
        if type(raw["checkpoints"]) is not list:
            _fail("HOST_CLOCK_FIELDS_INVALID", "checkpoint JSON array required")
        return cls(
            provider=HostClockProvider.from_dict(cast(Mapping[str, object], raw["provider"])),
            anchor=ClockSample.from_dict(cast(Mapping[str, object], raw["anchor"])),
            checkpoints=tuple(
                HostClockCheckpoint.from_dict(cast(Mapping[str, object], item))
                for item in raw["checkpoints"]
            ),
            policy_path=cast(str, raw["policy_path"]),
            policy_sha256=cast(str, raw["policy_sha256"]),
            bound_formula_version=cast(str, raw["bound_formula_version"]),
            host_clock_model=cast(str, raw["host_clock_model"]),
            hidden_host_adjustment_proof=cast(str, raw["hidden_host_adjustment_proof"]),
            absolute_utc_accuracy_proof=cast(str, raw["absolute_utc_accuracy_proof"]),
            child_bound_authority=cast(str, raw["child_bound_authority"]),
        )


def replay_admission_bounds(evidence: HostClockEvidence) -> tuple[int, ...]:
    """Recompute anchor plus all checkpoint bounds; never consult a live clock."""

    if type(evidence) is not HostClockEvidence:
        _fail("HOST_CLOCK_FIELDS_INVALID", "exact HostClockEvidence required")
    anchor = evidence.anchor
    _sample_encoding(anchor)
    previous = anchor
    bounds = [_bound(evidence.provider, anchor, anchor, anchor.utc_ns, None)]
    labels: set[str] = set()
    for sequence, checkpoint in enumerate(evidence.checkpoints, 1):
        if checkpoint.sequence != sequence or checkpoint.label in labels:
            _fail("HOST_CLOCK_FIELDS_INVALID", "contiguous sequence and unique labels required")
        labels.add(checkpoint.label)
        sample = checkpoint.sample
        _sample_encoding(sample)
        if sample.utc_ns < previous.utc_ns:
            _fail("HOST_CLOCK_UTC_BACKWARD", "raw UTC checkpoint precedes prior raw UTC")
        if sample.counter_before_ns < previous.counter_after_ns:
            _fail("HOST_CLOCK_COUNTER_BACKWARD", "checkpoint brackets overlap or counter regressed")
        expected = _bound(
            evidence.provider, anchor, sample, bounds[-1], checkpoint.inherited_child_bound_ns
        )
        if checkpoint.admission_bound_ns != expected:
            _fail("HOST_CLOCK_BOUND_MISMATCH", "checkpoint bound differs from full-chain replay")
        bounds.append(expected)
        previous = sample
    return tuple(bounds)


def append_clock_checkpoint(
    evidence: HostClockEvidence,
    *,
    label: str,
    sample: ClockSample,
    inherited_child_bound_ns: int | None = None,
) -> HostClockEvidence:
    """Append only to the original anchor; the caller verifies child provenance."""

    prior = replay_admission_bounds(evidence)[-1]
    if type(sample) is not ClockSample:
        _fail("HOST_CLOCK_FIELDS_INVALID", "exact ClockSample required")
    if inherited_child_bound_ns is not None:
        _ns(inherited_child_bound_ns, "inherited_child_bound_ns")
    # Construct with the prospective bound, then the evidence validates the
    # complete chronology and the stored derived value through the same replay.
    checkpoint = HostClockCheckpoint(
        len(evidence.checkpoints) + 1,
        label,
        sample,
        inherited_child_bound_ns,
        _bound(evidence.provider, evidence.anchor, sample, prior, inherited_child_bound_ns),
    )
    return HostClockEvidence(
        evidence.provider, evidence.anchor, (*evidence.checkpoints, checkpoint)
    )


def require_clock_evidence_extension(prior: HostClockEvidence, current: HostClockEvidence) -> None:
    """Require a strict extension of the exact retained provider/anchor/prefix.

    This checks structural lineage only. Consumers must bind both evidence byte
    identities to the same independently verified execution and source scope.
    """

    replay_admission_bounds(prior)
    replay_admission_bounds(current)
    prior_count = len(prior.checkpoints)
    if (
        prior.provider != current.provider
        or prior.anchor != current.anchor
        or prior.policy_path != current.policy_path
        or prior.policy_sha256 != current.policy_sha256
        or len(current.checkpoints) <= prior_count
        or current.checkpoints[:prior_count] != prior.checkpoints
    ):
        _fail(
            "HOST_CLOCK_EXTENSION_INVALID",
            "same provider, anchor and complete checkpoint prefix required",
        )


def datetime_to_utc_ns(value: datetime) -> int:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        _fail("HOST_CLOCK_FIELDS_INVALID", "aware exact datetime required")
    delta = value.astimezone(UTC) - _EPOCH
    return _ns(
        (delta.days * 86400 + delta.seconds) * NS_PER_SECOND
        + delta.microseconds * NS_PER_MICROSECOND,
        "datetime nanoseconds",
    )


def utc_ns_to_datetime_floor(value: int) -> datetime:
    """Raw display/lower-bound conversion only; never an admission upper bound."""

    return _EPOCH + timedelta(microseconds=_ns(value, "UTC nanoseconds") // NS_PER_MICROSECOND)


def utc_ns_to_datetime_ceil(value: int) -> datetime:
    """Round the admission upper bound outward without a float conversion."""

    value = _ns(value, "UTC nanoseconds")
    microseconds, remainder = divmod(value, NS_PER_MICROSECOND)
    return _EPOCH + timedelta(microseconds=microseconds + bool(remainder))


def deadline_allows(admission_bound_ns: int, deadline: datetime) -> bool:
    """Strict upper-limit comparison shared by deadline, manifest and lease gates."""

    return _ns(admission_bound_ns, "admission_bound_ns") < datetime_to_utc_ns(deadline)
