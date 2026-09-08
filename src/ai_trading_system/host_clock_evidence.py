"""Fixed Windows UTC/QPC sampler for the reviewed trusted-host clock contract.

Reads exact policy bytes and clocks only. Source execution, leases, child-bound
provenance and authorization remain obligations of the calling consumer.
"""

from __future__ import annotations

import hashlib
import math
import platform
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Literal, Protocol, cast

from ai_trading_system.contracts.host_clock_evidence import (
    POLICY_PATH,
    POLICY_SHA256,
    ClockSample,
    HostClockEvidence,
    HostClockEvidenceError,
    HostClockMetadata,
    HostClockProvider,
    append_clock_checkpoint,
)
from ai_trading_system.contracts.prospective_event_time_evidence import canonical_json_bytes
from ai_trading_system.data.immutable_publish import (
    DataPublicationError,
    read_contained_artifact_bytes,
)


# Private wrappers are the unit-test seam; never replace global stdlib clocks.
def _utc_ns() -> int:
    return time.time_ns()


def _counter_ns() -> int:
    return time.perf_counter_ns()


class _ClockInfo(Protocol):
    @property
    def implementation(self) -> str: ...

    @property
    def resolution(self) -> float: ...

    @property
    def monotonic(self) -> bool: ...

    @property
    def adjustable(self) -> bool: ...


def _clock_info(name: str) -> _ClockInfo:
    # The fixed provider loop supplies these two stdlib names only.
    return time.get_clock_info(cast(Literal["time", "perf_counter"], name))


def _runtime_identity() -> tuple[str, str, str]:
    return (
        sys.platform,
        platform.python_implementation(),
        ".".join(str(value) for value in sys.version_info[:3]),
    )


def _thread_identity() -> int:
    return threading.get_ident()


@dataclass(frozen=True)
class ClockReadDiagnostic:
    """An unvalidated raw read; invalid values retain their type and spelling."""

    name: str
    value_type: str
    value_repr: str
    integer_value: int | None

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "value_type": self.value_type,
            "value_repr": self.value_repr,
            "integer_value": self.integer_value,
        }


@dataclass(frozen=True)
class HostClockFailureDiagnostic:
    """Preservable failed/partial reads, deliberately not HostClockEvidence."""

    schema_version: ClassVar[str] = "host_clock_failure_diagnostic.v1"
    attempted_label: str
    error_code: str
    error_type: str
    error_message: str
    prior_evidence: HostClockEvidence | None
    raw_readings: tuple[ClockReadDiagnostic, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "attempted_label": self.attempted_label,
            "error_code": self.error_code,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "prior_evidence": self.prior_evidence.to_dict() if self.prior_evidence else None,
            "raw_readings": [item.to_dict() for item in self.raw_readings],
            "valid_clock_evidence": False,
            "admission_allowed": False,
            "chain_resumable": False,
        }

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


def _read_provider() -> HostClockProvider:
    metadata = []
    for clock_name, info_name in (("TIME_NS", "time"), ("PERF_COUNTER_NS", "perf_counter")):
        try:
            info = _clock_info(info_name)
            resolution = info.resolution
            implementation, monotonic, adjustable = (
                info.implementation,
                info.monotonic,
                info.adjustable,
            )
        except (AttributeError, OSError, ValueError) as exc:
            raise HostClockEvidenceError(
                "HOST_CLOCK_PROVIDER_UNSUPPORTED", "clock metadata unavailable"
            ) from exc
        if type(resolution) is not float or not math.isfinite(resolution) or resolution <= 0:
            raise HostClockEvidenceError(
                "HOST_CLOCK_PROVIDER_UNSUPPORTED", "invalid reported resolution"
            )
        metadata.append(
            HostClockMetadata(clock_name, implementation, str(resolution), monotonic, adjustable)
        )
    operating_system, implementation, version = _runtime_identity()
    return HostClockProvider(operating_system, implementation, version, metadata[0], metadata[1])


def _check_policy(source_root: Path) -> None:
    try:
        content = read_contained_artifact_bytes(root=source_root, relative_path=POLICY_PATH)
    except (OSError, ValueError, DataPublicationError) as exc:
        raise HostClockEvidenceError(
            "HOST_CLOCK_POLICY_INVALID", "exact contained clock policy required"
        ) from exc
    if hashlib.sha256(content).hexdigest() != POLICY_SHA256:
        raise HostClockEvidenceError("HOST_CLOCK_POLICY_INVALID", "reviewed policy bytes differ")


class HostClockSampler:
    """One sequential measurement chain; a failed checkpoint is terminal.

    Recreating a sampler after failure is not a continuation of this chain.
    Consumers must retain failures and must not use this object as authority.
    """

    def __init__(self, *, source_root: Path) -> None:
        if not isinstance(source_root, Path):
            raise HostClockEvidenceError("HOST_CLOCK_FIELDS_INVALID", "source_root must be a Path")
        self._source_root = source_root
        self._owner_thread = _thread_identity()
        self._busy = False
        self._active_label: str | None = "anchor"
        self._partial_reads: list[ClockReadDiagnostic] = []
        self._failure: HostClockFailureDiagnostic | None = None
        self._failed = False
        try:
            _check_policy(self._source_root)
            self._provider = _read_provider()
            self._evidence = HostClockEvidence(self._provider, self._sample())
        except BaseException as exc:
            self._record_failure(exc, "anchor", None)
            raise
        finally:
            self._active_label = None
        self._partial_reads = []

    @classmethod
    def start(cls, *, source_root: Path) -> HostClockSampler:
        return cls(source_root=source_root)

    def _check_provider(self) -> None:
        if _read_provider() != self._provider:
            raise HostClockEvidenceError(
                "HOST_CLOCK_PROVIDER_CHANGED", "provider metadata changed during measurement"
            )

    def _sample(self) -> ClockSample:
        # No filesystem authority is acquired while sampling. Consumers may
        # hold an existing immutable-store root transaction here and must
        # recheck the policy outside it before/after the transaction.
        self._check_provider()
        before = _counter_ns()
        self._retain_read("counter_before_ns", before)
        utc = _utc_ns()
        self._retain_read("utc_ns", utc)
        # Provider checks stay inside the outer end counter. Filesystem policy
        # checks are separate, and their duration enters the next checkpoint
        # through the same original anchor.
        self._check_provider()
        after = _counter_ns()
        self._retain_read("counter_after_ns", after)
        return ClockSample(utc, before, after)

    def _retain_read(self, name: str, value: object) -> None:
        self._partial_reads.append(
            ClockReadDiagnostic(
                name, type(value).__name__, repr(value), value if type(value) is int else None
            )
        )

    def _record_failure(
        self, exc: BaseException, label: str, prior: HostClockEvidence | None
    ) -> None:
        self._failed = True
        if self._failure is None:
            code = (
                exc.code
                if isinstance(exc, HostClockEvidenceError)
                else "HOST_CLOCK_SAMPLING_INTERRUPTED"
            )
            self._failure = HostClockFailureDiagnostic(
                self._active_label or label,
                code,
                type(exc).__name__,
                str(exc),
                prior,
                tuple(self._partial_reads),
            )
        # Preserve partial reads even when a primitive raises OSError or the
        # constructor fails before a sampler handle can be returned. Exception
        # type/causality remain unchanged; this attachment grants no admission.
        exc.diagnostic = self._failure  # type: ignore[attr-defined]

    @property
    def failure_diagnostic(self) -> HostClockFailureDiagnostic | None:
        return self._failure

    def recheck_policy(self) -> None:
        """Recheck exact policy bytes outside any different-root store lock.

        This produces no timestamp or source-execution authority. The consumer
        must checkpoint afterward when its covered interval includes this work.
        """
        evidence = self.evidence
        if self._busy or _thread_identity() != self._owner_thread:
            error = HostClockEvidenceError(
                "HOST_CLOCK_CONCURRENT_ACCESS", "policy check requires the original idle owner"
            )
            self._record_failure(error, "policy_recheck", evidence)
            raise error
        self._busy = True
        self._active_label = "policy_recheck"
        self._partial_reads = []
        try:
            _check_policy(self._source_root)
            if self._failed:
                raise HostClockEvidenceError(
                    "HOST_CLOCK_CONCURRENT_ACCESS", "policy check interrupted"
                )
        except BaseException as exc:
            self._record_failure(exc, "policy_recheck", evidence)
            raise
        finally:
            self._busy = False
            self._active_label = None

    @property
    def evidence(self) -> HostClockEvidence:
        if self._failed:
            raise HostClockEvidenceError(
                "HOST_CLOCK_SAMPLER_FAILED", "failed chain cannot resume or produce a final witness"
            )
        return self._evidence

    @property
    def anchor(self) -> ClockSample:
        return self.evidence.anchor

    def checkpoint(
        self, label: str, *, inherited_child_bound_ns: int | None = None
    ) -> HostClockEvidence:
        evidence = self.evidence
        if self._busy or _thread_identity() != self._owner_thread:
            error = HostClockEvidenceError(
                "HOST_CLOCK_CONCURRENT_ACCESS", "one owner thread and non-reentrant chain required"
            )
            self._record_failure(error, label, evidence)
            raise error
        self._busy = True
        self._active_label = label
        self._partial_reads = []
        try:
            observed = self._sample()
            result = append_clock_checkpoint(
                evidence,
                label=label,
                sample=observed,
                inherited_child_bound_ns=inherited_child_bound_ns,
            )
            if self._failed or self._evidence is not evidence:
                raise HostClockEvidenceError(
                    "HOST_CLOCK_CONCURRENT_ACCESS", "measurement chain changed during checkpoint"
                )
        except BaseException as exc:
            self._record_failure(exc, label, evidence)
            raise
        finally:
            self._busy = False
            self._active_label = None
        self._evidence = result
        self._partial_reads = []
        return result
