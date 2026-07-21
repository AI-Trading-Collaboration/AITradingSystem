from __future__ import annotations

import errno
import importlib
import json
import os
import re
import socket
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Generic, Literal, TypeVar
from uuid import uuid4

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import canonical_json_bytes, write_json_atomic

POLICY_SCHEMA_VERSION = "external_request_cache_revalidation_coordination_policy.v1"
LEASE_EVENT_SCHEMA_VERSION = "external_request_cache_revalidation_lease_event.v1"
LEASE_POINTER_SCHEMA_VERSION = "external_request_cache_revalidation_lease_pointer.v1"
REPLAY_SCHEMA_VERSION = "external_request_cache_revalidation_lease_replay.v1"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "data"
    / "external_request_cache_revalidation_coordination_policy.yaml"
)

ProbeStatus = Literal["REUSABLE", "NEEDS_REVALIDATION", "INVALID"]
AcquisitionStatus = Literal["OWNER", "WAITER", "OWNER_FAILURE_BLOCKED"]
ExecutionStatus = Literal[
    "ALREADY_REUSABLE",
    "WINNER_DOUBLE_CHECK_REUSE",
    "WINNER_PUBLISHED",
    "WINNER_NON_REUSABLE_RESPONSE",
    "WAITER_REUSE",
]
TerminalOutcome = Literal[
    "PUBLISHED",
    "NON_REUSABLE_RESPONSE",
    "DOUBLE_CHECK_REUSE",
    "SUPERSEDED",
    "OWNER_FAILURE",
]
T = TypeVar("T")

_CACHE_KEY_PATTERN = re.compile(r"[0-9a-f]{64}")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_OWNER_ID_PATTERN = re.compile(r"owner-[a-z0-9-]{8,80}")
_REASON_CODE_PATTERN = re.compile(r"[A-Z0-9_:-]{2,80}")
_TRANSIENT_LOCK_ERRNOS = frozenset({errno.EACCES, errno.EAGAIN, errno.EDEADLK})


class RevalidationCoordinationError(RuntimeError):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


class RevalidationCoordinationIntegrityError(RevalidationCoordinationError):
    pass


class RevalidationCoordinationTimeout(RevalidationCoordinationError):
    pass


@dataclass(frozen=True)
class RevalidationCoordinationPolicy:
    policy_id: str
    policy_version: str
    status: str
    owner: str
    lease_ttl_seconds: int
    waiter_timeout_seconds: int
    initial_poll_interval_milliseconds: int
    maximum_poll_interval_milliseconds: int
    arbiter_timeout_seconds: int
    max_stale_takeovers_per_cause: int
    allow_waiter_retry_after_owner_failure: bool
    forbidden_field_tokens: tuple[str, ...]

    @property
    def version_ref(self) -> str:
        return f"{self.policy_id}@{self.policy_version}"


@dataclass(frozen=True)
class RevalidationProbe(Generic[T]):
    status: ProbeStatus
    generation_id: str | None
    body_sha256: str | None
    reason_code: str
    value: T | None = None

    def __post_init__(self) -> None:
        if self.status == "REUSABLE" and self.value is None:
            raise ValueError("REUSABLE probe must carry a value")
        if self.body_sha256 is not None and not _SHA256_PATTERN.fullmatch(self.body_sha256):
            raise ValueError("probe body_sha256 must be a lowercase SHA-256 digest")
        if self.status == "INVALID" and self.value is not None:
            raise ValueError("INVALID probe must not carry a value")
        if not _REASON_CODE_PATTERN.fullmatch(self.reason_code):
            raise ValueError("probe reason_code must be a redacted stable identifier")

    @property
    def causal_fingerprint(self) -> str:
        return _canonical_sha256(
            {
                "status": self.status,
                "generation_id": self.generation_id,
                "body_sha256": self.body_sha256,
                "reason_code": self.reason_code,
            }
        )

    def evidence(self) -> dict[str, str | None]:
        return {
            "status": self.status,
            "generation_id": self.generation_id,
            "body_sha256": self.body_sha256,
            "reason_code": self.reason_code,
            "causal_fingerprint": self.causal_fingerprint,
        }


@dataclass(frozen=True)
class RevalidationLease:
    cache_key: str
    generation: int
    owner_id: str
    owner_token: str
    owner_token_sha256: str
    acquired_at: datetime
    expires_at: datetime
    causal_fingerprint: str
    head_event_id: str


@dataclass(frozen=True)
class RevalidationLeaseAcquisition:
    status: AcquisitionStatus
    lease: RevalidationLease | None
    active_owner_id: str | None
    active_expires_at: datetime | None
    reason_code: str | None = None


@dataclass(frozen=True)
class RevalidationCoordinationExecution(Generic[T]):
    status: ExecutionStatus
    value: T
    lease_generation: int | None
    wait_seconds: float
    policy_version: str


@dataclass(frozen=True)
class RevalidationLeaseReplay:
    status: Literal["PASS", "FAIL"]
    cache_key: str
    event_count: int
    head_event_id: str | None
    current_state: str | None
    generation: int | None
    active_owner_id: str | None
    issues: tuple[str, ...]
    events: tuple[Mapping[str, Any], ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": REPLAY_SCHEMA_VERSION,
            "status": self.status,
            "cache_key": self.cache_key,
            "event_count": self.event_count,
            "head_event_id": self.head_event_id,
            "current_state": self.current_state,
            "generation": self.generation,
            "active_owner_id": self.active_owner_id,
            "issues": list(self.issues),
            "events": [dict(event) for event in self.events],
            "production_effect": "none",
        }


def load_revalidation_coordination_policy(
    path: Path = DEFAULT_POLICY_PATH,
) -> RevalidationCoordinationPolicy:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f"unable to load revalidation coordination policy: {path}") from exc
    if not isinstance(raw, Mapping) or raw.get("schema_version") != POLICY_SCHEMA_VERSION:
        raise ValueError("unsupported revalidation coordination policy schema_version")
    coordination = _required_mapping(raw, "coordination")
    evidence = _required_mapping(raw, "evidence")
    forbidden = evidence.get("forbidden_field_tokens")
    if not isinstance(forbidden, list) or not forbidden:
        raise ValueError("evidence.forbidden_field_tokens must be a non-empty list")
    policy = RevalidationCoordinationPolicy(
        policy_id=_required_text(raw, "policy_id"),
        policy_version=_required_text(raw, "policy_version"),
        status=_required_text(raw, "status"),
        owner=_required_text(raw, "owner"),
        lease_ttl_seconds=_positive_int(coordination, "lease_ttl_seconds"),
        waiter_timeout_seconds=_positive_int(coordination, "waiter_timeout_seconds"),
        initial_poll_interval_milliseconds=_positive_int(
            coordination, "initial_poll_interval_milliseconds"
        ),
        maximum_poll_interval_milliseconds=_positive_int(
            coordination, "maximum_poll_interval_milliseconds"
        ),
        arbiter_timeout_seconds=_positive_int(coordination, "arbiter_timeout_seconds"),
        max_stale_takeovers_per_cause=_nonnegative_int(
            coordination, "max_stale_takeovers_per_cause"
        ),
        allow_waiter_retry_after_owner_failure=_required_bool(
            coordination, "allow_waiter_retry_after_owner_failure"
        ),
        forbidden_field_tokens=tuple(_nonempty_string(value) for value in forbidden),
    )
    if policy.status != "reviewed_pilot_baseline":
        raise ValueError("coordination policy status must be reviewed_pilot_baseline")
    if policy.waiter_timeout_seconds <= policy.lease_ttl_seconds:
        raise ValueError("waiter timeout must exceed lease TTL to permit stale-owner takeover")
    if policy.initial_poll_interval_milliseconds > policy.maximum_poll_interval_milliseconds:
        raise ValueError("initial poll interval must not exceed maximum poll interval")
    if policy.max_stale_takeovers_per_cause < 1:
        raise ValueError("policy must permit at least one stale takeover")
    if policy.allow_waiter_retry_after_owner_failure:
        raise ValueError("pilot policy must fail closed after an explicit owner failure")
    if evidence.get("host_context") != "sha256_fingerprint":
        raise ValueError("evidence.host_context must be sha256_fingerprint")
    if evidence.get("owner_token_storage") != "sha256_only":
        raise ValueError("evidence.owner_token_storage must be sha256_only")
    if evidence.get("immutable_hash_chain") is not True:
        raise ValueError("evidence.immutable_hash_chain must be true")
    return policy


class ExternalRequestRevalidationCoordinator:
    """Per-cache-key cross-process lease with replayable, redacted evidence."""

    def __init__(
        self,
        request_dir: Path,
        *,
        cache_key: str,
        policy: RevalidationCoordinationPolicy | None = None,
    ) -> None:
        if not _CACHE_KEY_PATTERN.fullmatch(cache_key):
            raise ValueError("cache_key must be a lowercase SHA-256 digest")
        self.request_dir = Path(request_dir)
        self.cache_key = cache_key
        self.policy = policy or load_revalidation_coordination_policy()
        self.root = self.request_dir / "revalidation_coordination"
        self.events_root = self.root / "events"
        self.pointer_path = self.root / "current.json"
        self.arbiter_path = self.root / "arbiter.lock"

    def acquire(
        self,
        causal_probe: RevalidationProbe[Any],
        *,
        owner_id: str | None = None,
        now: datetime | None = None,
    ) -> RevalidationLeaseAcquisition:
        if causal_probe.status == "REUSABLE":
            raise ValueError("a reusable probe does not require a lease")
        if causal_probe.status == "INVALID":
            raise RevalidationCoordinationIntegrityError(
                "SOURCE_CACHE_INVALID", "source cache probe failed validation"
            )
        instant = _aware_utc(now or datetime.now(tz=UTC), field="now")
        candidate_owner_id = owner_id or f"owner-{uuid4().hex}"
        if not _OWNER_ID_PATTERN.fullmatch(candidate_owner_id):
            raise ValueError("owner_id must be a redacted owner-* identifier")
        with _exclusive_file_lock(
            self.arbiter_path,
            timeout_seconds=self.policy.arbiter_timeout_seconds,
            poll_seconds=self.policy.initial_poll_interval_milliseconds / 1000,
        ):
            replay = self._replay_or_raise()
            head = replay.events[-1] if replay.events else None
            if head is not None and head["state"] == "ACTIVE":
                expires_at = _required_datetime(head, "expires_at")
                if instant < expires_at:
                    return RevalidationLeaseAcquisition(
                        status="WAITER",
                        lease=None,
                        active_owner_id=str(head["owner_id"]),
                        active_expires_at=expires_at,
                    )
                takeover_count = sum(
                    1
                    for event in replay.events
                    if event["event_type"] == "STALE_TAKEOVER"
                    and event["causal_probe"]["causal_fingerprint"]
                    == causal_probe.causal_fingerprint
                )
                if takeover_count >= self.policy.max_stale_takeovers_per_cause:
                    return RevalidationLeaseAcquisition(
                        status="OWNER_FAILURE_BLOCKED",
                        lease=None,
                        active_owner_id=str(head["owner_id"]),
                        active_expires_at=expires_at,
                        reason_code="STALE_TAKEOVER_LIMIT_REACHED",
                    )
                return self._publish_active_lease(
                    event_type="STALE_TAKEOVER",
                    generation=int(head["generation"]) + 1,
                    causal_probe=causal_probe,
                    owner_id=candidate_owner_id,
                    instant=instant,
                    previous=head,
                )
            if (
                head is not None
                and head["state"] == "FAILED"
                and head["causal_probe"]["causal_fingerprint"] == causal_probe.causal_fingerprint
                and not self.policy.allow_waiter_retry_after_owner_failure
            ):
                return RevalidationLeaseAcquisition(
                    status="OWNER_FAILURE_BLOCKED",
                    lease=None,
                    active_owner_id=str(head["owner_id"]),
                    active_expires_at=_required_datetime(head, "expires_at"),
                    reason_code="OWNER_FAILURE_RETRY_DISABLED",
                )
            return self._publish_active_lease(
                event_type="ACQUIRED",
                generation=1 if head is None else int(head["generation"]) + 1,
                causal_probe=causal_probe,
                owner_id=candidate_owner_id,
                instant=instant,
                previous=head,
            )

    def complete(
        self,
        lease: RevalidationLease,
        *,
        outcome: TerminalOutcome,
        published_probe: RevalidationProbe[Any] | None = None,
        now: datetime | None = None,
    ) -> None:
        instant = _aware_utc(now or datetime.now(tz=UTC), field="now")
        state: Literal["COMPLETED", "FAILED"] = (
            "FAILED" if outcome == "OWNER_FAILURE" else "COMPLETED"
        )
        with _exclusive_file_lock(
            self.arbiter_path,
            timeout_seconds=self.policy.arbiter_timeout_seconds,
            poll_seconds=self.policy.initial_poll_interval_milliseconds / 1000,
        ):
            replay = self._replay_or_raise()
            head = self._require_current_lease(replay, lease)
            self._publish_terminal_event(
                lease=lease,
                head=head,
                state=state,
                outcome=outcome,
                published_probe=published_probe,
                instant=instant,
            )

    def publish_if_current_owner(
        self,
        lease: RevalidationLease,
        live_value: T,
        *,
        publish: Callable[[T], None],
        probe: Callable[[], RevalidationProbe[T]],
        now: datetime | None = None,
    ) -> RevalidationProbe[T]:
        """Fence a short cache publish against stale/taken-over lease owners."""

        instant = _aware_utc(now or datetime.now(tz=UTC), field="now")
        with _exclusive_file_lock(
            self.arbiter_path,
            timeout_seconds=self.policy.arbiter_timeout_seconds,
            poll_seconds=self.policy.initial_poll_interval_milliseconds / 1000,
        ):
            replay = self._replay_or_raise()
            head = self._require_current_lease(replay, lease)
            try:
                publish(live_value)
                published = probe()
            except BaseException:
                self._publish_terminal_event(
                    lease=lease,
                    head=head,
                    state="FAILED",
                    outcome="OWNER_FAILURE",
                    published_probe=None,
                    instant=instant,
                )
                raise
            if published.status == "INVALID":
                self._publish_terminal_event(
                    lease=lease,
                    head=head,
                    state="FAILED",
                    outcome="OWNER_FAILURE",
                    published_probe=published,
                    instant=instant,
                )
                raise RevalidationCoordinationError(
                    "REVALIDATION_DID_NOT_PUBLISH_REUSABLE_GENERATION",
                    "cache publish left an invalid generation",
                )
            if published.causal_fingerprint == lease.causal_fingerprint:
                self._publish_terminal_event(
                    lease=lease,
                    head=head,
                    state="FAILED",
                    outcome="OWNER_FAILURE",
                    published_probe=published,
                    instant=instant,
                )
                raise RevalidationCoordinationError(
                    "REVALIDATION_DID_NOT_PUBLISH_NEW_GENERATION",
                    "cache publish did not create new causal evidence",
                )
            outcome: TerminalOutcome = (
                "PUBLISHED" if published.status == "REUSABLE" else "NON_REUSABLE_RESPONSE"
            )
            self._publish_terminal_event(
                lease=lease,
                head=head,
                state="COMPLETED",
                outcome=outcome,
                published_probe=published,
                instant=instant,
            )
            return published

    def replay(self) -> RevalidationLeaseReplay:
        try:
            with _exclusive_file_lock(
                self.arbiter_path,
                timeout_seconds=self.policy.arbiter_timeout_seconds,
                poll_seconds=self.policy.initial_poll_interval_milliseconds / 1000,
            ):
                return self._replay_or_raise()
        except RevalidationCoordinationIntegrityError as exc:
            return RevalidationLeaseReplay(
                status="FAIL",
                cache_key=self.cache_key,
                event_count=0,
                head_event_id=None,
                current_state=None,
                generation=None,
                active_owner_id=None,
                issues=(exc.code,),
                events=(),
            )

    def execute(
        self,
        *,
        probe: Callable[[], RevalidationProbe[T]],
        fetch: Callable[[], T],
        publish: Callable[[T], None],
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> RevalidationCoordinationExecution[T]:
        started = monotonic()
        deadline = started + self.policy.waiter_timeout_seconds
        current = probe()
        if current.status == "INVALID":
            raise RevalidationCoordinationIntegrityError(
                "SOURCE_CACHE_INVALID", "source cache probe failed validation"
            )
        if current.status == "REUSABLE":
            return self._execution("ALREADY_REUSABLE", current, None, monotonic() - started)

        owner_id = f"owner-{uuid4().hex}"
        interval = self.policy.initial_poll_interval_milliseconds / 1000
        maximum_interval = self.policy.maximum_poll_interval_milliseconds / 1000
        waited_for_prior_owner = False
        while True:
            if waited_for_prior_owner and monotonic() >= deadline:
                raise RevalidationCoordinationTimeout(
                    "WAITER_TIMEOUT",
                    "waiter deadline elapsed before a new causal lease could be acquired",
                )
            acquired = self.acquire(current, owner_id=owner_id)
            if acquired.status == "OWNER_FAILURE_BLOCKED":
                raise RevalidationCoordinationError(
                    acquired.reason_code or "OWNER_FAILURE_BLOCKED",
                    "policy forbids another live request for this causal state",
                )
            if acquired.status == "OWNER":
                assert acquired.lease is not None
                lease = acquired.lease
                double_checked = probe()
                if double_checked.status == "INVALID":
                    self.complete(lease, outcome="OWNER_FAILURE")
                    raise RevalidationCoordinationIntegrityError(
                        "SOURCE_CACHE_INVALID", "winner double-check failed validation"
                    )
                if double_checked.status == "REUSABLE":
                    self.complete(
                        lease,
                        outcome="DOUBLE_CHECK_REUSE",
                        published_probe=double_checked,
                    )
                    return self._execution(
                        "WINNER_DOUBLE_CHECK_REUSE",
                        double_checked,
                        lease.generation,
                        monotonic() - started,
                    )
                if double_checked.causal_fingerprint != current.causal_fingerprint:
                    self.complete(lease, outcome="SUPERSEDED", published_probe=double_checked)
                    current = double_checked
                    continue
                try:
                    live_value = fetch()
                except BaseException:
                    try:
                        self.complete(lease, outcome="OWNER_FAILURE")
                    except RevalidationCoordinationError as completion_error:
                        if completion_error.code != "STALE_LEASE_OWNER":
                            raise
                    raise
                published = self.publish_if_current_owner(
                    lease,
                    live_value,
                    publish=publish,
                    probe=probe,
                )
                if published.status == "NEEDS_REVALIDATION":
                    return RevalidationCoordinationExecution(
                        status="WINNER_NON_REUSABLE_RESPONSE",
                        value=live_value,
                        lease_generation=lease.generation,
                        wait_seconds=max(0.0, monotonic() - started),
                        policy_version=self.policy.version_ref,
                    )
                return RevalidationCoordinationExecution(
                    status="WINNER_PUBLISHED",
                    value=live_value,
                    lease_generation=lease.generation,
                    wait_seconds=max(0.0, monotonic() - started),
                    policy_version=self.policy.version_ref,
                )

            waited_for_prior_owner = True
            while monotonic() < deadline:
                observed = probe()
                if observed.status == "INVALID":
                    raise RevalidationCoordinationIntegrityError(
                        "SOURCE_CACHE_INVALID", "waiter probe failed validation"
                    )
                if observed.status == "REUSABLE":
                    return self._execution("WAITER_REUSE", observed, None, monotonic() - started)
                with _exclusive_file_lock(
                    self.arbiter_path,
                    timeout_seconds=self.policy.arbiter_timeout_seconds,
                    poll_seconds=self.policy.initial_poll_interval_milliseconds / 1000,
                ):
                    replay = self._replay_or_raise()
                if not replay.events or replay.current_state != "ACTIVE":
                    current = observed
                    break
                expires_at = _required_datetime(replay.events[-1], "expires_at")
                if datetime.now(tz=UTC) >= expires_at:
                    current = observed
                    break
                sleep(interval)
                interval = min(maximum_interval, interval * 1.5)
            else:
                raise RevalidationCoordinationTimeout(
                    "WAITER_TIMEOUT",
                    "no reusable generation published within "
                    f"{self.policy.waiter_timeout_seconds}s",
                )

    def _require_current_lease(
        self,
        replay: RevalidationLeaseReplay,
        lease: RevalidationLease,
    ) -> Mapping[str, Any]:
        if not replay.events:
            raise RevalidationCoordinationIntegrityError(
                "LEASE_HEAD_MISSING", "lease evidence has no current head"
            )
        head = replay.events[-1]
        if (
            head["state"] != "ACTIVE"
            or int(head["generation"]) != lease.generation
            or head["owner_id"] != lease.owner_id
            or head["owner_token_sha256"] != sha256(lease.owner_token.encode()).hexdigest()
            or head["event_id"] != lease.head_event_id
        ):
            raise RevalidationCoordinationError(
                "STALE_LEASE_OWNER", "lease no longer owns the current generation"
            )
        return head

    def _publish_terminal_event(
        self,
        *,
        lease: RevalidationLease,
        head: Mapping[str, Any],
        state: Literal["COMPLETED", "FAILED"],
        outcome: TerminalOutcome,
        published_probe: RevalidationProbe[Any] | None,
        instant: datetime,
    ) -> None:
        event = self._event_payload(
            event_type="COMPLETED" if state == "COMPLETED" else "FAILED",
            state=state,
            generation=lease.generation,
            owner_id=lease.owner_id,
            owner_token_sha256=lease.owner_token_sha256,
            acquired_at=lease.acquired_at,
            expires_at=lease.expires_at,
            occurred_at=instant,
            causal_probe=dict(head["causal_probe"]),
            previous=head,
            outcome=outcome,
            published_probe=(published_probe.evidence() if published_probe is not None else None),
        )
        self._publish_event_and_pointer(event)

    def _execution(
        self,
        status: ExecutionStatus,
        probe: RevalidationProbe[T],
        lease_generation: int | None,
        wait_seconds: float,
    ) -> RevalidationCoordinationExecution[T]:
        if probe.value is None:
            raise RevalidationCoordinationIntegrityError(
                "REUSABLE_VALUE_MISSING", "reusable probe did not carry a value"
            )
        return RevalidationCoordinationExecution(
            status=status,
            value=probe.value,
            lease_generation=lease_generation,
            wait_seconds=max(0.0, wait_seconds),
            policy_version=self.policy.version_ref,
        )

    def _publish_active_lease(
        self,
        *,
        event_type: Literal["ACQUIRED", "STALE_TAKEOVER"],
        generation: int,
        causal_probe: RevalidationProbe[Any],
        owner_id: str,
        instant: datetime,
        previous: Mapping[str, Any] | None,
    ) -> RevalidationLeaseAcquisition:
        owner_token = uuid4().hex + uuid4().hex
        token_sha256 = sha256(owner_token.encode()).hexdigest()
        expires_at = instant + timedelta(seconds=self.policy.lease_ttl_seconds)
        event = self._event_payload(
            event_type=event_type,
            state="ACTIVE",
            generation=generation,
            owner_id=owner_id,
            owner_token_sha256=token_sha256,
            acquired_at=instant,
            expires_at=expires_at,
            occurred_at=instant,
            causal_probe=causal_probe.evidence(),
            previous=previous,
            outcome=None,
            published_probe=None,
        )
        self._publish_event_and_pointer(event)
        return RevalidationLeaseAcquisition(
            status="OWNER",
            lease=RevalidationLease(
                cache_key=self.cache_key,
                generation=generation,
                owner_id=owner_id,
                owner_token=owner_token,
                owner_token_sha256=token_sha256,
                acquired_at=instant,
                expires_at=expires_at,
                causal_fingerprint=causal_probe.causal_fingerprint,
                head_event_id=str(event["event_id"]),
            ),
            active_owner_id=owner_id,
            active_expires_at=expires_at,
        )

    def _event_payload(
        self,
        *,
        event_type: str,
        state: str,
        generation: int,
        owner_id: str,
        owner_token_sha256: str,
        acquired_at: datetime,
        expires_at: datetime,
        occurred_at: datetime,
        causal_probe: Mapping[str, Any],
        previous: Mapping[str, Any] | None,
        outcome: str | None,
        published_probe: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        previous_event_id = None if previous is None else str(previous["event_id"])
        previous_event_sha256 = (
            None if previous is None else _canonical_sha256(_without_event_sha(previous))
        )
        body: dict[str, Any] = {
            "schema_version": LEASE_EVENT_SCHEMA_VERSION,
            "event_type": event_type,
            "state": state,
            "cache_key": self.cache_key,
            "generation": generation,
            "owner_id": owner_id,
            "owner_token_sha256": owner_token_sha256,
            "host_fingerprint": sha256(socket.gethostname().encode()).hexdigest(),
            "process_id": os.getpid(),
            "policy_version": self.policy.version_ref,
            "acquired_at": acquired_at.isoformat(),
            "expires_at": expires_at.isoformat(),
            "occurred_at": occurred_at.isoformat(),
            "causal_probe": dict(causal_probe),
            "previous_event_id": previous_event_id,
            "previous_event_sha256": previous_event_sha256,
            "outcome": outcome,
            "published_probe": None if published_probe is None else dict(published_probe),
            "production_effect": "none",
        }
        event_id = f"revalidation-event-{_canonical_sha256(body)[:24]}"
        body["event_id"] = event_id
        body["event_sha256"] = _canonical_sha256(body)
        return body

    def _publish_event_and_pointer(self, event: Mapping[str, Any]) -> None:
        self.events_root.mkdir(parents=True, exist_ok=True)
        event_path = self.events_root / f"{event['event_id']}.json"
        if event_path.exists():
            try:
                existing = json.loads(event_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise RevalidationCoordinationIntegrityError(
                    "LEASE_EVENT_COLLISION", str(event_path)
                ) from exc
            if existing != event:
                raise RevalidationCoordinationIntegrityError(
                    "LEASE_EVENT_COLLISION", str(event_path)
                )
        else:
            write_json_atomic(event_path, event)
        event_file_sha256 = sha256(event_path.read_bytes()).hexdigest()
        pointer_body: dict[str, Any] = {
            "schema_version": LEASE_POINTER_SCHEMA_VERSION,
            "cache_key": self.cache_key,
            "head_event_id": event["event_id"],
            "head_event_path": f"events/{event_path.name}",
            "head_event_file_sha256": event_file_sha256,
            "state": event["state"],
            "generation": event["generation"],
            "policy_version": self.policy.version_ref,
            "production_effect": "none",
        }
        pointer_body["pointer_sha256"] = _canonical_sha256(pointer_body)
        write_json_atomic(self.pointer_path, pointer_body)

    def _replay_or_raise(self) -> RevalidationLeaseReplay:
        if not self.pointer_path.exists():
            if self.events_root.exists() and any(self.events_root.glob("*.json")):
                raise RevalidationCoordinationIntegrityError(
                    "LEASE_POINTER_MISSING", "events exist without current pointer"
                )
            return RevalidationLeaseReplay(
                status="PASS",
                cache_key=self.cache_key,
                event_count=0,
                head_event_id=None,
                current_state=None,
                generation=None,
                active_owner_id=None,
                issues=(),
                events=(),
            )
        try:
            pointer = json.loads(self.pointer_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RevalidationCoordinationIntegrityError(
                "LEASE_POINTER_INVALID", str(self.pointer_path)
            ) from exc
        if not isinstance(pointer, Mapping):
            raise RevalidationCoordinationIntegrityError(
                "LEASE_POINTER_INVALID", "pointer must be a mapping"
            )
        pointer_checksum = pointer.get("pointer_sha256")
        if (
            pointer.get("schema_version") != LEASE_POINTER_SCHEMA_VERSION
            or pointer.get("cache_key") != self.cache_key
            or pointer.get("policy_version") != self.policy.version_ref
            or not isinstance(pointer_checksum, str)
            or pointer_checksum != _canonical_sha256(_without_key(pointer, "pointer_sha256"))
        ):
            raise RevalidationCoordinationIntegrityError(
                "LEASE_POINTER_INVALID", "pointer checksum or identity mismatch"
            )
        head_path = _contained_event_path(self.root, pointer.get("head_event_path"))
        expected_file_sha = pointer.get("head_event_file_sha256")
        if head_path is None or not isinstance(expected_file_sha, str):
            raise RevalidationCoordinationIntegrityError(
                "LEASE_POINTER_INVALID", "head event reference is invalid"
            )
        events_reversed: list[Mapping[str, Any]] = []
        expected_id = pointer.get("head_event_id")
        visited: set[str] = set()
        while expected_id is not None:
            if not isinstance(expected_id, str) or expected_id in visited:
                raise RevalidationCoordinationIntegrityError(
                    "LEASE_EVENT_CHAIN_INVALID", "cycle or invalid event id"
                )
            visited.add(expected_id)
            event_path = self.events_root / f"{expected_id}.json"
            if event_path != head_path and not event_path.is_relative_to(self.events_root):
                raise RevalidationCoordinationIntegrityError(
                    "LEASE_EVENT_CHAIN_INVALID", "event path escapes root"
                )
            try:
                raw = event_path.read_bytes()
                event = json.loads(raw.decode("utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RevalidationCoordinationIntegrityError(
                    "LEASE_EVENT_INVALID", str(event_path)
                ) from exc
            if event_path == head_path and sha256(raw).hexdigest() != expected_file_sha:
                raise RevalidationCoordinationIntegrityError(
                    "LEASE_EVENT_INVALID", "head event file checksum mismatch"
                )
            self._validate_event(event, expected_id=expected_id)
            events_reversed.append(event)
            expected_id = event.get("previous_event_id")
        events = tuple(reversed(events_reversed))
        self._validate_transitions(events)
        head = events[-1]
        if pointer.get("state") != head["state"] or pointer.get("generation") != head["generation"]:
            raise RevalidationCoordinationIntegrityError(
                "LEASE_POINTER_INVALID", "pointer state does not match head event"
            )
        return RevalidationLeaseReplay(
            status="PASS",
            cache_key=self.cache_key,
            event_count=len(events),
            head_event_id=str(head["event_id"]),
            current_state=str(head["state"]),
            generation=int(head["generation"]),
            active_owner_id=str(head["owner_id"]) if head["state"] == "ACTIVE" else None,
            issues=(),
            events=events,
        )

    def _validate_event(self, event: Any, *, expected_id: str) -> None:
        if not isinstance(event, Mapping):
            raise RevalidationCoordinationIntegrityError(
                "LEASE_EVENT_INVALID", "event must be a mapping"
            )
        event_sha = event.get("event_sha256")
        if (
            event.get("schema_version") != LEASE_EVENT_SCHEMA_VERSION
            or event.get("cache_key") != self.cache_key
            or event.get("event_id") != expected_id
            or event.get("policy_version") != self.policy.version_ref
            or event.get("production_effect") != "none"
            or not isinstance(event_sha, str)
            or event_sha != _canonical_sha256(_without_key(event, "event_sha256"))
            or not _SHA256_PATTERN.fullmatch(str(event.get("owner_token_sha256")))
            or not _SHA256_PATTERN.fullmatch(str(event.get("host_fingerprint")))
            or not _OWNER_ID_PATTERN.fullmatch(str(event.get("owner_id")))
            or not isinstance(event.get("process_id"), int)
        ):
            raise RevalidationCoordinationIntegrityError(
                "LEASE_EVENT_INVALID", f"event identity/checksum mismatch: {expected_id}"
            )
        _required_datetime(event, "acquired_at")
        _required_datetime(event, "expires_at")
        _required_datetime(event, "occurred_at")
        causal = event.get("causal_probe")
        if not isinstance(causal, Mapping):
            raise RevalidationCoordinationIntegrityError(
                "LEASE_EVENT_INVALID", "causal_probe must be a mapping"
            )
        _validate_probe_evidence(causal)
        published = event.get("published_probe")
        if published is not None:
            if not isinstance(published, Mapping):
                raise RevalidationCoordinationIntegrityError(
                    "LEASE_EVENT_INVALID", "published_probe must be a mapping"
                )
            _validate_probe_evidence(published)
        forbidden = tuple(token.lower() for token in self.policy.forbidden_field_tokens)
        for key in _walk_mapping_keys(event):
            lowered = key.lower()
            if any(token in lowered for token in forbidden):
                raise RevalidationCoordinationIntegrityError(
                    "LEASE_EVIDENCE_NOT_REDACTED", f"forbidden field: {key}"
                )

    def _validate_transitions(self, events: tuple[Mapping[str, Any], ...]) -> None:
        previous: Mapping[str, Any] | None = None
        for event in events:
            event_type = event.get("event_type")
            state = event.get("state")
            generation = event.get("generation")
            if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
                raise RevalidationCoordinationIntegrityError(
                    "LEASE_EVENT_CHAIN_INVALID", "generation must be positive"
                )
            if previous is None:
                valid = event_type == "ACQUIRED" and state == "ACTIVE" and generation == 1
            elif event_type == "STALE_TAKEOVER":
                valid = (
                    previous["state"] == "ACTIVE"
                    and state == "ACTIVE"
                    and generation == int(previous["generation"]) + 1
                    and _required_datetime(event, "occurred_at")
                    >= _required_datetime(previous, "expires_at")
                )
            elif event_type == "ACQUIRED":
                valid = (
                    previous["state"] in {"COMPLETED", "FAILED"}
                    and state == "ACTIVE"
                    and generation == int(previous["generation"]) + 1
                )
            elif event_type in {"COMPLETED", "FAILED"}:
                valid = (
                    previous["state"] == "ACTIVE"
                    and state == ("COMPLETED" if event_type == "COMPLETED" else "FAILED")
                    and generation == previous["generation"]
                    and event["owner_id"] == previous["owner_id"]
                    and event["owner_token_sha256"] == previous["owner_token_sha256"]
                )
            else:
                valid = False
            if not valid:
                raise RevalidationCoordinationIntegrityError(
                    "LEASE_EVENT_CHAIN_INVALID", f"invalid transition to {event_type}"
                )
            expected_previous_sha = (
                None if previous is None else _canonical_sha256(_without_event_sha(previous))
            )
            if (
                event.get("previous_event_id")
                != (None if previous is None else previous["event_id"])
                or event.get("previous_event_sha256") != expected_previous_sha
            ):
                raise RevalidationCoordinationIntegrityError(
                    "LEASE_EVENT_CHAIN_INVALID", "previous event binding mismatch"
                )
            previous = event


@contextmanager
def _exclusive_file_lock(
    path: Path,
    *,
    timeout_seconds: float,
    poll_seconds: float,
) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b", buffering=0)
    deadline = time.monotonic() + timeout_seconds
    acquired = False
    try:
        while not acquired:
            try:
                _try_lock(handle)
            except OSError as exc:
                if exc.errno not in _TRANSIENT_LOCK_ERRNOS and not isinstance(exc, PermissionError):
                    raise
                if time.monotonic() >= deadline:
                    raise RevalidationCoordinationTimeout(
                        "ARBITER_TIMEOUT", f"unable to lock {path}"
                    ) from exc
                time.sleep(poll_seconds)
            else:
                acquired = True
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
            os.fsync(handle.fileno())
        yield
    finally:
        if acquired:
            _unlock(handle)
        handle.close()


def _try_lock(handle: Any) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        fcntl: Any = importlib.import_module("fcntl")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock(handle: Any) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        fcntl: Any = importlib.import_module("fcntl")
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _validate_probe_evidence(probe: Mapping[str, Any]) -> None:
    status = probe.get("status")
    body_sha = probe.get("body_sha256")
    if status not in {"REUSABLE", "NEEDS_REVALIDATION", "INVALID"}:
        raise RevalidationCoordinationIntegrityError("LEASE_EVENT_INVALID", "invalid probe status")
    if body_sha is not None and not _SHA256_PATTERN.fullmatch(str(body_sha)):
        raise RevalidationCoordinationIntegrityError(
            "LEASE_EVENT_INVALID", "invalid probe body checksum"
        )
    expected = _canonical_sha256(
        {
            "status": status,
            "generation_id": probe.get("generation_id"),
            "body_sha256": body_sha,
            "reason_code": probe.get("reason_code"),
        }
    )
    if (
        not _REASON_CODE_PATTERN.fullmatch(str(probe.get("reason_code")))
        or probe.get("causal_fingerprint") != expected
    ):
        raise RevalidationCoordinationIntegrityError(
            "LEASE_EVENT_INVALID", "probe fingerprint mismatch"
        )


def _required_datetime(payload: Mapping[str, Any], field: str) -> datetime:
    value = payload.get(field)
    if not isinstance(value, str):
        raise RevalidationCoordinationIntegrityError(
            "LEASE_EVENT_INVALID", f"{field} must be an ISO datetime"
        )
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise RevalidationCoordinationIntegrityError(
            "LEASE_EVENT_INVALID", f"{field} must be an ISO datetime"
        ) from exc
    return _aware_utc(parsed, field=field)


def _contained_event_path(root: Path, value: Any) -> Path | None:
    if not isinstance(value, str) or not value.startswith("events/"):
        return None
    candidate = (root / value).resolve()
    try:
        candidate.relative_to((root / "events").resolve())
    except ValueError:
        return None
    return candidate


def _walk_mapping_keys(value: Any) -> Iterator[str]:
    if isinstance(value, Mapping):
        for key, item in value.items():
            yield str(key)
            yield from _walk_mapping_keys(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_mapping_keys(item)


def _without_key(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    return {item_key: item for item_key, item in payload.items() if item_key != key}


def _without_event_sha(payload: Mapping[str, Any]) -> dict[str, Any]:
    return _without_key(payload, "event_sha256")


def _canonical_sha256(payload: Mapping[str, Any]) -> str:
    return sha256(canonical_json_bytes(payload, indent=None, trailing_newline=False)).hexdigest()


def _aware_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _required_mapping(payload: Mapping[str, Any], field: str) -> Mapping[str, Any]:
    value = payload.get(field)
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    return value


def _required_text(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be non-empty text")
    return value.strip()


def _positive_int(payload: Mapping[str, Any], field: str) -> int:
    value = payload.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _nonnegative_int(payload: Mapping[str, Any], field: str) -> int:
    value = payload.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _required_bool(payload: Mapping[str, Any], field: str) -> bool:
    value = payload.get(field)
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be boolean")
    return value


def _nonempty_string(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("forbidden field tokens must be non-empty strings")
    return value.strip()
