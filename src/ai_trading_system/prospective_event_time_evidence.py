"""Local S3a timing sidecars, independent of producer, DQ and OOS admission.

The trusted host recorder observes a write interval, not a provider publication
time. A completion witness attests payload durability; its own final persistence
still needs a future parent-executor acknowledgement. No function grants capture.
"""

from __future__ import annotations

import hashlib
import re
import stat
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn, cast

from ai_trading_system.contracts.host_clock_evidence import (
    POLICY_PATH as HOST_CLOCK_POLICY_PATH,
)
from ai_trading_system.contracts.host_clock_evidence import (
    POLICY_SHA256 as HOST_CLOCK_POLICY_SHA256,
)
from ai_trading_system.contracts.host_clock_evidence import (
    HostClockEvidence,
    datetime_to_utc_ns,
    deadline_allows,
    require_clock_evidence_extension,
    utc_ns_to_datetime_ceil,
    utc_ns_to_datetime_floor,
)
from ai_trading_system.contracts.prospective_event_time_evidence import (
    EventBinding,
    PayloadMember,
    RecordingPlan,
    SessionTiming,
    TemporalEvidenceError,
    canonical_json_bytes,
    parse_utc_datetime,
    strict_json_loads,
    utc_datetime_text,
)
from ai_trading_system.data.immutable_publish import (
    DataPublicationIntegrityError,
    exclusive_store_maintenance,
    read_contained_artifact_bytes,
    write_contained_artifact_bytes,
)
from ai_trading_system.host_clock_evidence import HostClockSampler
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutLeaseHandle,
    CheckoutOperationClass,
    resolve_checkout_identity,
)
from ai_trading_system.platform.architecture.parallel_control_kernel import parse_lease_event
from ai_trading_system.trading_calendar import (
    US_EQUITY_MARKET_TIMEZONE,
    is_us_equity_trading_day,
    us_equity_market_session,
)
from ai_trading_system.us_equity_special_closure_policy import (
    CURRENT_US_EQUITY_SPECIAL_CLOSURE_POLICY_RELATIVE_PATH,
    default_us_equity_special_closure_policy,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

SOURCE_ROOT = Path(__file__).resolve().parents[2]
LEGACY_POLICY_PATH = "config/research/prospective_event_time_evidence_v1.yaml"
LEGACY_POLICY_SHA256 = "364257f8c0959a6f042c646fc0ef86c4e9c8ba7351754cc15a8bd3b0db70b466"
POLICY_PATH = "config/research/prospective_event_time_evidence_v2.yaml"
# Exact reviewed, unadopted policy identity; changing it is a new reviewed wave.
# See the linked S3a requirement, rather than treating any caller YAML as policy.
POLICY_SHA256 = "f88db52fc70f9ccdcf84a58666883e17f8d8921a7c6713c66552f91c59fb0662"
_POLICIES = {LEGACY_POLICY_PATH: LEGACY_POLICY_SHA256, POLICY_PATH: POLICY_SHA256}
_CALENDAR_PATHS = (
    "src/ai_trading_system/trading_calendar.py",
    "src/ai_trading_system/us_equity_special_closure_policy.py",
    CURRENT_US_EQUITY_SPECIAL_CLOSURE_POLICY_RELATIVE_PATH.as_posix(),
)
_STAGES = {"ACTIVATION": 0, "INPUTS_OBSERVED": 1, "SIGNAL_RECORDED": 2}
# Historical v1 verifier only. Preserve its original inner-interval semantics;
# v2 does not infer a relationship between UTC and counter rates from this unit.
_UTC_RESOLUTION_NS = 1_000
_COMPLETION_LOCATOR = re.compile(
    r"streams/[0-9a-f]{64}/(?:activation|sessions/[0-9]{4}-[0-9]{2}-[0-9]{2}/(?:inputs|signal))/completion[.]json"
)
_SAFETY_V1: dict[str, object] = {
    "temporal_evidence_only": True,
    "real_activation_adopted": False,
    "observation_authorized": False,
    "provider_available_at_status": "NOT_ESTABLISHED",
    "oos_admission_status": "NOT_ESTABLISHED",
    "signal_semantics_status": "NOT_VALIDATED_BY_TIME_LAYER",
    "input_closure_status": "DECLARED_MEMBERS_ONLY",
    "declared_source_execution_attested": False,
    "witness_own_durability_time_established": False,
    "parent_executor_acknowledgement": "NOT_PRESENT",
    "production_effect": "none",
    "broker_action": "none",
}
_SAFETY = {
    **_SAFETY_V1,
    "host_clock_model": "TRUSTED_LOCAL_HOST_MODEL",
    "hidden_host_adjustment_proof": "NOT_ESTABLISHED",
    "absolute_utc_accuracy_proof": "NOT_ESTABLISHED",
    "original_return_observation_recreated_by_replay": False,
}


def _fail(code: str, message: str) -> NoReturn:
    raise TemporalEvidenceError(code, message)


def _object(value: object, fields: set[str] | None = None) -> dict[str, Any]:
    if type(value) is not dict or (fields is not None and set(value) != fields):
        _fail("TEMPORAL_FIELDS_INVALID", "exact JSON object fields required")
    return cast(dict[str, Any], value)


def _equal(actual: object, expected: object, code: str) -> None:
    if canonical_json_bytes(actual) != canonical_json_bytes(expected):
        _fail(code, "bound values differ")


def _binding(path: str, content: bytes) -> EventBinding:
    return EventBinding(path, hashlib.sha256(content).hexdigest(), len(content))


def _date(value: object) -> date:
    if type(value) is not date:
        _fail("TEMPORAL_DATE_INVALID", "exact date required")
    return value


def _date_text(value: object) -> date:
    if type(value) is not str:
        _fail("TEMPORAL_DATE_INVALID", "ISO date required")
    try:
        result = date.fromisoformat(value)
    except ValueError as exc:
        raise TemporalEvidenceError("TEMPORAL_DATE_INVALID", str(exc)) from exc
    if result.isoformat() != value:
        _fail("TEMPORAL_DATE_INVALID", "canonical ISO date required")
    return result


def _utc_now() -> datetime:
    # Production APIs expose no clock parameter. Synthetic tests patch this
    # private primitive and retain the fixed unadopted evidence-role marker.
    return datetime.now(UTC)


def _instant(value: datetime) -> datetime:
    return parse_utc_datetime(utc_datetime_text(value))


def _check_elapsed(started: datetime, completed: datetime, elapsed: object) -> None:
    if type(elapsed) is not int or elapsed < 0:
        _fail("TEMPORAL_MONOTONIC_CLOCK_INVALID", "nonnegative elapsed time required")
    utc_elapsed_ns = ((completed - started) // timedelta(microseconds=1)) * _UTC_RESOLUTION_NS
    if completed < started or utc_elapsed_ns + _UTC_RESOLUTION_NS < elapsed:
        _fail("TEMPORAL_CLOCK_BACKWARD", "UTC interval contradicts its inner monotonic interval")


def _root(path: Path) -> Path:
    if not path.is_absolute():
        _fail("TEMPORAL_STORE_ROOT_INVALID", "explicit absolute existing root required")
    for component in (*reversed(path.parents), path):
        metadata = component.lstat()
        if stat.S_ISLNK(metadata.st_mode) or getattr(metadata, "st_file_attributes", 0) & getattr(
            stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400
        ):
            _fail("TEMPORAL_STORE_REPARSE", str(component))
        if not stat.S_ISDIR(metadata.st_mode):
            _fail("TEMPORAL_STORE_ROOT_INVALID", "directory required")
    return path.resolve(strict=True)


def _read(root: Path, binding: EventBinding) -> bytes:
    if type(binding) is not EventBinding:
        _fail("TEMPORAL_BINDING_INVALID", "exact EventBinding required")
    content = read_contained_artifact_bytes(root=root, relative_path=binding.relative_path)
    if _binding(binding.relative_path, content) != binding:
        _fail("TEMPORAL_CONTENT_MISMATCH", binding.relative_path)
    return content


def _optional(root: Path, path: str) -> bytes | None:
    try:
        return read_contained_artifact_bytes(root=root, relative_path=path)
    except DataPublicationIntegrityError as exc:
        if exc.code == "CONTAINED_ARTIFACT_MISSING":
            return None
        if exc.code == "ARTIFACT_BOUND_DIRECTORY_FAILED" and isinstance(
            exc.__cause__, FileNotFoundError
        ):
            return None
        raise


@dataclass(frozen=True)
class TimeEvidencePolicy:
    source_root: Path
    binding: EventBinding
    calendar_bindings: tuple[EventBinding, ...]
    clock_policy_binding: EventBinding | None = None


def load_time_evidence_policy(
    *, source_root: Path, policy_path: str = POLICY_PATH
) -> TimeEvidencePolicy:
    """Read the exact named policy and current reviewed calendar; never adopt it."""
    source = source_root.resolve(strict=True)
    if source != SOURCE_ROOT:
        _fail("TEMPORAL_SOURCE_ROOT_MISMATCH", "use the recorder's actual source checkout")
    if type(policy_path) is not str or policy_path not in _POLICIES:
        _fail("TEMPORAL_POLICY_IDENTITY_MISMATCH", str(policy_path))
    content = read_contained_artifact_bytes(root=source, relative_path=policy_path)
    policy_binding = _binding(policy_path, content)
    if policy_binding.sha256 != _POLICIES[policy_path]:
        _fail("TEMPORAL_POLICY_IDENTITY_MISMATCH", policy_path)
    policy = _object(load_strict_yaml_text(content.decode("utf-8"), label=policy_path))
    if policy.get("status") != "REVIEWED_ENGINEERING_CONTRACT_ADOPTION_BLOCKED":
        _fail("TEMPORAL_POLICY_ADOPTION_INVALID", policy_path)
    bindings = tuple(
        _binding(path, read_contained_artifact_bytes(root=source, relative_path=path))
        for path in _CALENDAR_PATHS
    )
    if default_us_equity_special_closure_policy().sha256 != bindings[-1].sha256:
        _fail("TEMPORAL_CALENDAR_CACHE_DRIFT", "loaded closure policy differs from live bytes")
    clock_binding = None
    if policy_path == POLICY_PATH:
        clock_binding = _binding(
            HOST_CLOCK_POLICY_PATH,
            read_contained_artifact_bytes(root=source, relative_path=HOST_CLOCK_POLICY_PATH),
        )
        if clock_binding.sha256 != HOST_CLOCK_POLICY_SHA256:
            _fail("TEMPORAL_CLOCK_POLICY_MISMATCH", HOST_CLOCK_POLICY_PATH)
    return TimeEvidencePolicy(source, policy_binding, bindings, clock_binding)


def _policy_live(policy: TimeEvidencePolicy) -> None:
    if (
        type(policy) is not TimeEvidencePolicy
        or load_time_evidence_policy(
            source_root=policy.source_root, policy_path=policy.binding.relative_path
        )
        != policy
    ):
        _fail("TEMPORAL_POLICY_CALENDAR_DRIFT", "policy/calendar changed")


def _is_v2(policy: TimeEvidencePolicy) -> bool:
    return policy.binding.relative_path == POLICY_PATH


def _policy_safety(policy: TimeEvidencePolicy) -> dict[str, object]:
    return _SAFETY if _is_v2(policy) else _SAFETY_V1


def _next_session(value: date) -> date:
    try:
        candidate = value + timedelta(days=1)
        while not is_us_equity_trading_day(candidate):
            candidate += timedelta(days=1)
        return candidate
    except (ValueError, OverflowError) as exc:
        raise TemporalEvidenceError("TEMPORAL_CALENDAR_INVALID", str(exc)) from exc


def _close(value: date) -> datetime:
    session = us_equity_market_session(value)
    if not session.is_trading_day or session.close_time is None:
        _fail("TEMPORAL_NOT_XNYS_SESSION", value.isoformat())
    return datetime.combine(value, session.close_time, US_EQUITY_MARKET_TIMEZONE).astimezone(UTC)


def session_timing(feature_session: date, *, policy: TimeEvidencePolicy) -> SessionTiming:
    """Use reviewed scheduled closes, including DST, partial and special sessions."""
    _policy_live(policy)
    result = _session_timing(feature_session)
    _policy_live(policy)
    return result


def _session_timing(feature_session: date) -> SessionTiming:
    # Called under a store authority only after policy/calendar capture; the
    # caller revalidates that capture outside the store authority before witness.
    feature = _date(feature_session)
    feature_close = _close(feature)
    effective = _next_session(feature)
    end = _next_session(effective)
    return SessionTiming(feature, effective, end, feature_close, _close(effective), _close(end))


def first_feature_session(activation_completed_at: datetime, *, policy: TimeEvidencePolicy) -> date:
    _policy_live(policy)
    result = _next_session(
        _instant(activation_completed_at).astimezone(US_EQUITY_MARKET_TIMEZONE).date()
    )
    _policy_live(policy)
    return result


def _slot(plan: RecordingPlan, kind: str, feature: date | None) -> str:
    prefix = f"streams/{plan.stream_id}"
    if kind == "ACTIVATION" and feature is None:
        return f"{prefix}/activation"
    if kind not in {"INPUTS_OBSERVED", "SIGNAL_RECORDED"} or feature is None:
        _fail("TEMPORAL_EVENT_KIND_INVALID", kind)
    stage = "inputs" if kind == "INPUTS_OBSERVED" else "signal"
    return f"{prefix}/sessions/{_date(feature).isoformat()}/{stage}"


def _members(values: tuple[PayloadMember, ...]) -> tuple[PayloadMember, ...]:
    if (
        type(values) is not tuple
        or not values
        or any(type(item) is not PayloadMember for item in values)
        or len({item.role for item in values}) != len(values)
    ):
        _fail("TEMPORAL_MEMBERS_INVALID", "nonempty immutable members with unique roles required")
    return tuple(sorted(values, key=lambda item: item.role))


def _covered(path: str, declaration: str) -> bool:
    # S4D treats Windows path claims case-insensitively and by components.
    target = PurePosixPath(path.casefold())
    scope = PurePosixPath(declaration.casefold())
    return target == scope or scope in target.parents


def _check_snapshot(
    event_content: bytes,
    intent_content: bytes,
    *,
    store_relative_path: str,
    plan: RecordingPlan,
    started_at: datetime,
    completed_at: datetime,
    admission_bound_ns: int | None = None,
) -> None:
    raw_event = _object(strict_json_loads(event_content))
    event = parse_lease_event(raw_event)
    _equal(raw_event, event.to_dict(), "TEMPORAL_LEASE_EVENT_INVALID")
    intent = _object(strict_json_loads(intent_content))
    identity = _object(intent.get("workspace_identity"))
    lease = event.lease
    if (
        intent.get("schema_version") != "checkout_operation_intent.v1"
        or intent.get("operation_class") not in {"domain_mutation", "shared_mutation"}
        or intent.get("actor") != lease.actor
        or f"checkout:{intent.get('intent_id')}" != lease.change_id
        or intent.get("base_commit") != plan.declared_source_commit
        or identity.get("head_commit") != plan.declared_source_commit
        or lease.base_commit != plan.declared_source_commit
        or event.to_state != "ACTIVE"
        or lease.state != "ACTIVE"
        or lease.acquired_at is None
        or lease.expires_at is None
        or not (
            parse_utc_datetime(lease.acquired_at)
            <= started_at
            <= completed_at
            < parse_utc_datetime(lease.expires_at)
        )
        or parse_utc_datetime(event.occurred_at) > started_at
        or not any(
            claim.kind == "path"
            and claim.access.value == "WRITE"
            and _covered(store_relative_path, claim.resource_id)
            for claim in lease.resources
        )
    ):
        _fail("TEMPORAL_LEASE_BINDING_INVALID", "active scoped S4D evidence required")
    if admission_bound_ns is not None and not deadline_allows(
        admission_bound_ns, parse_utc_datetime(lease.expires_at)
    ):
        _fail("TEMPORAL_LEASE_BOUND_EXPIRED", "complete clock bound reaches lease expiry")
    declared = [*intent.get("owned_paths", []), *intent.get("shared_paths", [])]
    if not all(type(item) is str for item in declared) or not any(
        _covered(store_relative_path, item) for item in declared
    ):
        _fail("TEMPORAL_LEASE_SCOPE_INVALID", store_relative_path)


def _live_lease(
    handle: CheckoutLeaseHandle, root: Path, plan: RecordingPlan, at: datetime
) -> tuple[bytes, bytes, str]:
    if type(handle) is not CheckoutLeaseHandle or handle.released:
        _fail("TEMPORAL_LEASE_REQUIRED", "active S4D mutation handle required")
    guard = handle.guard
    intent = handle.decision.intent
    identity = resolve_checkout_identity(guard.project_root)
    if (
        identity != intent.workspace_identity
        or intent.operation_class
        not in {CheckoutOperationClass.DOMAIN_MUTATION, CheckoutOperationClass.SHARED_MUTATION}
        or not root.is_relative_to(guard.project_root)
        or root == guard.project_root
    ):
        _fail("TEMPORAL_LEASE_CHECKOUT_MISMATCH", "store and current checkout must match intent")
    relative = root.relative_to(guard.project_root).as_posix()
    # Check every original owned directory, not only its resolved final target.
    path = guard.project_root
    for part in PurePosixPath(relative).parts:
        path = path / part
        _root(path)
    replay = guard.replay()
    heads = {item.lease_id: item for item in replay.active_leases}
    if replay.status != "PASS" or handle.lease_id not in heads:
        _fail("TEMPORAL_LEASE_INACTIVE", handle.lease_id)
    event_id = dict(replay.head_event_ids)[handle.lease_id]
    event_path = guard.store.events_root / handle.lease_id / f"{event_id}.json"
    event_bytes = read_contained_artifact_bytes(
        root=guard.project_root,
        relative_path=event_path.relative_to(guard.project_root).as_posix(),
    )
    intent_bytes = read_contained_artifact_bytes(
        root=guard.project_root,
        relative_path=handle.decision.intent_path.relative_to(guard.project_root).as_posix(),
    )
    _equal(strict_json_loads(intent_bytes), intent.to_dict(), "TEMPORAL_LEASE_INTENT_DRIFT")
    _check_snapshot(
        event_bytes,
        intent_bytes,
        store_relative_path=relative,
        plan=plan,
        started_at=at,
        completed_at=at,
    )
    return event_bytes, intent_bytes, relative


@dataclass(frozen=True)
class RecordedTemporalEvidence:
    """Finite verified local facts; downstream never accepts this DTO as authority."""

    binding: EventBinding
    plan: RecordingPlan
    event_kind: str
    feature_session: date | None
    previous_event: EventBinding | None
    started_at: datetime
    payload_durable_completed_at: datetime
    first_feature_session: date
    timing: SessionTiming | None
    temporal_status: str
    clock_evidence: HostClockEvidence | None = None
    payload_admission_bound_ns: int | None = None
    return_clock_evidence: HostClockEvidence | None = None

    @property
    def admission_bound_ns(self) -> int:
        evidence = self.return_clock_evidence or self.clock_evidence
        if evidence is None:
            _fail("TEMPORAL_LEGACY_CLOCK_BOUND_UNAVAILABLE", "v1 has no outer clock evidence")
        return evidence.admission_bound_ns

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "schema_version": (
                "prospective_time_evidence.v2"
                if self.clock_evidence
                else "prospective_time_evidence.v1"
            ),
            "event": self.binding.to_dict(),
            "plan": self.plan.to_dict(),
            "event_kind": self.event_kind,
            "feature_session": self.feature_session.isoformat() if self.feature_session else None,
            "previous_event": self.previous_event.to_dict() if self.previous_event else None,
            "started_at": utc_datetime_text(self.started_at),
            "payload_durable_completed_at": utc_datetime_text(self.payload_durable_completed_at),
            "first_feature_session": self.first_feature_session.isoformat(),
            "session_timing": self.timing.to_dict() if self.timing else None,
            "temporal_status": self.temporal_status,
            "calendar_close_authority": "REVIEWED_SCHEDULED_CLOSE",
            **(_SAFETY if self.clock_evidence else _SAFETY_V1),
        }
        if self.clock_evidence is not None:
            result.update(
                clock_evidence=self.clock_evidence.to_dict(),
                payload_admission_bound_ns=self.payload_admission_bound_ns,
                return_clock_evidence=(
                    self.return_clock_evidence.to_dict() if self.return_clock_evidence else None
                ),
            )
        return result


def _semantic_intent(
    plan: RecordingPlan,
    kind: str,
    feature: date | None,
    previous: EventBinding | None,
    members: tuple[PayloadMember, ...],
    policy: TimeEvidencePolicy,
) -> dict[str, object]:
    slot = _slot(plan, kind, feature)
    result: dict[str, object] = {
        "schema_version": (
            "prospective_time_intent.v2" if _is_v2(policy) else "prospective_time_intent.v1"
        ),
        "plan": plan.to_dict(),
        "stream_id": plan.stream_id,
        "event_kind": kind,
        "stage_sequence": _STAGES[kind],
        "feature_session": feature.isoformat() if feature else None,
        "previous_event": previous.to_dict() if previous else None,
        "policy_binding": policy.binding.to_dict(),
        "calendar_bindings": [item.to_dict() for item in policy.calendar_bindings],
        "payload_bindings": [
            {
                "role": item.role,
                "artifact": _binding(f"{slot}/payload_{item.role}.bin", item.content).to_dict(),
            }
            for item in members
        ],
    }
    if _is_v2(policy):
        assert policy.clock_policy_binding is not None
        result["clock_policy_binding"] = policy.clock_policy_binding.to_dict()
    return result


def _parent(
    root: Path,
    plan: RecordingPlan,
    kind: str,
    feature: date | None,
    previous: EventBinding | None,
    policy: TimeEvidencePolicy,
) -> RecordedTemporalEvidence | None:
    if kind == "ACTIVATION":
        if previous is not None or feature is not None:
            _fail("TEMPORAL_PREDECESSOR_INVALID", kind)
        return None
    if previous is None or feature is None:
        _fail("TEMPORAL_PREDECESSOR_INVALID", kind)
    expected_kind = "ACTIVATION" if kind == "INPUTS_OBSERVED" else "INPUTS_OBSERVED"
    expected_feature = None if expected_kind == "ACTIVATION" else feature
    if (
        type(previous) is not EventBinding
        or previous.relative_path
        != f"{_slot(plan, expected_kind, expected_feature)}/completion.json"
    ):
        _fail("TEMPORAL_PREDECESSOR_INVALID", "wrong predecessor slot")
    parent = verify_time_evidence(store_root=root, event=previous, policy=policy)
    if (
        parent.plan != plan
        or parent.event_kind != expected_kind
        or (expected_kind == "INPUTS_OBSERVED" and parent.feature_session != feature)
        or feature < parent.first_feature_session
    ):
        _fail("TEMPORAL_PREDECESSOR_INVALID", "plan, feature, stage or activation differs")
    return parent


def _result_state(
    *,
    kind: str,
    feature: date | None,
    parent: RecordedTemporalEvidence | None,
    started: datetime,
    completed: datetime,
    policy: TimeEvidencePolicy,
    admission_bound_ns: int | None = None,
    started_raw_utc_ns: int | None = None,
) -> tuple[date, SessionTiming | None, str]:
    if completed < started or (
        parent is not None
        and started
        < (
            utc_ns_to_datetime_floor(parent.clock_evidence.latest_sample.utc_ns)
            if parent.clock_evidence is not None
            else parent.payload_durable_completed_at
        )
    ):
        _fail("TEMPORAL_CLOCK_BACKWARD", "event clock precedes start or predecessor")
    if _is_v2(policy):
        if admission_bound_ns is None or type(started_raw_utc_ns) is not int:
            _fail(
                "TEMPORAL_CLOCK_EVIDENCE_REQUIRED",
                "v2 requires original raw start and replayed bound",
            )
        if parent is not None and (
            parent.clock_evidence is None
            or started_raw_utc_ns < parent.clock_evidence.latest_sample.utc_ns
        ):
            _fail("TEMPORAL_CLOCK_BACKWARD", "raw ns event anchor precedes predecessor")
    if kind == "ACTIVATION":
        upper = (
            utc_ns_to_datetime_ceil(admission_bound_ns)
            if admission_bound_ns is not None
            else completed
        )
        first = _next_session(upper.astimezone(US_EQUITY_MARKET_TIMEZONE).date())
        return first, None, "ACTIVATION_RECORDED"
    if feature is None or parent is None:
        _fail("TEMPORAL_PREDECESSOR_INVALID", kind)
    timing = _session_timing(feature)
    if started <= timing.feature_close_at:
        _fail("TEMPORAL_FEATURE_NOT_CLOSED", "write must start strictly after the feature close")
    timely = (
        deadline_allows(admission_bound_ns, timing.effective_close_at)
        if admission_bound_ns is not None
        else completed < timing.effective_close_at
    )
    status = "TIMELY_PAYLOAD" if timely else "LATE_PAYLOAD"
    return parent.first_feature_session, timing, status


def _existing_completion(
    root: Path,
    completion_path: str,
    content: bytes,
    semantic: dict[str, object],
    policy: TimeEvidencePolicy,
) -> RecordedTemporalEvidence:
    evidence = verify_time_evidence(
        store_root=root, event=_binding(completion_path, content), policy=policy
    )
    witness = _object(strict_json_loads(content))
    intent = _object(strict_json_loads(_read(root, EventBinding.from_dict(witness["intent"]))))
    _equal(intent["semantic"], semantic, "TEMPORAL_EVENT_CONFLICT")
    # No clock or live lease is acquired, and no original return observation is
    # manufactured from a retained payload witness.
    return evidence


def _record(
    *,
    store_root: Path,
    plan: RecordingPlan,
    kind: str,
    feature: date | None,
    previous: EventBinding | None,
    members: tuple[PayloadMember, ...],
    policy: TimeEvidencePolicy,
    lease_handle: CheckoutLeaseHandle,
) -> RecordedTemporalEvidence:
    _policy_live(policy)
    if type(plan) is not RecordingPlan:
        _fail("TEMPORAL_PLAN_INVALID", "exact declaration type required")
    members = _members(members)
    if kind == "ACTIVATION" and tuple(item.binding for item in members) != plan.definition_bindings:
        _fail("TEMPORAL_DEFINITION_MISMATCH", "persist the exact declared definition bytes")
    if kind == "SIGNAL_RECORDED" and tuple(item.role for item in members) != ("signal",):
        _fail("TEMPORAL_SIGNAL_MEMBER_INVALID", "one complete signal payload required")
    root = _root(store_root)
    semantic = _semantic_intent(plan, kind, feature, previous, members, policy)
    slot = _slot(plan, kind, feature)
    completion_path = f"{slot}/completion.json"
    parent = _parent(root, plan, kind, feature, previous, policy)
    original = _optional(root, completion_path)
    if original is not None:
        return _existing_completion(root, completion_path, original, semantic, policy)
    if not _is_v2(policy):
        _fail("TEMPORAL_LEGACY_RECORDING_DISABLED", "v1 policy is retained-verification only")
    lease_event, lease_intent, relative = _live_lease(
        lease_handle, root, plan, _instant(_utc_now())
    )
    sampler = HostClockSampler.start(source_root=policy.source_root)
    sampler.recheck_policy()
    started = utc_ns_to_datetime_floor(sampler.anchor.utc_ns)
    # Root-bound store authority may not read a different policy/checkout root.
    # Reserve/persist payload first; revalidate external authorities before a
    # second store transaction publishes the witness. Any gap is INCOMPLETE.
    with exclusive_store_maintenance(store_root=root) as maintenance:
        existing = _optional(root, completion_path)
        if existing is None:
            expected_paths = (
                f"{slot}/intent.json",
                f"{slot}/lease_event.json",
                f"{slot}/lease_intent.json",
                *(f"{slot}/payload_{item.role}.bin" for item in members),
            )
            if any(_optional(root, path) is not None for path in expected_paths):
                _fail(
                    "TEMPORAL_INCOMPLETE_EVENT",
                    "preserve this slot; never backdate a missing witness",
                )
            initial = sampler.checkpoint(
                "pre_payload",
                inherited_child_bound_ns=parent.admission_bound_ns if parent is not None else None,
            )
            _result_state(
                kind=kind,
                feature=feature,
                parent=parent,
                started=started,
                completed=utc_ns_to_datetime_floor(initial.latest_sample.utc_ns),
                policy=policy,
                admission_bound_ns=initial.admission_bound_ns,
                started_raw_utc_ns=sampler.anchor.utc_ns,
            )
            _check_snapshot(
                lease_event,
                lease_intent,
                store_relative_path=relative,
                plan=plan,
                started_at=started,
                completed_at=utc_ns_to_datetime_floor(initial.latest_sample.utc_ns),
                admission_bound_ns=initial.admission_bound_ns,
            )
            intent = {
                "semantic": semantic,
                "original_store_relative_path": relative,
                "lease_event": _binding(f"{slot}/lease_event.json", lease_event).to_dict(),
                "lease_intent": _binding(f"{slot}/lease_intent.json", lease_intent).to_dict(),
            }
            intent_bytes = canonical_json_bytes(intent)
            payload_writes = (
                (f"{slot}/intent.json", intent_bytes),
                (f"{slot}/lease_event.json", lease_event),
                (f"{slot}/lease_intent.json", lease_intent),
                *((f"{slot}/payload_{item.role}.bin", item.content) for item in members),
            )
            for path, content in payload_writes:
                write_contained_artifact_bytes(
                    root=root, relative_path=path, content=content, immutable=True
                )
            # The writer above has returned after fsync, namespace sync and attestation.
            # Preserve raw UTC separately; admission uses the replayed whole
            # outer interval, including lock acquisition and every payload write.
            payload_clock = sampler.checkpoint("payload_complete")
            elapsed = (
                payload_clock.latest_sample.counter_after_ns - sampler.anchor.counter_before_ns
            )
            completed = utc_ns_to_datetime_floor(payload_clock.latest_sample.utc_ns)
            first, timing, status = _result_state(
                kind=kind,
                feature=feature,
                parent=parent,
                started=started,
                completed=completed,
                policy=policy,
                admission_bound_ns=payload_clock.admission_bound_ns,
                started_raw_utc_ns=sampler.anchor.utc_ns,
            )
            _check_snapshot(
                lease_event,
                lease_intent,
                store_relative_path=relative,
                plan=plan,
                started_at=started,
                completed_at=completed,
                admission_bound_ns=payload_clock.admission_bound_ns,
            )
            maintenance.mark_committed()
    if existing is not None:
        return _existing_completion(root, completion_path, existing, semantic, policy)
    lease_checked_at = _instant(_utc_now())
    if lease_checked_at < completed:
        _fail("TEMPORAL_CLOCK_BACKWARD", "lease recheck clock precedes payload completion")
    _live_lease(lease_handle, root, plan, lease_checked_at)
    _policy_live(policy)
    sampler.recheck_policy()
    if _parent(root, plan, kind, feature, previous, policy) != parent:
        _fail("TEMPORAL_PREDECESSOR_INVALID", "predecessor changed during payload persistence")
    post_payload = sampler.checkpoint("post_payload_guards")
    if utc_ns_to_datetime_floor(post_payload.latest_sample.utc_ns) < lease_checked_at:
        _fail("TEMPORAL_CLOCK_BACKWARD", "postguard clock precedes lease recheck")
    with exclusive_store_maintenance(store_root=root) as maintenance:
        if _optional(root, completion_path) is not None:
            _fail("TEMPORAL_EVENT_CONFLICT", "completion appeared before this recorder's witness")
        for path, content in payload_writes:
            _read(root, _binding(path, content))
        witness_clock = sampler.checkpoint("witness_precommit")
        witness_started = utc_ns_to_datetime_floor(witness_clock.latest_sample.utc_ns)
        if witness_started < lease_checked_at:
            _fail("TEMPORAL_CLOCK_BACKWARD", "witness start clock precedes lease recheck")
        _check_snapshot(
            lease_event,
            lease_intent,
            store_relative_path=relative,
            plan=plan,
            started_at=witness_started,
            completed_at=witness_started,
            admission_bound_ns=witness_clock.admission_bound_ns,
        )
        witness = {
            "schema_version": "prospective_time_completion.v2",
            "intent": _binding(f"{slot}/intent.json", intent_bytes).to_dict(),
            "started_at": utc_datetime_text(started),
            "payload_durable_completed_at": utc_datetime_text(completed),
            "payload_outer_elapsed_ns": elapsed,
            "payload_admission_bound_ns": payload_clock.admission_bound_ns,
            "witness_write_started_at": utc_datetime_text(witness_started),
            "clock_evidence": witness_clock.to_dict(),
            "first_feature_session": first.isoformat(),
            "session_timing": timing.to_dict() if timing else None,
            "temporal_status": status,
            "safety": dict(_SAFETY),
        }
        witness_bytes = canonical_json_bytes(witness)
        write_contained_artifact_bytes(
            root=root, relative_path=completion_path, content=witness_bytes, immutable=True
        )
        maintenance.mark_committed()
    verified = verify_time_evidence(
        store_root=root, event=_binding(completion_path, witness_bytes), policy=policy
    )
    verified_at = _instant(_utc_now())
    if verified_at < witness_started:
        _fail("TEMPORAL_CLOCK_BACKWARD", "final check clock precedes witness start")
    _live_lease(lease_handle, root, plan, verified_at)
    _policy_live(policy)
    sampler.recheck_policy()
    returned = sampler.checkpoint("recorder_return")
    if utc_ns_to_datetime_floor(returned.latest_sample.utc_ns) < verified_at:
        _fail("TEMPORAL_CLOCK_BACKWARD", "return observation precedes final live check")
    _check_snapshot(
        lease_event,
        lease_intent,
        store_relative_path=relative,
        plan=plan,
        started_at=started,
        completed_at=utc_ns_to_datetime_floor(returned.latest_sample.utc_ns),
        admission_bound_ns=returned.admission_bound_ns,
    )
    assert verified.clock_evidence is not None
    require_clock_evidence_extension(verified.clock_evidence, returned)
    return replace(verified, return_clock_evidence=returned)


def record_activation(
    *,
    store_root: Path,
    plan: RecordingPlan,
    definitions: tuple[PayloadMember, ...],
    policy: TimeEvidencePolicy,
    lease_handle: CheckoutLeaseHandle,
) -> RecordedTemporalEvidence:
    return _record(
        store_root=store_root,
        plan=plan,
        kind="ACTIVATION",
        feature=None,
        previous=None,
        members=definitions,
        policy=policy,
        lease_handle=lease_handle,
    )


def record_local_input_observation(
    *,
    store_root: Path,
    plan: RecordingPlan,
    feature_session: date,
    inputs: tuple[PayloadMember, ...],
    activation: EventBinding,
    policy: TimeEvidencePolicy,
    lease_handle: CheckoutLeaseHandle,
) -> RecordedTemporalEvidence:
    return _record(
        store_root=store_root,
        plan=plan,
        kind="INPUTS_OBSERVED",
        feature=_date(feature_session),
        previous=activation,
        members=inputs,
        policy=policy,
        lease_handle=lease_handle,
    )


def record_signal_completion(
    *,
    store_root: Path,
    plan: RecordingPlan,
    feature_session: date,
    signal: PayloadMember,
    inputs: EventBinding,
    policy: TimeEvidencePolicy,
    lease_handle: CheckoutLeaseHandle,
) -> RecordedTemporalEvidence:
    return _record(
        store_root=store_root,
        plan=plan,
        kind="SIGNAL_RECORDED",
        feature=_date(feature_session),
        previous=inputs,
        members=(signal,),
        policy=policy,
        lease_handle=lease_handle,
    )


def verify_time_evidence(
    *,
    store_root: Path,
    event: EventBinding,
    policy: TimeEvidencePolicy,
) -> RecordedTemporalEvidence:
    """Re-read exact bound bytes; no active lease, DQ, producer or event writes."""
    _policy_live(policy)
    root = _root(store_root)
    if type(event) is not EventBinding or not _COMPLETION_LOCATOR.fullmatch(event.relative_path):
        _fail("TEMPORAL_EVENT_LOCATOR_INVALID", "canonical completion locator required")
    witness_bytes = _read(root, event)
    version2 = _is_v2(policy)
    clock_fields = (
        {
            "payload_outer_elapsed_ns",
            "payload_admission_bound_ns",
            "witness_write_started_at",
            "clock_evidence",
        }
        if version2
        else {"monotonic_elapsed_ns"}
    )
    witness = _object(
        strict_json_loads(witness_bytes),
        {
            "schema_version",
            "intent",
            "started_at",
            "payload_durable_completed_at",
            *clock_fields,
            "first_feature_session",
            "session_timing",
            "temporal_status",
            "safety",
        },
    )
    if (
        witness["schema_version"]
        != ("prospective_time_completion.v2" if version2 else "prospective_time_completion.v1")
        or canonical_json_bytes(witness) != witness_bytes
    ):
        _fail("TEMPORAL_WITNESS_INVALID", "canonical completion witness required")
    _equal(witness["safety"], _policy_safety(policy), "TEMPORAL_SAFETY_INVALID")
    elapsed = witness["payload_outer_elapsed_ns" if version2 else "monotonic_elapsed_ns"]
    if type(elapsed) is not int or elapsed < 0:
        _fail("TEMPORAL_MONOTONIC_CLOCK_INVALID", "nonnegative elapsed time required")
    intent_binding = EventBinding.from_dict(witness["intent"])
    if intent_binding.relative_path != str(
        PurePosixPath(event.relative_path).with_name("intent.json")
    ):
        _fail("TEMPORAL_EVENT_LOCATOR_INVALID", "intent must be in the same fixed event slot")
    intent_bytes = _read(root, intent_binding)
    intent = _object(
        strict_json_loads(intent_bytes),
        {"semantic", "original_store_relative_path", "lease_event", "lease_intent"},
    )
    if canonical_json_bytes(intent) != intent_bytes:
        _fail("TEMPORAL_INTENT_INVALID", "canonical immutable intent required")
    semantic = _object(
        intent["semantic"],
        {
            "schema_version",
            "plan",
            "stream_id",
            "event_kind",
            "stage_sequence",
            "feature_session",
            "previous_event",
            "policy_binding",
            "calendar_bindings",
            "payload_bindings",
            *({"clock_policy_binding"} if version2 else set()),
        },
    )
    plan = RecordingPlan.from_dict(semantic["plan"])
    kind = semantic["event_kind"]
    feature = (
        _date_text(semantic["feature_session"]) if semantic["feature_session"] is not None else None
    )
    if type(kind) is not str or kind not in _STAGES:
        _fail("TEMPORAL_EVENT_KIND_INVALID", str(kind))
    slot = _slot(plan, kind, feature)
    if (
        event.relative_path != f"{slot}/completion.json"
        or intent_binding.relative_path != f"{slot}/intent.json"
    ):
        _fail("TEMPORAL_EVENT_LOCATOR_INVALID", "wrong fixed event slot")
    previous = (
        EventBinding.from_dict(semantic["previous_event"])
        if semantic["previous_event"] is not None
        else None
    )
    # Validate the predecessor's fixed stage/path before recursing; cycles and
    # same-stage/cross-stream references therefore fail at depth at most three.
    if kind == "ACTIVATION":
        if previous is not None:
            _fail("TEMPORAL_PREDECESSOR_INVALID", kind)
    else:
        expected_kind = "ACTIVATION" if kind == "INPUTS_OBSERVED" else "INPUTS_OBSERVED"
        expected_feature = None if expected_kind == "ACTIVATION" else feature
        if (
            previous is None
            or previous.relative_path
            != f"{_slot(plan, expected_kind, expected_feature)}/completion.json"
        ):
            _fail("TEMPORAL_PREDECESSOR_INVALID", "wrong predecessor slot")
    rows = semantic["payload_bindings"]
    if type(rows) is not list or not rows:
        _fail("TEMPORAL_MEMBERS_INVALID", "nonempty member list required")
    members_list: list[PayloadMember] = []
    for raw in rows:
        row = _object(raw, {"role", "artifact"})
        binding = EventBinding.from_dict(row["artifact"])
        role = row["role"]
        PayloadMember(role, b"")  # Check the role before following a member locator.
        if binding.relative_path != f"{slot}/payload_{role}.bin":
            _fail("TEMPORAL_MEMBER_LOCATOR_INVALID", binding.relative_path)
        member = PayloadMember(role, _read(root, binding))
        members_list.append(member)
    members = _members(tuple(members_list))
    if kind == "ACTIVATION" and tuple(item.binding for item in members) != plan.definition_bindings:
        _fail("TEMPORAL_DEFINITION_MISMATCH", "activation definition bytes differ")
    if kind == "SIGNAL_RECORDED" and tuple(item.role for item in members) != ("signal",):
        _fail("TEMPORAL_SIGNAL_MEMBER_INVALID", kind)
    _equal(
        semantic,
        _semantic_intent(plan, kind, feature, previous, members, policy),
        "TEMPORAL_INTENT_BINDING_MISMATCH",
    )
    parent = _parent(root, plan, kind, feature, previous, policy)
    started = parse_utc_datetime(witness["started_at"])
    completed = parse_utc_datetime(witness["payload_durable_completed_at"])
    clock_evidence = None
    payload_bound = None
    witness_started = completed
    if version2:
        clock_evidence = HostClockEvidence.from_dict(witness["clock_evidence"])
        checkpoints = clock_evidence.checkpoints
        if tuple(row.label for row in checkpoints) != (
            "pre_payload",
            "payload_complete",
            "post_payload_guards",
            "witness_precommit",
        ):
            _fail(
                "TEMPORAL_CLOCK_STAGE_INVALID", "complete fixed recorder checkpoint chain required"
            )
        payload = checkpoints[1]
        payload_bound = witness["payload_admission_bound_ns"]
        if type(payload_bound) is not int or payload_bound != payload.admission_bound_ns:
            _fail("TEMPORAL_CLOCK_BOUND_INVALID", "payload bound differs from original checkpoint")
        witness_started = parse_utc_datetime(witness["witness_write_started_at"])
        if (
            started != utc_ns_to_datetime_floor(clock_evidence.anchor.utc_ns)
            or completed != utc_ns_to_datetime_floor(payload.sample.utc_ns)
            or witness_started != utc_ns_to_datetime_floor(clock_evidence.latest_sample.utc_ns)
            or elapsed != payload.sample.counter_after_ns - clock_evidence.anchor.counter_before_ns
            or checkpoints[0].inherited_child_bound_ns
            != (parent.admission_bound_ns if parent is not None else None)
            or any(row.inherited_child_bound_ns is not None for row in checkpoints[1:])
        ):
            _fail(
                "TEMPORAL_CLOCK_BINDING_INVALID",
                "raw observations, outer duration or predecessor differ",
            )
    else:
        _check_elapsed(started, completed, elapsed)
    first, timing, status = _result_state(
        kind=kind,
        feature=feature,
        parent=parent,
        started=started,
        completed=completed,
        policy=policy,
        admission_bound_ns=payload_bound,
        started_raw_utc_ns=clock_evidence.anchor.utc_ns if clock_evidence else None,
    )
    _equal(witness["first_feature_session"], first.isoformat(), "TEMPORAL_FIRST_SESSION_INVALID")
    _equal(
        witness["session_timing"],
        timing.to_dict() if timing else None,
        "TEMPORAL_SESSION_TIMING_INVALID",
    )
    _equal(witness["temporal_status"], status, "TEMPORAL_STATUS_INVALID")
    lease_event = EventBinding.from_dict(intent["lease_event"])
    lease_intent = EventBinding.from_dict(intent["lease_intent"])
    if (
        lease_event.relative_path != f"{slot}/lease_event.json"
        or lease_intent.relative_path != f"{slot}/lease_intent.json"
    ):
        _fail("TEMPORAL_LEASE_LOCATOR_INVALID", slot)
    relative = intent["original_store_relative_path"]
    EventBinding(relative, "0" * 64, 0)  # Validate a portable path, never a permission token.
    _check_snapshot(
        _read(root, lease_event),
        _read(root, lease_intent),
        store_relative_path=relative,
        plan=plan,
        started_at=started,
        completed_at=witness_started,
        admission_bound_ns=clock_evidence.admission_bound_ns if clock_evidence else None,
    )
    _policy_live(policy)
    _read(root, event)
    _read(root, intent_binding)
    return RecordedTemporalEvidence(
        event,
        plan,
        kind,
        feature,
        previous,
        started,
        completed,
        first,
        timing,
        status,
        clock_evidence,
        payload_bound,
    )


def verify_recorder_return_evidence(
    *,
    store_root: Path,
    event: EventBinding,
    return_clock_evidence: HostClockEvidence,
    policy: TimeEvidencePolicy,
) -> RecordedTemporalEvidence:
    """Replay a caller-retained original return observation; never create one.

    Source execution and the original successful-call association remain the
    parent consumer's obligations. This replays the complete child prefix and
    its retained lease expiry without consulting any live clock or lease.
    """
    verified = verify_time_evidence(store_root=store_root, event=event, policy=policy)
    if verified.clock_evidence is None:
        _fail("TEMPORAL_LEGACY_RETURN_EVIDENCE_INVALID", "v1 has no compatible outer prefix")
    require_clock_evidence_extension(verified.clock_evidence, return_clock_evidence)
    tail = return_clock_evidence.checkpoints[len(verified.clock_evidence.checkpoints) :]
    if (
        len(tail) != 1
        or tail[0].label != "recorder_return"
        or tail[0].inherited_child_bound_ns is not None
    ):
        _fail("TEMPORAL_RETURN_CLOCK_STAGE_INVALID", "one original postguard observation required")
    root = _root(store_root)
    witness = _object(strict_json_loads(_read(root, event)))
    intent = _object(strict_json_loads(_read(root, EventBinding.from_dict(witness["intent"]))))
    _check_snapshot(
        _read(root, EventBinding.from_dict(intent["lease_event"])),
        _read(root, EventBinding.from_dict(intent["lease_intent"])),
        store_relative_path=intent["original_store_relative_path"],
        plan=verified.plan,
        started_at=verified.started_at,
        completed_at=utc_ns_to_datetime_floor(return_clock_evidence.latest_sample.utc_ns),
        admission_bound_ns=return_clock_evidence.admission_bound_ns,
    )
    return replace(verified, return_clock_evidence=return_clock_evidence)


def project_session_coverage(
    *,
    store_root: Path,
    activation: EventBinding,
    through_feature_session: date,
    checked_at: datetime,
    policy: TimeEvidencePolicy,
) -> dict[str, object]:
    """Read only the explicit plan's canonical session slots, never market rows."""
    root = _root(store_root)
    activated = verify_time_evidence(store_root=root, event=activation, policy=policy)
    if activated.event_kind != "ACTIVATION":
        _fail("TEMPORAL_ACTIVATION_REQUIRED", activation.relative_path)
    end = _date(through_feature_session)
    review_time = _instant(checked_at)
    if (
        review_time < activated.payload_durable_completed_at
        or end < activated.first_feature_session
    ):
        _fail(
            "TEMPORAL_COVERAGE_RANGE_INVALID",
            "requested review precedes activation or first feature",
        )
    rows: list[dict[str, object]] = []
    feature = activated.first_feature_session
    while feature <= end:
        timing = session_timing(feature, policy=policy)
        slot = _slot(activated.plan, "SIGNAL_RECORDED", feature)
        content = _optional(root, f"{slot}/completion.json")
        event_binding: EventBinding | None = None
        if content is not None:
            event_binding = _binding(f"{slot}/completion.json", content)
            evidence = verify_time_evidence(store_root=root, event=event_binding, policy=policy)
            after_review = (
                evidence.payload_admission_bound_ns > datetime_to_utc_ns(review_time)
                if evidence.payload_admission_bound_ns is not None
                else evidence.payload_durable_completed_at > review_time
            )
            if after_review:
                status = "NOT_RECORDED_BY_REVIEW_TIME"
            else:
                status = (
                    "RECORDED_PAYLOAD" if evidence.temporal_status == "TIMELY_PAYLOAD" else "LATE"
                )
        else:
            input_slot = _slot(activated.plan, "INPUTS_OBSERVED", feature)
            input_completion = _optional(root, f"{input_slot}/completion.json")
            if input_completion is not None:
                verify_time_evidence(
                    store_root=root,
                    event=_binding(f"{input_slot}/completion.json", input_completion),
                    policy=policy,
                )
            incomplete = any(
                _optional(root, f"{slot}/{name}") is not None
                for name in (
                    "intent.json",
                    "lease_event.json",
                    "lease_intent.json",
                    "payload_signal.bin",
                )
            ) or (
                input_completion is None
                and any(
                    _optional(root, f"{input_slot}/{name}") is not None
                    for name in ("intent.json", "lease_event.json", "lease_intent.json")
                )
            )
            status = (
                "INCOMPLETE"
                if incomplete
                else ("GAP" if review_time >= timing.effective_close_at else "NOT_DUE")
            )
        rows.append(
            {
                "feature_session": feature.isoformat(),
                "effective_session": timing.effective_session.isoformat(),
                "decision_deadline_at": utc_datetime_text(timing.effective_close_at),
                "status": status,
                "missed_deadline": review_time >= timing.effective_close_at
                and status != "RECORDED_PAYLOAD",
                "event": event_binding.to_dict() if event_binding else None,
            }
        )
        feature = _next_session(feature)
    _read(root, activation)
    _policy_live(policy)
    return {
        "schema_version": (
            "prospective_session_coverage.v2"
            if _is_v2(policy)
            else "prospective_session_coverage.v1"
        ),
        "plan": activated.plan.to_dict(),
        "activation": activation.to_dict(),
        "requested_start": activated.first_feature_session.isoformat(),
        "requested_end": end.isoformat(),
        "evaluated_end": rows[-1]["feature_session"],
        "checked_at": utc_datetime_text(review_time),
        "calendar_close_authority": "REVIEWED_SCHEDULED_CLOSE",
        "expected_sessions": rows,
        "old_gap_backfill_allowed": False,
        "future_session_recovery_allowed": True,
        **_policy_safety(policy),
    }
