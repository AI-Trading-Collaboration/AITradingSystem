"""Synthetic disk/clock evidence only; these tests never admit research outcomes."""

from __future__ import annotations

import hashlib
import json
import socket
import subprocess
from collections import deque
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import Barrier, Event, local
from typing import Any

import pytest

import ai_trading_system.host_clock_evidence as host_clock
import ai_trading_system.prospective_event_time_evidence as recorder
from ai_trading_system.contracts.host_clock_evidence import (
    ClockSample,
    HostClockEvidence,
    HostClockEvidenceError,
    append_clock_checkpoint,
    datetime_to_utc_ns,
)
from ai_trading_system.contracts.prospective_event_time_evidence import (
    EventBinding,
    PayloadMember,
    RecordingPlan,
    TemporalEvidenceError,
    canonical_json_bytes,
)
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutLeaseGuard,
    CheckoutLeaseHandle,
    CheckoutOperationClass,
)

ACTIVATED = datetime(2025, 3, 5, 22, tzinfo=UTC)
FEATURE = date(2025, 3, 6)
INPUT_TIME = datetime(2025, 3, 6, 21, 1, tzinfo=UTC)
SIGNAL_TIME = INPUT_TIME + timedelta(minutes=1)
DEFINITIONS = (PayloadMember("definition", b"synthetic definition v1"),)
INPUTS = (PayloadMember("prices", b"synthetic prices, not market data"),)
SIGNAL = PayloadMember("signal", b"synthetic opaque signal, not an algorithm result")


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()


def _checkout(root: Path) -> None:
    root.mkdir(parents=True)
    (root / "seed.txt").write_text("synthetic recorder fixture\n", encoding="utf-8")
    (root / ".gitignore").write_text("outputs/\n", encoding="utf-8")
    _git(root, "init", "-b", "synthetic-event-time")
    _git(root, "config", "user.email", "synthetic-event-time@example.invalid")
    _git(root, "config", "user.name", "Synthetic Event Time Test")
    _git(root, "config", "core.autocrlf", "false")
    _git(root, "add", "seed.txt", ".gitignore")
    _git(root, "commit", "-m", "synthetic fixture")


@dataclass
class _Clock:
    value: datetime = ACTIVATED

    def __post_init__(self) -> None:
        self.pending: deque[datetime] = deque()

    def __call__(self) -> datetime:
        if self.pending:
            self.value = self.pending.popleft()
        return self.value

    def event(self, started: datetime, completed: datetime | None = None) -> None:
        self.value = started
        self.pending = deque(
            (started, started, started, completed or started + timedelta(seconds=1))
        )


@dataclass
class _Harness:
    root: Path
    store: Path
    guard: CheckoutLeaseGuard
    plan: RecordingPlan
    policy: recorder.TimeEvidencePolicy
    clock: _Clock
    handle: CheckoutLeaseHandle | None = None
    acquisitions: int = 0
    acquired_at: datetime = ACTIVATED

    def renew(self, at: datetime, *, scope: str = "outputs/events") -> CheckoutLeaseHandle:
        self.release()
        self.acquisitions += 1
        self.acquired_at = at - timedelta(minutes=1)
        decision, self.handle = self.guard.acquire(
            intent_id=f"synthetic-event-{self.acquisitions}",
            task_id="TRADING-2564-SYNTHETIC-TEST",
            thread_id="synthetic-recorder-test",
            actor="integration-coordinator",
            operation_class=CheckoutOperationClass.SHARED_MUTATION,
            shared_paths=(scope,),
            base_commit=self.plan.declared_source_commit,
            now=self.acquired_at,
        )
        assert decision.status == "PASS", decision.reason_codes
        assert self.handle is not None
        self.clock.value = at
        self.clock.pending.clear()
        return self.handle

    def release(self) -> None:
        if self.handle is not None and not self.handle.released:
            instant = self.clock.value
            if instant.tzinfo is None:
                instant = self.acquired_at + timedelta(seconds=1)
            self.handle.release(
                outcome="synthetic_test_complete",
                at=max(instant, self.acquired_at + timedelta(seconds=1)),
            )

    def activate(self) -> recorder.RecordedTemporalEvidence:
        self.renew(ACTIVATED)
        self.clock.event(ACTIVATED)
        assert self.handle is not None
        return recorder.record_activation(
            store_root=self.store,
            plan=self.plan,
            definitions=DEFINITIONS,
            policy=self.policy,
            lease_handle=self.handle,
        )

    def inputs(
        self,
        activation: EventBinding,
        *,
        feature: date = FEATURE,
        at: datetime = INPUT_TIME,
    ) -> recorder.RecordedTemporalEvidence:
        self.renew(at)
        self.clock.event(at)
        assert self.handle is not None
        return recorder.record_local_input_observation(
            store_root=self.store,
            plan=self.plan,
            feature_session=feature,
            inputs=INPUTS,
            activation=activation,
            policy=self.policy,
            lease_handle=self.handle,
        )

    def signal(
        self,
        inputs: EventBinding,
        *,
        feature: date = FEATURE,
        at: datetime = SIGNAL_TIME,
        completed: datetime | None = None,
        content: PayloadMember = SIGNAL,
    ) -> recorder.RecordedTemporalEvidence:
        self.clock.event(at, completed)
        assert self.handle is not None
        return recorder.record_signal_completion(
            store_root=self.store,
            plan=self.plan,
            feature_session=feature,
            signal=content,
            inputs=inputs,
            policy=self.policy,
            lease_handle=self.handle,
        )

    def verify(self, event: EventBinding) -> recorder.RecordedTemporalEvidence:
        return recorder.verify_time_evidence(store_root=self.store, event=event, policy=self.policy)

    def coverage(self, activation: EventBinding, end: date, checked: datetime) -> dict[str, Any]:
        return recorder.project_session_coverage(
            store_root=self.store,
            activation=activation,
            through_feature_session=end,
            checked_at=checked,
            policy=self.policy,
        )


@pytest.fixture
def harness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[_Harness]:
    root = tmp_path / "trading-2564-synthetic-recorder"
    _checkout(root)
    guard = CheckoutLeaseGuard(
        project_root=root,
        runtime_root=root / "outputs/guard",
        policy_path=recorder.SOURCE_ROOT / "config/architecture/arch_005_s4d_checkout_guard.yaml",
        parallel_policy_path=(
            recorder.SOURCE_ROOT / "config/architecture/arch_005_parallel_control_policy.yaml"
        ),
    )
    store = root / "outputs/events"
    store.mkdir(parents=True)
    clock = _Clock()
    monkeypatch.setattr(recorder, "_utc_now", clock)
    # These explicit synthetic primitives affect only this clock runtime;
    # global stdlib time, leases and store-lock timing remain real.
    monkeypatch.setattr(host_clock, "_utc_ns", lambda: datetime_to_utc_ns(recorder._utc_now()))
    monkeypatch.setattr(host_clock, "_counter_ns", lambda: 0)
    instance = _Harness(
        root,
        store,
        guard,
        RecordingPlan(
            "synthetic-v1",
            "FIVE_CANDIDATE",
            _git(root, "rev-parse", "HEAD"),
            tuple(member.binding for member in DEFINITIONS),
        ),
        recorder.load_time_evidence_policy(source_root=recorder.SOURCE_ROOT),
        clock,
    )
    yield instance
    instance.release()


def _binding(path: str, content: bytes) -> EventBinding:
    return EventBinding(path, hashlib.sha256(content).hexdigest(), len(content))


def _read_json(harness: _Harness, path: str) -> dict[str, Any]:
    return json.loads((harness.store / path).read_bytes())


def _tamper(
    harness: _Harness,
    event: EventBinding,
    mutate: Callable[[dict[str, Any]], None],
    *,
    intent: bool = False,
) -> EventBinding:
    """Adversarial retained-byte editing, including recomputed outer hashes."""
    witness = _read_json(harness, event.relative_path)
    if intent:
        path = witness["intent"]["relative_path"]
        payload = _read_json(harness, path)
        mutate(payload)
        content = canonical_json_bytes(payload)
        (harness.store / path).write_bytes(content)
        witness["intent"] = _binding(path, content).to_dict()
    else:
        mutate(witness)
    content = canonical_json_bytes(witness)
    (harness.store / event.relative_path).write_bytes(content)
    return _binding(event.relative_path, content)


def _forbidden(*args: Any, **kwargs: Any) -> Any:
    raise AssertionError("unexpected writer, clock, external call or active-lease dependency")


@pytest.mark.parametrize("mode", ["live", "retained"])
def test_submicrosecond_predecessor_regression_is_rejected_without_v1_reinterpretation(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    offset = 900
    monkeypatch.setattr(
        host_clock, "_utc_ns", lambda: datetime_to_utc_ns(recorder._utc_now()) + offset
    )
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    assert inputs.clock_evidence is not None
    at = recorder.utc_ns_to_datetime_floor(inputs.clock_evidence.latest_sample.utc_ns)
    if mode == "live":
        offset = 800
        with pytest.raises(TemporalEvidenceError, match="raw ns event anchor precedes predecessor"):
            harness.signal(inputs.binding, at=at)
        slot = harness.store / f"streams/{harness.plan.stream_id}/sessions/{FEATURE}/signal"
        assert not (slot / "intent.json").exists()
        assert not (slot / "completion.json").exists()
        return
    signal = harness.signal(inputs.binding, at=at)
    assert signal.clock_evidence is not None

    def mutate(witness: dict[str, Any]) -> None:
        original = HostClockEvidence.from_dict(witness["clock_evidence"])
        rebuilt = HostClockEvidence(
            original.provider, replace(original.anchor, utc_ns=original.anchor.utc_ns - 100)
        )
        for checkpoint in original.checkpoints:
            rebuilt = append_clock_checkpoint(
                rebuilt,
                label=checkpoint.label,
                sample=checkpoint.sample,
                inherited_child_bound_ns=checkpoint.inherited_child_bound_ns,
            )
        # Both encoded datetimes stay identical, and all derived bounds rehash.
        assert recorder.utc_ns_to_datetime_floor(rebuilt.anchor.utc_ns) == signal.started_at
        witness["clock_evidence"] = rebuilt.to_dict()
        witness["payload_admission_bound_ns"] = rebuilt.checkpoints[1].admission_bound_ns

    damaged = _tamper(harness, signal.binding, mutate)
    with pytest.raises(TemporalEvidenceError, match="raw ns event anchor precedes predecessor"):
        harness.verify(damaged)


def test_completion_clock_sample_follows_all_real_payload_writer_returns(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    activation = harness.activate()
    observed = harness.inputs(activation.binding)
    calls: list[str] = []
    original = recorder.write_contained_artifact_bytes
    started, completed = SIGNAL_TIME, SIGNAL_TIME + timedelta(seconds=3)

    def writer(**kwargs: Any) -> Any:
        name = Path(kwargs["relative_path"]).name
        calls.append(f"enter:{name}")
        result = original(**kwargs)
        calls.append(f"returned:{name}")
        return result

    def clock() -> datetime:
        calls.append("clock")
        return completed if "returned:payload_signal.bin" in calls else started

    monkeypatch.setattr(recorder, "write_contained_artifact_bytes", writer)
    monkeypatch.setattr(recorder, "_utc_now", clock)
    result = harness.signal(observed.binding)
    payload_return = calls.index("returned:payload_signal.bin")
    witness_enter = calls.index("enter:completion.json")
    assert calls[payload_return + 1 : witness_enter]
    assert set(calls[payload_return + 1 : witness_enter]) == {"clock"}
    assert result.started_at == started
    assert result.payload_durable_completed_at == completed
    assert result.to_dict()["witness_own_durability_time_established"] is False


def test_timely_signal_is_opaque_unadopted_temporal_evidence(harness: _Harness) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    signal = harness.signal(inputs.binding)
    assert replace(signal, return_clock_evidence=None) == harness.verify(signal.binding)
    assert signal.return_clock_evidence is not None
    assert signal.first_feature_session == FEATURE
    assert signal.temporal_status == "TIMELY_PAYLOAD"
    assert signal.plan.primary_window_start == date(2021, 2, 22)
    report = signal.to_dict()
    assert report["real_activation_adopted"] is False
    assert report["observation_authorized"] is False
    assert report["provider_available_at_status"] == "NOT_ESTABLISHED"
    assert report["oos_admission_status"] == "NOT_ESTABLISHED"
    assert report["signal_semantics_status"] == "NOT_VALIDATED_BY_TIME_LAYER"
    assert report["input_closure_status"] == "DECLARED_MEMBERS_ONLY"
    assert report["declared_source_execution_attested"] is False
    assert report["parent_executor_acknowledgement"] == "NOT_PRESENT"
    assert report["production_effect"] == report["broker_action"] == "none"
    payload_path = Path(signal.binding.relative_path).parent / "payload_signal.bin"
    assert (harness.store / payload_path).read_bytes() == SIGNAL.content


def test_stalled_raw_utc_with_positive_counter_elapsed_is_not_misreported_as_rollback(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    ticks = iter(range(0, 60_000_000, 5_000_000))
    monkeypatch.setattr(host_clock, "_counter_ns", lambda: next(ticks))
    signal = harness.signal(inputs.binding, at=SIGNAL_TIME, completed=SIGNAL_TIME)
    assert signal.started_at == signal.payload_durable_completed_at == SIGNAL_TIME
    assert signal.temporal_status == "TIMELY_PAYLOAD"
    assert signal.clock_evidence is not None and signal.return_clock_evidence is not None
    assert signal.payload_admission_bound_ns > datetime_to_utc_ns(SIGNAL_TIME)
    assert (
        signal.return_clock_evidence.admission_bound_ns >= signal.clock_evidence.admission_bound_ns
    )
    assert harness.verify(signal.binding).temporal_status == "TIMELY_PAYLOAD"
    coverage = harness.coverage(activation.binding, FEATURE, SIGNAL_TIME)
    assert coverage["expected_sessions"][0]["status"] == "NOT_RECORDED_BY_REVIEW_TIME"


def test_derived_bound_reaches_lease_expiry_before_payload_even_when_raw_is_live(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    harness.renew(SIGNAL_TIME)
    expiry = harness.acquired_at + timedelta(seconds=harness.guard.policy.lease_ttl_seconds)
    remaining = datetime_to_utc_ns(expiry) - datetime_to_utc_ns(SIGNAL_TIME)
    ticks = iter((0, 0, remaining, remaining))
    monkeypatch.setattr(host_clock, "_counter_ns", lambda: next(ticks))
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_LEASE_BOUND_EXPIRED"):
        harness.signal(inputs.binding, at=SIGNAL_TIME, completed=SIGNAL_TIME)
    assert not list(harness.store.glob("streams/*/sessions/*/signal/intent.json"))


def test_original_return_observation_replays_after_expiry_without_clock_or_lease(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    activation = harness.activate()
    assert activation.return_clock_evidence is not None
    harness.release()
    with monkeypatch.context() as guard:
        guard.setattr(recorder, "_live_lease", _forbidden)
        guard.setattr(recorder, "_utc_now", _forbidden)
        guard.setattr(host_clock, "_utc_ns", _forbidden)
        guard.setattr(host_clock, "_counter_ns", _forbidden)
        guard.setattr(host_clock, "_read_provider", _forbidden)
        returned = recorder.verify_recorder_return_evidence(
            store_root=harness.store,
            event=activation.binding,
            return_clock_evidence=activation.return_clock_evidence,
            policy=harness.policy,
        )
        assert returned == activation
        assert harness.verify(activation.binding).return_clock_evidence is None
        replay = recorder.record_activation(
            store_root=harness.store,
            plan=harness.plan,
            definitions=DEFINITIONS,
            policy=harness.policy,
            lease_handle=harness.handle,
        )
        assert replay.return_clock_evidence is None


@pytest.mark.parametrize("damage", ["no_extension", "provider", "extra_stage", "expiry"])
def test_return_observation_must_extend_exact_child_prefix_and_original_lease(
    harness: _Harness, damage: str
) -> None:
    activation = harness.activate()
    original = activation.return_clock_evidence
    frozen = activation.clock_evidence
    assert original is not None and frozen is not None
    if damage == "no_extension":
        changed = frozen
    elif damage == "provider":
        changed = replace(original, provider=replace(original.provider, python_version="3.11.10"))
    elif damage == "extra_stage":
        changed = append_clock_checkpoint(
            original, label="unbound_later_stage", sample=original.latest_sample
        )
    else:
        expiry = harness.acquired_at + timedelta(seconds=harness.guard.policy.lease_ttl_seconds)
        duration = datetime_to_utc_ns(expiry) - frozen.anchor.utc_ns
        changed = append_clock_checkpoint(
            frozen,
            label="recorder_return",
            sample=ClockSample(frozen.latest_sample.utc_ns, duration, duration),
        )
    with pytest.raises(
        ValueError,
        match="HOST_CLOCK_EXTENSION_INVALID|TEMPORAL_RETURN_CLOCK_STAGE_INVALID|TEMPORAL_LEASE_BOUND_EXPIRED",
    ):
        recorder.verify_recorder_return_evidence(
            store_root=harness.store,
            event=activation.binding,
            return_clock_evidence=changed,
            policy=harness.policy,
        )


def test_clock_rejection_retains_original_backward_raw_tuple_and_partial_payload(
    harness: _Harness,
) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_UTC_BACKWARD") as caught:
        harness.signal(
            inputs.binding, at=SIGNAL_TIME, completed=SIGNAL_TIME - timedelta(microseconds=1)
        )
    diagnostic = caught.value.diagnostic
    assert isinstance(diagnostic, host_clock.HostClockFailureDiagnostic)
    assert diagnostic.attempted_label == "payload_complete"
    assert diagnostic.raw_readings[1].integer_value == datetime_to_utc_ns(SIGNAL_TIME) - 1000
    assert diagnostic.prior_evidence is not None
    assert diagnostic.prior_evidence.latest_sample.utc_ns == datetime_to_utc_ns(SIGNAL_TIME)
    slot = harness.store / f"streams/{harness.plan.stream_id}/sessions/{FEATURE}/signal"
    assert (slot / "payload_signal.bin").read_bytes() == SIGNAL.content
    assert not (slot / "completion.json").exists()


def _legacy_activation_fixture(
    harness: _Harness, *, elapsed_ns: int = 0
) -> tuple[EventBinding, recorder.TimeEvidencePolicy]:
    """Build explicitly synthetic original-v1 wire bytes; never enable a v1 writer."""
    activation = harness.activate()
    policy = recorder.load_time_evidence_policy(
        source_root=recorder.SOURCE_ROOT, policy_path=recorder.LEGACY_POLICY_PATH
    )
    witness = _read_json(harness, activation.binding.relative_path)
    assert HostClockEvidence.from_dict(witness["clock_evidence"]) == activation.clock_evidence
    intent_relative = witness["intent"]["relative_path"]
    intent = _read_json(harness, intent_relative)
    semantic = intent["semantic"]
    semantic["schema_version"] = "prospective_time_intent.v1"
    semantic["policy_binding"] = policy.binding.to_dict()
    del semantic["clock_policy_binding"]
    intent_content = canonical_json_bytes(intent)
    (harness.store / intent_relative).write_bytes(intent_content)
    legacy = {
        "schema_version": "prospective_time_completion.v1",
        "intent": _binding(intent_relative, intent_content).to_dict(),
        "started_at": witness["started_at"],
        "payload_durable_completed_at": witness["payload_durable_completed_at"],
        "monotonic_elapsed_ns": elapsed_ns,
        "first_feature_session": witness["first_feature_session"],
        "session_timing": None,
        "temporal_status": "ACTIVATION_RECORDED",
        "safety": dict(recorder._SAFETY_V1),
    }
    content = canonical_json_bytes(legacy)
    (harness.store / activation.binding.relative_path).write_bytes(content)
    return _binding(activation.binding.relative_path, content), policy


@pytest.mark.parametrize("elapsed_ns,accepted", [(1_000_001_000, True), (1_000_001_001, False)])
def test_v1_retained_inner_interval_math_is_not_reinterpreted_as_outer_envelope(
    harness: _Harness, elapsed_ns: int, accepted: bool
) -> None:
    event, policy = _legacy_activation_fixture(harness, elapsed_ns=elapsed_ns)
    if accepted:
        original = recorder.verify_time_evidence(
            store_root=harness.store, event=event, policy=policy
        )
        assert original.clock_evidence is None and original.return_clock_evidence is None
        assert original.to_dict()["schema_version"] == "prospective_time_evidence.v1"
        with pytest.raises(TemporalEvidenceError, match="TEMPORAL_LEGACY_CLOCK_BOUND_UNAVAILABLE"):
            _ = original.admission_bound_ns
    else:
        with pytest.raises(TemporalEvidenceError, match="TEMPORAL_CLOCK_BACKWARD"):
            recorder.verify_time_evidence(store_root=harness.store, event=event, policy=policy)


def test_v1_is_readonly_and_cannot_be_mixed_into_new_v2_recording(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    event, policy = _legacy_activation_fixture(harness)
    retained_bytes = (harness.store / event.relative_path).read_bytes()
    harness.release()
    assert harness.handle is not None
    with monkeypatch.context() as guard:
        guard.setattr(recorder, "_utc_now", _forbidden)
        guard.setattr(recorder, "_live_lease", _forbidden)
        guard.setattr(host_clock, "_utc_ns", _forbidden)
        replay = recorder.record_activation(
            store_root=harness.store,
            plan=harness.plan,
            definitions=DEFINITIONS,
            policy=policy,
            lease_handle=harness.handle,
        )
        assert replay.binding == event and replay.return_clock_evidence is None
        with pytest.raises(TemporalEvidenceError, match="TEMPORAL_LEGACY_RECORDING_DISABLED"):
            recorder.record_activation(
                store_root=harness.store,
                plan=replace(harness.plan, plan_id="synthetic-legacy-new-v1"),
                definitions=DEFINITIONS,
                policy=policy,
                lease_handle=harness.handle,
            )
        with pytest.raises(TemporalEvidenceError):
            recorder.record_local_input_observation(
                store_root=harness.store,
                plan=harness.plan,
                feature_session=FEATURE,
                inputs=INPUTS,
                activation=event,
                policy=harness.policy,
                lease_handle=harness.handle,
            )
    assert (harness.store / event.relative_path).read_bytes() == retained_bytes
    assert not list(harness.store.glob("streams/*/sessions/*/inputs/intent.json"))


@pytest.mark.parametrize("offset", [-1, 0])
def test_feature_close_is_strict_write_start_lower_bound(harness: _Harness, offset: int) -> None:
    activation = harness.activate()
    at = datetime(2025, 3, 6, 21, tzinfo=UTC) + timedelta(microseconds=offset)
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_FEATURE_NOT_CLOSED"):
        harness.inputs(activation.binding, at=at)
    assert not list(harness.store.glob("streams/*/sessions/*/inputs/intent.json"))


@pytest.mark.parametrize(
    "offset,status", [(-1, "TIMELY_PAYLOAD"), (0, "LATE_PAYLOAD"), (1, "LATE_PAYLOAD")]
)
def test_completion_deadline_is_strict_and_late_bytes_remain(
    harness: _Harness, offset: int, status: str
) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    deadline = datetime(2025, 3, 7, 21, tzinfo=UTC)
    start = deadline - timedelta(minutes=1)
    harness.renew(start)
    result = harness.signal(
        inputs.binding, at=start, completed=deadline + timedelta(microseconds=offset)
    )
    assert result.temporal_status == status
    assert harness.verify(result.binding).temporal_status == status
    rows = harness.coverage(activation.binding, FEATURE, deadline + timedelta(seconds=1))
    assert rows["expected_sessions"][0]["status"] == ("RECORDED_PAYLOAD" if offset < 0 else "LATE")
    assert rows["observation_authorized"] is False


@pytest.mark.parametrize(
    "activation,expected",
    [
        (datetime(2025, 3, 7, 14, tzinfo=UTC), date(2025, 3, 10)),
        (datetime(2025, 3, 8, 0, tzinfo=UTC), date(2025, 3, 10)),
        (datetime(2025, 1, 8, 22, tzinfo=UTC), date(2025, 1, 10)),
        (datetime(2025, 11, 27, 18, tzinfo=UTC), date(2025, 11, 28)),
        (datetime(2025, 3, 7, 2, tzinfo=UTC), date(2025, 3, 7)),
    ],
)
def test_first_feature_uses_next_new_york_calendar_date(
    activation: datetime, expected: date
) -> None:
    policy = recorder.load_time_evidence_policy(source_root=recorder.SOURCE_ROOT)
    assert recorder.first_feature_session(activation, policy=policy) == expected


@pytest.mark.parametrize(
    "feature,effective,end,feature_hour,effective_hour",
    [
        (date(2025, 3, 7), date(2025, 3, 10), date(2025, 3, 11), 21, 20),
        (date(2025, 10, 31), date(2025, 11, 3), date(2025, 11, 4), 20, 21),
        (date(2025, 11, 26), date(2025, 11, 28), date(2025, 12, 1), 21, 18),
        (date(2025, 7, 3), date(2025, 7, 7), date(2025, 7, 8), 17, 20),
        (date(2025, 1, 8), date(2025, 1, 10), date(2025, 1, 13), 21, 21),
    ],
)
def test_calendar_binds_dst_half_days_holidays_and_special_closure(
    feature: date, effective: date, end: date, feature_hour: int, effective_hour: int
) -> None:
    policy = recorder.load_time_evidence_policy(source_root=recorder.SOURCE_ROOT)
    timing = recorder.session_timing(feature, policy=policy)
    assert timing.effective_session == effective
    assert timing.first_return_end_session == end
    assert timing.feature_close_at == datetime.combine(feature, datetime.min.time(), UTC).replace(
        hour=feature_hour
    )
    assert timing.effective_close_at.hour == effective_hour
    assert timing.effective_close_at.tzinfo == UTC


@pytest.mark.parametrize("feature", [date(2025, 3, 8), date(2025, 1, 9), date(2025, 7, 4)])
def test_non_session_feature_is_rejected(feature: date) -> None:
    policy = recorder.load_time_evidence_policy(source_root=recorder.SOURCE_ROOT)
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_NOT_XNYS_SESSION"):
        recorder.session_timing(feature, policy=policy)


def test_pre_activation_feature_cannot_be_backfilled(harness: _Harness) -> None:
    activation = harness.activate()
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_PREDECESSOR_INVALID"):
        harness.inputs(activation.binding, feature=date(2025, 3, 5))


def test_same_key_same_bytes_returns_original_witness_without_writer(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    signal = harness.signal(inputs.binding)
    original = (harness.store / signal.binding.relative_path).read_bytes()
    monkeypatch.setattr(recorder, "write_contained_artifact_bytes", _forbidden)
    replay = harness.signal(inputs.binding, at=SIGNAL_TIME + timedelta(minutes=15))
    assert replay == replace(signal, return_clock_evidence=None)
    assert (harness.store / replay.binding.relative_path).read_bytes() == original


def test_same_key_different_bytes_is_conflict_and_keeps_original(harness: _Harness) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    signal = harness.signal(inputs.binding)
    original = (harness.store / signal.binding.relative_path).read_bytes()
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_EVENT_CONFLICT"):
        harness.signal(inputs.binding, content=PayloadMember("signal", b"changed"))
    assert (harness.store / signal.binding.relative_path).read_bytes() == original
    assert harness.verify(signal.binding) == replace(signal, return_clock_evidence=None)


def test_activation_is_singleton_and_definition_bytes_must_match_plan(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    activation = harness.activate()
    harness.clock.event(ACTIVATED + timedelta(minutes=5))
    assert harness.handle is not None
    with monkeypatch.context() as patch:
        patch.setattr(recorder, "write_contained_artifact_bytes", _forbidden)
        replay = recorder.record_activation(
            store_root=harness.store,
            plan=harness.plan,
            definitions=DEFINITIONS,
            policy=harness.policy,
            lease_handle=harness.handle,
        )
    assert replay == replace(activation, return_clock_evidence=None)
    assert replay.return_clock_evidence is None
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_DEFINITION_MISMATCH"):
        recorder.record_activation(
            store_root=harness.store,
            plan=harness.plan,
            definitions=(PayloadMember("definition", b"changed synthetic definition"),),
            policy=harness.policy,
            lease_handle=harness.handle,
        )
    assert len(list(harness.store.glob("streams/*/activation/completion.json"))) == 1


@pytest.mark.parametrize("failure_path", ["intent.json", "payload_signal.bin", "completion.json"])
def test_crash_without_witness_is_never_recovered_as_success_and_next_feature_recovers(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, failure_path: str
) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    original = recorder.write_contained_artifact_bytes

    def crashing_writer(**kwargs: Any) -> Any:
        name = Path(kwargs["relative_path"]).name
        if name == failure_path == "completion.json":
            raise OSError("synthetic crash before witness")
        result = original(**kwargs)
        if name == failure_path:
            raise OSError("synthetic crash after artifact persistence")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(recorder, "write_contained_artifact_bytes", crashing_writer)
        with pytest.raises(OSError, match="synthetic crash"):
            harness.signal(inputs.binding)
    slot = harness.store / f"streams/{harness.plan.stream_id}/sessions/{FEATURE}/signal"
    retained = {path.name: path.read_bytes() for path in slot.iterdir() if path.is_file()}
    assert "intent.json" in retained and "completion.json" not in retained
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_INCOMPLETE_EVENT"):
        harness.signal(inputs.binding, at=SIGNAL_TIME + timedelta(minutes=1))
    assert retained == {path.name: path.read_bytes() for path in slot.iterdir() if path.is_file()}
    next_feature = date(2025, 3, 7)
    at = datetime(2025, 3, 7, 21, 1, tzinfo=UTC)
    next_inputs = harness.inputs(activation.binding, feature=next_feature, at=at)
    next_signal = harness.signal(
        next_inputs.binding, feature=next_feature, at=at + timedelta(minutes=1)
    )
    assert next_signal.temporal_status == "TIMELY_PAYLOAD"
    coverage = harness.coverage(
        activation.binding, next_feature, datetime(2025, 3, 10, 21, tzinfo=UTC)
    )
    assert [row["status"] for row in coverage["expected_sessions"]] == [
        "INCOMPLETE",
        "RECORDED_PAYLOAD",
    ]


@pytest.mark.parametrize(
    "field,value,code",
    [
        ("temporal_status", "TIMELY_PAYLOAD", "TEMPORAL_STATUS_INVALID"),
        ("started_at", "2025-03-05T22:00:00", "TEMPORAL_TIME_INVALID"),
        (
            "payload_durable_completed_at",
            "2025-03-05T21:59:59+00:00",
            "TEMPORAL_CLOCK_BINDING_INVALID",
        ),
        ("first_feature_session", "2025-03-05", "TEMPORAL_FIRST_SESSION_INVALID"),
        ("payload_outer_elapsed_ns", True, "TEMPORAL_MONOTONIC_CLOCK_INVALID"),
        ("payload_outer_elapsed_ns", -1, "TEMPORAL_MONOTONIC_CLOCK_INVALID"),
        ("safety", {"observation_authorized": True}, "TEMPORAL_SAFETY_INVALID"),
    ],
)
def test_rehashed_witness_tampering_is_rejected(
    harness: _Harness, field: str, value: object, code: str
) -> None:
    activation = harness.activate()
    event = _tamper(harness, activation.binding, lambda witness: witness.update({field: value}))
    with pytest.raises(TemporalEvidenceError, match=code):
        harness.verify(event)


@pytest.mark.parametrize(
    "field,value",
    [
        ("stage_sequence", 1),
        ("stream_id", "0" * 64),
        (
            "policy_binding",
            {"relative_path": recorder.POLICY_PATH, "sha256": "0" * 64, "size_bytes": 1},
        ),
    ],
)
def test_rehashed_intent_identity_tampering_is_rejected(
    harness: _Harness, field: str, value: object
) -> None:
    activation = harness.activate()
    event = _tamper(
        harness,
        activation.binding,
        lambda intent: intent["semantic"].update({field: value}),
        intent=True,
    )
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_INTENT_BINDING_MISMATCH"):
        harness.verify(event)


def test_payload_tampering_is_detected_by_retained_hash(harness: _Harness) -> None:
    activation = harness.activate()
    path = harness.store / Path(activation.binding.relative_path).parent / "payload_definition.bin"
    path.write_bytes(b"tampered definition")
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_CONTENT_MISMATCH"):
        harness.verify(activation.binding)


def test_wrong_intent_locator_is_rejected_before_reading_target(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    activation = harness.activate()
    unrelated_path = "unrelated_contained_file.json"
    unrelated_bytes = b'{"synthetic":"must not be read"}'
    (harness.store / unrelated_path).write_bytes(unrelated_bytes)
    event = _tamper(
        harness,
        activation.binding,
        lambda witness: witness.update(intent=_binding(unrelated_path, unrelated_bytes).to_dict()),
    )
    original = recorder.read_contained_artifact_bytes
    reads: list[str] = []

    def read(**kwargs: Any) -> bytes:
        reads.append(kwargs["relative_path"])
        assert kwargs["relative_path"] != unrelated_path, "unvalidated intent target was read"
        return original(**kwargs)

    monkeypatch.setattr(recorder, "read_contained_artifact_bytes", read)
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_.*LOCATOR_INVALID"):
        harness.verify(event)
    assert unrelated_path not in reads


def test_same_stage_cycle_is_rejected_before_recursive_read(harness: _Harness) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    event = _tamper(
        harness,
        inputs.binding,
        lambda intent: intent["semantic"].update(previous_event=inputs.binding.to_dict()),
        intent=True,
    )
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_PREDECESSOR_INVALID"):
        harness.verify(event)


def test_wrong_stage_and_cross_feature_predecessors_fail_before_new_artifacts(
    harness: _Harness,
) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_PREDECESSOR_INVALID"):
        harness.signal(activation.binding)
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_PREDECESSOR_INVALID"):
        harness.signal(inputs.binding, feature=date(2025, 3, 7))
    assert not list(harness.store.glob("streams/*/sessions/*/signal/intent.json"))


@pytest.mark.parametrize("mode", ["wrong_stream", "wrong_feature"])
def test_record_entry_rejects_wrong_predecessor_locator_before_reading_it(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    if mode == "wrong_stream":
        previous = replace(
            activation.binding,
            relative_path=f"streams/{'0' * 64}/activation/completion.json",
        )
    else:
        previous = inputs.binding
    original = recorder.read_contained_artifact_bytes
    reads: list[str] = []

    def read(**kwargs: Any) -> bytes:
        reads.append(kwargs["relative_path"])
        assert kwargs["relative_path"] != previous.relative_path, "wrong predecessor was read"
        return original(**kwargs)

    monkeypatch.setattr(recorder, "read_contained_artifact_bytes", read)
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_PREDECESSOR_INVALID"):
        if mode == "wrong_stream":
            assert harness.handle is not None
            recorder.record_local_input_observation(
                store_root=harness.store,
                plan=harness.plan,
                feature_session=FEATURE,
                inputs=INPUTS,
                activation=previous,
                policy=harness.policy,
                lease_handle=harness.handle,
            )
        else:
            harness.signal(previous, feature=date(2025, 3, 7))
    assert previous.relative_path not in reads


@pytest.mark.parametrize("mode", ["naive", "completion_backward", "parent_backward"])
def test_untrusted_or_backward_recorder_clocks_fail_closed(harness: _Harness, mode: str) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    start, complete = SIGNAL_TIME, SIGNAL_TIME + timedelta(seconds=1)
    if mode == "naive":
        start = start.replace(tzinfo=None)
    elif mode == "completion_backward":
        complete = start - timedelta(microseconds=1)
    else:
        start = inputs.payload_durable_completed_at - timedelta(microseconds=1)
    code = {
        "naive": "TEMPORAL_TIME_INVALID",
        "completion_backward": "HOST_CLOCK_UTC_BACKWARD",
        "parent_backward": "TEMPORAL_CLOCK_BACKWARD",
    }[mode]
    with pytest.raises(ValueError, match=code):
        harness.signal(inputs.binding, at=start, completed=complete)
    assert not list(harness.store.glob("streams/*/sessions/*/signal/completion.json"))


def test_positive_raw_utc_cannot_hide_twenty_second_outer_interval_across_deadline(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    deadline = datetime(2025, 3, 7, 21, tzinfo=UTC)
    start = deadline - timedelta(seconds=10)
    harness.renew(start)
    samples = iter((0, 0, 0, 0, *([20_000_000_000] * 8)))
    monkeypatch.setattr(host_clock, "_counter_ns", lambda: next(samples))
    # UTC remains forward and apparently timely; the full elapsed budget still
    # crosses D. Preserve the genuine payload as LATE, never classify it timely.
    result = harness.signal(inputs.binding, at=start, completed=deadline - timedelta(seconds=5))
    assert result.temporal_status == "LATE_PAYLOAD"
    assert harness.verify(result.binding).temporal_status == "LATE_PAYLOAD"
    slot = harness.store / f"streams/{harness.plan.stream_id}/sessions/{FEATURE}/signal"
    assert (slot / "payload_signal.bin").read_bytes() == SIGNAL.content
    assert (slot / "completion.json").exists()


def test_retained_witness_rejects_outer_duration_different_from_raw_counter_evidence(
    harness: _Harness,
) -> None:
    activation = harness.activate()
    event = _tamper(
        harness,
        activation.binding,
        lambda witness: witness.update(payload_outer_elapsed_ns=2_000_000_000),
    )
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_CLOCK_BINDING_INVALID"):
        harness.verify(event)


def test_post_payload_lease_check_cannot_hide_a_later_utc_rollback(
    harness: _Harness,
) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    harness.clock.pending = deque(
        (
            SIGNAL_TIME,
            SIGNAL_TIME,
            SIGNAL_TIME,
            SIGNAL_TIME + timedelta(seconds=2),
            SIGNAL_TIME + timedelta(seconds=1),
        )
    )
    assert harness.handle is not None
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_CLOCK_BACKWARD"):
        recorder.record_signal_completion(
            store_root=harness.store,
            plan=harness.plan,
            feature_session=FEATURE,
            signal=SIGNAL,
            inputs=inputs.binding,
            policy=harness.policy,
            lease_handle=harness.handle,
        )
    assert not list(harness.store.glob("streams/*/sessions/*/signal/completion.json"))


def test_activation_first_feature_uses_completion_upper_bound_across_market_midnight(
    harness: _Harness,
) -> None:
    started = datetime(2025, 3, 6, 4, 59, tzinfo=UTC)
    completed = datetime(2025, 3, 6, 5, 1, tzinfo=UTC)
    harness.renew(started)
    harness.clock.event(started, completed)
    assert harness.handle is not None
    activation = recorder.record_activation(
        store_root=harness.store,
        plan=harness.plan,
        definitions=DEFINITIONS,
        policy=harness.policy,
        lease_handle=harness.handle,
    )
    assert activation.first_feature_session == date(2025, 3, 7)
    assert activation.started_at == started
    assert activation.payload_durable_completed_at == completed


@pytest.mark.parametrize(
    "field,value",
    [
        ("feature_session", "2025-03-05"),
        ("effective_session", "2025-03-10"),
        ("effective_close_at", "2025-03-07T20:00:00+00:00"),
        ("first_return_end_session", "2025-03-11"),
    ],
)
def test_rehashed_session_clock_cannot_change_feature_effective_or_return_anchor(
    harness: _Harness, field: str, value: str
) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    signal = harness.signal(inputs.binding)
    event = _tamper(
        harness, signal.binding, lambda witness: witness["session_timing"].update({field: value})
    )
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_SESSION_TIMING_INVALID"):
        harness.verify(event)


@pytest.mark.parametrize(
    "mode,code",
    [
        ("scope", "TEMPORAL_LEASE_BINDING_INVALID"),
        ("expired", "TEMPORAL_LEASE_BINDING_INVALID"),
        ("released", "TEMPORAL_LEASE_REQUIRED"),
        ("checkout", "TEMPORAL_LEASE_CHECKOUT_MISMATCH"),
        ("source_commit", "TEMPORAL_LEASE_BINDING_INVALID"),
    ],
)
def test_real_s4d_lease_scope_expiry_release_checkout_and_source_are_enforced(
    harness: _Harness, mode: str, code: str
) -> None:
    harness.renew(ACTIVATED, scope="outputs/elsewhere" if mode == "scope" else "outputs/events")
    plan = harness.plan
    store = harness.store
    at = ACTIVATED
    if mode == "expired":
        at = harness.acquired_at + timedelta(seconds=harness.guard.policy.lease_ttl_seconds)
    elif mode == "released":
        harness.release()
    elif mode == "checkout":
        other = harness.root.parent / "other-synthetic-checkout"
        _checkout(other)
        store = other / "outputs/events"
        store.mkdir(parents=True)
    elif mode == "source_commit":
        plan = replace(plan, declared_source_commit="0" * 40)
    harness.clock.event(at)
    assert harness.handle is not None
    with pytest.raises(TemporalEvidenceError, match=code):
        recorder.record_activation(
            store_root=store,
            plan=plan,
            definitions=DEFINITIONS,
            policy=harness.policy,
            lease_handle=harness.handle,
        )
    assert not list(store.glob("streams/*/activation/completion.json"))


def test_lease_expiry_during_payload_write_preserves_incomplete_slot(
    harness: _Harness,
) -> None:
    harness.renew(ACTIVATED)
    deadline = harness.acquired_at + timedelta(seconds=harness.guard.policy.lease_ttl_seconds)
    harness.clock.event(deadline - timedelta(seconds=1), deadline)
    assert harness.handle is not None
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_LEASE_BINDING_INVALID"):
        recorder.record_activation(
            store_root=harness.store,
            plan=harness.plan,
            definitions=DEFINITIONS,
            policy=harness.policy,
            lease_handle=harness.handle,
        )
    slot = harness.store / f"streams/{harness.plan.stream_id}/activation"
    assert (slot / "intent.json").is_file()
    assert (slot / "payload_definition.bin").is_file()
    assert not (slot / "completion.json").exists()


def test_two_threads_same_key_keep_one_event_and_completed_replay_is_idempotent(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    assert harness.handle is not None
    barrier = Barrier(2)
    original = recorder.write_contained_artifact_bytes
    writes: list[str] = []

    def writer(**kwargs: Any) -> Any:
        result = original(**kwargs)
        writes.append(kwargs["relative_path"])
        return result

    def run() -> recorder.RecordedTemporalEvidence | TemporalEvidenceError:
        barrier.wait(timeout=10)
        assert harness.handle is not None
        try:
            return recorder.record_signal_completion(
                store_root=harness.store,
                plan=harness.plan,
                feature_session=FEATURE,
                signal=SIGNAL,
                inputs=inputs.binding,
                policy=harness.policy,
                lease_handle=harness.handle,
            )
        except TemporalEvidenceError as exc:
            return exc

    monkeypatch.setattr(recorder, "_utc_now", lambda: SIGNAL_TIME)
    monkeypatch.setattr(recorder, "write_contained_artifact_bytes", writer)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run) for _ in range(2)]
        results = [future.result(timeout=30) for future in futures]
    successes = [
        result for result in results if isinstance(result, recorder.RecordedTemporalEvidence)
    ]
    assert successes
    assert all(
        replace(result, return_clock_evidence=None)
        == replace(successes[0], return_clock_evidence=None)
        for result in successes
    )
    assert sum(result.return_clock_evidence is not None for result in successes) == 1
    for result in results:
        if isinstance(result, TemporalEvidenceError):
            assert result.code in {"TEMPORAL_INCOMPLETE_EVENT", "TEMPORAL_EVENT_CONFLICT"}
    assert sum(path.endswith("/completion.json") for path in writes) == 1
    assert sum(path.endswith("/payload_signal.bin") for path in writes) == 1
    monkeypatch.setattr(recorder, "write_contained_artifact_bytes", _forbidden)
    assert harness.signal(inputs.binding) == replace(successes[0], return_clock_evidence=None)


def test_same_key_completed_while_waiting_for_store_lock_returns_original_timestamp(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    first_at_witness = Event()
    second_waiting_for_lock = Event()
    role = local()
    original_maintenance = recorder.exclusive_store_maintenance
    original_writer = recorder.write_contained_artifact_bytes
    completion_writes: list[str] = []

    @contextmanager
    def maintenance(**kwargs: Any) -> Iterator[Any]:
        if role.name == "second":
            second_waiting_for_lock.set()
        with original_maintenance(**kwargs) as held:
            yield held

    def writer(**kwargs: Any) -> Any:
        if kwargs["relative_path"].endswith("/completion.json"):
            assert role.name == "first", "waiting caller attempted to publish a second witness"
            # First caller holds the real second-phase store lock. Keep it until
            # the second caller has finished preflight and is waiting to enter.
            first_at_witness.set()
            assert second_waiting_for_lock.wait(timeout=30)
            completion_writes.append(kwargs["relative_path"])
        return original_writer(**kwargs)

    def run(name: str) -> recorder.RecordedTemporalEvidence:
        role.name = name
        assert harness.handle is not None
        return recorder.record_signal_completion(
            store_root=harness.store,
            plan=harness.plan,
            feature_session=FEATURE,
            signal=SIGNAL,
            inputs=inputs.binding,
            policy=harness.policy,
            lease_handle=harness.handle,
        )

    monkeypatch.setattr(recorder, "_utc_now", lambda: SIGNAL_TIME)
    monkeypatch.setattr(recorder, "exclusive_store_maintenance", maintenance)
    monkeypatch.setattr(recorder, "write_contained_artifact_bytes", writer)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(run, "first")
        assert first_at_witness.wait(timeout=30)
        second = executor.submit(run, "second")
        original = first.result(timeout=30)
        replay = second.result(timeout=30)
    assert replay == replace(original, return_clock_evidence=None)
    assert original.return_clock_evidence is not None and replay.return_clock_evidence is None
    assert replay.payload_durable_completed_at == SIGNAL_TIME
    assert completion_writes == [original.binding.relative_path]


@pytest.mark.parametrize("change", ["policy", "lease"])
def test_authority_change_between_payload_and_witness_prevents_completion(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    original_writer = recorder.write_contained_artifact_bytes
    original_policy_loader = recorder.load_time_evidence_policy
    original_live_lease = recorder._live_lease
    payload_persisted = False

    def writer(**kwargs: Any) -> Any:
        nonlocal payload_persisted
        result = original_writer(**kwargs)
        if kwargs["relative_path"].endswith("/payload_signal.bin"):
            payload_persisted = True
        return result

    def load_policy(**kwargs: Any) -> recorder.TimeEvidencePolicy:
        policy = original_policy_loader(**kwargs)
        if payload_persisted:
            return replace(policy, binding=replace(policy.binding, sha256="0" * 64))
        return policy

    def live_lease(*args: Any, **kwargs: Any) -> Any:
        if payload_persisted:
            harness.release()
        return original_live_lease(*args, **kwargs)

    monkeypatch.setattr(recorder, "write_contained_artifact_bytes", writer)
    if change == "policy":
        monkeypatch.setattr(recorder, "load_time_evidence_policy", load_policy)
        code = "TEMPORAL_POLICY_CALENDAR_DRIFT"
    else:
        monkeypatch.setattr(recorder, "_live_lease", live_lease)
        code = "TEMPORAL_LEASE_REQUIRED"
    with pytest.raises(TemporalEvidenceError, match=code):
        harness.signal(inputs.binding)
    slot = harness.store / f"streams/{harness.plan.stream_id}/sessions/{FEATURE}/signal"
    assert (slot / "payload_signal.bin").read_bytes() == SIGNAL.content
    assert not (slot / "completion.json").exists()


def test_readonly_verifier_and_coverage_need_no_live_lease_writer_clock_or_network(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ai_trading_system.data.named_quality_execution as named_quality
    import ai_trading_system.data.quality as quality

    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    signal = harness.signal(inputs.binding)
    harness.release()
    before = {
        path.relative_to(harness.store).as_posix(): path.read_bytes()
        for path in harness.store.rglob("*")
        if path.is_file()
    }
    with monkeypatch.context() as patch:
        patch.setattr(recorder, "write_contained_artifact_bytes", _forbidden)
        patch.setattr(recorder, "exclusive_store_maintenance", _forbidden)
        patch.setattr(recorder, "_utc_now", _forbidden)
        patch.setattr(recorder, "_live_lease", _forbidden)
        patch.setattr(subprocess, "run", _forbidden)
        patch.setattr(socket, "create_connection", _forbidden)
        patch.setattr(socket.socket, "connect", _forbidden)
        patch.setattr(quality, "validate_data_cache", _forbidden)
        patch.setattr(named_quality, "capture_named_publication", _forbidden)
        patch.setattr(named_quality, "run_named_data_quality_execution", _forbidden)
        patch.setattr(host_clock, "_utc_ns", _forbidden)
        patch.setattr(host_clock, "_counter_ns", _forbidden)
        patch.setattr(host_clock, "_read_provider", _forbidden)
        assert harness.verify(signal.binding) == replace(signal, return_clock_evidence=None)
        coverage = harness.coverage(
            activation.binding, date(2025, 3, 11), datetime(2025, 3, 12, 21, tzinfo=UTC)
        )
    after = {
        path.relative_to(harness.store).as_posix(): path.read_bytes()
        for path in harness.store.rglob("*")
        if path.is_file()
    }
    assert before == after
    rows = coverage["expected_sessions"]
    assert [row["feature_session"] for row in rows] == [
        "2025-03-06",
        "2025-03-07",
        "2025-03-10",
        "2025-03-11",
    ]
    assert [row["status"] for row in rows] == ["RECORDED_PAYLOAD", "GAP", "GAP", "GAP"]
    assert coverage["old_gap_backfill_allowed"] is False
    assert coverage["future_session_recovery_allowed"] is True


def test_coverage_distinguishes_not_due_deadline_gap_and_future_record(harness: _Harness) -> None:
    activation = harness.activate()
    inputs = harness.inputs(activation.binding)
    signal = harness.signal(inputs.binding)
    historical = harness.coverage(activation.binding, FEATURE, INPUT_TIME)
    assert historical["expected_sessions"][0]["status"] == "NOT_RECORDED_BY_REVIEW_TIME"
    before_deadline = datetime(2025, 3, 10, 20, tzinfo=UTC) - timedelta(microseconds=1)
    future_feature = date(2025, 3, 7)
    before = harness.coverage(activation.binding, future_feature, before_deadline)
    at = harness.coverage(
        activation.binding, future_feature, before_deadline + timedelta(microseconds=1)
    )
    assert before["expected_sessions"][-1]["status"] == "NOT_DUE"
    assert at["expected_sessions"][-1]["status"] == "GAP"
    assert harness.verify(signal.binding).temporal_status == "TIMELY_PAYLOAD"


@pytest.mark.parametrize(
    "stage,fragment",
    [
        ("inputs", "lease_event.json"),
        ("inputs", "lease_intent.json"),
        ("signal", "lease_event.json"),
        ("signal", "lease_intent.json"),
        ("signal", "payload_signal.bin"),
    ],
)
def test_fixed_orphan_fragments_remain_incomplete_in_canonical_coverage(
    harness: _Harness, stage: str, fragment: str
) -> None:
    activation = harness.activate()
    slot = harness.store / f"streams/{harness.plan.stream_id}/sessions/{FEATURE}/{stage}"
    slot.mkdir(parents=True)
    path = slot / fragment
    content = b"synthetic orphan, not a complete event"
    path.write_bytes(content)
    coverage = harness.coverage(activation.binding, FEATURE, datetime(2025, 3, 7, 21, tzinfo=UTC))
    assert coverage["expected_sessions"][0]["status"] == "INCOMPLETE"
    assert coverage["expected_sessions"][0]["missed_deadline"] is True
    assert path.read_bytes() == content


def test_later_legal_feature_recovers_after_canonical_gap(harness: _Harness) -> None:
    activation = harness.activate()
    later = date(2025, 3, 10)
    at = datetime(2025, 3, 10, 20, 1, tzinfo=UTC)
    inputs = harness.inputs(activation.binding, feature=later, at=at)
    signal = harness.signal(inputs.binding, feature=later, at=at + timedelta(minutes=1))
    rows = harness.coverage(activation.binding, later, at + timedelta(minutes=2))
    assert [row["status"] for row in rows["expected_sessions"]] == [
        "GAP",
        "GAP",
        "RECORDED_PAYLOAD",
    ]
    assert signal.timing is not None
    assert signal.timing.effective_session == date(2025, 3, 11)
    assert signal.timing.first_return_end_session == date(2025, 3, 12)
