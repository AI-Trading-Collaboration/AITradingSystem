from __future__ import annotations

import hashlib
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import ai_trading_system.host_clock_evidence as clocks
from ai_trading_system.contracts.host_clock_evidence import (
    POLICY_PATH,
    POLICY_SHA256,
    HostClockEvidence,
    HostClockEvidenceError,
)
from ai_trading_system.data.immutable_publish import exclusive_store_maintenance

pytestmark = pytest.mark.fast_unit
_UTC = 1_700_000_000_000_000_000


@dataclass
class _ClockSource:
    utc: deque[int] = field(default_factory=lambda: deque((_UTC, _UTC + 100, _UTC + 200)))
    counters: deque[int] = field(
        default_factory=lambda: deque((1_000, 5_000, 6_000, 10_000, 11_000, 12_000))
    )
    order: list[str] = field(default_factory=list)
    counter_implementation: str = "QueryPerformanceCounter()"
    resolution: float = 1e-7

    def read_utc(self) -> int:
        self.order.append("UTC")
        return self.utc.popleft()

    def read_counter(self) -> int:
        self.order.append("COUNTER")
        return self.counters.popleft()

    def info(self, name: str) -> SimpleNamespace:
        if name == "time":
            return SimpleNamespace(
                implementation="GetSystemTimeAsFileTime()",
                resolution=0.015625,
                monotonic=False,
                adjustable=True,
            )
        assert name == "perf_counter"
        return SimpleNamespace(
            implementation=self.counter_implementation,
            resolution=self.resolution,
            monotonic=True,
            adjustable=False,
        )


@pytest.fixture
def source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, _ClockSource]:
    content = (Path(__file__).resolve().parents[1] / POLICY_PATH).read_bytes()
    assert hashlib.sha256(content).hexdigest() == POLICY_SHA256
    policy = tmp_path / POLICY_PATH
    policy.parent.mkdir(parents=True)
    policy.write_bytes(content)
    clock_source = _ClockSource()
    monkeypatch.setattr(clocks, "_utc_ns", clock_source.read_utc)
    monkeypatch.setattr(clocks, "_counter_ns", clock_source.read_counter)
    monkeypatch.setattr(clocks, "_clock_info", clock_source.info)
    monkeypatch.setattr(clocks, "_runtime_identity", lambda: ("win32", "CPython", "3.11.9"))
    return tmp_path, clock_source


def test_sampler_uses_fixed_outer_call_order_and_permanent_anchor(
    source: tuple[Path, _ClockSource],
) -> None:
    root, clock_source = source
    sampler = clocks.HostClockSampler.start(source_root=root)
    anchor = sampler.anchor
    initial = sampler.evidence
    first = sampler.checkpoint("payload", inherited_child_bound_ns=_UTC + 100_000)
    final = sampler.checkpoint("postguard")
    assert clock_source.order == ["COUNTER", "UTC", "COUNTER"] * 3
    assert anchor == final.anchor == initial.anchor
    assert initial.checkpoints == ()
    assert first.admission_bound_ns == final.admission_bound_ns == _UTC + 100_000
    assert final.latest_sample.utc_ns == _UTC + 200
    assert first.checkpoints[0] == final.checkpoints[0]
    assert sampler.evidence == HostClockEvidence.from_json_bytes(final.canonical_bytes)
    assert not list(root.rglob("*.json"))


def test_utc_read_and_post_read_authority_latency_are_inside_counter_outer(
    source: tuple[Path, _ClockSource],
) -> None:
    root, _ = source
    sampler = clocks.HostClockSampler.start(source_root=root)
    assert sampler.evidence.admission_bound_ns == _UTC + 4_200
    assert sampler.checkpoint("complete").admission_bound_ns == _UTC + 9_200


@pytest.mark.parametrize("kind", ["utc", "counter", "provider", "resolution", "policy"])
def test_failed_checkpoint_is_terminal_and_does_not_allow_clock_retry(
    source: tuple[Path, _ClockSource], kind: str
) -> None:
    root, clock_source = source
    sampler = clocks.HostClockSampler.start(source_root=root)
    if kind == "utc":
        clock_source.utc[0] = _UTC - 100
    elif kind == "counter":
        clock_source.counters[0] = 4_999
    elif kind == "provider":
        clock_source.counter_implementation = "GetTickCount64()"
    elif kind == "resolution":
        clock_source.resolution = 1e-6
    else:
        policy = root / POLICY_PATH
        policy.write_bytes(policy.read_bytes() + b"\n")
    prior = sampler.evidence
    with pytest.raises(HostClockEvidenceError) as caught:
        if kind == "policy":
            sampler.recheck_policy()
        else:
            sampler.checkpoint("complete")
    diagnostic = sampler.failure_diagnostic
    assert diagnostic is not None and caught.value.diagnostic is diagnostic
    assert diagnostic.prior_evidence is prior
    assert diagnostic.attempted_label == ("policy_recheck" if kind == "policy" else "complete")
    assert diagnostic.to_dict()["admission_allowed"] is False
    assert diagnostic.to_dict()["chain_resumable"] is False
    expected_names = (
        ["counter_before_ns", "utc_ns", "counter_after_ns"] if kind in {"utc", "counter"} else []
    )
    assert [row.name for row in diagnostic.raw_readings] == expected_names
    if kind == "utc":
        assert diagnostic.raw_readings[1].integer_value == _UTC - 100
    for operation in (
        lambda: sampler.evidence,
        lambda: sampler.anchor,
        lambda: sampler.checkpoint("retry"),
    ):
        with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_SAMPLER_FAILED"):
            operation()


def test_policy_drift_during_sampling_is_rejected_by_required_external_recheck(
    source: tuple[Path, _ClockSource], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, clock_source = source
    sampler = clocks.HostClockSampler.start(source_root=root)

    def mutate_policy_during_read() -> int:
        policy = root / POLICY_PATH
        policy.write_bytes(policy.read_bytes() + b"\n")
        return clock_source.read_utc()

    monkeypatch.setattr(clocks, "_utc_ns", mutate_policy_during_read)
    evidence = sampler.checkpoint("complete")
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_POLICY_INVALID"):
        sampler.recheck_policy()
    assert clock_source.order == ["COUNTER", "UTC", "COUNTER", "COUNTER", "UTC", "COUNTER"]
    diagnostic = sampler.failure_diagnostic
    assert diagnostic is not None
    assert diagnostic.raw_readings == ()
    assert diagnostic.prior_evidence is evidence
    assert evidence.latest_sample.utc_ns == _UTC + 100


def test_provider_drift_after_utc_read_retains_partial_tuple(
    source: tuple[Path, _ClockSource], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, clock_source = source
    sampler = clocks.HostClockSampler.start(source_root=root)

    def mutate_provider_during_read() -> int:
        clock_source.counter_implementation = "GetTickCount64()"
        return clock_source.read_utc()

    monkeypatch.setattr(clocks, "_utc_ns", mutate_provider_during_read)
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_PROVIDER_UNSUPPORTED"):
        sampler.checkpoint("complete")
    diagnostic = sampler.failure_diagnostic
    assert diagnostic is not None
    assert [row.name for row in diagnostic.raw_readings] == ["counter_before_ns", "utc_ns"]
    assert diagnostic.raw_readings[1].integer_value == _UTC + 100


def test_checkpoint_inside_existing_store_transaction_never_reads_another_root(
    source: tuple[Path, _ClockSource], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _ = source
    store = root / "events"
    store.mkdir()
    sampler = clocks.HostClockSampler.start(source_root=root)
    sampler.recheck_policy()

    def forbidden_file_read(**kwargs: Any) -> bytes:
        raise AssertionError("clock checkpoint tried to acquire filesystem authority")

    with exclusive_store_maintenance(store_root=store):
        with monkeypatch.context() as guard:
            guard.setattr(clocks, "read_contained_artifact_bytes", forbidden_file_read)
            observed = sampler.checkpoint("payload_complete")
    sampler.recheck_policy()
    final = sampler.checkpoint("after_policy_recheck")
    assert final.anchor == observed.anchor
    assert final.checkpoints[:-1] == observed.checkpoints


@pytest.mark.parametrize("after_checkpoint", [False, True])
@pytest.mark.parametrize("operation", ["checkpoint", "policy_recheck"])
def test_non_owner_access_terminates_chain_without_sampling(
    source: tuple[Path, _ClockSource],
    monkeypatch: pytest.MonkeyPatch,
    after_checkpoint: bool,
    operation: str,
) -> None:
    root, clock_source = source
    sampler = clocks.HostClockSampler.start(source_root=root)
    if after_checkpoint:
        sampler.checkpoint("completed_prior")
    owner = clocks._thread_identity()
    monkeypatch.setattr(clocks, "_thread_identity", lambda: owner + 1)
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_CONCURRENT_ACCESS"):
        if operation == "checkpoint":
            sampler.checkpoint("foreign_thread")
        else:
            sampler.recheck_policy()
    assert clock_source.order == ["COUNTER", "UTC", "COUNTER"] * (2 if after_checkpoint else 1)
    assert sampler.failure_diagnostic is not None
    assert sampler.failure_diagnostic.attempted_label == (
        "foreign_thread" if operation == "checkpoint" else "policy_recheck"
    )
    assert sampler.failure_diagnostic.raw_readings == ()
    monkeypatch.setattr(clocks, "_thread_identity", lambda: owner)
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_SAMPLER_FAILED"):
        sampler.checkpoint("cannot_resume")


def test_reentrant_checkpoint_cannot_publish_over_a_failed_chain(
    source: tuple[Path, _ClockSource], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _ = source
    sampler = clocks.HostClockSampler.start(source_root=root)

    def nested_read() -> int:
        with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_CONCURRENT_ACCESS"):
            sampler.checkpoint("nested")
        return _UTC + 100

    monkeypatch.setattr(clocks, "_utc_ns", nested_read)
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_CONCURRENT_ACCESS"):
        sampler.checkpoint("outer")
    assert sampler.failure_diagnostic is not None
    assert sampler.failure_diagnostic.attempted_label == "outer"
    assert len(sampler.failure_diagnostic.raw_readings) == 1
    assert sampler.failure_diagnostic.raw_readings[0].name == "counter_before_ns"
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_SAMPLER_FAILED"):
        _ = sampler.evidence


def test_reentrant_policy_check_retains_its_own_empty_read_scope(
    source: tuple[Path, _ClockSource], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, clock_source = source
    sampler = clocks.HostClockSampler.start(source_root=root)

    def nested_policy_check(source_root: Path) -> None:
        with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_CONCURRENT_ACCESS"):
            sampler.checkpoint("nested_during_policy")

    monkeypatch.setattr(clocks, "_check_policy", nested_policy_check)
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_CONCURRENT_ACCESS"):
        sampler.recheck_policy()
    assert sampler.failure_diagnostic is not None
    assert sampler.failure_diagnostic.attempted_label == "policy_recheck"
    assert sampler.failure_diagnostic.raw_readings == ()
    assert clock_source.order == ["COUNTER", "UTC", "COUNTER"]
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_SAMPLER_FAILED"):
        _ = sampler.evidence


def test_anchor_failure_preserves_raw_tuple_on_original_exception(
    source: tuple[Path, _ClockSource],
) -> None:
    root, clock_source = source
    clock_source.counters[1] = 999
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_COUNTER_BACKWARD") as caught:
        clocks.HostClockSampler.start(source_root=root)
    diagnostic = caught.value.diagnostic
    assert isinstance(diagnostic, clocks.HostClockFailureDiagnostic)
    assert diagnostic.prior_evidence is None
    assert diagnostic.attempted_label == "anchor"
    assert [row.integer_value for row in diagnostic.raw_readings] == [1000, _UTC, 999]


def test_interrupted_anchor_retains_partial_reads_without_replacing_exception(
    source: tuple[Path, _ClockSource], monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _ = source
    failure = OSError("synthetic UTC read interruption")

    def interrupted_utc() -> int:
        raise failure

    monkeypatch.setattr(clocks, "_utc_ns", interrupted_utc)
    with pytest.raises(OSError) as caught:
        clocks.HostClockSampler.start(source_root=root)
    assert caught.value is failure
    diagnostic = failure.diagnostic  # type: ignore[attr-defined]
    assert isinstance(diagnostic, clocks.HostClockFailureDiagnostic)
    assert diagnostic.error_code == "HOST_CLOCK_SAMPLING_INTERRUPTED"
    assert [row.integer_value for row in diagnostic.raw_readings] == [1000]


@pytest.mark.parametrize("identity", [("linux", "CPython", "3.11.9"), ("win32", "PyPy", "3.11.9")])
def test_unsupported_runtime_does_not_sample_or_choose_a_different_provider(
    source: tuple[Path, _ClockSource],
    monkeypatch: pytest.MonkeyPatch,
    identity: tuple[str, str, str],
) -> None:
    root, clock_source = source
    monkeypatch.setattr(clocks, "_runtime_identity", lambda: identity)
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_PROVIDER_UNSUPPORTED"):
        clocks.HostClockSampler.start(source_root=root)
    assert clock_source.order == []


@pytest.mark.parametrize("changed", [True, 1, -1.0, 0.0, float("nan"), float("inf")])
def test_clock_info_requires_real_finite_positive_float_resolution(
    source: tuple[Path, _ClockSource], monkeypatch: pytest.MonkeyPatch, changed: Any
) -> None:
    root, clock_source = source

    def info(name: str) -> SimpleNamespace:
        result = clock_source.info(name)
        result.resolution = changed
        return result

    monkeypatch.setattr(clocks, "_clock_info", info)
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_PROVIDER_UNSUPPORTED"):
        clocks.HostClockSampler.start(source_root=root)
    assert clock_source.order == []


def test_missing_policy_and_incomplete_metadata_fail_with_typed_errors(
    source: tuple[Path, _ClockSource], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, clock_source = source
    missing = tmp_path / "missing_policy"
    missing.mkdir()
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_POLICY_INVALID"):
        clocks.HostClockSampler.start(source_root=missing)
    monkeypatch.setattr(clocks, "_clock_info", lambda name: SimpleNamespace())
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_PROVIDER_UNSUPPORTED"):
        clocks.HostClockSampler.start(source_root=root)
    assert clock_source.order == []
