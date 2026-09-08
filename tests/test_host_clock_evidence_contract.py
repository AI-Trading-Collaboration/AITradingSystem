from __future__ import annotations

import copy
import hashlib
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import localcontext
from typing import Any

import pytest

from ai_trading_system.contracts.host_clock_evidence import (
    MAX_CLOCK_NS,
    POLICY_PATH,
    POLICY_SHA256,
    ClockSample,
    HostClockEvidence,
    HostClockEvidenceError,
    HostClockMetadata,
    HostClockProvider,
    append_clock_checkpoint,
    datetime_to_utc_ns,
    deadline_allows,
    replay_admission_bounds,
    require_clock_evidence_extension,
    utc_ns_to_datetime_ceil,
    utc_ns_to_datetime_floor,
)

pytestmark = pytest.mark.fast_unit
_UTC = 1_700_000_000_000_000_000


def _provider(resolution: str = "1e-07") -> HostClockProvider:
    return HostClockProvider(
        "win32",
        "CPython",
        "3.11.9",
        HostClockMetadata("TIME_NS", "GetSystemTimeAsFileTime()", "0.015625", False, True),
        HostClockMetadata("PERF_COUNTER_NS", "QueryPerformanceCounter()", resolution, True, False),
    )


def _evidence() -> HostClockEvidence:
    return HostClockEvidence(_provider(), ClockSample(_UTC, 10_000, 10_100))


def test_roundtrip_binds_fixed_policy_raw_samples_and_full_checkpoint_history() -> None:
    initial = _evidence()
    first = append_clock_checkpoint(
        initial, label="payload", sample=ClockSample(_UTC + 100, 10_200, 20_000)
    )
    final = append_clock_checkpoint(
        first, label="witness", sample=ClockSample(_UTC + 200, 20_100, 21_000)
    )
    assert replay_admission_bounds(final) == (_UTC + 300, _UTC + 10_200, _UTC + 11_200)
    assert final.anchor == initial.anchor
    assert initial.checkpoints == ()
    assert final.checkpoints[0] == first.checkpoints[0]
    assert final.latest_sample.utc_ns == _UTC + 200 < first.admission_bound_ns
    assert final.policy_path == POLICY_PATH and final.policy_sha256 == POLICY_SHA256
    assert (
        final.absolute_utc_accuracy_proof == final.hidden_host_adjustment_proof == "NOT_ESTABLISHED"
    )
    assert final.child_bound_authority == "CONSUMER_MUST_INDEPENDENTLY_VERIFY"
    for value in (
        final,
        final.provider,
        final.provider.utc_clock,
        final.anchor,
        final.checkpoints[0],
    ):
        assert type(value).from_dict(value.to_dict()) == value
        assert type(value).from_json_bytes(value.canonical_bytes) == value
        assert value.canonical_sha256 == hashlib.sha256(value.canonical_bytes).hexdigest()
    with pytest.raises(FrozenInstanceError):
        final.anchor = final.latest_sample  # type: ignore[misc]


def test_outer_endpoint_latency_and_quantization_are_both_included() -> None:
    initial = HostClockEvidence(_provider(), ClockSample(_UTC, 1_000, 5_000))
    measured = append_clock_checkpoint(
        initial, label="complete", sample=ClockSample(_UTC, 6_000, 10_000)
    )
    # The two UTC readings are equal. All 9,000 ns in the counter outer bracket
    # still count, plus the two distinct 100 ns endpoint encoding bounds.
    assert measured.admission_bound_ns == _UTC + 9_200


@pytest.mark.parametrize(
    "resolution,endpoint_ns", [("1e-07", 100), ("3.333333333333333e-07", 334), ("1e-10", 1)]
)
def test_each_reported_counter_endpoint_rounds_outward_independently(
    resolution: str, endpoint_ns: int
) -> None:
    provider = _provider(resolution)
    evidence = HostClockEvidence(provider, ClockSample(_UTC, 0, 0))
    assert provider.counter_clock.resolution_ns == endpoint_ns
    assert evidence.admission_bound_ns == _UTC + 2 * endpoint_ns


def test_zero_tick_elapsed_does_not_erase_quantization_near_deadline() -> None:
    # Deliberately coarse recorded QPC frequency, without a subjective tolerance.
    evidence = HostClockEvidence(_provider("0.01"), ClockSample(_UTC, 0, 0))
    evidence = append_clock_checkpoint(evidence, label="complete", sample=ClockSample(_UTC, 0, 0))
    assert evidence.admission_bound_ns == _UTC + 20_000_000
    assert not deadline_allows(
        evidence.admission_bound_ns, utc_ns_to_datetime_ceil(_UTC + 5_000_000)
    )


def test_positive_raw_utc_cannot_hide_twenty_seconds_across_a_ten_second_deadline() -> None:
    evidence = HostClockEvidence(_provider(), ClockSample(_UTC, 0, 0))
    evidence = append_clock_checkpoint(
        evidence,
        label="complete",
        sample=ClockSample(_UTC + 5_000_000_000, 20_000_000_000, 20_000_000_000),
    )
    assert evidence.latest_sample.utc_ns > evidence.anchor.utc_ns
    assert not deadline_allows(
        evidence.admission_bound_ns, utc_ns_to_datetime_ceil(_UTC + 10_000_000_000)
    )


def test_forward_wall_jump_prior_bound_and_verified_child_bound_remain_in_history() -> None:
    initial = _evidence()
    first = append_clock_checkpoint(
        initial,
        label="child",
        sample=ClockSample(_UTC + 100, 10_100, 10_200),
        inherited_child_bound_ns=_UTC + 1_000_000,
    )
    second = append_clock_checkpoint(
        first, label="postguard", sample=ClockSample(_UTC + 200, 10_200, 10_300)
    )
    assert first.admission_bound_ns == second.admission_bound_ns == _UTC + 1_000_000
    third = append_clock_checkpoint(
        second, label="final", sample=ClockSample(_UTC + 2_000_000, 10_300, 10_400)
    )
    assert third.admission_bound_ns == third.latest_sample.utc_ns
    # Removing the checkpoint carrying the child would orphan a stored bound.
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_BOUND_MISMATCH"):
        replace(second, checkpoints=(replace(second.checkpoints[1], sequence=1),))


@pytest.mark.parametrize(
    "sample,code",
    [
        (ClockSample(_UTC - 100, 10_100, 10_200), "HOST_CLOCK_UTC_BACKWARD"),
        (ClockSample(_UTC, 10_000, 10_200), "HOST_CLOCK_COUNTER_BACKWARD"),
        (ClockSample(_UTC + 1, 10_100, 10_200), "HOST_CLOCK_SAMPLE_ENCODING_INVALID"),
    ],
)
def test_raw_metadata_encoding_and_counter_chronology_fail_closed(
    sample: ClockSample, code: str
) -> None:
    with pytest.raises(HostClockEvidenceError, match=code):
        append_clock_checkpoint(_evidence(), label="complete", sample=sample)


@pytest.mark.parametrize("field", ["utc_ns", "counter_before_ns", "counter_after_ns"])
@pytest.mark.parametrize("value", [True, False, 1.0, "1", None, -1, MAX_CLOCK_NS + 1])
def test_ns_fields_require_nonnegative_exact_bounded_integers(field: str, value: Any) -> None:
    args: dict[str, Any] = {"utc_ns": _UTC, "counter_before_ns": 0, "counter_after_ns": 0}
    args[field] = value
    with pytest.raises(HostClockEvidenceError):
        ClockSample(**args)


def test_inner_counter_reversal_and_derived_integer_overflow_fail_closed() -> None:
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_COUNTER_BACKWARD"):
        ClockSample(_UTC, 2, 1)
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_NS_OVERFLOW"):
        HostClockEvidence(_provider(), ClockSample(MAX_CLOCK_NS - MAX_CLOCK_NS % 100, 0, 100))


@pytest.mark.parametrize(
    "value", [True, 1, 0.015625, "NaN", "nan", "inf", "0.0", "-1.0", "1E-07", "0.010", "1e+100"]
)
def test_reported_resolution_is_canonical_finite_decimal_not_caller_tolerance(value: Any) -> None:
    with pytest.raises(HostClockEvidenceError):
        replace(_provider().counter_clock, reported_resolution_seconds=value)


@pytest.mark.parametrize(
    "field,value",
    [
        ("clock_name", "monotonic"),
        ("implementation", "GetTickCount64()"),
        ("monotonic", False),
        ("monotonic", 1),
        ("adjustable", True),
        ("adjustable", 0),
    ],
)
def test_unknown_clock_provider_or_attributes_are_not_equivalent(field: str, value: Any) -> None:
    with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_PROVIDER_UNSUPPORTED"):
        replace(_provider().counter_clock, **{field: value})


@pytest.mark.parametrize(
    "field,value",
    [
        ("platform", "linux"),
        ("python_implementation", "PyPy"),
        ("python_version", "3.11"),
        ("python_version", "03.011.009"),
        ("profile_id", "other_v1"),
    ],
)
def test_runtime_profile_is_explicit_and_exact(field: str, value: Any) -> None:
    with pytest.raises(HostClockEvidenceError):
        replace(_provider(), **{field: value})


def test_runtime_version_is_recorded_not_silently_used_as_source_authorization() -> None:
    assert replace(_provider(), python_version="3.12.9").python_version == "3.12.9"
    with pytest.raises(HostClockEvidenceError):
        replace(_provider(), utc_clock=_provider().counter_clock)


@pytest.mark.parametrize(
    "field,value",
    [
        ("policy_path", "other.yaml"),
        ("policy_sha256", "a" * 64),
        ("bound_formula_version", "other_v1"),
        ("hidden_host_adjustment_proof", "PASS"),
        ("absolute_utc_accuracy_proof", "PASS"),
        ("child_bound_authority", "VERIFIED"),
        ("host_clock_model", "EXTERNAL_UTC_PROOF"),
    ],
)
def test_fixed_policy_and_non_authority_claims_cannot_be_widened(field: str, value: Any) -> None:
    with pytest.raises(HostClockEvidenceError):
        replace(_evidence(), **{field: value})


def test_retained_replay_rejects_metadata_bound_anchor_and_sequence_tamper() -> None:
    evidence = append_clock_checkpoint(
        _evidence(), label="complete", sample=ClockSample(_UTC + 100, 10_200, 20_000)
    )
    payload: dict[str, Any] = evidence.to_dict()
    mutations = []
    changed = copy.deepcopy(payload)
    changed["checkpoints"][0]["admission_bound_ns"] += 1
    mutations.append(changed)
    changed = copy.deepcopy(payload)
    changed["provider"]["counter_clock"]["reported_resolution_seconds"] = "1e-06"
    mutations.append(changed)
    changed = copy.deepcopy(payload)
    changed["anchor"]["counter_before_ns"] += 1
    mutations.append(changed)
    changed = copy.deepcopy(payload)
    changed["checkpoints"][0]["sequence"] = 2
    mutations.append(changed)
    for changed in mutations:
        with pytest.raises(HostClockEvidenceError):
            HostClockEvidence.from_dict(changed)
    with pytest.raises(HostClockEvidenceError):
        append_clock_checkpoint(
            evidence, label="complete", sample=ClockSample(_UTC + 200, 20_000, 21_000)
        )


@pytest.mark.parametrize("location", ["root", "provider", "anchor", "checkpoint", "metadata"])
def test_exact_fields_reject_unknown_nested_data(location: str) -> None:
    evidence = append_clock_checkpoint(
        _evidence(), label="complete", sample=ClockSample(_UTC + 100, 10_100, 10_200)
    )
    raw: dict[str, Any] = evidence.to_dict()
    target = {
        "root": raw,
        "provider": raw["provider"],
        "anchor": raw["anchor"],
        "checkpoint": raw["checkpoints"][0],
        "metadata": raw["provider"]["counter_clock"],
    }[location]
    target["tolerance"] = 1000
    with pytest.raises(HostClockEvidenceError):
        HostClockEvidence.from_dict(raw)


def test_json_is_canonical_and_duplicate_fields_cannot_override_policy() -> None:
    evidence = _evidence()
    for changed in (
        b" " + evidence.canonical_bytes,
        b'{"schema_version":"a","schema_version":"b"}',
        b'{"x":NaN}',
    ):
        with pytest.raises(HostClockEvidenceError):
            HostClockEvidence.from_json_bytes(changed)


def test_integer_datetime_conversion_and_strict_deadline_equality() -> None:
    instant = datetime(2025, 3, 7, 21, tzinfo=UTC)
    ns = datetime_to_utc_ns(instant)
    assert datetime_to_utc_ns(instant.astimezone(timezone(timedelta(hours=9)))) == ns
    assert utc_ns_to_datetime_floor(ns + 999) == instant
    assert utc_ns_to_datetime_ceil(ns + 1) == instant + timedelta(microseconds=1)
    assert utc_ns_to_datetime_ceil(ns) == utc_ns_to_datetime_floor(ns) == instant
    assert deadline_allows(ns - 1, instant)
    assert not deadline_allows(ns, instant)
    assert not deadline_allows(ns + 1, instant)
    for invalid in (True, 1.0, -1, MAX_CLOCK_NS + 1):
        with pytest.raises(HostClockEvidenceError):
            utc_ns_to_datetime_ceil(invalid)
    with pytest.raises(HostClockEvidenceError):
        deadline_allows(ns, instant.replace(tzinfo=None))


@pytest.mark.parametrize("precision", [1, 2, 6, 28, 80])
def test_counter_quantization_is_independent_of_ambient_decimal_context(precision: int) -> None:
    with localcontext() as context:
        context.prec = precision
        provider = _provider("1.0526315789473686e-08")
        evidence = HostClockEvidence(provider, ClockSample(_UTC, 0, 0))
        assert provider.counter_clock.resolution_ns == 11
        assert evidence.admission_bound_ns == _UTC + 22


def test_extension_requires_same_provider_anchor_and_every_prior_checkpoint() -> None:
    initial = _evidence()
    first = append_clock_checkpoint(
        initial, label="payload", sample=ClockSample(_UTC + 100, 10_200, 20_000)
    )
    final = append_clock_checkpoint(
        first, label="witness", sample=ClockSample(_UTC + 200, 20_100, 21_000)
    )
    require_clock_evidence_extension(initial, first)
    require_clock_evidence_extension(first, final)
    wrong_provider = HostClockEvidence(
        replace(initial.provider, python_version="3.11.10"), initial.anchor
    )
    wrong_anchor = HostClockEvidence(initial.provider, ClockSample(_UTC, 10_001, 10_100))
    wrong_prefix = append_clock_checkpoint(
        initial, label="another_payload", sample=first.latest_sample
    )
    for prior, current in (
        (first, first),
        (final, first),
        (wrong_provider, final),
        (wrong_anchor, final),
        (wrong_prefix, final),
    ):
        with pytest.raises(HostClockEvidenceError, match="HOST_CLOCK_EXTENSION_INVALID"):
            require_clock_evidence_extension(prior, current)
