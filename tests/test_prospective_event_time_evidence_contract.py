"""Synthetic declarations only: no activation, recorder, calendar lookup or business run."""

from __future__ import annotations

import builtins
import hashlib
import io
import json
import socket
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any

import pytest

from ai_trading_system.contracts.prospective_event_time_evidence import (
    EVIDENCE_ROLE,
    PRIMARY_WINDOW_START,
    RETURN_CLOCK,
    TIMING_VERSION,
    ArtifactBinding,
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

_SHA = "a" * 64
_COMMIT = "1" * 40


def _plan(**changes: Any) -> RecordingPlan:
    values: dict[str, Any] = {
        "plan_id": "synthetic_event_time_v1",
        "consumer_family": "FIVE_CANDIDATE",
        "declared_source_commit": _COMMIT,
        "definition_bindings": (PayloadMember("definition", b"synthetic definition").binding,),
    }
    values.update(changes)
    return RecordingPlan(**values)


def _timing(**changes: Any) -> SessionTiming:
    values: dict[str, Any] = {
        "feature_session": date(2026, 11, 27),
        "effective_session": date(2026, 11, 30),
        "first_return_end_session": date(2026, 12, 1),
        "feature_close_at": datetime(2026, 11, 27, 18, tzinfo=UTC),
        "effective_close_at": datetime(2026, 11, 30, 21, tzinfo=UTC),
        "first_return_end_close_at": datetime(2026, 12, 1, 21, tzinfo=UTC),
    }
    values.update(changes)
    return SessionTiming(**values)


def test_canonical_json_is_sorted_utf8_and_preserves_boolean_types() -> None:
    payload = {"z": [False, 0, True, 1, None], "a": {"title": "合成证据"}}
    expected = '{"a":{"title":"合成证据"},"z":[false,0,true,1,null]}'.encode()
    assert canonical_json_bytes(payload) == expected
    assert strict_json_loads(expected) == payload
    assert type(strict_json_loads(b"false")) is bool
    assert type(strict_json_loads(b"0")) is int
    assert canonical_json_bytes({"b": 2, "a": 1}) == canonical_json_bytes({"a": 1, "b": 2})


@pytest.mark.parametrize(
    "content",
    [
        b'{"role":1,"role":2}',
        b'{"nested":{"role":1,"r\\u006fle":2}}',
        b'[{"x":1,"x":2}]',
        b'{"x":NaN}',
        b'{"x":Infinity}',
        b'{"x":-Infinity}',
        b'{"x":1e999}',
        b'{"x":"\\ud800"}',
        b'{"x":1} trailing',
        b"\xff",
        bytearray(b"{}"),
        "{}",
    ],
)
def test_strict_json_rejects_ambiguous_invalid_and_mutable_input(content: Any) -> None:
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_JSON_INVALID"):
        strict_json_loads(content)


@pytest.mark.parametrize(
    "payload",
    [float("nan"), float("inf"), float("-inf"), {1: "key"}, (1, 2), b"raw", {"x": "\ud800"}],
)
def test_canonical_json_rejects_non_json_or_nonfinite_values(payload: Any) -> None:
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_JSON_INVALID"):
        canonical_json_bytes(payload)


def test_canonical_json_rejects_cycles_without_rejecting_shared_values() -> None:
    shared: list[object] = [1]
    assert canonical_json_bytes([shared, shared]) == b"[[1],[1]]"
    shared.append(shared)
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_JSON_INVALID"):
        canonical_json_bytes(shared)


def test_payload_binds_exact_immutable_bytes_without_exporting_content_or_a_path() -> None:
    member = PayloadMember("signal", b"\x00synthetic signal\n")
    binding = member.binding
    assert binding == ArtifactBinding("signal", hashlib.sha256(member.content).hexdigest(), 18)
    assert binding.to_dict() == {
        "role": "signal",
        "sha256": hashlib.sha256(member.content).hexdigest(),
        "size_bytes": len(member.content),
    }
    assert ArtifactBinding.from_json_bytes(binding.canonical_bytes) == binding
    assert PayloadMember("empty", b"").binding.size_bytes == 0
    with pytest.raises(FrozenInstanceError):
        member.content = b"replacement"  # type: ignore[misc]


@pytest.mark.parametrize(
    "role", ["", "Signal", "../signal", "a/b", "a\\b", "a.b", "_signal", "信号", "1signal", True]
)
def test_payload_and_binding_reject_non_identifier_roles(role: Any) -> None:
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_FIELDS_INVALID"):
        PayloadMember(role, b"synthetic")
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_FIELDS_INVALID"):
        ArtifactBinding(role, _SHA, 1)


@pytest.mark.parametrize("content", ["text", bytearray(b"x"), memoryview(b"x"), None])
def test_payload_rejects_mutable_or_implicitly_encoded_content(content: Any) -> None:
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_FIELDS_INVALID"):
        PayloadMember("signal", content)


@pytest.mark.parametrize(
    "changes",
    [
        {"sha256": "A" * 64},
        {"sha256": "a" * 63},
        {"sha256": 123},
        {"size_bytes": True},
        {"size_bytes": -1},
        {"size_bytes": 1.0},
        {"size_bytes": "1"},
    ],
)
def test_hash_and_size_are_strict_for_both_binding_types(changes: dict[str, Any]) -> None:
    for original in (
        ArtifactBinding("signal", _SHA, 1),
        EventBinding("stream/activation.json", _SHA, 1),
    ):
        with pytest.raises(TemporalEvidenceError, match="TEMPORAL_FIELDS_INVALID"):
            replace(original, **changes)


def test_plan_round_trip_identity_covers_complete_canonical_declaration() -> None:
    definition = PayloadMember("definition", b"synthetic definition").binding
    policy = PayloadMember("policy", b"synthetic policy").binding
    plan = _plan(definition_bindings=(policy, definition))
    assert plan.definition_bindings == (definition, policy)
    assert plan.canonical_bytes == _plan(definition_bindings=(definition, policy)).canonical_bytes
    assert RecordingPlan.from_dict(plan.to_dict()) == plan
    assert RecordingPlan.from_json_bytes(plan.canonical_bytes) == plan
    assert RecordingPlan.from_json_bytes(json.dumps(plan.to_dict(), indent=2).encode()) == plan
    assert plan.stream_id == hashlib.sha256(plan.canonical_bytes).hexdigest()
    assert len(plan.stream_id) == 64
    assert plan.timing_version == TIMING_VERSION == "NEXT_XNYS_CLOSE_FORWARD_V1"
    assert plan.return_clock == RETURN_CLOCK == "EFFECTIVE_SESSION_CLOSE_TO_NEXT_XNYS_SESSION_CLOSE"
    assert plan.primary_window_start == PRIMARY_WINDOW_START == date(2021, 2, 22)
    assert plan.evidence_role == EVIDENCE_ROLE == "SYNTHETIC_OR_UNADOPTED_TEMPORAL_EVIDENCE"
    assert plan.real_adoption is plan.observation_authorized is False
    assert plan.temporal_evidence_only is True
    assert plan.provider_available_at_status == plan.oos_admission_status == "NOT_ESTABLISHED"
    assert plan.production_effect == plan.broker_action == "none"

    changes: list[dict[str, Any]] = [
        {"plan_id": "synthetic_event_time_v2"},
        {"consumer_family": "COMPOSER"},
        {"declared_source_commit": "2" * 40},
        {"definition_bindings": (replace(definition, sha256="b" * 64), policy)},
        {
            "definition_bindings": (
                replace(definition, size_bytes=definition.size_bytes + 1),
                policy,
            )
        },
        {"definition_bindings": (replace(definition, role="model"), policy)},
    ]
    ids = {replace(plan, **change).stream_id for change in changes}
    assert len(ids) == len(changes) and plan.stream_id not in ids


@pytest.mark.parametrize(
    "plan_id", ["synthetic_v1", "synthetic.v1", "synthetic@1.0.0", "TRADING_2564_S3A_V1"]
)
def test_plan_accepts_explicit_versioned_identifiers(plan_id: str) -> None:
    assert _plan(plan_id=plan_id).plan_id == plan_id


@pytest.mark.parametrize(
    "changes",
    [
        {"plan_id": "synthetic"},
        {"plan_id": "../synthetic_v1"},
        {"plan_id": "synthetic v1"},
        {"plan_id": "synthetic_v1\n"},
        {"plan_id": 1},
        {"consumer_family": "OTHER"},
        {"consumer_family": True},
        {"declared_source_commit": "a" * 64},
        {"declared_source_commit": "A" * 40},
        {"declared_source_commit": 1},
        {"definition_bindings": []},
        {"definition_bindings": ()},
        {"definition_bindings": ({"role": "definition", "sha256": _SHA, "size_bytes": 1},)},
        {"definition_bindings": (ArtifactBinding("definition", _SHA, 1),) * 2},
    ],
)
def test_plan_rejects_unversioned_or_ambiguous_identity(changes: dict[str, Any]) -> None:
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_FIELDS_INVALID"):
        _plan(**changes)


@pytest.mark.parametrize(
    "field,value",
    [
        ("timing_version", "LEGACY"),
        ("return_clock", "FEATURE_CLOSE_TO_NEXT_CLOSE"),
        ("primary_window_start", date(2022, 12, 1)),
        ("primary_window_start", datetime(2021, 2, 22, tzinfo=UTC)),
        ("evidence_role", "REAL_PROSPECTIVE_EVIDENCE"),
        ("real_adoption", True),
        ("real_adoption", 0),
        ("temporal_evidence_only", False),
        ("temporal_evidence_only", 1),
        ("observation_authorized", True),
        ("observation_authorized", 0),
        ("provider_available_at_status", "PASS"),
        ("oos_admission_status", "PASS"),
        ("production_effect", "enabled"),
        ("broker_action", "order"),
    ],
)
def test_plan_cannot_upgrade_fixed_time_or_admission_declarations(field: str, value: Any) -> None:
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_FIELDS_INVALID"):
        _plan(**{field: value})
    payload = _plan().to_dict()
    payload[field] = value.isoformat() if type(value) is date else value
    with pytest.raises(TemporalEvidenceError):
        RecordingPlan.from_dict(payload)


def test_plain_serialized_metadata_does_not_require_a_real_commit_or_calendar() -> None:
    # All-zero commit and ordered weekend dates remain declarations. No Git or
    # calendar fact may be inferred from a successful DTO parse.
    plan = _plan(declared_source_commit="0" * 40)
    timing = SessionTiming(
        date(2026, 9, 5),
        date(2026, 9, 6),
        date(2026, 9, 7),
        datetime(2026, 9, 5, 20, tzinfo=UTC),
        datetime(2026, 9, 6, 20, tzinfo=UTC),
        datetime(2026, 9, 7, 20, tzinfo=UTC),
    )
    assert plan.real_adoption is False
    assert SessionTiming.from_json_bytes(timing.canonical_bytes) == timing


def test_session_timing_normalizes_explicit_offsets_without_losing_the_instant() -> None:
    timing = _timing()
    offset = timezone(timedelta(hours=-5))
    offset_timing = replace(timing, feature_close_at=datetime(2026, 11, 27, 13, tzinfo=offset))
    assert offset_timing == timing and offset_timing.feature_close_at.tzinfo is UTC
    payload = timing.to_dict()
    payload["feature_close_at"] = "2026-11-27T13:00:00-05:00"
    assert SessionTiming.from_dict(payload) == timing
    assert SessionTiming.from_json_bytes(timing.canonical_bytes) == timing
    assert parse_utc_datetime("2026-11-27T18:00:00Z") == timing.feature_close_at
    assert parse_utc_datetime("2026-11-27T18:00:00.123456+00:00").microsecond == 123456
    assert utc_datetime_text(offset_timing.feature_close_at) == "2026-11-27T18:00:00+00:00"


@pytest.mark.parametrize(
    "value",
    [
        "2026-11-27",
        "2026-11-27T18:00:00",
        "2026-11-27 18:00:00+00:00",
        "20261127T180000Z",
        "2026-11-27T24:00:00Z",
        "2026-11-27T18:00:00+24:00",
        "2026-11-27T18:00:00-00:00",
        "2026-11-27T18:00:00+00:99",
        "2026-11-27T18:00:00.1234567Z",
        True,
        0,
        None,
    ],
)
def test_time_parser_never_assumes_a_timezone_or_silently_changes_precision(value: Any) -> None:
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_TIME_INVALID"):
        parse_utc_datetime(value)


@pytest.mark.parametrize(
    "changes",
    [
        {"feature_session": "2026-11-27"},
        {"feature_session": datetime(2026, 11, 27, tzinfo=UTC)},
        {"effective_session": date(2026, 11, 27)},
        {"first_return_end_session": date(2026, 11, 28)},
        {"feature_close_at": datetime(2026, 11, 27, 18)},
        {"effective_close_at": datetime(2026, 11, 27, 18, tzinfo=UTC)},
        {"first_return_end_close_at": datetime(2026, 11, 28, 18, tzinfo=UTC)},
        {"feature_close_at": "2026-11-27T18:00:00Z"},
    ],
)
def test_session_timing_requires_exact_date_types_and_strict_order(changes: dict[str, Any]) -> None:
    with pytest.raises(TemporalEvidenceError):
        _timing(**changes)


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/events/a.json",
        "//server/share/a",
        "C:/events/a",
        "C:events/a",
        "a\\b",
        "a//b",
        "./a",
        "a/../b",
        "a/./b",
        "a/",
        "a:b",
        "a\x00b",
        "a\nb",
        "a?b",
        "a*b",
        "a|b",
        "a< b",
        'a"b',
        "a.",
        "a/b ",
        " a/b",
        "CON",
        "a/nUl.txt",
        "COM1.json",
        "a/LPT9",
        "COM¹.txt",
    ],
)
def test_event_binding_rejects_paths_that_are_not_portable_and_relative(path: str) -> None:
    with pytest.raises(TemporalEvidenceError, match="TEMPORAL_PATH_INVALID"):
        EventBinding(path, _SHA, 1)


def test_event_binding_round_trip_keeps_exact_path_hash_and_size() -> None:
    binding = EventBinding("streams/abc/sessions/2026-11-27/signal/completion.json", _SHA, 123)
    assert EventBinding.from_dict(binding.to_dict()) == binding
    assert EventBinding.from_json_bytes(binding.canonical_bytes) == binding
    with pytest.raises(FrozenInstanceError):
        binding.sha256 = "b" * 64  # type: ignore[misc]


@pytest.mark.parametrize(
    "value",
    [
        ArtifactBinding("signal", _SHA, 1),
        EventBinding("activation/completion.json", _SHA, 1),
        _plan(),
        _timing(),
    ],
)
def test_dto_parsers_reject_missing_extra_and_wrong_schema_fields(value: Any) -> None:
    cls = type(value)
    payload = value.to_dict()
    extra = {**payload, "verified": True}
    missing = dict(payload)
    missing.pop(next(iter(missing)))
    invalid_payloads: tuple[object, ...] = (extra, missing, [], None)
    for wrong in invalid_payloads:
        with pytest.raises(TemporalEvidenceError, match="TEMPORAL_FIELDS_INVALID"):
            cls.from_dict(wrong)
    if "schema_version" in payload:
        with pytest.raises(TemporalEvidenceError, match="TEMPORAL_FIELDS_INVALID"):
            cls.from_dict({**payload, "schema_version": "unreviewed.v2"})


def test_contract_construction_and_round_trip_perform_no_file_or_network_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("pure time declarations performed I/O")

    with monkeypatch.context() as guard:
        guard.setattr(builtins, "open", forbidden)
        guard.setattr(io, "open", forbidden)
        guard.setattr(socket, "socket", forbidden)
        plan, timing = _plan(), _timing()
        assert RecordingPlan.from_json_bytes(plan.canonical_bytes) == plan
        assert SessionTiming.from_json_bytes(timing.canonical_bytes) == timing
        binding = PayloadMember("signal", b"synthetic signal").binding
        assert ArtifactBinding.from_json_bytes(binding.canonical_bytes) == binding
