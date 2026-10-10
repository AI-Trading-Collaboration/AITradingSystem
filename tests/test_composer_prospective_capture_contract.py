"""Composer declarations; synthetic inputs, no model run and no real-checkout child."""

from __future__ import annotations

import builtins
import hashlib
import io
import json
import socket
import subprocess
from collections.abc import Callable
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

import pytest

from ai_trading_system.contracts.composer_prospective_capture import (
    CLOCK_STAGES,
    ComposerCaptureManifest,
    ComposerCaptureRequest,
    ComposerCompletionAcknowledgement,
    ComposerOwnerReview,
    validate_composer_clock_prefix,
)
from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.contracts.host_clock_evidence import (
    ClockSample,
    HostClockEvidence,
    HostClockMetadata,
    HostClockProvider,
    append_clock_checkpoint,
    datetime_to_utc_ns,
    deadline_allows,
)
from ai_trading_system.contracts.named_data_quality_execution import (
    COMPOSER_INPUT_POLICY_PATH,
    COMPOSER_INPUT_POLICY_SHA256,
    COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_PATH,
    COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_SHA256,
    EQUAL_RISK_PRICE_REGISTRY_PATH,
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256,
    NamedArtifactBinding,
    NamedComposerInputScope,
    NamedDQExecutionRequest,
    NamedDQRoots,
    NamedEqualRiskPriceScope,
    NamedSnapshotSelector,
)
from ai_trading_system.contracts.prospective_capture_execution import RecorderReturnObservation

_COMMIT = "1" * 40
_SHA = "a" * 64
_READINESS = date(2026, 11, 25)
_FEATURE = date(2026, 11, 27)
_EFFECTIVE = date(2026, 11, 30)
_REVIEWED = datetime(2026, 11, 24, 20, tzinfo=UTC)
_DEADLINE = datetime(2026, 11, 30, 21, tzinfo=UTC)
_SEGMENTS = ("training", "exact_cash", "primary")
_SYNTHETIC_PREFIX = "outputs/architecture/trading_2560_composer_known_snapshot/synthetic/"
_OWNER_DECISION = (
    "owner_instruction:TRADING-2560:2026-09-09:continue_known_snapshot_prospective_research"
)


def _input_policy() -> NamedArtifactBinding:
    # An explicit synthetic declaration, never an asserted on-disk policy seal.
    return NamedArtifactBinding(
        "EXECUTION", COMPOSER_INPUT_POLICY_PATH, COMPOSER_INPUT_POLICY_SHA256, 128
    )


def _scope(segment: str = "training", as_of: date = _FEATURE) -> NamedComposerInputScope:
    return NamedComposerInputScope(as_of, segment, _input_policy())


def _manifest(**changes: Any) -> ComposerCaptureManifest:
    output = _SYNTHETIC_PREFIX + "synthetic_composer_v1"
    values: dict[str, Any] = {
        "manifest_id": "synthetic_composer_v1",
        "roots": NamedDQRoots(
            source_root="/synthetic/source",
            publication_root="/synthetic/publication",
            execution_root="/synthetic/execution",
            evidence_root="/synthetic/execution/" + output + "/dq",
        ),
        "candidate_commit": _COMMIT,
        "output_relative_path": output,
        "source_output_relative_path": "outputs/synthetic_source",
        "readiness_as_of": _READINESS,
        "allowed_feature_sessions": (_FEATURE,),
        "expires_at": _DEADLINE,
        "evidence_purpose": "SYNTHETIC_ENGINEERING",
    }
    values.update(changes)
    return ComposerCaptureManifest(**values)


def _review(manifest: ComposerCaptureManifest | None = None, **changes: Any) -> ComposerOwnerReview:
    manifest = manifest or _manifest()
    values: dict[str, Any] = {
        "manifest_id": manifest.manifest_id,
        "manifest_sha256": manifest.canonical_sha256,
        "decision_ref": "synthetic_review:TRADING-2560:bounded_contract_test",
        "reviewed_at": _REVIEWED,
        "authorization_state": "STANDING_OWNER_SCOPE",
        "evidence_purpose": manifest.evidence_purpose,
    }
    values.update(changes)
    return ComposerOwnerReview(**values)


def _named_request(
    manifest: ComposerCaptureManifest, operation: str, segment: str
) -> NamedDQExecutionRequest:
    feature = manifest.readiness_as_of if operation == "readiness" else _FEATURE
    scope = _scope(segment, feature).dq_scope
    return NamedDQExecutionRequest(
        roots=replace(
            manifest.roots,
            evidence_root=manifest.roots.evidence_root + "/" + operation + "/" + segment,
        ),
        selector=NamedSnapshotSelector("synthetic_pointer", _SHA, "synthetic_transaction", _SHA),
        scope=scope,
        source_output_relative_path=manifest.source_output_relative_path,
        policy_path="config/data_quality.yaml",
        execution_profile_id="manual.v1",
        candidate_commit=manifest.candidate_commit,
        source_manifest_path=manifest.source_manifest_path,
        source_manifest_sha256=manifest.source_manifest_sha256,
        # Keep the genuine common window distinct from the declared F price range.
        expected_evaluated_window=DataQualityDateWindow(scope.requested_window.start, _READINESS),
    )


def _request(operation: str = "capture", **changes: Any) -> ComposerCaptureRequest:
    manifest = changes.pop("manifest", None) or _manifest()
    review = _review(manifest)
    values: dict[str, Any] = {
        "operation": operation,
        "manifest": manifest,
        "owner_review": NamedArtifactBinding(
            "EXECUTION",
            manifest.output_relative_path + "/control/owner_review.json",
            review.canonical_sha256,
            len(review.canonical_bytes),
        ),
        "roots": manifest.roots,
        "candidate_commit": manifest.candidate_commit,
        "source_manifest_path": manifest.source_manifest_path,
        "source_manifest_sha256": manifest.source_manifest_sha256,
        "policy_path": manifest.policy_path,
        "feature_session": (
            None
            if operation == "activate"
            else _READINESS
            if operation == "readiness"
            else _FEATURE
        ),
        "named_dq_requests": (
            ()
            if operation == "activate"
            else tuple(_named_request(manifest, operation, segment) for segment in _SEGMENTS)
        ),
    }
    values.update(changes)
    return ComposerCaptureRequest(**values)


def _clock(
    operation: str = "capture",
    *,
    start: datetime | None = None,
    counter_step_ns: int = 1000,
    raw_increment_ns: int = 1000,
    resolution: str = "1e-07",
) -> tuple[HostClockEvidence, tuple[RecorderReturnObservation, ...]]:
    default_start = datetime(2026, 11, 27 if operation == "capture" else 25, 18, 0, 1, tzinfo=UTC)
    instant = datetime_to_utc_ns(start or default_start)
    provider = HostClockProvider(
        "win32",
        "CPython",
        "3.11.9",
        HostClockMetadata("TIME_NS", "GetSystemTimeAsFileTime()", "0.015625", False, True),
        HostClockMetadata("PERF_COUNTER_NS", "QueryPerformanceCounter()", resolution, True, False),
    )
    cursor = 0
    reads = 0

    def sample() -> ClockSample:
        nonlocal cursor, reads
        before = cursor
        cursor += counter_step_ns
        observed = instant + reads * raw_increment_ns
        reads += 1
        result = ClockSample(observed, before, cursor)
        cursor += counter_step_ns
        return result

    parent = HostClockEvidence(provider, sample())
    returns = []
    for label in CLOCK_STAGES[operation]:
        child_bound = None
        if label in {"inputs_return", "witness_return"}:
            child = HostClockEvidence(provider, sample())
            for stage in (
                "pre_payload",
                "payload_complete",
                "post_payload_guards",
                "witness_precommit",
                "recorder_return",
            ):
                child = append_clock_checkpoint(child, label=stage, sample=sample())
            event = NamedArtifactBinding(
                "EXECUTION", "outputs/synthetic/" + label + ".json", _SHA, 1
            )
            returns.append(RecorderReturnObservation(event, child))
            child_bound = child.admission_bound_ns
        parent = append_clock_checkpoint(
            parent, label=label, sample=sample(), inherited_child_bound_ns=child_bound
        )
    return parent, tuple(returns)


def _ack(operation: str = "capture", **changes: Any) -> ComposerCompletionAcknowledgement:
    request = _request(operation)
    clock, returns = _clock(operation)
    values: dict[str, Any] = {
        "request_id": request.request_id,
        "request_sha256": request.canonical_sha256,
        "manifest_sha256": request.manifest.canonical_sha256,
        "operation": operation,
        "feature_session": request.feature_session,
        "candidate_commit": _COMMIT,
        "execution_identity_sha256": _SHA,
        "source_hold_id": "hold-" + "1" * 20,
        "recorder_event": returns[-1].event,
        "clock_evidence": clock,
        "recorder_returns": returns,
        "first_feature_session": _FEATURE,
        "decision_effective_session": None if operation == "activate" else _EFFECTIVE,
        "decision_deadline": None if operation == "activate" else _DEADLINE,
        "authorization_state": "STANDING_OWNER_SCOPE",
        "evidence_purpose": "SYNTHETIC_ENGINEERING",
        "technical_validation_state": (
            "ACTIVATION_ACKNOWLEDGED" if operation == "activate" else "CAPTURE_ACKNOWLEDGED"
        ),
    }
    values.update(changes)
    return ComposerCompletionAcknowledgement(**values)


def _declaration(kind: str) -> Any:
    return cast(
        dict[str, Callable[[], Any]],
        {
            "manifest": _manifest,
            "review": _review,
            "activation": lambda: _request("activate"),
            "readiness": lambda: _request("readiness"),
            "capture": _request,
            "activation_ack": lambda: _ack("activate"),
            "capture_ack": _ack,
        },
    )[kind]()


_DECLARATIONS = (
    "manifest",
    "review",
    "activation",
    "readiness",
    "capture",
    "activation_ack",
    "capture_ack",
)


def test_profile_is_a_real_reviewed_pin_not_a_test_replacement() -> None:
    assert COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_PATH.endswith(
        "named_composer_prospective_sources_v2.json"
    )
    assert len(COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_SHA256) == 64
    assert set(COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_SHA256) <= set("0123456789abcdef")


@pytest.mark.parametrize("kind", _DECLARATIONS)
def test_declarations_round_trip_canonical_json_and_preserve_identity(kind: str) -> None:
    value = _declaration(kind)
    assert type(value).from_dict(value.to_dict()) == value
    assert type(value).from_json_bytes(value.canonical_bytes) == value
    assert value.canonical_sha256 == hashlib.sha256(value.canonical_bytes).hexdigest()
    with pytest.raises(FrozenInstanceError):
        value.production_effect = "changed"


@pytest.mark.parametrize("kind", _DECLARATIONS)
def test_declarations_reject_unknown_missing_and_legacy_schema_fields(kind: str) -> None:
    value = _declaration(kind)
    for mutation in ("unknown", "missing", "legacy"):
        raw = value.to_dict()
        if mutation == "unknown":
            raw["allow_stale"] = True
        elif mutation == "missing":
            raw.pop("schema_version")
        else:
            raw["schema_version"] = "prospective_capture_manifest.v1"
        with pytest.raises(ValueError):
            type(value).from_dict(raw)


@pytest.mark.parametrize("kind", _DECLARATIONS)
def test_json_rejects_duplicate_keys_noncanonical_bytes_and_nonobjects(kind: str) -> None:
    value = _declaration(kind)
    duplicate = b'{"schema_version":"duplicate",' + value.canonical_bytes.lstrip()[1:]
    for content in (duplicate, value.canonical_bytes + b" ", b"[]", b"null", b"\xff"):
        with pytest.raises(ValueError):
            type(value).from_json_bytes(content)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_json_rejects_nonfinite_scalar_tokens(value: float) -> None:
    raw = _manifest().to_dict()
    raw["orders"] = value
    with pytest.raises(ValueError):
        ComposerCaptureManifest.from_json_bytes(json.dumps(raw).encode())


@pytest.mark.parametrize(
    "field,value",
    [
        ("activation_attempt_maximum", True),
        ("activation_attempt_maximum", 1.0),
        ("activation_attempt_maximum", "1"),
        ("canonical_dq_calls_per_stage_maximum", 3.0),
        ("canonical_dq_calls_per_manifest_maximum", "6"),
        ("parent_canonical_dq_call_maximum", False),
        ("orders", False),
        ("orders", 0.0),
        ("provider_calls_allowed", 0),
        ("future_observation_outcome_access_allowed", "false"),
        ("readiness_as_of", "2026-11-25"),
        ("allowed_feature_sessions", [_FEATURE]),
        ("allowed_feature_sessions", (_FEATURE.isoformat(),)),
        ("expires_at", _DEADLINE.isoformat()),
    ],
)
def test_manifest_requires_exact_constructor_scalars(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        _manifest(**{field: value})


@pytest.mark.parametrize(
    "field,value",
    [
        ("activation_attempt_maximum", 2),
        ("readiness_attempt_maximum", 2),
        ("attempts_per_feature_session", 2),
        ("canonical_dq_calls_per_stage_maximum", 4),
        ("canonical_dq_calls_per_manifest_maximum", 7),
        ("parent_canonical_dq_call_maximum", 1),
        ("future_observation_outcome_access_allowed", True),
        ("provider_calls_allowed", True),
        ("cache_mutation_allowed", True),
        ("orders", 1),
        ("fills", 1),
        ("positions", 1),
        ("production_effect", "paper"),
        ("broker_action", "place_order"),
        ("consumer_family", "FIVE_CANDIDATE"),
        ("timing_version", "SAME_SESSION_CLOSE"),
        ("scope_order", ("training", "primary", "exact_cash")),
        ("source_manifest_path", FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH),
        ("source_manifest_sha256", FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256),
        ("input_policy_sha256", "b" * 64),
    ],
)
def test_manifest_cannot_expand_frozen_scope_or_action_maxima(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        _manifest(**{field: value})


@pytest.mark.parametrize(
    "changes",
    [
        {"allowed_feature_sessions": ()},
        {"allowed_feature_sessions": (_FEATURE, _EFFECTIVE)},
        {"allowed_feature_sessions": (_FEATURE, _FEATURE)},
        {"allowed_feature_sessions": (_READINESS,)},
        {"allowed_feature_sessions": (date(2026, 11, 24),)},
        {"readiness_as_of": date(2025, 12, 2)},
        {"expires_at": _DEADLINE.replace(tzinfo=None)},
        {"manifest_id": "synthetic_composer"},
        {"candidate_commit": "main"},
        {"evidence_purpose": "HISTORICAL_REPLAY"},
        {"output_relative_path": "outputs/research/composer_prospective/synthetic_composer_v1"},
        {"source_output_relative_path": "../source"},
    ],
)
def test_manifest_cannot_backfill_readiness_or_invent_output_scope(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        _manifest(**changes)


def test_manifest_accepts_data_raw_source_identity_without_granting_source_writes() -> None:
    manifest = _manifest(source_output_relative_path="data/raw")
    assert ComposerCaptureManifest.from_json_bytes(manifest.canonical_bytes) == manifest
    for operation in ("activate", "readiness", "capture"):
        request = _request(operation, manifest=manifest)
        assert request.required_write_paths == (manifest.output_relative_path,)
        assert all(
            child.source_output_relative_path == "data/raw" for child in request.named_dq_requests
        )
        assert "data/raw" not in request.required_write_paths
    assert manifest.cache_mutation_allowed is False


@pytest.mark.parametrize(
    "source_path",
    [
        "/data/raw",
        "D:/data/raw",
        r"D:\data\raw",
        r"\\host\share\data\raw",
        "../data/raw",
        "data/../raw",
        "data/raw/../../outside",
    ],
)
def test_manifest_rejects_absolute_or_traversing_source_identity(source_path: str) -> None:
    with pytest.raises(ValueError):
        _manifest(source_output_relative_path=source_path)


@pytest.mark.parametrize("protected", ["publication_root", "source_output", "evidence_root"])
def test_manifest_output_must_be_disjoint_from_inputs_and_own_exact_dq_root(protected: str) -> None:
    manifest = _manifest()
    output = manifest.roots.execution_root + "/" + manifest.output_relative_path
    with pytest.raises(ValueError):
        if protected == "publication_root":
            replace(manifest, roots=replace(manifest.roots, publication_root=output + "/inputs"))
        elif protected == "source_output":
            replace(
                manifest,
                roots=replace(manifest.roots, source_root=manifest.roots.execution_root),
                source_output_relative_path=manifest.output_relative_path + "/source",
            )
        else:
            replace(manifest, roots=replace(manifest.roots, evidence_root="/synthetic/elsewhere"))


@pytest.mark.parametrize(
    "segment,start,tickers,secondary",
    [
        ("training", date(2018, 1, 2), ("QQQ", "TQQQ", "SHY"), True),
        ("exact_cash", date(2020, 5, 28), ("SGOV",), False),
        ("primary", date(2021, 2, 22), ("QQQ", "SGOV", "TQQQ"), True),
    ],
)
def test_segment_scope_preserves_original_calendar_roles_and_secondary_requirements(
    segment: str, start: date, tickers: tuple[str, ...], secondary: bool
) -> None:
    declaration = _scope(segment)
    scope = declaration.dq_scope
    assert NamedComposerInputScope.from_dict(declaration.to_dict()) == declaration
    assert scope.as_of == _FEATURE
    assert scope.requested_window == DataQualityDateWindow(start, _FEATURE)
    assert scope.expected_price_tickers == tickers
    assert scope.expected_rate_series == ("DGS2", "DGS10", "DTWEXBGS")
    assert scope.require_secondary_prices is secondary
    assert scope.input_roles == (
        ("prices", "rates", "secondary_prices") if secondary else ("prices", "rates")
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"segment": "five_candidate"},
        {"segment": "TRAINING"},
        {"as_of": date(2025, 12, 2)},
        {"as_of": "2026-11-27"},
        {"segment": True},
    ],
)
def test_composer_scope_rejects_wrong_role_and_untyped_dates(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(_scope(), **changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"root_role": "SOURCE"},
        {"relative_path": "config/research/legacy_input_policy.yaml"},
        {"sha256": "b" * 64},
        {"size_bytes": 0},
    ],
)
def test_composer_scope_requires_complete_named_input_policy(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(_scope(), input_policy_binding=replace(_input_policy(), **changes))


def test_equal_risk_price_declaration_cannot_be_relabelled_as_composer_scope() -> None:
    old = NamedEqualRiskPriceScope(
        _FEATURE,
        DataQualityDateWindow(date(2021, 2, 22), _FEATURE),
        NamedArtifactBinding("EXECUTION", EQUAL_RISK_PRICE_REGISTRY_PATH, _SHA, 1),
    )
    with pytest.raises(ValueError):
        NamedComposerInputScope.from_dict(old.to_dict())


@pytest.mark.parametrize("operation", ["readiness", "capture"])
def test_three_requests_keep_common_window_separate_and_each_stage_owns_its_receipts(
    operation: str,
) -> None:
    request = _request(operation)
    assert len(request.named_dq_requests) == 3
    assert request.manifest.canonical_dq_calls_per_stage_maximum == 3
    assert request.manifest.canonical_dq_calls_per_manifest_maximum == 6
    assert request.manifest.parent_canonical_dq_call_maximum == 0
    assert len({row.selector for row in request.named_dq_requests}) == 1
    assert len({row.roots.evidence_root for row in request.named_dq_requests}) == 3
    for segment, row in zip(_SEGMENTS, request.named_dq_requests, strict=True):
        assert (
            row.roots.evidence_root == request.roots.evidence_root + "/" + operation + "/" + segment
        )
        assert row.expected_evaluated_window is not None
        assert row.expected_evaluated_window.end == _READINESS
        assert row.policy_path == "config/data_quality.yaml"
    assert request.required_write_paths == (request.manifest.output_relative_path,)


def test_readiness_is_a_distinct_operation_and_never_a_capture_for_the_old_session() -> None:
    readiness, capture = _request("readiness"), _request("capture")
    assert readiness.request_id != capture.request_id
    assert readiness.operation_relative_path.endswith("/readiness")
    assert capture.operation_relative_path.endswith("/sessions/2026-11-27")
    assert not (
        {row.roots.evidence_root for row in readiness.named_dq_requests}
        & {row.roots.evidence_root for row in capture.named_dq_requests}
    )
    with pytest.raises(ValueError):
        replace(readiness, operation="capture")
    with pytest.raises(ValueError):
        replace(capture, feature_session=_READINESS)
    activation = _request("activate")
    assert activation.feature_session is None and activation.named_dq_requests == ()
    with pytest.raises(ValueError):
        replace(activation, feature_session=_FEATURE)
    with pytest.raises(ValueError):
        replace(activation, named_dq_requests=capture.named_dq_requests)


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "reordered", "extra", "list"])
def test_capture_rejects_missing_duplicate_reordered_or_untyped_segment_sets(mutation: str) -> None:
    request = _request()
    rows = request.named_dq_requests
    mutations: dict[str, Any] = {
        "missing": rows[:2],
        "duplicate": (rows[0], rows[0], rows[2]),
        "reordered": tuple(reversed(rows)),
        "extra": (*rows, rows[0]),
        "list": list(rows),
    }
    with pytest.raises(ValueError):
        replace(request, named_dq_requests=mutations[mutation])


@pytest.mark.parametrize(
    "field,value",
    [
        ("selector", NamedSnapshotSelector("another_pointer", _SHA, "another_transaction", _SHA)),
        ("candidate_commit", "2" * 40),
        ("source_manifest_path", FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH),
        ("source_manifest_sha256", FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256),
        ("source_output_relative_path", "outputs/another_source"),
        ("policy_path", "config/research/another_dq_policy.yaml"),
    ],
)
def test_segments_cannot_mix_snapshot_code_profile_or_dq_policy(field: str, value: object) -> None:
    request = _request()
    first, second, third = request.named_dq_requests
    changed = replace(second, **{field: cast(Any, value)})
    with pytest.raises(ValueError):
        replace(request, named_dq_requests=(first, changed, third))


@pytest.mark.parametrize(
    "field", ["source_root", "publication_root", "execution_root", "evidence_root"]
)
def test_segment_cannot_escape_its_roots_or_reuse_another_stage_evidence(field: str) -> None:
    request = _request()
    first, second, third = request.named_dq_requests
    target = (
        request.roots.evidence_root + "/readiness/exact_cash"
        if field == "evidence_root"
        else "/synthetic/another_root"
    )
    changed = replace(second, roots=replace(second.roots, **{field: target}))
    with pytest.raises(ValueError):
        replace(request, named_dq_requests=(first, changed, third))


@pytest.mark.parametrize("index", [0, 2])
@pytest.mark.parametrize("mutation", ["secondary", "rates", "tickers", "window", "as_of"])
def test_full_training_and_primary_requirements_cannot_be_reduced(
    index: int, mutation: str
) -> None:
    request = _request()
    rows = list(request.named_dq_requests)
    row = rows[index]
    changes = cast(
        dict[str, Any],
        {
            "secondary": {"require_secondary_prices": False, "input_roles": ("prices", "rates")},
            "rates": {"expected_rate_series": ("DGS2", "DGS10")},
            "tickers": {"expected_price_tickers": ("QQQ",)},
            "window": {"requested_window": DataQualityDateWindow(date(2022, 1, 3), _FEATURE)},
            "as_of": {"as_of": _EFFECTIVE},
        }[mutation],
    )
    rows[index] = replace(row, scope=replace(row.scope, **changes), expected_evaluated_window=None)
    with pytest.raises(ValueError):
        replace(request, named_dq_requests=tuple(rows))


def test_exact_cash_scope_cannot_drop_rates_or_move_sgov_start_to_primary() -> None:
    request = _request()
    first, cash, primary = request.named_dq_requests
    for changes in (
        {"expected_rate_series": ("DGS10",)},
        {"requested_window": DataQualityDateWindow(date(2021, 2, 22), _FEATURE)},
        {"require_secondary_prices": True, "input_roles": ("prices", "rates", "secondary_prices")},
    ):
        changed = replace(
            cash, scope=replace(cash.scope, **changes), expected_evaluated_window=None
        )
        with pytest.raises(ValueError):
            replace(request, named_dq_requests=(first, changed, primary))


def test_nested_json_cannot_smuggle_an_unreviewed_scope_option() -> None:
    raw = cast(dict[str, Any], _request().to_dict())
    raw["named_dq_requests"][0]["scope"]["allow_stale_rates"] = True
    with pytest.raises(ValueError):
        ComposerCaptureRequest.from_dict(raw)


@pytest.mark.parametrize(
    "field,value",
    [
        ("candidate_commit", "2" * 40),
        ("source_manifest_sha256", "b" * 64),
        ("source_manifest_path", FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH),
        ("policy_path", "config/research/prospective_capture_execution_v2.yaml"),
        ("operation", "maturity"),
        ("feature_session", _EFFECTIVE),
    ],
)
def test_request_cannot_drift_from_exact_manifest(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        replace(_request(), **{field: cast(Any, value)})


@pytest.mark.parametrize(
    "changes",
    [
        {"root_role": "PUBLICATION"},
        {"relative_path": "outputs/synthetic/another_review.json"},
        {"size_bytes": 0},
    ],
)
def test_owner_artifact_requires_exact_contained_nonempty_binding(changes: dict[str, Any]) -> None:
    request = _request()
    with pytest.raises(ValueError):
        replace(request, owner_review=replace(request.owner_review, **changes))


@pytest.mark.parametrize(
    "changes",
    [
        {"manifest_id": "another_composer_v1"},
        {"manifest_sha256": "b" * 64},
        {"reviewed_at": _DEADLINE},
        {"reviewed_at": _DEADLINE + timedelta(seconds=1)},
    ],
)
def test_owner_review_must_bind_exact_manifest_before_expiry(changes: dict[str, Any]) -> None:
    manifest = _manifest()
    _review(manifest).assert_manifest(manifest)
    with pytest.raises(ValueError):
        _review(manifest, **changes).assert_manifest(manifest)


@pytest.mark.parametrize(
    "changes",
    [
        {"authorization_state": "RETROSPECTIVELY_REVIEWED"},
        {"authorization_state": "UNAUTHORIZED_ACTION_INCIDENT"},
        {"decision_ref": "owner_instruction:TRADING-2560:old_implementation_only"},
        {"reviewed_at": _REVIEWED.replace(tzinfo=None)},
        {"cryptographic_owner_signature_claimed": True},
        {"future_observation_outcome_access_allowed": True},
        {"actor": "integration-coordinator"},
        {"risk_tier": "R3_PRODUCTION_OR_BROKER"},
    ],
)
def test_owner_review_cannot_upgrade_history_or_outcome_authority(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        _review(**changes)


def test_prospective_owner_scope_cannot_accept_a_synthetic_review() -> None:
    manifest = _manifest()
    research_output = "outputs/research/composer_prospective/" + manifest.manifest_id
    research = replace(
        manifest,
        evidence_purpose="PROSPECTIVE_RESEARCH",
        output_relative_path=research_output,
        roots=replace(
            manifest.roots,
            evidence_root=manifest.roots.execution_root + "/" + research_output + "/dq",
        ),
    )
    with pytest.raises(ValueError):
        _review(research)
    _review(research, decision_ref=_OWNER_DECISION).assert_manifest(research)
    with pytest.raises(ValueError):
        _review(manifest).assert_manifest(research)


def test_review_and_expiry_require_explicit_utc_normalization_without_backdating() -> None:
    zone = timezone(timedelta(hours=9))
    with pytest.raises(ValueError):
        _manifest(expires_at=_DEADLINE.astimezone(zone))
    with pytest.raises(ValueError):
        _review(reviewed_at=_REVIEWED.astimezone(zone))
    manifest = _manifest(expires_at=_DEADLINE.astimezone(zone).astimezone(UTC))
    review = _review(manifest, reviewed_at=_REVIEWED.astimezone(zone).astimezone(UTC))
    assert manifest.expires_at == _DEADLINE and manifest.expires_at.tzinfo is UTC
    assert review.reviewed_at == _REVIEWED and review.reviewed_at.tzinfo is UTC


@pytest.mark.parametrize("operation", ["activate", "capture", "readiness"])
def test_clock_prefixes_preserve_original_child_order(operation: str) -> None:
    clock, returns = _clock(operation)
    validate_composer_clock_prefix(clock, returns, operation)
    for length in range(len(clock.checkpoints) + 1):
        prefix = HostClockEvidence(clock.provider, clock.anchor, clock.checkpoints[:length])
        count = sum(row.label in {"inputs_return", "witness_return"} for row in prefix.checkpoints)
        validate_composer_clock_prefix(prefix, returns[:count], operation)


def test_ack_requires_complete_clock_even_when_the_failure_prefix_is_valid() -> None:
    ack = _ack()
    prefix = HostClockEvidence(
        ack.clock_evidence.provider, ack.clock_evidence.anchor, ack.clock_evidence.checkpoints[:-1]
    )
    validate_composer_clock_prefix(prefix, ack.recorder_returns, "capture")
    with pytest.raises(ValueError):
        replace(ack, clock_evidence=prefix)


def test_ack_keeps_coarse_utc_clock_valid_with_independent_counter_progress() -> None:
    clock, returns = _clock(counter_step_ns=1_000_000, raw_increment_ns=0)
    ack = _ack(clock_evidence=clock, recorder_returns=returns, recorder_event=returns[-1].event)
    assert clock.anchor.utc_ns == clock.latest_sample.utc_ns
    assert clock.latest_sample.counter_after_ns > clock.anchor.counter_before_ns
    assert ack.technical_validation_state == "CAPTURE_ACKNOWLEDGED"


@pytest.mark.parametrize(
    "offset_us,status", [(-1, "CAPTURE_ACKNOWLEDGED"), (0, "LATE"), (1, "LATE")]
)
def test_ack_deadline_uses_strict_original_bound_including_endpoint_quantization(
    offset_us: int, status: str
) -> None:
    clock, returns = _clock(
        start=_DEADLINE - timedelta(microseconds=2) + timedelta(microseconds=offset_us),
        counter_step_ns=0,
        raw_increment_ns=0,
        resolution="1e-06",
    )
    assert clock.admission_bound_ns == datetime_to_utc_ns(_DEADLINE) + offset_us * 1000
    assert deadline_allows(clock.admission_bound_ns, _DEADLINE) is (status != "LATE")
    ack = _ack(
        clock_evidence=clock,
        recorder_returns=returns,
        recorder_event=returns[-1].event,
        technical_validation_state=status,
    )
    with pytest.raises(ValueError):
        replace(
            ack, technical_validation_state="LATE" if status != "LATE" else "CAPTURE_ACKNOWLEDGED"
        )


@pytest.mark.parametrize("value", [None, True, "caller clock", {}, 123])
def test_ack_rejects_arbitrary_clock_scalars_and_mappings(value: object) -> None:
    with pytest.raises(ValueError):
        _ack(clock_evidence=value)


@pytest.mark.parametrize("mutation", ["missing", "swapped", "duplicate", "wrong_final", "list"])
def test_ack_rejects_missing_reordered_duplicate_or_wrong_recorder_returns(mutation: str) -> None:
    ack = _ack()
    returns = ack.recorder_returns
    changes = cast(
        dict[str, Any],
        {
            "missing": {"recorder_returns": returns[:1]},
            "swapped": {"recorder_returns": tuple(reversed(returns))},
            "duplicate": {"recorder_returns": (returns[0], returns[0])},
            "wrong_final": {"recorder_event": returns[0].event},
            "list": {"recorder_returns": list(returns)},
        }[mutation],
    )
    with pytest.raises(ValueError):
        replace(ack, **changes)


@pytest.mark.parametrize("mutation", ["stage", "foreign_bound", "child_bound", "child_provider"])
def test_ack_rejects_valid_host_evidence_with_wrong_composer_causality(mutation: str) -> None:
    ack = _ack()
    clock = ack.clock_evidence
    rebuilt = HostClockEvidence(clock.provider, clock.anchor)
    for checkpoint in clock.checkpoints:
        label = checkpoint.label
        inherited = checkpoint.inherited_child_bound_ns
        if mutation == "stage" and label == "fit_complete":
            label = "preview_complete"
        elif mutation == "foreign_bound" and label == "pre_dispatch":
            inherited = clock.anchor.utc_ns
        elif mutation == "child_bound" and label == "inputs_return":
            assert inherited is not None
            inherited += 1
        rebuilt = append_clock_checkpoint(
            rebuilt, label=label, sample=checkpoint.sample, inherited_child_bound_ns=inherited
        )
    returns = ack.recorder_returns
    if mutation == "child_provider":
        first = returns[0]
        provider = replace(first.clock_evidence.provider, python_version="3.11.10")
        child = replace(first.clock_evidence, provider=provider)
        returns = (replace(first, clock_evidence=child), returns[1])
    with pytest.raises(ValueError):
        replace(ack, clock_evidence=rebuilt, recorder_returns=returns)


@pytest.mark.parametrize(
    "changes",
    [
        {"operation": "readiness"},
        {"request_id": "composer_capture_request_" + "b" * 64},
        {"candidate_commit": "main"},
        {"source_hold_id": "hold-unknown"},
        {"source_hold_id": "lease-" + "1" * 20},
        {"first_feature_session": _EFFECTIVE},
        {"decision_effective_session": _FEATURE},
        {"feature_session": None},
        {"decision_deadline": None},
        {"authorization_state": "RETROSPECTIVELY_REVIEWED"},
        {"future_observation_outcome_access_allowed": True},
        {"historical_provider_available_at": "PROVEN"},
        {"acknowledgement_own_durability_time_claimed": True},
        {"production_effect": "paper"},
        {"broker_action": "order"},
    ],
)
def test_ack_cannot_invent_identity_activation_timing_or_research_authority(
    changes: dict[str, Any],
) -> None:
    with pytest.raises(ValueError):
        replace(_ack(), **changes)


def test_activation_ack_cannot_claim_a_feature_signal_or_return_clock() -> None:
    ack = _ack("activate")
    for changes in (
        {"feature_session": _FEATURE},
        {"decision_effective_session": _EFFECTIVE},
        {"decision_deadline": _DEADLINE},
        {"technical_validation_state": "CAPTURE_ACKNOWLEDGED"},
    ):
        with pytest.raises(ValueError):
            replace(ack, **changes)


def test_declaration_construction_does_not_read_write_launch_or_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("pure Composer declaration attempted I/O")

    monkeypatch.setattr(builtins, "open", forbidden)
    monkeypatch.setattr(io, "open", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    for kind in _DECLARATIONS:
        value = _declaration(kind)
        assert type(value).from_json_bytes(value.canonical_bytes) == value
