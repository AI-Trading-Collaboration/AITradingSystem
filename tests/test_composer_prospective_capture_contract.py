"""Composer declarations and candidate-bound seals; synthetic inputs, no model run."""

from __future__ import annotations

import builtins
import csv
import hashlib
import io
import json
import math
import shutil
import socket
import subprocess
from collections.abc import Callable
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

import named_data_quality_support as _named_support
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
    require_clock_evidence_extension,
    utc_ns_to_datetime_ceil,
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
    NamedDQExecutionReceipt,
    NamedDQExecutionRequest,
    NamedDQRoots,
    NamedDQSuccessfulDispatchBinding,
    NamedEqualRiskPriceScope,
    NamedSnapshotSelector,
    composer_dq_scope,
)
from ai_trading_system.contracts.prospective_capture_execution import RecorderReturnObservation
from ai_trading_system.data.named_quality_dispatch import CHILD_TIMEOUT_SECONDS

# Existing startup/guard allowance; add each nested child process budget below.
_ACTUAL_COMPOSER_ROUTE_OVERHEAD_SECONDS = 300


class _ComposerParentProof(Protocol):
    def __call__(
        self,
        *,
        request: ComposerCaptureRequest,
        transaction_input: str,
        lease_id: str,
    ) -> dict[str, Any]: ...


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
            else _READINESS if operation == "readiness" else _FEATURE
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
        "source_lease_id": "lease-" + "1" * 20,
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
        "named_composer_prospective_sources_v1.json"
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
        {"source_lease_id": "lease-unknown"},
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


# Actual-candidate tests below use the existing parent association protocol and
# pytest tmp-path retention. Only generated synthetic publication/receipts live
# below tmp_path; parent evidence remains in the existing validation resource.
# They require the coordinator's committed clean source, publication transaction
# and active lease. Missing authority fails closed; these tests never self-skip,
# create another checkout, fabricate a seal, or read a real market cache.
_SEALED_AS_OF = date(2026, 9, 3)
_SEALED_START = date(2018, 1, 2)

# A finite test-owned verifier probe. The support module records these exact
# bytes' SHA256 in its normal parent evidence. Selecting this TEST script never
# replaces a production constant, source manifest, clock, verifier or context.
_COMPOSER_SEALED_INPUTS_PROBE = r"""
import argparse
import hashlib
import importlib
import json
import os
import pickle
import runpy
import sys
from dataclasses import replace
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("bootstrap", type=Path)
parser.add_argument("--request", required=True, type=Path)
parser.add_argument("--request-sha256", required=True)
parser.add_argument("--source-lease-id", required=True)
parser.add_argument("--operation", choices=("verify",), required=True)
parser.add_argument("--receipt-path", required=True)
parser.add_argument("--receipt-sha256", required=True)
parser.add_argument("--run-dispatch-path", required=True)
parser.add_argument("--run-dispatch-sha256", required=True)
args = parser.parse_args()
session = None
try:
    if not sys.flags.isolated:
        raise ValueError("COMPOSER_TEST_PROBE_ISOLATED_CHILD_REQUIRED")
    entry = runpy.run_path(str(args.bootstrap))
    raw = entry["_initial_file_bytes"](args.request.parent, args.request.name)
    if hashlib.sha256(raw).hexdigest() != args.request_sha256:
        raise ValueError("COMPOSER_TEST_PROBE_REQUEST_SHA_MISMATCH")
    request = entry["_json_object"](raw)
    session = entry["NamedBootstrapSession"](
        request, operation="verify", source_lease_id=args.source_lease_id
    )
    session.load()
    contracts = importlib.import_module(
        "ai_trading_system.contracts.named_data_quality_execution"
    )
    typed = contracts.NamedDQExecutionRequest.from_dict(request)
    worker = importlib.import_module(entry["WORKER_MODULE"])
    verified = worker.verify_named_data_quality_execution_receipt(
        typed,
        receipt_path=args.receipt_path,
        receipt_sha256=args.receipt_sha256,
        run_dispatch_path=args.run_dispatch_path,
        run_dispatch_sha256=args.run_dispatch_sha256,
        bootstrap=session,
    )
    receipt = verified.receipt
    checks = {}
    try:
        pickle.dumps(verified)
    except TypeError:
        checks["seal_not_serializable"] = True
    else:
        checks["seal_not_serializable"] = False
    try:
        verified.bytes_for("rates", required_scope=typed.scope)
    except ValueError as exc:
        checks["old_common_window_not_widened"] = "not covered" in str(exc)
    else:
        checks["old_common_window_not_widened"] = False
    registry = next(
        row for row in receipt.execution_dependencies
        if row.relative_path == contracts.EQUAL_RISK_PRICE_REGISTRY_PATH
    )
    old_scope = contracts.NamedEqualRiskPriceScope(
        typed.scope.as_of,
        contracts.DataQualityDateWindow(
            contracts.EQUAL_RISK_PRIMARY_START, typed.scope.as_of
        ),
        registry,
    )
    is_composer = typed.source_manifest_path == contracts.COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_PATH
    digests = {}
    if is_composer:
        segment = next(
            label for label in contracts.COMPOSER_SCOPE_ORDER
            if contracts.composer_dq_scope(as_of=typed.scope.as_of, segment=label) == typed.scope
        )
        policy_binding = next(
            row for row in receipt.execution_dependencies
            if row.relative_path == contracts.COMPOSER_INPUT_POLICY_PATH
        )
        scope = contracts.NamedComposerInputScope(typed.scope.as_of, segment, policy_binding)
        prices, rates, sessions, next_session = verified.inputs_for_composer_segment(
            required_scope=scope
        )
        digests = {
            "prices": hashlib.sha256(prices).hexdigest(),
            "rates": hashlib.sha256(rates).hexdigest(),
        }
        original_members = {row.role: row.member.sha256 for row in receipt.inputs}
        checks["original_price_rate_bytes"] = all(
            digest == original_members[role] for role, digest in digests.items()
        )
        checks["original_price_calendar"] = (
            sessions[0] == typed.scope.requested_window.start
            and sessions[-1] == typed.scope.as_of
            and next_session > typed.scope.as_of
        )
        invalid_scopes = {
            "policy_size": replace(
                scope, input_policy_binding=replace(
                    policy_binding, size_bytes=policy_binding.size_bytes + 1
                )
            ),
            "other_segment": replace(
                scope, segment="primary" if segment != "primary" else "training"
            ),
        }
        for label, invalid in invalid_scopes.items():
            try:
                verified.inputs_for_composer_segment(required_scope=invalid)
            except ValueError:
                checks[label + "_rejected"] = True
            else:
                checks[label + "_rejected"] = False
        for label, accessor in (
            ("equal_risk", verified.prices_for_equal_risk_preview),
            ("five_candidate", verified.inputs_for_five_candidate_preview),
            ("prospective_five", verified.inputs_for_prospective_five_candidate_preview),
        ):
            try:
                accessor(required_scope=old_scope)
            except ValueError:
                checks[label + "_profile_not_relabelled"] = True
            else:
                checks[label + "_profile_not_relabelled"] = False
        closure = dict(verified.recording_closure_for_composer())
        closure_manifest = json.loads(closure["closure_manifest"])
        checks["rates_explicit_current_known_role"] = (
            closure_manifest["schema_version"] == "composer_verified_input_closure.v1"
            and closure_manifest["rates_feature_access_granted"] is True
            and next(
                row["semantic_role"] for row in closure_manifest["members"]
                if row["role"] == "input_rates"
            ) == "CURRENT_KNOWN_RATE_FEATURE"
        )
    else:
        segment = "legacy_primary"
        scope = contracts.NamedComposerInputScope(
            typed.scope.as_of, "primary",
            contracts.NamedArtifactBinding(
                "EXECUTION", contracts.COMPOSER_INPUT_POLICY_PATH,
                contracts.COMPOSER_INPUT_POLICY_SHA256, 1,
            ),
        )
        try:
            verified.inputs_for_composer_segment(required_scope=scope)
        except ValueError as exc:
            checks["legacy_profile_cannot_grant_composer_rates"] = (
                "separate reviewed exact source manifest" in str(exc)
            )
        else:
            checks["legacy_profile_cannot_grant_composer_rates"] = False
        old_values = verified.inputs_for_prospective_five_candidate_preview(
            required_scope=old_scope
        )
        checks["old_accessor_has_only_price_and_registry_bytes"] = (
            len(old_values) == 4
            and hashlib.sha256(old_values[0]).hexdigest()
            == next(row.member.sha256 for row in receipt.inputs if row.role == "prices")
            and hashlib.sha256(old_values[1]).hexdigest() == registry.sha256
        )
        closure = dict(verified.recording_closure_for_prospective())
        closure_manifest = json.loads(closure["closure_manifest"])
        checks["legacy_rates_remain_guard_only"] = (
            closure_manifest["rates_feature_access_granted"] is False
            and next(
                row["semantic_role"] for row in closure_manifest["members"]
                if row["role"] == "input_rates"
            ) == "DQ_GUARD_ONLY"
        )
    checks["historical_pit_not_claimed"] = (
        closure_manifest["provider_available_at_status"] == "NOT_ESTABLISHED"
    )
    result = {
        "schema_version": "named_data_quality_bootstrap_result.v1",
        "profile": "COMPOSER_FIXED_SEAL_TEST_PROBE",
        "status": "PASS",
        "request_id": typed.request_id,
        "process_id": os.getpid(),
        "source_lease_id": session.source_lease_id,
        "receipt_id": receipt.receipt_id,
        "original_dq_pid": receipt.execution_observation.execution_pid,
        "verifier_pid": verified.verifier_pid,
        "execution_identity_sha256": session.context.identity.stable_identity_sha256,
        "canonical_dq_call_count": session.canonical_dq_call_count,
        "segment": segment,
        "requested_window": typed.scope.requested_window.to_dict(),
        "evaluated_window": receipt.evaluated_window.to_dict(),
        "input_sha256": digests,
        "policy_sha256": receipt.policy.sha256,
        "provider_available_at_status": closure_manifest["provider_available_at_status"],
        "aggregate_readiness_claimed": False,
        "historical_primary_pit_claimed": False,
        "verified_input_seal_exported": False,
        "dispatch_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    if session.canonical_dq_call_count != 0:
        raise ValueError("COMPOSER_TEST_PROBE_DQ_DISPATCH_FORBIDDEN")
    session.assert_execution_unchanged(stage="TERMINAL")
    result["child_started_at"] = session.started_at
    result["child_terminal_checked_at"] = session.terminal_checked_at
    session.close()
    try:
        verified.inputs_for_composer_segment(required_scope=scope)
    except session.context_module.NamedExecutionContextError as exc:
        checks["closed_context_accessor_rejected"] = exc.code == "NAMED_CONTEXT_REQUIRED"
    else:
        checks["closed_context_accessor_rejected"] = False
    if not all(checks.values()):
        raise ValueError("COMPOSER_TEST_PROBE_BOUNDARY_FAILED: " + repr(checks))
    result["probe_checks"] = checks
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
except (ValueError, OSError, ImportError, RuntimeError, SyntaxError, TypeError) as exc:
    print(json.dumps({
        "schema_version": "named_data_quality_bootstrap_result.v1",
        "profile": "COMPOSER_FIXED_SEAL_TEST_PROBE",
        "status": "BLOCKED",
        "reason_code": getattr(exc, "code", "COMPOSER_TEST_PROBE_FAILED"),
        "detail": str(exc),
        "canonical_dq_call_count": 0 if session is None else session.canonical_dq_call_count,
        "verified_input_seal_exported": False,
        "dispatch_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }, ensure_ascii=False, sort_keys=True))
    raise SystemExit(2)
finally:
    if session is not None:
        session.close()
"""


def _actual_composer_publication(
    tmp_path: Path, *, case: str = "normal"
) -> _named_support.NamedExecutionFixture:
    from ai_trading_system.data.download_publication import (
        DownloadArtifactCandidate,
        DownloadSourceBinding,
        publish_download_transaction,
    )
    from ai_trading_system.trading_calendar import is_us_equity_trading_day

    days = []
    day = _SEALED_START
    while day <= _SEALED_AS_OF:
        if is_us_equity_trading_day(day):
            days.append(day)
        day += timedelta(days=1)
    prices = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary = [prices[0]]
    for index, day in enumerate(days):
        for ticker in ("QQQ", "TQQQ", "SHY", "SGOV"):
            if ticker == "SGOV" and day < date(2020, 5, 28):
                continue
            if case == "missing_training_price" and ticker == "SHY" and day == _SEALED_START:
                continue
            amplitude = 0.0001 if ticker in {"SHY", "SGOV"} else 0.005
            value = 100 * math.exp(0.0001 * index + amplitude * math.sin(index / 7))
            prices.append(
                f"{day},{ticker},{value},{value * 1.01},{value * 0.99}," f"{value},{value},1000\n"
            )
            comparison = (
                value * 1.25
                if case == "secondary_conflict" and day == _SEALED_AS_OF and ticker == "QQQ"
                else value
            )
            secondary.append(
                f"{day},{ticker},{comparison},{comparison * 1.01},{comparison * 0.99},"
                f"{comparison},{comparison},1000\n"
            )
    rates = ["date,series,value\n"]
    rate_end = _SEALED_AS_OF - timedelta(days=15) if case == "stale_rates" else days[-2]
    for day in days:
        if day <= rate_end:
            for series in ("DGS2", "DGS10", "DTWEXBGS"):
                rates.append(f"{day},{series},{110 if series == 'DTWEXBGS' else 1.5}\n")
    inputs = {
        "prices": "".join(prices).encode(),
        "rates": "".join(rates).encode(),
        "secondary_prices": "".join(secondary).encode(),
    }
    source_root = tmp_path / "synthetic-source"
    source_output = source_root / "data/raw"
    publication_root = tmp_path / "synthetic-publication"
    evidence_root = tmp_path / "synthetic-evidence"
    evidence_root.mkdir(parents=True)
    artifacts, bindings = [], []
    for role, content in inputs.items():
        records = list(csv.DictReader(io.StringIO(content.decode())))
        dimension = "series" if role == "rates" else "ticker"
        event_id = "synthetic-composer:" + role
        artifacts.append(
            DownloadArtifactCandidate(
                role=role,
                filename=(
                    "prices_marketstack_daily.csv"
                    if role == "secondary_prices"
                    else role + "_daily.csv"
                ),
                content=content,
                row_count=len(records),
                source_event_ids=(event_id,),
            )
        )
        bindings.append(
            DownloadSourceBinding(
                source_event_id=event_id,
                artifact_role=role,
                source_kind="LIVE_PROVIDER",  # Existing publisher fixture enum; no network call.
                source_id=event_id,
                provider="SYNTHETIC_FIXTURE",
                endpoint="memory:" + event_id,
                request_parameters={"synthetic_only": True, "network_request_count": 0},
                winning_row_count=len(records),
                allocation_mode="REMAINDER",
                winning_row_keys=tuple(sorted((row[dimension], row["date"]) for row in records)),
            )
        )
    publication = publish_download_transaction(
        output_dir=source_output,
        requested_start=_SEALED_START,
        requested_end=_SEALED_AS_OF,
        published_at=datetime(2026, 9, 3, 21, tzinfo=UTC),
        artifacts=tuple(artifacts),
        source_bindings=tuple(bindings),
    )
    pointer = json.loads(publication.discovery_pointer_path.read_bytes())
    # Both sides are freshly created, test-owned paths; no execution source or
    # real cache is copied. Pytest retains failed fixtures for diagnosis normally.
    shutil.copytree(source_output, publication_root)
    request = NamedDQExecutionRequest(
        roots=NamedDQRoots(
            source_root.as_posix(),
            publication_root.as_posix(),
            _named_support.ROOT.as_posix(),
            evidence_root.as_posix(),
        ),
        selector=NamedSnapshotSelector(
            pointer["pointer_id"],
            publication.discovery_pointer_sha256,
            publication.transaction_id,
            publication.transaction_manifest_sha256,
        ),
        scope=composer_dq_scope(as_of=_SEALED_AS_OF, segment="training"),
        source_output_relative_path="data/raw",
        policy_path="config/data_quality.yaml",
        execution_profile_id="manual.v1",
        candidate_commit=_named_support._git(_named_support.ROOT, "rev-parse", "HEAD"),
        source_manifest_path=COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_PATH,
        source_manifest_sha256=COMPOSER_PROSPECTIVE_SOURCE_MANIFEST_SHA256,
        expected_evaluated_window=(
            DataQualityDateWindow(_SEALED_START, days[-2]) if case == "normal" else None
        ),
    )
    return _named_support.NamedExecutionFixture(
        request, source_root, publication_root, evidence_root, publication, inputs
    )


def _assert_actual_parent(result: _named_support.ParentDispatchResult, *, calls: int) -> None:
    parent = result.parent_receipt
    assert parent["status"] == "PASS" and parent["terminal_state"] == "EXITED"
    assert parent["failure"] is None
    assert parent["observed_canonical_dq_call_count"] == calls
    assert parent["synthetic_inputs_only"] is True
    assert parent["real_market_dq_call_count"] == 0
    assert parent["source_checkout_copied"] is False
    assert parent["lease_acquired_or_mutated"] is False
    assert parent["verified_input_seal_exported"] is False
    assert parent["production_effect"] == parent["broker_action"] == "none"
    for role in ("pre_dispatch_proof", "post_dispatch_proof"):
        binding = parent[role]
        content = Path(binding["path"]).read_bytes()
        assert hashlib.sha256(content).hexdigest() == binding["sha256"]
        proof = json.loads(content)
        assert proof["status"] == proof["fence_binding"]["status"] == "PASS"
        assert proof["candidate_commit"] == parent["candidate_commit"]
        assert proof["active_lease"]["lease_id"] == parent["source_lease_id"]


def _actual_composer_probe(
    fixture: _named_support.NamedExecutionFixture,
    run: _named_support.ParentDispatchResult,
    monkeypatch: pytest.MonkeyPatch,
) -> _named_support.ParentDispatchResult:
    with monkeypatch.context() as select_test_probe:
        select_test_probe.setattr(
            _named_support, "_SEALED_INPUTS_TEST_PROBE", _COMPOSER_SEALED_INPUTS_PROBE
        )
        result = _named_support.dispatch_actual_candidate_child(
            fixture, operation="verify", successful_run=run, test_probe=True
        )
    assert result.returncode == 0, result.child_result
    _assert_actual_parent(result, calls=0)
    assert (
        result.parent_receipt["fixed_test_probe_sha256"]
        == hashlib.sha256(_COMPOSER_SEALED_INPUTS_PROBE.encode()).hexdigest()
    )
    assert all(result.child_result["probe_checks"].values())
    return result


@pytest.mark.integration
def test_actual_composer_three_segment_seals_keep_lagged_rates_and_distinct_window_roles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _actual_composer_publication(tmp_path)
    original_publication = {
        path.relative_to(fixture.publication_root)
        .as_posix(): hashlib.sha256(path.read_bytes())
        .hexdigest()
        for path in fixture.publication_root.rglob("*")
        if path.is_file()
    }
    seen_receipts, digests = [], []
    policy_sha = hashlib.sha256(
        (_named_support.ROOT / "config/data_quality.yaml").read_bytes()
    ).hexdigest()
    for segment in _SEGMENTS:
        scope = composer_dq_scope(as_of=_SEALED_AS_OF, segment=segment)
        evidence_root = fixture.evidence_root / segment
        evidence_root.mkdir()
        request = replace(
            fixture.request,
            scope=scope,
            roots=replace(fixture.request.roots, evidence_root=evidence_root.as_posix()),
            expected_evaluated_window=DataQualityDateWindow(
                scope.requested_window.start, date(2026, 9, 2)
            ),
        )
        selected = replace(fixture, request=request, evidence_root=evidence_root)
        run = _named_support.dispatch_actual_candidate_child(selected)
        assert run.returncode == 0, run.child_result
        _assert_actual_parent(run, calls=1)
        result = _actual_composer_probe(selected, run, monkeypatch).child_result
        assert result["segment"] == segment
        assert result["requested_window"] == scope.requested_window.to_dict()
        assert (
            result["evaluated_window"]
            == cast(DataQualityDateWindow, request.expected_evaluated_window).to_dict()
        )
        assert result["policy_sha256"] == policy_sha
        assert result["aggregate_readiness_claimed"] is False
        assert result["historical_primary_pit_claimed"] is False
        assert result["provider_available_at_status"] == "NOT_ESTABLISHED"
        seen_receipts.append(result["receipt_id"])
        digests.append(result["input_sha256"])
    assert len(set(seen_receipts)) == 3
    assert digests[0] == digests[1] == digests[2]
    assert original_publication == {
        path.relative_to(fixture.publication_root)
        .as_posix(): hashlib.sha256(path.read_bytes())
        .hexdigest()
        for path in fixture.publication_root.rglob("*")
        if path.is_file()
    }


@pytest.mark.integration
@pytest.mark.parametrize("case", ["missing_training_price", "stale_rates", "secondary_conflict"])
def test_actual_composer_input_policy_does_not_weaken_canonical_dq(
    tmp_path: Path, case: str
) -> None:
    fixture = _actual_composer_publication(tmp_path, case=case)
    result = _named_support.dispatch_actual_candidate_child(fixture)
    _assert_actual_parent(result, calls=1)
    # Exit zero closes a completed DQ execution; report FAIL denies data readiness.
    assert result.returncode == 0, result.child_result
    assert result.child_result["status"] == "FAIL"
    assert result.child_result["dispatch_allowed"] is False
    assert result.child_result["verified_input_seal_exported"] is False
    assert result.run_dispatch_path is not None and result.run_dispatch_sha256 is not None
    content = (fixture.evidence_root / result.child_result["receipt_path"]).read_bytes()
    assert hashlib.sha256(content).hexdigest() == result.child_result["receipt_sha256"]
    receipt = NamedDQExecutionReceipt.from_json_bytes(content)
    dispatch_content = (_named_support.ROOT / result.run_dispatch_path).read_bytes()
    assert hashlib.sha256(dispatch_content).hexdigest() == result.run_dispatch_sha256
    dispatch = NamedDQSuccessfulDispatchBinding.from_json_bytes(dispatch_content)
    dispatch.assert_matches_receipt(receipt, receipt_path=result.child_result["receipt_path"])
    assert dispatch.proof_semantics == "TRUSTED_COORDINATOR_CORRELATION_ONLY"
    assert dispatch.dispatch_allowed is False
    assert (
        dispatch.parent_receipt.relative_path
        == result.parent_receipt_path.relative_to(_named_support.ROOT).as_posix()
    )
    parent_content = result.parent_receipt_path.read_bytes()
    assert hashlib.sha256(parent_content).hexdigest() == dispatch.parent_receipt.sha256
    assert len(parent_content) == dispatch.parent_receipt.size_bytes
    assert json.loads(parent_content) == result.parent_receipt
    expected_issue = {
        "missing_training_price": "prices_requested_window_coverage_missing",
        "stale_rates": "rates_stale",
        "secondary_conflict": "secondary_prices_close_mismatch",
    }[case]
    assert expected_issue in receipt.report.issue_codes
    assert receipt.report.status == "FAIL" and receipt.data_quality_evidence.ready is False
    assert (
        receipt.policy.sha256
        == hashlib.sha256(
            (_named_support.ROOT / "config/data_quality.yaml").read_bytes()
        ).hexdigest()
    )


@pytest.mark.integration
def test_actual_composer_rates_require_new_profile_and_old_preservation_stays_guard_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ai_trading_system.contracts.named_data_quality_execution import (
        PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
        PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
    )

    fixture = _actual_composer_publication(tmp_path)
    scope = composer_dq_scope(as_of=_SEALED_AS_OF, segment="primary")
    request = replace(
        fixture.request,
        scope=replace(scope, input_roles=("prices", "rates"), require_secondary_prices=False),
        source_manifest_path=PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
        source_manifest_sha256=PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
        expected_evaluated_window=DataQualityDateWindow(date(2021, 2, 22), date(2026, 9, 2)),
    )
    fixture = replace(fixture, request=request)
    run = _named_support.dispatch_actual_candidate_child(fixture)
    assert run.returncode == 0, run.child_result
    _assert_actual_parent(run, calls=1)
    result = _actual_composer_probe(fixture, run, monkeypatch).child_result
    assert result["segment"] == "legacy_primary"
    assert result["input_sha256"] == {}
    assert result["historical_primary_pit_claimed"] is False


def _actual_composer_route(
    request: ComposerCaptureRequest,
    *,
    output: Path,
    invocation: int,
) -> dict[str, Any]:
    """Use the existing real parent guards and hermetic production CLI launch.

    Unlike the seal-only probe, this path supplies no test code to the child and
    replaces no function, context, seal, clock, policy, or interpreter identity.
    """
    transaction, lease_id = _named_support._parent_environment()
    before = cast(_ComposerParentProof, _named_support._live_parent_proof)(
        request=request, transaction_input=transaction, lease_id=lease_id
    )
    request_path = output / "control" / (request.operation + "_request.json")
    assert request_path.read_bytes() == request.canonical_bytes
    launch = _named_support._current_child_python_launch()
    command = [
        launch.executable,
        "-I",
        "-B",
        "-X",
        "utf8",
        str(_named_support.ROOT / _named_support.BOOTSTRAP_PATH),
        "--operation",
        "composer-" + request.operation,
        "--request",
        str(request_path),
        "--request-sha256",
        request.canonical_sha256,
        "--source-lease-id",
        lease_id,
    ]
    started = datetime.now(UTC)
    terminal_state = "EXITED"
    with subprocess.Popen(
        command,
        cwd=_named_support.ROOT,
        env=launch.environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as process:
        spawned = datetime.now(UTC)
        try:
            # Three sequential children can exceed the prior 300-second total.
            # Keep each production child bound; budget startup and live audits too.
            stdout, stderr = process.communicate(
                timeout=_ACTUAL_COMPOSER_ROUTE_OVERHEAD_SECONDS
                + len(request.named_dq_requests) * CHILD_TIMEOUT_SECONDS
            )
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            terminal_state = "TIMED_OUT_AND_CHILD_REAPED"
        child_pid, returncode = process.pid, process.returncode
    terminal = datetime.now(UTC)
    # Preserve the original process bytes even if the later live guard fails.
    audit = output / "test_parent" / f"{request.operation}_{invocation}"
    stdout_binding = _named_support._write_new(audit / "stdout.json", stdout)
    stderr_binding = _named_support._write_new(audit / "stderr.txt", stderr)
    try:
        after = cast(_ComposerParentProof, _named_support._live_parent_proof)(
            request=request, transaction_input=transaction, lease_id=lease_id
        )
    except (ValueError, OSError, RuntimeError, KeyError) as exc:
        after = {
            "status": "BLOCKED",
            "checked_at": datetime.now(UTC).isoformat(),
            "detail": str(exc),
        }
    parent = {
        "schema_version": "composer_routes_actual_candidate_test_parent.v1",
        "candidate_commit": request.candidate_commit,
        "request_sha256": request.canonical_sha256,
        "operation": "composer-" + request.operation,
        "invocation": invocation,
        "child_pid": child_pid,
        "returncode": returncode,
        "terminal_state": terminal_state,
        "command": command,
        "launch_audit": launch.audit,
        "started_at": started.isoformat(),
        "spawn_observed_at": spawned.isoformat(),
        "terminal_at": terminal.isoformat(),
        "pre_guard": before,
        "post_guard": after,
        "stdout": stdout_binding,
        "stderr": stderr_binding,
        "test_probe_supplied": False,
        "source_clock_or_lease_replaced": False,
        "synthetic_inputs_only": True,
        "real_market_dq_call_count": 0,
        "production_effect": "none",
        "broker_action": "none",
    }
    _named_support._write_new(audit / "parent.json", _named_support._json_bytes(parent))
    assert after["status"] == before["status"] == "PASS", after
    assert before["request_sha256"] == after["request_sha256"] == request.canonical_sha256
    assert before["candidate_commit"] == after["candidate_commit"] == request.candidate_commit
    assert (
        datetime.fromisoformat(before["checked_at"])
        <= started
        <= spawned
        <= terminal
        <= datetime.fromisoformat(after["checked_at"])
    )
    assert terminal_state == "EXITED" and returncode == 0, (stdout, stderr)
    result = cast(dict[str, Any], json.loads(stdout))
    assert (
        started
        <= datetime.fromisoformat(result["child_started_at"])
        <= datetime.fromisoformat(result["child_terminal_checked_at"])
        <= terminal
    )
    assert result["source_lease_id"] == lease_id
    assert result["candidate_commit"] == request.candidate_commit
    if invocation == 0:
        assert result["parent_pid"] == child_pid
        assert result.get("idempotent_replay", False) is False
    else:
        assert result["idempotent_replay"] is True
        assert result["this_invocation_canonical_dq_call_count"] == 0
    return result


def _actual_composer_artifact_tree(output: Path) -> dict[str, str]:
    """Only original production artifact paths belonging to this synthetic run."""
    return {
        path.relative_to(output).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for name in ("activate", "readiness", "timing", "dq", "sessions")
        for path in (output / name).rglob("*")
        if path.is_file()
    }


@pytest.mark.integration
def test_actual_composer_activation_and_readiness_routes_use_real_guards_and_replay(
    tmp_path: Path,
) -> None:
    from ai_trading_system.prospective_event_time_evidence import (
        first_feature_session,
        load_time_evidence_policy,
        session_timing,
    )

    fixture = _actual_composer_publication(tmp_path)
    policy = load_time_evidence_policy(source_root=_named_support.ROOT)
    now = datetime.now(UTC)
    first = first_feature_session(now, policy=policy)
    timing = session_timing(first, policy=policy)
    manifest_id = "synthetic_composer_routes_" + uuid4().hex + "_v1"
    relative = _SYNTHETIC_PREFIX + manifest_id
    output = _named_support.ROOT / relative
    roots = replace(fixture.request.roots, evidence_root=(output / "dq").as_posix())
    manifest = ComposerCaptureManifest(
        manifest_id=manifest_id,
        roots=roots,
        candidate_commit=fixture.request.candidate_commit,
        output_relative_path=relative,
        source_output_relative_path="data/raw",
        readiness_as_of=_SEALED_AS_OF,
        allowed_feature_sessions=(first,),
        expires_at=timing.effective_close_at,
        evidence_purpose="SYNTHETIC_ENGINEERING",
    )
    review = ComposerOwnerReview(
        manifest_id=manifest_id,
        manifest_sha256=manifest.canonical_sha256,
        decision_ref="synthetic_review:TRADING-2560:actual-composer-routes",
        reviewed_at=now,
        authorization_state="STANDING_OWNER_SCOPE",
        evidence_purpose="SYNTHETIC_ENGINEERING",
    )
    owner_binding = NamedArtifactBinding(
        "EXECUTION",
        relative + "/control/owner_review.json",
        review.canonical_sha256,
        len(review.canonical_bytes),
    )
    activation = ComposerCaptureRequest(
        operation="activate",
        manifest=manifest,
        owner_review=owner_binding,
        roots=roots,
        candidate_commit=manifest.candidate_commit,
        source_manifest_path=manifest.source_manifest_path,
        source_manifest_sha256=manifest.source_manifest_sha256,
        policy_path=manifest.policy_path,
        feature_session=None,
        named_dq_requests=(),
    )
    children = []
    for segment in _SEGMENTS:
        scope = composer_dq_scope(as_of=_SEALED_AS_OF, segment=segment)
        children.append(
            replace(
                fixture.request,
                roots=replace(roots, evidence_root=roots.evidence_root + "/readiness/" + segment),
                scope=scope,
                expected_evaluated_window=DataQualityDateWindow(
                    scope.requested_window.start, date(2026, 9, 2)
                ),
            )
        )
    readiness = replace(
        activation,
        operation="readiness",
        feature_session=_SEALED_AS_OF,
        named_dq_requests=tuple(children),
    )
    transaction, lease_id = _named_support._parent_environment()
    cast(_ComposerParentProof, _named_support._live_parent_proof)(
        request=activation, transaction_input=transaction, lease_id=lease_id
    )
    assert not output.exists()
    task_output = _named_support.ROOT / "outputs/architecture/trading_2560_composer_known_snapshot"
    assert output.resolve().is_relative_to((task_output / "synthetic").resolve())
    _named_support._write_new(
        task_output / (manifest_id + "_lifecycle.json"),
        _named_support._json_bytes(
            {
                "schema_version": "temporary_workspace_lifecycle.v1",
                "task_id": manifest.task_id,
                "owner": "integration_coordinator",
                "purpose": (
                    "实际 Composer activation/readiness 路由合成测试；" "零模型与真实 observation。"
                ),
                "absolute_path": output.as_posix(),
                "candidate_commit": manifest.candidate_commit,
                "source_lease_id": lease_id,
                "registered_at": datetime.now(UTC).isoformat(),
                "preexisting_path": False,
                "exit_condition": (
                    "保留原始父终态、guards、clock、三段 DQ 与幂等回放证据；"
                    "协调方汇总验证结果并确认无进程依赖后按此精确路径清理。"
                ),
                "production_effect": "none",
                "broker_action": "none",
            }
        ),
    )
    _named_support._write_new(output / "control/owner_review.json", review.canonical_bytes)
    for request in (activation, readiness):
        _named_support._write_new(
            output / "control" / (request.operation + "_request.json"), request.canonical_bytes
        )
        original_tree = None
        original_result = None
        for invocation in range(2):
            result = _actual_composer_route(request, output=output, invocation=invocation)
            assert result["schema_version"] == "composer_prospective_capture_result.v1"
            assert result["status"] == (
                "ACTIVATED" if request.operation == "activate" else "READINESS_PASS"
            ), result
            assert result["parent_canonical_dq_call_count"] == 0
            assert result["model_fit_call_count"] == result["real_market_dq_call_count"] == 0
            assert result["historical_training_accessed"] is False
            assert result["capture_admitted"] is result["real_observation_admitted"] is False
            assert result["future_observation_outcome_accessed"] is False
            assert result["orders"] == result["fills"] == result["positions"] == 0
            assert result["provider_action_count"] == result["cache_mutation_count"] == 0
            prefix = HostClockEvidence.from_dict(result["parent_clock_evidence"])
            terminal = HostClockEvidence.from_dict(result["terminal_clock_evidence"])
            require_clock_evidence_extension(prefix, terminal)
            assert terminal.checkpoints[-1].label == "terminal_precommit"
            assert tuple(row.label for row in prefix.checkpoints) == CLOCK_STAGES[request.operation]
            assert result["terminal_precommit_proof"]["active_lease"]["lease_id"] == lease_id
            if request.operation == "activate":
                assert result["activation_admitted"] is True
                assert result["canonical_dq_call_count"] == 0
                assert result["dq_dispatches"] == []
                binding = NamedArtifactBinding.from_dict(result["acknowledgement"])
                content = (_named_support.ROOT / binding.relative_path).read_bytes()
                assert hashlib.sha256(content).hexdigest() == binding.sha256
                ack = ComposerCompletionAcknowledgement.from_json_bytes(content)
                assert ack.clock_evidence == prefix
                assert (
                    ack.first_feature_session
                    == first
                    == first_feature_session(
                        utc_ns_to_datetime_ceil(ack.clock_evidence.admission_bound_ns),
                        policy=policy,
                    )
                )
                assert len(ack.recorder_returns) == len(result["recorder_returns"]) == 1
                assert ack.acknowledgement_own_durability_time_claimed is False
            else:
                assert result["activation_admitted"] is False
                assert result["canonical_dq_call_count"] == 3
                assert result["technical_validation_state"] == "SEGMENTED_INPUT_DQ_PASS_NO_SIGNAL"
                assert result["recorder_returns"] == []
                rows = result["dq_dispatches"]
                assert [row["segment"] for row in rows] == list(_SEGMENTS)
                assert all(
                    row["status"] == "PASS"
                    and row["canonical_dq_call_count"] == 1
                    and row["counter_observation_state"] == "KNOWN"
                    and row["terminal_state"] == "EXITED"
                    for row in rows
                )
                binding = NamedArtifactBinding.from_dict(result["segmented_dq_identity"])
                content = (_named_support.ROOT / binding.relative_path).read_bytes()
                assert hashlib.sha256(content).hexdigest() == binding.sha256
                aggregate = json.loads(content)
                assert aggregate["scope_order"] == list(_SEGMENTS)
                assert aggregate["source_snapshot"] == fixture.request.selector.to_dict()
                assert aggregate["historical_pit_claim_allowed"] is False
                assert aggregate["historical_provider_available_at"] == "NOT_ESTABLISHED"
                assert (
                    aggregate["rates_content_sha256"]
                    == hashlib.sha256(fixture.input_bytes["rates"]).hexdigest()
                )
                for segment, row in zip(_SEGMENTS, aggregate["segments"], strict=True):
                    required = composer_dq_scope(as_of=_SEALED_AS_OF, segment=segment)
                    assert row["segment"] == segment and row["dq_status"] == "PASS"
                    assert row["requested_window"] == required.requested_window.to_dict()
                    assert (
                        row["evaluated_window"]
                        == DataQualityDateWindow(
                            required.requested_window.start, date(2026, 9, 2)
                        ).to_dict()
                    )
            current_tree = _actual_composer_artifact_tree(output)
            if invocation == 0:
                original_result, original_tree = result, current_tree
            else:
                assert original_result is not None and original_tree is not None
                assert result["parent_pid"] == original_result["parent_pid"]
                assert current_tree == original_tree
                assert result["dq_dispatches"] == original_result["dq_dispatches"]
            assert not (output / "sessions").exists()
