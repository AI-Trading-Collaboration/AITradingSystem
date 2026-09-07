"""Pure S3b declarations only; no live source, authorization, calendar or DQ proof."""

from __future__ import annotations

import builtins
import hashlib
import io
import json
import socket
import subprocess
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system import trading_calendar
from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.contracts.named_data_quality_execution import (
    EQUAL_RISK_GUARD_RATE_SERIES,
    EQUAL_RISK_PRICE_SOURCE_MANIFEST_PATH,
    EQUAL_RISK_PRICE_SOURCE_MANIFEST_SHA256,
    EQUAL_RISK_PRICE_TICKERS,
    EQUAL_RISK_PRIMARY_START,
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256,
    PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
    PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
    NamedArtifactBinding,
    NamedDQExecutionRequest,
    NamedDQRoots,
    NamedSnapshotSelector,
)
from ai_trading_system.contracts.prospective_capture_execution import (
    CAPTURE_POLICY_PATH,
    CAPTURE_RETURN_CLOCK,
    CAPTURE_TASK_ID,
    CAPTURE_TIMING_VERSION,
    ParentCompletionAcknowledgement,
    ProspectiveCaptureManifest,
    ProspectiveCaptureOwnerReview,
    ProspectiveCaptureRequest,
)
from ai_trading_system.contracts.prospective_event_time_evidence import canonical_json_bytes

_COMMIT = "1" * 40
_SHA = "a" * 64
_FEATURE = date(2026, 11, 27)
_NEXT_FEATURE = date(2026, 11, 30)
_REVIEWED = datetime(2026, 11, 25, 20, tzinfo=UTC)
_EXPIRES = datetime(2026, 12, 2, tzinfo=UTC)
_DEADLINE = datetime(2026, 11, 30, 21, tzinfo=UTC)
_SYNTHETIC_PREFIX = "outputs/architecture/trading_2564_s3b_prospective_capture/synthetic/"
_RESEARCH_PREFIX = "outputs/research/prospective_capture/"


def _manifest(
    *,
    purpose: str = "SYNTHETIC_ENGINEERING",
    manifest_id: str = "synthetic_capture_v1",
    **changes: Any,
) -> ProspectiveCaptureManifest:
    prefix = _SYNTHETIC_PREFIX if purpose == "SYNTHETIC_ENGINEERING" else _RESEARCH_PREFIX
    output = prefix + manifest_id
    values: dict[str, Any] = {
        "manifest_id": manifest_id,
        "roots": NamedDQRoots(
            source_root="/synthetic/source",
            publication_root="/synthetic/publication",
            execution_root="/synthetic/execution",
            evidence_root="/synthetic/execution/" + output + "/dq",
        ),
        "candidate_commit": _COMMIT,
        "source_output_relative_path": "outputs/synthetic_source",
        "output_relative_path": output,
        "allowed_feature_sessions": (_FEATURE, _NEXT_FEATURE),
        "expires_at": _EXPIRES,
        "evidence_purpose": purpose,
        "require_secondary_prices": False,
    }
    values.update(changes)
    return ProspectiveCaptureManifest(**values)


def _review(
    manifest: ProspectiveCaptureManifest | None = None, **changes: Any
) -> ProspectiveCaptureOwnerReview:
    manifest = manifest or _manifest()
    prefix = (
        "synthetic_review:TRADING-2564:"
        if manifest.evidence_purpose == "SYNTHETIC_ENGINEERING"
        else "owner_decision:TRADING-2564:"
    )
    values: dict[str, Any] = {
        "manifest_id": manifest.manifest_id,
        "manifest_sha256": manifest.canonical_sha256,
        "decision_ref": prefix + "synthetic_scope_review",
        "reviewed_at": _REVIEWED,
        "authorization_state": "STANDING_OWNER_SCOPE",
        "evidence_purpose": manifest.evidence_purpose,
    }
    values.update(changes)
    return ProspectiveCaptureOwnerReview(**values)


def _named_request(
    manifest: ProspectiveCaptureManifest, *, feature: date = _FEATURE, **changes: Any
) -> NamedDQExecutionRequest:
    values: dict[str, Any] = {
        "roots": manifest.roots,
        "selector": NamedSnapshotSelector("synthetic_pointer", _SHA, "synthetic_transaction", _SHA),
        "scope": manifest.dq_scope(feature),
        "source_output_relative_path": manifest.source_output_relative_path,
        "policy_path": "config/data_quality.yaml",
        "execution_profile_id": "manual.v1",
        "candidate_commit": manifest.candidate_commit,
        "source_manifest_path": manifest.source_manifest_path,
        "source_manifest_sha256": manifest.source_manifest_sha256,
        # Guard-only rates may legitimately end one session before the consumed
        # prices. The parent envelope must preserve that original DQ assertion.
        "expected_evaluated_window": DataQualityDateWindow(
            EQUAL_RISK_PRIMARY_START, date(2026, 11, 25)
        ),
    }
    values.update(changes)
    return NamedDQExecutionRequest(**values)


def _request(
    operation: str = "capture",
    *,
    manifest: ProspectiveCaptureManifest | None = None,
    **changes: Any,
) -> ProspectiveCaptureRequest:
    manifest = manifest or _manifest()
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
        "feature_session": None if operation == "activate" else _FEATURE,
        "named_dq_request": None if operation == "activate" else _named_request(manifest),
    }
    values.update(changes)
    return ProspectiveCaptureRequest(**values)


def _ack(operation: str = "capture", **changes: Any) -> ParentCompletionAcknowledgement:
    request = _request(operation)
    start = (
        datetime(2026, 11, 25, 21, 59, 59, tzinfo=UTC)
        if operation == "activate"
        else datetime(2026, 11, 27, 18, 0, 1, tzinfo=UTC)
    )
    values: dict[str, Any] = {
        "request_id": request.request_id,
        "request_sha256": request.canonical_sha256,
        "manifest_id": request.manifest.manifest_id,
        "manifest_sha256": request.manifest.canonical_sha256,
        "operation": operation,
        "feature_session": request.feature_session,
        "candidate_commit": request.candidate_commit,
        "execution_identity_sha256": _SHA,
        "parent_pid": 123,
        "source_lease_id": "lease-" + "a" * 20,
        "recorder_event": NamedArtifactBinding(
            "EXECUTION", request.timing_relative_path + "/synthetic_event.json", _SHA, 1
        ),
        "recording_call_started_at": start,
        "witness_bundle_observed_at": start + timedelta(seconds=1),
        "monotonic_elapsed_ns": 1_000_000_000,
        "first_feature_session": _FEATURE,
        "decision_effective_session": None if operation == "activate" else _NEXT_FEATURE,
        "decision_deadline": None if operation == "activate" else _DEADLINE,
        "authorization_state": "STANDING_OWNER_SCOPE",
        "evidence_purpose": "SYNTHETIC_ENGINEERING",
        "technical_validation_state": (
            "ACTIVATION_ACKNOWLEDGED" if operation == "activate" else "CAPTURE_ACKNOWLEDGED"
        ),
    }
    values.update(changes)
    return ParentCompletionAcknowledgement(**values)


def _declaration(kind: str) -> Any:
    return {
        "manifest": _manifest,
        "review": _review,
        "activation": lambda: _request("activate"),
        "capture": _request,
        "activation_ack": lambda: _ack("activate"),
        "capture_ack": _ack,
    }[kind]()


_KINDS = ("manifest", "review", "activation", "capture", "activation_ack", "capture_ack")


@pytest.mark.parametrize("kind", _KINDS)
def test_complete_declarations_round_trip_canonically_and_remain_immutable(kind: str) -> None:
    value = _declaration(kind)
    assert type(value).from_dict(value.to_dict()) == value
    assert type(value).from_json_bytes(value.canonical_bytes) == value
    assert value.canonical_bytes == canonical_json_bytes(value.to_dict())
    assert value.canonical_sha256 == hashlib.sha256(value.canonical_bytes).hexdigest()
    with pytest.raises(FrozenInstanceError):
        value.production_effect = "enabled"


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("damage", ("unknown", "missing", "schema", "integer_as_boolean"))
def test_strict_field_inventory_and_types_cannot_add_authority(kind: str, damage: str) -> None:
    value = _declaration(kind)
    payload = value.to_dict()
    if damage == "unknown":
        payload["execution_authorized"] = True
    elif damage == "missing":
        del payload[next(key for key in payload if key != "schema_version")]
    elif damage == "schema":
        payload["schema_version"] = "other_capture.v1"
    else:
        # Every envelope has either a direct or nested safety boolean.
        target = payload["manifest"] if kind in {"activation", "capture"} else payload
        field = next(key for key, item in target.items() if type(item) is bool)
        target[field] = int(target[field])
    with pytest.raises(ValueError):
        type(value).from_dict(payload)


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("damage", ("pretty", "trailing", "duplicate", "mutable", "not_object"))
def test_json_input_must_be_unambiguous_immutable_and_canonical(kind: str, damage: str) -> None:
    value = _declaration(kind)
    content: Any = value.canonical_bytes
    if damage == "pretty":
        content = json.dumps(value.to_dict(), indent=2).encode()
    elif damage == "trailing":
        content += b" trailing"
    elif damage == "duplicate":
        content = b'{"schema_version":"duplicate",' + content[1:]
    elif damage == "mutable":
        content = bytearray(content)
    else:
        content = b"[]"
    with pytest.raises(ValueError):
        type(value).from_json_bytes(content)


@pytest.mark.parametrize(
    "content", (b"\xff", b'{"value":NaN}', b'{"value":1e999}', b'{"x":"\\ud800"}')
)
def test_manifest_rejects_invalid_utf8_or_nonfinite_json(content: bytes) -> None:
    with pytest.raises(ValueError):
        ProspectiveCaptureManifest.from_json_bytes(content)


@pytest.mark.parametrize("secondary", (False, True))
def test_manifest_fixes_primary_scope_and_explicit_optional_secondary(secondary: bool) -> None:
    manifest = _manifest(require_secondary_prices=secondary)
    scope = manifest.dq_scope(_FEATURE)
    assert scope.as_of == _FEATURE
    assert scope.requested_window == DataQualityDateWindow(date(2021, 2, 22), _FEATURE)
    assert scope.expected_price_tickers == EQUAL_RISK_PRICE_TICKERS
    assert scope.expected_rate_series == EQUAL_RISK_GUARD_RATE_SERIES
    assert scope.require_secondary_prices is secondary
    assert set(scope.input_roles) == (
        {"prices", "rates", "secondary_prices"} if secondary else {"prices", "rates"}
    )
    assert manifest.activation_attempt_maximum == manifest.attempts_per_feature_session == 1
    assert manifest.canonical_dq_calls_per_capture_maximum == 1
    assert manifest.parent_canonical_dq_call_maximum == 0


@pytest.mark.parametrize(
    "feature", (date(2026, 11, 25), date(2026, 12, 1), "2026-11-27", None, _REVIEWED)
)
def test_manifest_does_not_expand_its_explicit_session_allowlist(feature: Any) -> None:
    with pytest.raises(ValueError):
        _manifest().dq_scope(feature)


@pytest.mark.parametrize(
    "field,value",
    [
        ("manifest_id", "unversioned"),
        ("manifest_id", "../capture_v1"),
        ("manifest_id", "capture_v0"),
        ("candidate_commit", "1" * 39),
        ("candidate_commit", "A" * 40),
        ("source_output_relative_path", "inputs/source"),
        ("source_output_relative_path", "outputs/../source"),
        ("source_output_relative_path", "outputs//source"),
        ("source_output_relative_path", "outputs\\source"),
        ("source_output_relative_path", "/outputs/source"),
        ("output_relative_path", _SYNTHETIC_PREFIX + "another_v1"),
        ("output_relative_path", _RESEARCH_PREFIX + "synthetic_capture_v1"),
        ("allowed_feature_sessions", ()),
        ("allowed_feature_sessions", (_NEXT_FEATURE, _FEATURE)),
        ("allowed_feature_sessions", (_FEATURE, _FEATURE)),
        ("allowed_feature_sessions", (date(2021, 2, 19),)),
        ("allowed_feature_sessions", [_FEATURE]),
        ("allowed_feature_sessions", (_REVIEWED,)),
        ("expires_at", _EXPIRES.replace(tzinfo=None)),
        ("expires_at", _EXPIRES.astimezone(timezone(timedelta(hours=9)))),
        ("evidence_purpose", "LIVE_RESEARCH"),
        ("require_secondary_prices", 1),
        ("source_manifest_path", FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH),
        ("source_manifest_sha256", FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256),
        ("policy_path", "config/data_quality.yaml"),
        ("consumer_family", "COMPOSER"),
        ("timing_version", "LEGACY_CLOSE_FORWARD"),
        ("return_clock", "FEATURE_SESSION_CLOSE_TO_NEXT_XNYS_SESSION_CLOSE"),
        ("task_id", "TRADING-2563"),
        ("snapshot_selection_rule", "LATEST"),
        ("activation_attempt_maximum", 2),
        ("attempts_per_feature_session", 2),
        ("canonical_dq_calls_per_capture_maximum", 2),
        ("parent_canonical_dq_call_maximum", 1),
        ("outcome_access_allowed", True),
        ("provider_calls_allowed", True),
        ("orders_allowed", True),
        ("fills_allowed", True),
        ("production_effect", "enabled"),
        ("broker_action", "order"),
    ],
)
def test_manifest_rejects_invalid_paths_scope_or_execution_expansion(
    field: str, value: Any
) -> None:
    with pytest.raises(ValueError):
        replace(_manifest(), **{field: value})


@pytest.mark.parametrize(
    "overlap", ("execution", "output_parent", "output", "control", "dq", "dq_child")
)
def test_publication_cannot_overlap_any_part_of_the_allowed_write_root(overlap: str) -> None:
    manifest = _manifest()
    output = manifest.roots.execution_root + "/" + manifest.output_relative_path
    publication = {
        "execution": manifest.roots.execution_root,
        "output_parent": manifest.roots.execution_root + "/outputs",
        "output": output,
        "control": output + "/control",
        "dq": output + "/dq",
        "dq_child": output + "/dq/immutable",
    }[overlap]
    with pytest.raises(ValueError):
        replace(manifest, roots=replace(manifest.roots, publication_root=publication))


@pytest.mark.parametrize("publication_suffix", ("", "/control", "/dq"))
def test_windows_case_alias_cannot_turn_the_output_into_a_publication_root(
    publication_suffix: str,
) -> None:
    manifest = _manifest()
    output = "C:/Synthetic/Execution/" + manifest.output_relative_path
    roots = NamedDQRoots(
        "D:/Synthetic/Source",
        output.lower() + publication_suffix,
        "C:/Synthetic/Execution",
        output + "/dq",
    )
    with pytest.raises(ValueError):
        replace(manifest, roots=roots)


def test_dq_evidence_must_be_the_exact_owned_subdirectory_and_siblings_are_allowed() -> None:
    manifest = _manifest()
    with pytest.raises(ValueError):
        replace(manifest, roots=replace(manifest.roots, evidence_root="/synthetic/other_evidence"))
    output = manifest.roots.execution_root + "/" + manifest.output_relative_path
    sibling = replace(manifest.roots, publication_root=output + "_other")
    assert replace(manifest, roots=sibling).roots.publication_root == output + "_other"


@pytest.mark.parametrize("overlap", ("parent", "output", "child", "windows_case"))
def test_source_output_cannot_overlap_the_complete_owned_write_root(overlap: str) -> None:
    manifest = _manifest()
    source_output = {
        "parent": "outputs/architecture",
        "output": manifest.output_relative_path,
        "child": manifest.output_relative_path + "/control",
        "windows_case": "outputs/" + manifest.output_relative_path.removeprefix("outputs/").upper(),
    }[overlap]
    if overlap == "windows_case":
        roots = NamedDQRoots(
            "c:/synthetic/execution",
            "D:/Synthetic/Publication",
            "C:/Synthetic/Execution",
            "C:/Synthetic/Execution/" + manifest.output_relative_path + "/dq",
        )
    else:
        roots = replace(manifest.roots, source_root=manifest.roots.execution_root)
    with pytest.raises(
        ValueError, match="original source output and capture output must be disjoint"
    ):
        replace(manifest, roots=roots, source_output_relative_path=source_output)
    # A similarly prefixed sibling is disjoint; path-component containment is
    # required rather than rejecting an unrelated directory by string prefix.
    assert replace(
        manifest,
        roots=roots,
        source_output_relative_path=manifest.output_relative_path + "_other",
    ).source_output_relative_path.endswith("_other")


def test_synthetic_and_research_manifest_review_identity_cannot_be_relabelled() -> None:
    synthetic, research = _manifest(), _manifest(purpose="PROSPECTIVE_RESEARCH")
    for manifest in (synthetic, research):
        review = _review(manifest)
        review.assert_manifest(manifest)
        assert not review.cryptographic_owner_signature_claimed
        assert not review.outcome_access_authorized
    assert synthetic.canonical_sha256 != research.canonical_sha256
    assert synthetic.output_relative_path != research.output_relative_path
    with pytest.raises(ValueError):
        _review(synthetic).assert_manifest(research)
    with pytest.raises(ValueError):
        _review(research).assert_manifest(synthetic)


@pytest.mark.parametrize(
    "field,value",
    [
        ("manifest_id", "unversioned"),
        ("manifest_sha256", "A" * 64),
        ("decision_ref", "owner_decision:TRADING-2563:old_one_shot"),
        ("decision_ref", "synthetic_review:TRADING-2563:old_one_shot"),
        ("decision_ref", "synthetic_review:TRADING-2564:"),
        ("decision_ref", "owner_decision:TRADING-2564:synthetic_as_real"),
        ("authorization_state", "RETROSPECTIVELY_REVIEWED"),
        ("authorization_state", "UNAUTHORIZED_ACTION_INCIDENT"),
        ("authorization_state", "APPROVED"),
        ("evidence_purpose", "LIVE_RESEARCH"),
        ("reviewed_at", _REVIEWED.replace(tzinfo=None)),
        ("actor", "automated agent"),
        ("status", "CAPTURE_COMPLETE"),
        ("reviewed_timing_version", "LEGACY"),
        ("complete_witness_before_deadline_required", False),
        ("host_clock_trust_acknowledged", False),
        ("outcome_access_authorized", True),
        ("risk_tier", "R3_PRODUCTION_OR_BROKER"),
        ("cryptographic_owner_signature_claimed", True),
    ],
)
def test_owner_review_rejects_old_one_shot_or_unreviewed_authority(field: str, value: Any) -> None:
    with pytest.raises(ValueError):
        replace(_review(), **{field: value})


@pytest.mark.parametrize("state", ("EXACT_PREAUTHORIZED", "STANDING_OWNER_SCOPE"))
def test_new_bounded_authorization_states_do_not_require_a_machine_token(state: str) -> None:
    manifest = _manifest()
    review = _review(manifest, authorization_state=state)
    review.assert_manifest(manifest)
    assert review.authorization_state == state


@pytest.mark.parametrize(
    "change",
    [
        {"candidate_commit": "2" * 40},
        {"require_secondary_prices": True},
        {"allowed_feature_sessions": (_FEATURE,)},
        {"expires_at": _EXPIRES + timedelta(days=1)},
    ],
)
def test_review_binds_every_manifest_detail_not_just_its_label(change: dict[str, Any]) -> None:
    manifest = _manifest()
    with pytest.raises(ValueError):
        _review(manifest).assert_manifest(replace(manifest, **change))


@pytest.mark.parametrize("reviewed_at", (_EXPIRES, _EXPIRES + timedelta(microseconds=1)))
def test_review_must_precede_manifest_expiration(reviewed_at: datetime) -> None:
    with pytest.raises(ValueError):
        _review(reviewed_at=reviewed_at).assert_manifest(_manifest())


def test_activation_has_no_dummy_dq_or_feature_and_control_path_is_external() -> None:
    activation = _request("activate")
    assert activation.feature_session is activation.named_dq_request is None
    assert activation.owner_review.relative_path == (
        activation.manifest.output_relative_path + "/control/owner_review.json"
    )
    assert activation.owner_review.root_role == "EXECUTION"
    assert (
        activation.operation_relative_path
        == activation.manifest.output_relative_path + "/activation"
    )
    assert activation.timing_relative_path == activation.manifest.output_relative_path + "/timing"
    assert activation.required_write_paths == (activation.manifest.output_relative_path,)
    assert activation.request_id == "prospective_capture_request_" + activation.canonical_sha256


@pytest.mark.parametrize("change", ("feature", "named_request", "both"))
def test_activation_cannot_smuggle_a_historical_feature_or_dq_scope(change: str) -> None:
    activation = _request("activate")
    changes: dict[str, Any] = {}
    if change in {"feature", "both"}:
        changes["feature_session"] = _FEATURE
    if change in {"named_request", "both"}:
        changes["named_dq_request"] = _named_request(activation.manifest)
    with pytest.raises(ValueError):
        replace(activation, **changes)


@pytest.mark.parametrize(
    "field,value",
    [
        ("operation", "verify"),
        ("candidate_commit", "2" * 40),
        ("source_manifest_path", FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH),
        ("source_manifest_sha256", FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256),
        ("policy_path", "config/data_quality.yaml"),
        ("feature_session", None),
        ("feature_session", date(2026, 11, 25)),
        ("feature_session", _NEXT_FEATURE),
        ("named_dq_request", None),
    ],
)
def test_capture_request_cannot_drift_from_manifest_and_named_scope(field: str, value: Any) -> None:
    with pytest.raises(ValueError):
        replace(_request(), **{field: value})


@pytest.mark.parametrize(
    "damage", ("root_role", "legacy_input_path", "other_manifest", "root_drift")
)
def test_request_owner_control_is_bound_to_its_exact_run_root(damage: str) -> None:
    request = _request()
    if damage == "root_drift":
        changes = {"roots": replace(request.roots, source_root="/synthetic/other_source")}
    else:
        review = request.owner_review
        if damage == "root_role":
            review = replace(review, root_role="SOURCE")
        elif damage == "legacy_input_path":
            review = replace(review, relative_path="inputs/research/owner_review.json")
        else:
            review = replace(
                review,
                relative_path=_SYNTHETIC_PREFIX + "other_v1/control/owner_review.json",
            )
        changes = {"owner_review": review}
    with pytest.raises(ValueError):
        replace(request, **changes)


@pytest.mark.parametrize(
    "damage",
    (
        "roots",
        "commit",
        "old57",
        "old59",
        "source_output",
        "dq_policy",
        "short_window",
        "two_assets",
        "one_rate",
        "secondary",
    ),
)
def test_named_dq_child_request_cannot_borrow_old_or_partial_scope(damage: str) -> None:
    request = _request()
    child = request.named_dq_request
    assert child is not None
    changes: dict[str, Any]
    if damage == "roots":
        changes = {"roots": replace(child.roots, publication_root="/synthetic/another_publication")}
    elif damage == "commit":
        changes = {"candidate_commit": "2" * 40}
    elif damage == "old57":
        changes = {
            "source_manifest_path": EQUAL_RISK_PRICE_SOURCE_MANIFEST_PATH,
            "source_manifest_sha256": EQUAL_RISK_PRICE_SOURCE_MANIFEST_SHA256,
        }
    elif damage == "old59":
        changes = {
            "source_manifest_path": FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
            "source_manifest_sha256": FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256,
        }
    elif damage == "source_output":
        changes = {"source_output_relative_path": "outputs/other_source"}
    elif damage == "dq_policy":
        changes = {"policy_path": "config/other_quality.yaml"}
    else:
        scope_changes: dict[str, Any] = {
            "short_window": {
                "requested_window": DataQualityDateWindow(date(2021, 2, 23), _FEATURE)
            },
            "two_assets": {"expected_price_tickers": ("QQQ", "SGOV")},
            "one_rate": {"expected_rate_series": ("DGS10",)},
            "secondary": {
                "input_roles": ("prices", "rates", "secondary_prices"),
                "require_secondary_prices": True,
            },
        }[damage]
        changes = {"scope": replace(child.scope, **scope_changes)}
        if damage == "short_window":
            changes["expected_evaluated_window"] = None
    with pytest.raises(ValueError):
        replace(request, named_dq_request=replace(child, **changes))


def test_exact_snapshot_and_guard_window_remain_in_request_identity() -> None:
    request = _request()
    child = request.named_dq_request
    assert child is not None and child.expected_evaluated_window is not None
    assert child.scope.requested_window.end == _FEATURE
    assert child.expected_evaluated_window.end < _FEATURE
    assert child.source_manifest_path == PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH
    assert child.source_manifest_sha256 == PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256
    assert request.operation_relative_path.endswith("/sessions/2026-11-27")
    changed = replace(
        request,
        named_dq_request=replace(child, selector=replace(child.selector, pointer_sha256="b" * 64)),
    )
    assert changed.request_id != request.request_id
    # Exact snapshot identity changes do not create a second attempt slot.
    assert changed.operation_relative_path == request.operation_relative_path
    assert request.policy_path == CAPTURE_POLICY_PATH
    assert request.manifest.task_id == CAPTURE_TASK_ID


@pytest.mark.parametrize(
    "field,value",
    [
        ("request_id", "prospective_capture_request_" + "b" * 64),
        ("request_sha256", "b" * 64),
        ("manifest_id", "unversioned"),
        ("manifest_sha256", "A" * 64),
        ("candidate_commit", "2" * 39),
        ("execution_identity_sha256", "not-a-hash"),
        ("parent_pid", 0),
        ("parent_pid", True),
        ("source_lease_id", "lease-" + "A" * 20),
        ("source_lease_id", "lease-" + "a" * 19),
        ("monotonic_elapsed_ns", -1),
        ("monotonic_elapsed_ns", True),
        ("monotonic_elapsed_ns", 1.0),
        ("authorization_state", "RETROSPECTIVELY_REVIEWED"),
        ("authorization_state", "UNAUTHORIZED_ACTION_INCIDENT"),
        ("evidence_purpose", "LIVE_RESEARCH"),
        ("source_execution_attested", False),
        ("parent_canonical_dq_call_count", 1),
        ("acknowledgement_own_durability_time_claimed", True),
        ("outcome_access_authorized", True),
        ("provider_available_at_status", "PASS"),
        ("production_effect", "enabled"),
        ("broker_action", "order"),
    ],
)
def test_parent_ack_rejects_identity_type_or_safety_drift(field: str, value: Any) -> None:
    with pytest.raises(ValueError):
        replace(_ack(), **{field: value})


@pytest.mark.parametrize(
    "field", ("recording_call_started_at", "witness_bundle_observed_at", "decision_deadline")
)
@pytest.mark.parametrize("zone", (None, timezone(timedelta(hours=9))))
def test_ack_requires_utc_aware_instants_for_all_clock_fields(field: str, zone: Any) -> None:
    ack = _ack()
    original = getattr(ack, field)
    changed = original.replace(tzinfo=None) if zone is None else original.astimezone(zone)
    with pytest.raises(ValueError):
        replace(ack, **{field: changed})


def test_ack_rejects_backward_or_partially_rolled_back_utc() -> None:
    ack = _ack()
    with pytest.raises(ValueError, match="UTC must enclose"):
        replace(
            ack,
            witness_bundle_observed_at=ack.recording_call_started_at - timedelta(microseconds=1),
        )
    with pytest.raises(ValueError, match="UTC must enclose"):
        replace(ack, monotonic_elapsed_ns=1_000_001_001)
    with pytest.raises(ValueError, match="UTC must enclose"):
        replace(
            ack,
            witness_bundle_observed_at=ack.recording_call_started_at
            + timedelta(microseconds=500_000),
        )


def test_ack_allows_only_datetime_precision_for_inner_monotonic_lower_bound() -> None:
    ack = _ack()
    exact = replace(ack, monotonic_elapsed_ns=1_000_001_000)
    assert exact.monotonic_elapsed_ns == 1_000_001_000
    with pytest.raises(ValueError):
        replace(exact, monotonic_elapsed_ns=exact.monotonic_elapsed_ns + 1)


@pytest.mark.parametrize(
    "offset_us,status",
    [(-1, "CAPTURE_ACKNOWLEDGED"), (0, "LATE"), (1, "LATE")],
)
def test_complete_witness_must_be_strictly_before_deadline(offset_us: int, status: str) -> None:
    observed = _DEADLINE + timedelta(microseconds=offset_us)
    ack = _ack(
        recording_call_started_at=observed - timedelta(seconds=1),
        witness_bundle_observed_at=observed,
        technical_validation_state=status,
    )
    assert ack.technical_validation_state == status
    other = "LATE" if status == "CAPTURE_ACKNOWLEDGED" else "CAPTURE_ACKNOWLEDGED"
    with pytest.raises(ValueError, match="strict deadline classification"):
        replace(ack, technical_validation_state=other)


@pytest.mark.parametrize(
    "field,value",
    [
        ("operation", "verify"),
        ("feature_session", None),
        ("feature_session", date(2026, 11, 25)),
        ("decision_effective_session", None),
        ("decision_effective_session", _FEATURE),
        ("decision_deadline", None),
        ("first_feature_session", _NEXT_FEATURE),
        ("technical_validation_state", "ACTIVATION_ACKNOWLEDGED"),
    ],
)
def test_capture_ack_requires_its_declared_activation_order_and_capture_fields(
    field: str, value: Any
) -> None:
    with pytest.raises(ValueError):
        replace(_ack(), **{field: value})


@pytest.mark.parametrize(
    "field,value",
    [
        ("feature_session", _FEATURE),
        ("decision_effective_session", _NEXT_FEATURE),
        ("decision_deadline", _DEADLINE),
        ("technical_validation_state", "CAPTURE_ACKNOWLEDGED"),
    ],
)
def test_activation_ack_does_not_invent_a_dq_or_return_interval(field: str, value: Any) -> None:
    with pytest.raises(ValueError):
        replace(_ack("activate"), **{field: value})


def test_recorder_event_must_be_an_execution_root_binding() -> None:
    ack = _ack()
    with pytest.raises(ValueError):
        replace(ack, recorder_event=replace(ack.recorder_event, root_role="PUBLICATION"))


def _forbid(*args: object, **kwargs: object) -> Any:
    raise AssertionError("pure declarations attempted business, clock, calendar or filesystem I/O")


def test_pure_contract_does_not_claim_first_feature_calendar_or_runtime_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as guard:
        guard.setattr(builtins, "open", _forbid)
        guard.setattr(io, "open", _forbid)
        guard.setattr(Path, "read_bytes", _forbid)
        guard.setattr(Path, "write_bytes", _forbid)
        guard.setattr(socket, "socket", _forbid)
        guard.setattr(subprocess, "run", _forbid)
        guard.setattr(trading_calendar, "is_us_equity_trading_day", _forbid)
        guard.setattr(trading_calendar, "us_equity_market_session", _forbid)
        for kind in _KINDS:
            value = _declaration(kind)
            assert type(value).from_json_bytes(value.canonical_bytes) == value
        # The caller's DTO date is deliberately not calendar evidence. Actual
        # admission must derive and compare first F using complete ACK time and
        # the bound canonical calendar, including NY-date changes after S3a.
        activation = _ack("activate", first_feature_session=date(2026, 11, 26))
        assert activation.first_feature_session == date(2026, 11, 26)
        assert activation.acknowledgement_own_durability_time_claimed is False
        assert activation.outcome_access_authorized is False
        assert activation.provider_available_at_status == "NOT_ESTABLISHED"
        assert _manifest().timing_version == CAPTURE_TIMING_VERSION
        assert _manifest().return_clock == CAPTURE_RETURN_CLOCK
