"""S3b production capture parent: exact sources, one DQ child, local preview, ACK.

This is a finite manual research entry, not a scheduler or outcome engine. The
operator's review is a trusted input, not a cryptographic owner signature. All
original attempts and partial bytes survive failure; only original complete
results may be returned on a repeated key.
"""

from __future__ import annotations

import hashlib
import os
import time
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn

from ai_trading_system.contracts.named_data_quality_execution import (
    EQUAL_RISK_PRICE_REGISTRY_PATH,
    NamedArtifactBinding,
    NamedDQExecutionReceipt,
    NamedDQSuccessfulDispatchBinding,
    NamedEqualRiskPriceScope,
)
from ai_trading_system.contracts.named_execution_context import require_named_execution_context
from ai_trading_system.contracts.prospective_capture_execution import (
    CAPTURE_POLICY_PATH,
    CAPTURE_POLICY_SHA256,
    ParentCompletionAcknowledgement,
    ProspectiveCaptureOwnerReview,
    ProspectiveCaptureRequest,
)
from ai_trading_system.contracts.prospective_event_time_evidence import (
    EventBinding,
    PayloadMember,
    RecordingPlan,
    canonical_json_bytes,
    parse_utc_datetime,
    strict_json_loads,
)
from ai_trading_system.data.immutable_publish import (
    DataPublicationIntegrityError,
    _bound_directory,
    _root_authority,
    exclusive_store_maintenance,
    read_contained_artifact_bytes,
    write_contained_artifact_bytes,
)
from ai_trading_system.data.named_quality_dispatch import (
    NamedQualityDispatchError,
    NamedQualityDispatchResult,
    NamedQualityTerminalObservation,
    dispatch_named_quality_child,
    recheck_named_capture_lease,
    restore_named_capture_lease,
    verify_retained_named_capture_proof,
)
from ai_trading_system.data.named_quality_execution import (
    NamedBootstrapAuthority,
    _preview_calendar_witness,
    verify_named_data_quality_execution_receipt,
)
from ai_trading_system.platform.architecture.checkout_guard import CheckoutLeaseHandle
from ai_trading_system.prospective_event_time_evidence import (
    POLICY_PATH as TIME_POLICY_PATH,
)
from ai_trading_system.prospective_event_time_evidence import (
    RecordedTemporalEvidence,
    TimeEvidencePolicy,
    first_feature_session,
    load_time_evidence_policy,
    record_activation,
    record_local_input_observation,
    record_signal_completion,
    session_timing,
    verify_time_evidence,
)
from ai_trading_system.simple_baseline_named_preview import (
    build_prospective_simple_baseline_preview,
    rebuild_prospective_simple_baseline_preview,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day

_RESULT_SCHEMA = "prospective_capture_execution_result.v1"
_ATTEMPT_SCHEMA = "prospective_capture_attempt.v1"
_SAFETY = {
    "outcome_access_authorized": False,
    "legacy_observation_ledger_mutated": False,
    "provider_available_at_status": "NOT_ESTABLISHED",
    "oos_status": "NOT_ESTABLISHED",
    "provider_call_count": 0,
    "download_count": 0,
    "order_count": 0,
    "fill_count": 0,
    "production_effect": "none",
    "broker_action": "none",
}


class ProspectiveCaptureExecutionError(ValueError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.prospective_child_canonical_dq_call_count: int | None = None
        self.prospective_dq_parent_receipt: object = None
        super().__init__(f"{code}: {detail}")


def _fail(code: str, detail: str) -> NoReturn:
    raise ProspectiveCaptureExecutionError(code, detail)


def _now() -> datetime:
    return datetime.now(UTC)


def _monotonic_ns() -> int:
    return time.monotonic_ns()


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _object(value: object) -> dict[str, Any]:
    if type(value) is not dict or any(type(key) is not str for key in value):
        _fail("PROSPECTIVE_CAPTURE_OBJECT_INVALID", "strict JSON object required")
    return value


def _same(left: object, right: object, label: str) -> None:
    if canonical_json_bytes(left) != canonical_json_bytes(right):
        _fail("PROSPECTIVE_CAPTURE_BINDING_MISMATCH", label)


def _binding(relative: str, content: bytes) -> NamedArtifactBinding:
    return NamedArtifactBinding("EXECUTION", relative, _sha(content), len(content))


def _read(root: Path, binding: NamedArtifactBinding) -> bytes:
    if type(binding) is not NamedArtifactBinding or binding.root_role != "EXECUTION":
        _fail("PROSPECTIVE_CAPTURE_ROOT_ROLE_INVALID", "execution artifact required")
    content = read_contained_artifact_bytes(root=root, relative_path=binding.relative_path)
    if len(content) != binding.size_bytes or _sha(content) != binding.sha256:
        _fail("PROSPECTIVE_CAPTURE_ARTIFACT_CHANGED", binding.relative_path)
    return content


def _write(root: Path, relative: str, content: bytes) -> NamedArtifactBinding:
    write_contained_artifact_bytes(
        root=root, relative_path=relative, content=content, immutable=True
    )
    result = _binding(relative, content)
    if _read(root, result) != content:
        _fail("PROSPECTIVE_CAPTURE_WRITE_VERIFICATION_FAILED", relative)
    return result


def _canonical_object(content: bytes) -> dict[str, Any]:
    value = _object(strict_json_loads(content))
    if canonical_json_bytes(value) != content:
        _fail("PROSPECTIVE_CAPTURE_NONCANONICAL", "canonical immutable JSON required")
    return value


def _optional(root: Path, relative: str) -> bytes | None:
    try:
        return read_contained_artifact_bytes(root=root, relative_path=relative)
    except DataPublicationIntegrityError as exc:
        if exc.code == "CONTAINED_ARTIFACT_MISSING" or (
            exc.code == "ARTIFACT_BOUND_DIRECTORY_FAILED"
            and isinstance(exc.__cause__, FileNotFoundError)
        ):
            return None
        raise


def _directory(root: Path, relative: str) -> Path:
    target = root / relative
    # Existing descriptor-bound creation is safe before any same-key file
    # write. File recognition/publication then happens under store maintenance.
    with (
        _root_authority(root),
        _bound_directory(root, target, "prospective capture store", create=True),
    ):
        pass
    return target


def _controls(
    request: ProspectiveCaptureRequest,
    bootstrap: NamedBootstrapAuthority,
    *,
    allow_unclaimed_activation: bool = False,
) -> tuple[
    ProspectiveCaptureOwnerReview, tuple[PayloadMember, ...], RecordingPlan, TimeEvidencePolicy
]:
    context = require_named_execution_context()
    if (
        bootstrap.context is not context
        or bootstrap.canonical_dq_call_count != 0
        or bootstrap.operation not in {"activate", "capture"}
        or context.identity.execution_root != request.roots.execution_root
        or context.identity.candidate_commit != request.candidate_commit
        or context.identity.source_manifest_path != request.source_manifest_path
        or context.identity.source_manifest_sha256 != request.source_manifest_sha256
    ):
        _fail("PROSPECTIVE_CAPTURE_CONTEXT_MISMATCH", request.request_id)
    bootstrap.assert_execution_unchanged(stage="PROSPECTIVE_PRE_CONTROL")
    root = Path(request.roots.execution_root)
    review_bytes = _read(root, request.owner_review)
    review = ProspectiveCaptureOwnerReview.from_json_bytes(review_bytes)
    review.assert_manifest(request.manifest)
    policy = bootstrap.dependencies.get(CAPTURE_POLICY_PATH)
    if policy is None or _sha(policy.content) != CAPTURE_POLICY_SHA256:
        _fail("PROSPECTIVE_CAPTURE_POLICY_MISMATCH", CAPTURE_POLICY_PATH)
    for path in (TIME_POLICY_PATH, EQUAL_RISK_PRICE_REGISTRY_PATH):
        if path not in bootstrap.dependencies:
            _fail("PROSPECTIVE_CAPTURE_DEPENDENCY_MISSING", path)
    time_policy = load_time_evidence_policy(source_root=root)
    if any(not is_us_equity_trading_day(day) for day in request.manifest.allowed_feature_sessions):
        _fail("PROSPECTIVE_CAPTURE_NONSESSION_ALLOWLIST", request.manifest.manifest_id)
    definitions = tuple(
        sorted(
            (
                PayloadMember("run_manifest", request.manifest.canonical_bytes),
                PayloadMember("owner_review", review_bytes),
                PayloadMember("capture_policy", policy.content),
                PayloadMember("time_policy", bootstrap.dependencies[TIME_POLICY_PATH].content),
                PayloadMember(
                    "strategy_registry",
                    bootstrap.dependencies[EQUAL_RISK_PRICE_REGISTRY_PATH].content,
                ),
                PayloadMember(
                    "execution_identity", canonical_json_bytes(context.identity.to_dict())
                ),
            ),
            key=lambda row: row.role,
        )
    )
    activation_attempt = _optional(
        root, request.manifest.output_relative_path + "/activation/attempt.json"
    )
    if activation_attempt is None:
        if not allow_unclaimed_activation or request.operation != "activate":
            _fail("PROSPECTIVE_CAPTURE_ACTIVATION_ATTEMPT_REQUIRED", request.manifest.manifest_id)
    else:
        attempt = _canonical_object(activation_attempt)
        if (
            attempt.get("schema_version") != _ATTEMPT_SCHEMA
            or attempt.get("request_sha256") != _activation_request(request).canonical_sha256
            or attempt.get("manifest_sha256") != request.manifest.canonical_sha256
            or attempt.get("candidate_commit") != request.candidate_commit
            or attempt.get("execution_identity_sha256") != context.identity.stable_identity_sha256
        ):
            _fail("PROSPECTIVE_CAPTURE_ACTIVATION_ATTEMPT_CHANGED", request.manifest.manifest_id)
        definitions = tuple(
            sorted(
                (*definitions, PayloadMember("activation_attempt", activation_attempt)),
                key=lambda row: row.role,
            )
        )
    plan = RecordingPlan(
        plan_id=request.manifest.manifest_id,
        consumer_family="FIVE_CANDIDATE",
        declared_source_commit=request.candidate_commit,
        definition_bindings=tuple(item.binding for item in definitions),
    )
    bootstrap.assert_execution_unchanged(stage="PROSPECTIVE_POST_CONTROL")
    return review, definitions, plan, time_policy


def _check_live(
    request: ProspectiveCaptureRequest,
    bootstrap: NamedBootstrapAuthority,
    lease: CheckoutLeaseHandle,
    review: ProspectiveCaptureOwnerReview,
    *,
    at: datetime | None = None,
) -> dict[str, Any]:
    instant = _now() if at is None else at
    if not review.reviewed_at <= instant < request.manifest.expires_at:
        _fail("PROSPECTIVE_CAPTURE_SCOPE_NOT_LIVE", "review is future-dated or manifest expired")
    source_checked = parse_utc_datetime(
        bootstrap.assert_execution_unchanged(stage="PROSPECTIVE_LIVE")
    )
    if bootstrap.source_lease_id != lease.lease_id or bootstrap.canonical_dq_call_count != 0:
        _fail("PROSPECTIVE_CAPTURE_PARENT_SCOPE_CHANGED", request.request_id)
    proof = recheck_named_capture_lease(
        lease,
        candidate_commit=request.candidate_commit,
        required_paths=request.required_write_paths,
    )
    if source_checked > parse_utc_datetime(proof["checked_at"]):
        _fail("PROSPECTIVE_CAPTURE_CLOCK_ROLLBACK", "source and lease proof chronology")
    return {
        **proof,
        "schema_version": "prospective_capture_parent_proof.v1",
        "request_sha256": request.canonical_sha256,
        "parent_pid": os.getpid(),
        "parent_canonical_dq_call_count": 0,
        "parent_execution_identity_sha256": (
            require_named_execution_context().stable_identity_sha256
        ),
        "source_checked_at": source_checked.isoformat(),
    }


def _reserve(
    request: ProspectiveCaptureRequest,
    bootstrap: NamedBootstrapAuthority,
    lease: CheckoutLeaseHandle,
    review: ProspectiveCaptureOwnerReview,
) -> bool:
    """Claim before any DQ/recorder call; a repeated attempt never invokes them."""
    _check_live(request, bootstrap, lease, review)
    root = Path(request.roots.execution_root)
    relative = request.operation_relative_path
    directory = _directory(root, relative)
    # No cross-root read or lease check while holding this existing store lock.
    with exclusive_store_maintenance(store_root=directory):
        content = _optional(directory, "attempt.json")
        if content is not None:
            attempt = _canonical_object(content)
            if (
                attempt.get("schema_version") != _ATTEMPT_SCHEMA
                or attempt.get("request_sha256") != request.canonical_sha256
            ):
                _fail("PROSPECTIVE_CAPTURE_ATTEMPT_COLLISION", relative)
            return False
        _write(directory, "request.json", request.canonical_bytes)
        value = {
            "schema_version": _ATTEMPT_SCHEMA,
            "request_id": request.request_id,
            "request_sha256": request.canonical_sha256,
            "manifest_sha256": request.manifest.canonical_sha256,
            "candidate_commit": request.candidate_commit,
            "execution_identity_sha256": (
                require_named_execution_context().identity.stable_identity_sha256
            ),
            "parent_pid": os.getpid(),
            "source_lease_id": lease.lease_id,
            "started_at": _now().isoformat(),
            "retry_allowed": False,
        }
        write_contained_artifact_bytes(
            root=directory,
            relative_path="attempt.json",
            content=canonical_json_bytes(value),
            immutable=True,
        )
    _check_live(request, bootstrap, lease, review)
    return True


def _retained_attempt(request: ProspectiveCaptureRequest) -> dict[str, Any] | None:
    root = Path(request.roots.execution_root)
    relative = request.operation_relative_path
    content = _optional(root, relative + "/attempt.json")
    if content is None:
        return None
    attempt = _canonical_object(content)
    expected = {
        "schema_version": _ATTEMPT_SCHEMA,
        "request_id": request.request_id,
        "request_sha256": request.canonical_sha256,
        "manifest_sha256": request.manifest.canonical_sha256,
        "candidate_commit": request.candidate_commit,
        "execution_identity_sha256": (
            require_named_execution_context().identity.stable_identity_sha256
        ),
        "retry_allowed": False,
    }
    if set(attempt) != {*expected, "parent_pid", "source_lease_id", "started_at"}:
        _fail("PROSPECTIVE_CAPTURE_RETAINED_ATTEMPT_CHANGED", relative)
    for key, value in expected.items():
        _same(attempt.get(key), value, "attempt: " + key)
    if (
        type(attempt["parent_pid"]) is not int
        or attempt["parent_pid"] <= 0
        or type(attempt["source_lease_id"]) is not str
        or not attempt["source_lease_id"].startswith("lease-")
        or _optional(root, relative + "/request.json") != request.canonical_bytes
    ):
        _fail("PROSPECTIVE_CAPTURE_RETAINED_ATTEMPT_CHANGED", relative)
    parse_utc_datetime(attempt["started_at"])
    return attempt


def _replay_existing(
    request: ProspectiveCaptureRequest,
    bootstrap: NamedBootstrapAuthority,
    review: ProspectiveCaptureOwnerReview,
) -> dict[str, object] | None:
    attempt = _retained_attempt(request)
    if attempt is None:
        return None
    root = Path(request.roots.execution_root)
    if _optional(root, request.operation_relative_path + "/result.json") is None:
        return {
            **_base_result(request, review),
            "parent_pid": attempt["parent_pid"],
            "status": "INCOMPLETE",
            "technical_validation_state": "INCOMPLETE_ORIGINAL_ATTEMPT",
            "counter_observation_state": "UNKNOWN",
            "canonical_dq_call_count": None,
            "real_market_dq_call_count": (
                None if review.evidence_purpose == "PROSPECTIVE_RESEARCH" else 0
            ),
            "idempotent_replay": True,
            "this_invocation_canonical_dq_call_count": 0,
            "retry_allowed": False,
        }
    original = verify_prospective_capture_result(request, bootstrap=bootstrap)
    return {**original, "idempotent_replay": True, "this_invocation_canonical_dq_call_count": 0}


def _event_binding(request: ProspectiveCaptureRequest, event: EventBinding) -> NamedArtifactBinding:
    return NamedArtifactBinding(
        "EXECUTION",
        request.timing_relative_path + "/" + event.relative_path,
        event.sha256,
        event.size_bytes,
    )


def _local_event(request: ProspectiveCaptureRequest, binding: NamedArtifactBinding) -> EventBinding:
    prefix = request.timing_relative_path + "/"
    if binding.root_role != "EXECUTION" or not binding.relative_path.startswith(prefix):
        _fail("PROSPECTIVE_CAPTURE_EVENT_LOCATOR_INVALID", binding.relative_path)
    return EventBinding(binding.relative_path[len(prefix) :], binding.sha256, binding.size_bytes)


def _event_members(
    request: ProspectiveCaptureRequest,
    binding: NamedArtifactBinding,
    *,
    policy: TimeEvidencePolicy,
    source_lease_id: str | None = None,
) -> tuple[RecordedTemporalEvidence, dict[str, bytes]]:
    """Read only the fixed locators after the S3a verifier has checked the chain."""
    store = Path(request.roots.execution_root) / request.timing_relative_path
    local = _local_event(request, binding)
    event = verify_time_evidence(store_root=store, event=local, policy=policy)

    def bound_content(item: EventBinding) -> bytes:
        content = read_contained_artifact_bytes(root=store, relative_path=item.relative_path)
        if len(content) != item.size_bytes or _sha(content) != item.sha256:
            _fail("PROSPECTIVE_CAPTURE_RECORDED_MEMBER_DRIFT", item.relative_path)
        return content

    witness = _canonical_object(bound_content(local))
    intent_binding = EventBinding.from_dict(witness["intent"])
    slot = PurePosixPath(local.relative_path).parent
    if intent_binding.relative_path != (slot / "intent.json").as_posix():
        _fail("PROSPECTIVE_CAPTURE_EVENT_LOCATOR_INVALID", "same-slot intent required")
    intent = _canonical_object(bound_content(intent_binding))
    if source_lease_id is not None:
        lease_binding = EventBinding.from_dict(intent["lease_event"])
        if lease_binding.relative_path != (slot / "lease_event.json").as_posix():
            _fail("PROSPECTIVE_CAPTURE_EVENT_LOCATOR_INVALID", "fixed lease event required")
        original_lease = _object(strict_json_loads(bound_content(lease_binding)))
        if _object(original_lease.get("lease")).get("lease_id") != source_lease_id:
            _fail("PROSPECTIVE_CAPTURE_ACK_LEASE_MISMATCH", "recorder's original S4D lease differs")
    members: dict[str, bytes] = {}
    for row in intent["semantic"]["payload_bindings"]:
        artifact = EventBinding.from_dict(row["artifact"])
        PayloadMember(row["role"], b"")
        if artifact.relative_path != (slot / ("payload_" + row["role"] + ".bin")).as_posix():
            _fail("PROSPECTIVE_CAPTURE_EVENT_LOCATOR_INVALID", "fixed payload locator required")
        value = bound_content(artifact)
        members[row["role"]] = value
    return event, members


def _acknowledge(
    request: ProspectiveCaptureRequest,
    bootstrap: NamedBootstrapAuthority,
    review: ProspectiveCaptureOwnerReview,
    event: RecordedTemporalEvidence,
    *,
    started_at: datetime,
    completed_at: datetime,
    elapsed_ns: int,
    first_feature: date,
    policy: TimeEvidencePolicy,
) -> ParentCompletionAcknowledgement:
    if not started_at <= event.started_at <= event.payload_durable_completed_at <= completed_at:
        _fail("PROSPECTIVE_CAPTURE_ACK_BEFORE_WRITER", "parent interval must enclose recorder")
    if not review.reviewed_at <= started_at <= completed_at < request.manifest.expires_at:
        _fail("PROSPECTIVE_CAPTURE_ACK_SCOPE_EXPIRED", request.request_id)
    timing = (
        session_timing(request.feature_session, policy=policy) if request.feature_session else None
    )
    state = (
        "ACTIVATION_ACKNOWLEDGED"
        if timing is None
        else ("CAPTURE_ACKNOWLEDGED" if completed_at < timing.effective_close_at else "LATE")
    )
    return ParentCompletionAcknowledgement(
        request_id=request.request_id,
        request_sha256=request.canonical_sha256,
        manifest_id=request.manifest.manifest_id,
        manifest_sha256=request.manifest.canonical_sha256,
        operation=request.operation,
        feature_session=request.feature_session,
        candidate_commit=request.candidate_commit,
        execution_identity_sha256=require_named_execution_context().identity.stable_identity_sha256,
        parent_pid=os.getpid(),
        source_lease_id=bootstrap.source_lease_id,
        recorder_event=_event_binding(request, event.binding),
        recording_call_started_at=started_at,
        witness_bundle_observed_at=completed_at,
        monotonic_elapsed_ns=elapsed_ns,
        first_feature_session=first_feature,
        decision_effective_session=timing.effective_session if timing else None,
        decision_deadline=timing.effective_close_at if timing else None,
        authorization_state=review.authorization_state,
        evidence_purpose=review.evidence_purpose,
        technical_validation_state=state,
    )


def _base_result(
    request: ProspectiveCaptureRequest, review: ProspectiveCaptureOwnerReview
) -> dict[str, Any]:
    return {
        "schema_version": _RESULT_SCHEMA,
        "request_id": request.request_id,
        "request_sha256": request.canonical_sha256,
        "operation": request.operation,
        "manifest_id": request.manifest.manifest_id,
        "manifest_sha256": request.manifest.canonical_sha256,
        "feature_session": request.feature_session.isoformat() if request.feature_session else None,
        "candidate_commit": request.candidate_commit,
        "execution_root": request.roots.execution_root,
        "authorization_state": review.authorization_state,
        "evidence_purpose": review.evidence_purpose,
        "synthetic_inputs_only": review.evidence_purpose == "SYNTHETIC_ENGINEERING",
        "parent_pid": os.getpid(),
        "parent_canonical_dq_call_count": 0,
        "real_observation_admitted": False,
        "capture_admitted": False,
        "activation_admitted": False,
        "idempotent_replay": False,
        **_SAFETY,
    }


def _check_result_projection(
    result: dict[str, Any],
    request: ProspectiveCaptureRequest,
    review: ProspectiveCaptureOwnerReview,
    attempt: dict[str, Any],
) -> None:
    expected = _base_result(request, review)
    expected["parent_pid"] = attempt["parent_pid"]
    status = result.get("status")
    if status not in {"ACTIVATED", "CAPTURED", "BLOCKED", "LATE"}:
        _fail("PROSPECTIVE_CAPTURE_RESULT_STATE_INVALID", str(status))
    activated, captured = status == "ACTIVATED", status == "CAPTURED"
    if (activated and request.operation != "activate") or (
        (captured or status == "LATE") and request.operation != "capture"
    ):
        _fail("PROSPECTIVE_CAPTURE_RESULT_OPERATION_INVALID", str(status))
    expected.update(
        activation_admitted=activated,
        capture_admitted=captured,
        real_observation_admitted=captured and review.evidence_purpose == "PROSPECTIVE_RESEARCH",
        retry_allowed=False,
        technical_validation_state={
            "ACTIVATED": "ACTIVATION_ACKNOWLEDGED",
            "CAPTURED": "CAPTURE_ACKNOWLEDGED",
            "BLOCKED": "BLOCKED",
            "LATE": "LATE",
        }[status],
    )
    for key, value in expected.items():
        _same(result.get(key), value, "result: " + key)
    mandatory = {
        *expected,
        "status",
        "canonical_dq_call_count",
        "counter_observation_state",
        "real_market_dq_call_count",
        "recorder_entered",
        "completed_at",
    }
    optional = {
        "acknowledgement",
        "observation",
        "first_feature_session",
        "dq_parent_receipt",
        "pre_recording_proof",
        "post_recording_proof",
    }
    if status == "BLOCKED":
        mandatory.update({"reason_code", "detail"})
        if any(
            type(result.get(key)) is not str or not result[key] for key in ("reason_code", "detail")
        ):
            _fail("PROSPECTIVE_CAPTURE_RESULT_REASON_INVALID", request.request_id)
    if not mandatory <= set(result) or set(result) - mandatory - optional:
        _fail("PROSPECTIVE_CAPTURE_RESULT_FIELDS_INVALID", request.request_id)
    count = result["canonical_dq_call_count"]
    if count is not None and (type(count) is not int or count < 0):
        _fail("PROSPECTIVE_CAPTURE_RESULT_COUNTER_INVALID", request.request_id)
    _same(
        result["counter_observation_state"],
        "UNKNOWN" if count is None else "KNOWN",
        "counter state",
    )
    _same(
        result["real_market_dq_call_count"],
        count if review.evidence_purpose == "PROSPECTIVE_RESEARCH" else 0,
        "real DQ counter",
    )
    if (activated and count != 0) or (captured and count != 1):
        _fail("PROSPECTIVE_CAPTURE_RESULT_COUNTER_INVALID", request.request_id)
    if type(result["recorder_entered"]) is not bool or (
        status != "BLOCKED" and result["recorder_entered"] is not True
    ):
        _fail("PROSPECTIVE_CAPTURE_RESULT_RECORDER_INVALID", request.request_id)
    if parse_utc_datetime(result["completed_at"]) < parse_utc_datetime(attempt["started_at"]):
        _fail("PROSPECTIVE_CAPTURE_RESULT_CHRONOLOGY_INVALID", request.request_id)
    root = Path(request.roots.execution_root)
    if "dq_parent_receipt" in result:
        parent_binding = NamedArtifactBinding.from_dict(result["dq_parent_receipt"])
        if (
            request.operation != "capture"
            or parent_binding.relative_path
            != request.operation_relative_path + "/dq_dispatch/parent_receipt.json"
        ):
            _fail("PROSPECTIVE_CAPTURE_PARENT_LOCATOR_INVALID", request.request_id)
        parent = _canonical_object(_read(root, parent_binding))
        assert request.named_dq_request is not None
        for key, value in {
            "schema_version": "named_data_quality_parent_dispatch.v1",
            "profile": "PROSPECTIVE_FIVE_CANDIDATE_PRODUCTION_PARENT",
            "status_semantics": "PARENT_ASSOCIATION_AND_PROCESS_OBSERVATION_ONLY",
            "request_id": request.named_dq_request.request_id,
            "operation": "run",
            "parent_operation": "capture",
            "candidate_commit": request.candidate_commit,
            "execution_root": request.roots.execution_root,
            "parent_pid": attempt["parent_pid"],
            "source_lease_id": attempt["source_lease_id"],
            "observed_canonical_dq_call_count": count,
            "counter_observation_state": result["counter_observation_state"],
            "parent_canonical_dq_call_count": 0,
        }.items():
            _same(parent.get(key), value, "result parent: " + key)
        if captured and parent.get("status") != "PASS":
            _fail("PROSPECTIVE_CAPTURE_PARENT_EVIDENCE_INVALID", request.request_id)
    elif (
        captured
        or (count is not None and count != 0)
        or _optional(root, request.operation_relative_path + "/dq_dispatch/parent_receipt.json")
        is not None
    ):
        _fail("PROSPECTIVE_CAPTURE_COUNTER_EVIDENCE_MISSING", request.request_id)
    if status != "BLOCKED" and "acknowledgement" not in result:
        _fail("PROSPECTIVE_CAPTURE_ACK_REQUIRED", request.request_id)
    if captured and "observation" not in result:
        _fail("PROSPECTIVE_CAPTURE_OBSERVATION_REQUIRED", request.request_id)
    if activated and ("first_feature_session" not in result or "observation" in result):
        _fail("PROSPECTIVE_CAPTURE_ACTIVATION_RESULT_INVALID", request.request_id)


def _closed_input_check(
    members: Mapping[str, bytes], request: ProspectiveCaptureRequest
) -> NamedDQExecutionReceipt:
    """Verify recorded identities without reopening any original market input."""
    if "dq_receipt" not in members or "closure_manifest" not in members:
        _fail("PROSPECTIVE_CAPTURE_CLOSURE_MISSING", "receipt and complete closure required")
    receipt = NamedDQExecutionReceipt.from_json_bytes(members["dq_receipt"])
    if (
        receipt.request != request.named_dq_request
        or receipt.report.status != "PASS"
        or not receipt.data_quality_evidence.ready
    ):
        _fail("PROSPECTIVE_CAPTURE_DQ_BINDING_INVALID", request.request_id)
    closure = _canonical_object(members["closure_manifest"])
    expected_header = {
        "schema_version": "prospective_verified_input_closure.v1",
        "request_id": receipt.request.request_id,
        "receipt_id": receipt.receipt_id,
        "execution_identity_sha256": receipt.execution.stable_identity_sha256,
        "all_dq_input_roles": sorted(item.role for item in receipt.inputs),
        "recording_only": True,
        "rates_feature_access_granted": False,
        "provider_available_at_status": "NOT_ESTABLISHED",
    }
    _same({key: closure.get(key) for key in expected_header}, expected_header, "closure header")
    if set(closure) != {*expected_header, "members"} or type(closure["members"]) is not list:
        _fail("PROSPECTIVE_CAPTURE_CLOSURE_INVALID", "exact closure fields required")
    bound: dict[str, NamedArtifactBinding] = {}
    for row in closure["members"]:
        if type(row) is not dict or set(row) != {"role", "binding", "semantic_role"}:
            _fail("PROSPECTIVE_CAPTURE_CLOSURE_INVALID", "strict member row")
        role = row["role"]
        if type(role) is not str or role in bound or role not in members:
            _fail("PROSPECTIVE_CAPTURE_CLOSURE_INVALID", "missing or repeated role")
        binding = NamedArtifactBinding.from_dict(row["binding"])
        content = members[role]
        if len(content) != binding.size_bytes or _sha(content) != binding.sha256:
            _fail("PROSPECTIVE_CAPTURE_CLOSURE_INVALID", role)
        bound[role] = binding
        expected_semantic = (
            "PRICE_FEATURE"
            if role == "input_prices"
            else (
                "DQ_GUARD_ONLY"
                if role.startswith("input_")
                else (
                    "EXECUTION_DEPENDENCY"
                    if role.startswith("dependency_")
                    else "VERIFIED_PROVENANCE"
                )
            )
        )
        _same(row["semantic_role"], expected_semantic, "closure semantic role")
    if list(bound) != sorted(bound) or set(members) != {
        *bound,
        "closure_manifest",
        "capture_request",
        "activation_ack",
    }:
        _fail("PROSPECTIVE_CAPTURE_CLOSURE_ROLE_SET_INVALID", "exact ordered complete closure")
    expected: dict[str, NamedArtifactBinding] = {
        "input_" + item.role: item.member for item in receipt.inputs
    }
    publication = receipt.publication
    expected.update(
        {
            "publication_pointer": publication.pointer,
            "publication_transaction": publication.transaction,
            "snapshot_manifest": publication.snapshot_manifest,
            "source_event": publication.source_event,
            "commit_anchor": publication.anchor_pointer,
            "source_manifest": receipt.manifest.member,
            "dq_report": NamedArtifactBinding(
                "EVIDENCE", receipt.report.path, receipt.report.sha256, receipt.report.size_bytes
            ),
        }
    )
    for ordinal, item in enumerate(
        sorted(receipt.execution_dependencies, key=lambda row: row.relative_path)
    ):
        expected[f"dependency_{ordinal:03d}"] = item
    successful = NamedDQSuccessfulDispatchBinding.from_json_bytes(members["dq_successful_dispatch"])
    successful.assert_matches_receipt(receipt, receipt_path=successful.receipt.relative_path)
    expected.update(dq_receipt=successful.receipt, dq_parent=successful.parent_receipt)
    for role, binding in expected.items():
        if bound.get(role) != binding:
            _fail("PROSPECTIVE_CAPTURE_CLOSURE_BINDING_INVALID", role)
    fixed_parent = {
        "dq_successful_dispatch",
        "dq_parent_request",
        "dq_child_stdout",
        "dq_child_stderr",
        "dq_pre_guard",
        "dq_post_guard",
    }
    if set(bound) != {*expected, *fixed_parent}:
        _fail(
            "PROSPECTIVE_CAPTURE_CLOSURE_ROLE_SET_INVALID", "full original receipt closure required"
        )
    parent = _canonical_object(members["dq_parent"])
    dispatch_prefix = request.operation_relative_path + "/dq_dispatch/"
    for role, filename in {
        "dq_parent": "parent_receipt.json",
        "dq_successful_dispatch": "successful_run_dispatch.json",
        "dq_parent_request": "request.json",
        "dq_child_stdout": "child_stdout.json",
        "dq_child_stderr": "child_stderr.txt",
        "dq_pre_guard": "pre_dispatch_proof.json",
        "dq_post_guard": "post_dispatch_proof.json",
    }.items():
        if (
            bound[role].root_role != "EXECUTION"
            or bound[role].relative_path != dispatch_prefix + filename
        ):
            _fail("PROSPECTIVE_CAPTURE_PARENT_LOCATOR_INVALID", role)
    for role, key in (
        ("dq_parent_request", "request"),
        ("dq_child_stdout", "child_stdout"),
        ("dq_child_stderr", "child_stderr"),
        ("dq_pre_guard", "pre_dispatch_proof"),
        ("dq_post_guard", "post_dispatch_proof"),
    ):
        artifact = _object(parent.get(key))
        binding = bound[role]
        _same(
            artifact,
            {
                "path": request.roots.execution_root + "/" + binding.relative_path,
                "sha256": binding.sha256,
                "size_bytes": binding.size_bytes,
            },
            "parent member: " + role,
        )
    if members["dq_parent_request"] != receipt.request.canonical_bytes:
        _fail("PROSPECTIVE_CAPTURE_PARENT_REQUEST_INVALID", receipt.request.request_id)
    stdout = _object(strict_json_loads(members["dq_child_stdout"]))
    pre = _canonical_object(members["dq_pre_guard"])
    post = _canonical_object(members["dq_post_guard"])
    for key, value in {
        "schema_version": "named_data_quality_parent_dispatch.v1",
        "profile": "PROSPECTIVE_FIVE_CANDIDATE_PRODUCTION_PARENT",
        "status_semantics": "PARENT_ASSOCIATION_AND_PROCESS_OBSERVATION_ONLY",
        "status": "PASS",
        "candidate_commit": request.candidate_commit,
        "execution_root": request.roots.execution_root,
        "request_id": receipt.request.request_id,
        "operation": "run",
        "parent_operation": "capture",
        "returncode": 0,
        "observed_canonical_dq_call_count": 1,
        "parent_canonical_dq_call_count": 0,
        "lease_acquired_or_mutated": False,
        "verified_input_seal_exported": False,
        "dispatch_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }.items():
        _same(parent.get(key), value, "parent identity: " + key)
    for key, value in {
        "schema_version": "named_data_quality_bootstrap_result.v1",
        "status": "PASS",
        "request_id": receipt.request.request_id,
        "source_lease_id": successful.source_lease_id,
        "process_id": successful.execution_pid,
        "receipt_id": receipt.receipt_id,
        "receipt_path": successful.receipt.relative_path,
        "receipt_sha256": receipt.canonical_sha256,
        "canonical_dq_call_count": 1,
        "verified_input_seal_exported": False,
        "dispatch_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        "child_started_at": successful.child_started_at.isoformat(),
        "child_terminal_checked_at": successful.child_terminal_checked_at.isoformat(),
    }.items():
        _same(stdout.get(key), value, "child identity: " + key)
    required = tuple(
        sorted(
            (
                request.operation_relative_path + "/dq_dispatch",
                Path(request.roots.evidence_root)
                .relative_to(Path(request.roots.execution_root))
                .as_posix(),
            )
        )
    )
    for proof in (pre, post):
        _verify_source_parent_proof(
            proof,
            request=request,
            request_sha256=receipt.request.canonical_sha256,
            parent_pid=parent["parent_pid"],
            source_lease_id=successful.source_lease_id,
            required_paths=required,
            schema="named_dq_existing_parent_proof.v1",
        )
    if not (
        parse_utc_datetime(pre["checked_at"])
        <= parse_utc_datetime(parent["spawn_requested_at"])
        <= successful.child_started_at
        <= receipt.started_at
        <= receipt.ended_at
        <= receipt.execution_observation.terminal_checked_at
        <= successful.child_terminal_checked_at
        <= parse_utc_datetime(parent["terminal_observed_at"])
        <= parse_utc_datetime(post["source_checked_at"])
        <= successful.parent_postchecked_at
    ):
        _fail("PROSPECTIVE_CAPTURE_PARENT_CHRONOLOGY_INVALID", request.request_id)
    if (
        parent.get("status") != "PASS"
        or parent.get("returncode") != 0
        or parent.get("terminal_state") != "EXITED"
        or parent.get("observed_canonical_dq_call_count") != 1
        or parent.get("counter_observation_state") != "KNOWN"
        or parent.get("failure") is not None
        or parent.get("child_pid") != successful.execution_pid
        or parent.get("source_lease_id") != successful.source_lease_id
        or parent.get("child_result") != stdout
        or stdout.get("receipt_sha256") != receipt.canonical_sha256
        or stdout.get("receipt_id") != receipt.receipt_id
        or stdout.get("process_id") != successful.execution_pid
        or stdout.get("canonical_dq_call_count") != 1
        or post.get("status") != "PASS"
        or post.get("checked_at") != successful.parent_postchecked_at.isoformat()
        or _object(post.get("active_lease")).get("lease_id") != successful.source_lease_id
        or _object(post.get("active_lease")).get("state") != "ACTIVE"
    ):
        _fail(
            "PROSPECTIVE_CAPTURE_PARENT_EVIDENCE_INVALID", "successful original execution required"
        )
    return receipt


def _verify_source_parent_proof(
    proof: dict[str, Any],
    *,
    request: ProspectiveCaptureRequest,
    request_sha256: str,
    parent_pid: int,
    source_lease_id: str,
    required_paths: tuple[str, ...],
    schema: str,
) -> None:
    for key, value in {
        "schema_version": schema,
        "request_sha256": request_sha256,
        "parent_pid": parent_pid,
        "parent_canonical_dq_call_count": 0,
        "parent_execution_identity_sha256": (
            require_named_execution_context().stable_identity_sha256
        ),
    }.items():
        _same(proof.get(key), value, "original source parent proof: " + key)
    source_checked = parse_utc_datetime(proof["source_checked_at"])
    checked = parse_utc_datetime(proof["checked_at"])
    if source_checked > checked:
        _fail("PROSPECTIVE_CAPTURE_PARENT_CHRONOLOGY_INVALID", request.request_id)
    verify_retained_named_capture_proof(
        proof,
        execution_root=Path(request.roots.execution_root),
        candidate_commit=request.candidate_commit,
        required_paths=required_paths,
        source_lease_id=source_lease_id,
        checked_at=checked,
    )


def verify_prospective_capture_result(
    request: ProspectiveCaptureRequest, *, bootstrap: NamedBootstrapAuthority
) -> dict[str, object]:
    """Read-only exact-key replay, requiring an originally published result."""
    result = _canonical_object(
        read_contained_artifact_bytes(
            root=Path(request.roots.execution_root),
            relative_path=request.operation_relative_path + "/result.json",
        )
    )
    return _verify_prospective_capture_projection(request, bootstrap=bootstrap, result=result)


def _verify_prospective_capture_projection(
    request: ProspectiveCaptureRequest,
    *,
    bootstrap: NamedBootstrapAuthority,
    result: dict[str, Any],
) -> dict[str, object]:
    """Same full proof path before publication and when reading retained bytes."""
    review, definitions, plan, policy = _controls(request, bootstrap)
    root = Path(request.roots.execution_root)
    relative = request.operation_relative_path
    if (
        read_contained_artifact_bytes(root=root, relative_path=relative + "/request.json")
        != request.canonical_bytes
    ):
        _fail("PROSPECTIVE_CAPTURE_RETAINED_REQUEST_CHANGED", relative)
    attempt = _retained_attempt(request)
    if attempt is None:
        _fail("PROSPECTIVE_CAPTURE_RETAINED_ATTEMPT_CHANGED", relative)
    _check_result_projection(result, request, review, attempt)
    original_proofs: dict[str, dict[str, Any]] = {}
    for role in ("pre_recording_proof", "post_recording_proof"):
        if role not in result:
            continue
        binding = NamedArtifactBinding.from_dict(result[role])
        if binding.relative_path != relative + "/" + role + ".json":
            _fail("PROSPECTIVE_CAPTURE_PARENT_LOCATOR_INVALID", role)
        proof = _canonical_object(_read(root, binding))
        _verify_source_parent_proof(
            proof,
            request=request,
            request_sha256=request.canonical_sha256,
            parent_pid=attempt["parent_pid"],
            source_lease_id=attempt["source_lease_id"],
            required_paths=request.required_write_paths,
            schema="prospective_capture_parent_proof.v1",
        )
        original_proofs[role] = proof
    if "acknowledgement" not in result:
        bootstrap.assert_execution_unchanged(stage="PROSPECTIVE_RETAINED_TERMINAL")
        return result
    ack_binding = NamedArtifactBinding.from_dict(result["acknowledgement"])
    if ack_binding.relative_path != relative + "/acknowledgement.json":
        _fail("PROSPECTIVE_CAPTURE_ACK_LOCATOR_INVALID", relative)
    ack = ParentCompletionAcknowledgement.from_json_bytes(_read(root, ack_binding))
    if set(original_proofs) != {"pre_recording_proof", "post_recording_proof"}:
        _fail("PROSPECTIVE_CAPTURE_ACK_PARENT_PROOFS_REQUIRED", relative)
    pre = original_proofs["pre_recording_proof"]
    post = original_proofs["post_recording_proof"]
    if not (
        parse_utc_datetime(attempt["started_at"])
        <= parse_utc_datetime(pre["source_checked_at"])
        <= parse_utc_datetime(pre["checked_at"])
        <= ack.recording_call_started_at
        <= ack.witness_bundle_observed_at
        <= parse_utc_datetime(post["source_checked_at"])
        <= parse_utc_datetime(post["checked_at"])
        <= parse_utc_datetime(result["completed_at"])
    ):
        _fail("PROSPECTIVE_CAPTURE_ACK_PARENT_CHRONOLOGY_INVALID", relative)
    context = require_named_execution_context()
    for key, value in {
        "request_id": request.request_id,
        "request_sha256": request.canonical_sha256,
        "manifest_id": request.manifest.manifest_id,
        "manifest_sha256": request.manifest.canonical_sha256,
        "operation": request.operation,
        "feature_session": request.feature_session,
        "candidate_commit": request.candidate_commit,
        "execution_identity_sha256": context.identity.stable_identity_sha256,
        "parent_pid": result["parent_pid"],
        "source_lease_id": attempt["source_lease_id"],
        "authorization_state": review.authorization_state,
        "evidence_purpose": review.evidence_purpose,
    }.items():
        if getattr(ack, key) != value:
            _fail("PROSPECTIVE_CAPTURE_ACK_BINDING_INVALID", key)
    if (
        not review.reviewed_at
        <= parse_utc_datetime(attempt["started_at"])
        <= ack.recording_call_started_at
        <= ack.witness_bundle_observed_at
        < request.manifest.expires_at
    ):
        _fail("PROSPECTIVE_CAPTURE_ACK_CHRONOLOGY_INVALID", request.request_id)
    event_slot = (
        "activation"
        if request.feature_session is None
        else "sessions/" + request.feature_session.isoformat() + "/signal"
    )
    if (
        ack.recorder_event.relative_path
        != request.timing_relative_path
        + "/streams/"
        + plan.stream_id
        + "/"
        + event_slot
        + "/completion.json"
    ):
        _fail("PROSPECTIVE_CAPTURE_EVENT_LOCATOR_INVALID", "exact plan/session/stage required")
    event, members = _event_members(
        request, ack.recorder_event, policy=policy, source_lease_id=ack.source_lease_id
    )
    if (
        event.plan != plan
        or not ack.recording_call_started_at
        <= event.started_at
        <= event.payload_durable_completed_at
        <= ack.witness_bundle_observed_at
    ):
        _fail("PROSPECTIVE_CAPTURE_ACK_BEFORE_WRITER", relative)
    if request.operation == "activate":
        if event.event_kind != "ACTIVATION" or members != {
            row.role: row.content for row in definitions
        }:
            _fail("PROSPECTIVE_CAPTURE_ACTIVATION_DEFINITIONS_INVALID", relative)
        if ack.first_feature_session != first_feature_session(
            ack.witness_bundle_observed_at, policy=policy
        ):
            _fail("PROSPECTIVE_CAPTURE_FIRST_FEATURE_INVALID", relative)
        if result.get("status") == "ACTIVATED":
            _same(
                result["first_feature_session"],
                ack.first_feature_session.isoformat(),
                "activation first F",
            )
    else:
        activation_request = _activation_request(request)
        activation_result = verify_prospective_capture_result(
            activation_request, bootstrap=bootstrap
        )
        if activation_result.get("status") != "ACTIVATED":
            _fail("PROSPECTIVE_CAPTURE_ACTIVATION_REQUIRED", relative)
        activation_ack = ParentCompletionAcknowledgement.from_json_bytes(
            _read(
                root, NamedArtifactBinding.from_dict(_object(activation_result["acknowledgement"]))
            )
        )
        assert request.feature_session is not None
        timing = session_timing(request.feature_session, policy=policy)
        if (
            ack.first_feature_session != activation_ack.first_feature_session
            or ack.decision_effective_session != timing.effective_session
            or ack.decision_deadline != timing.effective_close_at
            or ack.recording_call_started_at <= timing.feature_close_at
            or (
                ack.technical_validation_state == "CAPTURE_ACKNOWLEDGED"
                and ack.witness_bundle_observed_at >= timing.effective_close_at
            )
            or (
                ack.technical_validation_state == "LATE"
                and ack.witness_bundle_observed_at < timing.effective_close_at
            )
            or event.event_kind != "SIGNAL_RECORDED"
            or event.feature_session != request.feature_session
            or set(members) != {"signal"}
            or event.previous_event is None
        ):
            _fail("PROSPECTIVE_CAPTURE_TIMING_INVALID", relative)
        inputs_event, inputs = _event_members(
            request,
            _event_binding(request, event.previous_event),
            policy=policy,
            source_lease_id=ack.source_lease_id,
        )
        if (
            inputs_event.event_kind != "INPUTS_OBSERVED"
            or inputs_event.feature_session != request.feature_session
            or inputs_event.previous_event != _local_event(request, activation_ack.recorder_event)
            or inputs.get("capture_request") != request.canonical_bytes
            or inputs.get("activation_ack") != activation_ack.canonical_bytes
        ):
            _fail("PROSPECTIVE_CAPTURE_INPUT_CHAIN_INVALID", relative)
        receipt = _closed_input_check(inputs, request)
        successful = NamedDQSuccessfulDispatchBinding.from_json_bytes(
            inputs["dq_successful_dispatch"]
        )
        _same(
            result.get("dq_parent_receipt"),
            successful.parent_receipt.to_dict(),
            "original child parent result binding",
        )
        if not (
            ack.recording_call_started_at
            <= parse_utc_datetime(_canonical_object(inputs["dq_pre_guard"])["source_checked_at"])
            <= successful.parent_postchecked_at
            <= inputs_event.started_at
            <= inputs_event.payload_durable_completed_at
            <= event.started_at
        ):
            _fail("PROSPECTIVE_CAPTURE_ACK_DQ_CHRONOLOGY_INVALID", relative)
        original_parent = _canonical_object(inputs["dq_parent"])
        if (
            original_parent.get("parent_pid") != ack.parent_pid
            or original_parent.get("source_lease_id") != ack.source_lease_id
            or receipt.execution != context.identity
        ):
            _fail("PROSPECTIVE_CAPTURE_ACK_PARENT_MISMATCH", "original DQ parent/source differs")
        preview = _canonical_object(members["signal"])
        registry_ordinal, registry = next(
            (ordinal, item)
            for ordinal, item in enumerate(
                sorted(receipt.execution_dependencies, key=lambda row: row.relative_path)
            )
            if item.relative_path == EQUAL_RISK_PRICE_REGISTRY_PATH
        )
        assert request.named_dq_request is not None
        sessions, next_session = _preview_calendar_witness(request.named_dq_request)
        assert next_session is not None
        rebuilt = rebuild_prospective_simple_baseline_preview(
            receipt=receipt,
            successful_dispatch=NamedDQSuccessfulDispatchBinding.from_json_bytes(
                inputs["dq_successful_dispatch"]
            ),
            required_scope=NamedEqualRiskPriceScope(
                as_of=request.feature_session,
                requested_window=receipt.request.scope.requested_window,
                registry_binding=registry,
            ),
            prices=inputs["input_prices"],
            registry=inputs[f"dependency_{registry_ordinal:03d}"],
            sessions=sessions,
            next_session=next_session,
        )
        _same(preview, rebuilt, "complete preview recomputation from closed inputs")
        if result["status"] != "CAPTURED":
            bootstrap.assert_execution_unchanged(stage="PROSPECTIVE_RETAINED_TERMINAL")
            return result
        if ack.technical_validation_state != "CAPTURE_ACKNOWLEDGED":
            _fail("PROSPECTIVE_CAPTURE_RESULT_AUTHORITY_INVALID", relative)
        observation_binding = NamedArtifactBinding.from_dict(result["observation"])
        if observation_binding.relative_path != relative + "/observation.json":
            _fail("PROSPECTIVE_CAPTURE_OBSERVATION_LOCATOR_INVALID", relative)
        observation = _canonical_object(_read(root, observation_binding))
        _same(
            observation,
            _observation(request, ack_binding, preview, review),
            "observation projection",
        )
        if (
            result.get("status") != "CAPTURED"
            or result.get("capture_admitted") is not True
            or result.get("activation_admitted") is not False
        ):
            _fail("PROSPECTIVE_CAPTURE_RESULT_AUTHORITY_INVALID", relative)
        if result.get("real_observation_admitted") is not (
            review.evidence_purpose == "PROSPECTIVE_RESEARCH"
        ):
            _fail("PROSPECTIVE_CAPTURE_EVIDENCE_PURPOSE_INVALID", relative)
    bootstrap.assert_execution_unchanged(stage="PROSPECTIVE_RETAINED_TERMINAL")
    return result


def _activation_request(request: ProspectiveCaptureRequest) -> ProspectiveCaptureRequest:
    return ProspectiveCaptureRequest(
        operation="activate",
        manifest=request.manifest,
        owner_review=request.owner_review,
        roots=request.roots,
        candidate_commit=request.candidate_commit,
        source_manifest_path=request.source_manifest_path,
        source_manifest_sha256=request.source_manifest_sha256,
        policy_path=request.policy_path,
        feature_session=None,
        named_dq_request=None,
    )


def _observation(
    request: ProspectiveCaptureRequest,
    ack: NamedArtifactBinding,
    preview: Mapping[str, object],
    review: ProspectiveCaptureOwnerReview,
) -> dict[str, object]:
    return {
        "schema_version": "prospective_five_candidate_observation.v1",
        "request_id": request.request_id,
        "manifest_id": request.manifest.manifest_id,
        "feature_session": request.feature_session.isoformat() if request.feature_session else None,
        "acknowledgement": ack.to_dict(),
        "preview": dict(preview),
        "capture_admitted": True,
        "real_observation_admitted": review.evidence_purpose == "PROSPECTIVE_RESEARCH",
        "evidence_purpose": review.evidence_purpose,
        "authorization_state": review.authorization_state,
        "technical_validation_state": "COMPLETE_SOURCE_DQ_PREVIEW_AND_TIMING_VERIFIED",
        "observation_is_projection_of_predeadline_signal": True,
        "observation_own_predeadline_durability_claimed": False,
        **_SAFETY,
    }


def bootstrap_worker(
    payload: Mapping[str, object], *, operation: str, bootstrap: NamedBootstrapAuthority
) -> dict[str, object]:
    request = ProspectiveCaptureRequest.from_dict(payload)
    if operation != request.operation or operation != bootstrap.operation:
        _fail("PROSPECTIVE_CAPTURE_OPERATION_MISMATCH", operation)
    review, definitions, plan, policy = _controls(
        request, bootstrap, allow_unclaimed_activation=operation == "activate"
    )
    root = Path(request.roots.execution_root)
    replayed = _replay_existing(request, bootstrap, review)
    if replayed is not None:
        return replayed
    lease = restore_named_capture_lease(
        execution_root=root,
        source_lease_id=bootstrap.source_lease_id,
        candidate_commit=request.candidate_commit,
        required_paths=request.required_write_paths,
    )
    if not _reserve(request, bootstrap, lease, review):
        replayed = _replay_existing(request, bootstrap, review)
        assert replayed is not None
        return replayed
    result = _base_result(request, review)
    dq: NamedQualityDispatchResult | None = None
    terminal_observation: NamedQualityTerminalObservation | None = None
    dq_entered = False
    record_entered = False
    try:
        # Freeze the original activation parent's identity into its definition
        # payload; later captures reuse those exact attempt bytes.
        review, definitions, plan, policy = _controls(request, bootstrap)
        # The contained writer creates and verifies the dedicated timing root.
        timing_root = _directory(root, request.timing_relative_path)
        with exclusive_store_maintenance(store_root=timing_root):
            _write(
                timing_root,
                "namespace.json",
                canonical_json_bytes(
                    {
                        "schema_version": "prospective_capture_timing_namespace.v1",
                        "manifest_id": request.manifest.manifest_id,
                        "manifest_sha256": request.manifest.canonical_sha256,
                        "stream_id": plan.stream_id,
                    }
                ),
            )
        activation_ack: ParentCompletionAcknowledgement | None = None
        preview: dict[str, object] | None = None
        # Enclose the entire DQ/verification/recording interval so a clock
        # rollback before the recorder cannot disguise late source work.
        pre = _check_live(request, bootstrap, lease, review)
        result["pre_recording_proof"] = _write(
            root,
            request.operation_relative_path + "/pre_recording_proof.json",
            canonical_json_bytes(pre),
        ).to_dict()
        started = _now()
        monotonic_started = _monotonic_ns()
        if started < parse_utc_datetime(pre["checked_at"]):
            _fail("PROSPECTIVE_CAPTURE_CLOCK_ROLLBACK", "parent interval starts before preguard")
        if operation == "capture":
            activation_result = verify_prospective_capture_result(
                _activation_request(request), bootstrap=bootstrap
            )
            if activation_result.get("status") != "ACTIVATED":
                _fail("PROSPECTIVE_CAPTURE_ACTIVATION_REQUIRED", request.request_id)
            activation_ack = ParentCompletionAcknowledgement.from_json_bytes(
                _read(
                    root,
                    NamedArtifactBinding.from_dict(_object(activation_result["acknowledgement"])),
                )
            )
            assert request.feature_session is not None and request.named_dq_request is not None
            timing = session_timing(request.feature_session, policy=policy)
            now = _now()
            if (
                request.feature_session < activation_ack.first_feature_session
                or started <= timing.feature_close_at
                or not timing.feature_close_at < now < timing.effective_close_at
            ):
                _fail(
                    "PROSPECTIVE_CAPTURE_OUTSIDE_FEATURE_WINDOW",
                    request.feature_session.isoformat(),
                )
            _check_live(request, bootstrap, lease, review)
            dq_entered = True
            dq = dispatch_named_quality_child(
                request.named_dq_request,
                bootstrap=bootstrap,
                lease=lease,
                output_relative_path=request.operation_relative_path + "/dq_dispatch",
            )
            if (
                dq.status != "PASS"
                or dq.canonical_dq_call_count != 1
                or dq.counter_observation_state != "KNOWN"
            ):
                _fail("PROSPECTIVE_CAPTURE_DQ_CHILD_BLOCKED", dq.status)
            assert dq.receipt_path is not None and dq.receipt_sha256 is not None
            verified = verify_named_data_quality_execution_receipt(
                request.named_dq_request,
                receipt_path=dq.receipt_path,
                receipt_sha256=dq.receipt_sha256,
                run_dispatch_path=dq.run_dispatch_path,
                run_dispatch_sha256=dq.run_dispatch_sha256,
                bootstrap=bootstrap,
            )
            registry = next(
                item
                for item in verified.receipt.execution_dependencies
                if item.relative_path == EQUAL_RISK_PRICE_REGISTRY_PATH
            )
            scope = NamedEqualRiskPriceScope(
                as_of=request.feature_session,
                requested_window=request.named_dq_request.scope.requested_window,
                registry_binding=registry,
            )
            preview = build_prospective_simple_baseline_preview(verified, required_scope=scope)
            closure = tuple(
                PayloadMember(role, content)
                for role, content in verified.recording_closure_for_prospective()
            )
            inputs = (
                *closure,
                PayloadMember("capture_request", request.canonical_bytes),
                PayloadMember("activation_ack", activation_ack.canonical_bytes),
            )
        _check_live(request, bootstrap, lease, review)
        record_entered = True
        if operation == "activate":
            event = record_activation(
                store_root=root / request.timing_relative_path,
                plan=plan,
                definitions=definitions,
                policy=policy,
                lease_handle=lease,
            )
        else:
            assert (
                activation_ack is not None
                and request.feature_session is not None
                and preview is not None
            )
            input_event = record_local_input_observation(
                store_root=root / request.timing_relative_path,
                plan=plan,
                feature_session=request.feature_session,
                inputs=tuple(inputs),
                activation=_local_event(request, activation_ack.recorder_event),
                policy=policy,
                lease_handle=lease,
            )
            event = record_signal_completion(
                store_root=root / request.timing_relative_path,
                plan=plan,
                feature_session=request.feature_session,
                signal=PayloadMember("signal", canonical_json_bytes(preview)),
                inputs=input_event.binding,
                policy=policy,
                lease_handle=lease,
            )
        elapsed = _monotonic_ns() - monotonic_started
        completed = _now()  # This is after the complete recorder/witness returns.
        first = (
            first_feature_session(completed, policy=policy)
            if activation_ack is None
            else activation_ack.first_feature_session
        )
        ack = _acknowledge(
            request,
            bootstrap,
            review,
            event,
            started_at=started,
            completed_at=completed,
            elapsed_ns=elapsed,
            first_feature=first,
            policy=policy,
        )
        post = _check_live(request, bootstrap, lease, review)
        if completed > parse_utc_datetime(post["source_checked_at"]):
            _fail(
                "PROSPECTIVE_CAPTURE_CLOCK_ROLLBACK",
                "source postguard predates witness observation",
            )
        result["post_recording_proof"] = _write(
            root,
            request.operation_relative_path + "/post_recording_proof.json",
            canonical_json_bytes(post),
        ).to_dict()
        ack_binding = _write(
            root, request.operation_relative_path + "/acknowledgement.json", ack.canonical_bytes
        )
        result["acknowledgement"] = ack_binding.to_dict()
        result["technical_validation_state"] = ack.technical_validation_state
        if operation == "capture" and ack.technical_validation_state == "LATE":
            result["status"] = "LATE"
        elif operation == "activate":
            result.update(
                status="ACTIVATED",
                activation_admitted=True,
                first_feature_session=first.isoformat(),
            )
        else:
            assert preview is not None
            observation = _observation(request, ack_binding, preview, review)
            observation_binding = _write(
                root,
                request.operation_relative_path + "/observation.json",
                canonical_json_bytes(observation),
            )
            result.update(
                status="CAPTURED",
                capture_admitted=True,
                real_observation_admitted=review.evidence_purpose == "PROSPECTIVE_RESEARCH",
                observation=observation_binding.to_dict(),
            )
        _check_live(request, bootstrap, lease, review)
    except (ValueError, OSError, RuntimeError, KeyError, StopIteration, TypeError) as exc:
        if isinstance(exc, NamedQualityDispatchError):
            terminal_observation = exc.terminal_observation
        result.update(
            status="BLOCKED",
            capture_admitted=False,
            activation_admitted=False,
            real_observation_admitted=False,
            technical_validation_state="BLOCKED",
            reason_code=getattr(exc, "code", "PROSPECTIVE_CAPTURE_EXECUTION_FAILED"),
            detail=str(exc),
        )
    count = (
        dq.canonical_dq_call_count
        if dq is not None
        else (
            terminal_observation.canonical_dq_call_count
            if terminal_observation is not None
            else None if dq_entered else 0
        )
    )
    result.update(
        canonical_dq_call_count=count,
        counter_observation_state="KNOWN" if count is not None else "UNKNOWN",
        recorder_entered=record_entered,
        retry_allowed=False,
    )
    if dq is not None:
        result["dq_parent_receipt"] = dq.parent_receipt.to_dict()
    elif terminal_observation is not None and terminal_observation.parent_receipt is not None:
        result["dq_parent_receipt"] = terminal_observation.parent_receipt.to_dict()
    result["real_market_dq_call_count"] = (
        count if review.evidence_purpose == "PROSPECTIVE_RESEARCH" else 0
    )
    result["completed_at"] = _now().isoformat()
    # If the postguard itself failed, do not bypass it to write a final status.
    # attempt/child/event bytes remain inspectable and the slot stays incomplete.
    try:
        final_pre = _check_live(request, bootstrap, lease, review)
        if parse_utc_datetime(final_pre["source_checked_at"]) < parse_utc_datetime(
            result["completed_at"]
        ):
            _fail("PROSPECTIVE_CAPTURE_CLOCK_ROLLBACK", "terminal guard predates result completion")
        _verify_prospective_capture_projection(request, bootstrap=bootstrap, result=result)
        _write(root, request.operation_relative_path + "/result.json", canonical_json_bytes(result))
        final_post = _check_live(request, bootstrap, lease, review)
        if parse_utc_datetime(final_post["source_checked_at"]) < parse_utc_datetime(
            final_pre["checked_at"]
        ):
            _fail("PROSPECTIVE_CAPTURE_CLOCK_ROLLBACK", "terminal publication guard chronology")
    except (ValueError, OSError, RuntimeError, KeyError, TypeError) as exc:
        # The CLI may lose the return value if its terminal guard fails. These
        # observed child counters are separate from this parent's fixed zero.
        failure = ProspectiveCaptureExecutionError(
            getattr(exc, "code", "PROSPECTIVE_CAPTURE_TERMINAL_FAILED"), str(exc)
        )
        failure.prospective_child_canonical_dq_call_count = count
        failure.prospective_dq_parent_receipt = result.get("dq_parent_receipt")
        raise failure from exc
    return result
