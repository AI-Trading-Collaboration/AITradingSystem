"""One bounded Composer observation using captured current-revision inputs.

Canonical DQ remains in three independent children. The parent verifies the
same bytes, records them before fit, and cannot read the new observation's
future outcome. Historical training access is explicitly a different role.
"""

from __future__ import annotations

import hashlib
import io
import math
import os
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, cast

import pandas as pd

from ai_trading_system.contracts.composer_prospective_capture import (
    CAPTURE_POLICY_PATH,
    CAPTURE_POLICY_SHA256,
    CLOCK_STAGES,
    ComposerCaptureError,
    ComposerCaptureRequest,
    ComposerCompletionAcknowledgement,
    ComposerOwnerReview,
    require,
    validate_composer_clock_prefix,
)
from ai_trading_system.contracts.host_clock_evidence import (
    HostClockEvidence,
    deadline_allows,
    require_clock_evidence_extension,
    utc_ns_to_datetime_ceil,
    utc_ns_to_datetime_floor,
)
from ai_trading_system.contracts.named_data_quality_execution import (
    COMPOSER_INPUT_POLICY_PATH,
    COMPOSER_INPUT_POLICY_SHA256,
    COMPOSER_SCOPE_ORDER,
    COMPOSER_SCOPE_SPECS,
    NamedArtifactBinding,
    NamedComposerInputScope,
    VerifiedNamedInputs,
)
from ai_trading_system.contracts.named_execution_context import require_named_execution_context
from ai_trading_system.contracts.prospective_capture_execution import RecorderReturnObservation
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
    dispatch_named_quality_child,
    recheck_named_capture_lease,
    restore_named_capture_lease,
    verify_retained_named_capture_proof,
)
from ai_trading_system.data.named_quality_execution import (
    NamedBootstrapAuthority,
    verify_named_data_quality_execution_receipt,
)
from ai_trading_system.first_layer_composer_v2_current_session_producer import (
    build_known_snapshot_preview,
    load_current_session_producer_policy,
)
from ai_trading_system.first_layer_operational_forecast import _json_number
from ai_trading_system.host_clock_evidence import HostClockSampler
from ai_trading_system.platform.architecture.checkout_guard import CheckoutLeaseHandle
from ai_trading_system.prospective_event_time_evidence import (
    RecordedTemporalEvidence,
    TimeEvidencePolicy,
    first_feature_session,
    load_time_evidence_policy,
    record_activation,
    record_local_input_observation,
    record_signal_completion,
    session_timing,
    verify_recorder_return_evidence,
    verify_time_evidence,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day

_ATTEMPT_SCHEMA = "composer_prospective_attempt.v1"
_RESULT_SCHEMA = "composer_prospective_capture_result.v1"
_CURRENT_POLICY_PATH = "config/research/first_layer_composer_v2_current_session_producer_v1.yaml"


def _now() -> datetime:
    return datetime.now(UTC)


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _fit_audit_json(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """Preserve the producer's explicit Infinity sentinel without non-JSON numbers."""
    return [
        {
            str(key): (
                _json_number(value) if isinstance(value, float) and math.isinf(value) else value
            )
            for key, value in row.items()
        }
        for row in frame.to_dict(orient="records")
    ]


def _object(value: object) -> dict[str, Any]:
    require(
        type(value) is dict and all(type(key) is str for key in value), "strict string-key object"
    )
    return cast(dict[str, Any], value)


def _json(content: bytes) -> dict[str, Any]:
    result = _object(strict_json_loads(content))
    require(canonical_json_bytes(result) == content, "canonical JSON bytes")
    return result


def _same(left: object, right: object, detail: str) -> None:
    require(
        canonical_json_bytes(left) == canonical_json_bytes(right),
        detail,
        "COMPOSER_RETAINED_BINDING_MISMATCH",
    )


def _binding(relative: str, content: bytes) -> NamedArtifactBinding:
    return NamedArtifactBinding("EXECUTION", relative, _sha(content), len(content))


def _read(root: Path, binding: NamedArtifactBinding) -> bytes:
    require(binding.root_role == "EXECUTION", "contained execution artifact")
    content = read_contained_artifact_bytes(root=root, relative_path=binding.relative_path)
    require(
        _binding(binding.relative_path, content) == binding,
        binding.relative_path,
        "COMPOSER_ARTIFACT_DRIFT",
    )
    return content


def _write(root: Path, relative: str, content: bytes) -> NamedArtifactBinding:
    write_contained_artifact_bytes(
        root=root, relative_path=relative, content=content, immutable=True
    )
    result = _binding(relative, content)
    require(_read(root, result) == content, "post-write bytes")
    return result


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
    NamedArtifactBinding("EXECUTION", relative, "0" * 64, 0)
    target = root / relative
    with (
        _root_authority(root),
        _bound_directory(root, target, "Composer capture store", create=True),
    ):
        pass
    return target


def _activation_request(request: ComposerCaptureRequest) -> ComposerCaptureRequest:
    return replace(request, operation="activate", feature_session=None, named_dq_requests=())


def _controls(
    request: ComposerCaptureRequest, bootstrap: NamedBootstrapAuthority
) -> tuple[ComposerOwnerReview, tuple[PayloadMember, ...], RecordingPlan, TimeEvidencePolicy]:
    context = require_named_execution_context()
    require(
        bootstrap.context is context
        and bootstrap.canonical_dq_call_count == 0
        and bootstrap.operation in {"composer-activate", "composer-readiness", "composer-capture"}
        and context.identity.execution_root == request.roots.execution_root
        and context.identity.candidate_commit == request.candidate_commit
        and context.identity.source_manifest_path == request.source_manifest_path
        and context.identity.source_manifest_sha256 == request.source_manifest_sha256,
        "same zero-DQ Composer parent context",
        "COMPOSER_CONTEXT_MISMATCH",
    )
    bootstrap.assert_execution_unchanged(stage="COMPOSER_PRE_CONTROL")
    root = Path(request.roots.execution_root)
    review_bytes = _read(root, request.owner_review)
    review = ComposerOwnerReview.from_json_bytes(review_bytes)
    review.assert_manifest(request.manifest)
    for path, digest in (
        (CAPTURE_POLICY_PATH, CAPTURE_POLICY_SHA256),
        (COMPOSER_INPUT_POLICY_PATH, COMPOSER_INPUT_POLICY_SHA256),
    ):
        dependency = bootstrap.dependencies.get(path)
        require(
            dependency is not None and _sha(dependency.content) == digest,
            path,
            "COMPOSER_POLICY_MISMATCH",
        )
    policy = load_time_evidence_policy(source_root=root)
    require(
        all(
            is_us_equity_trading_day(day)
            for day in (
                *request.manifest.allowed_feature_sessions,
                request.manifest.readiness_as_of,
            )
        ),
        "canonical XNYS session allowlist",
    )
    timing = session_timing(request.manifest.allowed_feature_sessions[0], policy=policy)
    require(
        request.manifest.expires_at <= timing.effective_close_at,
        "manifest cannot extend beyond decision close",
    )
    definitions = [
        PayloadMember("run_manifest", request.manifest.canonical_bytes),
        PayloadMember("owner_review", review_bytes),
        PayloadMember("execution_identity", canonical_json_bytes(context.identity.to_dict())),
    ]
    for ordinal, (_path, value) in enumerate(sorted(bootstrap.dependencies.items())):
        definitions.append(PayloadMember(f"definition_{ordinal:03d}", value.content))
    activation_attempt = _optional(
        root, request.manifest.output_relative_path + "/activate/attempt.json"
    )
    if activation_attempt is not None:
        attempt = _json(activation_attempt)
        require(
            attempt.get("schema_version") == _ATTEMPT_SCHEMA
            and attempt.get("request_sha256") == _activation_request(request).canonical_sha256
            and attempt.get("candidate_commit") == request.candidate_commit
            and attempt.get("execution_identity_sha256") == context.stable_identity_sha256,
            "unchanged activation attempt",
            "COMPOSER_ACTIVATION_ATTEMPT_DRIFT",
        )
        definitions.append(PayloadMember("activation_attempt", activation_attempt))
    elif request.operation == "capture":
        raise ComposerCaptureError("COMPOSER_ACTIVATION_REQUIRED", "no original activation attempt")
    members = tuple(sorted(definitions, key=lambda item: item.role))
    plan = RecordingPlan(
        plan_id=request.manifest.manifest_id,
        consumer_family="COMPOSER",
        declared_source_commit=request.candidate_commit,
        definition_bindings=tuple(item.binding for item in members),
    )
    bootstrap.assert_execution_unchanged(stage="COMPOSER_POST_CONTROL")
    return review, members, plan, policy


def _check_live(
    request: ComposerCaptureRequest,
    bootstrap: NamedBootstrapAuthority,
    lease: CheckoutLeaseHandle,
    review: ComposerOwnerReview,
) -> dict[str, Any]:
    require(
        review.reviewed_at <= _now() < request.manifest.expires_at,
        "review/expiry is not live",
        "COMPOSER_SCOPE_NOT_LIVE",
    )
    source_checked = parse_utc_datetime(bootstrap.assert_execution_unchanged(stage="COMPOSER_LIVE"))
    require(
        bootstrap.source_lease_id == lease.lease_id and bootstrap.canonical_dq_call_count == 0,
        "zero-DQ original parent lease",
    )
    proof = recheck_named_capture_lease(
        lease,
        candidate_commit=request.candidate_commit,
        required_paths=request.required_write_paths,
    )
    require(
        source_checked <= parse_utc_datetime(proof["checked_at"]), "source/lease clock rollback"
    )
    return {
        **proof,
        "source_checked_at": source_checked.isoformat(),
        "request_sha256": request.canonical_sha256,
        "parent_pid": os.getpid(),
        "parent_canonical_dq_call_count": 0,
        "parent_execution_identity_sha256": (
            require_named_execution_context().stable_identity_sha256
        ),
    }


def _check_retained_proof(
    request: ComposerCaptureRequest, proof: dict[str, Any], attempt: dict[str, Any]
) -> None:
    for key, value in {
        "request_sha256": request.canonical_sha256,
        "parent_pid": attempt["parent_pid"],
        "parent_canonical_dq_call_count": 0,
        "parent_execution_identity_sha256": (
            require_named_execution_context().stable_identity_sha256
        ),
    }.items():
        _same(proof.get(key), value, "retained source proof: " + key)
    require(
        parse_utc_datetime(proof["source_checked_at"]) <= parse_utc_datetime(proof["checked_at"]),
        "retained source chronology",
    )
    verify_retained_named_capture_proof(
        proof,
        execution_root=Path(request.roots.execution_root),
        candidate_commit=request.candidate_commit,
        required_paths=request.required_write_paths,
        source_lease_id=attempt["source_lease_id"],
        checked_at=parse_utc_datetime(proof["checked_at"]),
    )


def _clock_scope(
    request: ComposerCaptureRequest, clock: HostClockEvidence, proof: dict[str, Any]
) -> None:
    require(
        deadline_allows(clock.admission_bound_ns, request.manifest.expires_at)
        and deadline_allows(
            clock.admission_bound_ns, parse_utc_datetime(proof["active_lease"]["expires_at"])
        )
        and parse_utc_datetime(proof["checked_at"])
        <= utc_ns_to_datetime_floor(clock.latest_sample.utc_ns),
        "manifest/lease conservative clock bounds",
        "COMPOSER_CLOCK_SCOPE_EXPIRED",
    )


def _reserve(
    request: ComposerCaptureRequest,
    bootstrap: NamedBootstrapAuthority,
    lease: CheckoutLeaseHandle,
    review: ComposerOwnerReview,
) -> bool:
    _check_live(request, bootstrap, lease, review)
    root = Path(request.roots.execution_root)
    directory = _directory(root, request.operation_relative_path)
    with exclusive_store_maintenance(store_root=directory):
        existing = _optional(directory, "attempt.json")
        if existing is not None:
            require(
                _json(existing).get("request_sha256") == request.canonical_sha256,
                "same stage different request",
                "COMPOSER_ATTEMPT_COLLISION",
            )
            return False
        _write(directory, "request.json", request.canonical_bytes)
        _write(
            directory,
            "attempt.json",
            canonical_json_bytes(
                {
                    "schema_version": _ATTEMPT_SCHEMA,
                    "request_id": request.request_id,
                    "request_sha256": request.canonical_sha256,
                    "manifest_sha256": request.manifest.canonical_sha256,
                    "candidate_commit": request.candidate_commit,
                    "execution_identity_sha256": (
                        require_named_execution_context().stable_identity_sha256
                    ),
                    "parent_pid": os.getpid(),
                    "source_lease_id": lease.lease_id,
                    "started_at": _now().isoformat(),
                    "retry_allowed": False,
                }
            ),
        )
    _check_live(request, bootstrap, lease, review)
    return True


def _attempt(request: ComposerCaptureRequest) -> dict[str, Any] | None:
    root = Path(request.roots.execution_root)
    content = _optional(root, request.operation_relative_path + "/attempt.json")
    if content is None:
        return None
    value = _json(content)
    expected = {
        "schema_version": _ATTEMPT_SCHEMA,
        "request_id": request.request_id,
        "request_sha256": request.canonical_sha256,
        "manifest_sha256": request.manifest.canonical_sha256,
        "candidate_commit": request.candidate_commit,
        "execution_identity_sha256": require_named_execution_context().stable_identity_sha256,
        "retry_allowed": False,
    }
    require(
        set(value) == {*expected, "parent_pid", "source_lease_id", "started_at"},
        "exact retained attempt fields",
    )
    for key, item in expected.items():
        _same(value[key], item, "attempt: " + key)
    require(type(value["parent_pid"]) is int and value["parent_pid"] > 0, "original PID")
    require(
        _optional(root, request.operation_relative_path + "/request.json")
        == request.canonical_bytes,
        "original request bytes",
    )
    parse_utc_datetime(value["started_at"])
    return value


def _event_binding(request: ComposerCaptureRequest, event: EventBinding) -> NamedArtifactBinding:
    return NamedArtifactBinding(
        "EXECUTION",
        request.timing_relative_path + "/" + event.relative_path,
        event.sha256,
        event.size_bytes,
    )


def _local_event(request: ComposerCaptureRequest, binding: NamedArtifactBinding) -> EventBinding:
    prefix = request.timing_relative_path + "/"
    require(
        binding.root_role == "EXECUTION" and binding.relative_path.startswith(prefix),
        "contained event locator",
    )
    return EventBinding(binding.relative_path[len(prefix) :], binding.sha256, binding.size_bytes)


def _event_members(
    request: ComposerCaptureRequest,
    binding: NamedArtifactBinding,
    *,
    policy: TimeEvidencePolicy,
    source_lease_id: str,
) -> tuple[RecordedTemporalEvidence, dict[str, bytes]]:
    store = Path(request.roots.execution_root) / request.timing_relative_path
    local = _local_event(request, binding)
    event = verify_time_evidence(store_root=store, event=local, policy=policy)

    def bound_content(item: EventBinding) -> bytes:
        content = read_contained_artifact_bytes(root=store, relative_path=item.relative_path)
        require(
            len(content) == item.size_bytes and _sha(content) == item.sha256, "event member bytes"
        )
        return content

    witness = _json(bound_content(local))
    slot = PurePosixPath(local.relative_path).parent
    intent_binding = EventBinding.from_dict(witness["intent"])
    require(intent_binding.relative_path == (slot / "intent.json").as_posix(), "same-slot intent")
    intent = _json(bound_content(intent_binding))
    lease_binding = EventBinding.from_dict(intent["lease_event"])
    require(
        lease_binding.relative_path == (slot / "lease_event.json").as_posix(), "same-slot lease"
    )
    lease = _object(strict_json_loads(bound_content(lease_binding)))
    require(_object(lease["lease"]).get("lease_id") == source_lease_id, "original recorder lease")
    result = {}
    for row in intent["semantic"]["payload_bindings"]:
        PayloadMember(row["role"], b"")
        artifact = EventBinding.from_dict(row["artifact"])
        require(
            artifact.relative_path == (slot / ("payload_" + row["role"] + ".bin")).as_posix(),
            "fixed payload locator",
        )
        result[row["role"]] = bound_content(artifact)
    return event, result


def _aggregate(
    request: ComposerCaptureRequest, verified: tuple[VerifiedNamedInputs, ...]
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], tuple[PayloadMember, ...]]:
    """All three sealed scopes precede any parse/fit; no source path re-open."""
    require(len(verified) == len(COMPOSER_SCOPE_ORDER), "complete strict-PASS segment set")
    policy_binding = next(
        (
            row
            for row in verified[0].receipt.execution_dependencies
            if row.relative_path == COMPOSER_INPUT_POLICY_PATH
        ),
        None,
    )
    require(policy_binding is not None, "captured input policy")
    assert policy_binding is not None and request.feature_session is not None
    captured = []
    closure = []
    rows = []
    source_price_sha = source_rates_sha = None
    for segment, item, dq_request in zip(
        COMPOSER_SCOPE_ORDER, verified, request.named_dq_requests, strict=True
    ):
        require(
            type(item) is VerifiedNamedInputs and item.receipt.request == dq_request,
            "original verified segment identity",
        )
        price_content, rate_content, sessions, next_session = item.inputs_for_composer_segment(
            required_scope=NamedComposerInputScope(request.feature_session, segment, policy_binding)
        )
        identity = (_sha(price_content), _sha(rate_content))
        if source_price_sha is None:
            source_price_sha, source_rates_sha = identity
        require(
            identity == (source_price_sha, source_rates_sha),
            "one price/rates snapshot across all segments",
        )
        captured.append((segment, price_content, rate_content, sessions, next_session))
        for role, content in item.recording_closure_for_composer():
            closure.append(PayloadMember(segment + "_" + role, content))
        receipt = item.receipt
        rows.append(
            {
                "segment": segment,
                "request_id": dq_request.request_id,
                "receipt_id": receipt.receipt_id,
                "receipt_sha256": receipt.canonical_sha256,
                "requested_window": dq_request.scope.requested_window.to_dict(),
                "evaluated_window": receipt.evaluated_window.to_dict(),
                "dq_status": receipt.report.status,
                "price_consistency_start_date": receipt.price_consistency_start_date.isoformat(),
                "rate_consistency_start_date": receipt.rate_consistency_start_date.isoformat(),
                "policy": receipt.policy.to_dict(),
                "input_sha256": {i.role: i.member.sha256 for i in receipt.inputs},
            }
        )
    # Only required instrument/date projections are parsed into model frames.
    price_rows = pd.read_csv(io.BytesIO(captured[0][1]))
    rate_rows = pd.read_csv(io.BytesIO(captured[0][2]))
    require({"ticker", "date", "adj_close"}.issubset(price_rows.columns), "price schema")
    require({"series", "date", "value"}.issubset(rate_rows.columns), "rates schema")
    selected_prices = price_rows.loc[
        price_rows["ticker"].isin(("QQQ", "TQQQ", "SHY", "SGOV"))
    ].copy()
    selected_rates = rate_rows.loc[rate_rows["series"].isin(("DGS2", "DGS10", "DTWEXBGS"))].copy()
    end = pd.Timestamp(request.feature_session)
    for frame, key in ((selected_prices, "ticker"), (selected_rates, "series")):
        parsed = pd.to_datetime(frame["date"], errors="coerce")
        require(
            bool(parsed.notna().all())
            and not (parsed > end).any()
            and not frame.duplicated([key, "date"]).any(),
            "invalid, future or duplicate source row",
            "COMPOSER_INPUT_ROWS_INVALID",
        )
        frame["date"] = parsed
    selected_prices = selected_prices.loc[
        (selected_prices["date"] >= pd.Timestamp("2018-01-02"))
        & (
            (selected_prices["ticker"] != "SGOV")
            | (selected_prices["date"] >= pd.Timestamp("2020-05-28"))
        )
    ]
    selected_rates = selected_rates.loc[selected_rates["date"] >= pd.Timestamp("2018-01-02")]
    prices = selected_prices.pivot(index="date", columns="ticker", values="adj_close").sort_index()
    rates = selected_rates.pivot(index="date", columns="series", values="value").sort_index()
    require(
        set(prices.columns) == {"QQQ", "TQQQ", "SHY", "SGOV"}
        and set(rates.columns) == {"DGS2", "DGS10", "DTWEXBGS"},
        "complete model instruments",
    )
    for segment, _, _, sessions, next_session in captured:
        _, tickers, _ = COMPOSER_SCOPE_SPECS[segment]
        require(
            not prices.reindex(pd.DatetimeIndex(sessions))[list(tickers)].isna().any().any(),
            "complete requested price coverage",
            "COMPOSER_PRICE_COVERAGE_INCOMPLETE",
        )
        require(next_session == captured[0][4], "same next-session calendar")
    aggregate = {
        "schema_version": "composer_segmented_dq_identity.v1",
        "feature_session": request.feature_session.isoformat(),
        "decision_effective_session": captured[0][4].isoformat(),
        "scope_order": list(COMPOSER_SCOPE_ORDER),
        "segments": rows,
        "input_policy_sha256": COMPOSER_INPUT_POLICY_SHA256,
        "prices_content_sha256": source_price_sha,
        "rates_content_sha256": source_rates_sha,
        "source_snapshot": request.named_dq_requests[0].selector.to_dict(),
        "execution_identity_sha256": require_named_execution_context().stable_identity_sha256,
        "status": "PASS",
        "historical_provider_available_at": "NOT_ESTABLISHED",
        "historical_pit_claim_allowed": False,
        "future_observation_outcome_access_allowed": False,
    }
    closure.append(PayloadMember("segmented_dq_identity", canonical_json_bytes(aggregate)))
    return prices, rates, aggregate, tuple(sorted(closure, key=lambda member: member.role))


def _dq_row(segment: str, result: NamedQualityDispatchResult) -> dict[str, Any]:
    return {
        "segment": segment,
        "status": result.status,
        "receipt_path": result.receipt_path,
        "receipt_sha256": result.receipt_sha256,
        "run_dispatch_path": result.run_dispatch_path,
        "run_dispatch_sha256": result.run_dispatch_sha256,
        "parent_receipt": result.parent_receipt.to_dict(),
        "canonical_dq_call_count": result.canonical_dq_call_count,
        "counter_observation_state": result.counter_observation_state,
        "returncode": result.returncode,
        "terminal_state": result.terminal_state,
    }


def _base_result(request: ComposerCaptureRequest, review: ComposerOwnerReview) -> dict[str, Any]:
    return {
        "schema_version": _RESULT_SCHEMA,
        "request_id": request.request_id,
        "request_sha256": request.canonical_sha256,
        "manifest_sha256": request.manifest.canonical_sha256,
        "candidate_commit": request.candidate_commit,
        "operation": request.operation,
        "feature_session": request.feature_session.isoformat() if request.feature_session else None,
        "authorization_state": review.authorization_state,
        "evidence_purpose": review.evidence_purpose,
        "status": "BLOCKED",
        "technical_validation_state": "NOT_COMPLETED",
        "activation_admitted": False,
        "capture_admitted": False,
        "real_observation_admitted": False,
        "parent_canonical_dq_call_count": 0,
        "model_fit_call_count": 0,
        "dq_dispatches": [],
        "historical_training_accessed": False,
        "future_observation_outcome_access_allowed": False,
        "future_observation_outcome_accessed": False,
        "provider_action_count": 0,
        "cache_mutation_count": 0,
        "data_download_count": 0,
        "orders": 0,
        "fills": 0,
        "positions": 0,
        "maturity_update_count": 0,
        "scoreboard_count": 0,
        "retry_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _original_events(
    request: ComposerCaptureRequest,
    ack: ComposerCompletionAcknowledgement,
    plan: RecordingPlan,
    policy: TimeEvidencePolicy,
) -> tuple[dict[str, bytes], ...]:
    expected_slots = (
        ("activation",)
        if request.operation == "activate"
        else (
            f"sessions/{request.feature_session}/inputs",
            f"sessions/{request.feature_session}/signal",
        )
    )
    require(len(ack.recorder_returns) == len(expected_slots), "all original recorder returns")
    result = []
    for returned, slot in zip(ack.recorder_returns, expected_slots, strict=True):
        expected = f"{request.timing_relative_path}/streams/{plan.stream_id}/{slot}/completion.json"
        require(returned.event.relative_path == expected, "exact stream/event slot")
        event, payloads = _event_members(
            request, returned.event, policy=policy, source_lease_id=ack.source_lease_id
        )
        verified = verify_recorder_return_evidence(
            store_root=Path(request.roots.execution_root) / request.timing_relative_path,
            event=_local_event(request, returned.event),
            return_clock_evidence=returned.clock_evidence,
            policy=policy,
        )
        require(event.plan == plan and verified.binding == event.binding, "original event plan")
        result.append(payloads)
    return tuple(result)


def _verify_dispatch_rows(
    request: ComposerCaptureRequest, rows: list[dict[str, Any]], bootstrap: NamedBootstrapAuthority
) -> tuple[VerifiedNamedInputs, ...]:
    require(len(rows) <= len(COMPOSER_SCOPE_ORDER), "bounded dispatch rows")
    verified = []
    for segment, dq_request, row in zip(
        COMPOSER_SCOPE_ORDER, request.named_dq_requests, rows, strict=False
    ):
        require(row["segment"] == segment, "original fixed dispatch sequence")
        parent_binding = NamedArtifactBinding.from_dict(row["parent_receipt"])
        parent = _json(_read(Path(request.roots.execution_root), parent_binding))
        require(
            parent_binding.relative_path
            == request.operation_relative_path + "/dq_dispatch/" + segment + "/parent_receipt.json",
            "fixed child parent locator",
        )
        _verify_dispatch_parent(request, dq_request, row, parent)
        # The named verifier checks the full original terminal/receipt/guard chain
        # for every successful row. Failed rows cannot mint a feature capability.
        if row["status"] == "PASS":
            require(
                row["canonical_dq_call_count"] == 1
                and row["counter_observation_state"] == "KNOWN"
                and row["returncode"] == 0
                and row["terminal_state"] == "EXITED",
                "strict original child PASS",
            )
            verified.append(
                verify_named_data_quality_execution_receipt(
                    dq_request,
                    receipt_path=row["receipt_path"],
                    receipt_sha256=row["receipt_sha256"],
                    run_dispatch_path=row["run_dispatch_path"],
                    run_dispatch_sha256=row["run_dispatch_sha256"],
                    bootstrap=bootstrap,
                )
            )
        else:
            require(
                parent.get("request_id") == dq_request.request_id
                and parent.get("status") != "PASS",
                "failed child identity",
            )
    return tuple(verified)


def _verify_dispatch_parent(
    request: ComposerCaptureRequest,
    dq_request: Any,
    row: dict[str, Any],
    parent: dict[str, Any],
) -> None:
    """Bind observed counters even for children that cannot grant an input seal."""
    for key, value in {
        "schema_version": "named_data_quality_parent_dispatch.v1",
        "profile": "COMPOSER_PROSPECTIVE_PRODUCTION_PARENT",
        "parent_operation": "composer-" + request.operation,
        "candidate_commit": request.candidate_commit,
        "execution_root": request.roots.execution_root,
        "request_id": dq_request.request_id,
        "observed_canonical_dq_call_count": row["canonical_dq_call_count"],
        "counter_observation_state": row["counter_observation_state"],
        "returncode": row["returncode"],
        "terminal_state": row["terminal_state"],
        "parent_canonical_dq_call_count": 0,
    }.items():
        _same(parent.get(key), value, "original child parent: " + key)
    root = Path(request.roots.execution_root)
    binding = _object(parent["request"])
    segment = COMPOSER_SCOPE_ORDER[request.named_dq_requests.index(dq_request)]
    relative = request.operation_relative_path + "/dq_dispatch/" + segment + "/request.json"
    require(binding["path"] == (root / relative).as_posix(), "fixed original child request locator")
    content = read_contained_artifact_bytes(root=root, relative_path=relative)
    require(
        len(content) == binding["size_bytes"]
        and _sha(content) == binding["sha256"]
        and content == dq_request.canonical_bytes,
        "original child request bytes",
    )


def _actual_counter(result: dict[str, Any]) -> tuple[int | None, str]:
    counts = [row["canonical_dq_call_count"] for row in result["dq_dispatches"]]
    terminal = result.get("failed_dispatch_terminal_observation")
    if terminal is not None:
        counts.append(terminal["canonical_dq_call_count"])
    known = result.get("unobserved_dispatch") is False and all(
        type(value) is int and value >= 0 for value in counts
    )
    return (sum(counts), "KNOWN") if known else (None, "UNKNOWN")


def verify_composer_capture_result(
    request: ComposerCaptureRequest, *, bootstrap: NamedBootstrapAuthority
) -> dict[str, Any]:
    """Exact-key retained replay; no DQ, model fit, event or outcome writes."""
    result = _json(
        read_contained_artifact_bytes(
            root=Path(request.roots.execution_root),
            relative_path=request.operation_relative_path + "/result.json",
        )
    )
    return _verify_composer_projection(request, result, bootstrap=bootstrap)


def _verify_composer_projection(
    request: ComposerCaptureRequest,
    result: dict[str, Any],
    *,
    bootstrap: NamedBootstrapAuthority,
    terminal_required: bool = True,
) -> dict[str, Any]:
    review, definitions, plan, policy = _controls(request, bootstrap)
    root = Path(request.roots.execution_root)
    attempt = _attempt(request)
    require(attempt is not None, "original attempt required")
    assert attempt is not None
    for key, value in _base_result(request, review).items():
        if key not in {
            "status",
            "technical_validation_state",
            "activation_admitted",
            "capture_admitted",
            "real_observation_admitted",
            "model_fit_call_count",
            "dq_dispatches",
            "historical_training_accessed",
        }:
            _same(result.get(key), value, "fixed result: " + key)
    _same(result["source_lease_id"], attempt["source_lease_id"], "original source lease")
    _same(result["parent_pid"], attempt["parent_pid"], "original source PID")
    if terminal_required:
        proof = _object(result["terminal_precommit_proof"])
        _check_retained_proof(request, proof, attempt)
        terminal = HostClockEvidence.from_dict(result["terminal_clock_evidence"])
        require(
            bool(terminal.checkpoints)
            and terminal.checkpoints[-1].label == "terminal_precommit"
            and terminal.checkpoints[-1].inherited_child_bound_ns is None,
            "terminal original clock",
        )
        prefix = replace(terminal, checkpoints=terminal.checkpoints[:-1])
        _clock_scope(request, terminal, proof)
        require(
            parse_utc_datetime(attempt["started_at"])
            <= utc_ns_to_datetime_floor(terminal.anchor.utc_ns)
            <= parse_utc_datetime(result["completed_at"])
            <= parse_utc_datetime(proof["source_checked_at"]),
            "original terminal chronology",
        )
    else:
        prefix = HostClockEvidence.from_dict(result["parent_clock_evidence"])
    _same(result["parent_clock_evidence"], prefix.to_dict(), "original terminal prefix projection")
    returns = tuple(RecorderReturnObservation.from_dict(row) for row in result["recorder_returns"])
    validate_composer_clock_prefix(prefix, returns, request.operation)
    rows = result["dq_dispatches"]
    require(type(rows) is list, "original child rows")
    verified = _verify_dispatch_rows(request, rows, bootstrap)
    failed = result.get("failed_dispatch_terminal_observation")
    if failed is not None:
        require(
            result["status"] == "BLOCKED" and len(rows) < len(request.named_dq_requests),
            "one terminal failed child after successful prefix",
        )
        _same(
            failed["evidence_state"],
            (
                "PARENT_BINDING_VERIFIED"
                if failed["parent_receipt"] is not None
                else "ORIGINAL_PARENT_OBSERVATION_ONLY"
            ),
            "failed counter evidence scope",
        )
        if failed["parent_receipt"] is not None:
            binding = NamedArtifactBinding.from_dict(failed["parent_receipt"])
            segment = COMPOSER_SCOPE_ORDER[len(rows)]
            require(
                binding.relative_path
                == request.operation_relative_path
                + "/dq_dispatch/"
                + segment
                + "/parent_receipt.json",
                "original failed child parent locator",
            )
            _verify_dispatch_parent(
                request, request.named_dq_requests[len(rows)], failed, _json(_read(root, binding))
            )
    count, counter_state = _actual_counter(result)
    _same(result["canonical_dq_call_count"], count, "actual aggregate counter")
    _same(result["counter_observation_state"], counter_state, "actual counter state")
    _same(
        result["real_market_dq_call_count"],
        count if review.evidence_purpose == "PROSPECTIVE_RESEARCH" else 0,
        "actual market counter",
    )
    require(type(result["unobserved_dispatch"]) is bool, "explicit dispatch observation flag")
    require(
        type(result["model_fit_call_count"]) is int
        and result["model_fit_call_count"] in (0, 1)
        and (request.operation == "capture" or result["model_fit_call_count"] == 0),
        "bounded original fit count",
    )
    _same(
        result["historical_training_accessed"],
        result["model_fit_call_count"] == 1,
        "original historical fit access",
    )
    require(
        count is None or 0 <= count <= (0 if request.operation == "activate" else 3),
        "stage action maximum",
    )
    status = result["status"]
    if status in {"READINESS_PASS", "CAPTURED"}:
        require(len(verified) == 3 and count == 3, "three original strict PASS children")
        _, _, aggregate, closure = _aggregate(request, verified)
        identity_binding = NamedArtifactBinding.from_dict(result["segmented_dq_identity"])
        require(
            identity_binding.relative_path
            == request.operation_relative_path + "/segmented_dq_identity.json",
            "fixed aggregate locator",
        )
        _same(_json(_read(root, identity_binding)), aggregate, "original DQ aggregate")
    if status in {"ACTIVATED", "CAPTURED"}:
        binding = NamedArtifactBinding.from_dict(result["acknowledgement"])
        require(
            binding.relative_path == request.operation_relative_path + "/acknowledgement.json",
            "fixed acknowledgement locator",
        )
        ack = ComposerCompletionAcknowledgement.from_json_bytes(_read(root, binding))
        _same(
            result["technical_validation_state"],
            ack.technical_validation_state,
            "acknowledged technical state",
        )
        for key, value in {
            "request_id": request.request_id,
            "request_sha256": request.canonical_sha256,
            "manifest_sha256": request.manifest.canonical_sha256,
            "candidate_commit": request.candidate_commit,
            "execution_identity_sha256": require_named_execution_context().stable_identity_sha256,
            "source_lease_id": attempt["source_lease_id"],
            "operation": request.operation,
            "feature_session": request.feature_session,
            "authorization_state": review.authorization_state,
            "evidence_purpose": review.evidence_purpose,
        }.items():
            require(getattr(ack, key) == value, "acknowledgement identity: " + key)
        if terminal_required:
            require_clock_evidence_extension(ack.clock_evidence, terminal)
        require(
            ack.clock_evidence == prefix and ack.recorder_returns == returns,
            "original complete acknowledgement prefix",
        )
        payloads = _original_events(request, ack, plan, policy)
        if status == "ACTIVATED":
            require(
                request.operation == "activate"
                and count == 0
                and result["model_fit_call_count"] == 0,
                "activation zero DQ/model",
            )
            _same(
                {member.role: member.content.hex() for member in definitions},
                {role: content.hex() for role, content in payloads[0].items()},
                "activation definitions",
            )
            first = first_feature_session(
                utc_ns_to_datetime_ceil(ack.clock_evidence.admission_bound_ns), policy=policy
            )
            require(
                first == ack.first_feature_session == request.manifest.allowed_feature_sessions[0],
                "actual activation first F",
            )
            _same(result["first_feature_session"], first.isoformat(), "activation result first F")
            require(
                result["activation_admitted"] is True
                and result["capture_admitted"] is False
                and result["real_observation_admitted"] is False,
                "activation admission projection",
            )
        else:
            require(
                request.operation == "capture"
                and result["model_fit_call_count"] == 1
                and result["historical_training_accessed"] is True,
                "capture original fit count",
            )
            activation = verify_composer_capture_result(
                _activation_request(request), bootstrap=bootstrap
            )
            require(activation["status"] == "ACTIVATED", "original complete activation")
            activation_ack = ComposerCompletionAcknowledgement.from_json_bytes(
                _read(root, NamedArtifactBinding.from_dict(activation["acknowledgement"]))
            )
            require(
                ack.first_feature_session == activation_ack.first_feature_session,
                "original activation origin",
            )
            assert request.feature_session is not None
            timing = session_timing(request.feature_session, policy=policy)
            require(
                ack.decision_effective_session == timing.effective_session
                and ack.decision_deadline == timing.effective_close_at
                and utc_ns_to_datetime_floor(ack.clock_evidence.anchor.utc_ns)
                > timing.feature_close_at
                and deadline_allows(
                    ack.clock_evidence.admission_bound_ns, timing.effective_close_at
                ),
                "executable next-close timing",
            )
            expected_inputs = (
                *closure,
                PayloadMember("capture_request", request.canonical_bytes),
                PayloadMember("activation_ack", activation_ack.canonical_bytes),
            )
            _same(
                {member.role: member.content.hex() for member in expected_inputs},
                {role: content.hex() for role, content in payloads[0].items()},
                "original same-bytes input closure",
            )
            signal = _json(payloads[1]["signal"])
            _validate_signal_identity(request, signal, aggregate, ack.recorder_returns[0])
            observation_binding = NamedArtifactBinding.from_dict(result["observation"])
            require(
                observation_binding.relative_path
                == request.operation_relative_path + "/observation.json",
                "fixed observation locator",
            )
            _same(
                _json(_read(root, observation_binding)),
                _observation(request, binding, signal, review),
                "original observation projection",
            )
            require(
                result["capture_admitted"] is True
                and result["activation_admitted"] is False
                and result["real_observation_admitted"]
                == (review.evidence_purpose == "PROSPECTIVE_RESEARCH"),
                "capture admission projection",
            )
    elif status == "READINESS_PASS":
        _same(
            result["technical_validation_state"],
            "SEGMENTED_INPUT_DQ_PASS_NO_SIGNAL",
            "readiness technical state",
        )
        require(
            request.operation == "readiness"
            and result["model_fit_call_count"] == 0
            and result["historical_training_accessed"] is False
            and not returns
            and tuple(row.label for row in prefix.checkpoints) == CLOCK_STAGES["readiness"],
            "readiness is input inspection only",
        )
        require(
            not any(
                result[key]
                for key in ("activation_admitted", "capture_admitted", "real_observation_admitted")
            ),
            "readiness grants no observation",
        )
    else:
        _same(result["technical_validation_state"], "BLOCKED", "blocked technical state")
        require(
            status == "BLOCKED"
            and not any(
                result[key]
                for key in ("activation_admitted", "capture_admitted", "real_observation_admitted")
            ),
            "failed attempt cannot admit research",
        )
    bootstrap.assert_execution_unchanged(stage="COMPOSER_RETAINED_TERMINAL")
    return result


def _validate_signal_identity(
    request: ComposerCaptureRequest,
    signal: dict[str, Any],
    aggregate: dict[str, Any],
    input_return: RecorderReturnObservation,
) -> None:
    require(
        signal["schema_version"] == "composer_current_known_recorded_signal.v1",
        "recorded signal schema",
    )
    _same(signal["input_event"], input_return.event.to_dict(), "actual input witness")
    require(
        signal["local_known_admission_bound_ns"] == input_return.clock_evidence.admission_bound_ns,
        "actual R admission bound",
    )
    preview = _object(signal["preview"])
    identity = _object(preview["observation_identity_preview"])
    require(
        (
            preview["feature_session"] == request.feature_session.isoformat()
            if request.feature_session
            else False
        ),
        "feature identity",
    )
    require(
        identity["policy_sha256"] == COMPOSER_INPUT_POLICY_SHA256
        and identity["dq_receipt_sha256"] == _sha(canonical_json_bytes(aggregate))
        and identity["source_sha256"] == _sha(canonical_json_bytes(aggregate["source_snapshot"]))
        and preview["decision_date"] == aggregate["decision_effective_session"]
        and preview["input_policy_sha256"] == COMPOSER_INPUT_POLICY_SHA256
        and preview["historical_pit_claim_allowed"] is False
        and preview["future_observation_outcome_access"] is False
        and signal["rates_content_sha256"] == aggregate["rates_content_sha256"]
        and signal["future_observation_outcome_accessed"] is False,
        "known-snapshot/aggregate/actual source identity",
    )


def _observation(
    request: ComposerCaptureRequest,
    ack_binding: NamedArtifactBinding,
    signal: dict[str, Any],
    review: ComposerOwnerReview,
) -> dict[str, Any]:
    preview = _object(signal["preview"])
    return {
        "schema_version": "composer_known_snapshot_observation.v1",
        "manifest_id": request.manifest.manifest_id,
        "request_id": request.request_id,
        "feature_session": preview["feature_session"],
        "decision_effective_session": preview["decision_date"],
        "return_origin": "EFFECTIVE_SESSION_CLOSE",
        "trend_state": preview["trend_state"],
        "action": preview["action"],
        "producer_observation_identity": preview["observation_identity_preview"],
        "recorded_signal_sha256": _sha(canonical_json_bytes(signal)),
        "acknowledgement": ack_binding.to_dict(),
        "local_known_admission_bound_ns": signal["local_known_admission_bound_ns"],
        "historical_training_access": preview["historical_training_access"],
        "rate_disclosures": preview["rate_disclosures"],
        "input_policy_sha256": preview["input_policy_sha256"],
        "future_observation_outcome_accessed": False,
        "outcome_status": "MATURITY_NOT_EVALUATED",
        "authorization_state": review.authorization_state,
        "evidence_purpose": review.evidence_purpose,
        "historical_provider_available_at": "NOT_ESTABLISHED",
        "historical_pit_claim_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def bootstrap_worker(
    payload: Mapping[str, object], *, operation: str, bootstrap: NamedBootstrapAuthority
) -> dict[str, object]:
    request = ComposerCaptureRequest.from_dict(payload)
    require(
        operation == "composer-" + request.operation == bootstrap.operation,
        "exact routed Composer operation",
    )
    review, definitions, plan, policy = _controls(request, bootstrap)
    root = Path(request.roots.execution_root)
    old_attempt = _attempt(request)
    if old_attempt is not None:
        if _optional(root, request.operation_relative_path + "/result.json") is None:
            return {
                **_base_result(request, review),
                "status": "INCOMPLETE",
                "technical_validation_state": "INCOMPLETE_ORIGINAL_ATTEMPT",
                "canonical_dq_call_count": None,
                "counter_observation_state": "UNKNOWN",
                "idempotent_replay": True,
                "this_invocation_canonical_dq_call_count": 0,
            }
        return {
            **verify_composer_capture_result(request, bootstrap=bootstrap),
            "idempotent_replay": True,
            "this_invocation_canonical_dq_call_count": 0,
        }
    lease = restore_named_capture_lease(
        execution_root=root,
        source_lease_id=bootstrap.source_lease_id,
        candidate_commit=request.candidate_commit,
        required_paths=request.required_write_paths,
    )
    if not _reserve(request, bootstrap, lease, review):
        return bootstrap_worker(payload, operation=operation, bootstrap=bootstrap)
    result = _base_result(request, review)
    result.update(parent_pid=os.getpid(), source_lease_id=lease.lease_id, unobserved_dispatch=False)
    sampler: HostClockSampler | None = None
    returns: list[RecorderReturnObservation] = []
    ack: ComposerCompletionAcknowledgement | None = None

    def checkpoint(label: str, event: RecordedTemporalEvidence | None = None) -> HostClockEvidence:
        assert sampler is not None
        proof = _check_live(request, bootstrap, lease, review)
        bound = None
        if event is not None:
            require(event.return_clock_evidence is not None, "original recorder return required")
            assert event.return_clock_evidence is not None
            returned = RecorderReturnObservation(
                _event_binding(request, event.binding), event.return_clock_evidence
            )
            returns.append(returned)
            original = verify_recorder_return_evidence(
                store_root=root / request.timing_relative_path,
                event=event.binding,
                return_clock_evidence=returned.clock_evidence,
                policy=policy,
            )
            bound = original.admission_bound_ns
        sampler.recheck_policy()
        clock = sampler.checkpoint(label, inherited_child_bound_ns=bound)
        validate_composer_clock_prefix(clock, tuple(returns), request.operation)
        _clock_scope(request, clock, proof)
        return clock

    try:
        review, definitions, plan, policy = _controls(request, bootstrap)
        pre = _check_live(request, bootstrap, lease, review)
        sampler = HostClockSampler.start(source_root=root)
        require(
            utc_ns_to_datetime_floor(sampler.evidence.anchor.utc_ns)
            >= parse_utc_datetime(pre["checked_at"]),
            "parent starts after live preguard",
        )
        timing_root = _directory(root, request.timing_relative_path)
        activation_ack = None
        verified = []
        if request.operation != "activate":
            if request.operation == "capture":
                activation = verify_composer_capture_result(
                    _activation_request(request), bootstrap=bootstrap
                )
                require(activation["status"] == "ACTIVATED", "original complete activation")
                activation_ack = ComposerCompletionAcknowledgement.from_json_bytes(
                    _read(root, NamedArtifactBinding.from_dict(activation["acknowledgement"]))
                )
                assert request.feature_session is not None
                timing = session_timing(request.feature_session, policy=policy)
                require(
                    request.feature_session >= activation_ack.first_feature_session
                    and timing.feature_close_at
                    < utc_ns_to_datetime_floor(sampler.evidence.anchor.utc_ns)
                    and _now() < timing.effective_close_at,
                    "capture requires F close < now < D close",
                    "COMPOSER_OUTSIDE_FEATURE_WINDOW",
                )
            checkpoint("pre_dispatch")
            for segment, dq_request in zip(
                COMPOSER_SCOPE_ORDER, request.named_dq_requests, strict=True
            ):
                # The child writer treats evidence_root as an existing authority.
                # Create only the declared segment under the live parent lease.
                _check_live(request, bootstrap, lease, review)
                evidence_root = Path(dq_request.roots.evidence_root)
                _directory(root, evidence_root.relative_to(root).as_posix())
                result["unobserved_dispatch"] = True
                dq = dispatch_named_quality_child(
                    dq_request,
                    bootstrap=bootstrap,
                    lease=lease,
                    output_relative_path=request.operation_relative_path
                    + "/dq_dispatch/"
                    + segment,
                )
                result["unobserved_dispatch"] = False
                result["dq_dispatches"].append(_dq_row(segment, dq))
                require(
                    dq.status == "PASS"
                    and dq.canonical_dq_call_count == 1
                    and dq.counter_observation_state == "KNOWN",
                    segment,
                    "COMPOSER_DQ_CHILD_BLOCKED",
                )
                assert dq.receipt_path is not None and dq.receipt_sha256 is not None
                verified.append(
                    verify_named_data_quality_execution_receipt(
                        dq_request,
                        receipt_path=dq.receipt_path,
                        receipt_sha256=dq.receipt_sha256,
                        run_dispatch_path=dq.run_dispatch_path,
                        run_dispatch_sha256=dq.run_dispatch_sha256,
                        bootstrap=bootstrap,
                    )
                )
            checkpoint("dq_verified")
            prices, rates, aggregate, closure = _aggregate(request, tuple(verified))
            aggregate_bytes = canonical_json_bytes(aggregate)
            result["segmented_dq_identity"] = _write(
                root,
                request.operation_relative_path + "/segmented_dq_identity.json",
                aggregate_bytes,
            ).to_dict()
            if request.operation == "readiness":
                result.update(
                    status="READINESS_PASS",
                    technical_validation_state="SEGMENTED_INPUT_DQ_PASS_NO_SIGNAL",
                )
            else:
                assert activation_ack is not None and request.feature_session is not None
                checkpoint("pre_input_recorder")
                input_event = record_local_input_observation(
                    store_root=timing_root,
                    plan=plan,
                    feature_session=request.feature_session,
                    inputs=(
                        *closure,
                        PayloadMember("capture_request", request.canonical_bytes),
                        PayloadMember("activation_ack", activation_ack.canonical_bytes),
                    ),
                    activation=_local_event(request, activation_ack.recorder_event),
                    policy=policy,
                    lease_handle=lease,
                )
                checkpoint("inputs_return", input_event)
                loaded = load_current_session_producer_policy(project_root=root)
                require(
                    loaded.file_sha256
                    == _sha(bootstrap.dependencies[_CURRENT_POLICY_PATH].content),
                    "captured current-session policy",
                )
                result["historical_training_accessed"] = True
                result["model_fit_call_count"] = 1
                preview = build_known_snapshot_preview(
                    loaded_policy=loaded,
                    input_policy_content=bootstrap.dependencies[COMPOSER_INPUT_POLICY_PATH].content,
                    feature_session=request.feature_session,
                    prices=prices,
                    rates=rates,
                    data_quality_status="PASS",
                    dq_receipt_sha256=_sha(aggregate_bytes),
                    source_sha256=_sha(canonical_json_bytes(aggregate["source_snapshot"])),
                )
                checkpoint("fit_complete")
                input_return = returns[0]
                signal = {
                    "schema_version": "composer_current_known_recorded_signal.v1",
                    "preview": dict(preview.preview),
                    "fit_audit": _fit_audit_json(preview.fit_audit),
                    "producer_receipt": dict(preview.receipt),
                    "input_event": input_return.event.to_dict(),
                    "local_known_admission_bound_ns": (
                        input_return.clock_evidence.admission_bound_ns
                    ),
                    "rates_content_sha256": aggregate["rates_content_sha256"],
                    "future_observation_outcome_accessed": False,
                }
                _validate_signal_identity(request, signal, aggregate, input_return)
                event = record_signal_completion(
                    store_root=timing_root,
                    plan=plan,
                    feature_session=request.feature_session,
                    signal=PayloadMember("signal", canonical_json_bytes(signal)),
                    inputs=input_event.binding,
                    policy=policy,
                    lease_handle=lease,
                )
        else:
            checkpoint("pre_recorder")
            event = record_activation(
                store_root=timing_root,
                plan=plan,
                definitions=definitions,
                policy=policy,
                lease_handle=lease,
            )
        if request.operation != "readiness":
            checkpoint("witness_return", event)
            covered = checkpoint("ack_covered_through")
            first = (
                activation_ack.first_feature_session
                if activation_ack
                else first_feature_session(
                    utc_ns_to_datetime_ceil(covered.admission_bound_ns), policy=policy
                )
            )
            require(
                first == request.manifest.allowed_feature_sessions[0],
                "first F must follow actual complete activation",
                "COMPOSER_FIRST_FEATURE_CHANGED",
            )
            ack_timing = (
                session_timing(request.feature_session, policy=policy)
                if request.feature_session
                else None
            )
            state = (
                "ACTIVATION_ACKNOWLEDGED"
                if ack_timing is None
                else (
                    "CAPTURE_ACKNOWLEDGED"
                    if deadline_allows(covered.admission_bound_ns, ack_timing.effective_close_at)
                    else "LATE"
                )
            )
            ack = ComposerCompletionAcknowledgement(
                request_id=request.request_id,
                request_sha256=request.canonical_sha256,
                manifest_sha256=request.manifest.canonical_sha256,
                operation=request.operation,
                feature_session=request.feature_session,
                candidate_commit=request.candidate_commit,
                execution_identity_sha256=require_named_execution_context().stable_identity_sha256,
                source_lease_id=lease.lease_id,
                recorder_event=_event_binding(request, event.binding),
                clock_evidence=covered,
                recorder_returns=tuple(returns),
                first_feature_session=first,
                decision_effective_session=ack_timing.effective_session if ack_timing else None,
                decision_deadline=ack_timing.effective_close_at if ack_timing else None,
                authorization_state=review.authorization_state,
                evidence_purpose=review.evidence_purpose,
                technical_validation_state=state,
            )
            ack_binding = _write(
                root, request.operation_relative_path + "/acknowledgement.json", ack.canonical_bytes
            )
            result["acknowledgement"] = ack_binding.to_dict()
            require(
                state != "LATE", "signal completed after decision deadline", "COMPOSER_LATE_CAPTURE"
            )
            if request.operation == "activate":
                result.update(
                    status="ACTIVATED",
                    activation_admitted=True,
                    technical_validation_state=state,
                    first_feature_session=first.isoformat(),
                )
            else:
                result["observation"] = _write(
                    root,
                    request.operation_relative_path + "/observation.json",
                    canonical_json_bytes(_observation(request, ack_binding, signal, review)),
                ).to_dict()
                result.update(
                    status="CAPTURED",
                    capture_admitted=True,
                    real_observation_admitted=review.evidence_purpose == "PROSPECTIVE_RESEARCH",
                    technical_validation_state=state,
                )
    except (ValueError, OSError, RuntimeError, KeyError, TypeError, StopIteration) as exc:
        if isinstance(exc, NamedQualityDispatchError) and exc.terminal_observation is not None:
            result["unobserved_dispatch"] = False
            result["failed_dispatch_terminal_observation"] = {
                "evidence_state": (
                    "PARENT_BINDING_VERIFIED"
                    if exc.terminal_observation.parent_receipt is not None
                    else "ORIGINAL_PARENT_OBSERVATION_ONLY"
                ),
                "canonical_dq_call_count": exc.terminal_observation.canonical_dq_call_count,
                "counter_observation_state": exc.terminal_observation.counter_observation_state,
                "returncode": exc.terminal_observation.returncode,
                "terminal_state": exc.terminal_observation.terminal_state,
                "parent_receipt": (
                    exc.terminal_observation.parent_receipt.to_dict()
                    if exc.terminal_observation.parent_receipt
                    else None
                ),
            }
        result.update(
            status="BLOCKED",
            technical_validation_state="BLOCKED",
            activation_admitted=False,
            capture_admitted=False,
            real_observation_admitted=False,
            reason_code=getattr(
                exc, "code", getattr(exc, "reason_code", "COMPOSER_EXECUTION_FAILED")
            ),
            detail=str(exc),
        )
    count, counter_state = _actual_counter(result)
    result.update(
        canonical_dq_call_count=count,
        counter_observation_state=counter_state,
        real_market_dq_call_count=count if review.evidence_purpose == "PROSPECTIVE_RESEARCH" else 0,
        recorder_returns=[row.to_dict() for row in returns],
        completed_at=_now().isoformat(),
    )
    try:
        require(
            sampler is not None and sampler.failure_diagnostic is None,
            "original clock cannot be resumed after failure",
            "COMPOSER_CLOCK_CHAIN_INCOMPLETE",
        )
        assert sampler is not None
        result["parent_clock_evidence"] = sampler.evidence.to_dict()
        _verify_composer_projection(request, result, bootstrap=bootstrap, terminal_required=False)
        final_proof = _check_live(request, bootstrap, lease, review)
        sampler.recheck_policy()
        terminal_clock = sampler.checkpoint("terminal_precommit")
        _clock_scope(request, terminal_clock, final_proof)
        result["terminal_precommit_proof"] = final_proof
        result["terminal_clock_evidence"] = terminal_clock.to_dict()
        attempt = _attempt(request)
        assert attempt is not None
        _check_retained_proof(request, final_proof, attempt)
        require(
            parse_utc_datetime(result["completed_at"])
            <= parse_utc_datetime(final_proof["source_checked_at"]),
            "terminal guard follows verification",
        )
        require_clock_evidence_extension(
            HostClockEvidence.from_dict(result["parent_clock_evidence"]), terminal_clock
        )
        _write(root, request.operation_relative_path + "/result.json", canonical_json_bytes(result))
    except (ValueError, OSError, RuntimeError, KeyError, TypeError) as exc:
        failure = ComposerCaptureError(getattr(exc, "code", "COMPOSER_TERMINAL_FAILED"), str(exc))
        failure.prospective_child_canonical_dq_call_count = count
        failure.prospective_recorder_return_observations = [row.to_dict() for row in returns]
        failure.prospective_clock_failure_diagnostic = (
            sampler.failure_diagnostic.to_dict() if sampler and sampler.failure_diagnostic else None
        )
        raise failure from exc
    return result
