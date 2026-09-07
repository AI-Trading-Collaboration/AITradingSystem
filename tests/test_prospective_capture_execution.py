"""Synthetic parent tests with real S4D, S3a and contained immutable writes.

The named context and source-bootstrap objects are explicit doubles. These
tests establish parent state/clock/replay behavior, never actual Git-byte source
execution, canonical DQ, market evidence, owner review or investment authority.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from test_named_data_quality_execution_contract import _receipt as synthetic_receipt
from test_named_quality_dispatch import _git
from test_prospective_capture_execution_contract import _manifest, _request, _review

import ai_trading_system.data.named_quality_dispatch as dispatch
import ai_trading_system.prospective_capture_execution as capture
import ai_trading_system.prospective_event_time_evidence as recorder
from ai_trading_system.contracts.named_data_quality_execution import (
    EQUAL_RISK_PRICE_REGISTRY_PATH,
    NamedArtifactBinding,
    NamedDQExecutionReceipt,
    NamedDQRoots,
    NamedDQSuccessfulDispatchBinding,
    NamedExecutionObservation,
)
from ai_trading_system.contracts.prospective_capture_execution import (
    CAPTURE_POLICY_PATH,
    ParentCompletionAcknowledgement,
    ProspectiveCaptureRequest,
)
from ai_trading_system.contracts.prospective_event_time_evidence import (
    EventBinding,
    canonical_json_bytes,
    strict_json_loads,
)
from ai_trading_system.data.named_quality_execution import (
    NamedBootstrapAuthority,
    _preview_calendar_witness,
)
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutLeaseGuard,
    CheckoutLeaseHandle,
    CheckoutOperationClass,
)
from ai_trading_system.simple_baseline_named_preview import (
    rebuild_prospective_simple_baseline_preview,
)

ROOT = Path(__file__).resolve().parents[1]
ACTIVATED = datetime(2026, 11, 25, 22, tzinfo=UTC)


@dataclass
class _Clock:
    value: datetime = ACTIVATED

    def __call__(self) -> datetime:
        return self.value


@dataclass
class _Harness:
    root: Path
    request: ProspectiveCaptureRequest
    bootstrap: Any
    lease: CheckoutLeaseHandle
    clock: _Clock

    def renew(self, at: datetime) -> None:
        if not self.lease.released:
            self.lease.release(outcome="synthetic_clock_transition", at=self.clock.value)
        self.clock.value = at
        decision, lease = self.lease.guard.acquire(
            intent_id="synthetic-capture-renewed",
            task_id="TRADING-2564-SYNTHETIC",
            thread_id="synthetic-capture-parent",
            actor="integration-coordinator",
            operation_class=CheckoutOperationClass.SHARED_MUTATION,
            shared_paths=self.request.required_write_paths,
            base_commit=self.request.candidate_commit,
            now=at - timedelta(seconds=1),
        )
        assert decision.status == "PASS" and lease is not None
        self.lease = lease
        self.bootstrap.source_lease_id = lease.lease_id

    def run(self, request: ProspectiveCaptureRequest | None = None) -> dict[str, Any]:
        request = request or self.request
        self.bootstrap.operation = request.operation
        return capture.bootstrap_worker(
            request.to_dict(),
            operation=request.operation,
            bootstrap=cast(NamedBootstrapAuthority, self.bootstrap),
        )

    def read(self, relative: str) -> dict[str, Any]:
        return cast(dict[str, Any], strict_json_loads((self.root / relative).read_bytes()))

    def overwrite_result(self, result: dict[str, Any]) -> None:
        # Deliberate hostile disk mutation, outside the production immutable API.
        (self.root / self.request.operation_relative_path / "result.json").write_bytes(
            canonical_json_bytes(result)
        )


@pytest.fixture
def harness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[_Harness]:
    root = tmp_path / "synthetic-capture-parent"
    root.mkdir()
    (root / ".gitignore").write_text("outputs/\n", encoding="utf-8")
    dependencies = (
        CAPTURE_POLICY_PATH,
        recorder.POLICY_PATH,
        EQUAL_RISK_PRICE_REGISTRY_PATH,
    )
    for relative in (
        "config/architecture/arch_005_s4d_checkout_guard.yaml",
        "config/architecture/arch_005_parallel_control_policy.yaml",
        *dependencies,
        *recorder._CALENDAR_PATHS,
    ):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((ROOT / relative).read_bytes())
    _git(root, "init", "-b", "synthetic-capture-parent")
    _git(root, "config", "user.email", "capture@example.invalid")
    _git(root, "config", "user.name", "Synthetic Capture")
    _git(root, "config", "core.autocrlf", "false")
    _git(root, "add", ".gitignore", "config", "src")
    _git(root, "commit", "-m", "synthetic capture fixture")
    commit = _git(root, "rev-parse", "HEAD")
    output = _manifest().output_relative_path
    manifest = _manifest(
        roots=NamedDQRoots(
            (tmp_path / "synthetic-source").as_posix(),
            (tmp_path / "synthetic-publication").as_posix(),
            root.as_posix(),
            (root / output / "dq").as_posix(),
        ),
        candidate_commit=commit,
    )
    review = _review(manifest)
    request = _request("activate", manifest=manifest)
    target = root / request.owner_review.relative_path
    target.parent.mkdir(parents=True)
    target.write_bytes(review.canonical_bytes)
    guard = CheckoutLeaseGuard(
        project_root=root,
        policy_path=root / "config/architecture/arch_005_s4d_checkout_guard.yaml",
        parallel_policy_path=root / "config/architecture/arch_005_parallel_control_policy.yaml",
    )
    decision, lease = guard.acquire(
        intent_id="synthetic-capture-parent",
        task_id="TRADING-2564-SYNTHETIC",
        thread_id="synthetic-capture-parent",
        actor="integration-coordinator",
        operation_class=CheckoutOperationClass.SHARED_MUTATION,
        shared_paths=(output,),
        base_commit=commit,
        now=ACTIVATED - timedelta(minutes=1),
    )
    assert decision.status == "PASS" and lease is not None
    identity = replace(
        synthetic_receipt().execution,
        execution_root=root.as_posix(),
        candidate_commit=commit,
        source_manifest_path=request.source_manifest_path,
        source_manifest_sha256=request.source_manifest_sha256,
    )
    context = SimpleNamespace(
        identity=identity,
        process_id=os.getpid(),
        stable_identity_sha256=identity.stable_identity_sha256,
    )
    clock = _Clock()
    bootstrap = SimpleNamespace(
        operation="activate",
        context=context,
        canonical_dq_call_count=0,
        source_lease_id=lease.lease_id,
        dependencies={
            path: SimpleNamespace(content=(root / path).read_bytes()) for path in dependencies
        },
        assert_execution_unchanged=lambda **_: clock.value.isoformat(),
    )
    monkeypatch.setattr(capture, "require_named_execution_context", lambda: context)
    monkeypatch.setattr(dispatch, "require_named_execution_context", lambda: context)
    monkeypatch.setattr(recorder, "SOURCE_ROOT", root)
    monkeypatch.setattr(capture, "_now", clock)
    monkeypatch.setattr(dispatch, "_now", clock)
    monkeypatch.setattr(recorder, "_utc_now", clock)
    monkeypatch.setattr(capture, "_monotonic_ns", lambda: 0)
    monkeypatch.setattr(time, "monotonic_ns", lambda: 0)
    value = _Harness(root, request, bootstrap, lease, clock)
    yield value
    if not value.lease.released:
        value.lease.release(
            outcome="synthetic_capture_complete", at=clock.value + timedelta(seconds=1)
        )


def _forbidden(*args: Any, **kwargs: Any) -> Any:
    raise AssertionError("unexpected new execution while replaying retained evidence")


def _synthetic_terminal_parent(
    harness: _Harness, request: ProspectiveCaptureRequest, *, count: int, status: str
) -> dict[str, Any]:
    assert request.named_dq_request is not None
    return {
        "schema_version": "named_data_quality_parent_dispatch.v1",
        "profile": "PROSPECTIVE_FIVE_CANDIDATE_PRODUCTION_PARENT",
        "status_semantics": "PARENT_ASSOCIATION_AND_PROCESS_OBSERVATION_ONLY",
        "request_id": request.named_dq_request.request_id,
        "operation": "run",
        "parent_operation": "capture",
        "candidate_commit": request.candidate_commit,
        "execution_root": request.roots.execution_root,
        "parent_pid": os.getpid(),
        "source_lease_id": harness.lease.lease_id,
        "observed_canonical_dq_call_count": count,
        "counter_observation_state": "KNOWN",
        "parent_canonical_dq_call_count": 0,
        "status": status,
    }


def _synthetic_full_capture(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> tuple[ProspectiveCaptureRequest, list[int]]:
    """Prepare a declared DQ/verification double, never mint VerifiedNamedInputs.

    Original publication/report bytes are explicit synthetic declarations. Their
    internal hash/DTO closure, real S4D snapshots and actual preview arithmetic
    exercise this parent's validation, not the replaced DQ/source verifier.
    """
    original = synthetic_receipt()
    request = _request("capture", manifest=harness.request.manifest)
    assert request.named_dq_request is not None and request.feature_session is not None
    dq_request = request.named_dq_request
    sessions, next_session = _preview_calendar_witness(dq_request)
    assert next_session is not None
    prices = (
        "date,ticker,adj_close\n"
        + "".join(
            f"{day.isoformat()},{ticker},{100 + i * 0.1 + ((i * ordinal) % 11) * 0.01:.5f}\n"
            for i, day in enumerate(sessions)
            for ordinal, ticker in enumerate(("QQQ", "SGOV", "TQQQ"), start=1)
        )
    ).encode()
    registry = (harness.root / EQUAL_RISK_PRICE_REGISTRY_PATH).read_bytes()
    rows: dict[str, tuple[NamedArtifactBinding, bytes]] = {}

    def bind(role: str, binding: NamedArtifactBinding, content: bytes) -> NamedArtifactBinding:
        actual = replace(binding, sha256=capture._sha(content), size_bytes=len(content))
        rows[role] = actual, content
        return actual

    def provenance(role: str, binding: NamedArtifactBinding) -> NamedArtifactBinding:
        return bind(role, binding, canonical_json_bytes({"synthetic_declaration_only": role}))

    pointer_binding = provenance("publication_pointer", original.publication.pointer)
    publication = replace(
        original.publication,
        pointer_id=dq_request.selector.pointer_id,
        pointer=pointer_binding,
        transaction_id=dq_request.selector.transaction_id,
        transaction=provenance("publication_transaction", original.publication.transaction),
        snapshot_manifest=provenance("snapshot_manifest", original.publication.snapshot_manifest),
        source_event=provenance("source_event", original.publication.source_event),
        anchor_pointer_id=dq_request.selector.pointer_id,
        anchor_pointer=bind(
            "commit_anchor", original.publication.anchor_pointer, rows["publication_pointer"][1]
        ),
        transaction_window=dq_request.scope.requested_window,
    )
    dq_request = replace(
        dq_request,
        selector=replace(
            dq_request.selector,
            pointer_sha256=publication.pointer.sha256,
            transaction_sha256=publication.transaction.sha256,
        ),
    )
    request = replace(request, named_dq_request=dq_request)
    manifest = replace(
        original.manifest, member=provenance("source_manifest", original.manifest.member)
    )
    inputs = tuple(
        replace(
            item,
            member=bind(
                "input_" + item.role, item.member, prices if item.role == "prices" else b"rates"
            ),
            row_count=len(sessions) * 3 if item.role == "prices" else 1,
            observed_min_date=dq_request.scope.requested_window.start,
            observed_max_date=request.feature_session if item.role == "prices" else sessions[-2],
            source_relative_path=dq_request.source_output_relative_path + "/" + item.role + ".csv",
        )
        for item in original.inputs
    )
    registry_binding = NamedArtifactBinding(
        "EXECUTION", EQUAL_RISK_PRICE_REGISTRY_PATH, capture._sha(registry), len(registry)
    )
    deps = tuple(
        sorted(
            (*original.execution_dependencies, registry_binding), key=lambda row: row.relative_path
        )
    )
    dependencies = []
    for ordinal, binding in enumerate(deps):
        content = registry if binding == registry_binding else b"synthetic dependency declaration"
        dependencies.append(bind(f"dependency_{ordinal:03d}", binding, content))
    policy_binding = next(row for row in dependencies if row.relative_path == original.policy.path)
    calendar_binding = next(
        row for row in dependencies if row.relative_path == original.calendar_policy.relative_path
    )
    report_bytes = b"Synthetic DQ PASS declaration only; no validator was executed.\n"
    report = replace(
        original.report, sha256=capture._sha(report_bytes), size_bytes=len(report_bytes)
    )
    rows["dq_report"] = (
        NamedArtifactBinding("EVIDENCE", report.path, report.sha256, report.size_bytes),
        report_bytes,
    )
    now = harness.clock.value
    assert dq_request.expected_evaluated_window is not None and request.feature_session is not None
    receipt: NamedDQExecutionReceipt = replace(
        original,
        request=dq_request,
        publication=publication,
        manifest=manifest,
        inputs=inputs,
        started_at=now,
        checked_at=now,
        ended_at=now,
        evaluated_window=dq_request.expected_evaluated_window,
        execution=harness.bootstrap.context.identity,
        execution_observation=NamedExecutionObservation(
            7654, harness.lease.lease_id, now, now, now
        ),
        execution_dependencies=tuple(dependencies),
        policy=replace(original.policy, sha256=policy_binding.sha256),
        calendar_policy=calendar_binding,
        calendar=replace(original.calendar, special_closure_policy_sha256=calendar_binding.sha256),
        report=report,
        data_quality_evidence=replace(
            original.data_quality_evidence,
            checked_at=now,
            as_of=request.feature_session,
            report_sha256=report.sha256,
        ),
    )
    calls: list[int] = []
    current: dict[str, Any] = {}

    def synthetic_dispatch(*args: Any, **kwargs: Any) -> dispatch.NamedQualityDispatchResult:
        calls.append(1)
        required = tuple(
            sorted(
                (
                    request.operation_relative_path + "/dq_dispatch",
                    request.manifest.output_relative_path + "/dq",
                )
            )
        )
        prefix = request.operation_relative_path + "/dq_dispatch/"
        pre = dispatch._parent_proof(
            dq_request, harness.bootstrap, harness.lease, required, stage="SYNTHETIC_CHILD_PRE"
        )
        post = dispatch._parent_proof(
            dq_request, harness.bootstrap, harness.lease, required, stage="SYNTHETIC_CHILD_POST"
        )
        receipt_path = "named_data_quality/executions/" + receipt.receipt_id + "/receipt.json"
        receipt_binding = NamedArtifactBinding(
            "EVIDENCE", receipt_path, receipt.canonical_sha256, len(receipt.canonical_bytes)
        )
        rows["dq_receipt"] = receipt_binding, receipt.canonical_bytes
        stdout = {
            "schema_version": "named_data_quality_bootstrap_result.v1",
            "status": "PASS",
            "request_id": dq_request.request_id,
            "source_lease_id": harness.lease.lease_id,
            "process_id": 7654,
            "receipt_id": receipt.receipt_id,
            "receipt_path": receipt_path,
            "receipt_sha256": receipt.canonical_sha256,
            "canonical_dq_call_count": 1,
            "verified_input_seal_exported": False,
            "dispatch_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
            "child_started_at": now.isoformat(),
            "child_terminal_checked_at": now.isoformat(),
        }
        parent_parts = {}
        for role, key, filename, content in (
            ("dq_parent_request", "request", "request.json", dq_request.canonical_bytes),
            ("dq_child_stdout", "child_stdout", "child_stdout.json", canonical_json_bytes(stdout)),
            ("dq_child_stderr", "child_stderr", "child_stderr.txt", b""),
            (
                "dq_pre_guard",
                "pre_dispatch_proof",
                "pre_dispatch_proof.json",
                canonical_json_bytes(pre),
            ),
            (
                "dq_post_guard",
                "post_dispatch_proof",
                "post_dispatch_proof.json",
                canonical_json_bytes(post),
            ),
        ):
            binding = capture._write(harness.root, prefix + filename, content)
            rows[role] = binding, content
            parent_parts[key] = {
                "path": (harness.root / binding.relative_path).as_posix(),
                "sha256": binding.sha256,
                "size_bytes": binding.size_bytes,
            }
        parent = {
            "schema_version": "named_data_quality_parent_dispatch.v1",
            "profile": "PROSPECTIVE_FIVE_CANDIDATE_PRODUCTION_PARENT",
            "status_semantics": "PARENT_ASSOCIATION_AND_PROCESS_OBSERVATION_ONLY",
            "status": "PASS",
            "candidate_commit": request.candidate_commit,
            "execution_root": request.roots.execution_root,
            "request_id": dq_request.request_id,
            "operation": "run",
            "parent_operation": "capture",
            "returncode": 0,
            "observed_canonical_dq_call_count": 1,
            "parent_canonical_dq_call_count": 0,
            "parent_pid": os.getpid(),
            "source_lease_id": harness.lease.lease_id,
            "lease_acquired_or_mutated": False,
            "verified_input_seal_exported": False,
            "dispatch_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
            "spawn_requested_at": now.isoformat(),
            "terminal_observed_at": now.isoformat(),
            "terminal_state": "EXITED",
            "counter_observation_state": "KNOWN",
            "failure": None,
            "child_pid": 7654,
            "child_result": stdout,
            **parent_parts,
        }
        content = canonical_json_bytes(parent)
        parent_binding = capture._write(harness.root, prefix + "parent_receipt.json", content)
        rows["dq_parent"] = parent_binding, content
        successful = NamedDQSuccessfulDispatchBinding(
            receipt=receipt_binding,
            receipt_id=receipt.receipt_id,
            request_id=dq_request.request_id,
            request_sha256=dq_request.canonical_sha256,
            execution_identity_sha256=receipt.execution.stable_identity_sha256,
            candidate_commit=request.candidate_commit,
            execution_root=request.roots.execution_root,
            execution_pid=7654,
            source_lease_id=harness.lease.lease_id,
            child_started_at=now,
            child_terminal_checked_at=now,
            parent_postchecked_at=now,
            parent_receipt=parent_binding,
        )
        successful_binding = capture._write(
            harness.root, prefix + "successful_run_dispatch.json", successful.canonical_bytes
        )
        rows["dq_successful_dispatch"] = successful_binding, successful.canonical_bytes
        closure = {
            "schema_version": "prospective_verified_input_closure.v1",
            "request_id": dq_request.request_id,
            "receipt_id": receipt.receipt_id,
            "execution_identity_sha256": receipt.execution.stable_identity_sha256,
            "all_dq_input_roles": sorted(item.role for item in receipt.inputs),
            "recording_only": True,
            "rates_feature_access_granted": False,
            "provider_available_at_status": "NOT_ESTABLISHED",
            "members": [
                {
                    "role": role,
                    "binding": binding.to_dict(),
                    "semantic_role": (
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
                    ),
                }
                for role, (binding, _) in sorted(rows.items())
            ],
        }
        closed = tuple((role, content) for role, (_, content) in sorted(rows.items())) + (
            ("closure_manifest", canonical_json_bytes(closure)),
        )
        current.update(successful=successful, closed=closed)
        return dispatch.NamedQualityDispatchResult(
            "PASS",
            receipt_path,
            receipt.canonical_sha256,
            successful_binding.relative_path,
            successful_binding.sha256,
            parent_binding,
            1,
            "KNOWN",
            0,
            "EXITED",
        )

    verified = SimpleNamespace(
        receipt=receipt,
        synthetic_verifier_double=True,
        recording_closure_for_prospective=lambda: current["closed"],
    )

    def synthetic_preview(value: object, *, required_scope: Any) -> dict[str, object]:
        assert value is verified
        return rebuild_prospective_simple_baseline_preview(
            receipt=receipt,
            successful_dispatch=current["successful"],
            required_scope=required_scope,
            prices=prices,
            registry=registry,
            sessions=sessions,
            next_session=next_session,
        )

    monkeypatch.setattr(capture, "dispatch_named_quality_child", synthetic_dispatch)
    monkeypatch.setattr(
        capture, "verify_named_data_quality_execution_receipt", lambda *args, **kwargs: verified
    )
    monkeypatch.setattr(capture, "build_prospective_simple_baseline_preview", synthetic_preview)
    return request, calls


def test_activation_real_s4d_recorder_and_parent_ack_replay_after_expiry(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = harness.run()
    assert result["status"] == "ACTIVATED"
    assert result["activation_admitted"] and not result["real_observation_admitted"]
    assert result["canonical_dq_call_count"] == result["real_market_dq_call_count"] == 0
    ack_bytes = (harness.root / result["acknowledgement"]["relative_path"]).read_bytes()
    ack = ParentCompletionAcknowledgement.from_json_bytes(ack_bytes)
    assert ack.first_feature_session == date(2026, 11, 27)
    assert ack.source_lease_id == harness.lease.lease_id and ack.parent_pid == os.getpid()
    harness.lease.release(outcome="synthetic_capture_complete", at=harness.clock.value)
    harness.clock.value = harness.request.manifest.expires_at + timedelta(days=1)
    monkeypatch.setattr(capture, "restore_named_capture_lease", _forbidden)
    monkeypatch.setattr(capture, "record_activation", _forbidden)
    monkeypatch.setattr(capture, "dispatch_named_quality_child", _forbidden)
    replay = harness.run()
    assert replay == {
        **result,
        "idempotent_replay": True,
        "this_invocation_canonical_dq_call_count": 0,
    }
    assert (harness.root / result["acknowledgement"]["relative_path"]).read_bytes() == ack_bytes


def test_incomplete_original_attempt_replays_after_lease_release_and_manifest_expiry(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    review = _review(harness.request.manifest)
    assert capture._reserve(harness.request, harness.bootstrap, harness.lease, review)
    attempt_path = harness.root / harness.request.operation_relative_path / "attempt.json"
    original = attempt_path.read_bytes()
    harness.lease.release(outcome="synthetic_capture_complete", at=harness.clock.value)
    harness.clock.value = harness.request.manifest.expires_at + timedelta(days=1)
    monkeypatch.setattr(capture, "restore_named_capture_lease", _forbidden)
    monkeypatch.setattr(capture, "record_activation", _forbidden)
    replay = harness.run()
    assert replay["status"] == "INCOMPLETE" and replay["idempotent_replay"]
    assert replay["counter_observation_state"] == "UNKNOWN"
    assert replay["canonical_dq_call_count"] is None
    assert not replay["capture_admitted"] and not replay["retry_allowed"]
    assert attempt_path.read_bytes() == original


@pytest.mark.parametrize("status", ["ACTIVATED", "BLOCKED"])
def test_retained_terminal_projection_rejects_forged_admission_counters_or_pid(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, status: str
) -> None:
    if status == "BLOCKED":

        def fail_recorder(**kwargs: Any) -> Any:
            raise RuntimeError("synthetic recorder refusal")

        monkeypatch.setattr(capture, "record_activation", fail_recorder)
    result = harness.run()
    assert result["status"] == status
    # Reuse one real immutable production for independent hostile projections;
    # replay is read-only, so recreating ten Git stores adds no test coverage.
    for field, value in (
        ("real_observation_admitted", True),
        ("capture_admitted", True),
        ("parent_pid", True),
        ("parent_canonical_dq_call_count", 1),
        ("canonical_dq_call_count", True),
        ("counter_observation_state", "UNKNOWN"),
        ("real_market_dq_call_count", 1),
        ("order_count", 1),
        ("retry_allowed", True),
        ("unreviewed_extension", "authority"),
    ):
        harness.overwrite_result({**result, field: value})
        with pytest.raises(ValueError):
            harness.run()
    harness.overwrite_result(result)
    assert harness.run()["status"] == status


def test_retained_activation_ack_requires_original_parent_proof_even_after_rehash(
    harness: _Harness,
) -> None:
    result = harness.run()
    assert result["status"] == "ACTIVATED"
    binding = result["post_recording_proof"]
    path = harness.root / binding["relative_path"]
    original = path.read_bytes()
    for mutation in ("lease", "pid", "source", "raw_bytes", "missing_post"):
        projection = dict(result)
        if mutation == "missing_post":
            projection.pop("post_recording_proof")
        else:
            proof = cast(dict[str, Any], strict_json_loads(original))
            if mutation == "lease":
                proof["active_lease"]["lease_id"] = "lease-" + "a" * 20
            elif mutation == "pid":
                proof["parent_pid"] += 1
            elif mutation == "source":
                proof["parent_execution_identity_sha256"] = "a" * 64
            elif mutation == "raw_bytes":
                proof["lease_event_bytes_hex"] += "20"
            content = canonical_json_bytes(proof)
            path.write_bytes(content)
            projection["post_recording_proof"] = capture._binding(
                binding["relative_path"], content
            ).to_dict()
        harness.overwrite_result(projection)
        with pytest.raises(ValueError):
            harness.run()
        path.write_bytes(original)
    harness.overwrite_result(result)
    assert harness.run()["status"] == "ACTIVATED"


def test_parent_ack_crossing_market_midnight_advances_first_feature(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    # S3a finishes on Nov 26 market time, whose next session is Nov 27.
    # Observing its return after Nov 27 midnight must advance the parent F
    # to Nov 30, while preserving the lower layer's original Nov 27 field.
    harness.renew(datetime(2026, 11, 27, 4, 59, 59, tzinfo=UTC))
    original = recorder.record_activation

    def crossing(**kwargs: Any) -> recorder.RecordedTemporalEvidence:
        event = original(**kwargs)
        harness.clock.value += timedelta(seconds=2)
        return event

    monkeypatch.setattr(capture, "record_activation", crossing)
    result = harness.run()
    assert result["status"] == "ACTIVATED"
    assert result["first_feature_session"] == "2026-11-30"


@pytest.mark.parametrize("count", [None, 1, 2])
def test_failed_dq_dispatch_preserves_observed_child_counter_and_never_enters_recorder(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, count: int | None
) -> None:
    assert harness.run()["status"] == "ACTIVATED"
    harness.renew(datetime(2026, 11, 27, 18, 1, tzinfo=UTC))
    request = _request("capture", manifest=harness.request.manifest)
    calls = []

    def fail_dispatch(*args: Any, **kwargs: Any) -> dispatch.NamedQualityDispatchResult:
        calls.append(1)
        if count is None:
            raise RuntimeError("synthetic failure after entering child dispatch; count unavailable")
        relative = request.operation_relative_path + "/dq_dispatch/parent_receipt.json"
        # This is an explicit synthetic failed-child receipt, not a successful
        # canonical DQ proof. The terminal projection must retain the violation.
        parent = _synthetic_terminal_parent(harness, request, count=count, status="BLOCKED")
        parent_binding = capture._write(harness.root, relative, canonical_json_bytes(parent))
        return dispatch.NamedQualityDispatchResult(
            "BLOCKED", None, None, None, None, parent_binding, count, "KNOWN", 1, "EXITED"
        )

    monkeypatch.setattr(capture, "dispatch_named_quality_child", fail_dispatch)
    monkeypatch.setattr(capture, "record_local_input_observation", _forbidden)
    monkeypatch.setattr(capture, "record_signal_completion", _forbidden)
    result = harness.run(request)
    assert result["status"] == "BLOCKED" and not result["recorder_entered"]
    assert (
        result["canonical_dq_call_count"] == count and result["parent_canonical_dq_call_count"] == 0
    )
    assert result["counter_observation_state"] == ("UNKNOWN" if count is None else "KNOWN")
    assert not result["capture_admitted"] and not result["real_observation_admitted"]
    assert calls == [1]
    monkeypatch.setattr(capture, "dispatch_named_quality_child", _forbidden)
    assert harness.run(request)["idempotent_replay"] is True


@pytest.mark.parametrize("parent_published", [True, False])
def test_partial_terminal_observation_retains_known_counter_without_fabricating_result(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, parent_published: bool
) -> None:
    assert harness.run()["status"] == "ACTIVATED"
    harness.renew(datetime(2026, 11, 27, 18, 1, tzinfo=UTC))
    request = _request("capture", manifest=harness.request.manifest)
    calls: list[int] = []

    def failed_publication(*args: Any, **kwargs: Any) -> Any:
        calls.append(1)
        parent_binding = None
        if parent_published:
            # The parent association already completed; only the subsequent
            # successful-dispatch publication failed. Never rewrite this PASS.
            parent = _synthetic_terminal_parent(harness, request, count=1, status="PASS")
            parent_binding = capture._write(
                harness.root,
                request.operation_relative_path + "/dq_dispatch/parent_receipt.json",
                canonical_json_bytes(parent),
            )
        raise dispatch.NamedQualityDispatchError(
            "NAMED_PARENT_TERMINAL_PUBLICATION_FAILED",
            "synthetic terminal artifact failure after observing child count 1",
            terminal_observation=dispatch.NamedQualityTerminalObservation(
                canonical_dq_call_count=1,
                counter_observation_state="KNOWN",
                returncode=0,
                terminal_state="EXITED",
                parent_receipt=parent_binding,
            ),
        )

    monkeypatch.setattr(capture, "dispatch_named_quality_child", failed_publication)
    monkeypatch.setattr(capture, "record_local_input_observation", _forbidden)
    monkeypatch.setattr(capture, "record_signal_completion", _forbidden)
    result_path = harness.root / request.operation_relative_path / "result.json"
    if parent_published:
        result = harness.run(request)
        assert result["status"] == "BLOCKED" and result["canonical_dq_call_count"] == 1
        assert result["counter_observation_state"] == "KNOWN"
        assert result["parent_canonical_dq_call_count"] == 0 and result["recorder_entered"] is False
        assert "dq_parent_receipt" in result and result_path.exists()
        parent_path = harness.root / result["dq_parent_receipt"]["relative_path"]
        parent_bytes = parent_path.read_bytes()
        assert cast(dict[str, Any], strict_json_loads(parent_bytes))["status"] == "PASS"
    else:
        with pytest.raises(capture.ProspectiveCaptureExecutionError) as caught:
            harness.run(request)
        assert caught.value.prospective_child_canonical_dq_call_count == 1
        assert caught.value.prospective_dq_parent_receipt is None
        assert not result_path.exists()
    monkeypatch.setattr(capture, "dispatch_named_quality_child", _forbidden)
    replay = harness.run(request)
    assert replay["status"] == ("BLOCKED" if parent_published else "INCOMPLETE")
    assert replay["idempotent_replay"] is True and calls == [1]
    assert not (
        harness.root / request.operation_relative_path / "dq_dispatch/successful_run_dispatch.json"
    ).exists()
    if parent_published:
        assert parent_path.read_bytes() == parent_bytes


@pytest.mark.parametrize("during_publication", [False, True])
def test_result_publication_clock_rollback_is_reported_after_ack(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, during_publication: bool
) -> None:
    original_check = capture._check_live
    original_write = capture._write
    checks_after_ack = 0
    ack_path = harness.root / harness.request.operation_relative_path / "acknowledgement.json"
    result_relative = harness.request.operation_relative_path + "/result.json"

    def check_live(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal checks_after_ack
        if ack_path.exists():
            checks_after_ack += 1
            if not during_publication and checks_after_ack == 2:
                harness.clock.value -= timedelta(microseconds=1)
        return original_check(*args, **kwargs)

    def write(root: Path, relative: str, content: bytes) -> NamedArtifactBinding:
        binding = original_write(root, relative, content)
        if during_publication and relative == result_relative:
            harness.clock.value -= timedelta(microseconds=1)
        return binding

    monkeypatch.setattr(capture, "_check_live", check_live)
    monkeypatch.setattr(capture, "_write", write)
    with pytest.raises(capture.ProspectiveCaptureExecutionError, match="CLOCK_ROLLBACK") as caught:
        harness.run()
    assert caught.value.prospective_child_canonical_dq_call_count == 0
    assert (harness.root / result_relative).exists() is during_publication


def _rehash_signal_projection(
    harness: _Harness, request: ProspectiveCaptureRequest, result: dict[str, Any]
) -> dict[str, Any]:
    """Deliberately rewrite every affected hash so arithmetic must catch tamper."""
    root = harness.root
    timing_root = root / request.timing_relative_path
    ack = ParentCompletionAcknowledgement.from_json_bytes(
        (root / result["acknowledgement"]["relative_path"]).read_bytes()
    )
    completion_path = root / ack.recorder_event.relative_path
    completion = cast(dict[str, Any], strict_json_loads(completion_path.read_bytes()))
    intent_path = timing_root / completion["intent"]["relative_path"]
    intent = cast(dict[str, Any], strict_json_loads(intent_path.read_bytes()))
    payload = intent["semantic"]["payload_bindings"][0]["artifact"]
    signal_path = timing_root / payload["relative_path"]
    preview = cast(dict[str, Any], strict_json_loads(signal_path.read_bytes()))
    preview["candidates"][0]["target_weights"]["QQQ"] += 0.01
    preview["preview_id"] = "prospective_five_candidate_preview_" + capture._sha(
        canonical_json_bytes({key: value for key, value in preview.items() if key != "preview_id"})
    )
    content = canonical_json_bytes(preview)
    signal_path.write_bytes(content)
    intent["semantic"]["payload_bindings"][0]["artifact"] = EventBinding(
        payload["relative_path"], capture._sha(content), len(content)
    ).to_dict()
    content = canonical_json_bytes(intent)
    intent_path.write_bytes(content)
    completion["intent"] = EventBinding(
        intent_path.relative_to(timing_root).as_posix(), capture._sha(content), len(content)
    ).to_dict()
    content = canonical_json_bytes(completion)
    completion_path.write_bytes(content)
    event_binding = capture._binding(ack.recorder_event.relative_path, content)
    rewritten_ack = replace(ack, recorder_event=event_binding)
    ack_relative = result["acknowledgement"]["relative_path"]
    (root / ack_relative).write_bytes(rewritten_ack.canonical_bytes)
    ack_binding = capture._binding(ack_relative, rewritten_ack.canonical_bytes)
    observation_relative = result["observation"]["relative_path"]
    observation = capture._observation(request, ack_binding, preview, _review(request.manifest))
    content = canonical_json_bytes(observation)
    (root / observation_relative).write_bytes(content)
    rewritten_result = {
        **result,
        "acknowledgement": ack_binding.to_dict(),
        "observation": capture._binding(observation_relative, content).to_dict(),
    }
    (root / request.operation_relative_path / "result.json").write_bytes(
        canonical_json_bytes(rewritten_result)
    )
    # The lower layer legitimately validates only opaque payload/time metadata.
    # All hashes now match; this must reach the higher layer's exact recompute.
    event = recorder.verify_time_evidence(
        store_root=timing_root,
        event=capture._local_event(request, event_binding),
        policy=recorder.load_time_evidence_policy(source_root=root),
    )
    assert event.temporal_status == "TIMELY_PAYLOAD"
    return rewritten_result


def test_complete_synthetic_capture_replays_and_rejects_rehashed_preview_tamper(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert harness.run()["status"] == "ACTIVATED"
    harness.renew(datetime(2026, 11, 27, 18, 1, tzinfo=UTC))
    request, calls = _synthetic_full_capture(harness, monkeypatch)
    result = harness.run(request)
    assert result["status"] == "CAPTURED" and result["capture_admitted"] is True
    assert result["real_observation_admitted"] is False and result["synthetic_inputs_only"] is True
    assert result["canonical_dq_call_count"] == 1 and result["parent_canonical_dq_call_count"] == 0
    assert result["real_market_dq_call_count"] == 0 and calls == [1]
    assert harness.run(request)["status"] == "CAPTURED" and calls == [1]
    _rehash_signal_projection(harness, request, result)
    with pytest.raises(ValueError, match="complete preview recomputation from closed inputs"):
        harness.run(request)
    assert calls == [1]


def test_complete_timely_synthetic_payload_with_late_parent_ack_is_not_admitted(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert harness.run()["status"] == "ACTIVATED"
    harness.renew(datetime(2026, 11, 30, 20, 59, 58, tzinfo=UTC))
    request, calls = _synthetic_full_capture(harness, monkeypatch)
    original = recorder.record_signal_completion

    def late_parent(**kwargs: Any) -> recorder.RecordedTemporalEvidence:
        event = original(**kwargs)
        assert event.temporal_status == "TIMELY_PAYLOAD"
        harness.clock.value += timedelta(seconds=2)
        return event

    monkeypatch.setattr(capture, "record_signal_completion", late_parent)
    result = harness.run(request)
    assert result["status"] == result["technical_validation_state"] == "LATE"
    assert result["capture_admitted"] is False and result["real_observation_admitted"] is False
    assert "observation" not in result and calls == [1]
    assert result["canonical_dq_call_count"] == 1 and result["parent_canonical_dq_call_count"] == 0
    replay = harness.run(request)
    assert replay["status"] == "LATE" and replay["idempotent_replay"] is True and calls == [1]


def test_postguard_failure_preserves_incomplete_slot_without_creating_terminal_success(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = recorder.record_activation

    def expire_after_witness(**kwargs: Any) -> recorder.RecordedTemporalEvidence:
        event = original(**kwargs)
        harness.clock.value = harness.request.manifest.expires_at
        return event

    monkeypatch.setattr(capture, "record_activation", expire_after_witness)
    with pytest.raises(capture.ProspectiveCaptureExecutionError) as caught:
        harness.run()
    assert caught.value.prospective_child_canonical_dq_call_count == 0
    directory = harness.root / harness.request.operation_relative_path
    assert (directory / "attempt.json").exists() and not (directory / "result.json").exists()
    monkeypatch.setattr(capture, "record_activation", _forbidden)
    assert harness.run()["status"] == "INCOMPLETE"
