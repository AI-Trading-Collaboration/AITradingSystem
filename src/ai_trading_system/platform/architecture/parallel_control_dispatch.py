from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ai_trading_system.platform.architecture.parallel_control import (
    ParallelControlError,
    ValidationEvidence,
    validate_evidence_binding,
)
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    ExecutionLease,
    FileExecutionLeaseStore,
    ParallelControlPolicy,
    ReadinessDecision,
    TaskControlRecord,
    evaluate_task_readiness,
    validate_dependency_graph,
)
from ai_trading_system.platform.architecture.parallel_control_scheduler import (
    PilotSpec,
    ShadowGovernanceAudit,
)
from ai_trading_system.platform.artifacts import write_json_atomic

DISPATCH_REPORT_SCHEMA_VERSION = "controlled_three_lane_dispatch.v1"
LANE_RESULT_SCHEMA_VERSION = "controlled_lane_execution_result.v1"

LaneAdapter = Callable[[TaskControlRecord], Mapping[str, object]]
CoordinatorAdapter = Callable[[Sequence["LaneExecutionResult"]], Mapping[str, object]]


@dataclass(frozen=True)
class LaneExecutionResult:
    task_id: str
    change_id: str
    lane_id: str
    actor: str
    attempt: int
    lease_id: str
    status: str
    reason_codes: tuple[str, ...]
    artifact_path: str | None
    artifact_sha256: str | None
    validation_tier: str
    evidence_binding_status: str
    payload: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": LANE_RESULT_SCHEMA_VERSION,
            "task_id": self.task_id,
            "change_id": self.change_id,
            "lane_id": self.lane_id,
            "actor": self.actor,
            "attempt": self.attempt,
            "lease_id": self.lease_id,
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "artifact_path": self.artifact_path,
            "artifact_sha256": self.artifact_sha256,
            "validation_tier": self.validation_tier,
            "evidence_binding_status": self.evidence_binding_status,
            "payload": dict(self.payload),
            "task_governance_status_mutated": False,
            "production_effect": "none",
            "broker_action": "none",
        }


@dataclass(frozen=True)
class ControlledDispatchReport:
    status: str
    pilot_id: str
    policy_version: str
    current_base_commit: str
    started_at: str
    shadow_governance_audit: Mapping[str, object]
    domain_attempts: tuple[LaneExecutionResult, ...]
    final_domain_results: tuple[LaneExecutionResult, ...]
    coordinator_result: LaneExecutionResult
    failure_isolation: Mapping[str, object]
    lease_store_path: str
    lease_replay: Mapping[str, object]
    artifact_checksums: tuple[tuple[str, str], ...]

    def _body(self) -> dict[str, object]:
        return {
            "schema_version": DISPATCH_REPORT_SCHEMA_VERSION,
            "status": self.status,
            "pilot_id": self.pilot_id,
            "policy_version": self.policy_version,
            "current_base_commit": self.current_base_commit,
            "started_at": self.started_at,
            "shadow_governance_audit": dict(self.shadow_governance_audit),
            "domain_attempts": [result.to_dict() for result in self.domain_attempts],
            "final_domain_results": [result.to_dict() for result in self.final_domain_results],
            "coordinator_result": self.coordinator_result.to_dict(),
            "failure_isolation": dict(self.failure_isolation),
            "lease_store_path": self.lease_store_path,
            "lease_replay": dict(self.lease_replay),
            "artifact_checksums": [
                {"path": path, "sha256": digest} for path, digest in self.artifact_checksums
            ],
            "dispatch_allowed": True,
            "dispatch_scope": "S4_EXPLICIT_PILOT_ALLOWLIST_ONLY",
            "lease_acquisition_allowed": True,
            "canonical_source_cutover": False,
            "source_of_truth": "LEGACY_MARKDOWN_ONLY",
            "task_governance_status_mutated": False,
            "generated_task_view_written": False,
            "strategy_logic_changed": False,
            "strategy_threshold_changed": False,
            "paper_shadow_changed": False,
            "production_effect": "none",
            "broker_action": "none",
        }

    @property
    def dispatch_id(self) -> str:
        return f"controlled-dispatch-{_canonical_sha256(self._body())[:20]}"

    def to_dict(self) -> dict[str, object]:
        return {"dispatch_id": self.dispatch_id, **self._body()}


class ControlledThreeLaneDispatcher:
    def __init__(
        self,
        *,
        project_root: Path,
        runtime_root: Path,
        policy: ParallelControlPolicy,
    ) -> None:
        self.project_root = project_root.resolve()
        self.runtime_root = runtime_root.resolve()
        if not self.runtime_root.is_relative_to(self.project_root):
            raise ParallelControlError("DISPATCH_RUNTIME_OUTSIDE_ROOT", str(runtime_root))
        self.policy = policy
        self.lease_store = FileExecutionLeaseStore(
            self.runtime_root / "lease_store",
            policy=policy,
        )

    def run(
        self,
        spec: PilotSpec,
        *,
        audit: ShadowGovernanceAudit,
        current_base_commit: str,
        domain_adapters: Mapping[str, LaneAdapter],
        coordinator_adapter: CoordinatorAdapter,
        actors: Mapping[str, str],
        started_at: datetime,
    ) -> ControlledDispatchReport:
        start = _utc(started_at)
        if audit.status != "PASS":
            raise ParallelControlError("S4_SHADOW_AUDIT_REQUIRED", audit.status)
        selected = audit.decision.selected
        if len(selected) != self.policy.max_parallel_domain_lanes:
            raise ParallelControlError("S4_DOMAIN_LANE_COUNT", str(len(selected)))
        task_by_change = spec.task_by_change_id()
        selected_changes = tuple(str(row["change_id"]) for row in selected)
        if set(selected_changes) != set(domain_adapters):
            raise ParallelControlError("S4_ADAPTER_BINDING", ",".join(selected_changes))
        graph = validate_dependency_graph([task.task_id for task in spec.tasks], spec.dependencies)
        readiness_by_change: dict[str, ReadinessDecision] = {}
        for task in spec.tasks:
            readiness_by_change[task.manifest.change_id] = evaluate_task_readiness(
                task,
                dependencies=spec.dependencies,
                observed_statuses={},
                graph_report=graph,
                current_base_commit=current_base_commit,
                policy=self.policy,
            )
        acquisitions: dict[str, ExecutionLease] = {}
        for index, row in enumerate(selected, start=1):
            change_id = str(row["change_id"])
            task = task_by_change[change_id]
            actor = _actor(actors, change_id)
            acquired = self.lease_store.acquire(
                task=task,
                readiness=readiness_by_change[change_id],
                lane_id=str(row["lane_id"]),
                actor=actor,
                current_base_commit=current_base_commit,
                now=start + timedelta(milliseconds=index),
            )
            if acquired.status != "ACTIVE":
                raise ParallelControlError("S4_LEASE_ACQUISITION", change_id)
            acquisitions[change_id] = acquired.lease
        with ThreadPoolExecutor(max_workers=self.policy.max_parallel_domain_lanes) as pool:
            futures = {
                change_id: pool.submit(
                    self._execute_domain_attempt,
                    task_by_change[change_id],
                    adapter=domain_adapters[change_id],
                    lease=acquisitions[change_id],
                    actor=_actor(actors, change_id),
                    attempt=1,
                    inject_failure=change_id == self.policy.failure_injection_change_id,
                )
                for change_id in selected_changes
            }
            first_attempts = tuple(
                sorted(
                    (future.result() for future in futures.values()), key=lambda row: row.change_id
                )
            )
        attempt_rows: list[LaneExecutionResult] = list(first_attempts)
        final_by_change: dict[str, LaneExecutionResult] = {}
        for result in first_attempts:
            task = task_by_change[result.change_id]
            if result.status == "PASS":
                self.lease_store.release(
                    result.lease_id,
                    actor=result.actor,
                    now=start + timedelta(seconds=2),
                    evidence_refs=(str(result.artifact_path),),
                )
                final_by_change[result.change_id] = result
                continue
            self.lease_store.expire(
                result.lease_id,
                actor=result.actor,
                now=start + timedelta(seconds=2),
                reason_code="EXECUTION_FAILED",
            )
            recovery_actor = _text(spec.failure_recovery.get("recovery_actor"), "recovery_actor")
            recovered_lease = self.lease_store.reassign(
                result.lease_id,
                task=task,
                readiness=readiness_by_change[result.change_id],
                lane_id=result.lane_id,
                actor=recovery_actor,
                current_base_commit=current_base_commit,
                now=start + timedelta(seconds=3),
            )
            recovered = self._execute_domain_attempt(
                task,
                adapter=domain_adapters[result.change_id],
                lease=recovered_lease.lease,
                actor=recovery_actor,
                attempt=2,
                inject_failure=False,
            )
            attempt_rows.append(recovered)
            if recovered.status != "PASS":
                self.lease_store.expire(
                    recovered.lease_id,
                    actor=recovery_actor,
                    now=start + timedelta(seconds=4),
                    reason_code="RECOVERY_EXECUTION_FAILED",
                )
                raise ParallelControlError("S4_RECOVERY_FAILED", result.change_id)
            self.lease_store.release(
                recovered.lease_id,
                actor=recovery_actor,
                now=start + timedelta(seconds=4),
                evidence_refs=(str(recovered.artifact_path),),
            )
            final_by_change[result.change_id] = recovered
        final_domain = tuple(final_by_change[change_id] for change_id in sorted(final_by_change))
        observed = {result.task_id: "EXECUTION_PASS" for result in final_domain}
        coordinator_tasks = [
            task for task in spec.tasks if task.manifest.lane_role.value == "COORDINATOR"
        ]
        if len(coordinator_tasks) != 1:
            raise ParallelControlError("S4_COORDINATOR_COUNT", str(len(coordinator_tasks)))
        coordinator = coordinator_tasks[0]
        coordinator_readiness = evaluate_task_readiness(
            coordinator,
            dependencies=spec.dependencies,
            observed_statuses=observed,
            graph_report=graph,
            current_base_commit=current_base_commit,
            policy=self.policy,
        )
        if coordinator_readiness.status != "READY":
            raise ParallelControlError(
                "S4_COORDINATOR_NOT_READY", ",".join(coordinator_readiness.reason_codes)
            )
        coordinator_actor = _actor(actors, coordinator.manifest.change_id)
        coordinator_lease = self.lease_store.acquire(
            task=coordinator,
            readiness=coordinator_readiness,
            lane_id="integration-coordinator",
            actor=coordinator_actor,
            current_base_commit=current_base_commit,
            now=start + timedelta(seconds=5),
        )
        if coordinator_lease.status != "ACTIVE":
            raise ParallelControlError("S4_COORDINATOR_LEASE", coordinator.manifest.change_id)
        coordinator_result = self._execute_coordinator(
            coordinator,
            adapter=coordinator_adapter,
            domain_results=final_domain,
            lease=coordinator_lease.lease,
            actor=coordinator_actor,
        )
        if coordinator_result.status != "PASS":
            self.lease_store.expire(
                coordinator_result.lease_id,
                actor=coordinator_actor,
                now=start + timedelta(seconds=6),
                reason_code="COORDINATOR_EXECUTION_FAILED",
            )
            raise ParallelControlError("S4_COORDINATOR_FAILED", coordinator.manifest.change_id)
        self.lease_store.release(
            coordinator_result.lease_id,
            actor=coordinator_actor,
            now=start + timedelta(seconds=6),
            evidence_refs=(str(coordinator_result.artifact_path),),
        )
        replay = self.lease_store.replay()
        if replay.status != "PASS" or replay.active_leases:
            raise ParallelControlError("S4_FINAL_LEASE_REPLAY", replay.status)
        unaffected_change = _text(
            spec.failure_recovery.get("expected_unaffected_change_id"),
            "expected_unaffected_change_id",
        )
        unaffected = next(
            result for result in first_attempts if result.change_id == unaffected_change
        )
        failed = next(
            result
            for result in first_attempts
            if result.change_id == self.policy.failure_injection_change_id
        )
        recovered = final_by_change[self.policy.failure_injection_change_id]
        failure_isolation = {
            "status": "PASS",
            "injected_failure_change_id": failed.change_id,
            "first_attempt_status": failed.status,
            "unaffected_change_id": unaffected.change_id,
            "unaffected_first_attempt_status": unaffected.status,
            "recovered_attempt": recovered.attempt,
            "recovered_status": recovered.status,
            "reassigned_lease_id": recovered.lease_id,
            "other_lane_blocked": False,
        }
        artifacts = tuple(
            sorted(
                (str(result.artifact_path), str(result.artifact_sha256))
                for result in (*final_domain, coordinator_result)
                if result.artifact_path is not None and result.artifact_sha256 is not None
            )
        )
        report = ControlledDispatchReport(
            status="PASS",
            pilot_id=spec.pilot_id,
            policy_version=self.policy.policy_version,
            current_base_commit=current_base_commit,
            started_at=start.isoformat(),
            shadow_governance_audit=audit.to_dict(),
            domain_attempts=tuple(
                sorted(attempt_rows, key=lambda row: (row.change_id, row.attempt))
            ),
            final_domain_results=final_domain,
            coordinator_result=coordinator_result,
            failure_isolation=failure_isolation,
            lease_store_path=self.lease_store.root.relative_to(self.project_root).as_posix(),
            lease_replay=replay.to_dict(),
            artifact_checksums=artifacts,
        )
        write_json_atomic(
            self._report_artifact_path(coordinator),
            report.to_dict(),
        )
        return report

    def _execute_domain_attempt(
        self,
        task: TaskControlRecord,
        *,
        adapter: LaneAdapter,
        lease: ExecutionLease,
        actor: str,
        attempt: int,
        inject_failure: bool,
    ) -> LaneExecutionResult:
        tier = task.manifest.required_validation_tiers[0]
        if inject_failure:
            return LaneExecutionResult(
                task_id=task.task_id,
                change_id=task.manifest.change_id,
                lane_id=lease.lane_id,
                actor=actor,
                attempt=attempt,
                lease_id=lease.lease_id,
                status="FAIL",
                reason_codes=("CONTROLLED_REHEARSAL_FIRST_ATTEMPT",),
                artifact_path=None,
                artifact_sha256=None,
                validation_tier=tier,
                evidence_binding_status="NOT_RUN",
                payload={"failure_injected": True},
            )
        return self._execute_and_bind(
            task,
            callback=lambda: adapter(task),
            lease=lease,
            actor=actor,
            attempt=attempt,
            tier=tier,
        )

    def _execute_coordinator(
        self,
        task: TaskControlRecord,
        *,
        adapter: CoordinatorAdapter,
        domain_results: Sequence[LaneExecutionResult],
        lease: ExecutionLease,
        actor: str,
    ) -> LaneExecutionResult:
        return self._execute_and_bind(
            task,
            callback=lambda: adapter(domain_results),
            lease=lease,
            actor=actor,
            attempt=1,
            tier=task.manifest.required_validation_tiers[0],
        )

    def _execute_and_bind(
        self,
        task: TaskControlRecord,
        *,
        callback: Callable[[], Mapping[str, object]],
        lease: ExecutionLease,
        actor: str,
        attempt: int,
        tier: str,
    ) -> LaneExecutionResult:
        try:
            payload = dict(callback())
        except Exception as exc:  # fail-closed adapter boundary
            return LaneExecutionResult(
                task_id=task.task_id,
                change_id=task.manifest.change_id,
                lane_id=lease.lane_id,
                actor=actor,
                attempt=attempt,
                lease_id=lease.lease_id,
                status="FAIL",
                reason_codes=("ADAPTER_EXCEPTION", type(exc).__name__),
                artifact_path=None,
                artifact_sha256=None,
                validation_tier=tier,
                evidence_binding_status="NOT_RUN",
                payload={"error": str(exc)},
            )
        if payload.get("status") != "PASS" or payload.get("production_effect") != "none":
            return LaneExecutionResult(
                task_id=task.task_id,
                change_id=task.manifest.change_id,
                lane_id=lease.lane_id,
                actor=actor,
                attempt=attempt,
                lease_id=lease.lease_id,
                status="FAIL",
                reason_codes=("ADAPTER_RESULT_FAIL_CLOSED",),
                artifact_path=None,
                artifact_sha256=None,
                validation_tier=tier,
                evidence_binding_status="NOT_RUN",
                payload=payload,
            )
        artifact_path = self._artifact_path(task)
        write_json_atomic(
            artifact_path,
            {
                "schema_version": "controlled_lane_artifact.v1",
                "task_id": task.task_id,
                "change_id": task.manifest.change_id,
                "attempt": attempt,
                "status": "PASS",
                "payload": payload,
                "task_governance_status_mutated": False,
                "production_effect": "none",
                "broker_action": "none",
            },
        )
        digest = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
        relative = artifact_path.relative_to(self.project_root).as_posix()
        evidence = ValidationEvidence(
            evidence_id=f"evidence:{task.manifest.change_id}:{attempt}",
            change_id=task.manifest.change_id,
            tier=tier,
            status="PASS",
            artifact_path=relative,
            artifact_sha256=digest,
            base_commit=task.manifest.base_commit,
            change_manifest_sha256=task.manifest.sha256,
            production_effect="none",
        )
        binding = validate_evidence_binding(
            task.manifest,
            [evidence],
            project_root=self.project_root,
        )
        status = "PASS" if binding.status == "PASS" else "FAIL"
        return LaneExecutionResult(
            task_id=task.task_id,
            change_id=task.manifest.change_id,
            lane_id=lease.lane_id,
            actor=actor,
            attempt=attempt,
            lease_id=lease.lease_id,
            status=status,
            reason_codes=(
                ("ADAPTER_AND_EVIDENCE_PASS",) if status == "PASS" else ("EVIDENCE_BINDING_FAIL",)
            ),
            artifact_path=relative,
            artifact_sha256=digest,
            validation_tier=tier,
            evidence_binding_status=binding.status,
            payload=payload,
        )

    def _artifact_path(self, task: TaskControlRecord) -> Path:
        candidates = (*task.manifest.owned_paths, *task.manifest.shared_paths)
        if not candidates:
            raise ParallelControlError("S4_ARTIFACT_PATH_MISSING", task.task_id)
        selected = next(
            (candidate for candidate in candidates if candidate.endswith("/dispatch_summary.json")),
            candidates[0],
        )
        path = (self.project_root / selected).resolve()
        if not path.is_relative_to(self.project_root):
            raise ParallelControlError("S4_ARTIFACT_OUTSIDE_ROOT", str(path))
        return path

    def _report_artifact_path(self, task: TaskControlRecord) -> Path:
        candidate = next(
            (
                path
                for path in (*task.manifest.owned_paths, *task.manifest.shared_paths)
                if path.endswith("/controlled_dispatch_report.json")
            ),
            None,
        )
        if candidate is None:
            raise ParallelControlError("S4_REPORT_PATH_MISSING", task.task_id)
        path = (self.project_root / candidate).resolve()
        if not path.is_relative_to(self.project_root):
            raise ParallelControlError("S4_REPORT_OUTSIDE_ROOT", str(path))
        return path


def validate_controlled_dispatch_report(
    report_path: Path,
    *,
    project_root: Path,
    policy: ParallelControlPolicy,
) -> dict[str, object]:
    root = project_root.resolve()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    checks: list[dict[str, object]] = []

    def check(check_id: str, passed: bool) -> None:
        checks.append({"check_id": check_id, "passed": passed})

    report_body = {key: value for key, value in payload.items() if key != "dispatch_id"}
    check(
        "dispatch_id",
        payload.get("dispatch_id") == f"controlled-dispatch-{_canonical_sha256(report_body)[:20]}",
    )
    check("schema", payload.get("schema_version") == DISPATCH_REPORT_SCHEMA_VERSION)
    check("status", payload.get("status") == "PASS")
    check("policy", payload.get("policy_version") == policy.policy_version)
    check("dispatch_scope", payload.get("dispatch_scope") == "S4_EXPLICIT_PILOT_ALLOWLIST_ONLY")
    check("dispatch_allowed", payload.get("dispatch_allowed") is True)
    check("lease_allowed", payload.get("lease_acquisition_allowed") is True)
    check("source_of_truth", payload.get("source_of_truth") == "LEGACY_MARKDOWN_ONLY")
    check("canonical_cutover", payload.get("canonical_source_cutover") is False)
    for field in (
        "task_governance_status_mutated",
        "generated_task_view_written",
        "strategy_logic_changed",
        "strategy_threshold_changed",
        "paper_shadow_changed",
    ):
        check(f"safety:{field}", payload.get(field) is False)
    check("production_effect", payload.get("production_effect") == "none")
    check("broker_action", payload.get("broker_action") == "none")
    domain = payload.get("final_domain_results")
    check(
        "two_domain_results",
        isinstance(domain, list)
        and len(domain) == policy.max_parallel_domain_lanes
        and all(isinstance(row, dict) and row.get("status") == "PASS" for row in domain),
    )
    coordinator = payload.get("coordinator_result")
    check("coordinator_pass", isinstance(coordinator, dict) and coordinator.get("status") == "PASS")
    failure = payload.get("failure_isolation")
    check(
        "failure_isolation",
        isinstance(failure, dict)
        and failure.get("status") == "PASS"
        and failure.get("first_attempt_status") == "FAIL"
        and failure.get("unaffected_first_attempt_status") == "PASS"
        and failure.get("recovered_status") == "PASS"
        and failure.get("other_lane_blocked") is False,
    )
    replay = payload.get("lease_replay")
    check(
        "lease_replay",
        isinstance(replay, dict)
        and replay.get("status") == "PASS"
        and replay.get("active_leases") == [],
    )
    lease_store_path = (root / str(payload.get("lease_store_path"))).resolve()
    check(
        "lease_store_path",
        lease_store_path.is_relative_to(root) and lease_store_path.is_dir(),
    )
    checksums = payload.get("artifact_checksums")
    checksum_pass = isinstance(checksums, list)
    if checksum_pass:
        for row in checksums:
            if not isinstance(row, dict):
                checksum_pass = False
                break
            candidate = (root / str(row.get("path"))).resolve()
            if not candidate.is_relative_to(root) or not candidate.is_file():
                checksum_pass = False
                break
            if hashlib.sha256(candidate.read_bytes()).hexdigest() != row.get("sha256"):
                checksum_pass = False
                break
    check("artifact_checksums", checksum_pass)
    passed = all(bool(row["passed"]) for row in checks)
    return {
        "schema_version": "controlled_three_lane_dispatch_validation.v1",
        "status": "PASS" if passed else "FAIL",
        "dispatch_id": payload.get("dispatch_id"),
        "checks": checks,
        "failed_check_count": sum(1 for row in checks if not row["passed"]),
        "task_governance_status_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _actor(actors: Mapping[str, str], change_id: str) -> str:
    return _text(actors.get(change_id), f"actor:{change_id}")


def _canonical_sha256(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ParallelControlError("S4_TEXT_REQUIRED", field)
    return value.strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ParallelControlError("S4_TIMEZONE_REQUIRED", value.isoformat())
    return value.astimezone(UTC)
