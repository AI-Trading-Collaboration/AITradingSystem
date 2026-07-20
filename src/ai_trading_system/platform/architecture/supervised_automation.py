from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
from ai_trading_system.platform.architecture.parallel_control_kernel import (
    FileExecutionLeaseStore,
    ParallelControlPolicy,
    TaskControlRecord,
    parse_task_dependency,
    validate_dependency_graph,
)
from ai_trading_system.platform.architecture.parallel_control_scheduler import (
    PilotSpec,
    parse_task_control_record,
    run_shadow_governance_cycles,
)
from ai_trading_system.platform.artifacts import write_json_atomic, write_text_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_SCHEMA_VERSION = "arch_005_supervised_automation_policy.v1"
PILOT_SCHEMA_VERSION = "arch_005_s4a_supervised_pilot.v1"
WORKER_RESULT_SCHEMA_VERSION = "supervised_worker_execution.v1"
INTEGRATION_CANDIDATE_SCHEMA_VERSION = "supervised_integration_candidate.v1"
RUN_REPORT_SCHEMA_VERSION = "supervised_automation_run.v1"
VALIDATION_SCHEMA_VERSION = "supervised_automation_validation.v1"
ORPHAN_AUDIT_SCHEMA_VERSION = "supervised_orphan_audit.v1"

_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_LANE_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,47}$")
_SAFE_BRANCH_PREFIX_RE = re.compile(r"^codex/[a-z0-9][a-z0-9-/]*/$")
_UNSAFE_PATH_PARTS = frozenset({".env", ".aws", ".ssh", "credentials", "secrets"})
_UNSAFE_PATH_SUFFIXES = (".key", ".pem", ".p12", ".pfx")


@dataclass(frozen=True)
class WorkerCommand:
    command_id: str
    argv: tuple[str, ...]
    timeout_seconds: int
    validation_tier: str


@dataclass(frozen=True)
class SupervisedAutomationPolicy:
    policy_id: str
    version: str
    status: str
    control_policy: ParallelControlPolicy
    worktree_root: str
    branch_prefix: str
    require_clean_coordinator: bool
    max_workers: int
    max_timeout_seconds: int
    max_stdout_bytes: int
    max_stderr_bytes: int
    inherited_environment_allowlist: tuple[str, ...]
    commands: tuple[WorkerCommand, ...]
    source_of_truth: str

    @property
    def policy_version(self) -> str:
        return f"{self.policy_id}@{self.version}"

    def command_by_id(self) -> dict[str, WorkerCommand]:
        return {command.command_id: command for command in self.commands}


@dataclass(frozen=True)
class WorkerBinding:
    change_id: str
    command_id: str
    actor: str
    lane_id: str


@dataclass(frozen=True)
class SupervisedPilotSpec:
    pilot: PilotSpec
    workers: tuple[WorkerBinding, ...]

    def worker_by_change_id(self) -> dict[str, WorkerBinding]:
        return {worker.change_id: worker for worker in self.workers}


@dataclass(frozen=True)
class WorkerExecutionResult:
    task_id: str
    change_id: str
    lane_id: str
    actor: str
    command_id: str
    command_argv: tuple[str, ...]
    validation_tier: str
    change_manifest_sha256: str
    owned_paths: tuple[str, ...]
    shared_paths: tuple[str, ...]
    module_ids: tuple[str, ...]
    contract_claims: tuple[dict[str, str], ...]
    worktree_path: str
    branch: str
    base_commit: str
    initial_head: str
    final_head: str
    started_at: str
    finished_at: str
    duration_seconds: float
    pid: int | None
    exit_code: int | None
    timed_out: bool
    stdout_path: str
    stdout_sha256: str
    stdout_bytes: int
    stderr_path: str
    stderr_sha256: str
    stderr_bytes: int
    changed_paths: tuple[str, ...]
    unexpected_changed_paths: tuple[str, ...]
    status: str
    reason_codes: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": WORKER_RESULT_SCHEMA_VERSION,
            "task_id": self.task_id,
            "change_id": self.change_id,
            "lane_id": self.lane_id,
            "actor": self.actor,
            "command_id": self.command_id,
            "command_argv": list(self.command_argv),
            "validation_tier": self.validation_tier,
            "change_manifest_sha256": self.change_manifest_sha256,
            "resource_claims": {
                "owned_paths": list(self.owned_paths),
                "shared_paths": list(self.shared_paths),
                "module_ids": list(self.module_ids),
                "contract_claims": list(self.contract_claims),
            },
            "worktree_path": self.worktree_path,
            "branch": self.branch,
            "base_commit": self.base_commit,
            "initial_head": self.initial_head,
            "final_head": self.final_head,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": round(self.duration_seconds, 6),
            "pid": self.pid,
            "exit_code": self.exit_code,
            "timed_out": self.timed_out,
            "stdout_path": self.stdout_path,
            "stdout_sha256": self.stdout_sha256,
            "stdout_bytes": self.stdout_bytes,
            "stderr_path": self.stderr_path,
            "stderr_sha256": self.stderr_sha256,
            "stderr_bytes": self.stderr_bytes,
            "changed_paths": list(self.changed_paths),
            "unexpected_changed_paths": list(self.unexpected_changed_paths),
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "task_governance_status_mutated": False,
            "automatic_commit_performed": False,
            "automatic_merge_performed": False,
            "automatic_push_performed": False,
            "production_effect": "none",
            "broker_action": "none",
        }


def load_supervised_automation_policy(path: Path) -> SupervisedAutomationPolicy:
    payload = _mapping(safe_load_yaml_path(path), "policy")
    _exact_keys(
        payload,
        {
            "schema_version",
            "policy_id",
            "version",
            "status",
            "owner",
            "approved_by",
            "approval_ref",
            "rationale",
            "review",
            "control",
            "workspace",
            "execution",
            "commands",
            "safety",
        },
        "SUPERVISED_POLICY_FIELDS",
    )
    if payload.get("schema_version") != POLICY_SCHEMA_VERSION:
        raise ParallelControlError("SUPERVISED_POLICY_SCHEMA", str(payload.get("schema_version")))
    if payload.get("status") != "REVIEWED_SUPERVISED_BASELINE":
        raise ParallelControlError("SUPERVISED_POLICY_STATUS", str(payload.get("status")))
    policy_id = _identifier(payload.get("policy_id"), "policy_id")
    version = _text(payload.get("version"), "version")
    control = _mapping(payload.get("control"), "control")
    workspace = _mapping(payload.get("workspace"), "workspace")
    execution = _mapping(payload.get("execution"), "execution")
    safety = _mapping(payload.get("safety"), "safety")
    _exact_keys(
        safety,
        {
            "source_of_truth",
            "task_governance_status_mutation_allowed",
            "generated_task_view_write_allowed",
            "automatic_commit_allowed",
            "automatic_merge_allowed",
            "automatic_push_allowed",
            "automatic_pr_allowed",
            "strategy_logic_change_allowed",
            "strategy_threshold_change_allowed",
            "candidate_expansion_allowed",
            "paper_shadow_change_allowed",
            "production_effect",
            "broker_action",
        },
        "SUPERVISED_POLICY_SAFETY_FIELDS",
    )
    _validate_safety(safety)
    worktree_root = _text(workspace.get("worktree_root"), "worktree_root")
    root_parts = PurePosixPath(worktree_root).parts
    if len(root_parts) != 2 or root_parts[0] != ".." or root_parts[1] in {"", ".", ".."}:
        raise ParallelControlError(
            "SUPERVISED_WORKTREE_ROOT",
            "worktree_root must be one fixed sibling directory",
        )
    branch_prefix = _text(workspace.get("branch_prefix"), "branch_prefix")
    if not _SAFE_BRANCH_PREFIX_RE.fullmatch(branch_prefix):
        raise ParallelControlError("SUPERVISED_BRANCH_PREFIX", branch_prefix)
    max_workers = _positive_int(execution.get("max_workers"), "max_workers")
    max_timeout = _positive_int(execution.get("max_timeout_seconds"), "max_timeout_seconds")
    default_timeout = _positive_int(
        execution.get("default_timeout_seconds"), "default_timeout_seconds"
    )
    if default_timeout > max_timeout:
        raise ParallelControlError("SUPERVISED_DEFAULT_TIMEOUT", str(default_timeout))
    commands_payload = payload.get("commands")
    if not isinstance(commands_payload, list) or not commands_payload:
        raise ParallelControlError("SUPERVISED_COMMANDS", "commands must be a non-empty list")
    commands: list[WorkerCommand] = []
    for raw_command in commands_payload:
        row = _mapping(raw_command, "command")
        _exact_keys(
            row,
            {"command_id", "argv", "timeout_seconds", "validation_tier"},
            "SUPERVISED_COMMAND_FIELDS",
        )
        argv = _strings(row.get("argv"), "argv")
        if not argv or argv[0] != "{python}" or any("{" in arg or "}" in arg for arg in argv[1:]):
            raise ParallelControlError(
                "SUPERVISED_COMMAND_ARGV",
                "argv must start with exact {python} and contain no other placeholders",
            )
        timeout = _positive_int(row.get("timeout_seconds"), "timeout_seconds")
        if timeout > max_timeout:
            raise ParallelControlError("SUPERVISED_COMMAND_TIMEOUT", str(timeout))
        commands.append(
            WorkerCommand(
                command_id=_identifier(row.get("command_id"), "command_id"),
                argv=argv,
                timeout_seconds=timeout,
                validation_tier=_identifier(row.get("validation_tier"), "validation_tier"),
            )
        )
    if len(commands) != len({item.command_id for item in commands}):
        raise ParallelControlError("SUPERVISED_COMMAND_DUPLICATE", "command ids must be unique")
    allowlisted_tasks = _strings(control.get("allowlisted_task_ids"), "allowlisted_task_ids")
    allowlisted_actors = _strings(control.get("allowlisted_actors"), "allowlisted_actors")
    hard_dependency_types = _strings(control.get("hard_dependency_types"), "hard_dependency_types")
    if set(hard_dependency_types) != {"blocks_start", "blocks_completion"}:
        raise ParallelControlError(
            "SUPERVISED_HARD_DEPENDENCY_TYPES", ",".join(hard_dependency_types)
        )
    priority_order = _strings(control.get("priority_order"), "priority_order")
    if priority_order != ("P0", "P1", "P2", "P3"):
        raise ParallelControlError("SUPERVISED_PRIORITY_ORDER", ",".join(priority_order))
    control_policy = ParallelControlPolicy(
        policy_id=policy_id,
        version=version,
        status=str(payload.get("status")),
        eligible_governance_statuses=_strings(
            control.get("eligible_governance_statuses"), "eligible_governance_statuses"
        ),
        hard_dependency_types=hard_dependency_types,
        require_requirement_refs=_boolean(
            control.get("require_requirement_refs"), "require_requirement_refs"
        ),
        require_acceptance_criteria=_boolean(
            control.get("require_acceptance_criteria"), "require_acceptance_criteria"
        ),
        max_parallel_domain_lanes=_positive_int(
            control.get("max_parallel_domain_lanes"), "max_parallel_domain_lanes"
        ),
        max_total_active_leases=_positive_int(
            control.get("max_total_active_leases"), "max_total_active_leases"
        ),
        priority_order=priority_order,
        tie_breakers=_strings(control.get("tie_breakers"), "tie_breakers"),
        fairness_mode=_text(control.get("fairness_mode"), "fairness_mode"),
        lease_ttl_seconds=_positive_int(control.get("lease_ttl_seconds"), "lease_ttl_seconds"),
        max_reassignments=_non_negative_int(control.get("max_reassignments"), "max_reassignments"),
        arbiter_ttl_seconds=_positive_int(
            control.get("arbiter_ttl_seconds"), "arbiter_ttl_seconds"
        ),
        require_exact_base_commit=_boolean(
            control.get("require_exact_base_commit"), "require_exact_base_commit"
        ),
        require_manifest_hash_binding=_boolean(
            control.get("require_manifest_hash_binding"), "require_manifest_hash_binding"
        ),
        allowlisted_task_ids=allowlisted_tasks,
        allowlisted_actors=allowlisted_actors,
        failure_injection_change_id="none",
        source_of_truth=str(safety.get("source_of_truth")),
    )
    if max_workers != control_policy.max_parallel_domain_lanes:
        raise ParallelControlError("SUPERVISED_WORKER_CAPACITY", str(max_workers))
    if max_workers != 2 or control_policy.max_total_active_leases != 2:
        raise ParallelControlError(
            "SUPERVISED_NARROW_CAPACITY", "S4A requires exactly two workers and leases"
        )
    return SupervisedAutomationPolicy(
        policy_id=policy_id,
        version=version,
        status=str(payload.get("status")),
        control_policy=control_policy,
        worktree_root=worktree_root,
        branch_prefix=branch_prefix,
        require_clean_coordinator=_boolean(
            workspace.get("require_clean_coordinator"), "require_clean_coordinator"
        ),
        max_workers=max_workers,
        max_timeout_seconds=max_timeout,
        max_stdout_bytes=_positive_int(execution.get("max_stdout_bytes"), "max_stdout_bytes"),
        max_stderr_bytes=_positive_int(execution.get("max_stderr_bytes"), "max_stderr_bytes"),
        inherited_environment_allowlist=_strings(
            execution.get("inherited_environment_allowlist"),
            "inherited_environment_allowlist",
        ),
        commands=tuple(commands),
        source_of_truth=str(safety.get("source_of_truth")),
    )


def load_supervised_pilot_spec(
    path: Path,
    *,
    current_base_commit: str,
    policy: SupervisedAutomationPolicy,
) -> SupervisedPilotSpec:
    _commit(current_base_commit, "current_base_commit")
    payload = _mapping(safe_load_yaml_path(path), "pilot")
    _exact_keys(
        payload,
        {
            "schema_version",
            "pilot_id",
            "policy_id",
            "base_commit_source",
            "tasks",
            "dependencies",
            "governance_cycles",
            "workers",
            "safety",
        },
        "SUPERVISED_PILOT_FIELDS",
    )
    if payload.get("schema_version") != PILOT_SCHEMA_VERSION:
        raise ParallelControlError("SUPERVISED_PILOT_SCHEMA", str(payload.get("schema_version")))
    if payload.get("policy_id") != policy.policy_id:
        raise ParallelControlError("SUPERVISED_PILOT_POLICY", str(payload.get("policy_id")))
    if payload.get("base_commit_source") != "runtime_head":
        raise ParallelControlError(
            "SUPERVISED_PILOT_BASE_SOURCE", str(payload.get("base_commit_source"))
        )
    raw_tasks = payload.get("tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise ParallelControlError("SUPERVISED_PILOT_TASKS", "tasks must be non-empty")
    tasks = tuple(
        sorted(
            (
                parse_task_control_record(
                    _mapping(row, "task"), current_base_commit=current_base_commit
                )
                for row in raw_tasks
            ),
            key=lambda task: task.task_id,
        )
    )
    if {task.task_id for task in tasks} != set(policy.control_policy.allowlisted_task_ids):
        raise ParallelControlError(
            "SUPERVISED_PILOT_TASK_ALLOWLIST", "task set differs from policy allowlist"
        )
    if len(tasks) != len({task.manifest.change_id for task in tasks}):
        raise ParallelControlError("SUPERVISED_PILOT_CHANGE_DUPLICATE", "duplicate change id")
    raw_dependencies = payload.get("dependencies")
    if not isinstance(raw_dependencies, list):
        raise ParallelControlError("SUPERVISED_PILOT_DEPENDENCIES", "must be a list")
    dependencies = tuple(
        sorted(parse_task_dependency(_mapping(row, "dependency")) for row in raw_dependencies)
    )
    graph = validate_dependency_graph([task.task_id for task in tasks], dependencies)
    if graph.status != "PASS":
        raise ParallelControlError("SUPERVISED_PILOT_GRAPH", graph.issues[0].code)
    cycles = _strings(payload.get("governance_cycles"), "governance_cycles")
    if len(cycles) < 2:
        raise ParallelControlError("SUPERVISED_PILOT_CYCLES", "at least two are required")
    raw_workers = payload.get("workers")
    if not isinstance(raw_workers, list) or not raw_workers:
        raise ParallelControlError("SUPERVISED_PILOT_WORKERS", "workers must be non-empty")
    commands = policy.command_by_id()
    tasks_by_change = {task.manifest.change_id: task for task in tasks}
    workers: list[WorkerBinding] = []
    for raw_worker in raw_workers:
        row = _mapping(raw_worker, "worker")
        _exact_keys(
            row,
            {"change_id", "command_id", "actor", "lane_id"},
            "SUPERVISED_WORKER_FIELDS",
        )
        binding = WorkerBinding(
            change_id=_identifier(row.get("change_id"), "change_id"),
            command_id=_identifier(row.get("command_id"), "command_id"),
            actor=_identifier(row.get("actor"), "actor"),
            lane_id=_lane(row.get("lane_id")),
        )
        task = tasks_by_change.get(binding.change_id)
        command = commands.get(binding.command_id)
        if task is None or command is None:
            raise ParallelControlError("SUPERVISED_WORKER_BINDING", binding.change_id)
        if binding.actor not in policy.control_policy.allowlisted_actors:
            raise ParallelControlError("SUPERVISED_WORKER_ACTOR", binding.actor)
        if command.validation_tier not in task.manifest.required_validation_tiers:
            raise ParallelControlError("SUPERVISED_WORKER_VALIDATION_TIER", command.validation_tier)
        workers.append(binding)
    if {worker.change_id for worker in workers} != set(tasks_by_change):
        raise ParallelControlError(
            "SUPERVISED_WORKER_COVERAGE", "every task requires exactly one worker"
        )
    if len(workers) != len({worker.change_id for worker in workers}) or len(workers) != len(
        {worker.lane_id for worker in workers}
    ):
        raise ParallelControlError("SUPERVISED_WORKER_DUPLICATE", "change/lane must be unique")
    safety = _mapping(payload.get("safety"), "safety")
    _exact_keys(
        safety,
        {
            "production_effect",
            "broker_action",
            "task_governance_status_mutation_allowed",
            "generated_task_view_write_allowed",
            "automatic_commit_allowed",
            "automatic_merge_allowed",
            "automatic_push_allowed",
            "strategy_logic_change_allowed",
            "candidate_expansion_allowed",
        },
        "SUPERVISED_PILOT_SAFETY_FIELDS",
    )
    _validate_safety(safety, require_source_of_truth=False)
    pilot = PilotSpec(
        pilot_id=_identifier(payload.get("pilot_id"), "pilot_id"),
        policy_id=policy.policy_id,
        tasks=tasks,
        dependencies=dependencies,
        governance_cycles=cycles,
        failure_recovery={},
        safety=safety,
    )
    return SupervisedPilotSpec(pilot=pilot, workers=tuple(sorted(workers, key=lambda x: x.lane_id)))


class SupervisedAutomationController:
    def __init__(
        self,
        *,
        project_root: Path,
        runtime_root: Path,
        policy_path: Path,
        pilot_path: Path,
    ) -> None:
        self.project_root = project_root.resolve()
        self.runtime_root = runtime_root.resolve()
        self.policy_path = policy_path.resolve()
        self.pilot_path = pilot_path.resolve()
        self.policy = load_supervised_automation_policy(self.policy_path)

    def plan(self, *, base_commit: str | None = None) -> dict[str, object]:
        base = base_commit or _git(self.project_root, "rev-parse", "HEAD")
        _commit(base, "base_commit")
        spec = load_supervised_pilot_spec(
            self.pilot_path, current_base_commit=base, policy=self.policy
        )
        audit = run_shadow_governance_cycles(
            spec.pilot,
            policy=self.policy.control_policy,
            current_base_commit=base,
            observed_statuses={},
        )
        expected = {worker.change_id for worker in spec.workers}
        selected = {str(row["change_id"]) for row in audit.decision.selected}
        status = "PASS" if audit.status == "PASS" and selected == expected else "FAIL"
        return {
            "schema_version": "supervised_automation_plan.v1",
            "status": status,
            "policy_version": self.policy.policy_version,
            "pilot_id": spec.pilot.pilot_id,
            "base_commit": base,
            "selected_change_ids": sorted(selected),
            "worker_count": len(spec.workers),
            "shadow_governance_audit": audit.to_dict(),
            "dispatch_allowed_by_this_artifact": False,
            "task_governance_status_mutated": False,
            "production_effect": "none",
            "broker_action": "none",
        }

    def run(self, *, started_at: datetime) -> Path:
        start = _utc(started_at)
        base = _git(self.project_root, "rev-parse", "HEAD")
        _commit(base, "base_commit")
        if self.policy.require_clean_coordinator and _changed_paths(self.project_root):
            raise ParallelControlError(
                "SUPERVISED_COORDINATOR_DIRTY", "coordinator worktree must be clean"
            )
        spec = load_supervised_pilot_spec(
            self.pilot_path, current_base_commit=base, policy=self.policy
        )
        plan = self.plan(base_commit=base)
        if plan["status"] != "PASS":
            raise ParallelControlError("SUPERVISED_PLAN_BLOCKED", "shadow plan failed")
        run_id = _run_id(base, start)
        run_root = (self.runtime_root / "runs" / run_id).resolve()
        if run_root.exists():
            raise ParallelControlError("SUPERVISED_RUN_EXISTS", str(run_root))
        run_root.mkdir(parents=True)
        worktree_root = _worktree_root(self.project_root, self.policy)
        worktree_run_root = (worktree_root / run_id).resolve()
        _require_contained(worktree_run_root, worktree_root, "SUPERVISED_WORKTREE_ESCAPE")
        worktree_run_root.mkdir(parents=True, exist_ok=False)
        worktrees: dict[str, tuple[Path, str]] = {}
        for binding in spec.workers:
            path = (worktree_run_root / binding.lane_id).resolve()
            _require_contained(path, worktree_run_root, "SUPERVISED_WORKTREE_ESCAPE")
            branch = f"{self.policy.branch_prefix}{run_id}/{binding.lane_id}"
            _validate_git_branch(self.project_root, branch)
            _git(self.project_root, "worktree", "add", "-b", branch, str(path), base)
            if (
                _git(path, "rev-parse", "HEAD") != base
                or _git(path, "branch", "--show-current") != branch
            ):
                raise ParallelControlError("SUPERVISED_WORKTREE_IDENTITY", binding.lane_id)
            worktrees[binding.change_id] = (path, branch)
        lease_store = FileExecutionLeaseStore(
            run_root / "leases", policy=self.policy.control_policy
        )
        audit_payload = _mapping(plan["shadow_governance_audit"], "shadow_governance_audit")
        readiness_rows = _mapping(audit_payload.get("decision"), "decision").get("readiness")
        if not isinstance(readiness_rows, list):
            raise ParallelControlError("SUPERVISED_READINESS", "missing readiness rows")
        readiness_by_change = {
            str(row["change_id"]): row for row in readiness_rows if isinstance(row, Mapping)
        }
        task_by_change = spec.pilot.task_by_change_id()
        # Recompute typed readiness through the deterministic scheduler result instead of
        # trusting serialized YAML rows.
        audit = run_shadow_governance_cycles(
            spec.pilot,
            policy=self.policy.control_policy,
            current_base_commit=base,
            observed_statuses={},
        )
        typed_readiness = {row.change_id: row for row in audit.decision.readiness}
        acquisitions: dict[str, object] = {}
        for binding in spec.workers:
            if binding.change_id not in readiness_by_change:
                raise ParallelControlError("SUPERVISED_READINESS", binding.change_id)
            acquisition = lease_store.acquire(
                task=task_by_change[binding.change_id],
                readiness=typed_readiness[binding.change_id],
                lane_id=binding.lane_id,
                actor=binding.actor,
                current_base_commit=base,
                now=start,
            )
            if acquisition.status != "ACTIVE":
                raise ParallelControlError(
                    "SUPERVISED_LEASE_BLOCKED", ",".join(acquisition.reason_codes)
                )
            acquisitions[binding.change_id] = acquisition
        commands = self.policy.command_by_id()
        results: dict[str, WorkerExecutionResult] = {}
        with ThreadPoolExecutor(max_workers=self.policy.max_workers) as pool:
            futures = {}
            for binding in spec.workers:
                task = task_by_change[binding.change_id]
                worktree_path, branch = worktrees[binding.change_id]
                future = pool.submit(
                    _execute_worker,
                    task=task,
                    binding=binding,
                    command=commands[binding.command_id],
                    worktree=worktree_path,
                    branch=branch,
                    base_commit=base,
                    run_root=run_root,
                    policy=self.policy,
                )
                futures[future] = binding
            for future in as_completed(futures):
                binding = futures[future]
                try:
                    result = future.result()
                except Exception as exc:  # fail-closed evidence for infrastructure exceptions
                    worktree_path, branch = worktrees[binding.change_id]
                    result = _exception_worker_result(
                        task=task_by_change[binding.change_id],
                        binding=binding,
                        command=commands[binding.command_id],
                        worktree=worktree_path,
                        branch=branch,
                        base_commit=base,
                        run_root=run_root,
                        exc=exc,
                    )
                results[binding.change_id] = result
        worker_refs: list[dict[str, object]] = []
        terminal_at = datetime.now(UTC)
        for binding in spec.workers:
            result = results[binding.change_id]
            artifact_relative = f"workers/{binding.lane_id}/worker_result.json"
            artifact_path = run_root / artifact_relative
            write_json_atomic(artifact_path, result.to_dict())
            artifact_sha = _sha256(artifact_path)
            acquisition = acquisitions[binding.change_id]
            if result.status == "PASS":
                lease_store.release(
                    acquisition.lease.lease_id,
                    actor=binding.actor,
                    now=terminal_at,
                    evidence_refs=(artifact_relative,),
                )
            else:
                lease_store.expire(
                    acquisition.lease.lease_id,
                    actor=binding.actor,
                    now=terminal_at,
                    reason_code="WORKER_EXECUTION_FAILED",
                )
            worker_refs.append(
                {
                    "change_id": binding.change_id,
                    "lane_id": binding.lane_id,
                    "artifact_path": artifact_relative,
                    "artifact_sha256": artifact_sha,
                    "status": result.status,
                    "worktree_path": result.worktree_path,
                    "branch": result.branch,
                    "base_commit": result.base_commit,
                    "final_head": result.final_head,
                    "command_id": result.command_id,
                    "validation_tier": result.validation_tier,
                    "change_manifest_sha256": result.change_manifest_sha256,
                    "changed_paths": list(result.changed_paths),
                }
            )
        replay = lease_store.replay()
        candidate_body = {
            "schema_version": INTEGRATION_CANDIDATE_SCHEMA_VERSION,
            "run_id": run_id,
            "base_commit": base,
            "policy_version": self.policy.policy_version,
            "pilot_id": spec.pilot.pilot_id,
            "status": (
                "AWAITING_HUMAN_COORDINATOR_APPROVAL"
                if all(row["status"] == "PASS" for row in worker_refs)
                and replay.status == "PASS"
                and not replay.active_leases
                else "BLOCKED_WORKER_OR_LEASE_FAILURE"
            ),
            "worker_evidence": sorted(worker_refs, key=lambda row: str(row["change_id"])),
            "lease_replay": replay.to_dict(),
            "human_coordinator_approval_required": True,
            "human_coordinator_approved": False,
            "merge_allowed": False,
            "automatic_commit_allowed": False,
            "automatic_merge_allowed": False,
            "automatic_push_allowed": False,
            "automatic_pr_allowed": False,
            "task_governance_status_mutated": False,
            "production_effect": "none",
            "broker_action": "none",
        }
        candidate = {
            "candidate_id": f"integration-candidate-{_canonical_sha256(candidate_body)[:20]}",
            **candidate_body,
        }
        candidate_relative = "integration_candidate.json"
        candidate_path = run_root / candidate_relative
        write_json_atomic(candidate_path, candidate)
        finished = datetime.now(UTC)
        report_body = {
            "schema_version": RUN_REPORT_SCHEMA_VERSION,
            "run_id": run_id,
            "status": (
                "PASS" if candidate["status"] == "AWAITING_HUMAN_COORDINATOR_APPROVAL" else "FAIL"
            ),
            "base_commit": base,
            "policy_version": self.policy.policy_version,
            "policy_sha256": _sha256(self.policy_path),
            "pilot_id": spec.pilot.pilot_id,
            "pilot_spec_sha256": _sha256(self.pilot_path),
            "started_at": start.isoformat(),
            "finished_at": finished.isoformat(),
            "duration_seconds": round((finished - start).total_seconds(), 6),
            "shadow_governance_audit": plan["shadow_governance_audit"],
            "workers": sorted(worker_refs, key=lambda row: str(row["change_id"])),
            "integration_candidate_path": candidate_relative,
            "integration_candidate_sha256": _sha256(candidate_path),
            "lease_store_path": "leases",
            "lease_replay": replay.to_dict(),
            "human_coordinator_approval_required": True,
            "human_coordinator_approved": False,
            "merge_allowed": False,
            "canonical_source_cutover": False,
            "task_governance_status_mutated": False,
            "automatic_commit_performed": False,
            "automatic_merge_performed": False,
            "automatic_push_performed": False,
            "automatic_pr_performed": False,
            "source_of_truth": self.policy.source_of_truth,
            "production_effect": "none",
            "broker_action": "none",
        }
        report = {
            "report_id": f"supervised-run-{_canonical_sha256(report_body)[:20]}",
            **report_body,
        }
        report_path = run_root / "supervised_run_report.json"
        write_json_atomic(report_path, report)
        return report_path


def validate_supervised_run(
    report_path: Path,
    *,
    project_root: Path,
    policy_path: Path,
    pilot_path: Path,
) -> dict[str, object]:
    report_path = report_path.resolve()
    run_root = report_path.parent
    report = _mapping(json.loads(report_path.read_text(encoding="utf-8")), "report")
    checks: list[dict[str, object]] = []

    def check(check_id: str, passed: bool, detail: str = "") -> None:
        checks.append({"check_id": check_id, "passed": passed, "detail": detail})

    base = str(report.get("base_commit"))
    try:
        _commit(base, "base_commit")
        policy = load_supervised_automation_policy(policy_path)
        spec = load_supervised_pilot_spec(pilot_path, current_base_commit=base, policy=policy)
    except (OSError, ValueError, ParallelControlError) as exc:
        return _failed_validation(checks, f"LOAD_FAILED:{exc}")
    body = dict(report)
    claimed_report_id = body.pop("report_id", None)
    check(
        "report_id",
        claimed_report_id == f"supervised-run-{_canonical_sha256(body)[:20]}",
    )
    check("report_schema", report.get("schema_version") == RUN_REPORT_SCHEMA_VERSION)
    check("report_status", report.get("status") == "PASS")
    check("policy_sha256", report.get("policy_sha256") == _sha256(policy_path))
    check("pilot_sha256", report.get("pilot_spec_sha256") == _sha256(pilot_path))
    check("policy_version", report.get("policy_version") == policy.policy_version)
    check("git_base_exists", _git_commit_exists(project_root.resolve(), base))
    workers = report.get("workers")
    worker_rows = workers if isinstance(workers, list) else []
    check("worker_count", len(worker_rows) == len(spec.workers))
    task_by_change = spec.pilot.task_by_change_id()
    binding_by_change = spec.worker_by_change_id()
    worker_artifacts_pass = True
    worktree_identity_pass = True
    for row_value in worker_rows:
        if not isinstance(row_value, Mapping):
            worker_artifacts_pass = False
            continue
        row = row_value
        change_id = str(row.get("change_id"))
        binding = binding_by_change.get(change_id)
        task = task_by_change.get(change_id)
        artifact_relative = str(row.get("artifact_path"))
        try:
            artifact_path = _contained_artifact(run_root, artifact_relative)
            artifact_sha = _sha256(artifact_path)
            payload = _mapping(
                json.loads(artifact_path.read_text(encoding="utf-8")), "worker_result"
            )
            stdout = _contained_artifact(run_root, str(payload.get("stdout_path")))
            stderr = _contained_artifact(run_root, str(payload.get("stderr_path")))
            expected_paths = (
                () if task is None else (*task.manifest.owned_paths, *task.manifest.shared_paths)
            )
            changed = tuple(str(item) for item in payload.get("changed_paths", []))
            unexpected = _unexpected_paths(changed, expected_paths)
            expected_command = (
                None if binding is None else policy.command_by_id()[binding.command_id]
            )
            expected_argv = (
                []
                if expected_command is None
                else [
                    str(Path(sys.executable)) if arg == "{python}" else arg
                    for arg in expected_command.argv
                ]
            )
            resource_claims = payload.get("resource_claims")
            expected_claims = (
                {}
                if task is None
                else {
                    "owned_paths": list(task.manifest.owned_paths),
                    "shared_paths": list(task.manifest.shared_paths),
                    "module_ids": list(task.manifest.module_ids),
                    "contract_claims": [claim.to_dict() for claim in task.manifest.contract_claims],
                }
            )
            row_ok = (
                binding is not None
                and task is not None
                and artifact_sha == row.get("artifact_sha256")
                and payload.get("schema_version") == WORKER_RESULT_SCHEMA_VERSION
                and payload.get("status") == "PASS"
                and payload.get("base_commit") == base
                and payload.get("initial_head") == base
                and payload.get("final_head") == base
                and payload.get("command_id") == binding.command_id
                and payload.get("command_argv") == expected_argv
                and payload.get("actor") == binding.actor
                and payload.get("lane_id") == binding.lane_id
                and payload.get("change_manifest_sha256") == task.manifest.sha256
                and resource_claims == expected_claims
                and payload.get("stdout_sha256") == _sha256(stdout)
                and payload.get("stdout_bytes") == stdout.stat().st_size
                and payload.get("stderr_sha256") == _sha256(stderr)
                and payload.get("stderr_bytes") == stderr.stat().st_size
                and not unexpected
                and not any(_secret_like_path(item) for item in changed)
            )
            worker_artifacts_pass = worker_artifacts_pass and row_ok
            worktree = Path(str(payload.get("worktree_path"))).resolve()
            expected_root = _worktree_root(project_root.resolve(), policy)
            identity_ok = (
                worktree.exists()
                and worktree.is_relative_to(expected_root)
                and _git(worktree, "rev-parse", "HEAD") == base
                and _git(worktree, "branch", "--show-current") == payload.get("branch")
                and tuple(_changed_paths(worktree)) == changed
            )
            worktree_identity_pass = worktree_identity_pass and identity_ok
        except (
            OSError,
            ValueError,
            json.JSONDecodeError,
            ParallelControlError,
            subprocess.CalledProcessError,
        ):
            worker_artifacts_pass = False
            worktree_identity_pass = False
    check("worker_artifacts", worker_artifacts_pass)
    check("worktree_identity", worktree_identity_pass)
    candidate_ok = False
    try:
        candidate_path = _contained_artifact(
            run_root, str(report.get("integration_candidate_path"))
        )
        candidate = _mapping(json.loads(candidate_path.read_text(encoding="utf-8")), "candidate")
        candidate_body = dict(candidate)
        candidate_id = candidate_body.pop("candidate_id", None)
        candidate_ok = (
            _sha256(candidate_path) == report.get("integration_candidate_sha256")
            and candidate_id == f"integration-candidate-{_canonical_sha256(candidate_body)[:20]}"
            and candidate.get("schema_version") == INTEGRATION_CANDIDATE_SCHEMA_VERSION
            and candidate.get("status") == "AWAITING_HUMAN_COORDINATOR_APPROVAL"
            and candidate.get("human_coordinator_approval_required") is True
            and candidate.get("human_coordinator_approved") is False
            and candidate.get("merge_allowed") is False
            and candidate.get("production_effect") == "none"
            and candidate.get("broker_action") == "none"
        )
    except (OSError, ValueError, json.JSONDecodeError, ParallelControlError):
        candidate_ok = False
    check("integration_candidate", candidate_ok)
    try:
        lease_path = _contained_artifact(run_root, str(report.get("lease_store_path")))
        replay = (
            FileExecutionLeaseStore(lease_path, policy=policy.control_policy).replay().to_dict()
        )
        lease_ok = (
            replay == report.get("lease_replay")
            and replay.get("status") == "PASS"
            and replay.get("active_leases") == []
        )
    except (OSError, ValueError, ParallelControlError):
        lease_ok = False
    check("lease_replay", lease_ok)
    safety_ok = all(
        (
            report.get("human_coordinator_approval_required") is True,
            report.get("human_coordinator_approved") is False,
            report.get("merge_allowed") is False,
            report.get("canonical_source_cutover") is False,
            report.get("task_governance_status_mutated") is False,
            report.get("automatic_commit_performed") is False,
            report.get("automatic_merge_performed") is False,
            report.get("automatic_push_performed") is False,
            report.get("automatic_pr_performed") is False,
            report.get("source_of_truth") == "LEGACY_MARKDOWN_ONLY",
            report.get("production_effect") == "none",
            report.get("broker_action") == "none",
        )
    )
    check("safety_boundary", safety_ok)
    passed = all(bool(row["passed"]) for row in checks)
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "status": "PASS" if passed else "FAIL",
        "report_id": report.get("report_id"),
        "checks": checks,
        "failed_check_count": sum(1 for row in checks if not row["passed"]),
        "human_coordinator_approval_required": True,
        "merge_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def audit_supervised_orphans(
    report_path: Path,
    *,
    project_root: Path,
    policy_path: Path,
) -> dict[str, object]:
    report_path = report_path.resolve()
    run_root = report_path.parent
    report = _mapping(json.loads(report_path.read_text(encoding="utf-8")), "report")
    policy = load_supervised_automation_policy(policy_path)
    issues: list[dict[str, str]] = []
    try:
        lease_path = _contained_artifact(run_root, str(report.get("lease_store_path")))
        replay = FileExecutionLeaseStore(lease_path, policy=policy.control_policy).replay()
        if replay.status != "PASS":
            issues.append({"code": "LEASE_REPLAY_INVALID", "resource": str(lease_path)})
        for lease in replay.active_leases:
            issues.append({"code": "ACTIVE_LEASE", "resource": lease.lease_id})
    except (OSError, ValueError, ParallelControlError):
        issues.append({"code": "LEASE_STORE_MISSING_OR_INVALID", "resource": "leases"})
    expected_root = _worktree_root(project_root.resolve(), policy)
    workers = report.get("workers")
    for row in workers if isinstance(workers, list) else []:
        if not isinstance(row, Mapping):
            issues.append({"code": "WORKER_ROW_INVALID", "resource": "workers"})
            continue
        artifact_relative = str(row.get("artifact_path"))
        try:
            artifact = _contained_artifact(run_root, artifact_relative)
            payload = _mapping(json.loads(artifact.read_text(encoding="utf-8")), "worker")
        except (OSError, ValueError, json.JSONDecodeError, ParallelControlError):
            issues.append({"code": "WORKER_ARTIFACT_MISSING", "resource": artifact_relative})
            continue
        worktree = Path(str(payload.get("worktree_path"))).resolve()
        if not worktree.exists() or not worktree.is_relative_to(expected_root):
            issues.append({"code": "WORKTREE_MISSING_OR_UNSCOPED", "resource": str(worktree)})
            continue
        changed = _changed_paths(worktree)
        if changed:
            issues.append({"code": "UNREVIEWED_DIRTY_WORKTREE", "resource": ",".join(changed)})
    return {
        "schema_version": ORPHAN_AUDIT_SCHEMA_VERSION,
        "status": "PASS" if not issues else "FAIL",
        "report_id": report.get("report_id"),
        "issues": issues,
        "issue_count": len(issues),
        "cleanup_performed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def cleanup_clean_supervised_worktrees(
    report_path: Path,
    *,
    project_root: Path,
    policy_path: Path,
    coordinator_approved: bool,
) -> dict[str, object]:
    if coordinator_approved is not True:
        raise ParallelControlError(
            "SUPERVISED_CLEANUP_APPROVAL_REQUIRED", "explicit coordinator approval is required"
        )
    report = _mapping(json.loads(report_path.read_text(encoding="utf-8")), "report")
    policy = load_supervised_automation_policy(policy_path)
    expected_root = _worktree_root(project_root.resolve(), policy)
    candidates: list[Path] = []
    workers = report.get("workers")
    for row in workers if isinstance(workers, list) else []:
        if not isinstance(row, Mapping):
            raise ParallelControlError("SUPERVISED_CLEANUP_WORKER", "invalid worker row")
        worktree = Path(str(row.get("worktree_path"))).resolve()
        _require_contained(worktree, expected_root, "SUPERVISED_CLEANUP_ESCAPE")
        if not worktree.exists():
            raise ParallelControlError("SUPERVISED_CLEANUP_MISSING", str(worktree))
        if _changed_paths(worktree):
            raise ParallelControlError("SUPERVISED_CLEANUP_DIRTY", str(worktree))
        candidates.append(worktree)
    for worktree in candidates:
        _git(project_root.resolve(), "worktree", "remove", str(worktree))
    return {
        "schema_version": "supervised_cleanup_result.v1",
        "status": "PASS",
        "removed_worktrees": [str(path) for path in candidates],
        "branches_deleted": [],
        "coordinator_approved": True,
        "force_used": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _execute_worker(
    *,
    task: TaskControlRecord,
    binding: WorkerBinding,
    command: WorkerCommand,
    worktree: Path,
    branch: str,
    base_commit: str,
    run_root: Path,
    policy: SupervisedAutomationPolicy,
) -> WorkerExecutionResult:
    started = datetime.now(UTC)
    monotonic_start = time.monotonic()
    initial_head = _git(worktree, "rev-parse", "HEAD")
    initial_branch = _git(worktree, "branch", "--show-current")
    if initial_head != base_commit or initial_branch != branch or _changed_paths(worktree):
        raise ParallelControlError("SUPERVISED_WORKTREE_PRECONDITION", binding.lane_id)
    lane_root = run_root / "workers" / binding.lane_id
    lane_root.mkdir(parents=True, exist_ok=False)
    stdout_relative = f"workers/{binding.lane_id}/stdout.log"
    stderr_relative = f"workers/{binding.lane_id}/stderr.log"
    stdout_path = run_root / stdout_relative
    stderr_path = run_root / stderr_relative
    argv = tuple(str(Path(sys.executable)) if arg == "{python}" else arg for arg in command.argv)
    environment = {
        key: value
        for key, value in os.environ.items()
        if key.upper() in set(policy.inherited_environment_allowlist)
    }
    environment["PYTHONPATH"] = str(worktree / "src")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    creationflags = 0
    start_new_session = False
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        start_new_session = True
    process: subprocess.Popen[bytes] | None = None
    timed_out = False
    with stdout_path.open("wb") as stdout_stream, stderr_path.open("wb") as stderr_stream:
        process = subprocess.Popen(
            argv,
            cwd=worktree,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=stdout_stream,
            stderr=stderr_stream,
            shell=False,
            creationflags=creationflags,
            start_new_session=start_new_session,
        )
        try:
            exit_code = process.wait(timeout=command.timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            _terminate_process_tree(process)
            exit_code = process.wait(timeout=30)
    finished = datetime.now(UTC)
    final_head = _git(worktree, "rev-parse", "HEAD")
    final_branch = _git(worktree, "branch", "--show-current")
    changed = tuple(_changed_paths(worktree))
    unexpected = _unexpected_paths(
        changed, (*task.manifest.owned_paths, *task.manifest.shared_paths)
    )
    reasons: list[str] = []
    if timed_out:
        reasons.append("COMMAND_TIMEOUT")
    if exit_code != 0:
        reasons.append("COMMAND_EXIT_NONZERO")
    if stdout_path.stat().st_size > policy.max_stdout_bytes:
        reasons.append("STDOUT_BUDGET_EXCEEDED")
    if stderr_path.stat().st_size > policy.max_stderr_bytes:
        reasons.append("STDERR_BUDGET_EXCEEDED")
    if final_head != base_commit:
        reasons.append("WORKER_HEAD_CHANGED")
    if final_branch != branch:
        reasons.append("WORKER_BRANCH_CHANGED")
    if unexpected:
        reasons.append("UNEXPECTED_CHANGED_PATH")
    if any(_secret_like_path(path) for path in changed):
        reasons.append("SECRET_LIKE_PATH_DETECTED")
    if not reasons:
        reasons.append("EXECUTION_AND_GIT_GATES_PASS")
    return WorkerExecutionResult(
        task_id=task.task_id,
        change_id=binding.change_id,
        lane_id=binding.lane_id,
        actor=binding.actor,
        command_id=command.command_id,
        command_argv=argv,
        validation_tier=command.validation_tier,
        change_manifest_sha256=task.manifest.sha256,
        owned_paths=task.manifest.owned_paths,
        shared_paths=task.manifest.shared_paths,
        module_ids=task.manifest.module_ids,
        contract_claims=tuple(claim.to_dict() for claim in task.manifest.contract_claims),
        worktree_path=str(worktree),
        branch=branch,
        base_commit=base_commit,
        initial_head=initial_head,
        final_head=final_head,
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        duration_seconds=time.monotonic() - monotonic_start,
        pid=None if process is None else process.pid,
        exit_code=exit_code,
        timed_out=timed_out,
        stdout_path=stdout_relative,
        stdout_sha256=_sha256(stdout_path),
        stdout_bytes=stdout_path.stat().st_size,
        stderr_path=stderr_relative,
        stderr_sha256=_sha256(stderr_path),
        stderr_bytes=stderr_path.stat().st_size,
        changed_paths=changed,
        unexpected_changed_paths=unexpected,
        status="PASS" if reasons == ["EXECUTION_AND_GIT_GATES_PASS"] else "FAIL",
        reason_codes=tuple(sorted(reasons)),
    )


def _exception_worker_result(
    *,
    task: TaskControlRecord,
    binding: WorkerBinding,
    command: WorkerCommand,
    worktree: Path,
    branch: str,
    base_commit: str,
    run_root: Path,
    exc: Exception,
) -> WorkerExecutionResult:
    lane_root = run_root / "workers" / binding.lane_id
    lane_root.mkdir(parents=True, exist_ok=True)
    stdout_relative = f"workers/{binding.lane_id}/stdout.log"
    stderr_relative = f"workers/{binding.lane_id}/stderr.log"
    stdout_path = run_root / stdout_relative
    stderr_path = run_root / stderr_relative
    stdout_path.touch(exist_ok=True)
    write_text_atomic(stderr_path, f"{type(exc).__name__}: {exc}\n")
    now = datetime.now(UTC).isoformat()
    try:
        final_head = _git(worktree, "rev-parse", "HEAD")
        changed = tuple(_changed_paths(worktree))
    except (OSError, subprocess.CalledProcessError):
        final_head = ""
        changed = ()
    return WorkerExecutionResult(
        task_id=task.task_id,
        change_id=binding.change_id,
        lane_id=binding.lane_id,
        actor=binding.actor,
        command_id=command.command_id,
        command_argv=tuple(command.argv),
        validation_tier=command.validation_tier,
        change_manifest_sha256=task.manifest.sha256,
        owned_paths=task.manifest.owned_paths,
        shared_paths=task.manifest.shared_paths,
        module_ids=task.manifest.module_ids,
        contract_claims=tuple(claim.to_dict() for claim in task.manifest.contract_claims),
        worktree_path=str(worktree),
        branch=branch,
        base_commit=base_commit,
        initial_head=base_commit,
        final_head=final_head,
        started_at=now,
        finished_at=now,
        duration_seconds=0.0,
        pid=None,
        exit_code=None,
        timed_out=False,
        stdout_path=stdout_relative,
        stdout_sha256=_sha256(stdout_path),
        stdout_bytes=stdout_path.stat().st_size,
        stderr_path=stderr_relative,
        stderr_sha256=_sha256(stderr_path),
        stderr_bytes=stderr_path.stat().st_size,
        changed_paths=changed,
        unexpected_changed_paths=_unexpected_paths(
            changed, (*task.manifest.owned_paths, *task.manifest.shared_paths)
        ),
        status="FAIL",
        reason_codes=("WORKER_INFRASTRUCTURE_EXCEPTION",),
    )


def _terminate_process_tree(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    if process.poll() is None:
        process.kill()


def _worktree_root(project_root: Path, policy: SupervisedAutomationPolicy) -> Path:
    root = (project_root / Path(policy.worktree_root)).resolve()
    if root.parent != project_root.parent or root == project_root:
        raise ParallelControlError("SUPERVISED_WORKTREE_ROOT", str(root))
    return root


def _validate_git_branch(project_root: Path, branch: str) -> None:
    if not branch.startswith("codex/") or ".." in branch:
        raise ParallelControlError("SUPERVISED_BRANCH_ESCAPE", branch)
    result = subprocess.run(
        ["git", "check-ref-format", "--branch", branch],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or result.stdout.strip() != branch:
        raise ParallelControlError("SUPERVISED_BRANCH_INVALID", branch)


def _git(project_root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _git_commit_exists(project_root: Path, commit: str) -> bool:
    result = subprocess.run(
        ["git", "cat-file", "-e", f"{commit}^{{commit}}"],
        cwd=project_root,
        check=False,
        capture_output=True,
    )
    return result.returncode == 0


def _changed_paths(worktree: Path) -> list[str]:
    rows: set[str] = set()
    for args in (
        ("diff", "--name-only", "--cached"),
        ("diff", "--name-only"),
        ("ls-files", "--others", "--exclude-standard"),
    ):
        output = _git(worktree, *args)
        rows.update(line.strip().replace("\\", "/") for line in output.splitlines() if line.strip())
    return sorted(rows)


def _unexpected_paths(changed: Sequence[str], allowed: Sequence[str]) -> tuple[str, ...]:
    normalized_allowed = tuple(path.rstrip("/") for path in allowed)
    return tuple(
        sorted(
            path
            for path in changed
            if not any(path == item or path.startswith(f"{item}/") for item in normalized_allowed)
        )
    )


def _secret_like_path(path: str) -> bool:
    lowered = path.lower()
    parts = set(PurePosixPath(lowered).parts)
    return bool(parts & _UNSAFE_PATH_PARTS) or lowered.endswith(_UNSAFE_PATH_SUFFIXES)


def _contained_artifact(root: Path, relative: str) -> Path:
    pure = PurePosixPath(relative)
    if pure.is_absolute() or ".." in pure.parts or not pure.parts:
        raise ParallelControlError("SUPERVISED_ARTIFACT_PATH", relative)
    path = (root / Path(*pure.parts)).resolve()
    _require_contained(path, root.resolve(), "SUPERVISED_ARTIFACT_ESCAPE")
    return path


def _require_contained(path: Path, root: Path, code: str) -> None:
    if path == root or not path.is_relative_to(root):
        raise ParallelControlError(code, str(path))


def _validate_safety(safety: Mapping[str, Any], *, require_source_of_truth: bool = True) -> None:
    if safety.get("production_effect") != "none" or safety.get("broker_action") != "none":
        raise ParallelControlError("SUPERVISED_SAFETY", "production and broker must be none")
    if require_source_of_truth and safety.get("source_of_truth") != "LEGACY_MARKDOWN_ONLY":
        raise ParallelControlError("SUPERVISED_SOURCE_OF_TRUTH", str(safety.get("source_of_truth")))
    for key, value in safety.items():
        if key.endswith("_allowed") and value is not False:
            raise ParallelControlError("SUPERVISED_UNSAFE_PERMISSION", key)


def _failed_validation(checks: list[dict[str, object]], detail: str) -> dict[str, object]:
    checks.append({"check_id": "load", "passed": False, "detail": detail})
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "status": "FAIL",
        "checks": checks,
        "failed_check_count": 1,
        "human_coordinator_approval_required": True,
        "merge_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _run_id(base_commit: str, started_at: datetime) -> str:
    identity = f"{base_commit}|{started_at.isoformat()}|arch-005-s4a"
    return f"supervised-{hashlib.sha256(identity.encode()).hexdigest()[:16]}"


def _canonical_sha256(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ParallelControlError("SUPERVISED_TIMEZONE", "timezone-aware datetime required")
    return value.astimezone(UTC)


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ParallelControlError("SUPERVISED_MAPPING_REQUIRED", field)
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str], code: str) -> None:
    if set(value) != expected:
        raise ParallelControlError(code, f"expected={sorted(expected)} actual={sorted(value)}")


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ParallelControlError("SUPERVISED_TEXT_REQUIRED", field)
    return value.strip()


def _identifier(value: object, field: str) -> str:
    text = _text(value, field)
    if not _ID_RE.fullmatch(text):
        raise ParallelControlError("SUPERVISED_IDENTIFIER", f"{field}:{text}")
    return text


def _lane(value: object) -> str:
    text = _text(value, "lane_id")
    if not _LANE_RE.fullmatch(text):
        raise ParallelControlError("SUPERVISED_LANE", text)
    return text


def _commit(value: object, field: str) -> str:
    text = _text(value, field)
    if not _COMMIT_RE.fullmatch(text):
        raise ParallelControlError("SUPERVISED_COMMIT", f"{field}:{text}")
    return text


def _strings(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ParallelControlError("SUPERVISED_LIST_REQUIRED", field)
    rows = tuple(_text(item, field) for item in value)
    if len(rows) != len(set(rows)):
        raise ParallelControlError("SUPERVISED_LIST_DUPLICATE", field)
    return rows


def _boolean(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ParallelControlError("SUPERVISED_BOOLEAN_REQUIRED", field)
    return value


def _positive_int(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ParallelControlError("SUPERVISED_POSITIVE_INT", field)
    return value


def _non_negative_int(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ParallelControlError("SUPERVISED_NON_NEGATIVE_INT", field)
    return value
