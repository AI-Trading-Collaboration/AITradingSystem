"""DEVX-016 S3: the reviewed commands a publication run wraps.

The fence CLI (acquire, checkpoint, release, replay), the five official generators, the checkout
guard's worktree audit and the governed preflight. Nothing here re-implements a rule: each helper
runs the existing command, keeps its full output as an evidence log and turns a non-zero exit or an
unreadable result into a StepFailed with the log path, so the step that called it fails closed.
"""

from __future__ import annotations

import json
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from ai_trading_system.platform.architecture.publication_orchestrator import StepFailed
from ai_trading_system.platform.architecture.publication_scope import DerivedPublicationScope
from ai_trading_system.platform.artifacts.writer import write_bytes_atomic

FENCE_SCRIPT = "scripts/architecture_arch005_publication_fence.py"
GUARD_SCRIPT = "scripts/architecture_arch005_checkout_guard.py"
READINESS_SCRIPT = "scripts/validation_readiness.py"
LEASE_SEAL_SCRIPT = "scripts/architecture_arch005_lease_seal.py"
PREFLIGHT_SCRIPT = "tools/codex_skills/run-governed-development/scripts/preflight.py"
TRANSACTION_ROOT = "outputs/architecture/arch_005_integration_publication_fence/transactions"

# The five official generators, in the reviewed fence-policy order. The commands are the ones the
# manual flow ran; {head} is the exact candidate commit the Atlas generator binds.
GENERATOR_COMMANDS: Mapping[str, tuple[str, ...]] = {
    "canonical-task-source": ("scripts/architecture_arch005_task_source.py", "validate"),
    "architecture-manifests": ("scripts/architecture_devex.py", "generate"),
    "atlas-authority": (
        "scripts/render_atlas_strategy_research_page.py",
        "--repository-root",
        ".",
        "--exact-commit",
        "{head}",
    ),
    "report-flow-authority": ("scripts/architecture_report_catalog_flow_authority.py", "build"),
    "compatibility-authority": ("scripts/architecture_compatibility_authority.py", "build"),
}

# Wall-clock baselines measured on this host (DEVX-022 section 15.1 and the DEVX-016 C1 chain);
# they only drive the SLOW_STEP report, never a pass/fail decision.
BASELINE_ACQUIRE_SECONDS = 60.0
BASELINE_GENERATOR_ROUND_SECONDS = 180.0
BASELINE_RELEASE_SECONDS = 70.0
BASELINE_GIT_SECONDS = 30.0
BASELINE_PREFLIGHT_SECONDS = 60.0
BASELINE_READINESS_SECONDS = 75.0
# LOCAL_MAIN_FF_PRE and REMOTE_PUSH_PRE re-verify the whole closure (several lease-store replays):
# measured 190-205 s and 217-218 s on the C2 and the DEVX-023 P chains (DEVX-022 section 17.8).
BASELINE_FENCE_CLOSURE_SECONDS = 210.0
# A06 rebuilds the replay seal (a full validation of every chain) and verifies it against the
# serial replay: 35.2 s + 37.2 s on the real store of 6,375 events (DEVX-023 section 10.9.2).
# Reporting baseline only.
BASELINE_SEAL_SECONDS = 90.0


@dataclass(frozen=True)
class CommandResult:
    argv: tuple[str, ...]
    exit_code: int
    stdout: str
    stderr: str
    elapsed_seconds: float
    log_path: str


class CommandRunner(Protocol):
    def __call__(self, argv: Sequence[str], *, log_name: str) -> CommandResult: ...


class SubprocessCommandRunner:
    """Runs a command in the repository root and keeps its full output as an evidence log."""

    def __init__(self, *, cwd: Path, evidence_dir: Path, env: Mapping[str, str]) -> None:
        self.cwd = cwd
        self.evidence_dir = evidence_dir
        self.env = dict(env)

    def __call__(self, argv: Sequence[str], *, log_name: str) -> CommandResult:
        started = time.monotonic()
        completed = subprocess.run(
            list(argv),
            cwd=self.cwd,
            env=self.env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        elapsed = time.monotonic() - started
        self.evidence_dir.mkdir(parents=True, exist_ok=True)
        log = self.evidence_dir / f"{log_name}.log"
        body = (
            f"$ {' '.join(argv)}\n--- stdout\n{completed.stdout}\n--- stderr\n{completed.stderr}\n"
        )
        # A run-evidence log under the git-ignored outputs tree, not a governed artifact.
        write_bytes_atomic(log, body.encode("utf-8"))
        return CommandResult(
            tuple(argv), completed.returncode, completed.stdout, completed.stderr, elapsed, str(log)
        )


@dataclass(frozen=True)
class RunConfig:
    run_id: str
    repository_root: Path
    python: str
    evidence_dir: Path
    actor: str
    thread_id: str
    task_id: str
    expected_main: str
    scope: DerivedPublicationScope
    phase_order: tuple[str, ...]
    parent_run: str | None
    commit_trailer: str | None
    contract_change: bool = False
    max_generator_rounds: int = 3
    # Re-derives the scope from the CURRENT head (frozen_base..HEAD); used before the formal
    # acquire to prove the prep commits only added shared (generated) paths.
    scope_check: Callable[[], DerivedPublicationScope] | None = None

    def transaction_path(self, transaction_id: str) -> Path:
        return self.repository_root / TRANSACTION_ROOT / transaction_id / "transaction.json"


def command_failure(code: str, message: str, result: CommandResult | None = None) -> StepFailed:
    detail: dict[str, Any] = {}
    if result is not None:
        detail = {"exit_code": result.exit_code, "log": result.log_path}
    return StepFailed(code, message, detail)


def json_object(result: CommandResult, *, code: str) -> dict[str, Any]:
    if result.exit_code != 0:
        raise command_failure(
            code, f"exit {result.exit_code}: {' '.join(result.argv[2:6])}", result
        )
    try:
        parsed = json.loads(result.stdout)
    except json.JSONDecodeError:
        parsed = None
    if not isinstance(parsed, dict):
        raise command_failure(code, "stdout is not a JSON object", result)
    return parsed


class FenceCli:
    """The reviewed fence CLI, one method per command the manual flow ran."""

    def __init__(self, config: RunConfig, runner: CommandRunner) -> None:
        self.config = config
        self.runner = runner

    def _call(self, argv: Sequence[str], label: str, *, code: str) -> dict[str, Any]:
        result = self.runner(tuple(argv), log_name=f"{self.config.run_id}_{label}")
        return {**json_object(result, code=code), "_log": result.log_path}

    def acquire(
        self, transaction_id: str, *, lane_head: str, with_full_parent: bool, label: str
    ) -> dict[str, Any]:
        config = self.config
        argv = [
            config.python, "-B", FENCE_SCRIPT, "acquire",
            "--transaction-id", transaction_id,
            "--task-id", config.task_id,
            "--change-id", transaction_id,
            "--thread-id", config.thread_id,
            "--actor", config.actor,
            "--frozen-base", config.expected_main,
            "--lane-head", lane_head,
            "--expected-main", config.expected_main,
        ]  # fmt: skip
        for path in config.scope.owned_paths:
            argv += ["--owned-path", path]
        for path in (*config.scope.shared_paths, *config.scope.resource_paths):
            argv += ["--shared-path", path]
        for generator in config.scope.generator_ids:
            argv += ["--generator-id", generator]
        for tier in config.scope.required_validation_tiers:
            argv += ["--required-tier", tier]
        if with_full_parent:
            if not config.parent_run:
                raise StepFailed("PUBLICATION_RUN_PARENT_RUN_REQUIRED", "正式事务需要 --parent-run")
            argv += ["--full-parent", config.parent_run]
        return self._call(argv, label, code="PUBLICATION_RUN_FENCE_ACQUIRE_FAILED")

    def checkpoint(
        self, transaction_id: str, phase: str, *, label: str, generators: bool = False
    ) -> dict[str, Any]:
        config = self.config
        argv = [
            config.python, "-B", FENCE_SCRIPT, "checkpoint",
            "--transaction", str(config.transaction_path(transaction_id)),
            "--phase", phase,
            "--actor", config.actor,
        ]  # fmt: skip
        if generators:
            for generator in config.scope.generator_ids:
                argv += ["--generator-id", generator]
        return self._call(argv, label, code=f"PUBLICATION_RUN_FENCE_{phase}_FAILED")

    def release(
        self, transaction_id: str, *, outcome: str, evidence: str, label: str
    ) -> dict[str, Any]:
        config = self.config
        argv = [
            config.python, "-B", FENCE_SCRIPT, "release",
            "--transaction", str(config.transaction_path(transaction_id)),
            "--actor", config.actor,
            "--outcome", outcome,
            "--evidence", evidence,
        ]  # fmt: skip
        return self._call(argv, label, code="PUBLICATION_RUN_FENCE_RELEASE_FAILED")

    def replay(self, transaction_id: str, *, label: str) -> dict[str, Any] | None:
        path = self.config.transaction_path(transaction_id)
        if not path.exists():
            return None
        argv = [self.config.python, "-B", FENCE_SCRIPT, "replay", "--transaction", str(path)]
        return self._call(argv, label, code="PUBLICATION_RUN_FENCE_REPLAY_FAILED")

    def phase_reached(
        self, transaction_id: str, phase: str, *, label: str
    ) -> dict[str, Any] | None:
        """The replay if the transaction is at or beyond `phase`, else None (a resume probe)."""
        order = self.config.phase_order
        replayed = self.replay(transaction_id, label=label)
        if replayed is None or replayed.get("status") != "PASS":
            return None
        current = str(replayed.get("phase"))
        if current in {"FAILED", "RELEASED"}:
            return replayed
        if current in order and phase in order and order.index(current) >= order.index(phase):
            return replayed
        return None


@dataclass(frozen=True)
class AuditResult:
    status: str
    dirty_paths: tuple[str, ...]


def worktree_audit(config: RunConfig, runner: CommandRunner, label: str) -> AuditResult:
    argv = [config.python, "-B", GUARD_SCRIPT, "worktree-audit"]
    result = runner(tuple(argv), log_name=f"{config.run_id}_{label}")
    body = json_object(result, code="PUBLICATION_RUN_WORKTREE_AUDIT_FAILED")
    dirty = body.get("dirty_paths")
    if body.get("status") != "PASS" or not isinstance(dirty, list):
        raise command_failure(
            "PUBLICATION_RUN_WORKTREE_AUDIT_FAILED", str(body.get("status")), result
        )
    return AuditResult("PASS", tuple(str(path) for path in dirty))


def lease_heartbeat(
    config: RunConfig, runner: CommandRunner, *, lease_id: str, label: str
) -> dict[str, Any]:
    """The sanctioned lease heartbeat (the validation driver and every fence checkpoint use it too).

    Used while the run waits for the owner's push, so the lease does not expire in their absence.
    """
    argv = [
        config.python, "-B", GUARD_SCRIPT, "heartbeat",
        "--lease-id", lease_id,
        "--actor", config.actor,
    ]  # fmt: skip
    result = runner(tuple(argv), log_name=f"{config.run_id}_{label}")
    body = json_object(result, code="PUBLICATION_RUN_LEASE_HEARTBEAT_FAILED")
    if body.get("status") != "PASS":
        raise command_failure(
            "PUBLICATION_RUN_LEASE_HEARTBEAT_FAILED", str(body.get("status")), result
        )
    return {"status": "PASS", "log": result.log_path}


def git_text(runner: CommandRunner, config: RunConfig, label: str, *args: str) -> str:
    result = runner(("git", *args), log_name=f"{config.run_id}_{label}")
    if result.exit_code != 0:
        raise command_failure("PUBLICATION_RUN_GIT_FAILED", f"git {' '.join(args[:3])}", result)
    return result.stdout.strip()


def _generator_argv(config: RunConfig, generator_id: str, head: str) -> list[str]:
    command = GENERATOR_COMMANDS.get(generator_id)
    if command is None:
        raise StepFailed("PUBLICATION_RUN_GENERATOR_UNKNOWN", generator_id)
    return [config.python, "-B", *[part.format(head=head) for part in command]]


def run_generators(
    config: RunConfig, runner: CommandRunner, *, head: str, label: str
) -> dict[str, float]:
    """The five official generators in the declared order; any non-zero exit fails the step."""
    elapsed: dict[str, float] = {}
    for generator in config.scope.generator_ids:
        result = runner(
            tuple(_generator_argv(config, generator, head)),
            log_name=f"{config.run_id}_{label}_{generator}",
        )
        elapsed[generator] = round(result.elapsed_seconds, 1)
        if result.exit_code != 0:
            raise command_failure("PUBLICATION_RUN_GENERATOR_FAILED", generator, result)
    return elapsed


def commit_generated(
    config: RunConfig, runner: CommandRunner, audit: AuditResult, *, subject: str, label: str
) -> str:
    """Stage exactly the audited generated paths (inside the derived scope) and commit them."""
    allowed = tuple(config.scope.shared_paths)
    outside = [
        path
        for path in audit.dirty_paths
        if not any(path == entry or path.startswith(entry.rstrip("/") + "/") for entry in allowed)
    ]
    if outside:
        raise StepFailed(
            "PUBLICATION_RUN_DIRTY_OUTSIDE_SCOPE",
            f"{len(outside)} 个脏路径不在生成物范围内",
            {"paths": outside[:20]},
        )
    git_text(runner, config, f"{label}_add", "add", "--", *audit.dirty_paths)
    message = subject
    if config.commit_trailer:
        message += f"\n\n{config.commit_trailer}"
    git_text(runner, config, f"{label}_commit", "commit", "-q", "-m", message)
    return git_text(runner, config, f"{label}_head", "rev-parse", "HEAD")


def _preflight_argv(config: RunConfig, *, stage: str, base: str, transaction_id: str) -> list[str]:
    argv = [
        config.python, "-B", PREFLIGHT_SCRIPT,
        "--repo", ".",
        "--mode", "SINGLE_LANE",
        "--task-id", config.task_id,
        "--role", "coordinator",
        "--stage", stage,
        "--expected-base", base,
    ]  # fmt: skip
    if config.contract_change:
        argv.append("--contract-change")
    if stage == "CLOSEOUT":
        argv.append("--remote-action")
    argv += ["--publication-transaction", str(config.transaction_path(transaction_id))]
    for path in config.scope.owned_paths:
        argv += ["--claim", f"task={path}"]
    for path in (*config.scope.shared_paths, *config.scope.resource_paths):
        argv += ["--coordinator-path", path]
    return argv


def governed_preflight(
    config: RunConfig,
    runner: CommandRunner,
    *,
    stage: str,
    base: str,
    transaction_id: str,
    label: str,
) -> dict[str, Any]:
    result = runner(
        tuple(_preflight_argv(config, stage=stage, base=base, transaction_id=transaction_id)),
        log_name=f"{config.run_id}_{label}",
    )
    # The preflight exits non-zero for BLOCKED but still prints its JSON: read it either way.
    try:
        body = json.loads(result.stdout)
    except json.JSONDecodeError:
        raise command_failure("PUBLICATION_RUN_PREFLIGHT_UNREADABLE", stage, result) from None
    if not isinstance(body, dict) or body.get("status") != "PASS" or result.exit_code != 0:
        status = body.get("status") if isinstance(body, dict) else "?"
        blockers = body.get("blockers") if isinstance(body, dict) else None
        raise StepFailed(
            "PUBLICATION_RUN_PREFLIGHT_BLOCKED",
            f"{stage} 预检状态为 {status}",
            {"blockers": blockers, "log": result.log_path},
        )
    return {"status": "PASS", "warnings": body.get("warnings", []), "log": result.log_path}
