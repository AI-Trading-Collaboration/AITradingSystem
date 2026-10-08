"""DEVX-016 S3: `plan | run | resume | status` of the single publication command.

`run` starts or continues ONE publication run: it derives the scope from the candidate diff, keeps a
hash-chained journal under outputs/architecture/publication_runs/<run_id>/, executes the stages in
the manual flow's order and stops at the owner authorization gate. `resume` is `run` with the same
run id: the journal, not the arguments, decides where to continue. The command only orchestrates the
existing reviewed commands; it never grants a push and never decides anything the fence decides.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture.integration_publication_fence import (
    load_publication_fence_policy,
)
from ai_trading_system.platform.architecture.publication_commands import (
    CommandResult,
    CommandRunner,
    RunConfig,
)
from ai_trading_system.platform.architecture.publication_journal import (
    PublicationJournal,
    PublicationJournalError,
    PublicationRunLock,
)
from ai_trading_system.platform.architecture.publication_orchestrator import (
    OUTCOME_AWAITING_AUTHORIZATION,
    OUTCOME_COMPLETE,
    OUTCOME_PAUSED,
    PublicationRunEngine,
    RunSummary,
    Step,
    StepContext,
)
from ai_trading_system.platform.architecture.publication_publish_steps import (
    DetachedLauncher,
    FactsCollector,
    PublishConfig,
    build_publication_steps,
    build_validation_steps,
)
from ai_trading_system.platform.architecture.publication_scope import (
    DEFAULT_POLICY_PATH,
    DerivedPublicationScope,
    PublicationScopeError,
    PublicationScopePolicy,
    changed_paths_between,
    derive_publication_scope,
    load_publication_scope_policy,
)
from ai_trading_system.platform.architecture.publication_steps import (
    build_formal_steps,
    build_precondition_steps,
    build_prepare_steps,
)
from ai_trading_system.platform.artifacts.writer import canonical_json_bytes, write_bytes_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

RUNS_ROOT = Path("outputs/architecture/publication_runs")
DEFAULT_FENCE_POLICY_PATH = Path("config/architecture/arch_005_integration_publication_fence.yaml")
DEFAULT_GUARD_POLICY_PATH = Path("config/architecture/arch_005_s4d_checkout_guard.yaml")
RUN_SCHEMA_VERSION = "devx_016_publication_run.v1"
EXIT_COMPLETE = 0
EXIT_FAILED = 2
EXIT_AWAITING_AUTHORIZATION = 3
EXIT_PAUSED = 4
# How long `--wait-for-owner-push` waits by default; the lease is renewed meanwhile, so this is a
# bound on a forgotten run, not a lease limit.
DEFAULT_OWNER_WAIT_MINUTES = 720.0
# What the owner / agent should do next for the refusals that have a standard answer.
REFUSAL_HINTS = {
    "PUBLICATION_SCOPE_PATH_FORBIDDEN": (
        "候选改动了普通发布不允许触碰的路径（范围策略文件本身、known-unrelated 排除项等）："
        "这类候选走手工链，"
        "或先把该路径从候选里移出（DEVX-016 第 9 节 C2 的记录说明了手工链）"
    ),
}


@dataclass(frozen=True)
class CliEnvironment:
    """Everything the command touches outside the repository, injectable for tests."""

    repository_root: Path
    python: str
    make_runner: Callable[[Path], CommandRunner]
    launcher: DetachedLauncher
    collector: FactsCollector
    interpreter: Callable[[], tuple[str, tuple[int, int]]]
    free_disk_gb: Callable[[], float]
    sleep: Callable[[float], None]
    monotonic: Callable[[], float]
    now: Callable[[], datetime]
    git: Callable[[Sequence[str]], str]


def default_environment(repository_root: Path) -> CliEnvironment:
    """The real host wiring: subprocess runner, WMI launcher, process/lease/disk observations."""
    import os
    import shutil
    import time

    from ai_trading_system.platform.architecture.checkout_guard import CheckoutLeaseGuard
    from ai_trading_system.platform.architecture.publication_commands import (
        SubprocessCommandRunner,
    )
    from ai_trading_system.platform.architecture.publication_services import (
        HostFactsCollector,
        WmiDetachedLauncher,
        run_plain,
        subprocess_powershell,
    )

    child_env = {
        **os.environ,
        "PYTHONPATH": str(repository_root / "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }

    def git(args: Sequence[str]) -> str:
        completed = run_plain(("git", *args), cwd=repository_root, env=child_env)
        if completed.exit_code != 0:
            raise PublicationJournalError("PUBLICATION_RUN_GIT_FAILED", " ".join(args[:3]))
        return completed.stdout.strip()

    powershell = subprocess_powershell(
        lambda argv: run_plain(argv, cwd=repository_root, env=child_env)
    )

    def lease_replay() -> tuple[str, tuple[str, ...]]:
        replay = CheckoutLeaseGuard(project_root=repository_root).replay()
        return replay.status, tuple(sorted(lease.lease_id for lease in replay.active_leases))

    return CliEnvironment(
        repository_root=repository_root,
        python=sys.executable,
        make_runner=lambda evidence: SubprocessCommandRunner(
            cwd=repository_root, evidence_dir=evidence, env=child_env
        ),
        launcher=WmiDetachedLauncher(
            run_powershell=powershell,
            environment={
                "PYTHONPATH": str(repository_root / "src"),
                "PYTHONDONTWRITEBYTECODE": "1",
            },
        ),
        collector=HostFactsCollector(
            repository_root=repository_root,
            run_powershell=powershell,
            git=git,
            lease_replay=lease_replay,
        ),
        interpreter=lambda: (sys.executable, (sys.version_info.major, sys.version_info.minor)),
        free_disk_gb=lambda: shutil.disk_usage(repository_root).free / 1e9,
        sleep=time.sleep,
        monotonic=time.monotonic,
        now=lambda: datetime.now(tz=UTC),
        git=git,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="One-command ordinary publication run (DEVX-016 S3)"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "run", "resume", "status"):
        command = sub.add_parser(name)
        command.add_argument("--run-id", required=(name != "plan"))
        command.add_argument("--expected-main", help="frozen base; default: local main now")
        command.add_argument("--scope-policy", type=Path, default=DEFAULT_POLICY_PATH)
        if name == "plan":
            command.add_argument("--allow-proposed-policy", action="store_true")
        if name in {"run", "resume"}:
            command.add_argument("--parent-run", help="failed Full summary the formal Full binds")
            command.add_argument("--authorization", type=Path)
            command.add_argument("--task-id", default="GOV-007_PRE_MIGRATION_CONVERGENCE_PROGRAM")
            command.add_argument("--actor", default="integration-coordinator")
            command.add_argument("--thread-id", default="publication-run")
            command.add_argument("--contract-change", action="store_true")
            command.add_argument("--commit-trailer")
            command.add_argument("--until")
            command.add_argument("--retry")
            command.add_argument("--takeover", action="store_true")
            # Owner opt-in: without it this command never pushes (the owner pushes themselves).
            command.add_argument("--push-by-command", action="store_true")
            # Owner-push mode: stay alive (renewing the lease) until the owner has pushed.
            command.add_argument("--wait-for-owner-push", action="store_true")
            command.add_argument(
                "--owner-wait-minutes", type=float, default=DEFAULT_OWNER_WAIT_MINUTES
            )
    return parser


def run_directory(repository_root: Path, run_id: str) -> Path:
    if not run_id or any(c in run_id for c in '\\/:*?"<>| ') or run_id.startswith("."):
        raise PublicationJournalError("PUBLICATION_RUN_ID_INVALID", run_id)
    return repository_root / RUNS_ROOT / run_id


def _forbidden_from_guard(repository_root: Path) -> tuple[str, ...]:
    payload = safe_load_yaml_path(repository_root / DEFAULT_GUARD_POLICY_PATH)
    rows = payload.get("known_unrelated_exclusions", []) if isinstance(payload, dict) else []
    return tuple(str(row["path"]) for row in rows if isinstance(row, dict) and row.get("path"))


def derive_scope(
    environment: CliEnvironment,
    *,
    expected_main: str,
    head: str,
    scope_policy: Path,
    allow_proposed: bool = False,
) -> tuple[DerivedPublicationScope, str, PublicationScopePolicy]:
    policy = load_publication_scope_policy(
        environment.repository_root / scope_policy, allow_proposed=allow_proposed
    )
    fence = load_publication_fence_policy(environment.repository_root / DEFAULT_FENCE_POLICY_PATH)
    diff = changed_paths_between(environment.repository_root, expected_main, head)
    scope = derive_publication_scope(
        policy,
        diff_paths=diff,
        generator_ids=fence.allowed_generator_ids,
        required_validation_tiers=fence.required_formal_tiers,
        extra_forbidden=_forbidden_from_guard(environment.repository_root),
    )
    fence_sha = hashlib.sha256(
        (environment.repository_root / DEFAULT_FENCE_POLICY_PATH).read_bytes()
    ).hexdigest()
    return scope, fence_sha, policy


def command_plan(args: argparse.Namespace, environment: CliEnvironment) -> dict[str, Any]:
    expected = args.expected_main or environment.git(["rev-parse", "main"])
    head = environment.git(["rev-parse", "HEAD"])
    scope, _, policy = derive_scope(
        environment,
        expected_main=expected,
        head=head,
        scope_policy=args.scope_policy,
        allow_proposed=bool(getattr(args, "allow_proposed_policy", False)),
    )
    return {
        "schema_version": "devx_016_publication_plan.v1",
        "expected_main": expected,
        "head": head,
        "scope": scope.to_dict(),
        "policy_status": policy.status,
        "production_effect": "none",
        "broker_action": "none",
    }


def _load_or_create_run_record(
    args: argparse.Namespace, environment: CliEnvironment, run_dir: Path
) -> dict[str, Any]:
    record_path = run_dir / "run.json"
    if record_path.exists():
        loaded = json.loads(record_path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise PublicationJournalError("PUBLICATION_RUN_RECORD_INVALID", str(record_path))
        record: dict[str, Any] = loaded
        if (
            record.get("schema_version") != RUN_SCHEMA_VERSION
            or record.get("run_id") != args.run_id
        ):
            raise PublicationJournalError("PUBLICATION_RUN_RECORD_INVALID", str(record_path))
        conflicts = [
            field
            for field, given in (
                ("parent_run", args.parent_run),
                ("expected_main", args.expected_main),
            )
            if given is not None and given != record.get(field)
        ]
        if args.contract_change and not record.get("contract_change"):
            conflicts.append("contract_change")
        if conflicts:
            raise PublicationJournalError("PUBLICATION_RUN_RECORD_CONFLICT", ",".join(conflicts))
        return record
    expected = args.expected_main or environment.git(["rev-parse", "main"])
    head = environment.git(["rev-parse", "HEAD"])
    scope, fence_sha, policy = derive_scope(
        environment, expected_main=expected, head=head, scope_policy=args.scope_policy
    )
    record = {
        "schema_version": RUN_SCHEMA_VERSION,
        "run_id": args.run_id,
        "expected_main": expected,
        "start_head": head,
        "start_branch": environment.git(["branch", "--show-current"]),
        "parent_run": args.parent_run,
        "contract_change": bool(args.contract_change),
        "commit_trailer": args.commit_trailer,
        "task_id": args.task_id,
        "actor": args.actor,
        "thread_id": args.thread_id,
        "scope": scope.to_dict(),
        "limits": {
            "max_hours_since_full_end": policy.max_hours_since_full_end,
            "min_free_disk_gb": policy.min_free_disk_gb,
            "max_generator_rounds": policy.max_generator_rounds,
        },
        "scope_policy_sha256": policy.sha256,
        "scope_policy_path": str(args.scope_policy),
        "fence_policy_sha256": fence_sha,
        "created_at": environment.now().isoformat(),
        "production_effect": "none",
        "broker_action": "none",
    }
    run_dir.mkdir(parents=True, exist_ok=True)
    write_bytes_atomic(record_path, canonical_json_bytes(record))
    return record


def _scope_from_record(record: Mapping[str, Any]) -> DerivedPublicationScope:
    scope = record["scope"]
    return DerivedPublicationScope(
        owned_paths=tuple(scope["owned_paths"]),
        shared_paths=tuple(scope["shared_paths"]),
        generator_ids=tuple(scope["generator_ids"]),
        required_validation_tiers=tuple(scope["required_validation_tiers"]),
        diff_paths=tuple(scope["diff_paths"]),
        shared_diff_paths=tuple(scope["shared_diff_paths"]),
        resource_paths=tuple(scope.get("resource_paths", ())),
        policy_id=str(scope["policy_id"]),
        policy_version=str(scope["policy_version"]),
        policy_sha256=str(scope["policy_sha256"]),
    )


def build_engine(
    record: Mapping[str, Any],
    environment: CliEnvironment,
    run_dir: Path,
    args: argparse.Namespace,
) -> PublicationRunEngine:
    fence = load_publication_fence_policy(environment.repository_root / DEFAULT_FENCE_POLICY_PATH)
    evidence = run_dir / "evidence"
    runner = environment.make_runner(evidence)
    run_config = RunConfig(
        run_id=str(record["run_id"]),
        repository_root=environment.repository_root,
        python=environment.python,
        evidence_dir=evidence,
        actor=str(record["actor"]),
        thread_id=str(record["thread_id"]),
        task_id=str(record["task_id"]),
        expected_main=str(record["expected_main"]),
        scope=_scope_from_record(record),
        phase_order=tuple(fence.phase_order),
        parent_run=record.get("parent_run"),
        commit_trailer=record.get("commit_trailer"),
        contract_change=bool(record.get("contract_change")),
        max_generator_rounds=int(record["limits"]["max_generator_rounds"]),
        scope_check=lambda: derive_scope(
            environment,
            expected_main=str(record["expected_main"]),
            head=environment.git(["rev-parse", "HEAD"]),
            scope_policy=Path(str(record.get("scope_policy_path", DEFAULT_POLICY_PATH))),
        )[0],
    )
    publish = PublishConfig(
        run=run_config,
        launcher=environment.launcher,
        collector=environment.collector,
        sleep=environment.sleep,
        now=environment.now,
        authorization_path=getattr(args, "authorization", None) or run_dir / "authorization.json",
        start_branch=str(record["start_branch"]),
        max_hours_since_full_end=float(record["limits"]["max_hours_since_full_end"]),
        push_by_command=bool(getattr(args, "push_by_command", False)),
        owner_push_wait_seconds=(
            60.0 * float(getattr(args, "owner_wait_minutes", DEFAULT_OWNER_WAIT_MINUTES))
            if getattr(args, "wait_for_owner_push", False)
            else 0.0
        ),
    )
    steps: list[Step] = [
        *build_precondition_steps(
            run_config,
            runner,
            live_processes=environment.collector.live_processes,
            interpreter=environment.interpreter,
            free_disk_gb=environment.free_disk_gb,
            min_free_disk_gb=float(record["limits"]["min_free_disk_gb"]),
        ),
        *build_prepare_steps(run_config, runner),
        *build_formal_steps(run_config, runner),
        *build_validation_steps(publish, runner),
        *build_publication_steps(publish, runner),
    ]
    return PublicationRunEngine(
        steps=steps,
        journal=PublicationJournal(run_dir / "journal.jsonl"),
        context=StepContext(run_id=run_config.run_id),
        monotonic=environment.monotonic,
        now=environment.now,
    )


def exit_code(summary: RunSummary) -> int:
    return {
        OUTCOME_COMPLETE: EXIT_COMPLETE,
        OUTCOME_AWAITING_AUTHORIZATION: EXIT_AWAITING_AUTHORIZATION,
        OUTCOME_PAUSED: EXIT_PAUSED,
    }.get(summary.outcome, EXIT_FAILED)


def command_run(
    args: argparse.Namespace, environment: CliEnvironment
) -> tuple[dict[str, Any], int]:
    run_dir = run_directory(environment.repository_root, args.run_id)
    record = _load_or_create_run_record(args, environment, run_dir)
    engine = build_engine(record, environment, run_dir, args)
    lock = PublicationRunLock(run_dir / "run.lock")
    lock.acquire(owner=f"{args.actor}@{environment.now().isoformat()}", takeover=args.takeover)
    try:
        summary = engine.run(until=args.until, retry=args.retry)
    finally:
        lock.release()
    return summary.to_dict(), exit_code(summary)


def command_status(
    args: argparse.Namespace, environment: CliEnvironment
) -> tuple[dict[str, Any], int]:
    run_dir = run_directory(environment.repository_root, args.run_id)
    record = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
    engine = build_engine(record, environment, run_dir, args)
    summary = engine.status()
    return summary.to_dict(), exit_code(summary)


def main(argv: Sequence[str] | None = None, *, environment: CliEnvironment | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    if environment is None:
        raise SystemExit("an environment is required (the script wires the real one)")
    try:
        if args.command == "plan":
            payload, code = command_plan(args, environment), EXIT_COMPLETE
        elif args.command == "status":
            payload, code = command_status(args, environment)
        else:
            payload, code = command_run(args, environment)
    except (PublicationScopeError, PublicationJournalError) as error:
        payload = {"status": "REFUSED", "code": error.code, "message": error.message}
        if error.code in REFUSAL_HINTS:
            payload["next_action"] = REFUSAL_HINTS[error.code]
        code = EXIT_FAILED
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return code


__all__ = [
    "CliEnvironment",
    "CommandResult",
    "build_engine",
    "build_parser",
    "command_plan",
    "command_run",
    "command_status",
    "default_environment",
    "derive_scope",
    "exit_code",
    "main",
    "run_directory",
]
