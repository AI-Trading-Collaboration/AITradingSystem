"""DEVX-016 S3: the validation and publication steps of a run (stages D and E).

Stage D launches the validation driver detached and waits for it; stage E is the manual post-Full
sequence: pre-publication checklist, local main fast-forward through the fence's own
`local-publish`, fetch, REMOTE_PUSH_PRE, CLOSEOUT preflight, the owner authorization gate, the
ordinary push, SHA verification, CLEANUP_PRE and the completed release.

Safety shape of this module:
- by default the command NEVER pushes: the push step waits (AWAITING) for the owner to run the
  ordinary push in their own terminal and then proves it against the remote (the harness that
  hosts the agent refused the agent's own push at the fifth publication, and the owner ran it);
- with `push_by_command` (an explicit owner opt-in) the push step is an ORDINARY
  `git push origin main` built by one function that refuses every force/delete/mirror/refspec
  variant, and it cannot run before the authorization-gate step is DONE (the engine never executes
  a step after one that is AWAITING_AUTHORIZATION);
- no step repairs remote history, merges, rebases or opens a pull request;
- every step with an external effect carries a probe against reality (git, the remote, the fence),
  so a resumed run proves instead of repeating.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from ai_trading_system.platform.architecture.publication_checks import (
    PrePublishFacts,
    PublicationAuthorizationError,
    checks_summary,
    evaluate_pre_publish_checks,
    load_authorization,
)
from ai_trading_system.platform.architecture.publication_commands import (
    BASELINE_GIT_SECONDS,
    FENCE_SCRIPT,
    CommandResult,
    CommandRunner,
    FenceCli,
    RunConfig,
    command_failure,
    git_text,
    governed_preflight,
    worktree_audit,
)
from ai_trading_system.platform.architecture.publication_orchestrator import (
    Step,
    StepAwaitingAuthorization,
    StepContext,
    StepFailed,
)
from ai_trading_system.platform.architecture.publication_validation import (
    PASS_STATUS,
    STAGE_BASELINE_SECONDS,
    TERMINAL_STATUSES,
    read_progress,
)

VALIDATE_SCRIPT = "scripts/architecture_arch005_validate_candidate.py"
# Baselines (reporting only): the real worker took 54 min on d23 and 71-86 min before S3c/W.
BASELINE_VALIDATION_SECONDS = float(sum(STAGE_BASELINE_SECONDS.values()))
BASELINE_LOCAL_PUBLISH_SECONDS = 54 * 60.0
FORBIDDEN_PUSH_TOKENS = frozenset(
    {"--force", "-f", "--force-with-lease", "--mirror", "--delete", "-d", "--prune", "--all"}
)


class DetachedLauncher(Protocol):
    """Starts a process outside this process tree (it must survive the orchestrator)."""

    def launch(
        self, argv: Sequence[str], *, cwd: Path, stdout_path: Path, stderr_path: Path
    ) -> int: ...

    def is_running(self, pid: int) -> bool: ...


class FactsCollector(Protocol):
    def live_processes(self) -> tuple[str, ...]: ...

    def pre_publish_facts(
        self, *, expected_main: str, expected_lease: str, full_ended_at: datetime
    ) -> PrePublishFacts: ...


@dataclass(frozen=True)
class PublishConfig:
    run: RunConfig
    launcher: DetachedLauncher
    collector: FactsCollector
    sleep: Callable[[float], None]
    now: Callable[[], datetime]
    authorization_path: Path
    start_branch: str
    max_hours_since_full_end: float = 2.0
    # Owner opt-in: only then may this command itself run `git push origin main`.
    push_by_command: bool = False
    poll_seconds: float = 30.0


def ordinary_push_argv() -> list[str]:
    """The only push this tool ever issues: `git push origin main`, nothing else."""
    argv = ["git", "push", "origin", "main"]
    if any(token in FORBIDDEN_PUSH_TOKENS or token.startswith("+") for token in argv):
        raise StepFailed("PUBLICATION_RUN_PUSH_NOT_ORDINARY", " ".join(argv))
    return argv


def _remote_tip(runner: CommandRunner, run: RunConfig, label: str) -> str | None:
    result = runner(
        ("git", "ls-remote", "origin", "refs/heads/main"), log_name=f"{run.run_id}_{label}"
    )
    if result.exit_code != 0:
        raise command_failure("PUBLICATION_RUN_REMOTE_UNREADABLE", "git ls-remote", result)
    first = result.stdout.strip().split("\n")[0].split()
    return first[0] if first else None


def build_validation_steps(config: PublishConfig, runner: CommandRunner) -> list[Step]:
    run = config.run

    def launch(context: StepContext) -> Mapping[str, Any]:
        live = config.collector.live_processes()
        if live:
            raise StepFailed(
                "PUBLICATION_RUN_PROCESSES_PRESENT",
                "正式验证期间不允许存在其他 python/pytest 进程",
                {"processes": list(live)[:10]},
            )
        progress = run.evidence_dir / f"{run.run_id}_validation_progress.json"
        if progress.exists():
            raise StepFailed("PUBLICATION_RUN_VALIDATION_EXISTS", str(progress))
        candidate = context.fact("C24.formal_validation_pre", "candidate_sha")
        argv = [
            run.python, "-B", VALIDATE_SCRIPT,
            "--run-id", run.run_id,
            "--candidate-sha", candidate,
            "--expected-main", run.expected_main,
            "--task-id", run.task_id,
            "--actor", run.actor,
            "--transaction", str(run.transaction_path(f"{run.run_id}-formal")),
            "--transaction-sha256", context.fact("C24.formal_validation_pre", "transaction_sha256"),
            "--lease-id", context.fact("C24.formal_validation_pre", "lease_id"),
            "--parent-run", str(run.parent_run),
            "--evidence-dir", str(run.evidence_dir),
            "--readiness", context.fact("C26.readiness", "readiness_path"),
        ]  # fmt: skip
        pid = config.launcher.launch(
            argv,
            cwd=run.repository_root,
            stdout_path=run.evidence_dir / f"{run.run_id}_driver_stdout.log",
            stderr_path=run.evidence_dir / f"{run.run_id}_driver_stderr.log",
        )
        return {"pid": pid, "progress_path": str(progress), "argv": argv}

    def launched(context: StepContext) -> Mapping[str, Any] | None:
        progress = run.evidence_dir / f"{run.run_id}_validation_progress.json"
        return {"progress_path": str(progress), "pid": None} if progress.exists() else None

    def wait(context: StepContext) -> Mapping[str, Any]:
        progress_path = Path(context.fact("D30.validation_launch", "progress_path"))
        pid = context.facts["D30.validation_launch"].get("pid")
        while True:
            progress = read_progress(progress_path)
            status = None if progress is None else str(progress.get("status"))
            if status in TERMINAL_STATUSES and progress is not None:
                return _validation_outcome(progress, run)
            if pid is None and progress is not None:
                pid = progress.get("coordinator_pid")  # a resumed run did not launch it
            if pid is not None and not config.launcher.is_running(int(pid)):
                # The driver may have written its terminal status and exited between the read
                # above and this check; only a still non-terminal record means it died.
                progress = read_progress(progress_path)
                status = None if progress is None else str(progress.get("status"))
                if status in TERMINAL_STATUSES and progress is not None:
                    return _validation_outcome(progress, run)
                raise StepFailed(
                    "PUBLICATION_RUN_DRIVER_DIED",
                    f"验证驱动 pid {pid} 已退出，状态为 {status}",
                    {"progress": str(progress_path)},
                )
            config.sleep(config.poll_seconds)

    return [
        Step("D30.validation_launch", "脱离式启动验证驱动", launch,
             mutating=True, already_done=launched),
        Step("D31.validation_wait", "等待验证完成（4 个 pre-Full tier + Full）", wait,
             baseline_seconds=BASELINE_VALIDATION_SECONDS),
    ]  # fmt: skip


def _validation_outcome(progress: Mapping[str, Any], run: RunConfig) -> Mapping[str, Any]:
    results = [dict(row) for row in progress.get("results", [])]
    slow = [
        row["stage"]
        for row in results
        if row.get("baseline_seconds") and row["elapsed_seconds"] > 1.25 * row["baseline_seconds"]
    ]
    if progress.get("status") != PASS_STATUS:
        raise StepFailed(
            "PUBLICATION_RUN_VALIDATION_NOT_GREEN",
            f"{progress.get('status')}（阶段 {progress.get('stage')}）",
            {"results": results, "detail": progress.get("detail")},
        )
    summary = run.repository_root / "outputs" / "validation_runtime" / f"{run.run_id}-full"
    return {
        "stage_results": results,
        "slow_stages": slow,
        "full_summary_path": str(summary / "test_runtime_summary.json"),
    }


def _fence_phase_step(
    fence: FenceCli,
    transaction_id: str,
    step_id: str,
    title: str,
    phase: str,
    *,
    baseline: float,
) -> Step:
    def execute(context: StepContext) -> Mapping[str, Any]:
        body = fence.checkpoint(transaction_id, phase, label=step_id.replace(".", "_"))
        return {"phase": body.get("phase")}

    def probe(context: StepContext) -> Mapping[str, Any] | None:
        replayed = fence.phase_reached(transaction_id, phase, label=f"{step_id}_probe")
        return None if replayed is None else {"phase": replayed.get("phase")}

    return Step(
        step_id, title, execute, baseline_seconds=baseline, mutating=True, already_done=probe
    )


def build_publication_steps(config: PublishConfig, runner: CommandRunner) -> list[Step]:
    run = config.run
    fence = FenceCli(run, runner)
    formal_id = f"{run.run_id}-formal"

    def candidate(context: StepContext) -> str:
        return str(context.fact("C24.formal_validation_pre", "candidate_sha"))

    def pre_publish(context: StepContext) -> Mapping[str, Any]:
        summary_path = Path(context.fact("D31.validation_wait", "full_summary_path"))
        try:
            ended = json.loads(summary_path.read_text(encoding="utf-8"))["ended_at_utc"]
            full_ended = datetime.fromisoformat(str(ended).replace("Z", "+00:00"))
        except (OSError, ValueError, KeyError) as exc:
            raise StepFailed("PUBLICATION_RUN_FULL_SUMMARY_UNREADABLE", str(summary_path)) from exc
        facts = config.collector.pre_publish_facts(
            expected_main=run.expected_main,
            expected_lease=str(context.fact("C24.formal_validation_pre", "lease_id")),
            full_ended_at=full_ended,
        )
        outcome = checks_summary(
            evaluate_pre_publish_checks(
                facts, max_hours_since_full_end=config.max_hours_since_full_end
            )
        )
        if not outcome["all_ok"]:
            raise StepFailed(
                "PUBLICATION_RUN_PRE_PUBLISH_CHECKS_FAILED",
                "发布前检查未全部通过",
                {"checks": outcome["checks"]},
            )
        return {"checks": outcome["checks"]}

    def audit(context: StepContext) -> Mapping[str, Any]:
        head = git_text(runner, run, "E51_head", "rev-parse", "HEAD")
        result = worktree_audit(run, runner, "E51_audit")
        if head != candidate(context) or result.dirty_paths:
            raise StepFailed(
                "PUBLICATION_RUN_CANDIDATE_CHANGED",
                "HEAD 不是冻结的候选，或树不干净",
                {"head": head, "dirty": list(result.dirty_paths)[:20]},
            )
        return {"head": head}

    def local_publish_launch(context: StepContext) -> Mapping[str, Any]:
        stdout = run.evidence_dir / f"{run.run_id}_local_publish.stdout"
        stderr = run.evidence_dir / f"{run.run_id}_local_publish.stderr"
        argv = [
            run.python, "-B", FENCE_SCRIPT, "local-publish",
            "--transaction", str(run.transaction_path(formal_id)),
            "--actor", run.actor,
        ]  # fmt: skip
        pid = config.launcher.launch(
            argv, cwd=run.repository_root, stdout_path=stdout, stderr_path=stderr
        )
        return {"pid": pid, "stdout": str(stdout), "stderr": str(stderr)}

    def local_published(context: StepContext) -> Mapping[str, Any] | None:
        main = git_text(runner, run, "E53_probe_main", "rev-parse", "main")
        if main == candidate(context):
            return {"pid": None, "main": main, "stdout": None, "stderr": None}
        return None

    def local_publish_wait(context: StepContext) -> Mapping[str, Any]:
        facts = context.facts["E53.local_publish_launch"]
        pid = facts.get("pid")
        while pid is not None and config.launcher.is_running(int(pid)):
            config.sleep(config.poll_seconds)
        main = git_text(runner, run, "E54_main", "rev-parse", "main")
        status = None
        if facts.get("stdout"):
            try:
                status = json.loads(Path(facts["stdout"]).read_text(encoding="utf-8")).get("status")
            except (OSError, ValueError):
                status = None
        if main != candidate(context) or (facts.get("stdout") and status != "LOCAL_PUBLISHED"):
            raise StepFailed(
                "PUBLICATION_RUN_LOCAL_PUBLISH_NOT_DONE",
                f"worker 状态 {status}；main 为 {main}",
                {
                    "next": "查看 worker 输出，再执行 local-publication-recover；"
                    "失败的尝试需要新事务和新的 Full",
                    "stdout": facts.get("stdout"),
                },
            )
        return {"main": main, "worker_status": status}

    def fetch(context: StepContext) -> Mapping[str, Any]:
        git_text(runner, run, "E55_fetch", "fetch", "origin", "main")
        ancestor = runner(
            ("git", "merge-base", "--is-ancestor", "origin/main", candidate(context)),
            log_name=f"{run.run_id}_E55_ancestry",
        )
        if ancestor.exit_code != 0:
            raise StepFailed(
                "PUBLICATION_RUN_REMOTE_DIVERGED",
                "origin/main 不是候选的祖先；这里不会做任何修复",
                {"origin_main": git_text(runner, run, "E55_origin", "rev-parse", "origin/main")},
            )
        return {"origin_main": git_text(runner, run, "E55_origin_main", "rev-parse", "origin/main")}

    def closeout_preflight(context: StepContext) -> Mapping[str, Any]:
        return governed_preflight(
            run, runner, stage="CLOSEOUT", base=candidate(context),
            transaction_id=formal_id, label="E57_closeout_preflight",
        )  # fmt: skip

    def gate(context: StepContext) -> Mapping[str, Any]:
        if not config.push_by_command:
            # This command cannot push: nothing to authorize, the owner's own push is the act.
            return {"authorization": "NOT_REQUIRED_OWNER_PUSHES", "candidate": candidate(context)}
        path = config.authorization_path
        if not path.exists():
            raise StepAwaitingAuthorization(
                "没有针对该候选的 owner 授权记录",
                {"candidate": candidate(context), "expected_path": str(path)},
            )
        formal = context.facts["C20.formal_acquire"]
        frozen_at = datetime.fromisoformat(
            str(
                context.facts["C24.formal_validation_pre"].get(
                    "_recorded_at", "1970-01-01T00:00:00+00:00"
                )
            )
        )
        try:
            authorization = load_authorization(
                path,
                candidate_sha=candidate(context),
                expected_main=run.expected_main,
                not_before=frozen_at,
                now=config.now(),
            )
        except PublicationAuthorizationError as error:
            raise StepFailed(error.code, error.message, {"path": str(path)}) from error
        return {
            "authorized_candidate": authorization.candidate_sha,
            "authorized_by": authorization.authorized_by,
            "source": authorization.source,
            "transaction_id": formal["transaction_id"],
        }

    def push(context: StepContext) -> Mapping[str, Any]:
        argv = ordinary_push_argv()
        if not config.push_by_command:
            tip = _remote_tip(runner, run, "E59_owner_probe")
            if tip == candidate(context):
                return {"push_log": None, "remote_tip": tip, "pushed_by": "OWNER_TERMINAL"}
            raise StepAwaitingAuthorization(
                "main 的推送由 owner 在自己的终端执行（这条命令默认从不推送）",
                {
                    "candidate": candidate(context),
                    "remote_tip": tip,
                    "owner_command": " ".join(argv),
                },
            )
        result = runner(tuple(argv), log_name=f"{run.run_id}_E59_push")
        if result.exit_code != 0:
            raise command_failure("PUBLICATION_RUN_PUSH_FAILED", "git push origin main", result)
        return {"push_log": result.log_path}

    def pushed(context: StepContext) -> Mapping[str, Any] | None:
        tip = _remote_tip(runner, run, "E59_probe")
        if tip == candidate(context):
            return {"push_log": None, "remote_tip": tip}
        return None

    def verify(context: StepContext) -> Mapping[str, Any]:
        expected = candidate(context)
        git_text(runner, run, "E60_fetch", "fetch", "origin", "main")
        seen = {
            "main": git_text(runner, run, "E60_main", "rev-parse", "main"),
            "origin_main": git_text(runner, run, "E60_origin", "rev-parse", "origin/main"),
            "remote_tip": _remote_tip(runner, run, "E60_remote"),
        }
        if any(value != expected for value in seen.values()):
            raise StepFailed(
                "PUBLICATION_RUN_SHA_MISMATCH",
                "本地 main、origin/main 与远端 tip 必须都等于候选",
                {"expected": expected, **seen},
            )
        return {"candidate": expected, **seen}

    def release(context: StepContext) -> Mapping[str, Any]:
        # The push log is the evidence; a push proven by the probe has none, so the SHA
        # verification's own log (always present at this point) stands in.
        push_log = context.facts["E59.push"].get("push_log")
        evidence = (
            str(push_log)
            if push_log and Path(str(push_log)).exists()
            else str(run.evidence_dir / f"{run.run_id}_E60_main.log")
        )
        body = fence.release(formal_id, outcome="completed", evidence=evidence, label="E62_release")
        return {"final_phase": body.get("final_phase"), "outcome": body.get("outcome")}

    def released(context: StepContext) -> Mapping[str, Any] | None:
        replayed = fence.replay(formal_id, label="E62_probe")
        if replayed is not None and replayed.get("phase") == "RELEASED":
            return {"final_phase": "RELEASED", "outcome": "COMPLETED"}
        return None

    def restore_branch(context: StepContext) -> Mapping[str, Any]:
        current = git_text(runner, run, "E63_branch", "branch", "--show-current")
        if current == config.start_branch:
            return {"branch": current, "switched": False}
        branch_tip = git_text(runner, run, "E63_tip", "rev-parse", config.start_branch)
        if branch_tip != candidate(context):
            return {"branch": current, "switched": False, "note": "start branch differs from main"}
        git_text(runner, run, "E63_checkout", "checkout", config.start_branch)
        return {"branch": config.start_branch, "switched": True}

    return [
        Step("E50.pre_publish_checks", "发布前检查（6 项）", pre_publish),
        Step("E51.worktree_audit", "worktree 审计：干净，HEAD 为冻结候选", audit),
        _fence_phase_step(fence, formal_id, "E52.local_main_ff_pre", "检查点 LOCAL_MAIN_FF_PRE",
                          "LOCAL_MAIN_FF_PRE", baseline=BASELINE_GIT_SECONDS * 2),
        Step("E53.local_publish_launch", "local-publish worker（脱离式，仅 ff）",
             local_publish_launch, mutating=True, already_done=local_published),
        Step("E54.local_publish_wait", "等待 LOCAL_PUBLISHED", local_publish_wait,
             baseline_seconds=BASELINE_LOCAL_PUBLISH_SECONDS),
        Step("E55.fetch", "fetch origin main；origin/main 必须是候选的祖先", fetch,
             baseline_seconds=BASELINE_GIT_SECONDS),
        _fence_phase_step(fence, formal_id, "E56.remote_push_pre", "检查点 REMOTE_PUSH_PRE",
                          "REMOTE_PUSH_PRE", baseline=BASELINE_GIT_SECONDS * 2),
        Step("E57.closeout_preflight", "governed CLOSEOUT 预检", closeout_preflight,
             baseline_seconds=BASELINE_GIT_SECONDS * 2),
        Step("E58.authorization_gate", "owner 对本候选（普通推送）的授权", gate),
        Step("E59.push", "main 的普通推送（默认由 owner 执行，绝不 force）", push, mutating=True,
             already_done=pushed, baseline_seconds=BASELINE_GIT_SECONDS),
        Step("E60.verify_shas", "本地 main = origin/main = 远端 tip = 候选", verify,
             baseline_seconds=BASELINE_GIT_SECONDS),
        _fence_phase_step(fence, formal_id, "E61.cleanup_pre", "检查点 CLEANUP_PRE",
                          "CLEANUP_PRE", baseline=BASELINE_GIT_SECONDS * 2),
        Step("E62.release", "释放正式事务（completed）", release,
             mutating=True, already_done=released, baseline_seconds=BASELINE_GIT_SECONDS * 2),
        Step("E63.restore_branch", "HEAD 回到起始分支", restore_branch),
    ]  # fmt: skip


__all__ = [
    "BASELINE_LOCAL_PUBLISH_SECONDS",
    "VALIDATE_SCRIPT",
    "CommandResult",
    "DetachedLauncher",
    "FactsCollector",
    "PublishConfig",
    "build_publication_steps",
    "build_validation_steps",
    "ordinary_push_argv",
]
