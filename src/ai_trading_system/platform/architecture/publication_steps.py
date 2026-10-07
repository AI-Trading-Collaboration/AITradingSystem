"""DEVX-016 S3: the steps of an ordinary publication run (stages B and C: before validation).

Stage B brings the generated state to its fixed point inside a prep transaction; stage C acquires
the formal transaction on the resulting clean head and runs the governed checks. A step that
changes the world (a transaction phase, a commit) is `mutating` and carries a probe that asks the
fence or git whether the effect is already there, so a resumed run never repeats it.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture.publication_commands import (
    BASELINE_ACQUIRE_SECONDS,
    BASELINE_GENERATOR_ROUND_SECONDS,
    BASELINE_GIT_SECONDS,
    BASELINE_PREFLIGHT_SECONDS,
    BASELINE_READINESS_SECONDS,
    BASELINE_RELEASE_SECONDS,
    READINESS_SCRIPT,
    CommandRunner,
    FenceCli,
    RunConfig,
    command_failure,
    commit_generated,
    git_text,
    governed_preflight,
    json_object,
    run_generators,
    worktree_audit,
)
from ai_trading_system.platform.architecture.publication_orchestrator import (
    Step,
    StepContext,
    StepFailed,
)
from ai_trading_system.platform.artifacts.writer import write_bytes_atomic

DEVEX_SCRIPT = "scripts/architecture_devex.py"
# `architecture_devex.py validate` before the generators have run reports exactly these freshness
# findings; anything else (above all the dependency gate) is a defect to fix BEFORE a prep round.
EXPECTED_PRE_GENERATION_VIOLATIONS = frozenset(
    {"module_manifest_fresh", "test_manifest_fresh", "aggregate_shadow_index_reproducible"}
)
REQUIRED_PYTHON = (3, 11)


def build_precondition_steps(
    config: RunConfig,
    runner: CommandRunner,
    *,
    live_processes: Callable[[], Sequence[str]],
    interpreter: Callable[[], tuple[str, tuple[int, int]]],
    free_disk_gb: Callable[[], float],
    min_free_disk_gb: float,
) -> list[Step]:
    """Stage A: everything that must be true before the first transaction is acquired."""

    def interpreter_step(context: StepContext) -> Mapping[str, Any]:
        executable, version = interpreter()
        venv = (config.repository_root / ".venv").resolve()
        inside = Path(executable).resolve().is_relative_to(venv)
        if not inside or version[:2] != REQUIRED_PYTHON:
            raise StepFailed(
                "PUBLICATION_RUN_INTERPRETER",
                "本命令必须使用项目 .venv 的 Python 3.11（绝不能用系统 Python）",
                {"executable": executable, "version": list(version)},
            )
        return {"executable": executable, "version": list(version)}

    def git_state(context: StepContext) -> Mapping[str, Any]:
        head = git_text(runner, config, "A01_head", "rev-parse", "HEAD")
        main = git_text(runner, config, "A01_main", "rev-parse", "main")
        branch = git_text(runner, config, "A01_branch", "branch", "--show-current")
        if main != config.expected_main:
            raise StepFailed(
                "PUBLICATION_RUN_MAIN_MOVED",
                "本地 main 不是本次 run 启动时冻结的基线",
                {"main": main, "expected": config.expected_main},
            )
        if not branch or branch == "main":
            raise StepFailed(
                "PUBLICATION_RUN_BRANCH",
                "请在任务分支上工作，不要直接在 main 上",
                {"branch": branch},
            )
        ancestor = runner(
            ("git", "merge-base", "--is-ancestor", "main", "HEAD"),
            log_name=f"{config.run_id}_A01_ancestry",
        )
        if ancestor.exit_code != 0:
            raise StepFailed("PUBLICATION_RUN_NOT_DESCENDED", "main 不是 HEAD 的祖先")
        return {"head": head, "main": main, "branch": branch}

    def audit_step(context: StepContext) -> Mapping[str, Any]:
        result = worktree_audit(config, runner, "A02_audit")
        if result.dirty_paths:
            raise StepFailed(
                "PUBLICATION_RUN_TREE_NOT_CLEAN",
                "请先提交或暂存所有改动：run 必须从干净的树开始",
                {"paths": list(result.dirty_paths)[:20]},
            )
        return {"dirty_paths": 0}

    def processes_step(context: StepContext) -> Mapping[str, Any]:
        live = list(live_processes())
        if live:
            raise StepFailed(
                "PUBLICATION_RUN_PROCESSES_PRESENT",
                "发布 run 期间不允许存在其他 python/pytest/git 进程",
                {"processes": live[:10]},
            )
        return {"processes": 0}

    def disk_step(context: StepContext) -> Mapping[str, Any]:
        free = free_disk_gb()
        if free < min_free_disk_gb:
            raise StepFailed(
                "PUBLICATION_RUN_DISK_LOW",
                f"磁盘空闲 {free:.0f} GB，策略要求至少 {min_free_disk_gb:.0f} GB",
                {"free_gb": round(free, 1), "required_gb": min_free_disk_gb},
            )
        return {"free_gb": round(free, 1), "required_gb": min_free_disk_gb}

    def dependency_gate(context: StepContext) -> Mapping[str, Any]:
        result = runner(
            (config.python, "-B", DEVEX_SCRIPT, "validate"),
            log_name=f"{config.run_id}_A05_validate",
        )
        try:
            body = json.loads(result.stdout)
        except json.JSONDecodeError:
            raise command_failure(
                "PUBLICATION_RUN_VALIDATE_UNREADABLE", "architecture validate", result
            ) from None
        gate = body.get("dependency_gate", {}) if isinstance(body, dict) else {}
        unexpected = [
            row
            for row in (body.get("violations", []) if isinstance(body, dict) else [])
            if row.get("rule_id") not in EXPECTED_PRE_GENERATION_VIOLATIONS
        ]
        if gate.get("status") != "PASS" or unexpected:
            raise StepFailed(
                "PUBLICATION_RUN_DEPENDENCY_GATE",
                "架构控制面报告了生成器轮次无法修复的问题",
                {"gate": gate.get("status"), "violations": unexpected[:10], "log": result.log_path},
            )
        return {"dependency_gate": "PASS", "freshness_pending": body.get("violation_count", 0)}

    return [
        Step("A00.interpreter", ".venv 的 Python 3.11 正在运行本命令", interpreter_step),
        Step("A01.git_state", "main 为冻结基线，HEAD 是其后代且在任务分支上",
             git_state, baseline_seconds=BASELINE_GIT_SECONDS),
        Step("A02.clean_tree", "worktree 审计：没有任何脏文件", audit_step,
             baseline_seconds=BASELINE_GIT_SECONDS),
        Step("A03.quiet_host", "没有其他 python/pytest/git 进程", processes_step),
        Step("A04.disk_space", "磁盘空闲空间达到策略下限", disk_step),
        Step("A05.dependency_gate", "architecture validate：依赖门 PASS（只读）",
             dependency_gate, baseline_seconds=BASELINE_ACQUIRE_SECONDS),
    ]  # fmt: skip


def build_prepare_steps(config: RunConfig, runner: CommandRunner) -> list[Step]:
    """Stage B: the prep transaction brings the generated state to its fixed point (zero diff)."""
    fence = FenceCli(config, runner)
    prep_id = f"{config.run_id}-prep"

    def acquire(context: StepContext) -> Mapping[str, Any]:
        head = git_text(runner, config, "B10_head", "rev-parse", "HEAD")
        body = fence.acquire(prep_id, lane_head=head, with_full_parent=False, label="B10_acquire")
        return {
            "transaction_id": prep_id,
            "transaction_sha256": body.get("transaction_sha256"),
            "lease_id": body.get("lease_id"),
            "lane_head": head,
        }

    def acquired(context: StepContext) -> Mapping[str, Any] | None:
        replayed = fence.replay(prep_id, label="B10_probe")
        if replayed is None or replayed.get("status") != "PASS":
            return None
        return {
            "transaction_id": prep_id,
            "transaction_sha256": replayed.get("transaction_sha256"),
            "lease_id": replayed.get("lease_id"),
            "lane_head": git_text(runner, config, "B10_probe_head", "rev-parse", "HEAD"),
        }

    def task_phase(context: StepContext) -> Mapping[str, Any]:
        body = fence.checkpoint(prep_id, "TASK_SOURCE_PRE_WRITE", label="B11_task_phase")
        return {"phase": body.get("phase")}

    def task_phase_done(context: StepContext) -> Mapping[str, Any] | None:
        replayed = fence.phase_reached(prep_id, "TASK_SOURCE_PRE_WRITE", label="B11_probe")
        return None if replayed is None else {"phase": replayed.get("phase")}

    def generators_round_one(context: StepContext) -> Mapping[str, Any]:
        head = context.fact("B10.prep_acquire", "lane_head")
        fence.checkpoint(prep_id, "GENERATED_REBUILD_PRE", label="B12_pre", generators=True)
        elapsed = run_generators(config, runner, head=head, label="B12_gen")
        post = fence.checkpoint(
            prep_id, "GENERATED_REBUILD_POST", label="B12_post", generators=True
        )
        return {"generator_seconds": elapsed, "post_log": post["_log"]}

    def generators_done(context: StepContext) -> Mapping[str, Any] | None:
        replayed = fence.phase_reached(prep_id, "GENERATED_REBUILD_POST", label="B12_probe")
        return None if replayed is None else {"phase": replayed.get("phase")}

    def converge(context: StepContext) -> Mapping[str, Any]:
        """Commit what the generators changed and regenerate until the tree stays clean."""
        head = git_text(runner, config, "B13_head", "rev-parse", "HEAD")
        commits: list[str] = []
        for round_number in range(1, config.max_generator_rounds + 1):
            audit = worktree_audit(config, runner, f"B13_audit_{round_number}")
            if not audit.dirty_paths:
                return {"rounds": round_number - 1, "commits": commits, "head": head}
            head = commit_generated(
                config,
                runner,
                audit,
                subject=(
                    f"{config.run_id}: reseal generated manifests and authorities "
                    f"(round {round_number})"
                ),
                label=f"B13_r{round_number}",
            )
            commits.append(head)
            run_generators(config, runner, head=head, label=f"B13_gen{round_number}")
        final = worktree_audit(config, runner, "B13_audit_final")
        if final.dirty_paths:
            raise StepFailed(
                "PUBLICATION_RUN_GENERATORS_NOT_CONVERGENT",
                f"{config.max_generator_rounds} 轮之后树仍然不干净",
                {"paths": list(final.dirty_paths)[:20]},
            )
        return {"rounds": config.max_generator_rounds, "commits": commits, "head": head}

    def release(context: StepContext) -> Mapping[str, Any]:
        evidence = context.fact("B12.prep_generators", "post_log")
        body = fence.release(prep_id, outcome="failed", evidence=evidence, label="B15_release")
        return {"final_phase": body.get("final_phase"), "outcome": body.get("outcome")}

    def released(context: StepContext) -> Mapping[str, Any] | None:
        replayed = fence.replay(prep_id, label="B15_probe")
        if replayed is not None and replayed.get("phase") in {"FAILED", "RELEASED"}:
            return {"final_phase": replayed.get("phase")}
        return None

    return [
        Step("B10.prep_acquire", "获取 prep 事务", acquire,
             baseline_seconds=BASELINE_ACQUIRE_SECONDS, mutating=True, already_done=acquired),
        Step("B11.prep_task_phase", "prep：TASK_SOURCE_PRE_WRITE", task_phase,
             baseline_seconds=BASELINE_ACQUIRE_SECONDS, mutating=True,
             already_done=task_phase_done),
        Step("B12.prep_generators", "prep：生成器第 1 轮（PRE/POST 检查点）",
             generators_round_one, baseline_seconds=BASELINE_GENERATOR_ROUND_SECONDS,
             mutating=True, already_done=generators_done),
        Step("B13.prep_converge", "提交生成物并重新生成到零差异", converge,
             baseline_seconds=BASELINE_GENERATOR_ROUND_SECONDS, mutating=True),
        Step("B15.prep_release", "释放 prep 事务（按设计以 failed 收口）", release,
             baseline_seconds=BASELINE_RELEASE_SECONDS, mutating=True, already_done=released),
    ]  # fmt: skip


def build_formal_steps(config: RunConfig, runner: CommandRunner) -> list[Step]:
    """Stage C: the formal transaction up to FORMAL_VALIDATION_PRE, then the governed checks."""
    fence = FenceCli(config, runner)
    formal_id = f"{config.run_id}-formal"

    def acquire(context: StepContext) -> Mapping[str, Any]:
        if config.scope_check is not None:
            current = config.scope_check()
            if current.owned_paths != config.scope.owned_paths:
                raise StepFailed(
                    "PUBLICATION_RUN_SCOPE_CHANGED",
                    "最终候选的 owned 范围与 run 启动时冻结的不同",
                    {
                        "frozen": list(config.scope.owned_paths),
                        "current": list(current.owned_paths),
                    },
                )
        head = git_text(runner, config, "C20_head", "rev-parse", "HEAD")
        audit = worktree_audit(config, runner, "C20_audit")
        if audit.dirty_paths:
            raise StepFailed(
                "PUBLICATION_RUN_TREE_NOT_CLEAN",
                "正式事务需要干净且已提交的候选",
                {"paths": list(audit.dirty_paths)[:20]},
            )
        body = fence.acquire(formal_id, lane_head=head, with_full_parent=True, label="C20_acquire")
        return {
            "transaction_id": formal_id,
            "transaction_sha256": body.get("transaction_sha256"),
            "lease_id": body.get("lease_id"),
            "candidate_head": head,
        }

    def acquired(context: StepContext) -> Mapping[str, Any] | None:
        replayed = fence.replay(formal_id, label="C20_probe")
        if replayed is None or replayed.get("status") != "PASS":
            return None
        return {
            "transaction_id": formal_id,
            "transaction_sha256": replayed.get("transaction_sha256"),
            "lease_id": replayed.get("lease_id"),
            "candidate_head": git_text(runner, config, "C20_probe_head", "rev-parse", "HEAD"),
        }

    def task_phase(context: StepContext) -> Mapping[str, Any]:
        body = fence.checkpoint(formal_id, "TASK_SOURCE_PRE_WRITE", label="C21_task_phase")
        return {"phase": body.get("phase")}

    def phase_probe(phase: str, label: str) -> Callable[[StepContext], Mapping[str, Any] | None]:
        def probe(context: StepContext) -> Mapping[str, Any] | None:
            replayed = fence.phase_reached(formal_id, phase, label=label)
            return None if replayed is None else {"phase": replayed.get("phase")}

        return probe

    def generators(context: StepContext) -> Mapping[str, Any]:
        head = context.fact("C20.formal_acquire", "candidate_head")
        fence.checkpoint(formal_id, "GENERATED_REBUILD_PRE", label="C22_pre", generators=True)
        elapsed = run_generators(config, runner, head=head, label="C22_gen")
        fence.checkpoint(formal_id, "GENERATED_REBUILD_POST", label="C22_post", generators=True)
        audit = worktree_audit(config, runner, "C22_audit")
        if audit.dirty_paths:
            raise StepFailed(
                "PUBLICATION_RUN_GENERATED_DRIFT",
                "生成器改动了最终候选树：prep 没有达到零差异",
                {"paths": list(audit.dirty_paths)[:20]},
            )
        return {"generator_seconds": elapsed, "zero_diff": True}

    def commit_pre(context: StepContext) -> Mapping[str, Any]:
        body = fence.checkpoint(formal_id, "CANDIDATE_COMMIT_PRE", label="C23_commit_pre")
        return {"phase": body.get("phase")}

    def validation_pre(context: StepContext) -> Mapping[str, Any]:
        head = context.fact("C20.formal_acquire", "candidate_head")
        body = fence.checkpoint(formal_id, "FORMAL_VALIDATION_PRE", label="C24_validation_pre")
        if body.get("candidate_sha") != head:
            raise StepFailed(
                "PUBLICATION_RUN_CANDIDATE_MISMATCH",
                "围栏记录的候选不是本 run 获取事务时的 HEAD",
                {"fence": body.get("candidate_sha"), "head": head},
            )
        return {
            "candidate_sha": head,
            "transaction_sha256": body.get("transaction_sha256"),
            "lease_id": body.get("lease_id"),
        }

    def preflight(context: StepContext) -> Mapping[str, Any]:
        return governed_preflight(
            config,
            runner,
            stage="INTEGRATION",
            base=config.expected_main,
            transaction_id=formal_id,
            label="C25_preflight",
        )

    def readiness(context: StepContext) -> Mapping[str, Any]:
        head = context.fact("C24.formal_validation_pre", "candidate_sha")
        argv = [
            config.python, "-B", READINESS_SCRIPT,
            "--repository-root", ".",
            "--candidate-sha", head,
        ]  # fmt: skip
        result = runner(tuple(argv), log_name=f"{config.run_id}_C26_readiness")
        body = json_object(result, code="PUBLICATION_RUN_READINESS_FAILED")
        if body.get("status") != "PASS" or body.get("candidate_sha") != head:
            raise StepFailed(
                "PUBLICATION_RUN_READINESS_FAILED",
                f"就绪度为 {body.get('status')}（候选 {body.get('candidate_sha')}）",
                {"blockers": body.get("blockers"), "log": result.log_path},
            )
        record = config.evidence_dir / f"{config.run_id}_readiness.json"
        write_bytes_atomic(record, result.stdout.encode("utf-8"))
        return {
            "status": "PASS",
            "candidate_sha": head,
            "log": result.log_path,
            "readiness_path": str(record),
        }

    return [
        Step("C20.formal_acquire", "获取正式事务（干净且已提交的候选）",
             acquire, baseline_seconds=BASELINE_ACQUIRE_SECONDS, mutating=True,
             already_done=acquired),
        Step("C21.formal_task_phase", "正式：TASK_SOURCE_PRE_WRITE", task_phase,
             baseline_seconds=BASELINE_ACQUIRE_SECONDS, mutating=True,
             already_done=phase_probe("TASK_SOURCE_PRE_WRITE", "C21_probe")),
        Step("C22.formal_generators", "正式：生成器零差异（PRE/POST）", generators,
             baseline_seconds=BASELINE_GENERATOR_ROUND_SECONDS, mutating=True,
             already_done=phase_probe("GENERATED_REBUILD_POST", "C22_probe")),
        Step("C23.formal_commit_pre", "正式：CANDIDATE_COMMIT_PRE", commit_pre,
             baseline_seconds=BASELINE_ACQUIRE_SECONDS, mutating=True,
             already_done=phase_probe("CANDIDATE_COMMIT_PRE", "C23_probe")),
        Step("C24.formal_validation_pre", "正式：FORMAL_VALIDATION_PRE（精确候选）",
             validation_pre, baseline_seconds=BASELINE_ACQUIRE_SECONDS, mutating=True,
             already_done=phase_probe("FORMAL_VALIDATION_PRE", "C24_probe")),
        Step("C25.governed_preflight", "governed INTEGRATION 预检", preflight,
             baseline_seconds=BASELINE_PREFLIGHT_SECONDS),
        Step("C26.readiness", "验证就绪度（7 项检查）", readiness,
             baseline_seconds=BASELINE_READINESS_SECONDS),
    ]  # fmt: skip
