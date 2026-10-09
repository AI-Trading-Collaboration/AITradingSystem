"""Shared test fakes for the publication run tests: a scripted repository, fence and launcher."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture.publication_checks import PrePublishFacts
from ai_trading_system.platform.architecture.publication_commands import (
    FENCE_SCRIPT,
    GENERATOR_COMMANDS,
    GUARD_SCRIPT,
    LEASE_SEAL_SCRIPT,
    PREFLIGHT_SCRIPT,
    READINESS_SCRIPT,
    TRANSACTION_ROOT,
    CommandResult,
    RunConfig,
)
from ai_trading_system.platform.architecture.publication_publish_steps import VALIDATE_SCRIPT
from ai_trading_system.platform.architecture.publication_scope import DerivedPublicationScope
from ai_trading_system.platform.architecture.publication_steps import DEVEX_SCRIPT

T0 = datetime(2026, 10, 7, 0, 0, tzinfo=UTC)
PHASES = (
    "ACQUIRED",
    "TASK_SOURCE_PRE_WRITE",
    "GENERATED_REBUILD_PRE",
    "GENERATED_REBUILD_POST",
    "CANDIDATE_COMMIT_PRE",
    "FORMAL_VALIDATION_PRE",
    "FULL_DISPATCHED",
    "FORMAL_VALIDATION_RESULT",
    "LOCAL_MAIN_FF_PRE",
    "REMOTE_PUSH_PRE",
    "CLEANUP_PRE",
    "RELEASED",
)
GENERATORS = tuple(GENERATOR_COMMANDS)
GENERATED = "registry/architecture_compatibility_authority"
MAIN0 = "a" * 40


def make_scope() -> DerivedPublicationScope:
    return DerivedPublicationScope(
        owned_paths=("src/pkg/a.py", "tests/test_a.py"),
        shared_paths=("docs/system_flow.md", GENERATED, "inputs/architecture"),
        generator_ids=GENERATORS,
        required_validation_tiers=("architecture", "contract", "full"),
        diff_paths=("src/pkg/a.py", "tests/test_a.py", "docs/system_flow.md"),
        shared_diff_paths=("docs/system_flow.md",),
        policy_id="TEST",
        policy_version="1.0.0",
        policy_sha256="0" * 64,
    )


class World:
    """A scripted repository + fence + remote: records every command and what each prints."""

    def __init__(self, tmp_path: Path) -> None:
        self.repo = tmp_path / "repo"
        (self.repo / TRANSACTION_ROOT).mkdir(parents=True)
        (self.repo / "ev").mkdir()
        self.head = "h0"
        self.main = MAIN0
        self.origin_main = MAIN0
        self.remote_tip = MAIN0
        self.branch = "lane"
        self.lane_tip = "h0"
        self.commit_count = 0
        self.dirty: list[str] = []
        self.dirty_after_generator_runs: list[list[str]] = []
        self.preflight_status = "PASS"
        self.readiness_status = "PASS"
        self.remote_is_ancestor = True
        self.main_is_ancestor = True
        self.devex_exit = 1
        self.devex_output: str | None = None
        self.devex_body: dict[str, Any] = {
            "dependency_gate": {"status": "PASS", "violation_count": 0},
            "violation_count": 3,
            "violations": [
                {"rule_id": "module_manifest_fresh"},
                {"rule_id": "test_manifest_fresh"},
                {"rule_id": "aggregate_shadow_index_reproducible"},
            ],
        }
        # DEVX-023 candidate B: the replay seal CLI (A06). Scripted like the other commands.
        self.seal_build_exit = 0
        self.seal_build_output: str | None = None
        self.seal_build_body: dict[str, Any] = {
            "status": "PASS",
            "command": "build",
            "seal_sha256": "5" * 64,
            "kernel_fingerprint": "6" * 64,
            "report": {
                "chains": 903,
                "events": 6375,
                "sealed_chains": 901,
                "sealed_events": 6371,
                "skipped_non_terminal": 2,
                "seconds": 34.6,
            },
        }
        self.seal_verify_exit = 0
        self.seal_verify_body: dict[str, Any] = {
            "status": "PASS",
            "command": "verify",
            "ok": True,
            "reason": None,
            "differing_fields": [],
            "sealed_seconds": 3.05,
            "full_seconds": 33.08,
        }
        self.push_exit = 0
        self.ls_remote_failures = 0  # the next N `git ls-remote` calls fail (a network blip)
        self.heartbeat_status = "PASS"
        self.transactions: dict[str, str] = {}
        self.calls: list[tuple[str, ...]] = []
        self.pushes: list[tuple[str, ...]] = []

    def named(self, *needles: str) -> list[tuple[str, ...]]:
        return [call for call in self.calls if all(n in call for n in needles)]

    def _result(self, argv: Sequence[str], body: Any, code: int = 0) -> CommandResult:
        text = body if isinstance(body, str) else json.dumps(body)
        log = self.repo / "ev" / f"log{len(self.calls)}.log"
        log.write_text(text, encoding="utf-8")
        return CommandResult(tuple(argv), code, text, "", 0.5, str(log))

    def __call__(self, argv: Sequence[str], *, log_name: str) -> CommandResult:
        argv = tuple(argv)
        self.calls.append(argv)
        if argv[0] == "git":
            return self._git(argv)
        script = argv[2] if len(argv) > 2 else ""
        if script == FENCE_SCRIPT:
            return self._fence(argv)
        if script == GUARD_SCRIPT:
            if argv[3] == "heartbeat":
                return self._result(argv, {"status": self.heartbeat_status, "action": "heartbeat"})
            return self._result(argv, {"status": "PASS", "dirty_paths": list(self.dirty)})
        if script == LEASE_SEAL_SCRIPT:
            if argv[3] == "build":
                text = (
                    self.seal_build_output
                    if self.seal_build_output is not None
                    else self.seal_build_body
                )
                return self._result(argv, text, self.seal_build_exit)
            return self._result(argv, self.seal_verify_body, self.seal_verify_exit)
        if script == DEVEX_SCRIPT and "validate" in argv:
            text = self.devex_output if self.devex_output is not None else self.devex_body
            return self._result(argv, text, self.devex_exit)
        if script == PREFLIGHT_SCRIPT:
            code = 0 if self.preflight_status == "PASS" else 2
            body = {"status": self.preflight_status, "blockers": [{"code": "X"}], "warnings": []}
            return self._result(argv, body, code)
        if script == READINESS_SCRIPT:
            body = {"status": self.readiness_status, "candidate_sha": self.head, "checks": []}
            return self._result(argv, body)
        for generator, command in GENERATOR_COMMANDS.items():
            if script == command[0]:
                if generator == GENERATORS[-1] and self.dirty_after_generator_runs:
                    self.dirty = self.dirty_after_generator_runs.pop(0)
                return self._result(argv, "")
        raise AssertionError(f"unexpected command {argv}")

    def _git(self, argv: tuple[str, ...]) -> CommandResult:
        verb = argv[1]
        if verb == "rev-parse":
            ref = argv[2]
            values = {
                self.branch: self.lane_tip,  # first: a later key wins, so main stays main
                "HEAD": self.head,
                "main": self.main,
                "origin/main": self.origin_main,
            }
            return self._result(argv, values[ref] + "\n")
        if verb == "add":
            return self._result(argv, "")
        if verb == "commit":
            self.commit_count += 1
            self.head = f"h{self.commit_count}"
            self.lane_tip = self.head
            self.dirty = []
            return self._result(argv, "")
        if verb == "fetch":
            self.origin_main = self.remote_tip
            return self._result(argv, "")
        if verb == "merge-base":
            ok = self.main_is_ancestor if argv[3] == "main" else self.remote_is_ancestor
            return self._result(argv, "", 0 if ok else 1)
        if verb == "ls-remote":
            if self.ls_remote_failures > 0:
                self.ls_remote_failures -= 1
                return self._result(argv, "", 128)
            return self._result(argv, f"{self.remote_tip}\trefs/heads/main\n")
        if verb == "push":
            self.pushes.append(argv)
            if self.push_exit == 0:
                self.remote_tip = self.main
            return self._result(argv, "pushed", self.push_exit)
        if verb == "branch":
            return self._result(argv, self.branch + "\n")
        if verb == "checkout":
            self.branch = argv[2]
            return self._result(argv, "")
        raise AssertionError(f"unexpected git {argv}")

    def _fence(self, argv: tuple[str, ...]) -> CommandResult:
        command = argv[3]
        if command == "acquire":
            transaction_id = argv[argv.index("--transaction-id") + 1]
            self.transactions[transaction_id] = "ACQUIRED"
            path = self.repo / TRANSACTION_ROOT / transaction_id / "transaction.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}", encoding="utf-8")
            return self._result(argv, self._body(transaction_id))
        transaction_id = Path(argv[argv.index("--transaction") + 1]).parent.name
        if command == "checkpoint":
            self.transactions[transaction_id] = argv[argv.index("--phase") + 1]
            return self._result(argv, self._body(transaction_id))
        if command == "release":
            outcome = argv[argv.index("--outcome") + 1]
            self.transactions[transaction_id] = "FAILED" if outcome == "failed" else "RELEASED"
            return self._result(
                argv, {"final_phase": self.transactions[transaction_id], "outcome": outcome.upper()}
            )
        if command == "replay":
            return self._result(argv, self._body(transaction_id))
        raise AssertionError(f"unexpected fence command {argv}")

    def _body(self, transaction_id: str) -> dict[str, Any]:
        phase = self.transactions[transaction_id]
        reached = phase in PHASES and PHASES.index(phase) >= PHASES.index("CANDIDATE_COMMIT_PRE")
        return {
            "status": "PASS",
            "phase": phase,
            "transaction_id": transaction_id,
            "transaction_sha256": f"sha-{transaction_id}",
            "lease_id": f"lease-{transaction_id}",
            "candidate_sha": self.head if reached else None,
        }


class FakeLauncher:
    """A detached launcher whose launches apply scripted side effects to the World."""

    def __init__(self, world: World) -> None:
        self.world = world
        self.launched: list[tuple[str, ...]] = []
        self.environments: list[dict[str, str]] = []  # per launch, parallel to ``launched``
        self.running_polls = 2
        self._remaining: dict[int, int] = {}
        self.driver_status = "VALIDATION_PASS_AWAITING_PUBLICATION_REVIEW"
        self.driver_results: list[dict[str, Any]] | None = None
        self.driver_dies = False
        self.worker_status = "LOCAL_PUBLISHED"

    def launch(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        stdout_path: Path,
        stderr_path: Path,
        environment: Mapping[str, str] | None = None,
    ) -> int:
        argv = tuple(argv)
        self.launched.append(argv)
        self.environments.append(dict(environment or {}))
        pid = 5000 + len(self.launched)
        self._remaining[pid] = self.running_polls
        if VALIDATE_SCRIPT in argv:
            self._driver_side_effect(argv)
        elif "local-publish" in argv:
            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            stdout_path.write_text(
                json.dumps({"status": self.worker_status, "candidate_sha": self.world.head}),
                encoding="utf-8",
            )
            if self.worker_status == "LOCAL_PUBLISHED":
                self.world.main = self.world.head
        return pid

    def _driver_side_effect(self, argv: tuple[str, ...]) -> None:
        run_id = argv[argv.index("--run-id") + 1]
        evidence = Path(argv[argv.index("--evidence-dir") + 1])
        if self.driver_dies:
            return
        results = self.driver_results or [
            {
                "stage": "named-parent-positive",
                "exit_code": 0,
                "elapsed_seconds": 500.0,
                "baseline_seconds": 502.0,
            },
            {
                "stage": "full",
                "exit_code": 0,
                "elapsed_seconds": 8000.0,
                "baseline_seconds": 8738.0,
            },
        ]
        progress = {"status": self.driver_status, "stage": "complete", "results": results}
        if self.driver_status != "VALIDATION_PASS_AWAITING_PUBLICATION_REVIEW":
            progress["stage"] = "integration"
        (evidence / f"{run_id}_validation_progress.json").write_text(
            json.dumps(progress), encoding="utf-8"
        )
        summary_dir = self.world.repo / "outputs" / "validation_runtime" / f"{run_id}-full"
        summary_dir.mkdir(parents=True, exist_ok=True)
        (summary_dir / "test_runtime_summary.json").write_text(
            json.dumps({"ended_at_utc": "2026-10-07T00:00:00Z"}), encoding="utf-8"
        )

    def is_running(self, pid: int) -> bool:
        if self._remaining.get(pid, 0) > 0:
            self._remaining[pid] -= 1
            return True
        return False


class FakeCollector:
    def __init__(self, world: World) -> None:
        self.world = world
        self.processes: tuple[str, ...] = ()
        self.git_locks: tuple[str, ...] = ()
        self.active: tuple[str, ...] | None = None
        self.hours_after_full_end = 0.5

    def live_processes(self) -> tuple[str, ...]:
        return self.processes

    def settled_live_processes(self) -> tuple[str, ...]:
        return self.processes

    def pre_publish_facts(
        self, *, expected_main: str, expected_lease: str, full_ended_at: datetime
    ) -> PrePublishFacts:
        from datetime import timedelta

        return PrePublishFacts(
            main=self.world.main,
            expected_main=expected_main,
            orig_head=None,
            git_locks=self.git_locks,
            live_processes=self.processes,
            lease_replay_status="PASS",
            active_leases=self.active if self.active is not None else (expected_lease,),
            expected_lease=expected_lease,
            full_ended_at=full_ended_at,
            observed_at=full_ended_at + timedelta(hours=self.hours_after_full_end),
        )


def make_run_config(world: World, **overrides: Any) -> RunConfig:
    fields: dict[str, Any] = {
        "run_id": "r1",
        "repository_root": world.repo,
        "python": "py",
        "evidence_dir": world.repo / "ev",
        "actor": "integration-coordinator",
        "thread_id": "thread-1",
        "task_id": "TASK",
        "expected_main": MAIN0,
        "scope": make_scope(),
        "phase_order": PHASES,
        "parent_run": "outputs/validation_runtime/parent/test_runtime_summary.json",
        "commit_trailer": None,
        "contract_change": False,
    }
    fields.update(overrides)
    return RunConfig(**fields)
