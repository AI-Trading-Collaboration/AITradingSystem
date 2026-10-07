"""DEVX-016 S3: stages B and C run the reviewed commands in the manual order and fail closed."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.platform.architecture.publication_commands import (
    FENCE_SCRIPT,
    GENERATOR_COMMANDS,
    GUARD_SCRIPT,
    PREFLIGHT_SCRIPT,
    READINESS_SCRIPT,
    TRANSACTION_ROOT,
    CommandResult,
    RunConfig,
)
from ai_trading_system.platform.architecture.publication_journal import PublicationJournal
from ai_trading_system.platform.architecture.publication_orchestrator import (
    OUTCOME_COMPLETE,
    OUTCOME_STOPPED_FAILED,
    PublicationRunEngine,
    StepContext,
)
from ai_trading_system.platform.architecture.publication_scope import DerivedPublicationScope
from ai_trading_system.platform.architecture.publication_steps import (
    build_formal_steps,
    build_prepare_steps,
)

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


def _scope() -> DerivedPublicationScope:
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


class FakeWorld:
    """A scripted repository + fence: records every command and simulates what each prints."""

    def __init__(self, tmp_path: Path) -> None:
        self.repo = tmp_path / "repo"
        (self.repo / TRANSACTION_ROOT).mkdir(parents=True)
        self.head = "h0"
        self.commit_count = 0
        self.dirty: list[str] = []
        self.dirty_after_generator_runs: list[list[str]] = []
        self.failing_generator: str | None = None
        self.preflight_status = "PASS"
        self.readiness_status = "PASS"
        self.candidate_override: str | None = None
        self.transactions: dict[str, str] = {}
        self.calls: list[tuple[str, ...]] = []
        self.staged: list[tuple[str, ...]] = []

    # -- helpers
    def named(self, *needles: str) -> list[tuple[str, ...]]:
        return [call for call in self.calls if all(n in call for n in needles)]

    def _result(self, argv: Sequence[str], body: Any, code: int = 0) -> CommandResult:
        text = json.dumps(body) if not isinstance(body, str) else body
        return CommandResult(tuple(argv), code, text, "", 0.5, f"{self.repo}/log")

    def _transaction_id(self, argv: Sequence[str]) -> str:
        return Path(argv[argv.index("--transaction") + 1]).parent.name

    def __call__(self, argv: Sequence[str], *, log_name: str) -> CommandResult:
        argv = tuple(argv)
        self.calls.append(argv)
        if argv[0] == "git":
            return self._git(argv)
        script = argv[2] if len(argv) > 2 else ""
        if script == FENCE_SCRIPT:
            return self._fence(argv)
        if script == GUARD_SCRIPT:
            return self._result(argv, {"status": "PASS", "dirty_paths": list(self.dirty)})
        if script == PREFLIGHT_SCRIPT:
            code = 0 if self.preflight_status == "PASS" else 2
            body = {"status": self.preflight_status, "blockers": [{"code": "X"}], "warnings": []}
            return self._result(argv, body, code)
        if script == READINESS_SCRIPT:
            body = {
                "status": self.readiness_status,
                "candidate_sha": self.candidate_override or self.head,
            }
            return self._result(argv, body)
        for generator, command in GENERATOR_COMMANDS.items():
            if script == command[0]:
                return self._generator(argv, generator)
        raise AssertionError(f"unexpected command {argv}")

    def _git(self, argv: tuple[str, ...]) -> CommandResult:
        if argv[1:3] == ("rev-parse", "HEAD"):
            return self._result(argv, self.head)
        if argv[1] == "add":
            self.staged.append(argv[3:])
            return self._result(argv, "")
        if argv[1] == "commit":
            self.commit_count += 1
            self.head = f"h{self.commit_count}"
            self.dirty = []
            return self._result(argv, "")
        raise AssertionError(f"unexpected git {argv}")

    def _generator(self, argv: tuple[str, ...], generator: str) -> CommandResult:
        if generator == self.failing_generator:
            return CommandResult(argv, 1, "", "boom", 0.5, f"{self.repo}/log")
        if generator == GENERATORS[-1] and self.dirty_after_generator_runs:
            self.dirty = self.dirty_after_generator_runs.pop(0)
        return self._result(argv, "")

    def _fence(self, argv: tuple[str, ...]) -> CommandResult:
        command = argv[3]
        if command == "acquire":
            transaction_id = argv[argv.index("--transaction-id") + 1]
            self.transactions[transaction_id] = "ACQUIRED"
            path = self.repo / TRANSACTION_ROOT / transaction_id / "transaction.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}", encoding="utf-8")
            return self._result(argv, self._body(transaction_id))
        transaction_id = self._transaction_id(argv)
        if command == "checkpoint":
            phase = argv[argv.index("--phase") + 1]
            if phase.startswith("GENERATED_REBUILD"):
                declared = [
                    argv[i + 1] for i, value in enumerate(argv) if value == "--generator-id"
                ]
                if declared != list(GENERATORS):
                    return CommandResult(
                        argv, 1, "", "PUBLICATION_GENERATOR_ORDER_MISMATCH", 0.1, "log"
                    )
            self.transactions[transaction_id] = phase
            return self._result(argv, self._body(transaction_id))
        if command == "release":
            outcome = argv[argv.index("--outcome") + 1]
            self.transactions[transaction_id] = "FAILED" if outcome == "failed" else "RELEASED"
            body = {"final_phase": self.transactions[transaction_id], "outcome": outcome.upper()}
            return self._result(argv, body)
        if command == "replay":
            return self._result(argv, self._body(transaction_id))
        raise AssertionError(f"unexpected fence command {argv}")

    def _body(self, transaction_id: str) -> dict[str, Any]:
        phase = self.transactions[transaction_id]
        candidate = None
        if (
            PHASES.index(phase) >= PHASES.index("CANDIDATE_COMMIT_PRE")
            if phase in PHASES
            else False
        ):
            candidate = self.head
        return {
            "status": "PASS",
            "phase": phase,
            "transaction_id": transaction_id,
            "transaction_sha256": f"sha-{transaction_id}",
            "lease_id": f"lease-{transaction_id}",
            "candidate_sha": candidate,
        }


def _config(world: FakeWorld, **overrides: Any) -> RunConfig:
    fields: dict[str, Any] = {
        "run_id": "r1",
        "repository_root": world.repo,
        "python": "py",
        "evidence_dir": world.repo / "ev",
        "actor": "integration-coordinator",
        "thread_id": "thread-1",
        "task_id": "TASK",
        "expected_main": "m0",
        "scope": _scope(),
        "phase_order": PHASES,
        "parent_run": "outputs/validation_runtime/parent/test_runtime_summary.json",
        "commit_trailer": "Co-Authored-By: Test <test@example.com>",
        "contract_change": True,
    }
    fields.update(overrides)
    return RunConfig(**fields)


def _engine(tmp_path: Path, world: FakeWorld, config: RunConfig, stages: str = "BC") -> Any:
    steps = []
    if "B" in stages:
        steps += build_prepare_steps(config, world)
    if "C" in stages:
        steps += build_formal_steps(config, world)
    journal = PublicationJournal(tmp_path / "run" / "journal.jsonl")
    return (
        PublicationRunEngine(
            steps=steps,
            journal=journal,
            context=StepContext(run_id=config.run_id),
            monotonic=lambda: 0.0,
            now=lambda: T0,
        ),
        journal,
    )


def test_stages_b_and_c_run_the_manual_flow_in_order_and_freeze_the_candidate(
    tmp_path: Path,
) -> None:
    world = FakeWorld(tmp_path)
    world.dirty_after_generator_runs = [
        ["registry/architecture_compatibility_authority/f1.json"],
        [],
    ]
    engine, journal = _engine(tmp_path, world, _config(world))
    summary = engine.run()
    assert summary.outcome == OUTCOME_COMPLETE, summary.to_dict()
    fence_commands = [
        (
            call[3],
            (
                call[call.index("--phase") + 1]
                if "--phase" in call
                else call[call.index("--outcome") + 1] if "--outcome" in call else ""
            ),
        )
        for call in world.calls
        if len(call) > 3 and call[2] == FENCE_SCRIPT and call[3] != "replay"
    ]
    assert fence_commands == [
        ("acquire", ""),
        ("checkpoint", "TASK_SOURCE_PRE_WRITE"),
        ("checkpoint", "GENERATED_REBUILD_PRE"),
        ("checkpoint", "GENERATED_REBUILD_POST"),
        ("release", "failed"),
        ("acquire", ""),
        ("checkpoint", "TASK_SOURCE_PRE_WRITE"),
        ("checkpoint", "GENERATED_REBUILD_PRE"),
        ("checkpoint", "GENERATED_REBUILD_POST"),
        ("checkpoint", "CANDIDATE_COMMIT_PRE"),
        ("checkpoint", "FORMAL_VALIDATION_PRE"),
    ]
    # the prep run produced one reseal commit (round 1 dirty), then the tree stayed clean
    assert world.commit_count == 1 and world.staged == [
        ("registry/architecture_compatibility_authority/f1.json",)
    ]
    detail = journal.replay().last_detail("C24.formal_validation_pre")
    assert detail is not None
    assert detail["candidate_sha"] == "h1" and detail["transaction_sha256"] == "sha-r1-formal"
    assert journal.replay().last_detail("B13.prep_converge")["rounds"] == 1  # type: ignore[index]
    commit_calls = world.named("commit")
    assert "Co-Authored-By: Test <test@example.com>" in commit_calls[0][-1]
    assert "(round 1)" in commit_calls[0][-1]


def test_acquire_arguments_come_from_the_derived_scope_and_only_formal_binds_a_parent(
    tmp_path: Path,
) -> None:
    world = FakeWorld(tmp_path)
    engine, _ = _engine(tmp_path, world, _config(world))
    engine.run()
    prep, formal = world.named("acquire")
    for call in (prep, formal):
        owned = [call[i + 1] for i, v in enumerate(call) if v == "--owned-path"]
        shared = [call[i + 1] for i, v in enumerate(call) if v == "--shared-path"]
        generators = [call[i + 1] for i, v in enumerate(call) if v == "--generator-id"]
        tiers = [call[i + 1] for i, v in enumerate(call) if v == "--required-tier"]
        assert owned == ["src/pkg/a.py", "tests/test_a.py"]
        assert shared == ["docs/system_flow.md", GENERATED, "inputs/architecture"]
        assert generators == list(GENERATORS) and tiers == ["architecture", "contract", "full"]
        assert (
            call[call.index("--frozen-base") + 1] == "m0" == call[call.index("--expected-main") + 1]
        )
        assert call[call.index("--actor") + 1] == "integration-coordinator"
    assert "--full-parent" not in prep and "--full-parent" in formal
    assert formal[formal.index("--full-parent") + 1].endswith("test_runtime_summary.json")
    assert prep[prep.index("--transaction-id") + 1] == "r1-prep"
    assert formal[formal.index("--transaction-id") + 1] == "r1-formal"


def test_the_governed_preflight_and_readiness_use_the_scope_and_the_frozen_candidate(
    tmp_path: Path,
) -> None:
    world = FakeWorld(tmp_path)
    engine, _ = _engine(tmp_path, world, _config(world))
    engine.run()
    (preflight,) = world.named(PREFLIGHT_SCRIPT)
    assert preflight[preflight.index("--stage") + 1] == "INTEGRATION"
    assert preflight[preflight.index("--expected-base") + 1] == "m0"
    assert "--contract-change" in preflight and "--remote-action" not in preflight
    claims = [preflight[i + 1] for i, v in enumerate(preflight) if v == "--claim"]
    coordinator = [preflight[i + 1] for i, v in enumerate(preflight) if v == "--coordinator-path"]
    assert claims == ["task=src/pkg/a.py", "task=tests/test_a.py"]
    assert coordinator == ["docs/system_flow.md", GENERATED, "inputs/architecture"]
    (readiness,) = world.named(READINESS_SCRIPT)
    assert readiness[readiness.index("--candidate-sha") + 1] == world.head


def test_a_failing_generator_stops_the_prep_and_later_steps_never_run(tmp_path: Path) -> None:
    world = FakeWorld(tmp_path)
    world.failing_generator = "atlas-authority"
    engine, journal = _engine(tmp_path, world, _config(world))
    summary = engine.run()
    assert (
        summary.outcome == OUTCOME_STOPPED_FAILED and summary.stopped_step == "B12.prep_generators"
    )
    failed = journal.replay().last_detail("B12.prep_generators", status="FAILED")
    assert failed is not None and failed["code"] == "PUBLICATION_RUN_GENERATOR_FAILED"
    assert failed["message"] == "atlas-authority"
    assert not world.named("release")  # the prep transaction is left for inspection, not released


def test_generators_that_never_converge_fail_after_the_configured_rounds(tmp_path: Path) -> None:
    world = FakeWorld(tmp_path)
    world.dirty_after_generator_runs = [["inputs/architecture/x.yaml"]] * 10
    world.dirty = ["inputs/architecture/x.yaml"]
    engine, journal = _engine(tmp_path, world, _config(world, max_generator_rounds=2), "B")
    summary = engine.run()
    assert summary.stopped_step == "B13.prep_converge"
    failed = journal.replay().last_detail("B13.prep_converge", status="FAILED")
    assert failed is not None and failed["code"] == "PUBLICATION_RUN_GENERATORS_NOT_CONVERGENT"
    assert world.commit_count == 2


def test_dirty_paths_outside_the_generated_scope_are_never_staged(tmp_path: Path) -> None:
    world = FakeWorld(tmp_path)
    world.dirty = ["src/pkg/a.py", "inputs/architecture/ok.yaml"]
    engine, journal = _engine(tmp_path, world, _config(world), "B")
    engine.run(until="B12.prep_generators")
    summary = engine.run()
    assert summary.stopped_step == "B13.prep_converge"
    failed = journal.replay().last_detail("B13.prep_converge", status="FAILED")
    assert failed is not None and failed["code"] == "PUBLICATION_RUN_DIRTY_OUTSIDE_SCOPE"
    assert failed["paths"] == ["src/pkg/a.py"]
    assert world.staged == [] and world.commit_count == 0  # nothing was staged or committed


def test_a_resume_after_a_crash_probes_the_fence_instead_of_repeating_a_checkpoint(
    tmp_path: Path,
) -> None:
    world = FakeWorld(tmp_path)
    engine, journal = _engine(tmp_path, world, _config(world), "B")
    engine.run(until="B10.prep_acquire")
    # simulate: the checkpoint reached the fence but the process died before journaling DONE
    world.transactions["r1-prep"] = "TASK_SOURCE_PRE_WRITE"
    journal.append(step_id="B11.prep_task_phase", status="STARTED", now=T0)
    before = len(world.named("checkpoint"))
    engine.run(until="B11.prep_task_phase")
    assert len(world.named("checkpoint")) == before  # not issued a second time
    detail = journal.replay().last_detail("B11.prep_task_phase")
    assert detail is not None and detail["_recovered"] is True


@pytest.mark.parametrize(
    "setup,step,code",
    [
        ("preflight", "C25.governed_preflight", "PUBLICATION_RUN_PREFLIGHT_BLOCKED"),
        ("readiness", "C26.readiness", "PUBLICATION_RUN_READINESS_FAILED"),
        ("candidate", "C26.readiness", "PUBLICATION_RUN_READINESS_FAILED"),
        ("drift", "C22.formal_generators", "PUBLICATION_RUN_GENERATED_DRIFT"),
        ("dirty", "C20.formal_acquire", "PUBLICATION_RUN_TREE_NOT_CLEAN"),
    ],
)
def test_the_formal_stage_fails_closed_on_each_governed_check(
    tmp_path: Path, setup: str, step: str, code: str
) -> None:
    world = FakeWorld(tmp_path)
    if setup == "preflight":
        world.preflight_status = "BLOCKED"
    elif setup == "readiness":
        world.readiness_status = "BLOCKED"
    elif setup == "candidate":
        world.candidate_override = "someone-else"
    elif setup == "drift":
        world.dirty_after_generator_runs = [["inputs/architecture/late.yaml"]]
    engine, journal = _engine(tmp_path, world, _config(world), "C")
    if setup == "dirty":
        world.dirty = ["inputs/architecture/x.yaml"]
    summary = engine.run()
    assert summary.outcome == OUTCOME_STOPPED_FAILED and summary.stopped_step == step
    failed = journal.replay().last_detail(step, status="FAILED")
    assert failed is not None and failed["code"] == code


def test_a_formal_transaction_without_a_parent_run_is_refused_before_any_fence_call(
    tmp_path: Path,
) -> None:
    world = FakeWorld(tmp_path)
    engine, journal = _engine(tmp_path, world, _config(world, parent_run=None), "C")
    summary = engine.run()
    assert summary.stopped_step == "C20.formal_acquire"
    failed = journal.replay().last_detail("C20.formal_acquire", status="FAILED")
    assert failed is not None and failed["code"] == "PUBLICATION_RUN_PARENT_RUN_REQUIRED"
    assert not world.named("acquire")


def test_the_formal_acquire_refuses_when_the_owned_scope_changed_since_the_run_started(
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    world = FakeWorld(tmp_path)
    frozen = _scope()
    changed = replace(frozen, owned_paths=(*frozen.owned_paths, "src/pkg/sneaked_in.py"))
    config = _config(world, scope_check=lambda: changed)
    engine, journal = _engine(tmp_path, world, config, "C")
    summary = engine.run()
    assert summary.stopped_step == "C20.formal_acquire"
    failed = journal.replay().last_detail("C20.formal_acquire", status="FAILED")
    assert failed is not None and failed["code"] == "PUBLICATION_RUN_SCOPE_CHANGED"
    assert failed["current"][-1] == "src/pkg/sneaked_in.py"
    assert world.named("acquire") == []

    same = _config(FakeWorld(tmp_path / "ok"), scope_check=lambda: frozen)
    world_ok = FakeWorld(tmp_path / "ok2")
    engine_ok, _ = _engine(
        tmp_path / "ok2", world_ok, _config(world_ok, scope_check=lambda: frozen), "C"
    )
    assert engine_ok.run().outcome == OUTCOME_COMPLETE
    assert same is not None
