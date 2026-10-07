"""DEVX-016 S3: the whole run (prep, formal, validation, publication) against a scripted world."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from publication_run_support import (
    MAIN0,
    T0,
    FakeCollector,
    FakeLauncher,
    World,
    make_run_config,
)

from ai_trading_system.platform.architecture.publication_checks import (
    AUTHORIZATION_SCHEMA_VERSION,
    AUTHORIZATION_SCOPE,
)
from ai_trading_system.platform.architecture.publication_journal import PublicationJournal
from ai_trading_system.platform.architecture.publication_orchestrator import (
    OUTCOME_AWAITING_AUTHORIZATION,
    OUTCOME_COMPLETE,
    OUTCOME_NEEDS_REVIEW,
    OUTCOME_STOPPED_FAILED,
    PublicationRunEngine,
    StepContext,
)
from ai_trading_system.platform.architecture.publication_publish_steps import (
    FORBIDDEN_PUSH_TOKENS,
    PublishConfig,
    build_publication_steps,
    build_validation_steps,
    ordinary_push_argv,
)
from ai_trading_system.platform.architecture.publication_steps import (
    build_formal_steps,
    build_prepare_steps,
)


class Harness:
    def __init__(self, tmp_path: Path, *, push_by_command: bool = True) -> None:
        self.world = World(tmp_path)
        self.launcher = FakeLauncher(self.world)
        self.collector = FakeCollector(self.world)
        self.run_config = make_run_config(self.world)
        self.authorization = self.world.repo / "ev" / "authorization.json"
        self.sleeps = 0
        self.journal = PublicationJournal(tmp_path / "run" / "journal.jsonl")
        self.publish = PublishConfig(
            run=self.run_config,
            launcher=self.launcher,
            collector=self.collector,
            sleep=self._sleep,
            now=lambda: T0,
            authorization_path=self.authorization,
            start_branch="lane",
            poll_seconds=0.0,
            push_by_command=push_by_command,
        )
        steps = (
            build_prepare_steps(self.run_config, self.world)
            + build_formal_steps(self.run_config, self.world)
            + build_validation_steps(self.publish, self.world)
            + build_publication_steps(self.publish, self.world)
        )
        self.engine = PublicationRunEngine(
            steps=steps,
            journal=self.journal,
            context=StepContext(run_id="r1"),
            monotonic=lambda: 0.0,
            now=lambda: T0,
        )

    def _sleep(self, seconds: float) -> None:
        self.sleeps += 1

    def authorize(self, **overrides: Any) -> None:
        body: dict[str, Any] = {
            "schema_version": AUTHORIZATION_SCHEMA_VERSION,
            "scope": AUTHORIZATION_SCOPE,
            "candidate_sha": self.world.head,
            "expected_main": MAIN0,
            "force_push": False,
            "pull_request": False,
            "authorized_by": "project_owner",
            "authorized_at": T0.isoformat(),
            "source": "test: passes -> publish directly",
        }
        body.update(overrides)
        self.authorization.write_text(json.dumps(body), encoding="utf-8")


def test_the_run_stops_at_the_authorization_gate_without_pushing_then_completes(
    tmp_path: Path,
) -> None:
    h = Harness(tmp_path)
    summary = h.engine.run()
    assert summary.outcome == OUTCOME_AWAITING_AUTHORIZATION
    assert summary.stopped_step == "E58.authorization_gate"
    assert h.world.pushes == [] and h.world.remote_tip == MAIN0  # nothing was pushed
    done = [row.step_id for row in summary.rows if row.status == "DONE"]
    assert done[-1] == "E57.closeout_preflight"
    assert h.world.main == h.world.head  # the local fast-forward already happened before the gate
    # local-publish was launched detached, exactly once, by the reviewed fence command
    (worker,) = [c for c in h.launcher.launched if "local-publish" in c]
    assert worker[worker.index("--actor") + 1] == "integration-coordinator"

    h.authorize()
    final = h.engine.run()
    assert final.outcome == OUTCOME_COMPLETE, final.to_dict()
    assert h.world.pushes == [("git", "push", "origin", "main")]
    assert h.world.remote_tip == h.world.head
    assert h.world.transactions["r1-formal"] == "RELEASED"
    assert h.world.branch == "lane"
    assert h.engine.run().outcome == OUTCOME_COMPLETE and len(h.world.pushes) == 1  # idempotent


def test_by_default_the_command_never_pushes_and_waits_for_the_owner(tmp_path: Path) -> None:
    h = Harness(tmp_path, push_by_command=False)
    h.authorize()  # a valid record does not make the command push unless the owner opted in
    summary = h.engine.run()
    assert summary.outcome == OUTCOME_AWAITING_AUTHORIZATION
    assert summary.stopped_step == "E59.push"
    assert "git push origin main" in summary.next_action
    assert h.world.pushes == [] and h.world.remote_tip == MAIN0
    gate = h.journal.replay().last_detail("E58.authorization_gate")
    assert gate is not None and gate["authorization"] == "NOT_REQUIRED_OWNER_PUSHES"
    waiting = h.journal.replay().last_detail("E59.push", status="AWAITING_AUTHORIZATION")
    assert waiting is not None and waiting["owner_command"] == "git push origin main"
    assert h.engine.run().outcome == OUTCOME_AWAITING_AUTHORIZATION and h.world.pushes == []


def test_the_owners_own_push_is_proven_against_the_remote_then_the_run_completes(
    tmp_path: Path,
) -> None:
    h = Harness(tmp_path, push_by_command=False)
    assert h.engine.run().stopped_step == "E59.push"
    h.world.remote_tip = h.world.head  # the owner ran the ordinary push in their terminal
    final = h.engine.run()
    assert final.outcome == OUTCOME_COMPLETE, final.to_dict()
    assert h.world.pushes == []  # this tool never issued a push
    done = h.journal.replay().last_detail("E59.push")
    assert done is not None and done["pushed_by"] == "OWNER_TERMINAL"
    assert h.world.transactions["r1-formal"] == "RELEASED" and h.world.branch == "lane"


def test_an_owner_push_of_something_else_is_not_accepted_as_the_candidate(tmp_path: Path) -> None:
    h = Harness(tmp_path, push_by_command=False)
    h.engine.run()
    h.world.remote_tip = "f" * 40  # the remote moved to a commit that is not the candidate
    summary = h.engine.run()
    assert summary.outcome == OUTCOME_AWAITING_AUTHORIZATION
    assert summary.stopped_step == "E59.push" and h.world.pushes == []
    assert not h.world.named("release", "--outcome", "completed")


def test_a_wrong_or_broader_authorization_never_pushes(tmp_path: Path) -> None:
    for overrides, code in (
        ({"candidate_sha": "d" * 40}, "PUBLICATION_AUTHORIZATION_CANDIDATE"),
        ({"force_push": True}, "PUBLICATION_AUTHORIZATION_BROAD"),
        ({"scope": "ANYTHING"}, "PUBLICATION_AUTHORIZATION_SCOPE"),
    ):
        case = tmp_path / code
        case.mkdir()
        h = Harness(case)
        h.engine.run()
        h.authorize(**overrides)
        summary = h.engine.run()
        assert summary.outcome == OUTCOME_STOPPED_FAILED
        assert summary.stopped_step == "E58.authorization_gate"
        detail = h.journal.replay().last_detail("E58.authorization_gate", status="FAILED")
        assert detail["code"] == code  # type: ignore[index]
        assert h.world.pushes == []


def test_a_crash_after_the_push_is_proven_by_the_remote_and_never_pushed_twice(
    tmp_path: Path,
) -> None:
    h = Harness(tmp_path)
    h.engine.run()
    h.authorize()
    h.engine.run(until="E58.authorization_gate")
    # the push reached the remote but the process died before DONE was journaled
    h.world.remote_tip = h.world.head
    h.journal.append(step_id="E59.push", status="STARTED", now=T0)
    summary = h.engine.run()
    assert summary.outcome == OUTCOME_COMPLETE and h.world.pushes == []
    assert h.journal.replay().last_detail("E59.push")["_recovered"] is True  # type: ignore[index]


def test_a_crash_before_the_push_without_proof_stops_for_review(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    h.engine.run()
    h.authorize()
    h.engine.run(until="E58.authorization_gate")
    h.journal.append(step_id="E59.push", status="STARTED", now=T0)  # remote tip unchanged
    summary = h.engine.run()
    assert summary.outcome == OUTCOME_NEEDS_REVIEW and h.world.pushes == []


def test_a_failed_push_stops_before_verification_and_release(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    h.engine.run()
    h.authorize()
    h.world.push_exit = 1
    summary = h.engine.run()
    assert summary.outcome == OUTCOME_STOPPED_FAILED and summary.stopped_step == "E59.push"
    assert h.world.transactions["r1-formal"] != "RELEASED"
    assert not h.world.named("release", "--outcome", "completed")


def test_a_diverged_remote_is_reported_and_never_repaired(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    h.world.remote_is_ancestor = False
    summary = h.engine.run()
    assert summary.outcome == OUTCOME_STOPPED_FAILED and summary.stopped_step == "E55.fetch"
    failed = h.journal.replay().last_detail("E55.fetch", status="FAILED")
    assert failed is not None and failed["code"] == "PUBLICATION_RUN_REMOTE_DIVERGED"
    assert h.world.pushes == [] and not h.world.named("push")
    assert not any(c[:2] == ("git", "merge") or c[:2] == ("git", "rebase") for c in h.world.calls)


def test_a_worker_that_needs_recovery_stops_the_run_with_guidance(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    h.launcher.worker_status = "RECOVERY_REQUIRED"
    summary = h.engine.run()
    assert (
        summary.outcome == OUTCOME_STOPPED_FAILED
        and summary.stopped_step == "E54.local_publish_wait"
    )
    failed = h.journal.replay().last_detail("E54.local_publish_wait", status="FAILED")
    assert failed is not None and "local-publication-recover" in failed["next"]
    assert h.world.named("REMOTE_PUSH_PRE") == []


def test_the_pre_publish_checklist_blocks_the_fast_forward(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    h.collector.git_locks = ("AUTO_MERGE.lock",)
    summary = h.engine.run()
    assert (
        summary.outcome == OUTCOME_STOPPED_FAILED
        and summary.stopped_step == "E50.pre_publish_checks"
    )
    assert [c for c in h.launcher.launched if "local-publish" in c] == []
    assert h.world.named("LOCAL_MAIN_FF_PRE") == []


def test_validation_needs_a_quiet_host_and_reports_failures_and_dead_drivers(
    tmp_path: Path,
) -> None:
    cases = {
        "busy": ("PUBLICATION_RUN_PROCESSES_PRESENT", "D30.validation_launch"),
        "red": ("PUBLICATION_RUN_VALIDATION_NOT_GREEN", "D31.validation_wait"),
        "died": ("PUBLICATION_RUN_DRIVER_DIED", "D31.validation_wait"),
    }
    for name, (code, step) in cases.items():
        case = tmp_path / name
        case.mkdir()
        h = Harness(case)
        if name == "busy":
            h.collector.processes = ("1|2|python.exe|pytest",)
        elif name == "red":
            h.launcher.driver_status = "STOPPED"
        else:
            h.launcher.driver_dies = True
        summary = h.engine.run()
        assert summary.outcome == OUTCOME_STOPPED_FAILED and summary.stopped_step == step
        failed = h.journal.replay().last_detail(step, status="FAILED")
        assert failed is not None and failed["code"] == code
        assert h.world.named("LOCAL_MAIN_FF_PRE") == []


def test_slow_stages_are_flagged_against_the_baselines(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    h.launcher.driver_results = [
        {"stage": "named-parent-positive", "exit_code": 0, "elapsed_seconds": 751.0,
         "baseline_seconds": 502.0},
        {"stage": "contract-validation", "exit_code": 0, "elapsed_seconds": 292.0,
         "baseline_seconds": 281.0},
    ]  # fmt: skip
    h.engine.run(until="D31.validation_wait")
    detail = h.journal.replay().last_detail("D31.validation_wait")
    assert detail is not None and detail["slow_stages"] == ["named-parent-positive"]


def test_the_push_argv_is_ordinary_and_every_force_variant_is_unreachable() -> None:
    assert ordinary_push_argv() == ["git", "push", "origin", "main"]
    assert not (set(ordinary_push_argv()) & FORBIDDEN_PUSH_TOKENS)
    assert {"--force", "-f", "--force-with-lease", "--delete", "--mirror"} <= FORBIDDEN_PUSH_TOKENS


@pytest.mark.parametrize("step_id", ["E50.pre_publish_checks", "E58.authorization_gate"])
def test_no_step_after_the_gate_can_run_before_it_is_done(tmp_path: Path, step_id: str) -> None:
    h = Harness(tmp_path)
    ids = [step.step_id for step in h.engine.steps]
    assert ids.index("E58.authorization_gate") < ids.index("E59.push") < ids.index("E62.release")
    assert ids.index("E50.pre_publish_checks") < ids.index("E53.local_publish_launch")
    assert step_id in ids


def test_a_driver_that_exits_right_after_its_terminal_status_is_not_reported_dead(
    tmp_path: Path,
) -> None:
    h = Harness(tmp_path)
    h.launcher.driver_status = "RUNNING"  # the progress record is still non-terminal at first read
    h.engine.run(until="D30.validation_launch")
    progress = next(h.world.repo.glob("ev/*_validation_progress.json"))

    def finish_then_report_gone(pid: int) -> bool:
        # between the first read and the liveness probe the driver wrote its result and exited
        payload = json.loads(progress.read_text(encoding="utf-8"))
        payload["status"] = "VALIDATION_PASS_AWAITING_PUBLICATION_REVIEW"
        progress.write_text(json.dumps(payload), encoding="utf-8")
        return False

    h.launcher.is_running = finish_then_report_gone  # type: ignore[method-assign]
    summary = h.engine.run(until="D31.validation_wait")
    assert summary.outcome == "PAUSED_AT_REQUESTED_STEP", summary.to_dict()


def test_a_resumed_wait_takes_the_driver_pid_from_the_progress_record(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    h.engine.run(until="C26.readiness")
    progress = h.world.repo / "ev" / "r1_validation_progress.json"
    progress.write_text(
        json.dumps({"status": "RUNNING", "stage": "full", "coordinator_pid": 4242}),
        encoding="utf-8",
    )
    # a crash left D30 STARTED without DONE: the probe finds the progress file and has no pid
    h.journal.append(step_id="D30.validation_launch", status="STARTED", now=T0)
    seen: list[int] = []

    def gone(pid: int) -> bool:
        seen.append(pid)
        return False

    h.launcher.is_running = gone  # type: ignore[method-assign]
    summary = h.engine.run(until="D31.validation_wait")
    assert summary.outcome == OUTCOME_STOPPED_FAILED and seen == [4242]
    failed = h.journal.replay().last_detail("D31.validation_wait", status="FAILED")
    assert failed is not None and failed["code"] == "PUBLICATION_RUN_DRIVER_DIED"
