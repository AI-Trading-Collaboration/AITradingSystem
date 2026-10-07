"""DEVX-016 S3: the validation driver keeps the candidate identity and stops at first failure."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.platform.architecture.publication_validation import (
    PASS_STATUS,
    STAGE_BASELINE_SECONDS,
    CandidateValidationDriver,
    ValidationConfig,
    ValidationDriverError,
    build_stages,
    read_progress,
)

CANDIDATE = "c" * 40
MAIN = "a" * 40
TXN_SHA = "t" * 64
LEASE = "lease-1"


def _config(tmp_path: Path) -> ValidationConfig:
    evidence = tmp_path / "ev"
    transaction = tmp_path / "tx" / "transaction.json"
    readiness = evidence / "readiness.json"
    evidence.mkdir()
    readiness.write_text(
        json.dumps({"status": "PASS", "candidate_sha": CANDIDATE}), encoding="utf-8"
    )
    return ValidationConfig(
        run_id="r1",
        repository_root=tmp_path / "repo",
        python="py",
        candidate_sha=CANDIDATE,
        expected_main=MAIN,
        task_id="TASK",
        actor="integration-coordinator",
        transaction=transaction,
        transaction_sha256=TXN_SHA,
        lease_id=LEASE,
        parent_run=tmp_path
        / "outputs"
        / "validation_runtime"
        / "parent"
        / "test_runtime_summary.json",
        evidence_dir=evidence,
        progress_path=evidence / "progress.json",
        readiness_path=readiness,
        boundary_id="b-1",
        environment={"PYTHONPATH": "src"},
    )


class FakeProcess:
    def __init__(self, pid: int, exit_code: int, polls: int) -> None:
        self.pid = pid
        self._exit_code = exit_code
        self._remaining = polls
        self.returncode: int | None = None

    def poll(self) -> int | None:
        if self._remaining > 0:
            self._remaining -= 1
            return None
        self.returncode = self._exit_code
        return self.returncode


class FakeWorld:
    def __init__(self) -> None:
        self.head = CANDIDATE
        self.main = MAIN
        self.dirty: list[str] = []
        self.txn_sha = TXN_SHA
        self.heartbeat_exit = 0
        self.commands: list[tuple[str, ...]] = []
        self.started: list[tuple[str, ...]] = []
        self.exit_codes: dict[str, int] = {}
        self.polls_per_stage = 1
        self.clock = 0.0
        self.clock_step = 0.0
        self.events: list[tuple[str, ...]] = []

    def run_command(self, argv: Sequence[str]) -> tuple[int, str]:
        argv = tuple(argv)
        self.commands.append(argv)
        if argv[:2] == ("git", "rev-parse"):
            return 0, (self.head if argv[2] == "HEAD" else self.main) + "\n"
        if "worktree-audit" in argv:
            return 0, json.dumps({"status": "PASS", "dirty_paths": self.dirty})
        if "validate" in argv:
            body = {
                "status": "PASS",
                "candidate_sha": CANDIDATE,
                "transaction_sha256": self.txn_sha,
                "lease_id": LEASE,
            }
            return 0, json.dumps(body)
        if "heartbeat" in argv:
            self.events.append(("heartbeat",))
            return self.heartbeat_exit, "{}"
        raise AssertionError(argv)

    def start_process(self, argv: Sequence[str], log: Path, env: Mapping[str, str]) -> FakeProcess:
        argv = tuple(argv)
        self.started.append(argv)
        log.write_text("log\n", encoding="utf-8")
        stage = argv[3] if "run_validation_tier.py" in " ".join(argv) else "named-parent-positive"
        self.events.append(("start", stage))
        return FakeProcess(
            1000 + len(self.started), self.exit_codes.get(stage, 0), self.polls_per_stage
        )

    def sleep(self, seconds: float) -> None:
        self.clock += self.clock_step

    def monotonic(self) -> float:
        return self.clock


def _driver(config: ValidationConfig, world: FakeWorld) -> CandidateValidationDriver:
    return CandidateValidationDriver(
        config,
        run_command=world.run_command,
        start_process=world.start_process,
        sleep=world.sleep,
        monotonic=world.monotonic,
    )


def test_the_stage_list_is_the_fixed_manual_order_with_the_right_arguments(tmp_path: Path) -> None:
    stages = build_stages(_config(tmp_path))
    assert [s.stage_id for s in stages] == [
        "named-parent-positive",
        "contract-validation",
        "integration",
        "reproducibility",
        "architecture-fitness",
        "full",
    ]
    first = stages[0]
    assert first.argv[:5] == ("py", "-B", "-m", "pytest", "-n")
    assert first.argv[-1] == "tests/test_named_data_quality_actual_candidate.py" and first.heartbeat
    for stage in stages[1:]:
        argv = stage.argv
        assert argv[3] == stage.stage_id and "--write-runtime-artifact" in argv
        assert argv[argv.index("--trigger-reason") + 1] == "failure_fix_rerun"
        assert argv[argv.index("--parent-run") + 1].endswith("test_runtime_summary.json")
        assert argv[argv.index("--task-id") + 1] == "TASK"
        assert argv[argv.index("--boundary-id") + 1] == "b-1"
        assert stage.artifact_dir is not None and stage.artifact_dir.name == f"r1-{stage.stage_id}"
        assert ("--pytest-arg=-x" in argv) == (stage.stage_id == "architecture-fitness")
        assert ("--publication-transaction" in argv) == (stage.stage_id == "full")
        assert stage.heartbeat == (stage.stage_id != "full")
    assert set(STAGE_BASELINE_SECONDS) == {s.stage_id for s in stages}


def test_a_green_run_records_every_stage_and_rechecks_identity_before_each(tmp_path: Path) -> None:
    config = _config(tmp_path)
    world = FakeWorld()
    assert _driver(config, world).run() == 0
    progress = read_progress(config.progress_path)
    assert progress is not None and progress["status"] == PASS_STATUS
    assert progress["formal_authority"] is False and progress["production_effect"] == "none"
    assert [row["stage"] for row in progress["results"]] == [
        s.stage_id for s in build_stages(config)
    ]
    assert all(row["exit_code"] == 0 for row in progress["results"])
    assert (
        progress["results"][0]["baseline_seconds"]
        == STAGE_BASELINE_SECONDS["named-parent-positive"]
    )
    head_checks = [c for c in world.commands if c[:3] == ("git", "rev-parse", "HEAD")]
    assert len(head_checks) == 6 and len(world.started) == 6


def test_the_run_stops_at_the_first_failing_stage(tmp_path: Path) -> None:
    config = _config(tmp_path)
    world = FakeWorld()
    world.exit_codes["integration"] = 1
    assert _driver(config, world).run() == 1
    progress = read_progress(config.progress_path)
    assert (
        progress is not None
        and progress["status"] == "STOPPED"
        and progress["stage"] == "integration"
    )
    assert [row["stage"] for row in progress["results"]] == [
        "named-parent-positive",
        "contract-validation",
        "integration",
    ]
    assert len(world.started) == 3  # reproducibility, architecture-fitness and full never started


@pytest.mark.parametrize(
    "change,code",
    [
        ("head", "VALIDATION_CANDIDATE_IDENTITY"),
        ("main", "VALIDATION_CANDIDATE_IDENTITY"),
        ("dirty", "VALIDATION_WORKTREE_DIRTY"),
        ("transaction", "VALIDATION_ADMISSION_CHANGED"),
    ],
)
def test_a_changed_identity_blocks_before_the_stage_starts(
    tmp_path: Path, change: str, code: str
) -> None:
    config = _config(tmp_path)
    world = FakeWorld()
    driver = _driver(config, world)
    if change == "head":
        world.head = "d" * 40
    elif change == "main":
        world.main = "e" * 40
    elif change == "dirty":
        world.dirty = ["src/x.py"]
    else:
        world.txn_sha = "x" * 64
    with pytest.raises(ValidationDriverError) as raised:
        driver.run()
    assert raised.value.code == code and world.started == []
    progress = read_progress(config.progress_path)
    assert progress is not None and progress["status"] == "BLOCKED"
    assert code in progress["detail"]


def test_the_identity_is_rechecked_between_stages_not_only_at_admission(tmp_path: Path) -> None:
    config = _config(tmp_path)
    world = FakeWorld()
    original = world.start_process

    def start_then_move_head(argv: Sequence[str], log: Path, env: Mapping[str, str]) -> FakeProcess:
        process = original(argv, log, env)
        if len(world.started) == 2:
            world.head = "f" * 40  # something moved HEAD while stage 2 was running
        return process

    driver = CandidateValidationDriver(
        config,
        run_command=world.run_command,
        start_process=start_then_move_head,
        sleep=world.sleep,
        monotonic=world.monotonic,
    )
    with pytest.raises(ValidationDriverError) as raised:
        driver.run()
    assert raised.value.code == "VALIDATION_CANDIDATE_IDENTITY" and len(world.started) == 2


def test_readiness_must_be_pass_for_exactly_this_candidate(tmp_path: Path) -> None:
    for index, body in enumerate(
        (
            {"status": "BLOCKED", "candidate_sha": CANDIDATE},
            {"status": "PASS", "candidate_sha": "d" * 40},
        )
    ):
        case = tmp_path / f"case{index}"
        case.mkdir()
        config = _config(case)
        config.readiness_path.write_text(json.dumps(body), encoding="utf-8")
        world = FakeWorld()
        with pytest.raises(ValidationDriverError) as raised:
            _driver(config, world).run()
        assert raised.value.code == "VALIDATION_READINESS_NOT_PASS" and world.started == []


def test_existing_stage_evidence_and_a_second_driver_are_refused(tmp_path: Path) -> None:
    config = _config(tmp_path)
    stages = build_stages(config)
    stages[0].log_path.write_text("old\n", encoding="utf-8")
    world = FakeWorld()
    with pytest.raises(ValidationDriverError) as raised:
        _driver(config, world).run()
    assert raised.value.code == "VALIDATION_EVIDENCE_EXISTS" and world.started == []

    (tmp_path / "second").mkdir()
    config2 = _config(tmp_path / "second")
    first = _driver(config2, FakeWorld())
    assert first.run() == 0
    with pytest.raises(ValidationDriverError) as raised:
        _driver(config2, FakeWorld()).run()
    assert raised.value.code == "VALIDATION_PROGRESS_EXISTS"


def test_heartbeats_keep_the_lease_alive_for_pre_full_stages_and_a_failed_one_stops_the_run(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    world = FakeWorld()
    world.polls_per_stage = 3
    world.clock_step = 400.0  # each poll loop iteration advances past the 300 s interval
    assert _driver(config, world).run() == 0
    beats = [c for c in world.commands if "heartbeat" in c]
    assert beats and all(c[c.index("--lease-id") + 1] == LEASE for c in beats)
    # the Full manages its own lease heartbeat: none from the driver once it has started
    after_full = world.events[world.events.index(("start", "full")) :]
    assert ("heartbeat",) not in after_full
    assert world.events.count(("start", "architecture-fitness")) == 1
    assert ("heartbeat",) in world.events[: world.events.index(("start", "full"))]

    (tmp_path / "b").mkdir()
    config_b = _config(tmp_path / "b")
    world_b = FakeWorld()
    world_b.polls_per_stage = 3
    world_b.clock_step = 400.0
    world_b.heartbeat_exit = 1
    assert _driver(config_b, world_b).run() == 2  # child exited 0 but the lease needs review
    progress = read_progress(config_b.progress_path)
    assert progress is not None and progress["status"] == "STOPPED"
    assert progress["heartbeat_failed"] is True and len(world_b.started) == 1


def test_a_progress_record_taken_over_by_another_driver_is_never_overwritten(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    world = FakeWorld()
    original = world.start_process

    def hijack(argv: Sequence[str], log: Path, env: Mapping[str, str]) -> FakeProcess:
        process = original(argv, log, env)
        payload = json.loads(config.progress_path.read_text(encoding="utf-8"))
        payload["coordinator_id"] = "someone-else"
        config.progress_path.write_text(json.dumps(payload), encoding="utf-8")
        return process

    driver = CandidateValidationDriver(
        config,
        run_command=world.run_command,
        start_process=hijack,
        sleep=world.sleep,
        monotonic=world.monotonic,
    )
    with pytest.raises(ValidationDriverError) as raised:
        driver.run()
    assert raised.value.code == "VALIDATION_PROGRESS_NOT_OWNED"
    final: dict[str, Any] | None = read_progress(config.progress_path)
    assert final is not None and final["coordinator_id"] == "someone-else"
