"""DEVX-016 S3: stage A refuses to start a run on a host or tree that cannot finish it."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from publication_run_support import T0, World, make_run_config

from ai_trading_system.platform.architecture.publication_journal import PublicationJournal
from ai_trading_system.platform.architecture.publication_orchestrator import (
    OUTCOME_COMPLETE,
    OUTCOME_STOPPED_FAILED,
    PublicationRunEngine,
    StepContext,
)
from ai_trading_system.platform.architecture.publication_steps import build_precondition_steps


class Host:
    def __init__(self, world: World) -> None:
        self.venv = world.repo / ".venv" / "Scripts" / "python.exe"
        self.executable = str(self.venv)
        self.version = (3, 11)
        self.processes: tuple[str, ...] = ()
        self.free_gb = 500.0


def _engine(
    tmp_path: Path, world: World, host: Host, *, seal_rebuild: bool = False
) -> tuple[PublicationRunEngine, PublicationJournal]:
    config = make_run_config(world)
    steps = build_precondition_steps(
        config,
        world,
        live_processes=lambda: host.processes,
        interpreter=lambda: (host.executable, host.version),
        free_disk_gb=lambda: host.free_gb,
        min_free_disk_gb=100.0,
        seal_rebuild=seal_rebuild,
    )
    journal = PublicationJournal(tmp_path / "run" / "journal.jsonl")
    engine = PublicationRunEngine(
        steps=steps,
        journal=journal,
        context=StepContext(run_id="r1"),
        monotonic=lambda: 0.0,
        now=lambda: T0,
    )
    return engine, journal


def _failed(journal: PublicationJournal, step: str) -> dict[str, Any]:
    detail = journal.replay().last_detail(step, status="FAILED")
    assert detail is not None
    return dict(detail)


def test_a_healthy_host_and_tree_pass_every_precondition(tmp_path: Path) -> None:
    world = World(tmp_path)
    engine, journal = _engine(tmp_path, world, Host(world))
    summary = engine.run()
    assert summary.outcome == OUTCOME_COMPLETE, summary.to_dict()
    assert journal.replay().last_detail("A01.git_state")["branch"] == "lane"  # type: ignore[index]
    assert journal.replay().last_detail("A05.dependency_gate")["freshness_pending"] == 3  # type: ignore[index]


@pytest.mark.parametrize(
    "mutate,step,code",
    [
        (
            lambda w, h: setattr(h, "executable", "C:/Python314/python.exe"),
            "A00.interpreter",
            "PUBLICATION_RUN_INTERPRETER",
        ),
        (
            lambda w, h: setattr(h, "version", (3, 14)),
            "A00.interpreter",
            "PUBLICATION_RUN_INTERPRETER",
        ),
        (lambda w, h: setattr(w, "main", "b" * 40), "A01.git_state", "PUBLICATION_RUN_MAIN_MOVED"),
        (lambda w, h: setattr(w, "branch", "main"), "A01.git_state", "PUBLICATION_RUN_BRANCH"),
        (
            lambda w, h: setattr(w, "main_is_ancestor", False),
            "A01.git_state",
            "PUBLICATION_RUN_NOT_DESCENDED",
        ),
        (
            lambda w, h: setattr(w, "dirty", ["src/x.py"]),
            "A02.clean_tree",
            "PUBLICATION_RUN_TREE_NOT_CLEAN",
        ),
        (
            lambda w, h: setattr(h, "processes", ("9|1|python.exe|pytest",)),
            "A03.quiet_host",
            "PUBLICATION_RUN_PROCESSES_PRESENT",
        ),
        (lambda w, h: setattr(h, "free_gb", 12.0), "A04.disk_space", "PUBLICATION_RUN_DISK_LOW"),
    ],
)
def test_each_precondition_fails_closed_with_a_readable_code(
    tmp_path: Path, mutate: Any, step: str, code: str
) -> None:
    world = World(tmp_path)
    host = Host(world)
    mutate(world, host)
    engine, journal = _engine(tmp_path, world, host)
    summary = engine.run()
    assert summary.outcome == OUTCOME_STOPPED_FAILED and summary.stopped_step == step
    assert _failed(journal, step)["code"] == code
    assert world.named("acquire") == []  # nothing was acquired


def test_the_dependency_gate_finding_that_cost_a_prep_round_is_caught_before_acquire(
    tmp_path: Path,
) -> None:
    world = World(tmp_path)
    world.devex_body = {
        "dependency_gate": {"status": "FAIL", "violation_count": 1},
        "violation_count": 1,
        "violations": [
            {
                "rule_id": "NEW_DIRECT_ARTIFACT_WRITER_FORBIDDEN",
                "path": "src/ai_trading_system/platform/architecture/x.py",
            }
        ],
    }
    engine, journal = _engine(tmp_path, world, Host(world))
    summary = engine.run()
    assert summary.stopped_step == "A05.dependency_gate"
    failed = _failed(journal, "A05.dependency_gate")
    assert failed["code"] == "PUBLICATION_RUN_DEPENDENCY_GATE"
    assert failed["violations"][0]["rule_id"] == "NEW_DIRECT_ARTIFACT_WRITER_FORBIDDEN"


def test_only_the_three_freshness_findings_are_tolerated_before_generation(tmp_path: Path) -> None:
    world = World(tmp_path)
    world.devex_body["violations"].append({"rule_id": "module_orphan"})
    world.devex_body["violation_count"] = 4
    engine, journal = _engine(tmp_path, world, Host(world))
    summary = engine.run()
    assert summary.stopped_step == "A05.dependency_gate"
    assert _failed(journal, "A05.dependency_gate")["violations"] == [{"rule_id": "module_orphan"}]


def test_an_unreadable_validate_output_fails_instead_of_passing(tmp_path: Path) -> None:
    world = World(tmp_path)
    world.devex_output = "Traceback (most recent call last): ..."
    engine, journal = _engine(tmp_path, world, Host(world))
    summary = engine.run()
    assert summary.stopped_step == "A05.dependency_gate"
    assert _failed(journal, "A05.dependency_gate")["code"] == "PUBLICATION_RUN_VALIDATE_UNREADABLE"
