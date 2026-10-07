"""DEVX-016 S3: the plan/run/status commands derive scope, keep a journal and stop at the gate."""

from __future__ import annotations

import json
import shutil
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from publication_run_support import (
    T0,
    FakeCollector,
    FakeLauncher,
    World,
)

from ai_trading_system.platform.architecture.publication_checks import (
    AUTHORIZATION_SCHEMA_VERSION,
    AUTHORIZATION_SCOPE,
)
from ai_trading_system.platform.architecture.publication_cli import (
    EXIT_AWAITING_AUTHORIZATION,
    EXIT_COMPLETE,
    EXIT_FAILED,
    CliEnvironment,
    build_parser,
    main,
)

ROOT = Path(__file__).resolve().parents[1]
SCOPE_POLICY = """\
schema_version: devx_016_publication_scope_policy.v1
policy_id: TEST-SCOPE
version: 1.0.0
status: OWNER_APPROVED_ENFORCED
owner: Project Owner
approval_ref: owner_decision:test
rationale: test fixture
review_condition: when the fence policy changes
shared_paths:
  - docs/system_flow.md
  - inputs/architecture
  - registry/architecture_compatibility_authority
allowed_owned_prefixes:
  - src/
  - tests/
  - docs/requirements/
forbidden_paths:
  - AGENTS.md
  - outputs/
resource_paths:
  - outputs/architecture/integration_revalidation
limits:
  max_hours_since_full_end: 2.0
  min_free_disk_gb: 100
  max_generator_rounds: 3
"""


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()


class Fixture:
    def __init__(self, tmp_path: Path, *, candidate_files: Sequence[str]) -> None:
        self.world = World(tmp_path)
        repo = self.world.repo
        for name in (
            "arch_005_integration_publication_fence.yaml",
            "arch_005_s4d_checkout_guard.yaml",
        ):
            target = repo / "config" / "architecture" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(ROOT / "config" / "architecture" / name, target)
        scope = repo / "config" / "architecture" / "devx_016_publication_scope.v1.yaml"
        scope.write_text(SCOPE_POLICY, encoding="utf-8", newline="\n")
        (repo / ".gitignore").write_text("outputs/\nev/\n", encoding="utf-8")
        _git(repo, "init", "-b", "main")
        _git(repo, "config", "user.email", "cli@example.com")
        _git(repo, "config", "user.name", "CLI Test")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "base")
        self.base = _git(repo, "rev-parse", "HEAD")
        _git(repo, "switch", "-c", "lane")
        for name in candidate_files:
            path = repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("x\n", encoding="utf-8")
        _git(repo, "add", "-A")
        _git(repo, "commit", "-m", "candidate")
        self.head = _git(repo, "rev-parse", "HEAD")
        self.world.main = self.world.origin_main = self.world.remote_tip = self.base
        self.world.head = self.world.lane_tip = self.head
        self.launcher = FakeLauncher(self.world)
        self.collector = FakeCollector(self.world)

    def environment(self) -> CliEnvironment:
        world = self.world

        def git(args: Sequence[str]) -> str:
            return _git(world.repo, *args)

        return CliEnvironment(
            repository_root=world.repo,
            python="py",
            make_runner=lambda evidence: world,
            launcher=self.launcher,
            collector=self.collector,
            interpreter=lambda: (str(world.repo / ".venv" / "Scripts" / "python.exe"), (3, 11)),
            free_disk_gb=lambda: 500.0,
            sleep=lambda seconds: None,
            monotonic=lambda: 0.0,
            now=lambda: T0,
            git=git,
        )

    def invoke(self, capsys: pytest.CaptureFixture[str], *args: str) -> tuple[int, dict[str, Any]]:
        code = main(list(args), environment=self.environment())
        return code, json.loads(capsys.readouterr().out)

    def authorize(self) -> None:
        body = {
            "schema_version": AUTHORIZATION_SCHEMA_VERSION,
            "scope": AUTHORIZATION_SCOPE,
            "candidate_sha": self.head,
            "expected_main": self.base,
            "force_push": False,
            "pull_request": False,
            "authorized_by": "project_owner",
            "authorized_at": T0.isoformat(),
            "source": "test",
        }
        path = self.world.repo / "outputs" / "architecture" / "publication_runs" / "r1"
        path.mkdir(parents=True, exist_ok=True)
        (path / "authorization.json").write_text(json.dumps(body), encoding="utf-8")


FILES = ("src/pkg/a.py", "tests/test_a.py", "docs/system_flow.md")


def test_plan_derives_the_scope_from_the_candidate_diff(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fx = Fixture(tmp_path, candidate_files=FILES)
    code, plan = fx.invoke(capsys, "plan", "--expected-main", fx.base)
    assert code == EXIT_COMPLETE
    scope = plan["scope"]
    assert scope["owned_paths"] == ["src/pkg/a.py", "tests/test_a.py"]
    assert scope["shared_diff_paths"] == ["docs/system_flow.md"]
    assert scope["generator_ids"][0] == "canonical-task-source" and len(scope["generator_ids"]) == 5
    assert scope["required_validation_tiers"][-1] == "full"
    assert plan["production_effect"] == "none"


def test_plan_refuses_a_candidate_that_touches_a_forbidden_path_before_any_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fx = Fixture(tmp_path, candidate_files=(*FILES, "AGENTS.md"))
    code, body = fx.invoke(capsys, "plan")
    assert code == EXIT_FAILED and body["status"] == "REFUSED"
    assert body["code"] == "PUBLICATION_SCOPE_PATH_FORBIDDEN" and "AGENTS.md" in body["message"]


def test_run_stops_at_the_gate_then_resumes_to_completion_after_authorization(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fx = Fixture(tmp_path, candidate_files=FILES)
    code, summary = fx.invoke(
        capsys, "run", "--run-id", "r1", "--parent-run", "p.json", "--push-by-command"
    )
    assert code == EXIT_AWAITING_AUTHORIZATION
    assert summary["outcome"] == "AWAITING_AUTHORIZATION"
    assert summary["stopped_step"] == "E58.authorization_gate"
    run_dir = fx.world.repo / "outputs" / "architecture" / "publication_runs" / "r1"
    record = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
    assert record["expected_main"] == fx.base and record["start_head"] == fx.head
    assert record["scope"]["owned_paths"] == ["src/pkg/a.py", "tests/test_a.py"]
    assert not (run_dir / "run.lock").exists()  # released
    assert fx.world.pushes == []

    code, status = fx.invoke(capsys, "status", "--run-id", "r1")
    assert (
        code == EXIT_AWAITING_AUTHORIZATION and status["stopped_step"] == "E58.authorization_gate"
    )

    fx.authorize()
    code, summary = fx.invoke(capsys, "resume", "--run-id", "r1", "--push-by-command")
    assert code == EXIT_COMPLETE and summary["outcome"] == "COMPLETE"
    assert fx.world.pushes == [("git", "push", "origin", "main")]
    assert summary["slow_steps"] == []


def test_by_default_the_owner_pushes_in_their_terminal_and_resume_proves_it(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fx = Fixture(tmp_path, candidate_files=FILES)
    code, summary = fx.invoke(capsys, "run", "--run-id", "r1", "--parent-run", "p.json")
    assert code == EXIT_AWAITING_AUTHORIZATION and summary["stopped_step"] == "E59.push"
    assert "git push origin main" in summary["next_action"] and fx.world.pushes == []
    fx.authorize()  # a record alone never makes the command push without the opt-in flag
    code, summary = fx.invoke(capsys, "resume", "--run-id", "r1")
    assert code == EXIT_AWAITING_AUTHORIZATION and fx.world.pushes == []
    fx.world.remote_tip = fx.head  # the owner pushed
    code, summary = fx.invoke(capsys, "resume", "--run-id", "r1")
    assert code == EXIT_COMPLETE and summary["outcome"] == "COMPLETE"
    assert fx.world.pushes == []


def test_the_push_flag_is_an_explicit_opt_in() -> None:
    parser = build_parser()
    assert parser.parse_args(["run", "--run-id", "r"]).push_by_command is False
    assert parser.parse_args(["resume", "--run-id", "r"]).push_by_command is False
    assert parser.parse_args(["run", "--run-id", "r", "--push-by-command"]).push_by_command


def test_a_resume_with_a_different_parent_run_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fx = Fixture(tmp_path, candidate_files=FILES)
    fx.invoke(capsys, "run", "--run-id", "r1", "--parent-run", "p.json")
    code, body = fx.invoke(capsys, "resume", "--run-id", "r1", "--parent-run", "other.json")
    assert code == EXIT_FAILED and body["code"] == "PUBLICATION_RUN_RECORD_CONFLICT"
    assert "parent_run" in body["message"]


def test_a_leftover_lock_blocks_the_run_until_it_is_explicitly_taken_over(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fx = Fixture(tmp_path, candidate_files=FILES)
    fx.invoke(capsys, "run", "--run-id", "r1", "--parent-run", "p.json")
    run_dir = fx.world.repo / "outputs" / "architecture" / "publication_runs" / "r1"
    (run_dir / "run.lock").write_text(
        json.dumps({"acquired_at": "2026-10-07T00:00:00+00:00"}), encoding="utf-8"
    )
    code, body = fx.invoke(capsys, "resume", "--run-id", "r1")
    assert code == EXIT_FAILED and body["code"] == "PUBLICATION_RUN_LOCKED"
    code, summary = fx.invoke(capsys, "resume", "--run-id", "r1", "--takeover")
    assert code == EXIT_AWAITING_AUTHORIZATION and not (run_dir / "run.lock").exists()


@pytest.mark.parametrize("run_id", ["", "has space", "../escape", ".hidden", "a:b"])
def test_run_ids_cannot_escape_the_runs_directory(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], run_id: str
) -> None:
    fx = Fixture(tmp_path, candidate_files=FILES)
    code = main(["status", "--run-id", run_id], environment=fx.environment())
    body = json.loads(capsys.readouterr().out)
    assert code == EXIT_FAILED and body["code"] == "PUBLICATION_RUN_ID_INVALID"


def test_until_pauses_the_run_after_the_named_step(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fx = Fixture(tmp_path, candidate_files=FILES)
    code, summary = fx.invoke(
        capsys, "run", "--run-id", "r1", "--parent-run", "p.json", "--until", "C26.readiness"
    )
    assert (
        summary["outcome"] == "PAUSED_AT_REQUESTED_STEP"
        and summary["stopped_step"] == "C26.readiness"
    )
    assert not any("local-publish" in c for c in fx.launcher.launched)


def test_plan_can_inspect_a_proposed_policy_but_run_never_accepts_it(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fx = Fixture(tmp_path, candidate_files=FILES)
    policy = fx.world.repo / "config" / "architecture" / "devx_016_publication_scope.v1.yaml"
    policy.write_text(
        SCOPE_POLICY.replace("OWNER_APPROVED_ENFORCED", "PROPOSED_PENDING_OWNER_REVIEW"),
        encoding="utf-8",
        newline="\n",
    )
    code, body = fx.invoke(capsys, "plan")
    assert code == EXIT_FAILED and body["code"] == "PUBLICATION_SCOPE_POLICY_STATUS"
    code, plan = fx.invoke(capsys, "plan", "--allow-proposed-policy")
    assert code == EXIT_COMPLETE and plan["policy_status"] == "PROPOSED_PENDING_OWNER_REVIEW"
    assert plan["scope"]["owned_paths"] == ["src/pkg/a.py", "tests/test_a.py"]
    code, body = fx.invoke(capsys, "run", "--run-id", "r1", "--parent-run", "p.json")
    assert code == EXIT_FAILED and body["code"] == "PUBLICATION_SCOPE_POLICY_STATUS"
    assert not (fx.world.repo / "outputs" / "architecture" / "publication_runs" / "r1").exists()


def test_a_commit_that_adds_an_owned_path_during_the_run_is_caught_before_the_formal_acquire(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fx = Fixture(tmp_path, candidate_files=FILES)
    code, summary = fx.invoke(
        capsys, "run", "--run-id", "r1", "--parent-run", "p.json", "--until", "B15.prep_release"
    )
    assert summary["outcome"] == "PAUSED_AT_REQUESTED_STEP"
    repo = fx.world.repo
    (repo / "src" / "pkg" / "b.py").write_text("x\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "late addition")
    code, summary = fx.invoke(capsys, "resume", "--run-id", "r1")
    assert code == EXIT_FAILED and summary["stopped_step"] == "C20.formal_acquire"
    assert fx.world.named("acquire", "r1-formal") == []
