"""DEVX-031: ship.py --publish with a failing ``git push``.

Owner decision owner_decision:DEVX-031:2026-10-14:parallel_pi_lanes.

S4a's publish lost its SSH connection at the push after local main had already moved to the gated
commit. The push is retried; when it still fails and origin did not move, local main goes back to
its old base so the same publish command can be run again. Throw-away git repositories.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

from tools.gov008 import ship

POLICY = {
    "schema_version": "gov008_ship_policy.v1",
    "tree_clean_exclusions": [],
    "zones": {"C": ["secret/**"], "A": ["docs/**", "*.md"]},
    "gates": {"A": [], "B": [], "C": []},
}


def run_git(repo: Path, *args: str) -> str:
    done = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    return done.stdout.strip()


def commit_file(repo: Path, rel: str, text: str, message: str) -> None:
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    run_git(repo, "add", "-A")
    run_git(repo, "commit", "-q", "-m", message)


@pytest.fixture()
def repo(tmp_path: Path) -> Path:
    run_git(tmp_path, "init", "-q", "-b", "main")
    run_git(tmp_path, "config", "user.email", "t@example.invalid")
    run_git(tmp_path, "config", "user.name", "t")
    run_git(tmp_path, "config", "core.autocrlf", "false")
    commit_file(tmp_path, ship.POLICY_PATH, yaml.safe_dump(POLICY), "policy")
    commit_file(tmp_path, "README.md", "start\n", "init")
    run_git(tmp_path, "switch", "-c", "task/x")
    return tmp_path


def _with_origin(repo: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    origin = tmp_path_factory.mktemp("origin") / "origin.git"
    run_git(origin.parent, "init", "--bare", "-b", "main", str(origin))
    run_git(repo, "remote", "add", "origin", str(origin))
    run_git(repo, "push", "origin", "main")
    return origin


class FlakyPush:
    """Wraps ship.git: the first ``fail`` pushes raise like a dropped SSH connection."""

    def __init__(
        self, monkeypatch: pytest.MonkeyPatch, fail: int, *, before: str = "", fetch: bool = True
    ) -> None:
        self.real = ship.git
        self.pushes = 0
        self.fail = fail
        self.before = before  # runs before the failing push, e.g. someone else pushing first
        self.fetch_works = fetch
        monkeypatch.setattr(ship, "git", self)
        monkeypatch.setattr(ship, "PUSH_RETRY_DELAYS", (0.0, 0.0))

    def __call__(self, repo: Path, *args: str, **kwargs: object) -> str:
        if args[:1] == ("push",):
            self.pushes += 1
            if self.pushes <= self.fail:
                if self.before:
                    run_git(Path(self.before), "push", "origin", "main")
                raise ship.ShipError("git push origin main failed: Connection closed by remote")
        if args[:1] == ("fetch",) and self.pushes and not self.fetch_works:
            raise ship.ShipError("git fetch origin main failed: Connection closed by remote")
        return self.real(repo, *args, **kwargs)  # type: ignore[arg-type]


def _gated_branch(repo: Path, tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, str]:
    origin = _with_origin(repo, tmp_path_factory)
    before = run_git(repo, "rev-parse", "main")
    commit_file(repo, "docs/a.md", "x\n", "docs: add a")
    ship.gate(repo, "main")
    return origin, before


def test_publish_push_retries_a_dropped_connection(
    repo: Path, tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    origin, _ = _gated_branch(repo, tmp_path_factory)
    flaky = FlakyPush(monkeypatch, fail=2)
    done = ship.publish(repo, "main", push=True)
    assert flaky.pushes == 3
    assert run_git(origin, "rev-parse", "main") == done["shipped_sha"]


def test_publish_push_failure_puts_local_main_back_when_origin_did_not_move(
    repo: Path, tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    origin, before = _gated_branch(repo, tmp_path_factory)
    flaky = FlakyPush(monkeypatch, fail=99)
    with pytest.raises(ship.ShipError, match="run the same publish command again"):
        ship.publish(repo, "main", push=True)
    assert flaky.pushes == 3
    assert run_git(repo, "rev-parse", "main") == before == run_git(origin, "rev-parse", "main")
    flaky.fail = 0
    done = ship.publish(repo, "main", push=True)
    assert run_git(origin, "rev-parse", "main") == done["shipped_sha"]


def test_publish_push_that_arrived_despite_the_error_counts_as_pushed(
    repo: Path, tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    origin, _ = _gated_branch(repo, tmp_path_factory)
    real_push = ship.git

    def lost_reply(repo_: Path, *args: str, **kwargs: object) -> str:
        if args[:1] == ("push",):
            real_push(repo_, *args)  # type: ignore[arg-type]
            raise ship.ShipError("git push origin main failed: reply lost")
        return real_push(repo_, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(ship, "git", lost_reply)
    monkeypatch.setattr(ship, "PUSH_RETRY_DELAYS", (0.0, 0.0))
    done = ship.publish(repo, "main", push=True)
    assert run_git(origin, "rev-parse", "main") == done["shipped_sha"]


def test_publish_push_failure_leaves_main_alone_when_origin_cannot_be_checked(
    repo: Path, tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    origin, before = _gated_branch(repo, tmp_path_factory)
    FlakyPush(monkeypatch, fail=99, fetch=False)
    with pytest.raises(ship.ShipError, match="origin could not be checked"):
        ship.publish(repo, "main", push=True)
    assert run_git(repo, "rev-parse", "main") != before
    assert run_git(origin, "rev-parse", "main") == before


def test_publish_push_failure_leaves_main_alone_when_origin_moved_meanwhile(
    repo: Path, tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    origin, before = _gated_branch(repo, tmp_path_factory)
    other = tmp_path_factory.mktemp("other-moved") / "clone"
    run_git(other.parent, "clone", str(origin), str(other))
    run_git(other, "config", "user.email", "o@example.invalid")
    run_git(other, "config", "user.name", "o")
    commit_file(other, "docs/elsewhere.md", "y\n", "someone else")
    FlakyPush(monkeypatch, fail=99, before=str(other))
    with pytest.raises(ship.ShipError, match="moved to"):
        ship.publish(repo, "main", push=True)
    assert run_git(repo, "rev-parse", "main") != before
    assert run_git(origin, "rev-parse", "main") != before
