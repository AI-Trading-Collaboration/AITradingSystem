# ruff: noqa: E501
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "agent"))
import brief  # noqa: E402


def make_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-b", "main"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "t@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Tester"], cwd=repo, check=True)
    (repo / "docs/requirements").mkdir(parents=True)
    (repo / "docs/requirements/REQ.md").write_text("owner_decision:DEVX-1:2026-10-11:ok\n", encoding="utf-8")
    (repo / "x.txt").write_text("x\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=repo, check=True)
    return repo


def text(**overrides: object) -> str:
    data = {
        "task": "DEVX-1",
        "run": "DEVX-1-01",
        "template": "mechanical-change",
        "zone": "B",
        "model": "local",
        "thinking": "default",
        "tools": "default",
        "owner_decision": "none",
        "allow_done": "false",
        "ship": "publish",
        "allowed_paths": ["scripts/agent/**"],
        "read_paths": ["README.md"],
    }
    data.update(overrides)
    lines = ["---"]
    for key, value in data.items():
        if isinstance(value, list):
            if value:
                lines.append(f"{key}:")
                lines.extend(f"  - {item}" for item in value)
            else:
                lines.append(f"{key}: []")
        else:
            lines.append(f"{key}: {value}")
    lines.append("---")
    lines.append("# body")
    return "\n".join(lines) + "\n"


def test_show_valid_brief_outputs_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo = make_repo(tmp_path)
    p = tmp_path / "brief.md"
    p.write_text(text(), encoding="utf-8")
    assert brief.show(p, repo) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["allow_done"] is False
    assert data["allowed_paths"] == ["scripts/agent/**"]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("zone", "D", "zone must"),
        ("model", "remote", "model must"),
        ("tools", "read,bash", "tools must"),
        ("ship", "execute", "ship must"),
        ("allow_done", "maybe", "allow_done must"),
        ("allowed_paths", [], "allowed_paths must"),
    ],
)
def test_validation_failures(tmp_path: Path, capsys: pytest.CaptureFixture[str], field: str, value: object, message: str) -> None:
    repo = make_repo(tmp_path)
    p = tmp_path / "brief.md"
    p.write_text(text(**{field: value}), encoding="utf-8")
    assert brief.show(p, repo) == 2
    assert message in capsys.readouterr().err


@pytest.mark.parametrize("thinking", ["high", "xhigh"])
def test_thinking_high_and_xhigh_are_valid(tmp_path: Path, thinking: str) -> None:
    repo = make_repo(tmp_path)
    assert brief.validate(brief.parse_text(text(thinking=thinking)), repo) == []


def test_invalid_thinking_is_rejected(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo = make_repo(tmp_path)
    p = tmp_path / "brief.md"
    p.write_text(text(thinking="turbo"), encoding="utf-8")
    assert brief.show(p, repo) == 2
    assert "thinking must be one of" in capsys.readouterr().err


def test_required_fields_duplicate_keys_and_run_prefix(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    data = brief.parse_text(text(task=""))
    assert any("task is required" in e for e in brief.validate(data, repo))
    data = brief.parse_text(text(run="OTHER-1"))
    assert any("run must start" in e for e in brief.validate(data, repo))
    duplicated = text().replace("run: DEVX-1-01", "run: DEVX-1-01\ntask: AGAIN")
    with pytest.raises(brief.BriefError) as exc:
        brief.parse_text(duplicated)
    assert "duplicate field" in str(exc.value)


def test_readonly_may_have_empty_allowed_paths(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    data = brief.parse_text(text(tools="readonly", allowed_paths=[], ship="none"))
    assert brief.validate(data, repo) == []


def test_zone_c_owner_decision_must_be_committed_to_main_not_worktree(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    (repo / "docs/requirements/REQ.md").write_text("owner_decision:DEVX-1:2026-10-11:worktree\n", encoding="utf-8")
    data = brief.parse_text(text(zone="C", owner_decision="owner_decision:DEVX-1:2026-10-11:worktree"))
    assert any("not found" in e for e in brief.validate(data, repo))


def test_zone_c_owner_decision_must_be_on_main(tmp_path: Path) -> None:
    repo = make_repo(tmp_path)
    good = brief.parse_text(text(zone="C", owner_decision="owner_decision:DEVX-1:2026-10-11:ok"))
    assert brief.validate(good, repo) == []
    bad_format = brief.parse_text(text(zone="C", owner_decision="owner_decision:DEVX-1:bad"))
    assert any("zone C requires" in e for e in brief.validate(bad_format, repo))
    missing = brief.parse_text(text(zone="C", owner_decision="owner_decision:DEVX-1:2026-10-11:missing"))
    assert any("not found" in e for e in brief.validate(missing, repo))
