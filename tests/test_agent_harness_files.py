"""Harness-neutral agent files stay loadable: .agents/skills and the pi guard extension."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SKILLS_ROOT = ROOT / ".agents" / "skills"
GUARD_TEST = ROOT / ".pi" / "extensions" / "aits-guard" / "guard.test.ts"
# Agent Skills naming rule (shared by pi, Codex and Claude Code): lowercase letters, digits and
# single hyphens, at most 64 characters; descriptions at most 1024 characters.
SKILL_NAME = re.compile(r"[a-z0-9]+(-[a-z0-9]+)*")
MAX_NAME = 64
MAX_DESCRIPTION = 1024
# Node strips TypeScript types without flags from 23.6 on.
MIN_NODE = (23, 6)


def _skill_files() -> list[Path]:
    return sorted(SKILLS_ROOT.glob("*/SKILL.md"))


def test_skills_exist() -> None:
    assert {path.parent.name for path in _skill_files()} >= {
        "ship-change",
        "task-records",
        "periodic-operations",
        "research-run",
    }


@pytest.mark.parametrize("path", _skill_files(), ids=lambda path: path.parent.name)
def test_skill_frontmatter_is_valid(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    match = re.match(r"---\r?\n(.*?)\r?\n---\r?\n", text, re.S)
    assert match, "SKILL.md must start with YAML frontmatter"
    meta = yaml.safe_load(match.group(1))
    name = meta.get("name")
    description = meta.get("description")
    assert isinstance(name, str) and SKILL_NAME.fullmatch(name) and len(name) <= MAX_NAME
    assert name == path.parent.name
    assert isinstance(description, str) and 0 < len(description) <= MAX_DESCRIPTION
    assert text[match.end() :].strip(), "skill body is empty"


def _node_version(node: str) -> tuple[int, int] | None:
    done = subprocess.run([node, "--version"], capture_output=True, text=True, timeout=30)
    match = re.match(r"v(\d+)\.(\d+)", done.stdout.strip())
    return (int(match.group(1)), int(match.group(2))) if match else None


def test_pi_guard_extension_node_tests_pass() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed; the pi guard extension tests need Node >= 23.6")
    version = _node_version(node)
    if version is None or version < MIN_NODE:
        pytest.skip(f"node {version} cannot run TypeScript directly; need >= {MIN_NODE}")
    done = subprocess.run(
        [node, "--test", str(GUARD_TEST)],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=ROOT,
    )
    assert done.returncode == 0, done.stdout + done.stderr
