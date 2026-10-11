# ruff: noqa: E501
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PS1 = [
    ROOT / "scripts/agent/agent-run.ps1",
    ROOT / "scripts/agent/run-brief.ps1",
    ROOT / "scripts/agent/setup-agent-account.ps1",
]


def powershell() -> str:
    exe = shutil.which("powershell.exe") or shutil.which("powershell")
    if exe is None:
        raise pytest.skip.Exception(
            "powershell.exe not found; PowerShell syntax tests require Windows PowerShell"
        )
    return exe


@pytest.mark.parametrize("path", PS1, ids=lambda p: p.name)
def test_powershell_scripts_parse(path: Path) -> None:
    ps = powershell()
    cmd = (
        "$errors=$null;"
        f"[System.Management.Automation.Language.Parser]::ParseFile('{path}', [ref]$null, [ref]$errors) > $null;"
        "if ($errors.Count -gt 0) { $errors | ForEach-Object { Write-Error $_ }; exit 1 }"
    )
    done = subprocess.run([ps, "-NoProfile", "-Command", cmd], text=True, capture_output=True, timeout=60)
    assert done.returncode == 0, done.stdout + done.stderr


def write_brief(path: Path) -> None:
    path.write_text(
        """---
task: DEVX-1
run: DEVX-1-01
template: mechanical-change
zone: B
model: local
thinking: default
tools: default
owner_decision: none
allow_done: false
ship: publish
allowed_paths:
  - scripts/agent/**
read_paths: []
---
# DEVX-1

## 等价检查

不适用。
""",
        encoding="utf-8",
    )


def test_run_brief_dry_run_prints_command_env_and_tools(tmp_path: Path) -> None:
    ps = powershell()
    brief = tmp_path / "brief.md"
    write_brief(brief)
    done = subprocess.run(
        [
            ps,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ROOT / "scripts/agent/run-brief.ps1"),
            str(brief),
            "-Repo",
            str(ROOT),
            "-DryRun",
            "-Direct",
            "-Now",
            "2026-10-11T12:00:00+09:00",
        ],
        text=True,
        capture_output=True,
        timeout=60,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    assert "pi -p --mode json" in done.stdout
    assert "--tools read,bash,edit,write" in done.stdout
    assert "AITS_PI_RUN_DIR" in done.stdout
    assert "AITS_PI_ALLOWED_PATHS=scripts/agent/**" in done.stdout


def test_run_brief_refuses_daily_window(tmp_path: Path) -> None:
    ps = powershell()
    brief = tmp_path / "brief.md"
    write_brief(brief)
    done = subprocess.run(
        [
            ps,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ROOT / "scripts/agent/run-brief.ps1"),
            str(brief),
            "-Repo",
            str(ROOT),
            "-DryRun",
            "-Now",
            "2026-10-11T09:30:00+09:00",
        ],
        text=True,
        capture_output=True,
        timeout=60,
    )
    assert done.returncode == 2
    assert "日报窗口" in done.stdout
