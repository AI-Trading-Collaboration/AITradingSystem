# ruff: noqa: E501, E702
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PS1 = [ROOT / "scripts/agent/agent-run.ps1", ROOT / "scripts/agent/run-brief.ps1", ROOT / "scripts/agent/setup-agent-account.ps1"]


def powershell() -> str:
    exe = shutil.which("powershell.exe") or shutil.which("powershell")
    if exe is None:
        raise pytest.skip.Exception("powershell.exe not found; PowerShell tests require Windows PowerShell")
    return exe


@pytest.mark.parametrize("path", PS1, ids=lambda p: p.name)
def test_powershell_scripts_parse(path: Path) -> None:
    ps = powershell()
    cmd = "$errors=$null;" f"[System.Management.Automation.Language.Parser]::ParseFile('{path}', [ref]$null, [ref]$errors) > $null;" "if ($errors.Count -gt 0) { $errors | ForEach-Object { Write-Error $_ }; exit 1 }"
    done = subprocess.run([ps, "-NoProfile", "-Command", cmd], text=True, capture_output=True, timeout=60)
    assert done.returncode == 0, done.stdout + done.stderr


def write_brief(path: Path, *, valid: bool = True) -> None:
    path.write_text(
        f"""---
task: DEVX-1
run: {'DEVX-1-01' if valid else 'OTHER-01'}
template: mechanical-change
zone: {'B' if valid else 'D'}
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


def write_config(path: Path, runs_root: Path) -> None:
    path.write_text(json.dumps({"runs_root": str(runs_root), "models": {"local": "local/model", "cloud": "openai/gpt-5.5"}, "daily_windows_jst": [{"start": "09:20", "end": "10:30"}], "scheduled_task_name": "nope", "agent_clone": str(ROOT), "queue_file": str(runs_root / "queue/request.json")}), encoding="utf-8")


def run_brief(ps: str, brief: Path, config: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/run-brief.ps1"), str(brief), "-Repo", str(ROOT), "-ConfigPath", str(config), *extra], text=True, capture_output=True, timeout=60)


def test_run_brief_dry_run_prints_command_env_and_tools(tmp_path: Path) -> None:
    ps = powershell(); brief = tmp_path / "brief.md"; config = tmp_path / "config.json"
    write_brief(brief); write_config(config, tmp_path / "runs")
    done = run_brief(ps, brief, config, "-DryRun", "-Direct", "-Now", "2026-10-11T12:00:00+09:00")
    assert done.returncode == 0, done.stdout + done.stderr
    assert "pi -p @" in done.stdout and "--mode json" in done.stdout
    assert "--tools read,bash,edit,write" in done.stdout
    assert "AITS_PI_RUN_DIR" in done.stdout and "AITS_PI_ALLOWED_PATHS=scripts/agent/**" in done.stdout


def test_run_brief_refuses_daily_window(tmp_path: Path) -> None:
    ps = powershell(); brief = tmp_path / "brief.md"; config = tmp_path / "config.json"
    write_brief(brief); write_config(config, tmp_path / "runs")
    done = run_brief(ps, brief, config, "-DryRun", "-Now", "2026-10-11T09:30:00+09:00")
    assert done.returncode == 2 and "日报窗口" in done.stdout


def test_run_brief_invalid_brief_prints_all_errors(tmp_path: Path) -> None:
    ps = powershell(); brief = tmp_path / "brief.md"; config = tmp_path / "config.json"
    write_brief(brief, valid=False); write_config(config, tmp_path / "runs")
    done = run_brief(ps, brief, config, "-DryRun", "-Now", "2026-10-11T12:00:00+09:00")
    assert done.returncode == 2
    assert "zone must" in done.stdout and "run must start" in done.stdout


def test_run_brief_fix_model_upgrade_and_existing_attempts(tmp_path: Path) -> None:
    ps = powershell(); brief = tmp_path / "brief.md"; fix = tmp_path / "fix 02.md"; config = tmp_path / "config.json"; runs = tmp_path / "runs"
    write_brief(brief); fix.write_text("fix", encoding="utf-8"); write_config(config, runs)
    run_dir = runs / "DEVX-1-01"; (run_dir / "attempts/01").mkdir(parents=True); (run_dir / "session").mkdir()
    (run_dir / "session/s.jsonl").write_text("", encoding="utf-8")
    (run_dir / "attempts/01/exit.json").write_text(json.dumps({"stopReason": "error", "exit_code": 0}), encoding="utf-8")
    (run_dir / "acceptance.json").write_text(json.dumps({"reviews": [{"verdict": "rejected"}, {"verdict": "rejected"}]}), encoding="utf-8")
    done = run_brief(ps, brief, config, "-DryRun", "-Now", "2026-10-11T12:00:00+09:00")
    assert done.returncode == 2 and "已有 attempts" in done.stdout
    done = run_brief(ps, brief, config, "-Fix", str(fix), "-DryRun", "-Now", "2026-10-11T12:00:00+09:00")
    assert done.returncode == 0, done.stdout + done.stderr
    assert "openai/gpt-5.5" in done.stdout and "--session" in done.stdout and "Attempt=02" in done.stdout


def write_fake_pi(path: Path) -> None:
    path.write_text(
        """import json, os, sys
cap = os.environ.get('FAKE_PI_CAPTURE')
if '--export' in sys.argv:
    if len(sys.argv) >= 4:
        open(sys.argv[-1], 'w', encoding='utf-8').write('<html></html>')
    sys.exit(0)
stdin_data = sys.stdin.read()
if cap:
    data = {'argv': sys.argv[1:], 'cwd': os.getcwd(), 'env': {k:v for k,v in os.environ.items() if k.startswith('AITS_PI_')}, 'stdin_empty': stdin_data in ('', '\ufeff'), 'stdin_repr': repr(stdin_data)}
    open(cap, 'w', encoding='utf-8').write(json.dumps(data, ensure_ascii=False))
stop = os.environ.get('FAKE_PI_STOP', 'stop')
print(json.dumps({'type':'message_end','message':{'role':'assistant','stopReason':stop,'timestamp':'2026-10-11T00:00:00Z','provider':'fake','model':'fake','usage':{'cost':{'total':0}},'content':'ok'}}))
print(json.dumps({'type':'agent_settled','aborted':False}))
sys.exit(0)
""",
        encoding="utf-8",
    )


def test_agent_run_executes_fake_pi_direct_and_request_modes(tmp_path: Path) -> None:
    ps = powershell(); fake = tmp_path / "fake_pi.py"; write_fake_pi(fake)
    prompt = tmp_path / "提示 文件.md"; prompt.write_text("hello", encoding="utf-8")
    run_dir = tmp_path / "run"; capture = tmp_path / "cap.json"
    env = os.environ.copy(); env["FAKE_PI_CAPTURE"] = str(capture); env["PYTHON"] = sys.executable; env["AITS_PI_OWNER_DECISION"] = "leak"
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/agent-run.ps1"), "-Repo", str(ROOT), "-RunDir", str(run_dir), "-Attempt", "01", "-PromptFile", str(prompt), "-ModelId", "fake/model", "-PiCommand", str(fake)], text=True, capture_output=True, timeout=60, env=env)
    assert done.returncode == 0, done.stdout + done.stderr
    assert (run_dir / "attempts/01/events.jsonl").read_text(encoding="utf-8").strip()
    raw = (run_dir / "attempts/01/exit.json").read_bytes(); assert not raw.startswith(b"\xef\xbb\xbf")
    assert json.loads(raw.decode("utf-8"))["stopReason"] == "stop"
    cap = json.loads(capture.read_text(encoding="utf-8"))
    assert f"@{prompt}" in cap["argv"] and "--session-dir" in cap["argv"]
    assert "AITS_PI_OWNER_DECISION" not in cap["env"] and cap["stdin_empty"] is True

    req = tmp_path / "request.json"; run2 = tmp_path / "run2"; capture2 = tmp_path / "cap2.json"; prompt2 = tmp_path / "p.md"; prompt2.write_text("x", encoding="utf-8")
    req.write_text(json.dumps({"repo": str(ROOT), "run_dir": str(run2), "attempt": "01", "purpose": "run", "prompt_file": str(prompt2), "model_id": "fake/model", "tools": "default", "session_file": None, "aits_env": {"AITS_PI_RUN": "DEVX"}}), encoding="utf-8")
    env["FAKE_PI_CAPTURE"] = str(capture2)
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/agent-run.ps1"), "-RequestFile", str(req), "-PiCommand", str(fake)], text=True, capture_output=True, timeout=60, env=env)
    assert done.returncode == 0, done.stdout + done.stderr
    assert not req.exists() and (run2 / "attempts/01/request.json").exists()


def test_agent_run_model_error_returns_one(tmp_path: Path) -> None:
    ps = powershell(); fake = tmp_path / "fake_pi.py"; write_fake_pi(fake)
    prompt = tmp_path / "p.md"; prompt.write_text("hello", encoding="utf-8")
    env = os.environ.copy(); env["FAKE_PI_STOP"] = "error"; env["PYTHON"] = sys.executable
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/agent-run.ps1"), "-Repo", str(ROOT), "-RunDir", str(tmp_path / "run"), "-Attempt", "01", "-PromptFile", str(prompt), "-ModelId", "fake/model", "-PiCommand", str(fake)], text=True, capture_output=True, timeout=60, env=env)
    assert done.returncode == 1
