# ruff: noqa: E501, E702
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PS1 = [ROOT / "scripts/agent/agent-run.ps1", ROOT / "scripts/agent/run-brief.ps1", ROOT / "scripts/agent/setup-agent-account.ps1", ROOT / "scripts/agent/publish.ps1"]


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
stdin_bytes = sys.stdin.buffer.read()
if cap:
    data = {'argv': sys.argv[1:], 'cwd': os.getcwd(), 'env': {k:v for k,v in os.environ.items() if k.startswith('AITS_PI_')}, 'stdin_bytes': len(stdin_bytes)}
    open(cap, 'w', encoding='utf-8').write(json.dumps(data, ensure_ascii=False))
wait_for = os.environ.get('FAKE_PI_WAIT_FOR')
if wait_for:
    first = json.dumps({'type':'progress','message':'中文'}, ensure_ascii=False).encode('utf-8') + b'\\n'
    sys.stdout.buffer.write(first); sys.stdout.buffer.flush()
    sys.stderr.buffer.write(b'e' * 200000); sys.stderr.buffer.flush()
    import time
    deadline = time.time() + 60
    while time.time() < deadline and not os.path.exists(wait_for):
        time.sleep(0.05)
stop = os.environ.get('FAKE_PI_STOP', 'stop')
print(json.dumps({'type':'message_end','message':{'role':'assistant','stopReason':stop,'timestamp':'2026-10-11T00:00:00Z','provider':'fake','model':'fake','usage':{'cost':{'total':0}},'content':'ok'}}, ensure_ascii=False))
print(json.dumps({'type':'agent_settled','aborted':False}, ensure_ascii=False))
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
    assert "AITS_PI_OWNER_DECISION" not in cap["env"] and cap["stdin_bytes"] == 0

    req = tmp_path / "request.json"; run2 = tmp_path / "run2"; capture2 = tmp_path / "cap2.json"; prompt2 = tmp_path / "p.md"; prompt2.write_text("x", encoding="utf-8")
    req.write_text(json.dumps({"repo": str(ROOT), "run_dir": str(run2), "attempt": "01", "purpose": "run", "prompt_file": str(prompt2), "model_id": "fake/model", "tools": "default", "session_file": None, "aits_env": {"AITS_PI_RUN": "DEVX"}}), encoding="utf-8")
    env["FAKE_PI_CAPTURE"] = str(capture2)
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/agent-run.ps1"), "-RequestFile", str(req), "-PiCommand", str(fake)], text=True, capture_output=True, timeout=60, env=env)
    assert done.returncode == 0, done.stdout + done.stderr
    assert not req.exists() and (run2 / "attempts/01/request.json").exists()


def write_fake_pi_wrappers(tmp_path: Path) -> tuple[Path, Path]:
    (tmp_path / "pi.ps1").write_text(
        """#!/usr/bin/env pwsh
$basedir=Split-Path $MyInvocation.MyCommand.Definition -Parent
$launcher=Join-Path $basedir "pi-launcher.js"
if ($MyInvocation.ExpectingInput) {
  $input | & node $launcher $args
} else {
  & node $launcher $args
}
exit $LASTEXITCODE
""",
        encoding="utf-8",
    )
    (tmp_path / "pi.cmd").write_text('@ECHO off\nnode "%~dp0pi-launcher.js" %*\n', encoding="utf-8")
    (tmp_path / "pi-launcher.js").write_text(
        """const fs = require('fs');
const cap = process.env.FAKE_PI_CAPTURE;
const chunks = [];
process.stdin.on('data', c => chunks.push(c));
process.stdin.on('end', () => {
  if (cap) fs.writeFileSync(cap, JSON.stringify({argv: process.argv.slice(2), env: Object.fromEntries(Object.entries(process.env).filter(([k]) => k.startsWith('AITS_PI_'))), stdinBytes: Buffer.concat(chunks).length}));
  console.log(JSON.stringify({type:'message_end', message:{role:'assistant', stopReason:'stop', timestamp:'2026-10-11T00:00:00Z', provider:'fake', model:'fake', usage:{cost:{total:0}}, content:'ok'}}));
  console.log(JSON.stringify({type:'agent_settled', aborted:false}));
});
""",
        encoding="utf-8",
    )
    return tmp_path / "pi.ps1", tmp_path / "pi.cmd"


@pytest.mark.parametrize("wrapper_name", ["ps1", "cmd"])
def test_agent_run_resolves_real_pi_wrappers_and_empty_stdin(tmp_path: Path, wrapper_name: str) -> None:
    if shutil.which("node.exe") is None and shutil.which("node") is None:
        raise pytest.skip.Exception("node is not installed; pi wrapper resolution smoke test needs node")
    ps = powershell()
    pi_ps1, pi_cmd = write_fake_pi_wrappers(tmp_path)
    wrapper = pi_ps1 if wrapper_name == "ps1" else pi_cmd
    prompt = tmp_path / "提示 文件.md"; prompt.write_text("hello", encoding="utf-8")
    run_dir = tmp_path / f"run-{wrapper_name}"; capture = tmp_path / f"cap-{wrapper_name}.json"
    env = os.environ.copy(); env["FAKE_PI_CAPTURE"] = str(capture); env["AITS_PI_OWNER_DECISION"] = "leak"
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/agent-run.ps1"), "-Repo", str(ROOT), "-RunDir", str(run_dir), "-Attempt", "01", "-PromptFile", str(prompt), "-ModelId", "fake/model", "-PiCommand", str(wrapper)], text=True, capture_output=True, timeout=60, env=env)
    assert done.returncode == 0, done.stdout + done.stderr
    cap = json.loads(capture.read_text(encoding="utf-8"))
    assert f"@{prompt}" in cap["argv"]
    assert cap["stdinBytes"] == 0
    assert "AITS_PI_OWNER_DECISION" not in cap["env"]


def test_agent_run_model_error_returns_one(tmp_path: Path) -> None:
    ps = powershell(); fake = tmp_path / "fake_pi.py"; write_fake_pi(fake)
    prompt = tmp_path / "p.md"; prompt.write_text("hello", encoding="utf-8")
    env = os.environ.copy(); env["FAKE_PI_STOP"] = "error"; env["PYTHON"] = sys.executable
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/agent-run.ps1"), "-Repo", str(ROOT), "-RunDir", str(tmp_path / "run"), "-Attempt", "01", "-PromptFile", str(prompt), "-ModelId", "fake/model", "-PiCommand", str(fake)], text=True, capture_output=True, timeout=60, env=env)
    assert done.returncode == 1


def test_agent_run_streams_events_while_pi_runs(tmp_path: Path) -> None:
    ps = powershell(); fake = tmp_path / "fake_pi.py"; write_fake_pi(fake)
    prompt = tmp_path / "p.md"; prompt.write_text("hello", encoding="utf-8")
    run_dir = tmp_path / "run"; sentinel = tmp_path / "continue.flag"
    env = os.environ.copy(); env["FAKE_PI_WAIT_FOR"] = str(sentinel); env["PYTHON"] = sys.executable
    proc = subprocess.Popen([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/agent-run.ps1"), "-Repo", str(ROOT), "-RunDir", str(run_dir), "-Attempt", "01", "-PromptFile", str(prompt), "-ModelId", "fake/model", "-PiCommand", str(fake)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    events = run_dir / "attempts/01/events.jsonl"
    first = b""
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            if events.exists():
                first = events.read_bytes().splitlines()[0] if events.read_bytes() else b""
                if first:
                    break
        except OSError:
            pass
        time.sleep(0.05)
    assert first == json.dumps({"type": "progress", "message": "中文"}, ensure_ascii=False).encode("utf-8")
    assert proc.poll() is None
    assert not sentinel.exists()
    sentinel.write_text("go", encoding="utf-8")
    stdout, stderr = proc.communicate(timeout=60)
    assert proc.returncode == 0, stdout.decode(errors="replace") + stderr.decode(errors="replace")
    text_newline = os.linesep.encode()
    expected = (
        json.dumps({"type": "progress", "message": "中文"}, ensure_ascii=False).encode("utf-8") + b"\n" +
        json.dumps({"type":"message_end","message":{"role":"assistant","stopReason":"stop","timestamp":"2026-10-11T00:00:00Z","provider":"fake","model":"fake","usage":{"cost":{"total":0}},"content":"ok"}}, ensure_ascii=False).encode("utf-8") + text_newline +
        json.dumps({"type":"agent_settled","aborted":False}, ensure_ascii=False).encode("utf-8") + text_newline
    )
    assert events.read_bytes() == expected
    assert (run_dir / "attempts/01/stderr.log").stat().st_size == 200000
    assert json.loads((run_dir / "attempts/01/exit.json").read_text(encoding="utf-8"))["stopReason"] == "stop"
