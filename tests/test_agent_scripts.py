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
PS1 = [ROOT / "scripts/agent/agent-run.ps1", ROOT / "scripts/agent/run-brief.ps1", ROOT / "scripts/agent/setup-agent-account.ps1", ROOT / "scripts/agent/publish.ps1", ROOT / "scripts/agent/approve.ps1"]


def powershell() -> str:
    exe = shutil.which("powershell.exe") or shutil.which("powershell")
    if exe is None:
        raise pytest.skip.Exception("powershell.exe not found; PowerShell tests require Windows PowerShell")
    return exe


def test_setup_requires_desktop_edition() -> None:
    text = (ROOT / "scripts/agent/setup-agent-account.ps1").read_text(encoding="utf-8-sig")
    assert text.splitlines()[0] == "#Requires -PSEdition Desktop"


@pytest.mark.parametrize("path", PS1, ids=lambda p: p.name)
def test_powershell_scripts_parse(path: Path) -> None:
    ps = powershell()
    cmd = "$errors=$null;" f"[System.Management.Automation.Language.Parser]::ParseFile('{path}', [ref]$null, [ref]$errors) > $null;" "if ($errors.Count -gt 0) { $errors | ForEach-Object { Write-Error $_ }; exit 1 }"
    done = subprocess.run([ps, "-NoProfile", "-Command", cmd], text=True, capture_output=True, timeout=60)
    assert done.returncode == 0, done.stdout + done.stderr


# Windows PowerShell 5.1 defines these in the Microsoft.PowerShell.Utility module script, not the binary
# snap-in. Started from PowerShell 7 it inherits PowerShell 7's PSModulePath, cannot load that module and
# reports them as not recognized (DEVX-029: approve.ps1 used Get-FileHash).
UTILITY_MODULE_FUNCTIONS = ("Get-FileHash", "New-TemporaryFile", "New-Guid", "Format-Hex", "Import-PowerShellDataFile", "ConvertFrom-SddlString")


@pytest.mark.parametrize("path", PS1, ids=lambda p: p.name)
def test_powershell_scripts_avoid_utility_module_functions(path: Path) -> None:
    code = [line for line in path.read_text(encoding="utf-8-sig").lower().splitlines() if not line.lstrip().startswith("#")]
    assert [name for name in UTILITY_MODULE_FUNCTIONS if any(name.lower() in line for line in code)] == []


def write_brief(path: Path, *, valid: bool = True, model: str = "local", thinking: str = "default") -> None:
    path.write_text(
        f"""---
task: DEVX-1
run: {'DEVX-1-01' if valid else 'OTHER-01'}
template: mechanical-change
zone: {'B' if valid else 'D'}
model: {model}
thinking: {thinking}
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


def write_config(path: Path, runs_root: Path, *, review_root: bool = True) -> None:
    config = {"runs_root": str(runs_root), "models": {"local": "local/model", "cloud": "openai/gpt-5.5"}, "default_thinking": {"cloud": "high"}, "daily_windows_jst": [{"start": "09:20", "end": "10:30"}], "scheduled_task_name": "nope", "agent_clone": str(ROOT), "queue_file": str(runs_root / "queue/request.json")}
    if review_root:
        config["review_root"] = str(runs_root.parent / "reviews")
    path.write_text(json.dumps(config), encoding="utf-8")


def write_review(runs_root: Path, run: str, reviews: list[dict]) -> Path:
    path = runs_root.parent / "reviews" / run / "acceptance.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"reviews": reviews}), encoding="utf-8")
    return path


def run_brief(ps: str, brief: Path, config: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/run-brief.ps1"), str(brief), "-Repo", str(ROOT), "-ConfigPath", str(config), *extra], text=True, capture_output=True, timeout=60)


def test_run_brief_dry_run_prints_command_env_and_tools(tmp_path: Path) -> None:
    ps = powershell(); brief = tmp_path / "brief.md"; config = tmp_path / "config.json"
    write_brief(brief); write_config(config, tmp_path / "runs")
    done = run_brief(ps, brief, config, "-DryRun", "-Direct", "-Now", "2026-10-11T12:00:00+09:00")
    assert done.returncode == 0, done.stdout + done.stderr
    assert "pi -p @" in done.stdout and "--mode json" in done.stdout
    assert "--tools read,bash,edit,write --approve" in done.stdout
    assert "AITS_PI_RUN_DIR" in done.stdout and "AITS_PI_ALLOWED_PATHS=scripts/agent/**" in done.stdout


def test_run_brief_state_root_goes_to_the_run_environment(tmp_path: Path) -> None:
    ps = powershell(); brief = tmp_path / "brief.md"; config = tmp_path / "config.json"
    write_brief(brief); write_config(config, tmp_path / "runs")
    state = tmp_path / "main checkout"; state.mkdir()
    now = ["-DryRun", "-Direct", "-Now", "2026-10-11T12:00:00+09:00"]
    done = run_brief(ps, brief, config, *now, "-StateRoot", str(state))
    assert done.returncode == 0, done.stdout + done.stderr
    assert f"AITS_STATE_ROOT={state}" in done.stdout.splitlines()
    assert "AITS_STATE_ROOT" not in run_brief(ps, brief, config, *now).stdout
    for bad in ("relative", str(tmp_path / "missing")):
        done = run_brief(ps, brief, config, *now, "-StateRoot", bad)
        assert done.returncode == 2 and "-StateRoot" in done.stdout


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
    write_review(runs, "DEVX-1-01", [{"verdict": "rejected"}, {"verdict": "rejected"}])
    done = run_brief(ps, brief, config, "-DryRun", "-Now", "2026-10-11T12:00:00+09:00")
    assert done.returncode == 2 and "已有 attempts" in done.stdout
    done = run_brief(ps, brief, config, "-Fix", str(fix), "-DryRun", "-Now", "2026-10-11T12:00:00+09:00")
    assert done.returncode == 0, done.stdout + done.stderr
    assert "openai/gpt-5.5" in done.stdout and "--session" in done.stdout and "Attempt=02" in done.stdout


def test_run_brief_cloud_default_thinking_and_explicit_override(tmp_path: Path) -> None:
    ps = powershell(); brief = tmp_path / "brief.md"; config = tmp_path / "config.json"
    write_config(config, tmp_path / "runs")
    for model, thinking, expected in [
        ("cloud", "default", "--model openai/gpt-5.5:high"),
        ("cloud", "medium", "--model openai/gpt-5.5:medium"),
        ("local", "default", "--model local/model "),
    ]:
        write_brief(brief, model=model, thinking=thinking)
        done = run_brief(ps, brief, config, "-DryRun", "-Direct", "-Now", "2026-10-11T12:00:00+09:00")
        assert done.returncode == 0, done.stdout + done.stderr
        assert expected in done.stdout
        if model == "local":
            assert "local/model:" not in done.stdout
    assert not (tmp_path / "runs").exists()


def test_run_brief_fix_local_after_one_rejection_uses_cloud_high(tmp_path: Path) -> None:
    ps = powershell(); brief = tmp_path / "brief.md"; fix = tmp_path / "fix.md"; config = tmp_path / "config.json"; runs = tmp_path / "runs"
    write_brief(brief); fix.write_text("fix", encoding="utf-8"); write_config(config, runs)
    run_dir = runs / "DEVX-1-01"; (run_dir / "attempts/01").mkdir(parents=True); (run_dir / "session").mkdir()
    (run_dir / "session/s.jsonl").write_text("", encoding="utf-8")
    (run_dir / "attempts/01/exit.json").write_text(json.dumps({"stopReason": "stop", "exit_code": 0}), encoding="utf-8")
    # A rejection written into the run dir (which the agent account can write) does not count; only review_root does.
    (run_dir / "acceptance.json").write_text(json.dumps({"reviews": [{"verdict": "rejected"}]}), encoding="utf-8")
    for reviews, expected, effective in [
        ([{"verdict": "rejected"}], "--model openai/gpt-5.5:high", "cloud"),
        ([], "--model local/model ", "local"),
    ]:
        write_review(runs, "DEVX-1-01", reviews)
        done = run_brief(ps, brief, config, "-Fix", str(fix), "-DryRun", "-Now", "2026-10-11T12:00:00+09:00")
        assert done.returncode == 0, done.stdout + done.stderr
        assert expected in done.stdout
        assert f"EffectiveModel={effective}" in done.stdout


def test_run_brief_refuses_config_without_review_root(tmp_path: Path) -> None:
    ps = powershell(); brief = tmp_path / "brief.md"; config = tmp_path / "config.json"
    write_brief(brief); write_config(config, tmp_path / "runs", review_root=False)
    done = run_brief(ps, brief, config, "-DryRun", "-Now", "2026-10-11T12:00:00+09:00")
    assert done.returncode == 2 and "review_root" in done.stdout


def test_run_brief_queue_mode_reports_schtasks_failure(tmp_path: Path) -> None:
    ps = powershell(); brief = tmp_path / "brief.md"; config = tmp_path / "config.json"; runs = tmp_path / "runs"
    write_brief(brief); write_config(config, runs)
    done = run_brief(ps, brief, config, "-Now", "2026-10-11T12:00:00+09:00", "-TimeoutSeconds", "30", "-PickupSeconds", "5")
    assert done.returncode == 2, done.stdout + done.stderr
    assert "schtasks /Run exit" in done.stdout
    assert "计划任务启动失败" in done.stdout
    assert not (runs / "queue/request.json").exists()


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
    assert "--approve" in cap["argv"]
    assert "AITS_PI_OWNER_DECISION" not in cap["env"] and cap["stdin_bytes"] == 0

    req = tmp_path / "request.json"; run2 = tmp_path / "run2"; capture2 = tmp_path / "cap2.json"; prompt2 = tmp_path / "p.md"; prompt2.write_text("x", encoding="utf-8")
    req.write_text(json.dumps({"repo": str(ROOT), "run_dir": str(run2), "attempt": "01", "purpose": "run", "prompt_file": str(prompt2), "model_id": "fake/model", "tools": "default", "session_file": None, "aits_env": {"AITS_PI_RUN": "DEVX"}}), encoding="utf-8")
    env["FAKE_PI_CAPTURE"] = str(capture2)
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/agent-run.ps1"), "-RequestFile", str(req), "-PiCommand", str(fake)], text=True, capture_output=True, timeout=60, env=env)
    assert done.returncode == 0, done.stdout + done.stderr
    assert not req.exists() and (run2 / "attempts/01/request.json").exists()


FAKE_PI_ENV_PY = """import json, os, re, sys
if '--export' in sys.argv:
    sys.exit(0)
sys.stdin.buffer.read()
keys = sorted(k for k in os.environ if re.search('KEY|TOKEN|SECRET|PASSWORD', k.upper()) and not k.upper().startswith('GIT_CONFIG_KEY_'))
out = {'api_keys': keys, 'keep': os.environ.get('AITS_TEST_KEEP'), 'git_key': os.environ.get('GIT_CONFIG_KEY_7')}
open(os.environ['FAKE_PI_CAPTURE'], 'w', encoding='utf-8').write(json.dumps(out))
print(json.dumps({'type': 'message_end', 'message': {'role': 'assistant', 'stopReason': 'stop', 'content': 'ok'}}))
sys.exit(0)
"""


def test_agent_run_removes_secret_env_from_pi(tmp_path: Path) -> None:
    # pi's tool results go to its model: a token printed by `printenv` leaked that way on 2026-10-16.
    ps = powershell(); fake = tmp_path / "fake_pi.py"; fake.write_text(FAKE_PI_ENV_PY, encoding="utf-8")
    prompt = tmp_path / "p.md"; prompt.write_text("x", encoding="utf-8"); capture = tmp_path / "cap.json"
    env = os.environ.copy()
    env.update({"FAKE_PI_CAPTURE": str(capture), "PYTHON": sys.executable, "OPENAI_API_KEY": "leak", "FMP_API_KEY": "leak", "AITS_TEST_KEEP": "kept"})
    env.update({"TIINGO_API_TOKEN": "leak", "Vendor_Client_Secret": "leak", "DB_PASSWORD": "leak", "SIGNING_KEY_FILE": "leak", "GIT_CONFIG_KEY_7": "user.name"})
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/agent-run.ps1"), "-Repo", str(ROOT), "-RunDir", str(tmp_path / "run"), "-Attempt", "01", "-PromptFile", str(prompt), "-ModelId", "fake/model", "-PiCommand", str(fake)], text=True, capture_output=True, timeout=60, env=env)
    assert done.returncode == 0, done.stdout + done.stderr
    assert json.loads(capture.read_text(encoding="utf-8")) == {"api_keys": [], "keep": "kept", "git_key": "user.name"}


FAKE_PI_LAUNCHER_JS = """const fs = require('fs');
const cap = process.env.FAKE_PI_CAPTURE;
const chunks = [];
process.stdin.on('data', c => chunks.push(c));
process.stdin.on('end', () => {
  if (cap) fs.writeFileSync(cap, JSON.stringify({argv: process.argv.slice(2), env: Object.fromEntries(Object.entries(process.env).filter(([k]) => k.startsWith('AITS_PI_'))), stdinBytes: Buffer.concat(chunks).length}));
  console.log(JSON.stringify({type:'message_end', message:{role:'assistant', stopReason:'stop', timestamp:'2026-10-11T00:00:00Z', provider:'fake', model:'fake', usage:{cost:{total:0}}, content:'ok'}}));
  console.log(JSON.stringify({type:'agent_settled', aborted:false}));
});
"""


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
    (tmp_path / "pi-launcher.js").write_text(FAKE_PI_LAUNCHER_JS, encoding="utf-8")
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
    assert "--approve" in cap["argv"]
    assert cap["stdinBytes"] == 0
    assert "AITS_PI_OWNER_DECISION" not in cap["env"]


@pytest.mark.parametrize("in_run_dir", [True, False])
def test_run_brief_direct_runs_a_brief_that_already_sits_in_the_run_dir(tmp_path: Path, in_run_dir: bool) -> None:
    # lane.py start hands run-brief the package's own <run_dir>/brief.md, which is also where the prompt copy goes (DEVX-031-03).
    if shutil.which("node.exe") is None and shutil.which("node") is None:
        raise pytest.skip.Exception("node is not installed; the fake pi wrapper needs node")
    ps = powershell(); runs = tmp_path / "runs"; run_dir = runs / "DEVX-1-01"; run_dir.mkdir(parents=True)
    brief = (run_dir if in_run_dir else tmp_path) / "brief.md"; config = tmp_path / "config.json"
    write_brief(brief); write_config(config, runs)
    bin_dir = tmp_path / "bin"; bin_dir.mkdir(); write_fake_pi_wrappers(bin_dir)
    env = os.environ.copy(); env["PATH"] = str(bin_dir) + os.pathsep + env["PATH"]
    before = brief.read_bytes()
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/run-brief.ps1"), str(brief), "-Repo", str(ROOT), "-ConfigPath", str(config), "-Direct", "-Now", "2026-10-11T12:00:00+09:00"], text=True, capture_output=True, timeout=120, env=env)
    assert done.returncode == 0, done.stdout + done.stderr
    assert "Cannot overwrite" not in done.stdout + done.stderr
    assert brief.read_bytes() == before
    assert (run_dir / "brief.md").read_bytes() == before
    assert json.loads((run_dir / "attempts/01/exit.json").read_text(encoding="utf-8-sig"))["stopReason"] == "stop"


def test_agent_run_resolves_npm_cmd_shim(tmp_path: Path) -> None:
    if shutil.which("node.exe") is None and shutil.which("node") is None:
        raise pytest.skip.Exception("node is not installed; npm cmd shim resolution smoke test needs node")
    ps = powershell()
    pi_root = tmp_path / "aits pi"
    cli = pi_root / "node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js"
    cli.parent.mkdir(parents=True)
    cli.write_text(FAKE_PI_LAUNCHER_JS, encoding="utf-8")
    pi_cmd = pi_root / "pi.cmd"
    pi_cmd.write_text(
        "@ECHO off\r\n"
        "GOTO start\r\n"
        ":find_dp0\r\n"
        "SET dp0=%~dp0\r\n"
        "EXIT /b\r\n"
        ":start\r\n"
        "SETLOCAL\r\n"
        "CALL :find_dp0\r\n"
        "\r\n"
        "IF EXIST \"%dp0%\\node.exe\" (\r\n"
        "  SET \"_prog=%dp0%\\node.exe\"\r\n"
        ") ELSE (\r\n"
        "  SET \"_prog=node\"\r\n"
        "  SET PATHEXT=%PATHEXT:;.JS;=;%\r\n"
        ")\r\n"
        "\r\n"
        "endLocal & goto #_undefined_# 2>NUL || title %COMSPEC% & \"%_prog%\"  \"%dp0%\\node_modules\\@earendil-works\\pi-coding-agent\\dist\\bundle\\cli.js\" %*\r\n",
        encoding="ascii",
    )
    prompt = tmp_path / "prompt.md"; prompt.write_text("hello", encoding="utf-8")
    run_dir = tmp_path / "run-npm"; capture = tmp_path / "cap-npm.json"
    env = os.environ.copy(); env["FAKE_PI_CAPTURE"] = str(capture); env["AITS_PI_OWNER_DECISION"] = "leak"
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/agent-run.ps1"), "-Repo", str(ROOT), "-RunDir", str(run_dir), "-Attempt", "01", "-PromptFile", str(prompt), "-ModelId", "fake/model", "-PiCommand", str(pi_cmd)], text=True, capture_output=True, timeout=60, env=env)
    assert done.returncode == 0, done.stdout + done.stderr
    cap = json.loads(capture.read_text(encoding="utf-8"))
    assert f"@{prompt}" in cap["argv"]
    assert "--approve" in cap["argv"]
    assert cap["stdinBytes"] == 0


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


def write_function_loader_script(path: Path, body: str) -> None:
    path.write_text(
        f"""
$scriptPath = '{ROOT / "scripts/agent/setup-agent-account.ps1"}'
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($scriptPath, [ref]$tokens, [ref]$errors)
if ($errors.Count -gt 0) {{ $errors | ForEach-Object {{ Write-Error $_ }}; exit 1 }}
$functions = $ast.FindAll({{ param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst] }}, $true)
foreach ($f in $functions) {{ . ([scriptblock]::Create($f.Extent.Text)) }}
{body}
""".lstrip(),
        encoding="ascii",
    )


def test_setup_test_right_masks(tmp_path: Path) -> None:
    ps = powershell(); script = tmp_path / "test-right.ps1"
    write_function_loader_script(
        script,
        """
$rows = @(
    @(1179817, $false, $true),
    @(-1610612736, $false, $true),
    @(1245631, $true, $true),
    @(4, $true, $false),
    @(2032127, $true, $true),
    @(268435456, $true, $true),
    @(1048576, $false, $false)
)
$out = @()
foreach ($row in $rows) {
    $actualWrite = Test-Right $row[0] "write"
    $actualRead = Test-Right $row[0] "read"
    $out += [pscustomobject]@{ Rights = $row[0]; Write = $actualWrite; Read = $actualRead }
}
$out | ConvertTo-Json
""",
    )
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script)], text=True, capture_output=True, timeout=60)
    assert done.returncode == 0, done.stdout + done.stderr
    assert json.loads(done.stdout) == [
        {"Rights": 1179817, "Write": False, "Read": True},
        {"Rights": -1610612736, "Write": False, "Read": True},
        {"Rights": 1245631, "Write": True, "Read": True},
        {"Rights": 4, "Write": True, "Read": False},
        {"Rights": 2032127, "Write": True, "Read": True},
        {"Rights": 268435456, "Write": True, "Read": True},
        {"Rights": 1048576, "Write": False, "Read": False},
    ]



def test_setup_restricted_dir_acl_matches_only_the_exact_protected_acl(tmp_path: Path) -> None:
    ps = powershell(); script = tmp_path / "restricted-acl.ps1"
    target = tmp_path / "locked"; target.mkdir()
    write_function_loader_script(
        script,
        f"""
$RightsFull = 2032127; $RightsModify = 1245631; $RightsReadExecute = 1179817
$me = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$dir = '{target}'
$want = @{{ "S-1-5-32-544" = $RightsFull; "S-1-5-18" = $RightsFull; $me = $RightsFull; "S-1-5-32-545" = $RightsModify }}
$out = [ordered]@{{}}
$out.inherited = Test-RestrictedDirAcl $dir $want
icacls $dir /inheritance:r /grant:r "*S-1-5-32-544:(OI)(CI)F" "*S-1-5-18:(OI)(CI)F" "*${{me}}:(OI)(CI)F" "*S-1-5-32-545:(OI)(CI)M" | Out-Null
$out.exact = Test-RestrictedDirAcl $dir $want
$readOnly = @{{ "S-1-5-32-544" = $RightsFull; "S-1-5-18" = $RightsFull; $me = $RightsFull; "S-1-5-32-545" = $RightsReadExecute }}
$out.wrong_rights = Test-RestrictedDirAcl $dir $readOnly
icacls $dir /grant "*S-1-5-11:(OI)(CI)RX" | Out-Null
$out.extra_ace = Test-RestrictedDirAcl $dir $want
$out.missing = Test-RestrictedDirAcl ($dir + "-missing") $want
$out | ConvertTo-Json
""",
    )
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script)], text=True, capture_output=True, timeout=60)
    assert done.returncode == 0, done.stdout + done.stderr
    assert json.loads(done.stdout) == {"inherited": False, "exact": True, "wrong_rights": False, "extra_ace": False, "missing": False}



def test_setup_build_agent_venv_locks_venv_and_keeps_junction_targets(tmp_path: Path) -> None:
    ps = powershell(); script = tmp_path / "build-venv.ps1"
    base_python = Path(sys.base_prefix) / "python.exe"
    if not base_python.exists():
        pytest.skip(f"base Python not found at {base_python}")
    clone = tmp_path / "clone"; venv = clone / ".venv"; staging = tmp_path / "staging"; staging.mkdir()
    keep = tmp_path / "keep"; keep.mkdir(); (keep / "keep.txt").write_text("keep", encoding="ascii")
    venv.mkdir(parents=True)
    subprocess.run(["cmd", "/c", "mklink", "/J", str(venv / "link"), str(keep)], check=True, text=True, capture_output=True, timeout=60)
    requirements = tmp_path / "requirements.txt"; requirements.write_text("", encoding="ascii")
    write_function_loader_script(
        script,
        f"""
$ErrorActionPreference = "Stop"
$RightsFull = 2032127; $RightsModify = 1245631; $RightsReadExecute = 1179817
$me = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$frozen = @(Build-AgentVenv '{base_python}' '{venv}' '{staging}' '{requirements}' '{clone / "src"}' $me "S-1-5-32-545")
$want = @{{ "S-1-5-32-544" = $RightsFull; "S-1-5-18" = $RightsFull; $me = $RightsFull; "S-1-5-32-545" = $RightsReadExecute }}
[ordered]@{{ frozen = $frozen.Count; locked = (Test-RestrictedDirAcl '{venv}' $want); staging_left = @(Get-ChildItem -LiteralPath '{staging}').Count }} | ConvertTo-Json
""",
    )
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script)], text=True, capture_output=True, timeout=300)
    assert done.returncode == 0, done.stdout + done.stderr
    assert json.loads(done.stdout[done.stdout.index("{"):]) == {"frozen": 0, "locked": True, "staging_left": 0}
    assert (keep / "keep.txt").read_text(encoding="ascii") == "keep"
    assert not (venv / "link").exists()
    assert (venv / "Scripts/python.exe").exists()
    assert (venv / "Lib/site-packages/aits_agent_clone.pth").read_bytes() == (str(clone / "src") + "\r\n").encode("utf-8")


def test_setup_add_problem_deduplicates(tmp_path: Path) -> None:
    ps = powershell(); script = tmp_path / "add-problem.ps1"
    write_function_loader_script(
        script,
        """
$WarningPreference = "SilentlyContinue"
$ProblemList = New-Object System.Collections.ArrayList
Add-Problem "same problem"
Add-Problem "same problem"
Add-Problem "other problem"
$ProblemList | ForEach-Object { $_ }
""",
    )
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script)], text=True, capture_output=True, timeout=60)
    assert done.returncode == 0, done.stdout + done.stderr
    assert done.stdout.splitlines() == ["same problem", "other problem"]


def test_setup_check_only_reports_once_and_exits_one(tmp_path: Path) -> None:
    ps = powershell(); work = tmp_path / "work"; repo = tmp_path / "repo"
    work.mkdir(); repo.mkdir()
    subprocess.run(["icacls", str(work), "/grant", "*S-1-5-11:(M)"], check=True, text=True, capture_output=True, timeout=60)
    subprocess.run(["icacls", str(work), "/grant", "*S-1-5-11:(OI)(CI)(IO)(M)"], check=True, text=True, capture_output=True, timeout=60)
    subprocess.run(["icacls", str(repo), "/grant", "*S-1-5-32-545:(OI)(CI)(RX)"], check=True, text=True, capture_output=True, timeout=60)
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/setup-agent-account.ps1"), "-CheckOnly", "-WorkRoot", str(work), "-Repo", str(repo)], text=True, capture_output=True, timeout=120)
    output = done.stdout + done.stderr
    assert done.returncode == 1, output
    assert output.count(f"{work} is writable by") == 1
    assert f"{work} is writable by" in output and "(S-1-5-11)" in output
    assert f"{repo} is readable by" in output and "(S-1-5-32-545)" in output
    assert f"{repo} is writable by" not in output


def system_safe_directories() -> list[str]:
    done = subprocess.run(["git", "config", "--system", "--get-all", "safe.directory"], text=True, capture_output=True, timeout=60)
    return done.stdout.splitlines()


def ps_command(ps: str, command: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([ps, "-NoProfile", "-Command", command], text=True, capture_output=True, timeout=60)


def run_setup_whatif(ps: str, tmp_path: Path, *, pi_command: Path, base_python_home: Path | None = None) -> subprocess.CompletedProcess[str]:
    work = tmp_path / "work"; repo = tmp_path / "repo"
    agent = tmp_path / "agent"; wt = tmp_path / "wt"; runs = tmp_path / "runs"; reviews = tmp_path / "reviews"
    work.mkdir(); repo.mkdir()
    if base_python_home is not None:
        (repo / ".venv").mkdir()
        (repo / ".venv/pyvenv.cfg").write_text(f"home = {base_python_home}\n", encoding="ascii")
    return subprocess.run(
        [
            ps,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ROOT / "scripts/agent/setup-agent-account.ps1"),
            "-WhatIf",
            "-WorkRoot",
            str(work),
            "-Repo",
            str(repo),
            "-UserName",
            "aits-whatif-test",
            "-AgentClone",
            str(agent),
            "-AgentWorktree",
            str(wt),
            "-RunsRoot",
            str(runs),
            "-QueueFile",
            str(runs / "queue/request.json"),
            "-ReviewRoot",
            str(reviews),
            "-TaskName",
            "AITradingSystem pi agent whatif test",
            "-PiCommand",
            str(pi_command),
        ],
        stdin=subprocess.DEVNULL,
        text=True,
        capture_output=True,
        timeout=120,
    )


def test_setup_whatif_changes_nothing(tmp_path: Path) -> None:
    ps = powershell(); pi_dir = tmp_path / "aits pi"; pi_dir.mkdir(); pi_command = pi_dir / "pi.cmd"; pi_command.write_text("", encoding="ascii")
    agent = tmp_path / "agent"; wt = tmp_path / "wt"; runs = tmp_path / "runs"
    before = system_safe_directories()
    done = run_setup_whatif(ps, tmp_path, pi_command=pi_command)
    output = done.stdout + done.stderr
    if "Full setup stopped because CheckOnly found problems" in output:
        pytest.skip("WhatIf CheckOnly found problems on this machine: " + output)
    assert done.returncode == 0, output
    assert "What if:" in output
    assert "Password for local standard user" not in output
    assert not agent.exists() and not wt.exists() and not runs.exists()
    assert not (tmp_path / "reviews").exists()
    assert system_safe_directories() == before
    user_check = ps_command(ps, "if (Get-LocalUser -Name 'aits-whatif-test' -ErrorAction SilentlyContinue) { 'user-present' } else { 'user-absent' }")
    assert user_check.returncode == 0, user_check.stdout + user_check.stderr
    assert user_check.stdout.strip() == "user-absent"
    task_check = ps_command(ps, "if (Get-ScheduledTask -TaskName 'AITradingSystem pi agent whatif test' -ErrorAction SilentlyContinue) { 'task-present' } else { 'task-absent' }")
    assert task_check.returncode == 0, task_check.stdout + task_check.stderr
    assert task_check.stdout.strip() == "task-absent"
    flattened = output.replace("\r", "").replace("\n", "")
    assert '-PiCommand "' + str(pi_command) in flattened
    assert "pi command: " in output


def test_setup_whatif_shows_password_and_base_python_steps(tmp_path: Path) -> None:
    ps = powershell(); pi_dir = tmp_path / "aits pi"; pi_dir.mkdir(); pi_command = pi_dir / "pi.cmd"; pi_command.write_text("", encoding="ascii")
    basepy = tmp_path / "basepy"; basepy.mkdir()
    before = subprocess.run(["icacls", str(basepy)], check=True, text=True, capture_output=True, timeout=60).stdout
    done = run_setup_whatif(ps, tmp_path, pi_command=pi_command, base_python_home=basepy)
    output = done.stdout + done.stderr
    if "Full setup stopped because CheckOnly found problems" in output:
        pytest.skip("WhatIf CheckOnly found problems on this machine: " + output)
    assert done.returncode == 0, output
    assert "require a password" in output
    assert "grant aits-whatif-test read and execute on the base Python" in output
    flattened = output.replace("\r", "").replace("\n", "")
    assert "SeBatchLogonRight" in flattened
    assert "already has SeBatchLogonRight" not in flattened
    assert "--exclude-editable" in flattened
    assert "Main venv python not found" not in flattened
    assert "readable but not writable by aits-whatif-test" in flattened
    assert ".pth file instead of an editable install" in flattened
    assert "no access for aits-whatif-test" in flattened
    after = subprocess.run(["icacls", str(basepy)], check=True, text=True, capture_output=True, timeout=60).stdout
    assert after == before


def test_setup_whatif_reports_missing_pi(tmp_path: Path) -> None:
    ps = powershell(); pi_command = tmp_path / "missing" / "pi.cmd"
    agent = tmp_path / "agent"; wt = tmp_path / "wt"; runs = tmp_path / "runs"
    done = run_setup_whatif(ps, tmp_path, pi_command=pi_command)
    output = done.stdout + done.stderr
    if "Full setup stopped because CheckOnly found problems" in output:
        pytest.skip("WhatIf CheckOnly found problems on this machine: " + output)
    assert done.returncode == 0, output
    assert "pi command not found:" in output
    assert "Full setup would stop here" in output
    assert not agent.exists() and not wt.exists() and not runs.exists()


APP_PACKAGE_SIDS = ("S-1-15-2-1", "S-1-15-2-2")


def grant_app_package_read(path: Path) -> None:
    for sid in APP_PACKAGE_SIDS:
        subprocess.run(["icacls", str(path), "/grant", f"*{sid}:(OI)(CI)(RX)"], check=True, text=True, capture_output=True, timeout=60)


def test_setup_check_only_accepts_app_package_aces(tmp_path: Path) -> None:
    ps = powershell(); work = tmp_path / "work"; repo = tmp_path / "repo"
    work.mkdir(); repo.mkdir()
    grant_app_package_read(work); grant_app_package_read(repo)
    done = subprocess.run([ps, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(ROOT / "scripts/agent/setup-agent-account.ps1"), "-CheckOnly", "-WorkRoot", str(work), "-Repo", str(repo)], text=True, capture_output=True, timeout=120)
    output = done.stdout + done.stderr
    assert "IdentityNotMapped" not in output and "Translate" not in output, output
    assert f"{work} is" not in output and f"{repo} is" not in output, output
    if done.returncode != 0:
        pytest.skip("CheckOnly found machine-level problems on this machine: " + output)
    assert "CheckOnly passed." in output


def test_setup_whatif_accepts_app_package_aces_on_pi_dir(tmp_path: Path) -> None:
    ps = powershell(); pi_dir = tmp_path / "aits pi"; pi_dir.mkdir(); pi_command = pi_dir / "pi.cmd"; pi_command.write_text("", encoding="ascii")
    grant_app_package_read(pi_dir)
    done = run_setup_whatif(ps, tmp_path, pi_command=pi_command)
    output = done.stdout + done.stderr
    if "Full setup stopped because CheckOnly found problems" in output:
        pytest.skip("WhatIf CheckOnly found problems on this machine: " + output)
    assert done.returncode == 0, output
    assert "IdentityNotMapped" not in output and "Translate" not in output, output
    assert "pi command: " in output
    assert "Full setup would stop here" not in output
