"""DEVX-016 S3: the detached launcher and host fact collector build exactly the manual commands."""

from __future__ import annotations

import base64
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture.publication_commands import CommandResult
from ai_trading_system.platform.architecture.publication_services import (
    DetachedLaunchError,
    HostFactsCollector,
    WmiDetachedLauncher,
    build_detached_command_line,
    subprocess_powershell,
)

NOW = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


def _result(stdout: str, code: int = 0) -> CommandResult:
    return CommandResult(("powershell.exe",), code, stdout, "", 0.1, "log")


def test_the_detached_command_line_matches_the_manual_launch_form(tmp_path: Path) -> None:
    line = build_detached_command_line(
        ["D:\\repo\\.venv\\Scripts\\python.exe", "-B", "scripts\\drv.py", "--run-id", "r1"],
        stdout_path=tmp_path / "out.log",
        stderr_path=tmp_path / "err.log",
        environment={"PYTHONPATH": "D:\\repo\\src", "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert line == (
        'cmd.exe /d /c "set PYTHONPATH=D:\\repo\\src&& set PYTHONDONTWRITEBYTECODE=1&& '
        '"D:\\repo\\.venv\\Scripts\\python.exe" "-B" "scripts\\drv.py" "--run-id" "r1" '
        f'1> "{tmp_path / "out.log"}" 2> "{tmp_path / "err.log"}""'
    )


@pytest.mark.parametrize("token", ['bad"quote', "line\nbreak", "100%", "carriage\rreturn"])
def test_unsafe_arguments_are_refused_instead_of_being_escaped(tmp_path: Path, token: str) -> None:
    with pytest.raises(DetachedLaunchError) as raised:
        build_detached_command_line(
            ["py", token], stdout_path=tmp_path / "o", stderr_path=tmp_path / "e", environment={}
        )
    assert raised.value.code == "DETACHED_ARGUMENT_UNSAFE"


@pytest.mark.parametrize("key,value", [("A B", "1"), ("PYTHONPATH", "x&&calc"), ("K", "a|b")])
def test_unsafe_environment_entries_are_refused(tmp_path: Path, key: str, value: str) -> None:
    with pytest.raises(DetachedLaunchError) as raised:
        build_detached_command_line(
            ["py"], stdout_path=tmp_path / "o", stderr_path=tmp_path / "e", environment={key: value}
        )
    assert raised.value.code == "DETACHED_ENVIRONMENT_UNSAFE"


def test_launch_creates_the_process_through_wmi_and_returns_its_pid(tmp_path: Path) -> None:
    seen: list[str] = []

    def powershell(script: str) -> CommandResult:
        seen.append(script)
        return _result('{"ProcessId":4242,"ReturnValue":0}')

    launcher = WmiDetachedLauncher(
        run_powershell=powershell, environment={"PYTHONDONTWRITEBYTECODE": "1"}
    )
    pid = launcher.launch(
        ["py", "-B", "drv.py"],
        cwd=tmp_path / "it's",
        stdout_path=tmp_path / "o.log",
        stderr_path=tmp_path / "e.log",
    )
    assert pid == 4242
    (script,) = seen
    assert "Invoke-CimMethod -ClassName Win32_Process -MethodName Create" in script
    assert "set PYTHONDONTWRITEBYTECODE=1&&" in script
    assert "it''s" in script  # single quotes doubled for the PowerShell literal


def test_launched_processes_have_no_visible_window(tmp_path: Path) -> None:
    seen: list[str] = []

    def powershell(script: str) -> CommandResult:
        seen.append(script)
        return _result('{"ProcessId":7,"ReturnValue":0}')

    WmiDetachedLauncher(run_powershell=powershell, environment={}).launch(
        ["py"], cwd=tmp_path, stdout_path=tmp_path / "o", stderr_path=tmp_path / "e"
    )
    (script,) = seen
    # the startup information is built first and handed to Create: ShowWindow = 0 (SW_HIDE)
    assert script.index("Win32_ProcessStartup") < script.index("Invoke-CimMethod")
    assert "ShowWindow = [uint16]0" in script
    assert "ProcessStartupInformation = $si" in script
    # CREATE_NO_WINDOW through CreateFlags is rejected by WMI (return code 21): it must not be used
    assert "CreateFlags" not in script


@pytest.mark.parametrize(
    "output,code,error",
    [
        ("not json", 0, "DETACHED_LAUNCH_UNREADABLE"),
        ('{"ProcessId":0,"ReturnValue":8}', 0, "DETACHED_LAUNCH_FAILED"),
        ('{"ProcessId":7,"ReturnValue":0}', 1, "DETACHED_LAUNCH_FAILED"),
        ('{"ReturnValue":0}', 0, "DETACHED_LAUNCH_FAILED"),
    ],
)
def test_a_launch_that_does_not_report_a_pid_is_a_failure(
    tmp_path: Path, output: str, code: int, error: str
) -> None:
    launcher = WmiDetachedLauncher(
        run_powershell=lambda script: _result(output, code), environment={}
    )
    with pytest.raises(DetachedLaunchError) as raised:
        launcher.launch(
            ["py"], cwd=tmp_path, stdout_path=tmp_path / "o", stderr_path=tmp_path / "e"
        )
    assert raised.value.code == error


def test_is_running_reads_the_process_count() -> None:
    answers = {"1": True, "0": False, "": False}
    for text, expected in answers.items():
        launcher = WmiDetachedLauncher(
            run_powershell=lambda script, t=text: _result(t), environment={}
        )
        assert launcher.is_running(123) is expected


def test_scripts_are_passed_to_powershell_as_an_encoded_command() -> None:
    captured: list[Sequence[str]] = []

    def run(argv: Sequence[str]) -> CommandResult:
        captured.append(argv)
        return _result("ok")

    subprocess_powershell(run)("Write-Output 'héllo'")
    argv = captured[0]
    assert argv[:4] == ["powershell.exe", "-NoProfile", "-NonInteractive", "-EncodedCommand"]
    assert base64.b64decode(argv[4]).decode("utf-16-le") == "Write-Output 'héllo'"


def _collector(tmp_path: Path, listing: str, **overrides: object) -> HostFactsCollector:
    arguments: dict[str, object] = {
        "repository_root": tmp_path,
        "run_powershell": lambda script: _result(listing),
        "git": lambda args: "m" * 40,
        "lease_replay": lambda: ("PASS", ("lease-1",)),
        "now": lambda: NOW,
        "own_pids": (100, 101),
    }
    arguments.update(overrides)
    return HostFactsCollector(**arguments)  # type: ignore[arg-type]


def test_live_processes_exclude_the_orchestrator_and_its_parent_and_ignore_noise(
    tmp_path: Path,
) -> None:
    listing = "\n".join(
        [
            "100|9|python.exe|python orchestrator",  # self
            "200|101|git.exe|git status",  # child of the orchestrator's parent
            "300|1|python.exe|python -m pytest -n 16",  # a foreign test run
            "garbage line",
            "|||",
        ]
    )
    assert _collector(tmp_path, listing).live_processes() == (
        "300|1|python.exe|python -m pytest -n 16",
    )


def test_pre_publish_facts_collect_git_locks_orig_head_main_and_the_lease(tmp_path: Path) -> None:
    git_dir = tmp_path / ".git"
    (git_dir / "refs" / "heads").mkdir(parents=True)
    (git_dir / "ORIG_HEAD").write_text("o" * 40 + "\n", encoding="ascii")
    (git_dir / "AUTO_MERGE.lock").write_text("", encoding="ascii")
    (git_dir / "refs" / "heads" / "main.lock").write_text("", encoding="ascii")
    facts = _collector(tmp_path, "").pre_publish_facts(
        expected_main="m" * 40, expected_lease="lease-1", full_ended_at=NOW
    )
    assert facts.orig_head == "o" * 40 and facts.main == "m" * 40
    assert facts.git_locks == ("AUTO_MERGE.lock", "refs/heads/main.lock")
    assert facts.lease_replay_status == "PASS" and facts.active_leases == ("lease-1",)
    assert facts.live_processes == () and facts.observed_at == NOW
    clean = tmp_path / "clean"
    (clean / ".git").mkdir(parents=True)
    facts = _collector(clean, "").pre_publish_facts(
        expected_main="m" * 40, expected_lease="lease-1", full_ended_at=NOW
    )
    assert facts.orig_head is None and facts.git_locks == ()
