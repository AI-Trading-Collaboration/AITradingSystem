"""DEVX-016 S3: the Windows host services of a publication run.

A detached launcher (WMI `Win32_Process.Create`, so the child sits outside the desktop agent app's
job tree and survives it: the lesson of the 2026-09-30 app auto-update that killed a formal Full),
and the fact collector behind the pre-publication checklist. Both are thin: they run the same
PowerShell, git and replay calls the manual flow ran and return plain facts.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.platform.architecture.publication_checks import PrePublishFacts
from ai_trading_system.platform.architecture.publication_commands import CommandResult

PowerShellRunner = Callable[[str], CommandResult]

# The desktop app polls the repository's git status in short bursts (git.exe command lines with
# `-c core.hooksPath=NUL -c safe.directory=* -c core.fsmonitor=`). A burst that overlaps one
# observation of the host must not stop a chain (E50 failed once on it, 2026-10-09); a process
# that stays must. Engineering retry parameters, not an investment threshold: 5 observations
# 6 s apart (a 24 s window).
PROCESS_SETTLE_ATTEMPTS = 5
PROCESS_SETTLE_INTERVAL_SECONDS = 6.0


class DetachedLaunchError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def build_detached_command_line(
    argv: Sequence[str],
    *,
    stdout_path: Path,
    stderr_path: Path,
    environment: Mapping[str, str],
) -> str:
    """`cmd.exe /d /c "set K=V&& "<exe>" args 1> "<out>" 2> "<err>""` as the manual launch used."""
    for token in (*argv, str(stdout_path), str(stderr_path)):
        if '"' in token or "\n" in token or "\r" in token or "%" in token:
            raise DetachedLaunchError("DETACHED_ARGUMENT_UNSAFE", token)
    for key, value in environment.items():
        if not key.isidentifier() or any(c in value for c in '"&|<>^%\n\r'):
            raise DetachedLaunchError("DETACHED_ENVIRONMENT_UNSAFE", key)
    settings = "".join(f"set {key}={value}&& " for key, value in environment.items())
    quoted = " ".join(f'"{token}"' for token in argv)
    return f'cmd.exe /d /c "{settings}{quoted} 1> "{stdout_path}" 2> "{stderr_path}""'


def _encode(script: str) -> list[str]:
    encoded = base64.b64encode(script.encode("utf-16-le")).decode("ascii")
    return ["powershell.exe", "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded]


# Detached processes must not open a console window (the owner saw three empty terminal windows and
# closing the wrong one would have aborted a Full). ShowWindow = 0 is SW_HIDE; adding CreateFlags
# CREATE_NO_WINDOW is rejected by WMI (return code 21) and is not needed (verified 2026-10-08: the
# process runs and no terminal host process is started).
HIDDEN_WINDOW_STARTUP = (
    "$si = New-CimInstance -ClassName Win32_ProcessStartup -Namespace root/cimv2 -ClientOnly "
    "-Property @{ ShowWindow = [uint16]0 };"
)


class WmiDetachedLauncher:
    def __init__(
        self,
        *,
        run_powershell: PowerShellRunner,
        environment: Mapping[str, str],
    ) -> None:
        self.run_powershell = run_powershell
        self.environment = dict(environment)

    def launch(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        stdout_path: Path,
        stderr_path: Path,
        environment: Mapping[str, str] | None = None,
    ) -> int:
        """``environment`` is added to the launcher's base variables for this one process only."""
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        command_line = build_detached_command_line(
            argv,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            environment={**self.environment, **(environment or {})},
        )
        quoted_line = command_line.replace("'", "''")
        quoted_cwd = str(cwd).replace("'", "''")
        script = (
            f"{HIDDEN_WINDOW_STARTUP} "
            "$r = Invoke-CimMethod -ClassName Win32_Process -MethodName Create "
            f"-Arguments @{{ CommandLine = '{quoted_line}'; CurrentDirectory = '{quoted_cwd}'; "
            "ProcessStartupInformation = $si }; "
            "$r | Select-Object ProcessId, ReturnValue | ConvertTo-Json -Compress"
        )
        result = self.run_powershell(script)
        try:
            body = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise DetachedLaunchError("DETACHED_LAUNCH_UNREADABLE", result.stdout[:200]) from exc
        if result.exit_code != 0 or body.get("ReturnValue") != 0 or not body.get("ProcessId"):
            raise DetachedLaunchError("DETACHED_LAUNCH_FAILED", result.stdout[:200])
        return int(body["ProcessId"])

    def is_running(self, pid: int) -> bool:
        script = (
            f"(Get-Process -Id {int(pid)} -ErrorAction SilentlyContinue | Measure-Object).Count"
        )
        result = self.run_powershell(script)
        return result.stdout.strip() not in {"", "0"}


def run_plain(argv: Sequence[str], *, cwd: Path, env: Mapping[str, str]) -> CommandResult:
    """A command whose output is only needed in memory (no evidence log)."""
    started = time.monotonic()
    completed = subprocess.run(
        list(argv),
        cwd=cwd,
        env=dict(env),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return CommandResult(
        tuple(argv),
        completed.returncode,
        completed.stdout,
        completed.stderr,
        time.monotonic() - started,
        "",
    )


def subprocess_powershell(run: Callable[[Sequence[str]], CommandResult]) -> PowerShellRunner:
    def execute(script: str) -> CommandResult:
        return run(_encode(script))

    return execute


class HostFactsCollector:
    """Observes the host for the pre-publication checklist (processes, locks, ORIG_HEAD, lease)."""

    def __init__(
        self,
        *,
        repository_root: Path,
        run_powershell: PowerShellRunner,
        git: Callable[[Sequence[str]], str],
        lease_replay: Callable[[], tuple[str, tuple[str, ...]]],
        now: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
        own_pids: Sequence[int] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.repository_root = repository_root
        self.run_powershell = run_powershell
        self.git = git
        self.lease_replay = lease_replay
        self.now = now
        self.own_pids = tuple(own_pids) if own_pids is not None else (os.getpid(), os.getppid())
        self.sleep = sleep

    def live_processes(self) -> tuple[str, ...]:
        script = (
            "Get-CimInstance Win32_Process -Filter \"Name='git.exe' OR Name='python.exe'\" | "
            "ForEach-Object { '{0}|{1}|{2}|{3}' -f $_.ProcessId, $_.ParentProcessId, $_.Name, "
            "$_.CommandLine }"
        )
        found: list[str] = []
        for line in self.run_powershell(script).stdout.splitlines():
            parts = line.split("|", 3)
            if len(parts) != 4 or not parts[0].strip().isdigit():
                continue
            pid, parent = int(parts[0]), int(parts[1]) if parts[1].strip().isdigit() else -1
            if pid in self.own_pids or parent in self.own_pids:
                continue
            found.append(line[:160])
        return tuple(found)

    def settled_live_processes(self) -> tuple[str, ...]:
        """Live foreign git/python processes after bounded re-observation (S3 follow-up g).

        Clean at any observation means clean; if every observation sees a process the last one is
        returned, so a process that stays still fails the caller exactly as before.
        """
        processes: tuple[str, ...] = ()
        for attempt in range(PROCESS_SETTLE_ATTEMPTS):
            processes = self.live_processes()
            if not processes:
                return ()
            if attempt + 1 < PROCESS_SETTLE_ATTEMPTS:
                self.sleep(PROCESS_SETTLE_INTERVAL_SECONDS)
        return processes

    def pre_publish_facts(
        self, *, expected_main: str, expected_lease: str, full_ended_at: datetime
    ) -> PrePublishFacts:
        git_dir = self.repository_root / ".git"
        orig = git_dir / "ORIG_HEAD"
        orig_head = orig.read_text(encoding="ascii").strip() if orig.exists() else None
        locks = sorted(p.relative_to(git_dir).as_posix() for p in git_dir.glob("*.lock"))
        for sub in ("refs/heads", "refs/remotes", "refs/tags"):
            base = git_dir / sub
            if base.exists():
                locks += sorted(p.relative_to(git_dir).as_posix() for p in base.rglob("*.lock"))
        processes = self.settled_live_processes()
        status, active = self.lease_replay()
        return PrePublishFacts(
            main=self.git(["rev-parse", "main"]),
            expected_main=expected_main,
            orig_head=orig_head,
            git_locks=tuple(locks),
            live_processes=processes,
            lease_replay_status=status,
            active_leases=active,
            expected_lease=expected_lease,
            full_ended_at=full_ended_at,
            observed_at=self.now(),
        )
