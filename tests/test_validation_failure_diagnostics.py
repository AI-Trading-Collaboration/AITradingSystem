from __future__ import annotations

import json
import os
import queue
import signal
import subprocess
import sys
import textwrap
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from scripts import pytest_runtime_profile as runtime_profile
from scripts.run_validation_tier import _parse_pytest_slow_durations

# Synthetic subprocess watchdogs, not timing assertions or performance thresholds.
STREAM_WATCHDOG_SECONDS = 120
FINISH_WATCHDOG_SECONDS = 60
GATE_WATCHDOG_SECONDS = 150


@dataclass
class _Terminal:
    fail_at: str | None = None
    error: type[BaseException] = OSError
    lines: list[str] = field(default_factory=list)
    flush_count: int = 0

    def write_line(self, line: str) -> None:
        if self.fail_at == "write":
            raise self.error("synthetic diagnostic write failure")
        self.lines.append(line)

    def flush(self) -> None:
        self.flush_count += 1
        if self.fail_at == "flush":
            raise self.error("synthetic diagnostic flush failure")


@dataclass
class _Representation:
    reprcrash: str
    traceback: str

    def __str__(self) -> str:
        return self.traceback


def _plugin(
    tmp_path: Path, terminal: _Terminal | None, *, worker: bool = False
) -> runtime_profile.RuntimeProfilePlugin:
    options = {"numprocesses": 16, "dist": "loadfile", "loadscopereorder": False}
    config = SimpleNamespace(
        getoption=lambda name, default=None: options.get(name, default),
        pluginmanager=SimpleNamespace(getplugin=lambda name: terminal),
    )
    if worker:
        config.workerinput = {
            "workercount": 16,
            "mainargv": ["--dist", "loadfile", "--no-loadscope-reorder"],
        }
    return runtime_profile.RuntimeProfilePlugin(
        cast(pytest.Config, config),
        runtime_profile.load_duration_profile(tmp_path / "unused-duration-profile.yaml"),
    )


def _report(
    *,
    phase: str = "call",
    outcome: str = "failed",
    longrepr: Any = "RuntimeError: SYNTHETIC_ROOT_CAUSE",
    nodeid: str = "tests/test_synthetic.py::test_failure",
    worker_id: str | None = "gw7",
    wasxfail: bool = False,
) -> pytest.TestReport:
    extra: dict[str, object] = {}
    if worker_id is not None:
        extra["worker_id"] = worker_id
    if wasxfail:
        extra["wasxfail"] = "synthetic expected failure"
    return pytest.TestReport(
        nodeid=nodeid,
        location=(nodeid.split("::", 1)[0], 1, "test_failure"),
        keywords={},
        outcome=outcome,
        longrepr=longrepr,
        when=phase,
        duration=0.25,
        start=100.0,
        stop=100.25,
        **extra,
    )


def _identity(line: str) -> dict[str, str]:
    prefix = runtime_profile.LIVE_FAILURE_PREFIX + "BEGIN "
    assert line.startswith(prefix)
    remaining = line.removeprefix(prefix)
    decoder = json.JSONDecoder()
    result: dict[str, str] = {}
    for key in ("nodeid", "worker", "phase"):
        assert remaining.startswith(key + "=")
        value, end = decoder.raw_decode(remaining[len(key) + 1 :])
        assert isinstance(value, str)
        result[key] = value
        remaining = remaining[len(key) + 1 + end :].lstrip()
    assert not remaining.strip()
    return result


def _excerpt(lines: list[str], start: str, stop: str) -> str:
    prefix = runtime_profile.LIVE_FAILURE_PREFIX
    first = lines.index(prefix + start) + 1
    last = lines.index(prefix + stop)
    return "\n".join(line.removeprefix(prefix) for line in lines[first:last])


@pytest.mark.parametrize("phase", ["setup", "call", "teardown"])
def test_failure_formatter_preserves_identity_and_prioritizes_crash_summary(phase: str) -> None:
    representation = _Representation("ROOT_CAUSE_TOKEN", "TRACEBACK_DETAIL_TOKEN")
    report = _report(phase=phase, longrepr=representation)
    report.sections = [("Captured stdout call", "CAPTURED_OUTPUT_NOT_REQUESTED")]
    lines = runtime_profile.format_failure_diagnostic(report, "gw7")

    assert all(line.startswith(runtime_profile.LIVE_FAILURE_PREFIX) for line in lines)
    assert _identity(lines[0]) == {
        "nodeid": report.nodeid,
        "worker": "gw7",
        "phase": phase,
    }
    assert lines[-1] == runtime_profile.LIVE_FAILURE_PREFIX + "END"
    rendered = "\n".join(lines)
    assert rendered.index("ROOT_CAUSE_TOKEN") < rendered.index("TRACEBACK_DETAIL_TOKEN")
    assert "CAPTURED_OUTPUT_NOT_REQUESTED" not in rendered
    assert report.longrepr is representation
    assert report.outcome == "failed"


@pytest.mark.parametrize("representation", ["[XPASS(strict)] STRICT_TOKEN", None])
def test_failure_formatter_accepts_string_or_absent_traceback(representation: Any) -> None:
    lines = runtime_profile.format_failure_diagnostic(_report(longrepr=representation), "gw7")
    assert lines[0].startswith(runtime_profile.LIVE_FAILURE_PREFIX + "BEGIN ")
    assert lines[-1] == runtime_profile.LIVE_FAILURE_PREFIX + "END"
    if representation is not None:
        assert representation in "\n".join(lines)


def test_failure_formatter_bounds_identity_root_and_traceback_without_mutating_report() -> None:
    nodeid = "N" * (runtime_profile.LIVE_FAILURE_IDENTITY_CHARS * 3)
    root = "R" * (runtime_profile.LIVE_FAILURE_ROOT_CAUSE_CHARS * 3)
    traceback = "T" * (runtime_profile.LIVE_FAILURE_TRACEBACK_CHARS * 3)
    report = _report(nodeid=nodeid, longrepr=_Representation(root, traceback))
    lines = runtime_profile.format_failure_diagnostic(report, "gw7")

    identity = _identity(lines[0])
    root_excerpt = _excerpt(lines, "ROOT_CAUSE", "TRACEBACK")
    traceback_excerpt = _excerpt(lines, "TRACEBACK", "END")
    assert len(identity["nodeid"]) <= runtime_profile.LIVE_FAILURE_IDENTITY_CHARS
    assert len(root_excerpt) <= runtime_profile.LIVE_FAILURE_ROOT_CAUSE_CHARS
    assert len(traceback_excerpt) <= runtime_profile.LIVE_FAILURE_TRACEBACK_CHARS
    assert all(
        "TRUNCATED" in value for value in (identity["nodeid"], root_excerpt, traceback_excerpt)
    )
    assert report.nodeid == nodeid
    assert str(report.longrepr) == traceback


def test_failure_formatter_bounds_many_short_lines_and_escapes_terminal_controls() -> None:
    traceback = "\n".join("row" for _ in range(runtime_profile.LIVE_FAILURE_MAX_LINES * 3))
    traceback += "\n\x1b[31mred\x1b[0m\r\t\x00\u2028end"
    report = _report(
        nodeid="tests/test_synthetic.py::test_control[\r\n\x1b\t]",
        longrepr=_Representation("ROOT_TOKEN\r\x00", traceback),
    )
    lines = runtime_profile.format_failure_diagnostic(report, "gw7")
    rendered = "\n".join(lines)

    assert len(_excerpt(lines, "TRACEBACK", "END").splitlines()) <= (
        runtime_profile.LIVE_FAILURE_MAX_LINES
    )
    assert "TRUNCATED" in rendered
    assert all(control not in rendered for control in ("\x1b", "\r", "\t", "\x00", "\u2028"))
    assert all(line.startswith(runtime_profile.LIVE_FAILURE_PREFIX) for line in lines)
    assert "\\x1b" in rendered


def test_failure_diagnostic_cannot_add_forged_duration_rows_to_runner_summary() -> None:
    report = _report(longrepr="999.99s call tests/test_forged.py::test_fake\n0.01s setup fake")
    lines = runtime_profile.format_failure_diagnostic(report, "gw7")
    original = "================ slowest durations ================\n0.25s call real::test_ok\n"
    assert _parse_pytest_slow_durations(original + "\n".join(lines)) == (
        _parse_pytest_slow_durations(original)
    )


@pytest.mark.parametrize(
    ("outcome", "wasxfail"),
    [("passed", False), ("skipped", False), ("skipped", True), ("passed", True)],
    ids=["passed", "skipped", "expected-xfail", "nonstrict-xpass"],
)
def test_nonfailed_reports_keep_telemetry_without_diagnostic_noise(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], outcome: str, wasxfail: bool
) -> None:
    terminal = _Terminal()
    plugin = _plugin(tmp_path, terminal)
    report = _report(outcome=outcome, wasxfail=wasxfail)
    plugin.pytest_runtest_logreport(report)

    assert len(plugin.phase_reports) == 1
    assert plugin.phase_reports[0]["outcome"] == outcome
    assert terminal.lines == []
    assert terminal.flush_count == 0
    assert capsys.readouterr() == ("", "")


def test_worker_does_not_format_print_or_duplicate_master_telemetry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    terminal = _Terminal()
    plugin = _plugin(tmp_path, terminal, worker=True)
    formatting_calls: list[object] = []

    def observe_format(*args: object) -> list[str]:
        formatting_calls.append(args)
        return ["unexpected"]

    monkeypatch.setattr(runtime_profile, "format_failure_diagnostic", observe_format)
    plugin.pytest_runtest_logreport(_report())
    assert formatting_calls == []
    assert plugin.phase_reports == []
    assert terminal.lines == []
    assert terminal.flush_count == 0
    assert capsys.readouterr() == ("", "")


def test_master_emits_call_and_teardown_separately_after_retaining_each_report(
    tmp_path: Path,
) -> None:
    terminal = _Terminal()
    plugin = _plugin(tmp_path, terminal)
    reports = [_report(phase="call"), _report(phase="teardown")]
    for report in reports:
        plugin.pytest_runtest_logreport(report)

    identities = [
        _identity(line)
        for line in terminal.lines
        if line.startswith(runtime_profile.LIVE_FAILURE_PREFIX + "BEGIN ")
    ]
    assert [row["phase"] for row in identities] == ["call", "teardown"]
    assert [row["phase"] for row in plugin.phase_reports] == ["call", "teardown"]
    assert terminal.flush_count == 2
    assert [row["outcome"] for row in plugin.phase_reports] == ["failed", "failed"]
    assert all(report.outcome == "failed" for report in reports)


@pytest.mark.parametrize("source", ["gateway", "serial-master"])
def test_missing_worker_id_retains_existing_gateway_or_master_identity(
    tmp_path: Path, source: str
) -> None:
    terminal = _Terminal()
    plugin = _plugin(tmp_path, terminal)
    report = _report(worker_id=None)
    if source == "gateway":
        report.node = SimpleNamespace(gateway=SimpleNamespace(id="gw11"))
    plugin.pytest_runtest_logreport(report)
    expected = "gw11" if source == "gateway" else "master"
    begin = next(
        line
        for line in terminal.lines
        if line.startswith(runtime_profile.LIVE_FAILURE_PREFIX + "BEGIN ")
    )
    assert _identity(begin)["worker"] == expected
    assert plugin.phase_reports[0]["worker_id"] == expected


def test_missing_terminal_reporter_sends_complete_nonterminal_block_to_stderr(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    plugin = _plugin(tmp_path, None)
    plugin.pytest_runtest_logreport(_report())
    captured = capsys.readouterr()
    assert captured.out == ""
    lines = [line for line in captured.err.splitlines() if line]
    assert _identity(lines[0])["phase"] == "call"
    assert lines[-1] == runtime_profile.LIVE_FAILURE_PREFIX + "END"
    assert all(line.startswith(runtime_profile.LIVE_FAILURE_PREFIX) for line in lines)
    assert "SYNTHETIC_ROOT_CAUSE" in captured.err
    assert plugin.phase_reports[0]["outcome"] == "failed"


@pytest.mark.parametrize("failure_at", ["format", "write", "flush"])
def test_diagnostic_errors_leave_failed_report_authoritative(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure_at: str,
) -> None:
    terminal = _Terminal(fail_at=failure_at)
    plugin = _plugin(tmp_path, terminal)
    report = _report()
    if failure_at == "format":

        def broken_format(*args: object) -> list[str]:
            raise RuntimeError("synthetic formatting failure")

        monkeypatch.setattr(runtime_profile, "format_failure_diagnostic", broken_format)
    plugin.pytest_runtest_logreport(report)
    captured = capsys.readouterr()

    assert captured.out == ""
    assert runtime_profile.LIVE_FAILURE_PREFIX + "DIAGNOSTIC_UNAVAILABLE " in captured.err
    assert ("RuntimeError" if failure_at == "format" else "OSError") in captured.err
    assert report.outcome == "failed"
    assert plugin.phase_reports == [
        {
            "nodeid": report.nodeid,
            "phase": "call",
            "start": 100.0,
            "stop": 100.25,
            "duration": 0.25,
            "outcome": "failed",
            "worker_id": "gw7",
        }
    ]


def test_all_diagnostic_sinks_unavailable_do_not_raise_or_discard_telemetry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class UnavailableStderr:
        def write(self, text: str) -> None:
            raise OSError("synthetic stderr unavailable")

        def flush(self) -> None:
            raise OSError("synthetic stderr unavailable")

    plugin = _plugin(tmp_path, _Terminal(fail_at="write"))
    with monkeypatch.context() as context:
        context.setattr(runtime_profile.sys, "stderr", UnavailableStderr())
        plugin.pytest_runtest_logreport(_report())
    assert len(plugin.phase_reports) == 1
    assert plugin.phase_reports[0]["outcome"] == "failed"


@pytest.mark.parametrize("error", [KeyboardInterrupt, SystemExit])
def test_diagnostic_boundary_does_not_swallow_process_control(
    tmp_path: Path, error: type[BaseException]
) -> None:
    plugin = _plugin(tmp_path, _Terminal(fail_at="write", error=error))
    with pytest.raises(error):
        plugin.pytest_runtest_logreport(_report())
    assert plugin.phase_reports[0]["outcome"] == "failed"


def test_existing_telemetry_errors_are_not_hidden_by_diagnostic_exception_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    terminal = _Terminal()
    plugin = _plugin(tmp_path, terminal)
    report = _report()
    monkeypatch.setattr(report, "duration", "invalid existing telemetry")
    with pytest.raises((TypeError, ValueError)):
        plugin.pytest_runtest_logreport(report)
    assert plugin.phase_reports == []
    assert terminal.lines == []


def _reap_driver(process: subprocess.Popen[str]) -> None:
    try:
        process.wait(timeout=FINISH_WATCHDOG_SECONDS)
    except subprocess.TimeoutExpired:
        # The PID comes directly from this test's Popen. On timeout, also stop
        # its pytest/xdist descendants so a failing assertion cannot leak them.
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                check=False,
                capture_output=True,
                timeout=FINISH_WATCHDOG_SECONDS,
            )
        else:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=FINISH_WATCHDOG_SECONDS)
        raise


def test_real_16_worker_diagnostics_cross_runner_pipe_before_release_and_session_exit(
    tmp_path: Path,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    suite = tmp_path / "suite"
    suite.mkdir()
    config_path = tmp_path / "pytest.ini"
    config_path.write_text("[pytest]\n", encoding="utf-8")
    main_names = [
        "test_setup_error",
        "test_call_failure",
        "test_call_and_teardown_failure",
        "test_strict_xpass",
        "test_xfail_wrong_error",
        "test_pass",
        "test_skip",
        "test_expected_xfail",
        "test_nonstrict_xpass",
        "test_last_gate",
    ]
    (suite / "test_00_failures.py").write_text(
        textwrap.dedent(f"""\
            import time
            from pathlib import Path
            import pytest

            ROOT = Path(__file__).resolve().parent.parent

            @pytest.fixture
            def setup_error():
                raise RuntimeError("SETUP_LIVE_TOKEN")

            @pytest.fixture
            def teardown_error():
                yield
                raise RuntimeError("TEARDOWN_LIVE_TOKEN")

            def test_setup_error(setup_error):
                pass

            def test_call_failure():
                raise RuntimeError("CALL_LIVE_TOKEN")

            def test_call_and_teardown_failure(teardown_error):
                raise RuntimeError("DUAL_CALL_LIVE_TOKEN")

            @pytest.mark.xfail(strict=True, reason="STRICT_XPASS_LIVE_TOKEN")
            def test_strict_xpass():
                pass

            @pytest.mark.xfail(raises=ValueError, reason="different expected exception")
            def test_xfail_wrong_error():
                raise TypeError("WRONG_EXCEPTION_LIVE_TOKEN")

            def test_pass():
                assert True

            def test_skip():
                pytest.skip("ordinary skip")

            @pytest.mark.xfail(reason="expected assertion")
            def test_expected_xfail():
                assert False

            @pytest.mark.xfail(strict=False, reason="ordinary nonstrict XPASS")
            def test_nonstrict_xpass():
                pass

            def test_last_gate():
                (ROOT / "gate_entered").write_text("waiting", encoding="utf-8")
                deadline = time.monotonic() + {GATE_WATCHDOG_SECONDS}
                while not (ROOT / "release").exists():
                    if time.monotonic() >= deadline:
                        pytest.fail("test harness did not release the gate")
                    time.sleep(0.02)
                (ROOT / "gate_completed").write_text("passed", encoding="utf-8")
            """),
        encoding="utf-8",
    )
    # Sixteen file groups give every loadfile worker real work; no worker is
    # declared healthy merely because it collected tests but stayed inactive.
    aux_nodeids: list[str] = []
    for index in range(15):
        name = f"test_aux_{index:02d}.py"
        (suite / name).write_text("def test_auxiliary():\n    assert True\n", encoding="utf-8")
        aux_nodeids.append(f"suite/{name}::test_auxiliary")
    profile_path = tmp_path / "test_runtime_profile.json"
    result_path = tmp_path / "runner_result.json"
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-n",
        "16",
        "--dist",
        "loadfile",
        "-p",
        "xdist.plugin",
        "-p",
        "scripts.pytest_runtime_profile",
        "--aits-duration-profile",
        str(repo_root / "inputs/architecture/arch_004g2_full_duration_profile.yaml"),
        "--no-loadscope-reorder",
        "--rootdir",
        str(tmp_path),
        "--confcutdir",
        str(tmp_path),
        "-c",
        str(config_path),
        "suite",
        "-q",
        "--durations=1",
    ]
    driver = tmp_path / "run_streaming_validation.py"
    driver.write_text(
        textwrap.dedent("""\
            import json
            from pathlib import Path
            from scripts.run_validation_tier import _run_command

            root = Path(__file__).resolve().parent
            command = json.loads((root / "command.json").read_text(encoding="utf-8"))
            result = _run_command(command, cwd=root)
            (root / "runner_result.json").write_text(json.dumps(result), encoding="utf-8")
            raise SystemExit(result["exit_code"])
            """),
        encoding="utf-8",
    )
    (tmp_path / "command.json").write_text(json.dumps(command), encoding="utf-8")
    env = dict(os.environ)
    env.pop("PYTEST_ADDOPTS", None)
    env.pop("PYTEST_PLUGINS", None)
    env.pop(runtime_profile.RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV, None)
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    env["PYTHONPATH"] = os.pathsep.join([str(repo_root), str(repo_root / "src")])
    env[runtime_profile.RUNTIME_PROFILE_OUTPUT_ENV] = str(profile_path)
    env[runtime_profile.RUNTIME_PROFILE_FORMAL_SELECTION_ENV] = "0"
    process = subprocess.Popen(
        [sys.executable, str(driver)],
        cwd=tmp_path,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        start_new_session=os.name != "nt",
    )
    received: list[str] = []
    streamed: queue.Queue[str | None] = queue.Queue()

    def read_output() -> None:
        assert process.stdout is not None
        try:
            for line in process.stdout:
                received.append(line)
                streamed.put(line)
        finally:
            streamed.put(None)

    reader = threading.Thread(target=read_output, daemon=True)
    reader.start()
    prefix = runtime_profile.LIVE_FAILURE_PREFIX
    expected_failures = {
        ("test_setup_error", "setup"): "SETUP_LIVE_TOKEN",
        ("test_call_failure", "call"): "CALL_LIVE_TOKEN",
        ("test_call_and_teardown_failure", "call"): "DUAL_CALL_LIVE_TOKEN",
        ("test_call_and_teardown_failure", "teardown"): "TEARDOWN_LIVE_TOKEN",
        ("test_strict_xpass", "call"): "STRICT_XPASS_LIVE_TOKEN",
        ("test_xfail_wrong_error", "call"): "WRONG_EXCEPTION_LIVE_TOKEN",
    }
    begins: list[dict[str, str]] = []
    finished_blocks = 0
    deadline = time.monotonic() + STREAM_WATCHDOG_SECONDS
    try:
        while finished_blocks < len(expected_failures) or not (tmp_path / "gate_entered").exists():
            assert process.poll() is None, "driver exited before the live-diagnostic handshake"
            remaining = deadline - time.monotonic()
            assert remaining > 0, "live diagnostic watchdog expired:\n" + "".join(received)
            try:
                line = streamed.get(timeout=min(remaining, 0.1))
            except queue.Empty:
                continue
            assert line is not None, "stdout closed before release:\n" + "".join(received)
            if prefix in line:
                assert line.startswith(prefix), "diagnostic attached to a pytest progress line"
            if line.startswith(prefix + "BEGIN "):
                begins.append(_identity(line.rstrip("\n")))
            if line.rstrip("\n") == prefix + "END":
                finished_blocks += 1
        assert process.poll() is None
        assert not profile_path.exists()
        assert not result_path.exists()
        assert not (tmp_path / "gate_completed").exists()
        assert len(begins) == len(expected_failures)
        observed = {(row["nodeid"].split("::")[-1], row["phase"]) for row in begins}
        assert observed == set(expected_failures)
        live_output = "".join(received)
        assert all(token in live_output for token in expected_failures.values())
        assert "DIAGNOSTIC_UNAVAILABLE" not in live_output
    finally:
        (tmp_path / "release").write_text("release", encoding="utf-8")
        try:
            _reap_driver(process)
        finally:
            reader.join(timeout=FINISH_WATCHDOG_SECONDS)
            if process.stdout is not None:
                process.stdout.close()
        assert not reader.is_alive(), "stdout reader remained active after process cleanup"

    assert process.returncode == 1, "".join(received)
    assert (tmp_path / "gate_completed").read_text(encoding="utf-8") == "passed"
    result = json.loads(result_path.read_text(encoding="utf-8"))
    assert result["exit_code"] == 1
    assert result["command"] == command
    assert result["pytest_output"] == "".join(received)
    assert sum(line.startswith(prefix + "BEGIN ") for line in received) == len(expected_failures)
    assert sum(line.rstrip("\n") == prefix + "END" for line in received) == len(expected_failures)
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    assert profile["schema_version"] == "test_runtime_profile.v1"
    assert profile["pytest_exitstatus"] == 1
    assert profile["pytest_outcome_authoritative"] is True
    assert profile["pytest_outcome_overridden"] is False
    assert profile["profile_status"] == profile["telemetry_status"] == "PASS"
    assert profile["performance_evidence_status"] == "FAIL"
    assert profile["stable_full_improvement_claimed"] is False
    assert profile["scheduler"]["formal_full_selection_eligible"] is False
    assert profile["scheduler"]["expected_worker_count"] == 16
    assert profile["scheduler"]["xdist_dist"] == "loadfile"
    assert profile["scheduler"]["loadscope_reorder_disabled"] is True
    expected_nodeids = [f"suite/test_00_failures.py::{name}" for name in main_names] + aux_nodeids
    assert profile["collection"]["nodeids"] == expected_nodeids
    assert profile["collection"]["complete"] is True
    assert profile["collection"]["observed_worker_count"] == 16
    assert profile["node_count"] == 25
    assert profile["file_count"] == profile["worker_count"] == 16
    assert profile["telemetry"]["phase_report_count"] == 74
    assert profile["outcome_counts"] == {"failed": 5, "passed": 18, "skipped": 2}
    failed_phases = {
        (node["nodeid"], phase["phase"], phase["worker_id"])
        for node in profile["nodes"]
        for phase in node["phases"]
        if phase["outcome"] == "failed"
    }
    assert failed_phases == {(row["nodeid"], row["phase"], row["worker"]) for row in begins}


@pytest.mark.parametrize("long_name", [False, True], ids=["control-characters", "over-budget"])
def test_unavailable_notice_bounds_and_escapes_exception_type_from_real_format_failure(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    long_name: bool,
) -> None:
    exception_name = "FormatError\n999.99s call tests/test_forged.py::test_fake\x1b[31m"
    if long_name:
        exception_name += "X" * (runtime_profile.LIVE_FAILURE_IDENTITY_CHARS * 3)
        exception_name += "TAIL_CLASS_NAME"
    exception_type = type(exception_name, (Exception,), {})

    class BrokenRepresentation:
        reprcrash = "root summary available before traceback rendering"

        def __str__(self) -> str:
            raise exception_type("synthetic traceback rendering failure")

    representation = BrokenRepresentation()
    report = _report(longrepr=representation)
    plugin = _plugin(tmp_path, _Terminal())
    plugin.pytest_runtest_logreport(report)
    captured = capsys.readouterr()

    assert captured.out == ""
    notice_lines = [line for line in captured.err.splitlines() if line]
    assert len(notice_lines) == 1
    assert all(line.startswith(runtime_profile.LIVE_FAILURE_PREFIX) for line in notice_lines)
    assert all(control not in captured.err for control in ("\x1b", "\r", "\t", "\x00"))
    notice_prefix = runtime_profile.LIVE_FAILURE_PREFIX + "DIAGNOSTIC_UNAVAILABLE "
    assert notice_lines[0].startswith(notice_prefix)
    type_excerpt, _ = json.JSONDecoder().raw_decode(notice_lines[0].removeprefix(notice_prefix))
    assert isinstance(type_excerpt, str)
    assert "FormatError" in type_excerpt
    assert len(type_excerpt) <= runtime_profile.LIVE_FAILURE_IDENTITY_CHARS
    if long_name:
        assert "TRUNCATED" in type_excerpt
    else:
        assert "\\n" in captured.err
        assert "\\x1b" in captured.err
    original = "0.25s call tests/test_real.py::test_real\n"
    assert _parse_pytest_slow_durations(original + captured.err) == (
        _parse_pytest_slow_durations(original)
    )
    assert report.longrepr is representation
    assert report.outcome == "failed"
    assert len(plugin.phase_reports) == 1
    assert plugin.phase_reports[0]["outcome"] == "failed"
    assert plugin.phase_reports[0]["nodeid"] == report.nodeid


@pytest.mark.parametrize(
    ("field_name", "limit"),
    [
        ("nodeid", runtime_profile.LIVE_FAILURE_IDENTITY_CHARS),
        ("root", runtime_profile.LIVE_FAILURE_ROOT_CAUSE_CHARS),
        ("traceback", runtime_profile.LIVE_FAILURE_TRACEBACK_CHARS),
    ],
    ids=["identity-budget", "root-cause-budget", "traceback-budget"],
)
def test_line_clipping_cannot_expand_an_excerpt_past_its_character_budget(
    field_name: str, limit: int
) -> None:
    # A character-budget-length value can still exceed the line budget. The
    # truncation marker itself must fit both limits, including its newlines.
    newline_count = runtime_profile.LIVE_FAILURE_MAX_LINES + 1
    boundary_value = "A" * (limit - newline_count) + "\n" * newline_count
    assert len(boundary_value) == limit
    report = _report(
        nodeid=boundary_value if field_name == "nodeid" else "tests/test_boundary.py::test_case",
        longrepr=_Representation(
            boundary_value if field_name == "root" else "ROOT_TOKEN",
            boundary_value if field_name == "traceback" else "TRACEBACK_TOKEN",
        ),
    )
    lines = runtime_profile.format_failure_diagnostic(report, "gw7")
    excerpts = {
        "nodeid": _identity(lines[0])["nodeid"],
        "root": _excerpt(lines, "ROOT_CAUSE", "TRACEBACK"),
        "traceback": _excerpt(lines, "TRACEBACK", "END"),
    }
    selected = excerpts[field_name]
    assert len(selected) <= limit
    assert len(selected.splitlines()) <= runtime_profile.LIVE_FAILURE_MAX_LINES
    assert "TRUNCATED" in selected
    assert all(line.startswith(runtime_profile.LIVE_FAILURE_PREFIX) for line in lines)
