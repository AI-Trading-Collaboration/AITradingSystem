"""DATA-025: a failed capture component keeps the end of its error text.

The capture used to store only the first 600 characters of a failed command's stderr, so the final
exception line (the part that names the real cause) was lost. The summary, the blocker
classification and the retry rules keep working on those same first characters; the end of the
text is stored next to them in a new ``error_tail`` field.
"""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path

from test_daily_input_capture import _runner_for_components, _write_policy

import ai_trading_system.daily_input_capture as capture_module
from ai_trading_system.daily_input_capture import (
    build_daily_input_capture_components,
    capture_daily_inputs,
    daily_input_capture_paths,
    load_daily_input_capture_policy,
    validate_daily_input_capture_manifest,
)

SECRET = "SECRETVALUE9876"
ENV = {"FMP_API_KEY": SECRET, "PATH": "x"}


def test_summary_is_still_exactly_the_first_600_cleaned_characters() -> None:
    text = "Traceback (most recent call last):\r\n" + "frame line\n" * 300 + "FINAL"
    cleaned = text.replace("\r", " ").replace("\n", " ").strip()
    summary = capture_module._sanitize_error_summary(text, {})
    assert summary == cleaned[:600]
    assert len(summary) == 600
    assert "FINAL" not in summary
    assert capture_module._sanitize_error_summary("provider failure\n", {}) == "provider failure"


def test_tail_is_none_for_short_errors_and_present_from_601_characters() -> None:
    assert capture_module._error_tail("provider failure\n", {}) is None
    assert capture_module._error_tail("x" * 600, {}) is None
    assert capture_module._error_tail("x" * 601, {}) == "x" * 601
    assert capture_module._error_tail("", {}) is None


def test_tail_is_the_last_800_cleaned_characters() -> None:
    text = "start\n" + "middle line\n" * 400 + "requests.exceptions.SSLError: final cause\n"
    tail = capture_module._error_tail(text, {})
    cleaned = text.replace("\r", " ").replace("\n", " ").strip()
    assert tail == cleaned[-800:]
    assert len(tail) == 800
    assert tail.endswith("requests.exceptions.SSLError: final cause")
    assert "start" not in tail


def test_tail_redacts_secrets_before_cutting_even_across_the_cut() -> None:
    cleaned_length = 3000
    cut_at = cleaned_length - 800
    text = "a" * (cut_at - 5) + SECRET + "b" * (cleaned_length - cut_at - len(SECRET) + 5)
    assert len(text) == cleaned_length
    tail = capture_module._error_tail(text, ENV)
    assert tail is not None
    assert "SECRET" not in tail and "VALUE" not in tail and "9876" not in tail
    summary = capture_module._sanitize_error_summary(SECRET + "tail", ENV)
    assert SECRET not in summary and summary.startswith("[REDACTED]")
    assert "[REDACTED]" in capture_module._error_tail("z" * 700 + SECRET, ENV)


def test_tail_does_not_redact_names_that_are_not_secret_fragments() -> None:
    env = {"SEC_USER_AGENT": "agent contact", "FMP_API_KEY": ""}
    text = "q" * 700 + " agent contact"
    assert capture_module._error_tail(text, env).endswith("agent contact")


LONG_STDERR = (
    "Traceback (most recent call last):\n"
    '  File ".../urllib3/connection.py", line 796, in connect\n'
    "    sock_and_verified = _ssl_wrap_socket\n"
    + "    frame\n" * 120
    + "requests.exceptions.SSLError: final-cause-line 429 rate limit\n"
)


def _capture_with_long_sec_error(tmp_path: Path):
    policy_path = tmp_path / "capture_policy.yaml"
    _write_policy(policy_path, tmp_path)
    policy = load_daily_input_capture_policy(policy_path, project_root=tmp_path)
    paths = daily_input_capture_paths(date(2026, 7, 27), policy=policy)
    components = build_daily_input_capture_components(
        as_of=date(2026, 7, 27),
        paths=paths,
        policy=policy,
        project_root=tmp_path,
    )
    calls: list[str] = []
    base = _runner_for_components(components, failing_component="sec_companyfacts", calls=calls)

    def runner(command: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[str]:
        done = base(command, **kwargs)
        if "download-sec-companyfacts" in command:
            return subprocess.CompletedProcess(command, done.returncode, done.stdout, LONG_STDERR)
        return done

    result = capture_daily_inputs(
        as_of=date(2026, 7, 27),
        project_root=tmp_path,
        policy_path=policy_path,
        runner=runner,
        snapshotter=lambda _component: None,
        generated_at=datetime(2026, 7, 28, 0, 30, tzinfo=UTC),
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    return result, manifest, policy_path


def _component(manifest: dict, component_id: str) -> dict:
    return next(row for row in manifest["component_results"] if row["component_id"] == component_id)


def test_failed_component_records_the_tail_and_keeps_summary_and_blocker(tmp_path: Path) -> None:
    result, manifest, policy_path = _capture_with_long_sec_error(tmp_path)
    cleaned = LONG_STDERR.replace("\r", " ").replace("\n", " ").strip()

    sec = _component(manifest, "sec_companyfacts")
    assert sec["status"] == "FAIL"
    assert sec["error_summary"] == cleaned[:600]
    assert sec["error_summary"].startswith("Traceback (most recent call last):")
    assert "final-cause-line" not in sec["error_summary"]
    assert sec["error_tail"] == cleaned[-800:]
    assert sec["error_tail"].endswith("final-cause-line 429 rate limit")
    # the blocker is still classified from the first 600 characters ("ssl"), not from the tail
    # ("429 rate limit"), so retry decisions do not change
    assert sec["blocker_code"] == "PROVIDER_UNAVAILABLE"
    assert sec["stderr_line_count"] == len(LONG_STDERR.splitlines())

    attempt = sec["attempt_history"][-1]
    assert attempt["error_summary"] == sec["error_summary"]
    assert attempt["error_tail"] == sec["error_tail"]

    validation = validate_daily_input_capture_manifest(
        result.manifest_path, project_root=tmp_path, policy_path=policy_path
    )
    assert validation["status"] == "PASS"


def test_passing_and_short_failures_carry_no_tail(tmp_path: Path) -> None:
    _result, manifest, _policy = _capture_with_long_sec_error(tmp_path)
    for component_id in ("market_macro", "fmp_forward_pit", "fmp_valuation"):
        row = _component(manifest, component_id)
        assert row["status"] == "PASS"
        assert row.get("error_tail") is None


def test_short_failure_text_has_no_tail(tmp_path: Path) -> None:
    policy_path = tmp_path / "capture_policy.yaml"
    _write_policy(policy_path, tmp_path)
    policy = load_daily_input_capture_policy(policy_path, project_root=tmp_path)
    paths = daily_input_capture_paths(date(2026, 7, 27), policy=policy)
    components = build_daily_input_capture_components(
        as_of=date(2026, 7, 27), paths=paths, policy=policy, project_root=tmp_path
    )
    result = capture_daily_inputs(
        as_of=date(2026, 7, 27),
        project_root=tmp_path,
        policy_path=policy_path,
        runner=_runner_for_components(components, failing_component="sec_companyfacts", calls=[]),
        snapshotter=lambda _component: None,
        generated_at=datetime(2026, 7, 28, 0, 30, tzinfo=UTC),
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    sec = _component(manifest, "sec_companyfacts")
    assert sec["error_summary"] == "provider failure"
    assert sec["error_tail"] is None
