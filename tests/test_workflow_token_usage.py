from __future__ import annotations

import copy
import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.reports.workflow_token_usage import (
    collect_workflow_token_usage,
    validate_workflow_token_usage,
)

START = datetime(2026, 8, 31, tzinfo=UTC)
END = START + timedelta(days=7)
LIMITS = {
    "max_files": 20,
    "max_total_bytes": 100_000,
    "max_line_bytes": 10_000,
    "max_responses": 100,
}


def _meta(project: Path | str, thread: str = "thread-a") -> dict[str, Any]:
    return {
        "type": "session_meta",
        "timestamp": (START - timedelta(days=14)).isoformat(),
        "payload": {"cwd": str(project), "id": thread},
    }


def _response(
    response_id: str = "resp-a", timestamp: datetime = START, thread: str = "thread-a"
) -> dict[str, Any]:
    return {
        "type": "token_usage_record",
        "timestamp": timestamp.isoformat(),
        "payload": {
            "response_id": response_id,
            "thread_id": thread,
            "usage": {
                "input_tokens": 100,
                "cached_input_tokens": 80,
                "output_tokens": 20,
                "reasoning_output_tokens": 5,
                "total_tokens": 120,
            },
            "thread_token_usage": {"total_tokens": 9_000_000},
        },
    }


def _write(path: Path, *events: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
    return path


def _collect(
    root: Path | list[Path], project: Path | str, limits: dict[str, int] | None = None
) -> dict[str, Any]:
    result = collect_workflow_token_usage(
        log_roots=root if isinstance(root, list) else [root],
        project_roots=[Path(project)],
        window_start=START,
        window_end=END,
        limits=limits or LIMITS,
    )
    assert validate_workflow_token_usage(result) == []
    return result


def _gaps(result: dict[str, Any]) -> set[str]:
    return {row["code"] for row in result["telemetry_gaps"]}


def test_unique_responses_cache_and_reasoning_are_not_double_counted(tmp_path: Path) -> None:
    project = tmp_path / "project"
    logs = tmp_path / "logs"
    event = _response()
    first = _write(logs / "2026-09-01.jsonl", _meta(project), event)
    _write(
        logs / "2026-09-02.jsonl",
        _meta(project),
        event,
        _response("resp-b"),
        {"type": "event_msg", "payload": {"type": "token_count", "total_tokens": 9_000_000}},
    )
    result = _collect([logs, first], project)
    assert result["status"] == "COMPLETE"
    assert result["response_count"] == 2
    assert result["duplicate_response_count"] == 1
    assert result["source_count"] == 2
    assert result["totals"] == {
        "input_tokens": 200,
        "cached_input_tokens": 160,
        "uncached_input_tokens": 40,
        "output_tokens": 40,
        "reasoning_output_tokens": 10,
        "total_tokens": 240,
    }


def test_conflicting_identity_removes_every_copy_even_across_files(tmp_path: Path) -> None:
    project, logs = tmp_path / "project", tmp_path / "logs"
    changed = _response()
    changed["payload"]["usage"]["cached_input_tokens"] = 79
    _write(logs / "a.jsonl", _meta(project), _response(), _response())
    _write(logs / "b.jsonl", _meta(project), changed, _response("safe"))
    result = _collect(logs, project)
    assert result["status"] == "PARTIAL"
    assert result["response_count"] == 1
    assert result["totals"]["total_tokens"] == 120
    assert "CONFLICTING_RESPONSE_ID" in _gaps(result)
    assert result["coverage"]["excluded_response_ids"] == 1


@pytest.mark.parametrize("bad_first", [True, False])
def test_invalid_duplicate_poisoning_is_independent_of_record_order(
    tmp_path: Path, bad_first: bool
) -> None:
    bad = _response()
    bad["payload"]["usage"]["input_tokens"] = False
    events = [bad, _response()] if bad_first else [_response(), bad]
    source = _write(tmp_path / "log.jsonl", _meta(tmp_path), *events)
    result = _collect(source, tmp_path)
    assert result["response_count"] == 0
    assert result["totals"]["total_tokens"] == 0
    assert "INVALID_USAGE_COUNTS" in _gaps(result)


def test_project_match_is_exact_and_other_project_body_is_never_read(tmp_path: Path) -> None:
    project, logs = tmp_path / "project", tmp_path / "logs"
    _write(logs / "own.jsonl", _meta(project), _response())
    for index, other in enumerate((tmp_path / "project-other", project / "child")):
        path = _write(logs / f"other-{index}.jsonl", _meta(other))
        with path.open("a", encoding="utf-8") as stream:
            stream.write("PRIVATE OTHER PROJECT SECRET" * 100_000)
    result = _collect(logs, project)
    assert result["status"] == "COMPLETE"
    assert result["response_count"] == 1
    assert result["coverage"]["other_project_files_excluded"] == 2
    assert result["coverage"]["bytes_read"] < 2000
    assert result["source_count"] == 1
    assert "other-" not in json.dumps(result)


def test_windows_cwd_case_and_separators_normalize_without_prefix_match(tmp_path: Path) -> None:
    _write(tmp_path / "a.jsonl", _meta("d:\\WORK\\Project\\"), _response())
    _write(tmp_path / "b.jsonl", _meta("D:/work/project-other"), _response("other"))
    result = _collect(tmp_path, "D:/Work/PROJECT")
    assert result["status"] == "COMPLETE"
    assert result["response_count"] == 1


def test_fork_history_is_excluded_from_own_session_usage(tmp_path: Path) -> None:
    source = _write(
        tmp_path / "log.jsonl",
        _meta(tmp_path),
        _response(thread="parent-thread"),
        _response("own-response"),
    )
    result = _collect(source, tmp_path)
    assert result["response_count"] == 1
    assert result["coverage"]["inherited_response_records_excluded"] == 1
    assert result["status"] == "COMPLETE"


def test_window_is_utc_right_open_and_not_inferred_from_mtime(tmp_path: Path) -> None:
    source = _write(
        tmp_path / "log.jsonl",
        _meta(tmp_path),
        _response("before", START - timedelta(microseconds=1)),
        _response("first", START),
        _response("last", END - timedelta(microseconds=1)),
        _response("after", END),
    )
    os.utime(source, (0, 0))
    result = _collect(source, tmp_path)
    assert result["response_count"] == 2
    assert result["coverage"]["unique_responses_outside_window"] == 2
    assert result["window"]["start_inclusive_utc"] == START.isoformat()


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("input_tokens", True),
        ("input_tokens", -1),
        ("output_tokens", 20.0),
        ("cached_input_tokens", 101),
        ("reasoning_output_tokens", 21),
        ("total_tokens", 121),
        ("total_tokens", "120"),
        ("input_tokens", None),
    ],
)
def test_invalid_counts_are_gaps_not_zero_measurements(
    tmp_path: Path, key: str, value: Any
) -> None:
    event = _response()
    event["payload"]["usage"][key] = value
    source = _write(tmp_path / "log.jsonl", _meta(tmp_path), event)
    result = _collect(source, tmp_path)
    assert result["status"] == "PARTIAL"
    assert result["response_count"] == 0
    assert "INVALID_USAGE_COUNTS" in _gaps(result)


@pytest.mark.parametrize("timestamp", [None, "2026-09-01T01:00:00", "not-a-time"])
def test_invalid_event_timestamp_is_explicit_gap(tmp_path: Path, timestamp: Any) -> None:
    event = _response()
    event["timestamp"] = timestamp
    source = _write(tmp_path / "log.jsonl", _meta(tmp_path), event)
    result = _collect(source, tmp_path)
    assert result["response_count"] == 0
    assert "INVALID_RESPONSE_TIMESTAMP" in _gaps(result)


@pytest.mark.parametrize("field", ["thread_id", "response_id"])
def test_missing_response_identity_is_not_admitted(tmp_path: Path, field: str) -> None:
    event = _response()
    del event["payload"][field]
    source = _write(tmp_path / "log.jsonl", _meta(tmp_path), event)
    result = _collect(source, tmp_path)
    assert result["response_count"] == 0
    assert result["status"] == "PARTIAL"


def test_json_errors_are_reported_without_content(tmp_path: Path) -> None:
    source = _write(tmp_path / "log.jsonl", _meta(tmp_path), _response())
    with source.open("ab") as stream:
        stream.write(
            b'PRIVATE MALFORMED SECRET\n{"type":"event_msg","type":"token_usage_record"}\n'
        )
    result = _collect(source, tmp_path)
    assert result["response_count"] == 1
    assert "INVALID_JSON_RECORD" in _gaps(result)
    assert "PRIVATE" not in json.dumps(result)


@pytest.mark.parametrize("first", [{"type": "event_msg"}, _meta("relative/path")])
def test_missing_or_invalid_initial_attribution_stops_file(tmp_path: Path, first: Any) -> None:
    source = _write(tmp_path / "log.jsonl", first, _response())
    result = _collect(source, tmp_path)
    assert result["status"] == "UNAVAILABLE"
    assert result["source_count"] == 0
    assert result["coverage"]["usage_records_examined"] == 0


def test_repeated_metadata_invalidates_the_entire_source(tmp_path: Path) -> None:
    source = _write(tmp_path / "log.jsonl", _meta(tmp_path), _response(), _meta(tmp_path / "other"))
    result = _collect(source, tmp_path)
    assert result["response_count"] == 0
    assert result["sources"][0]["stable"] is False
    assert "CONFLICTING_SESSION_META" in _gaps(result)


def test_same_identity_repeated_metadata_preserves_resumed_session(tmp_path: Path) -> None:
    repeated = _meta(tmp_path)
    repeated["timestamp"] = (START + timedelta(days=1)).isoformat()
    repeated["payload"]["new_non_identity_metadata"] = "not exported"
    source = _write(
        tmp_path / "log.jsonl", _meta(tmp_path), _response(), repeated, _response("after-resume")
    )
    result = _collect(source, tmp_path)
    assert result["status"] == "COMPLETE"
    assert result["response_count"] == 2
    assert result["coverage"]["repeated_session_meta_records"] == 1


def test_changed_session_id_invalidates_the_entire_source(tmp_path: Path) -> None:
    source = _write(
        tmp_path / "log.jsonl", _meta(tmp_path), _response(), _meta(tmp_path, "other-thread")
    )
    result = _collect(source, tmp_path)
    assert result["response_count"] == 0
    assert "CONFLICTING_SESSION_META" in _gaps(result)


@pytest.mark.parametrize("offset", [timedelta(), timedelta(days=1)])
def test_creation_at_or_after_window_end_skips_body_using_real_timestamp(
    tmp_path: Path, offset: timedelta
) -> None:
    logs = tmp_path / "logs"
    # Misleading old filename cannot override real metadata creation time.
    future = _meta(tmp_path)
    future["payload"]["timestamp"] = (END + offset).isoformat()
    path = _write(logs / "2000-01-01.jsonl", future)
    with path.open("a", encoding="utf-8") as stream:
        stream.write("LARGE SECRET POST-WINDOW BODY" * 100_000)
    _write(logs / "2000-01-02.jsonl", _meta(tmp_path), _response())
    result = _collect(logs, tmp_path)
    assert result["status"] == "COMPLETE"
    assert result["response_count"] == 1
    assert result["coverage"]["sessions_created_after_window_excluded"] == 1
    assert result["coverage"]["bytes_read"] < 2000
    assert "SECRET" not in json.dumps(result)


def test_old_long_running_session_is_scanned_despite_misleading_future_filename(
    tmp_path: Path,
) -> None:
    source = _write(
        tmp_path / "2099-01-01.jsonl",
        _meta(tmp_path),
        _response("old", START - timedelta(days=1)),
        _response("within"),
    )
    result = _collect(source, tmp_path)
    assert result["status"] == "COMPLETE"
    assert result["response_count"] == 1
    assert result["coverage"]["sessions_created_after_window_excluded"] == 0


@pytest.mark.parametrize("creation", [None, "2026-08-01T00:00:00", "invalid"])
def test_invalid_creation_time_scans_with_gap_instead_of_filename_inference(
    tmp_path: Path, creation: Any
) -> None:
    meta = _meta(tmp_path)
    meta["payload"]["timestamp"] = creation
    source = _write(tmp_path / "2099-01-01.jsonl", meta, _response())
    result = _collect(source, tmp_path)
    assert result["status"] == "PARTIAL"
    assert result["response_count"] == 1
    assert "INVALID_SESSION_CREATION_TIMESTAMP" in _gaps(result)


def test_response_cannot_predate_known_session_creation(tmp_path: Path) -> None:
    meta = _meta(tmp_path)
    meta["payload"]["timestamp"] = (START + timedelta(days=1)).isoformat()
    source = _write(tmp_path / "log.jsonl", meta, _response())
    result = _collect(source, tmp_path)
    assert result["response_count"] == 0
    assert "RESPONSE_PRECEDES_SESSION_CREATION" in _gaps(result)


def test_line_budget_reads_no_more_than_bounded_sentinel(tmp_path: Path) -> None:
    source = _write(
        tmp_path / "log.jsonl",
        _meta(tmp_path),
        _response(),
        {"type": "event_msg", "payload": "PRIVATE" * 1000},
    )
    result = _collect(source, tmp_path, {**LIMITS, "max_line_bytes": 500})
    assert result["response_count"] == 1
    assert "LINE_BYTE_LIMIT_REACHED" in _gaps(result)
    assert result["sources"][0]["hash_scope"] == "READ_PREFIX"
    assert "PRIVATE" not in json.dumps(result)


def test_total_byte_budget_is_global_and_exact(tmp_path: Path) -> None:
    meta = _meta(tmp_path)
    first = _response()
    source = _write(tmp_path / "log.jsonl", meta, first, _response("second"))
    byte_limit = len((json.dumps(meta) + "\n" + json.dumps(first) + "\n").encode()) + 10
    result = _collect(source, tmp_path, {**LIMITS, "max_total_bytes": byte_limit})
    assert result["response_count"] == 1
    assert result["coverage"]["bytes_read"] == byte_limit
    assert "TOTAL_BYTE_LIMIT_REACHED" in _gaps(result)
    assert result["sources"][0]["hash_scope"] == "READ_PREFIX"


def test_response_budget_counts_duplicates_and_marks_partial(tmp_path: Path) -> None:
    source = _write(tmp_path / "log.jsonl", _meta(tmp_path), _response(), _response())
    result = _collect(source, tmp_path, {**LIMITS, "max_responses": 1})
    assert result["response_count"] == 1
    assert result["coverage"]["usage_records_examined"] == 1
    assert "RESPONSE_LIMIT_REACHED" in _gaps(result)


def test_file_budget_applies_across_roots_and_prefers_stable_newer_filename(tmp_path: Path) -> None:
    first = _write(tmp_path / "2026-08-01.jsonl", _meta(tmp_path), _response("old"))
    last = _write(tmp_path / "2026-09-01.jsonl", _meta(tmp_path), _response("new"))
    result = _collect([first, last], tmp_path, {**LIMITS, "max_files": 1})
    assert result["source_count"] == 1
    assert result["sources"][0]["path"] == str(last)
    assert "FILE_LIMIT_REACHED" in _gaps(result)


def test_directory_enumeration_is_bounded_and_deterministic(tmp_path: Path) -> None:
    for name in ("c", "a", "b"):
        _write(tmp_path / f"{name}.jsonl", _meta(tmp_path), _response(name))
    result = _collect(tmp_path, tmp_path, {**LIMITS, "max_files": 2})
    assert result["status"] == "UNAVAILABLE"
    assert result["coverage"]["files_examined"] == 0
    assert "DIRECTORY_ENTRY_LIMIT_REACHED" in _gaps(result)


def test_source_hash_binds_only_reported_read_bytes_and_never_prompt(tmp_path: Path) -> None:
    source = _write(
        tmp_path / "log.jsonl",
        _meta(tmp_path),
        _response(),
        {"type": "event_msg", "payload": {"message": "SECRET USER PROMPT"}},
    )
    result = _collect(source, tmp_path)
    identity = result["sources"][0]
    assert identity["sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert identity["bytes_read"] == source.stat().st_size
    assert identity["hash_scope"] == "ENTIRE_FILE"
    assert "SECRET" not in json.dumps(result)


def test_source_changed_during_scan_is_not_admitted(tmp_path: Path, monkeypatch: Any) -> None:
    source = _write(tmp_path / "log.jsonl", _meta(tmp_path), _response())
    real_fstat = os.fstat
    calls = 0

    def changing_fstat(fd: int) -> os.stat_result:
        nonlocal calls
        calls += 1
        if calls == 2:
            with source.open("ab") as stream:
                stream.write(b"\n")
        return real_fstat(fd)

    monkeypatch.setattr(os, "fstat", changing_fstat)
    result = _collect(source, tmp_path)
    assert result["response_count"] == 0
    assert "SOURCE_CHANGED_DURING_SCAN" in _gaps(result)
    assert result["sources"][0]["stable"] is False


def test_symlink_escape_is_never_opened(tmp_path: Path) -> None:
    outside = _write(tmp_path / "outside.jsonl", _meta(tmp_path), _response())
    logs = tmp_path / "logs"
    logs.mkdir()
    link = logs / "escape.jsonl"
    try:
        link.symlink_to(outside)
    except OSError as exc:
        pytest.skip(f"host does not permit symlink creation: {exc.winerror}")
    result = _collect(logs, tmp_path)
    assert result["coverage"]["bytes_read"] == 0
    assert "UNSAFE_PATH" in _gaps(result)


def test_missing_logs_and_missing_instrumentation_are_explicit(tmp_path: Path) -> None:
    missing = _collect(tmp_path / "missing", tmp_path)
    assert missing["status"] == "UNAVAILABLE"
    assert "LOG_ROOT_UNAVAILABLE" in _gaps(missing)
    source = _write(tmp_path / "log.jsonl", _meta(tmp_path), {"type": "event_msg"})
    empty = _collect(source, tmp_path)
    assert empty["status"] == "PARTIAL"
    assert "NO_RESPONSE_USAGE_RECORDS" in _gaps(empty)


@pytest.mark.parametrize("change", ["naive", "reversed", "boolean_limit", "no_projects"])
def test_invalid_collection_contract_fails_before_read(tmp_path: Path, change: str) -> None:
    arguments: dict[str, Any] = {
        "log_roots": [tmp_path],
        "project_roots": [tmp_path],
        "window_start": START,
        "window_end": END,
        "limits": LIMITS,
    }
    if change == "naive":
        arguments["window_start"] = START.replace(tzinfo=None)
    elif change == "reversed":
        arguments["window_end"] = START
    elif change == "boolean_limit":
        arguments["limits"] = {**LIMITS, "max_files": True}
    else:
        arguments["project_roots"] = []
    with pytest.raises(ValueError):
        collect_workflow_token_usage(**arguments)


@pytest.mark.parametrize(
    "change", ["total", "boolean", "source_count", "source_prompt", "status", "window", "budget"]
)
def test_validator_rejects_tampering(tmp_path: Path, change: str) -> None:
    source = _write(tmp_path / "log.jsonl", _meta(tmp_path), _response())
    payload = copy.deepcopy(_collect(source, tmp_path))
    if change == "total":
        payload["totals"]["total_tokens"] += 1
    elif change == "boolean":
        payload["response_count"] = True
    elif change == "source_count":
        payload["source_count"] += 1
    elif change == "source_prompt":
        payload["sources"][0]["prompt"] = "SECRET"
    elif change == "status":
        payload["status"] = "PARTIAL"
    elif change == "window":
        payload["window"]["end_exclusive_utc"] = "2026-09-01T00:00:00"
    else:
        payload["coverage"]["bytes_read"] = LIMITS["max_total_bytes"] + 1
    errors = validate_workflow_token_usage(payload)
    assert errors
    assert "SECRET" not in str(errors)


@pytest.mark.parametrize("change", ["unstable", "source_bytes", "too_many_responses"])
def test_validator_checks_source_and_coverage_consistency(tmp_path: Path, change: str) -> None:
    source = _write(tmp_path / "log.jsonl", _meta(tmp_path), _response())
    payload = _collect(source, tmp_path)
    if change == "unstable":
        payload["sources"][0]["stable"] = False
    elif change == "source_bytes":
        payload["coverage"]["bytes_read"] = 0
    else:
        payload["response_count"] = payload["coverage"]["usage_records_examined"] + 1
    assert validate_workflow_token_usage(payload)
