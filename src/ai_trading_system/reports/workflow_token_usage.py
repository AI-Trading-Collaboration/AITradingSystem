"""Bounded, read-only response telemetry; raw conversation text is never returned.

COMPLETE describes the explicitly supplied log roots, not account-wide usage.
Budgets count examined files/response records, including excluded records. Directory
enumeration is bounded too: an oversized directory is rejected as a whole instead
of selecting an OS-dependent subset. No timestamp is inferred from file mtime.
"""

from __future__ import annotations

import hashlib
import json
import ntpath
import os
import stat
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "workflow_token_usage.v1"
_TOKEN_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
_LIMIT_FIELDS = ("max_files", "max_total_bytes", "max_line_bytes", "max_responses")


@dataclass(frozen=True)
class _Response:
    timestamp: datetime
    thread_id: str
    usage: tuple[int, ...]


@dataclass
class _Scan:
    limits: Mapping[str, int]
    project_roots: set[str]
    window_end: datetime
    gaps: Counter[str] = field(default_factory=Counter)
    coverage: Counter[str] = field(default_factory=Counter)
    sources: list[dict[str, Any]] = field(default_factory=list)
    responses: dict[str, _Response | None] = field(default_factory=dict)
    duplicate_count: int = 0
    stopped: bool = False

    def admit(self, response_id: str, response: _Response | None) -> None:
        if response_id not in self.responses:
            self.responses[response_id] = response
        elif self.responses[response_id] is None:
            return
        elif response is None or self.responses[response_id] != response:
            self.responses[response_id] = None
            self.gaps["CONFLICTING_RESPONSE_ID"] += 1
        else:
            self.duplicate_count += 1


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("token usage timestamps must be timezone-aware datetimes")
    return value.astimezone(UTC)


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("missing timestamp")
    return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _project_identity(value: str) -> str:
    # Windows drive/UNC paths may also occur in portable synthetic fixtures.
    drive, _ = ntpath.splitdrive(value)
    if drive:
        if not ntpath.isabs(value):
            raise ValueError("project cwd must be absolute")
        return ntpath.normcase(ntpath.normpath(value))
    path = Path(value)
    if not path.is_absolute():
        raise ValueError("project cwd must be absolute")
    return os.path.normcase(os.path.normpath(value))


def _safe_path(path: Path, root: Path) -> bool:
    """Reject links/junctions in every ancestor before resolving or opening bytes."""
    try:
        for component in (path, *path.parents):
            info = component.lstat()
            if stat.S_ISLNK(info.st_mode) or (
                getattr(info, "st_file_attributes", 0)
                & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
            ):
                return False
        path.resolve(strict=True).relative_to(root.resolve(strict=True))
        return True
    except (OSError, ValueError, RuntimeError):
        return False


def _discover(root: Path, scan: _Scan) -> list[Path]:
    if not root.exists():
        scan.gaps["LOG_ROOT_UNAVAILABLE"] += 1
        return []
    if not _safe_path(root, root):
        scan.gaps["UNSAFE_PATH"] += 1
        return []
    if root.is_file():
        return [root] if root.suffix.lower() == ".jsonl" else []
    files: list[Path] = []
    pending = [root]
    visited = 0
    while pending:
        directory = pending.pop()
        visited += 1
        if visited > scan.limits["max_files"]:
            scan.gaps["DIRECTORY_LIMIT_REACHED"] += 1
            break
        entries: list[Path] = []
        try:
            with os.scandir(directory) as iterator:
                for entry in iterator:
                    entries.append(Path(entry.path))
                    if len(entries) > scan.limits["max_files"]:
                        break
        except OSError:
            scan.gaps["DIRECTORY_UNREADABLE"] += 1
            continue
        if len(entries) > scan.limits["max_files"]:
            scan.gaps["DIRECTORY_ENTRY_LIMIT_REACHED"] += 1
            continue
        for path in sorted(entries, key=lambda item: (str(item).casefold(), str(item))):
            if not _safe_path(path, root):
                scan.gaps["UNSAFE_PATH"] += 1
            elif path.is_dir():
                pending.append(path)
            elif path.is_file() and path.suffix.lower() == ".jsonl":
                files.append(path)
                if len(files) > scan.limits["max_files"]:
                    scan.gaps["FILE_DISCOVERY_LIMIT_REACHED"] += 1
                    # Reject this root's incomplete inventory deterministically.
                    return []
    # Stable date/file names prioritize recent logs without relying on mtime.
    return sorted(files, key=lambda item: (str(item).casefold(), str(item)), reverse=True)


def _usage(raw: Any) -> tuple[int, ...]:
    if not isinstance(raw, Mapping):
        raise ValueError("usage must be a mapping")
    values: list[int] = []
    for key in _TOKEN_FIELDS:
        value = raw.get(key)
        if type(value) is not int or value < 0:
            raise ValueError("usage must contain nonnegative integer counts")
        values.append(value)
    input_tokens, cached, output, reasoning, total = values
    if cached > input_tokens or reasoning > output or total != input_tokens + output:
        raise ValueError("inconsistent usage counts")
    return tuple(values)


def _unique_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _record(
    event: Mapping[str, Any], thread_id: str, created_at: datetime | None, scan: _Scan
) -> tuple[str, _Response | None] | None:
    payload = event.get("payload")
    if not isinstance(payload, Mapping):
        scan.gaps["INVALID_USAGE_PAYLOAD"] += 1
        return None
    response_thread = payload.get("thread_id")
    if not isinstance(response_thread, str) or not response_thread.strip():
        scan.gaps["MISSING_RESPONSE_THREAD_ID"] += 1
        return None
    if response_thread != thread_id:
        scan.coverage["inherited_response_records_excluded"] += 1
        return None
    response_id = payload.get("response_id")
    if not isinstance(response_id, str) or not response_id.strip():
        scan.gaps["MISSING_RESPONSE_ID"] += 1
        return None
    try:
        timestamp = _timestamp(event.get("timestamp"))
    except (ValueError, OverflowError):
        scan.gaps["INVALID_RESPONSE_TIMESTAMP"] += 1
        return response_id, None
    if created_at is not None and timestamp < created_at:
        scan.gaps["RESPONSE_PRECEDES_SESSION_CREATION"] += 1
        return response_id, None
    try:
        usage = _usage(payload.get("usage"))
    except (ValueError, TypeError, OverflowError):
        scan.gaps["INVALID_USAGE_COUNTS"] += 1
        return response_id, None
    return response_id, _Response(timestamp, thread_id, usage)


def _session_created_at(event: Mapping[str, Any], meta: Mapping[str, Any]) -> datetime:
    # payload.timestamp is the session creation field. Older records expose only
    # the first metadata event timestamp; never substitute a filename or mtime.
    return _timestamp(meta["timestamp"] if "timestamp" in meta else event.get("timestamp"))


def _same_session(meta: Any, thread_id: str, identity: str) -> bool:
    if not isinstance(meta, Mapping) or meta.get("id") != thread_id:
        return False
    try:
        return isinstance(meta.get("cwd"), str) and _project_identity(meta["cwd"]) == identity
    except ValueError:
        return False


def _read_source(path: Path, root: Path, scan: _Scan) -> None:
    if not _safe_path(path, root):
        scan.gaps["UNSAFE_PATH"] += 1
        return
    scan.coverage["files_examined"] += 1
    source: dict[str, Any] | None = None
    rows: list[tuple[str, _Response | None]] = []
    digest = hashlib.sha256()
    bytes_read = 0
    line_number = 0
    thread_id: str | None = None
    created_at: datetime | None = None
    identity: str | None = None
    stable = True
    try:
        before = path.stat()
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        with os.fdopen(os.open(path, flags), "rb") as stream:
            opened = os.fstat(stream.fileno())
            if not stat.S_ISREG(opened.st_mode) or (before.st_dev, before.st_ino) != (
                opened.st_dev,
                opened.st_ino,
            ):
                scan.gaps["SOURCE_IDENTITY_CHANGED"] += 1
                return
            while bytes_read < opened.st_size:
                remaining = scan.limits["max_total_bytes"] - scan.coverage["bytes_read"]
                if remaining <= 0:
                    scan.gaps["TOTAL_BYTE_LIMIT_REACHED"] += 1
                    scan.stopped = True
                    break
                raw = stream.readline(min(scan.limits["max_line_bytes"] + 1, remaining))
                if not raw:
                    break
                line_number += 1
                bytes_read += len(raw)
                scan.coverage["bytes_read"] += len(raw)
                digest.update(raw)
                if len(raw) > scan.limits["max_line_bytes"]:
                    scan.gaps["LINE_BYTE_LIMIT_REACHED"] += 1
                    break
                if not raw.endswith(b"\n") and bytes_read < opened.st_size:
                    scan.gaps["TOTAL_BYTE_LIMIT_REACHED"] += 1
                    scan.stopped = True
                    break
                try:
                    event = json.loads(raw.decode("utf-8-sig"), object_pairs_hook=_unique_json_keys)
                    if not isinstance(event, Mapping):
                        raise ValueError("event must be a mapping")
                except (UnicodeError, ValueError, RecursionError):
                    scan.gaps["INVALID_JSON_RECORD"] += 1
                    if thread_id is None:
                        break
                    continue
                if thread_id is None:
                    meta = event.get("payload")
                    if event.get("type") != "session_meta" or not isinstance(meta, Mapping):
                        scan.gaps["MISSING_INITIAL_SESSION_META"] += 1
                        break
                    try:
                        cwd = meta.get("cwd")
                        if not isinstance(cwd, str):
                            raise ValueError("missing cwd")
                        identity = _project_identity(cwd)
                    except ValueError:
                        scan.gaps["INVALID_PROJECT_ATTRIBUTION"] += 1
                        break
                    if identity not in scan.project_roots:
                        scan.coverage["other_project_files_excluded"] += 1
                        return
                    if not isinstance(meta.get("id"), str) or not meta["id"].strip():
                        scan.gaps["MISSING_SESSION_ID"] += 1
                        break
                    thread_id = meta["id"]
                    try:
                        created_at = _session_created_at(event, meta)
                    except (ValueError, OverflowError):
                        scan.gaps["INVALID_SESSION_CREATION_TIMESTAMP"] += 1
                    if created_at is not None and created_at >= scan.window_end:
                        # Own responses cannot predate their session creation.
                        # Forked history is separately excluded by thread id.
                        scan.coverage["sessions_created_after_window_excluded"] += 1
                        return
                    source = {"path": str(path), "file_size_bytes": opened.st_size}
                elif event.get("type") == "session_meta":
                    if identity is None or not _same_session(
                        event.get("payload"), thread_id, identity
                    ):
                        scan.gaps["CONFLICTING_SESSION_META"] += 1
                        stable = False
                        break
                    scan.coverage["repeated_session_meta_records"] += 1
                elif event.get("type") == "token_usage_record":
                    if scan.coverage["usage_records_examined"] >= scan.limits["max_responses"]:
                        scan.gaps["RESPONSE_LIMIT_REACHED"] += 1
                        scan.stopped = True
                        break
                    scan.coverage["usage_records_examined"] += 1
                    parsed = _record(event, thread_id, created_at, scan)
                    if parsed is not None:
                        rows.append(parsed)
            after = os.fstat(stream.fileno())
        current = path.stat()
        if any(
            (item.st_size, item.st_mtime_ns, item.st_dev, item.st_ino)
            != (opened.st_size, opened.st_mtime_ns, opened.st_dev, opened.st_ino)
            for item in (after, current)
        ):
            scan.gaps["SOURCE_CHANGED_DURING_SCAN"] += 1
            stable = False
        if opened.st_size == 0:
            scan.gaps["MISSING_INITIAL_SESSION_META"] += 1
    except OSError:
        scan.gaps["SOURCE_UNREADABLE"] += 1
        stable = False
    if source is not None:
        source.update(
            bytes_read=bytes_read,
            sha256=digest.hexdigest(),
            hash_scope="ENTIRE_FILE" if bytes_read == source["file_size_bytes"] else "READ_PREFIX",
            lines_examined=line_number,
            usage_records_observed=len(rows),
            stable=stable,
        )
        scan.sources.append(source)
        if stable:
            for response_id, response in rows:
                scan.admit(response_id, response)


def collect_workflow_token_usage(
    *,
    log_roots: Sequence[Path],
    project_roots: Sequence[Path],
    window_start: datetime,
    window_end: datetime,
    limits: Mapping[str, int],
) -> dict[str, Any]:
    """Collect validated unique self-responses in a timezone-aware right-open window.

    Exact normalized cwd membership is required; descendants and sibling worktrees
    must be explicitly supplied. Any telemetry gap makes coverage PARTIAL, or
    UNAVAILABLE when no project source is readable. Observed totals under PARTIAL
    must never be interpreted as complete project usage or billed account cost.
    """
    start, end = _utc(window_start), _utc(window_end)
    if end <= start:
        raise ValueError("token usage window must have start < end")
    if any(type(limits.get(key)) is not int or limits[key] <= 0 for key in _LIMIT_FIELDS):
        raise ValueError("all token usage scan limits must be positive integers")
    projects = {_project_identity(str(root)) for root in project_roots}
    if not projects:
        raise ValueError("at least one explicit project root is required")
    scan = _Scan(limits=limits, project_roots=projects, window_end=end)
    roots = sorted({Path(os.path.abspath(root)) for root in log_roots}, key=str, reverse=True)
    seen_files: set[str] = set()
    for root in roots:
        for path in _discover(root, scan):
            identity = os.path.normcase(str(path))
            if identity in seen_files:
                continue
            seen_files.add(identity)
            if scan.coverage["files_examined"] >= limits["max_files"]:
                scan.gaps["FILE_LIMIT_REACHED"] += 1
                scan.stopped = True
                break
            if scan.stopped:
                break
            _read_source(path, root, scan)
        if scan.stopped:
            break
    if not scan.sources:
        scan.gaps["NO_PROJECT_SOURCES"] += 1
    elif scan.coverage["usage_records_examined"] == 0:
        scan.gaps["NO_RESPONSE_USAGE_RECORDS"] += 1
    totals = dict.fromkeys((*_TOKEN_FIELDS, "uncached_input_tokens"), 0)
    response_count = 0
    for response in scan.responses.values():
        if response is None:
            continue
        if not start <= response.timestamp < end:
            scan.coverage["unique_responses_outside_window"] += 1
            continue
        response_count += 1
        for key, value in zip(_TOKEN_FIELDS, response.usage, strict=True):
            totals[key] += value
    totals["uncached_input_tokens"] = totals["input_tokens"] - totals["cached_input_tokens"]
    coverage: dict[str, Any] = {
        key: scan.coverage[key]
        for key in (
            "files_examined",
            "bytes_read",
            "usage_records_examined",
            "other_project_files_excluded",
            "sessions_created_after_window_excluded",
            "repeated_session_meta_records",
            "inherited_response_records_excluded",
            "unique_responses_outside_window",
        )
    }
    coverage.update(
        scan_mode="BOUNDED_SNAPSHOT",
        limits={key: limits[key] for key in _LIMIT_FIELDS},
        excluded_response_ids=sum(value is None for value in scan.responses.values()),
        scope="EXPLICIT_LOG_ROOTS_AND_EXACT_PROJECT_CWD",
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "UNAVAILABLE" if not scan.sources else "PARTIAL" if scan.gaps else "COMPLETE",
        "window": {
            "start_inclusive_utc": start.isoformat(),
            "end_exclusive_utc": end.isoformat(),
        },
        "totals": totals,
        "response_count": response_count,
        "duplicate_response_count": scan.duplicate_count,
        "source_count": len(scan.sources),
        "sources": scan.sources,
        "coverage": coverage,
        "telemetry_gaps": [
            {"code": code, "count": count} for code, count in sorted(scan.gaps.items())
        ],
    }


def validate_workflow_token_usage(payload: Any) -> list[str]:
    """Validate the compact collector contract without opening its source logs."""
    if not isinstance(payload, Mapping):
        return ["token_usage:not_mapping"]
    errors: list[str] = []
    if payload.get("schema_version") != SCHEMA_VERSION:
        errors.append("token_usage:schema_version")
    status = payload.get("status")
    if status not in ("COMPLETE", "PARTIAL", "UNAVAILABLE"):
        errors.append("token_usage:status")
    window = payload.get("window")
    try:
        if not isinstance(window, Mapping) or _timestamp(
            window.get("start_inclusive_utc")
        ) >= _timestamp(window.get("end_exclusive_utc")):
            raise ValueError("invalid window")
    except (ValueError, OverflowError):
        errors.append("token_usage:window")
    totals = payload.get("totals")
    try:
        usage = _usage(totals)
        if (
            not isinstance(totals, Mapping)
            or type(totals.get("uncached_input_tokens")) is not int
            or totals["uncached_input_tokens"] != usage[0] - usage[1]
        ):
            raise ValueError("invalid uncached count")
    except (ValueError, TypeError, OverflowError):
        errors.append("token_usage:totals")
    for key in ("response_count", "duplicate_response_count", "source_count"):
        if type(payload.get(key)) is not int or payload[key] < 0:
            errors.append(f"token_usage:{key}")
    sources = payload.get("sources")
    gaps = payload.get("telemetry_gaps")
    if not isinstance(sources, list) or payload.get("source_count") != len(sources):
        errors.append("token_usage:sources")
    else:
        for source in sources:
            if not isinstance(source, Mapping):
                errors.append("token_usage:source_shape")
                continue
            allowed = {
                "path",
                "file_size_bytes",
                "bytes_read",
                "sha256",
                "hash_scope",
                "lines_examined",
                "usage_records_observed",
                "stable",
            }
            if set(source) != allowed or not isinstance(source.get("path"), str):
                errors.append("token_usage:source_shape")
            source_counts_valid = all(
                type(source.get(key)) is int and source[key] >= 0
                for key in (
                    "file_size_bytes",
                    "bytes_read",
                    "lines_examined",
                    "usage_records_observed",
                )
            )
            if not source_counts_valid:
                errors.append("token_usage:source_counts")
            elif source["bytes_read"] > source["file_size_bytes"] and source.get("stable"):
                errors.append("token_usage:source_bytes")
            sha = source.get("sha256")
            if (
                not isinstance(sha, str)
                or len(sha) != hashlib.sha256().digest_size * 2
                or any(char not in "0123456789abcdef" for char in sha)
            ):
                errors.append("token_usage:source_hash")
            if source.get("hash_scope") not in ("ENTIRE_FILE", "READ_PREFIX"):
                errors.append("token_usage:source_hash_scope")
            elif source_counts_valid and (
                (source["bytes_read"] == source["file_size_bytes"])
                != (source["hash_scope"] == "ENTIRE_FILE")
            ):
                errors.append("token_usage:source_hash_scope")
            if type(source.get("stable")) is not bool:
                errors.append("token_usage:source_stability")
            if status == "COMPLETE" and (
                source.get("stable") is not True or source.get("hash_scope") != "ENTIRE_FILE"
            ):
                errors.append("token_usage:complete_with_incomplete_source")
    if not isinstance(gaps, list) or any(
        not isinstance(gap, Mapping)
        or set(gap) != {"code", "count"}
        or not isinstance(gap.get("code"), str)
        or not gap["code"]
        or type(gap.get("count")) is not int
        or gap["count"] <= 0
        for gap in gaps
    ):
        errors.append("token_usage:gaps")
    elif (
        (status == "COMPLETE" and (gaps or not sources))
        or (status == "PARTIAL" and (not gaps or not sources))
        or (status == "UNAVAILABLE" and (sources or not gaps))
    ):
        errors.append("token_usage:status_coverage")
    coverage = payload.get("coverage")
    if not isinstance(coverage, Mapping):
        errors.append("token_usage:coverage")
    else:
        if coverage.get("scan_mode") != "BOUNDED_SNAPSHOT" or coverage.get("scope") != (
            "EXPLICIT_LOG_ROOTS_AND_EXACT_PROJECT_CWD"
        ):
            errors.append("token_usage:coverage_scope")
        for key in (
            "files_examined",
            "bytes_read",
            "usage_records_examined",
            "other_project_files_excluded",
            "sessions_created_after_window_excluded",
            "repeated_session_meta_records",
            "inherited_response_records_excluded",
            "unique_responses_outside_window",
            "excluded_response_ids",
        ):
            if type(coverage.get(key)) is not int or coverage[key] < 0:
                errors.append(f"token_usage:coverage_{key}")
        limits = coverage.get("limits")
        if not isinstance(limits, Mapping) or any(
            type(limits.get(key)) is not int or limits[key] <= 0 for key in _LIMIT_FIELDS
        ):
            errors.append("token_usage:coverage_limits")
        else:
            for key, limit_key in (
                ("files_examined", "max_files"),
                ("bytes_read", "max_total_bytes"),
                ("usage_records_examined", "max_responses"),
            ):
                if type(coverage.get(key)) is int and coverage[key] > limits[limit_key]:
                    errors.append(f"token_usage:coverage_{key}_limit")
        for observed, available, error in (
            (payload.get("source_count"), coverage.get("files_examined"), "source_count"),
            (
                payload.get("response_count"),
                coverage.get("usage_records_examined"),
                "response_count",
            ),
        ):
            if type(observed) is int and type(available) is int and observed > available:
                errors.append(f"token_usage:{error}_exceeds_coverage")
        if isinstance(sources, list) and all(
            isinstance(source, Mapping) and type(source.get("bytes_read")) is int
            for source in sources
        ):
            source_bytes = sum(source["bytes_read"] for source in sources)
            if type(coverage.get("bytes_read")) is int and source_bytes > coverage["bytes_read"]:
                errors.append("token_usage:source_bytes_exceed_coverage")
    if (
        type(payload.get("response_count")) is int
        and payload["response_count"] == 0
        and isinstance(totals, Mapping)
        and any(totals.values())
    ):
        errors.append("token_usage:zero_responses_with_tokens")
    return sorted(set(errors))
