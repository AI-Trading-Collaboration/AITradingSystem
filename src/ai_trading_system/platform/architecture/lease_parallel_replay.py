"""DEVX-023 P: opt-in chain-level parallel replay of the lease store.

Why: a replay parses and validates every event of every lease chain, and about 90% of the time of
the stage-1 named-DQ tests and of a real publication is spent in these replays (DEVX-022 17.7,
DEVX-023 10.8.1). The validation is per chain (unique ids, causal head, state transitions,
execution transitions); only the final ACTIVE-overlap check spans chains. Whole chains can therefore
be replayed by worker processes and merged with the same assembly function the serial replay uses.

What it is not: it persists nothing, caches nothing across calls and does not move the trust
boundary. The serial replay stays the authority. With the switch unset nothing here is imported,
and every anomaly (pool failure, timeout, a worker error, an event whose lease id differs from its
directory, an unsafe ``__main__``) returns None so the caller runs the serial replay and gets the
serial result.
"""

from __future__ import annotations

import contextlib
import json
import multiprocessing
import os
import pickle
import re
import sys
from collections.abc import Callable, Mapping
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from ai_trading_system.platform.architecture import parallel_control_kernel as kernel
from ai_trading_system.platform.architecture.parallel_control import (
    ControlIssue,
    ParallelControlError,
)

PARALLEL_REPLAY_ENV = "AITS_LEASE_PARALLEL_REPLAY"
# With this set to "1" every parallel result is also compared with a serial replay and a difference
# raises LEASE_PARALLEL_REPLAY_MISMATCH (the same idea as AITS_LEASE_SECTION_REPLAY_VERIFY).
VERIFY_ENV = "AITS_LEASE_PARALLEL_REPLAY_VERIFY"

# Pilot baselines (performance only, no investment interpretation; DEVX-023 section 12.2 item 3).
# Exit condition: recalibrate from the measurements of the publications that enable the switch.
# 4-8 worker processes were equally fast on the 877-chain real store (7.6-8.0 s against 17.7-20.7 s
# serial); 16 workers took 8.8 s and 32 took 10.7 s because only start-up cost grows.
DEFAULT_WORKERS = 4
MAX_WORKERS = 8
# Below this many event files a serial replay takes a few seconds at most and a pool's start-up
# (about 2 s on Windows, where workers are spawned and import the kernel) does not pay for itself.
MIN_EVENTS = 600
# Upper bound for the whole parallel replay; on expiry the caller runs the serial replay.
WAIT_SECONDS = 600.0

_OFF_VALUES = frozenset({"", "0", "1", "off", "false", "no"})
_AUTO_VALUES = frozenset({"auto", "on", "true", "yes"})
_MAIN_GUARD = re.compile(r"""if\s+__name__\s*==\s*['"]__main__['"]""")

# Diagnostics only (tests and measurements): why the last call did not return a parallel replay.
LAST_FALLBACK_REASON: str | None = None


def configured_workers(environ: Mapping[str, str] | None = None) -> int:
    """0 means serial: unset, empty, 0/1/off/false/no or anything unparsable (fail safe)."""
    source = os.environ if environ is None else environ
    raw = source.get(PARALLEL_REPLAY_ENV, "").strip().lower()
    if raw in _OFF_VALUES:
        return 0
    if raw in _AUTO_VALUES:
        return DEFAULT_WORKERS
    try:
        requested = int(raw)
    except ValueError:
        return 0
    return min(requested, MAX_WORKERS) if requested >= 2 else 0


def replay_if_enabled(
    *,
    events_root: Path,
    blobs_root: Path,
    serial: Callable[[], kernel.LeaseReplay],
) -> kernel.LeaseReplay | None:
    """The parallel replay, or None when the caller must run the serial one."""
    global LAST_FALLBACK_REASON
    LAST_FALLBACK_REASON = None
    workers = configured_workers()
    if not workers:
        LAST_FALLBACK_REASON = "disabled"
        return None
    replay = _replay_in_parallel(events_root, blobs_root, workers)
    if replay is None:
        return None
    if os.environ.get(VERIFY_ENV) == "1" and serial() != replay:
        raise ParallelControlError(
            "LEASE_PARALLEL_REPLAY_MISMATCH", "the parallel replay differs from the serial one"
        )
    return replay


def _main_is_safe_for_spawn() -> bool:
    """A spawned worker re-imports the caller's ``__main__`` module; that must not re-run it.

    No file (interactive, ``-c``, execnet/xdist workers) has nothing to re-run. A script or module
    with a ``if __name__ == "__main__"`` guard is safe. Anything else (including a path that cannot
    be read, such as a console-script stub) is not worth the risk: the serial replay runs.
    """
    main = sys.modules.get("__main__")
    path = getattr(main, "__file__", None)
    if main is None or path is None:
        return True
    try:
        source = Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return _MAIN_GUARD.search(source) is not None


def _chains_by_size(events_root: Path) -> list[tuple[int, int, str]]:
    """(bytes, event files, directory) per chain directory, heaviest first."""
    chains: list[tuple[int, int, str]] = []
    with os.scandir(events_root) as entries:
        for entry in entries:
            if not entry.is_dir():
                continue
            total = files = 0
            with os.scandir(entry.path) as items:
                for item in items:
                    if item.name.lower().endswith(".json"):
                        total += item.stat().st_size
                        files += 1
            chains.append((total, files, entry.path))
    chains.sort(reverse=True)
    return chains


def _replay_in_parallel(
    events_root: Path, blobs_root: Path, workers: int
) -> kernel.LeaseReplay | None:
    global LAST_FALLBACK_REASON
    if not events_root.exists():
        LAST_FALLBACK_REASON = "no_events"
        return None
    try:
        chains = _chains_by_size(events_root)
    except OSError as exc:
        LAST_FALLBACK_REASON = f"scan_failed:{type(exc).__name__}"
        return None
    if sum(files for _size, files, _path in chains) < MIN_EVENTS:
        LAST_FALLBACK_REASON = "small_store"
        return None
    if not _main_is_safe_for_spawn():
        LAST_FALLBACK_REASON = "unsafe_main"
        return None
    pool: ProcessPoolExecutor | None = None
    try:
        pool = ProcessPoolExecutor(
            max_workers=min(workers, len(chains)),
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_init_worker,
            initargs=(str(blobs_root),),
        )
        results = list(
            pool.map(
                _replay_chain,
                [path for _size, _files, path in chains],
                chunksize=1,
                timeout=WAIT_SECONDS,
            )
        )
        pool.shutdown(wait=True)
        pool = None
        heads: list[kernel.ExecutionLease] = []
        head_ids: list[tuple[str, str]] = []
        issues: set[ControlIssue] = set()
        event_count = 0
        for name, blob, mismatched in results:
            if mismatched:
                LAST_FALLBACK_REASON = f"lease_id_differs_from_directory:{name}"
                return None
            part = pickle.loads(blob)
            heads.extend(part.lease_heads)
            head_ids.extend(part.head_event_ids)
            issues.update(part.issues)
            event_count += part.event_count
        return kernel._assemble_lease_replay(heads, head_ids, issues, event_count=event_count)
    except Exception as exc:  # noqa: BLE001 - any failure of the parallel route means "run serial"
        LAST_FALLBACK_REASON = f"parallel_failed:{type(exc).__name__}"
        return None
    finally:
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)


_BLOBS: kernel.ExternalizedRowsReader | None = None
_SCOPES: contextlib.ExitStack | None = None


def _init_worker(blobs_root: str) -> None:
    """Per worker process: the blob reader and the same per-call memo scopes the serial replay uses.

    The pool lives for exactly one replay call, so the scopes have the serial call's lifetime.
    """
    from ai_trading_system.platform.architecture.workflow_coordination import (
        replay_validation_scope,
    )

    global _BLOBS, _SCOPES
    _BLOBS = kernel.ExternalizedRowsReader(Path(blobs_root))
    _SCOPES = contextlib.ExitStack()
    _SCOPES.enter_context(replay_validation_scope())
    _SCOPES.enter_context(kernel._rows_sharing_scope())


def _replay_chain(directory: str) -> tuple[str, bytes, bool]:
    """Parse and validate ONE chain exactly like the serial replay does, then pickle the result."""
    from ai_trading_system.platform.architecture.workflow_coordination import (
        _validate_checked_execution_transition,
    )

    chain = Path(directory)
    events: list[kernel.LeaseEvent] = []
    issues: set[ControlIssue] = set()
    for path in sorted(chain.glob("*.json")):
        try:
            events.append(
                kernel.parse_lease_event(json.loads(path.read_text(encoding="utf-8")), blobs=_BLOBS)
            )
        except (OSError, json.JSONDecodeError, ParallelControlError) as exc:
            issues.add(kernel._issue("LEASE_EVENT_INVALID", (), path.as_posix(), str(exc)))
    mismatched = any(event.lease.lease_id != chain.name for event in events)
    part = kernel._replay_lease_events(
        events,
        initial_issues=tuple(issues),
        execution_transition=_validate_checked_execution_transition,
    )
    return chain.name, pickle.dumps(part, protocol=pickle.HIGHEST_PROTOCOL), mismatched


__all__ = [
    "DEFAULT_WORKERS",
    "MAX_WORKERS",
    "MIN_EVENTS",
    "PARALLEL_REPLAY_ENV",
    "VERIFY_ENV",
    "WAIT_SECONDS",
    "configured_workers",
    "replay_if_enabled",
]
