"""pytest plugin for the GOV-008 PR suite: keep only listed test files, deselect slow nodes.

Use as ``python -m pytest tests -p tools.gov008.pr_filter`` from the repository root.

Why a plugin and not ~970 file arguments: pytest 9 on Windows resolves every explicit file argument
by comparing it with each directory entry (an ``lstat`` per pair). 970 arguments x ~1,400 entries
made worker collection take tens of minutes at ~20% CPU (measured 2026-10-10, GOV-008 P2 spike S1).
One directory argument plus a set lookup is linear: the same suite then ran in 7m40s cold.

This plugin is a transition device. After the P4 deletions the excluded tests no longer exist, the
keep list disappears, and the nightly boundary becomes a ``slow`` marker on the few slow nodes.

Lists default to docs/requirements/GOV-008_lists/pr_keep.txt and pr_slow.txt; the environment
variables PR_KEEP_FILE and PR_SLOW_FILE override them (used by experiments).
"""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LISTS = ROOT / "docs/requirements/GOV-008_lists"


def _read(path: Path) -> set[str]:
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


KEEP = _read(Path(os.environ.get("PR_KEEP_FILE", LISTS / "pr_keep.txt")))
_slow_path = Path(os.environ.get("PR_SLOW_FILE", LISTS / "pr_slow.txt"))
SLOW = _read(_slow_path) if _slow_path.exists() else set()


def pytest_ignore_collect(collection_path, config):  # type: ignore[no-untyped-def]
    if collection_path.is_file() and collection_path.name.startswith("test_"):
        try:
            rel = collection_path.resolve().relative_to(ROOT).as_posix()
        except ValueError:
            return None
        return rel not in KEEP
    return None


def pytest_collection_modifyitems(config, items):  # type: ignore[no-untyped-def]
    if not SLOW:
        return
    kept, dropped = [], []
    for item in items:
        (dropped if item.nodeid in SLOW else kept).append(item)
    if dropped:
        config.hook.pytest_deselected(items=dropped)
        items[:] = kept
