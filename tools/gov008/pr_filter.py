"""pytest plugin for the GOV-008 PR suite transition: skip the test files that are being deleted.

Use as ``python -m pytest tests -m "not slow" -p tools.gov008.pr_filter`` from the repository root.
The PR/nightly boundary is the ``slow`` marker (single tests of 30 s or more), not a list.

Why a plugin and not a file-argument list: pytest 9 on Windows resolves every explicit file
argument by comparing it with each directory entry (an ``lstat`` per pair). 970 arguments x ~1,400
entries made worker collection take tens of minutes at ~20% CPU (measured 2026-10-10, GOV-008 P2
spike S1). One directory argument plus a set lookup is linear: the same suite then ran in 7m40s.

The list is an EXCLUDE list, never an include list: a new test file is collected by default, so a
test added after the triage can never be silently left out of the gate.

This plugin is a transition device. After the P4 deletions the excluded tests no longer exist, the
list disappears, and the PR suite is plain ``pytest tests -m "not slow"``.

The list defaults to docs/requirements/GOV-008_lists/pr_exclude.txt; PR_EXCLUDE_FILE overrides it.
"""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EXCLUDE_FILE = Path(
    os.environ.get("PR_EXCLUDE_FILE", ROOT / "docs/requirements/GOV-008_lists/pr_exclude.txt")
)
EXCLUDE = {
    line.strip() for line in EXCLUDE_FILE.read_text(encoding="utf-8").splitlines() if line.strip()
}


def pytest_ignore_collect(collection_path, config):  # type: ignore[no-untyped-def]
    if collection_path.is_file() and collection_path.name.startswith("test_"):
        try:
            rel = collection_path.resolve().relative_to(ROOT).as_posix()
        except ValueError:
            return None
        return True if rel in EXCLUDE else None
    return None
