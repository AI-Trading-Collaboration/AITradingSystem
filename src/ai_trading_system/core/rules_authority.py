"""Pinned project-rules authority that survives later rewrites of AGENTS.md.

Some research records pin the exact bytes of ``AGENTS.md`` as their "project engineering rules"
authority. Rewriting the rules must neither silently invalidate those records nor force editing the
records after the fact (owner_decision:GOV-008:2026-10-10:historical_rules_authority). A pin is
therefore satisfied when the current file has the pinned bytes, or when ``AGENTS.md`` had exactly
those bytes in the commit that last changed the record, i.e. the pin was true when the record was
written. Callers keep checking the *current* file for the rule text they require, so a rule that is
really removed is still caught. Only this one path gets the historical route; every other authority
stays an exact byte comparison.
"""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

from ai_trading_system.core.provenance import git_output

RULES_AUTHORITY_PATH = "AGENTS.md"
# A local git call, not a network request: bound it like other git calls in core.provenance.
_GIT_TIMEOUT_SECONDS = 30


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _bytes_at(project_root: Path, commit: str, relative: str) -> bytes | None:
    try:
        done = subprocess.run(
            ["git", "-C", str(project_root), "show", f"{commit}:{relative}"],
            capture_output=True,
            timeout=_GIT_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout if done.returncode == 0 else None


def rules_authority_matches(*, project_root: Path, pinned_sha256: str, record_path: Path) -> bool:
    """True when the pinned AGENTS.md bytes are current, or were when the record was written."""
    current = project_root / RULES_AUTHORITY_PATH
    if current.is_file() and _sha256(current.read_bytes()) == pinned_sha256:
        return True
    try:
        record = record_path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return False
    commit = git_output(project_root, "log", "-1", "--format=%H", "--", record)
    if not commit:
        return False
    written = _bytes_at(project_root, commit, RULES_AUTHORITY_PATH)
    return written is not None and _sha256(written) == pinned_sha256
