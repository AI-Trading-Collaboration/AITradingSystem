"""Run provenance: which code, configuration and data produced a result (GOV-008 guarantee G2).

A result is only reproducible if the commit, whether the code was modified, and the exact content of
every configuration file are recorded next to it. Existing run manifests recorded configuration
paths only. ``provenance_block`` is the single place that adds the missing facts, so research and
daily commands do not each re-implement them.

The block never raises: a missing git repository or an unreadable file is recorded as an explicit
``unavailable`` marker, because a run must not fail on provenance but must not hide the gap either.
"""

from __future__ import annotations

import hashlib
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

# Paths whose modification changes what a run computes. Documentation and task records are excluded
# so that writing notes does not mark a run as produced by modified code.
CODE_PATHS = ("src", "config", "scripts", "tools")
UNAVAILABLE = "unavailable"


def git_output(root: Path, *args: str) -> str | None:
    try:
        done = subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout.strip() if done.returncode == 0 else None


def file_sha256(path: Path) -> str:
    try:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return UNAVAILABLE


def git_provenance(anchor: Path) -> dict[str, Any]:
    """Commit, branch and whether result-affecting code differs from that commit."""
    directory = anchor if anchor.is_dir() else anchor.parent
    root_text = git_output(directory, "rev-parse", "--show-toplevel")
    if root_text is None:
        return {"commit": UNAVAILABLE, "branch": UNAVAILABLE, "code_modified": UNAVAILABLE}
    root = Path(root_text)
    commit = git_output(root, "rev-parse", "HEAD") or UNAVAILABLE
    branch = git_output(root, "branch", "--show-current") or UNAVAILABLE
    status = git_output(root, "status", "--porcelain", "--untracked-files=no", "--", *CODE_PATHS)
    return {
        "commit": commit,
        "branch": branch,
        "code_modified": UNAVAILABLE if status is None else bool(status),
    }


def provenance_block(config_paths: Mapping[str, Path]) -> dict[str, Any]:
    """Provenance for a run manifest: git facts plus a content hash for every configuration file."""
    anchor = next(iter(config_paths.values()), Path.cwd())
    return {
        "git": git_provenance(Path(anchor)),
        "config_sha256": {
            key: file_sha256(Path(path)) for key, path in sorted(config_paths.items())
        },
    }
