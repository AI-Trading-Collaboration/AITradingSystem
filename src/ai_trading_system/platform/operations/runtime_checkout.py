"""Release identity of the runtime checkout that runs ``aits ops daily-run`` (OPS-082).

Terminal recovery has to know which code the recovered run uses and that it differs from the code
of the failed parent run. That identity used to come from a deployment receipt written by the
deleted ``deployment-acceptance`` command. Decision (e) of
``owner_decision:OPS-082:2026-10-11:deterministic_scheduler_v1`` replaces the receipt with a
direct check of the checkout itself: the working tree is clean and
``HEAD`` is a commit reachable from ``origin/main``. ``HEAD`` then takes the place of the receipt's
``candidate_commit`` in the comparison with the parent run manifest's ``git_commit``.

``origin/main`` is the local remote-tracking ref; the runtime update procedure in the operations
runbook refreshes it with ``git fetch origin main`` before checking out a shipped commit. The check
never fetches, so recovery makes no network call.
"""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

ORIGIN_MAIN_REF = "refs/remotes/origin/main"
# Protocol timeout for local git plumbing calls; not a policy threshold.
_GIT_TIMEOUT_SECONDS = 60
_COMMIT = re.compile(r"[0-9a-f]{40}")
# Variables that would make git inspect a different repository than ``root``.
_GIT_LOCATION_ENV = (
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_COMMON_DIR",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
)


class RuntimeCheckoutError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class RuntimeCheckoutState:
    root: Path
    head_commit: str
    origin_main_commit: str
    dirty_entries: tuple[str, ...]
    head_on_origin_main: bool

    @property
    def clean(self) -> bool:
        return not self.dirty_entries


def inspect_runtime_checkout(root: Path) -> RuntimeCheckoutState:
    """Read HEAD, origin/main, cleanliness and reachability without changing the checkout."""

    resolved = Path(root).resolve()
    head = _rev_parse(resolved, "HEAD")
    origin_main = _rev_parse(resolved, ORIGIN_MAIN_REF)
    status = _git(resolved, "status", "--porcelain=v1", "--untracked-files=normal")
    if status.returncode != 0:
        raise RuntimeCheckoutError("RUNTIME_CHECKOUT_GIT_FAILED", _git_failure("status", status))
    dirty = tuple(line for line in status.stdout.splitlines() if line.strip())
    ancestor = _git(resolved, "merge-base", "--is-ancestor", head, origin_main)
    if ancestor.returncode not in (0, 1):
        raise RuntimeCheckoutError(
            "RUNTIME_CHECKOUT_GIT_FAILED", _git_failure("merge-base --is-ancestor", ancestor)
        )
    return RuntimeCheckoutState(
        root=resolved,
        head_commit=head,
        origin_main_commit=origin_main,
        dirty_entries=dirty,
        head_on_origin_main=ancestor.returncode == 0,
    )


def require_clean_runtime_release(root: Path) -> RuntimeCheckoutState:
    """Return the checkout state, or raise when it is dirty or HEAD is not on origin/main."""

    state = inspect_runtime_checkout(root)
    if not state.clean:
        raise RuntimeCheckoutError(
            "RUNTIME_CHECKOUT_DIRTY",
            f"root={state.root};uncommitted_entries={len(state.dirty_entries)};"
            f"first={state.dirty_entries[0]}",
        )
    if not state.head_on_origin_main:
        raise RuntimeCheckoutError(
            "RUNTIME_CHECKOUT_HEAD_NOT_ON_ORIGIN_MAIN",
            f"root={state.root};head={state.head_commit};origin_main={state.origin_main_commit}",
        )
    return state


def _rev_parse(root: Path, ref: str) -> str:
    completed = _git(root, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}")
    commit = completed.stdout.strip().lower()
    if completed.returncode != 0 or _COMMIT.fullmatch(commit) is None:
        raise RuntimeCheckoutError(
            "RUNTIME_CHECKOUT_GIT_FAILED", _git_failure(f"rev-parse {ref}", completed)
        )
    return commit


def _git(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ("git", "-C", str(root), *args),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            timeout=_GIT_TIMEOUT_SECONDS,
            env=_git_env(os.environ),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeCheckoutError(
            "RUNTIME_CHECKOUT_GIT_FAILED", f"root={root};git {args[0]}: {exc}"
        ) from exc


def _git_env(env: Mapping[str, str]) -> dict[str, str]:
    return {name: value for name, value in env.items() if name not in _GIT_LOCATION_ENV}


def _git_failure(command: str, completed: subprocess.CompletedProcess[str]) -> str:
    detail = (completed.stderr or completed.stdout or "").strip().splitlines()
    return f"git {command} exit={completed.returncode};{detail[0] if detail else 'no output'}"
