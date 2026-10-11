"""GOV-008 P2 spike: ``ship`` runs the new gate locally and fast-forwards main.

Flow: task branch -> local gate -> ``Gate:`` trailer -> ``main`` moves to the same tree ->
ordinary push.
There is no lease store, no phase machine and no regenerated authority: git guarantees main only
moves to a descendant of itself, and the Gate trailer records which tree was tested.

This is an agent-run gate, not an independent verifier (no branch protection; owner decision
2026-10-10). Zone C changes (see config/gov008_ship.yaml) must carry an ``Owner-Decision:`` trailer.

usage:
  python tools/gov008/ship.py --repo . --dry-run          # plan only: zones, gate, trailer checks
  python tools/gov008/ship.py --repo . --execute [--push] # run gate, amend trailer, move main
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import yaml

POLICY_PATH = "config/gov008_ship.yaml"
OWNER_DECISION_TRAILER = "Owner-Decision"
GATE_TRAILER = "Gate"
# On a failing gate command the terminal shows only this many trailing lines, enough to see pytest's
# short test summary. Display truncation only: the full output always goes to the gate log file.
GATE_FAILURE_TAIL_LINES = 60
GATE_LOG_DIRNAME = "gov008-ship"


class ShipError(RuntimeError):
    """The change must not be shipped; the message says why."""


def glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Translate ``*`` (one path segment part), ``**`` (any depth) and ``?`` into a regex."""
    out = []
    i = 0
    while i < len(pattern):
        char = pattern[i]
        if pattern.startswith("**", i):
            out.append(".*")
            i += 2
            if pattern.startswith("/", i):
                i += 1
                out[-1] = "(?:.*/)?"
        elif char == "*":
            out.append("[^/]*")
            i += 1
        elif char == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(char))
            i += 1
    return re.compile("^" + "".join(out) + "$")


def classify(paths: list[str], policy: dict) -> dict[str, list[str]]:
    zone_c = [glob_to_regex(p) for p in policy["zones"]["C"]]
    zone_a = [glob_to_regex(p) for p in policy["zones"]["A"]]
    result: dict[str, list[str]] = {"A": [], "B": [], "C": []}
    for path in paths:
        if any(rx.match(path) for rx in zone_c):
            result["C"].append(path)
        elif any(rx.match(path) for rx in zone_a):
            result["A"].append(path)
        else:
            result["B"].append(path)
    return result


def highest_zone(by_zone: dict[str, list[str]]) -> str:
    for zone in ("C", "B", "A"):
        if by_zone[zone]:
            return zone
    return "A"


def git(repo: Path, *args: str, check: bool = True) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if check and proc.returncode != 0:
        raise ShipError(f"git {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc.stdout.strip()


def is_ancestor(repo: Path, ancestor: str, descendant: str) -> bool:
    proc = subprocess.run(
        ["git", "-C", str(repo), "merge-base", "--is-ancestor", ancestor, descendant],
        capture_output=True,
        check=False,
    )
    return proc.returncode == 0


def load_policy(repo: Path) -> dict:
    return yaml.safe_load((repo / POLICY_PATH).read_text(encoding="utf-8"))


def dirty_paths(repo: Path, exclusions: list[str]) -> list[str]:
    """Dirty/untracked paths; each registered unrelated path is excluded by literal pathspec."""
    spec = [".", *[f":(exclude,literal){p}" for p in exclusions]]
    out = git(repo, "status", "--porcelain", "--untracked-files=all", "--", *spec)
    return [line[3:] for line in out.splitlines() if line]


def owner_decisions(repo: Path, base: str) -> list[str]:
    out = git(
        repo, "log", f"--format=%(trailers:key={OWNER_DECISION_TRAILER},valueonly)", f"{base}..HEAD"
    )
    return [line.strip() for line in out.splitlines() if line.strip()]


def plan(repo: Path, base: str = "main") -> dict:
    """Everything ship would decide, without running tests or moving refs."""
    policy = load_policy(repo)
    branch = git(repo, "branch", "--show-current")
    problems: list[str] = []
    if branch in ("", base):
        problems.append(f"must run from a task branch, not '{branch or 'detached HEAD'}'")
    if not is_ancestor(repo, base, "HEAD"):
        problems.append(
            f"{base} is not an ancestor of HEAD: rebase or merge {base} first (ship never does)"
        )
    dirty = dirty_paths(repo, policy.get("tree_clean_exclusions", []))
    if dirty:
        problems.append(f"working tree is not clean: {dirty[:5]}")
    changed = [p for p in git(repo, "diff", "--name-only", f"{base}..HEAD").splitlines() if p]
    by_zone = classify(changed, policy)
    zone = highest_zone(by_zone) if changed else "A"
    decisions = owner_decisions(repo, base)
    if by_zone["C"] and not decisions:
        problems.append(
            f"zone C paths changed ({by_zone['C'][:3]}...) but no commit carries an "
            f"'{OWNER_DECISION_TRAILER}:' trailer"
        )
    return {
        "branch": branch,
        "base": base,
        "base_sha": git(repo, "rev-parse", base),
        "head_sha": git(repo, "rev-parse", "HEAD"),
        "tree": git(repo, "rev-parse", "HEAD^{tree}"),
        "changed_paths": len(changed),
        "paths_by_zone": {z: len(v) for z, v in by_zone.items()},
        "zone": zone,
        "owner_decisions": decisions,
        "gate": policy["gates"][zone],
        "problems": problems,
        "ok": not problems,
    }


def git_dir(repo: Path) -> Path:
    out = git(repo, "rev-parse", "--git-dir")
    path = Path(out)
    return path if path.is_absolute() else (repo / path).resolve()


def run_gate(repo: Path, commands: list[list[str]]) -> dict:
    """Run the gate quietly: full stdout/stderr of every command goes to a log file under the git
    directory (never the working tree), the terminal gets one summary line per command. On failure
    the tail of that command's output is printed and the ShipError names the log file."""
    started = time.monotonic()
    log_dir = git_dir(repo) / GATE_LOG_DIRNAME
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"gate-{time.strftime('%Y%m%d-%H%M%S')}-{os.getpid()}.log"
    with log_path.open("w", encoding="utf-8", errors="replace") as log:
        for argv in commands:
            resolved = [sys.executable if a == "python" else a for a in argv]
            cmd_started = time.monotonic()
            proc = subprocess.run(
                resolved,
                cwd=repo,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
            seconds = round(time.monotonic() - cmd_started, 1)
            output = (proc.stdout or "") + (proc.stderr or "")
            log.write(f"\n===== command: {' '.join(argv)} =====\n")
            log.write(output)
            log.write(f"\n===== exit={proc.returncode} seconds={seconds} =====\n")
            log.flush()
            non_empty = [line for line in output.splitlines() if line.strip()]
            summary = non_empty[-1] if non_empty else "(no output)"
            print(
                f"gate: {' '.join(argv)} -> exit={proc.returncode} seconds={seconds} | {summary}"
            )
            if proc.returncode != 0:
                tail_lines = output.splitlines()[-GATE_FAILURE_TAIL_LINES:]
                print(
                    f"gate command failed; last {len(tail_lines)} lines of output "
                    f"(full output: {log_path}):\n" + "\n".join(tail_lines),
                    file=sys.stderr,
                )
                raise ShipError(
                    f"gate command failed (exit {proc.returncode}): {' '.join(argv)}; "
                    f"full gate output: {log_path}"
                )
    return {
        "seconds": round(time.monotonic() - started, 1),
        "commands": len(commands),
        "log": str(log_path),
    }


def gate_trailer(plan_: dict, result: dict) -> str:
    return (
        f"zone={plan_['zone']} tree={plan_['tree'][:12]} commands={result['commands']} "
        f"seconds={result['seconds']} PASS"
    )


def ship(repo: Path, base: str, push: bool) -> dict:
    """Run the gate, record it on the tip commit, move ``base`` to it, optionally push."""
    p = plan(repo, base)
    if not p["ok"]:
        raise ShipError("; ".join(p["problems"]))
    result = run_gate(repo, p["gate"])
    message = git(repo, "log", "-1", "--format=%B")
    amended = subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "interpret-trailers",
            "--trailer",
            f"{GATE_TRAILER}: {gate_trailer(p, result)}",
        ],
        input=message + "\n",
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    ).stdout
    git(repo, "commit", "--amend", "-m", amended.strip())
    new_head = git(repo, "rev-parse", "HEAD")
    if git(repo, "rev-parse", "HEAD^{tree}") != p["tree"]:
        raise ShipError("tree changed while amending the Gate trailer: refusing to move main")
    if push:
        # The remote must not have moved past or away from the tested base: only a fast-forward of
        # the remote main is ever pushed, and a diverged remote is reported, never repaired.
        git(repo, "fetch", "origin", base)
        if not is_ancestor(repo, f"origin/{base}", new_head):
            raise ShipError(
                f"origin/{base} is not an ancestor of the shipped commit: stop and report"
            )
    # compare-and-swap fast-forward: fails if main moved since the plan was made
    git(repo, "update-ref", f"refs/heads/{base}", new_head, p["base_sha"])
    if push:
        git(repo, "push", "origin", base)
        git(repo, "fetch", "origin", base)
        remote = git(repo, "rev-parse", f"origin/{base}")
        if remote != new_head:
            raise ShipError(f"after push origin/{base}={remote} differs from {new_head}")
    return {"shipped_sha": new_head, "pushed": push, "gate": result, "plan": p}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--base", default="main")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--push", action="store_true", help="with --execute: ordinary push of base")
    args = parser.parse_args()
    repo = args.repo.resolve()
    try:
        # Compact one-line JSON: the full plan/ship result stays machine-readable without spending
        # ~30 lines (and thousands of tokens in a small model context) on indentation.
        if args.dry_run:
            print(json.dumps(plan(repo, args.base), ensure_ascii=False, separators=(", ", ": ")))
            return 0
        print(
            json.dumps(ship(repo, args.base, args.push), ensure_ascii=False, separators=(", ", ": "))
        )
        return 0
    except ShipError as exc:
        print(f"SHIP REFUSED: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
