"""GOV-008 ship gate: run local gates, record ``Gate:``, and publish by fast-forward.

Flow: task branch -> local gate -> ``Gate:`` trailer -> ``main`` moves to the same tree ->
ordinary push. ``--gate`` can stop after recording the tested tree; ``--publish`` verifies that
record without running branch code, then fast-forwards the base branch. ``--publish`` can also
check the reviewed patch (``--expected-patch``) instead of a predicted tree, and, for zone C, owner
decision records under the review root (``--review-root``) instead of a per-tree acceptance.

This is an agent-run gate, not an independent verifier (no branch protection; owner decision
2026-10-10). Zone C changes (see config/gov008_ship.yaml) must carry an ``Owner-Decision:`` trailer.

usage:
  python tools/gov008/ship.py --repo . --dry-run          # plan only: zones, gate, trailer checks
  python tools/gov008/ship.py --repo . --execute [--push] # run gate, amend trailer, move main
  python tools/gov008/ship.py --repo . --gate             # run gate and amend trailer only
  python tools/gov008/ship.py --repo . --publish [--push] # verify Gate trailer, move main
  python tools/gov008/ship.py --check-decision-record <file> # validate one owner decision record
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
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
GATE_RE = re.compile(
    r"^zone=(?P<zone>[ABC]) tree=(?P<tree>[0-9a-f]{12,40}) commands=(?P<commands>\d+) "
    r"seconds=(?P<seconds>\d+(?:\.\d+)?) PASS$"
)
OWNER_DECISION_RE = re.compile(
    r"^owner_decision:(?P<task>[A-Za-z0-9_.-]+):(?P<date>\d{4}-\d{2}-\d{2}):(?P<slug>[A-Za-z0-9_.-]+)$"
)
# Owner decision records (owner_decision:DEVX-029:2026-10-13:decision_scoped_signoff): one record
# per owner decision in <review root>/decisions/, written from the owner's own words. It lets zone C
# paths inside its c_paths publish without a per-tree acceptance until it expires. 14 days is the
# longest a record may stay valid, so a stale decision cannot keep approving changes.
DECISION_SCHEMA = "aits.owner_decision/1"
DECISION_RECORD_DIR = "decisions"
DECISION_MAX_DAYS = 14
# Rendered task views are regenerated on whatever main a package lands on; the gate's staleness test
# ties them to tasks/*.yaml, so the reviewed patch fingerprint leaves them out.
RENDERED_VIEWS = ("docs/task_register.md", "docs/task_register_completed.md")
# Fixed diff options so the fingerprint does not depend on local git configuration.
PATCH_DIFF_OPTIONS = (
    "--no-color",
    "--no-ext-diff",
    "--no-textconv",
    "--no-renames",
    "--binary",
    "--full-index",
    "--unified=3",
    "--inter-hunk-context=0",
    "--diff-algorithm=myers",
    "--indent-heuristic",
    "--src-prefix=a/",
    "--dst-prefix=b/",
)
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


def load_policy_from_ref(repo: Path, ref: str) -> dict:
    return yaml.safe_load(git(repo, "show", f"{ref}:{POLICY_PATH}"))


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


def plan(repo: Path, base: str = "main", policy: dict | None = None) -> dict:
    """Everything ship would decide, without running tests or moving refs."""
    policy = load_policy(repo) if policy is None else policy
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


def decision_record_name(decision: str) -> str:
    match = OWNER_DECISION_RE.fullmatch(decision)
    if match is None:
        raise ShipError(f"owner decision id is malformed: {decision!r}")
    return f"{match['task']}.{match['date']}.{match['slug']}.json"


def validate_decision_record(data: object, today: dt.date) -> dict:
    """Check one owner decision record; return it or raise ShipError naming every problem."""
    if not isinstance(data, dict):
        raise ShipError("decision record is not a JSON object")
    problems: list[str] = []
    if data.get("schema") != DECISION_SCHEMA:
        problems.append(f"schema must be {DECISION_SCHEMA}")
    decision = data.get("decision")
    if not isinstance(decision, str) or OWNER_DECISION_RE.fullmatch(decision) is None:
        problems.append("decision must be owner_decision:<task>:<YYYY-MM-DD>:<slug>")
    for key in ("owner_words", "expected_result"):
        if not isinstance(data.get(key), str) or not data[key].strip():
            problems.append(f"{key} must be a non-empty string")
    c_paths = data.get("c_paths")
    if (
        not isinstance(c_paths, list)
        or not c_paths
        or not all(isinstance(g, str) and g.strip() and not g.startswith("/") for g in c_paths)
    ):
        problems.append("c_paths must be a non-empty list of repository-relative globs")
    approved: dt.date | None = None
    try:
        approved_at = dt.datetime.strptime(str(data.get("approved_at")), "%Y-%m-%dT%H:%M:%SZ")
        approved = approved_at.date()
    except ValueError:
        problems.append("approved_at must be YYYY-MM-DDTHH:MM:SSZ (UTC)")
    try:
        expires = dt.date.fromisoformat(str(data.get("expires")))
    except ValueError:
        problems.append("expires must be YYYY-MM-DD")
    else:
        if approved is not None and expires > approved + dt.timedelta(days=DECISION_MAX_DAYS):
            problems.append(f"expires is more than {DECISION_MAX_DAYS} days after approved_at")
        if approved is not None and expires < approved:
            problems.append("expires is before approved_at")
        if expires < today:
            problems.append(f"expired on {expires.isoformat()}")
    if problems:
        raise ShipError("decision record: " + "; ".join(problems))
    return data


def read_decision_record(path: Path, today: dt.date) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        raise ShipError(f"decision record {path} cannot be read: {exc}") from exc
    return validate_decision_record(data, today)


def check_decision_records(
    repo: Path, base: str, policy: dict, review_root: Path, today: dt.date | None = None
) -> dict:
    """Zone C without a per-tree acceptance: every changed C path needs a valid decision record."""
    today = dt.datetime.now(dt.UTC).date() if today is None else today
    if "decision_record_excluded" not in policy:
        raise ShipError(
            "base policy has no decision_record_excluded list: decision records are off"
        )
    changed = [p for p in git(repo, "diff", "--name-only", f"{base}..HEAD").splitlines() if p]
    c_paths = classify(changed, policy)["C"]
    excluded = [glob_to_regex(g) for g in policy["decision_record_excluded"]]
    blocked = [p for p in c_paths if any(rx.match(p) for rx in excluded)]
    if blocked:
        raise ShipError(
            f"zone C paths {blocked[:5]} are rules or boundaries that a decision record cannot "
            "approve; they need the owner's per-tree acceptance"
        )
    decisions = list(dict.fromkeys(owner_decisions(repo, base)))
    if not decisions:
        raise ShipError(f"no '{OWNER_DECISION_TRAILER}:' trailer to look up a decision record")
    records = []
    for decision in decisions:
        path = review_root / DECISION_RECORD_DIR / decision_record_name(decision)
        if not path.is_file():
            raise ShipError(f"no decision record for {decision} at {path}")
        record = read_decision_record(path, today)
        if record["decision"] != decision:
            raise ShipError(f"decision record {path} is for {record['decision']}, not {decision}")
        records.append(record)
    allowed = [glob_to_regex(g) for record in records for g in record["c_paths"]]
    uncovered = [p for p in c_paths if not any(rx.match(p) for rx in allowed)]
    if uncovered:
        raise ShipError(f"zone C paths {uncovered[:5]} are outside the decision records' c_paths")
    return {"decisions": decisions, "zone_c_paths": len(c_paths)}


def patch_fingerprint(repo: Path, base: str, policy: dict) -> str:
    """``git patch-id --stable`` of base..HEAD without rendered views and registered exclusions."""
    excluded = [*policy.get("tree_clean_exclusions", []), *RENDERED_VIEWS]
    spec = [".", *[f":(exclude,literal){p}" for p in excluded]]
    diff = subprocess.run(
        ["git", "-C", str(repo), "diff", *PATCH_DIFF_OPTIONS, f"{base}..HEAD", "--", *spec],
        capture_output=True,
        check=False,
    )
    if diff.returncode != 0:
        stderr = diff.stderr.decode(errors="replace")
        raise ShipError(f"git diff for the patch fingerprint failed: {stderr}")
    if not diff.stdout.strip():
        raise ShipError("the patch is empty")
    pid = subprocess.run(
        ["git", "-C", str(repo), "patch-id", "--stable"],
        input=diff.stdout,
        capture_output=True,
        check=False,
    )
    fields = pid.stdout.decode().split()
    if pid.returncode != 0 or not fields:
        raise ShipError(f"git patch-id failed: {pid.stderr.decode(errors='replace')}")
    return fields[0]


def message_fingerprint(repo: Path, rev: str = "HEAD") -> str:
    """SHA-256 of the commit message without Gate trailer lines (UTF-8, one trailing newline)."""
    message = git(repo, "log", "-1", "--format=%B", rev)
    lines = [line for line in message.splitlines() if not line.startswith(f"{GATE_TRAILER}: ")]
    return hashlib.sha256(("\n".join(lines).rstrip() + "\n").encode("utf-8")).hexdigest()


def check_reviewed_patch(
    repo: Path, base: str, policy: dict, expected_patch: str, expected_message: str
) -> dict:
    if re.fullmatch(r"[0-9a-f]{40}", expected_patch) is None:
        raise ShipError("expected patch must be a 40-character lowercase hex patch id")
    if re.fullmatch(r"[0-9a-f]{64}", expected_message) is None:
        raise ShipError("expected message must be a 64-character lowercase hex SHA-256")
    count = git(repo, "rev-list", "--count", f"{base}..HEAD")
    if count != "1":
        raise ShipError(f"a reviewed patch publishes exactly one commit on {base}, found {count}")
    patch = patch_fingerprint(repo, base, policy)
    if patch != expected_patch:
        raise ShipError(f"patch {patch} does not match the reviewed patch {expected_patch}")
    message = message_fingerprint(repo)
    if message != expected_message:
        raise ShipError(
            f"commit message {message} does not match the reviewed message {expected_message}"
        )
    return {"patch": patch, "message": message}


def git_dir(repo: Path) -> Path:
    out = git(repo, "rev-parse", "--git-dir")
    path = Path(out)
    return path if path.is_absolute() else (repo / path).resolve()


def run_gate(repo: Path, commands: list[list[str]]) -> dict:
    """Run the gate quietly and log full stdout/stderr under the git directory."""
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
            print(f"gate: {' '.join(argv)} -> exit={proc.returncode} seconds={seconds} | {summary}")
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


def append_gate_trailer(repo: Path, trailer: str) -> str:
    message = git(repo, "log", "-1", "--format=%B")
    amended = subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "interpret-trailers",
            "--trailer",
            f"{GATE_TRAILER}: {trailer}",
        ],
        input=message + "\n",
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    ).stdout
    git(repo, "commit", "--amend", "-m", amended.strip())
    return git(repo, "rev-parse", "HEAD")


def move_base_and_maybe_push(
    repo: Path, base: str, new_head: str, old_base_sha: str, push: bool
) -> None:
    if push:
        # The remote must not have moved past or away from the tested base: only a fast-forward of
        # the remote main is ever pushed, and a diverged remote is reported, never repaired.
        git(repo, "fetch", "origin", base)
        if not is_ancestor(repo, f"origin/{base}", new_head):
            raise ShipError(
                f"origin/{base} is not an ancestor of the shipped commit: stop and report"
            )
    # compare-and-swap fast-forward: fails if main moved since the plan was made
    git(repo, "update-ref", f"refs/heads/{base}", new_head, old_base_sha)
    if push:
        git(repo, "push", "origin", base)
        git(repo, "fetch", "origin", base)
        remote = git(repo, "rev-parse", f"origin/{base}")
        if remote != new_head:
            raise ShipError(f"after push origin/{base}={remote} differs from {new_head}")


def read_gate_trailer(repo: Path) -> str:
    out = git(repo, "log", "-1", f"--format=%(trailers:key={GATE_TRAILER},valueonly)")
    trailers = [line.strip() for line in out.splitlines() if line.strip()]
    if not trailers:
        raise ShipError("missing Gate trailer on HEAD")
    return trailers[-1]


def validate_gate_trailer(repo: Path, trailer: str, plan_: dict) -> None:
    match = GATE_RE.fullmatch(trailer)
    if match is None:
        raise ShipError(
            "Gate trailer format is invalid; expected "
            "zone=<A|B|C> tree=<12-40 hex> commands=<digits> seconds=<digits> PASS"
        )
    tree_prefix = match.group("tree")
    if not plan_["tree"].startswith(tree_prefix):
        raise ShipError(
            f"Gate trailer tree {tree_prefix} does not match HEAD tree {plan_['tree']}"
        )
    zone = match.group("zone")
    if zone != plan_["zone"]:
        raise ShipError(f"Gate trailer zone {zone} does not match planned zone {plan_['zone']}")
    commands = int(match.group("commands"))
    expected_commands = len(plan_["gate"])
    if commands != expected_commands:
        raise ShipError(
            f"Gate trailer commands {commands} does not match policy commands {expected_commands}"
        )


def gate(repo: Path, base: str) -> dict:
    p = plan(repo, base)
    if not p["ok"]:
        raise ShipError("; ".join(p["problems"]))
    tree = p["tree"]
    result = run_gate(repo, p["gate"])
    new_head = append_gate_trailer(repo, gate_trailer(p, result))
    if git(repo, "rev-parse", "HEAD^{tree}") != tree:
        raise ShipError("tree changed while amending the Gate trailer: refusing to move main")
    return {"gated_sha": new_head, "tree": tree, "gate": result, "plan": p}


def publish(
    repo: Path,
    base: str,
    push: bool,
    review_root: Path | None = None,
    expected_patch: str | None = None,
    expected_message: str | None = None,
) -> dict:
    """Verify HEAD's Gate trailer against base policy, then fast-forward ``base``.

    With ``expected_patch`` the commit must be the reviewed patch and message. With
    ``review_root`` a zone C change also needs decision records covering its zone C paths
    (publish.ps1 passes it when there is no per-tree acceptance)."""
    policy = load_policy_from_ref(repo, base)
    p = plan(repo, base, policy=policy)
    if not p["ok"]:
        raise ShipError("; ".join(p["problems"]))
    trailer = read_gate_trailer(repo)
    validate_gate_trailer(repo, trailer, p)
    result: dict = {}
    if expected_patch is not None or expected_message is not None:
        result["reviewed_patch"] = check_reviewed_patch(
            repo, base, policy, expected_patch or "", expected_message or ""
        )
    if review_root is not None and p["zone"] == "C":
        result["decision_records"] = check_decision_records(repo, base, policy, review_root)
    new_head = git(repo, "rev-parse", "HEAD")
    move_base_and_maybe_push(repo, base, new_head, p["base_sha"], push)
    return {"shipped_sha": new_head, "pushed": push, "gate_trailer": trailer, "plan": p, **result}


def ship(repo: Path, base: str, push: bool) -> dict:
    """Run the gate, record it on the tip commit, move ``base`` to it, optionally push."""
    p = plan(repo, base)
    if not p["ok"]:
        raise ShipError("; ".join(p["problems"]))
    result = run_gate(repo, p["gate"])
    new_head = append_gate_trailer(repo, gate_trailer(p, result))
    if git(repo, "rev-parse", "HEAD^{tree}") != p["tree"]:
        raise ShipError("tree changed while amending the Gate trailer: refusing to move main")
    move_base_and_maybe_push(repo, base, new_head, p["base_sha"], push)
    return {"shipped_sha": new_head, "pushed": push, "gate": result, "plan": p}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--base", default="main")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--gate", action="store_true")
    mode.add_argument("--publish", action="store_true")
    mode.add_argument("--check-decision-record", type=Path, metavar="FILE")
    parser.add_argument(
        "--push", action="store_true", help="with --execute or --publish: ordinary push of base"
    )
    parser.add_argument("--review-root", type=Path, help="with --publish: zone C decision records")
    parser.add_argument("--expected-patch", help="with --publish: reviewed git patch-id --stable")
    parser.add_argument(
        "--expected-message", help="with --publish: SHA-256 of the reviewed message"
    )
    args = parser.parse_args()
    if args.push and not (args.execute or args.publish):
        parser.error("--push can only be used with --execute or --publish")
    if (args.review_root or args.expected_patch or args.expected_message) and not args.publish:
        parser.error("--review-root, --expected-patch and --expected-message need --publish")
    if bool(args.expected_patch) != bool(args.expected_message):
        parser.error("--expected-patch and --expected-message go together")
    repo = args.repo.resolve()
    try:
        # Compact one-line JSON: the full plan/ship result stays machine-readable without spending
        # ~30 lines (and thousands of tokens in a small model context) on indentation.
        if args.check_decision_record is not None:
            path = args.check_decision_record
            record = read_decision_record(path, dt.datetime.now(dt.UTC).date())
            result = {
                "decision": record["decision"],
                "record_name": decision_record_name(record["decision"]),
                "c_paths": record["c_paths"],
                "expected_result": record["expected_result"],
                "expires": record["expires"],
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        elif args.dry_run:
            result = plan(repo, args.base)
        elif args.gate:
            result = gate(repo, args.base)
        elif args.publish:
            result = publish(
                repo,
                args.base,
                args.push,
                review_root=args.review_root,
                expected_patch=args.expected_patch,
                expected_message=args.expected_message,
            )
        else:
            result = ship(repo, args.base, args.push)
        print(json.dumps(result, ensure_ascii=False, separators=(", ", ": ")))
        return 0
    except ShipError as exc:
        print(f"SHIP REFUSED: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
