"""GOV-008 ship gate: run local gates, record ``Gate:``, and publish by fast-forward.

Flow: task branch -> local gate -> ``Gate:`` trailer -> ``main`` moves to the same tree ->
ordinary push. ``--gate`` can stop after recording the tested tree; ``--publish`` verifies that
record without running branch code, then fast-forwards the base branch. ``--publish`` can also
check the reviewed patch (``--expected-patch``) instead of a predicted tree, and, for zone C, owner
decision records under the review root (``--review-root``) instead of a per-tree acceptance.

Research records (``research_records`` in the policy) ship on their own: a change that touches one
touches nothing else, and every plan checks them mechanically (new files or appended lines only,
numbered and chained after the records on the base, made on a published commit without local code
changes). ``--replay-records`` re-creates such a one-commit change on a base that moved meanwhile.

Parallel lanes (``owner_decision:DEVX-031:2026-10-14:parallel_pi_lanes``): several reviewed
one-commit changes wait for their turn to publish. ``--replay`` re-creates any other one-commit
change on the moved base with ``git merge-tree``, refuses a conflict outside the rendered task
views, and checks that the change itself (``--expected-change``: the patch fingerprint without
context lines) and the message stay the same. It is the only way an agent moves a change onto a
newer base; the gate runs again afterwards.

This is an agent-run gate, not an independent verifier (no branch protection; owner decision
2026-10-10). Zone C changes (see config/gov008_ship.yaml) must carry an ``Owner-Decision:`` trailer.

usage:
  python tools/gov008/ship.py --repo . --dry-run          # plan only: zones, gate, trailer checks
  python tools/gov008/ship.py --repo . --execute [--push] # run gate, amend trailer, move main
  python tools/gov008/ship.py --repo . --gate             # run gate and amend trailer only
  python tools/gov008/ship.py --repo . --publish [--push] # verify Gate trailer, move main
  python tools/gov008/ship.py --repo . --replay-records   # records-only commit onto a moved main
  python tools/gov008/ship.py --repo . --replay           # any other one-commit change onto it
  python tools/gov008/ship.py --check-decision-record <file> # validate one owner decision record
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import posixpath
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
CHANGE_DIFF_OPTIONS = tuple("--unified=0" if o == "--unified=3" else o for o in PATCH_DIFF_OPTIONS)
# On a failing gate command the terminal shows only this many trailing lines, enough to see pytest's
# short test summary. Display truncation only: the full output always goes to the gate log file.
GATE_FAILURE_TAIL_LINES = 60
GATE_LOG_DIRNAME = "gov008-ship"
# Research records (owner_decision:TRADING-2565:2026-10-13:research_throughput): a numbered record
# is one file per record, named like the bench names them; a commit field holds a full commit id.
RECORD_FILE_RE = re.compile(r"^(?P<number>[0-9]{4,})\.json$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
# A {field} placeholder in a files_at_commit path takes a record value that names one file.
RECORD_PATH_FIELD_RE = re.compile(r"\{(?P<field>[A-Za-z0-9_.]+)\}")
RECORD_PATH_VALUE_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")


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


def git(repo: Path, *args: str, check: bool = True, env: dict | None = None) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
        env=env,
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


def record_rule(path: str, policy: dict) -> tuple[str, dict] | None:
    """("numbered" | "appended", rule) for a research record path, or None for any other path."""
    records = policy.get("research_records") or {}
    for kind in ("numbered", "appended"):
        for rule in records.get(kind, []):
            if glob_to_regex(rule["glob"]).match(path):
                return kind, rule
    return None


def blob_bytes(repo: Path, rev: str, path: str) -> bytes | None:
    """The file's bytes at rev, or None when rev has no such file."""
    proc = subprocess.run(
        ["git", "-C", str(repo), "cat-file", "blob", f"{rev}:{path}"],
        capture_output=True,
        check=False,
    )
    return proc.stdout if proc.returncode == 0 else None


def chain_digest(data: bytes) -> str:
    """The research ledger's link hash: sha256 with CRLF read as LF (research_bench.ledger)."""
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


_MISSING = object()


def _field(record: object, dotted: str) -> object:
    value = record
    for key in dotted.split("."):
        if not isinstance(value, dict) or key not in value:
            return _MISSING
        value = value[key]
    return value


def _json_object(data: bytes) -> dict | None:
    try:
        value = json.loads(data.decode("utf-8"))
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def _record_path(template: str, record: dict, directory: str) -> str | None:
    """A files_at_commit path with {dir} and {field} filled in; None if a value is no file name."""
    values: list[str] = []

    def fill(match: re.Match[str]) -> str:
        name = match["field"]
        value = directory if name == "dir" else _field(record, name)
        if name != "dir" and (
            not isinstance(value, str) or RECORD_PATH_VALUE_RE.fullmatch(value) is None
        ):
            values.append(name)
            return ""
        return str(value)

    filled = posixpath.normpath(RECORD_PATH_FIELD_RE.sub(fill, template))
    if values or filled.startswith(("../", "/")) or filled in (".", ".."):
        return None
    return filled


def provenance_problems(
    repo: Path, name: str, record: dict, rule: dict, directory: str, published: str
) -> list[str]:
    """The record was made on a published commit, without local code changes, from its files."""
    problems: list[str] = []
    commit = _field(record, rule["commit_field"])
    if not isinstance(commit, str) or COMMIT_RE.fullmatch(commit) is None:
        return [f"{name}: {rule['commit_field']} is not a full commit id"]
    if not is_ancestor(repo, commit, published):
        return [f"{name}: {rule['commit_field']} {commit[:12]} is not a published commit"]
    if "modified_field" in rule and _field(record, rule["modified_field"]) is not False:
        problems.append(f"{name}: {rule['modified_field']} is not false (local code changes)")
    for hash_field, template in rule.get("files_at_commit", {}).items():
        digest = _field(record, hash_field)
        source = _record_path(template, record, directory)
        if source is None:
            problems.append(f"{name}: cannot name the file for {hash_field} from {template}")
            continue
        blob = blob_bytes(repo, commit, source)
        if blob is None:
            problems.append(f"{name}: {source} is not in commit {commit[:12]}")
        elif digest not in (hashlib.sha256(blob).hexdigest(), chain_digest(blob)):
            problems.append(f"{name}: {hash_field} is not the hash of {source} at {commit[:12]}")
    return problems


def _record_numbers(repo: Path, rev: str, directory: str) -> dict[int, str]:
    out = git(repo, "ls-tree", "--name-only", rev, "--", f"{directory}/")
    numbers: dict[int, str] = {}
    for path in out.splitlines():
        match = RECORD_FILE_RE.fullmatch(posixpath.basename(path))
        if match and posixpath.dirname(path) == directory:
            numbers[int(match["number"])] = path
    return numbers


def numbered_problems(
    repo: Path, old: str, new: str, directory: str, paths: list[str], rule: dict, published: str
) -> list[str]:
    """New records in one directory: numbered on from old's last record and chained to it."""
    existing = _record_numbers(repo, old, directory)
    problems: list[str] = []
    numbered: list[tuple[int, str]] = []
    for path in paths:
        match = RECORD_FILE_RE.fullmatch(posixpath.basename(path))
        if match is None:
            problems.append(f"{path}: a record file is named NNNN.json")
        else:
            numbered.append((int(match["number"]), path))
    if problems:
        return problems
    last = max(existing, default=0)
    previous_blob = blob_bytes(repo, old, existing[last]) if existing else None
    previous = None if previous_blob is None else chain_digest(previous_blob)
    for expected, (number, path) in enumerate(sorted(numbered), start=last + 1):
        if number != expected:
            return [*problems, f"{path}: expected record {expected:04d} next in {directory}"]
        data = blob_bytes(repo, new, path) or b""
        record = _json_object(data)
        if record is None:
            problems.append(f"{path} is not a JSON object")
        elif record.get("prev_sha256") != previous:
            problems.append(f"{path}: prev_sha256 does not match the record before it")
        else:
            problems += provenance_problems(
                repo, path, record, rule, posixpath.dirname(path), published
            )
        previous = chain_digest(data)
    return problems


def appended_problems(
    repo: Path, old: str, new: str, path: str, rule: dict, published: str
) -> list[str]:
    """Lines added at the end of a JSON-lines record file, each chained to the line before it."""
    before = blob_bytes(repo, old, path) or b""
    after = blob_bytes(repo, new, path) or b""
    if not after.startswith(before) or (before and not before.endswith(b"\n")):
        return [f"{path}: existing lines changed; a record file only gains lines at its end"]
    added = after[len(before) :]
    if not added.endswith(b"\n"):
        return [f"{path}: the added lines do not end with a newline"]
    lines = [line for line in before.replace(b"\r\n", b"\n").split(b"\n") if line]
    previous = chain_digest(lines[-1]) if lines else None
    problems: list[str] = []
    for position, line in enumerate(added.split(b"\n")[:-1], start=len(lines) + 1):
        name = f"{path} line {position}"
        record = _json_object(line)
        if record is None:
            problems.append(f"{name} is not a JSON object")
        elif record.get("prev_sha256") != previous:
            problems.append(f"{name}: prev_sha256 does not match the line before it")
        else:
            problems += provenance_problems(
                repo, name, record, rule, posixpath.dirname(path), published
            )
        previous = chain_digest(line)
    return problems


def records_problems(repo: Path, old: str, new: str, policy: dict, published: str) -> list[str]:
    """Mechanical checks for a records-only change old..new (owner decision research_throughput)."""
    out = git(repo, "diff", "--name-status", "--no-renames", f"{old}..{new}")
    status = {line.split("\t", 1)[1]: line[0] for line in out.splitlines() if line}
    modes = git(repo, "ls-tree", new, "--", *status) if status else ""
    regular = {line.split("\t", 1)[1] for line in modes.splitlines() if line.startswith("100644 ")}
    problems: list[str] = []
    groups: dict[tuple[str, int], list[str]] = {}
    for path, letter in sorted(status.items()):
        found = record_rule(path, policy)
        if found is None:
            problems.append(f"{path} is not a research record")
            continue
        kind, rule = found
        if kind == "numbered" and letter != "A":
            problems.append(f"{path}: a numbered record is only ever added (status {letter})")
        elif letter not in ("A", "M"):
            problems.append(f"{path}: a record file is only ever appended (status {letter})")
        elif path not in regular:
            problems.append(f"{path}: a record is a regular file (mode 100644)")
        elif kind == "numbered":
            index = policy["research_records"]["numbered"].index(rule)
            groups.setdefault((posixpath.dirname(path), index), []).append(path)
        else:
            problems += appended_problems(repo, old, new, path, rule, published)
    for (directory, index), paths in sorted(groups.items()):
        rule = policy["research_records"]["numbered"][index]
        problems += numbered_problems(repo, old, new, directory, paths, rule, published)
    return problems


def plan(repo: Path, base: str = "main", policy: dict | None = None) -> dict:
    """Everything ship would decide, without running tests or moving refs."""
    policy = load_policy(repo) if policy is None else policy
    branch = git(repo, "branch", "--show-current")
    problems: list[str] = []
    if branch in ("", base):
        problems.append(f"must run from a task branch, not '{branch or 'detached HEAD'}'")
    ancestor = is_ancestor(repo, base, "HEAD")
    if not ancestor:
        problems.append(
            f"{base} is not an ancestor of HEAD: rebase or merge {base} first (ship never does; "
            "one records-only commit: --replay-records)"
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
    gate_commands = policy["gates"][zone]
    records = [p for p in changed if record_rule(p, policy) is not None]
    research: dict | None = None
    if records and ancestor:
        others = [p for p in changed if record_rule(p, policy) is None]
        if others:
            problems.append(
                f"research records ship on their own; this change also has {others[:5]}"
            )
        else:
            problems += records_problems(repo, base, "HEAD", policy, published=base)
            gate_commands = policy["gates"]["records"]
            research = {"paths": len(records)}
            if git(repo, "rev-list", "--count", f"{base}..HEAD") == "1":
                research["patch"] = patch_fingerprint(repo, base, policy)
                research["message"] = message_fingerprint(repo)
    task_globs = policy.get("task_records", [])
    if (
        research is None
        and zone == "B"
        and task_globs
        and "tasks" in policy["gates"]
        and all(any(glob_to_regex(g).match(p) for g in task_globs) for p in by_zone["B"])
    ):
        # Task cards only (plus zone A records): the card schema and the rendered views are what a
        # change can break, and the task gate checks exactly those instead of the product suite.
        gate_commands = policy["gates"]["tasks"]
    result = {
        "branch": branch,
        "base": base,
        "base_sha": git(repo, "rev-parse", base),
        "head_sha": git(repo, "rev-parse", "HEAD"),
        "tree": git(repo, "rev-parse", "HEAD^{tree}"),
        "changed_paths": len(changed),
        "paths_by_zone": {z: len(v) for z, v in by_zone.items()},
        "zone": zone,
        "owner_decisions": decisions,
        "gate": gate_commands,
        "problems": problems,
        "ok": not problems,
    }
    if research is not None:
        result["research_records"] = research
    if ancestor and changed and git(repo, "rev-list", "--count", f"{base}..HEAD") == "1":
        # What a review names (DEVX-031): publish checks one of these with the message.
        try:
            result["fingerprints"] = {
                "patch": patch_fingerprint(repo, base, policy),
                "change": change_fingerprint(repo, base, policy),
                "message": message_fingerprint(repo),
            }
        except ShipError as exc:
            result["fingerprints"] = {"error": str(exc)}
    return result


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


def patch_fingerprint(repo: Path, base: str, policy: dict, head: str = "HEAD") -> str:
    """``git patch-id --stable`` of base..head without rendered views and registered exclusions."""
    return _fingerprint(repo, base, policy, head, PATCH_DIFF_OPTIONS)


def change_fingerprint(repo: Path, base: str, policy: dict, head: str = "HEAD") -> str:
    """The patch fingerprint without context lines: the lines each file gains and loses.

    A change moved onto a newer base keeps it when the base only changed lines near it, which the
    patch fingerprint counts through its context lines (DEVX-031). Where the lines land is left to
    the gate, which runs again on the moved change."""
    return _fingerprint(repo, base, policy, head, CHANGE_DIFF_OPTIONS)


def _fingerprint(repo: Path, base: str, policy: dict, head: str, options: tuple[str, ...]) -> str:
    excluded = [*policy.get("tree_clean_exclusions", []), *RENDERED_VIEWS]
    spec = [".", *[f":(exclude,literal){p}" for p in excluded]]
    diff = subprocess.run(
        ["git", "-C", str(repo), "diff", *options, f"{base}..{head}", "--", *spec],
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
    repo: Path,
    base: str,
    policy: dict,
    expected_patch: str | None,
    expected_message: str,
    expected_change: str | None = None,
) -> dict:
    """The branch is one commit on base with the reviewed patch (or change) and message."""
    if (expected_patch is None) == (expected_change is None):
        raise ShipError("name exactly one of the reviewed patch and the reviewed change")
    expected = expected_patch if expected_patch is not None else expected_change
    kind = "patch" if expected_patch is not None else "change"
    if re.fullmatch(r"[0-9a-f]{40}", expected or "") is None:
        raise ShipError(f"expected {kind} must be a 40-character lowercase hex patch id")
    if re.fullmatch(r"[0-9a-f]{64}", expected_message) is None:
        raise ShipError("expected message must be a 64-character lowercase hex SHA-256")
    count = git(repo, "rev-list", "--count", f"{base}..HEAD")
    if count != "1":
        raise ShipError(f"a reviewed {kind} publishes exactly one commit on {base}, found {count}")
    fingerprint = patch_fingerprint if kind == "patch" else change_fingerprint
    actual = fingerprint(repo, base, policy)
    if actual != expected:
        raise ShipError(f"{kind} {actual} does not match the reviewed {kind} {expected}")
    message = message_fingerprint(repo)
    if message != expected_message:
        raise ShipError(
            f"commit message {message} does not match the reviewed message {expected_message}"
        )
    return {kind: actual, "message": message}


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


def require_clean_after_gate(repo: Path) -> None:
    """The gate tested the working tree: refuse to record PASS if it is no longer the commit."""
    exclusions = load_policy(repo).get("tree_clean_exclusions", [])
    dirty = dirty_paths(repo, exclusions)
    if dirty:
        raise ShipError(
            f"working tree changed while the gate ran, so the result is not for the commit: "
            f"{dirty[:5]}"
        )


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


# Seconds before the 2nd and 3rd push attempt: a dropped connection is retried (AGENTS.md).
PUSH_RETRY_DELAYS = (2.0, 5.0)


def push_or_restore(repo: Path, base: str, new_head: str, old_base_sha: str) -> None:
    """Push ``base``; when every attempt fails, put local ``base`` back unless the remote moved.

    Local ``base`` was already moved to ``new_head``. If the remote still has the old base the
    publish can simply be run again, so local ``base`` goes back with a compare-and-swap; if the
    push actually arrived (a lost reply) it counts as pushed; anything else is left for a person.
    """
    failure = ""
    attempts = 0
    for delay in (0.0, *PUSH_RETRY_DELAYS):
        if delay:
            time.sleep(delay)
        attempts += 1
        try:
            git(repo, "push", "origin", base)
            return
        except ShipError as exc:
            failure = str(exc)
    try:
        git(repo, "fetch", "origin", base)
        remote = git(repo, "rev-parse", f"origin/{base}")
    except ShipError as exc:
        raise ShipError(
            f"git push origin {base} failed {attempts} times ({failure}) and origin could not be "
            f"checked ({exc}): local {base} stays at {new_head}; when the network is back run "
            f"`git fetch origin {base}` and, if origin/{base} is still {old_base_sha}, "
            f"`git push origin {base}`"
        ) from exc
    if remote == new_head:
        return
    if remote == old_base_sha:
        git(repo, "update-ref", f"refs/heads/{base}", old_base_sha, new_head)
        raise ShipError(
            f"git push origin {base} failed {attempts} times ({failure}); origin/{base} did not "
            f"move, so local {base} is back at {old_base_sha}: run the same publish command again"
        )
    raise ShipError(
        f"git push origin {base} failed {attempts} times ({failure}) and origin/{base} moved to "
        f"{remote}: local {base} stays at {new_head}; stop and report"
    )


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
        push_or_restore(repo, base, new_head, old_base_sha)
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
        raise ShipError(f"Gate trailer tree {tree_prefix} does not match HEAD tree {plan_['tree']}")
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
    require_clean_after_gate(repo)
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
    expected_change: str | None = None,
) -> dict:
    """Verify HEAD's Gate trailer against base policy, then fast-forward ``base``.

    With ``expected_patch`` (or ``expected_change``) the commit must be the reviewed patch (or
    change) and message. With
    ``review_root`` a zone C change also needs decision records covering its zone C paths
    (publish.ps1 passes it when there is no per-tree acceptance)."""
    policy = load_policy_from_ref(repo, base)
    p = plan(repo, base, policy=policy)
    if not p["ok"]:
        raise ShipError("; ".join(p["problems"]))
    trailer = read_gate_trailer(repo)
    validate_gate_trailer(repo, trailer, p)
    result: dict = {}
    if expected_patch is not None or expected_change is not None or expected_message is not None:
        result["reviewed_patch"] = check_reviewed_patch(
            repo, base, policy, expected_patch, expected_message or "", expected_change
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
    require_clean_after_gate(repo)
    new_head = append_gate_trailer(repo, gate_trailer(p, result))
    if git(repo, "rev-parse", "HEAD^{tree}") != p["tree"]:
        raise ShipError("tree changed while amending the Gate trailer: refusing to move main")
    move_base_and_maybe_push(repo, base, new_head, p["base_sha"], push)
    return {"shipped_sha": new_head, "pushed": push, "gate": result, "plan": p}


def blob_id(repo: Path, rev: str, path: str) -> str | None:
    return git(repo, "rev-parse", "--verify", "--quiet", f"{rev}:{path}", check=False) or None


def commit_like(repo: Path, old: str, tree: str, parent: str) -> str:
    """A commit of ``tree`` on ``parent`` with ``old``'s author and message, minus Gate trailers."""
    name, email, date = git(
        repo, "log", "-1", "--format=%an%x00%ae%x00%ad", "--date=raw", old
    ).split("\x00")
    message = git(repo, "log", "-1", "--format=%B", old)
    lines = [line for line in message.splitlines() if not line.startswith(f"{GATE_TRAILER}: ")]
    author = {"GIT_AUTHOR_NAME": name, "GIT_AUTHOR_EMAIL": email, "GIT_AUTHOR_DATE": date}
    # The message goes to git as bytes: text-mode stdin turns "\n" into "\r\n" on Windows.
    message_file = git_dir(repo) / f"gov008-replay-{os.getpid()}.msg"
    message_file.write_bytes(("\n".join(lines).rstrip() + "\n").encode("utf-8"))
    try:
        return git(
            repo,
            "commit-tree",
            tree,
            "-p",
            parent,
            "-F",
            str(message_file),
            env={**os.environ, **author},
        )
    finally:
        message_file.unlink(missing_ok=True)


def replay_records(repo: Path, base: str = "main") -> dict:
    """Re-create the branch's one records-only commit on the current ``base``.

    Research records are made on a published commit and ship on their own. When ``base`` moved
    meanwhile, this puts the same added records and appended lines on top of it, with the same
    message, author and patch fingerprint, and refuses anything that would not apply unchanged."""
    policy = load_policy_from_ref(repo, base)
    branch = git(repo, "branch", "--show-current")
    if branch in ("", base):
        raise ShipError(f"must run from a task branch, not '{branch or 'detached HEAD'}'")
    dirty = dirty_paths(repo, policy.get("tree_clean_exclusions", []))
    if dirty:
        raise ShipError(f"working tree is not clean: {dirty[:5]}")
    old = git(repo, "rev-parse", "HEAD")
    new_base = git(repo, "rev-parse", base)
    if is_ancestor(repo, new_base, old):
        raise ShipError(f"{base} is already an ancestor of HEAD: nothing to replay")
    parent = git(repo, "rev-parse", "--verify", "--quiet", f"{old}^", check=False)
    if not parent or git(repo, "merge-base", new_base, old) != parent:
        raise ShipError(f"replay takes exactly one commit on top of a commit of {base}")
    changed = [p for p in git(repo, "diff", "--name-only", f"{parent}..{old}").splitlines() if p]
    if not changed or any(record_rule(p, policy) is None for p in changed):
        raise ShipError("replay takes a commit that changes research records and nothing else")
    problems = records_problems(repo, parent, old, policy, published=new_base)
    if problems:
        raise ShipError("research records: " + "; ".join(problems))
    moved = [p for p in changed if blob_id(repo, parent, p) != blob_id(repo, new_base, p)]
    if moved:
        raise ShipError(f"{base} changed {moved[:5]} since the records were made: refusing")
    index = git_dir(repo) / f"gov008-replay-{os.getpid()}.index"
    env = {**os.environ, "GIT_INDEX_FILE": str(index)}
    try:
        git(repo, "read-tree", new_base, env=env)
        for path in changed:
            blob = blob_id(repo, old, path)
            git(repo, "update-index", "--add", "--cacheinfo", f"100644,{blob},{path}", env=env)
        tree = git(repo, "write-tree", env=env)
    finally:
        index.unlink(missing_ok=True)
    new = commit_like(repo, old, tree, new_base)
    patch = patch_fingerprint(repo, parent, policy, head=old)
    if patch_fingerprint(repo, new_base, policy, head=new) != patch:
        raise ShipError("the replayed commit's patch differs from the original: refusing")
    if message_fingerprint(repo, new) != message_fingerprint(repo, old):
        raise ShipError("the replayed commit's message differs from the original: refusing")
    git(repo, "update-ref", f"refs/heads/{branch}", new, old)
    git(repo, "read-tree", "-m", "-u", old, new)
    return {
        "branch": branch,
        "replayed_from": old,
        "replayed_sha": new,
        "base_sha": new_base,
        "tree": tree,
        "research_records": len(changed),
        "patch": patch,
        "message": message_fingerprint(repo, new),
    }


def replay(repo: Path, base: str = "main", python: str | None = None) -> dict:
    """Re-create the branch's one reviewed commit on the current ``base`` (DEVX-031).

    For a change that waited while other changes published. ``git merge-tree`` merges it onto the
    moved base without touching the working tree; a conflict refuses, except in the rendered task
    views, which come from the base and are rendered again (``tools/tasks.py render``). The new
    commit keeps the author, the message without Gate trailers, and the change fingerprint; it needs
    a new gate."""
    policy = load_policy_from_ref(repo, base)
    branch = git(repo, "branch", "--show-current")
    if branch in ("", base):
        raise ShipError(f"must run from a task branch, not '{branch or 'detached HEAD'}'")
    dirty = dirty_paths(repo, policy.get("tree_clean_exclusions", []))
    if dirty:
        raise ShipError(f"working tree is not clean: {dirty[:5]}")
    old = git(repo, "rev-parse", "HEAD")
    new_base = git(repo, "rev-parse", base)
    if is_ancestor(repo, new_base, old):
        raise ShipError(f"{base} is already an ancestor of HEAD: nothing to replay")
    parent = git(repo, "rev-parse", "--verify", "--quiet", f"{old}^", check=False)
    if not parent or git(repo, "merge-base", new_base, old) != parent:
        raise ShipError(f"replay takes exactly one commit on top of a commit of {base}")
    changed = [p for p in git(repo, "diff", "--name-only", f"{parent}..{old}").splitlines() if p]
    if not changed:
        raise ShipError("the commit to replay changes nothing")
    if all(record_rule(p, policy) is not None for p in changed):
        raise ShipError("a research records commit replays with --replay-records")
    change = change_fingerprint(repo, parent, policy, head=old)
    message = message_fingerprint(repo, old)
    merged = subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "merge-tree",
            "--write-tree",
            "--name-only",
            "--no-messages",
            f"--merge-base={parent}",
            new_base,
            old,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if merged.returncode not in (0, 1):
        raise ShipError(f"git merge-tree failed: {merged.stderr.strip()}")
    lines = [line for line in merged.stdout.splitlines() if line.strip()]
    if not lines:
        raise ShipError("git merge-tree printed no tree")
    conflicts = sorted(set(lines[1:])) if merged.returncode == 1 else []
    others = [p for p in conflicts if p not in RENDERED_VIEWS]
    if others:
        raise ShipError(f"the change conflicts with {base} in {others[:10]}: refusing")
    views = [v for v in RENDERED_VIEWS if v in changed or v in conflicts]
    index = git_dir(repo) / f"gov008-replay-{os.getpid()}.index"
    env = {**os.environ, "GIT_INDEX_FILE": str(index)}
    try:
        git(repo, "read-tree", lines[0], env=env)
        for view in views:
            blob = blob_id(repo, new_base, view)
            if blob is None:
                git(repo, "update-index", "--force-remove", "--", view, env=env)
            else:
                git(repo, "update-index", "--add", "--cacheinfo", f"100644,{blob},{view}", env=env)
        tree = git(repo, "write-tree", env=env)
    finally:
        index.unlink(missing_ok=True)
    new = commit_like(repo, old, tree, new_base)
    git(repo, "update-ref", f"refs/heads/{branch}", new, old)
    git(repo, "read-tree", "-m", "-u", old, new)
    if views:
        # The views are rendered from tasks/*.yaml; the gate's staleness test checks the result.
        rendered_run = subprocess.run(
            [python or sys.executable, "tools/tasks.py", "render"],
            cwd=repo,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if rendered_run.returncode != 0:
            raise ShipError(
                f"tools/tasks.py render failed (branch is now {new}, was {old}): "
                f"{rendered_run.stderr.strip()[-500:]}"
            )
        for view in RENDERED_VIEWS:
            if (repo / view).exists():
                git(repo, "add", "--", view)
            else:
                git(repo, "rm", "-q", "--cached", "--ignore-unmatch", "--", view)
        rendered = git(repo, "write-tree")
        if rendered != tree:
            newer = commit_like(repo, old, rendered, new_base)
            git(repo, "update-ref", f"refs/heads/{branch}", newer, new)
            new, tree = newer, rendered
    if change_fingerprint(repo, new_base, policy, head=new) != change:
        raise ShipError(
            f"the replayed change differs from the original: refusing (branch is now {new}, "
            f"was {old})"
        )
    if message_fingerprint(repo, new) != message:
        raise ShipError(
            f"the replayed message differs from the original: refusing (branch is now {new}, "
            f"was {old})"
        )
    return {
        "branch": branch,
        "replayed_from": old,
        "replayed_sha": new,
        "base_sha": new_base,
        "tree": tree,
        "rendered_views": views,
        "patch": patch_fingerprint(repo, new_base, policy, head=new),
        "change": change,
        "message": message,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--base", default="main")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--gate", action="store_true")
    mode.add_argument("--publish", action="store_true")
    mode.add_argument("--replay-records", action="store_true")
    mode.add_argument("--replay", action="store_true")
    mode.add_argument("--check-decision-record", type=Path, metavar="FILE")
    parser.add_argument(
        "--push", action="store_true", help="with --execute or --publish: ordinary push of base"
    )
    parser.add_argument("--review-root", type=Path, help="with --publish: zone C decision records")
    parser.add_argument("--expected-patch", help="with --publish: reviewed git patch-id --stable")
    parser.add_argument(
        "--expected-change", help="with --publish: reviewed change fingerprint (no context lines)"
    )
    parser.add_argument(
        "--expected-message", help="with --publish: SHA-256 of the reviewed message"
    )
    args = parser.parse_args()
    if args.push and not (args.execute or args.publish):
        parser.error("--push can only be used with --execute or --publish")
    reviewed = (args.expected_patch, args.expected_change, args.expected_message)
    if (args.review_root or any(reviewed)) and not args.publish:
        parser.error(
            "--review-root, --expected-patch, --expected-change and --expected-message need "
            "--publish"
        )
    if args.expected_patch and args.expected_change:
        parser.error("use either --expected-patch or --expected-change, not both")
    if bool(args.expected_patch or args.expected_change) != bool(args.expected_message):
        parser.error("--expected-patch (or --expected-change) and --expected-message go together")
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
        elif args.replay_records:
            result = replay_records(repo, args.base)
        elif args.replay:
            result = replay(repo, args.base)
        elif args.publish:
            result = publish(
                repo,
                args.base,
                args.push,
                review_root=args.review_root,
                expected_patch=args.expected_patch,
                expected_message=args.expected_message,
                expected_change=args.expected_change,
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
