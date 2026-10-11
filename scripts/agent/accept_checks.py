# ruff: noqa: E501
"""Mechanical acceptance checks for pi agent runs.

The script reads git state and run artifacts, writes checks.json, and never imports code from the
candidate branch.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

SCHEMA = "aits.pi.accept-checks/0"
GATE_RE = re.compile(
    r"^zone=(?P<zone>[ABC]) tree=(?P<tree>[0-9a-f]{12,40}) commands=(?P<commands>\d+) "
    r"seconds=(?P<seconds>\d+(?:\.\d+)?) PASS$"
)


def run_git(repo: Path, args: list[str], ok: tuple[int, ...] = (0,)) -> subprocess.CompletedProcess[str]:
    res = subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, timeout=60)
    if res.returncode not in ok:
        raise RuntimeError(f"git {' '.join(args)} failed: {res.stderr}")
    return res


def git_lines(repo: Path, args: list[str], ok: tuple[int, ...] = (0,)) -> list[str]:
    out = run_git(repo, args, ok).stdout
    return [line for line in out.splitlines() if line]


def git_nul(repo: Path, args: list[str], ok: tuple[int, ...] = (0,)) -> list[str]:
    out = run_git(repo, args, ok).stdout
    return [item for item in out.split("\0") if item]


def load_brief(run_dir: Path) -> dict[str, Any]:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import brief  # type: ignore

    return brief.parse_file(run_dir / "brief.md")


def parse_policy(text: str) -> dict[str, Any]:
    zones: dict[str, list[str]] = {"A": [], "C": []}
    exclusions: list[str] = []
    section: str | None = None
    zone: str | None = None
    for raw in text.splitlines():
        line = raw.rstrip()
        stripped = line.strip()
        if stripped.startswith("tree_clean_exclusions:"):
            section = "exclusions"
            zone = None
            continue
        if stripped.startswith("zones:"):
            section = "zones"
            continue
        if section == "exclusions" and stripped.startswith("- "):
            exclusions.append(stripped[2:].strip().strip('"'))
            continue
        m = re.match(r"^\s{2}([AC]):\s*$", line)
        if section == "zones" and m:
            zone = m.group(1)
            continue
        if section == "zones" and zone in zones and stripped.startswith("- "):
            zones[zone].append(stripped[2:].strip().strip('"'))
    return {"zones": zones, "tree_clean_exclusions": exclusions}


def norm(path: str) -> str:
    return path.replace("\\", "/").strip("/")


def glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Match ship.py semantics: * and ? stay within one path segment; ** may cross /."""
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


def match_any(path: str, patterns: list[str]) -> bool:
    p = norm(path)
    return any(glob_to_regex(norm(pat)).match(p) for pat in patterns)


def zone_for(path: str, policy: dict[str, Any]) -> str:
    if match_any(path, policy["zones"].get("C", [])):
        return "C"
    if match_any(path, policy["zones"].get("A", [])):
        return "A"
    return "B"


def exclusion_pathspecs(policy: dict[str, Any]) -> list[str]:
    return [f":(exclude,literal){p}" for p in policy.get("tree_clean_exclusions", [])]


def head_is_current(repo: Path, head: str) -> bool:
    return run_git(repo, ["rev-parse", "HEAD"]).stdout.strip() == run_git(repo, ["rev-parse", head]).stdout.strip()


def changed_files(repo: Path, base: str, head: str, policy: dict[str, Any]) -> tuple[list[str], str]:
    files = set(git_lines(repo, ["diff", "--name-only", f"{base}...{head}"], ok=(0,)))
    detail = "base...head plus working tree"
    if head_is_current(repo, head):
        spec = ["--", ".", *exclusion_pathspecs(policy)]
        files.update(git_nul(repo, ["diff", "--name-only", "-z", *spec], ok=(0,)))
        files.update(git_nul(repo, ["diff", "--cached", "--name-only", "-z", *spec], ok=(0,)))
        files.update(git_nul(repo, ["ls-files", "--others", "--exclude-standard", "-z", *spec], ok=(0,)))
    else:
        detail = "base...head only; working tree ignored because HEAD is not --head"
    return sorted(norm(f) for f in files if f), detail


def planned_zone(repo: Path, base: str, head: str, policy: dict[str, Any]) -> str:
    changed = [norm(f) for f in git_lines(repo, ["diff", "--name-only", f"{base}..{head}"], ok=(0,)) if f]
    zones = {"A": [], "B": [], "C": []}
    for f in changed:
        zones[zone_for(f, policy)].append(f)
    for zone in ("C", "B", "A"):
        if zones[zone]:
            return zone
    return "A"


def diff_text(repo: Path, base: str, head: str, policy: dict[str, Any]) -> str:
    spec = ["--", ".", *exclusion_pathspecs(policy)]
    parts = [run_git(repo, ["diff", f"{base}...{head}", *spec], ok=(0,)).stdout]
    if not head_is_current(repo, head):
        return "\n".join(parts)
    wd = run_git(repo, ["diff", *spec], ok=(0,)).stdout
    cached = run_git(repo, ["diff", "--cached", *spec], ok=(0,)).stdout
    untracked_parts = []
    for path in git_nul(repo, ["ls-files", "--others", "--exclude-standard", "-z", *spec], ok=(0,)):
        untracked_parts.append(run_git(repo, ["diff", "--no-index", "--", "NUL", path], ok=(0, 1)).stdout)
    return "\n".join([*parts, wd, cached, *untracked_parts])


def check_allowed(files: list[str], allowed: list[str], scope_detail: str) -> tuple[bool, str]:
    bad = [f for f in files if not match_any(f, allowed)]
    detail = "all changes are within allowed_paths" if not bad else "out of scope: " + ", ".join(bad)
    return (not bad, f"{detail} ({scope_detail})")


def check_zones(repo: Path, files: list[str], policy: dict[str, Any], brief_data: dict[str, Any]) -> tuple[bool, str, dict[str, list[str]]]:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import brief  # type: ignore

    zones = {"A": [], "B": [], "C": []}
    for f in files:
        zones[zone_for(f, policy)].append(f)
    c_files = zones["C"]
    errors: list[str] = []
    if c_files:
        if brief_data.get("zone") != "C":
            errors.append("C-zone files require brief zone C")
        errors.extend([e for e in brief.validate(brief_data, repo) if "owner_decision" in e or "owner decision" in e])
    detail = "; ".join(f"{z}:{len(v)}" for z, v in zones.items())
    if errors:
        detail += " " + "; ".join(errors) + ": " + ", ".join(c_files)
    return (not errors, detail, zones)


def check_exclusions_and_views(files: list[str], policy: dict[str, Any]) -> tuple[bool, str]:
    bad = [f for f in files if f in [norm(x) for x in policy.get("tree_clean_exclusions", [])]]
    rendered = {"docs/task_register.md", "docs/task_register_completed.md"}
    touched_rendered = rendered.intersection(files)
    touched_tasks = [f for f in files if match_any(f, ["tasks/*.yaml"])]
    problems = []
    if bad:
        problems.append("tree_clean_exclusions touched: " + ", ".join(bad))
    if touched_rendered and not touched_tasks:
        problems.append("task rendered views changed without tasks/*.yaml")
    return (not problems, "; ".join(problems) or "no excluded files or orphan task views")


SKIP_RE = re.compile(
    r"@?pytest\.mark\.(skip|skipif|xfail)\b|pytest\.mark\.(skip|skipif|xfail)\b|"
    r"(?:^|[^.\w])mark\.(skip|skipif|xfail)\b|"
    r"pytest\.skip\.Exception\(|pytest\.skip\(|pytest\.importorskip\(|pytest\.xfail\(|"
    r"(?:^|[^.\w])(skip|xfail|importorskip)\(|"
    r"unittest\.SkipTest\b|\bSkipTest\(|"
    r"unittest\.(skip|skipIf|skipUnless|expectedFailure)\(|"
    r"@(?:skip|skipIf|skipUnless|expectedFailure)\b|"
    r"from\s+pytest\s+import\s+.*\b(skip|xfail|importorskip|mark)\b|"
    r"\b(?:test|it|describe)\.(?:skip|todo)\(|\bt\.(?:skip|todo)\(|"
    r"\{\s*(?:skip|todo)\s*:"
)
TS_TEST_RE = re.compile(r"^\s*(test|it|describe)(\.\w+)?\(")
PY_TEST_RE = re.compile(r"def test_|async def test_|\bit\(")


def limited_list(items: list[str], limit: int) -> list[str]:
    if len(items) <= limit:
        return items
    return [*items[:limit], f"+{len(items) - limit} more"]


def check_test_weakening(diff: str) -> tuple[bool, str]:
    in_test = False
    removed_bad: list[str] = []
    skip_review: list[str] = []
    tolerance: list[str] = []
    current = ""
    current_file = ""
    new_line: int | None = None
    removed_assert_by_file: dict[str, int] = {}
    added_assert_by_file: dict[str, int] = {}
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            current = line
            match = re.search(r" b/(.+)$", line)
            current_file = match.group(1) if match else ""
            in_test = bool(re.search(r" b/(tests/|.*\.test\.ts$)", line))
            new_line = None
            continue
        hunk = re.match(r"@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@", line)
        if hunk:
            new_line = int(hunk.group(1))
            continue
        if not in_test:
            continue
        body = line[1:] if line[:1] in "+-" else line
        line_no = new_line
        test_re = TS_TEST_RE if current_file.endswith(".test.ts") else PY_TEST_RE
        if line.startswith("-") and test_re.search(body):
            removed_assert_by_file[current + "#tests"] = removed_assert_by_file.get(current + "#tests", 0) + 1
        if line.startswith("+") and test_re.search(body):
            added_assert_by_file[current + "#tests"] = added_assert_by_file.get(current + "#tests", 0) + 1
        if line.startswith("-") and re.search(r"\bassert\b", body):
            removed_assert_by_file[current] = removed_assert_by_file.get(current, 0) + 1
        if line.startswith("+") and re.search(r"\bassert\b", body):
            added_assert_by_file[current] = added_assert_by_file.get(current, 0) + 1
        if line.startswith("+") and SKIP_RE.search(body):
            skip_review.append(f"{current_file}:{line_no or '?'}: {body.strip()}")
        if line[:1] in "+-" and re.search(r"approx\(|\babs=|\brel=|\batol\b|\brtol\b", body):
            tolerance.append(body.strip()[:120])
        if not line.startswith("-") and new_line is not None:
            new_line += 1
    for file_key, count in removed_assert_by_file.items():
        if added_assert_by_file.get(file_key, 0) < count:
            removed_bad.append(file_key)
    problems = []
    if removed_bad:
        problems.append(f"removed asserts/tests in {len(set(removed_bad))} diff(s)")
    if skip_review:
        problems.append("skip/xfail additions for review: " + "; ".join(limited_list(skip_review, 12)))
    if tolerance:
        problems.append("tolerance changes: " + "; ".join(limited_list(tolerance, 6)))
    return (not removed_bad, "; ".join(problems) or "no test weakening patterns")


def file_at(repo: Path, rev: str, path: str) -> str | None:
    res = subprocess.run(["git", "show", f"{rev}:{path}"], cwd=repo, capture_output=True, text=True, timeout=30)
    if res.returncode != 0:
        return None
    return res.stdout


def parse_status_notes(text: str | None) -> tuple[str | None, str]:
    if not text:
        return None, ""
    status = None
    notes_lines: list[str] = []
    in_notes = False
    for line in text.splitlines():
        if line.startswith("status:"):
            status = line.split(":", 1)[1].strip().strip('"')
        if line.startswith("notes:"):
            in_notes = True
            notes_lines.append(line)
            continue
        if in_notes:
            if line and not line.startswith(" "):
                in_notes = False
            else:
                notes_lines.append(line)
    return status, "\n".join(notes_lines)


def check_tasks(repo: Path, base: str, head: str, files: list[str], allow_done: bool) -> tuple[bool, str]:
    problems: list[str] = []
    for f in files:
        if not match_any(f, ["tasks/*.yaml"]):
            continue
        old = file_at(repo, base, f)
        new = file_at(repo, head, f) or (repo / f).read_text(encoding="utf-8")
        old_status, old_notes = parse_status_notes(old)
        new_status, new_notes = parse_status_notes(new)
        if not allow_done and new_status == "DONE" and old_status != "DONE":
            problems.append(f"{f} status changed to DONE while allow_done=false")
        if old_notes and new_notes and not new_notes.startswith(old_notes):
            problems.append(f"{f} notes were not append-only")
    return (not problems, "; ".join(problems) or "task notes/status rules passed")


def check_report(run_dir: Path) -> tuple[bool, str]:
    path = run_dir / "report.json"
    if not path.exists():
        return False, "report.json 缺失（守卫在运行结束时写入，运行中缺失是预期的）"
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    blocked = data.get("blocked") or []
    rules = sorted({str(b.get("rule") or b.get("reason") or b.get("tool") or "unknown") for b in blocked})
    oos = data.get("out_of_scope_files") or []
    detail = f"blocked={len(blocked)} rules={','.join(rules) if rules else '-'} out_of_scope={len(oos)}"
    if oos:
        detail += " files=" + ", ".join(map(str, oos))
    return (not oos, detail)


def check_equivalence(run_dir: Path) -> tuple[bool, str]:
    brief_text = (run_dir / "brief.md").read_text(encoding="utf-8")
    m = re.search(r"^##\s*等价检查.*?\n(.*?)(?=^##\s|\Z)", brief_text, re.S | re.M)
    section = m.group(1).strip() if m else "不适用"
    if section.startswith("不适用"):
        return True, "equivalence check not applicable"
    path = run_dir / "equivalence.json"
    if not path.exists():
        return False, "equivalence.json is required"
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    return (data.get("ok") is True, "equivalence.json ok=true" if data.get("ok") is True else "equivalence.json ok is not true")


def check_gate(repo: Path, base: str, head: str, policy: dict[str, Any], brief_data: dict[str, Any]) -> tuple[bool, str]:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import brief  # type: ignore

    sha = run_git(repo, ["rev-parse", head]).stdout.strip()
    tree = run_git(repo, ["rev-parse", f"{sha}^{{tree}}"]).stdout.strip()
    trailers = git_lines(repo, ["log", "-1", "--format=%(trailers:key=Gate,valueonly)", sha], ok=(0,))
    gate = trailers[-1].strip() if trailers else ""
    problems: list[str] = []
    if not gate:
        problems.append("missing Gate trailer on head")
    else:
        m = GATE_RE.fullmatch(gate)
        if not m:
            problems.append("Gate trailer format is invalid")
        else:
            gate_zone, gate_tree = m.group("zone"), m.group("tree")
            planned = planned_zone(repo, base, head, policy)
            if not tree.startswith(gate_tree):
                problems.append(f"Gate trailer tree {gate_tree} does not match head tree {tree}")
            if gate_zone != planned:
                problems.append(f"Gate trailer zone {gate_zone} does not match planned zone {planned}")
            if gate_zone == "C" and brief_data.get("zone") != "C":
                problems.append("Gate trailer zone=C but brief zone is not C")
    if brief_data.get("zone") == "C":
        owners = git_lines(repo, ["log", "--format=%(trailers:key=Owner-Decision,valueonly)", f"{base}..{head}"], ok=(0,))
        owners = [owner.strip() for owner in owners if owner.strip()]
        distinct = list(dict.fromkeys(owners))
        if not owners:
            problems.append("missing Owner-Decision trailer in base..head")
        elif str(brief_data.get("owner_decision") or "") not in owners:
            problems.append("brief owner_decision is not among Owner-Decision trailers in base..head")
        for owner in distinct:
            checked = dict(brief_data)
            checked["owner_decision"] = owner
            problems.extend([e for e in brief.validate(checked, repo) if "owner_decision" in e or "owner decision" in e])
    return (not problems, "; ".join(problems) or "gate trailers passed")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run mechanical acceptance checks for a pi agent run.")
    ap.add_argument("--repo", type=Path, required=True)
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--head", required=True)
    ap.add_argument("--base", default="main")
    ap.add_argument("--gate", action="store_true")
    args = ap.parse_args(argv)

    repo = args.repo.resolve()
    run_dir = args.run_dir.resolve()
    brief = load_brief(run_dir)
    policy = parse_policy(run_git(repo, ["show", f"{args.base}:config/gov008_ship.yaml"]).stdout)
    files, scope_detail = changed_files(repo, args.base, args.head, policy)
    diff = diff_text(repo, args.base, args.head, policy)
    head_sha = run_git(repo, ["rev-parse", args.head]).stdout.strip()

    checks: list[dict[str, Any]] = []
    ok, detail = check_allowed(files, list(brief.get("allowed_paths") or []), scope_detail)
    checks.append({"name": "changes within allowed_paths", "ok": ok, "detail": detail})
    ok, detail, zones = check_zones(repo, files, policy, brief)
    checks.append({"name": "file zones match brief", "ok": ok, "detail": detail, "zones": zones})
    ok, detail = check_exclusions_and_views(files, policy)
    checks.append({"name": "exclusions and task views", "ok": ok, "detail": detail})
    ok, detail = check_test_weakening(diff)
    checks.append({"name": "tests not weakened", "ok": ok, "detail": detail})
    ok, detail = check_tasks(repo, args.base, args.head, files, bool(brief.get("allow_done")))
    checks.append({"name": "task notes append-only and allow_done", "ok": ok, "detail": detail})
    ok, detail = check_report(run_dir)
    checks.append({"name": "run report", "ok": ok, "detail": detail})
    ok, detail = check_equivalence(run_dir)
    checks.append({"name": "equivalence artifact", "ok": ok, "detail": detail})
    if args.gate:
        ok, detail = check_gate(repo, args.base, args.head, policy, brief)
        checks.append({"name": "gate trailers", "ok": ok, "detail": detail})

    result = {"schema": SCHEMA, "head": head_sha, "checks": checks}
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "checks.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if all(c["ok"] for c in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
