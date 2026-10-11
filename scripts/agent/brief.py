"""Parse and validate pi agent brief front matter.

Only the deliberately small front-matter subset used by the workbench templates is supported.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

FIELDS = {
    "task",
    "run",
    "template",
    "zone",
    "model",
    "thinking",
    "tools",
    "owner_decision",
    "allow_done",
    "ship",
    "allowed_paths",
    "read_paths",
}
LIST_FIELDS = {"allowed_paths", "read_paths"}
OWNER_RE = re.compile(r"^owner_decision:[A-Za-z0-9_.-]+:\d{4}-\d{2}-\d{2}:[A-Za-z0-9_.-]+$")


class BriefError(ValueError):
    def __init__(self, errors: list[str]) -> None:
        super().__init__("; ".join(errors))
        self.errors = errors


def _bool_value(value: str) -> bool | str:
    low = value.strip().lower()
    if low == "true":
        return True
    if low == "false":
        return False
    return value


def parse_text(text: str) -> dict[str, Any]:
    if not text.startswith("---\n") and not text.startswith("---\r\n"):
        raise BriefError(["missing front matter"])
    lines = text.splitlines()
    if not lines or lines[0] != "---":
        raise BriefError(["front matter must start with ---"])
    out: dict[str, Any] = {}
    current: str | None = None
    closed = False
    for line in lines[1:]:
        if line == "---":
            closed = True
            break
        if line.startswith("  - "):
            if current not in LIST_FIELDS:
                raise BriefError([f"list item without list field: {line}"])
            out.setdefault(current, []).append(line[4:].strip())
            continue
        if ":" not in line:
            if line.strip():
                raise BriefError([f"unsupported front matter line: {line}"])
            continue
        key, raw = line.split(":", 1)
        key = key.strip()
        value = raw.strip()
        if key not in FIELDS:
            raise BriefError([f"unsupported field: {key}"])
        current = key
        if value == "[]":
            out[key] = []
        elif key in LIST_FIELDS and value == "":
            out[key] = []
        elif key in LIST_FIELDS:
            out[key] = [value]
        elif key == "allow_done":
            out[key] = _bool_value(value)
        else:
            out[key] = value
    if not closed:
        raise BriefError(["front matter must end with ---"])
    for field in LIST_FIELDS:
        out.setdefault(field, [])
    return out


def parse_file(path: Path) -> dict[str, Any]:
    return parse_text(path.read_text(encoding="utf-8-sig"))


def _git_grep_main(repo: Path, needle: str) -> bool:
    result = subprocess.run(
        ["git", "grep", "-F", "-e", needle, "main", "--", "docs/requirements"],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result.returncode == 0


def validate(data: dict[str, Any], repo: Path) -> list[str]:
    errors: list[str] = []
    if data.get("zone") not in {"A", "B", "C"}:
        errors.append("zone must be A, B or C")
    if data.get("model") not in {"local", "cloud"}:
        errors.append("model must be local or cloud")
    if data.get("tools") not in {"default", "readonly"}:
        errors.append("tools must be default or readonly")
    if data.get("ship") not in {"publish", "none"}:
        errors.append("ship must be publish or none")
    if not isinstance(data.get("allow_done"), bool):
        errors.append("allow_done must be boolean")
    allowed = data.get("allowed_paths") or []
    if data.get("tools") != "readonly" and not allowed:
        errors.append("allowed_paths must not be empty for non-readonly tasks")
    if data.get("zone") == "C":
        owner = str(data.get("owner_decision") or "")
        if not OWNER_RE.fullmatch(owner):
            errors.append("zone C requires owner_decision:<task>:<YYYY-MM-DD>:<slug>")
        elif not _git_grep_main(repo, owner):
            errors.append("zone C owner_decision was not found in main:docs/requirements")
    return errors


def show(path: Path, repo: Path) -> int:
    try:
        data = parse_file(path)
    except BriefError as exc:
        for error in exc.errors:
            print(error, file=sys.stderr)
        return 2
    errors = validate(data, repo)
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 2
    print(json.dumps(data, ensure_ascii=False, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Parse and validate pi agent brief front matter.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_show = sub.add_parser("show")
    p_show.add_argument("brief", type=Path)
    p_show.add_argument("--repo", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.cmd == "show":
        return show(args.brief, args.repo)
    return 2


if __name__ == "__main__":
    sys.exit(main())
