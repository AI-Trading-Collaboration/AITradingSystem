"""依据 owner_decision:DEVX-034:2026-10-25:work_overview_private 生成私有工作总览。

只读仓库指定 ref 的已提交内容、生产状态根的调度摘要和报告质量门禁，
以及发布包目录的 current.json 和 deploy_log.jsonl；只写 --out 一个文件，不联网。
日报正文、价格和供应商原始数据不进输出；读不到的块写“读不到”，不让生成失败。
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ai_trading_system import paths, research_site
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = "work_overview.v1"
MAINLINES_PATH = "docs/work_overview/mainlines.yaml"
MAINLINES_SCHEMA = "work_mainlines.v1"
# 展示协议：日历覆盖两周，提交和部署只展示最近的记录，不作投资判断。
CALENDAR_DAYS = 14
RECENT_COMMITS = 10
DEPLOYS = 5
ACTIVE_STATUSES = (
    "IN_PROGRESS",
    "VALIDATING",
    "PROPOSED",
    "READY",
    "BLOCKED_OWNER_INPUT",
    "BLOCKED_EXTERNAL",
)
TREE_PATHS = (
    "tasks",
    "docs/work_overview",
    "docs/requirements",
    "research",
    "config/research",
    "src/ai_trading_system/research_screens/signals",
)
DAY_RESULTS = ("PASS", "RESCUED", "FAILED", "NO_NEW_DAY", "NO_RUN")
JST = timezone(timedelta(hours=9))
_CARD_FIELDS = ("id", "area", "status", "priority", "next_owner", "next_step")
_SCHEDULER = "outputs/run_control/scheduler"
_REPORTS = "outputs/reports"
_SUMMARY_NAME = re.compile(r"(\d{4}-\d{2}-\d{2})_((?:PRIMARY|RESCUE)(?:_\d+)?)\.summary\.json")
_GATE_NAME = re.compile(r"report_quality_gate_(\d{4}-\d{2}-\d{2})\.json")


def waits_for_owner(status: str | None, next_owner: str | None) -> bool:
    """只看负责人本身，不把括号内的说明当作等待对象。"""
    if status == "BLOCKED_OWNER_INPUT":
        return True
    if status not in ACTIVE_STATUSES or not isinstance(next_owner, str):
        return False
    owner = re.split(r"[（(]", next_owner, maxsplit=1)[0].lower()
    return "baker" in owner or "owner" in owner


def day_result(runs: Sequence[Mapping[str, Any]]) -> str:
    """按已排序的调用，以第一次成功决定当天结论。"""
    if not runs:
        return "NO_RUN"
    for run in runs:
        if run.get("status") == "PASS":
            return "PASS" if run.get("window") == "PRIMARY" else "RESCUED"
    if all(run.get("status") == "RUN_CONTROL_ALREADY_COMPLETE" for run in runs):
        return "NO_NEW_DAY"
    return "FAILED"


def _reason(exc: Exception) -> str:
    lines = str(exc).splitlines()
    message = lines[0] if lines else ""
    return f"{type(exc).__name__}: {message}" if message else type(exc).__name__


def _unreadable(what: str) -> dict[str, Any]:
    return {"status": "unreadable", "error": f"读不到：{what}"}


def _ok(**fields: Any) -> dict[str, Any]:
    return {"status": "ok", "error": None, **fields}


def _guard(label: str, problems: list[str], action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return action()
    except Exception as exc:
        reason = _reason(exc)
        problems.append(f"{label}: 读不到（{reason}）")
        return _unreadable(f"{label}（{reason}）")


def _git(repo: Path, *args: str) -> bytes:
    options = ["-c", "core.autocrlf=false"] if args[0] in ("archive", "log") else []
    return subprocess.run(
        ["git", "-C", str(repo), *options, *args], check=True, capture_output=True
    ).stdout


def _git_text(repo: Path, *args: str) -> str:
    return _git(repo, *args).decode("utf-8").strip()


def _iso(stamp: str) -> str | None:
    """统一不同 Git 版本的 UTC 写法，保留原时差而不转换时区。"""
    try:
        return datetime.fromisoformat(stamp.strip()).isoformat()
    except ValueError:
        return None


def _jst_day(value: str) -> str:
    return datetime.fromisoformat(value).astimezone(JST).date().isoformat()


def _cards(tree: Path, repo: Path, commit: str, problems: list[str]) -> dict[str, dict[str, Any]]:
    cards: dict[str, dict[str, Any]] = {}
    for path in sorted((tree / "tasks").glob("*.yaml")):
        relative = f"tasks/{path.name}"
        try:
            raw = safe_load_yaml_path(path)
        except Exception as exc:
            problems.append(f"{relative}: 读不到（{_reason(exc)}）")
            continue
        if not isinstance(raw, Mapping) or not isinstance(raw.get("id"), str):
            problems.append(f"{relative}: 读不到（没有 id）")
            continue
        card = {
            key: raw.get(key) if isinstance(raw.get(key), str) else None for key in _CARD_FIELDS
        }
        updated = None
        try:
            timestamp = _git_text(repo, "log", "-1", "--format=%cI", commit, "--", relative)
            if timestamp:
                updated = _jst_day(timestamp)
        except (OSError, subprocess.CalledProcessError, ValueError):
            pass
        card["updated"] = updated
        cards[raw["id"]] = card
    return cards


def _mainlines(tree: Path) -> Mapping[str, Any]:
    raw = safe_load_yaml_path(tree / MAINLINES_PATH)
    if not isinstance(raw, Mapping) or raw.get("schema_version") != MAINLINES_SCHEMA:
        raise ValueError("schema_version 不是 work_mainlines.v1")
    lines = raw.get("mainlines")
    research_cards = raw.get("research_cards", [])
    if not isinstance(lines, list) or not _string_list(research_cards):
        raise ValueError("mainlines 或 research_cards 不是列表")
    for line in lines:
        if (
            not isinstance(line, Mapping)
            or not all(
                isinstance(line.get(key), str) for key in ("id", "title", "goal", "milestone")
            )
            or not _string_list(line.get("cards"))
        ):
            raise ValueError("主线缺少字符串字段或 cards 不是字符串列表")
    return raw


def _string_list(value: object) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def _engineering(
    tree: Path, repo: Path, commit: str, cards: dict[str, dict[str, Any]], problems: list[str]
) -> dict[str, Any]:
    try:
        raw = _mainlines(tree)
    except Exception as exc:
        problems.append(f"{MAINLINES_PATH}: 读不到（{_reason(exc)}）")
        return _unreadable(MAINLINES_PATH)
    listed = [card_id for line in raw["mainlines"] for card_id in line["cards"]]
    listed.extend(raw.get("research_cards", []))
    duplicates = sorted(card_id for card_id, count in Counter(listed).items() if count > 1)
    missing = list(dict.fromkeys(card_id for card_id in listed if card_id not in cards))
    unassigned = [
        cards[card_id]
        for card_id in sorted(cards)
        if cards[card_id]["status"] in ACTIVE_STATUSES and card_id not in listed
    ]
    problems.extend(f"主线文件：没有卡 {card_id}" for card_id in missing)
    problems.extend(f"主线文件：{card_id} 出现了不止一次" for card_id in duplicates)
    problems.extend(f"主线文件：在办的卡 {card['id']} 不在任何主线里" for card in unassigned)
    lines = []
    for line in raw["mainlines"]:
        members = [cards[card_id] for card_id in line["cards"] if card_id in cards]
        lines.append(
            {
                **{key: line[key] for key in ("id", "title", "goal", "milestone")},
                "cards": members,
                "counts": dict(Counter(card["status"] for card in members)),
            }
        )
    recent = []
    for row in _git_text(
        repo, "log", "-n", str(RECENT_COMMITS), "--format=%H%x1f%cI%x1f%s", commit
    ).splitlines():
        sha, timestamp, subject = row.split("\x1f", 2)
        recent.append({"commit": sha[:12], "date": _jst_day(timestamp), "subject": subject})
    return _ok(
        updated=str(raw["updated"]) if raw.get("updated") is not None else None,
        mainlines=lines,
        research_cards=[
            cards[card_id] for card_id in raw.get("research_cards", []) if card_id in cards
        ],
        unassigned=unassigned,
        duplicates=duplicates,
        missing=missing,
        totals=dict(Counter(card["status"] for card in cards.values())),
        recent=recent,
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _pick(value: Any, keys: Sequence[str]) -> dict[str, Any]:
    mapping = _mapping(value)
    return {key: mapping.get(key) for key in keys}


def _research(tree: Path) -> dict[str, Any]:
    raw = research_site.collect(tree)
    return _ok(
        summary=raw.get("summary"),
        charter=_pick(raw.get("charter"), ("period", "path", "term", "direction")),
        forward_ledger=_pick(raw.get("forward_ledger"), ("days", "start", "end")),
        forward_records=_pick(raw.get("forward_records"), ("count", "pending")),
        families=_mapping(raw.get("families")).get("status_counts"),
    )


def _repository(
    repo: Path, ref: str, problems: list[str]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    source: dict[str, Any] = {"ref": ref, "commit": None, "commit_time": None}
    unavailable = _unreadable(f"仓库 {ref}")
    try:
        commit = _git_text(repo, "rev-parse", "--verify", f"{ref}^{{commit}}")
        timestamp = _git_text(repo, "log", "-1", "--format=%cI", commit)
    except (OSError, subprocess.CalledProcessError):
        problems.append(f"仓库：读不到 {ref}")
        return source, unavailable.copy(), unavailable.copy(), unavailable.copy()
    source.update(commit=commit, commit_time=_iso(timestamp))
    try:
        with tempfile.TemporaryDirectory(prefix="work_overview_") as directory:
            tree = Path(directory)
            included = []
            for relative in TREE_PATHS:
                try:
                    _git(repo, "cat-file", "-e", f"{commit}:{relative}")
                except subprocess.CalledProcessError:
                    continue
                included.append(relative)
            if included:
                archive = _git(repo, "archive", "--format=tar", commit, "--", *included)
                with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
                    bundle.extractall(tree, filter="data")
            cards = _cards(tree, repo, commit, problems)
            waiting = _ok(
                cards=sorted(
                    (
                        card
                        for card in cards.values()
                        if waits_for_owner(card["status"], card["next_owner"])
                    ),
                    key=lambda card: (card["priority"] or "P9", card["id"]),
                )
            )
            engineering = _guard(
                "工程", problems, lambda: _engineering(tree, repo, commit, cards, problems)
            )
            research = _guard("研究", problems, lambda: _research(tree))
            return source, waiting, engineering, research
    except Exception as exc:
        problems.append(f"仓库：读不到 {ref} 的文件（{_reason(exc)}）")
        return source, unavailable.copy(), unavailable.copy(), unavailable.copy()


def _json_object(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("不是 JSON 对象")
    return raw


def _minutes(started: Any, finished: Any) -> float | None:
    if not isinstance(started, str) or not isinstance(finished, str):
        return None
    try:
        duration = datetime.fromisoformat(finished) - datetime.fromisoformat(started)
        return round(duration.total_seconds() / 60, 1)
    except (ValueError, TypeError, OverflowError):
        return None


def _failures(value: Any, key: str) -> list[Any]:
    if not isinstance(value, list):
        return []
    return [
        item.get(key)
        for item in value
        if isinstance(item, Mapping) and item.get("status") == "FAIL"
    ]


def _run(raw: Mapping[str, Any], name: str) -> dict[str, Any]:
    return {
        "name": name,
        **_pick(raw, ("window", "as_of", "started_at", "finished_at", "release_id")),
        "status": raw.get("cli_status"),
        "blocker": raw.get("blocker_category"),
        "exit_code": raw.get("daily_run_exit_code"),
        "retry": _mapping(raw.get("source_failure_retry")).get("outcome"),
        "minutes": _minutes(raw.get("started_at"), raw.get("finished_at")),
        "failed_steps": _failures(raw.get("steps"), "step_id"),
        "failed_sources": _failures(raw.get("capture_components"), "component_id"),
    }


def _calendar(state_root: Path, today: date, problems: list[str]) -> dict[str, Any]:
    calendar = [
        (today - timedelta(days=offset)).isoformat() for offset in reversed(range(CALENDAR_DAYS))
    ]
    runs: dict[str, list[dict[str, Any]]] = {day: [] for day in calendar}
    scheduler = paths.join_root(state_root, _SCHEDULER)
    for path in sorted(scheduler.iterdir(), key=lambda path: path.name):
        match = _SUMMARY_NAME.fullmatch(path.name)
        if match is None or match[1] not in runs:
            continue
        try:
            raw = _json_object(path)
        except Exception as exc:
            problems.append(f"{_SCHEDULER}/{path.name}: 读不到（{_reason(exc)}）")
            continue
        runs[match[1]].append(_run(raw, match[2]))
    days = []
    for day in calendar:
        ordered = sorted(runs[day], key=lambda run: (str(run["started_at"] or ""), run["name"]))
        decided = next((run for run in ordered if run["status"] == "PASS"), None)
        last = decided if decided is not None else (ordered[-1] if ordered else {})
        days.append(
            {
                "date": day,
                "result": day_result(ordered),
                "as_of": last.get("as_of"),
                "release_id": last.get("release_id"),
                "minutes": decided["minutes"] if decided is not None else None,
                "runs": ordered,
            }
        )
    counts = Counter(day["result"] for day in days)
    return _ok(days=days, counts={result: counts[result] for result in DAY_RESULTS})


def _quality_gate(state_root: Path, problems: list[str]) -> dict[str, Any]:
    reports = paths.join_root(state_root, _REPORTS)
    candidates = [
        (match[1], path) for path in reports.iterdir() if (match := _GATE_NAME.fullmatch(path.name))
    ]
    if not candidates:
        raise FileNotFoundError("没有报告质量门禁文件")
    as_of, path = max(candidates, key=lambda item: item[0])
    try:
        raw = _json_object(path)
    except Exception as exc:
        problems.append(f"{_REPORTS}/{path.name}: 读不到（{_reason(exc)}）")
        return _unreadable(f"{_REPORTS}/{path.name}")
    blocking = raw.get("blocking_quality_issues")
    warnings = raw.get("warning_quality_issues")
    return _ok(
        as_of=as_of,
        result=raw.get("status"),
        blocking=len(blocking) if isinstance(blocking, list) else None,
        warnings=len(warnings) if isinstance(warnings, list) else None,
    )


def _release(
    daily_root: Path, repo: Path, commit: str | None, problems: list[str]
) -> dict[str, Any]:
    try:
        raw = _json_object(daily_root / "current.json")
    except Exception as exc:
        problems.append(f"current.json: 读不到（{_reason(exc)}）")
        return _unreadable("current.json")
    behind = None
    source = raw.get("source_commit")
    if commit and isinstance(source, str):
        try:
            behind = int(_git_text(repo, "rev-list", "--count", f"{source}..{commit}"))
        except (OSError, subprocess.CalledProcessError, ValueError):
            problems.append(f"发布包：仓库里找不到提交 {source[:12]}")
    deploys = []
    log = daily_root / "deploy_log.jsonl"
    if log.exists():
        for line in log.read_text(encoding="utf-8").splitlines():
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if isinstance(entry, dict):
                deploys.append(_pick(entry, ("at", "action", "outcome", "release_id")))
    return _ok(
        **_pick(raw, ("release_id", "source_commit", "activated_at", "previous_release_id")),
        commits_behind=behind,
        deploys=deploys[-DEPLOYS:],
    )


def build(
    repo: Path,
    *,
    state_root: Path | None = None,
    daily_root: Path | None = None,
    today: date,
    ref: str = "main",
) -> dict[str, Any]:
    """汇总只读输入；各块的问题留在输出里，不影响其他来源。"""
    problems: list[str] = []
    source, waiting, engineering, research = _repository(repo, ref, problems)
    if state_root is None:
        daily = _unreadable("没有给 --state-root")
        quality = _unreadable("没有给 --state-root")
        problems.extend(
            ("日报: 读不到（没有给 --state-root）", "报告质量门禁: 读不到（没有给 --state-root）")
        )
    else:
        daily = _guard("日报", problems, lambda: _calendar(state_root, today, problems))
        quality = _guard("报告质量门禁", problems, lambda: _quality_gate(state_root, problems))
    if daily_root is None:
        release = _unreadable("没有给 --daily-root")
        problems.append("发布包: 读不到（没有给 --daily-root）")
    else:
        release = _guard(
            "发布包", problems, lambda: _release(daily_root, repo, source["commit"], problems)
        )
    daily.update(release=release, quality_gate=quality)
    return {
        "schema_version": SCHEMA_VERSION,
        "today": today.isoformat(),
        "source": source,
        "waiting": waiting,
        "engineering": engineering,
        "daily": daily,
        "research": research,
        "collaboration": {"status": "later", "error": None, "note": "第 2 版接入（开发日报副本）"},
        "problems": problems,
    }


def dumps(model: Mapping[str, Any]) -> str:
    """稳定序列化为 UTF-8 可表示的 JSON 文本。"""
    return json.dumps(model, ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False) + "\n"


def write(model: Mapping[str, Any], out: Path) -> None:
    """同目录写临时文件后替换，避免读到半份输出。"""
    text = dumps(model)
    out.parent.mkdir(parents=True, exist_ok=True)
    temporary = out.with_name(out.name + ".tmp")
    temporary.write_text(text, encoding="utf-8", newline="\n")
    os.replace(temporary, out)


def main(argv: Sequence[str] | None = None) -> int:
    """生成供私有 App 使用的工作总览 JSON。"""
    parser = argparse.ArgumentParser(description="从已提交的仓库和运行摘要生成只读工作总览。")
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--ref", default="main")
    parser.add_argument("--state-root", type=Path)
    parser.add_argument("--daily-root", type=Path)
    parser.add_argument("--today", type=date.fromisoformat, default=datetime.now(JST).date())
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    out = args.out.resolve()
    if any(
        out.is_relative_to(root.resolve())
        for root in (args.repo, args.state_root, args.daily_root)
        if root is not None
    ):
        print(
            f"work_overview: 输出文件必须在仓库、生产状态根和发布包目录之外：{args.out}",
            file=sys.stderr,
        )
        return 2
    model = build(
        args.repo,
        state_root=args.state_root,
        daily_root=args.daily_root,
        today=args.today,
        ref=args.ref,
    )
    write(model, args.out)
    waiting = len(model["waiting"]["cards"]) if model["waiting"]["status"] == "ok" else "–"
    counts = model["daily"].get("counts", {})
    print(
        f"work_overview: 等 Baker 的 {waiting} 件 · 日报 PASS {counts.get('PASS', '–')} · "
        f"补跑 {counts.get('RESCUED', '–')} · 失败 {counts.get('FAILED', '–')} · "
        f"问题 {len(model['problems'])} · {args.out}"
    )
    return 0
