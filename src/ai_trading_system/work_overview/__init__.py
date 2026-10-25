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
from ai_trading_system.yaml_loader import load_strict_yaml_text, safe_load_yaml_path

SCHEMA_VERSION = "work_overview.v1"
ROADMAP_PATH = "docs/work_overview/roadmap.yaml"
ROADMAP_SCHEMA = "work_roadmap.v1"
# 展示协议：日历覆盖两周，勾掉的里程碑覆盖一周，不作投资判断。
CALENDAR_DAYS = 14
CHECKED_DAYS = 7
STAGE_STATES = ("done", "now", "later", "deferred")
MILESTONE_STATES = ("done", "now", "waiting", "todo", "deferred")
BLOCKER_LABELS = {
    "DATA_SOURCE_UNAVAILABLE": "数据源取不到",
    "DATA_QUALITY_FAILED": "数据质量没过",
    "CODE_DEFECT": "代码问题",
    "CONTROL_PLANE": "运行控制问题",
    "WAITING_FOR_NEW_AS_OF": "等待新交易日",
}
STEP_LABELS = {
    "capture_daily_inputs": "取当天的输入",
    "download_data": "下载行情",
    "market_data_recover_freshness": "恢复行情新鲜度",
    "validate_data": "校验数据",
    "official_policy_sources": "取官方政策源",
    "pit_snapshots_fetch_fmp_forward": "取 FMP 前向快照",
    "pit_snapshots_project_fmp_forward": "整理 FMP 前向快照",
    "pit_snapshots_build_manifest": "建立快照清单",
    "pit_snapshots_validate": "校验快照",
    "sec_companyfacts": "取 SEC 公司财务数据",
    "sec_metrics": "计算 SEC 指标",
    "tsm_ir_sec_metrics_merge": "合并台积电财务指标",
    "sec_metrics_validation": "校验 SEC 指标",
    "valuation_snapshots": "归档估值快照",
    "score_daily": "打分",
    "forward_evidence_dry_run_daily": "核对前向证据",
    "reports_dashboard": "生成报告面板",
    "sec_pit_shadow_observe": "记录 SEC 时点影子观察",
    "sec_pit_shadow_monitor": "核对 SEC 时点影子记录",
    "score_change_attribution": "解释分数变化",
    "market_panel": "生成市场面板",
    "market_data_freshness": "核对行情新鲜度",
    "artifact_lineage": "记录产物血缘",
    "validate_artifact_lineage": "校验产物血缘",
    "report_index": "建立报告索引",
    "documentation_contract": "核对文档约定",
    "reader_brief": "生成读者简报",
    "report_quality_gate": "报告质量门禁",
    "validate_reader_brief": "校验读者简报",
    "pipeline_health": "管线健康检查",
    "secret_hygiene": "密钥扫描",
}
SOURCE_LABELS = {
    "market_macro": "市场宏观数据",
    "fmp_forward_pit": "FMP 前向快照",
    "sec_companyfacts": "SEC 公司财务数据",
    "fmp_valuation": "FMP 估值数据",
    "official_policy_sources": "官方政策源",
}
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


def asks_owner(status: str | None, next_owner: str | None) -> bool:
    """区分 Baker 本人和 operations owner 等角色；括号里的说明不算。"""
    if status == "BLOCKED_OWNER_INPUT":
        return True
    if status not in ACTIVE_STATUSES or not isinstance(next_owner, str):
        return False
    owner = re.split(r"[（(]", next_owner, maxsplit=1)[0].lower()
    return any(name in owner for name in ("baker", "project owner", "项目 owner"))


def run_label(name: str) -> str:
    """调度窗口的展示名称，文件名里的重跑编号保持可见。"""
    window, _, attempt = name.partition("_")
    label = {"PRIMARY": "12:00 主跑", "RESCUE": "17:30 补跑"}.get(window, window)
    return f"{label}（第 {attempt} 次）" if attempt else label


def run_reason(run: Mapping[str, Any]) -> str | None:
    """仅翻译结构化状态和失败 id，不带入摘要里的错误正文或路径。"""
    status = run.get("status")
    if status == "PASS":
        return None
    if status == "RUN_CONTROL_ALREADY_COMPLETE":
        return "这个交易日已经出过报告，没有再跑"
    if status == "RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED":
        return "前一次失败后要先人工恢复，这次没有跑"
    blocker = run.get("blocker")
    prefix = ""
    if isinstance(blocker, str) and blocker and blocker != "NONE":
        prefix = BLOCKER_LABELS.get(blocker, f"其他原因（{blocker}）")
    details = []
    sources = run.get("failed_sources")
    if isinstance(sources, list):
        labels = [SOURCE_LABELS.get(item, item) for item in sources if isinstance(item, str)]
        if labels:
            details.append("、".join(labels) + "取数出错")
    steps = run.get("failed_steps")
    if isinstance(steps, list):
        details.extend(
            STEP_LABELS.get(item, item) + "没过" for item in steps if isinstance(item, str)
        )
    detail = "，".join(details)
    if prefix or detail:
        return f"{prefix}：{detail}" if prefix and detail else prefix or detail
    return f"没过（{status}）" if status and status != "FAIL" else "没过"


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
    if isinstance(exc, subprocess.CalledProcessError):
        return f"CalledProcessError: git 退出码 {exc.returncode}"
    if isinstance(exc, OSError) and exc.strerror is not None:
        return f"{type(exc).__name__}: {exc.strerror}"
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


def _cards(tree: Path, problems: list[str]) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    cards: dict[str, dict[str, Any]] = {}
    card_paths: dict[str, str] = {}
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
        cards[raw["id"]] = card
        card_paths[raw["id"]] = relative
    return cards, card_paths


def _text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("路线图字段应是非空字符串")
    return value


def _day(value: Any) -> str:
    if type(value) is date:
        return value.isoformat()
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is None:
        raise ValueError("路线图日期应是 YYYY-MM-DD")
    return date.fromisoformat(value).isoformat()


def _optional(raw: Mapping[str, Any], key: str, parse: Callable[[Any], str]) -> str | None:
    return parse(raw[key]) if key in raw else None


def _object(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("路线图字段应是对象")
    return value


def _list(value: Any) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError("路线图字段应是列表")
    return value


def _state(value: Any, allowed: Sequence[str]) -> str:
    text = _text(value)
    if text not in allowed:
        raise ValueError("路线图状态不在约定内")
    return text


def _unique(value: Any, seen: set[str]) -> str:
    text = _text(value)
    if text in seen:
        raise ValueError("路线图 id 重复")
    seen.add(text)
    return text


def load_roadmap(root: Path) -> dict[str, Any]:
    """严格核对格式并规范日期；业务规则留给生成时报告，不拒绝整块。"""
    raw = _object(
        load_strict_yaml_text(paths.join_root(root, ROADMAP_PATH).read_text(encoding="utf-8"))
    )
    if raw.get("schema_version") != ROADMAP_SCHEMA:
        raise ValueError("schema_version 不是 work_roadmap.v1")
    updated = _day(raw.get("updated"))
    names = {_text(key): _text(value) for key, value in _object(raw.get("cards")).items()}
    lines = []
    line_ids: set[str] = set()
    for value in _list(raw.get("lines")):
        line = _object(value)
        line_id = _unique(line.get("id"), line_ids)
        if re.fullmatch(r"[a-z0-9_]+", line_id) is None:
            raise ValueError("路线图线的 id 只能用小写字母、数字和下划线")
        layers = []
        for value in _list(line.get("layers", [])):
            layer = _object(value)
            layers.append(
                {
                    "title": _text(layer.get("title")),
                    "state": _state(layer.get("state"), ("done", "now", "later")),
                }
            )
        stages = []
        stage_ids: set[str] = set()
        for value in _list(line.get("stages")):
            stage = _object(value)
            stage_id = _unique(stage.get("id"), stage_ids)
            milestones = []
            for value in _list(stage.get("milestones", [])):
                item = _object(value)
                milestones.append(
                    {
                        "text": _text(item.get("text")),
                        "state": _state(item.get("state"), MILESTONE_STATES),
                        "date": _optional(item, "date", _day),
                        "note": _optional(item, "note", _text),
                        "cards": [_text(card) for card in _list(item.get("cards", []))],
                    }
                )
            stages.append(
                {
                    "id": stage_id,
                    "title": _text(stage.get("title")),
                    "state": _state(stage.get("state"), STAGE_STATES),
                    "done_on": _optional(stage, "done_on", _day),
                    "note": _optional(stage, "note", _text),
                    "milestones": milestones,
                }
            )
        if not stages:
            raise ValueError("路线图每条线至少有一段")
        lines.append(
            {
                "id": line_id,
                "kind": _state(line.get("kind"), ("research", "engineering")),
                "title": _text(line.get("title")),
                "goal": _text(line.get("goal")),
                "layers": layers,
                "stages": stages,
            }
        )
    return {"updated": updated, "names": names, "lines": lines}


def _roadmap(
    raw: dict[str, Any],
    today: date,
    cards: dict[str, dict[str, Any]],
    shown: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    lines = []
    checked = []
    first = (today - timedelta(days=CHECKED_DAYS - 1)).isoformat()
    for line in raw["lines"]:
        stages = []
        for stage in line["stages"]:
            context = {
                "line": line["id"],
                "line_title": line["title"],
                "stage": stage["id"],
                "stage_title": stage["title"],
            }
            done_on = stage["done_on"]
            if stage["state"] == "done" and done_on and first <= done_on <= today.isoformat():
                checked.append(
                    {**context, "date": done_on, "kind": "stage", "text": stage["title"]}
                )
            milestones = []
            for item in stage["milestones"]:
                milestones.append(
                    {
                        **item,
                        "cards": [
                            shown(card_id)
                            for card_id in dict.fromkeys(item["cards"])
                            if card_id in cards
                        ],
                    }
                )
                day = item["date"]
                if item["state"] == "done" and day and first <= day <= today.isoformat():
                    checked.append(
                        {**context, "date": day, "kind": "milestone", "text": item["text"]}
                    )
            stages.append({**stage, "milestones": milestones})
        current = next((stage for stage in stages if stage["state"] == "now"), None)
        counts = Counter(stage["state"] for stage in stages)
        lines.append(
            {
                **line,
                "stages": stages,
                "counts": {state: counts[state] for state in STAGE_STATES},
                "current": {
                    "stage": current["id"],
                    "title": current["title"],
                    "milestone": next(
                        (item["text"] for item in current["milestones"] if item["state"] == "now"),
                        None,
                    ),
                    "next": next(
                        (item["text"] for item in current["milestones"] if item["state"] == "todo"),
                        None,
                    ),
                }
                if current
                else None,
            }
        )
    return _ok(
        updated=raw["updated"],
        lines=lines,
        checked=sorted(checked, key=lambda entry: entry["date"], reverse=True),
    )


def _events(roadmap: dict[str, Any]) -> list[dict[str, Any]] | None:
    if roadmap["status"] != "ok":
        return None
    return [
        {
            "line": line["id"],
            "line_title": line["title"],
            "stage": stage["id"],
            "stage_title": stage["title"],
            **{key: item[key] for key in ("text", "date", "note", "cards")},
        }
        for line in roadmap["lines"]
        for stage in line["stages"]
        for item in stage["milestones"]
        if item["state"] == "waiting"
    ]


def _engineering(
    raw: dict[str, Any],
    roadmap: dict[str, Any],
    repo: Path,
    commit: str,
    cards: dict[str, dict[str, Any]],
    problems: list[str],
    shown: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    listed: list[str] = []
    duplicates: set[str] = set()
    for line in raw["lines"]:
        if sum(stage["state"] == "now" for stage in line["stages"]) > 1:
            problems.append(f"路线图：{line['title']}有不止一段写着现在")
        for stage in line["stages"]:
            for item in stage["milestones"]:
                listed.extend(item["cards"])
                duplicates.update(key for key, count in Counter(item["cards"]).items() if count > 1)
    listed = list(dict.fromkeys(listed))
    missing = [card_id for card_id in listed if card_id not in cards]
    problems.extend(f"路线图：没有卡 {card_id}" for card_id in missing)
    problems.extend(f"路线图：{card_id} 在同一个里程碑里写了两次" for card_id in sorted(duplicates))
    for card_id in listed:
        if card_id in cards and card_id not in raw["names"]:
            problems.append(f"路线图：卡 {card_id} 没有中文名")
        if card_id in cards and cards[card_id]["status"] in ACTIVE_STATUSES:
            updated = shown(card_id)["updated"]
            if updated and updated > raw["updated"]:
                problems.append(
                    f"路线图：{card_id} 在 {updated} 改过，路线图的 updated 还是 {raw['updated']}"
                )
    unassigned = [
        shown(card_id)
        for card_id in sorted(cards)
        if cards[card_id]["status"] in ACTIVE_STATUSES and card_id not in listed
    ]
    problems.extend(f"路线图：在办的卡 {card['id']} 不在任何里程碑里" for card in unassigned)
    lines = []
    research_cards: dict[str, dict[str, Any]] = {}
    for line in roadmap["lines"]:
        members = {
            card["id"]: card
            for stage in line["stages"]
            for item in stage["milestones"]
            for card in item["cards"]
        }
        if line["kind"] == "research":
            research_cards.update(members)
            continue
        current = line["current"]
        milestone = "没有进行中的阶段"
        if current:
            milestone = current["title"]
            if current["milestone"]:
                milestone += f"：{current['milestone']}"
        lines.append(
            {
                **{key: line[key] for key in ("id", "title", "goal")},
                "milestone": milestone,
                "cards": list(members.values()),
                "counts": dict(Counter(card["status"] for card in members.values())),
            }
        )
    recent = []
    for row in _git_text(
        repo, "log", "-n", str(RECENT_COMMITS), "--format=%H%x1f%cI%x1f%s", commit
    ).splitlines():
        sha, timestamp, subject = row.split("\x1f", 2)
        recent.append({"commit": sha[:12], "date": _jst_day(timestamp), "subject": subject})
    return _ok(
        updated=raw["updated"],
        mainlines=lines,
        research_cards=list(research_cards.values()),
        unassigned=unassigned,
        duplicates=sorted(duplicates),
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
    repo: Path, ref: str, today: date, problems: list[str]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    source: dict[str, Any] = {"ref": ref, "commit": None, "commit_time": None}
    unavailable = _unreadable(f"仓库 {ref}")
    try:
        commit = _git_text(repo, "rev-parse", "--verify", f"{ref}^{{commit}}")
        timestamp = _git_text(repo, "log", "-1", "--format=%cI", commit)
    except (OSError, subprocess.CalledProcessError):
        problems.append(f"仓库：读不到 {ref}")
        return (
            source,
            unavailable.copy(),
            unavailable.copy(),
            unavailable.copy(),
            unavailable.copy(),
        )
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
            cards, card_paths = _cards(tree, problems)
            raw = None
            try:
                raw = load_roadmap(tree)
            except Exception as exc:
                problems.append(f"{ROADMAP_PATH}: 读不到（{_reason(exc)}）")
            names = raw["names"] if raw is not None else {}
            shown_cards: dict[str, dict[str, Any]] = {}

            def shown(card_id: str) -> dict[str, Any]:
                if card_id not in shown_cards:
                    updated = None
                    try:
                        timestamp = _git_text(
                            repo, "log", "-1", "--format=%cI", commit, "--", card_paths[card_id]
                        )
                        if timestamp:
                            updated = _jst_day(timestamp)
                    except (OSError, subprocess.CalledProcessError, ValueError):
                        pass
                    shown_cards[card_id] = {
                        **cards[card_id],
                        "name": names.get(card_id),
                        "updated": updated,
                    }
                return shown_cards[card_id]

            roadmap = _unreadable(ROADMAP_PATH)
            engineering = _unreadable(ROADMAP_PATH)
            if raw is not None:
                roadmap = _guard("路线图", problems, lambda: _roadmap(raw, today, cards, shown))
                engineering = _guard(
                    "工程",
                    problems,
                    lambda: _engineering(raw, roadmap, repo, commit, cards, problems, shown),
                )

            def waiting_cards(
                rule: Callable[[str | None, str | None], bool],
            ) -> list[dict[str, Any]]:
                return sorted(
                    (
                        shown(card["id"])
                        for card in cards.values()
                        if rule(card["status"], card["next_owner"])
                    ),
                    key=lambda card: (card["priority"] or "P9", card["id"]),
                )

            waiting = _ok(
                cards=waiting_cards(waits_for_owner),
                asks=waiting_cards(asks_owner),
                events=_events(roadmap),
            )
            research = _guard("研究", problems, lambda: _research(tree))
            return source, waiting, roadmap, engineering, research
    except Exception as exc:
        problems.append(f"仓库：读不到 {ref} 的文件（{_reason(exc)}）")
        return (
            source,
            unavailable.copy(),
            unavailable.copy(),
            unavailable.copy(),
            unavailable.copy(),
        )


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
    run = {
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
    return {**run, "label": run_label(name), "reason": run_reason(run)}


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
                "reason": next((run["reason"] for run in ordered if run["status"] != "PASS"), None)
                if day_result(ordered) in ("RESCUED", "FAILED")
                else None,
                "as_of": last.get("as_of"),
                "release_id": last.get("release_id"),
                "minutes": decided["minutes"] if decided is not None else None,
                "runs": ordered,
            }
        )
    counts = Counter(day["result"] for day in days)
    streak = 0
    rescued = 0
    for entry in reversed(days):
        result = entry["result"]
        if result == "NO_NEW_DAY" or (entry["date"] == today.isoformat() and result == "NO_RUN"):
            continue
        if result not in ("PASS", "RESCUED"):
            break
        streak += 1
        rescued += result == "RESCUED"
    last_failed = next(
        (
            {"date": day["date"], "reason": day["reason"]}
            for day in reversed(days)
            if day["result"] == "FAILED"
        ),
        None,
    )
    return _ok(
        days=days,
        counts={result: counts[result] for result in DAY_RESULTS},
        streak=streak,
        streak_rescued=rescued,
        last_failed=last_failed,
    )


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
    source, waiting, roadmap, engineering, research = _repository(repo, ref, today, problems)
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
        "roadmap": roadmap,
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
    waiting = len(model["waiting"]["asks"]) if model["waiting"]["status"] == "ok" else "–"
    events = model["waiting"].get("events")
    event_count = len(events) if events is not None else "–"
    counts = model["daily"].get("counts", {})
    print(
        f"work_overview: 要 Baker 做的 {waiting} 件 · 在等的事 {event_count} 件 · "
        f"日报 PASS {counts.get('PASS', '–')} · "
        f"补跑 {counts.get('RESCUED', '–')} · 失败 {counts.get('FAILED', '–')} · "
        f"问题 {len(model['problems'])} · {args.out}"
    )
    return 0
