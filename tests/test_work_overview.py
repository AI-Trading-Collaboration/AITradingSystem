"""Acceptance tests for the work overview generator (DEVX-034, runs 02 to 04).

Written before the implementation. The requirement is docs/requirements/DEVX-034_Work_Overview.md,
sections 8 and 9. The fixture is a small git repository (task cards, the roadmap file and two
research records), a production state root (scheduler summaries, report quality gates, and files
the generator must never read) and a release directory (current.json, deploy_log.jsonl).
"""

from __future__ import annotations

import builtins
import importlib.util
import io
import json
import os
import re
import socket
import subprocess
import sys
from datetime import date
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

from ai_trading_system import research_site
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.work_overview import (
    ACTIVE_STATUSES,
    BLOCKER_LABELS,
    CALENDAR_DAYS,
    CHECKED_DAYS,
    MILESTONE_STATES,
    ROADMAP_PATH,
    SCHEMA_VERSION,
    SOURCE_LABELS,
    STAGE_STATES,
    STEP_LABELS,
    TREE_PATHS,
    asks_owner,
    build,
    day_result,
    dumps,
    load_roadmap,
    main,
    run_label,
    run_reason,
    waits_for_owner,
)

TODAY = date(2026, 10, 25)
FIRST = "2026-10-20T10:00:00+09:00"
SECOND = "2026-10-22T23:30:00+00:00"  # 2026-10-23 in JST
SECRETS = (
    "SENTINEL-BRIEF-TEXT",
    "SENTINEL-PRICE-123.45",
    "SENTINEL-LEDGER-ROW",
    "SENTINEL-ERROR-EXCERPT",
    "SENTINEL-FINDING",
    "SENTINEL-STEP-DETAIL",
    "SENTINEL-DEPLOY-DETAIL",
    "SENTINEL-GATE-LIMITATION",
    "SENTINEL-LOG-LINE",
    "SENTINEL-OLD-SUMMARY",
)

CARD_KEYS = {"id", "area", "status", "priority", "next_owner", "next_step", "name", "updated"}
RUN_KEYS = {
    "name",
    "label",
    "reason",
    "window",
    "as_of",
    "status",
    "blocker",
    "started_at",
    "finished_at",
    "minutes",
    "release_id",
    "exit_code",
    "retry",
    "failed_steps",
    "failed_sources",
}


# ---- fixture ------------------------------------------------------------------


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(text.encode("utf-8"))  # LF on every platform


def _git(repo: Path, *args: str, when: str = FIRST) -> str:
    env = {**os.environ, "GIT_AUTHOR_DATE": when, "GIT_COMMITTER_DATE": when}
    done = subprocess.run(
        [
            "git",
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
            *args,
        ],
        cwd=repo,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return done.stdout.strip()


def _card(
    task_id: str,
    status: str,
    next_owner: str,
    *,
    priority: str = "P2",
    next_step: str | None = None,
) -> str:
    return yaml.safe_dump(
        {
            "schema_version": "task.v1",
            "id": task_id,
            "area": f"Fixture / {task_id} 的领域",
            "priority": priority,
            "status": status,
            "next_owner": next_owner,
            "next_step": next_step or f"{task_id} 的下一步",
            "acceptance": "验收",
            "notes": "production_effect=none",
        },
        sort_keys=False,
        allow_unicode=True,
    )


CARDS = {
    "OPS-1": ("IN_PROGRESS", "Claude（写需求和验收测试）", "P1"),
    "OPS-2": ("VALIDATING", "operations owner（读 17:30 摘要）", "P2"),
    "NOTE-1": ("READY", "Claude（等 owner 决定以后再做）", "P2"),
    "WAIT-1": ("PROPOSED", "Baker（定方向）", "P0"),
    "ORPHAN-1": ("BLOCKED_OWNER_INPUT", "Claude", "P3"),
    "DONE-1": ("DONE", "无", "P2"),
    "OLD-1": ("BASELINE_DONE", "Project Owner", "P2"),
    "TRADING-1": ("IN_PROGRESS", "Claude（研究自动循环）", "P1"),
}


NAMES = {
    "OPS-1": "日报提速",
    "OPS-2": "补跑时自动重试",
    "NOTE-1": "备忘",
    "WAIT-1": "定方向",
    "DONE-1": "做完的卡",
    "TRADING-1": "研究计划",
}


def _lines() -> list[dict[str, Any]]:
    return [
        {
            "id": "research",
            "kind": "research",
            "title": "研究计划",
            "goal": "找到方向信号",
            "layers": [
                {"title": "方向信号", "state": "now"},
                {"title": "期权实现", "state": "later"},
            ],
            "stages": [
                {
                    "id": "S0",
                    "title": "定方向",
                    "state": "done",
                    "done_on": "2026-10-12",
                    "milestones": [
                        {
                            "text": "方向定了",
                            "state": "done",
                            "date": "2026-10-12",
                            "cards": ["TRADING-1"],
                        }
                    ],
                },
                {
                    "id": "S1",
                    "title": "找方向信号",
                    "state": "now",
                    "milestones": [
                        {
                            "text": "第 6 期只记账",
                            "state": "now",
                            "date": "2026-11-20",
                            "note": "每周五追加",
                            "cards": ["TRADING-1"],
                        },
                        {"text": "续期卡", "state": "todo", "date": "2026-11-20"},
                    ],
                },
                {"id": "S2", "title": "期权实现", "state": "later", "note": "等 S1"},
            ],
        },
        {
            "id": "daily_run",
            "kind": "engineering",
            "title": "日报运行与发布",
            "goal": "日报每天跑通",
            "stages": [
                {
                    "id": "1",
                    "title": "从发布包运行",
                    "state": "done",
                    "done_on": "2026-10-19",
                    "milestones": [
                        {
                            "text": "发布包上线",
                            "state": "done",
                            "date": "2026-10-18",
                            "cards": ["OPS-1"],
                        }
                    ],
                },
                {
                    "id": "2",
                    "title": "提速",
                    "state": "now",
                    "note": "中位数明显下降",
                    "milestones": [
                        {
                            "text": "扫描改增量",
                            "state": "done",
                            "date": "2026-10-24",
                            "cards": ["OPS-1"],
                        },
                        {
                            "text": "补跑时自动重试",
                            "state": "waiting",
                            "date": "2026-10-13",
                            "note": "等第一次真实故障",
                            "cards": ["OPS-2"],
                        },
                        {
                            "text": "换包以后仍按增量扫",
                            "state": "now",
                            "cards": ["OPS-1", "NOTE-1"],
                        },
                        {"text": "命令行启动耗时", "state": "todo", "cards": []},
                    ],
                },
            ],
        },
        {
            "id": "display",
            "kind": "engineering",
            "title": "展示与知识",
            "goal": "一个地方看清全部工作",
            "stages": [
                {
                    "id": "1",
                    "title": "第 1 版",
                    "state": "done",
                    "done_on": "2026-10-19",
                    "milestones": [
                        {
                            "text": "生成器",
                            "state": "done",
                            "date": "2026-10-19",
                            "cards": ["DONE-1"],
                        }
                    ],
                },
                {
                    "id": "2",
                    "title": "第 2 版",
                    "state": "later",
                    "milestones": [{"text": "协作块", "state": "todo", "cards": ["WAIT-1"]}],
                },
                {
                    "id": "3",
                    "title": "知识核心",
                    "state": "deferred",
                    "note": "要做时重开",
                    "milestones": [{"text": "知识核心", "state": "deferred"}],
                },
            ],
        },
    ]


def _roadmap(**changes: Any) -> str:
    raw: dict[str, Any] = {
        "schema_version": "work_roadmap.v1",
        "updated": "2026-10-25",
        "cards": dict(NAMES),
        "lines": _lines(),
    }
    raw.update(changes)
    return yaml.safe_dump(raw, sort_keys=False, allow_unicode=True)


CHARTER = (
    "# 研究章程 第 3 期\n\n## 第 3 期的方向\n\n只记账。\n\n## 1. 期限\n\n四周，到 2026-11-20。\n"
)
WEEKLY = "# 前瞻账本周汇总\n\n账本 7 行，2026-10-12..2026-10-20。\n\n## 三条线\n\n略\n"


def _repo(tmp_path: Path) -> tuple[Path, str, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    for task_id, (status, owner, priority) in CARDS.items():
        _write(repo / "tasks" / f"{task_id}.yaml", _card(task_id, status, owner, priority=priority))
    _write(repo / ROADMAP_PATH, _roadmap())
    _write(repo / "research" / "_loop" / "charter_p3.md", CHARTER)
    _write(repo / "research" / "_forward" / "weekly.md", WEEKLY)
    _write(repo / "research" / "_forward" / "ledger.jsonl", '{"row": "SENTINEL-LEDGER-ROW"}\n')
    _write(repo / "README.md", "fixture\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "OPS-1: first records")
    first = _git(repo, "rev-parse", "HEAD")
    status, owner, priority = CARDS["OPS-2"]
    _write(
        repo / "tasks" / "OPS-2.yaml",
        _card("OPS-2", status, owner, priority=priority, next_step="读 17:30 摘要"),
    )
    _git(repo, "add", "-A", when=SECOND)
    _git(repo, "commit", "-q", "-m", "OPS-2: next step", when=SECOND)
    return repo, first, _git(repo, "rev-parse", "HEAD")


def _summary(
    window: str,
    as_of: str,
    status: str | None,
    started: str,
    finished: str,
    *,
    release: str | None = None,
    failed_step: str | None = None,
    failed_source: str | None = None,
    retry: str | None = None,
) -> str:
    steps = [
        {"step_id": "capture_daily_inputs", "status": "PASS", "detail": "SENTINEL-STEP-DETAIL"}
    ]
    if failed_step:
        steps.append({"step_id": failed_step, "status": "FAIL", "detail": "SENTINEL-STEP-DETAIL"})
        steps.append({"step_id": "reader_brief", "status": "BLOCKED", "detail": ""})
    sources = [{"component_id": "fmp_valuation", "status": "PASS", "error_excerpt": ""}]
    if failed_source:
        sources.append(
            {
                "component_id": failed_source,
                "status": "FAIL",
                "error_excerpt": "SENTINEL-ERROR-EXCERPT https://example.invalid/?apikey=SENTINEL",
            }
        )
    raw: dict[str, Any] = {
        "schema_version": "ops_daily_scheduler_summary.v1",
        "window": window,
        "as_of": as_of,
        "cli_status": status,
        "blocker_category": "NONE" if status == "PASS" else "DATA_SOURCE_UNAVAILABLE",
        "blocker_category_label": "无阻断",
        "started_at": started,
        "finished_at": finished,
        "daily_run_exit_code": 0 if status in ("PASS", "RUN_CONTROL_ALREADY_COMPLETE") else 1,
        "findings": [{"category": "NONE", "reason": "SENTINEL-FINDING"}],
        "steps": steps,
        "capture_components": sources,
        "log_path": "D:\\SENTINEL-LOG-LINE\\x.log",
        "source_failure_retry": {"label": None, "outcome": retry},
    }
    if release:
        raw["release_id"] = release
    return json.dumps(raw, ensure_ascii=False, indent=1)


DONE = "RUN_CONTROL_ALREADY_COMPLETE"


def _state(tmp_path: Path) -> Path:
    state = tmp_path / "state"
    scheduler = state / "outputs" / "run_control" / "scheduler"

    def put(name: str, text: str) -> None:
        _write(scheduler / f"{name}.summary.json", text)

    # Outside the 14 days: never opened.
    put("2026-10-11_PRIMARY", "SENTINEL-OLD-SUMMARY {not json")
    put(
        "2026-10-13_PRIMARY",
        _summary(
            "PRIMARY",
            "2026-10-11",
            "FAIL",
            "2026-10-13T09:30:00+09:00",
            "2026-10-13T09:33:00+09:00",
            failed_step="score_daily",
        ),
    )
    put(
        "2026-10-13_RESCUE",
        _summary(
            "RESCUE",
            "2026-10-12",
            "RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED",
            "2026-10-13T17:30:00+09:00",
            "2026-10-13T17:30:05+09:00",
        ),
    )
    put(
        "2026-10-14_PRIMARY",
        _summary(
            "PRIMARY",
            "2026-10-13",
            "PASS",
            "2026-10-14T12:00:00+09:00",
            "2026-10-14T12:09:00+09:00",
        ),
    )
    put("2026-10-14_PRIMARY_2", "{broken")
    put(
        "2026-10-17_PRIMARY",
        _summary(
            "PRIMARY",
            "2026-10-16",
            "FAIL",
            "2026-10-17T12:00:00+09:00",
            "2026-10-17T12:05:00+09:00",
            failed_source="market_macro",
        ),
    )
    put(
        "2026-10-17_PRIMARY_2",
        _summary(
            "PRIMARY",
            "2026-10-16",
            "PASS",
            "2026-10-17T18:05:00+09:00",
            "2026-10-17T18:20:00+09:00",
        ),
    )
    put(
        "2026-10-17_RESCUE",
        _summary(
            "RESCUE",
            "2026-10-16",
            "PASS",
            "2026-10-17T17:30:00+09:00",
            "2026-10-17T17:36:00+09:00",
            retry="RETRIED",
        ),
    )
    for day in ("2026-10-18", "2026-10-19"):
        put(
            f"{day}_PRIMARY",
            _summary(
                "PRIMARY", "2026-10-16", DONE, f"{day}T12:00:00+09:00", f"{day}T12:00:02+09:00"
            ),
        )
        put(
            f"{day}_RESCUE",
            _summary(
                "RESCUE", "2026-10-16", DONE, f"{day}T17:30:00+09:00", f"{day}T17:30:02+09:00"
            ),
        )
    put(
        "2026-10-20_PRIMARY",
        _summary(
            "PRIMARY",
            "2026-10-19",
            "FAIL",
            "2026-10-20T12:00:00+09:00",
            "2026-10-20T12:16:00+09:00",
            release="R1",
            failed_step="pipeline_health",
            failed_source="market_macro",
        ),
    )
    put(
        "2026-10-20_RESCUE",
        _summary(
            "RESCUE",
            "2026-10-19",
            "FAIL",
            "2026-10-20T17:30:00+09:00",
            "2026-10-20T17:34:00+09:00",
            release="R1",
        ),
    )
    put(
        "2026-10-20_RESCUE_2",
        _summary(
            "RESCUE",
            "2026-10-19",
            "PASS",
            "2026-10-20T18:12:03.963+09:00",
            "2026-10-20T18:16:13.000+09:00",
            release="R1",
            retry="RETRIED",
        ),
    )
    _write(scheduler / "2026-10-21_PRIMARY.summary.md", "SENTINEL-LOG-LINE\n")
    _write(scheduler / "2026-10-21_PRIMARY.log", "SENTINEL-LOG-LINE\n")
    put(
        "2026-10-22_PRIMARY",
        _summary(
            "PRIMARY",
            "2026-10-21",
            "FAIL",
            "2026-10-22T12:48:00+09:00",
            "2026-10-22T12:51:00+09:00",
            release="R0",
        ),
    )
    put(
        "2026-10-22_RESCUE",
        _summary(
            "RESCUE",
            "2026-10-21",
            "PASS",
            "2026-10-22T17:30:00.951+09:00",
            "2026-10-22T17:34:27.616+09:00",
            release="R1",
            retry="RETRIED",
        ),
    )
    put(
        "2026-10-23_PRIMARY",
        _summary(
            "PRIMARY",
            "2026-10-22",
            "PASS",
            "2026-10-23T12:00:01.845+09:00",
            "2026-10-23T12:17:33.000+09:00",
            release="R2",
        ),
    )
    put(
        "2026-10-23_RESCUE",
        _summary(
            "RESCUE",
            "2026-10-22",
            DONE,
            "2026-10-23T17:30:01+09:00",
            "2026-10-23T17:30:05+09:00",
            release="R2",
        ),
    )
    put(
        "2026-10-24_PRIMARY",
        _summary(
            "PRIMARY",
            "2026-10-23",
            "PASS",
            "2026-10-24T03:00:06+00:00",
            "2026-10-24T03:14:04+00:00",
            release="R2",
        ),
    )
    _write(scheduler / "notes.json", '"SENTINEL-LOG-LINE"\n')
    reports = state / "outputs" / "reports"
    gate = {
        "status": "PASS_WITH_WARNINGS",
        "blocking_quality_issues": [],
        "warning_quality_issues": [{"id": 1}, {"id": 2}, {"id": 3}],
        "limitations": ["SENTINEL-GATE-LIMITATION"],
    }
    _write(reports / "report_quality_gate_2026-10-23.json", json.dumps(gate))
    old = dict(gate, status="FAIL", blocking_quality_issues=[{"id": 9}])
    _write(reports / "report_quality_gate_2026-10-22.json", json.dumps(old))
    _write(reports / "report_quality_gate_2026-10-23.md", "SENTINEL-GATE-LIMITATION\n")
    _write(reports / "owner_daily_brief_2026-10-23.md", "SENTINEL-BRIEF-TEXT\n")
    _write(reports / "reader_brief_2026-10-23.md", "SENTINEL-BRIEF-TEXT\n")
    _write(
        state / "data" / "raw" / "prices_daily.csv",
        "date,close\n2026-10-23,SENTINEL-PRICE-123.45\n",
    )
    _write(state / "outputs" / "run_control" / "daily" / "states" / "x.json", '"SENTINEL-LOG-LINE"')
    return state


def _daily(tmp_path: Path, source_commit: str) -> Path:
    daily = tmp_path / "daily"
    _write(
        daily / "current.json",
        json.dumps(
            {
                "schema_version": "aits_daily_current.v1",
                "release_id": "R2",
                "source_commit": source_commit,
                "activated_at": "2026-10-22T08:59:06Z",
                "previous_release_id": "R1",
                "state_root": "D:\\SENTINEL-LOG-LINE",
            }
        ),
    )
    lines = [
        {
            "action": "deploy",
            "at": f"2026-10-1{n}T08:00:00Z",
            "outcome": "ACTIVATED",
            "release_id": f"R0{n}",
            "code": None,
            "detail": "SENTINEL-DEPLOY-DETAIL",
        }
        for n in range(5)
    ]
    text = "\n".join(json.dumps(line) for line in lines[:3]) + "\nnot json\n"
    text += "\n".join(json.dumps(line) for line in lines[3:]) + "\n"
    text += (
        json.dumps(
            {
                "action": "rollback",
                "at": "2026-10-21T08:00:00Z",
                "outcome": "REJECTED",
                "release_id": "R1",
                "detail": "SENTINEL-DEPLOY-DETAIL",
            }
        )
        + "\n"
    )
    _write(daily / "deploy_log.jsonl", text)
    _write(daily / "releases" / "R2" / "notes.txt", "SENTINEL-LOG-LINE\n")
    return daily


@pytest.fixture
def world(tmp_path: Path) -> dict[str, Any]:
    repo, first, second = _repo(tmp_path)
    return {
        "repo": repo,
        "first": first,
        "second": second,
        "state": _state(tmp_path),
        "daily": _daily(tmp_path, first),
    }


def _build(world: dict[str, Any], **changes: Any) -> dict[str, Any]:
    options: dict[str, Any] = {
        "state_root": world["state"],
        "daily_root": world["daily"],
        "today": TODAY,
    }
    options.update(changes)
    return build(world["repo"], **options)


# ---- constants and small rules ---------------------------------------------------


def test_constants() -> None:
    assert SCHEMA_VERSION == "work_overview.v1"
    assert ROADMAP_PATH == "docs/work_overview/roadmap.yaml"
    assert CALENDAR_DAYS == 14
    assert CHECKED_DAYS == 7
    assert STAGE_STATES == ("done", "now", "later", "deferred")
    assert MILESTONE_STATES == ("done", "now", "waiting", "todo", "deferred")
    assert set(ACTIVE_STATUSES) == {
        "IN_PROGRESS",
        "VALIDATING",
        "PROPOSED",
        "READY",
        "BLOCKED_OWNER_INPUT",
        "BLOCKED_EXTERNAL",
    }
    assert TREE_PATHS == (
        "tasks",
        "docs/work_overview",
        "docs/requirements",
        "research",
        "config/research",
        "src/ai_trading_system/research_screens/signals",
    )


@pytest.mark.parametrize(
    ("status", "owner", "expected"),
    [
        ("IN_PROGRESS", "Baker（定方向）", True),
        ("VALIDATING", "operations owner（读 17:30 摘要）", True),
        ("READY", "Project Owner", True),
        ("PROPOSED", "系统实现 + 项目 owner 后续复核", True),
        ("BLOCKED_EXTERNAL", "外部条件 (owner: none)", False),
        ("IN_PROGRESS", "Claude（等 owner 决定）", False),
        ("IN_PROGRESS", "Claude (owner later)", False),
        ("IN_PROGRESS", "BAKER", True),
        ("BLOCKED_OWNER_INPUT", "Claude", True),
        ("BLOCKED_OWNER_INPUT", None, True),
        ("DONE", "Baker", False),
        ("BASELINE_DONE", "Project Owner", False),
        ("DEFERRED", "Baker", False),
        ("IN_PROGRESS", None, False),
        ("IN_PROGRESS", "", False),
    ],
)
def test_waits_for_owner(status: str, owner: str | None, expected: bool) -> None:
    assert waits_for_owner(status, owner) is expected


@pytest.mark.parametrize(
    ("runs", "expected"),
    [
        ([], "NO_RUN"),
        ([("PRIMARY", "PASS")], "PASS"),
        ([("PRIMARY", "PASS"), ("RESCUE", DONE)], "PASS"),
        ([("PRIMARY", "FAIL"), ("RESCUE", "PASS")], "RESCUED"),
        ([("PRIMARY", "FAIL"), ("RESCUE", "FAIL"), ("RESCUE", "PASS")], "RESCUED"),
        ([("PRIMARY", "FAIL"), ("RESCUE", "PASS"), ("PRIMARY", "PASS")], "RESCUED"),
        ([("PRIMARY", "FAIL"), ("PRIMARY", "PASS"), ("RESCUE", DONE)], "PASS"),
        ([("PRIMARY", DONE), ("RESCUE", DONE)], "NO_NEW_DAY"),
        ([("PRIMARY", DONE)], "NO_NEW_DAY"),
        ([("PRIMARY", DONE), ("RESCUE", None)], "FAILED"),
        (
            [("PRIMARY", "FAIL"), ("RESCUE", "RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED")],
            "FAILED",
        ),
        ([("PRIMARY", "FAIL")], "FAILED"),
        ([("RESCUE", DONE), ("RESCUE", "FAIL")], "FAILED"),
    ],
)
def test_day_result(runs: list[tuple[str, str | None]], expected: str) -> None:
    assert day_result([{"window": window, "status": status} for window, status in runs]) == expected


# ---- the whole model ---------------------------------------------------------------


def test_top_level_and_source(world: dict[str, Any]) -> None:
    model = _build(world)
    assert set(model) == {
        "schema_version",
        "today",
        "source",
        "waiting",
        "roadmap",
        "engineering",
        "daily",
        "research",
        "collaboration",
        "problems",
    }
    assert model["schema_version"] == SCHEMA_VERSION
    assert model["today"] == "2026-10-25"
    assert model["source"] == {
        "ref": "main",
        "commit": world["second"],
        "commit_time": "2026-10-22T23:30:00+00:00",
    }
    assert model["collaboration"]["status"] == "later"
    problems = model["problems"]
    assert len(problems) == 2
    assert "路线图：在办的卡 ORPHAN-1 不在任何里程碑里" in problems
    assert any(
        problem.startswith(
            "outputs/run_control/scheduler/2026-10-14_PRIMARY_2.summary.json: 读不到"
        )
        for problem in problems
    )


def test_waiting_cards(world: dict[str, Any]) -> None:
    waiting = _build(world)["waiting"]
    assert waiting["status"] == "ok" and waiting["error"] is None
    cards = waiting["cards"]
    # P0 first, then by priority and id; owner only inside the brackets does not count.
    assert [card["id"] for card in cards] == ["WAIT-1", "OPS-2", "ORPHAN-1"]
    assert all(set(card) == CARD_KEYS for card in cards)
    assert cards[0] == {
        "id": "WAIT-1",
        "area": "Fixture / WAIT-1 的领域",
        "status": "PROPOSED",
        "priority": "P0",
        "next_owner": "Baker（定方向）",
        "next_step": "WAIT-1 的下一步",
        "name": "定方向",
        "updated": "2026-10-20",
    }
    assert cards[1]["next_step"] == "读 17:30 摘要"
    assert cards[1]["updated"] == "2026-10-23"  # the second commit, 23:30Z, is the 23rd in JST


def test_engineering_mainlines(world: dict[str, Any]) -> None:
    engineering = _build(world)["engineering"]
    assert set(engineering) == {
        "status",
        "error",
        "updated",
        "mainlines",
        "research_cards",
        "unassigned",
        "duplicates",
        "missing",
        "totals",
        "recent",
    }
    assert engineering["status"] == "ok"
    assert engineering["updated"] == "2026-10-25"
    lines = engineering["mainlines"]
    assert [line["id"] for line in lines] == ["daily_run", "display"]
    assert set(lines[0]) == {"id", "title", "goal", "milestone", "cards", "counts"}
    assert lines[0]["title"] == "日报运行与发布"
    assert lines[0]["goal"] == "日报每天跑通"
    assert lines[0]["milestone"] == "提速：换包以后仍按增量扫"
    assert lines[1]["milestone"] == "没有进行中的阶段"
    assert [card["id"] for card in lines[0]["cards"]] == ["OPS-1", "OPS-2", "NOTE-1"]
    assert [card["id"] for card in lines[1]["cards"]] == ["DONE-1", "WAIT-1"]
    assert all(set(card) == CARD_KEYS for line in lines for card in line["cards"])
    assert lines[0]["counts"] == {"IN_PROGRESS": 1, "VALIDATING": 1, "READY": 1}
    assert lines[1]["counts"] == {"PROPOSED": 1, "DONE": 1}
    assert [card["id"] for card in engineering["research_cards"]] == ["TRADING-1"]
    assert [card["id"] for card in engineering["unassigned"]] == ["ORPHAN-1"]
    assert engineering["duplicates"] == []
    assert engineering["missing"] == []
    assert engineering["totals"] == {
        "IN_PROGRESS": 2,
        "VALIDATING": 1,
        "READY": 1,
        "PROPOSED": 1,
        "BLOCKED_OWNER_INPUT": 1,
        "DONE": 1,
        "BASELINE_DONE": 1,
    }
    assert engineering["recent"] == [
        {"commit": world["second"][:12], "date": "2026-10-23", "subject": "OPS-2: next step"},
        {"commit": world["first"][:12], "date": "2026-10-20", "subject": "OPS-1: first records"},
    ]


def test_roadmap_rule_violations(world: dict[str, Any]) -> None:
    repo = world["repo"]
    lines = _lines()
    lines[1]["stages"][1]["milestones"][2]["cards"] = ["OPS-1", "NOTE-1", "OPS-1", "GONE-1"]
    lines[2]["stages"][1]["state"] = "now"
    lines[2]["stages"][2]["state"] = "now"
    names = {key: value for key, value in NAMES.items() if key != "NOTE-1"}
    _write(repo / ROADMAP_PATH, _roadmap(updated="2026-10-20", cards=names, lines=lines))
    _git(repo, "commit", "-q", "-am", "DEVX-1: break the roadmap rules")
    model = _build(world)
    engineering = model["engineering"]
    assert engineering["status"] == "ok" and model["roadmap"]["status"] == "ok"
    assert engineering["duplicates"] == ["OPS-1"]
    assert engineering["missing"] == ["GONE-1"]
    assert [card["id"] for card in engineering["unassigned"]] == ["ORPHAN-1"]
    milestone = model["roadmap"]["lines"][1]["stages"][1]["milestones"][2]
    assert [card["id"] for card in milestone["cards"]] == ["OPS-1", "NOTE-1"]
    assert milestone["cards"][1]["name"] is None
    for problem in (
        "路线图：没有卡 GONE-1",
        "路线图：OPS-1 在同一个里程碑里写了两次",
        "路线图：卡 NOTE-1 没有中文名",
        "路线图：展示与知识有不止一段写着现在",
        # OPS-2 changed on 10-23, after the roadmap's updated; the cards changed on 10-20 did not.
        "路线图：OPS-2 在 2026-10-23 改过，路线图的 updated 还是 2026-10-20",
        "路线图：在办的卡 ORPHAN-1 不在任何里程碑里",
    ):
        assert problem in model["problems"]
    assert sum(problem.startswith("路线图：") for problem in model["problems"]) == 6


def test_unassigned_lists_every_active_card(world: dict[str, Any]) -> None:
    repo = world["repo"]
    _write(repo / ROADMAP_PATH, _roadmap(lines=[]))
    _git(repo, "commit", "-q", "-am", "DEVX-1: empty roadmap")
    engineering = _build(world)["engineering"]
    assert [card["id"] for card in engineering["unassigned"]] == [
        "NOTE-1",
        "OPS-1",
        "OPS-2",
        "ORPHAN-1",
        "TRADING-1",
        "WAIT-1",
    ]
    assert engineering["mainlines"] == [] and engineering["research_cards"] == []


def test_daily_calendar(world: dict[str, Any]) -> None:
    daily = _build(world)["daily"]
    assert set(daily) == {
        "status",
        "error",
        "days",
        "counts",
        "streak",
        "streak_rescued",
        "last_failed",
        "release",
        "quality_gate",
    }
    assert daily["status"] == "ok" and daily["error"] is None
    days = {day["date"]: day for day in daily["days"]}
    assert [day["date"] for day in daily["days"]] == [f"2026-10-{n}" for n in range(12, 26)]
    assert all(
        set(day) == {"date", "result", "reason", "as_of", "minutes", "release_id", "runs"}
        for day in daily["days"]
    )
    results = {date_: day["result"] for date_, day in days.items()}
    assert results == {
        "2026-10-12": "NO_RUN",
        "2026-10-13": "FAILED",
        "2026-10-14": "PASS",
        "2026-10-15": "NO_RUN",
        "2026-10-16": "NO_RUN",
        "2026-10-17": "RESCUED",
        "2026-10-18": "NO_NEW_DAY",
        "2026-10-19": "NO_NEW_DAY",
        "2026-10-20": "RESCUED",
        "2026-10-21": "NO_RUN",
        "2026-10-22": "RESCUED",
        "2026-10-23": "PASS",
        "2026-10-24": "PASS",
        "2026-10-25": "NO_RUN",
    }
    assert daily["counts"] == {"PASS": 3, "RESCUED": 3, "FAILED": 1, "NO_NEW_DAY": 2, "NO_RUN": 5}
    # Ordered by start time: the 17:30 rescue passed before the 18:05 rerun.
    assert [run["name"] for run in days["2026-10-17"]["runs"]] == ["PRIMARY", "RESCUE", "PRIMARY_2"]
    assert days["2026-10-17"]["minutes"] == 6.0
    assert days["2026-10-20"]["minutes"] == 4.2
    assert days["2026-10-22"]["minutes"] == 4.4
    assert days["2026-10-23"]["minutes"] == 17.5
    assert days["2026-10-24"]["minutes"] == 14.0
    assert days["2026-10-13"]["minutes"] is None
    assert days["2026-10-12"] == {
        "date": "2026-10-12",
        "result": "NO_RUN",
        "reason": None,
        "as_of": None,
        "minutes": None,
        "release_id": None,
        "runs": [],
    }
    assert days["2026-10-13"]["as_of"] == "2026-10-12"  # the last run when none passed
    assert days["2026-10-23"]["release_id"] == "R2"
    assert days["2026-10-22"]["release_id"] == "R1"  # the run that passed, not the first
    assert days["2026-10-24"]["as_of"] == "2026-10-23"
    assert [run["name"] for run in days["2026-10-14"]["runs"]] == ["PRIMARY"]
    assert days["2026-10-20"]["runs"][0] == {
        "name": "PRIMARY",
        "label": "12:00 主跑",
        "reason": "数据源取不到：市场宏观数据取数出错，管线健康检查没过",
        "window": "PRIMARY",
        "as_of": "2026-10-19",
        "status": "FAIL",
        "blocker": "DATA_SOURCE_UNAVAILABLE",
        "started_at": "2026-10-20T12:00:00+09:00",
        "finished_at": "2026-10-20T12:16:00+09:00",
        "minutes": 16.0,
        "release_id": "R1",
        "exit_code": 1,
        "retry": None,
        "failed_steps": ["pipeline_health"],
        "failed_sources": ["market_macro"],
    }
    rescue = days["2026-10-20"]["runs"][2]
    assert set(rescue) == RUN_KEYS
    assert (rescue["name"], rescue["window"], rescue["status"], rescue["retry"]) == (
        "RESCUE_2",
        "RESCUE",
        "PASS",
        "RETRIED",
    )
    assert rescue["failed_steps"] == [] and rescue["failed_sources"] == []


def test_release_and_deploys(world: dict[str, Any]) -> None:
    release = _build(world)["daily"]["release"]
    assert release == {
        "status": "ok",
        "error": None,
        "release_id": "R2",
        "source_commit": world["first"],
        "activated_at": "2026-10-22T08:59:06Z",
        "previous_release_id": "R1",
        "commits_behind": 1,
        "deploys": [
            {
                "at": "2026-10-11T08:00:00Z",
                "action": "deploy",
                "outcome": "ACTIVATED",
                "release_id": "R01",
            },
            {
                "at": "2026-10-12T08:00:00Z",
                "action": "deploy",
                "outcome": "ACTIVATED",
                "release_id": "R02",
            },
            {
                "at": "2026-10-13T08:00:00Z",
                "action": "deploy",
                "outcome": "ACTIVATED",
                "release_id": "R03",
            },
            {
                "at": "2026-10-14T08:00:00Z",
                "action": "deploy",
                "outcome": "ACTIVATED",
                "release_id": "R04",
            },
            {
                "at": "2026-10-21T08:00:00Z",
                "action": "rollback",
                "outcome": "REJECTED",
                "release_id": "R1",
            },
        ],
    }


def test_release_built_from_the_ref_is_zero_behind(world: dict[str, Any]) -> None:
    daily = _daily(world["daily"].parent / "level", world["second"])
    model = _build(world, daily_root=daily)
    assert model["daily"]["release"]["commits_behind"] == 0
    assert not any(problem.startswith("发布包") for problem in model["problems"])


def test_release_commit_not_in_the_repository(world: dict[str, Any]) -> None:
    daily = _daily(world["daily"].parent / "other", "f" * 40)
    model = _build(world, daily_root=daily)
    release = model["daily"]["release"]
    assert release["status"] == "ok" and release["release_id"] == "R2"
    assert release["commits_behind"] is None
    assert any(problem.startswith("发布包") for problem in model["problems"])


def test_quality_gate_reads_the_newest(world: dict[str, Any]) -> None:
    assert _build(world)["daily"]["quality_gate"] == {
        "status": "ok",
        "error": None,
        "as_of": "2026-10-23",
        "result": "PASS_WITH_WARNINGS",
        "blocking": 0,
        "warnings": 3,
    }


def test_research_from_research_site(world: dict[str, Any]) -> None:
    research = _build(world)["research"]
    assert set(research) == {
        "status",
        "error",
        "summary",
        "charter",
        "forward_ledger",
        "forward_records",
        "families",
    }
    assert research["status"] == "ok"
    assert research["charter"] == {
        "period": 3,
        "path": "research/_loop/charter_p3.md",
        "term": "四周，到 2026-11-20。",
        "direction": "只记账。",
    }
    assert research["forward_ledger"] == {"days": 7, "start": "2026-10-12", "end": "2026-10-20"}
    assert research["forward_records"] == {"count": 0, "pending": 0}
    assert research["families"] == {"开放": 0, "停车": 0, "关闭": 0}
    assert research["summary"]["active"] == 0 and research["summary"]["global_trials"] == 0


def test_research_collects_an_extracted_copy_of_main(
    world: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[Path] = []
    real = research_site.collect

    def fake(tree: Path, **_: Any) -> dict[str, Any]:
        seen.append(tree)
        assert tree.resolve() != world["repo"].resolve()
        assert not tree.resolve().is_relative_to(world["repo"].resolve())
        assert sorted(path.name for path in (tree / "tasks").glob("*.yaml")) == sorted(
            f"{task_id}.yaml" for task_id in CARDS
        )
        assert (tree / "tasks" / "OPS-2.yaml").read_text(encoding="utf-8").count("读 17:30 摘要")
        assert not (tree / "README.md").exists()  # only TREE_PATHS are extracted
        assert not (tree / ".git").exists()
        return real(tree)

    monkeypatch.setattr(research_site, "collect", fake)
    model = _build(world)
    assert len(seen) == 1
    assert not seen[0].exists()  # the temporary copy is removed afterwards
    assert model["research"]["charter"]["period"] == 3


def test_reads_main_not_the_working_tree(world: dict[str, Any]) -> None:
    repo = world["repo"]
    _git(repo, "switch", "-q", "-c", "pi/draft")
    _write(repo / "tasks" / "NEW-1.yaml", _card("NEW-1", "IN_PROGRESS", "Baker"))
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "NEW-1: on a branch")
    _write(repo / "tasks" / "OPS-1.yaml", _card("OPS-1", "IN_PROGRESS", "Baker（草稿）"))
    _write(repo / "research" / "_loop" / "charter_p9.md", CHARTER.replace("3", "9"))
    model = _build(world)
    assert model["source"]["commit"] == world["second"]
    assert [card["id"] for card in model["waiting"]["cards"]] == ["WAIT-1", "OPS-2", "ORPHAN-1"]
    assert "NEW-1" not in json.dumps(model)
    assert model["research"]["charter"]["period"] == 3
    branch = _build(world, ref="pi/draft")
    assert branch["source"]["ref"] == "pi/draft"
    assert "NEW-1" in [card["id"] for card in branch["waiting"]["cards"]]
    assert "Baker（草稿）" not in json.dumps(branch)


# ---- blocks that cannot be read --------------------------------------------------------


def test_no_state_root_and_no_daily_root(world: dict[str, Any]) -> None:
    model = _build(world, state_root=None, daily_root=None)
    daily = model["daily"]
    assert daily["status"] == "unreadable" and daily["error"].startswith("读不到")
    assert "days" not in daily
    assert daily["release"]["status"] == "unreadable"
    assert daily["release"]["error"].startswith("读不到")
    assert daily["quality_gate"]["status"] == "unreadable"
    assert model["waiting"]["status"] == "ok" and model["engineering"]["status"] == "ok"


def test_empty_state_and_daily_roots(world: dict[str, Any], tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    daily = _build(world, state_root=empty, daily_root=empty)["daily"]
    assert daily["status"] == "unreadable" and daily["error"].startswith("读不到")
    assert daily["release"]["status"] == "unreadable"
    assert daily["quality_gate"]["status"] == "unreadable"


def _roadmap_unreadable(model: dict[str, Any]) -> None:
    assert model["roadmap"]["status"] == "unreadable"
    assert model["roadmap"]["error"].startswith("读不到")
    assert model["engineering"]["status"] == "unreadable"
    waiting = model["waiting"]
    assert waiting["status"] == "ok" and waiting["events"] is None
    assert [card["id"] for card in waiting["cards"]] == ["WAIT-1", "OPS-2", "ORPHAN-1"]
    assert [card["id"] for card in waiting["asks"]] == ["WAIT-1", "ORPHAN-1"]
    assert all(card["name"] is None for card in waiting["asks"])
    assert any(problem.startswith(ROADMAP_PATH) for problem in model["problems"])
    assert model["daily"]["status"] == "ok" and model["research"]["status"] == "ok"


def test_missing_roadmap_file(world: dict[str, Any]) -> None:
    repo = world["repo"]
    _git(repo, "rm", "-q", ROADMAP_PATH)
    _git(repo, "commit", "-q", "-m", "DEVX-1: drop the roadmap file")
    _roadmap_unreadable(_build(world))


def _broken_line(**changes: Any) -> list[dict[str, Any]]:
    lines = _lines()
    lines[1].update(changes)
    return lines


def _broken_stage(**changes: Any) -> list[dict[str, Any]]:
    lines = _lines()
    lines[1]["stages"][1].update(changes)
    return lines


def _broken_milestone(**changes: Any) -> list[dict[str, Any]]:
    lines = _lines()
    lines[1]["stages"][1]["milestones"][0].update(changes)
    return lines


@pytest.mark.parametrize(
    "text",
    [
        "[unclosed\n",
        "- a list\n",
        _roadmap(schema_version="work_roadmap.v0"),
        _roadmap(updated=None),
        _roadmap(updated="soon"),
        _roadmap(updated="2026-10-5"),
        _roadmap(updated="20261025"),
        _roadmap(cards=["OPS-1"]),
        _roadmap(cards={"OPS-1": ""}),
        _roadmap(cards={"OPS-1": 3}),
        _roadmap(lines="nope"),
        _roadmap(lines=_lines() + [_lines()[1]]),
        _roadmap(lines=_broken_line(id="Daily-Run")),
        _roadmap(lines=_broken_line(kind="ops")),
        _roadmap(lines=_broken_line(goal=None)),
        _roadmap(lines=_broken_line(stages=[])),
        _roadmap(lines=_broken_line(layers=[{"title": "x", "state": "soon"}])),
        _roadmap(lines=_broken_stage(id=2)),
        _roadmap(lines=_broken_stage(id="1")),
        _roadmap(lines=_broken_stage(state="half")),
        _roadmap(lines=_broken_stage(done_on="10-25")),
        _roadmap(lines=_broken_stage(milestones="none")),
        _roadmap(lines=_broken_milestone(state="maybe")),
        _roadmap(lines=_broken_milestone(text="")),
        _roadmap(lines=_broken_milestone(date="2026-13-01")),
        _roadmap(lines=_broken_milestone(cards="OPS-1")),
        _roadmap(lines=_broken_milestone(cards=["OPS-1", 7])),
        _roadmap(lines=_broken_milestone(note=5)),
        # A repeated key is refused, not silently replaced (the cards map is the usual place).
        _roadmap() + "cards:\n  OPS-1: 又一个名字\n",
    ],
)
def test_bad_roadmap_file(world: dict[str, Any], text: str) -> None:
    repo = world["repo"]
    _write(repo / ROADMAP_PATH, text)
    _git(repo, "commit", "-q", "-am", "DEVX-1: bad roadmap file")
    _roadmap_unreadable(_build(world))


def test_a_bad_card_is_a_problem_not_a_failure(world: dict[str, Any]) -> None:
    repo = world["repo"]
    _write(repo / "tasks" / "BAD-1.yaml", "id: [unclosed\n")
    _write(repo / "tasks" / "LIST-1.yaml", "- not a mapping\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "BAD-1: broken cards")
    model = _build(world)
    assert model["waiting"]["status"] == "ok"
    assert model["engineering"]["status"] == "ok"
    assert any(problem.startswith("tasks/BAD-1.yaml") for problem in model["problems"])
    assert any(problem.startswith("tasks/LIST-1.yaml") for problem in model["problems"])


def test_research_failure_is_contained(
    world: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(tree: Path, **_: Any) -> dict[str, Any]:
        raise RuntimeError("boom")

    monkeypatch.setattr(research_site, "collect", broken)
    model = _build(world)
    assert model["research"]["status"] == "unreadable"
    assert model["research"]["error"].startswith("读不到")
    assert model["engineering"]["status"] == "ok" and model["daily"]["status"] == "ok"
    assert any("boom" in problem for problem in model["problems"])


def test_repository_without_the_ref(world: dict[str, Any], tmp_path: Path) -> None:
    bare = tmp_path / "norepo"
    bare.mkdir()
    model = build(bare, state_root=world["state"], daily_root=world["daily"], today=TODAY)
    assert model["source"] == {"ref": "main", "commit": None, "commit_time": None}
    for block in ("waiting", "roadmap", "engineering", "research"):
        assert model[block]["status"] == "unreadable"
        assert model[block]["error"].startswith("读不到")
    assert model["daily"]["status"] == "ok"
    assert model["daily"]["release"]["status"] == "ok"
    assert model["daily"]["release"]["commits_behind"] is None


# ---- what it reads and writes --------------------------------------------------------------


def test_no_brief_text_prices_or_vendor_errors(world: dict[str, Any]) -> None:
    text = dumps(_build(world))
    for secret in SECRETS:
        assert secret not in text, secret
    assert "apikey" not in text
    assert str(world["state"]) not in text and str(world["daily"]) not in text
    assert str(world["repo"]) not in text


ALLOWED = (
    re.compile(
        r"outputs/run_control/scheduler/2026-10-(1[2-9]|2[0-5])_(PRIMARY|RESCUE)(_\d+)?"
        r"\.summary\.json"
    ),
    re.compile(r"outputs/reports/report_quality_gate_\d{4}-\d{2}-\d{2}\.json"),
    re.compile(r"current\.json"),
    re.compile(r"deploy_log\.jsonl"),
)


def test_opens_only_the_listed_production_files(
    world: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    opened: list[str] = []
    roots = [world["state"].resolve(), world["daily"].resolve()]
    real_open = builtins.open

    def spy(file: Any, *args: Any, **kwargs: Any) -> Any:
        if isinstance(file, str | os.PathLike):
            path = Path(file).resolve()
            for root in roots:
                if path.is_relative_to(root):
                    opened.append(path.relative_to(root).as_posix())
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", spy)
    monkeypatch.setattr(io, "open", spy)
    _build(world)
    assert opened, "the spy saw nothing"
    for rel in opened:
        assert any(pattern.fullmatch(rel) for pattern in ALLOWED), rel
    assert "outputs/run_control/scheduler/2026-10-11_PRIMARY.summary.json" not in opened
    assert "outputs/reports/report_quality_gate_2026-10-22.json" not in opened


def test_no_network(world: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("network used")

    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    model = _build(world)
    assert model["daily"]["status"] == "ok" and model["research"]["status"] == "ok"


def _snapshot(*roots: Path) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for root in roots:
        for path in sorted(root.rglob("*")):
            if path.is_file():
                files[path.as_posix()] = path.read_bytes()
            elif path.is_dir():
                files[path.as_posix() + "/"] = b""
    return files


def test_main_writes_only_the_output(world: dict[str, Any], tmp_path: Path) -> None:
    before = _snapshot(world["repo"], world["state"], world["daily"])
    out = tmp_path / "app" / "work" / "work.json"
    code = main(
        [
            "--repo",
            str(world["repo"]),
            "--state-root",
            str(world["state"]),
            "--daily-root",
            str(world["daily"]),
            "--today",
            "2026-10-25",
            "--out",
            str(out),
        ]
    )
    assert code == 0
    assert _snapshot(world["repo"], world["state"], world["daily"]) == before
    assert sorted(path.name for path in out.parent.iterdir()) == ["work.json"]


def test_main_output_is_deterministic(
    world: dict[str, Any], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    args = [
        "--repo",
        str(world["repo"]),
        "--state-root",
        str(world["state"]),
        "--daily-root",
        str(world["daily"]),
        "--today",
        "2026-10-25",
    ]
    first, second = tmp_path / "a" / "work.json", tmp_path / "b" / "work.json"
    assert main([*args, "--out", str(first)]) == 0
    assert main([*args, "--out", str(second)]) == 0
    data = first.read_bytes()
    assert data == second.read_bytes()
    assert data.endswith(b"\n") and b"\r\n" not in data
    assert data.decode("utf-8") == dumps(_build(world))
    assert (
        dumps(_build(world))
        == json.dumps(
            json.loads(data), ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False
        )
        + "\n"
    )
    printed = capsys.readouterr().out.strip().splitlines()
    assert len(printed) == 2
    assert printed[0].startswith("work_overview: 要 Baker 做的 2 件 · 在等的事 1 件 · 日报 PASS 3")
    assert str(first) in printed[0]


def test_main_overwrites_in_place(world: dict[str, Any], tmp_path: Path) -> None:
    out = tmp_path / "work.json"
    out.write_text("old\n", encoding="utf-8")
    args = ["--repo", str(world["repo"]), "--today", "2026-10-25", "--out", str(out)]
    assert main(args) == 0
    model = json.loads(out.read_text(encoding="utf-8"))
    assert model["daily"]["status"] == "unreadable"
    assert sorted(path.name for path in tmp_path.iterdir() if path.is_file()) == ["work.json"]


@pytest.mark.parametrize("inside", ["repo", "state", "daily"])
def test_main_refuses_an_output_inside_the_inputs(
    world: dict[str, Any], inside: str, capsys: pytest.CaptureFixture[str]
) -> None:
    out = world[inside] / "sub" / "work.json"
    before = _snapshot(world["repo"], world["state"], world["daily"])
    code = main(
        [
            "--repo",
            str(world["repo"]),
            "--state-root",
            str(world["state"]),
            "--daily-root",
            str(world["daily"]),
            "--today",
            "2026-10-25",
            "--out",
            str(out),
        ]
    )
    assert code == 2
    assert not out.exists()
    assert _snapshot(world["repo"], world["state"], world["daily"]) == before
    assert (
        "work_overview: 输出文件必须在仓库、生产状态根和发布包目录之外" in capsys.readouterr().err
    )


# ---- keeping the extracted tree in step with research_site --------------------------------------


def _repo_paths(source: str) -> list[str]:
    """Literal paths that research_site builds from its repo argument (repo / "a" / "b")."""
    found = []
    for match in re.finditer(r'\brepo((?:\s*/\s*"[^"]+")+)', source):
        found.append("/".join(re.findall(r'"([^"]+)"', match[1])))
    return found


def test_tree_paths_cover_what_research_site_reads() -> None:
    package = PROJECT_ROOT / "src" / "ai_trading_system" / "research_site"
    paths = [
        rel
        for name in ("collect.py", "zh.py")
        for rel in _repo_paths(package.joinpath(name).read_text(encoding="utf-8"))
    ]
    assert "research" in paths and "config/research/research_bench.yaml" in paths
    for rel in paths:
        assert any(rel == top or rel.startswith(top + "/") for top in TREE_PATHS), rel


def test_module_does_not_reach_for_the_network_or_private_files() -> None:
    package = PROJECT_ROOT / "src" / "ai_trading_system" / "work_overview"
    source = "\n".join(path.read_text(encoding="utf-8") for path in sorted(package.glob("*.py")))
    for word in (
        "urllib",
        "requests",
        "http.client",
        "import socket",
        "ledger.jsonl",
        "owner_daily_brief",
        "reader_brief_",  # the report file; the step id reader_brief is only a label key
        "daily/states",
        "data/raw",
    ):
        assert word not in source, word
    assert "from ai_trading_system import paths, research_site" in source
    assert "research_site.collect(" in source


# ---- cost and wording (run 03) ------------------------------------------------------------------


def _count_git(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    calls: list[list[str]] = []
    real = subprocess.run

    def spy(cmd: Any, *args: Any, **kwargs: Any) -> Any:
        if isinstance(cmd, list | tuple) and cmd and cmd[0] == "git":
            calls.append([str(part) for part in cmd])
        return real(cmd, *args, **kwargs)

    monkeypatch.setattr(subprocess, "run", spy)
    return calls


def test_cards_not_shown_cost_no_git_calls(
    world: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _count_git(monkeypatch)
    _build(world)
    before = len(calls)
    repo = world["repo"]
    for n in range(30):
        _write(repo / "tasks" / f"OLD-{n + 2}.yaml", _card(f"OLD-{n + 2}", "BASELINE_DONE", "无"))
    _git(repo, "add", "-A", when=SECOND)
    _git(repo, "commit", "-q", "-m", "OLD: thirty finished cards", when=SECOND)
    calls.clear()
    model = _build(world)
    assert model["engineering"]["totals"]["BASELINE_DONE"] == 31  # they were read
    assert len(calls) == before  # but nobody asked git about cards that are not shown
    # Each shown card is asked about at most once, however many blocks it appears in.
    asked = [part for call in calls for part in call if part.startswith("tasks/")]
    assert len(asked) == len(set(asked))


def test_reasons_carry_no_paths_or_command_lines(
    world: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gone = tmp_path / "gone-SENTINEL-PATH"
    repo = world["repo"]

    def broken(tree: Path, **_: Any) -> dict[str, Any]:
        raise subprocess.CalledProcessError(128, ["git", "-C", str(repo), "show", "x"])

    monkeypatch.setattr(research_site, "collect", broken)
    model = _build(world, state_root=gone / "state", daily_root=gone / "daily")
    text = dumps(model)
    assert model["daily"]["status"] == "unreadable"
    assert model["daily"]["release"]["status"] == "unreadable"
    assert model["daily"]["quality_gate"]["status"] == "unreadable"
    assert model["research"]["status"] == "unreadable"
    assert "SENTINEL-PATH" not in text and str(tmp_path) not in text
    assert str(repo) not in text and "'-C'" not in text
    assert any("git 退出码 128" in problem for problem in model["problems"])


def test_file_errors_name_the_error_not_the_file(
    world: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = world["state"] / "SENTINEL-PATH" / "x.json"

    def broken(tree: Path, **_: Any) -> dict[str, Any]:
        raise FileNotFoundError(2, "No such file or directory", str(secret))

    monkeypatch.setattr(research_site, "collect", broken)
    model = _build(world)
    assert model["research"]["status"] == "unreadable"
    assert "SENTINEL-PATH" not in dumps(model)
    assert any(
        "FileNotFoundError: No such file or directory" in problem for problem in model["problems"]
    )


# ---- roadmap first, plain words (run 04) --------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "owner", "expected"),
    [
        ("IN_PROGRESS", "Baker（定方向）", True),
        ("IN_PROGRESS", "BAKER", True),
        ("READY", "Project Owner", True),
        ("PROPOSED", "系统实现 + 项目 owner 后续复核", True),
        ("BLOCKED_EXTERNAL", "外部条件 / project owner", True),
        ("BLOCKED_OWNER_INPUT", "Claude", True),
        ("BLOCKED_OWNER_INPUT", None, True),
        # An owner role is not Baker himself: OPS-085 waits for an event, not for him.
        ("VALIDATING", "operations owner（读 17:30 摘要）", False),
        ("PROPOSED", "research-platform domain owner", False),
        ("IN_PROGRESS", "Claude（等 Baker 决定）", False),
        ("IN_PROGRESS", "Claude (Baker later)", False),
        ("DONE", "Baker", False),
        ("DEFERRED", "Project Owner", False),
        ("IN_PROGRESS", None, False),
        ("IN_PROGRESS", "", False),
    ],
)
def test_asks_owner(status: str, owner: str | None, expected: bool) -> None:
    assert asks_owner(status, owner) is expected


def test_asks_and_events(world: dict[str, Any]) -> None:
    waiting = _build(world)["waiting"]
    assert set(waiting) == {"status", "error", "cards", "asks", "events"}
    assert [card["id"] for card in waiting["asks"]] == ["WAIT-1", "ORPHAN-1"]
    assert all(set(card) == CARD_KEYS for card in waiting["asks"])
    assert waiting["asks"][0]["name"] == "定方向"
    assert waiting["asks"][1]["name"] is None  # ORPHAN-1 is in no milestone and has no name
    assert waiting["events"] == [
        {
            "line": "daily_run",
            "line_title": "日报运行与发布",
            "stage": "2",
            "stage_title": "提速",
            "text": "补跑时自动重试",
            "date": "2026-10-13",
            "note": "等第一次真实故障",
            "cards": [
                {
                    "id": "OPS-2",
                    "area": "Fixture / OPS-2 的领域",
                    "status": "VALIDATING",
                    "priority": "P2",
                    "next_owner": "operations owner（读 17:30 摘要）",
                    "next_step": "读 17:30 摘要",
                    "name": "补跑时自动重试",
                    "updated": "2026-10-23",
                }
            ],
        }
    ]


def test_roadmap_lines_stages_and_milestones(world: dict[str, Any]) -> None:
    roadmap = _build(world)["roadmap"]
    assert set(roadmap) == {"status", "error", "updated", "lines", "checked"}
    assert roadmap["status"] == "ok" and roadmap["error"] is None
    assert roadmap["updated"] == "2026-10-25"
    lines = roadmap["lines"]
    assert [line["id"] for line in lines] == ["research", "daily_run", "display"]
    line_keys = {"id", "kind", "title", "goal", "layers", "counts", "current", "stages"}
    assert all(set(line) == line_keys for line in lines)
    research, daily_run, display = lines
    assert (research["kind"], daily_run["kind"]) == ("research", "engineering")
    assert (daily_run["title"], daily_run["goal"]) == ("日报运行与发布", "日报每天跑通")
    assert research["layers"] == [
        {"title": "方向信号", "state": "now"},
        {"title": "期权实现", "state": "later"},
    ]
    assert daily_run["layers"] == []
    assert research["counts"] == {"done": 1, "now": 1, "later": 1, "deferred": 0}
    assert display["counts"] == {"done": 1, "now": 0, "later": 1, "deferred": 1}
    assert research["current"] == {
        "stage": "S1",
        "title": "找方向信号",
        "milestone": "第 6 期只记账",
        "next": "续期卡",
    }
    assert daily_run["current"] == {
        "stage": "2",
        "title": "提速",
        "milestone": "换包以后仍按增量扫",
        "next": "命令行启动耗时",
    }
    assert display["current"] is None
    assert research["stages"][2] == {
        "id": "S2",
        "title": "期权实现",
        "state": "later",
        "done_on": None,
        "note": "等 S1",
        "milestones": [],
    }
    stage = daily_run["stages"][1]
    assert set(stage) == {"id", "title", "state", "done_on", "note", "milestones"}
    assert (stage["id"], stage["title"], stage["state"], stage["done_on"], stage["note"]) == (
        "2",
        "提速",
        "now",
        None,
        "中位数明显下降",
    )
    assert daily_run["stages"][0]["done_on"] == "2026-10-19"
    milestones = stage["milestones"]
    assert all(set(item) == {"text", "state", "date", "note", "cards"} for item in milestones)
    assert [(item["text"], item["state"], item["date"]) for item in milestones] == [
        ("扫描改增量", "done", "2026-10-24"),
        ("补跑时自动重试", "waiting", "2026-10-13"),
        ("换包以后仍按增量扫", "now", None),
        ("命令行启动耗时", "todo", None),
    ]
    now = milestones[2]
    assert [card["id"] for card in now["cards"]] == ["OPS-1", "NOTE-1"]
    assert [card["name"] for card in now["cards"]] == ["日报提速", "备忘"]
    assert now["cards"][0]["updated"] == "2026-10-20"
    assert milestones[3]["cards"] == [] and milestones[3]["note"] is None
    assert research["stages"][1]["milestones"][1]["cards"] == []  # no cards key at all
    every = [
        card
        for line in lines
        for stage in line["stages"]
        for item in stage["milestones"]
        for card in item["cards"]
    ]
    assert every and all(set(card) == CARD_KEYS for card in every)


def test_checked_in_the_last_seven_days(world: dict[str, Any]) -> None:
    checked = _build(world)["roadmap"]["checked"]
    keys = {"date", "kind", "line", "line_title", "stage", "stage_title", "text"}
    assert all(set(entry) == keys for entry in checked)
    # Newest first, then in file order; 10-18 is the eighth day back and is not listed.
    assert [
        (entry["date"], entry["kind"], entry["line"], entry["stage"], entry["text"])
        for entry in checked
    ] == [
        ("2026-10-24", "milestone", "daily_run", "2", "扫描改增量"),
        ("2026-10-19", "stage", "daily_run", "1", "从发布包运行"),
        ("2026-10-19", "stage", "display", "1", "第 1 版"),
        ("2026-10-19", "milestone", "display", "1", "生成器"),
    ]
    assert (checked[0]["line_title"], checked[0]["stage_title"]) == ("日报运行与发布", "提速")
    later = _build(world, today=date(2026, 10, 26))["roadmap"]["checked"]
    assert [entry["text"] for entry in later] == ["扫描改增量"]
    # Only done counts: the now and todo milestones dated 11-20 are not checked off on 11-20.
    assert _build(world, today=date(2026, 11, 20))["roadmap"]["checked"] == []


def test_dates_without_quotes_are_read_as_days(world: dict[str, Any]) -> None:
    quoted = _build(world)["roadmap"]
    repo = world["repo"]
    lines = _lines()
    lines[1]["stages"][1]["milestones"][0]["date"] = date(2026, 10, 24)
    lines[1]["stages"][0]["done_on"] = date(2026, 10, 19)
    _write(repo / ROADMAP_PATH, _roadmap(updated=date(2026, 10, 25), lines=lines))
    _git(repo, "commit", "-q", "-am", "DEVX-1: dates without quotes")
    assert "updated: 2026-10-25\n" in (repo / ROADMAP_PATH).read_text(encoding="utf-8")
    model = _build(world)
    assert model["roadmap"] == quoted
    assert model["engineering"]["updated"] == "2026-10-25"


def test_every_card_carries_its_name(world: dict[str, Any]) -> None:
    model = _build(world)
    engineering = model["engineering"]
    assert {
        card["id"]: card["name"] for line in engineering["mainlines"] for card in line["cards"]
    } == {
        "OPS-1": "日报提速",
        "OPS-2": "补跑时自动重试",
        "NOTE-1": "备忘",
        "DONE-1": "做完的卡",
        "WAIT-1": "定方向",
    }
    assert [card["name"] for card in engineering["research_cards"]] == ["研究计划"]
    assert [card["name"] for card in model["waiting"]["cards"]] == [
        "定方向",
        "补跑时自动重试",
        None,
    ]
    assert engineering["unassigned"][0]["name"] is None


def test_an_empty_roadmap_has_no_events(world: dict[str, Any]) -> None:
    repo = world["repo"]
    _write(repo / ROADMAP_PATH, _roadmap(lines=[]))
    _git(repo, "commit", "-q", "-am", "DEVX-1: empty roadmap")
    model = _build(world)
    assert model["roadmap"]["status"] == "ok"
    assert model["roadmap"]["lines"] == [] and model["roadmap"]["checked"] == []
    assert model["waiting"]["events"] == []
    assert [card["id"] for card in model["waiting"]["asks"]] == ["WAIT-1", "ORPHAN-1"]


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("PRIMARY", "12:00 主跑"),
        ("RESCUE", "17:30 补跑"),
        ("RESCUE_2", "17:30 补跑（第 2 次）"),
        ("PRIMARY_3", "12:00 主跑（第 3 次）"),
    ],
)
def test_run_label(name: str, expected: str) -> None:
    assert run_label(name) == expected


@pytest.mark.parametrize(
    ("run", "expected"),
    [
        ({"status": "PASS", "blocker": "NONE"}, None),
        (
            {"status": "RUN_CONTROL_ALREADY_COMPLETE", "blocker": "WAITING_FOR_NEW_AS_OF"},
            "这个交易日已经出过报告，没有再跑",
        ),
        (
            {
                "status": "RUN_CONTROL_BLOCKED_TERMINAL_RECOVERY_REQUIRED",
                "blocker": "CONTROL_PLANE",
            },
            "前一次失败后要先人工恢复，这次没有跑",
        ),
        (
            {
                "status": "FAIL",
                "blocker": "DATA_SOURCE_UNAVAILABLE",
                "failed_sources": ["sec_companyfacts", "official_policy_sources"],
                "failed_steps": ["capture_daily_inputs"],
            },
            "数据源取不到：SEC 公司财务数据、官方政策源取数出错，取当天的输入没过",
        ),
        (
            {"status": "FAIL", "blocker": "CODE_DEFECT", "failed_steps": ["score_daily"]},
            "代码问题：打分没过",
        ),
        ({"status": "FAIL", "blocker": "DATA_QUALITY_FAILED"}, "数据质量没过"),
        ({"status": "FAIL", "blocker": "NONE", "failed_steps": ["secret_hygiene"]}, "密钥扫描没过"),
        (
            {
                "status": "FAIL",
                "blocker": "SOMETHING_NEW",
                "failed_steps": ["new_step"],
                "failed_sources": ["new_source"],
            },
            "其他原因（SOMETHING_NEW）：new_source取数出错，new_step没过",
        ),
        ({"status": "FAIL", "blocker": None, "failed_steps": [None, "score_daily"]}, "打分没过"),
        ({"status": "FAIL"}, "没过"),
        ({"status": None, "blocker": None}, "没过"),
        ({"status": "RUN_CONTROL_LOCKED"}, "没过（RUN_CONTROL_LOCKED）"),
    ],
)
def test_run_reason(run: dict[str, Any], expected: str | None) -> None:
    assert run_reason(run) == expected


def test_daily_reasons_and_streak(world: dict[str, Any]) -> None:
    daily = _build(world)["daily"]
    days = {day["date"]: day for day in daily["days"]}
    # Only days that were rescued or failed get a reason: the first run that did not pass.
    assert {key: day["reason"] for key, day in days.items() if day["reason"] is not None} == {
        "2026-10-13": "数据源取不到：打分没过",
        "2026-10-17": "数据源取不到：市场宏观数据取数出错",
        "2026-10-20": "数据源取不到：市场宏观数据取数出错，管线健康检查没过",
        "2026-10-22": "数据源取不到",
    }
    assert days["2026-10-13"]["runs"][1]["reason"] == "前一次失败后要先人工恢复，这次没有跑"
    assert days["2026-10-18"]["runs"][0]["reason"] == "这个交易日已经出过报告，没有再跑"
    assert days["2026-10-18"]["reason"] is None
    assert [run["label"] for run in days["2026-10-20"]["runs"]] == [
        "12:00 主跑",
        "17:30 补跑",
        "17:30 补跑（第 2 次）",
    ]
    assert days["2026-10-20"]["runs"][2]["reason"] is None
    assert all(set(run) == RUN_KEYS for day in daily["days"] for run in day["runs"])
    # Back from 10-25 (not run yet today): 10-24, 10-23 and 10-22 had a report; 10-21 had no run.
    assert (daily["streak"], daily["streak_rescued"]) == (3, 1)
    assert daily["last_failed"] == {"date": "2026-10-13", "reason": "数据源取不到：打分没过"}


def _state_with(root: Path, runs: dict[str, list[tuple[str, str]]]) -> Path:
    scheduler = root / "outputs" / "run_control" / "scheduler"
    scheduler.mkdir(parents=True)
    for day, calls in runs.items():
        for hour, (name, status) in enumerate(calls, start=12):
            _write(
                scheduler / f"{day}_{name}.summary.json",
                _summary(
                    name.split("_")[0],
                    day,
                    status,
                    f"{day}T{hour}:00:00+09:00",
                    f"{day}T{hour}:10:00+09:00",
                ),
            )
    return root


@pytest.mark.parametrize(
    ("runs", "streak", "rescued", "last_failed"),
    [
        (
            {
                "2026-10-25": [("PRIMARY", "PASS")],
                "2026-10-24": [("PRIMARY", DONE)],
                "2026-10-23": [("PRIMARY", "FAIL"), ("RESCUE", "PASS")],
                "2026-10-22": [("PRIMARY", "FAIL")],
                "2026-10-21": [("PRIMARY", "PASS")],
                "2026-10-20": [("PRIMARY", "FAIL")],
            },
            2,
            1,
            "2026-10-22",
        ),
        (
            {"2026-10-25": [("PRIMARY", "FAIL")], "2026-10-24": [("PRIMARY", "PASS")]},
            0,
            0,
            "2026-10-25",
        ),
        # Today not run yet is skipped; a past day with no run at all had no report.
        ({"2026-10-23": [("PRIMARY", "PASS")]}, 0, 0, None),
        ({"2026-10-24": [("PRIMARY", "PASS")], "2026-10-23": [("PRIMARY", "PASS")]}, 2, 0, None),
        ({}, 0, 0, None),
    ],
)
def test_streak_counts_trading_days_with_a_report(
    world: dict[str, Any],
    tmp_path: Path,
    runs: dict[str, list[tuple[str, str]]],
    streak: int,
    rescued: int,
    last_failed: str | None,
) -> None:
    daily = _build(world, state_root=_state_with(tmp_path / "state2", runs))["daily"]
    assert daily["status"] == "ok"
    assert (daily["streak"], daily["streak_rescued"]) == (streak, rescued)
    found = daily["last_failed"]
    assert (found["date"] if found else None) == last_failed


def test_a_day_that_passed_at_noon_has_no_reason(world: dict[str, Any], tmp_path: Path) -> None:
    runs = {"2026-10-24": [("PRIMARY", "FAIL"), ("PRIMARY_2", "PASS")]}
    daily = _build(world, state_root=_state_with(tmp_path / "state2", runs))["daily"]
    day = daily["days"][-2]
    assert (day["date"], day["result"], day["reason"]) == ("2026-10-24", "PASS", None)
    assert day["runs"][0]["reason"] == "数据源取不到"
    assert [run["label"] for run in day["runs"]] == ["12:00 主跑", "12:00 主跑（第 2 次）"]
    assert daily["last_failed"] is None


def test_stale_cards_are_active_cards_changed_after_the_roadmap(world: dict[str, Any]) -> None:
    repo = world["repo"]
    _write(repo / ROADMAP_PATH, _roadmap(updated="2026-10-19"))
    _git(repo, "commit", "-q", "-am", "DEVX-1: an older roadmap date")
    problems = _build(world)["problems"]
    stale = [problem for problem in problems if "路线图的 updated 还是 2026-10-19" in problem]
    # Every active card in a milestone changed later; DONE-1 is done and ORPHAN-1 is in none.
    assert stale == [
        f"路线图：{card} 在 {day} 改过，路线图的 updated 还是 2026-10-19"
        for card, day in (
            ("TRADING-1", "2026-10-20"),
            ("OPS-1", "2026-10-20"),
            ("OPS-2", "2026-10-23"),
            ("NOTE-1", "2026-10-20"),
            ("WAIT-1", "2026-10-20"),
        )
    ]


def _scheduler_summary_module() -> ModuleType:
    path = PROJECT_ROOT / "scripts" / "ops" / "daily_scheduler_summary.py"
    spec = importlib.util.spec_from_file_location("devx034_scheduler_summary", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_labels_cover_every_blocker_step_and_source() -> None:
    categories = _scheduler_summary_module().CATEGORY_LABELS
    assert set(BLOCKER_LABELS) == set(categories) - {"NONE"}
    plan = yaml.safe_load((PROJECT_ROOT / "config" / "scheduled_tasks.yaml").read_text("utf-8"))
    steps = {task["daily_plan_step_id"] for task in plan["cadences"]["daily_trading_day"]["tasks"]}
    assert steps and steps <= set(STEP_LABELS)
    from ai_trading_system import daily_input_capture

    assert set(SOURCE_LABELS) == set(daily_input_capture._SUPPORTED_COMPONENT_IDS)
    labels = [*BLOCKER_LABELS.values(), *STEP_LABELS.values(), *SOURCE_LABELS.values()]
    assert all(label.strip() for label in labels)


def test_the_committed_roadmap_is_readable_and_names_its_cards() -> None:
    roadmap = load_roadmap(PROJECT_ROOT)
    assert set(roadmap) == {"updated", "names", "lines"}
    tasks = {path.stem for path in (PROJECT_ROOT / "tasks").glob("*.yaml")}
    referenced = {
        card
        for line in roadmap["lines"]
        for stage in line["stages"]
        for milestone in stage["milestones"]
        for card in milestone["cards"]
    }
    assert referenced and referenced <= tasks, sorted(referenced - tasks)
    assert referenced <= set(roadmap["names"]), sorted(referenced - set(roadmap["names"]))
    for line in roadmap["lines"]:
        assert sum(stage["state"] == "now" for stage in line["stages"]) <= 1, line["id"]
    assert not (PROJECT_ROOT / "docs" / "work_overview" / "mainlines.yaml").exists()
