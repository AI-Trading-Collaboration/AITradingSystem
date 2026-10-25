"""Acceptance tests for the work overview generator (DEVX-034, run 02).

Written before the implementation. The requirement is docs/requirements/DEVX-034_Work_Overview.md,
section 8. The fixture is a small git repository (task cards, the mainline file and two research
records), a production state root (scheduler summaries, report quality gates, and files the
generator must never read) and a release directory (current.json, deploy_log.jsonl).
"""

from __future__ import annotations

import builtins
import io
import json
import os
import re
import socket
import subprocess
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system import research_site
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.work_overview import (
    ACTIVE_STATUSES,
    CALENDAR_DAYS,
    MAINLINES_PATH,
    SCHEMA_VERSION,
    TREE_PATHS,
    build,
    day_result,
    dumps,
    main,
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

CARD_KEYS = {"id", "area", "status", "priority", "next_owner", "next_step", "updated"}
RUN_KEYS = {
    "name",
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


def _mainlines(**changes: Any) -> str:
    raw: dict[str, Any] = {
        "schema_version": "work_mainlines.v1",
        "updated": "2026-10-25",
        "mainlines": [
            {
                "id": "daily_run",
                "title": "日报运行与发布",
                "goal": "日报每天跑通",
                "milestone": "从发布包运行",
                "cards": ["OPS-1", "OPS-2", "NOTE-1"],
            },
            {
                "id": "display",
                "title": "展示与知识",
                "goal": "一个地方看清全部工作",
                "milestone": "第 2 步",
                "cards": ["WAIT-1", "DONE-1"],
            },
        ],
        "research_cards": ["TRADING-1"],
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
    _write(repo / MAINLINES_PATH, _mainlines())
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
    assert MAINLINES_PATH == "docs/work_overview/mainlines.yaml"
    assert CALENDAR_DAYS == 14
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
    assert "主线文件：在办的卡 ORPHAN-1 不在任何主线里" in problems
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
    assert lines[0]["milestone"] == "从发布包运行"
    assert [card["id"] for card in lines[0]["cards"]] == ["OPS-1", "OPS-2", "NOTE-1"]
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


def test_mainline_rule_violations(world: dict[str, Any]) -> None:
    repo = world["repo"]
    lines = yaml.safe_load(_mainlines())["mainlines"]
    lines[1]["cards"] = ["WAIT-1", "OPS-1", "GONE-1", "GONE-1"]
    _write(
        repo / MAINLINES_PATH, _mainlines(mainlines=lines, research_cards=["TRADING-1", "WAIT-1"])
    )
    _git(repo, "commit", "-q", "-am", "DEVX-1: break the mainline rules")
    model = _build(world)
    engineering = model["engineering"]
    assert engineering["status"] == "ok"
    assert engineering["duplicates"] == ["GONE-1", "OPS-1", "WAIT-1"]
    assert engineering["missing"] == ["GONE-1"]
    # NOTE-1 is still in daily_run; DONE-1 left the file but is done.
    assert [card["id"] for card in engineering["unassigned"]] == ["ORPHAN-1"]
    assert [card["id"] for card in engineering["mainlines"][1]["cards"]] == ["WAIT-1", "OPS-1"]
    for problem in (
        "主线文件：没有卡 GONE-1",
        "主线文件：GONE-1 出现了不止一次",
        "主线文件：OPS-1 出现了不止一次",
        "主线文件：WAIT-1 出现了不止一次",
        "主线文件：在办的卡 ORPHAN-1 不在任何主线里",
    ):
        assert problem in model["problems"]
    assert sum(problem.startswith("主线文件：") for problem in model["problems"]) == 5


def test_unassigned_lists_every_active_card(world: dict[str, Any]) -> None:
    repo = world["repo"]
    _write(repo / MAINLINES_PATH, _mainlines(mainlines=[], research_cards=[]))
    _git(repo, "commit", "-q", "-am", "DEVX-1: empty mainlines")
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
    assert set(daily) == {"status", "error", "days", "counts", "release", "quality_gate"}
    assert daily["status"] == "ok" and daily["error"] is None
    days = {day["date"]: day for day in daily["days"]}
    assert [day["date"] for day in daily["days"]] == [f"2026-10-{n}" for n in range(12, 26)]
    assert all(
        set(day) == {"date", "result", "as_of", "minutes", "release_id", "runs"}
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


def test_missing_mainline_file(world: dict[str, Any]) -> None:
    repo = world["repo"]
    _git(repo, "rm", "-q", MAINLINES_PATH)
    _git(repo, "commit", "-q", "-m", "DEVX-1: drop the mainline file")
    model = _build(world)
    assert model["engineering"]["status"] == "unreadable"
    assert model["engineering"]["error"].startswith("读不到")
    assert model["waiting"]["status"] == "ok"
    assert [card["id"] for card in model["waiting"]["cards"]] == ["WAIT-1", "OPS-2", "ORPHAN-1"]
    assert any(problem.startswith(MAINLINES_PATH) for problem in model["problems"])


@pytest.mark.parametrize(
    "text",
    [
        "schema_version: work_mainlines.v0\nmainlines: []\n",
        "schema_version: work_mainlines.v1\nmainlines: nope\n",
        "schema_version: work_mainlines.v1\nmainlines:\n  - id: x\n    cards: [OPS-1]\n",
        "[unclosed\n",
    ],
)
def test_bad_mainline_file(world: dict[str, Any], text: str) -> None:
    repo = world["repo"]
    _write(repo / MAINLINES_PATH, text)
    _git(repo, "commit", "-q", "-am", "DEVX-1: bad mainline file")
    model = _build(world)
    assert model["engineering"]["status"] == "unreadable"
    assert model["waiting"]["status"] == "ok"


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
    for block in ("waiting", "engineering", "research"):
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
    assert printed[0].startswith("work_overview: 等 Baker 的 3 件")
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
        "reader_brief",
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
