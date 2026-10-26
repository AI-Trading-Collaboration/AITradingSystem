"""Acceptance tests for the research progress page (TRADING-2568, run 01).

Written before the implementation. The requirement is
docs/requirements/TRADING-2568_Research_Progress_Page.md.
The fixture repository holds one question per status, a ledger, a screening ledger and task cards.
"""

from __future__ import annotations

import html
import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as bench_ledger
from ai_trading_system.research_site import (
    SCHEMA_VERSION,
    TASK_PREFIXES,
    build_site,
    collect,
    main,
    markdown_to_html,
    render,
)

DECISION = "owner_decision:TRADING-2567:2026-10-14:site_fixture"
UNAPPROVED = "owner_decision:TRADING-2567:2026-10-14:not_yet"
LONG_QUESTION = "问" * 60 + "答" * 40
GIT = shutil.which("git")
needs_git = pytest.mark.skipif(GIT is None, reason="git is not installed")

LEDGER = """# 研究台账

说明文字。

## 总表

| # | 研究线（任务，时间） | 结论 | 评级 | 去向 |
|---|---|---|---|---|
| 1 | 日报评分 <规则> | 未测成；部分不可执行 | D | 生产在用 |
| 2 | 权重搜索 | 拒绝 | B | 关闭 |
| 3 | 简单基线 | 证据不足 | C | 留作基准 |
| 4 | 三个子问题 | Q4a 证据不足；Q4b 拒绝；Q4c 拒绝 | A | 关闭 |

## 别的表

| # | x | 拒绝 | y | z |
|---|---|---|---|---|
| 9 | 不该算进总表 | 拒绝 | A | - |
"""

SUB_QUESTIONS = """# QQQ-E1 结论

总的结论：保留。

| 子问题 | 结论 | 评级 | 依据 |
|---|---|---|---|
| Q1a（q1a） | 证据不足 | A | 9 条规则 3 条成立 |
| **Q1b（q1b）** | **拒绝** | A | 9 条规则 2 条成立 |
| Q1c | 待定 | - | 还没读 |

| | Q1a | Q1b |
|---|---|---|
| 最大回撤 | 拒绝 | 保留 |

| 结论 | 评级 |
|---|---|
| 未测成 | D |

附：结论：证据不足。
"""

NEGATIVE = """# 负结果清单

用途：开题前必读。

## 已证伪

| 结论 | 评级 |
|---|---|
| Composer v2 不稳健 | B |

## 未测成

| 方向 | 重开条件 |
|---|---|
| 指标族消融 | 在评估台上重测 |
"""


def _git(cwd: Path, *args: str) -> str:
    done = subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)
    return done.stdout.strip()


def _prereg(question_id: str, *, decision: str | None, question: str | None = None) -> str:
    raw = yaml.safe_load(
        PROJECT_ROOT.joinpath("research", "_template", "prereg.yaml").read_text(encoding="utf-8")
    )
    raw["question_id"] = question_id
    raw["owner_decision"] = decision
    raw["question"] = question or f"{question_id} 的研究问题"
    raw["data"]["snapshot_id"] = "qlh-fixture"
    return yaml.safe_dump(raw, sort_keys=False, allow_unicode=True)


def _question(
    repo: Path, question_id: str, *, decision: str | None = DECISION, question: str | None = None
) -> Path:
    directory = repo.joinpath("research", question_id)
    directory.mkdir(parents=True)
    directory.joinpath("prereg.yaml").write_text(
        _prereg(question_id, decision=decision, question=question), encoding="utf-8"
    )
    directory.joinpath("strategy.py").write_text(
        "def example(data, params):\n    return 1.0\n", encoding="utf-8"
    )
    return directory


def _run_body(
    question_id: str,
    number: int,
    *,
    status: str,
    sharpe: float,
    trial: int | None = None,
    role: str = "development",
) -> dict[str, Any]:
    trial = trial or number
    body: dict[str, Any] = {
        "run_id": f"{question_id}-{number:04d}",
        "strategy": {"id": "example", "function": "example", "module": "strategy.py"},
        "window": {"window_id": "dev", "role": role, "start": "1999-03-10", "end": None},
        "trial": {"key": f"key-{question_id}-{trial}", "number": trial, "new": trial == number},
        "status": status,
        "rules": None,
    }
    if status == "ok":
        body["result"] = {
            "strategy": {
                "geometric_annual_return": 0.0512,
                "max_drawdown": -0.3341,
                "sharpe_vs_cash": 0.41,
                "sharpe_daily": sharpe,
                "excess_skewness": -0.2,
                "excess_kurtosis": 3.0,
                "return_days": 2900,
            },
            "comparison": {"geometric_annual_return": -0.004, "max_drawdown": 0.081},
        }
        body["rules"] = [
            {"metric": "comparison.max_drawdown", "op": ">=", "value": 0.05, "holds": True},
            {"metric": "comparison.cvar_5", "op": ">=", "value": 0.1, "holds": False},
        ]
    else:
        body["problems"] = ["parameter window was not read"]
    return body


def _runs(directory: Path, bodies: list[dict[str, Any]], *, break_chain: bool = False) -> None:
    runs = directory.joinpath("runs")
    runs.mkdir()
    previous: str | None = None
    for seq, body in enumerate(bodies, start=1):
        record = {
            "schema_version": "research_run.v1",
            "question_id": directory.name,
            "seq": seq,
            "prev_sha256": "0" * 64 if (break_chain and seq == 2) else previous,
            **body,
        }
        data = json.dumps(record, indent=1, sort_keys=True).encode("utf-8") + b"\n"
        runs.joinpath(f"{seq:04d}.json").write_bytes(data)
        previous = bench_ledger.chain_digest(data)


def _reads(directory: Path, roles: list[str]) -> None:
    lines: list[bytes] = []
    previous: str | None = None
    for seq, role in enumerate(roles, start=1):
        entry = {
            "schema_version": "research_window_read.v1",
            "question_id": directory.name,
            "seq": seq,
            "prev_sha256": previous,
            "read_id": f"{directory.name}-{role}-{seq}",
            "role": role,
            "window_id": f"{role}_window",
            "recorded_at": f"2026-10-1{seq}T00:00:00+00:00",
        }
        line = json.dumps(entry, sort_keys=True).encode("utf-8")
        lines.append(line)
        previous = bench_ledger.chain_digest(line)
    directory.joinpath("window_reads.jsonl").write_bytes(b"\n".join(lines) + b"\n")


def _screen(repo: Path, number: int, signal: str, family: str, verdict: str, p: float) -> None:
    directory = repo.joinpath("research", "_screens", "ledger")
    directory.mkdir(parents=True, exist_ok=True)
    record = {
        "schema_version": "research_screen_record.v1",
        "number": number,
        "screen_id": f"S{number:04d}",
        "signal_id": signal,
        "module_sha256": family,
        "spec_sha256": "s" * 64,
        "primary_p_value": p,
        "verdict": verdict,
    }
    directory.joinpath(f"{number:04d}.json").write_text(json.dumps(record), encoding="utf-8")


def _task(repo: Path, name: str, task_id: str, status: str, next_step: str | None = None) -> None:
    repo.joinpath("tasks").mkdir(exist_ok=True)
    repo.joinpath("tasks", f"{name}.yaml").write_text(
        yaml.safe_dump(
            {
                "schema_version": "task.v1",
                "id": task_id,
                "area": "研究",
                "status": status,
                "next_step": next_step or f"{task_id} 的下一步",
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )


def _fixture(tmp_path: Path) -> Path:
    repo = tmp_path.joinpath("repo")
    repo.joinpath("docs", "requirements").mkdir(parents=True)
    repo.joinpath("docs", "requirements", "TRADING-2567.md").write_text(
        f"- {DECISION}：开题批准。\n", encoding="utf-8"
    )
    research = repo.joinpath("research")
    research.mkdir()
    research.joinpath("ledger.md").write_text(LEDGER, encoding="utf-8")
    research.joinpath("negative_results.md").write_text(NEGATIVE, encoding="utf-8")
    _question(repo, "QQQ-B1", decision=UNAPPROVED, question=LONG_QUESTION)
    _question(repo, "QQQ-C1")
    developing = _question(repo, "QQQ-A1", question="波动率 <script>alert(1)</script> & 仓位")
    _runs(
        developing,
        [
            _run_body("QQQ-A1", 1, status="ok", sharpe=0.031),
            _run_body("QQQ-A1", 2, status="check_failed", sharpe=0.0),
            _run_body("QQQ-A1", 3, status="ok", sharpe=0.024),
            _run_body("QQQ-A1", 4, status="ok", sharpe=0.031, trial=1),
        ],
    )
    validated = _question(repo, "QQQ-D1")
    _runs(
        validated,
        [
            _run_body("QQQ-D1", 1, status="ok", sharpe=0.02),
            _run_body("QQQ-D1", 2, status="ok", sharpe=0.02, trial=1, role="validation"),
        ],
    )
    _reads(validated, ["validation"])
    closed = _question(repo, "QQQ-E1")
    _runs(closed, [_run_body("QQQ-E1", 1, status="ok", sharpe=0.01)])
    _reads(closed, ["validation", "consistency_check"])
    closed.joinpath("decision.md").write_text(
        "# QQQ-E1 结论\n\n结论：**拒绝**。验证期没有通过。\n", encoding="utf-8"
    )
    broken = research.joinpath("QQQ-F1")
    broken.mkdir()
    broken.joinpath("prereg.yaml").write_text(
        "schema_version: research_prereg.v0\nquestion_id: QQQ-F1\n", encoding="utf-8"
    )
    template = research.joinpath("_template")
    template.mkdir()
    template.joinpath("prereg.yaml").write_text(_prereg("QQQ-Q0", decision=None), encoding="utf-8")
    _screen(repo, 1, "mom_63", "a" * 64, "inconclusive", 0.2)
    _screen(repo, 2, "credit_spread", "b" * 64, "eliminated", 0.9)
    _screen(repo, 3, "mom_63", "a" * 64, "candidate", 0.01)
    _task(repo, "TRADING-2565_RESEARCH_BENCH", "TRADING-2565_RESEARCH_BENCH", "IN_PROGRESS")
    _task(repo, "TRADING-2567", "TRADING-2567", "IN_PROGRESS")
    _task(repo, "TRADING-2569_JUDGMENT", "TRADING-2569_JUDGMENT", "PROPOSED")
    _task(
        repo,
        "TRADING-2566_SHORT",
        "TRADING-2566_SHORT",
        "IN_PROGRESS",
        "第一句要点。后面还有很长的说明。",
    )
    _task(repo, "TRADING-2566_LONG", "TRADING-2566_LONG", "IN_PROGRESS", "长" * 120)
    _task(repo, "TRADING-25650_OTHER", "TRADING-25650_OTHER", "PROPOSED")
    _task(repo, "OPS-001_DAILY", "OPS-001_DAILY", "DONE")
    return repo


def _git_fixture(tmp_path: Path) -> Path:
    repo = _fixture(tmp_path)
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "test")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "first records")
    repo.joinpath("research", "QQQ-C1", "notes.md").write_text("开发期准备\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "QQQ-C1 notes")
    repo.joinpath("README.md").write_text("unrelated\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "unrelated change")
    return repo


def _block(page: str, element_id: str) -> str:
    """The element with this id, from its opening tag to its matching closing tag."""

    opening = re.search(rf'<(\w+)\b[^>]*\bid="{re.escape(element_id)}"[^>]*>', page)
    assert opening is not None, element_id
    tag = opening.group(1)
    depth, position = 1, opening.end()
    for token in re.finditer(rf"<{tag}\b|</{tag}>", page[position:]):
        depth += (
            1
            if token.group(0).startswith("<" + tag) and not token.group(0).startswith("</")
            else -1
        )
        if depth == 0:
            return page[opening.start() : position + token.end()]
    raise AssertionError(f"{element_id} is not closed")


def _by_id(model: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {question["question_id"]: question for question in model["questions"]}


# 1. Questions and their place in the flow ------------------------------------------------------


def test_questions_are_listed_in_order_without_the_template(tmp_path: Path) -> None:
    model = collect(_fixture(tmp_path))
    assert model["schema_version"] == SCHEMA_VERSION == "research_site.v1"
    ids = [question["question_id"] for question in model["questions"]]
    assert ids == ["QQQ-A1", "QQQ-B1", "QQQ-C1", "QQQ-D1", "QQQ-E1", "QQQ-F1"]


@pytest.mark.parametrize(
    ("question_id", "status", "step", "approved"),
    [
        ("QQQ-B1", "awaiting_approval", 4, False),
        ("QQQ-C1", "awaiting_dev_runs", 5, True),
        ("QQQ-A1", "developing", 5, True),
        ("QQQ-D1", "validation_read", 6, True),
        ("QQQ-E1", "closed", 7, True),
    ],
)
def test_status_and_step_follow_the_files(
    tmp_path: Path, question_id: str, status: str, step: int, approved: bool
) -> None:
    question = _by_id(collect(_fixture(tmp_path)))[question_id]
    assert question["status"] == status
    assert question["step"] == step
    assert question["approved"] is approved
    assert question["problems"] == []


def test_a_consistency_check_read_is_step_seven(tmp_path: Path) -> None:
    # The bench's role is consistency_check (research_bench.settings.WINDOW_ROLES), TRADING-2565-74.
    repo = _fixture(tmp_path)
    _reads(_question(repo, "QQQ-G1"), ["validation", "consistency_check"])
    question = _by_id(collect(repo))["QQQ-G1"]
    assert (question["status"], question["step"]) == ("consistency_read", 7)
    assert question["next"] == "一致性期已读，等写结论。"
    assert question["problems"] == []


def test_next_step_texts(tmp_path: Path) -> None:
    questions = _by_id(collect(_fixture(tmp_path)))
    assert questions["QQQ-B1"]["next"] == "等你批准开题。"
    assert questions["QQQ-C1"]["next"] == "已批准开题，等 pi 跑开发期。"
    assert questions["QQQ-A1"]["next"] == (
        "开发期已跑 3 次试验；下一步 Claude 审实现，再读验证期。"
    )
    assert questions["QQQ-D1"]["next"] == "验证期已读，等写结论。"
    assert questions["QQQ-E1"]["next"] == "已结题。"


def test_closed_question_uses_the_step_before_its_decision(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    shutil.copy(
        repo.joinpath("research", "QQQ-E1", "decision.md"),
        repo.joinpath("research", "QQQ-D1", "decision.md"),
    )
    questions = _by_id(collect(repo))
    assert (questions["QQQ-D1"]["status"], questions["QQQ-D1"]["step"]) == ("closed", 6)
    assert questions["QQQ-E1"]["decision"]["words"] == [{"label": None, "word": "拒绝"}]
    assert "验证期没有通过" in questions["QQQ-E1"]["decision"]["markdown"]
    assert questions["QQQ-A1"]["decision"] is None


def test_decision_without_a_verdict_word(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    repo.joinpath("research", "QQQ-E1", "decision.md").write_text("还在写。\n", encoding="utf-8")
    model = collect(repo)
    question = _by_id(model)["QQQ-E1"]
    assert question["status"] == "closed"
    assert question["decision"] == {"words": [], "markdown": "还在写。\n"}
    assert any("decision.md" in problem for problem in question["problems"])
    assert any(p.startswith("QQQ-E1: ") and "decision.md" in p for p in model["problems"])


def test_decision_with_sub_questions_in_a_table(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    repo.joinpath("research", "QQQ-E1", "decision.md").write_text(SUB_QUESTIONS, encoding="utf-8")
    model = collect(repo)
    question = _by_id(model)["QQQ-E1"]
    assert question["decision"]["words"] == [
        {"label": None, "word": "保留"},
        {"label": "Q1a（q1a）", "word": "证据不足"},
        {"label": "Q1b（q1b）", "word": "拒绝"},
        {"label": None, "word": "未测成"},
        {"label": None, "word": "证据不足"},
    ]
    assert question["problems"] == []


def test_approval_is_read_from_requirement_documents(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    repo.joinpath("docs", "requirements", "OTHER.md").write_text(UNAPPROVED, encoding="utf-8")
    question = _by_id(collect(repo))["QQQ-B1"]
    assert question["approved"] is True
    assert question["status"] == "awaiting_dev_runs"
    repo.joinpath("docs", "requirements", "OTHER.md").unlink()
    repo.joinpath("docs", "OTHER.md").write_text(UNAPPROVED, encoding="utf-8")
    assert _by_id(collect(repo))["QQQ-B1"]["approved"] is False


def test_question_without_owner_decision_is_not_approved(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    _question(repo, "QQQ-G1", decision=None)
    question = _by_id(collect(repo))["QQQ-G1"]
    assert (question["approved"], question["status"], question["owner_decision"]) == (
        False,
        "awaiting_approval",
        None,
    )


# 2. Runs, trials and reads ------------------------------------------------------------------


def test_runs_trials_and_failures(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    model = collect(repo)
    question = _by_id(model)["QQQ-A1"]
    summary = bench_ledger.ledger_summary(repo.joinpath("research"))
    expected = summary["questions"]["QQQ-A1"]
    assert question["trials"] == expected["trials"] == 3
    assert question["trial_cap"] == expected["trial_cap"]
    assert (question["runs_total"], question["runs_failed"]) == (4, 1)
    assert question["strategies"] == ["example"]
    assert question["owner_decision"] == DECISION
    assert question["question"] == "波动率 <script>alert(1)</script> & 仓位"
    first, failed, third, repeat = question["runs"]
    assert (repeat["run_id"], repeat["trial"]) == ("QQQ-A1-0004", 1)
    assert first["run_id"] == "QQQ-A1-0001"
    assert (first["strategy"], first["role"], first["status"], first["trial"]) == (
        "example",
        "development",
        "ok",
        1,
    )
    assert first["geometric_annual_return"] == pytest.approx(0.0512)
    assert first["max_drawdown"] == pytest.approx(-0.3341)
    assert first["sharpe_vs_cash"] == pytest.approx(0.41)
    assert first["vs_matched_return"] == pytest.approx(-0.004)
    assert first["vs_matched_drawdown"] == pytest.approx(0.081)
    assert (first["rules_held"], first["rules_total"]) == (1, 2)
    deflated = {line["run_id"]: line["deflated_sharpe"] for line in expected["runs"]}
    assert first["deflated_sharpe"] == deflated["QQQ-A1-0001"]
    assert third["deflated_sharpe"] == deflated["QQQ-A1-0003"]
    assert first["deflated_sharpe"] is not None
    assert failed["status"] == "check_failed"
    for field in (
        "geometric_annual_return",
        "max_drawdown",
        "sharpe_vs_cash",
        "deflated_sharpe",
        "vs_matched_return",
        "vs_matched_drawdown",
        "rules_held",
        "rules_total",
    ):
        assert failed[field] is None, field


def test_reads_are_listed(tmp_path: Path) -> None:
    question = _by_id(collect(_fixture(tmp_path)))["QQQ-E1"]
    assert question["reads"] == [
        {
            "read_id": "QQQ-E1-validation-1",
            "role": "validation",
            "window_id": "validation_window",
            "recorded_at": "2026-10-11T00:00:00+00:00",
        },
        {
            "read_id": "QQQ-E1-consistency_check-2",
            "role": "consistency_check",
            "window_id": "consistency_check_window",
            "recorded_at": "2026-10-12T00:00:00+00:00",
        },
    ]


def test_summary(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    model = collect(repo)
    summary = model["summary"]
    assert summary["active"] == 5
    assert summary["closed"] == 1
    expected = bench_ledger.ledger_summary(repo.joinpath("research"))["global_trials"]
    assert summary["global_trials"] == expected == 5
    assert summary["failed_runs"] == 1
    assert summary["validation_reads"] == 2
    assert summary["chain_ok"] is True
    assert summary["problems"] == len(model["problems"]) == 1


# 3. Problems stay visible and never stop the page --------------------------------------------


def test_unreadable_prereg_is_an_error_question(tmp_path: Path) -> None:
    model = collect(_fixture(tmp_path))
    question = _by_id(model)["QQQ-F1"]
    assert question["status"] == "error"
    assert question["step"] is None
    assert question["next"] == "记录有问题，先修：见下面的问题。"
    assert question["runs"] == [] and question["reads"] == []
    assert question["problems"]
    assert any("question" in problem for problem in question["problems"])
    assert [problem for problem in model["problems"] if problem.startswith("QQQ-F1: ")]


def test_prereg_that_is_not_yaml(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    path = repo.joinpath("research", "QQQ-F1", "prereg.yaml")
    path.write_text("schema_version: [unclosed\n", encoding="utf-8")
    model = collect(repo)
    questions = _by_id(model)
    assert questions["QQQ-F1"]["status"] == "error"
    assert any("ledger_summary" in problem for problem in model["problems"])
    assert questions["QQQ-A1"]["trials"] == 3
    assert questions["QQQ-A1"]["status"] == "developing"
    assert all(run["deflated_sharpe"] is None for run in questions["QQQ-A1"]["runs"])
    assert model["summary"]["global_trials"] == 5
    render(model)


def test_question_id_must_match_the_directory(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    directory = repo.joinpath("research", "QQQ-H1")
    directory.mkdir()
    directory.joinpath("prereg.yaml").write_text(
        _prereg("QQQ-Z9", decision=DECISION), encoding="utf-8"
    )
    question = _by_id(collect(repo))["QQQ-H1"]
    assert question["status"] == "error"
    assert question["problems"]


def test_broken_run_chain(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    shutil.rmtree(repo.joinpath("research", "QQQ-A1", "runs"))
    _runs(
        repo.joinpath("research", "QQQ-A1"),
        [
            _run_body("QQQ-A1", 1, status="ok", sharpe=0.03),
            _run_body("QQQ-A1", 2, status="ok", sharpe=0.02),
        ],
        break_chain=True,
    )
    model = collect(repo)
    question = _by_id(model)["QQQ-A1"]
    assert question["status"] == "error"
    assert question["runs"] == []
    assert any("prev_sha256" in problem for problem in question["problems"])
    assert model["summary"]["chain_ok"] is False
    assert any(p.startswith("QQQ-A1: ") and "prev_sha256" in p for p in model["problems"])
    assert _by_id(model)["QQQ-D1"]["status"] == "validation_read"


def test_broken_read_chain(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    path = repo.joinpath("research", "QQQ-D1", "window_reads.jsonl")
    path.write_bytes(path.read_bytes().replace(b'"prev_sha256": null', b'"prev_sha256": "x"'))
    model = collect(repo)
    assert _by_id(model)["QQQ-D1"]["status"] == "error"
    assert model["summary"]["chain_ok"] is False


# 4. Ledger, negative results, screens and tasks --------------------------------------------


def test_ledger_rows_and_counts(tmp_path: Path) -> None:
    ledger = collect(_fixture(tmp_path))["ledger"]
    assert ledger["parse_ok"] is True
    assert ledger["markdown"] == LEDGER
    assert ledger["rows"] == [
        {
            "number": 1,
            "line": "日报评分 <规则>",
            "verdict": "未测成；部分不可执行",
            "words": ["未测成"],
            "rating": "D",
            "fate": "生产在用",
        },
        {
            "number": 2,
            "line": "权重搜索",
            "verdict": "拒绝",
            "words": ["拒绝"],
            "rating": "B",
            "fate": "关闭",
        },
        {
            "number": 3,
            "line": "简单基线",
            "verdict": "证据不足",
            "words": ["证据不足"],
            "rating": "C",
            "fate": "留作基准",
        },
        {
            "number": 4,
            "line": "三个子问题",
            "verdict": "Q4a 证据不足；Q4b 拒绝；Q4c 拒绝",
            "words": ["证据不足", "拒绝", "拒绝"],
            "rating": "A",
            "fate": "关闭",
        },
    ]
    assert ledger["counts"] == {"保留": 0, "拒绝": 3, "证据不足": 2, "未测成": 1}


def test_ledger_row_without_a_verdict_word(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    repo.joinpath("research", "ledger.md").write_text(
        LEDGER.replace("| 2 | 权重搜索 | 拒绝 |", "| 2 | 权重搜索 | 待定 |"), encoding="utf-8"
    )
    model = collect(repo)
    row = model["ledger"]["rows"][1]
    assert (row["number"], row["words"]) == (2, [])
    assert model["ledger"]["counts"] == {"保留": 0, "拒绝": 2, "证据不足": 2, "未测成": 1}
    assert [p for p in model["problems"] if "research/ledger.md" in p and "第 2 行" in p]


def test_ledger_that_cannot_be_parsed(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    repo.joinpath("research", "ledger.md").write_text("# 台账\n\n还没有表。\n", encoding="utf-8")
    model = collect(repo)
    assert model["ledger"]["parse_ok"] is False
    assert model["ledger"]["rows"] == []
    assert model["ledger"]["counts"] == {"保留": 0, "拒绝": 0, "证据不足": 0, "未测成": 0}
    assert any("research/ledger.md" in problem for problem in model["problems"])
    page = render(model)
    assert "还没有表。" in page


def test_missing_ledger_and_negative_results(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    repo.joinpath("research", "ledger.md").unlink()
    repo.joinpath("research", "negative_results.md").unlink()
    model = collect(repo)
    assert model["ledger"]["markdown"] is None
    assert model["ledger"]["parse_ok"] is False
    assert model["negative_results"] is None
    assert model["negative_sections"] == []
    assert any("research/ledger.md" in problem for problem in model["problems"])
    render(model)


def test_negative_results_are_kept_verbatim_and_split_into_sections(tmp_path: Path) -> None:
    model = collect(_fixture(tmp_path))
    assert model["negative_results"] == NEGATIVE
    assert model["negative_sections"] == [
        {"title": "已证伪", "markdown": "| 结论 | 评级 |\n|---|---|\n| Composer v2 不稳健 | B |"},
        {
            "title": "未测成",
            "markdown": "| 方向 | 重开条件 |\n|---|---|\n| 指标族消融 | 在评估台上重测 |",
        },
    ]


def test_negative_results_without_sections(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    repo.joinpath("research", "negative_results.md").write_text(
        "# 清单\n\n### 小节\n\n还没有。\n", encoding="utf-8"
    )
    model = collect(repo)
    assert model["negative_sections"] == []
    assert "还没有。" in render(model)


def test_screening_ledger(tmp_path: Path) -> None:
    screens = collect(_fixture(tmp_path))["screens"]
    assert screens["present"] is True
    assert (screens["records"], screens["family_size"]) == (3, 2)
    assert screens["counts"] == {"candidate": 1, "eliminated": 1, "inconclusive": 0}
    assert screens["members"] == [
        {
            "signal_id": "credit_spread",
            "number": 2,
            "verdict": "eliminated",
            "primary_p_value": 0.9,
            "records": 1,
        },
        {
            "signal_id": "mom_63",
            "number": 3,
            "verdict": "candidate",
            "primary_p_value": 0.01,
            "records": 2,
        },
    ]


def test_unreadable_screen_record_and_missing_screens(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    repo.joinpath("research", "_screens", "ledger", "0004.json").write_text("{", encoding="utf-8")
    repo.joinpath("research", "_screens", "ledger", "README.md").write_text("x", encoding="utf-8")
    model = collect(repo)
    assert model["screens"]["records"] == 3
    assert any("0004.json" in problem for problem in model["problems"])
    shutil.rmtree(repo.joinpath("research", "_screens"))
    screens = collect(repo)["screens"]
    assert screens == {
        "present": False,
        "records": 0,
        "family_size": 0,
        "counts": {"candidate": 0, "eliminated": 0, "inconclusive": 0},
        "members": [],
    }


def test_research_tasks(tmp_path: Path) -> None:
    assert TASK_PREFIXES == (
        "TRADING-2565",
        "TRADING-2566",
        "TRADING-2567",
        "TRADING-2568",
        "TRADING-2569",
        "DEVX-030",
    )
    tasks = collect(_fixture(tmp_path))["tasks"]
    assert tasks == [
        {
            "id": "TRADING-2565_RESEARCH_BENCH",
            "status": "IN_PROGRESS",
            "next_step": "TRADING-2565_RESEARCH_BENCH 的下一步",
            "area": "研究",
        },
        {
            "id": "TRADING-2566_LONG",
            "status": "IN_PROGRESS",
            "next_step": "长" * 120,
            "area": "研究",
        },
        {
            "id": "TRADING-2566_SHORT",
            "status": "IN_PROGRESS",
            "next_step": "第一句要点。后面还有很长的说明。",
            "area": "研究",
        },
        {
            "id": "TRADING-2567",
            "status": "IN_PROGRESS",
            "next_step": "TRADING-2567 的下一步",
            "area": "研究",
        },
        {
            "id": "TRADING-2569_JUDGMENT",
            "status": "PROPOSED",
            "next_step": "TRADING-2569_JUDGMENT 的下一步",
            "area": "研究",
        },
    ]


# 5. Git metadata -------------------------------------------------------------------------------


def test_without_git(tmp_path: Path) -> None:
    model = collect(_fixture(tmp_path), repo_url="https://github.com/o/r")
    assert model["source"] == {
        "commit": None,
        "commit_time": None,
        "repo_url": "https://github.com/o/r",
    }
    assert model["activity"] == []
    question = _by_id(model)["QQQ-A1"]
    assert question["last_activity"] is None
    assert question["links"] == {"prereg": None, "decision": None}


@needs_git
def test_directory_inside_another_repository_is_not_read_as_git(tmp_path: Path) -> None:
    outer = tmp_path.joinpath("outer")
    outer.mkdir()
    _git(outer, "init", "-q", "-b", "main")
    _git(outer, "config", "user.email", "test@example.com")
    _git(outer, "config", "user.name", "test")
    repo = _fixture(outer)
    _git(outer, "add", "-A")
    _git(outer, "commit", "-q", "-m", "outer")
    model = collect(repo)
    assert model["source"]["commit"] is None
    assert model["activity"] == []


@needs_git
def test_source_activity_and_links(tmp_path: Path) -> None:
    repo = _git_fixture(tmp_path)
    head = _git(repo, "rev-parse", "HEAD")
    model = collect(repo, repo_url="https://github.com/o/r/")
    assert model["source"]["commit"] == head
    assert model["source"]["commit_time"] == _git(repo, "log", "-1", "--format=%cI")
    assert model["source"]["repo_url"] == "https://github.com/o/r"
    subjects = [entry["subject"] for entry in model["activity"]]
    assert subjects == ["QQQ-C1 notes", "first records"]
    assert model["activity"][0]["commit"] == _git(repo, "rev-parse", "HEAD~1")
    assert re.match(r"^\d{4}-\d{2}-\d{2}T", model["activity"][0]["time"])
    questions = _by_id(model)
    assert questions["QQQ-C1"]["last_activity"]["subject"] == "QQQ-C1 notes"
    assert questions["QQQ-A1"]["last_activity"]["subject"] == "first records"
    assert questions["QQQ-A1"]["links"] == {
        "prereg": f"https://github.com/o/r/blob/{head}/research/QQQ-A1/prereg.yaml",
        "decision": None,
    }
    assert questions["QQQ-E1"]["links"]["decision"] == (
        f"https://github.com/o/r/blob/{head}/research/QQQ-E1/decision.md"
    )
    assert collect(repo)["questions"][0]["links"] == {"prereg": None, "decision": None}


@needs_git
def test_task_commits_count_as_activity(tmp_path: Path) -> None:
    repo = _git_fixture(tmp_path)
    _task(repo, "TRADING-2566_GUARDRAILS", "TRADING-2566_GUARDRAILS", "IN_PROGRESS")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "guardrail task")
    _task(repo, "OPS-002_OTHER", "OPS-002_OTHER", "DONE")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "ops task")
    subjects = [entry["subject"] for entry in collect(repo)["activity"]]
    assert subjects[0] == "guardrail task"
    assert "ops task" not in subjects


@needs_git
def test_activity_keeps_the_latest_twenty(tmp_path: Path) -> None:
    repo = _git_fixture(tmp_path)
    notes = repo.joinpath("research", "QQQ-C1", "notes.md")
    for number in range(25):
        notes.write_text(f"{number}\n", encoding="utf-8")
        _git(repo, "commit", "-q", "-am", f"note {number}")
    activity = collect(repo)["activity"]
    assert len(activity) == 20
    assert activity[0]["subject"] == "note 24"


# 6. The page --------------------------------------------------------------------------------


def test_page_shape_and_no_external_resources(tmp_path: Path) -> None:
    page = render(collect(_fixture(tmp_path)))
    assert page.lstrip().lower().startswith("<!doctype html>")
    assert 'lang="zh-CN"' in page
    assert '<meta charset="utf-8">' in page.lower()
    assert 'name="viewport"' in page
    assert "<title>研究进展</title>" in page
    assert "prefers-color-scheme: dark" in page
    lowered = page.lower()
    for banned in ("<script", "<link", "<img", "<iframe", "@import", "url("):
        assert banned not in lowered, banned
    assert 'href="http' not in page
    assert "errors.pydantic.dev" not in page


def test_page_escapes_repository_text(tmp_path: Path) -> None:
    page = render(collect(_fixture(tmp_path)))
    assert "<script>alert(1)</script>" not in page
    assert html.escape("波动率 <script>alert(1)</script> & 仓位", quote=False) in page
    assert "日报评分 &lt;规则&gt;" in page


def test_page_shows_each_question_and_its_next_step(tmp_path: Path) -> None:
    model = collect(_fixture(tmp_path))
    page = render(model)
    for question in model["questions"]:
        assert f'id="q-{question["question_id"]}"' in page
        assert html.escape(question["next"], quote=False) in page
    for row in model["ledger"]["rows"]:
        assert html.escape(row["line"], quote=False) in page
    assert "credit_spread" in page and "mom_63" in page
    assert "TRADING-2565_RESEARCH_BENCH" in page
    assert "Composer v2 不稳健" in page
    assert "按子问题计" in page


def test_problems_section_only_when_there_are_problems(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    model = collect(repo)
    page = render(model)
    assert 'id="problems"' in page
    for problem in model["problems"]:
        assert html.escape(problem, quote=False) in page
    shutil.rmtree(repo.joinpath("research", "QQQ-F1"))
    clean = collect(repo)
    assert clean["problems"] == []
    assert 'id="problems"' not in render(clean)


def test_route_map_links_to_existing_sections(tmp_path: Path) -> None:
    page = render(collect(_fixture(tmp_path)))
    route = _block(page, "route")
    for target in ("questions", "board", "negative", "screens", "quick", "judgment"):
        assert f'href="#{target}"' in route, target
        assert f'id="{target}"' in page, target
    assert 'class="node live" href="#questions"' in route
    assert 'class="node idle" href="#quick"' in route
    assert 'class="node idle" href="#judgment"' in route
    assert "J4 上线后启用" in route and "J3 上线后启用" in route
    assert 'class="node live" href="#screens"' in route
    repo = _fixture(tmp_path.joinpath("no-screens"))
    shutil.rmtree(repo.joinpath("research", "_screens"))
    idle = _block(render(collect(repo)), "route")
    assert 'class="node idle" href="#screens"' in idle and "S4b 上线后启用" in idle


def test_header_legend_lists_the_four_conclusion_words(tmp_path: Path) -> None:
    page = render(collect(_fixture(tmp_path)))
    legend = re.search(r'<p class="legend">(.*?)</p>', page, re.S)
    assert legend is not None
    assert re.findall(r'<span class="b (\w+)">([^<]*)</span>', legend.group(1)) == [
        ("keep", "保留"),
        ("rej", "拒绝"),
        ("weak", "证据不足"),
        ("fail", "未测成"),
    ]


def test_not_yet_online_panels_say_which_batch_enables_them(tmp_path: Path) -> None:
    page = render(collect(_fixture(tmp_path)))
    assert "J4 上线后启用" in _block(page, "quick")
    assert "J3 上线后启用" in _block(page, "judgment")


def test_active_questions_come_before_closed_ones(tmp_path: Path) -> None:
    page = render(collect(_fixture(tmp_path)))
    closed = page.index('id="q-QQQ-E1"')
    for active in ("QQQ-A1", "QQQ-B1", "QQQ-C1", "QQQ-D1", "QQQ-F1"):
        assert page.index(f'id="q-{active}"') < closed, active


def test_question_card_shows_each_conclusion_word(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    repo.joinpath("research", "QQQ-E1", "decision.md").write_text(SUB_QUESTIONS, encoding="utf-8")
    page = render(collect(repo))
    verdicts = re.search(r'<p class="verdicts">(.*?)</p>', _block(page, "q-QQQ-E1"), re.S)
    assert verdicts is not None
    assert re.findall(r'<span class="b (\w+)">([^<]*)</span>', verdicts.group(1)) == [
        ("keep", "保留"),
        ("weak", "Q1a（q1a） 证据不足"),
        ("rej", "Q1b（q1b） 拒绝"),
        ("fail", "未测成"),
        ("weak", "证据不足"),
    ]
    assert 'class="verdicts"' not in _block(page, "q-QQQ-A1")


def test_question_card_shows_trials_reads_and_failures(tmp_path: Path) -> None:
    model = collect(_fixture(tmp_path))
    page = render(model)
    developing = _block(page, "q-QQQ-A1")
    cap = _by_id(model)["QQQ-A1"]["trial_cap"]
    assert f"3 / {cap}" in developing
    assert '验证期</span><span class="n">未读</span>' in developing
    assert '失败运行</span><span class="n">1</span>' in developing
    assert '验证期</span><span class="n">读过 1 次</span>' in _block(page, "q-QQQ-D1")
    assert '<p class="state">已结题</p>' in _block(page, "q-QQQ-E1")
    assert '<p class="state">进行中</p>' in developing
    assert '<p class="state">记录有问题</p>' in _block(page, "q-QQQ-F1")


def test_validation_run_shows_rules_and_difference_from_the_fixed_position(tmp_path: Path) -> None:
    page = render(collect(_fixture(tmp_path)))
    validated = _block(page, "q-QQQ-D1")
    assert "规则成立" in validated and "1 / 2" in validated
    assert "+8.1 点" in validated and "-0.4 点" in validated
    assert "规则成立" not in _block(page, "q-QQQ-A1")


def test_long_question_text_is_cut_in_the_header_and_kept_whole_below(tmp_path: Path) -> None:
    page = render(collect(_fixture(tmp_path)))
    card = _block(page, "q-QQQ-B1")
    assert f'<span class="m">{LONG_QUESTION[:80]}…</span>' in card
    assert f"<details><summary>题目全文</summary><p>{LONG_QUESTION}</p></details>" in card
    short = _block(page, "q-QQQ-C1")
    assert "题目全文" not in short and "QQQ-C1 的研究问题" in short


def test_development_runs_and_decision_text_are_folded(tmp_path: Path) -> None:
    page = render(collect(_fixture(tmp_path)))
    assert "<details><summary>开发期 4 次运行</summary>" in _block(page, "q-QQQ-A1")
    closed = _block(page, "q-QQQ-E1")
    assert "<details><summary>结论全文</summary>" in closed
    assert "验证期没有通过" in closed
    assert 'data-label="年化"' in _block(page, "q-QQQ-A1")


def test_board_groups_rows_by_conclusion_word(tmp_path: Path) -> None:
    page = render(collect(_fixture(tmp_path)))
    assert '<details class="grp" id="grp-rej" open>' in page
    for closed in ("keep", "weak", "fail"):
        assert re.search(rf'<details class="grp" id="grp-{closed}">', page), closed
    rejected = _block(page, "grp-rej")
    weak = _block(page, "grp-weak")
    assert rejected.count("<li>") == 2 and "权重搜索" in rejected and "三个子问题" in rejected
    assert weak.count("<li>") == 2 and "简单基线" in weak and "三个子问题" in weak
    assert _block(page, "grp-fail").count("<li>") == 1
    assert "<strong>2</strong> 条" in rejected
    assert '<span class="m">拒绝</span>' not in rejected
    assert "Q4a 证据不足；Q4b 拒绝；Q4c 拒绝" in rejected
    assert "未测成；部分不可执行" in _block(page, "grp-fail")
    keep = _block(page, "grp-keep")
    assert "<strong>0</strong> 条" in keep and "还没有。" in keep and "<li>" not in keep


def test_ledger_row_without_a_word_gets_its_own_group(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    assert 'id="grp-none"' not in render(collect(repo))
    repo.joinpath("research", "ledger.md").write_text(
        LEDGER.replace("| 2 | 权重搜索 | 拒绝 |", "| 2 | 权重搜索 | 待定 |"), encoding="utf-8"
    )
    page = render(collect(repo))
    assert "权重搜索" in _block(page, "grp-none") and "无结论词" in _block(page, "grp-none")
    assert "权重搜索" not in _block(page, "grp-rej")


def test_negative_results_are_folded_by_section(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    page = render(collect(repo))
    negative = _block(page, "negative")
    assert '<details class="grp"><summary><strong>已证伪</strong></summary>' in negative
    assert '<details class="grp"><summary><strong>未测成</strong></summary>' in negative
    assert negative.index("Composer v2 不稳健") < negative.index("指标族消融")
    repo.joinpath("research", "negative_results.md").unlink()
    assert "没有找到 research/negative_results.md" in render(collect(repo))


def test_screens_panel_lists_members(tmp_path: Path) -> None:
    screens = _block(render(collect(_fixture(tmp_path))), "screens")
    assert "已筛 2 个信号" in screens
    rows = screens.split("<tr>")
    candidate = next(row for row in rows if "mom_63" in row)
    assert '<span class="b cand">候选</span>' in candidate and "0.010" in candidate
    eliminated = next(row for row in rows if "credit_spread" in row)
    assert '<span class="b out">淘汰</span>' in eliminated and "0.900" in eliminated


def test_tasks_show_the_first_sentence_and_fold_the_rest(tmp_path: Path) -> None:
    work = _block(render(collect(_fixture(tmp_path))), "work")
    assert "<summary>第一句要点</summary><p>第一句要点。后面还有很长的说明。</p>" in work
    assert f"<summary>{'长' * 96}…</summary>" in work and "长" * 120 in work
    assert "<strong>TRADING-2569</strong>" in work
    assert "<strong>TRADING-2569_JUDGMENT" not in work
    assert '<strong>TRADING-2565</strong> <span class="m">研究</span>' in work
    assert '<span class="b st-proposed">PROPOSED</span>' in work


def test_screens_placeholder_before_s4b(tmp_path: Path) -> None:
    repo = _fixture(tmp_path)
    shutil.rmtree(repo.joinpath("research", "_screens"))
    assert "S4b 上线后启用" in render(collect(repo))


@needs_git
def test_header_shows_the_commit_and_its_time_without_seconds(tmp_path: Path) -> None:
    repo = _git_fixture(tmp_path)
    source = collect(repo)["source"]
    when = source["commit_time"]
    meta = re.search(r'<p class="meta">(.*?)</p>', render(collect(repo)), re.S)
    assert meta is not None
    assert meta.group(1) == (
        f"数据截至 main <code>{source['commit'][:9]}</code> · {when[:10]} {when[11:16]} {when[19:]}"
    )
    assert "不在 git 仓库里" in render(collect(_fixture(tmp_path.joinpath("plain"))))


@needs_git
def test_page_links_only_with_a_repository_url(tmp_path: Path) -> None:
    repo = _git_fixture(tmp_path)
    head = _git(repo, "rev-parse", "HEAD")
    page = render(collect(repo, repo_url="https://github.com/o/r"))
    assert f'href="https://github.com/o/r/blob/{head}/research/QQQ-A1/prereg.yaml"' in page
    assert head[:9] in page
    assert 'href="http' not in render(collect(repo))


# 7. Markdown -----------------------------------------------------------------------------------


def test_markdown_blocks() -> None:
    text = (
        "# 标题\n\n## 小节\n\n一段 **粗** 和 `a<b` 文字。\n\n- 第一项\n- 第二项\n\n"
        "| 结论 | 评级 |\n|---|---|\n| 拒绝 | B |\n\n#### 四级\n"
    )
    out = markdown_to_html(text)
    assert "<h1>标题</h1>" in out
    assert "<h2>小节</h2>" in out
    assert "<h4>四级</h4>" in out
    assert "<strong>粗</strong>" in out
    assert "<code>a&lt;b</code>" in out
    assert out.count("<li>") == 2 and "<li>第一项</li>" in out and "<ul>" in out
    assert "<th>结论</th>" in out and "<th>评级</th>" in out
    assert '<td data-label="结论">拒绝</td>' in out and '<td data-label="评级">B</td>' in out
    assert '<div class="wrap"><table class="stack">' in out
    assert "<td>---</td>" not in out and "---" not in out
    assert re.search(r"<p>[^<]*一段", out)


def test_markdown_escapes_html() -> None:
    out = markdown_to_html('<b onclick="x">hi</b> & [链接](https://example.com)\n')
    assert "<b" not in out
    assert "&lt;b onclick=" in out
    assert "&amp;" in out
    assert "<a " not in out
    labelled = markdown_to_html('| a" onclick="x | b |\n|---|---|\n| 1 | 2 |\n')
    assert 'data-label="a" onclick' not in labelled
    assert 'data-label="a&quot; onclick=&quot;x"' in labelled


# 8. Output files and the command ---------------------------------------------------------------


def test_model_is_plain_json(tmp_path: Path) -> None:
    model = collect(_fixture(tmp_path))
    assert json.loads(json.dumps(model, allow_nan=False)) == model


@needs_git
def test_build_is_deterministic(tmp_path: Path) -> None:
    repo = _git_fixture(tmp_path)
    first = build_site(repo, tmp_path.joinpath("out1"), repo_url="https://github.com/o/r")
    second = build_site(
        repo, tmp_path.joinpath("out2", "nested"), repo_url="https://github.com/o/r"
    )
    assert first == tmp_path.joinpath("out1", "index.html")
    assert first.read_bytes() == second.read_bytes()
    assert str(tmp_path) not in first.read_text(encoding="utf-8")
    site = json.loads(tmp_path.joinpath("out1", "site.json").read_text(encoding="utf-8"))
    assert site == collect(repo, repo_url="https://github.com/o/r")
    assert (
        tmp_path.joinpath("out1", "site.json").read_bytes()
        == tmp_path.joinpath("out2", "nested", "site.json").read_bytes()
    )


def test_command(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repo = _fixture(tmp_path)
    out = tmp_path.joinpath("site")
    assert main(["--repo", str(repo), "--out", str(out)]) == 0
    assert out.joinpath("index.html").is_file()
    assert json.loads(out.joinpath("site.json").read_text(encoding="utf-8")) == collect(repo)
    assert "index.html" in capsys.readouterr().out


def test_real_repository_renders() -> None:
    model = collect(PROJECT_ROOT)
    assert model["ledger"]["parse_ok"] is True
    assert len(model["ledger"]["rows"]) >= 22
    assert "QQQ-Q1" in _by_id(model)
    q1 = _by_id(model)["QQQ-Q1"]
    if q1["decision"] is not None:
        assert [entry["word"] for entry in q1["decision"]["words"]] == ["证据不足", "拒绝"]
    assert "_template" not in _by_id(model)
    render(model)
