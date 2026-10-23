"""Acceptance tests for the research page's re-layout (TRADING-2568, run 06).

Written before the implementation. The requirement is
docs/requirements/TRADING-2568_Research_Progress_Page.md, section 5.8. As in run 03, the wording
asserted here is the wording of the page. The fixture repository is the run-03 fixture
(tests/test_research_site_zh.py) plus the record files the new blocks read: forward records, the
forward ledger's weekly summary, the family ledger, two charters and a prereg that names a screen.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml
from test_research_site_zh import _block, _repo, _screen

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_site import build_site, collect, markdown_to_html, render

FORWARD_RECORDS = """# 前瞻记录

规则见章程。写在核对之前，写下以后不改。

## 慢熊型

D 是策略在一个慢熊段里的段内收益减同平均敞口固定敞口的段内收益（点）。

核对时机：下一个慢熊段完整以后。

| 编号 | 写下 | 配置 | 信号的触发条件 | 慢熊段汇总 | 对下一个慢熊段的预期 | 核对 |
|---|---|---|---|---|---|---|
| F0001 | 2026-10-21 | fast_slow（QQQ-Q3） | 两个信号各管一半 | 10 段、+12.0 | D > 0 的概率 0.8 | 待核 |
| F0002 | 2026-10-21 | trend4（QQQ-Q2） | 四个信号各管四分之一 | 10 段、+11.5 | D > 0 的概率 0.8 | 2026-10-30：对（D +3.1） |

## 日报模型

日报模型那条线由前瞻记录台每天记一行。

| 编号 | 写下 | 配置 | 敞口 | 依据 | 对下一段的预期 | 核对 |
|---|---|---|---|---|---|---|
| F0005 | 2026-10-22 | 日报模型 <b>现在的版本</b> | 区间中点 | 无 | 长牛段每年少 1 到 3 点（概率 0.6） | 待核 |

| 编号 | 备注 |
|---|---|
| X | 格太少，不算一条记录 |
"""  # noqa: E501

WEEKLY = """# 前瞻记录台周汇总

- 运行：TRADING-2565-45。三条线：日报模型、固定敞口 0.8、F0001。
- 2026-10-12 到 2026-10-20 是回填，2026-10-21 起是前瞻。描述，不判。

账本 7 行，2026-10-12..2026-10-20。

## 自 2026-10-12（含回填）

| 线 | 交易日 | 缺收益 | 累计 | 最大回撤 | 平均敞口 |
|---|---|---|---|---|---|
| 日报模型（区间中点，t+2 持有） | 7 | 1 | -1.53% | -1.96% | 0.40 |
| 固定敞口 0.8 | 7 | 0 | -3.59% | -4.43% | 0.80 |

## 自 2026-10-21（前瞻）

| 线 | 交易日 | 缺收益 | 累计 | 最大回撤 | 平均敞口 |
|---|---|---|---|---|---|
| 日报模型（区间中点，t+2 持有） | 0 | 0 | — | — | — |

## 按周

| 周（最后一个交易日） | 交易日 | 日报模型 | 固定 |
|---|---|---|---|
| 2026-10-16 | 5 | -1.96% | -4.43% |
"""

FAMILIES = """# 机制家族台账

研究失败要变成方向。

距下次横读：正式题 0/3、试跑失败 0/3、信号 0/20、前瞻核对 0/1，上次横读 2026-10-22。

## 失败标签

| 标签 | 判据 |
|---|---|
| 机制不存在 | 左尾和回撤都不比对照好 |

## 家族

- 家族价：只用有验证读取的变体。
- 状态：有验证读取的变体至少 3 个、家族价低于 10%/点，为「关闭」。

| 家族 | 已试变体（筛选 / 正式） | 最好的验证结果 | 家族价 | 标签分布 | 状态 | 重开条件 | 线 |
|---|---|---|---|---|---|---|---|
| 经典趋势 | 筛选 0 / 正式 2（trend4、fast_slow） | fast_slow：左尾 +4.4% | 2.4%/点 | 有效果但代价太高 2 | 关闭 | 新机制 | QQQ-Z9 |
| 波动 | 筛选 1（vol 淘汰）/ 正式 1（q1a） | q1a：左尾 +7.2% | 4.5%/点 | 有效果但代价太高 1 | 停车 | 说明机制 | S0003 |
| 利率 | 筛选 2（slow 候选、fast 候选） | 没有验证读取 | — | — | 开放 | — | S0002、S0004 |
| 卖波动 | 描述性测量 1 | 没有验证读取 | — | 不打标签 | 停车 | 见清单 | TRADING-2567-20 |
| 坏行 | 只有七格 | — | — | — | 开放 | — |
"""  # noqa: E501

CHARTER_P1 = """# 研究章程 第 1 期

## 第 1 期的方向

旧的一期，页面不该读它。
"""

CHARTER_P2 = """# 研究章程 第 2 期

Baker 2026-10-22 签。

## 第 1 期小结

| 项 | 数 |
|---|---|
| 新筛的信号 | 0 |

## 第 2 期的方向

只做横读导出的两题：H-X 不对称再入，H-Y 条件化储备，H-X 先。
不新筛信号。

没选的：只记账。

## 1. 期限

这份发布到 main 那天起，到 2026-11-18。发布时第 1 期结束。

## 2. 目标

检验两条结构。

## 3. 两题的预注册

两题共同：都做 B 区探针。

### 3.1 H-X 不对称再入（经典趋势，换实现）

规则（原文）：

> 规则：敞口底 1.0；退出照 fast_slow 月底判；
> 再入改快。

规则写细：

- 两条腿各管一半敞口。

| 行 | 读数 | 预期（概率） | 失败淘汰什么 |
|---|---|---|---|
| 1 | 慢熊段 D 中位 ≥ +8 | 成立（0.7） | 「再入加快不伤慢熊效果」 |
| 2 | 长牛段代价减半 | 成立（0.5） | 「代价主要在再入迟」 |

判法：

| 行 | 成立 |
|---|---|
| 1 | D 中位 ≥ +8 且 D > 0 ≥ 8/10 |

### 3.2 H-Y 条件化储备（P1，换实现）

先写明：软泄漏可疑。没有引文，没有表。

## 4. 顺序和谁做

H-X 先。
"""

EXPECTED_TABLE = """| 行 | 读数 | 预期（概率） | 失败淘汰什么 |
|---|---|---|---|
| 1 | 慢熊段 D 中位 ≥ +8 | 成立（0.7） | 「再入加快不伤慢熊效果」 |
| 2 | 长牛段代价减半 | 成立（0.5） | 「代价主要在再入迟」 |"""

FAMILY_TABLE_HEAD = (
    "| 家族 | 已试变体（筛选 / 正式） | 最好的验证结果 | 家族价 | 标签分布 | 状态 | 重开条件 | 线 |"
)

NAV = [
    ("now", "现状"),
    ("account", "保险账"),
    ("forward", "前瞻记录"),
    ("charter", "当期题"),
    ("families", "家族台账"),
    ("funnel", "漏斗"),
    ("screens", "信号筛选"),
    ("trials", "试跑"),
    ("questions", "研究问题"),
    ("board", "结论板"),
    ("route", "研究路线"),
    ("glossary", "名词解释"),
]

ORDER = [
    "now",
    "account",
    "forward",
    "charter",
    "families",
    "funnel",
    "screens",
    "trials",
    "questions",
    "board",
    "negative",
    "route",
    "quick",
    "judgment",
    "work",
    "glossary",
]


# Helpers -------------------------------------------------------------------------------------


def _write(repo: Path, relative: str, text: str) -> None:
    path = repo.joinpath(*relative.split("/"))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _name_screen(repo: Path, question_id: str, screen_id: str) -> None:
    path = repo.joinpath("research", question_id, "prereg.yaml")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    raw["screens"] = [screen_id]
    path.write_text(yaml.safe_dump(raw, sort_keys=False, allow_unicode=True), encoding="utf-8")


def _layout_repo(tmp_path: Path) -> Path:
    """The run-03 fixture plus the files the new blocks read."""

    repo = _repo(tmp_path)
    _write(repo, "research/_loop/forward_records.md", FORWARD_RECORDS)
    _write(repo, "research/_forward/weekly.md", WEEKLY)
    _write(repo, "research/families.md", FAMILIES)
    _write(repo, "research/_loop/charter_p1.md", CHARTER_P1)
    _write(repo, "research/_loop/charter_p2.md", CHARTER_P2)
    # QQQ-Z3 was opened from the screen T0001 tried (S0004, the signal "fast").
    _name_screen(repo, "QQQ-Z3", "S0004")
    # A candidate nobody has tried and no family row names: "plain" has a spec but no curated name.
    _screen(repo, "plain", "d", "candidate", 0.02)
    return repo


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    return _layout_repo(tmp_path)


@pytest.fixture
def bare(tmp_path: Path) -> Path:
    """The run-03 fixture alone: none of the files the new blocks read."""

    return _repo(tmp_path, "bare")


def _section_order(page: str) -> list[str]:
    found = [(page.index(f'id="{name}"'), name) for name in ORDER]
    return [name for _, name in sorted(found)]


# 1. Forward records ---------------------------------------------------------------------------


def test_forward_records_rows_come_from_every_numbered_table(repo: Path) -> None:
    records = collect(repo)["forward_records"]
    assert records["present"] is True
    assert records["markdown"] == FORWARD_RECORDS
    assert [section["title"] for section in records["sections"]] == ["慢熊型", "日报模型"]
    assert [
        (row["id"], row["written"], row["config"], row["expected"], row["check"], row["section"])
        for row in records["rows"]
    ] == [
        (
            "F0001",
            "2026-10-21",
            "fast_slow（QQQ-Q3）",
            "D > 0 的概率 0.8",
            "待核",
            "慢熊型",
        ),
        (
            "F0002",
            "2026-10-21",
            "trend4（QQQ-Q2）",
            "D > 0 的概率 0.8",
            "2026-10-30：对（D +3.1）",
            "慢熊型",
        ),
        (
            "F0005",
            "2026-10-22",
            "日报模型 <b>现在的版本</b>",
            "长牛段每年少 1 到 3 点（概率 0.6）",
            "待核",
            "日报模型",
        ),
    ]
    assert (records["count"], records["pending"]) == (3, 2)


def test_forward_record_sections_split_tables_from_prose(repo: Path) -> None:
    sections = collect(repo)["forward_records"]["sections"]
    first, second = sections
    assert len(first["tables"]) == 1 and first["tables"][0].startswith("| 编号 | 写下 | 配置 |")
    assert first["tables"][0].endswith("| 2026-10-30：对（D +3.1） |")
    assert first["prose"] == (
        "D 是策略在一个慢熊段里的段内收益减同平均敞口固定敞口的段内收益（点）。\n\n"
        "核对时机：下一个慢熊段完整以后。"
    )
    assert first["markdown"].startswith("D 是策略") and "| F0001 |" in first["markdown"]
    assert len(second["tables"]) == 2 and second["tables"][1].startswith("| 编号 | 备注 |")
    assert second["prose"] == "日报模型那条线由前瞻记录台每天记一行。"


def test_missing_forward_records_are_not_a_problem(bare: Path) -> None:
    model = collect(bare)
    assert model["forward_records"] == {
        "present": False,
        "markdown": None,
        "sections": [],
        "rows": [],
        "count": 0,
        "pending": 0,
    }
    assert model["problems"] == []


def test_forward_records_without_rows_are_a_problem(repo: Path) -> None:
    _write(repo, "research/_loop/forward_records.md", "# 前瞻记录\n\n还没有。\n")
    model = collect(repo)
    assert model["forward_records"]["present"] is True
    assert model["forward_records"]["rows"] == [] and model["forward_records"]["count"] == 0
    assert [p for p in model["problems"] if "research/_loop/forward_records.md" in p]


# 2. Forward ledger (weekly summary only) ------------------------------------------------------


def test_forward_ledger_reads_the_weekly_summary_not_the_ledger(repo: Path) -> None:
    repo.joinpath("research", "_forward", "ledger.jsonl").write_text(
        '{"date": "2026-10-12", "ret_fixed": -0.0123}\n', encoding="utf-8"
    )
    ledger = collect(repo)["forward_ledger"]
    assert ledger["present"] is True and ledger["markdown"] == WEEKLY
    assert ledger["lead"] == (
        "- 运行：TRADING-2565-45。三条线：日报模型、固定敞口 0.8、F0001。\n"
        "- 2026-10-12 到 2026-10-20 是回填，2026-10-21 起是前瞻。描述，不判。\n\n"
        "账本 7 行，2026-10-12..2026-10-20。"
    )
    assert [section["title"] for section in ledger["sections"]] == [
        "自 2026-10-12（含回填）",
        "自 2026-10-21（前瞻）",
        "按周",
    ]
    assert ledger["sections"][0]["markdown"].startswith("| 线 | 交易日 |")
    assert (ledger["days"], ledger["start"], ledger["end"]) == (7, "2026-10-12", "2026-10-20")
    page = render(collect(repo))
    assert "ret_fixed" not in page and "-0.0123" not in page


def test_forward_ledger_without_the_count_sentence(repo: Path) -> None:
    _write(
        repo,
        "research/_forward/weekly.md",
        WEEKLY.replace("账本 7 行，2026-10-12..2026-10-20。", ""),
    )
    ledger = collect(repo)["forward_ledger"]
    assert (ledger["days"], ledger["start"], ledger["end"]) == (None, None, None)
    assert collect(repo)["problems"] == []


def test_missing_forward_ledger_is_not_a_problem(bare: Path) -> None:
    model = collect(bare)
    assert model["forward_ledger"] == {
        "present": False,
        "markdown": None,
        "lead": "",
        "sections": [],
        "days": None,
        "start": None,
        "end": None,
    }
    assert model["problems"] == []


# 3. Families ----------------------------------------------------------------------------------


def test_families_table_rows_and_counter(repo: Path) -> None:
    families = collect(repo)["families"]
    assert families["present"] is True and families["markdown"] == FAMILIES
    assert families["counter"] == (
        "距下次横读：正式题 0/3、试跑失败 0/3、信号 0/20、前瞻核对 0/1，上次横读 2026-10-22。"
    )
    assert families["notes"] == (
        "- 家族价：只用有验证读取的变体。\n"
        "- 状态：有验证读取的变体至少 3 个、家族价低于 10%/点，为「关闭」。"
    )
    assert families["table"].startswith(FAMILY_TABLE_HEAD)
    assert families["table"].endswith("| 坏行 | 只有七格 | — | — | — | 开放 | — |")
    assert [(row["family"], row["status"]) for row in families["rows"]] == [
        ("经典趋势", "关闭"),
        ("波动", "停车"),
        ("利率", "开放"),
        ("卖波动", "停车"),
    ]
    assert families["rows"][1] == {
        "family": "波动",
        "variants": "筛选 1（vol 淘汰）/ 正式 1（q1a）",
        "best": "q1a：左尾 +7.2%",
        "price": "4.5%/点",
        "tags": "有效果但代价太高 1",
        "status": "停车",
        "reopen": "说明机制",
        "line": "S0003",
    }
    assert families["status_counts"] == {"开放": 1, "停车": 2, "关闭": 1}
    assert families["by_signal"] == {"vol": "波动", "slow": "利率", "fast": "利率"}


def test_families_without_the_table_are_a_problem(repo: Path) -> None:
    _write(
        repo,
        "research/families.md",
        "# 机制家族台账\n\n距下次横读：正式题 0/3。\n\n## 家族\n\n还没有表。\n",
    )
    model = collect(repo)
    families = model["families"]
    assert families["present"] is True
    assert families["counter"] == "距下次横读：正式题 0/3。"
    assert (families["table"], families["notes"], families["rows"]) == (None, "", [])
    assert families["status_counts"] == {"开放": 0, "停车": 0, "关闭": 0}
    assert families["by_signal"] == {}
    assert [p for p in model["problems"] if "research/families.md" in p]


def test_missing_families_are_not_a_problem(bare: Path) -> None:
    model = collect(bare)
    assert model["families"] == {
        "present": False,
        "markdown": None,
        "counter": None,
        "notes": "",
        "table": None,
        "rows": [],
        "status_counts": {"开放": 0, "停车": 0, "关闭": 0},
        "by_signal": {},
    }
    assert model["problems"] == []


# 4. The current charter -----------------------------------------------------------------------


def test_charter_is_the_latest_period(repo: Path) -> None:
    charter = collect(repo)["charter"]
    assert charter["present"] is True
    assert (charter["path"], charter["period"]) == ("research/_loop/charter_p2.md", 2)
    assert charter["markdown"] == CHARTER_P2
    assert charter["term"] == "这份发布到 main 那天起，到 2026-11-18。发布时第 1 期结束。"
    assert (
        charter["direction"]
        == "只做横读导出的两题：H-X 不对称再入，H-Y 条件化储备，H-X 先。不新筛信号。"
    )


def test_charter_questions_are_the_subsections_of_the_prereg_section(repo: Path) -> None:
    questions = collect(repo)["charter"]["questions"]
    assert [q["title"] for q in questions] == [
        "H-X 不对称再入（经典趋势，换实现）",
        "H-Y 条件化储备（P1，换实现）",
    ]
    first, second = questions
    assert first["rule"] == "规则：敞口底 1.0；退出照 fast_slow 月底判；再入改快。"
    assert first["table"] == EXPECTED_TABLE
    assert first["markdown"].startswith("规则（原文）：") and first["markdown"].endswith("8/10 |")
    assert "### 3.2" not in first["markdown"] and "## 4." not in first["markdown"]
    assert (second["rule"], second["table"]) == (None, None)
    assert second["markdown"] == "先写明：软泄漏可疑。没有引文，没有表。"


def test_charter_without_a_prereg_section(repo: Path) -> None:
    _write(
        repo, "research/_loop/charter_p3.md", "# 研究章程 第 3 期\n\n## 第 3 期的方向\n\n只记账。\n"
    )
    charter = collect(repo)["charter"]
    assert (charter["period"], charter["direction"], charter["term"]) == (3, "只记账。", None)
    assert charter["questions"] == []
    assert collect(repo)["problems"] == []


def test_missing_charter_is_not_a_problem(bare: Path) -> None:
    model = collect(bare)
    assert model["charter"] == {
        "present": False,
        "path": None,
        "period": None,
        "markdown": None,
        "term": None,
        "direction": None,
        "questions": [],
    }
    assert model["problems"] == []


# 5. Funnel and fates --------------------------------------------------------------------------


def test_funnel_counts_screens_trials_questions_and_forward_records(repo: Path) -> None:
    model = collect(repo)
    assert model["funnel"] == {
        "screened": 4,
        "candidates": 2,
        "trials": 1,
        "passed": 1,
        "questions": 1,
        "kept": 0,
        "forward": 3,
    }
    assert [(plan["trial_id"], plan["screens"]) for plan in model["trials"]["plans"]] == [
        ("T0001", ["S0004"]),
        ("T0002", []),
    ]
    assert {q["question_id"]: q["screens"] for q in model["questions"]} == {
        "QQQ-Z1": [],
        "QQQ-Z3": ["S0004"],
    }


def test_funnel_on_the_bare_fixture(bare: Path) -> None:
    assert collect(bare)["funnel"] == {
        "screened": 3,
        "candidates": 1,
        "trials": 1,
        "passed": 1,
        "questions": 0,
        "kept": 0,
        "forward": 0,
    }


def test_screen_fates(repo: Path) -> None:
    model = collect(repo)
    fates = model["screen_fates"]
    assert set(fates) == set(model["screen_details"]) == {"2", "3", "4", "5"}
    assert fates["4"] == {
        "screen_id": "S0004",
        "trials": [{"trial_id": "T0001", "verdict": "pass", "verdict_name": "过门"}],
        "questions": [{"question_id": "QQQ-Z3", "state": "进行中"}],
        "family": {"name": "利率", "status": "开放"},
        "text": "试跑 T0001：过门；转 QQQ-Z3（进行中）",
    }
    assert fates["2"] == {
        "screen_id": "S0002",
        "trials": [],
        "questions": [],
        "family": {"name": "利率", "status": "开放"},
        "text": "家族「利率」开放",
    }
    assert fates["3"] == {
        "screen_id": "S0003",
        "trials": [],
        "questions": [],
        "family": {"name": "波动", "status": "停车"},
        "text": "家族「波动」停车",
    }
    assert fates["5"] == {
        "screen_id": "S0005",
        "trials": [],
        "questions": [],
        "family": None,
        "text": "没试跑",
    }
    assert all(
        set(member) == {"signal_id", "number", "verdict", "primary_p_value", "records"}
        for member in model["screens"]["members"]
    )


def test_fate_text_of_a_closed_question_and_an_untried_candidate_with_a_family(repo: Path) -> None:
    repo.joinpath("research", "QQQ-Z3", "decision.md").write_text(
        "# 结论\n\n结论：保留\n", encoding="utf-8"
    )
    _write(repo, "research/families.md", FAMILIES.replace("slow 候选、fast 候选", "plain 候选"))
    model = collect(repo)
    fates = model["screen_fates"]
    assert fates["4"]["questions"] == [{"question_id": "QQQ-Z3", "state": "已结题"}]
    assert fates["4"]["text"] == "试跑 T0001：过门；转 QQQ-Z3（已结题）"
    assert fates["5"]["text"] == "没试跑；家族「利率」开放"
    assert fates["2"] == {
        "screen_id": "S0002",
        "trials": [],
        "questions": [],
        "family": None,
        "text": "—",
    }
    assert model["funnel"]["kept"] == 1


def test_model_keeps_the_old_keys_and_adds_six(repo: Path) -> None:
    model = collect(repo)
    assert {
        "forward_records",
        "forward_ledger",
        "families",
        "charter",
        "funnel",
        "screen_fates",
    } <= set(model)
    assert model["schema_version"] == "research_site.v1"
    assert model["problems"] == []


# 6. Markdown ----------------------------------------------------------------------------------


def test_markdown_blockquote() -> None:
    html = markdown_to_html("前言\n\n> 规则：`a` **强**\n> 第二行 <b>\n\n- 项\n")
    assert html == (
        "<p>前言</p>\n"
        "<blockquote><p>规则：<code>a</code> <strong>强</strong>\n第二行 &lt;b&gt;</p>"
        "</blockquote>\n"
        "<ul><li>项</li></ul>"
    )


# 7. The page ----------------------------------------------------------------------------------


def test_nav_and_section_order(repo: Path) -> None:
    page = render(collect(repo))
    nav = re.search(r'<nav class="nav">(.*?)</nav>', page, re.S)
    assert nav is not None
    assert re.findall(r'href="#([\w-]+)">([^<]+)<', nav.group(1)) == NAV
    assert _section_order(page) == ORDER
    targets = set(re.findall(r'\bid="([^"]+)"', page))
    links = re.findall(r'href="#([^"]+)"', page)
    assert links and all(link in targets for link in links)


def test_now_block(repo: Path) -> None:
    now = _block(render(collect(repo)), "now")
    assert "<h2>现状</h2>" in now
    assert (
        "<p><strong>结论：</strong>保留 0 · 拒绝 0 · 证据不足 1 · 未测成 0（按子问题计）；"
        "筛选 4 个信号 → 候选 2 → 试跑 1 → 过门 1 → 开题 1 → 保留 0。</p>"
    ) in now
    assert (
        "<p><strong>前瞻：</strong>前瞻记录 3 条，待核 2 条；"
        "账本 7 个交易日（2026-10-12 到 2026-10-20）；"
        "下一次核对要等 QQQ 划出一个完整的慢熊段或长牛段。</p>"
    ) in now
    assert (
        "<p><strong>这期：</strong>第 2 期章程：这份发布到 main 那天起，到 2026-11-18。"
        "只做横读导出的两题：H-X 不对称再入，H-Y 条件化储备，H-X 先。不新筛信号。</p>"
    ) in now


def test_now_block_without_the_new_files(bare: Path) -> None:
    now = _block(render(collect(bare)), "now")
    assert "筛选 3 个信号 → 候选 1 → 试跑 1 → 过门 1 → 开题 0 → 保留 0。" in now
    assert "<p><strong>前瞻：</strong>还没有前瞻记录。</p>" in now
    assert "<p><strong>这期：</strong>还没有章程。</p>" in now


def test_now_block_without_the_ledger_count(repo: Path) -> None:
    _write(
        repo,
        "research/_forward/weekly.md",
        WEEKLY.replace("账本 7 行，2026-10-12..2026-10-20。", ""),
    )
    now = _block(render(collect(repo)), "now")
    assert (
        "<p><strong>前瞻：</strong>前瞻记录 3 条，待核 2 条；"
        "下一次核对要等 QQQ 划出一个完整的慢熊段或长牛段。</p>"
    ) in now


def test_forward_ledger_block(repo: Path) -> None:
    block = _block(render(collect(repo)), "account")
    assert "<h2>保险账（前瞻账本）</h2>" in block
    assert (
        '<p class="lead">每个交易日记三条线的敞口和收益，和同平均敞口的固定敞口比，每周五追加；'
        "只描述，不判，核对要等完整的慢熊段或长牛段。</p>"
    ) in block
    assert "<li>运行：TRADING-2565-45。三条线：日报模型、固定敞口 0.8、F0001。</li>" in block
    assert "<p>账本 7 行，2026-10-12..2026-10-20。</p>" in block
    assert "<h3>自 2026-10-12（含回填）</h3>" in block and "<h3>自 2026-10-21（前瞻）</h3>" in block
    assert '<td data-label="累计">-3.59%</td>' in block
    assert '<details class="grp"><summary><strong>按周</strong></summary>' in block
    assert "<h3>按周</h3>" not in block
    assert block.index("含回填") < block.index("（前瞻）") < block.index("按周")


def test_forward_ledger_block_without_the_file(bare: Path) -> None:
    block = _block(render(collect(bare)), "account")
    assert "<p>还没有前瞻账本：research/_forward/weekly.md</p>" in block


def test_forward_records_block(repo: Path) -> None:
    block = _block(render(collect(repo)), "forward")
    assert "<h2>前瞻记录</h2>" in block
    assert (
        '<p class="lead">写在核对之前、写下以后不改的预期；到期核对时在「核对」列追加结果。'
        "不计数、不进验证、不进组合。</p>"
    ) in block
    assert "<p>共 3 条，待核 2 条。</p>" in block
    assert "<h3>慢熊型</h3>" in block and "<h3>日报模型</h3>" in block
    assert '<td data-label="编号">F0001</td>' in block
    assert '<td data-label="核对">2026-10-30：对（D +3.1）</td>' in block
    assert "日报模型 &lt;b&gt;现在的版本&lt;/b&gt;" in block and "<b>现在的版本</b>" not in block
    assert '<details class="grp"><summary>说明</summary><p>D 是策略' in block
    assert (
        block.index("<h3>慢熊型</h3>")
        < block.index("F0001")
        < block.index("<summary>说明</summary>")
    )


def test_forward_records_block_without_the_file(bare: Path) -> None:
    block = _block(render(collect(bare)), "forward")
    assert "<p>还没有前瞻记录：research/_loop/forward_records.md</p>" in block


def test_charter_block(repo: Path) -> None:
    page = render(collect(repo, repo_url="https://example.test/repo/"))
    block = _block(page, "charter")
    assert "<h2>当期题</h2>" in block
    assert (
        '<p class="lead">第 2 期章程预注册的题：规则、预期和判法都在跑之前写下，'
        "读完填实际和对错。</p>"
    ) in block
    assert "<h3>H-X 不对称再入（经典趋势，换实现）</h3>" in block
    assert (
        '<p class="verdict-rule"><strong>规则：</strong>规则：敞口底 1.0；'
        "退出照 fast_slow 月底判；再入改快。</p>"
    ) in block
    assert '<td data-label="失败淘汰什么">「代价主要在再入迟」</td>' in block
    assert "<details><summary>全文（预期、判法、失败淘汰什么）</summary>" in block
    assert "<blockquote><p>规则：敞口底 1.0" in block
    assert "<h3>H-Y 条件化储备（P1，换实现）</h3>" in block
    assert block.count('<p class="verdict-rule">') == 1
    assert block.count("<details><summary>全文") == 2
    assert 'href="' not in block  # tmp_path is not a git repository: no commit, so no link
    model = collect(repo, repo_url="https://example.test/repo/")
    if model["source"]["commit"]:
        assert (
            f'<p><a href="https://example.test/repo/blob/{model["source"]["commit"]}'
            '/research/_loop/charter_p2.md">charter_p2.md</a></p>'
        ) in block


def test_charter_block_without_questions_or_file(repo: Path, bare: Path) -> None:
    _write(
        repo, "research/_loop/charter_p3.md", "# 研究章程 第 3 期\n\n## 第 3 期的方向\n\n只记账。\n"
    )
    block = _block(render(collect(repo)), "charter")
    assert "<p>这期章程没有预注册的题。</p>" in block
    assert "<p>还没有章程：research/_loop/charter_pN.md</p>" in _block(
        render(collect(bare)), "charter"
    )


def test_families_block(repo: Path) -> None:
    block = _block(render(collect(repo)), "families")
    assert "<h2>家族台账</h2>" in block
    assert (
        '<p class="lead">按机制家族累计失败：家族价和状态按台账里写下的规则机械算出，'
        "关闭的家族再开题必须写新机制。</p>"
    ) in block
    assert (
        "<p>距下次横读：正式题 0/3、试跑失败 0/3、信号 0/20、前瞻核对 0/1，"
        "上次横读 2026-10-22。</p>" in block
    )
    assert "<p>开放 1 · 停车 2 · 关闭 1</p>" in block
    # Run 07: seven short columns; the reopen condition is a fold under the table, one per family.
    assert (
        "<thead><tr><th>家族</th><th>状态</th><th>家族价</th><th>已试变体（筛选 / 正式）</th>"
        "<th>最好的验证结果</th><th>标签分布</th><th>线</th></tr></thead>"
    ) in block
    table = block[block.index("<table") : block.index("</table>")]
    assert "重开条件" not in table and "新机制" not in table
    assert (
        '<td data-label="家族">经典趋势</td><td data-label="状态">关闭</td>'
        '<td data-label="家族价">2.4%/点</td>' in block
    )
    assert '<td data-label="线">S0002、S0004</td>' in block
    assert block.count("<tr>") == 1 + 4
    folds = re.findall(r"<summary>重开条件：([^<]*)</summary><p>([^<]*)</p>", block)
    assert folds == [
        ("经典趋势（关闭）", "新机制"),
        ("波动（停车）", "说明机制"),
        ("卖波动（停车）", "见清单"),
    ]
    assert block.index("</table>") < block.index("重开条件：经典趋势")
    assert '<details class="grp"><summary>家族价和状态怎么算</summary><ul><li>家族价：' in block
    assert block.index("重开条件：卖波动") < block.index("家族价和状态怎么算")
    assert "横读怎么计" not in block
    assert "失败标签" not in block


def test_families_counter_shows_its_first_sentence_and_folds_the_rest(repo: Path) -> None:
    _write(
        repo,
        "research/families.md",
        FAMILIES.replace(
            "上次横读 2026-10-22。",
            "上次横读 2026-10-22（`owner_decision:X:2026-10-22:y`）。结题的 Claude 更新这一行；"
            "第 4 期的两题是探针。",
        ),
    )
    block = _block(render(collect(repo)), "families")
    assert (
        "<p>距下次横读：正式题 0/3、试跑失败 0/3、信号 0/20、前瞻核对 0/1，上次横读 2026-10-22"
        "（<code>owner_decision:X:2026-10-22:y</code>）。</p>"
        '<details class="grp"><summary>横读怎么计</summary><p>结题的 Claude 更新这一行；'
        "第 4 期的两题是探针。</p></details><p>开放 1 · 停车 2 · 关闭 1</p>"
    ) in block


def test_forward_record_ids_and_dates_do_not_wrap(repo: Path) -> None:
    page = render(collect(repo))
    assert "\n#forward th{white-space:normal}\n" in page
    assert "\n#forward td:nth-child(-n+2),#forward td:last-child{white-space:nowrap}\n" in page


def test_families_block_without_the_file(bare: Path) -> None:
    assert "<p>还没有家族台账：research/families.md</p>" in _block(
        render(collect(bare)), "families"
    )


def test_funnel_block(repo: Path) -> None:
    block = _block(render(collect(repo)), "funnel")
    assert "<h2>漏斗</h2>" in block
    assert '<p class="lead">从筛选到可信的结论，每一格都读自仓库里的记录。</p>' in block
    cells = re.findall(
        r'<a href="#([\w-]+)"><span class="big">(\d+)</span><span class="m">([^<]+)</span></a>',
        block,
    )
    assert cells == [
        ("screens", "4", "已筛信号"),
        ("screens", "2", "候选"),
        ("trials", "1", "试跑"),
        ("trials", "1", "过门"),
        ("questions", "1", "开题"),
        ("board", "0", "保留"),
    ]
    assert block.count('<span class="fa">→</span>') == 5
    assert '<p class="m">另一条线：前瞻记录 3 条，不计数、只核对，见「前瞻记录」。</p>' in block


def test_screens_table_has_a_fate_column_and_candidates_first(repo: Path) -> None:
    section = _block(render(collect(repo)), "screens")
    assert "已筛 4 个信号（共 5 条记录）：淘汰 1 · 证据不足 1 · 候选 2" in section
    assert (
        '<details class="grp"><summary><strong>筛选标准</strong></summary><div class="criteria">'
        in section
    )
    assert "<h3>怎么筛、凭什么淘汰</h3>" in section
    headers = re.findall(r"<th>([^<]*)</th>", section)
    assert headers == ["信号", "裁决", "去向", "主市场 p（未校正）", "记录条数", "依据"]
    rows = section.split("<tr>")[2:]
    order = [re.search(r'<span class="m">(\w+)</span>', row).group(1) for row in rows]  # type: ignore[union-attr]
    assert order == ["fast", "plain", "slow", "vol"]
    fast = next(row for row in rows if ">fast<" in row)
    assert (
        '<td data-label="去向">试跑 <a href="#trial-T0001">T0001</a>：过门；'
        '转 <a href="#q-QQQ-Z3">QQQ-Z3</a>（进行中）</td>'
    ) in fast
    assert '<td data-label="去向">没试跑</td>' in next(row for row in rows if ">plain<" in row)
    assert '<td data-label="去向">家族「利率」开放</td>' in next(
        row for row in rows if ">slow<" in row
    )
    assert '<td data-label="去向">家族「波动」停车</td>' in next(
        row for row in rows if ">vol<" in row
    )


def test_fate_links_only_to_existing_targets(repo: Path) -> None:
    _name_screen(repo, "QQQ-Z1", "S0005")
    page = render(collect(repo))
    plain = next(row for row in _block(page, "screens").split("<tr>") if ">plain<" in row)
    assert '转 <a href="#q-QQQ-Z1">QQQ-Z1</a>（进行中）' in plain
    targets = set(re.findall(r'\bid="([^"]+)"', page))
    assert all(link in targets for link in re.findall(r'href="#([^"]+)"', page))


def test_trials_are_folded_with_the_gate(repo: Path) -> None:
    section = _block(render(collect(repo)), "trials")
    assert (
        '<details class="grp"><summary><strong>门槛</strong></summary>'
        '<div class="criteria"><h3>试跑门槛</h3>' in section
    )
    assert (
        '<details class="grp"><summary>T0001 · 快线（21 日） · '
        '<span class="b pass">过门</span></summary>'
        '<article class="card plan" id="trial-T0001">'
    ) in section
    assert (
        '<details class="grp"><summary>T0002 · 配置 mix · '
        '<span class="b miss">没过</span></summary>'
        '<article class="card plan" id="trial-T0002">'
    ) in section
    assert "累计试验 6 = 研究题的开发试验 3 + 试跑配置 3" in section


def test_questions_are_folded_and_only_active_ones_open(repo: Path) -> None:
    repo.joinpath("research", "QQQ-Z3", "decision.md").write_text(
        "# 结论\n\n结论：证据不足\n", encoding="utf-8"
    )
    section = _block(render(collect(repo)), "questions")
    assert (
        '<details class="grp" open><summary>QQQ-Z1 · 双信号趋势策略能让左尾更温和吗？ · '
        "进行中</summary>"
        '<article class="card" id="q-QQQ-Z1">'
    ) in section
    assert (
        '<details class="grp"><summary>QQQ-Z3 · '
        "QQQ-Z3: does the trend strategy have a milder left tail? · 已结题</summary>"
        '<article class="card" id="q-QQQ-Z3">'
    ) in section
    assert section.index('id="q-QQQ-Z1"') < section.index('id="q-QQQ-Z3"')
    assert "先看一句话问题和「怎么判」，再看策略怎么买卖、通过门槛和验证期结果" in section


def test_route_map_keeps_its_nodes_at_the_foot(repo: Path) -> None:
    page = render(collect(repo))
    route = _block(page, "route")
    for target in ("questions", "board", "negative", "screens", "quick", "judgment"):
        assert f'href="#{target}"' in route, target
    assert "J4 上线后启用" in route and "J3 上线后启用" in route
    assert page.index('id="board"') < page.index('id="route"') < page.index('id="quick"')


def test_page_has_no_scripts_or_external_resources(repo: Path) -> None:
    page = render(collect(repo))
    lowered = page.lower()
    for banned in ("<script", "<link", "<img", "<iframe", "@import", "url("):
        assert banned not in lowered, banned
    assert 'href="http' not in page


def test_page_is_deterministic(repo: Path) -> None:
    assert render(collect(repo)) == render(collect(repo))
    first = build_site(repo, repo.parent.joinpath("out1"))
    second = build_site(repo, repo.parent.joinpath("out2"))
    assert first.read_bytes() == second.read_bytes()


# 8. The real repository -----------------------------------------------------------------------


def test_real_repository_has_the_new_blocks() -> None:
    model = collect(PROJECT_ROOT)
    assert model["problems"] == []
    records, ledger, families, charter = (
        model["forward_records"],
        model["forward_ledger"],
        model["families"],
        model["charter"],
    )
    assert records["present"] and records["count"] >= 5 and records["rows"][0]["id"] == "F0001"
    assert ledger["present"] and ledger["days"] is not None and ledger["sections"]
    assert families["present"] and families["rows"] and families["counter"] is not None
    assert families["by_signal"] and set(families["status_counts"]) == {"开放", "停车", "关闭"}
    assert charter["present"] and charter["period"] is not None and charter["period"] >= 4
    assert charter["direction"] and charter["term"]
    # Period 6 only keeps the forward ledger: its charter has no prereg section (TRADING-2565-59).
    assert charter["questions"] or charter["period"] == 6
    funnel = model["funnel"]
    assert funnel["screened"] == model["screens"]["family_size"] >= 17
    assert funnel["candidates"] == model["screens"]["counts"]["candidate"]
    assert funnel["trials"] == sum(bool(plan["screens"]) for plan in model["trials"]["plans"])
    assert funnel["passed"] <= funnel["trials"]
    assert funnel["questions"] == sum(bool(q["screens"]) for q in model["questions"]) >= 2
    assert funnel["kept"] <= funnel["questions"] and funnel["forward"] == records["count"]
    assert set(model["screen_fates"]) == set(model["screen_details"])
    assert any("试跑 T" in fate["text"] for fate in model["screen_fates"].values())
    page = render(model)
    for heading in ("现状", "保险账（前瞻账本）", "前瞻记录", "当期题", "家族台账", "漏斗"):
        assert f"<h2>{heading}</h2>" in page, heading
    assert "<th>去向</th>" in page
