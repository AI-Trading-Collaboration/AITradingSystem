"""Acceptance tests for the research page's plain-Chinese rules (TRADING-2568, run 03).

Written before the implementation. The requirement is
docs/requirements/TRADING-2568_Research_Progress_Page.md, section 5.7. The wording asserted here is
the wording of the page: a change to a sentence is a change to these tests.
The fixture repository holds signal specs, two questions, a screening ledger, a trial gate with two
plans and the two settings files; it is built from scratch so the real settings can move.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as bench_ledger
from ai_trading_system.research_bench import screen_ledger
from ai_trading_system.research_bench import trial as bench_trial
from ai_trading_system.research_site import build_site, collect, render, zh

SIGNALS_DIR = ("src", "ai_trading_system", "research_screens", "signals")
DECISION = "owner_decision:TRADING-2568:2026-10-19:rules_fixture"
HASH_A, HASH_B = "a" * 64, "b" * 64

SIGNAL_SPECS = {
    "fast": ("快线：最近 21 日的收益。后面的话不读。", 1, "return"),
    "slow": ("慢线：最近 252 日的收益。", 1, "return"),
    "vol": ("波动：最近 21 日的波动，越高越危险。", 1, "risk"),
    "spread": ("利差：越低越好。", -1, "return"),
    "calm": ("平静：越高越稳。", -1, "risk"),
    "plain": ("最近 5 日的收益。再说一句。", 1, "return"),
}

SIGNAL_NAMES = """schema_version: research_signal_names.v1
signals:
  fast:
    name: 快线（21 日）
    meaning: >-
      最近 21 日的收益，
      单位是百分点
  slow:
    name: 慢线
  vol:
    name: 波动 <b>粗体</b>
"""

SCREEN_SETTINGS = """schema_version: research_screen.v1
derisk_events: {derisk_ratio: 0.75, merge_gap_days: 5, protective_loss: -0.05, false_alarm_gain: 0.05, short_event_days: 21}
sources:
  - {source_id: baa10y, provider: fred, dataset: BAA10Y, frequency: daily, known_lag_trading_days: 1, description: credit spread}
screen:
  screen_end: "2010-12-31"
  qqq_series: actual
  qqq_eval_start: "1999-03-10"
  long_eval_start: "1927-07-01"
  horizons: [21, 63, 126]
  groups: 5
  warmup_days: 252
  min_days: 252
  drawdown_threshold: -0.10
  bootstrap_markets: [qqq, us_market]
  bootstrap_draws: 2000
  confidence: 0.95
  seed: 7
  lookahead_cut_dates: 20
  bh_q: 0.10
  primary_market: us_market
  primary_horizon: 21
  candidate_breadth: 0.666667
  eliminate_breadth: 0.5
  copy_corr: 0.9
production_effect: none
broker_action: none
"""  # noqa: E501

BENCH_SETTINGS = """schema_version: research_bench.v1
cost_model: config/research/transaction_cost_model.yaml
window_policy: config/research/primary_research_window_policy.yaml
window_registry: config/research/research_window_registry.yaml
research_root: research
approval_ref: main
trading_days_per_year: 252
cvar_levels: [0.01, 0.05]
rolling_years: 3
rolling_percentile: 0.10
lookahead_cut_dates: 20
episodes:
  - {id: dotcom_bear, start: "2000-03", end: "2002-10"}
  - {id: odd_episode, start: "2015-08", end: "2016-02"}
falsification:
  cost_bps: [4, 15, 25]
  delay_days: [1, 2]
  placebo_block_days: 21
  placebo_draws: 1000
  bootstrap_mean_block_days: 21
  bootstrap_draws: 10000
  confidence: 0.95
  seed: 7
production_effect: none
broker_action: none
"""

TRIAL_GATE = """schema_version: research_trial_gate.v1
owner_decision: owner_decision:TRADING-2565:2026-10-19:fixture_gate
max_configs_per_trial: 8
confidence: 0.95
markets:
  qqq:
    question_type: qqq_direction
    qqq_series: actual
    rules:
      - {metric: ci_high.cvar_5pct_relative_improvement, op: ">", value: 0.0}
      - {metric: comparison.geometric_annual_return, op: ">=", value: -0.010}
  us_market:
    question_type: us_market_trial
    rules:
      - {metric: ci_low.cvar_5pct_relative_improvement, op: ">", value: 0.0}
      - {metric: comparison.max_drawdown, op: ">=", value: 0.05}
production_effect: none
broker_action: none
"""

PLAN_T0001 = """schema_version: research_trial_plan.v1
trial_id: T0001
candidate: {signals: [fast], screens: [S0004]}
hypothesis_card: card-fixture-fast
from_validation_window: false
data: {snapshot_id: qlh-fixture, signal_snapshot_id: sig-0123456789ab}
primary: primary
configs:
  - id: primary
    function: signal_exposure
    params:
      signals: [{id: fast, module_sha256: HASH_A, spec_sha256: HASH_B, score: percentile}]
      mapping: threshold
      low: 0.5
      high: 1.0
      cut: 0.3
    rebalance: {schedule: month_end, band: 0.0}
  - id: hold
    function: fixed_exposure
    params: {exposure: 0.6}
    rebalance: {schedule: month_end, band: 0.0}
production_effect: none
broker_action: none
""".replace("HASH_A", HASH_A).replace("HASH_B", HASH_B)

PLAN_T0002 = """schema_version: research_trial_plan.v1
trial_id: T0002
owner_decision: owner_decision:TRADING-2567:2026-10-18:portfolio_fixture
hypothesis_card: card-fixture-mix
from_validation_window: true
data: {snapshot_id: qlh-fixture, signal_snapshot_id: sig-0123456789ab}
primary: mix
configs:
  - id: mix
    function: fixed_exposure
    reserve: treasury_10y
    params: {exposure: 0.6}
    rebalance: {schedule: week_end, band: 0.1}
production_effect: none
broker_action: none
"""

SUMMARY_Z1 = "趋势策略 trend 按两个信号的方向定仓位，每月调一次。对照是平均仓位相同的固定仓位。"
LEDGER = """# 研究台账

## 总表

| # | 研究线 | 结论 | 评级 | 去向 |
|---|---|---|---|---|
| 1 | 夹具 | 证据不足 | C | 留作基准 |
"""

NEGATIVE = """# 负结果清单

## 已证伪

| 结论 | 评级 |
|---|---|
| 夹具 | B |
"""

ZH_Z1 = """schema_version: research_zh.v1
title: 双信号趋势策略能让左尾更温和吗？
summary: >-
  趋势策略 trend 按两个信号的方向定仓位，每月调一次。
  对照是平均仓位相同的固定仓位。
verdict: >-
  保留：所有门槛成立。
  拒绝：点估计更差 <b>。
strategies:
  trend:
    name: 趋势策略
    role: 正式策略
  gate:
    name: 波动门
  custom_y:
    name: 手写策略
    rule:
      - {label: 规则, text: "手写的 规则"}
      - {label: 调仓, text: 每月一次}
"""


# Helpers -------------------------------------------------------------------------------------


def _entry(**changes: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "id": "s",
        "function": "signal_exposure",
        "params": {},
        "rebalance": {"schedule": "month_end", "band": 0.0},
        "delay_days": 0,
    }
    entry.update(changes)
    return entry


def _ref(signal: str, score: str = "sign") -> dict[str, str]:
    return {"id": signal, "module_sha256": HASH_A, "spec_sha256": HASH_B, "score": score}


def _params(refs: list[dict[str, str]], mapping: str, **numbers: Any) -> dict[str, Any]:
    return {"signals": refs, "mapping": mapping, **numbers}


def _write_specs(repo: Path) -> None:
    directory = repo.joinpath(*SIGNALS_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    for signal, (mechanism, sign, target) in SIGNAL_SPECS.items():
        raw = {"mechanism": mechanism, "expected_sign": sign, "target": target}
        directory.joinpath(f"{signal}.yaml").write_text(
            yaml.safe_dump(raw, allow_unicode=True), encoding="utf-8"
        )


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


def _lines(card: dict[str, Any]) -> list[tuple[str, str]]:
    return [(line["label"], line["text"]) for line in card["lines"]]


def _levels(card: dict[str, Any]) -> list[tuple[str, float]] | None:
    if card["levels"] is None:
        return None
    return [(level["when"], level["exposure"]) for level in card["levels"]]


@pytest.fixture
def specs(tmp_path: Path) -> Path:
    repo = tmp_path.joinpath("specs")
    _write_specs(repo)
    return repo


# 1. Text utilities ---------------------------------------------------------------------------


def test_clean_folds_lines_and_drops_gaps_between_chinese_words() -> None:
    assert zh.clean("  每月\n  最后一个\n交易日  ") == "每月最后一个交易日"
    assert zh.clean("仓位\n 100%，比 QQQ\n多") == "仓位 100%，比 QQQ 多"
    assert zh.clean("左尾 10% 更好") == "左尾 10% 更好"


@pytest.mark.parametrize(
    ("value", "digits", "expected"),
    [
        (0, 1, "0"),
        (-0.04, 1, "0"),
        (12.0, 1, "12"),
        (0.25, 3, "0.25"),
        (0.666667, 3, "0.667"),
        (100, 0, "100"),
        (10, 0, "10"),
        (1234.5, 1, "1234.5"),
        (2.0, 2, "2"),
        (1.5, 2, "1.5"),
    ],
)
def test_number_has_no_trailing_zeros(value: float, digits: int, expected: str) -> None:
    assert zh.number(value, digits) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0.8, "80%"),
        (1 - 0.7, "30%"),
        (0.666667, "66.7%"),
        (1.0, "100%"),
        (0.05, "5%"),
        (0.955, "95.5%"),
        (0.0, "0%"),
        (-0.1, "-10%"),
    ],
)
def test_percent(value: float, expected: str) -> None:
    assert zh.percent(value) == expected


# 2. Rules in words ---------------------------------------------------------------------------

CVAR = "cvar_5pct_relative_improvement"
RULE_TEXTS = [
    (f"comparison.{CVAR}", ">=", 0.10, None, "左尾亏损比对照至少小 10%"),
    (f"comparison.{CVAR}", ">", 0.0, None, "左尾亏损比对照小"),
    (f"comparison.{CVAR}", ">=", -0.05, None, "左尾亏损比对照最多大 5%"),
    ("comparison.max_drawdown", ">=", 0.05, None, "最大回撤比对照至少浅 5 个点"),
    ("comparison.max_drawdown", ">", 0.0, None, "最大回撤比对照浅"),
    ("comparison.max_drawdown", ">=", 0.0, None, "最大回撤不比对照深"),
    ("comparison.max_drawdown", ">=", -0.02, None, "最大回撤比对照最多深 2 个点"),
    ("comparison.geometric_annual_return", ">=", -0.01, None, "几何年化比对照最多少 1 个点"),
    ("comparison.geometric_annual_return", ">=", 0.0, None, "几何年化不比对照低"),
    ("comparison.geometric_annual_return", ">", 0.0, None, "几何年化比对照高"),
    ("comparison.geometric_annual_return", ">=", 0.02, None, "几何年化比对照至少多 2 个点"),
    (
        "comparison.rolling_geometric_return_low",
        ">=",
        0.0,
        None,
        "滚动 3 年几何年化的低位不比对照低",
    ),
    (
        "comparison.rolling_geometric_return_low",
        ">",
        0.01,
        None,
        "滚动 3 年几何年化的低位比对照多超过 1 个点",
    ),
    (f"ci_low.{CVAR}", ">", 0.0, 0.95, "左尾改善的 95% 区间下限 > 0（排除「没有改善」）"),
    (f"ci_low.{CVAR}", ">", 0.0, None, "左尾改善的置信区间下限 > 0（排除「没有改善」）"),
    (f"ci_high.{CVAR}", ">", 0.0, 0.95, "左尾改善的 95% 区间上限 > 0（还不能说比对照差）"),
    (f"ci_high.{CVAR}", ">", 0.0, None, "左尾改善的置信区间上限 > 0（还不能说比对照差）"),
    (
        "ci_low.max_drawdown",
        ">",
        0.0,
        0.975,
        "最大回撤改善的 97.5% 区间下限 > 0（排除「没有改善」）",
    ),
    ("ci_low.max_drawdown", ">=", 0.01, 0.95, "95% 区间的下限：最大回撤比对照至少浅 1 个点"),
    ("ci_low.max_drawdown", ">=", 0.01, None, "置信区间的下限：最大回撤比对照至少浅 1 个点"),
    (
        "cost_25.geometric_annual_return",
        ">=",
        -0.01,
        None,
        "单边成本 25 bps 下：几何年化比对照最多少 1 个点",
    ),
    (f"cost_4.{CVAR}", ">=", 0.1, None, "单边成本 4 bps 下：左尾亏损比对照至少小 10%"),
    (f"delay_2.{CVAR}", ">=", 0.1, None, "晚 2 天执行：左尾亏损比对照至少小 10%"),
    (
        "leave_year_min.max_drawdown",
        ">",
        0.0,
        None,
        "任意去掉一年重算，最差的一次仍然：最大回撤比对照浅",
    ),
    (
        f"leave_episode_min.{CVAR}",
        ">",
        0.0,
        None,
        "任意去掉一个固定行情段重算，最差的一次仍然：左尾亏损比对照小",
    ),
    (f"placebo_rank.{CVAR}", ">=", 0.95, None, "左尾改善胜过 95% 的随机择时（同平均仓位）"),
    (f"placebo_rank.{CVAR}", "<", 0.5, None, "左尾的随机择时排名 < 0.5"),
    ("comparison.max_drawdown", "<=", 0.0, None, "最大回撤相对对照 ≤ 0 个点"),
    (f"comparison.{CVAR}", "<", 0.1, None, "左尾相对对照 < 10%"),
    ("comparison.sharpe_vs_cash", ">=", 0.5, None, "comparison.sharpe_vs_cash ≥ 0.5"),
    ("mystery", ">=", 1, None, "mystery ≥ 1"),
    ("weird_section.max_drawdown", ">=", 0.05, None, "weird_section.max_drawdown ≥ 0.05"),
]


@pytest.mark.parametrize(("metric", "op", "value", "confidence", "expected"), RULE_TEXTS)
def test_rule_text(
    metric: str, op: str, value: float, confidence: float | None, expected: str
) -> None:
    assert zh.rule_text(metric, op, value, confidence) == expected


@pytest.mark.parametrize(
    ("metric", "observed", "expected"),
    [
        ("comparison.max_drawdown", 0.081, "+8.1 点"),
        ("comparison.max_drawdown", -0.2, "-20.0 点"),
        (f"comparison.{CVAR}", 0.2481, "+24.8%"),
        (f"comparison.{CVAR}", -0.05, "-5.0%"),
        (f"placebo_rank.{CVAR}", 0.973, "胜过 97.3%"),
        ("comparison.sharpe_vs_cash", 0.4123456, "0.4123"),
        ("comparison.max_drawdown", None, "—"),
    ],
)
def test_observed_text(metric: str, observed: float | None, expected: str) -> None:
    assert zh.observed_text(metric, observed) == expected


def test_rule_rows_keep_the_record_and_add_the_words() -> None:
    rows = zh.rule_rows(
        [
            {"metric": "comparison.max_drawdown", "op": ">=", "value": 0.05, "note": "english"},
            {
                "metric": f"ci_low.{CVAR}",
                "op": ">",
                "value": 0.0,
                "observed": -0.02,
                "holds": False,
            },
        ],
        0.95,
    )
    assert rows == [
        {
            "metric": "comparison.max_drawdown",
            "op": ">=",
            "value": 0.05,
            "text": "最大回撤比对照至少浅 5 个点",
        },
        {
            "metric": f"ci_low.{CVAR}",
            "op": ">",
            "value": 0.0,
            "text": "左尾改善的 95% 区间下限 > 0（排除「没有改善」）",
            "observed": -0.02,
            "observed_text": "-2.0%",
            "holds": False,
        },
    ]


# 3. The two curated text files ---------------------------------------------------------------


def test_signal_names_are_read_and_cleaned(tmp_path: Path) -> None:
    tmp_path.joinpath("signal_names.yaml").write_text(SIGNAL_NAMES, encoding="utf-8")
    problems: list[str] = []
    assert zh.load_signal_names(tmp_path, problems) == {
        "fast": {"name": "快线（21 日）", "meaning": "最近 21 日的收益，单位是百分点"},
        "slow": {"name": "慢线", "meaning": None},
        "vol": {"name": "波动 <b>粗体</b>", "meaning": None},
    }
    assert problems == []


def test_missing_signal_names_are_not_a_problem(tmp_path: Path) -> None:
    problems: list[str] = []
    assert zh.load_signal_names(tmp_path, problems) == {}
    assert problems == []


@pytest.mark.parametrize(
    "content",
    [
        "schema_version: wrong\nsignals: {}\n",
        "schema_version: research_signal_names.v1\nsignals: [a]\n",
        "schema_version: research_signal_names.v1\nsignals:\n  fast: {meaning: x}\n",
        "schema_version: research_signal_names.v1\nsignals:\n  fast: {name: 快线, color: red}\n",
        "schema_version: research_signal_names.v1\nsignals:\n  fast: {name: 3}\n",
        "- just\n- a list\n",
        "signals: [unclosed\n",
    ],
)
def test_broken_signal_names_become_one_problem(tmp_path: Path, content: str) -> None:
    tmp_path.joinpath("signal_names.yaml").write_text(content, encoding="utf-8")
    problems: list[str] = []
    assert zh.load_signal_names(tmp_path, problems) == {}
    assert len(problems) == 1 and "signal_names.yaml" in problems[0]


def test_question_text_is_read_and_cleaned(tmp_path: Path) -> None:
    tmp_path.joinpath("zh.yaml").write_text(ZH_Z1, encoding="utf-8")
    problems: list[str] = []
    text = zh.load_question_text(tmp_path, ["trend", "gate", "custom_y"], problems)
    assert problems == []
    assert text == {
        "title": "双信号趋势策略能让左尾更温和吗？",
        "summary": SUMMARY_Z1,
        "verdict": "保留：所有门槛成立。拒绝：点估计更差 <b>。",
        "strategies": {
            "trend": {"name": "趋势策略", "role": "正式策略", "rule": None},
            "gate": {"name": "波动门", "role": None, "rule": None},
            "custom_y": {
                "name": "手写策略",
                "role": None,
                "rule": [
                    {"label": "规则", "text": "手写的规则"},
                    {"label": "调仓", "text": "每月一次"},
                ],
            },
        },
    }


def test_question_text_without_file_or_optional_parts(tmp_path: Path) -> None:
    problems: list[str] = []
    assert zh.load_question_text(tmp_path, ["a"], problems) is None
    assert problems == []
    tmp_path.joinpath("zh.yaml").write_text(
        "schema_version: research_zh.v1\ntitle: 题\nsummary: 说明\n", encoding="utf-8"
    )
    assert zh.load_question_text(tmp_path, ["a"], problems) == {
        "title": "题",
        "summary": "说明",
        "verdict": None,
        "strategies": {},
    }
    assert problems == []


@pytest.mark.parametrize(
    "content",
    [
        "schema_version: research_zh.v0\ntitle: 题\nsummary: 说明\n",
        "schema_version: research_zh.v1\nsummary: 说明\n",
        "schema_version: research_zh.v1\ntitle: 题\nsummary: ''\n",
        "schema_version: research_zh.v1\ntitle: 题\nsummary: 说明\nextra: 1\n",
        "schema_version: research_zh.v1\ntitle: 题\nsummary: 说明\nstrategies:\n  zz: {name: 甲}\n",
        "schema_version: research_zh.v1\ntitle: 题\nsummary: 说明\nstrategies:\n  a: {role: 甲}\n",
        "schema_version: research_zh.v1\ntitle: 题\nsummary: 说明\nstrategies:\n"
        "  a: {name: 甲, x: 1}\n",
        "schema_version: research_zh.v1\ntitle: 题\nsummary: 说明\nstrategies:\n"
        "  a: {name: 甲, rule: [{label: 规则}]}\n",
        "schema_version: research_zh.v1\ntitle: 题\nsummary: 说明\nstrategies:\n"
        "  a: {name: 甲, rule: []}\n",
        "title: [unclosed\n",
    ],
)
def test_broken_question_text_becomes_one_problem(tmp_path: Path, content: str) -> None:
    tmp_path.joinpath("zh.yaml").write_text(content, encoding="utf-8")
    problems: list[str] = []
    assert zh.load_question_text(tmp_path, ["a"], problems) is None
    assert len(problems) == 1 and "zh.yaml" in problems[0]


# 4. Signals ----------------------------------------------------------------------------------


def test_signal_view_prefers_the_curated_name_and_meaning(specs: Path) -> None:
    names = {"fast": {"name": "快线（21 日）", "meaning": "最近 21 日的收益，单位是百分点"}}
    assert zh.signal_view(specs, "fast", names) == {
        "id": "fast",
        "name": "快线（21 日）",
        "meaning": "最近 21 日的收益，单位是百分点",
        "favourable": 1,
        "expectation": "信号越高，之后收益越高",
    }


@pytest.mark.parametrize(
    ("signal", "name", "meaning", "favourable", "expectation"),
    [
        ("slow", "慢线", "最近 252 日的收益", 1, "信号越高，之后收益越高"),
        ("vol", "波动", "最近 21 日的波动，越高越危险", -1, "信号越高，之后波动越大"),
        ("spread", "利差", "越低越好", -1, "信号越高，之后收益越低"),
        ("calm", "平静", "越高越稳", 1, "信号越高，之后波动越小"),
        ("plain", "plain", "最近 5 日的收益", 1, "信号越高，之后收益越高"),
    ],
)
def test_signal_view_falls_back_on_the_spec(
    specs: Path, signal: str, name: str, meaning: str, favourable: int, expectation: str
) -> None:
    assert zh.signal_view(specs, signal, {}) == {
        "id": signal,
        "name": name,
        "meaning": meaning,
        "favourable": favourable,
        "expectation": expectation,
    }


def test_signal_view_without_a_spec_says_nothing_it_does_not_know(specs: Path) -> None:
    assert zh.signal_view(specs, "nope", {}) == {
        "id": "nope",
        "name": "nope",
        "meaning": None,
        "favourable": None,
        "expectation": None,
    }


# 5. Strategy cards ---------------------------------------------------------------------------

CARD_CASES: dict[str, tuple[dict[str, Any], int | None]] = {
    "two_sign_linear": (
        _entry(params=_params([_ref("fast"), _ref("slow")], "linear", low=0, high=1)),
        None,
    ),
    "one_sign_threshold": (
        _entry(params=_params([_ref("slow")], "threshold", low=0, high=1)),
        None,
    ),
    "one_sign_threshold_neg": (
        _entry(params=_params([_ref("spread")], "threshold", low=0.25, high=1)),
        None,
    ),
    "pct_threshold_fav_neg": (
        _entry(
            params=_params([_ref("vol", "percentile")], "threshold", low=0.5, high=1.0, cut=0.2)
        ),
        None,
    ),
    "pct_threshold_fav_neg_warmup": (
        _entry(
            params=_params([_ref("vol", "percentile")], "threshold", low=0.5, high=1.0, cut=0.2)
        ),
        252,
    ),
    "pct_threshold_fav_pos": (
        _entry(params=_params([_ref("fast", "percentile")], "threshold", low=0, high=1.0, cut=0.7)),
        None,
    ),
    "pct_linear_plimits": (
        _entry(
            params=_params(
                [_ref("vol", "percentile")], "linear", low=0.5, high=1.5, p_low=0.2, p_high=0.8
            )
        ),
        None,
    ),
    "two_mixed_linear": (
        _entry(params=_params([_ref("fast"), _ref("vol", "percentile")], "linear", low=0, high=1)),
        None,
    ),
    "two_pct_linear": (
        _entry(
            params=_params(
                [_ref("fast", "percentile"), _ref("spread", "percentile")], "linear", low=0, high=1
            )
        ),
        None,
    ),
    "steps": (
        _entry(
            params=_params(
                [_ref("fast", "percentile")], "steps", edges=[0.33, 0.66], levels=[0.0, 0.5, 1.0]
            )
        ),
        None,
    ),
    "weekly_band_delay": (
        _entry(
            params=_params([_ref("slow")], "threshold", low=0, high=1),
            rebalance={"schedule": "week_end", "band": 0.2},
            delay_days=2,
        ),
        None,
    ),
    "daily": (
        _entry(
            params=_params([_ref("slow")], "threshold", low=0, high=1),
            rebalance={"schedule": "daily", "band": 0.0},
        ),
        None,
    ),
    "leveraged": (_entry(params=_params([_ref("slow")], "threshold", low=1, high=2)), None),
    "three_sign": (
        _entry(
            params=_params([_ref("fast"), _ref("slow"), _ref("spread")], "linear", low=0.25, high=1)
        ),
        None,
    ),
    "missing_spec": (_entry(params=_params([_ref("nope")], "threshold", low=0, high=1)), None),
    "fixed_cash": (_entry(function="fixed_exposure", params={"exposure": 0.6}), None),
    "fixed_t10": (
        _entry(
            function="fixed_exposure",
            params={"exposure": 0.6},
            reserve="treasury_10y",
            rebalance={"schedule": "week_end", "band": 0.1},
        ),
        None,
    ),
    "custom": (_entry(function="vol_exposure", params={"window": 21}), None),
}

CardExpected = tuple[str, str, list[tuple[str, str]], list[tuple[str, float]] | None]
EXPECTED_CARDS: dict[str, CardExpected] = {
    "custom": (
        "custom",
        "unavailable",
        [
            ("规则", "由代码里的 vol_exposure 定义，页面暂时没有中文说明"),
        ],
        None,
    ),
    "daily": (
        "signal_exposure",
        "generated",
        [
            ("信号", "「慢线」"),
            ("打分", "信号为正记 1 分，为负记 0 分（正好为 0 记 0.5 分）"),
            ("仓位", "信号为正（正好为 0 也算）时仓位 100%；信号为负时仓位 0%"),
            (
                "买入（加仓）",
                "信号为正（正好为 0 也算）、而仓位还不到 100% 时，调仓日买入 QQQ，补到 100%",
            ),
            ("卖出（减仓）", "信号为负、而仓位还高于 0% 时，调仓日卖出 QQQ 换成现金，降到 0%"),
            ("调仓", "每个交易日收盘后算目标仓位，下一个交易日收盘成交"),
            (
                "其他",
                (
                    "不加杠杆，仓位在 0% 到 100% 之间，其余是现金；"
                    "信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
                ),
            ),
        ],
        [
            ("信号为负", 0.0),
            ("信号为正（正好为 0 也算）", 1.0),
        ],
    ),
    "fixed_cash": (
        "fixed_exposure",
        "generated",
        [
            ("仓位", "固定持有 60% 的 QQQ，其余 40% 是现金；不看任何信号"),
            ("调仓", "每月最后一个交易日把仓位调回目标比例"),
        ],
        None,
    ),
    "fixed_t10": (
        "fixed_exposure",
        "generated",
        [
            (
                "仓位",
                (
                    "固定持有 60% 的 QQQ，其余 40% 是 10 年期国债（用 FRED DGS10 合成）；"
                    "不看任何信号"
                ),
            ),
            (
                "调仓",
                (
                    "每周最后一个交易日把仓位调回目标比例；"
                    "只有当前仓位偏离目标超过目标的 10% 时才真的交易"
                ),
            ),
        ],
        None,
    ),
    "leveraged": (
        "signal_exposure",
        "generated",
        [
            ("信号", "「慢线」"),
            ("打分", "信号为正记 1 分，为负记 0 分（正好为 0 记 0.5 分）"),
            ("仓位", "信号为正（正好为 0 也算）时仓位 200%；信号为负时仓位 100%"),
            (
                "买入（加仓）",
                "信号为正（正好为 0 也算）、而仓位还不到 200% 时，调仓日买入 QQQ，补到 200%",
            ),
            ("卖出（减仓）", "信号为负、而仓位还高于 100% 时，调仓日卖出 QQQ 换成现金，降到 100%"),
            ("调仓", "每月最后一个交易日收盘后算目标仓位，下一个交易日收盘成交；其余日子不动"),
            (
                "其他",
                (
                    "仓位最高 2 倍（用 2 倍、3 倍杠杆产品凑出来）；"
                    "信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
                ),
            ),
        ],
        [
            ("信号为负", 1.0),
            ("信号为正（正好为 0 也算）", 2.0),
        ],
    ),
    "missing_spec": (
        "signal_exposure",
        "unavailable",
        [
            ("信号", "「nope」"),
            ("规则", "读不到 nope 的信号规格，不知道哪个方向有利，页面写不出买卖规则"),
        ],
        None,
    ),
    "one_sign_threshold": (
        "signal_exposure",
        "generated",
        [
            ("信号", "「慢线」"),
            ("打分", "信号为正记 1 分，为负记 0 分（正好为 0 记 0.5 分）"),
            ("仓位", "信号为正（正好为 0 也算）时仓位 100%；信号为负时仓位 0%"),
            (
                "买入（加仓）",
                "信号为正（正好为 0 也算）、而仓位还不到 100% 时，调仓日买入 QQQ，补到 100%",
            ),
            ("卖出（减仓）", "信号为负、而仓位还高于 0% 时，调仓日卖出 QQQ 换成现金，降到 0%"),
            ("调仓", "每月最后一个交易日收盘后算目标仓位，下一个交易日收盘成交；其余日子不动"),
            (
                "其他",
                (
                    "不加杠杆，仓位在 0% 到 100% 之间，其余是现金；"
                    "信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
                ),
            ),
        ],
        [
            ("信号为负", 0.0),
            ("信号为正（正好为 0 也算）", 1.0),
        ],
    ),
    "one_sign_threshold_neg": (
        "signal_exposure",
        "generated",
        [
            ("信号", "「利差」"),
            ("打分", "信号为负记 1 分，为正记 0 分（正好为 0 记 0.5 分）"),
            ("仓位", "信号为负（正好为 0 也算）时仓位 100%；信号为正时仓位 25%"),
            (
                "买入（加仓）",
                "信号为负（正好为 0 也算）、而仓位还不到 100% 时，调仓日买入 QQQ，补到 100%",
            ),
            ("卖出（减仓）", "信号为正、而仓位还高于 25% 时，调仓日卖出 QQQ 换成现金，降到 25%"),
            ("调仓", "每月最后一个交易日收盘后算目标仓位，下一个交易日收盘成交；其余日子不动"),
            (
                "其他",
                (
                    "不加杠杆，仓位在 25% 到 100% 之间，其余是现金；"
                    "信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
                ),
            ),
        ],
        [
            ("信号为正", 0.25),
            ("信号为负（正好为 0 也算）", 1.0),
        ],
    ),
    "pct_linear_plimits": (
        "signal_exposure",
        "generated",
        [
            ("信号", "「波动」"),
            (
                "打分",
                (
                    "看信号在自己历史（到当天为止）里的位置：处在最低位记 1 分，"
                    "最高位记 0 分，中间按百分位"
                ),
            ),
            ("仓位", "随分数线性变化：分数 0.2 时仓位 50%，分数 0.8 时仓位 150%，中间按比例"),
            ("买入（加仓）", "信号走低（分数升高）时按比例加仓，最多到 150%，买入 QQQ"),
            ("卖出（减仓）", "信号走高（分数降低）时按比例减仓，最低到 50%，卖出 QQQ 换成现金"),
            ("调仓", "每月最后一个交易日收盘后算目标仓位，下一个交易日收盘成交；其余日子不动"),
            (
                "其他",
                (
                    "仓位最高 1.5 倍（用 2 倍、3 倍杠杆产品凑出来）；"
                    "信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
                ),
            ),
        ],
        [],
    ),
    "pct_threshold_fav_neg": (
        "signal_exposure",
        "generated",
        [
            ("信号", "「波动」"),
            (
                "打分",
                (
                    "看信号在自己历史（到当天为止）里的位置：处在最低位记 1 分，"
                    "最高位记 0 分，中间按百分位"
                ),
            ),
            (
                "仓位",
                (
                    "信号在自己历史的 80% 分位以下（分数 ≥ 0.2）时仓位 100%；"
                    "信号在自己历史的 80% 分位以上（分数 < 0.2）时仓位 50%"
                ),
            ),
            (
                "买入（加仓）",
                (
                    "信号在自己历史的 80% 分位以下（分数 ≥ 0.2）、而仓位还不到 100% 时，"
                    "调仓日买入 QQQ，补到 100%"
                ),
            ),
            (
                "卖出（减仓）",
                (
                    "信号在自己历史的 80% 分位以上（分数 < 0.2）、而仓位还高于 50% 时，"
                    "调仓日卖出 QQQ 换成现金，降到 50%"
                ),
            ),
            ("调仓", "每月最后一个交易日收盘后算目标仓位，下一个交易日收盘成交；其余日子不动"),
            (
                "其他",
                (
                    "不加杠杆，仓位在 50% 到 100% 之间，其余是现金；"
                    "信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
                ),
            ),
        ],
        [
            ("信号在自己历史的 80% 分位以上（分数 < 0.2）", 0.5),
            ("信号在自己历史的 80% 分位以下（分数 ≥ 0.2）", 1.0),
        ],
    ),
    "pct_threshold_fav_neg_warmup": (
        "signal_exposure",
        "generated",
        [
            ("信号", "「波动」"),
            (
                "打分",
                (
                    "看信号在自己历史（到当天为止）里的位置：处在最低位记 1 分，"
                    "最高位记 0 分，中间按百分位（信号要先有 252 个有值的交易日做历史，"
                    "之后才开始打分）"
                ),
            ),
            (
                "仓位",
                (
                    "信号在自己历史的 80% 分位以下（分数 ≥ 0.2）时仓位 100%；"
                    "信号在自己历史的 80% 分位以上（分数 < 0.2）时仓位 50%"
                ),
            ),
            (
                "买入（加仓）",
                (
                    "信号在自己历史的 80% 分位以下（分数 ≥ 0.2）、而仓位还不到 100% 时，"
                    "调仓日买入 QQQ，补到 100%"
                ),
            ),
            (
                "卖出（减仓）",
                (
                    "信号在自己历史的 80% 分位以上（分数 < 0.2）、而仓位还高于 50% 时，"
                    "调仓日卖出 QQQ 换成现金，降到 50%"
                ),
            ),
            ("调仓", "每月最后一个交易日收盘后算目标仓位，下一个交易日收盘成交；其余日子不动"),
            (
                "其他",
                (
                    "不加杠杆，仓位在 50% 到 100% 之间，其余是现金；"
                    "信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
                ),
            ),
        ],
        [
            ("信号在自己历史的 80% 分位以上（分数 < 0.2）", 0.5),
            ("信号在自己历史的 80% 分位以下（分数 ≥ 0.2）", 1.0),
        ],
    ),
    "pct_threshold_fav_pos": (
        "signal_exposure",
        "generated",
        [
            ("信号", "「快线」"),
            (
                "打分",
                (
                    "看信号在自己历史（到当天为止）里的位置：处在最高位记 1 分，"
                    "最低位记 0 分，中间按百分位"
                ),
            ),
            (
                "仓位",
                (
                    "信号在自己历史的 70% 分位以上（分数 ≥ 0.7）时仓位 100%；"
                    "信号在自己历史的 70% 分位以下（分数 < 0.7）时仓位 0%"
                ),
            ),
            (
                "买入（加仓）",
                (
                    "信号在自己历史的 70% 分位以上（分数 ≥ 0.7）、而仓位还不到 100% 时，"
                    "调仓日买入 QQQ，补到 100%"
                ),
            ),
            (
                "卖出（减仓）",
                (
                    "信号在自己历史的 70% 分位以下（分数 < 0.7）、而仓位还高于 0% 时，"
                    "调仓日卖出 QQQ 换成现金，降到 0%"
                ),
            ),
            ("调仓", "每月最后一个交易日收盘后算目标仓位，下一个交易日收盘成交；其余日子不动"),
            (
                "其他",
                (
                    "不加杠杆，仓位在 0% 到 100% 之间，其余是现金；"
                    "信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
                ),
            ),
        ],
        [
            ("信号在自己历史的 70% 分位以下（分数 < 0.7）", 0.0),
            ("信号在自己历史的 70% 分位以上（分数 ≥ 0.7）", 1.0),
        ],
    ),
    "steps": (
        "signal_exposure",
        "generated",
        [
            ("信号", "「快线」"),
            (
                "打分",
                (
                    "看信号在自己历史（到当天为止）里的位置：处在最高位记 1 分，"
                    "最低位记 0 分，中间按百分位"
                ),
            ),
            (
                "仓位",
                ("分数 [-∞, 0.33) 仓位 0%；分数 [0.33, 0.66) 仓位 50%；分数 [0.66, +∞) 仓位 100%"),
            ),
            (
                "买入（加仓）",
                "分数越过分界线 0.33、0.66 向上时，仓位升到下一档，最高 100%，买入 QQQ",
            ),
            (
                "卖出（减仓）",
                "分数越过同样的分界线向下时，仓位降到下一档，最低 0%，卖出 QQQ 换成现金",
            ),
            ("调仓", "每月最后一个交易日收盘后算目标仓位，下一个交易日收盘成交；其余日子不动"),
            (
                "其他",
                (
                    "不加杠杆，仓位在 0% 到 100% 之间，其余是现金；"
                    "信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
                ),
            ),
        ],
        [
            ("分数 [-∞, 0.33)", 0.0),
            ("分数 [0.33, 0.66)", 0.5),
            ("分数 [0.66, +∞)", 1.0),
        ],
    ),
    "three_sign": (
        "signal_exposure",
        "generated",
        [
            ("信号", "「快线」、「慢线」、「利差」"),
            (
                "打分",
                (
                    "「快线」为正记 1 分，为负记 0 分；「慢线」为正记 1 分，"
                    "为负记 0 分；「利差」为负记 1 分，为正记 0 分；"
                    "3 个信号的分数取平均"
                ),
            ),
            ("仓位", "按有利信号的个数定仓位，见下表"),
            ("买入（加仓）", "有利的信号变多时，仓位按下表上调，最高 100%，买入 QQQ"),
            ("卖出（减仓）", "有利的信号变少时，仓位按下表下调，最低 25%，卖出 QQQ 换成现金"),
            ("调仓", "每月最后一个交易日收盘后算目标仓位，下一个交易日收盘成交；其余日子不动"),
            (
                "其他",
                (
                    "不加杠杆，仓位在 25% 到 100% 之间，其余是现金；"
                    "信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
                ),
            ),
        ],
        [
            ("3/3 个信号有利", 1.0),
            ("2/3 个信号有利", 0.75),
            ("1/3 个信号有利", 0.5),
            ("0/3 个信号有利", 0.25),
        ],
    ),
    "two_mixed_linear": (
        "signal_exposure",
        "generated",
        [
            ("信号", "「快线」、「波动」"),
            (
                "打分",
                (
                    "「快线」为正记 1 分，为负记 0 分；「波动」在自己历史里最低位记 1 分，"
                    "最高位记 0 分；2 个信号的分数取平均"
                ),
            ),
            ("仓位", "随分数线性变化：分数 0 时仓位 0%，分数 1 时仓位 100%，中间按比例"),
            ("买入（加仓）", "分数升高时按比例加仓，最多到 100%，买入 QQQ"),
            ("卖出（减仓）", "分数降低时按比例减仓，最低到 0%，卖出 QQQ 换成现金"),
            ("调仓", "每月最后一个交易日收盘后算目标仓位，下一个交易日收盘成交；其余日子不动"),
            (
                "其他",
                (
                    "不加杠杆，仓位在 0% 到 100% 之间，其余是现金；"
                    "信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
                ),
            ),
        ],
        [],
    ),
    "two_pct_linear": (
        "signal_exposure",
        "generated",
        [
            ("信号", "「快线」、「利差」"),
            (
                "打分",
                (
                    "「快线」在自己历史里最高位记 1 分，最低位记 0 分；"
                    "「利差」在自己历史里最低位记 1 分，最高位记 0 分；"
                    "2 个信号的分数取平均"
                ),
            ),
            ("仓位", "随分数线性变化：分数 0 时仓位 0%，分数 1 时仓位 100%，中间按比例"),
            ("买入（加仓）", "分数升高时按比例加仓，最多到 100%，买入 QQQ"),
            ("卖出（减仓）", "分数降低时按比例减仓，最低到 0%，卖出 QQQ 换成现金"),
            ("调仓", "每月最后一个交易日收盘后算目标仓位，下一个交易日收盘成交；其余日子不动"),
            (
                "其他",
                (
                    "不加杠杆，仓位在 0% 到 100% 之间，其余是现金；"
                    "信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
                ),
            ),
        ],
        [],
    ),
    "two_sign_linear": (
        "signal_exposure",
        "generated",
        [
            ("信号", "「快线」、「慢线」"),
            ("打分", "信号为正记 1 分，为负记 0 分（正好为 0 记 0.5 分）；2 个信号的分数取平均"),
            ("仓位", "按有利信号的个数定仓位，见下表"),
            ("买入（加仓）", "有利的信号变多时，仓位按下表上调，最高 100%，买入 QQQ"),
            ("卖出（减仓）", "有利的信号变少时，仓位按下表下调，最低 0%，卖出 QQQ 换成现金"),
            ("调仓", "每月最后一个交易日收盘后算目标仓位，下一个交易日收盘成交；其余日子不动"),
            (
                "其他",
                (
                    "不加杠杆，仓位在 0% 到 100% 之间，其余是现金；"
                    "信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
                ),
            ),
        ],
        [
            ("2 个信号都为正", 1.0),
            ("1 个信号为正、1 个为负", 0.5),
            ("2 个信号都为负", 0.0),
        ],
    ),
    "weekly_band_delay": (
        "signal_exposure",
        "generated",
        [
            ("信号", "「慢线」"),
            ("打分", "信号为正记 1 分，为负记 0 分（正好为 0 记 0.5 分）"),
            ("仓位", "信号为正（正好为 0 也算）时仓位 100%；信号为负时仓位 0%"),
            (
                "买入（加仓）",
                "信号为正（正好为 0 也算）、而仓位还不到 100% 时，调仓日买入 QQQ，补到 100%",
            ),
            ("卖出（减仓）", "信号为负、而仓位还高于 0% 时，调仓日卖出 QQQ 换成现金，降到 0%"),
            (
                "调仓",
                (
                    "每周最后一个交易日收盘后算目标仓位，"
                    "下一个交易日收盘成交，再晚 2 天；其余日子不动；"
                    "只有当前仓位偏离目标超过目标的 20% 时才真的交易"
                ),
            ),
            (
                "其他",
                (
                    "不加杠杆，仓位在 0% 到 100% 之间，其余是现金；"
                    "信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
                ),
            ),
        ],
        [
            ("信号为负", 0.0),
            ("信号为正（正好为 0 也算）", 1.0),
        ],
    ),
}


def test_every_card_shape_is_covered() -> None:
    assert set(CARD_CASES) == set(EXPECTED_CARDS)


@pytest.mark.parametrize("case", sorted(CARD_CASES))
def test_strategy_card_wording(specs: Path, case: str) -> None:
    entry, warmup = CARD_CASES[case]
    kind, source, lines, levels = EXPECTED_CARDS[case]
    card = zh.strategy_card(
        entry, repo=specs, names={}, text=None, validated=False, warmup_days=warmup
    )
    assert set(card) == {
        "id",
        "name",
        "role",
        "validated",
        "kind",
        "signals",
        "lines",
        "levels",
        "source",
    }
    assert (card["id"], card["name"], card["role"], card["validated"]) == (
        "s",
        "s",
        "开发期变体",
        False,
    )
    assert (card["kind"], card["source"]) == (kind, source)
    assert _lines(card) == lines
    assert _levels(card) == levels


def test_card_signals_are_the_signal_views(specs: Path) -> None:
    entry, _ = CARD_CASES["two_sign_linear"]
    names = {"fast": {"name": "快线（21 日）", "meaning": None}}
    card = zh.strategy_card(entry, repo=specs, names=names, text=None, validated=True)
    assert [(s["id"], s["name"], s["favourable"]) for s in card["signals"]] == [
        ("fast", "快线（21 日）", 1),
        ("slow", "慢线", 1),
    ]
    assert card["lines"][0] == {"label": "信号", "text": "「快线（21 日）」、「慢线」"}
    assert card["role"] == "验证期读取" and card["validated"] is True


def test_curated_name_role_and_rule_override_the_generated_card(specs: Path) -> None:
    entry, _ = CARD_CASES["one_sign_threshold"]
    rule = [{"label": "规则", "text": "手写的规则"}]
    text = {"s": {"name": "趋势", "role": "正式策略 Q9", "rule": rule}}
    card = zh.strategy_card(entry, repo=specs, names={}, text=text, validated=True)
    assert (card["name"], card["role"], card["source"], card["lines"]) == (
        "趋势",
        "正式策略 Q9",
        "zh.yaml",
        rule,
    )
    named = {"s": {"name": "趋势", "role": None, "rule": None}}
    card = zh.strategy_card(entry, repo=specs, names={}, text=named, validated=False)
    assert (card["name"], card["role"], card["source"]) == ("趋势", "开发期变体", "generated")
    assert card["lines"][2] == {
        "label": "仓位",
        "text": "信号为正（正好为 0 也算）时仓位 100%；信号为负时仓位 0%",
    }


def test_curated_rule_also_covers_a_function_the_page_cannot_read(specs: Path) -> None:
    entry, _ = CARD_CASES["custom"]
    rule = [{"label": "规则", "text": "手写的规则"}]
    text = {"s": {"name": "手写", "role": None, "rule": rule}}
    card = zh.strategy_card(entry, repo=specs, names={}, text=text, validated=False)
    assert (card["kind"], card["source"], card["lines"]) == ("custom", "zh.yaml", rule)


# 6. Why a signal is eliminated ---------------------------------------------------------------

CRITERIA = {
    "primary_market": "us_market",
    "eliminate_breadth": 0.5,
    "candidate_breadth": 0.666667,
    "copy_corr": 0.9,
    "bh_q": 0.10,
    "confidence": 0.95,
}
BASE_INPUTS: dict[str, Any] = {
    "expected_sign": 1,
    "qqq_ic": 0.05,
    "qqq_interval": [-0.1, 0.2],
    "primary_interval": [0.01, 0.2],
    "breadth": 0.8,
    "leave_out_stable": True,
    "baseline_max_abs_corr": 0.3,
    "qqq_only": False,
}
OK_QQQ = ("ok", "QQQ 上方向和预期一致（秩相关 +0.050）")
OK_BREADTH = ("ok", "5 个市场里 4 个方向一致（80%，候选线 66.7%）")
OK_STABLE = ("ok", "去掉任意一个年代后，主市场的方向都不变")
INTERVAL_REVERSED = "美股大盘上信号和之后结果的秩相关，95% 区间 [{}, {}] 整体落在预期方向的反面"
BREADTH_LOW = "5 个市场里只有 2 个方向和预期一致（40%，淘汰线 50%）"
COPY = (
    "和基准信号（已实现波动、12-1 动量、10 个月均线距离）的最大秩相关 {}，"
    "达到重复线 0.9：基本是基准换了个名字"
)

REASON_CASES: list[tuple[str, dict[str, Any], dict[str, Any] | None, float | None, list[Any]]] = [
    (
        "interval_wrong_side_for_a_positive_signal",
        {"primary_interval": [-0.2, -0.05]},
        None,
        0.5,
        [("eliminate", INTERVAL_REVERSED.format("-0.20", "-0.05"))],
    ),
    (
        "interval_wrong_side_for_a_negative_signal",
        {"expected_sign": -1, "primary_interval": [0.05, 0.2], "qqq_ic": -0.02},
        None,
        0.5,
        [("eliminate", INTERVAL_REVERSED.format("+0.05", "+0.20"))],
    ),
    (
        "few_markets_agree",
        {"breadth": 0.4},
        {"direction_matches": 2, "direction_tested": 5},
        0.5,
        [("eliminate", BREADTH_LOW)],
    ),
    (
        "copy_of_a_baseline",
        {"baseline_max_abs_corr": 0.93},
        None,
        0.5,
        [("eliminate", COPY.format("0.93"))],
    ),
    (
        "every_reason_is_listed",
        {"primary_interval": [-0.2, -0.05], "breadth": 0.4, "baseline_max_abs_corr": 0.95},
        {"direction_matches": 2, "direction_tested": 5},
        0.5,
        [
            ("eliminate", INTERVAL_REVERSED.format("-0.20", "-0.05")),
            ("eliminate", BREADTH_LOW),
            ("eliminate", COPY.format("0.95")),
        ],
    ),
    (
        "candidate_meets_all_four",
        {},
        None,
        0.04,
        [OK_QQQ, OK_BREADTH, ("ok", "美股大盘主检验的调整后 p = 0.04（要 ≤ 0.1）"), OK_STABLE],
    ),
    (
        "two_of_three_counts_with_the_tolerance",
        {"breadth": 2 / 3},
        {"direction_matches": 2, "direction_tested": 3},
        0.1,
        [
            OK_QQQ,
            ("ok", "3 个市场里 2 个方向一致（66.7%，候选线 66.7%）"),
            ("ok", "美股大盘主检验的调整后 p = 0.1（要 ≤ 0.1）"),
            OK_STABLE,
        ],
    ),
    (
        "negative_signal_with_qqq_going_the_right_way",
        {"expected_sign": -1, "qqq_ic": -0.02, "primary_interval": [-0.2, 0.05]},
        None,
        0.04,
        [
            ("ok", "QQQ 上方向和预期一致（秩相关 -0.020）"),
            OK_BREADTH,
            ("ok", "美股大盘主检验的调整后 p = 0.04（要 ≤ 0.1）"),
            OK_STABLE,
        ],
    ),
    (
        "negative_signal_with_qqq_going_the_other_way",
        {"expected_sign": -1, "qqq_ic": 0.03, "primary_interval": [-0.2, 0.05]},
        None,
        0.04,
        [
            ("fail", "QQQ 上方向和预期一致（秩相关 +0.030）"),
            OK_BREADTH,
            ("ok", "美股大盘主检验的调整后 p = 0.04（要 ≤ 0.1）"),
            OK_STABLE,
        ],
    ),
    (
        "p_value_misses",
        {},
        None,
        0.37,
        [OK_QQQ, OK_BREADTH, ("fail", "美股大盘主检验的调整后 p = 0.37（要 ≤ 0.1）"), OK_STABLE],
    ),
    (
        "p_value_missing",
        {},
        None,
        None,
        [OK_QQQ, OK_BREADTH, ("fail", "美股大盘主检验的调整后 p = —（要 ≤ 0.1）"), OK_STABLE],
    ),
    (
        "qqq_on_the_wrong_side",
        {"qqq_ic": -0.03},
        None,
        0.04,
        [
            ("fail", "QQQ 上方向和预期一致（秩相关 -0.030）"),
            OK_BREADTH,
            ("ok", "美股大盘主检验的调整后 p = 0.04（要 ≤ 0.1）"),
            OK_STABLE,
        ],
    ),
    (
        "leave_out_unstable",
        {"leave_out_stable": False},
        None,
        0.04,
        [
            OK_QQQ,
            OK_BREADTH,
            ("ok", "美股大盘主检验的调整后 p = 0.04（要 ≤ 0.1）"),
            ("fail", "去掉任意一个年代后，主市场的方向都不变"),
        ],
    ),
    (
        "leave_out_unknown",
        {"leave_out_stable": None},
        None,
        0.04,
        [
            OK_QQQ,
            OK_BREADTH,
            ("ok", "美股大盘主检验的调整后 p = 0.04（要 ≤ 0.1）"),
            ("fail", "去掉任意一个年代后，主市场的方向都不变"),
        ],
    ),
    (
        "qqq_only_eliminated",
        {"qqq_only": True, "qqq_interval": [-0.3, -0.1]},
        None,
        None,
        [
            (
                "eliminate",
                "QQQ 上信号和之后结果的秩相关，95% 区间 [-0.30, -0.10] 整体落在预期方向的反面",
            )
        ],
    ),
    (
        "qqq_only_cannot_be_a_candidate",
        {"qqq_only": True},
        None,
        None,
        [("fail", "只在 QQQ 一个市场上筛，没有跨市场证据，不能直接成为候选")],
    ),
]


@pytest.mark.parametrize(
    ("inputs", "results", "adjusted_p", "expected"),
    [case[1:] for case in REASON_CASES],
    ids=[case[0] for case in REASON_CASES],
)
def test_screen_reasons(
    inputs: dict[str, Any],
    results: dict[str, Any] | None,
    adjusted_p: float | None,
    expected: list[tuple[str, str]],
) -> None:
    reasons = zh.screen_reasons(
        {**BASE_INPUTS, **inputs},
        results or {"direction_matches": 4, "direction_tested": 5},
        adjusted_p,
        CRITERIA,
    )
    assert [(reason["kind"], reason["text"]) for reason in reasons] == expected


def test_reasons_name_the_confidence_the_settings_give() -> None:
    reasons = zh.screen_reasons(
        {**BASE_INPUTS, "primary_interval": [-0.2, -0.05]},
        {"direction_matches": 4, "direction_tested": 5},
        0.5,
        {**CRITERIA, "confidence": 0.9},
    )
    assert "90% 区间 [-0.20, -0.05]" in reasons[0]["text"]


# 7. The fixture repository -------------------------------------------------------------------


def _prereg(question_id: str, strategies: list[dict[str, Any]], validated: int = 1) -> str:
    raw = yaml.safe_load(
        PROJECT_ROOT.joinpath("research", "_template", "prereg.yaml").read_text(encoding="utf-8")
    )
    raw["question_id"] = question_id
    raw["owner_decision"] = DECISION
    raw["question"] = f"{question_id}: does the trend strategy have a milder left tail?"
    raw["data"]["snapshot_id"] = "qlh-fixture"
    raw["strategies"] = strategies
    rules = [
        {"metric": f"comparison.{CVAR}", "op": ">=", "value": 0.10, "note": "left tail"},
        {"metric": "comparison.max_drawdown", "op": ">=", "value": 0.05, "note": "dd"},
        {"metric": f"ci_low.{CVAR}", "op": ">", "value": 0.0, "note": "interval"},
        {"metric": f"placebo_rank.{CVAR}", "op": ">=", "value": 0.95, "note": "random"},
    ]
    ids = [strategy["id"] for strategy in strategies[:validated]]
    raw["validation"] = {
        "strategies": ids,
        "rules": {ids[0]: rules, **{other: rules[1:2] for other in ids[1:]}},
        "confidence": 0.95,
    }
    return yaml.safe_dump(raw, sort_keys=False, allow_unicode=True)


def _strategies() -> list[dict[str, Any]]:
    month = {"schedule": "month_end", "band": 0.0}
    return [
        {
            "id": "trend",
            "function": "signal_exposure",
            "params": _params([_ref("fast"), _ref("slow")], "linear", low=0, high=1),
            "rebalance": month,
        },
        {
            "id": "gate",
            "function": "signal_exposure",
            "params": _params([_ref("vol", "percentile")], "threshold", low=0.5, high=1.0, cut=0.2),
            "rebalance": {"schedule": "week_end", "band": 0.2},
            "delay_days": 2,
        },
        {
            "id": "hold60",
            "function": "fixed_exposure",
            "params": {"exposure": 0.6},
            "rebalance": month,
        },
        {
            "id": "custom_x",
            "function": "vol_exposure",
            "params": {"window": 21},
            "rebalance": month,
        },
        {
            "id": "custom_y",
            "function": "vol_exposure",
            "params": {"window": 63},
            "rebalance": month,
        },
    ]


def _runs(directory: Path, bodies: list[dict[str, Any]]) -> None:
    runs = directory.joinpath("runs")
    runs.mkdir()
    previous: str | None = None
    for seq, body in enumerate(bodies, start=1):
        record = {
            "schema_version": "research_run.v1",
            "question_id": directory.name,
            "seq": seq,
            "prev_sha256": previous,
            **body,
        }
        data = json.dumps(record, indent=1, sort_keys=True).encode("utf-8") + b"\n"
        runs.joinpath(f"{seq:04d}.json").write_bytes(data)
        previous = bench_ledger.chain_digest(data)


def _run_body(question_id: str, number: int, strategy: str, role: str) -> dict[str, Any]:
    body: dict[str, Any] = {
        "run_id": f"{question_id}-{number:04d}",
        "strategy": {"id": strategy, "function": "signal_exposure", "module": "strategy.py"},
        "window": {"window_id": role, "role": role, "start": "1999-03-10", "end": None},
        "trial": {"key": f"key-{question_id}-{number}", "number": number, "new": True},
        "status": "ok",
        "result": {
            "strategy": {
                "geometric_annual_return": 0.0512,
                "max_drawdown": -0.3341,
                "sharpe_vs_cash": 0.41,
                "sharpe_daily": 0.02,
                "excess_skewness": -0.2,
                "excess_kurtosis": 3.0,
                "return_days": 2900,
            },
            "comparison": {"geometric_annual_return": -0.004, "max_drawdown": 0.081},
        },
        "rules": None,
    }
    if role == "validation":
        body["trial"] = {"key": f"key-{question_id}-1", "number": 1, "new": False}
        body["falsification_confidence"] = 0.95
        body["rules"] = [
            {
                "metric": "comparison.max_drawdown",
                "op": ">=",
                "value": 0.05,
                "observed": 0.081,
                "holds": True,
            },
            {
                "metric": f"ci_low.{CVAR}",
                "op": ">",
                "value": 0.0,
                "observed": -0.02,
                "holds": False,
            },
        ]
    return body


def _reads(directory: Path) -> None:
    entry = {
        "schema_version": "research_window_read.v1",
        "question_id": directory.name,
        "seq": 1,
        "prev_sha256": None,
        "read_id": f"{directory.name}-validation-1",
        "role": "validation",
        "window_id": "validation_window",
        "recorded_at": "2026-10-11T00:00:00+00:00",
    }
    directory.joinpath("window_reads.jsonl").write_bytes(
        json.dumps(entry, sort_keys=True).encode("utf-8") + b"\n"
    )


def _screen(
    repo: Path, signal: str, family: str, verdict: str, p: float | None, **inputs: Any
) -> dict[str, Any]:
    matches = inputs.pop("matches", 4)
    body = {
        "signal_id": signal,
        "module_sha256": family * 64,
        "spec_sha256": HASH_B,
        "params": {},
        "markets": ["qqq", "us_market"],
        "data": {"signal_snapshot_id": "sig-0123456789ab", "bench_snapshot_id": f"snap-{p}"},
        "code": {"commit": "c" * 40, "modified": False},
        "settings_sha256": HASH_A,
        "results": {
            "verdict_inputs": {**BASE_INPUTS, **inputs},
            "direction_matches": matches,
            "direction_tested": 5,
        },
        "primary_p_value": p,
        "verdict": verdict,
        "family_size": 3,
    }
    return screen_ledger.append_screen_record(repo, body)


def _trial_runs(repo: Path, bodies: list[dict[str, Any]]) -> None:
    runs = repo.joinpath("research", "_trials", "runs")
    runs.mkdir(parents=True)
    previous: str | None = None
    for seq, body in enumerate(bodies, start=1):
        record = {
            "schema_version": "research_trial_run.v1",
            "seq": seq,
            "run_id": f"trial-r{seq:04d}",
            "prev_sha256": previous,
            **body,
        }
        data = json.dumps(record, indent=1, sort_keys=True).encode("utf-8") + b"\n"
        runs.joinpath(f"{seq:04d}.json").write_bytes(data)
        previous = bench_ledger.chain_digest(data)


def _gate_rows(*rows: tuple[str, str, float, float, bool]) -> list[dict[str, Any]]:
    return [
        {"metric": metric, "op": op, "value": value, "observed": observed, "holds": holds}
        for metric, op, value, observed, holds in rows
    ]


def _repo(tmp_path: Path, name: str = "repo") -> Path:
    repo = tmp_path.joinpath(name)
    repo.joinpath("docs", "requirements").mkdir(parents=True)
    repo.joinpath("docs", "requirements", "TRADING-2568.md").write_text(
        f"- {DECISION}：夹具。\n", encoding="utf-8"
    )
    repo.joinpath("config", "research").mkdir(parents=True)
    repo.joinpath("config", "research", "research_screen.yaml").write_text(
        SCREEN_SETTINGS, encoding="utf-8"
    )
    repo.joinpath("config", "research", "research_bench.yaml").write_text(
        BENCH_SETTINGS, encoding="utf-8"
    )
    _write_specs(repo)
    research = repo.joinpath("research")
    research.mkdir()
    research.joinpath("signal_names.yaml").write_text(SIGNAL_NAMES, encoding="utf-8")
    research.joinpath("ledger.md").write_text(LEDGER, encoding="utf-8")
    research.joinpath("negative_results.md").write_text(NEGATIVE, encoding="utf-8")
    z1 = research.joinpath("QQQ-Z1")
    z1.mkdir()
    z1.joinpath("prereg.yaml").write_text(_prereg("QQQ-Z1", _strategies()), encoding="utf-8")
    z1.joinpath("zh.yaml").write_text(ZH_Z1, encoding="utf-8")
    _runs(
        z1,
        [
            _run_body("QQQ-Z1", 1, "trend", "development"),
            _run_body("QQQ-Z1", 2, "gate", "development"),
        ],
    )
    z3 = research.joinpath("QQQ-Z3")
    z3.mkdir()
    z3.joinpath("prereg.yaml").write_text(
        _prereg("QQQ-Z3", [_strategies()[i] for i in (0, 2, 1)], validated=2), encoding="utf-8"
    )
    _runs(
        z3,
        [
            _run_body("QQQ-Z3", 1, "trend", "development"),
            _run_body("QQQ-Z3", 2, "trend", "validation"),
        ],
    )
    _reads(z3)
    z3.joinpath("strategy.py").write_text(
        "def example(data, params):\n    return 1.0\n", encoding="utf-8"
    )
    _screen(repo, "fast", "a", "inconclusive", 0.5)
    # Recorded as a candidate before the family grew; the Benjamini-Hochberg p now says otherwise.
    _screen(repo, "slow", "b", "candidate", 0.2)
    _screen(
        repo,
        "vol",
        "c",
        "eliminated",
        0.9,
        breadth=0.4,
        matches=2,
    )
    _screen(repo, "fast", "a", "candidate", 0.01)
    trials = research.joinpath("_trials")
    trials.mkdir()
    trials.joinpath("prereg.yaml").write_text(TRIAL_GATE, encoding="utf-8")
    trials.joinpath("T0001.yaml").write_text(PLAN_T0001, encoding="utf-8")
    trials.joinpath("T0002.yaml").write_text(PLAN_T0002, encoding="utf-8")
    ci_low = f"ci_low.{CVAR}"
    _trial_runs(
        repo,
        [
            {
                "trial_id": "T0001",
                "config": "primary",
                "market": "us_market",
                "status": "ok",
                "gate": _gate_rows(
                    (ci_low, ">", 0.0, 0.04, True),
                    ("comparison.max_drawdown", ">=", 0.05, 0.11, True),
                ),
                "gate_holds": True,
            },
            {
                "trial_id": "T0001",
                "config": "primary",
                "market": "qqq",
                "status": "ok",
                "gate": _gate_rows(
                    (f"ci_high.{CVAR}", ">", 0.0, 0.05, True),
                    ("comparison.geometric_annual_return", ">=", -0.01, -0.004, True),
                ),
                "gate_holds": True,
            },
            {
                "trial_id": "T0002",
                "config": "mix",
                "market": "us_market",
                "status": "ok",
                "gate": _gate_rows(
                    (ci_low, ">", 0.0, -0.1, False),
                    ("comparison.max_drawdown", ">=", 0.04, 0.02, False),
                    ("comparison.geometric_annual_return", ">=", -0.01, -0.02, False),
                ),
                "gate_holds": False,
            },
        ],
    )
    return repo


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    return _repo(tmp_path)


# 8. What collect() adds ----------------------------------------------------------------------


def test_model_keeps_the_run_one_keys_and_adds_four(repo: Path) -> None:
    model = collect(repo)
    assert {
        "schema_version",
        "source",
        "summary",
        "questions",
        "ledger",
        "negative_results",
        "negative_sections",
        "screens",
        "tasks",
        "activity",
        "problems",
    } <= set(model)
    assert {"screen_details", "screen_criteria", "trials", "bench"} <= set(model)
    assert model["schema_version"] == "research_site.v1"
    assert json.loads(json.dumps(model, allow_nan=False)) == model


def test_the_fixture_has_no_problems(repo: Path) -> None:
    model = collect(repo)
    assert model["problems"] == []


def test_question_with_a_zh_file(repo: Path) -> None:
    question = {q["question_id"]: q for q in collect(repo)["questions"]}["QQQ-Z1"]
    assert question["zh"] == {
        "title": "双信号趋势策略能让左尾更温和吗？",
        "summary": SUMMARY_Z1,
        "verdict": "保留：所有门槛成立。拒绝：点估计更差 <b>。",
    }
    assert question["question"].startswith("QQQ-Z1: does the trend strategy")
    cards = question["strategy_cards"]
    assert [card["id"] for card in cards] == ["trend", "gate", "hold60", "custom_x", "custom_y"]
    assert [(card["name"], card["role"], card["validated"]) for card in cards] == [
        ("趋势策略", "正式策略", True),
        ("波动门", "开发期变体", False),
        ("hold60", "开发期变体", False),
        ("custom_x", "开发期变体", False),
        ("手写策略", "开发期变体", False),
    ]
    assert [card["source"] for card in cards] == [
        "generated",
        "generated",
        "generated",
        "unavailable",
        "zh.yaml",
    ]
    assert _lines(cards[0])[0] == ("信号", "「快线（21 日）」、「慢线」")
    assert _lines(cards[3]) == [("规则", "由代码里的 vol_exposure 定义，页面暂时没有中文说明")]
    assert _lines(cards[4]) == [("规则", "手写的规则"), ("调仓", "每月一次")]
    gate = _lines(cards[1])
    assert (
        "调仓",
        (
            "每周最后一个交易日收盘后算目标仓位，下一个交易日收盘成交，再晚 2 天；其余日子不动；"
            "只有当前仓位偏离目标超过目标的 20% 时才真的交易"
        ),
    ) in gate
    assert gate[1][1].endswith("（信号要先有 252 个有值的交易日做历史，之后才开始打分）")


def test_question_gates_are_the_validated_rules_in_words(repo: Path) -> None:
    question = {q["question_id"]: q for q in collect(repo)["questions"]}["QQQ-Z1"]
    assert question["gates"] == [
        {
            "strategy": "trend",
            "name": "趋势策略",
            "rules": [
                {
                    "metric": f"comparison.{CVAR}",
                    "op": ">=",
                    "value": 0.1,
                    "text": "左尾亏损比对照至少小 10%",
                },
                {
                    "metric": "comparison.max_drawdown",
                    "op": ">=",
                    "value": 0.05,
                    "text": "最大回撤比对照至少浅 5 个点",
                },
                {
                    "metric": f"ci_low.{CVAR}",
                    "op": ">",
                    "value": 0.0,
                    "text": "左尾改善的 95% 区间下限 > 0（排除「没有改善」）",
                },
                {
                    "metric": f"placebo_rank.{CVAR}",
                    "op": ">=",
                    "value": 0.95,
                    "text": "左尾改善胜过 95% 的随机择时（同平均仓位）",
                },
            ],
        }
    ]


def test_every_validated_strategy_has_its_own_gate(repo: Path) -> None:
    question = {q["question_id"]: q for q in collect(repo)["questions"]}["QQQ-Z3"]
    assert [(g["strategy"], g["name"], len(g["rules"])) for g in question["gates"]] == [
        ("trend", "trend", 4),
        ("hold60", "hold60", 1),
    ]
    assert question["gates"][1]["rules"][0]["text"] == "最大回撤比对照至少浅 5 个点"


def test_question_without_a_zh_file_is_generated_from_the_prereg(repo: Path) -> None:
    question = {q["question_id"]: q for q in collect(repo)["questions"]}["QQQ-Z3"]
    assert question["zh"] is None
    cards = question["strategy_cards"]
    assert [(card["id"], card["name"], card["role"]) for card in cards] == [
        ("trend", "trend", "验证期读取"),
        ("hold60", "hold60", "验证期读取"),
        ("gate", "gate", "开发期变体"),
    ]
    assert cards[0]["lines"][0] == {"label": "信号", "text": "「快线（21 日）」、「慢线」"}


def test_run_rules_are_in_words_with_the_measured_value(repo: Path) -> None:
    question = {q["question_id"]: q for q in collect(repo)["questions"]}["QQQ-Z3"]
    validation = [run for run in question["runs"] if run["role"] == "validation"][0]
    assert validation["rules"] == [
        {
            "metric": "comparison.max_drawdown",
            "op": ">=",
            "value": 0.05,
            "text": "最大回撤比对照至少浅 5 个点",
            "observed": 0.081,
            "observed_text": "+8.1 点",
            "holds": True,
        },
        {
            "metric": f"ci_low.{CVAR}",
            "op": ">",
            "value": 0.0,
            "text": "左尾改善的 95% 区间下限 > 0（排除「没有改善」）",
            "observed": -0.02,
            "observed_text": "-2.0%",
            "holds": False,
        },
    ]
    assert (validation["rules_held"], validation["rules_total"]) == (1, 2)
    development = [run for run in question["runs"] if run["role"] == "development"][0]
    assert development["rules"] is None


def test_broken_curated_files_are_problems_and_the_page_still_renders(repo: Path) -> None:
    repo.joinpath("research", "QQQ-Z1", "zh.yaml").write_text(
        ZH_Z1.replace("  gate:", "  nonexistent:"), encoding="utf-8"
    )
    repo.joinpath("research", "signal_names.yaml").write_text("signals: [oops\n", encoding="utf-8")
    model = collect(repo)
    assert any(p.startswith("QQQ-Z1: ") and "zh.yaml" in p for p in model["problems"])
    assert any("research/signal_names.yaml" in p for p in model["problems"])
    question = {q["question_id"]: q for q in model["questions"]}["QQQ-Z1"]
    assert question["zh"] is None and question["status"] != "error"
    assert question["strategy_cards"][0]["name"] == "trend"
    assert question["strategy_cards"][0]["lines"][0]["text"] == "「快线」、「慢线」"
    assert 'id="q-QQQ-Z1"' in render(model)


def test_screens_keep_their_shape_and_follow_the_current_verdict(repo: Path) -> None:
    model = collect(repo)
    screens = model["screens"]
    assert (screens["present"], screens["records"], screens["family_size"]) == (True, 4, 3)
    assert screens["counts"] == {"candidate": 1, "eliminated": 1, "inconclusive": 1}
    assert {m["signal_id"]: m["verdict"] for m in screens["members"]} == {
        "slow": "candidate",
        "vol": "eliminated",
        "fast": "candidate",
    }
    assert all(
        set(member) == {"signal_id", "number", "verdict", "primary_p_value", "records"}
        for member in screens["members"]
    )
    assert [(m["signal_id"], m["number"], m["records"]) for m in screens["members"]] == [
        ("slow", 2, 1),
        ("vol", 3, 1),
        ("fast", 4, 2),
    ]


def test_screen_criteria_and_details(repo: Path) -> None:
    model = collect(repo)
    assert model["screen_criteria"] == {
        "screen_end": "2010-12-31",
        "primary_market": "us_market",
        "primary_horizon": 21,
        "eliminate_breadth": 0.5,
        "candidate_breadth": 0.666667,
        "copy_corr": 0.9,
        "bh_q": 0.1,
        "confidence": 0.95,
        "warmup_days": 252,
    }
    details = model["screen_details"]
    assert set(details) == {"2", "3", "4"}
    fast = details["4"]
    assert (fast["name"], fast["meaning"]) == ("快线（21 日）", "最近 21 日的收益，单位是百分点")
    assert fast["expectation"] == "信号越高，之后收益越高"
    assert fast["verdict_now"] == "candidate"
    assert fast["adjusted_p"] == pytest.approx(0.03)
    assert (fast["direction_matches"], fast["direction_tested"]) == (4, 5)
    assert [(r["kind"], r["text"]) for r in fast["reasons"]] == [
        OK_QQQ,
        OK_BREADTH,
        ("ok", "美股大盘主检验的调整后 p = 0.03（要 ≤ 0.1）"),
        OK_STABLE,
    ]
    slow = details["2"]
    assert (slow["name"], slow["verdict_now"]) == ("慢线", "inconclusive")
    assert slow["adjusted_p"] == pytest.approx(0.3)
    assert slow["reasons"][2] == {
        "kind": "fail",
        "text": "美股大盘主检验的调整后 p = 0.3（要 ≤ 0.1）",
    }
    vol = details["3"]
    assert (vol["name"], vol["verdict_now"]) == ("波动 <b>粗体</b>", "eliminated")
    assert vol["expectation"] == "信号越高，之后波动越大"
    assert [(r["kind"], r["text"]) for r in vol["reasons"]] == [("eliminate", BREADTH_LOW)]


def test_broken_screen_chain_falls_back_on_the_recorded_verdicts(repo: Path) -> None:
    path = repo.joinpath("research", "_screens", "ledger", "0002.json")
    path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    model = collect(repo)
    assert any("筛选记录的 BH 校正" in p for p in model["problems"])
    assert model["screens"]["counts"] == {"candidate": 2, "eliminated": 1, "inconclusive": 0}
    assert all(
        detail["verdict_now"] is None and detail["adjusted_p"] is None
        for detail in model["screen_details"].values()
    )
    render(model)


def test_screen_settings_that_cannot_be_read_are_a_problem(repo: Path) -> None:
    repo.joinpath("config", "research", "research_screen.yaml").write_text(
        "schema_version: wrong\n", encoding="utf-8"
    )
    model = collect(repo)
    assert any(p.startswith("config/research/research_screen.yaml: ") for p in model["problems"])
    assert model["screen_criteria"] is None
    page = render(model)
    assert 'class="criteria"' not in _block(page, "screens")


def test_trials(repo: Path) -> None:
    model = collect(repo)
    trials = model["trials"]
    assert trials["present"] is True
    assert (trials["trial_configs"], trials["question_trials"], trials["global_trials"]) == (
        3,
        3,
        6,
    )
    assert {
        k: model["summary"][k] for k in ("trial_configs", "question_trials", "global_trials")
    } == {
        "trial_configs": 3,
        "question_trials": 3,
        "global_trials": 6,
    }
    gate = trials["gate"]
    assert gate["confidence"] == 0.95
    assert [(m["market"], m["name"]) for m in gate["markets"]] == [
        ("us_market", "美股大盘长历史（第一段，主检验）"),
        ("qqq", "QQQ 开发期（第二段，只查方向）"),
    ]
    assert [rule["text"] for rule in gate["markets"][0]["rules"]] == [
        "左尾改善的 95% 区间下限 > 0（排除「没有改善」）",
        "最大回撤比对照至少浅 5 个点",
    ]
    assert [rule["text"] for rule in gate["markets"][1]["rules"]] == [
        "左尾改善的 95% 区间上限 > 0（还不能说比对照差）",
        "几何年化比对照最多少 1 个点",
    ]
    first, second = trials["plans"]
    assert [p["trial_id"] for p in trials["plans"]] == ["T0001", "T0002"]
    assert first["verdict"] == "pass" and first["verdict_name"] == "过门"
    assert second["verdict"] == "fail" and second["verdict_name"] == "没过"
    assert [s["name"] for s in first["signals"]] == ["快线（21 日）"]
    assert second["signals"] == []
    assert first["owner_decision"] is None and second["owner_decision"].endswith(
        "portfolio_fixture"
    )
    assert (first["hypothesis_card"], first["from_validation_window"]) == (
        "card-fixture-fast",
        False,
    )
    assert second["from_validation_window"] is True
    assert (first["primary"], first["configs"]) == ("primary", ["primary", "hold"])
    assert _lines(first["card"])[1] == (
        "打分",
        "看信号在自己历史（到当天为止）里的位置：处在最高位记 1 分，最低位记 0 分，中间按百分位"
        "（信号要先有 252 个有值的交易日做历史，之后才开始打分）",
    )
    assert _lines(second["card"]) == [
        (
            "仓位",
            "固定持有 60% 的 QQQ，其余 40% 是 10 年期国债（用 FRED DGS10 合成）；不看任何信号",
        ),
        (
            "调仓",
            "每周最后一个交易日把仓位调回目标比例；只有当前仓位偏离目标超过目标的 10% 时才真的交易",
        ),
    ]
    assert [(m["market"], m["holds"], m["current_gate"]) for m in first["markets"]] == [
        ("us_market", True, True),
        ("qqq", True, True),
    ]
    us, qqq = second["markets"]
    assert (us["holds"], us["current_gate"]) == (False, False)
    assert (qqq["holds"], qqq["rules"]) == (None, [])
    assert [(r["text"], r["observed_text"], r["holds"]) for r in first["markets"][0]["rules"]] == [
        ("左尾改善的 95% 区间下限 > 0（排除「没有改善」）", "+4.0%", True),
        ("最大回撤比对照至少浅 5 个点", "+11.0 点", True),
    ]


RULER_GATE = TRIAL_GATE.replace(
    """      - {metric: comparison.max_drawdown, op: ">=", value: 0.05}
production_effect""",
    """      - {metric: comparison.max_drawdown, op: ">=", value: 0.05}
    portfolio_rules:
      windows: [us_market_1929_2010]
      rules:
        - {metric: portfolio.median_ratio, op: ">=", value: 0.10}
    decline_episodes:
      us_market_1929_2010:
        - {id: decline_192909, start: "1929-10", end: "1932-06"}
production_effect""",
)

PLAN_T0008 = """schema_version: research_trial_plan.v1
trial_id: T0008
owner_decision: owner_decision:TRADING-2565:2026-10-21:treasury_fixture
hypothesis_card: card-fixture-shiller
from_validation_window: true
data: {snapshot_id: qlh-fixture, signal_snapshot_id: sig-0123456789ab}
primary: mix
configs:
  - id: mix
    function: fixed_exposure
    reserve: treasury_10y_shiller
    params: {exposure: 0.6}
    rebalance: {schedule: month_end, band: 0.0}
production_effect: none
broker_action: none
"""


def test_a_reserve_mix_is_shown_by_the_portfolio_ruler(tmp_path: Path) -> None:
    # TRADING-2565-40: a fixed mix holding a reserve is judged by the gate's portfolio rules of
    # its window, and a window the rules do not name has no ruler.
    repo = _repo(tmp_path)
    trials = repo.joinpath("research", "_trials")
    trials.joinpath("prereg.yaml").write_text(RULER_GATE, encoding="utf-8")
    trials.joinpath("T0008.yaml").write_text(PLAN_T0008, encoding="utf-8")
    runs = trials.joinpath("runs")
    for path in runs.glob("*.json"):
        path.unlink()
    runs.rmdir()
    ruler = _gate_rows(("portfolio.median_ratio", ">=", 0.10, 0.04, False))
    _trial_runs(
        repo,
        [
            {
                "trial_id": "T0008",
                "config": "mix",
                "market": "us_market",
                "status": "ok",
                "window": {"window_id": "us_market_1929_2010"},
                "rule_set": "portfolio",
                "gate": ruler,
                "gate_holds": False,
                "gate_outcome": "fail",
            },
            {
                "trial_id": "T0008",
                "config": "mix",
                "market": "qqq",
                "status": "ok",
                "window": {"window_id": "qqq_long_history_development"},
                "rule_set": "portfolio",
                "gate": [],
                "gate_holds": None,
                "gate_outcome": "no_ruler",
            },
        ],
    )
    model = collect(repo)
    plan = next(p for p in model["trials"]["plans"] if p["trial_id"] == "T0008")
    us, qqq = plan["markets"]
    assert (us["market"], us["holds"], us["outcome"], us["current_gate"]) == (
        "us_market",
        False,
        "fail",
        True,
    )
    assert [r["holds"] for r in us["rules"]] == [False]
    assert (qqq["market"], qqq["holds"], qqq["outcome"]) == ("qqq", None, "no_ruler")
    assert _lines(plan["card"])[0] == (
        "仓位",
        "固定持有 60% 的 QQQ，其余 40% 是 10 年期国债（用 Shiller 长期利率按月合成）；不看任何信号",
    )
    page = render(model)
    assert "<strong>美股大盘长历史（第一段，主检验）</strong> · 没过</p>" in page
    assert (
        "<strong>QQQ 开发期（第二段，只查方向）</strong> · 没有尺子（不算过，也不算没过）" in page
    )
    # A timing record keeps being compared with the timing rules.
    other = next(p for p in model["trials"]["plans"] if p["trial_id"] == "T0001")
    assert all(m["outcome"] is None for m in other["markets"])


def test_without_trials_or_settings_nothing_is_invented(tmp_path: Path) -> None:
    repo = tmp_path.joinpath("bare")
    repo.joinpath("research").mkdir(parents=True)
    model = collect(repo)
    assert [p for p in model["problems"] if "ledger.md" not in p] == []
    assert model["trials"]["present"] is False and model["trials"]["plans"] == []
    assert model["bench"] is None and model["screen_criteria"] is None
    assert model["screen_details"] == {}
    page = render(model)
    assert "还没有试跑记录。" in _block(page, "trials")
    glossary = _block(page, "glossary")
    assert "对照" in glossary and "随机择时" not in glossary


def test_bench_numbers_come_from_the_settings(repo: Path) -> None:
    assert collect(repo)["bench"] == {
        "cvar_levels": [0.01, 0.05],
        "rolling_years": 3,
        "rolling_percentile": 0.1,
        "cost_bps": [4, 15, 25],
        "delay_days": [1, 2],
        "placebo_block_days": 21,
        "placebo_draws": 1000,
        "bootstrap_block_days": 21,
        "bootstrap_draws": 10000,
        "confidence": 0.95,
        "episodes": [
            {"id": "dotcom_bear", "name": "科网泡沫熊市", "start": "2000-03", "end": "2002-10"},
            {"id": "odd_episode", "name": "odd_episode", "start": "2015-08", "end": "2016-02"},
        ],
    }


# 9. The page ---------------------------------------------------------------------------------


def test_page_has_no_scripts_or_external_resources(repo: Path) -> None:
    page = render(collect(repo))
    lowered = page.lower()
    for banned in ("<script", "<link", "<img", "<iframe", "@import", "url("):
        assert banned not in lowered, banned
    assert 'href="http' not in page


def test_every_in_page_link_has_a_target(repo: Path) -> None:
    page = render(collect(repo))
    targets = set(re.findall(r'\bid="([^"]+)"', page))
    links = re.findall(r'href="#([^"]+)"', page)
    assert links and all(link in targets for link in links)
    nav = re.search(r'<nav class="nav">(.*?)</nav>', page, re.S)
    assert nav is not None
    assert re.findall(r'href="#(\w+)">([^<]+)<', nav.group(1)) == [
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


def test_header_explains_how_to_read_and_counts_trials_by_source(repo: Path) -> None:
    page = render(collect(repo))
    guide = re.search(r'<p class="guide">(.*?)</p>', page, re.S)
    assert guide is not None
    for phrase in ("怎么读", "怎么买、怎么卖、多久调一次仓", "信号筛选", "名词解释"):
        assert phrase in guide.group(1)
    assert "累计试验 6（题目 3 + 试跑 3）" in page


def test_question_card_leads_with_the_chinese_question(repo: Path) -> None:
    page = render(collect(repo))
    card = _block(page, "q-QQQ-Z1")
    assert '<span class="m">双信号趋势策略能让左尾更温和吗？</span>' in card
    assert (
        '<p class="zhsum">趋势策略 trend 按两个信号的方向定仓位，每月调一次。'
        "对照是平均仓位相同的固定仓位。</p>"
    ) in card
    assert '<p class="verdict-rule"><strong>怎么判：</strong>保留：所有门槛成立。' in card
    assert "点估计更差 &lt;b&gt;。" in card and "点估计更差 <b>" not in card
    assert (
        "<details><summary>英文原题</summary>"
        "<p>QQQ-Z1: does the trend strategy have a milder left tail?</p></details>"
    ) in card
    assert "还没有中文说明" not in card
    assert card.index("zhsum") < card.index("verdict-rule") < card.index("英文原题")


def test_question_card_without_zh_says_where_to_write_it(repo: Path) -> None:
    card = _block(render(collect(repo)), "q-QQQ-Z3")
    assert "还没有中文说明：research/QQQ-Z3/zh.yaml" in card
    assert 'class="zhsum"' not in card and "怎么判" not in card


def test_questions_lead_says_what_a_card_shows(repo: Path) -> None:
    section = _block(render(collect(repo)), "questions")
    assert "先看一句话问题和「怎么判」，再看策略怎么买卖、通过门槛和验证期结果" in section


def test_strategy_block_shows_signals_rules_and_the_gate(repo: Path) -> None:
    card = _block(render(collect(repo)), "q-QQQ-Z1")
    assert '<h4 class="sec-h">策略怎么买卖</h4>' in card
    blocks = card.split('<div class="strat">')
    assert len(blocks) == 2, "one formal strategy, one block"
    first = blocks[1]
    assert "趋势策略" in first and '<span class="id">trend</span>' in first
    assert '<span class="b role">正式策略</span>' in first
    assert '<span class="b role">走高有利</span>' in first
    assert "<strong>快线（21 日）</strong>" in first
    assert '<p class="m">最近 21 日的收益，单位是百分点</p>' in first
    assert "<dt>买入（加仓）</dt>" in first and "<dt>卖出（减仓）</dt>" in first
    assert "<dt>调仓</dt><dd>每月最后一个交易日收盘后算目标仓位" in first
    assert '<td data-label="情形">2 个信号都为正</td>' in first
    assert '<td data-label="目标仓位">100%</td>' in first
    assert "<summary>通过门槛：共 4 条（还没有运行结果）</summary>" in first
    assert (
        '<li><span class="mk">·</span><span class="t">最大回撤比对照至少浅 5 个点</span></li>'
        in first
    )
    assert "<dt>信号</dt>" not in first


def test_each_formal_strategy_gets_a_block_with_its_own_gate(repo: Path) -> None:
    card = _block(render(collect(repo)), "q-QQQ-Z3")
    blocks = card.split('<div class="strat">')[1:]
    assert len(blocks) == 2
    assert "<summary>通过门槛：1 / 2 条成立（实测是相对对照的数，正为更好）</summary>" in blocks[0]
    assert "<summary>通过门槛：共 1 条（还没有运行结果）</summary>" in blocks[1]
    assert "<summary>开发期变体 1 个（只在开发期看，不决定结论）</summary>" in card


def test_variants_are_folded_into_one_table(repo: Path) -> None:
    card = _block(render(collect(repo)), "q-QQQ-Z1")
    summary = "<summary>开发期变体 4 个（只在开发期看，不决定结论）</summary>"
    assert summary in card
    variants = card[card.index(summary) :]
    variants = variants[: variants.index("</details>")]
    assert re.findall(r"<th>([^<]*)</th>", variants) == ["变体", "说明"]
    rows = re.findall(
        r'<tr><td data-label="变体">([^<]*) <span class="m">([^<]*)</span></td>'
        r'<td data-label="说明">([^<]*)</td></tr>',
        variants,
    )
    assert [(name, strategy_id) for name, strategy_id, _ in rows] == [
        ("波动门", "gate"),
        ("hold60", "hold60"),
        ("custom_x", "custom_x"),
        ("手写策略", "custom_y"),
    ]
    notes = {strategy_id: note for _, strategy_id, note in rows}
    assert notes["hold60"] == (
        "固定持有 60% 的 QQQ，其余 40% 是现金；不看任何信号；每月最后一个交易日把仓位调回目标比例"
    )
    assert notes["gate"].startswith("信号在自己历史的 80% 分位以下（分数 ≥ 0.2）时仓位 100%；")
    assert notes["gate"].endswith("只有当前仓位偏离目标超过目标的 20% 时才真的交易")
    assert notes["custom_x"] == "由代码里的 vol_exposure 定义，页面暂时没有中文说明"
    assert notes["custom_y"] == "手写的规则；每月一次"


def test_measured_gate_marks_each_rule(repo: Path) -> None:
    card = _block(render(collect(repo)), "q-QQQ-Z3")
    assert "<summary>通过门槛：1 / 2 条成立（实测是相对对照的数，正为更好）</summary>" in card
    assert (
        '<li class="pass"><span class="mk">✓</span><span class="t">最大回撤比对照至少浅 5 个点'
        '</span><span class="v">+8.1 点</span></li>'
    ) in card
    assert (
        '<li class="miss"><span class="mk">✗</span><span class="t">左尾改善的 95% 区间下限 &gt; 0'
        "（排除「没有改善」）</span>"
    ) in card
    assert '<span class="v">-2.0%</span>' in card
    assert "验证期结果" in card


def test_screens_section_states_the_criteria_with_the_numbers(repo: Path) -> None:
    section = _block(render(collect(repo)), "screens")
    criteria = section[section.index('<div class="criteria">') :]
    criteria = criteria[: criteria.index("</div>")]
    assert "怎么筛、凭什么淘汰" in criteria
    assert "2010-12-31 以前的数据" in criteria and "之后 21 个交易日" in criteria
    assert "（共 5 个市场）" in criteria
    assert "淘汰</strong>（满足任意一条）" in criteria
    assert "95% 区间整体落在预期方向的反面" in criteria
    assert "方向和预期一致的市场不到 50%" in criteria
    assert "秩相关绝对值 ≥ 0.9" in criteria
    assert "候选</strong>（四条都要满足）" in criteria
    assert "方向一致的市场不少于 66.7%（三分之二）" in criteria
    assert "Benjamini-Hochberg 校正后 ≤ 0.1" in criteria
    assert "去掉任意一个年代后，主检验的方向不变" in criteria
    assert "证据不足</strong>：既没到淘汰线，也不够格当候选" in criteria


def test_screens_table_names_signals_and_gives_the_reasons(repo: Path) -> None:
    section = _block(render(collect(repo)), "screens")
    assert "已筛 3 个信号（共 4 条记录）：淘汰 1 · 证据不足 1 · 候选 1" in section
    headers = re.findall(r"<th>([^<]*)</th>", section)
    assert headers == ["信号", "裁决", "去向", "主市场 p（未校正）", "记录条数", "依据"]
    assert (
        '<span class="sigcell"><strong>快线（21 日）</strong><span class="m">fast</span>'
        '<span class="m">预期：信号越高，之后收益越高</span></span>'
    ) in section
    assert "<strong>波动 &lt;b&gt;粗体&lt;/b&gt;</strong>" in section
    assert '<span class="b cand">候选</span>' in section
    assert '<span class="b out">淘汰</span>' in section
    assert '<span class="b unk">证据不足</span>' in section
    assert (
        '<li class="ok"><span class="mk">✓</span>QQQ 上方向和预期一致（秩相关 +0.050）</li>'
        in section
    )
    assert '<li class="fail"><span class="mk">✗</span>美股大盘主检验的调整后 p = 0.3' in section
    assert (
        '<li class="eliminate"><span class="mk">✗</span>5 个市场里只有 2 个方向和预期一致'
        "（40%，淘汰线 50%）</li>"
    ) in section


def test_trials_section(repo: Path) -> None:
    section = _block(render(collect(repo)), "trials")
    assert "累计试验 6 = 研究题的开发试验 3 + 试跑配置 3" in section
    assert "<h3>试跑门槛</h3>" in section
    assert "<strong>美股大盘长历史（第一段，主检验）</strong>" in section
    assert "<li>最大回撤比对照至少浅 5 个点</li>" in section
    assert "对照：平均仓位相同的固定仓位。" in section
    first = _block(section, "trial-T0001")
    assert '<h3>T0001 <span class="m">快线（21 日）</span></h3>' in first
    assert '<span class="b pass">过门</span>' in first
    assert "主配置 primary · 共 2 个配置" in first
    assert "来源：card-fixture-fast" in first
    assert "主配置怎么买卖" in first and "<dt>打分</dt>" in first
    assert "<strong>QQQ 开发期（第二段，只查方向）</strong> · 过" in first
    assert "按当时的门判" not in first
    second = _block(section, "trial-T0002")
    assert '<h3>T0002 <span class="m">配置 mix</span></h3>' in second
    assert '<span class="b miss">没过</span>' in second
    assert "想法来自读过验证期的题目" in second
    assert (
        "固定持有 60% 的 QQQ，其余 40% 是 10 年期国债（用 FRED DGS10 合成）；不看任何信号" in second
    )
    assert (
        "<strong>美股大盘长历史（第一段，主检验）</strong> · 没过（按当时的门判，和现行门不同）"
        in second
    )
    assert "QQQ 开发期" not in second


def test_glossary_explains_terms_with_the_bench_numbers(repo: Path) -> None:
    glossary = _block(render(collect(repo)), "glossary")
    for term in ("对照", "左尾", "最大回撤", "几何年化", "预注册", "秩相关", "折算夏普"):
        assert f"<dt>{term}</dt>" in glossary
    assert "<dt>滚动 3 年几何年化的低位</dt>" in glossary and "最低 10% 位置" in glossary
    assert "21 天一块反复重抽 10000 次" in glossary
    assert "21 天一块打乱 1000 次" in glossary
    assert "4、15、25 bps 分别是 0.04%、0.15%、0.25%" in glossary
    assert "检查 1、2 天" in glossary
    assert "科网泡沫熊市 2000-03–2002-10" in glossary and "odd_episode 2015-08–2016-02" in glossary
    assert "Benjamini-Hochberg" in glossary


def test_page_escapes_curated_text(repo: Path) -> None:
    page = render(collect(repo))
    assert "<b>" not in page
    assert "波动 &lt;b&gt;粗体&lt;/b&gt;" in page


def test_output_inside_the_repository_is_refused(repo: Path) -> None:
    with pytest.raises(ValueError, match="仓库之外"):
        build_site(repo, repo.joinpath("site"))
    assert not repo.joinpath("site").exists()


def test_page_is_deterministic(repo: Path) -> None:
    assert render(collect(repo)) == render(collect(repo))
    first = build_site(repo, repo.parent.joinpath("out1"))
    second = build_site(repo, repo.parent.joinpath("out2", "nested"))
    assert first.read_bytes() == second.read_bytes()
    assert (
        repo.parent.joinpath("out1", "site.json").read_bytes()
        == repo.parent.joinpath("out2", "nested", "site.json").read_bytes()
    )


# 10. The real repository ---------------------------------------------------------------------


def test_real_repository_has_the_new_sections() -> None:
    model = collect(PROJECT_ROOT)
    assert model["problems"] == []
    assert model["bench"] is not None and model["screen_criteria"] is not None
    assert model["trials"]["present"] is True
    research = PROJECT_ROOT.joinpath("research")
    assert (
        model["summary"]["global_trials"]
        == bench_trial.trial_ledger_summary(research)["global_trials"]
    )
    assert len(model["screen_details"]) == model["screens"]["family_size"]
    for question in model["questions"]:
        if question["status"] == "error":
            continue
        assert question["strategy_cards"], question["question_id"]
        assert all(card["lines"] for card in question["strategy_cards"])
    page = render(model)
    assert "策略怎么买卖" in page and "怎么筛、凭什么淘汰" in page
