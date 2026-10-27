"""QQQ-Q5 (TRADING-2567): the preregistration is complete, its rules are the trial gate's direction
rules, its one strategy is QQQ-Q3's weekly sensitivity variant unchanged (QQQ-Q3's fast_slow with
the weekly schedule), its signals are the pinned files, it runs on the development window with every
parameter read and gives the same result as the QQQ-Q3 variant. Synthetic data only."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings, run_and_record, screen
from ai_trading_system.research_bench.data import BenchData, from_daily_frame
from ai_trading_system.research_bench.prereg import rule_problems

LOADED = load_settings()
QUESTION_DIR = PROJECT_ROOT.joinpath("research", "QQQ-Q5")
QUESTION = lg.load_question(QUESTION_DIR)
PREREG = QUESTION.prereg
Q3_DIR = PROJECT_ROOT.joinpath("research", "QQQ-Q3")
Q3 = lg.load_question(Q3_DIR)
QQQ_START = pd.Timestamp("1999-03-10")
DECISION = "owner_decision:TRADING-2567:2026-10-21:qqq_q5_prereg"
MAIN = ("fast_slow_weekly",)


def _yaml(*parts: str) -> dict[str, Any]:
    loaded: dict[str, Any] = yaml.safe_load(
        PROJECT_ROOT.joinpath(*parts).read_text(encoding="utf-8")
    )
    return loaded


def _data() -> BenchData:
    """Price index from 1986 and the QQQ series from its first trading day: the shapes of the
    snapshot, with synthetic numbers. The trend signals need no outside source."""

    index = pd.bdate_range("1986-01-02", "2011-06-30", name="date")
    rng = np.random.default_rng(7)
    regime = np.where((index.year >= 2000) & (index.year <= 2002), 0.03, 0.012)
    r = rng.normal(0.0004, 1.0, len(index)) * regime
    level = 1000.0 * np.cumprod(1.0 + r)
    daily = pd.DataFrame(
        {
            "etf_reference_return": np.where(index > QQQ_START, r, np.nan),
            "cash_return": 0.0001,
            "lev2_return": 2 * r,
            "lev3_return": 3 * r,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": 20.0,
            "vxn": 25.0,
            "vxo": 21.0,
            "cash_rate_annual": 0.03,
        },
        index=index,
    )
    return from_daily_frame(
        daily, qqq_series="actual", source={"snapshot_id": PREREG.data.snapshot_id}
    )


DATA = _data()


def test_the_preregistration_is_complete_and_its_rules_can_be_met() -> None:
    assert PREREG.question_id == "QQQ-Q5" and PREREG.question_type == "qqq_direction"
    assert PREREG.owner_decision == DECISION
    assert PREREG.data.snapshot_id == "qlh-20261013-0b0df53cb1a2"
    assert PREREG.data.qqq_series == "actual"
    assert PREREG.data.signal_snapshot_id is None
    # Classic trend signals are fixed baselines, not screened: no screening record is cited.
    assert PREREG.screens == () and PREREG.screen_family_size == 17
    assert PREREG.validation.strategies == MAIN
    assert PREREG.validation.confidence == 0.95
    assert rule_problems(PREREG, LOADED.settings) == []
    assert tuple(entry.id for entry in PREREG.strategies) == MAIN
    assert PREREG.trial_cap == 2 and PREREG.sensitivity == ()
    assert PREREG.sample_floor.min_return_days == 2000
    assert PREREG.prior_expectation.sources
    for entry in PREREG.strategies:
        assert callable(lg.load_strategy(QUESTION, entry.id))
        assert entry.leveraged_benchmarks is False, entry.id


def test_the_validated_strategy_faces_the_trial_gates_two_direction_rules() -> None:
    gate = _yaml("research", "_trials", "prereg.yaml")
    expected = [
        (rule["metric"], rule["op"], rule["value"]) for rule in gate["markets"]["qqq"]["rules"]
    ]
    assert expected == [
        ("ci_high.cvar_5pct_relative_improvement", ">", 0.0),
        ("comparison.geometric_annual_return", ">=", -0.010),
    ]
    assert gate["confidence"] == PREREG.validation.confidence
    rules = PREREG.validation.rules["fast_slow_weekly"]
    assert [(rule.metric, rule.op, rule.value) for rule in rules] == expected
    q4 = lg.load_question(PROJECT_ROOT.joinpath("research", "QQQ-Q4")).prereg
    assert rules == q4.validation.rules["dispersion"]


def test_the_pinned_signals_are_the_files_on_disk() -> None:
    for entry in PREREG.strategies:
        for ref in entry.params["signals"]:
            loaded = screen.load_signal(ref["id"], directory=screen.SIGNALS_DIR)
            assert (loaded.module_sha256, loaded.spec_sha256) == (
                ref["module_sha256"],
                ref["spec_sha256"],
            ), (entry.id, ref["id"])


def test_the_strategy_is_qqq_q3s_weekly_variant_unchanged() -> None:
    weekly = PREREG.strategy("fast_slow_weekly")
    assert weekly == Q3.prereg.strategy("fast_slow_weekly")
    monthly = Q3.prereg.strategy("fast_slow")
    assert weekly.function == monthly.function == "signal_exposure"
    assert weekly.params == monthly.params
    assert [(ref["id"], ref["score"]) for ref in weekly.params["signals"]] == [
        ("tsmom_21", "sign"),
        ("tsmom_252", "sign"),
    ]
    assert (weekly.params["mapping"], weekly.params["low"], weekly.params["high"]) == (
        "linear",
        0,
        1,
    )
    assert (weekly.rebalance.schedule, monthly.rebalance.schedule) == ("week_end", "month_end")
    assert weekly.rebalance.band == monthly.rebalance.band == 0.0
    assert weekly.delay_days == monthly.delay_days == 0
    # The strategy module is the same generic adapter as QQQ-Q3's.
    assert lg.load_strategy(QUESTION, "fast_slow_weekly") is lg.load_strategy(Q3, "fast_slow")


def test_the_monthly_reference_is_not_run_again() -> None:
    # QQQ-Q3's validation window is used up for the monthly rule: Q5 cites its published result.
    assert "fast_slow" not in {entry.id for entry in PREREG.strategies}
    decision = Q3_DIR.joinpath("decision.md").read_text(encoding="utf-8")
    assert "`QQQ-Q3-r0008`" in decision and "+4.4%（−6.5..+14.3）" in decision


def test_the_decision_is_in_the_requirements() -> None:
    text = PROJECT_ROOT.joinpath(
        "docs", "requirements", "TRADING-2567_QQQ_Research_Program_1999_Window.md"
    ).read_text(encoding="utf-8")
    assert f"- `{DECISION}`" in text


def test_the_trials_readme_says_why_there_is_no_trial_plan() -> None:
    text = PROJECT_ROOT.joinpath("research", "_trials", "README.md").read_text(encoding="utf-8")
    assert "## 不走试跑的正式题（QQQ-Q5）" in text and DECISION in text
    plans = sorted(
        path.name for path in PROJECT_ROOT.joinpath("research", "_trials").glob("T*.yaml")
    )
    for name in plans:
        assert "qqq_q5_prereg" not in _yaml("research", "_trials", name).get("owner_decision", "")


def test_the_strategy_runs_on_the_development_window_like_the_qqq_q3_variant(
    tmp_path: Path,
) -> None:
    directory = tmp_path.joinpath("QQQ-Q5")
    shutil.copytree(QUESTION_DIR, directory)
    question = lg.load_question(directory)
    recorded = run_and_record(question, DATA, "fast_slow_weekly", loaded=LOADED)
    assert recorded.record["status"] == "ok"
    result = recorded.record["result"]
    assert sorted(result["checks"]["parameters_unchecked"]) == ["mapping", "signals"]
    assert 0.0 < result["strategy"]["average_exposure"] <= 1.0
    q3_directory = tmp_path.joinpath("QQQ-Q3")
    shutil.copytree(Q3_DIR, q3_directory, ignore=shutil.ignore_patterns("runs", "*.jsonl"))
    reference = run_and_record(
        lg.load_question(q3_directory), DATA, "fast_slow_weekly", loaded=LOADED
    ).record["result"]
    # The same configuration as QQQ-Q3's variant: the same numbers and the same fingerprint.
    for section in ("strategy", "benchmarks", "comparison", "exposure_gap", "events"):
        same = json.dumps(result[section], sort_keys=True) == json.dumps(
            reference[section], sort_keys=True
        )
        assert same, section
    assert result["fingerprint"] == reference["fingerprint"]


def test_the_exposures_are_the_mapping_of_the_scores() -> None:
    inputs = DATA.inputs
    params = PREREG.strategy("fast_slow_weekly").params
    exposure = lg.load_strategy(QUESTION, "fast_slow_weekly")(inputs, params)
    valid = exposure.dropna()
    assert len(valid) > 5000
    assert {round(float(value), 12) for value in np.unique(valid)} == {0.0, 0.5, 1.0}
    # Daily targets are the mean of the two sign scores; only the schedule differs from monthly.
    returns = inputs["ndx_return"]
    scores = []
    for window in (21, 252):
        growth = np.log1p(returns).rolling(window, min_periods=window).sum()
        scores.append(
            pd.Series(
                np.where(growth > 0, 1.0, np.where(growth < 0, 0.0, 0.5)), index=inputs.index
            ).mask(growth.isna())
        )
    expected = pd.concat(scores, axis=1).mean(axis=1, skipna=False).ffill()
    both = expected.notna() & exposure.notna()
    assert both.sum() > 5000
    np.testing.assert_array_equal(exposure[both].to_numpy(), expected[both].to_numpy())


# The record lines are appended, never edited: earlier sentences stay where they are.
README = PROJECT_ROOT / "research/README.md"
CARD = PROJECT_ROOT / "tasks/TRADING-2567.yaml"
REGISTER = PROJECT_ROOT / "docs/task_register.md"
README_Q4_ROW = (
    "| `QQQ-Q4/` | 第四题（已结束，试跑 T0004 转正式题）：结论 `decision.md`；预注册 `prereg.yaml` "
    "和策略模块 `strategy.py`（都在 C 区）；运行记录在 `runs/`，验证窗口的读取记录在 `window_r"
    "eads.jsonl` |"
)
README_ROW = (
    "| `QQQ-Q5/` | 第五题（已结束，QQQ-Q3 的 fast_slow 改每周调仓，没有试跑计划）：结论 `decision.m"
    "d`；预注册 `prereg.yaml` 和策略模块 `strategy.py`（都在 C 区）；运行记录在 `runs/`，验证窗口的"
    "读取记录在 `window_reads.jsonl` |"
)
README_BULLET = (
    "- 第五题 `QQQ-Q5/` 已结束（2026-10-21）：QQQ-Q3 的 fast_slow 改每周调仓，证据不足，评级 B（软"
    "泄漏，降一档），见 `QQQ-Q5/decision.md`。左尾改善从月频的 +4.4% 到 +8.0%，几何年化每年少 2.9 "
    "个点（月频 1.8）；十条降风险规则在 2011–2021 年都没有以每年少不到 1 个点的代价改善左尾。失败标"
    "签「有效果但代价太高」加「软泄漏可疑」，Baker 2026-10-21 同意；经典趋势家族保持关闭，按预注册"
    "实现层到此为止，第 2 期剩 P1，见 `families.md`。"
)
NOTES_TAIL = (
    "；2026-10-21：Baker 选研究自动循环第 2 期第一批卡 H14（QQQ-Q3 的 fast_slow 改每周调仓）开题为"
    "第五题 QQQ-Q5（owner_decision:TRADING-2567:2026-10-21:qqq_q5_prereg）：只有 fast_slow_weekly "
    "一个配置，月频 fast_slow 用已发布的验证读取数作参照，验证窗口只查方向；TRADING-2567-36 记决定"
    "，TRADING-2567-37 开题，`research/QQQ-Q5/prereg.yaml` 和 `strategy.py` 进 main。production_ef"
    "fect=none；broker_action=none"
)
NOTES_CLOSE = (
    "；2026-10-21：QQQ-Q5 结题（读取 QQQ-Q5-w0001）：fast_slow_weekly 证据不足（左尾 +8.0%，几何每"
    "年少 2.9 个点），评级 B；Baker 同意结论和失败标签「有效果但代价太高；软泄漏可疑」，TRADING-256"
    "7-38 记 decision.md、台账第 27 条、families 和 scorecard。production_effect=none；broker_actio"
    "n=none"
)
# The card's step after the card review (TRADING-2567-39); the notes keep growing at the end.
NOTES_REVIEW = (
    "；2026-10-25：卡片整理（工作进展展示线程的在办卡核对，TRADING-2567"
    "-39 记）：next_owner 和 next_step 还停在第 2 期（P1 组合"
    "尺子），改成现在的样子：第 2 期到第 6 期都过了，QQQ-Q5 之后没有开着的 QQ"
    "Q 题，第 6 期只记账到 2026-11-20，下一道正式题等续期卡或有候选过试跑门。"
    "状态仍是 IN_PROGRESS，只改这两格和这条记录。production_effec"
    "t=none；broker_action=none"
)
# The step after the period-6 section 20 (TRADING-2565-78).
NOTES_P6_20 = (
    "；2026-10-27：第 6 期第 20 节（owner_decision:TRADIN"
    "G-2565:2026-10-27:loop_self_select_signed，TRA"
    "DING-2565-78 记）：研究自动循环在菜单内自选目标，next_owner 和 n"
    "ext_step 跟着改；开正式题仍要问 Baker。production_effect="
    "none；broker_action=none"
)
NEXT_OWNER = (
    "Claude（研究自动循环第 6 期：第 20 节起菜单内自选目标；"
    "下一道 QQQ 正式题等 11-20 续期卡或候选过试跑门）"
)
NEXT_STEP = (
    "第五题 QQQ-Q5 已结束（2026-10-21，`research/QQQ-Q5/decision.md`，台账第 27 条），"
    "之后没有开着的 QQQ 题。"
    "研究自动循环第 6 期（`research/_loop/charter_p6.md`，到 2026-11-20）只记账，"
    "前瞻账本每周五照记；"
    "第 13、14 节的两个检验力探针（TRADING-2565-61、-63）都是 below_50，不开题。"
    "第 20 节（2026-10-27 签，owner_decision:TRADING-2565:2026-10-27:loop_self_select_signed）"
    "起菜单内的方向过两道线就自己开、自己关"
    "（数据每月 ≤ 100 美元、花费每周 ≤ 250 美元），开正式题仍要问 Baker。"
    "下一道正式题等 2026-11-20 的续期卡，或研究自动循环有候选过试跑门再问 Baker。"
    "详见 `docs/requirements/TRADING-2567_QQQ_Research_Program_1999_Window.md`"
)


def test_the_research_readme_lists_the_fifth_question() -> None:
    lines = README.read_text(encoding="utf-8").splitlines()
    assert lines.count(README_ROW) == 1 and lines.count(README_BULLET) == 1
    assert lines.index(README_ROW) == lines.index(README_Q4_ROW) + 1
    assert lines[lines.index(README_BULLET) - 1].startswith("- 第四题 `QQQ-Q4/` 已结束")


def test_the_task_card_notes_are_only_appended() -> None:
    card = yaml.safe_load(CARD.read_text(encoding="utf-8"))
    assert card["notes"].count(NOTES_TAIL) == 1 and card["notes"].count(NOTES_CLOSE) == 1
    assert card["notes"].endswith(NOTES_TAIL + NOTES_CLOSE + NOTES_REVIEW + NOTES_P6_20)
    assert card["notes"].split(NOTES_TAIL)[0].endswith("broker_action=none")
    assert "qqq_q4_prereg" in card["notes"].split(NOTES_TAIL)[0]
    assert (card["next_owner"], card["next_step"]) == (NEXT_OWNER, NEXT_STEP)
    assert card["status"] == "IN_PROGRESS"
    register = REGISTER.read_text(encoding="utf-8")
    assert NOTES_TAIL in register and NOTES_CLOSE in register and NEXT_STEP in register
    assert NOTES_REVIEW in register and NOTES_P6_20 in register
