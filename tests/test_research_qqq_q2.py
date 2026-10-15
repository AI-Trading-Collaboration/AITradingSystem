"""QQQ-Q2 (TRADING-2567): the preregistration is complete, its signals are the pinned files, every
strategy runs on the development window with every parameter read, and the exposures are the
ones the mapping defines. Synthetic data only."""

from __future__ import annotations

import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings, run_and_record, screen
from ai_trading_system.research_bench.data import BenchData, from_daily_frame, with_signal_sources
from ai_trading_system.research_bench.prereg import rule_problems
from ai_trading_system.research_bench.screen_ledger import screen_records

LOADED = load_settings()
QUESTION_DIR = PROJECT_ROOT.joinpath("research", "QQQ-Q2")
QUESTION = lg.load_question(QUESTION_DIR)
PREREG = QUESTION.prereg
QQQ_START = pd.Timestamp("1999-03-10")
DECISION = "owner_decision:TRADING-2567:2026-10-15:qqq_q2_prereg"
MAIN = ("trend4", "ma10", "credit_gate")
SENSITIVITY = (
    "trend4_weekly",
    "trend3_long",
    "ma10_weekly",
    "credit_gate_top10",
    "credit_gate_top30",
    "credit_gate_low25",
    "credit_gate_weekly",
)
MOMENTUM = ("tsmom_21", "tsmom_63", "tsmom_126", "tsmom_252")


def _data() -> BenchData:
    """Price index from 1986, the QQQ series from its first trading day, a spread series from
    1986: the shapes of the snapshot, with synthetic numbers."""

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
    data = from_daily_frame(
        daily, qqq_series="actual", source={"snapshot_id": PREREG.data.snapshot_id}
    )
    spread = pd.DataFrame(
        {"value": 2.5 + np.cumsum(rng.normal(0.0, 0.04, len(index)))}, index=index
    )
    assert PREREG.data.signal_snapshot_id is not None
    return with_signal_sources(
        data, {"baa10y": spread}, {"baa10y": 1}, snapshot_id=PREREG.data.signal_snapshot_id
    )


DATA = _data()


def test_the_preregistration_is_complete_and_its_rules_can_be_met() -> None:
    assert PREREG.question_id == "QQQ-Q2" and PREREG.question_type == "qqq_direction"
    assert PREREG.owner_decision == DECISION
    assert PREREG.data.snapshot_id == "qlh-20261013-0b0df53cb1a2"
    assert PREREG.data.qqq_series == "actual"
    assert PREREG.data.signal_snapshot_id == "sig-1e9873757a84"
    assert PREREG.validation.strategies == MAIN
    assert PREREG.validation.confidence == 0.975
    assert rule_problems(PREREG, LOADED.settings) == []
    assert tuple(entry.id for entry in PREREG.strategies) == (*MAIN, *SENSITIVITY)
    assert len(PREREG.strategies) == 10 and PREREG.trial_cap == 20
    assert PREREG.sample_floor.min_return_days == 2000
    assert PREREG.prior_expectation.sources and PREREG.sensitivity
    for entry in PREREG.strategies:
        assert callable(lg.load_strategy(QUESTION, entry.id))


def test_the_three_validated_strategies_face_the_same_eleven_rules() -> None:
    rules = [PREREG.validation.rules[name] for name in MAIN]
    assert rules[0] == rules[1] == rules[2]
    assert [(rule.metric, rule.op, rule.value) for rule in rules[0]] == [
        ("comparison.cvar_5pct_relative_improvement", ">=", 0.10),
        ("ci_low.cvar_5pct_relative_improvement", ">", 0.0),
        ("comparison.max_drawdown", ">=", 0.05),
        ("comparison.geometric_annual_return", ">=", -0.010),
        ("cost_25.geometric_annual_return", ">=", -0.010),
        ("delay_2.cvar_5pct_relative_improvement", ">=", 0.10),
        ("leave_year_min.cvar_5pct_relative_improvement", ">", 0.0),
        ("leave_year_min.max_drawdown", ">", 0.0),
        ("leave_episode_min.cvar_5pct_relative_improvement", ">", 0.0),
        ("leave_episode_min.max_drawdown", ">", 0.0),
        ("placebo_rank.cvar_5pct_relative_improvement", ">=", 0.95),
    ]


def _signals(strategy_id: str) -> list[dict[str, str]]:
    return list(PREREG.strategy(strategy_id).params["signals"])


def test_the_pinned_signals_are_the_files_on_disk() -> None:
    for entry in PREREG.strategies:
        for ref in entry.params["signals"]:
            loaded = screen.load_signal(ref["id"], directory=screen.SIGNALS_DIR)
            assert (loaded.module_sha256, loaded.spec_sha256) == (
                ref["module_sha256"],
                ref["spec_sha256"],
            ), (entry.id, ref["id"])


def test_the_strategies_are_the_designs_in_the_decision() -> None:
    trend = PREREG.strategy("trend4").params
    assert [ref["id"] for ref in trend["signals"]] == list(MOMENTUM)
    assert {ref["score"] for ref in trend["signals"]} == {"sign"}
    assert {key: trend[key] for key in ("mapping", "low", "high")} == {
        "mapping": "linear",
        "low": 0,
        "high": 1,
    }
    ma = PREREG.strategy("ma10").params
    assert [(ref["id"], ref["score"]) for ref in ma["signals"]] == [("ma_distance_210", "sign")]
    assert (ma["mapping"], ma["low"], ma["high"], "cut" in ma) == ("threshold", 0, 1, False)
    gate = PREREG.strategy("credit_gate").params
    assert [(ref["id"], ref["score"]) for ref in gate["signals"]] == [
        ("credit_spread_level", "percentile")
    ]
    assert {key: gate[key] for key in ("mapping", "low", "high", "cut")} == {
        "mapping": "threshold",
        "low": 0.5,
        "high": 1.0,
        "cut": 0.2,
    }
    assert {name: PREREG.strategy(name).rebalance.schedule for name in MAIN} == {
        "trend4": "month_end",
        "ma10": "month_end",
        "credit_gate": "month_end",
    }
    for entry in PREREG.strategies:
        assert entry.rebalance.band == 0.0 and entry.delay_days == 0, entry.id


@pytest.mark.parametrize(
    ("variant", "base", "changes", "schedule"),
    [
        ("trend4_weekly", "trend4", {}, "week_end"),
        ("ma10_weekly", "ma10", {}, "week_end"),
        ("credit_gate_weekly", "credit_gate", {}, "week_end"),
        ("credit_gate_top10", "credit_gate", {"cut": 0.1}, "month_end"),
        ("credit_gate_top30", "credit_gate", {"cut": 0.3}, "month_end"),
        ("credit_gate_low25", "credit_gate", {"low": 0.25}, "month_end"),
    ],
)
def test_each_sensitivity_changes_one_thing_from_its_base(
    variant: str, base: str, changes: dict[str, float], schedule: str
) -> None:
    expected = {**PREREG.strategy(base).params, **changes}
    assert PREREG.strategy(variant).params == expected
    assert PREREG.strategy(variant).rebalance.schedule == schedule
    assert PREREG.strategy(variant).delay_days == 0


def test_trend3_long_drops_only_the_one_month_lookback() -> None:
    expected = [ref for ref in _signals("trend4") if ref["id"] != "tsmom_21"]
    assert _signals("trend3_long") == expected
    base = PREREG.strategy("trend4").params
    changed = PREREG.strategy("trend3_long").params
    assert {key: changed[key] for key in ("mapping", "low", "high")} == {
        key: base[key] for key in ("mapping", "low", "high")
    }


def test_the_screen_it_rests_on_is_in_the_screening_ledger() -> None:
    assert PREREG.screens == ("S0002",) and PREREG.screen_family_size == 4
    records = {record["screen_id"]: record for record in screen_records(PROJECT_ROOT)}
    assert records["S0002"]["signal_id"] == "credit_spread_level"
    gate_ref = _signals("credit_gate")[0]
    assert gate_ref["module_sha256"] == records["S0002"]["module_sha256"]
    assert gate_ref["spec_sha256"] == records["S0002"]["spec_sha256"]


def test_the_decision_is_in_the_requirements() -> None:
    text = PROJECT_ROOT.joinpath(
        "docs", "requirements", "TRADING-2567_QQQ_Research_Program_1999_Window.md"
    ).read_text(encoding="utf-8")
    assert f"- `{DECISION}`" in text


def test_every_strategy_runs_on_the_development_window_with_every_parameter_read(
    tmp_path: Path,
) -> None:
    directory = tmp_path.joinpath("QQQ-Q2")
    shutil.copytree(QUESTION_DIR, directory)
    question = lg.load_question(directory)
    for entry in question.prereg.strategies:
        recorded = run_and_record(question, DATA, entry.id, loaded=LOADED)
        assert recorded.record["status"] == "ok", entry.id
        checks = recorded.record["result"]["checks"]
        assert sorted(checks["parameters_unchecked"]) == ["mapping", "signals"], entry.id
        exposure = recorded.record["result"]["strategy"]["average_exposure"]
        assert 0.0 < exposure <= 1.0, entry.id


def test_the_exposures_are_the_mapping_of_the_scores() -> None:
    inputs = DATA.inputs
    levels = {
        "trend4": {0.0, 0.25, 0.5, 0.75, 1.0},
        "trend3_long": {0.0, 1 / 3, 2 / 3, 1.0},
        "ma10": {0.0, 1.0},
        "credit_gate": {0.5, 1.0},
        "credit_gate_low25": {0.25, 1.0},
    }
    for name, allowed in levels.items():
        strategy = lg.load_strategy(QUESTION, name)
        exposure = strategy(inputs, PREREG.strategy(name).params).dropna()
        assert len(exposure) > 5000, name
        assert all(
            any(abs(value - level) < 1e-12 for level in allowed) for value in np.unique(exposure)
        ), name
        assert len(np.unique(exposure)) >= 2, name


# The record lines are appended, never edited: earlier sentences stay where they are.
README = PROJECT_ROOT / "research/README.md"
CARD = PROJECT_ROOT / "tasks/TRADING-2567.yaml"
REGISTER = PROJECT_ROOT / "docs/task_register.md"
README_HEADING_OLD = "## 现在的状态（2026-10-14）"
README_HEADING = "## 现在的状态（2026-10-15）"
README_Q1_BULLET = (
    "- 第一题 `QQQ-Q1/` 已结束（2026-10-14）：Q1a 证据不足，Q1b 拒绝，见 `QQQ-Q1/decision.md`。"
    "按波动降仓有信息，但在 2011–2021 年不足以胜过同平均敞口的固定仓位。"
)
README_BULLET = (
    "- 第二题 `QQQ-Q2/` 已结束（2026-10-15）：Q2a（四个回看期趋势）证据不足，参照 10 个月均线和 Q2b"
    "（信用利差门）拒绝，见 `QQQ-Q2/decision.md`。三条降风险规则在 2011–2021 年都没有胜过"
    "同平均敞口的固定仓位；加上第一题，五条降风险规则在开发窗口都有效，在验证窗口都不成立。"
)
README_Q1_ROW = (
    "| `QQQ-Q1/` | 第一题（已结束）：结论 `decision.md`；预注册 `prereg.yaml` 和策略模块 "
    "`strategy.py`（都在 C 区）；运行记录在 `runs/`，验证窗口的读取记录在 `window_reads.jsonl` |"
)
README_ROW = (
    "| `QQQ-Q2/` | 第二题（已结束）：结论 `decision.md`；预注册 `prereg.yaml` 和策略模块 "
    "`strategy.py`（都在 C 区）；运行记录在 `runs/`，验证窗口的读取记录在 `window_reads.jsonl` |"
)
NOTES_TAIL = (
    "；2026-10-15：TRADING-2567-14 开第二题 QQQ-Q2：`research/QQQ-Q2/prereg.yaml` 和 `strategy.py`"
    " 带着信号模块（TRADING-2565-11）的哈希进 main，按 owner_decision:TRADING-2567:2026-10-15:"
    "qqq_q2_prereg 的记录批准；下一步开发窗口运行。production_effect=none；broker_action=none"
)
NEXT_OWNER = "Claude（第二题 QQQ-Q2：开发窗口运行的指令，核对实现后先告诉 Baker 再读验证窗口）"
NEXT_STEP = (
    "第二题 QQQ-Q2 已开题（预注册和策略模块在 main）。下一步：研究会话跑开发窗口（只检查实现，"
    "Baker 把指令贴进研究线程），Claude 核对实现没有问题后先告诉 Baker，再单独一次运行读验证窗口，"
    "不等回复。详见 `docs/requirements/TRADING-2567_QQQ_Research_Program_1999_Window.md`"
)


def test_the_research_readme_lists_the_second_question() -> None:
    text = README.read_text(encoding="utf-8")
    lines = text.splitlines()
    assert README_HEADING in lines and README_HEADING_OLD not in lines
    assert lines.count(README_BULLET) == 1 and lines.count(README_ROW) == 1
    assert lines.index(README_BULLET) == lines.index(README_Q1_BULLET) + 1
    assert lines.index(README_ROW) == lines.index(README_Q1_ROW) + 1


def test_the_task_card_notes_are_only_appended() -> None:
    card = yaml.safe_load(CARD.read_text(encoding="utf-8"))
    assert card["notes"].count(NOTES_TAIL) == 1
    assert card["notes"].split(NOTES_TAIL)[0].endswith("broker_action=none")
    assert "qqq_q2_prereg" in card["notes"].split(NOTES_TAIL)[0]
    assert card["status"] == "IN_PROGRESS"
    assert NOTES_TAIL in REGISTER.read_text(encoding="utf-8")
