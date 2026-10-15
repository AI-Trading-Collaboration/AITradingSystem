"""QQQ-Q3 (TRADING-2567): the preregistration is complete, its signals are the pinned files, every
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
from ai_trading_system.research_bench.data import BenchData, from_daily_frame
from ai_trading_system.research_bench.prereg import rule_problems

LOADED = load_settings()
QUESTION_DIR = PROJECT_ROOT.joinpath("research", "QQQ-Q3")
QUESTION = lg.load_question(QUESTION_DIR)
PREREG = QUESTION.prereg
QQQ_START = pd.Timestamp("1999-03-10")
DECISION = "owner_decision:TRADING-2567:2026-10-15:qqq_q3_prereg"
MAIN = ("fast_slow", "slow_only")
SENSITIVITY = (
    "fast_only",
    "fast_slow_weekly",
    "slow_only_weekly",
    "fast63_slow252",
    "fast21_slow126",
)


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
    assert PREREG.question_id == "QQQ-Q3" and PREREG.question_type == "qqq_direction"
    assert PREREG.owner_decision == DECISION
    assert PREREG.data.snapshot_id == "qlh-20261013-0b0df53cb1a2"
    assert PREREG.data.qqq_series == "actual"
    assert PREREG.data.signal_snapshot_id is None
    assert PREREG.validation.strategies == MAIN
    assert PREREG.validation.confidence == 0.95
    assert rule_problems(PREREG, LOADED.settings) == []
    assert tuple(entry.id for entry in PREREG.strategies) == (*MAIN, *SENSITIVITY)
    assert len(PREREG.strategies) == 7 and PREREG.trial_cap == 14
    assert PREREG.sample_floor.min_return_days == 2000
    assert PREREG.prior_expectation.sources and PREREG.sensitivity
    for entry in PREREG.strategies:
        assert callable(lg.load_strategy(QUESTION, entry.id))


def test_the_two_validated_strategies_face_the_same_eleven_rules() -> None:
    rules = [PREREG.validation.rules[name] for name in MAIN]
    assert rules[0] == rules[1]
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


def _shape(strategy_id: str) -> dict[str, object]:
    params = PREREG.strategy(strategy_id).params
    return {key: params.get(key) for key in ("mapping", "low", "high", "cut")}


def test_the_pinned_signals_are_the_files_on_disk() -> None:
    for entry in PREREG.strategies:
        for ref in entry.params["signals"]:
            loaded = screen.load_signal(ref["id"], directory=screen.SIGNALS_DIR)
            assert (loaded.module_sha256, loaded.spec_sha256) == (
                ref["module_sha256"],
                ref["spec_sha256"],
            ), (entry.id, ref["id"])


def test_the_no_screen_claim_matches_the_classic_signals() -> None:
    # Classic trend signals are fixed baselines, not screened: no screening record is cited.
    assert PREREG.screens == () and PREREG.screen_family_size == 4


def test_the_strategies_are_the_designs_in_the_decision() -> None:
    both = PREREG.strategy("fast_slow").params
    assert [ref["id"] for ref in both["signals"]] == ["tsmom_21", "tsmom_252"]
    assert {ref["score"] for ref in both["signals"]} == {"sign"}
    assert _shape("fast_slow") == {"mapping": "linear", "low": 0, "high": 1, "cut": None}
    slow = PREREG.strategy("slow_only").params
    assert [(ref["id"], ref["score"]) for ref in slow["signals"]] == [("tsmom_252", "sign")]
    assert _shape("slow_only") == {"mapping": "threshold", "low": 0, "high": 1, "cut": None}
    assert {name: PREREG.strategy(name).rebalance.schedule for name in MAIN} == {
        "fast_slow": "month_end",
        "slow_only": "month_end",
    }
    for entry in PREREG.strategies:
        assert entry.rebalance.band == 0.0 and entry.delay_days == 0, entry.id


@pytest.mark.parametrize(
    ("variant", "base"),
    [("fast_slow_weekly", "fast_slow"), ("slow_only_weekly", "slow_only")],
)
def test_the_weekly_variants_change_only_the_schedule(variant: str, base: str) -> None:
    assert PREREG.strategy(variant).params == PREREG.strategy(base).params
    assert PREREG.strategy(variant).rebalance.schedule == "week_end"
    assert PREREG.strategy(variant).delay_days == 0


def test_fast_only_keeps_only_the_fast_signal() -> None:
    assert _signals("fast_only") == _signals("fast_slow")[:1]
    assert _signals("fast_only")[0]["id"] == "tsmom_21"
    assert _shape("fast_only") == _shape("slow_only")
    assert PREREG.strategy("fast_only").rebalance.schedule == "month_end"


@pytest.mark.parametrize(
    ("variant", "position", "replaced_by"),
    [("fast63_slow252", 0, "tsmom_63"), ("fast21_slow126", 1, "tsmom_126")],
)
def test_each_lookback_variant_replaces_one_signal_of_fast_slow(
    variant: str, position: int, replaced_by: str
) -> None:
    base_ids = [ref["id"] for ref in _signals("fast_slow")]
    expected = [*base_ids]
    expected[position] = replaced_by
    assert [ref["id"] for ref in _signals(variant)] == expected
    assert {ref["score"] for ref in _signals(variant)} == {"sign"}
    assert _shape(variant) == _shape("fast_slow")
    assert PREREG.strategy(variant).rebalance.schedule == "month_end"


def test_the_decision_is_in_the_requirements() -> None:
    text = PROJECT_ROOT.joinpath(
        "docs", "requirements", "TRADING-2567_QQQ_Research_Program_1999_Window.md"
    ).read_text(encoding="utf-8")
    assert f"- `{DECISION}`" in text


def test_every_strategy_runs_on_the_development_window_with_every_parameter_read(
    tmp_path: Path,
) -> None:
    directory = tmp_path.joinpath("QQQ-Q3")
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
        "fast_slow": {0.0, 0.5, 1.0},
        "fast_slow_weekly": {0.0, 0.5, 1.0},
        "fast63_slow252": {0.0, 0.5, 1.0},
        "fast21_slow126": {0.0, 0.5, 1.0},
        "slow_only": {0.0, 1.0},
        "slow_only_weekly": {0.0, 1.0},
        "fast_only": {0.0, 1.0},
    }
    for name, allowed in levels.items():
        strategy = lg.load_strategy(QUESTION, name)
        exposure = strategy(inputs, PREREG.strategy(name).params).dropna()
        assert len(exposure) > 5000, name
        seen = {round(float(value), 12) for value in np.unique(exposure)}
        assert seen <= allowed, name
        assert len(seen) >= 2, name
    # The half position is the disagreement of the two signals, so it must occur.
    both = lg.load_strategy(QUESTION, "fast_slow")(inputs, PREREG.strategy("fast_slow").params)
    assert 0.5 in {round(float(value), 12) for value in both.dropna()}


# The record lines are appended, never edited: earlier sentences stay where they are.
README = PROJECT_ROOT / "research/README.md"
CARD = PROJECT_ROOT / "tasks/TRADING-2567.yaml"
REGISTER = PROJECT_ROOT / "docs/task_register.md"
README_Q2_BULLET = (
    "- 第二题 `QQQ-Q2/` 已结束（2026-10-15）：Q2a（四个回看期趋势）证据不足，参"
    "照 10 个月均线和 Q2b（信用利差门）拒绝，见 `QQQ-Q2/decision.md`。三条降风"
    "险规则在 2011–2021 年都没有胜过同平均敞口的固定仓位；加上第一题，五条降风"
    "险规则在开发窗口都有效，在验证窗口都不成立。"
)
README_Q2_ROW = (
    "| `QQQ-Q2/` | 第二题（已结束）：结论 `decision.md`；预注册 `prereg.yaml` "
    "和策略模块 `strategy.py`（都在 C 区）；运行记录在 `runs/`，验证窗口的读取"
    "记录在 `window_reads.jsonl` |"
)
README_BULLET = (
    "- 第三题 `QQQ-Q3/` 已结束（2026-10-15）：Q3（21 日和 252 日趋势信号，分歧时减半）证"
    "据不足，参照只看十二个月信号拒绝，见 `QQQ-Q3/decision.md`。它是 Q2 `trend4` 的子集，"
    "结果不是独立于 Q2a 的证据；七条降风险规则在 2011–2021 年都没有胜过同平均敞口的固定仓"
    "位。"
)
README_ROW = (
    "| `QQQ-Q3/` | 第三题（已结束）：结论 `decision.md`；预注册 `prereg.yaml` 和策略模块 "
    "`strategy.py`（都在 C 区）；运行记录在 `runs/`，验证窗口的读取记录在 `window_reads.j"
    "sonl` |"
)
NOTES_TAIL = (
    "；2026-10-15：TRADING-2567-24 开第三题 QQQ-Q3：`research/QQQ-Q3/prereg.yam"
    "l` 和 `strategy.py` 带着信号模块（TRADING-2565-11）的哈希进 main，按 owner"
    "_decision:TRADING-2567:2026-10-15:qqq_q3_prereg 的记录批准；下一步开发窗口"
    "运行。production_effect=none；broker_action=none"
)


def test_the_research_readme_lists_the_third_question() -> None:
    lines = README.read_text(encoding="utf-8").splitlines()
    assert lines.count(README_BULLET) == 1 and lines.count(README_ROW) == 1
    assert lines.index(README_BULLET) > lines.index(README_Q2_BULLET)
    assert lines.index(README_ROW) == lines.index(README_Q2_ROW) + 1


def test_the_task_card_notes_are_only_appended() -> None:
    card = yaml.safe_load(CARD.read_text(encoding="utf-8"))
    assert card["notes"].count(NOTES_TAIL) == 1
    assert card["notes"].split(NOTES_TAIL)[0].endswith("broker_action=none")
    assert "qqq_q3_prereg" in card["notes"].split(NOTES_TAIL)[0]
    assert card["status"] == "IN_PROGRESS"
    assert NOTES_TAIL in REGISTER.read_text(encoding="utf-8")
