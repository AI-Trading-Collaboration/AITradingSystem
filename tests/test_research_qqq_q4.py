"""QQQ-Q4 (TRADING-2567): the preregistration is complete, its rules are the trial gate's direction
rules, its deciding strategy and sensitivity are trial T0004's configurations unchanged, its signals
are the pinned files, every strategy runs on the development window with every parameter read, and
the exposures are the ones the mapping defines. Synthetic data only."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings, run_and_record, screen
from ai_trading_system.research_bench.data import BenchData, from_daily_frame, with_signal_sources
from ai_trading_system.research_bench.prereg import rule_problems
from ai_trading_system.research_bench.screen_settings import load_screen_settings

LOADED = load_settings()
QUESTION_DIR = PROJECT_ROOT.joinpath("research", "QQQ-Q4")
QUESTION = lg.load_question(QUESTION_DIR)
PREREG = QUESTION.prereg
QQQ_START = pd.Timestamp("1999-03-10")
DECISION = "owner_decision:TRADING-2567:2026-10-19:qqq_q4_prereg"
INDUSTRIES = "ff_49_industries_daily"
MAIN = ("dispersion", "rv21")
SENSITIVITY = ("dispersion_threshold", "dispersion_floor_half", "dispersion_weekly")
# Trial T0004's configurations and the Q4 strategies that copy them.
FROM_T0004 = {
    "primary": "dispersion",
    "threshold": "dispersion_threshold",
    "floor_half": "dispersion_floor_half",
    "weekly": "dispersion_weekly",
}


def _yaml(*parts: str) -> dict[str, Any]:
    loaded: dict[str, Any] = yaml.safe_load(
        PROJECT_ROOT.joinpath(*parts).read_text(encoding="utf-8")
    )
    return loaded


def _data() -> BenchData:
    """Price index from 1986, the QQQ series from its first trading day and 12 industry return
    series from 1986: the shapes of the snapshots, with synthetic numbers. Industry returns spread
    apart in the turbulent stretch 2000-2002, when the index is also more volatile."""

    index = pd.bdate_range("1986-01-02", "2011-06-30", name="date")
    rng = np.random.default_rng(7)
    turbulent = (index.year >= 2000) & (index.year <= 2002)
    regime = np.where(turbulent, 0.03, 0.012)
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
    spread = np.where(turbulent, 0.02, 0.008)[:, None]
    industries = pd.DataFrame(
        r[:, None] + rng.normal(0.0, 1.0, (len(index), 12)) * spread,
        index=index,
        columns=[f"ind{c:02d}" for c in range(12)],
    )
    assert PREREG.data.signal_snapshot_id is not None
    return with_signal_sources(
        data, {INDUSTRIES: industries}, {INDUSTRIES: 1}, snapshot_id=PREREG.data.signal_snapshot_id
    )


DATA = _data()


def test_the_preregistration_is_complete_and_its_rules_can_be_met() -> None:
    assert PREREG.question_id == "QQQ-Q4" and PREREG.question_type == "qqq_direction"
    assert PREREG.owner_decision == DECISION
    assert PREREG.data.snapshot_id == "qlh-20261013-0b0df53cb1a2"
    assert PREREG.data.qqq_series == "actual"
    assert PREREG.data.signal_snapshot_id == "sig-7ad8aa00b736"
    assert PREREG.screens == ("S0013",) and PREREG.screen_family_size == 15
    assert PREREG.validation.strategies == MAIN
    assert PREREG.validation.confidence == 0.95
    assert rule_problems(PREREG, LOADED.settings) == []
    assert tuple(entry.id for entry in PREREG.strategies) == (*MAIN, *SENSITIVITY)
    assert len(PREREG.strategies) == 5 and PREREG.trial_cap == 10
    assert PREREG.sample_floor.min_return_days == 2000
    assert PREREG.prior_expectation.sources and len(PREREG.sensitivity) == 3
    for entry in PREREG.strategies:
        assert callable(lg.load_strategy(QUESTION, entry.id))
        assert entry.rebalance.band == 0.0 and entry.delay_days == 0, entry.id
        assert entry.leveraged_benchmarks is False, entry.id


def test_both_validated_strategies_face_the_trial_gates_two_direction_rules() -> None:
    gate = _yaml("research", "_trials", "prereg.yaml")
    expected = [
        (rule["metric"], rule["op"], rule["value"]) for rule in gate["markets"]["qqq"]["rules"]
    ]
    assert expected == [
        ("ci_high.cvar_5pct_relative_improvement", ">", 0.0),
        ("comparison.geometric_annual_return", ">=", -0.010),
    ]
    assert gate["confidence"] == PREREG.validation.confidence
    for name in MAIN:
        rules = PREREG.validation.rules[name]
        assert [(rule.metric, rule.op, rule.value) for rule in rules] == expected, name


def test_the_pinned_signals_are_the_files_on_disk() -> None:
    for entry in PREREG.strategies:
        for ref in entry.params["signals"]:
            loaded = screen.load_signal(ref["id"], directory=screen.SIGNALS_DIR)
            assert (loaded.module_sha256, loaded.spec_sha256) == (
                ref["module_sha256"],
                ref["spec_sha256"],
            ), (entry.id, ref["id"])


def test_dispersion_and_its_sensitivity_are_t0004s_configurations_unchanged() -> None:
    plan = _yaml("research", "_trials", "T0004.yaml")
    assert plan["candidate"] == {"signals": ["industry_dispersion_21"], "screens": ["S0013"]}
    assert plan["data"] == {
        "snapshot_id": PREREG.data.snapshot_id,
        "signal_snapshot_id": PREREG.data.signal_snapshot_id,
    }
    configs = {config["id"]: config for config in plan["configs"]}
    assert set(configs) == set(FROM_T0004)
    for config_id, strategy_id in FROM_T0004.items():
        entry = PREREG.strategy(strategy_id)
        assert entry.function == configs[config_id]["function"] == "signal_exposure"
        assert entry.params == configs[config_id]["params"], strategy_id
        assert entry.rebalance.schedule == configs[config_id]["rebalance"]["schedule"], strategy_id
        assert entry.rebalance.band == configs[config_id]["rebalance"]["band"], strategy_id


def test_rv21_is_dispersion_with_the_realized_volatility_signal() -> None:
    dispersion = PREREG.strategy("dispersion").params
    rv21 = PREREG.strategy("rv21").params
    assert {key: value for key, value in rv21.items() if key != "signals"} == {
        key: value for key, value in dispersion.items() if key != "signals"
    }
    assert [(ref["id"], ref["score"]) for ref in rv21["signals"]] == [
        ("realized_vol_21", "percentile")
    ]
    assert [(ref["id"], ref["score"]) for ref in dispersion["signals"]] == [
        ("industry_dispersion_21", "percentile")
    ]
    assert PREREG.strategy("rv21").rebalance == PREREG.strategy("dispersion").rebalance


def test_the_decision_is_in_the_requirements() -> None:
    text = PROJECT_ROOT.joinpath(
        "docs", "requirements", "TRADING-2567_QQQ_Research_Program_1999_Window.md"
    ).read_text(encoding="utf-8")
    assert f"- `{DECISION}`" in text


def test_every_strategy_runs_on_the_development_window_with_every_parameter_read(
    tmp_path: Path,
) -> None:
    directory = tmp_path.joinpath("QQQ-Q4")
    shutil.copytree(QUESTION_DIR, directory)
    question = lg.load_question(directory)
    for entry in question.prereg.strategies:
        recorded = run_and_record(question, DATA, entry.id, loaded=LOADED)
        assert recorded.record["status"] == "ok", entry.id
        checks = recorded.record["result"]["checks"]
        assert sorted(checks["parameters_unchecked"]) == ["mapping", "signals"], entry.id
        exposure = recorded.record["result"]["strategy"]["average_exposure"]
        assert 0.0 < exposure < 1.0, entry.id


def _oriented_percentile(signal_id: str) -> pd.Series:
    loaded = screen.load_signal(signal_id, directory=screen.SIGNALS_DIR)
    prefix = f"source:{INDUSTRIES}:"
    columns = [column for column in DATA.inputs.columns if column.startswith(prefix)]
    sources = {INDUSTRIES: DATA.inputs[columns].rename(columns=lambda c: c.removeprefix(prefix))}
    values = loaded.function(DATA.inputs["ndx_return"], sources, dict(loaded.spec.params))
    warmup = load_screen_settings().settings.screen.warmup_days
    return 1.0 - screen.expanding_percentile(values, warmup)


def test_the_exposures_are_the_mapping_of_the_scores() -> None:
    inputs = DATA.inputs
    scores = {
        "dispersion": _oriented_percentile("industry_dispersion_21"),
        "rv21": _oriented_percentile("realized_vol_21"),
    }
    for name in ("dispersion", "rv21", "dispersion_weekly"):
        strategy = lg.load_strategy(QUESTION, name)
        exposure = strategy(inputs, PREREG.strategy(name).params)
        score = scores["rv21" if name == "rv21" else "dispersion"]
        valid = score.notna()
        assert valid.sum() > 5000, name
        np.testing.assert_allclose(exposure[valid], score[valid], atol=1e-12)
    threshold = lg.load_strategy(QUESTION, "dispersion_threshold")(
        inputs, PREREG.strategy("dispersion_threshold").params
    )
    valid = scores["dispersion"].notna()
    expected = np.where(scores["dispersion"][valid] >= 0.5, 1.0, 0.0)
    np.testing.assert_array_equal(threshold[valid].to_numpy(), expected)
    floor = lg.load_strategy(QUESTION, "dispersion_floor_half")(
        inputs, PREREG.strategy("dispersion_floor_half").params
    )
    np.testing.assert_allclose(floor[valid], 0.5 + 0.5 * scores["dispersion"][valid], atol=1e-12)
    # Low dispersion is the favoured end: the turbulent stretch carries less exposure.
    dispersion = lg.load_strategy(QUESTION, "dispersion")(
        inputs, PREREG.strategy("dispersion").params
    )
    years = pd.DatetimeIndex(inputs.index).year
    turbulent = (years >= 2000) & (years <= 2002)
    assert dispersion[turbulent].mean() < dispersion[~turbulent & valid].mean() - 0.2


# The record lines are appended, never edited: earlier sentences stay where they are.
README = PROJECT_ROOT / "research/README.md"
CARD = PROJECT_ROOT / "tasks/TRADING-2567.yaml"
REGISTER = PROJECT_ROOT / "docs/task_register.md"
README_Q3_ROW = (
    "| `QQQ-Q3/` | 第三题（已结束）：结论 `decision.md`；预注册 `prereg.yaml` 和策略模块 "
    "`strategy.py`（都在 C 区）；运行记录在 `runs/`，验证窗口的读取记录在 `window_reads.j"
    "sonl` |"
)
README_ROW = (
    "| `QQQ-Q4/` | 第四题（已结束，试跑 T0004 转正式题）：结论 `decision.md`；预注册 `prereg.yaml` "
    "和策略模块 `strategy.py`（都在 C 区）；运行记录在 `runs/`，验证窗口的读取记录在 `window_r"
    "eads.jsonl` |"
)
README_BULLET = (
    "- 第四题 `QQQ-Q4/` 已结束（2026-10-19）：离散度规则（试跑 T0004 转正式题）证据不足，参照 rv21"
    "（QQQ 自己的 21 日已实现波动，同法）证据不足，见 `QQQ-Q4/decision.md`。左尾方向对，几何年化每"
    "年少 1.6 个点，和第一题 q1a 几乎相同；九条降风险规则在 2011–2021 年都没有以每年少不到 1 个点的"
    "代价改善左尾。失败标签都是「有效果但代价太高」，Baker 2026-10-19 同意；「波动」家族关闭，「广"
    "度和相关」停车，见 `families.md`。"
)
NOTES_TAIL = (
    "；2026-10-19：Baker 选研究自动循环的 T0004（行业收益离散度）开题为第四题 QQQ-Q4（ow"
    "ner_decision:TRADING-2567:2026-10-19:qqq_q4_prereg）：离散度规则为主，QQQ 自己的 21"
    " 日已实现波动同法作对照，验证窗口只查方向；TRADING-2567-30 记决定，TRADING-2567-31"
    " 开题，`research/QQQ-Q4/prereg.yaml`、`strategy.py` 和对照信号 `realized_vol_21` 进 m"
    "ain。production_effect=none；broker_action=none"
)
NOTES_CLOSE = (
    "；2026-10-19：QQQ-Q4 结题（读取 QQQ-Q4-w0001）：dispersion 证据不足（左尾 +7.6%，几何每年少 1."
    "6 个点），参照 rv21 证据不足（几何每年少 3.2 个点）；Baker 同意结论和失败标签「有效果但代价太"
    "高」，TRADING-2567-32 记 decision.md、台账第 26 条、families 和 scorecard。production_effect=n"
    "one；broker_action=none"
)
NEXT_OWNER = "Claude（研究自动循环；下一道正式题等候选过试跑门）"
NEXT_STEP = (
    "第四题 QQQ-Q4 已结束：dispersion 和参照 rv21 都是证据不足（`research/QQQ-Q4/decision.md`，台账"
    "第 26 条），失败标签「有效果但代价太高」，「波动」家族关闭、「广度和相关」停车（`research/fami"
    "lies.md`）。下一道正式题等研究自动循环有候选过试跑门再问 Baker。详见 `docs/requirements/TRADIN"
    "G-2567_QQQ_Research_Program_1999_Window.md`"
)


def test_the_research_readme_lists_the_fourth_question() -> None:
    lines = README.read_text(encoding="utf-8").splitlines()
    assert lines.count(README_ROW) == 1 and lines.count(README_BULLET) == 1
    assert lines.index(README_ROW) == lines.index(README_Q3_ROW) + 1
    assert lines[lines.index(README_BULLET) - 1].startswith("- 第三批试跑 T0004–T0006")


def test_the_task_card_notes_are_only_appended() -> None:
    card = yaml.safe_load(CARD.read_text(encoding="utf-8"))
    assert card["notes"].count(NOTES_TAIL) == 1 and card["notes"].count(NOTES_CLOSE) == 1
    assert card["notes"].endswith(NOTES_TAIL + NOTES_CLOSE)
    assert card["notes"].split(NOTES_TAIL)[0].endswith("broker_action=none")
    assert "portfolio_p1" in card["notes"].split(NOTES_TAIL)[0]
    assert (card["next_owner"], card["next_step"]) == (NEXT_OWNER, NEXT_STEP)
    assert card["status"] == "IN_PROGRESS"
    register = REGISTER.read_text(encoding="utf-8")
    assert NOTES_TAIL in register and NOTES_CLOSE in register and NEXT_STEP in register
