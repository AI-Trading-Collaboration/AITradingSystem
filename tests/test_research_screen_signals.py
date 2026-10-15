"""First real screening signals (TRADING-2565-10): preregistered specs, formulas, no look-ahead,
the whole screen on synthetic data, and the append-only record lines. Synthetic data only."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import screen
from ai_trading_system.research_bench.screen_settings import load_screen_settings

SIGNALS = (
    "credit_spread_change_63",
    "credit_spread_level",
    "term_spread_level",
    "high_distance_252",
)
# The specification text was fixed before any screen ran; its LF-normalised SHA-256 is pinned.
SPEC_SHA256 = {
    "credit_spread_change_63": "306ada8f757cdb4953d7695331d9181932d1db530edd1ea157d2d07373bc2329",
    "credit_spread_level": "0e86cfd8d4265d723f237abea6caa2e2d8614a3914002271a06551dc53d56418",
    "term_spread_level": "1e07c96af17b9907cca6717ff7cefd1c18f689fd629939c03f2297316f601380",
    "high_distance_252": "01a39c5faa723b70862ee1e2eb45245c2016719ab65c969388687bbba07d57ba",
}
EXPECTED = {
    "credit_spread_change_63": (-1, "return", {"window": 63}, ("baa10y",)),
    "credit_spread_level": (1, "risk", {}, ("baa10y",)),
    "term_spread_level": (1, "return", {}, ("t10y3m",)),
    "high_distance_252": (1, "return", {"window": 252}, ()),
}
Signal = Callable[[pd.Series, Mapping[str, pd.DataFrame], Mapping[str, Any]], pd.Series]


def _load(signal_id: str) -> screen.LoadedSignal:
    return screen.load_signal(signal_id, directory=screen.SIGNALS_DIR)


def _frame(index: pd.DatetimeIndex, values: Any) -> pd.DataFrame:
    return pd.DataFrame({"value": np.asarray(values, dtype=float)}, index=index)


def _random_inputs(n: int, seed: int) -> tuple[pd.Series, dict[str, pd.DataFrame]]:
    rng = np.random.default_rng(seed)
    index = pd.bdate_range("2003-01-02", periods=n)
    returns = pd.Series(rng.normal(0.0004, 0.012, n), index=index)
    sources = {}
    for name, centre in (("baa10y", 2.5), ("t10y3m", 1.5)):
        values = centre + np.cumsum(rng.normal(0.0, 0.05, n))
        values[rng.integers(0, n, 8)] = np.nan
        sources[name] = _frame(index, values)
    return returns, sources


def _declared(signal_id: str, sources: Mapping[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    return {name: sources[name].copy() for name in _load(signal_id).spec.sources}


def _run(signal_id: str, returns: pd.Series, sources: Mapping[str, pd.DataFrame]) -> pd.Series:
    loaded = _load(signal_id)
    function: Signal = loaded.function
    return function(returns.copy(), _declared(signal_id, sources), dict(loaded.spec.params))


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_the_specification_is_the_preregistered_text(signal_id: str) -> None:
    loaded = _load(signal_id)
    sign, target, params, sources = EXPECTED[signal_id]
    assert loaded.spec.signal_id == signal_id
    assert (loaded.spec.expected_sign, loaded.spec.target) == (sign, target)
    assert loaded.spec.params == params and loaded.spec.sources == sources
    assert loaded.spec.markets == "all"
    assert loaded.spec.mechanism.strip() and loaded.spec.literature.strip() != "none"
    assert loaded.spec_sha256 == SPEC_SHA256[signal_id]
    raw = yaml.safe_load((screen.SIGNALS_DIR / f"{signal_id}.yaml").read_text(encoding="utf-8"))
    assert set(raw) == {
        "signal_id",
        "mechanism",
        "expected_sign",
        "target",
        "params",
        "sources",
        "markets",
        "literature",
    }


def test_credit_spread_change_is_the_difference_to_the_value_a_window_earlier() -> None:
    index = pd.bdate_range("2004-01-05", periods=5)
    returns = pd.Series(0.0, index=index)
    loaded = _load("credit_spread_change_63")
    function: Signal = loaded.function
    spread = _frame(index, [1.0, 1.5, 0.7, 2.0, 1.2])
    got = function(returns, {"baa10y": spread}, {"window": 2})
    pd.testing.assert_series_equal(
        got,
        pd.Series([np.nan, np.nan, -0.3, 0.5, 0.5], index=index),
        check_names=False,
        check_exact=False,
    )
    gap = function(returns, {"baa10y": _frame(index, [1.0, np.nan, 0.8, 2.0, 1.8])}, {"window": 2})
    pd.testing.assert_series_equal(
        gap,
        pd.Series([np.nan, np.nan, -0.2, np.nan, 1.0], index=index),
        check_names=False,
        check_exact=False,
    )


@pytest.mark.parametrize(
    ("signal_id", "source"), [("credit_spread_level", "baa10y"), ("term_spread_level", "t10y3m")]
)
def test_level_signals_are_the_aligned_source_value_and_nothing_else(
    signal_id: str, source: str
) -> None:
    index = pd.bdate_range("2004-01-05", periods=6)
    returns = pd.Series(0.01, index=index)
    own = _frame(index, [1.0, np.nan, 3.0, 4.5, -0.5, 2.0])
    other = _frame(index, [9.0, 8.0, 7.0, 6.0, 5.0, 4.0])
    sources = {"baa10y": other, "t10y3m": other}
    sources[source] = own
    got = _run(signal_id, returns, sources)
    pd.testing.assert_series_equal(got, own["value"], check_names=False)
    assert got.index.equals(index)


def test_high_distance_is_the_log_distance_below_the_window_high() -> None:
    index = pd.bdate_range("2004-01-05", periods=4)
    loaded = _load("high_distance_252")
    function: Signal = loaded.function
    got = function(pd.Series([0.0, 0.10, -0.10, 0.20], index=index), {}, {"window": 3})
    expected = [np.nan, np.nan, math.log(0.9), 0.0]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)

    index = pd.bdate_range("2004-01-05", periods=5)
    got = function(pd.Series([0.0, 0.1, 0.1, -0.2, -0.1], index=index), {}, {"window": 3})
    expected = [np.nan, np.nan, 0.0, math.log(0.8), math.log(0.72)]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)


def test_high_distance_is_missing_while_a_missing_return_is_in_the_window() -> None:
    index = pd.bdate_range("2004-01-05", periods=6)
    loaded = _load("high_distance_252")
    function: Signal = loaded.function
    returns = pd.Series([0.0, 0.1, np.nan, 0.1, 0.0, 0.0], index=index)
    got = function(returns, {}, {"window": 2})
    np.testing.assert_allclose(
        got.to_numpy(), [np.nan, 0.0, np.nan, np.nan, 0.0, 0.0], atol=1e-12, equal_nan=True
    )


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_a_value_never_depends_on_later_data(signal_id: str) -> None:
    returns, sources = _random_inputs(600, seed=11)
    full = _run(signal_id, returns, sources)
    for cut in (80, 260, 411):
        part = _run(signal_id, returns.iloc[:cut], {k: v.iloc[:cut] for k, v in sources.items()})
        pd.testing.assert_series_equal(part, full.iloc[:cut], check_exact=True)


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_the_output_is_a_finite_float_series_on_the_input_index(signal_id: str) -> None:
    returns, sources = _random_inputs(700, seed=5)
    got = _run(signal_id, returns, sources)
    assert isinstance(got, pd.Series) and got.index.equals(returns.index)
    assert got.dtype == float and not np.isinf(got.to_numpy()).any()
    assert got.notna().sum() > 300


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_inputs_are_not_modified(signal_id: str) -> None:
    returns, sources = _random_inputs(400, seed=3)
    before_returns = returns.copy()
    before_sources = {k: v.copy() for k, v in sources.items()}
    loaded = _load(signal_id)
    function: Signal = loaded.function
    declared = {name: sources[name] for name in loaded.spec.sources}
    function(returns, declared, dict(loaded.spec.params))
    pd.testing.assert_series_equal(returns, before_returns)
    for name, frame in sources.items():
        pd.testing.assert_frame_equal(frame, before_sources[name])


@pytest.mark.parametrize("signal_id", ["credit_spread_change_63", "high_distance_252"])
def test_the_window_parameter_changes_the_output(signal_id: str) -> None:
    returns, sources = _random_inputs(1500, seed=21)
    loaded = _load(signal_id)
    function: Signal = loaded.function
    window = loaded.spec.params["window"]
    declared = {name: sources[name] for name in loaded.spec.sources}
    base = function(returns, declared, {"window": window})
    other = function(returns, declared, {"window": window + 1})
    assert not np.allclose(base.to_numpy(), other.to_numpy(), equal_nan=True, atol=1e-12)


def _market(rng: np.random.Generator, n: int, start: str) -> pd.Series:
    index = pd.bdate_range(start, periods=n)
    return pd.Series(rng.normal(0.0004, 0.012, n), index=index)


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_each_signal_runs_through_the_whole_screen_on_synthetic_markets(signal_id: str) -> None:
    rng = np.random.default_rng(2026)
    rules = load_screen_settings().settings.screen.model_copy(
        update={
            "bootstrap_draws": 40,
            "min_days": 120,
            "warmup_days": 60,
            "screen_end": date(2010, 12, 31),
        }
    )
    markets = {
        "qqq": screen.ScreenMarket("qqq", _market(rng, 2200, "2003-01-02"), date(2004, 6, 1)),
        "us_market": screen.ScreenMarket(
            "us_market", _market(rng, 5200, "1992-01-06"), date(1993, 6, 1)
        ),
        "industry_a": screen.ScreenMarket(
            "industry_a", _market(rng, 5200, "1992-01-06"), date(1993, 6, 1)
        ),
    }
    index = pd.bdate_range("1988-01-04", periods=6200)
    sources = {
        name: _frame(index, centre + np.cumsum(rng.normal(0.0, 0.05, len(index))))
        for name, centre in (("baa10y", 2.5), ("t10y3m", 1.5))
    }
    lags = {"baa10y": 1, "t10y3m": 1}
    result = screen.screen_signal(_load(signal_id), markets, sources, lags, rules)
    assert set(result["markets"]) == set(markets)
    assert result["unchecked"] == []
    assert result["direction_tested"] == len(markets)
    assert "verdict_inputs" in result and result["breadth"] is not None
    for market_id, stats in result["markets"].items():
        assert stats["horizons"]["21"]["days"] > 120, market_id


def test_the_signals_readme_says_what_this_batch_added_and_the_loader_limit() -> None:
    text = (screen.SIGNALS_DIR / "README.md").read_text(encoding="utf-8")
    assert all(signal_id in text for signal_id in SIGNALS)
    assert "dataclass" in text


# The record lines are appended, never edited: S4d's own sentences stay where they are.
DOC = PROJECT_ROOT / "docs/requirements/TRADING-2565_Research_Bench.md"
CARD = PROJECT_ROOT / "tasks/TRADING-2565_RESEARCH_TRIAL_LEDGER_MULTIPLE_TESTING.yaml"
REGISTER = PROJECT_ROOT / "docs/task_register.md"
DOC_LINE = (
    "- 2026-10-14：更正：S4d（TRADING-2565-08）已发布到 main 8b2ba7fe（Gate PASS，zone C），"
    "上一行里的“尚无全仓绿灯”和“不发布”写于全仓测试还没跑完的时候，已不是事实。S4e（TRADING-2565-10）："
    "第一批真实信号（credit_spread_change_63、credit_spread_level、term_spread_level、"
    "high_distance_252），只用合成数据验收，不读真实快照；筛选要另按研究会话的指令跑。"
    "production_effect=none；broker_action=none。"
)
NOTES_TAIL = (
    "；2026-10-14：更正：S4d（TRADING-2565-08）已发布到 main 8b2ba7fe（Gate PASS），"
    "上面“全仓基线和 PR 检查超时……发布阻塞待核验”已不是事实；S4e（TRADING-2565-10）第一批真实信号"
)


def test_the_requirement_progress_has_the_s4e_line_once_after_the_s4d_line() -> None:
    lines = DOC.read_text(encoding="utf-8").rstrip("\n").splitlines()
    assert lines.count(DOC_LINE) == 1
    position = lines.index(DOC_LINE)
    assert lines.index("## 进展") < position
    assert lines[position - 1].startswith("- 2026-10-14：S4d（TRADING-2565-08）")
    assert "尚无全仓绿灯；保留证据，不修范围外测试，不发布。" in lines[position - 1]


def test_the_task_card_notes_are_only_appended() -> None:
    card = yaml.safe_load(CARD.read_text(encoding="utf-8"))
    notes = card["notes"]
    before, _, after = notes.partition(NOTES_TAIL)
    assert notes.count(NOTES_TAIL) == 1
    assert "发布阻塞待核验" in before
    assert after == "" or after.startswith("；")
    assert card["status"] == "IN_PROGRESS"
    assert NOTES_TAIL in REGISTER.read_text(encoding="utf-8")
