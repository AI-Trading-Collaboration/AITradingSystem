"""Research loop, first batch of screening signals on the whitelisted FRED series (TRADING-2565-13):
the preregistered specs, the formulas on hand-computed examples, no look-ahead, and the whole screen
on synthetic data. Synthetic data only."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from ai_trading_system.research_bench import screen
from ai_trading_system.research_bench.screen_settings import load_screen_settings

SIGNALS = ("vrp_21", "rate10_change_126", "bill3m_change_126")
FACTORS = "ff_factors_daily"
# The specification text was fixed before any screen ran; its LF-normalised SHA-256 is pinned.
SPEC_SHA256 = {
    "vrp_21": "56ebbaea12a1b4b15ff4714c561890b1bea26c7d4042e820e843873db2af6de6",
    "rate10_change_126": "a6c31113ee37bc9f1a88b350903b850fddd49255c07c16aa2db11dfb81aa7335",
    "bill3m_change_126": "966365c6b8a08670d811031aae7bec3590dd721b36def5fff8af8e388a711690",
}
EXPECTED = {
    "vrp_21": (1, "return", {"window": 21}, ("vixcls", FACTORS)),
    "rate10_change_126": (-1, "return", {"window": 126}, ("dgs10",)),
    "bill3m_change_126": (-1, "return", {"window": 126}, ("dtb3",)),
}
LAGS = {"vixcls": 1, "dgs10": 1, "dtb3": 1, FACTORS: 0}
Signal = Callable[[pd.Series, Mapping[str, pd.DataFrame], Mapping[str, Any]], pd.Series]


def _load(signal_id: str) -> screen.LoadedSignal:
    return screen.load_signal(signal_id, directory=screen.SIGNALS_DIR)


def _function(signal_id: str) -> Signal:
    function: Signal = _load(signal_id).function
    return function


def _frame(index: pd.DatetimeIndex, values: Any) -> pd.DataFrame:
    return pd.DataFrame({"value": np.asarray(values, dtype=float)}, index=index)


def _factors(index: pd.DatetimeIndex, market_excess: Any, rf: Any) -> pd.DataFrame:
    """The Ken French factor table: decimals, columns in the file's order."""

    n = len(index)
    return pd.DataFrame(
        {
            "Mkt-RF": np.asarray(market_excess, dtype=float),
            "SMB": np.linspace(-0.03, 0.04, n),
            "HML": np.linspace(0.05, -0.02, n),
            "RF": np.asarray(rf, dtype=float),
        },
        index=index,
    )


def _random_inputs(
    n: int, seed: int, start: str = "1990-01-02"
) -> tuple[pd.Series, dict[str, pd.DataFrame]]:
    rng = np.random.default_rng(seed)
    index = pd.bdate_range(start, periods=n)
    returns = pd.Series(rng.normal(0.0004, 0.012, n), index=index)
    returns.iloc[rng.integers(0, n, 3)] = np.nan
    sources: dict[str, pd.DataFrame] = {}
    for name, centre in (("vixcls", 20.0), ("dgs10", 6.0), ("dtb3", 4.0)):
        values = np.abs(centre + np.cumsum(rng.normal(0.0, 0.3, n)))
        values[rng.integers(0, n, 8)] = np.nan
        sources[name] = _frame(index, values)
    excess = rng.normal(0.0003, 0.011, n)
    excess[rng.integers(0, n, 5)] = np.nan
    sources[FACTORS] = _factors(index, excess, np.full(n, 0.0002))
    return returns, sources


def _run(signal_id: str, returns: pd.Series, sources: Mapping[str, pd.DataFrame]) -> pd.Series:
    loaded = _load(signal_id)
    declared = {name: sources[name].copy() for name in loaded.spec.sources}
    return _function(signal_id)(returns.copy(), declared, dict(loaded.spec.params))


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


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_every_source_a_spec_names_is_an_approved_screen_source(signal_id: str) -> None:
    approved = {s.source_id: s for s in load_screen_settings().settings.sources}
    for name in _load(signal_id).spec.sources:
        assert name in approved
        assert approved[name].known_lag_trading_days == LAGS[name]


# --- vrp_21 ---------------------------------------------------------------------------------


def _realized(values: list[float]) -> float:
    return 100.0 * math.sqrt(252.0 * sum(v * v for v in values) / len(values))


def test_vrp_is_vix_minus_the_annualised_realised_volatility_of_the_us_market() -> None:
    index = pd.bdate_range("2004-01-05", periods=4)
    returns = pd.Series([0.5, -0.5, 0.5, -0.5], index=index)
    vix = _frame(index, [20.0, 22.0, 18.0, 25.0])
    factors = _factors(index, [0.009, -0.021, 0.004, 0.029], [0.001] * 4)
    got = _function("vrp_21")(returns, {"vixcls": vix, FACTORS: factors}, {"window": 2})
    # Market return = Mkt-RF + RF; realised = 100 * sqrt(252 * mean of squares), no demeaning.
    market = [0.010, -0.020, 0.005, 0.030]
    expected = [
        np.nan,
        22.0 - _realized(market[0:2]),
        18.0 - _realized(market[1:3]),
        25.0 - _realized(market[2:4]),
    ]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-9, equal_nan=True)
    assert got.index.equals(index)


def test_vrp_is_missing_when_vix_or_a_market_return_in_the_window_is_missing() -> None:
    index = pd.bdate_range("2004-01-05", periods=5)
    returns = pd.Series(0.0, index=index)
    vix = _frame(index, [20.0, np.nan, 18.0, 25.0, 21.0])
    factors = _factors(index, [0.01, 0.02, np.nan, 0.01, -0.01], [0.0] * 5)
    got = _function("vrp_21")(returns, {"vixcls": vix, FACTORS: factors}, {"window": 2})
    # t1: VIX missing; t2, t3: a missing return in the window; t4: both present.
    expected = [np.nan, np.nan, np.nan, np.nan, 21.0 - _realized([0.01, -0.01])]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-9, equal_nan=True)


def test_vrp_ignores_the_screened_markets_own_returns() -> None:
    returns, sources = _random_inputs(600, seed=8)
    shuffled = pd.Series(np.random.default_rng(1).permutation(returns.to_numpy()), returns.index)
    pd.testing.assert_series_equal(
        _run("vrp_21", returns, sources), _run("vrp_21", shuffled, sources)
    )


# --- rate10_change_126 and bill3m_change_126 -----------------------------------------------


@pytest.mark.parametrize(
    ("signal_id", "source"), [("rate10_change_126", "dgs10"), ("bill3m_change_126", "dtb3")]
)
def test_rate_change_is_the_difference_to_the_value_a_window_earlier(
    signal_id: str, source: str
) -> None:
    index = pd.bdate_range("2004-01-05", periods=5)
    returns = pd.Series(0.01, index=index)
    function = _function(signal_id)
    got = function(returns, {source: _frame(index, [4.0, 4.5, 3.7, 5.0, 4.2])}, {"window": 2})
    np.testing.assert_allclose(
        got.to_numpy(), [np.nan, np.nan, -0.3, 0.5, 0.5], atol=1e-12, equal_nan=True
    )
    gap = function(returns, {source: _frame(index, [4.0, np.nan, 3.8, 5.0, 4.8])}, {"window": 2})
    # A missing value is not filled: the change at t3 compares with t1, which is missing.
    np.testing.assert_allclose(
        gap.to_numpy(), [np.nan, np.nan, -0.2, np.nan, 1.0], atol=1e-12, equal_nan=True
    )
    assert got.index.equals(index)


# --- all three ------------------------------------------------------------------------------


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_a_value_never_depends_on_later_data(signal_id: str) -> None:
    returns, sources = _random_inputs(700, seed=11)
    full = _run(signal_id, returns, sources)
    for cut in (90, 300, 523):
        part = _run(signal_id, returns.iloc[:cut], {k: v.iloc[:cut] for k, v in sources.items()})
        pd.testing.assert_series_equal(part, full.iloc[:cut], check_exact=True)


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_the_output_is_a_finite_float_series_on_the_input_index(signal_id: str) -> None:
    returns, sources = _random_inputs(900, seed=5)
    got = _run(signal_id, returns, sources)
    assert isinstance(got, pd.Series) and got.index.equals(returns.index)
    assert got.dtype == float and not np.isinf(got.to_numpy()).any()
    assert got.notna().sum() > 500


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_inputs_are_not_modified(signal_id: str) -> None:
    returns, sources = _random_inputs(500, seed=3)
    before_returns = returns.copy()
    before_sources = {k: v.copy() for k, v in sources.items()}
    loaded = _load(signal_id)
    declared = {name: sources[name] for name in loaded.spec.sources}
    loaded.function(returns, declared, dict(loaded.spec.params))
    pd.testing.assert_series_equal(returns, before_returns)
    for name, frame in sources.items():
        pd.testing.assert_frame_equal(frame, before_sources[name])


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_the_window_changes_the_output(signal_id: str) -> None:
    returns, sources = _random_inputs(1500, seed=21)
    loaded = _load(signal_id)
    declared = {k: sources[k] for k in loaded.spec.sources}
    params = dict(loaded.spec.params)
    base = loaded.function(returns, declared, params)
    other = loaded.function(returns, declared, {**params, "window": params["window"] + 1})
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
            "us_market", _market(rng, 5200, "1992-01-06"), date(1994, 6, 1)
        ),
        "industry_a": screen.ScreenMarket(
            "industry_a", _market(rng, 5200, "1992-01-06"), date(1994, 6, 1)
        ),
    }
    _, sources = _random_inputs(6200, seed=4, start="1988-01-04")
    result = screen.screen_signal(_load(signal_id), markets, sources, LAGS, rules)
    assert set(result["markets"]) == set(markets)
    assert result["unchecked"] == []
    assert result["direction_tested"] == len(markets)
    assert "verdict_inputs" in result and result["breadth"] is not None
    for market_id, stats in result["markets"].items():
        assert stats["horizons"]["21"]["days"] > 120, market_id


def test_the_signals_readme_names_this_batch() -> None:
    text = (screen.SIGNALS_DIR / "README.md").read_text(encoding="utf-8")
    assert all(signal_id in text for signal_id in SIGNALS)
    assert "TRADING-2565-13" in text
