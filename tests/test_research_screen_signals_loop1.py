"""Research loop, first batch of screening signals without new data (TRADING-2565-12): the
preregistered specs, the formulas on hand-computed examples, no look-ahead, and the whole screen on
synthetic data. Synthetic data only."""

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

SIGNALS = ("industry_breadth_210", "industry_corr_63", "dip_in_uptrend_10")
INDUSTRIES = "ff_49_industries_daily"
# The specification text was fixed before any screen ran; its LF-normalised SHA-256 is pinned.
SPEC_SHA256 = {
    "industry_breadth_210": "f6db0232670de95e4fbac20ed19aaef54823f56caa38e2b6483312fe7844eb83",
    "industry_corr_63": "f013ccbf6c2d88dff5bdf021963e39da44484f5b54a3477ec2902bbb3a07186c",
    "dip_in_uptrend_10": "19457c7a160585a866aaf506fb4e02bd6d5c22a3702af101f6394b8affc643c9",
}
EXPECTED = {
    "industry_breadth_210": (1, "risk", {"window": 210}, (INDUSTRIES,)),
    "industry_corr_63": (1, "return", {"window": 63}, (INDUSTRIES,)),
    "dip_in_uptrend_10": (-1, "return", {"short": 10, "long": 252}, ()),
}
Signal = Callable[[pd.Series, Mapping[str, pd.DataFrame], Mapping[str, Any]], pd.Series]


def _load(signal_id: str) -> screen.LoadedSignal:
    return screen.load_signal(signal_id, directory=screen.SIGNALS_DIR)


def _function(signal_id: str) -> Signal:
    function: Signal = _load(signal_id).function
    return function


def _industries(n: int, columns: int, seed: int, start: str = "1990-01-02") -> pd.DataFrame:
    """Correlated industry returns with late starts and a few gaps, like the Ken French file."""

    rng = np.random.default_rng(seed)
    index = pd.bdate_range(start, periods=n)
    common = rng.normal(0.0003, 0.008, n)
    loadings = rng.uniform(0.3, 1.5, columns)
    values = common[:, None] * loadings[None, :] + rng.normal(0.0, 0.009, (n, columns))
    for column in range(0, columns, 4):
        values[: int(rng.integers(50, n // 3)), column] = np.nan
    for _ in range(columns):
        values[int(rng.integers(0, n)), int(rng.integers(0, columns))] = np.nan
    names = [f"ind{c:02d}" for c in range(columns)]
    return pd.DataFrame(values, index=index, columns=names)


def _random_inputs(n: int, seed: int) -> tuple[pd.Series, dict[str, pd.DataFrame]]:
    rng = np.random.default_rng(seed + 1000)
    industries = _industries(n, 12, seed)
    returns = pd.Series(rng.normal(0.0004, 0.012, n), index=industries.index)
    returns.iloc[rng.integers(0, n, 3)] = np.nan
    return returns, {INDUSTRIES: industries}


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
    assert loaded.spec.mechanism.strip() and loaded.spec.literature.strip()
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


# --- industry_breadth_210 -------------------------------------------------------------------


def _frame(index: pd.DatetimeIndex, columns: dict[str, list[float]]) -> pd.DataFrame:
    return pd.DataFrame({k: np.asarray(v, dtype=float) for k, v in columns.items()}, index=index)


def test_breadth_is_the_share_of_industries_below_their_average_among_those_with_one() -> None:
    index = pd.bdate_range("2004-01-05", periods=4)
    returns = pd.Series(0.0, index=index)
    # window 2: price p_t = prod(1 + r); below when p_t < (p_{t-1} + p_t) / 2, i.e. r_t < 0.
    industries = _frame(
        index,
        {
            "a": [0.0, 0.1, -0.1, 0.2],
            "b": [0.0, -0.1, -0.1, 0.1],
            "c": [np.nan, np.nan, 0.1, -0.2],
        },
    )
    got = _function("industry_breadth_210")(returns, {INDUSTRIES: industries}, {"window": 2})
    # t0: no average yet -> missing; t1: a up, b down -> 1/2; t2: a, b down, c has one price
    # only -> 2/2; t3: a, b up, c down -> 1/3.
    np.testing.assert_allclose(
        got.to_numpy(), [np.nan, 0.5, 1.0, 1.0 / 3.0], atol=1e-12, equal_nan=True
    )
    assert got.index.equals(index)


def test_breadth_uses_the_window_average_of_the_price_not_of_the_returns() -> None:
    index = pd.bdate_range("2004-01-05", periods=5)
    returns = pd.Series(0.0, index=index)
    # Prices 1, 2, 2.2, 1.98, 1.98: the 3-day averages are -, -, 1.7333, 2.06, 2.0533.
    industries = _frame(index, {"a": [0.0, 1.0, 0.1, -0.1, 0.0]})
    got = _function("industry_breadth_210")(returns, {INDUSTRIES: industries}, {"window": 3})
    np.testing.assert_allclose(
        got.to_numpy(), [np.nan, np.nan, 0.0, 1.0, 1.0], atol=1e-12, equal_nan=True
    )


def test_breadth_is_missing_when_no_industry_has_an_average() -> None:
    index = pd.bdate_range("2004-01-05", periods=4)
    returns = pd.Series(0.0, index=index)
    industries = _frame(index, {"a": [0.01, np.nan, 0.02, 0.01], "b": [np.nan] * 4})
    got = _function("industry_breadth_210")(returns, {INDUSTRIES: industries}, {"window": 2})
    # A missing return leaves the industry without an average while it is in the window.
    np.testing.assert_allclose(
        got.to_numpy(), [np.nan, np.nan, np.nan, 0.0], atol=1e-12, equal_nan=True
    )


# --- industry_corr_63 -----------------------------------------------------------------------


def test_average_correlation_is_the_mean_of_the_pairwise_coefficients() -> None:
    index = pd.bdate_range("2004-01-05", periods=4)
    returns = pd.Series(0.0, index=index)
    a = [0.01, -0.02, 0.03, 0.01]
    industries = _frame(
        index,
        {"a": a, "b": [2.0 * x for x in a], "c": [-x for x in a]},
    )
    got = _function("industry_corr_63")(returns, {INDUSTRIES: industries}, {"window": 3})
    # Pairs (a, b) = 1, (a, c) = -1, (b, c) = -1.
    np.testing.assert_allclose(
        got.to_numpy(), [np.nan, np.nan, -1.0 / 3.0, -1.0 / 3.0], atol=1e-12, equal_nan=True
    )


def test_average_correlation_against_a_hand_computed_window() -> None:
    index = pd.bdate_range("2004-01-05", periods=3)
    returns = pd.Series(0.0, index=index)
    x = [1.0, 2.0, 4.0]
    y = [1.0, 3.0, 2.0]
    z = [3.0, 1.0, 2.0]
    industries = _frame(index, {"x": x, "y": y, "z": z})
    got = _function("industry_corr_63")(returns, {INDUSTRIES: industries}, {"window": 3})

    def corr(u: list[float], v: list[float]) -> float:
        mu, mv = sum(u) / 3, sum(v) / 3
        cov = sum((p - mu) * (q - mv) for p, q in zip(u, v, strict=True))
        su = math.sqrt(sum((p - mu) ** 2 for p in u))
        sv = math.sqrt(sum((q - mv) ** 2 for q in v))
        return cov / (su * sv)

    expected = (corr(x, y) + corr(x, z) + corr(y, z)) / 3.0
    np.testing.assert_allclose(got.to_numpy(), [np.nan, np.nan, expected], atol=1e-12)


def test_average_correlation_skips_industries_with_a_gap_or_no_movement() -> None:
    index = pd.bdate_range("2004-01-05", periods=5)
    returns = pd.Series(0.0, index=index)
    a = [0.01, -0.02, 0.03, 0.01, -0.01]
    industries = _frame(
        index,
        {
            "a": a,
            "b": [2.0 * x for x in a],
            "gap": [0.01, np.nan, 0.02, -0.01, 0.0],
            "flat": [0.0, 0.0, 0.0, 0.0, 0.0],
            "c": [-x for x in a],
        },
    )
    got = _function("industry_corr_63")(returns, {INDUSTRIES: industries}, {"window": 3})
    # t2, t3: "gap" has a missing value in the window, "flat" never moves -> a, b, c only.
    # t4: the gap has left the window; gap = [0.02, -0.01, 0.0] against a = [0.03, 0.01, -0.01].
    ga = np.corrcoef([0.02, -0.01, 0.0], [0.03, 0.01, -0.01])[0, 1]
    t4 = (1.0 - 1.0 - 1.0 + ga + ga - ga) / 6.0
    np.testing.assert_allclose(
        got.to_numpy(), [np.nan, np.nan, -1.0 / 3.0, -1.0 / 3.0, t4], atol=1e-12, equal_nan=True
    )


def test_average_correlation_needs_two_industries() -> None:
    index = pd.bdate_range("2004-01-05", periods=4)
    returns = pd.Series(0.0, index=index)
    industries = _frame(index, {"a": [0.01, -0.02, 0.03, 0.01], "b": [np.nan] * 4})
    got = _function("industry_corr_63")(returns, {INDUSTRIES: industries}, {"window": 3})
    assert got.isna().all()
    # Two industries are enough: the value is their one coefficient.
    industries["b"] = [0.02, 0.01, -0.01, 0.0]
    got = _function("industry_corr_63")(returns, {INDUSTRIES: industries}, {"window": 3})
    a, b = industries["a"].to_numpy(), industries["b"].to_numpy()
    expected = [np.nan, np.nan, np.corrcoef(a[:3], b[:3])[0, 1], np.corrcoef(a[1:], b[1:])[0, 1]]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)


# --- dip_in_uptrend_10 ----------------------------------------------------------------------


def test_dip_is_the_short_return_while_the_long_return_is_positive() -> None:
    index = pd.bdate_range("2004-01-05", periods=6)
    returns = pd.Series([0.10, 0.08, -0.04, -0.02, -0.20, 0.01], index=index)
    got = _function("dip_in_uptrend_10")(returns, {}, {"short": 2, "long": 3})
    # long (3 days) at t2: 1.1*1.08*0.96-1 > 0 -> short = 1.08*0.96-1; t3: 1.08*0.96*0.98-1 > 0
    # -> short = 0.96*0.98-1; t4: 0.96*0.98*0.8-1 < 0 -> missing; t5: 0.98*0.8*1.01-1 < 0.
    expected = [np.nan, np.nan, 1.08 * 0.96 - 1, 0.96 * 0.98 - 1, np.nan, np.nan]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)


def test_dip_is_missing_when_the_long_return_is_not_positive_or_a_return_is_missing() -> None:
    index = pd.bdate_range("2004-01-05", periods=5)
    returns = pd.Series([0.10, -0.2, 0.01, np.nan, 0.01], index=index)
    got = _function("dip_in_uptrend_10")(returns, {}, {"short": 1, "long": 2})
    # t1: long = 1.1*0.8-1 < 0; t2: long = 0.8*1.01-1 < 0; t3, t4: a missing return in the
    # window.
    assert got.isna().all()
    up = pd.Series([0.02, 0.03, -0.01, 0.04], index=index[:4])
    got = _function("dip_in_uptrend_10")(up, {}, {"short": 1, "long": 2})
    np.testing.assert_allclose(
        got.to_numpy(), [np.nan, 0.03, -0.01, 0.04], atol=1e-12, equal_nan=True
    )


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
    assert got.notna().sum() > 150


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


@pytest.mark.parametrize(
    ("signal_id", "name"),
    [
        ("industry_breadth_210", "window"),
        ("industry_corr_63", "window"),
        ("dip_in_uptrend_10", "short"),
        ("dip_in_uptrend_10", "long"),
    ],
)
def test_each_parameter_changes_the_output(signal_id: str, name: str) -> None:
    returns, sources = _random_inputs(1500, seed=21)
    loaded = _load(signal_id)
    declared = {k: sources[k] for k in loaded.spec.sources}
    params = dict(loaded.spec.params)
    base = loaded.function(returns, declared, params)
    other = loaded.function(returns, declared, {**params, name: params[name] + 1})
    assert not np.allclose(base.to_numpy(), other.to_numpy(), equal_nan=True, atol=1e-12)


def test_industry_signals_ignore_the_market_returns() -> None:
    returns, sources = _random_inputs(600, seed=8)
    shuffled = pd.Series(np.random.default_rng(1).permutation(returns.to_numpy()), returns.index)
    for signal_id in ("industry_breadth_210", "industry_corr_63"):
        pd.testing.assert_series_equal(
            _run(signal_id, returns, sources), _run(signal_id, shuffled, sources)
        )


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
    sources = {INDUSTRIES: _industries(6200, 10, seed=4, start="1988-01-04")}
    lags = {INDUSTRIES: 0}
    result = screen.screen_signal(_load(signal_id), markets, sources, lags, rules)
    assert set(result["markets"]) == set(markets)
    assert result["unchecked"] == []
    assert result["direction_tested"] == len(markets)
    assert "verdict_inputs" in result and result["breadth"] is not None
    for market_id, stats in result["markets"].items():
        assert stats["horizons"]["21"]["days"] > 120, market_id


def test_the_signals_readme_names_this_batch() -> None:
    text = (screen.SIGNALS_DIR / "README.md").read_text(encoding="utf-8")
    assert all(signal_id in text for signal_id in SIGNALS)
    assert "TRADING-2565-12" in text
