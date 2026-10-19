"""Research loop, second batch of screening signals (TRADING-2565-25): the preregistered specs, the
formulas on hand-computed examples, no look-ahead, a value early enough for the trial gate's first
stage, and the whole screen on synthetic data. Synthetic data only."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from ai_trading_system.research_bench import screen
from ai_trading_system.research_bench.screen_settings import load_screen_settings

SIGNALS = ("bad_vol_share_63", "reversal_profit_21")
INDUSTRIES = "ff_49_industries_daily"
# The specification text was fixed before any screen ran; its LF-normalised SHA-256 is pinned.
SPEC_SHA256 = {
    "bad_vol_share_63": "67cd95eba8edc22b130cc082181bd8d987636a47246b8650ca237ea0365f5195",
    "reversal_profit_21": "fba5a8bdc32802d513088aa876a09752bea3fb4a1a8c2535b9af494f22f28e36",
}
EXPECTED = {
    "bad_vol_share_63": (1, "risk", {"window": 63}, ()),
    "reversal_profit_21": (1, "risk", {"formation": 5, "window": 21}, (INDUSTRIES,)),
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


def _frame(index: pd.DatetimeIndex, columns: dict[str, list[float]]) -> pd.DataFrame:
    return pd.DataFrame({k: np.asarray(v, dtype=float) for k, v in columns.items()}, index=index)


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


# --- bad_vol_share_63 -----------------------------------------------------------------------


def test_bad_vol_share_against_hand_computed_windows() -> None:
    index = pd.bdate_range("2004-01-05", periods=5)
    returns = pd.Series([0.01, -0.02, 0.03, 0.0, -0.01], index=index)
    got = _function("bad_vol_share_63")(returns, {}, {"window": 3})
    # t2: down 0.0004, up 0.0001 + 0.0009; t3: down 0.0004, up 0.0009 (the zero day counts on
    # neither side); t4: down 0.0001, up 0.0009.
    expected = [np.nan, np.nan, -0.0006 / 0.0014, -0.0005 / 0.0013, -0.0008 / 0.0010]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)
    assert got.index.equals(index)


def test_bad_vol_share_is_one_for_down_days_only_and_minus_one_for_up_days_only() -> None:
    index = pd.bdate_range("2004-01-05", periods=4)
    down = pd.Series([-0.01, -0.03, -0.02, -0.05], index=index)
    up = pd.Series([0.01, 0.03, 0.0, 0.05], index=index)
    np.testing.assert_allclose(
        _function("bad_vol_share_63")(down, {}, {"window": 2}).to_numpy(),
        [np.nan, 1.0, 1.0, 1.0],
        atol=1e-12,
        equal_nan=True,
    )
    np.testing.assert_allclose(
        _function("bad_vol_share_63")(up, {}, {"window": 2}).to_numpy(),
        [np.nan, -1.0, -1.0, -1.0],
        atol=1e-12,
        equal_nan=True,
    )


def test_bad_vol_share_reads_the_mix_of_the_variance_not_its_level() -> None:
    returns, _ = _random_inputs(400, seed=17)
    base = _function("bad_vol_share_63")(returns, {}, {"window": 63})
    scaled = _function("bad_vol_share_63")(3.0 * returns, {}, {"window": 63})
    np.testing.assert_allclose(base.to_numpy(), scaled.to_numpy(), atol=1e-9, equal_nan=True)


def test_bad_vol_share_is_missing_with_a_gap_or_no_movement_in_the_window() -> None:
    index = pd.bdate_range("2004-01-05", periods=6)
    returns = pd.Series([0.01, np.nan, -0.02, 0.0, 0.0, 0.01], index=index)
    got = _function("bad_vol_share_63")(returns, {}, {"window": 2})
    # t1, t2: the missing return is in the window; t4: both days are flat; t3: one down day;
    # t5: one up day.
    np.testing.assert_allclose(
        got.to_numpy(), [np.nan, np.nan, np.nan, 1.0, np.nan, -1.0], atol=1e-12, equal_nan=True
    )


# --- reversal_profit_21 ---------------------------------------------------------------------


def _reversal_day(past: np.ndarray, today: np.ndarray) -> float:
    """Independent reference for one day: compounded formation returns, $1 long and $1 short."""

    formed = np.array([np.prod([1.0 + x for x in column]) - 1.0 for column in past.T])
    deviation = formed - formed.mean()
    longs = -deviation[deviation < 0].sum()
    shorts = deviation[deviation > 0].sum()
    weights = np.where(deviation < 0, -deviation / longs, -deviation / shorts)
    assert np.isclose(weights[weights > 0].sum(), 1.0)
    assert np.isclose(weights[weights < 0].sum(), -1.0)
    return float(np.dot(weights, today))


def test_reversal_buys_yesterdays_losers_and_sells_its_winners() -> None:
    index = pd.bdate_range("2004-01-05", periods=2)
    returns = pd.Series(0.0, index=index)
    industries = _frame(index, {"a": [0.02, 0.01], "b": [0.0, 0.05], "c": [-0.02, 0.03]})
    got = _function("reversal_profit_21")(
        returns, {INDUSTRIES: industries}, {"formation": 1, "window": 1}
    )
    # Deviations 0.02, 0, -0.02 -> weights -1, 0, 1 -> -0.01 + 0.03.
    np.testing.assert_allclose(got.to_numpy(), [np.nan, 0.02], atol=1e-12, equal_nan=True)


def test_reversal_compounds_the_formation_window_and_sums_the_holding_window() -> None:
    index = pd.bdate_range("2004-01-05", periods=5)
    returns = pd.Series(0.0, index=index)
    columns = {
        "a": [0.10, 0.10, 0.02, -0.01, 0.03],
        "b": [0.00, 0.00, -0.03, 0.02, 0.01],
        "c": [-0.10, 0.10, 0.01, 0.04, -0.02],
        "d": [0.05, -0.02, 0.00, 0.01, 0.02],
    }
    industries = _frame(index, columns)
    got = _function("reversal_profit_21")(
        returns, {INDUSTRIES: industries}, {"formation": 2, "window": 2}
    )
    values = industries.to_numpy()
    daily = [_reversal_day(values[t - 2 : t], values[t]) for t in (2, 3, 4)]
    expected = [np.nan, np.nan, np.nan, daily[0] + daily[1], daily[1] + daily[2]]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)
    assert got.index.equals(index)


def test_reversal_leaves_out_industries_with_a_gap_in_the_formation_window_or_today() -> None:
    index = pd.bdate_range("2004-01-05", periods=4)
    returns = pd.Series(0.0, index=index)
    columns = {
        "a": [0.02, 0.01, -0.01, 0.02],
        "b": [-0.01, 0.03, 0.02, -0.02],
        "c": [0.00, -0.02, 0.01, 0.03],
        "late": [np.nan, 0.04, -0.03, 0.01],
        "gone": [0.01, -0.01, 0.02, np.nan],
    }
    industries = _frame(index, columns)
    got = _function("reversal_profit_21")(
        returns, {INDUSTRIES: industries}, {"formation": 2, "window": 1}
    )
    v = industries.to_numpy()
    # t2: "late" has a gap in the formation days; t3: "gone" has no return today.
    t2 = _reversal_day(v[0:2][:, [0, 1, 2, 4]], v[2][[0, 1, 2, 4]])
    t3 = _reversal_day(v[1:3][:, [0, 1, 2, 3]], v[3][[0, 1, 2, 3]])
    np.testing.assert_allclose(got.to_numpy(), [np.nan, np.nan, t2, t3], atol=1e-12, equal_nan=True)


def test_reversal_has_no_value_without_two_industries_or_any_spread() -> None:
    index = pd.bdate_range("2004-01-05", periods=4)
    returns = pd.Series(0.0, index=index)
    alone = _frame(index, {"a": [0.01, -0.02, 0.03, 0.01], "b": [np.nan] * 4})
    got = _function("reversal_profit_21")(
        returns, {INDUSTRIES: alone}, {"formation": 1, "window": 1}
    )
    assert got.isna().all()
    flat = _frame(index, {"a": [0.01, 0.01, 0.02, 0.0], "b": [0.01, 0.01, -0.01, 0.03]})
    got = _function("reversal_profit_21")(
        returns, {INDUSTRIES: flat}, {"formation": 1, "window": 2}
    )
    # t1 and t2: both industries made 1% the day before -> no portfolio; only t3 forms one, and
    # a two-day sum on t3 needs t2 as well.
    assert got.isna().all()
    got = _function("reversal_profit_21")(
        returns, {INDUSTRIES: flat}, {"formation": 1, "window": 1}
    )
    # t3: deviations of t2 are 0.015, -0.015 -> weights -1, 1 -> -0.0 + 0.03.
    np.testing.assert_allclose(
        got.to_numpy(), [np.nan, np.nan, np.nan, 0.03], atol=1e-12, equal_nan=True
    )


def test_reversal_ignores_the_market_returns() -> None:
    returns, sources = _random_inputs(600, seed=8)
    shuffled = pd.Series(np.random.default_rng(1).permutation(returns.to_numpy()), returns.index)
    pd.testing.assert_series_equal(
        _run("reversal_profit_21", returns, sources), _run("reversal_profit_21", shuffled, sources)
    )


# --- both -----------------------------------------------------------------------------------


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
        ("bad_vol_share_63", "window"),
        ("reversal_profit_21", "formation"),
        ("reversal_profit_21", "window"),
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


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_the_score_exists_before_the_first_stage_of_the_trial_gate_opens(signal_id: str) -> None:
    # Stage 1 of the trial gate runs on the US market from 1929-01-02 and needs a finite target
    # at the last rebalance before it; the Ken French daily files start on 1926-07-01. The
    # percentile score needs the screen's warm-up of valid values first.
    returns, sources = _random_inputs(900, seed=29)
    index = pd.bdate_range("1926-07-01", periods=900)
    returns = returns.set_axis(index).fillna(0.001)
    sources = {k: v.set_axis(index).fillna(0.001) for k, v in sources.items()}
    got = _run(signal_id, returns, sources)
    warmup = load_screen_settings().settings.screen.warmup_days
    score = screen.expanding_percentile(got, warmup)
    assert pd.notna(score.loc[: pd.Timestamp("1928-12-31")].iloc[-1])


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
    assert "TRADING-2565-25" in text
