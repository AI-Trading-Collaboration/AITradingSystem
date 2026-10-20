"""Research loop, fourth batch of screening signals (TRADING-2565-30): the preregistered specs, the
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

SIGNALS = ("may_oct_share_30", "growth_over_value_252")
FACTORS = "ff_factors_daily"
# The specification text was fixed before any screen ran; its LF-normalised SHA-256 is pinned.
SPEC_SHA256 = {
    "may_oct_share_30": "ae9d5fc8c4871288fdb2712659e3cbba47415fb973b478dfcd54305e719d1bd1",
    "growth_over_value_252": "1b492e7c21d89007e89c02a2a2f9a24f392eda9678a91a5cc140774d2388ac4e",
}
EXPECTED = {
    "may_oct_share_30": (-1, "return", {"days": 30}, ()),
    "growth_over_value_252": (1, "risk", {"window": 252}, (FACTORS,)),
}
Signal = Callable[[pd.Series, Mapping[str, pd.DataFrame], Mapping[str, Any]], pd.Series]


def _load(signal_id: str) -> screen.LoadedSignal:
    return screen.load_signal(signal_id, directory=screen.SIGNALS_DIR)


def _function(signal_id: str) -> Signal:
    function: Signal = _load(signal_id).function
    return function


def _factors(index: pd.DatetimeIndex, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 500)
    n = len(index)
    frame = pd.DataFrame(
        {
            "Mkt-RF": rng.normal(0.0003, 0.011, n),
            "SMB": rng.normal(0.0, 0.005, n),
            "HML": rng.normal(0.0, 0.005, n),
            "RF": np.full(n, 0.0001),
        },
        index=index,
    )
    for _ in range(3):
        frame.iloc[int(rng.integers(0, n)), int(rng.integers(0, 4))] = np.nan
    return frame


def _random_inputs(n: int, seed: int) -> tuple[pd.Series, dict[str, pd.DataFrame]]:
    rng = np.random.default_rng(seed + 1000)
    index = pd.bdate_range("1990-01-02", periods=n)
    returns = pd.Series(rng.normal(0.0004, 0.012, n), index=index)
    returns.iloc[rng.integers(0, n, 3)] = np.nan
    return returns, {FACTORS: _factors(index, seed)}


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


# --- may_oct_share_30 -----------------------------------------------------------------------


def _may_oct(days: list[str], params: Mapping[str, Any] | None = None) -> pd.Series:
    index = pd.DatetimeIndex(days)
    return _function("may_oct_share_30")(
        pd.Series(0.0, index=index), {}, dict(params or {"days": 30})
    )


def test_may_oct_share_against_hand_counted_days() -> None:
    got = _may_oct(
        [
            "2004-04-01",  # Apr 2-30 (29 days) and May 1: 1 of 30
            "2004-04-15",  # Apr 16-30 (15) and May 1-15 (15): 15 of 30
            "2004-04-30",  # May 1-30: 30 of 30
            "2004-07-01",  # Jul 2-31: 30 of 30
            "2004-10-04",  # Oct 5-31 (27) and Nov 1-3 (3): 27 of 30
            "2004-10-29",  # Oct 30-31 (2) and Nov 1-28 (28): 2 of 30
            "2004-12-15",  # Dec 16 to Jan 14: 0 of 30
        ]
    )
    expected = [1 / 30, 15 / 30, 1.0, 1.0, 27 / 30, 2 / 30, 0.0]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12)


def test_may_oct_share_counts_the_leap_day() -> None:
    # 63 days after Feb 28: in 2004 (a leap year) Feb 29, Mar 1-31, Apr 1-30 and May 1, one day in
    # May; in 2003 Mar 1-31, Apr 1-30 and May 1-2, two days.
    got = _may_oct(["2003-02-28", "2004-02-28"], {"days": 63})
    np.testing.assert_allclose(got.to_numpy(), [2 / 63, 1 / 63], atol=1e-12)


def test_may_oct_share_uses_the_calendar_not_the_trading_days_in_the_index() -> None:
    dense = pd.bdate_range("2003-01-02", "2006-12-29")
    sparse = dense[::7]
    full = _function("may_oct_share_30")(pd.Series(0.0, index=dense), {}, {"days": 30})
    part = _function("may_oct_share_30")(pd.Series(0.0, index=sparse), {}, {"days": 30})
    pd.testing.assert_series_equal(part, full.loc[sparse], check_freq=False)


def test_may_oct_share_ignores_the_time_of_day() -> None:
    days = pd.DatetimeIndex(["2004-04-15", "2004-10-29"])
    stamped = days + pd.Timedelta(hours=16)
    base = _function("may_oct_share_30")(pd.Series(0.0, index=days), {}, {"days": 30})
    later = _function("may_oct_share_30")(pd.Series(0.0, index=stamped), {}, {"days": 30})
    np.testing.assert_allclose(base.to_numpy(), later.to_numpy(), atol=1e-12)
    assert later.index.equals(stamped)


def test_may_oct_share_has_a_value_on_every_day_even_without_returns() -> None:
    index = pd.bdate_range("2004-01-05", periods=300)
    returns = pd.Series(np.nan, index=index)
    got = _function("may_oct_share_30")(returns, {}, {"days": 30})
    assert got.notna().all() and got.between(0.0, 1.0).all()


# --- growth_over_value_252 ------------------------------------------------------------------


def _factor_frame(index: pd.DatetimeIndex, hml: list[float]) -> pd.DataFrame:
    n = len(index)
    return pd.DataFrame(
        {"Mkt-RF": [0.01] * n, "SMB": [-0.02] * n, "HML": hml, "RF": [0.0001] * n},
        index=index,
        dtype=float,
    )


def test_growth_over_value_against_hand_computed_windows() -> None:
    index = pd.bdate_range("2004-01-05", periods=4)
    factors = _factor_frame(index, [0.01, -0.02, 0.03, 0.005])
    got = _function("growth_over_value_252")(
        pd.Series(0.0, index=index), {FACTORS: factors}, {"window": 2}
    )
    expected = [np.nan, 1 - 1.01 * 0.98, 1 - 0.98 * 1.03, 1 - 1.03 * 1.005]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)
    assert got.index.equals(index)


def test_growth_over_value_has_no_value_while_hml_is_missing_in_the_window() -> None:
    index = pd.bdate_range("2004-01-05", periods=5)
    factors = _factor_frame(index, [0.01, np.nan, 0.02, -0.01, 0.0])
    got = _function("growth_over_value_252")(
        pd.Series(0.0, index=index), {FACTORS: factors}, {"window": 2}
    )
    np.testing.assert_allclose(
        got.to_numpy(),
        [np.nan, np.nan, np.nan, 1 - 1.02 * 0.99, 1 - 0.99 * 1.0],
        atol=1e-12,
        equal_nan=True,
    )


def test_growth_over_value_reads_only_hml() -> None:
    returns, sources = _random_inputs(600, seed=37)
    changed = sources[FACTORS].assign(**{"Mkt-RF": 0.5, "SMB": -0.5, "RF": 0.02})
    pd.testing.assert_series_equal(
        _run("growth_over_value_252", returns, sources),
        _run("growth_over_value_252", returns, {FACTORS: changed}),
    )


def test_growth_over_value_is_high_when_growth_has_beaten_value() -> None:
    index = pd.bdate_range("2004-01-05", periods=300)
    hml = [0.002] * 150 + [-0.002] * 150
    got = _function("growth_over_value_252")(
        pd.Series(0.0, index=index), {FACTORS: _factor_frame(index, hml)}, {"window": 100}
    )
    assert got.iloc[149] < 0 < got.iloc[-1]


def test_growth_over_value_refuses_a_table_without_hml() -> None:
    returns, sources = _random_inputs(400, seed=31)
    with pytest.raises((KeyError, ValueError), match="HML"):
        _function("growth_over_value_252")(
            returns, {FACTORS: sources[FACTORS].drop(columns=["HML"])}, {"window": 252}
        )


# --- both -----------------------------------------------------------------------------------


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_the_signal_ignores_the_market_returns(signal_id: str) -> None:
    returns, sources = _random_inputs(600, seed=8)
    shuffled = pd.Series(np.random.default_rng(1).permutation(returns.to_numpy()), returns.index)
    pd.testing.assert_series_equal(
        _run(signal_id, returns, sources), _run(signal_id, shuffled, sources)
    )


@pytest.mark.parametrize("signal_id", SIGNALS)
def test_a_value_never_depends_on_later_data(signal_id: str) -> None:
    returns, sources = _random_inputs(900, seed=11)
    full = _run(signal_id, returns, sources)
    for cut in (90, 400, 723):
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
    ("signal_id", "name"), [("may_oct_share_30", "days"), ("growth_over_value_252", "window")]
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
    index = pd.bdate_range("1988-01-04", periods=6200)
    sources = {FACTORS: _factors(index, seed=4)}
    lags = {FACTORS: 0}
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
    assert "TRADING-2565-30" in text
