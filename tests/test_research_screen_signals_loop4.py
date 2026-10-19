"""Research loop, third batch of screening signals (TRADING-2565-28): the preregistered specs, the
formulas on hand-computed examples, no look-ahead, a value early enough for the trial gate's first
stage, and the whole screen on synthetic data. Synthetic data only."""

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

SIGNALS = ("absorption_shift_63", "industry_dispersion_21", "defensive_lead_63")
INDUSTRIES = "ff_49_industries_daily"
FACTORS = "ff_factors_daily"
DEFENSIVE = ("Food", "Beer", "Smoke", "Hshld", "Drugs", "Util")
# The specification text was fixed before any screen ran; its LF-normalised SHA-256 is pinned.
SPEC_SHA256 = {
    "absorption_shift_63": "26a0b9e8db9f919c791274430bbed31b4816f96ad5547a3d40661fbd007dc255",
    "industry_dispersion_21": "efbaf8f95c1e79ddff36b0bf40012137914644bb6272fb1ea925ebf5a24270eb",
    "defensive_lead_63": "56cc14b56f3c08de131fccde3c2ffa33936d5f7c17cce6e4e83bf46aabe127d4",
}
EXPECTED = {
    "absorption_shift_63": (1, "risk", {"window": 63, "short": 15, "long": 252}, (INDUSTRIES,)),
    "industry_dispersion_21": (1, "risk", {"window": 21}, (INDUSTRIES,)),
    "defensive_lead_63": (1, "risk", {"window": 63}, (INDUSTRIES, FACTORS)),
}
Signal = Callable[[pd.Series, Mapping[str, pd.DataFrame], Mapping[str, Any]], pd.Series]


def _load(signal_id: str) -> screen.LoadedSignal:
    return screen.load_signal(signal_id, directory=screen.SIGNALS_DIR)


def _function(signal_id: str) -> Signal:
    function: Signal = _load(signal_id).function
    return function


def _industries(n: int, columns: int, seed: int, start: str = "1990-01-02") -> pd.DataFrame:
    """Correlated industry returns with late starts and a few gaps, like the Ken French file.

    The first six columns carry the names of the defensive industries; none of them starts late.
    """

    rng = np.random.default_rng(seed)
    index = pd.bdate_range(start, periods=n)
    common = rng.normal(0.0003, 0.008, n)
    loadings = rng.uniform(0.3, 1.5, columns)
    values = common[:, None] * loadings[None, :] + rng.normal(0.0, 0.009, (n, columns))
    for column in range(7, columns, 3):
        values[: int(rng.integers(50, n // 3)), column] = np.nan
    for _ in range(columns):
        values[int(rng.integers(0, n)), int(rng.integers(0, columns))] = np.nan
    names = list(DEFENSIVE) + [f"ind{c:02d}" for c in range(len(DEFENSIVE), columns)]
    return pd.DataFrame(values, index=index, columns=names)


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
    frame.iloc[int(rng.integers(0, n)), 0] = np.nan
    return frame


def _random_inputs(n: int, seed: int) -> tuple[pd.Series, dict[str, pd.DataFrame]]:
    rng = np.random.default_rng(seed + 1000)
    industries = _industries(n, 12, seed)
    returns = pd.Series(rng.normal(0.0004, 0.012, n), index=industries.index)
    returns.iloc[rng.integers(0, n, 3)] = np.nan
    return returns, {INDUSTRIES: industries, FACTORS: _factors(industries.index, seed)}


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


# --- absorption_shift_63 --------------------------------------------------------------------


def _absorption_ratio(block: np.ndarray) -> float:
    """Independent reference for one window: covariance by hand, eigenvalues by eigh."""

    centred = block - block.mean(axis=0)
    covariance = centred.T @ centred / (block.shape[0] - 1)
    eigenvalues = np.sort(np.linalg.eigh(covariance)[0])[::-1]
    return float(eigenvalues[: math.ceil(block.shape[1] / 5)].sum() / eigenvalues.sum())


def _absorption_reference(frame: pd.DataFrame, window: int, short: int, long: int) -> np.ndarray:
    values = frame.to_numpy()
    ratio = np.full(len(values), np.nan)
    for t in range(window - 1, len(values)):
        block = values[t - window + 1 : t + 1]
        block = block[:, ~np.isnan(block).any(axis=0)]
        if block.shape[1] >= 2:
            ratio[t] = _absorption_ratio(block)
    out = np.full(len(values), np.nan)
    for t in range(long - 1, len(values)):
        history = ratio[t - long + 1 : t + 1]
        if np.isnan(history).any() or np.std(history, ddof=1) == 0.0:
            continue
        out[t] = (ratio[t - short + 1 : t + 1].mean() - history.mean()) / np.std(history, ddof=1)
    return out


def test_absorption_ratio_of_two_orthogonal_industries() -> None:
    # Zero-mean, orthogonal columns: the covariance is diagonal with variances 1e-4 and 3e-4, so
    # the top ceil(2 / 5) = 1 eigenvalue explains 0.75. A one-day "shift" over a one-day history
    # has no standard deviation, so the check uses short = 1 and long = 2 over two windows.
    index = pd.bdate_range("2004-01-05", periods=4)
    industries = _frame(index, {"a": [0.01, 0.0, -0.01, 0.01], "b": [0.01, -0.02, 0.01, -0.01]})
    got = _function("absorption_shift_63")(
        pd.Series(0.0, index=index), {INDUSTRIES: industries}, {"window": 3, "short": 1, "long": 2}
    )
    first = _absorption_ratio(industries.to_numpy()[0:3])
    second = _absorption_ratio(industries.to_numpy()[1:4])
    assert first == pytest.approx(0.75, abs=1e-12)
    spread = np.std([first, second], ddof=1)
    expected = [np.nan, np.nan, np.nan, (second - (first + second) / 2) / spread]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)
    assert got.index.equals(index)


def test_absorption_keeps_the_top_fifth_of_the_eigenvalues_rounded_up() -> None:
    # Six zero-mean orthogonal columns with standard deviations 6, 5, ..., 1: k = 6 keeps the top
    # ceil(6 / 5) = 2 eigenvalues, (36 + 25) / 91.
    rng = np.random.default_rng(0)
    basis = np.linalg.qr(np.column_stack([np.ones(8), rng.normal(size=(8, 6))]))[0][:, 1:]
    values = basis * np.array([6.0, 5.0, 4.0, 3.0, 2.0, 1.0]) * 0.01
    assert _absorption_ratio(values) == pytest.approx(61 / 91, abs=1e-12)
    index = pd.bdate_range("2004-01-05", periods=10)
    stacked = np.vstack([values, rng.normal(0.0, 0.02, (2, 6))])
    industries = pd.DataFrame(stacked, index=index, columns=[f"c{i}" for i in range(6)])
    got = _function("absorption_shift_63")(
        pd.Series(0.0, index=index), {INDUSTRIES: industries}, {"window": 8, "short": 1, "long": 3}
    )
    np.testing.assert_allclose(
        got.to_numpy(),
        _absorption_reference(industries, 8, 1, 3),
        atol=1e-9,
        equal_nan=True,
    )


def test_absorption_matches_the_reference_with_gaps_and_late_starts() -> None:
    returns, sources = _random_inputs(400, seed=13)
    industries = sources[INDUSTRIES]
    got = _function("absorption_shift_63")(
        returns, {INDUSTRIES: industries}, {"window": 30, "short": 5, "long": 60}
    )
    np.testing.assert_allclose(
        got.to_numpy(), _absorption_reference(industries, 30, 5, 60), atol=1e-9, equal_nan=True
    )
    assert got.notna().sum() > 200


def test_absorption_has_no_value_with_fewer_than_two_complete_industries() -> None:
    index = pd.bdate_range("2004-01-05", periods=6)
    industries = _frame(
        index,
        {"a": [0.01, -0.02, 0.03, 0.01, 0.0, 0.02], "b": [0.02, np.nan, 0.01, 0.0, 0.01, -0.01]},
    )
    got = _function("absorption_shift_63")(
        pd.Series(0.0, index=index), {INDUSTRIES: industries}, {"window": 2, "short": 1, "long": 2}
    )
    # Windows ending on t1 and t2 contain b's gap, so only one industry is complete there; with
    # two industries the ratio of a 2 x 2 covariance is defined from t3 on.
    assert got.iloc[:4].isna().all()


def test_absorption_is_the_same_for_scaled_returns() -> None:
    returns, sources = _random_inputs(400, seed=19)
    params = {"window": 30, "short": 5, "long": 60}
    base = _function("absorption_shift_63")(returns, {INDUSTRIES: sources[INDUSTRIES]}, params)
    scaled = _function("absorption_shift_63")(
        returns, {INDUSTRIES: 2.5 * sources[INDUSTRIES]}, params
    )
    np.testing.assert_allclose(base.to_numpy(), scaled.to_numpy(), atol=1e-8, equal_nan=True)


# --- industry_dispersion_21 -----------------------------------------------------------------


def test_dispersion_against_hand_computed_windows() -> None:
    index = pd.bdate_range("2004-01-05", periods=4)
    industries = _frame(
        index,
        {
            "a": [0.01, 0.00, 0.02, 0.01],
            "b": [0.02, 0.00, np.nan, 0.03],
            "c": [0.03, 0.04, -0.02, 0.02],
        },
    )
    got = _function("industry_dispersion_21")(
        pd.Series(0.0, index=index), {INDUSTRIES: industries}, {"window": 2}
    )
    # Daily population standard deviations: sqrt(2e-4 / 3); sqrt(32e-4 / 9); t2 uses a and c
    # only: 0.02; sqrt(2e-4 / 3).
    daily = [math.sqrt(2e-4 / 3), math.sqrt(32e-4 / 9), 0.02, math.sqrt(2e-4 / 3)]
    expected = [np.nan] + [(daily[i - 1] + daily[i]) / 2 for i in (1, 2, 3)]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)
    assert got.index.equals(index)


def test_dispersion_has_no_value_on_a_day_with_fewer_than_two_industries() -> None:
    index = pd.bdate_range("2004-01-05", periods=4)
    industries = _frame(index, {"a": [0.01, 0.02, -0.01, 0.0], "b": [0.03, np.nan, 0.01, 0.02]})
    got = _function("industry_dispersion_21")(
        pd.Series(0.0, index=index), {INDUSTRIES: industries}, {"window": 2}
    )
    # t1 has one industry, so the windows ending on t1 and t2 have no value.
    np.testing.assert_allclose(
        got.to_numpy(), [np.nan, np.nan, np.nan, (0.01 + 0.01) / 2], atol=1e-12, equal_nan=True
    )


def test_dispersion_ignores_a_move_shared_by_every_industry() -> None:
    returns, sources = _random_inputs(300, seed=23)
    industries = sources[INDUSTRIES]
    shared = pd.Series(
        np.random.default_rng(4).normal(0.0, 0.02, len(industries)), industries.index
    )
    moved = industries.add(shared, axis=0)
    params = {"window": 21}
    np.testing.assert_allclose(
        _function("industry_dispersion_21")(returns, {INDUSTRIES: industries}, params).to_numpy(),
        _function("industry_dispersion_21")(returns, {INDUSTRIES: moved}, params).to_numpy(),
        atol=1e-12,
        equal_nan=True,
    )


# --- defensive_lead_63 ----------------------------------------------------------------------


def _defensive_frame(index: pd.DatetimeIndex, basket: dict[str, list[float]]) -> pd.DataFrame:
    columns = {name: basket.get(name, [np.nan] * len(index)) for name in DEFENSIVE}
    columns["Steel"] = [0.5] * len(index)
    return _frame(index, columns)


def test_defensive_lead_against_hand_computed_windows() -> None:
    index = pd.bdate_range("2004-01-05", periods=3)
    industries = _defensive_frame(
        index,
        {
            "Food": [0.01, 0.02, -0.01],
            "Smoke": [0.03, 0.00, 0.01],
            "Util": [0.02, 0.01, 0.00],
            "Drugs": [np.nan, 0.03, 0.02],
        },
    )
    factors = _frame(
        index,
        {"Mkt-RF": [0.005, -0.01, 0.02], "SMB": [0.0] * 3, "HML": [0.0] * 3, "RF": [0.001] * 3},
    )
    got = _function("defensive_lead_63")(
        pd.Series(0.0, index=index), {INDUSTRIES: industries, FACTORS: factors}, {"window": 2}
    )
    # Basket: t0 mean of 0.01, 0.03, 0.02 = 0.02; t1 mean of 0.02, 0, 0.01, 0.03 = 0.015; t2 mean
    # of -0.01, 0.01, 0, 0.02 = 0.005. Market: 0.006, -0.009, 0.021. Steel never counts.
    expected = [
        np.nan,
        1.02 * 1.015 - 1.006 * 0.991,
        1.015 * 1.005 - 0.991 * 1.021,
    ]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)
    assert got.index.equals(index)


def test_defensive_lead_needs_three_defensive_industries_that_day() -> None:
    index = pd.bdate_range("2004-01-05", periods=3)
    industries = _defensive_frame(
        index,
        {
            "Food": [0.01, 0.02, -0.01],
            "Beer": [0.02, np.nan, 0.01],
            "Hshld": [0.00, 0.01, 0.02],
        },
    )
    factors = _frame(
        index, {"Mkt-RF": [0.0] * 3, "SMB": [0.0] * 3, "HML": [0.0] * 3, "RF": [0.0] * 3}
    )
    got = _function("defensive_lead_63")(
        pd.Series(0.0, index=index), {INDUSTRIES: industries, FACTORS: factors}, {"window": 1}
    )
    # t1 has two defensive industries only.
    np.testing.assert_allclose(got.to_numpy(), [0.01, np.nan, 0.02 / 3], atol=1e-12, equal_nan=True)


def test_defensive_lead_has_no_value_while_the_market_is_missing_in_the_window() -> None:
    index = pd.bdate_range("2004-01-05", periods=4)
    industries = _defensive_frame(
        index, {name: [0.01, 0.0, 0.02, -0.01] for name in ("Food", "Beer", "Smoke")}
    )
    factors = _frame(
        index,
        {
            "Mkt-RF": [0.01, np.nan, 0.0, 0.01],
            "SMB": [0.0] * 4,
            "HML": [0.0] * 4,
            "RF": [0.0] * 4,
        },
    )
    got = _function("defensive_lead_63")(
        pd.Series(0.0, index=index), {INDUSTRIES: industries, FACTORS: factors}, {"window": 2}
    )
    np.testing.assert_allclose(
        got.to_numpy(),
        [np.nan, np.nan, np.nan, 1.02 * 0.99 - 1.0 * 1.01],
        atol=1e-12,
        equal_nan=True,
    )


def test_defensive_lead_refuses_an_industry_table_without_a_defensive_column() -> None:
    returns, sources = _random_inputs(200, seed=31)
    industries = sources[INDUSTRIES].drop(columns=["Hshld"])
    with pytest.raises(ValueError, match="Hshld"):
        _function("defensive_lead_63")(
            returns, {INDUSTRIES: industries, FACTORS: sources[FACTORS]}, {"window": 63}
        )


def test_defensive_lead_reads_only_the_defensive_columns_and_the_market() -> None:
    returns, sources = _random_inputs(400, seed=37)
    other = sources[INDUSTRIES].copy()
    other["ind07"] = other["ind07"] * -3.0 + 0.01
    changed_factors = sources[FACTORS].assign(SMB=0.5, HML=-0.5)
    base = _run("defensive_lead_63", returns, sources)
    moved = _run("defensive_lead_63", returns, {INDUSTRIES: other, FACTORS: changed_factors})
    pd.testing.assert_series_equal(base, moved)


# --- all three ------------------------------------------------------------------------------


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
    ("signal_id", "name"),
    [
        ("absorption_shift_63", "window"),
        ("absorption_shift_63", "short"),
        ("absorption_shift_63", "long"),
        ("industry_dispersion_21", "window"),
        ("defensive_lead_63", "window"),
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
    industries = _industries(6200, 10, seed=4, start="1988-01-04")
    sources = {INDUSTRIES: industries, FACTORS: _factors(industries.index, seed=4)}
    lags = {INDUSTRIES: 0, FACTORS: 0}
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
    assert "TRADING-2565-28" in text
