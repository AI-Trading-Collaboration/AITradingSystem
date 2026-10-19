"""realized_vol_21 (TRADING-2567-31): the reference signal of QQQ-Q4 is the screen baseline rv_21
as a module a preregistration can pin. Its specification, formula, no look-ahead, and the
exposure the Q4 mapping gives. Synthetic data only."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from ai_trading_system.research_bench import checks, screen
from ai_trading_system.research_bench.screen_settings import load_screen_settings
from ai_trading_system.research_bench.signal_strategy import signal_exposure

SIGNAL_ID = "realized_vol_21"
# The specification text was fixed before any result existed; its LF-normalised SHA-256 is pinned.
SPEC_SHA256 = "179342c45994b4be56354d0f4426075a889b0f21feba86c39831f1ff7446c970"
Signal = Callable[[pd.Series, Mapping[str, pd.DataFrame], Mapping[str, Any]], pd.Series]


def _load() -> screen.LoadedSignal:
    return screen.load_signal(SIGNAL_ID, directory=screen.SIGNALS_DIR)


def _function() -> Signal:
    function: Signal = _load().function
    return function


def _returns(n: int, seed: int, start: str = "1999-01-04") -> pd.Series:
    rng = np.random.default_rng(seed)
    scale = np.where(np.arange(n) % 400 < 120, 0.025, 0.010)
    return pd.Series(rng.normal(0.0004, 1.0, n) * scale, index=pd.bdate_range(start, periods=n))


def test_the_specification_is_the_preregistered_text() -> None:
    loaded = _load()
    assert loaded.spec.signal_id == SIGNAL_ID
    assert (loaded.spec.expected_sign, loaded.spec.target) == (1, "risk")
    assert loaded.spec.params == {"window": 21} and loaded.spec.sources == ()
    assert loaded.spec.markets == "all"
    assert loaded.spec.mechanism.strip() and loaded.spec.literature.strip() != "none"
    assert loaded.spec_sha256 == SPEC_SHA256
    raw = yaml.safe_load((screen.SIGNALS_DIR / f"{SIGNAL_ID}.yaml").read_text(encoding="utf-8"))
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


def test_it_is_the_screening_baseline_rv_21() -> None:
    returns = _returns(900, seed=29)
    got = _function()(returns.copy(), {}, {"window": 21})
    baseline = screen.baseline_signals(returns)["rv_21"]
    np.testing.assert_array_equal(got.to_numpy(), baseline.to_numpy())
    assert got.isna().sum() == 20


def test_by_hand_the_sample_standard_deviation_annualised() -> None:
    index = pd.bdate_range("2004-01-05", periods=5)
    returns = pd.Series([0.01, -0.01, 0.03, 0.0, 0.0], index=index)
    got = _function()(returns, {}, {"window": 2})
    root = math.sqrt(252)
    expected = [
        np.nan,
        math.sqrt(((0.01 - 0.0) ** 2 + (-0.01 - 0.0) ** 2) / 1) * root,
        math.sqrt(((-0.01 - 0.01) ** 2 + (0.03 - 0.01) ** 2) / 1) * root,
        math.sqrt(((0.03 - 0.015) ** 2 + (0.0 - 0.015) ** 2) / 1) * root,
        0.0,
    ]
    np.testing.assert_allclose(got.to_numpy(), expected, atol=1e-12, equal_nan=True)
    got = _function()(returns, {}, {"window": 3})
    assert got.iloc[2] == pytest.approx(np.std([0.01, -0.01, 0.03], ddof=1) * root, abs=1e-12)


def test_it_is_missing_while_a_missing_return_is_in_the_window() -> None:
    index = pd.bdate_range("2004-01-05", periods=6)
    returns = pd.Series([0.01, np.nan, 0.02, -0.01, 0.0, 0.01], index=index)
    got = _function()(returns, {}, {"window": 2})
    assert got.iloc[:3].isna().all() and got.iloc[3:].notna().all()


def test_a_value_never_depends_on_later_data() -> None:
    returns = _returns(700, seed=11)
    full = _function()(returns.copy(), {}, {"window": 21})
    for cut in (80, 260, 411):
        part = _function()(returns.iloc[:cut].copy(), {}, {"window": 21})
        pd.testing.assert_series_equal(part, full.iloc[:cut], check_exact=True)


def test_the_output_is_a_finite_float_series_on_the_input_index() -> None:
    returns = _returns(900, seed=5)
    got = _function()(returns, {}, {"window": 21})
    assert isinstance(got, pd.Series) and got.index.equals(returns.index)
    assert got.dtype == float and not np.isinf(got.to_numpy()).any()
    assert (got.dropna() >= 0).all() and got.notna().sum() == 880


def test_inputs_are_not_modified() -> None:
    returns = _returns(400, seed=3)
    before = returns.copy()
    _function()(returns, {}, {"window": 21})
    pd.testing.assert_series_equal(returns, before)


def test_the_window_parameter_changes_the_output() -> None:
    returns = _returns(1500, seed=21)
    base = _function()(returns, {}, {"window": 21})
    other = _function()(returns, {}, {"window": 22})
    assert not np.allclose(base.to_numpy(), other.to_numpy(), equal_nan=True, atol=1e-12)


# The mapping QQQ-Q4 preregisters for rv21 (research/QQQ-Q4/prereg.yaml): what it gives on
# synthetic data.
def _rv21() -> dict[str, Any]:
    loaded = _load()
    ref = {
        "id": SIGNAL_ID,
        "module_sha256": loaded.module_sha256,
        "spec_sha256": loaded.spec_sha256,
        "score": "percentile",
    }
    return {"signals": [ref], "mapping": "linear", "low": 0, "high": 1}


def test_rv21_exposure_is_one_minus_the_expanding_percentile_of_volatility() -> None:
    returns = _returns(1600, seed=8)
    got = signal_exposure(pd.DataFrame({"ndx_return": returns}), _rv21())
    warmup = load_screen_settings().settings.screen.warmup_days
    percentile = screen.expanding_percentile(screen.baseline_signals(returns)["rv_21"], warmup)
    valid = percentile.notna()
    assert valid.sum() > 1000 and got[~valid].isna().all()
    np.testing.assert_allclose(
        got[valid].to_numpy(), 1.0 - percentile[valid].to_numpy(), atol=1e-12
    )
    # Low volatility is the favoured end: the calm stretches carry more exposure than the
    # turbulent ones.
    calm = np.arange(len(returns)) % 400 >= 160
    assert got[valid & calm].mean() > got[valid & ~calm].mean() + 0.2


def test_the_rv21_mapping_reads_every_numeric_parameter_and_looks_ahead_of_nothing() -> None:
    returns = _returns(1800, seed=9)
    inputs = pd.DataFrame({"ndx_return": returns})
    start, end = pd.Timestamp("2002-01-02"), pd.Timestamp("2005-12-30")
    problems, unchecked = checks.check_parameters_consumed(
        signal_exposure, inputs, _rv21(), start, end
    )
    assert problems == [] and set(unchecked) == {"mapping", "signals"}
    dates = checks.cut_dates(pd.DatetimeIndex(inputs.index), start, end, 20)
    assert checks.check_no_lookahead(signal_exposure, inputs, _rv21(), dates) == []


def test_the_signals_readme_names_the_reference_signal() -> None:
    text = (screen.SIGNALS_DIR / "README.md").read_text(encoding="utf-8")
    assert SIGNAL_ID in text and "TRADING-2567-31" in text and "QQQ-Q4" in text
