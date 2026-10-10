"""Invariant G2: features at an as-of date never depend on data dated after it.

A look-ahead leak is the most damaging silent defect in a backtest: results look better and nothing
fails. The check recomputes the features with every later row replaced by wild values; the output
must not move. A deliberately leaky function proves that the check itself can fail.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date

import pandas as pd
import pytest

from ai_trading_system.config import (
    CoreBreadthFeatureConfig,
    FeatureConfig,
    RateFeatureConfig,
    RelativeStrengthPairConfig,
    VixFeatureConfig,
)
from ai_trading_system.features.market import build_market_features

AS_OF = date(2026, 4, 20)
LAST_DAY = "2026-04-30"
TICKERS = ["SPY", "SMH", "QQQ", "MSFT", "NVDA", "^VIX"]
RATE_SERIES = ["DGS2", "DGS10", "DTWEXBGS"]

Compute = Callable[[pd.DataFrame, pd.DataFrame, date], pd.DataFrame]


def _config() -> FeatureConfig:
    return FeatureConfig(
        moving_average_windows=[3],
        return_windows=[1, 2],
        relative_strength_pairs=[RelativeStrengthPairConfig(numerator="SMH", denominator="SPY")],
        vix=VixFeatureConfig(ticker="^VIX", moving_average_window=3, percentile_window=3),
        rates=RateFeatureConfig(
            change_series=["DGS2", "DGS10"],
            change_windows=[1, 2],
            return_series=["DTWEXBGS"],
        ),
        core_breadth=CoreBreadthFeatureConfig(long_moving_average_window=3),
    )


def _prices() -> pd.DataFrame:
    days = pd.date_range(end=LAST_DAY, periods=40, freq="D")
    rows = []
    for ticker_index, ticker in enumerate(TICKERS):
        for day_index, day in enumerate(days):
            close = 100.0 + ticker_index * 10.0 + day_index * (1.0 + ticker_index * 0.05)
            rows.append(
                {
                    "date": day.date().isoformat(),
                    "ticker": ticker,
                    "open": close - 0.5,
                    "high": close + 1.0,
                    "low": close - 1.0,
                    "close": close,
                    "adj_close": close,
                    "volume": 1_000_000 + ticker_index,
                }
            )
    return pd.DataFrame(rows)


def _rates() -> pd.DataFrame:
    days = pd.date_range(end=LAST_DAY, periods=40, freq="D")
    rows = []
    for series_index, series_id in enumerate(RATE_SERIES):
        for day_index, day in enumerate(days):
            rows.append(
                {
                    "date": day.date().isoformat(),
                    "series": series_id,
                    "value": 4.0 + series_index * 0.2 + day_index * 0.001,
                }
            )
    return pd.DataFrame(rows)


def _scramble_after(frame: pd.DataFrame, as_of: date, columns: list[str]) -> pd.DataFrame:
    """Replace every value dated after ``as_of`` with a wildly different one."""
    scrambled = frame.copy()
    later = scrambled["date"] > as_of.isoformat()
    for column in columns:
        scrambled.loc[later, column] = scrambled.loc[later, column] * 7.5 + 1234.5
    return scrambled


def assert_independent_of_future(compute: Compute) -> None:
    prices, rates = _prices(), _rates()
    reference = compute(prices, rates, AS_OF)
    assert not reference.empty, "the computation produced nothing, the check would be vacuous"
    scrambled_prices = _scramble_after(prices, AS_OF, ["open", "high", "low", "close", "adj_close"])
    scrambled_rates = _scramble_after(rates, AS_OF, ["value"])
    pd.testing.assert_frame_equal(reference, compute(scrambled_prices, scrambled_rates, AS_OF))


def _real_builder(prices: pd.DataFrame, rates: pd.DataFrame, as_of: date) -> pd.DataFrame:
    return build_market_features(
        prices=prices,
        rates=rates,
        config=_config(),
        as_of=as_of,
        core_watchlist=["MSFT", "NVDA"],
    ).to_frame()


def _leaky_builder(prices: pd.DataFrame, rates: pd.DataFrame, as_of: date) -> pd.DataFrame:
    """Looks at the final row of the whole frame instead of the as-of row: a real-world leak."""
    last_close = prices.sort_values("date").groupby("ticker")["close"].last()
    return last_close.rename("value").reset_index()


def test_market_features_do_not_depend_on_data_after_as_of() -> None:
    assert_independent_of_future(_real_builder)


def test_the_check_itself_detects_a_look_ahead_leak() -> None:
    with pytest.raises(AssertionError):
        assert_independent_of_future(_leaky_builder)
