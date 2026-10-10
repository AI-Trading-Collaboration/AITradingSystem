"""Invariant G1: a failed data-quality check stops downstream work and leaves no output behind.

AGENTS.md: any command that produces technical features, scores, backtests or reports from cached
data must run the data-quality gate first and stop on failure. This exercises the real CLI with a
cache that is valid (control) and with caches that carry one defect each.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import configured_price_tickers, configured_rate_series, load_universe

AS_OF = "2026-04-30"
PERIODS = 260


def _prices(tickers: list[str]) -> pd.DataFrame:
    dates = pd.date_range(end=AS_OF, periods=PERIODS, freq="D")
    rows = []
    for ticker_index, ticker in enumerate(tickers):
        base = 100.0 + ticker_index * 10.0
        step = 1.0 + ticker_index * 0.05
        for day_index, day in enumerate(dates):
            close = base + day_index * step
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


def _rates(series_ids: list[str]) -> pd.DataFrame:
    dates = pd.date_range(end=AS_OF, periods=PERIODS, freq="D")
    rows = []
    for series_index, series_id in enumerate(series_ids):
        base = 4.0 + series_index * 0.2
        for day_index, day in enumerate(dates):
            rows.append(
                {
                    "date": day.date().isoformat(),
                    "series": series_id,
                    "value": base + day_index * 0.001,
                }
            )
    return pd.DataFrame(rows)


def _duplicate_key(prices: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([prices, prices.iloc[[10]]], ignore_index=True)


def _non_positive_close(prices: pd.DataFrame) -> pd.DataFrame:
    broken = prices.copy()
    broken.loc[broken.index[100], ["close", "adj_close"]] = 0.0
    return broken


def _missing_required_ticker(prices: pd.DataFrame) -> pd.DataFrame:
    return prices[prices["ticker"] != "SMH"].reset_index(drop=True)


DEFECTS = {
    "duplicate_price_key": _duplicate_key,
    "non_positive_close": _non_positive_close,
    "missing_required_ticker": _missing_required_ticker,
}


def _invoke_build_features(tmp_path: Path, prices: pd.DataFrame):  # type: ignore[no-untyped-def]
    universe = load_universe()
    rates = _rates(configured_rate_series(universe))
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    prices.to_csv(prices_path, index=False)
    rates.to_csv(rates_path, index=False)
    outputs = {
        "features": tmp_path / "features_daily.csv",
        "report": tmp_path / "feature_summary.md",
        "availability": tmp_path / "feature_availability.md",
        "quality": tmp_path / "quality.md",
    }
    result = CliRunner().invoke(
        app,
        [
            "build-features",
            "--prices-path", str(prices_path),
            "--rates-path", str(rates_path),
            "--as-of", AS_OF,
            "--output-path", str(outputs["features"]),
            "--report-path", str(outputs["report"]),
            "--quality-report-path", str(outputs["quality"]),
            "--feature-availability-report-path", str(outputs["availability"]),
        ],
    )  # fmt: skip
    return result, outputs


def test_control_valid_cache_passes_the_gate_and_writes_features(tmp_path: Path) -> None:
    prices = _prices(configured_price_tickers(load_universe()))
    result, outputs = _invoke_build_features(tmp_path, prices)
    assert result.exit_code == 0, result.output
    assert outputs["features"].exists()


@pytest.mark.parametrize("defect", sorted(DEFECTS))
def test_data_quality_failure_blocks_build_features_and_writes_no_features(
    tmp_path: Path, defect: str
) -> None:
    prices = DEFECTS[defect](_prices(configured_price_tickers(load_universe())))
    result, outputs = _invoke_build_features(tmp_path, prices)
    assert result.exit_code != 0, f"{defect}: the gate let a defective cache through"
    assert not outputs["features"].exists(), f"{defect}: features were written despite the failure"
    # The failure must stay auditable: the quality report is written and says it failed.
    assert outputs["quality"].exists()
    assert "FAIL" in outputs["quality"].read_text(encoding="utf-8")
