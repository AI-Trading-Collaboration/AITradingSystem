from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.backtest.daily import (
    render_backtest_report,
    run_daily_score_backtest,
    write_backtest_daily_csv,
)
from ai_trading_system.cli import app
from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_features,
    load_portfolio,
    load_scoring_rules,
    load_universe,
)
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport


def test_run_daily_score_backtest_uses_next_day_returns() -> None:
    universe = load_universe()
    prices = _sample_prices(configured_price_tickers(universe), periods=320)
    rates = _sample_rates(configured_rate_series(universe), periods=320)

    result = run_daily_score_backtest(
        prices=prices,
        rates=rates,
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 30),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY", "QQQ"),
        cost_bps=5.0,
    )

    assert result.rows
    assert result.rows[0].return_date > result.rows[0].signal_date
    assert result.strategy_metrics.total_return > 0
    assert set(result.benchmark_metrics) == {"SPY", "QQQ"}


def test_render_and_write_backtest_outputs(tmp_path: Path) -> None:
    universe = load_universe()
    result = run_daily_score_backtest(
        prices=_sample_prices(configured_price_tickers(universe), periods=320),
        rates=_sample_rates(configured_rate_series(universe), periods=320),
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio_config=load_portfolio(),
        data_quality_report=_quality_report(),
        core_watchlist=universe.ai_chain["core_watchlist"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 30),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
    )

    daily_path = write_backtest_daily_csv(result, tmp_path / "daily.csv")
    markdown = render_backtest_report(
        result,
        data_quality_report_path=tmp_path / "quality.md",
        daily_output_path=daily_path,
    )

    assert daily_path.exists()
    assert "# 历史回测报告" in markdown
    assert "基准（SPY 买入持有）" in markdown


def test_backtest_cli_writes_report_and_daily_csv(tmp_path: Path) -> None:
    universe = load_universe()
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    report_path = tmp_path / "backtest.md"
    daily_path = tmp_path / "backtest_daily.csv"
    quality_path = tmp_path / "quality.md"
    _sample_prices(configured_price_tickers(universe), periods=320).to_csv(
        prices_path,
        index=False,
    )
    _sample_rates(configured_rate_series(universe), periods=320).to_csv(
        rates_path,
        index=False,
    )

    result = CliRunner().invoke(
        app,
        [
            "backtest",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--from",
            "2026-04-01",
            "--to",
            "2026-04-30",
            "--quality-as-of",
            "2026-05-02",
            "--report-path",
            str(report_path),
            "--daily-output-path",
            str(daily_path),
            "--quality-report-path",
            str(quality_path),
            "--benchmarks",
            "SPY,QQQ",
        ],
    )

    assert result.exit_code == 0
    assert report_path.exists()
    assert daily_path.exists()
    assert quality_path.exists()
    assert "回测状态：" in result.output


def _quality_report() -> DataQualityReport:
    return DataQualityReport(
        checked_at=pd.Timestamp("2026-05-01T00:00:00Z").to_pydatetime(),
        as_of=date(2026, 5, 2),
        price_summary=DataFileSummary(path=Path("prices.csv"), exists=True, rows=1),
        rate_summary=DataFileSummary(path=Path("rates.csv"), exists=True, rows=1),
        expected_price_tickers=("SPY",),
        expected_rate_series=("DGS10",),
        issues=(),
    )


def _sample_prices(tickers: list[str], periods: int) -> pd.DataFrame:
    dates = pd.date_range(end="2026-04-30", periods=periods, freq="D")
    rows: list[dict[str, object]] = []
    for ticker_index, ticker in enumerate(tickers):
        base = 100.0 + ticker_index * 10.0
        daily_step = 0.2 + ticker_index * 0.01
        for day_index, row_date in enumerate(dates):
            close = base + day_index * daily_step
            rows.append(
                {
                    "date": row_date.date().isoformat(),
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


def _sample_rates(series_ids: list[str], periods: int) -> pd.DataFrame:
    dates = pd.date_range(end="2026-04-30", periods=periods, freq="D")
    rows: list[dict[str, object]] = []
    for series_index, series_id in enumerate(series_ids):
        base = 4.0 + series_index * 0.2
        for day_index, row_date in enumerate(dates):
            rows.append(
                {
                    "date": row_date.date().isoformat(),
                    "series": series_id,
                    "value": base - day_index * 0.001,
                }
            )
    return pd.DataFrame(rows)
