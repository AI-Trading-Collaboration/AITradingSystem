from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

import ai_trading_system.cli as cli_module
from ai_trading_system.cli import app
from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.market_data import PriceRequest
from ai_trading_system.data.quality import validate_data_cache
from ai_trading_system.price_source_diagnostics import (
    build_yahoo_price_diagnostic_report,
    render_yahoo_price_diagnostic_report,
)


@dataclass(frozen=True)
class FakeYahooProvider:
    close_by_ticker: dict[str, float]

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        rows = []
        for ticker in request.tickers:
            close = self.close_by_ticker[ticker]
            rows.append(
                {
                    "date": "2026-04-30",
                    "ticker": ticker,
                    "open": close - 1.0,
                    "high": close + 1.0,
                    "low": close - 2.0,
                    "close": close,
                    "adj_close": close,
                    "volume": 100,
                }
            )
        return pd.DataFrame(rows)


@dataclass(frozen=True)
class FailingYahooProvider:
    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        raise AssertionError("Yahoo provider should not be called")


def test_yahoo_price_diagnostic_targets_marketstack_self_check_rows(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    secondary_path = tmp_path / "prices_marketstack_daily.csv"
    secondary = pd.read_csv(prices_path)
    secondary.loc[
        (secondary["ticker"] == "NVDA") & (secondary["date"] == "2026-04-30"),
        "close",
    ] = 0.0
    secondary.to_csv(secondary_path, index=False)
    quality_config = load_data_quality()
    quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(load_universe()),
        expected_rate_series=configured_rate_series(load_universe()),
        quality_config=quality_config,
        as_of=date(2026, 5, 2),
        secondary_prices_path=secondary_path,
        require_secondary_prices=True,
    )

    report = build_yahoo_price_diagnostic_report(
        primary_prices_path=prices_path,
        marketstack_prices_path=secondary_path,
        quality_report=quality_report,
        quality_config=quality_config,
        yahoo_provider=FakeYahooProvider({"NVDA": 104.0}),
        as_of=date(2026, 5, 2),
    )
    markdown = render_yahoo_price_diagnostic_report(report)

    assert report.status == "PASS"
    assert report.targets[0].ticker == "NVDA"
    assert report.targets[0].date == date(2026, 4, 30)
    assert report.row_count == 1
    assert len(report.checksum_sha256) == 64
    assert report.comparisons[0].verdict == "YAHOO_SUPPORTS_PRIMARY_MARKETSTACK_SELF_CHECK"
    assert "diagnostic only / production_effect=none" in markdown
    assert "Yahoo 失败、空结果或与 FMP 不一致只进入调查报告" in markdown


def test_yahoo_price_diagnostic_does_not_fetch_without_self_check_targets(
    tmp_path: Path,
) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    secondary_path = tmp_path / "prices_marketstack_daily.csv"
    pd.read_csv(prices_path).to_csv(secondary_path, index=False)
    quality_config = load_data_quality()
    quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(load_universe()),
        expected_rate_series=configured_rate_series(load_universe()),
        quality_config=quality_config,
        as_of=date(2026, 5, 2),
        secondary_prices_path=secondary_path,
        require_secondary_prices=True,
    )

    report = build_yahoo_price_diagnostic_report(
        primary_prices_path=prices_path,
        marketstack_prices_path=secondary_path,
        quality_report=quality_report,
        quality_config=quality_config,
        yahoo_provider=FailingYahooProvider(),
        as_of=date(2026, 5, 2),
    )

    assert report.status == "NO_MARKETSTACK_SELF_CHECK_ISSUES"
    assert report.targets == ()
    assert report.row_count == 0


def test_yahoo_price_diagnostic_cli_writes_report_without_mutating_caches(
    tmp_path: Path,
    monkeypatch,
) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    secondary_path = tmp_path / "prices_marketstack_daily.csv"
    secondary = pd.read_csv(prices_path)
    secondary.loc[
        (secondary["ticker"] == "NVDA") & (secondary["date"] == "2026-04-30"),
        "close",
    ] = 0.0
    secondary.to_csv(secondary_path, index=False)
    before_primary = prices_path.read_text(encoding="utf-8")
    before_secondary = secondary_path.read_text(encoding="utf-8")
    output_path = tmp_path / "yahoo_diag.md"
    monkeypatch.setattr(
        cli_module,
        "YFinancePriceProvider",
        lambda: FakeYahooProvider({"NVDA": 104.0}),
    )

    result = CliRunner().invoke(
        app,
        [
            "data-sources",
            "yahoo-price-diagnostic",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--marketstack-prices-path",
            str(secondary_path),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "Yahoo 价格诊断状态：PASS" in result.output
    assert output_path.exists()
    assert "Production effect：none" in output_path.read_text(encoding="utf-8")
    assert prices_path.read_text(encoding="utf-8") == before_primary
    assert secondary_path.read_text(encoding="utf-8") == before_secondary


def _write_valid_cache(tmp_path: Path) -> tuple[Path, Path]:
    tickers = configured_price_tickers(load_universe())
    rates = configured_rate_series(load_universe())
    price_rows = []
    for ticker in tickers:
        price_rows.extend(
            [
                _price_row("2026-04-29", ticker, 100.0),
                _price_row("2026-04-30", ticker, 104.0),
            ]
        )
    rate_rows = [
        {"date": "2026-04-30", "series": series, "value": 4.5}
        for series in rates
    ]
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)
    pd.DataFrame(rate_rows).to_csv(rates_path, index=False)
    return prices_path, rates_path


def _price_row(row_date: str, ticker: str, close: float) -> dict[str, object]:
    return {
        "date": row_date,
        "ticker": ticker,
        "open": close - 1.0,
        "high": close + 1.0,
        "low": close - 2.0,
        "close": close,
        "adj_close": close,
        "volume": 1000,
    }
