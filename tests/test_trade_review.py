from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_universe,
    load_watchlist,
)
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.trade_review import (
    build_trade_review_report,
    load_trade_record_store,
    render_trade_review_report,
    validate_trade_record_store,
    write_trade_review_report,
)


def test_validate_trade_record_store_passes_valid_trade(tmp_path: Path) -> None:
    trade_path = tmp_path / "trade.yaml"
    _write_valid_trade(trade_path)

    report = validate_trade_record_store(
        store=load_trade_record_store(trade_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )

    assert report.status == "PASS"
    assert report.trade_count == 1
    assert report.closed_count == 1


def test_validate_trade_record_store_warns_without_thesis(tmp_path: Path) -> None:
    trade_path = tmp_path / "trade.yaml"
    _write_valid_trade(trade_path, thesis_id=None)

    report = validate_trade_record_store(
        store=load_trade_record_store(trade_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is True
    assert "trade_without_thesis" in {issue.code for issue in report.issues}


def test_build_trade_review_report_compares_benchmarks(tmp_path: Path) -> None:
    trade_path = tmp_path / "trade.yaml"
    _write_valid_trade(trade_path)
    validation_report = validate_trade_record_store(
        store=load_trade_record_store(trade_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )

    review_report = build_trade_review_report(
        validation_report=validation_report,
        prices=_sample_prices(configured_price_tickers(load_universe()), periods=90),
        data_quality_report=_quality_report(),
        benchmark_tickers=("SPY", "QQQ", "SMH", "SOXX"),
    )

    assert review_report.status == "PASS"
    assert len(review_report.items) == 1
    assert review_report.items[0].trade_return > 0
    assert "Alpha" in review_report.items[0].attribution_label


def test_render_and_write_trade_review_report(tmp_path: Path) -> None:
    trade_path = tmp_path / "trade.yaml"
    _write_valid_trade(trade_path)
    validation_report = validate_trade_record_store(
        store=load_trade_record_store(trade_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )
    review_report = build_trade_review_report(
        validation_report=validation_report,
        prices=_sample_prices(configured_price_tickers(load_universe()), periods=90),
        data_quality_report=_quality_report(),
        benchmark_tickers=("SPY", "QQQ", "SMH", "SOXX"),
    )

    markdown = render_trade_review_report(
        review_report,
        data_quality_report_path=tmp_path / "quality.md",
    )
    output_path = write_trade_review_report(
        review_report,
        data_quality_report_path=tmp_path / "quality.md",
        output_path=tmp_path / "trade_review.md",
    )

    assert "# 交易复盘归因报告" in markdown
    assert "trade_nvda_2026_04" in markdown
    assert output_path.read_text(encoding="utf-8") == markdown


def test_review_trades_cli_enforces_data_quality_and_writes_report(tmp_path: Path) -> None:
    universe = load_universe()
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    trade_dir = tmp_path / "trades"
    trade_dir.mkdir()
    report_path = tmp_path / "trade_review.md"
    quality_path = tmp_path / "quality.md"
    _sample_prices(configured_price_tickers(universe), periods=320).to_csv(
        prices_path,
        index=False,
    )
    _sample_rates(configured_rate_series(universe), periods=320).to_csv(
        rates_path,
        index=False,
    )
    _write_valid_trade(trade_dir / "trade.yaml")

    result = CliRunner().invoke(
        app,
        [
            "review-trades",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--input-path",
            str(trade_dir),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(report_path),
            "--quality-report-path",
            str(quality_path),
        ],
    )

    assert result.exit_code == 0
    assert report_path.exists()
    assert quality_path.exists()
    assert "交易复盘状态：PASS" in result.output


def _write_valid_trade(
    path: Path,
    thesis_id: str | None = "nvda_ai_infra_2026_q2",
) -> None:
    thesis_line = f"thesis_id: {thesis_id}\n" if thesis_id else ""
    path.write_text(
        f"""trade_id: trade_nvda_2026_04
ticker: NVDA
direction: long
opened_at: 2026-04-01
closed_at: 2026-04-15
{thesis_line}entry_price: 100.0
exit_price: 112.0
position_size_pct: 0.12
fees: 0
tags:
  - ai_infra
notes: 测试用交易记录。
""",
        encoding="utf-8",
    )


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
    dates = pd.date_range(end="2026-05-02", periods=periods, freq="D")
    rows: list[dict[str, object]] = []
    for ticker_index, ticker in enumerate(tickers):
        base = 100.0 + ticker_index * 2.0
        daily_step = 0.05 + ticker_index * 0.005
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
    dates = pd.date_range(end="2026-05-02", periods=periods, freq="D")
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
