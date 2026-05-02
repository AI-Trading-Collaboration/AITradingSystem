from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import cast

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
from ai_trading_system.fundamentals.sec_features import (
    SecFundamentalFeatureRow,
    SecFundamentalFeaturesReport,
)
from ai_trading_system.fundamentals.sec_metrics import (
    PeriodType,
    SecFundamentalMetricsCsvValidationReport,
)


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


def test_run_daily_score_backtest_uses_point_in_time_sec_features() -> None:
    universe = load_universe()
    prices = _sample_prices(configured_price_tickers(universe), periods=320)
    rates = _sample_rates(configured_rate_series(universe), periods=320)
    signal_dates = pd.date_range("2026-04-01", "2026-04-29", freq="D")
    sec_reports = {
        item.date(): _fundamental_feature_report(item.date()) for item in signal_dates
    }

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
        benchmark_tickers=("SPY",),
        fundamental_feature_reports=sec_reports,
    )

    assert result.fundamental_feature_report_count == len(result.rows)
    assert result.rows[0].component_scores["fundamentals"] > 50


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
    regimes_path = tmp_path / "market_regimes.yaml"
    sec_companies_path = tmp_path / "sec_companies.yaml"
    sec_metrics_path = tmp_path / "fundamental_metrics.yaml"
    sec_features_config_path = tmp_path / "fundamental_features.yaml"
    sec_companyfacts_dir = tmp_path / "sec_companyfacts"
    sec_companyfacts_validation_path = tmp_path / "sec_companyfacts_validation.md"
    _sample_prices(configured_price_tickers(universe), periods=320).to_csv(
        prices_path,
        index=False,
    )
    _sample_rates(configured_rate_series(universe), periods=320).to_csv(
        rates_path,
        index=False,
    )
    regimes_path.write_text(
        "\n".join(
            [
                "default_backtest_regime: test_ai_regime",
                "regimes:",
                "  - regime_id: test_ai_regime",
                "    name: 测试 AI 行情",
                "    start_date: 2026-04-01",
                "    anchor_date: 2026-03-31",
                "    anchor_event: 测试锚定事件",
                "    description: 测试用市场阶段。",
                "    primary: true",
                "",
            ]
        ),
        encoding="utf-8",
    )
    _write_sec_companies_config(sec_companies_path)
    _write_sec_metrics_config(sec_metrics_path)
    _write_sec_features_config(sec_features_config_path)
    _write_sec_companyfacts_cache(sec_companyfacts_dir)

    result = CliRunner().invoke(
        app,
        [
            "backtest",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--to",
            "2026-04-30",
            "--regimes-path",
            str(regimes_path),
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
            "--sec-companies-path",
            str(sec_companies_path),
            "--sec-metrics-path",
            str(sec_metrics_path),
            "--fundamental-feature-config-path",
            str(sec_features_config_path),
            "--sec-companyfacts-dir",
            str(sec_companyfacts_dir),
            "--sec-companyfacts-validation-report-path",
            str(sec_companyfacts_validation_path),
        ],
    )

    assert result.exit_code == 0
    assert report_path.exists()
    assert daily_path.exists()
    assert quality_path.exists()
    assert sec_companyfacts_validation_path.exists()
    assert "回测状态：" in result.output
    assert "SEC 基本面切片：" in result.output
    assert "市场阶段：测试 AI 行情" in result.output
    assert "测试 AI 行情" in report_path.read_text(encoding="utf-8")


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


def _fundamental_feature_report(as_of: date) -> SecFundamentalFeaturesReport:
    rows = (
        _fundamental_feature(as_of, "NVDA", "gross_margin", "quarterly", 0.72),
        _fundamental_feature(as_of, "MSFT", "gross_margin", "quarterly", 0.69),
        _fundamental_feature(as_of, "NVDA", "operating_margin", "quarterly", 0.43),
        _fundamental_feature(as_of, "MSFT", "operating_margin", "quarterly", 0.42),
        _fundamental_feature(as_of, "NVDA", "net_margin", "quarterly", 0.35),
        _fundamental_feature(as_of, "MSFT", "net_margin", "quarterly", 0.34),
        _fundamental_feature(
            as_of,
            "NVDA",
            "research_and_development_intensity",
            "quarterly",
            0.12,
        ),
        _fundamental_feature(
            as_of,
            "MSFT",
            "research_and_development_intensity",
            "quarterly",
            0.13,
        ),
        _fundamental_feature(as_of, "NVDA", "capex_intensity", "annual", 0.14),
        _fundamental_feature(as_of, "MSFT", "capex_intensity", "annual", 0.16),
    )
    return SecFundamentalFeaturesReport(
        as_of=as_of,
        input_path=Path("point_in_time_metrics.csv"),
        validation_report=SecFundamentalMetricsCsvValidationReport(
            as_of=as_of,
            input_path=Path("point_in_time_metrics.csv"),
            row_count=20,
            as_of_row_count=20,
            expected_observation_count=20,
            observed_observation_count=20,
        ),
        rows=rows,
    )


def _fundamental_feature(
    as_of: date,
    ticker: str,
    feature_id: str,
    period_type: str,
    value: float,
) -> SecFundamentalFeatureRow:
    return SecFundamentalFeatureRow(
        as_of=as_of,
        ticker=ticker,
        period_type=cast(PeriodType, period_type),
        fiscal_year=2026,
        fiscal_period="Q1" if period_type == "quarterly" else "FY",
        end_date=as_of,
        filed_date=as_of,
        feature_id=feature_id,
        feature_name=feature_id.replace("_", " ").title(),
        value=value,
        unit="ratio",
        numerator_metric_id="numerator",
        denominator_metric_id="revenue",
        numerator_value=value * 1000,
        denominator_value=1000,
        source_metric_accessions="0000000000-26-000001",
        source_path=Path("point_in_time_metrics.csv"),
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


def _write_sec_companies_config(output_path: Path) -> None:
    output_path.write_text(
        """
companies:
  - ticker: NVDA
    cik: "0001045810"
    company_name: NVIDIA Corporation
    sec_metric_periods:
      - annual
    expected_taxonomies:
      - us-gaap
      - dei
""",
        encoding="utf-8",
    )


def _write_sec_metrics_config(output_path: Path) -> None:
    output_path.write_text(
        """
metrics:
  - metric_id: revenue
    name: Revenue
    description: SEC companyfacts 披露的总收入。
    preferred_periods:
      - annual
    concepts:
      - taxonomy: us-gaap
        concept: Revenues
        unit: USD
  - metric_id: gross_profit
    name: Gross Profit
    description: 已披露时使用收入扣除营业成本后的毛利。
    preferred_periods:
      - annual
    concepts:
      - taxonomy: us-gaap
        concept: GrossProfit
        unit: USD
""",
        encoding="utf-8",
    )


def _write_sec_features_config(output_path: Path) -> None:
    output_path.write_text(
        """
features:
  - feature_id: gross_margin
    name: Gross Margin
    description: 毛利除以收入。
    numerator_metric_id: gross_profit
    denominator_metric_id: revenue
    preferred_periods:
      - annual
""",
        encoding="utf-8",
    )


def _write_sec_companyfacts_cache(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "nvda_companyfacts.json"
    json_path.write_text(
        json.dumps(
            {
                "cik": 1045810,
                "entityName": "NVIDIA Corporation",
                "facts": {
                    "us-gaap": {
                        "Revenues": {
                            "units": {
                                "USD": [
                                    {
                                        "fy": 2025,
                                        "fp": "FY",
                                        "form": "10-K",
                                        "end": "2026-01-31",
                                        "filed": "2026-02-27",
                                        "val": 1000,
                                        "accn": "0001045810-26-000001",
                                    }
                                ]
                            }
                        },
                        "GrossProfit": {
                            "units": {
                                "USD": [
                                    {
                                        "fy": 2025,
                                        "fp": "FY",
                                        "form": "10-K",
                                        "end": "2026-01-31",
                                        "filed": "2026-02-27",
                                        "val": 650,
                                        "accn": "0001045810-26-000001",
                                    }
                                ]
                            }
                        },
                    },
                    "dei": {},
                },
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "ticker": "NVDA",
                "cik": "0001045810",
                "checksum_sha256": _sha256(json_path),
            }
        ]
    ).to_csv(output_dir / "sec_companyfacts_manifest.csv", index=False)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
