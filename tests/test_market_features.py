from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import (
    CoreBreadthFeatureConfig,
    FeatureConfig,
    RateFeatureConfig,
    RelativeStrengthPairConfig,
    VixFeatureConfig,
    configured_price_tickers,
    configured_rate_series,
    load_universe,
)
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.features.market import (
    MarketFeatureSet,
    build_market_features,
    render_feature_summary,
    write_features_csv,
)


def test_build_market_features_generates_expected_rows() -> None:
    feature_set = build_market_features(
        prices=_sample_prices(["SPY", "SMH", "QQQ", "MSFT", "NVDA", "^VIX"], periods=5),
        rates=_sample_rates(["DGS2", "DGS10"], periods=5),
        config=_small_feature_config(),
        as_of=date(2026, 4, 30),
        core_watchlist=["MSFT", "NVDA"],
    )

    assert feature_set.status == "PASS"
    assert _feature_value(feature_set, "SMH", "above_ma_3") == 1.0
    assert isinstance(_feature_value(feature_set, "SMH/SPY", "relative_strength_return_2d"), float)
    assert _feature_value(feature_set, "^VIX", "vix_percentile_3") == 1.0
    assert _feature_value(feature_set, "DGS10", "rate_change_2d") > 0
    assert _feature_value(feature_set, "AI_CORE_WATCHLIST", "above_ma_3_ratio") == 1.0


def test_build_market_features_warns_on_insufficient_history() -> None:
    config = FeatureConfig(
        moving_average_windows=[10],
        return_windows=[10],
        relative_strength_pairs=[RelativeStrengthPairConfig(numerator="SMH", denominator="SPY")],
        vix=VixFeatureConfig(ticker="^VIX", moving_average_window=10, percentile_window=10),
        rates=RateFeatureConfig(change_windows=[10]),
        core_breadth=CoreBreadthFeatureConfig(long_moving_average_window=10),
    )

    feature_set = build_market_features(
        prices=_sample_prices(["SPY", "SMH", "MSFT", "^VIX"], periods=3),
        rates=_sample_rates(["DGS10"], periods=3),
        config=config,
        as_of=date(2026, 4, 30),
        core_watchlist=["MSFT"],
    )

    assert feature_set.status == "PASS_WITH_WARNINGS"
    assert any(warning.code == "insufficient_history" for warning in feature_set.warnings)


def test_write_features_csv_upserts_as_of_rows(tmp_path: Path) -> None:
    first = build_market_features(
        prices=_sample_prices(["SPY", "SMH", "MSFT", "^VIX"], periods=5),
        rates=_sample_rates(["DGS10"], periods=5),
        config=_small_feature_config(),
        as_of=date(2026, 4, 29),
        core_watchlist=["MSFT"],
    )
    second = build_market_features(
        prices=_sample_prices(["SPY", "SMH", "MSFT", "^VIX"], periods=5),
        rates=_sample_rates(["DGS10"], periods=5),
        config=_small_feature_config(),
        as_of=date(2026, 4, 30),
        core_watchlist=["MSFT"],
    )
    output_path = tmp_path / "features_daily.csv"

    write_features_csv(first, output_path)
    write_features_csv(second, output_path)
    write_features_csv(second, output_path)

    stored = pd.read_csv(output_path)

    assert set(stored["as_of"]) == {"2026-04-29", "2026-04-30"}
    assert len(stored.loc[stored["as_of"] == "2026-04-30"]) == len(second.rows)


def test_render_feature_summary_includes_data_quality_status(tmp_path: Path) -> None:
    feature_set = build_market_features(
        prices=_sample_prices(["SPY", "SMH", "MSFT", "^VIX"], periods=5),
        rates=_sample_rates(["DGS10"], periods=5),
        config=_small_feature_config(),
        as_of=date(2026, 4, 30),
        core_watchlist=["MSFT"],
    )
    quality_report = _dummy_quality_report()

    markdown = render_feature_summary(
        feature_set,
        data_quality_report=quality_report,
        data_quality_report_path=tmp_path / "quality.md",
        features_path=tmp_path / "features.csv",
    )

    assert "- 数据质量状态：PASS" in markdown
    assert "## 按类别统计" in markdown


def test_build_features_cli_enforces_quality_gate_and_writes_outputs(tmp_path: Path) -> None:
    universe = load_universe()
    tickers = configured_price_tickers(universe)
    rate_series = configured_rate_series(universe)
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    features_path = tmp_path / "features_daily.csv"
    feature_report_path = tmp_path / "feature_summary.md"
    quality_report_path = tmp_path / "quality.md"
    _sample_prices(tickers, periods=260).to_csv(prices_path, index=False)
    _sample_rates(rate_series, periods=260).to_csv(rates_path, index=False)

    result = CliRunner().invoke(
        app,
        [
            "build-features",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2026-04-30",
            "--output-path",
            str(features_path),
            "--report-path",
            str(feature_report_path),
            "--quality-report-path",
            str(quality_report_path),
        ],
    )

    assert result.exit_code == 0
    assert features_path.exists()
    assert feature_report_path.exists()
    assert quality_report_path.exists()
    assert "特征构建状态：PASS" in result.output


def _small_feature_config() -> FeatureConfig:
    return FeatureConfig(
        moving_average_windows=[3],
        return_windows=[1, 2],
        relative_strength_pairs=[RelativeStrengthPairConfig(numerator="SMH", denominator="SPY")],
        vix=VixFeatureConfig(ticker="^VIX", moving_average_window=3, percentile_window=3),
        rates=RateFeatureConfig(change_windows=[1, 2]),
        core_breadth=CoreBreadthFeatureConfig(long_moving_average_window=3),
    )


def _sample_prices(tickers: list[str], periods: int) -> pd.DataFrame:
    dates = pd.date_range(end="2026-04-30", periods=periods, freq="D")
    rows: list[dict[str, object]] = []
    for ticker_index, ticker in enumerate(tickers):
        base = 100.0 + ticker_index * 10.0
        daily_step = 1.0 + ticker_index * 0.05
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
                    "value": base + day_index * 0.001,
                }
            )
    return pd.DataFrame(rows)


def _feature_value(feature_set: MarketFeatureSet, subject: str, feature: str) -> float:
    for row in feature_set.rows:
        if row.subject == subject and row.feature == feature:
            return row.value
    raise AssertionError(f"feature not found: {subject} {feature}")


def _dummy_quality_report() -> DataQualityReport:
    return DataQualityReport(
        checked_at=pd.Timestamp("2026-05-01T00:00:00Z").to_pydatetime(),
        as_of=date(2026, 4, 30),
        price_summary=DataFileSummary(path=Path("prices.csv"), exists=True, rows=1),
        rate_summary=DataFileSummary(path=Path("rates.csv"), exists=True, rows=1),
        expected_price_tickers=("SPY",),
        expected_rate_series=("DGS10",),
        issues=(),
    )
