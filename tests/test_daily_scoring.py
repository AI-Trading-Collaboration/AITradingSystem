from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_scoring_rules,
    load_universe,
)
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.features.market import MarketFeatureRow, MarketFeatureSet
from ai_trading_system.scoring.daily import (
    build_daily_score_report,
    render_daily_score_report,
    write_scores_csv,
)


def test_build_daily_score_report_uses_hard_data_and_placeholders() -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
    )

    assert report.recommendation.total_score > 50
    assert _component(report, "trend").source_type == "hard_data"
    assert _component(report, "fundamentals").source_type == "placeholder"
    assert report.status == "PASS_WITH_LIMITATIONS"
    assert report.recommendation.total_asset_ai_band.min_position >= 0.24


def test_build_daily_score_report_marks_insufficient_data() -> None:
    report = build_daily_score_report(
        feature_set=MarketFeatureSet(
            as_of=date(2026, 4, 30),
            rows=(
                MarketFeatureRow(
                    as_of=date(2026, 4, 30),
                    source_date=date(2026, 4, 30),
                    category="risk_sentiment",
                    subject="^VIX",
                    feature="vix_current",
                    value=18.0,
                    unit="index_level",
                    lookback=None,
                    source="prices_daily",
                ),
            ),
            warnings=(),
        ),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
    )

    assert _component(report, "trend").source_type == "insufficient_data"
    assert _component(report, "trend").score == 50


def test_write_scores_csv_upserts_as_of_rows(tmp_path: Path) -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
    )
    output_path = tmp_path / "scores_daily.csv"

    write_scores_csv(report, output_path)
    write_scores_csv(report, output_path)

    stored = pd.read_csv(output_path)

    assert set(stored["as_of"]) == {"2026-04-30"}
    assert len(stored) == len(report.components) + 1


def test_render_daily_score_report_includes_data_gate_and_limitations(tmp_path: Path) -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
    )

    markdown = render_daily_score_report(
        report,
        data_quality_report_path=tmp_path / "quality.md",
        feature_report_path=tmp_path / "features.md",
        features_path=tmp_path / "features.csv",
        scores_path=tmp_path / "scores.csv",
    )

    assert "- Data quality status: PASS" in markdown
    assert "fundamentals" in markdown
    assert "MVP placeholder" in markdown


def test_score_daily_cli_writes_report_and_scores(tmp_path: Path) -> None:
    universe = load_universe()
    tickers = configured_price_tickers(universe)
    rate_series = configured_rate_series(universe)
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    features_path = tmp_path / "features_daily.csv"
    scores_path = tmp_path / "scores_daily.csv"
    daily_report_path = tmp_path / "daily_score.md"
    feature_report_path = tmp_path / "feature_summary.md"
    quality_report_path = tmp_path / "quality.md"
    _sample_prices(tickers, periods=260).to_csv(prices_path, index=False)
    _sample_rates(rate_series, periods=260).to_csv(rates_path, index=False)

    result = CliRunner().invoke(
        app,
        [
            "score-daily",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2026-04-30",
            "--features-path",
            str(features_path),
            "--scores-path",
            str(scores_path),
            "--report-path",
            str(daily_report_path),
            "--feature-report-path",
            str(feature_report_path),
            "--quality-report-path",
            str(quality_report_path),
        ],
    )

    assert result.exit_code == 0
    assert daily_report_path.exists()
    assert scores_path.exists()
    assert features_path.exists()
    assert "Daily score status:" in result.output


def _feature_set() -> MarketFeatureSet:
    as_of = date(2026, 4, 30)
    rows = [
        _feature(as_of, "trend", "SPY", "above_ma_100", 1.0),
        _feature(as_of, "trend", "SPY", "above_ma_200", 1.0),
        _feature(as_of, "trend", "QQQ", "above_ma_100", 1.0),
        _feature(as_of, "trend", "QQQ", "above_ma_200", 1.0),
        _feature(as_of, "trend", "SMH", "above_ma_100", 1.0),
        _feature(as_of, "trend", "SMH", "above_ma_200", 1.0),
        _feature(as_of, "trend", "SOXX", "above_ma_100", 1.0),
        _feature(as_of, "trend", "SOXX", "above_ma_200", 1.0),
        _feature(as_of, "trend", "AI_CORE_WATCHLIST", "above_ma_200_ratio", 0.8),
        _feature(as_of, "relative_strength", "SMH/SPY", "relative_strength_return_20d", 0.04),
        _feature(as_of, "macro_liquidity", "DGS10", "rate_change_20d", -0.05),
        _feature(as_of, "macro_liquidity", "DGS2", "rate_change_20d", 0.05),
        _feature(as_of, "trend", "DX-Y.NYB", "return_20d", -0.02),
        _feature(as_of, "risk_sentiment", "^VIX", "vix_current", 17.0),
        _feature(as_of, "risk_sentiment", "^VIX", "vix_percentile_252", 0.35),
        _feature(as_of, "trend", "^VIX", "return_5d", -0.05),
    ]
    return MarketFeatureSet(as_of=as_of, rows=tuple(rows), warnings=())


def _feature(
    as_of: date,
    category: str,
    subject: str,
    feature: str,
    value: float,
) -> MarketFeatureRow:
    return MarketFeatureRow(
        as_of=as_of,
        source_date=as_of,
        category=category,
        subject=subject,
        feature=feature,
        value=value,
        unit="ratio",
        lookback=None,
        source="test",
    )


def _component(report, name: str):  # type: ignore[no-untyped-def]
    for component in report.components:
        if component.name == name:
            return component
    raise AssertionError(f"component not found: {name}")


def _quality_report() -> DataQualityReport:
    return DataQualityReport(
        checked_at=pd.Timestamp("2026-05-01T00:00:00Z").to_pydatetime(),
        as_of=date(2026, 4, 30),
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
                    "value": base - day_index * 0.001,
                }
            )
    return pd.DataFrame(rows)
