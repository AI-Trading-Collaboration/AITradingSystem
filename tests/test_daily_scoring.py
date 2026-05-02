from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import cast

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_risk_events,
    load_scoring_rules,
    load_universe,
)
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.features.market import MarketFeatureRow, MarketFeatureSet
from ai_trading_system.fundamentals.sec_features import (
    SecFundamentalFeatureRow,
    SecFundamentalFeaturesReport,
)
from ai_trading_system.fundamentals.sec_metrics import (
    SEC_FUNDAMENTAL_METRIC_COLUMNS,
    PeriodType,
    SecFundamentalMetricsCsvValidationReport,
)
from ai_trading_system.risk_events import (
    LoadedRiskEventOccurrence,
    RiskEventEvidenceSource,
    RiskEventOccurrence,
    RiskEventOccurrenceValidationReport,
    build_risk_event_occurrence_review_report,
)
from ai_trading_system.scoring.daily import (
    DailyManualReviewStatus,
    DailyReviewSummary,
    build_daily_score_report,
    render_daily_score_report,
    write_scores_csv,
)
from ai_trading_system.valuation import (
    LoadedValuationSnapshot,
    SnapshotMetric,
    ValuationSnapshot,
    ValuationValidationReport,
    build_valuation_review_report,
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


def test_build_daily_score_report_uses_sec_fundamental_features() -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        fundamental_feature_report=_fundamental_feature_report(),
    )

    fundamentals = _component(report, "fundamentals")

    assert fundamentals.source_type == "hard_data"
    assert fundamentals.coverage == 1.0
    assert fundamentals.score > 50
    assert any(
        signal.subject == "AI_CORE_MEDIAN"
        and signal.feature == "gross_margin_quarterly_median"
        for signal in fundamentals.signals
    )


def test_build_daily_score_report_uses_valuation_snapshots() -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        valuation_review_report=_valuation_review_report(),
    )

    valuation = _component(report, "valuation")

    assert valuation.source_type == "manual_input"
    assert valuation.coverage == 1.0
    assert valuation.score < 50


def test_build_daily_score_report_uses_risk_event_occurrences() -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        risk_event_occurrence_review_report=_risk_event_occurrence_review_report(),
    )

    policy = _component(report, "policy_geopolitics")

    assert policy.source_type == "manual_input"
    assert policy.coverage == 1.0
    assert policy.score < 50
    assert any(
        signal.subject == "POLICY_GEOPOLITICS"
        and signal.feature == "active_or_watch_l3_count"
        and signal.value == 1.0
        for signal in policy.signals
    )


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
        review_summary=DailyReviewSummary(
            thesis=DailyManualReviewStatus(
                name="交易 thesis",
                status="PASS_WITH_WARNINGS",
                summary="Thesis 1 个，活跃 1 个；需关注 1 个，已证伪 0 个。",
                warning_count=1,
                source_path=tmp_path / "trade_theses",
            ),
            risk_events=DailyManualReviewStatus(
                name="风险事件",
                status="PASS",
                summary="风险规则 3 条，活跃 1 条；活跃 L2/L3 规则 0 条。",
                source_path=tmp_path / "risk_events.yaml",
            ),
        ),
    )

    markdown = render_daily_score_report(
        report,
        data_quality_report_path=tmp_path / "quality.md",
        feature_report_path=tmp_path / "features.md",
        features_path=tmp_path / "features.csv",
        scores_path=tmp_path / "scores.csv",
    )

    assert "- 数据质量状态：PASS" in markdown
    assert "## 人工复核摘要" in markdown
    assert "交易 thesis" in markdown
    assert "基本面（fundamentals）" in markdown
    assert "基本面硬数据占位" in markdown


def test_render_daily_score_report_includes_sec_feature_gate(tmp_path: Path) -> None:
    report = build_daily_score_report(
        feature_set=_feature_set(),
        data_quality_report=_quality_report(),
        rules=load_scoring_rules(),
        total_risk_asset_min=0.60,
        total_risk_asset_max=0.80,
        fundamental_feature_report=_fundamental_feature_report(),
    )

    markdown = render_daily_score_report(
        report,
        data_quality_report_path=tmp_path / "quality.md",
        feature_report_path=tmp_path / "features.md",
        features_path=tmp_path / "features.csv",
        scores_path=tmp_path / "scores.csv",
        sec_metrics_validation_report_path=tmp_path / "sec_validation.md",
        sec_fundamental_feature_report_path=tmp_path / "sec_features.md",
        sec_fundamental_features_path=tmp_path / "sec_features.csv",
    )

    assert "- SEC 指标 CSV 校验状态：PASS" in markdown
    assert "- SEC 基本面特征状态：PASS" in markdown
    assert "sec_features.csv" in markdown


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
    sec_companies_path = tmp_path / "sec_companies.yaml"
    sec_metrics_path = tmp_path / "fundamental_metrics.yaml"
    fundamental_feature_config_path = tmp_path / "fundamental_features.yaml"
    sec_fundamentals_path = tmp_path / "sec_fundamentals.csv"
    sec_features_path = tmp_path / "sec_fundamental_features.csv"
    sec_feature_report_path = tmp_path / "sec_fundamental_features.md"
    sec_validation_report_path = tmp_path / "sec_fundamentals_validation.md"
    _sample_prices(tickers, periods=260).to_csv(prices_path, index=False)
    _sample_rates(rate_series, periods=260).to_csv(rates_path, index=False)
    _write_sec_companies_config(sec_companies_path)
    _write_sec_metrics_config(sec_metrics_path)
    _write_sec_features_config(fundamental_feature_config_path)
    _write_sec_metrics_csv(sec_fundamentals_path)

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
            "--sec-companies-path",
            str(sec_companies_path),
            "--sec-metrics-path",
            str(sec_metrics_path),
            "--fundamental-feature-config-path",
            str(fundamental_feature_config_path),
            "--sec-fundamentals-path",
            str(sec_fundamentals_path),
            "--sec-fundamental-features-path",
            str(sec_features_path),
            "--sec-fundamental-feature-report-path",
            str(sec_feature_report_path),
            "--sec-metrics-validation-report-path",
            str(sec_validation_report_path),
        ],
    )

    assert result.exit_code == 0
    assert daily_report_path.exists()
    assert scores_path.exists()
    assert features_path.exists()
    assert sec_features_path.exists()
    assert sec_feature_report_path.exists()
    assert sec_validation_report_path.exists()
    assert "人工复核摘要" in daily_report_path.read_text(encoding="utf-8")
    assert "SEC 基本面特征状态：PASS" in daily_report_path.read_text(encoding="utf-8")
    assert "风险事件发生记录状态：" in daily_report_path.read_text(encoding="utf-8")
    assert "每日评分状态：" in result.output


def _risk_event_occurrence_review_report():
    as_of = date(2026, 4, 30)
    validation_report = RiskEventOccurrenceValidationReport(
        as_of=as_of,
        input_path=Path("risk_event_occurrences"),
        config=load_risk_events(),
        occurrences=(
            LoadedRiskEventOccurrence(
                occurrence=RiskEventOccurrence(
                    occurrence_id="taiwan_geopolitical_escalation_2026_04_30",
                    event_id="taiwan_geopolitical_escalation",
                    status="active",
                    triggered_at=as_of,
                    last_confirmed_at=as_of,
                    evidence_sources=[
                        RiskEventEvidenceSource(
                            source_name="manual_policy_review",
                            source_type="manual_input",
                            captured_at=as_of,
                        )
                    ],
                    summary="测试用 L3 风险事件。",
                ),
                path=Path("risk_event_occurrences/taiwan.yaml"),
            ),
        ),
    )
    return build_risk_event_occurrence_review_report(validation_report)


def _fundamental_feature_report() -> SecFundamentalFeaturesReport:
    as_of = date(2026, 4, 30)
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
        input_path=Path("sec_fundamentals.csv"),
        validation_report=SecFundamentalMetricsCsvValidationReport(
            as_of=as_of,
            input_path=Path("sec_fundamentals.csv"),
            row_count=20,
            as_of_row_count=20,
            expected_observation_count=20,
            observed_observation_count=20,
        ),
        rows=rows,
    )


def _valuation_review_report():
    as_of = date(2026, 4, 30)
    validation_report = ValuationValidationReport(
        as_of=as_of,
        input_path=Path("valuation_snapshots"),
        snapshots=(
            LoadedValuationSnapshot(
                snapshot=ValuationSnapshot(
                    snapshot_id="nvda_valuation",
                    ticker="NVDA",
                    as_of=as_of,
                    source_type="manual_input",
                    source_name="manual_sheet",
                    captured_at=as_of,
                    valuation_metrics=(
                        SnapshotMetric(
                            metric_id="forward_pe",
                            value=36.0,
                            unit="ratio",
                            period="next_12m",
                        ),
                    ),
                    valuation_percentile=82.0,
                    overall_assessment="expensive",
                ),
                path=Path("nvda_valuation.yaml"),
            ),
        ),
    )
    return build_valuation_review_report(validation_report)


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
        source_path=Path("sec_fundamentals.csv"),
    )


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


def _write_sec_metrics_csv(output_path: Path) -> None:
    pd.DataFrame(
        [
            _sec_metric_record("revenue", "Revenue", 1000),
            _sec_metric_record("gross_profit", "Gross Profit", 650),
        ],
        columns=list(SEC_FUNDAMENTAL_METRIC_COLUMNS),
    ).to_csv(output_path, index=False)


def _sec_metric_record(metric_id: str, metric_name: str, value: float) -> dict[str, object]:
    return {
        "as_of": "2026-04-30",
        "ticker": "NVDA",
        "cik": "0001045810",
        "company_name": "NVIDIA Corporation",
        "metric_id": metric_id,
        "metric_name": metric_name,
        "period_type": "annual",
        "fiscal_year": 2026,
        "fiscal_period": "FY",
        "end_date": "2026-01-31",
        "filed_date": "2026-02-27",
        "form": "10-K",
        "taxonomy": "us-gaap",
        "concept": "Revenues" if metric_id == "revenue" else "GrossProfit",
        "unit": "USD",
        "value": value,
        "accession_number": "0001045810-26-000001",
        "source_path": "nvda_companyfacts.json",
    }
