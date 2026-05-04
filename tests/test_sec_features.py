from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import (
    FundamentalFeaturesConfig,
    FundamentalMetricConceptConfig,
    FundamentalMetricConfig,
    FundamentalMetricsConfig,
    FundamentalRatioFeatureConfig,
    SecCompaniesConfig,
    SecCompanyConfig,
)
from ai_trading_system.fundamentals.sec_features import (
    build_sec_fundamental_features_report,
    render_sec_fundamental_features_report,
    write_sec_fundamental_features_csv,
)
from ai_trading_system.fundamentals.sec_metrics import (
    SEC_FUNDAMENTAL_METRIC_COLUMNS,
    validate_sec_fundamental_metrics_csv,
)


def test_build_sec_fundamental_features_report_generates_ratio_features(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "sec_fundamentals.csv"
    _write_metrics_csv(input_path)
    validation = validate_sec_fundamental_metrics_csv(
        companies=_companies_config(),
        metrics=_metrics_config(),
        input_path=input_path,
        as_of=_date(),
    )

    report = build_sec_fundamental_features_report(
        companies=_companies_config(),
        feature_config=_features_config(),
        input_path=input_path,
        as_of=_date(),
        validation_report=validation,
    )
    markdown = render_sec_fundamental_features_report(
        report,
        validation_report_path=tmp_path / "sec_validation.md",
        output_csv_path=tmp_path / "features.csv",
    )

    assert report.status == "PASS"
    assert report.row_count == 1
    assert report.rows[0].feature_id == "gross_margin"
    assert report.rows[0].value == 0.65
    assert "- SEC 指标 CSV 校验状态：PASS" in markdown
    assert "config/fundamental_features.yaml" in markdown


def test_build_sec_fundamental_features_report_warns_and_skips_misaligned_metrics(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "sec_fundamentals.csv"
    _write_metrics_csv(input_path, denominator_accession="0001045810-26-999999")
    validation = validate_sec_fundamental_metrics_csv(
        companies=_companies_config(),
        metrics=_metrics_config(),
        input_path=input_path,
        as_of=_date(),
    )

    report = build_sec_fundamental_features_report(
        companies=_companies_config(),
        feature_config=_features_config(),
        input_path=input_path,
        as_of=_date(),
        validation_report=validation,
    )

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.row_count == 0
    assert report.warning_count == 1
    assert "sec_fundamental_feature_metric_alignment_mismatch" in {
        issue.code for issue in report.issues
    }


def test_write_sec_fundamental_features_csv_rejects_failed_report(tmp_path: Path) -> None:
    input_path = tmp_path / "sec_fundamentals.csv"
    _write_metrics_csv(input_path, gross_profit_value=0)
    validation = validate_sec_fundamental_metrics_csv(
        companies=_companies_config(),
        metrics=_metrics_config(),
        input_path=input_path,
        as_of=_date(),
    )
    report = build_sec_fundamental_features_report(
        companies=_companies_config(),
        feature_config=FundamentalFeaturesConfig(
            features=[
                FundamentalRatioFeatureConfig(
                    feature_id="revenue_to_gross_profit",
                    name="Revenue to Gross Profit",
                    description="收入除以毛利。",
                    numerator_metric_id="revenue",
                    denominator_metric_id="gross_profit",
                    preferred_periods=["annual"],
                )
            ]
        ),
        input_path=input_path,
        as_of=_date(),
        validation_report=validation,
    )

    try:
        write_sec_fundamental_features_csv(report, tmp_path / "features.csv")
    except ValueError as exc:
        assert "不能写入特征 CSV" in str(exc)
    else:
        raise AssertionError("expected failed report to reject CSV write")


def test_write_sec_fundamental_features_csv_upserts_as_of(tmp_path: Path) -> None:
    input_path = tmp_path / "sec_fundamentals.csv"
    _write_metrics_csv(input_path)
    validation = validate_sec_fundamental_metrics_csv(
        companies=_companies_config(),
        metrics=_metrics_config(),
        input_path=input_path,
        as_of=_date(),
    )
    report = build_sec_fundamental_features_report(
        companies=_companies_config(),
        feature_config=_features_config(),
        input_path=input_path,
        as_of=_date(),
        validation_report=validation,
    )
    output_path = tmp_path / "features.csv"

    write_sec_fundamental_features_csv(report, output_path)
    write_sec_fundamental_features_csv(report, output_path)
    stored = pd.read_csv(output_path)

    assert len(stored) == report.row_count
    assert set(stored["feature_id"]) == {"gross_margin"}


def test_fundamentals_cli_build_sec_features(tmp_path: Path) -> None:
    sec_config_path = tmp_path / "sec_companies.yaml"
    sec_config_path.write_text(
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
    metrics_path = tmp_path / "fundamental_metrics.yaml"
    metrics_path.write_text(
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
    feature_config_path = tmp_path / "fundamental_features.yaml"
    feature_config_path.write_text(
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
    input_path = tmp_path / "sec_fundamentals.csv"
    _write_metrics_csv(input_path)
    output_path = tmp_path / "sec_fundamental_features.csv"
    report_path = tmp_path / "sec_fundamental_features.md"
    validation_report_path = tmp_path / "sec_metrics_validation.md"

    result = CliRunner().invoke(
        app,
        [
            "fundamentals",
            "build-sec-features",
            "--sec-companies-path",
            str(sec_config_path),
            "--metrics-path",
            str(metrics_path),
            "--feature-config-path",
            str(feature_config_path),
            "--input-path",
            str(input_path),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(output_path),
            "--report-path",
            str(report_path),
            "--validation-report-path",
            str(validation_report_path),
        ],
    )

    assert result.exit_code == 0
    assert "SEC 基本面特征构建状态：PASS" in result.output
    assert output_path.exists()
    assert report_path.exists()
    assert validation_report_path.exists()


def _companies_config() -> SecCompaniesConfig:
    return SecCompaniesConfig(
        companies=[
            SecCompanyConfig(
                ticker="NVDA",
                cik="0001045810",
                company_name="NVIDIA Corporation",
                expected_taxonomies=["us-gaap", "dei"],
                sec_metric_periods=["annual"],
            )
        ]
    )


def _metrics_config() -> FundamentalMetricsConfig:
    return FundamentalMetricsConfig(
        metrics=[
            FundamentalMetricConfig(
                metric_id="revenue",
                name="Revenue",
                description="SEC companyfacts 披露的总收入。",
                preferred_periods=["annual"],
                concepts=[
                    FundamentalMetricConceptConfig(
                        taxonomy="us-gaap",
                        concept="Revenues",
                        unit="USD",
                    )
                ],
            ),
            FundamentalMetricConfig(
                metric_id="gross_profit",
                name="Gross Profit",
                description="已披露时使用收入扣除营业成本后的毛利。",
                preferred_periods=["annual"],
                concepts=[
                    FundamentalMetricConceptConfig(
                        taxonomy="us-gaap",
                        concept="GrossProfit",
                        unit="USD",
                    )
                ],
            ),
        ]
    )


def _features_config() -> FundamentalFeaturesConfig:
    return FundamentalFeaturesConfig(
        features=[
            FundamentalRatioFeatureConfig(
                feature_id="gross_margin",
                name="Gross Margin",
                description="毛利除以收入。",
                numerator_metric_id="gross_profit",
                denominator_metric_id="revenue",
                preferred_periods=["annual"],
            )
        ]
    )


def _write_metrics_csv(
    output_path: Path,
    denominator_accession: str = "0001045810-26-000001",
    gross_profit_value: float = 650,
) -> None:
    pd.DataFrame(
        [
            _metric_record(
                metric_id="revenue",
                metric_name="Revenue",
                value=1000,
                accession_number=denominator_accession,
            ),
            _metric_record(
                metric_id="gross_profit",
                metric_name="Gross Profit",
                value=gross_profit_value,
                accession_number="0001045810-26-000001",
            ),
        ],
        columns=list(SEC_FUNDAMENTAL_METRIC_COLUMNS),
    ).to_csv(output_path, index=False)


def _metric_record(
    metric_id: str,
    metric_name: str,
    value: float,
    accession_number: str,
) -> dict[str, object]:
    return {
        "as_of": "2026-05-02",
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
        "accession_number": accession_number,
        "source_path": "nvda_companyfacts.json",
    }


def _date() -> date:
    return date(2026, 5, 2)
