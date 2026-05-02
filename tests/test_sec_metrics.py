from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import (
    FundamentalDerivedMetricConfig,
    FundamentalMetricConceptConfig,
    FundamentalMetricConfig,
    FundamentalMetricsConfig,
    SecCompaniesConfig,
    SecCompanyConfig,
)
from ai_trading_system.fundamentals.sec_metrics import (
    build_sec_fundamental_metrics_report,
    render_sec_fundamental_metrics_report,
    render_sec_fundamental_metrics_validation_report,
    validate_sec_fundamental_metrics_csv,
    write_sec_fundamental_metrics_csv,
)
from ai_trading_system.fundamentals.sec_validation import validate_sec_companyfacts_cache


def test_build_sec_fundamental_metrics_report_extracts_latest_facts(tmp_path: Path) -> None:
    companies = _sec_config()
    json_path = _write_companyfacts(tmp_path, ticker="NVDA", cik="0001045810")
    _write_manifest(tmp_path, ticker="NVDA", cik="0001045810", json_path=json_path)
    validation = validate_sec_companyfacts_cache(companies, input_dir=tmp_path, as_of=_date())

    report = build_sec_fundamental_metrics_report(
        companies=companies,
        metrics=_metrics_config(),
        input_dir=tmp_path,
        as_of=_date(),
        validation_report=validation,
    )
    markdown = render_sec_fundamental_metrics_report(
        report,
        validation_report_path=tmp_path / "sec_validation.md",
        output_csv_path=tmp_path / "sec_fundamentals.csv",
    )

    assert report.status == "PASS"
    assert report.row_count == 2
    assert {(row.period_type, row.fiscal_year, row.value) for row in report.rows} == {
        ("annual", 2025, 2000.0),
        ("quarterly", 2026, 850.0),
    }
    assert "SEC 基本面指标摘要" in markdown
    assert "本报告只把 SEC companyfacts 原始 JSON 抽成结构化摘要" in markdown


def test_build_sec_fundamental_metrics_report_uses_only_filed_facts_as_of(
    tmp_path: Path,
) -> None:
    companies = _sec_config()
    json_path = _write_companyfacts(tmp_path, ticker="NVDA", cik="0001045810")
    _write_manifest(tmp_path, ticker="NVDA", cik="0001045810", json_path=json_path)
    validation = validate_sec_companyfacts_cache(
        companies,
        input_dir=tmp_path,
        as_of=date(2025, 2, 1),
    )

    report = build_sec_fundamental_metrics_report(
        companies=companies,
        metrics=_metrics_config(),
        input_dir=tmp_path,
        as_of=date(2025, 2, 1),
        validation_report=validation,
    )

    annual_rows = [row for row in report.rows if row.period_type == "annual"]
    assert len(annual_rows) == 1
    assert annual_rows[0].fiscal_year == 2024
    assert annual_rows[0].filed_date == date(2024, 2, 21)
    assert "sec_metric_missing" in {issue.code for issue in report.issues}


def test_build_sec_fundamental_metrics_report_chooses_latest_across_concepts(
    tmp_path: Path,
) -> None:
    companies = _sec_config()
    json_path = _write_companyfacts(tmp_path, ticker="NVDA", cik="0001045810")
    _write_manifest(tmp_path, ticker="NVDA", cik="0001045810", json_path=json_path)
    validation = validate_sec_companyfacts_cache(companies, input_dir=tmp_path, as_of=_date())

    report = build_sec_fundamental_metrics_report(
        companies=companies,
        metrics=_metrics_config_with_preferred_old_concept(),
        input_dir=tmp_path,
        as_of=_date(),
        validation_report=validation,
    )

    annual_rows = [row for row in report.rows if row.period_type == "annual"]
    assert len(annual_rows) == 1
    assert annual_rows[0].concept == "Revenues"
    assert annual_rows[0].fiscal_year == 2025
    assert annual_rows[0].value == 2000.0


def test_build_sec_fundamental_metrics_report_uses_single_quarter_not_ytd(
    tmp_path: Path,
) -> None:
    companies = _sec_config()
    json_path = _write_companyfacts(tmp_path, ticker="NVDA", cik="0001045810")
    _write_manifest(tmp_path, ticker="NVDA", cik="0001045810", json_path=json_path)
    validation = validate_sec_companyfacts_cache(companies, input_dir=tmp_path, as_of=_date())

    report = build_sec_fundamental_metrics_report(
        companies=companies,
        metrics=_metrics_config(),
        input_dir=tmp_path,
        as_of=_date(),
        validation_report=validation,
    )

    quarterly_rows = [row for row in report.rows if row.period_type == "quarterly"]
    assert len(quarterly_rows) == 1
    assert quarterly_rows[0].fiscal_period == "Q3"
    assert quarterly_rows[0].value == 850.0


def test_build_sec_fundamental_metrics_report_derives_missing_gross_profit(
    tmp_path: Path,
) -> None:
    companies = SecCompaniesConfig(
        companies=[
            SecCompanyConfig(
                ticker="GOOG",
                cik="0001652044",
                company_name="Alphabet Inc.",
                expected_taxonomies=["us-gaap", "dei"],
            )
        ]
    )
    json_path = _write_companyfacts_without_gross_profit(
        tmp_path,
        ticker="GOOG",
        cik="0001652044",
    )
    _write_manifest(tmp_path, ticker="GOOG", cik="0001652044", json_path=json_path)
    validation = validate_sec_companyfacts_cache(companies, input_dir=tmp_path, as_of=_date())

    report = build_sec_fundamental_metrics_report(
        companies=companies,
        metrics=_metrics_config_with_derived_gross_profit(),
        input_dir=tmp_path,
        as_of=_date(),
        validation_report=validation,
    )
    gross_profit_rows = {
        row.period_type: row for row in report.rows if row.metric_id == "gross_profit"
    }

    assert report.status == "PASS"
    assert report.row_count == 4
    assert not report.issues
    assert gross_profit_rows["annual"].value == 600.0
    assert gross_profit_rows["quarterly"].value == 250.0
    assert gross_profit_rows["annual"].taxonomy == "derived"
    assert gross_profit_rows["annual"].concept == "derived:revenue-cost_of_revenue"


def test_build_sec_fundamental_metrics_report_respects_company_sec_periods(
    tmp_path: Path,
) -> None:
    companies = SecCompaniesConfig(
        companies=[
            SecCompanyConfig(
                ticker="TSM",
                cik="0001046179",
                company_name="Taiwan Semiconductor Manufacturing Company Limited",
                expected_taxonomies=["ifrs-full", "dei"],
                sec_metric_periods=["annual"],
            )
        ]
    )
    json_path = _write_ifrs_annual_companyfacts(
        tmp_path,
        ticker="TSM",
        cik="0001046179",
    )
    _write_manifest(tmp_path, ticker="TSM", cik="0001046179", json_path=json_path)
    validation = validate_sec_companyfacts_cache(companies, input_dir=tmp_path, as_of=_date())

    report = build_sec_fundamental_metrics_report(
        companies=companies,
        metrics=_ifrs_metrics_config(),
        input_dir=tmp_path,
        as_of=_date(),
        validation_report=validation,
    )
    output_path = tmp_path / "sec_fundamentals.csv"
    write_sec_fundamental_metrics_csv(report, output_path)
    csv_validation = validate_sec_fundamental_metrics_csv(
        companies=companies,
        metrics=_ifrs_metrics_config(),
        input_path=output_path,
        as_of=_date(),
    )

    assert report.status == "PASS"
    assert report.row_count == 1
    assert {issue.code for issue in report.issues} == set()
    assert csv_validation.status == "PASS"
    assert csv_validation.expected_observation_count == 1
    assert csv_validation.observed_observation_count == 1


def test_build_sec_fundamental_metrics_report_requires_passing_validation(
    tmp_path: Path,
) -> None:
    companies = _sec_config()
    validation = validate_sec_companyfacts_cache(companies, input_dir=tmp_path, as_of=_date())

    with pytest.raises(ValueError, match="缓存校验必须通过"):
        build_sec_fundamental_metrics_report(
            companies=companies,
            metrics=_metrics_config(),
            input_dir=tmp_path,
            as_of=_date(),
            validation_report=validation,
        )


def test_write_sec_fundamental_metrics_csv_upserts_as_of(tmp_path: Path) -> None:
    companies = _sec_config()
    json_path = _write_companyfacts(tmp_path, ticker="NVDA", cik="0001045810")
    _write_manifest(tmp_path, ticker="NVDA", cik="0001045810", json_path=json_path)
    validation = validate_sec_companyfacts_cache(companies, input_dir=tmp_path, as_of=_date())
    report = build_sec_fundamental_metrics_report(
        companies=companies,
        metrics=_metrics_config(),
        input_dir=tmp_path,
        as_of=_date(),
        validation_report=validation,
    )
    output_path = tmp_path / "sec_fundamentals.csv"

    write_sec_fundamental_metrics_csv(report, output_path)
    write_sec_fundamental_metrics_csv(report, output_path)

    frame = pd.read_csv(output_path)
    assert len(frame) == report.row_count
    assert set(frame["metric_id"]) == {"revenue"}


def test_validate_sec_fundamental_metrics_csv_rejects_future_filed_date(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "sec_fundamentals.csv"
    pd.DataFrame(
        [
            {
                "as_of": "2026-05-02",
                "ticker": "NVDA",
                "cik": "0001045810",
                "company_name": "NVIDIA Corporation",
                "metric_id": "revenue",
                "metric_name": "Revenue",
                "period_type": "annual",
                "fiscal_year": 2026,
                "fiscal_period": "FY",
                "end_date": "2026-01-31",
                "filed_date": "2026-05-03",
                "form": "10-K",
                "taxonomy": "us-gaap",
                "concept": "Revenues",
                "unit": "USD",
                "value": 1000,
                "accession_number": "0001045810-26-000001",
                "source_path": str(tmp_path / "nvda_companyfacts.json"),
            }
        ]
    ).to_csv(output_path, index=False)

    report = validate_sec_fundamental_metrics_csv(
        companies=_sec_config(),
        metrics=_metrics_config(),
        input_path=output_path,
        as_of=_date(),
    )
    markdown = render_sec_fundamental_metrics_validation_report(report)

    assert not report.passed
    assert "sec_fundamental_metrics_filed_date_after_as_of" in {
        issue.code for issue in report.issues
    }
    assert "SEC 基本面指标 CSV 校验报告" in markdown


def test_fundamentals_cli_extract_sec_metrics(tmp_path: Path) -> None:
    sec_config_path = tmp_path / "sec_companies.yaml"
    sec_config_path.write_text(
        """
companies:
  - ticker: NVDA
    cik: "0001045810"
    company_name: NVIDIA Corporation
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
      - quarterly
    concepts:
      - taxonomy: us-gaap
        concept: Revenues
        unit: USD
""",
        encoding="utf-8",
    )
    json_path = _write_companyfacts(tmp_path, ticker="NVDA", cik="0001045810")
    _write_manifest(tmp_path, ticker="NVDA", cik="0001045810", json_path=json_path)
    output_path = tmp_path / "sec_fundamentals.csv"
    report_path = tmp_path / "sec_fundamentals.md"
    validation_report_path = tmp_path / "sec_validation.md"

    result = CliRunner().invoke(
        app,
        [
            "fundamentals",
            "extract-sec-metrics",
            "--sec-companies-path",
            str(sec_config_path),
            "--metrics-path",
            str(metrics_path),
            "--input-dir",
            str(tmp_path),
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
    assert "SEC 基本面指标抽取状态：PASS" in result.output
    assert output_path.exists()
    assert report_path.exists()
    assert validation_report_path.exists()


def test_fundamentals_cli_validate_sec_metrics(tmp_path: Path) -> None:
    sec_config_path = tmp_path / "sec_companies.yaml"
    sec_config_path.write_text(
        """
companies:
  - ticker: NVDA
    cik: "0001045810"
    company_name: NVIDIA Corporation
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
""",
        encoding="utf-8",
    )
    json_path = _write_companyfacts(tmp_path, ticker="NVDA", cik="0001045810")
    _write_manifest(tmp_path, ticker="NVDA", cik="0001045810", json_path=json_path)
    validation = validate_sec_companyfacts_cache(_sec_config(), input_dir=tmp_path, as_of=_date())
    report = build_sec_fundamental_metrics_report(
        companies=_sec_config(),
        metrics=_metrics_config(),
        input_dir=tmp_path,
        as_of=_date(),
        validation_report=validation,
    )
    metrics_csv_path = tmp_path / "sec_fundamentals.csv"
    write_sec_fundamental_metrics_csv(report, metrics_csv_path)
    validation_report_path = tmp_path / "sec_fundamentals_validation.md"

    result = CliRunner().invoke(
        app,
        [
            "fundamentals",
            "validate-sec-metrics",
            "--sec-companies-path",
            str(sec_config_path),
            "--metrics-path",
            str(metrics_path),
            "--input-path",
            str(metrics_csv_path),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(validation_report_path),
        ],
    )

    assert result.exit_code == 0
    assert "SEC 基本面指标 CSV 校验状态：PASS" in result.output
    assert validation_report_path.exists()


def _sec_config() -> SecCompaniesConfig:
    return SecCompaniesConfig(
        companies=[
            SecCompanyConfig(
                ticker="NVDA",
                cik="0001045810",
                company_name="NVIDIA Corporation",
                expected_taxonomies=["us-gaap", "dei"],
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
                preferred_periods=["annual", "quarterly"],
                concepts=[
                    FundamentalMetricConceptConfig(
                        taxonomy="us-gaap",
                        concept="Revenues",
                        unit="USD",
                    )
                ],
            )
        ]
    )


def _metrics_config_with_preferred_old_concept() -> FundamentalMetricsConfig:
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
                        concept="RevenueFromContractWithCustomerExcludingAssessedTax",
                        unit="USD",
                    ),
                    FundamentalMetricConceptConfig(
                        taxonomy="us-gaap",
                        concept="Revenues",
                        unit="USD",
                    ),
                ],
            )
        ]
    )


def _ifrs_metrics_config() -> FundamentalMetricsConfig:
    return FundamentalMetricsConfig(
        metrics=[
            FundamentalMetricConfig(
                metric_id="revenue",
                name="Revenue",
                description="SEC companyfacts 披露的总收入。",
                preferred_periods=["annual", "quarterly"],
                concepts=[
                    FundamentalMetricConceptConfig(
                        taxonomy="ifrs-full",
                        concept="Revenue",
                        unit="TWD",
                    )
                ],
            )
        ]
    )


def _metrics_config_with_derived_gross_profit() -> FundamentalMetricsConfig:
    return FundamentalMetricsConfig(
        metrics=[
            FundamentalMetricConfig(
                metric_id="revenue",
                name="Revenue",
                description="SEC companyfacts 披露的总收入。",
                preferred_periods=["annual", "quarterly"],
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
                description="收入扣除收入成本后的毛利。",
                preferred_periods=["annual", "quarterly"],
                concepts=[
                    FundamentalMetricConceptConfig(
                        taxonomy="us-gaap",
                        concept="GrossProfit",
                        unit="USD",
                    )
                ],
            ),
        ],
        supporting_metrics=[
            FundamentalMetricConfig(
                metric_id="cost_of_revenue",
                name="Cost of Revenue",
                description="派生毛利时使用的收入成本。",
                preferred_periods=["annual", "quarterly"],
                concepts=[
                    FundamentalMetricConceptConfig(
                        taxonomy="us-gaap",
                        concept="CostOfRevenue",
                        unit="USD",
                    )
                ],
            )
        ],
        derived_metrics=[
            FundamentalDerivedMetricConfig(
                metric_id="gross_profit",
                operation="difference",
                minuend_metric_id="revenue",
                subtrahend_metric_id="cost_of_revenue",
            )
        ],
    )


def _write_companyfacts(tmp_path: Path, ticker: str, cik: str) -> Path:
    json_path = tmp_path / f"{ticker.lower()}_companyfacts.json"
    json_path.write_text(
        json.dumps(
            {
                "cik": int(cik),
                "entityName": f"{ticker} Test Entity",
                "facts": {
                    "us-gaap": {
                        "RevenueFromContractWithCustomerExcludingAssessedTax": {
                            "units": {
                                "USD": [
                                    {
                                        "fy": 2022,
                                        "fp": "FY",
                                        "form": "10-K",
                                        "start": "2021-01-31",
                                        "end": "2022-01-30",
                                        "filed": "2022-03-18",
                                        "val": 700,
                                        "accn": "0001045810-22-000001",
                                    }
                                ]
                            }
                        },
                        "Revenues": {
                            "units": {
                                "USD": [
                                    {
                                        "fy": 2024,
                                        "fp": "FY",
                                        "form": "10-K",
                                        "end": "2024-01-28",
                                        "filed": "2024-02-21",
                                        "val": 1000,
                                        "accn": "0001045810-24-000001",
                                    },
                                    {
                                        "fy": 2025,
                                        "fp": "FY",
                                        "form": "10-K",
                                        "end": "2025-01-26",
                                        "filed": "2025-02-26",
                                        "val": 2000,
                                        "accn": "0001045810-25-000001",
                                    },
                                    {
                                        "fy": 2026,
                                        "fp": "Q1",
                                        "form": "10-Q",
                                        "start": "2025-01-27",
                                        "end": "2025-04-27",
                                        "filed": "2025-05-28",
                                        "val": 1600,
                                        "accn": "0001045810-25-000010",
                                    },
                                    {
                                        "fy": 2026,
                                        "fp": "Q1",
                                        "form": "10-Q",
                                        "start": "2025-01-27",
                                        "end": "2025-04-27",
                                        "filed": "2025-05-28",
                                        "frame": "CY2025Q1",
                                        "val": 650,
                                        "accn": "0001045810-25-000010",
                                    },
                                    {
                                        "fy": 2026,
                                        "fp": "Q3",
                                        "form": "10-Q",
                                        "start": "2025-01-27",
                                        "end": "2025-10-26",
                                        "filed": "2025-11-19",
                                        "val": 1900,
                                        "accn": "0001045810-25-000030",
                                    },
                                    {
                                        "fy": 2026,
                                        "fp": "Q3",
                                        "form": "10-Q",
                                        "start": "2025-07-28",
                                        "end": "2025-10-26",
                                        "filed": "2025-11-19",
                                        "frame": "CY2025Q3",
                                        "val": 850,
                                        "accn": "0001045810-25-000030",
                                    },
                                ]
                            }
                        }
                    },
                    "dei": {},
                },
            },
        ),
        encoding="utf-8",
    )
    return json_path


def _write_companyfacts_without_gross_profit(
    tmp_path: Path,
    ticker: str,
    cik: str,
) -> Path:
    json_path = tmp_path / f"{ticker.lower()}_companyfacts.json"
    json_path.write_text(
        json.dumps(
            {
                "cik": int(cik),
                "entityName": f"{ticker} Test Entity",
                "facts": {
                    "us-gaap": {
                        "Revenues": {
                            "units": {
                                "USD": [
                                    {
                                        "fy": 2025,
                                        "fp": "FY",
                                        "form": "10-K",
                                        "end": "2025-12-31",
                                        "filed": "2026-02-05",
                                        "frame": "CY2025",
                                        "val": 1000,
                                        "accn": "0001652044-26-000001",
                                    },
                                    {
                                        "fy": 2026,
                                        "fp": "Q1",
                                        "form": "10-Q",
                                        "start": "2026-01-01",
                                        "end": "2026-03-31",
                                        "filed": "2026-04-30",
                                        "frame": "CY2026Q1",
                                        "val": 400,
                                        "accn": "0001652044-26-000010",
                                    },
                                ]
                            }
                        },
                        "CostOfRevenue": {
                            "units": {
                                "USD": [
                                    {
                                        "fy": 2025,
                                        "fp": "FY",
                                        "form": "10-K",
                                        "end": "2025-12-31",
                                        "filed": "2026-02-05",
                                        "frame": "CY2025",
                                        "val": 400,
                                        "accn": "0001652044-26-000001",
                                    },
                                    {
                                        "fy": 2026,
                                        "fp": "Q1",
                                        "form": "10-Q",
                                        "start": "2026-01-01",
                                        "end": "2026-03-31",
                                        "filed": "2026-04-30",
                                        "frame": "CY2026Q1",
                                        "val": 150,
                                        "accn": "0001652044-26-000010",
                                    },
                                ]
                            }
                        },
                    },
                    "dei": {},
                },
            },
        ),
        encoding="utf-8",
    )
    return json_path


def _write_ifrs_annual_companyfacts(tmp_path: Path, ticker: str, cik: str) -> Path:
    json_path = tmp_path / f"{ticker.lower()}_companyfacts.json"
    json_path.write_text(
        json.dumps(
            {
                "cik": int(cik),
                "entityName": f"{ticker} Test Entity",
                "facts": {
                    "ifrs-full": {
                        "Revenue": {
                            "units": {
                                "TWD": [
                                    {
                                        "fy": 2024,
                                        "fp": "FY",
                                        "form": "20-F",
                                        "end": "2024-12-31",
                                        "filed": "2025-04-17",
                                        "frame": "CY2024",
                                        "val": 2894307000,
                                        "accn": "0001046179-25-000010",
                                    }
                                ]
                            }
                        }
                    },
                    "dei": {},
                },
            },
        ),
        encoding="utf-8",
    )
    return json_path


def _write_manifest(tmp_path: Path, ticker: str, cik: str, json_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "ticker": ticker,
                "cik": cik,
                "checksum_sha256": _sha256(json_path),
            }
        ]
    ).to_csv(tmp_path / "sec_companyfacts_manifest.csv", index=False)


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _date() -> date:
    return date(2026, 5, 2)
