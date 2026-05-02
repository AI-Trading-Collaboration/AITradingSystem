from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import (
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
        ("quarterly", 2026, 650.0),
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


def _write_companyfacts(tmp_path: Path, ticker: str, cik: str) -> Path:
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
                                        "end": "2025-04-27",
                                        "filed": "2025-05-28",
                                        "val": 650,
                                        "accn": "0001045810-25-000010",
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
