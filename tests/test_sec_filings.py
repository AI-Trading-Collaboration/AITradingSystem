from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import SecCompaniesConfig, SecCompanyConfig
from ai_trading_system.fundamentals.sec_filings import (
    SecFilingIndexRequest,
    SecFilingRequest,
    build_sec_accession_coverage_report,
    download_sec_filing_archive_indexes,
    download_sec_submissions,
    render_sec_accession_coverage_report,
)


@dataclass(frozen=True)
class FakeSecFilingArchiveProvider:
    def download_submissions(self, request: SecFilingRequest) -> dict[str, Any]:
        return {
            "cik": request.cik,
            "entityName": f"{request.ticker} Test Entity",
            "filings": {
                "recent": {
                    "accessionNumber": ["0001045810-26-000001"],
                    "filingDate": ["2026-03-15"],
                    "reportDate": ["2026-01-31"],
                    "acceptanceDateTime": ["2026-03-15T16:01:02.000Z"],
                    "form": ["10-K"],
                    "primaryDocument": ["nvda-20260131.htm"],
                },
                "files": [],
            },
        }

    def download_filing_index(self, request: SecFilingIndexRequest) -> dict[str, Any]:
        return {
            "directory": {
                "name": request.accession_number.replace("-", ""),
                "item": [
                    {"name": "index.html", "type": "text/html"},
                    {"name": "nvda-20260131.htm", "type": "text/html"},
                ],
            }
        }

    def submissions_endpoint_for(self, cik: str) -> str:
        return f"https://example.test/submissions/CIK{cik}.json"

    def filing_index_endpoint_for(self, cik: str, accession_number: str) -> str:
        return f"https://example.test/Archives/{cik}/{accession_number}/index.json"


def test_sec_submissions_archive_and_accession_coverage(tmp_path: Path) -> None:
    metrics_path = _write_metrics_csv(tmp_path / "sec_fundamentals.csv")
    submissions_summary = download_sec_submissions(
        config=_sec_config(),
        output_dir=tmp_path / "submissions",
        provider=FakeSecFilingArchiveProvider(),
        tickers=["NVDA"],
    )
    archive_summary = download_sec_filing_archive_indexes(
        metrics_path=metrics_path,
        as_of=_date(),
        output_dir=tmp_path / "sec_filings",
        provider=FakeSecFilingArchiveProvider(),
        request_delay_seconds=0,
    )
    report = build_sec_accession_coverage_report(
        metrics_path=metrics_path,
        submissions_dir=submissions_summary.output_dir,
        filing_archive_dir=archive_summary.output_dir,
        as_of=_date(),
    )
    markdown = render_sec_accession_coverage_report(report)

    assert submissions_summary.company_count == 1
    assert submissions_summary.filing_count == 1
    assert archive_summary.accession_count == 1
    assert report.status == "PASS"
    assert report.covered_count == 1
    assert report.rows[0].accepted_time == "2026-03-15T16:01:02.000Z"
    assert "SEC Accession Archive 覆盖报告" in markdown
    assert "0001045810-26-000001" in markdown


def test_sec_accession_coverage_reports_missing_archive_index(tmp_path: Path) -> None:
    metrics_path = _write_metrics_csv(tmp_path / "sec_fundamentals.csv")
    download_sec_submissions(
        config=_sec_config(),
        output_dir=tmp_path / "submissions",
        provider=FakeSecFilingArchiveProvider(),
        tickers=["NVDA"],
    )

    report = build_sec_accession_coverage_report(
        metrics_path=metrics_path,
        submissions_dir=tmp_path / "submissions",
        filing_archive_dir=tmp_path / "missing_filings",
        as_of=_date(),
    )

    assert report.status == "FAIL"
    assert "sec_accession_archive_index_missing" in {
        issue.code for issue in report.issues
    }
    assert report.rows[0].status == "MISSING_ARCHIVE_INDEX"


def test_sec_accession_coverage_cli_writes_report(tmp_path: Path) -> None:
    metrics_path = _write_metrics_csv(tmp_path / "sec_fundamentals.csv")
    submissions_summary = download_sec_submissions(
        config=_sec_config(),
        output_dir=tmp_path / "submissions",
        provider=FakeSecFilingArchiveProvider(),
        tickers=["NVDA"],
    )
    archive_summary = download_sec_filing_archive_indexes(
        metrics_path=metrics_path,
        as_of=_date(),
        output_dir=tmp_path / "sec_filings",
        provider=FakeSecFilingArchiveProvider(),
        request_delay_seconds=0,
    )
    output_path = tmp_path / "sec_accession_coverage.md"

    result = CliRunner().invoke(
        app,
        [
            "fundamentals",
            "sec-accession-coverage",
            "--as-of",
            _date().isoformat(),
            "--metrics-path",
            str(metrics_path),
            "--submissions-dir",
            str(submissions_summary.output_dir),
            "--filing-archive-dir",
            str(archive_summary.output_dir),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "SEC accession 覆盖状态：PASS" in result.output
    assert output_path.exists()
    assert "Accepted Time" in output_path.read_text(encoding="utf-8")


def _date() -> date:
    return date(2026, 5, 2)


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


def _write_metrics_csv(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "as_of": _date().isoformat(),
                "ticker": "NVDA",
                "cik": "0001045810",
                "company_name": "NVIDIA Corporation",
                "metric_id": "revenue",
                "metric_name": "Revenue",
                "period_type": "annual",
                "fiscal_year": 2026,
                "fiscal_period": "FY",
                "end_date": "2026-01-31",
                "filed_date": "2026-03-15",
                "form": "10-K",
                "taxonomy": "us-gaap",
                "concept": "Revenues",
                "unit": "USD",
                "value": 1000,
                "accession_number": "0001045810-26-000001",
                "source_path": "nvda_companyfacts.json",
            }
        ]
    ).to_csv(path, index=False)
    return path
