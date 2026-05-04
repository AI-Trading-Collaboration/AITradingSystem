from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from pytest import MonkeyPatch
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import (
    FundamentalMetricConceptConfig,
    FundamentalMetricConfig,
    FundamentalMetricsConfig,
    SecCompaniesConfig,
    SecCompanyConfig,
)
from ai_trading_system.fundamentals import tsm_ir as tsm_ir_module
from ai_trading_system.fundamentals.sec_metrics import (
    SecFundamentalMetricRow,
    validate_sec_fundamental_metric_rows,
)
from ai_trading_system.fundamentals.tsm_ir import (
    TsmIrHttpProvider,
    TsmIrQuarterlyReport,
    TsmIrQuarterlyResource,
    build_tsm_ir_quarterly_batch_import_report,
    build_tsm_ir_quarterly_report,
    build_tsm_ir_sec_metric_conversion_report,
    convert_tsm_ir_quarterly_metric_rows_to_sec_metric_rows,
    extract_tsm_ir_management_report_pdf_text,
    extract_tsm_ir_pdf_text,
    is_official_tsm_ir_url,
    load_tsm_ir_quarterly_import_manifest_csv,
    merge_tsm_ir_quarterly_rows_into_sec_metrics,
    merge_tsm_ir_quarterly_rows_into_sec_metrics_as_of,
    parse_tsm_ir_management_report_text,
    render_tsm_ir_pdf_text_extraction_report,
    render_tsm_ir_quarterly_batch_import_report,
    render_tsm_ir_quarterly_report,
    select_tsm_ir_quarterly_metric_rows_as_of,
    write_tsm_ir_pdf_text_extraction_report,
    write_tsm_ir_quarterly_batch_import_report,
    write_tsm_ir_quarterly_batch_metrics_csv,
    write_tsm_ir_quarterly_metrics_csv,
)

MANAGEMENT_REPORT_TEXT = """
Taiwan Semiconductor Manufacturing Company Limited
Management Report
2026 First Quarter
NT$M Except Per Share Data

Summary of Consolidated Financial Results
Net Revenue                            839,254
Gross Profit                           493,831
Gross Margin                            58.8%
Operating Income                       415,524
Research & Development                  79,551
Operating Margin                        49.5%
Net Income Attributable to Shareholders of the Parent
                                      361,560
Net Profit Margin                       43.1%

Capital Expenditures
In 1Q26, capital expenditures totaled US$9.4 billion.
"""


class FakeTsmIrProvider:
    def __init__(self, resources: dict[str, str]) -> None:
        self.resources = resources

    def download_text(self, url: str) -> str:
        return self.resources[url]


class FakeHttpResponse:
    def __init__(self, text: str, content_type: str = "text/html") -> None:
        self.content = text.encode("utf-8")
        self.headers = {"Content-Type": content_type}
        self.encoding = "utf-8"

    def raise_for_status(self) -> None:
        return None


class FakeRequestsModule:
    def __init__(self, responses: dict[str, FakeHttpResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, str], float]] = []

    def get(
        self,
        url: str,
        headers: dict[str, str],
        timeout: float,
    ) -> FakeHttpResponse:
        self.calls.append((url, headers, timeout))
        return self.responses[url]


def _fake_pypdf_module(
    page_texts: tuple[str | None, ...],
    opened_paths: list[Path],
) -> SimpleNamespace:
    class FakePdfPage:
        def __init__(self, text: str | None) -> None:
            self.text = text

        def extract_text(self) -> str | None:
            return self.text

    class FakePdfReader:
        def __init__(self, input_path: Path) -> None:
            opened_paths.append(input_path)
            self.pages = tuple(FakePdfPage(text) for text in page_texts)

    return SimpleNamespace(PdfReader=FakePdfReader)


def test_parse_tsm_ir_management_report_text_extracts_required_metrics() -> None:
    report = parse_tsm_ir_management_report_text(
        text=MANAGEMENT_REPORT_TEXT,
        source_url="https://investor.tsmc.com/english/quarterly-results/2026/q1",
        fiscal_year=2026,
        fiscal_period="Q1",
        as_of=date(2026, 5, 5),
        captured_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
    )

    metrics = {row.metric_id: row for row in report.rows}

    assert report.status == "PASS"
    assert set(metrics) == {
        "revenue",
        "gross_profit",
        "gross_margin",
        "operating_income",
        "research_and_development",
        "operating_margin",
        "net_income",
        "net_margin",
        "capex",
    }
    assert metrics["revenue"].value == 839254.0
    assert metrics["revenue"].unit == "TWD_millions"
    assert metrics["gross_margin"].value == 58.8
    assert metrics["gross_margin"].unit == "percent"
    assert metrics["research_and_development"].value == 79551.0
    assert metrics["research_and_development"].unit == "TWD_millions"
    assert metrics["capex"].value == 9.4
    assert metrics["capex"].unit == "USD_billions"
    assert report.checksum_sha256
    assert report.row_count == 9


def test_parse_tsm_ir_management_report_text_uses_explicit_filed_date() -> None:
    report = parse_tsm_ir_management_report_text(
        text=MANAGEMENT_REPORT_TEXT,
        source_url="https://investor.tsmc.com/english/quarterly-results/2026/q1",
        fiscal_year=2026,
        fiscal_period="Q1",
        as_of=date(2026, 5, 5),
        captured_at=datetime(2026, 5, 4, 8, 30, tzinfo=UTC),
        filed_date=date(2026, 4, 16),
    )

    assert report.status == "PASS"
    assert {row.filed_date for row in report.rows} == {date(2026, 4, 16)}


def test_parse_tsm_ir_management_report_text_handles_billion_scale_report_tables() -> None:
    real_format_text = """
Summary:
(Amounts are on consolidated basis and are in NT$ billions unless otherwise noted) 1Q26 4Q25 1Q25
Net Revenue (US$ billions) 35.90 33.73 25.53
Net Revenue 1,134.10 1,046.09 839.25
Gross Profit 751.30 651.99 493.40
Gross Margin 66.2% 62.3% 58.8%
Other Operating Income and Expenses 1.68 1.10 (1.13)
Operating Income 658.97 564.90 407.08
Research & Development (67.76) (64.86) (56.55)
Operating Margin 58.1% 54.0% 48.5%
Net Income Attributable to Shareholders of the Parent Company 572.48 505.74 361.56
Net Profit Margin 50.5% 48.3% 43.1%
Capital expenditures for TSMC on a consolidated basis totaled US$11.10 billion in 1Q26.
"""
    report = parse_tsm_ir_management_report_text(
        text=real_format_text,
        source_url="https://investor.tsmc.com/english/quarterly-results/2026/q1",
        fiscal_year=2026,
        fiscal_period="Q1",
        as_of=date(2026, 5, 2),
        captured_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
    )

    metrics = {row.metric_id: row for row in report.rows}

    assert report.status == "PASS"
    assert metrics["revenue"].value == 1134.10
    assert metrics["revenue"].unit == "TWD_billions"
    assert metrics["operating_income"].value == 658.97
    assert metrics["research_and_development"].value == 67.76
    assert metrics["research_and_development"].unit == "TWD_billions"
    assert metrics["capex"].value == 11.10
    assert metrics["capex"].unit == "USD_billions"


def test_parse_tsm_ir_management_report_text_warns_when_capex_absent() -> None:
    report = parse_tsm_ir_management_report_text(
        text=MANAGEMENT_REPORT_TEXT.replace(
            "Capital Expenditures\nIn 1Q26, capital expenditures totaled US$9.4 billion.",
            "",
        ),
        source_url="https://investor.tsmc.com/english/quarterly-results/2026/q1",
        fiscal_year=2026,
        fiscal_period="Q1",
        as_of=date(2026, 5, 2),
        captured_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
    )

    assert report.status == "PASS_WITH_WARNINGS"
    assert "capex" not in {row.metric_id for row in report.rows}
    assert "tsm_ir_metric_missing" in {issue.code for issue in report.issues}


def test_tsm_ir_official_url_validation() -> None:
    assert is_official_tsm_ir_url(
        "https://investor.tsmc.com/english/quarterly-results/2026/q1"
    )
    assert is_official_tsm_ir_url(
        "https://investor.tsmc.com/sites/ir/quarterly-results/2026/q1-management-report.pdf"
    )
    assert not is_official_tsm_ir_url(
        "https://example.com/english/quarterly-results/2026/q1"
    )

    report = parse_tsm_ir_management_report_text(
        text=MANAGEMENT_REPORT_TEXT,
        source_url="https://example.com/english/quarterly-results/2026/q1",
        fiscal_year=2026,
        fiscal_period="Q1",
        as_of=date(2026, 5, 2),
        captured_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
    )

    assert report.status == "FAIL"
    assert "tsm_ir_non_official_url" in {issue.code for issue in report.issues}


def test_extract_tsm_ir_management_report_pdf_text_writes_text_and_audit(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    input_path = tmp_path / "management_report.pdf"
    output_path = tmp_path / "management_report.txt"
    input_path.write_bytes(b"%PDF-1.7 fake payload")
    opened_paths: list[Path] = []
    fake_pypdf = _fake_pypdf_module(("First page text", "Second page text"), opened_paths)

    def fake_import_module(name: str) -> SimpleNamespace:
        assert name == "pypdf"
        return fake_pypdf

    monkeypatch.setattr(tsm_ir_module, "import_module", fake_import_module)
    extracted_at = datetime(2026, 4, 16, 8, 30, tzinfo=UTC)

    report = extract_tsm_ir_pdf_text(
        input_path=input_path,
        source_url=(
            "https://investor.tsmc.com/sites/ir/quarterly-results/2026/"
            "q1-management-report.pdf"
        ),
        output_path=output_path,
        extracted_at=extracted_at,
    )

    expected_text = "First page text\n\nSecond page text"

    assert report.status == "PASS"
    assert report.provider == "TSMC Investor Relations"
    assert report.input_path == input_path
    assert report.output_path == output_path
    assert report.extracted_at == extracted_at
    assert report.page_count == 2
    assert report.character_count == len(expected_text)
    assert report.char_count == len(expected_text)
    assert report.input_checksum_sha256 == hashlib.sha256(
        b"%PDF-1.7 fake payload"
    ).hexdigest()
    assert report.text_checksum_sha256 == hashlib.sha256(
        expected_text.encode("utf-8")
    ).hexdigest()
    assert report.checksum_sha256 == hashlib.sha256(expected_text.encode("utf-8")).hexdigest()
    assert report.issues == ()
    assert opened_paths == [input_path]
    assert output_path.read_text(encoding="utf-8") == expected_text
    markdown = render_tsm_ir_pdf_text_extraction_report(report)
    assert "TSMC IR PDF 文本抽取报告" in markdown
    assert "Input checksum_sha256" in markdown
    assert "Text checksum_sha256" in markdown
    report_path = tmp_path / "pdf_text_extraction.md"
    assert write_tsm_ir_pdf_text_extraction_report(report, report_path) == report_path
    assert report_path.read_text(encoding="utf-8") == markdown


def test_extract_tsm_ir_management_report_pdf_text_rejects_non_official_url(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    input_path = tmp_path / "management_report.pdf"
    output_path = tmp_path / "management_report.txt"
    input_path.write_bytes(b"%PDF-1.7 fake payload")

    def fake_import_module(name: str) -> SimpleNamespace:
        raise AssertionError(f"pypdf should not be imported for invalid source URL: {name}")

    monkeypatch.setattr(tsm_ir_module, "import_module", fake_import_module)

    report = extract_tsm_ir_management_report_pdf_text(
        input_path=input_path,
        output_path=output_path,
        source_url="https://example.com/management-report.pdf",
        extracted_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
    )

    assert report.status == "FAIL"
    assert report.page_count == 0
    assert report.char_count == 0
    assert report.checksum_sha256 == ""
    assert "tsm_ir_non_official_url" in {issue.code for issue in report.issues}
    assert not output_path.exists()


def test_extract_tsm_ir_management_report_pdf_text_reports_missing_input_path(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    input_path = tmp_path / "missing_management_report.pdf"
    output_path = tmp_path / "management_report.txt"

    def fake_import_module(name: str) -> SimpleNamespace:
        raise AssertionError(f"pypdf should not be imported for missing input path: {name}")

    monkeypatch.setattr(tsm_ir_module, "import_module", fake_import_module)

    report = extract_tsm_ir_management_report_pdf_text(
        input_path=input_path,
        output_path=output_path,
        source_url="https://investor.tsmc.com/sites/ir/management-report.pdf",
        extracted_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
    )

    assert report.status == "FAIL"
    assert report.input_path == input_path
    assert "tsm_ir_pdf_input_missing" in {issue.code for issue in report.issues}
    assert not output_path.exists()


def test_extract_tsm_ir_management_report_pdf_text_reports_missing_pypdf(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    input_path = tmp_path / "management_report.pdf"
    output_path = tmp_path / "management_report.txt"
    input_path.write_bytes(b"%PDF-1.7 fake payload")

    def fake_import_module(name: str) -> SimpleNamespace:
        assert name == "pypdf"
        raise ModuleNotFoundError("No module named 'pypdf'")

    monkeypatch.setattr(tsm_ir_module, "import_module", fake_import_module)

    report = extract_tsm_ir_management_report_pdf_text(
        input_path=input_path,
        output_path=output_path,
        source_url="https://investor.tsmc.com/sites/ir/management-report.pdf",
        extracted_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
    )

    assert report.status == "FAIL"
    assert "tsm_ir_pdf_dependency_missing" in {issue.code for issue in report.issues}
    assert not output_path.exists()


def test_extract_tsm_ir_management_report_pdf_text_reports_empty_extraction(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    input_path = tmp_path / "management_report.pdf"
    output_path = tmp_path / "management_report.txt"
    input_path.write_bytes(b"%PDF-1.7 fake payload")
    opened_paths: list[Path] = []
    fake_pypdf = _fake_pypdf_module((None, " \n\t "), opened_paths)

    def fake_import_module(name: str) -> SimpleNamespace:
        assert name == "pypdf"
        return fake_pypdf

    monkeypatch.setattr(tsm_ir_module, "import_module", fake_import_module)

    report = extract_tsm_ir_management_report_pdf_text(
        input_path=input_path,
        output_path=output_path,
        source_url="https://investor.tsmc.com/sites/ir/management-report.pdf",
        extracted_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
    )

    assert report.status == "FAIL"
    assert report.page_count == 2
    assert report.char_count == 0
    assert report.checksum_sha256 == ""
    assert "tsm_ir_pdf_text_empty" in {issue.code for issue in report.issues}
    assert opened_paths == [input_path]
    assert not output_path.exists()


def test_fundamentals_extract_tsm_ir_pdf_text_cli_writes_text_and_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    input_path = tmp_path / "management_report.pdf"
    output_path = tmp_path / "management_report.txt"
    report_path = tmp_path / "pdf_text_report.md"
    input_path.write_bytes(b"%PDF-1.7 fake payload")
    opened_paths: list[Path] = []
    fake_pypdf = _fake_pypdf_module(("First page text", "Second page text"), opened_paths)

    def fake_import_module(name: str) -> SimpleNamespace:
        assert name == "pypdf"
        return fake_pypdf

    monkeypatch.setattr(tsm_ir_module, "import_module", fake_import_module)

    result = CliRunner().invoke(
        app,
        [
            "fundamentals",
            "extract-tsm-ir-pdf-text",
            "--input-path",
            str(input_path),
            "--source-url",
            "https://investor.tsmc.com/sites/ir/management-report.pdf",
            "--output-path",
            str(output_path),
            "--as-of",
            "2026-05-02",
            "--extracted-at",
            "2026-04-16T08:30:00+00:00",
            "--report-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "TSMC IR PDF 文本抽取状态：PASS" in result.output
    assert "页数：2；字符数：33" in result.output
    assert opened_paths == [input_path]
    assert output_path.read_text(encoding="utf-8") == "First page text\n\nSecond page text"
    assert report_path.exists()
    assert "Input checksum_sha256" in report_path.read_text(encoding="utf-8")


def test_write_tsm_ir_quarterly_metrics_csv(tmp_path: Path) -> None:
    report = parse_tsm_ir_management_report_text(
        text=MANAGEMENT_REPORT_TEXT,
        source_url="https://investor.tsmc.com/english/quarterly-results/2026/q1",
        fiscal_year=2026,
        fiscal_period="Q1",
        as_of=date(2026, 5, 2),
        captured_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
    )
    output_path = tmp_path / "tsm_ir_metrics.csv"

    write_tsm_ir_quarterly_metrics_csv(report, output_path)
    write_tsm_ir_quarterly_metrics_csv(report, output_path)

    frame = pd.read_csv(output_path)
    assert len(frame) == report.row_count
    assert set(frame["metric_id"]) == {
        "revenue",
        "gross_profit",
        "gross_margin",
        "operating_income",
        "research_and_development",
        "operating_margin",
        "net_income",
        "net_margin",
        "capex",
    }
    assert set(frame["source_id"]) == {"tsm_investor_relations_quarterly_results"}


def test_render_tsm_ir_quarterly_report() -> None:
    report = parse_tsm_ir_management_report_text(
        text=MANAGEMENT_REPORT_TEXT,
        source_url="https://investor.tsmc.com/english/quarterly-results/2026/q1",
        fiscal_year=2026,
        fiscal_period="Q1",
        as_of=date(2026, 5, 2),
        captured_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
    )

    markdown = render_tsm_ir_quarterly_report(report)

    assert "# TSMC IR 季度基本面摘要" in markdown
    assert "TSMC Investor Relations" in markdown
    assert "指标行数：9" in markdown
    assert "checksum_sha256" in markdown
    assert "Net Revenue" in markdown


def test_build_tsm_ir_quarterly_report_uses_fake_provider_resource() -> None:
    resource = TsmIrQuarterlyResource(
        url="https://investor.tsmc.com/english/quarterly-results/2026/q1/management-report",
        resource_type="management_report_text",
    )
    provider = FakeTsmIrProvider({resource.url: MANAGEMENT_REPORT_TEXT})

    report = build_tsm_ir_quarterly_report(
        provider=provider,
        source_url="https://investor.tsmc.com/english/quarterly-results/2026/q1",
        resources=(resource,),
        fiscal_year=2026,
        fiscal_period="Q1",
        as_of=date(2026, 5, 2),
        captured_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
    )

    assert report.status == "PASS"
    assert report.source_url == resource.url


def test_http_provider_discovers_management_report_and_downloads_text() -> None:
    page_url = "https://investor.tsmc.com/english/quarterly-results/2026/q1"
    management_url = (
        "https://investor.tsmc.com/english/quarterly-results/2026/q1/"
        "management-report.txt"
    )
    page_html = f"""
    <html>
      <body>
        <a href="{management_url}"><span>Management Report</span></a>
      </body>
    </html>
    """
    fake_requests = FakeRequestsModule(
        {
            page_url: FakeHttpResponse(page_html, "text/html; charset=utf-8"),
            management_url: FakeHttpResponse(
                MANAGEMENT_REPORT_TEXT,
                "text/plain; charset=utf-8",
            ),
        }
    )
    provider = TsmIrHttpProvider(
        requests_module=fake_requests,
        timeout=5,
        user_agent="aits-test",
    )

    report = build_tsm_ir_quarterly_report(
        provider=provider,
        source_url=page_url,
        fiscal_year=2026,
        fiscal_period="Q1",
        as_of=date(2026, 5, 2),
        captured_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
    )

    assert report.status == "PASS"
    assert report.source_url == management_url
    assert [call[0] for call in fake_requests.calls] == [page_url, management_url]
    assert fake_requests.calls[0][1]["User-Agent"] == "aits-test"
    assert fake_requests.calls[0][2] == 5


def test_http_provider_ignores_non_anchor_preconnect_links() -> None:
    page_url = "https://investor.tsmc.com/english/quarterly-results/2026/q1"
    management_url = (
        "https://investor.tsmc.com/english/quarterly-results/2026/q1/"
        "management-report.txt"
    )
    page_html = f"""
    <html>
      <head>
        <link rel="preconnect" href="https://use.fontawesome.com">
      </head>
      <body>
        <a href="{management_url}">Management Report</a>
      </body>
    </html>
    """
    fake_requests = FakeRequestsModule(
        {
            page_url: FakeHttpResponse(page_html, "text/html; charset=utf-8"),
            management_url: FakeHttpResponse(
                MANAGEMENT_REPORT_TEXT,
                "text/plain; charset=utf-8",
            ),
        }
    )
    provider = TsmIrHttpProvider(requests_module=fake_requests)

    report = build_tsm_ir_quarterly_report(
        provider=provider,
        source_url=page_url,
        fiscal_year=2026,
        fiscal_period="Q1",
        as_of=date(2026, 5, 2),
        captured_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
    )

    assert report.status == "PASS"
    assert report.source_url == management_url
    assert [call[0] for call in fake_requests.calls] == [page_url, management_url]


def test_http_provider_reports_binary_management_report_requires_extracted_text() -> None:
    page_url = "https://investor.tsmc.com/english/quarterly-results/2026/q1"
    management_url = (
        "https://investor.tsmc.com/english/quarterly-results/2026/q1/"
        "management-report"
    )
    page_html = f'<a href="{management_url}">Management Report</a>'
    fake_requests = FakeRequestsModule(
        {
            page_url: FakeHttpResponse(page_html, "text/html; charset=utf-8"),
            management_url: FakeHttpResponse("%PDF-1.7 binary payload", "application/pdf"),
        }
    )
    provider = TsmIrHttpProvider(requests_module=fake_requests)

    report = build_tsm_ir_quarterly_report(
        provider=provider,
        source_url=page_url,
        fiscal_year=2026,
        fiscal_period="Q1",
        as_of=date(2026, 5, 2),
        captured_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
    )

    issue_messages = " ".join(issue.message for issue in report.issues)

    assert report.status == "FAIL"
    assert report.rows == ()
    assert report.source_url == management_url
    assert "tsm_ir_source_download_failed" in {issue.code for issue in report.issues}
    assert "already extracted Management Report text" in issue_messages
    assert "PDF/binary" in issue_messages


def test_fundamentals_fetch_tsm_ir_quarterly_cli_downloads_text_csv_and_report(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    page_url = "https://investor.tsmc.com/english/quarterly-results/2026/q1"
    management_url = (
        "https://investor.tsmc.com/english/quarterly-results/2026/q1/"
        "management-report.txt"
    )
    page_html = f'<a href="{management_url}">Management Report</a>'
    fake_requests = FakeRequestsModule(
        {
            page_url: FakeHttpResponse(page_html, "text/html; charset=utf-8"),
            management_url: FakeHttpResponse(
                MANAGEMENT_REPORT_TEXT,
                "text/plain; charset=utf-8",
            ),
        }
    )

    def fake_import_module(name: str) -> FakeRequestsModule:
        assert name == "requests"
        return fake_requests

    monkeypatch.setattr(tsm_ir_module, "import_module", fake_import_module)
    source_text_path = tmp_path / "management_report.txt"
    output_path = tmp_path / "tsm_ir_metrics.csv"
    report_path = tmp_path / "tsm_ir_report.md"

    result = CliRunner().invoke(
        app,
        [
            "fundamentals",
            "fetch-tsm-ir-quarterly",
            "--source-url",
            page_url,
            "--fiscal-year",
            "2026",
            "--fiscal-period",
            "Q1",
            "--as-of",
            "2026-05-02",
            "--captured-at",
            "2026-04-16T08:30:00+00:00",
            "--source-text-path",
            str(source_text_path),
            "--output-path",
            str(output_path),
            "--report-path",
            str(report_path),
            "--timeout",
            "5",
            "--user-agent",
            "aits-test",
        ],
    )

    assert result.exit_code == 0
    assert "TSMC IR 季度基本面抓取状态：PASS" in result.output
    assert "Management Report" in result.output
    assert source_text_path.read_text(encoding="utf-8") == MANAGEMENT_REPORT_TEXT
    assert output_path.exists()
    assert report_path.exists()
    assert [call[0] for call in fake_requests.calls] == [page_url, management_url]
    assert fake_requests.calls[0][1]["User-Agent"] == "aits-test"
    assert fake_requests.calls[0][2] == 5.0
    frame = pd.read_csv(output_path)
    assert len(frame) == 9
    assert set(frame["source_path"]) == {str(source_text_path)}


def test_convert_tsm_ir_rows_to_sec_metric_rows_excludes_margins_and_fills_company(
    tmp_path: Path,
) -> None:
    report = _parsed_report_with_source_path(tmp_path)
    company = _tsm_company_config()

    converted_rows = convert_tsm_ir_quarterly_metric_rows_to_sec_metric_rows(
        report.rows,
        company,
    )
    metrics = {row.metric_id: row for row in converted_rows}

    assert set(metrics) == {
        "revenue",
        "gross_profit",
        "operating_income",
        "research_and_development",
        "net_income",
        "capex",
    }
    assert metrics["revenue"].cik == "0001046179"
    assert metrics["revenue"].company_name == "Taiwan Semiconductor Manufacturing Company Limited"
    assert metrics["revenue"].taxonomy == "tsm-ir"
    assert metrics["revenue"].concept == "management_report:revenue"
    assert metrics["revenue"].form == "TSM-IR"
    assert metrics["revenue"].unit == "TWD_millions"
    assert metrics["research_and_development"].concept == (
        "management_report:research_and_development"
    )
    assert metrics["revenue"].source_path == tmp_path / "management_report.txt"
    assert "tsm_investor_relations_quarterly_results" in metrics["revenue"].accession_number
    assert "FY2026Q1" in metrics["revenue"].accession_number


def test_convert_tsm_ir_rows_reports_missing_config_or_mismatched_rows(tmp_path: Path) -> None:
    report = _parsed_report_with_source_path(tmp_path)
    missing_company_report = build_tsm_ir_sec_metric_conversion_report(
        report.rows,
        SecCompaniesConfig(
            companies=[
                SecCompanyConfig(
                    ticker="NVDA",
                    cik="0001045810",
                    company_name="NVIDIA Corporation",
                )
            ]
        ),
    )
    assert missing_company_report.status == "FAIL"
    assert "tsm_ir_sec_company_missing" in {issue.code for issue in missing_company_report.issues}

    bad_rows = (
        report.rows[0].__class__(
            **{
                **report.rows[0].__dict__,
                "ticker": "NVDA",
            }
        ),
    )
    row_report = build_tsm_ir_sec_metric_conversion_report(bad_rows, _tsm_company_config())
    assert row_report.status == "FAIL"
    assert "tsm_ir_sec_metric_ticker_mismatch" in {issue.code for issue in row_report.issues}


def test_merge_tsm_ir_rows_replaces_only_tsm_quarterly_duplicates(tmp_path: Path) -> None:
    report = _parsed_report_with_source_path(tmp_path)
    existing_revenue = _sec_metric_row(
        ticker="TSM",
        cik="0001046179",
        company_name="Taiwan Semiconductor Manufacturing Company Limited",
        metric_id="revenue",
        metric_name="Revenue",
        period_type="quarterly",
        value=1.0,
        source_path=tmp_path / "old_sec.csv",
    )
    existing_annual = _sec_metric_row(
        ticker="TSM",
        cik="0001046179",
        company_name="Taiwan Semiconductor Manufacturing Company Limited",
        metric_id="revenue",
        metric_name="Revenue",
        period_type="annual",
        value=2.0,
        source_path=tmp_path / "annual_sec.csv",
    )
    existing_nvda = _sec_metric_row(
        ticker="NVDA",
        cik="0001045810",
        company_name="NVIDIA Corporation",
        metric_id="revenue",
        metric_name="Revenue",
        period_type="quarterly",
        value=3.0,
        source_path=tmp_path / "nvda_sec.csv",
    )

    merged_rows = merge_tsm_ir_quarterly_rows_into_sec_metrics(
        (existing_revenue, existing_annual, existing_nvda),
        report.rows,
        _tsm_company_config(),
    )

    tsm_quarterly_revenue = [
        row
        for row in merged_rows
        if row.ticker == "TSM" and row.metric_id == "revenue" and row.period_type == "quarterly"
    ]
    assert len(tsm_quarterly_revenue) == 1
    assert tsm_quarterly_revenue[0].value == 839254.0
    assert existing_annual in merged_rows
    assert existing_nvda in merged_rows


def test_converted_tsm_ir_rows_validate_as_sec_metric_rows(tmp_path: Path) -> None:
    report = _parsed_report_with_source_path(tmp_path)
    company = _tsm_company_config()
    converted_rows = convert_tsm_ir_quarterly_metric_rows_to_sec_metric_rows(
        report.rows,
        company,
    )

    validation_report = validate_sec_fundamental_metric_rows(
        companies=SecCompaniesConfig(companies=[company]),
        metrics=_tsm_metrics_config(),
        rows=converted_rows,
        source_path=tmp_path / "sec_metrics.csv",
        as_of=report.as_of,
    )

    assert validation_report.status == "PASS"
    assert validation_report.observed_observation_count == 6


def test_fundamentals_import_tsm_ir_quarterly_cli_writes_csv_and_report(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "tsm_management_report.txt"
    output_path = tmp_path / "tsm_ir_metrics.csv"
    report_path = tmp_path / "tsm_ir_report.md"
    input_path.write_text(MANAGEMENT_REPORT_TEXT, encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "fundamentals",
            "import-tsm-ir-quarterly",
            "--input-path",
            str(input_path),
            "--source-url",
            "https://investor.tsmc.com/english/quarterly-results/2026/q1",
            "--fiscal-year",
            "2026",
            "--fiscal-period",
            "Q1",
            "--as-of",
            "2026-05-02",
            "--captured-at",
            "2026-05-02T08:30:00+00:00",
            "--filed-date",
            "2026-04-16",
            "--output-path",
            str(output_path),
            "--report-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "TSMC IR 季度基本面导入状态：PASS" in result.output
    assert output_path.exists()
    assert report_path.exists()
    frame = pd.read_csv(output_path)
    assert len(frame) == 9
    assert set(frame["filed_date"]) == {"2026-04-16"}


def test_tsm_ir_batch_manifest_import_writes_all_quarters(tmp_path: Path) -> None:
    q1_path = tmp_path / "q1_management_report.txt"
    q2_path = tmp_path / "q2_management_report.txt"
    q1_path.write_text(MANAGEMENT_REPORT_TEXT, encoding="utf-8")
    q2_path.write_text(
        MANAGEMENT_REPORT_TEXT.replace("2026 First Quarter", "2026 Second Quarter"),
        encoding="utf-8",
    )
    manifest_path = tmp_path / "tsm_ir_manifest.csv"
    manifest_path.write_text(
        "\n".join(
            [
                "fiscal_year,fiscal_period,source_url,input_path,filed_date",
                (
                    "2026,Q1,"
                    "https://investor.tsmc.com/english/quarterly-results/2026/q1,"
                    "q1_management_report.txt,2026-04-16"
                ),
                (
                    "2026,Q2,"
                    "https://investor.tsmc.com/english/quarterly-results/2026/q2,"
                    "q2_management_report.txt,2026-07-16"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "tsm_ir_metrics.csv"

    entries = load_tsm_ir_quarterly_import_manifest_csv(manifest_path)
    report = build_tsm_ir_quarterly_batch_import_report(
        manifest_path=manifest_path,
        as_of=date(2026, 8, 1),
        captured_at=datetime(2026, 7, 17, 8, 30, tzinfo=UTC),
        output_path=output_path,
    )
    csv_path = write_tsm_ir_quarterly_batch_metrics_csv(report, output_path)
    markdown = render_tsm_ir_quarterly_batch_import_report(report)
    report_path = write_tsm_ir_quarterly_batch_import_report(
        report,
        tmp_path / "batch_report.md",
    )

    assert entries[0].input_path == q1_path.resolve()
    assert entries[0].filed_date == date(2026, 4, 16)
    assert report.status == "PASS"
    assert report.entry_count == 2
    assert report.row_count == 18
    assert "批量季度基本面导入报告" in markdown
    assert csv_path == output_path
    assert report_path.exists()
    frame = pd.read_csv(output_path)
    assert len(frame) == 18
    assert set(frame["fiscal_period"]) == {"Q1", "Q2"}
    assert set(frame["filed_date"]) == {"2026-04-16", "2026-07-16"}


def test_tsm_ir_batch_import_reports_duplicate_quarter_and_does_not_write(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "q1_management_report.txt"
    input_path.write_text(MANAGEMENT_REPORT_TEXT, encoding="utf-8")
    manifest_path = tmp_path / "tsm_ir_manifest.csv"
    manifest_path.write_text(
        "\n".join(
            [
                "fiscal_year,fiscal_period,source_url,input_path",
                (
                    "2026,Q1,"
                    "https://investor.tsmc.com/english/quarterly-results/2026/q1,"
                    "q1_management_report.txt"
                ),
                (
                    "2026,Q1,"
                    "https://investor.tsmc.com/english/quarterly-results/2026/q1,"
                    "q1_management_report.txt"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    report = build_tsm_ir_quarterly_batch_import_report(
        manifest_path=manifest_path,
        as_of=date(2026, 5, 2),
        captured_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
        output_path=tmp_path / "tsm_ir_metrics.csv",
    )

    assert report.status == "FAIL"
    assert "tsm_ir_batch_duplicate_quarter" in {issue.code for issue in report.all_issues}
    with pytest.raises(ValueError, match="未通过"):
        write_tsm_ir_quarterly_batch_metrics_csv(report, tmp_path / "tsm_ir_metrics.csv")


def test_fundamentals_import_tsm_ir_quarterly_batch_cli_writes_csv_and_report(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "q1_management_report.txt"
    output_path = tmp_path / "tsm_ir_metrics.csv"
    report_path = tmp_path / "tsm_ir_batch_report.md"
    manifest_path = tmp_path / "tsm_ir_manifest.csv"
    input_path.write_text(MANAGEMENT_REPORT_TEXT, encoding="utf-8")
    manifest_path.write_text(
        "\n".join(
            [
                "fiscal_year,fiscal_period,source_url,input_path",
                (
                    "2026,Q1,"
                    "https://investor.tsmc.com/english/quarterly-results/2026/q1,"
                    "q1_management_report.txt"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "fundamentals",
            "import-tsm-ir-quarterly-batch",
            "--manifest-path",
            str(manifest_path),
            "--as-of",
            "2026-05-02",
            "--captured-at",
            "2026-04-16T08:30:00+00:00",
            "--output-path",
            str(output_path),
            "--report-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "TSMC IR 批量季度基本面导入状态：PASS" in result.output
    assert output_path.exists()
    assert report_path.exists()
    frame = pd.read_csv(output_path)
    assert len(frame) == 9


def test_tsm_ir_rows_select_and_merge_latest_quarter_as_of(tmp_path: Path) -> None:
    q1_report = parse_tsm_ir_management_report_text(
        text=MANAGEMENT_REPORT_TEXT,
        source_url="https://investor.tsmc.com/english/quarterly-results/2026/q1",
        fiscal_year=2026,
        fiscal_period="Q1",
        as_of=date(2026, 8, 1),
        captured_at=datetime(2026, 8, 1, 8, 30, tzinfo=UTC),
        source_path=tmp_path / "q1_management_report.txt",
        filed_date=date(2026, 4, 16),
    )
    q2_report = parse_tsm_ir_management_report_text(
        text=MANAGEMENT_REPORT_TEXT.replace("2026 First Quarter", "2026 Second Quarter"),
        source_url="https://investor.tsmc.com/english/quarterly-results/2026/q2",
        fiscal_year=2026,
        fiscal_period="Q2",
        as_of=date(2026, 8, 1),
        captured_at=datetime(2026, 8, 1, 8, 30, tzinfo=UTC),
        source_path=tmp_path / "q2_management_report.txt",
        filed_date=date(2026, 7, 16),
    )
    rows = (*q1_report.rows, *q2_report.rows)

    before_q1 = select_tsm_ir_quarterly_metric_rows_as_of(rows, date(2026, 4, 15))
    after_q1 = select_tsm_ir_quarterly_metric_rows_as_of(rows, date(2026, 5, 1))
    after_q2 = select_tsm_ir_quarterly_metric_rows_as_of(rows, date(2026, 8, 1))
    merged_rows = merge_tsm_ir_quarterly_rows_into_sec_metrics_as_of(
        existing_rows=(),
        tsm_rows=rows,
        tsm_company=_tsm_company_config(),
        as_of=date(2026, 8, 1),
    )

    assert before_q1 == ()
    assert {row.fiscal_period for row in after_q1} == {"Q1"}
    assert {row.fiscal_period for row in after_q2} == {"Q2"}
    assert len(merged_rows) == 6
    assert {row.as_of for row in merged_rows} == {date(2026, 8, 1)}
    assert {row.fiscal_period for row in merged_rows} == {"Q2"}


def test_fundamentals_merge_tsm_ir_sec_metrics_cli_writes_valid_sec_style_csv(
    tmp_path: Path,
) -> None:
    tsm_input_path = tmp_path / "tsm_ir_metrics.csv"
    sec_companies_path = tmp_path / "sec_companies.yaml"
    metrics_path = tmp_path / "fundamental_metrics.yaml"
    sec_input_path = tmp_path / "existing_sec_fundamentals.csv"
    output_path = tmp_path / "sec_fundamentals_2026-05-02.csv"
    validation_report_path = tmp_path / "sec_fundamentals_validation.md"
    report = _parsed_report_with_source_path(tmp_path)
    write_tsm_ir_quarterly_metrics_csv(report, tsm_input_path)
    _write_tsm_sec_companies_config(sec_companies_path)
    _write_tsm_metrics_config(metrics_path)

    result = CliRunner().invoke(
        app,
        [
            "fundamentals",
            "merge-tsm-ir-sec-metrics",
            "--input-path",
            str(tsm_input_path),
            "--sec-companies-path",
            str(sec_companies_path),
            "--metrics-path",
            str(metrics_path),
            "--sec-input-path",
            str(sec_input_path),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(output_path),
            "--validation-report-path",
            str(validation_report_path),
        ],
    )

    assert result.exit_code == 0
    assert "TSMC IR 合并后 SEC 指标校验状态：PASS" in result.output
    assert output_path.exists()
    assert validation_report_path.exists()
    frame = pd.read_csv(output_path)
    assert set(frame["metric_id"]) == {
        "revenue",
        "gross_profit",
        "operating_income",
        "research_and_development",
        "net_income",
        "capex",
    }
    assert set(frame["taxonomy"]) == {"tsm-ir"}
    assert set(frame["form"]) == {"TSM-IR"}


def test_fundamentals_merge_tsm_ir_sec_metrics_cli_selects_latest_available_quarter(
    tmp_path: Path,
) -> None:
    tsm_input_path = tmp_path / "tsm_ir_metrics.csv"
    sec_companies_path = tmp_path / "sec_companies.yaml"
    metrics_path = tmp_path / "fundamental_metrics.yaml"
    sec_input_path = tmp_path / "existing_sec_fundamentals.csv"
    output_path = tmp_path / "sec_fundamentals_2026-08-01.csv"
    validation_report_path = tmp_path / "sec_fundamentals_validation.md"
    q1_report = parse_tsm_ir_management_report_text(
        text=MANAGEMENT_REPORT_TEXT,
        source_url="https://investor.tsmc.com/english/quarterly-results/2026/q1",
        fiscal_year=2026,
        fiscal_period="Q1",
        as_of=date(2026, 8, 1),
        captured_at=datetime(2026, 8, 1, 8, 30, tzinfo=UTC),
        source_path=tmp_path / "q1_management_report.txt",
        filed_date=date(2026, 4, 16),
    )
    q2_report = parse_tsm_ir_management_report_text(
        text=MANAGEMENT_REPORT_TEXT.replace("2026 First Quarter", "2026 Second Quarter"),
        source_url="https://investor.tsmc.com/english/quarterly-results/2026/q2",
        fiscal_year=2026,
        fiscal_period="Q2",
        as_of=date(2026, 8, 1),
        captured_at=datetime(2026, 8, 1, 8, 30, tzinfo=UTC),
        source_path=tmp_path / "q2_management_report.txt",
        filed_date=date(2026, 7, 16),
    )
    write_tsm_ir_quarterly_metrics_csv(q1_report, tsm_input_path)
    write_tsm_ir_quarterly_metrics_csv(q2_report, tsm_input_path)
    _write_tsm_sec_companies_config(sec_companies_path)
    _write_tsm_metrics_config(metrics_path)

    result = CliRunner().invoke(
        app,
        [
            "fundamentals",
            "merge-tsm-ir-sec-metrics",
            "--input-path",
            str(tsm_input_path),
            "--sec-companies-path",
            str(sec_companies_path),
            "--metrics-path",
            str(metrics_path),
            "--sec-input-path",
            str(sec_input_path),
            "--as-of",
            "2026-08-01",
            "--output-path",
            str(output_path),
            "--validation-report-path",
            str(validation_report_path),
        ],
    )

    assert result.exit_code == 0
    frame = pd.read_csv(output_path)
    assert set(frame["fiscal_period"]) == {"Q2"}
    assert set(frame["as_of"]) == {"2026-08-01"}


def _parsed_report_with_source_path(tmp_path: Path) -> TsmIrQuarterlyReport:
    return parse_tsm_ir_management_report_text(
        text=MANAGEMENT_REPORT_TEXT,
        source_url="https://investor.tsmc.com/english/quarterly-results/2026/q1",
        fiscal_year=2026,
        fiscal_period="Q1",
        as_of=date(2026, 5, 2),
        captured_at=datetime(2026, 4, 16, 8, 30, tzinfo=UTC),
        source_path=tmp_path / "management_report.txt",
    )


def _write_tsm_sec_companies_config(output_path: Path) -> None:
    output_path.write_text(
        """
companies:
  - ticker: TSM
    cik: "0001046179"
    company_name: Taiwan Semiconductor Manufacturing Company Limited
    expected_taxonomies:
      - ifrs-full
    sec_metric_periods:
      - quarterly
""",
        encoding="utf-8",
    )


def _write_tsm_metrics_config(output_path: Path) -> None:
    output_path.write_text(
        """
metrics:
  - metric_id: revenue
    name: Revenue
    description: TSMC IR revenue.
    preferred_periods:
      - quarterly
    concepts:
      - taxonomy: tsm-ir
        concept: management_report:revenue
        unit: TWD_millions
  - metric_id: gross_profit
    name: Gross Profit
    description: TSMC IR gross profit.
    preferred_periods:
      - quarterly
    concepts:
      - taxonomy: tsm-ir
        concept: management_report:gross_profit
        unit: TWD_millions
  - metric_id: operating_income
    name: Operating Income
    description: TSMC IR operating income.
    preferred_periods:
      - quarterly
    concepts:
      - taxonomy: tsm-ir
        concept: management_report:operating_income
        unit: TWD_millions
  - metric_id: net_income
    name: Net Income
    description: TSMC IR net income.
    preferred_periods:
      - quarterly
    concepts:
      - taxonomy: tsm-ir
        concept: management_report:net_income
        unit: TWD_millions
  - metric_id: research_and_development
    name: Research and Development
    description: TSMC IR research and development expense.
    preferred_periods:
      - quarterly
    concepts:
      - taxonomy: tsm-ir
        concept: management_report:research_and_development
        unit: TWD_millions
  - metric_id: capex
    name: Capital Expenditures
    description: TSMC IR capital expenditures.
    preferred_periods:
      - quarterly
    concepts:
      - taxonomy: tsm-ir
        concept: management_report:capex
        unit: USD_billions
""",
        encoding="utf-8",
    )


def _tsm_company_config() -> SecCompanyConfig:
    return SecCompanyConfig(
        ticker="TSM",
        cik="0001046179",
        company_name="Taiwan Semiconductor Manufacturing Company Limited",
        expected_taxonomies=["ifrs-full"],
        sec_metric_periods=["quarterly"],
    )


def _tsm_metrics_config() -> FundamentalMetricsConfig:
    return FundamentalMetricsConfig(
        metrics=[
            FundamentalMetricConfig(
                metric_id=metric_id,
                name=metric_name,
                description=f"TSMC IR {metric_name}.",
                preferred_periods=["quarterly"],
                concepts=[
                    FundamentalMetricConceptConfig(
                        taxonomy="tsm-ir",
                        concept=f"management_report:{metric_id}",
                        unit=unit,
                    )
                ],
            )
            for metric_id, metric_name, unit in [
                ("revenue", "Revenue", "TWD_millions"),
                ("gross_profit", "Gross Profit", "TWD_millions"),
                ("operating_income", "Operating Income", "TWD_millions"),
                ("net_income", "Net Income", "TWD_millions"),
                (
                    "research_and_development",
                    "Research and Development",
                    "TWD_millions",
                ),
                ("capex", "Capital Expenditures", "USD_billions"),
            ]
        ]
    )


def _sec_metric_row(
    ticker: str,
    cik: str,
    company_name: str,
    metric_id: str,
    metric_name: str,
    period_type: str,
    value: float,
    source_path: Path,
) -> SecFundamentalMetricRow:
    return SecFundamentalMetricRow(
        as_of=date(2026, 5, 2),
        ticker=ticker,
        cik=cik,
        company_name=company_name,
        metric_id=metric_id,
        metric_name=metric_name,
        period_type=period_type,  # type: ignore[arg-type]
        fiscal_year=2026,
        fiscal_period="Q1" if period_type == "quarterly" else "FY",
        end_date=date(2026, 3, 31) if period_type == "quarterly" else date(2025, 12, 31),
        filed_date=date(2026, 4, 16),
        form="20-F" if period_type == "annual" else "10-Q",
        taxonomy="us-gaap",
        concept="Revenues",
        unit="USD",
        value=value,
        accession_number=f"{ticker}-{period_type}-{metric_id}",
        source_path=source_path,
    )
