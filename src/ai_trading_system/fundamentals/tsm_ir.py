from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from enum import StrEnum
from importlib import import_module
from numbers import Real
from pathlib import Path
from typing import Any, Literal, Protocol, cast
from urllib.parse import urljoin, urlparse

import pandas as pd

from ai_trading_system.config import SecCompaniesConfig, SecCompanyConfig
from ai_trading_system.fundamentals.sec_metrics import SecFundamentalMetricRow

TSM_IR_SOURCE_ID = "tsm_investor_relations_quarterly_results"
TSM_IR_PROVIDER_NAME = "TSMC Investor Relations"
TSM_IR_OFFICIAL_HOST = "investor.tsmc.com"
TsmIrResourceType = Literal[
    "quarterly_page",
    "management_report_text",
    "financial_statement_text",
]
_TSM_TICKER = "TSM"
_TSM_IR_SEC_METRIC_IDS = frozenset(
    {
        "revenue",
        "gross_profit",
        "operating_income",
        "net_income",
        "research_and_development",
        "capex",
    }
)


class TsmIrIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class TsmIrIssue:
    severity: TsmIrIssueSeverity
    code: str
    message: str
    metric_id: str | None = None
    source_url: str | None = None
    source_path: Path | None = None


@dataclass(frozen=True)
class TsmIrQuarterlyResource:
    url: str
    resource_type: TsmIrResourceType
    source_path: Path | None = None


class TsmIrQuarterlyProvider(Protocol):
    def download_text(self, url: str) -> str:
        """Download official TSMC IR quarterly page or extracted Management Report text."""


class TsmIrHttpProvider:
    def __init__(
        self,
        requests_module: Any | None = None,
        timeout: float = 30,
        user_agent: str = "ai-trading-system tsm-ir/0.1",
    ) -> None:
        if timeout <= 0:
            raise ValueError("TSMC IR HTTP timeout must be positive")
        normalized_user_agent = user_agent.strip()
        if not normalized_user_agent:
            raise ValueError("TSMC IR HTTP User-Agent must not be empty")
        self._requests_module = requests_module
        self.timeout = timeout
        self.user_agent = normalized_user_agent

    def download_text(self, url: str) -> str:
        if not is_official_tsm_ir_url(url):
            raise ValueError(
                "TSMC IR HTTP provider only downloads official "
                "investor.tsmc.com HTTPS URLs."
            )
        url_binary_reason = _binary_url_reason(url)
        if url_binary_reason is not None:
            raise ValueError(_extracted_text_required_message(url, url_binary_reason))

        requests = self._requests_module or cast(Any, import_module("requests"))
        response = requests.get(
            url,
            headers={
                "User-Agent": self.user_agent,
                "Accept": (
                    "text/html,text/plain,application/xhtml+xml,"
                    "application/xml;q=0.9,*/*;q=0.1"
                ),
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        headers = getattr(response, "headers", {}) or {}
        content_type = _response_content_type(headers)
        content_type_reason = _binary_content_type_reason(content_type)
        if content_type_reason is not None:
            raise ValueError(_extracted_text_required_message(url, content_type_reason))
        if content_type and not _is_text_content_type(content_type):
            raise ValueError(
                _extracted_text_required_message(
                    url,
                    f"response Content-Type is non-text ({content_type})",
                )
            )
        return _response_text(url, response, content_type)


@dataclass(frozen=True)
class TsmIrQuarterlyMetricRow:
    as_of: date
    ticker: str
    fiscal_year: int
    fiscal_period: str
    end_date: date
    filed_date: date | None
    captured_at: datetime
    metric_id: str
    metric_name: str
    period_type: Literal["quarterly"]
    unit: str
    value: float
    source_url: str
    source_path: Path | None
    source_id: str
    checksum_sha256: str


@dataclass(frozen=True)
class TsmIrQuarterlyImportManifestEntry:
    fiscal_year: int
    fiscal_period: str
    source_url: str
    input_path: Path
    filed_date: date | None = None


@dataclass(frozen=True)
class TsmIrQuarterlyReport:
    as_of: date
    ticker: str
    fiscal_year: int
    fiscal_period: str
    end_date: date
    captured_at: datetime
    source_url: str
    source_path: Path | None
    source_id: str
    provider: str
    checksum_sha256: str
    rows: tuple[TsmIrQuarterlyMetricRow, ...]
    issues: tuple[TsmIrIssue, ...] = field(default_factory=tuple)

    @property
    def row_count(self) -> int:
        return len(self.rows)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == TsmIrIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == TsmIrIssueSeverity.WARNING)

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


@dataclass(frozen=True)
class TsmIrPdfTextExtractionReport:
    provider: str
    source_url: str
    input_path: Path
    output_path: Path
    extracted_at: datetime
    page_count: int
    character_count: int
    input_checksum_sha256: str
    text_checksum_sha256: str
    issues: tuple[TsmIrIssue, ...] = field(default_factory=tuple)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == TsmIrIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == TsmIrIssueSeverity.WARNING)

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"

    @property
    def char_count(self) -> int:
        return self.character_count

    @property
    def checksum_sha256(self) -> str:
        return self.text_checksum_sha256


@dataclass(frozen=True)
class TsmIrSecMetricConversionReport:
    rows: tuple[SecFundamentalMetricRow, ...]
    issues: tuple[TsmIrIssue, ...] = field(default_factory=tuple)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == TsmIrIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == TsmIrIssueSeverity.WARNING)

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


@dataclass(frozen=True)
class TsmIrQuarterlyBatchImportReport:
    as_of: date
    captured_at: datetime
    manifest_path: Path
    output_path: Path | None
    reports: tuple[TsmIrQuarterlyReport, ...]
    issues: tuple[TsmIrIssue, ...] = field(default_factory=tuple)

    @property
    def entry_count(self) -> int:
        return len(self.reports)

    @property
    def row_count(self) -> int:
        return sum(report.row_count for report in self.reports)

    @property
    def all_issues(self) -> tuple[TsmIrIssue, ...]:
        issues = list(self.issues)
        for report in self.reports:
            issues.extend(report.issues)
        return tuple(issues)

    @property
    def error_count(self) -> int:
        return sum(
            1 for issue in self.all_issues if issue.severity == TsmIrIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1 for issue in self.all_issues if issue.severity == TsmIrIssueSeverity.WARNING
        )

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


@dataclass(frozen=True)
class _MetricSpec:
    metric_id: str
    metric_name: str
    label_patterns: tuple[str, ...]
    unit_kind: Literal["money", "percent", "capex"]
    required: bool = True


_METRIC_SPECS = (
    _MetricSpec("revenue", "Net Revenue", (r"Net\s+Revenue",), "money"),
    _MetricSpec("gross_profit", "Gross Profit", (r"Gross\s+Profit",), "money"),
    _MetricSpec("gross_margin", "Gross Margin", (r"Gross\s+Margin",), "percent"),
    _MetricSpec("operating_income", "Operating Income", (r"Operating\s+Income",), "money"),
    _MetricSpec(
        "research_and_development",
        "Research and Development",
        (r"Research\s*(?:&|and)\s*Development",),
        "money",
    ),
    _MetricSpec("operating_margin", "Operating Margin", (r"Operating\s+Margin",), "percent"),
    _MetricSpec(
        "net_income",
        "Net Income Attributable to Shareholders of the Parent",
        (
            r"Net\s+Income\s+Attributable\s+to\s+Shareholders\s+of\s+the\s+Parent",
            r"Net\s+Income\s+Attributable\s+to\s+Shareholders",
        ),
        "money",
    ),
    _MetricSpec("net_margin", "Net Profit Margin", (r"Net\s+Profit\s+Margin",), "percent"),
    _MetricSpec(
        "capex",
        "Capital Expenditures",
        (r"Capital\s+Expenditures", r"CapEx"),
        "capex",
        False,
    ),
)
_REQUIRED_METRIC_IDS = frozenset(spec.metric_id for spec in _METRIC_SPECS if spec.required)
_CSV_COLUMNS = (
    "as_of",
    "ticker",
    "fiscal_year",
    "fiscal_period",
    "end_date",
    "filed_date",
    "captured_at",
    "metric_id",
    "metric_name",
    "period_type",
    "unit",
    "value",
    "source_url",
    "source_path",
    "source_id",
    "checksum_sha256",
)
_TSM_IR_IMPORT_MANIFEST_COLUMNS = (
    "fiscal_year",
    "fiscal_period",
    "source_url",
    "input_path",
)


def build_tsm_ir_quarterly_report(
    provider: TsmIrQuarterlyProvider,
    source_url: str,
    resources: tuple[TsmIrQuarterlyResource, ...] = (),
    fiscal_year: int | None = None,
    fiscal_period: str | None = None,
    as_of: date | None = None,
    captured_at: datetime | None = None,
) -> TsmIrQuarterlyReport:
    captured_at = captured_at or datetime.now(tz=UTC)
    as_of = as_of or captured_at.date()
    source_issues = _source_url_issues(source_url)
    selected_resources = resources or tuple(
        _resources_from_page(source_url, provider, source_issues)
    )
    management_resource = _select_management_resource(selected_resources, source_issues)

    if management_resource is None:
        year, period = _period_from_url(source_url)
        return _empty_report(
            as_of=as_of,
            fiscal_year=fiscal_year or year or 0,
            fiscal_period=fiscal_period or period or "",
            source_url=source_url,
            source_path=None,
            captured_at=captured_at,
            checksum="",
            issues=source_issues,
        )

    source_issues.extend(_source_url_issues(management_resource.url))
    try:
        text = provider.download_text(management_resource.url)
    except Exception as exc:
        source_issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_source_download_failed",
                source_url=management_resource.url,
                source_path=management_resource.source_path,
                message=f"TSMC IR source resource could not be downloaded: {exc}",
            )
        )
        year, period = _period_from_url(source_url)
        return _empty_report(
            as_of=as_of,
            fiscal_year=fiscal_year or year or 0,
            fiscal_period=fiscal_period or period or "",
            source_url=management_resource.url,
            source_path=management_resource.source_path,
            captured_at=captured_at,
            checksum="",
            issues=source_issues,
        )

    year, period = _period_from_url(source_url)
    report = parse_tsm_ir_management_report_text(
        text=text,
        source_url=management_resource.url,
        fiscal_year=fiscal_year or year or 0,
        fiscal_period=fiscal_period or period or "",
        as_of=as_of,
        captured_at=captured_at,
        source_path=management_resource.source_path,
        filed_date=captured_at.date(),
        initial_issues=tuple(source_issues),
    )
    return report


def select_tsm_ir_management_report_resource(
    provider: TsmIrQuarterlyProvider,
    source_url: str,
) -> TsmIrQuarterlyResource:
    issues = _source_url_issues(source_url)
    resources = tuple(_resources_from_page(source_url, provider, issues))
    management_resource = _select_management_resource(resources, issues)
    if issues or management_resource is None:
        issue_summary = "; ".join(f"{issue.code}: {issue.message}" for issue in issues)
        raise ValueError(
            issue_summary
            or "TSMC IR quarterly page did not expose a Management Report resource."
        )
    return management_resource


def extract_tsm_ir_pdf_text(
    input_path: Path,
    source_url: str,
    output_path: Path,
    extracted_at: datetime | None = None,
) -> TsmIrPdfTextExtractionReport:
    """Extract local official TSMC IR Management Report PDF text with audit metadata."""

    extracted_at = extracted_at or datetime.now(tz=UTC)
    input_path = Path(input_path)
    output_path = Path(output_path)
    issues = _source_url_issues(source_url)
    if issues:
        return _pdf_text_extraction_report(
            source_url=source_url,
            input_path=input_path,
            output_path=output_path,
            extracted_at=extracted_at,
            issues=issues,
        )
    if not input_path.exists():
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_pdf_input_missing",
                source_url=source_url,
                source_path=input_path,
                message=f"TSMC IR Management Report PDF path does not exist: {input_path}",
            )
        )
        return _pdf_text_extraction_report(
            source_url=source_url,
            input_path=input_path,
            output_path=output_path,
            extracted_at=extracted_at,
            issues=issues,
        )
    if not input_path.is_file():
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_pdf_input_not_file",
                source_url=source_url,
                source_path=input_path,
                message=f"TSMC IR Management Report PDF path is not a file: {input_path}",
            )
        )
        return _pdf_text_extraction_report(
            source_url=source_url,
            input_path=input_path,
            output_path=output_path,
            extracted_at=extracted_at,
            issues=issues,
        )

    input_checksum = _sha256_file(input_path)
    try:
        pypdf = cast(Any, import_module("pypdf"))
    except ImportError as exc:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_pdf_dependency_missing",
                source_url=source_url,
                source_path=input_path,
                message=(
                    "TSMC IR PDF text extraction requires optional dependency pypdf; "
                    f"dynamic import failed: {exc}"
                ),
            )
        )
        return _pdf_text_extraction_report(
            source_url=source_url,
            input_path=input_path,
            output_path=output_path,
            extracted_at=extracted_at,
            input_checksum_sha256=input_checksum,
            issues=issues,
        )

    pdf_reader = getattr(pypdf, "PdfReader", None)
    if pdf_reader is None:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_pdf_reader_missing",
                source_url=source_url,
                source_path=input_path,
                message="Dynamic pypdf import did not expose PdfReader.",
            )
        )
        return _pdf_text_extraction_report(
            source_url=source_url,
            input_path=input_path,
            output_path=output_path,
            extracted_at=extracted_at,
            input_checksum_sha256=input_checksum,
            issues=issues,
        )

    try:
        reader = pdf_reader(input_path)
        pages = tuple(reader.pages)
    except Exception as exc:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_pdf_read_failed",
                source_url=source_url,
                source_path=input_path,
                message=f"TSMC IR Management Report PDF could not be read: {exc}",
            )
        )
        return _pdf_text_extraction_report(
            source_url=source_url,
            input_path=input_path,
            output_path=output_path,
            extracted_at=extracted_at,
            input_checksum_sha256=input_checksum,
            issues=issues,
        )

    page_texts: list[str] = []
    for page_number, page in enumerate(pages, start=1):
        try:
            page_text = page.extract_text()
        except Exception as exc:
            issues.append(
                TsmIrIssue(
                    severity=TsmIrIssueSeverity.ERROR,
                    code="tsm_ir_pdf_page_extract_failed",
                    source_url=source_url,
                    source_path=input_path,
                    message=(
                        "TSMC IR Management Report PDF page "
                        f"{page_number} text extraction failed: {exc}"
                    ),
                )
            )
            return _pdf_text_extraction_report(
                source_url=source_url,
                input_path=input_path,
                output_path=output_path,
                extracted_at=extracted_at,
                page_count=len(pages),
                input_checksum_sha256=input_checksum,
                issues=issues,
            )
        if page_text is None:
            page_text = ""
        if not isinstance(page_text, str):
            issues.append(
                TsmIrIssue(
                    severity=TsmIrIssueSeverity.ERROR,
                    code="tsm_ir_pdf_page_text_invalid",
                    source_url=source_url,
                    source_path=input_path,
                    message=(
                        "TSMC IR Management Report PDF page "
                        f"{page_number} returned non-text extraction output."
                    ),
                )
            )
            return _pdf_text_extraction_report(
                source_url=source_url,
                input_path=input_path,
                output_path=output_path,
                extracted_at=extracted_at,
                page_count=len(pages),
                input_checksum_sha256=input_checksum,
                issues=issues,
            )
        page_texts.append(page_text.strip())

    extracted_text = "\n\n".join(page_texts).strip()
    if not extracted_text:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_pdf_text_empty",
                source_url=source_url,
                source_path=input_path,
                message=(
                    "TSMC IR Management Report PDF did not yield any extractable text; "
                    "需要 OCR 或人工抽取，不能生成伪文本。"
                ),
            )
        )
        return _pdf_text_extraction_report(
            source_url=source_url,
            input_path=input_path,
            output_path=output_path,
            extracted_at=extracted_at,
            page_count=len(pages),
            input_checksum_sha256=input_checksum,
            issues=issues,
        )

    text_checksum = _sha256_text(extracted_text)
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(extracted_text, encoding="utf-8")
    except OSError as exc:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_pdf_text_write_failed",
                source_url=source_url,
                source_path=input_path,
                message=f"TSMC IR Management Report extracted text could not be written: {exc}",
            )
        )

    return _pdf_text_extraction_report(
        source_url=source_url,
        input_path=input_path,
        output_path=output_path,
        extracted_at=extracted_at,
        page_count=len(pages),
        character_count=len(extracted_text),
        input_checksum_sha256=input_checksum,
        text_checksum_sha256=text_checksum,
        issues=issues,
    )


def extract_tsm_ir_management_report_pdf_text(
    input_path: Path,
    output_path: Path,
    source_url: str,
    extracted_at: datetime | None = None,
) -> TsmIrPdfTextExtractionReport:
    return extract_tsm_ir_pdf_text(
        input_path=input_path,
        source_url=source_url,
        output_path=output_path,
        extracted_at=extracted_at,
    )


def parse_tsm_ir_management_report_text(
    text: str,
    source_url: str,
    fiscal_year: int,
    fiscal_period: str,
    as_of: date,
    captured_at: datetime,
    source_path: Path | None = None,
    filed_date: date | None = None,
    initial_issues: tuple[TsmIrIssue, ...] = (),
) -> TsmIrQuarterlyReport:
    issues = [*initial_issues, *_source_url_issues(source_url)]
    normalized_period = fiscal_period.upper()
    end_date = _quarter_end_date(fiscal_year, normalized_period)
    checksum = _sha256_text(text)
    money_unit = _money_unit(text)
    values: dict[str, tuple[float, str]] = {}

    if end_date > as_of:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_future_date",
                source_url=source_url,
                message=(
                    f"TSMC IR quarter end date {end_date.isoformat()} is after as_of "
                    f"{as_of.isoformat()}."
                ),
            )
        )
    if captured_at.date() > as_of:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_future_date",
                source_url=source_url,
                message=(
                    f"TSMC IR captured_at date {captured_at.date().isoformat()} is after as_of "
                    f"{as_of.isoformat()}."
                ),
            )
        )
    if filed_date is not None and filed_date > as_of:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_future_date",
                source_url=source_url,
                message=(
                    f"TSMC IR filed_date {filed_date.isoformat()} is after as_of "
                    f"{as_of.isoformat()}."
                ),
            )
        )

    for spec in _METRIC_SPECS:
        parsed = _parse_metric_value(text, spec, money_unit)
        if parsed is None:
            severity = (
                TsmIrIssueSeverity.ERROR
                if spec.metric_id in _REQUIRED_METRIC_IDS
                else TsmIrIssueSeverity.WARNING
            )
            issues.append(
                TsmIrIssue(
                    severity=severity,
                    code="tsm_ir_metric_missing",
                    metric_id=spec.metric_id,
                    source_url=source_url,
                    source_path=source_path,
                    message=(
                        "TSMC IR management report did not expose required metric "
                        f"{spec.metric_id}."
                    ),
                )
            )
            continue
        values[spec.metric_id] = parsed

    if not values:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_parse_failed",
                source_url=source_url,
                source_path=source_path,
                message="No TSMC IR quarterly metrics could be parsed from the source text.",
            )
        )

    rows = tuple(
        TsmIrQuarterlyMetricRow(
            as_of=as_of,
            ticker="TSM",
            fiscal_year=fiscal_year,
            fiscal_period=normalized_period,
            end_date=end_date,
            filed_date=filed_date or captured_at.date(),
            captured_at=captured_at,
            metric_id=spec.metric_id,
            metric_name=spec.metric_name,
            period_type="quarterly",
            unit=values[spec.metric_id][1],
            value=values[spec.metric_id][0],
            source_url=source_url,
            source_path=source_path,
            source_id=TSM_IR_SOURCE_ID,
            checksum_sha256=checksum,
        )
        for spec in _METRIC_SPECS
        if spec.metric_id in values
    )
    return TsmIrQuarterlyReport(
        as_of=as_of,
        ticker="TSM",
        fiscal_year=fiscal_year,
        fiscal_period=normalized_period,
        end_date=end_date,
        captured_at=captured_at,
        source_url=source_url,
        source_path=source_path,
        source_id=TSM_IR_SOURCE_ID,
        provider=TSM_IR_PROVIDER_NAME,
        checksum_sha256=checksum,
        rows=rows,
        issues=tuple(issues),
    )


def write_tsm_ir_quarterly_metrics_csv(
    report: TsmIrQuarterlyReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_frame = pd.DataFrame([_row_record(row) for row in report.rows], columns=list(_CSV_COLUMNS))
    if output_path.exists():
        existing = pd.read_csv(output_path)
        if "as_of" not in existing.columns:
            raise ValueError(f"existing TSM IR fundamentals file is missing as_of: {output_path}")
        key = (
            existing["as_of"].astype(str).eq(report.as_of.isoformat())
            & existing["ticker"].astype(str).str.upper().eq(report.ticker)
            & existing["fiscal_year"].astype(str).eq(str(report.fiscal_year))
            & existing["fiscal_period"].astype(str).str.upper().eq(report.fiscal_period)
        )
        existing = existing.loc[~key]
        new_frame = pd.concat([existing, new_frame], ignore_index=True)
    new_frame.to_csv(output_path, index=False)
    return output_path


def load_tsm_ir_quarterly_metric_rows_csv(
    input_path: Path,
) -> tuple[TsmIrQuarterlyMetricRow, ...]:
    frame = pd.read_csv(input_path)
    missing_columns = sorted(set(_CSV_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise ValueError(
            f"TSMC IR 季度指标 CSV 缺少字段：{', '.join(missing_columns)}。"
        )
    records = [
        {str(key): value for key, value in record.items()}
        for record in frame.to_dict(orient="records")
    ]
    return tuple(_tsm_ir_metric_row_from_record(record) for record in records)


def load_tsm_ir_quarterly_import_manifest_csv(
    manifest_path: Path,
) -> tuple[TsmIrQuarterlyImportManifestEntry, ...]:
    manifest_path = Path(manifest_path)
    frame = pd.read_csv(manifest_path)
    missing_columns = sorted(set(_TSM_IR_IMPORT_MANIFEST_COLUMNS) - set(frame.columns))
    if missing_columns:
        raise ValueError(
            "TSMC IR 批量导入 manifest 缺少字段："
            f"{', '.join(missing_columns)}。"
        )

    entries: list[TsmIrQuarterlyImportManifestEntry] = []
    records = [
        {str(key): value for key, value in record.items()}
        for record in frame.to_dict(orient="records")
    ]
    for row_number, record in enumerate(records, start=2):
        if _manifest_row_is_empty(record):
            continue
        fiscal_year = _required_manifest_int_value(record, "fiscal_year", row_number)
        fiscal_period = _required_manifest_string_value(
            record,
            "fiscal_period",
            row_number,
        ).upper()
        if not re.fullmatch(r"Q[1-4]", fiscal_period):
            raise ValueError(
                "TSMC IR 批量导入 manifest fiscal_period 必须是 Q1-Q4："
                f"第 {row_number} 行 fiscal_period={fiscal_period}。"
            )
        source_url = _required_manifest_string_value(record, "source_url", row_number)
        input_path_text = _required_manifest_string_value(record, "input_path", row_number)
        input_path = _manifest_input_path(manifest_path, input_path_text)
        filed_date = _manifest_optional_date_value(record, "filed_date", row_number)
        entries.append(
            TsmIrQuarterlyImportManifestEntry(
                fiscal_year=fiscal_year,
                fiscal_period=fiscal_period,
                source_url=source_url,
                input_path=input_path,
                filed_date=filed_date,
            )
        )
    return tuple(entries)


def build_tsm_ir_quarterly_batch_import_report(
    manifest_path: Path,
    as_of: date,
    captured_at: datetime,
    output_path: Path | None = None,
) -> TsmIrQuarterlyBatchImportReport:
    manifest_path = Path(manifest_path)
    output_path = Path(output_path) if output_path is not None else None
    issues: list[TsmIrIssue] = []
    try:
        entries = load_tsm_ir_quarterly_import_manifest_csv(manifest_path)
    except (OSError, ValueError, pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_batch_manifest_invalid",
                source_path=manifest_path,
                message=f"TSMC IR 批量导入 manifest 无法读取或校验失败：{exc}",
            )
        )
        return TsmIrQuarterlyBatchImportReport(
            as_of=as_of,
            captured_at=captured_at,
            manifest_path=manifest_path,
            output_path=output_path,
            reports=(),
            issues=tuple(issues),
        )

    if not entries:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_batch_manifest_empty",
                source_path=manifest_path,
                message="TSMC IR 批量导入 manifest 没有任何可导入季度。",
            )
        )

    seen_quarters: set[tuple[int, str]] = set()
    reports: list[TsmIrQuarterlyReport] = []
    for entry in entries:
        quarter_key = (entry.fiscal_year, entry.fiscal_period)
        if quarter_key in seen_quarters:
            issues.append(
                TsmIrIssue(
                    severity=TsmIrIssueSeverity.ERROR,
                    code="tsm_ir_batch_duplicate_quarter",
                    source_url=entry.source_url,
                    source_path=entry.input_path,
                    message=(
                        "TSMC IR 批量导入 manifest 包含重复季度："
                        f"{entry.fiscal_year} {entry.fiscal_period}。"
                    ),
                )
            )
        seen_quarters.add(quarter_key)

        if not entry.input_path.exists():
            reports.append(
                _empty_report(
                    as_of=as_of,
                    fiscal_year=entry.fiscal_year,
                    fiscal_period=entry.fiscal_period,
                    source_url=entry.source_url,
                    source_path=entry.input_path,
                    captured_at=captured_at,
                    checksum="",
                    issues=[
                        TsmIrIssue(
                            severity=TsmIrIssueSeverity.ERROR,
                            code="tsm_ir_batch_input_missing",
                            source_url=entry.source_url,
                            source_path=entry.input_path,
                            message=(
                                "TSMC IR 批量导入 manifest 指向的 Management "
                                f"Report 文本不存在：{entry.input_path}"
                            ),
                        )
                    ],
                )
            )
            continue

        if not entry.input_path.is_file():
            reports.append(
                _empty_report(
                    as_of=as_of,
                    fiscal_year=entry.fiscal_year,
                    fiscal_period=entry.fiscal_period,
                    source_url=entry.source_url,
                    source_path=entry.input_path,
                    captured_at=captured_at,
                    checksum="",
                    issues=[
                        TsmIrIssue(
                            severity=TsmIrIssueSeverity.ERROR,
                            code="tsm_ir_batch_input_not_file",
                            source_url=entry.source_url,
                            source_path=entry.input_path,
                            message=(
                                "TSMC IR 批量导入 manifest 指向的 Management "
                                f"Report 文本不是文件：{entry.input_path}"
                            ),
                        )
                    ],
                )
            )
            continue

        try:
            text = entry.input_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            reports.append(
                _empty_report(
                    as_of=as_of,
                    fiscal_year=entry.fiscal_year,
                    fiscal_period=entry.fiscal_period,
                    source_url=entry.source_url,
                    source_path=entry.input_path,
                    captured_at=captured_at,
                    checksum="",
                    issues=[
                        TsmIrIssue(
                            severity=TsmIrIssueSeverity.ERROR,
                            code="tsm_ir_batch_input_read_failed",
                            source_url=entry.source_url,
                            source_path=entry.input_path,
                            message=(
                                "TSMC IR 批量导入 Management Report 文本读取失败："
                                f"{exc}"
                            ),
                        )
                    ],
                )
            )
            continue

        reports.append(
            parse_tsm_ir_management_report_text(
                text=text,
                source_url=entry.source_url,
                fiscal_year=entry.fiscal_year,
                fiscal_period=entry.fiscal_period,
                as_of=as_of,
                captured_at=captured_at,
                source_path=entry.input_path,
                filed_date=entry.filed_date,
            )
        )

    return TsmIrQuarterlyBatchImportReport(
        as_of=as_of,
        captured_at=captured_at,
        manifest_path=manifest_path,
        output_path=output_path,
        reports=tuple(reports),
        issues=tuple(issues),
    )


def tsm_ir_quarterly_metric_rows_to_frame(
    rows: tuple[TsmIrQuarterlyMetricRow, ...],
) -> pd.DataFrame:
    return pd.DataFrame([_row_record(row) for row in rows], columns=list(_CSV_COLUMNS))


def build_tsm_ir_sec_metric_conversion_report(
    rows: tuple[TsmIrQuarterlyMetricRow, ...],
    tsm_company: SecCompaniesConfig | SecCompanyConfig,
) -> TsmIrSecMetricConversionReport:
    issues: list[TsmIrIssue] = []
    company = _resolve_tsm_company(tsm_company, issues)
    if company is None:
        return TsmIrSecMetricConversionReport(rows=(), issues=tuple(issues))

    converted_rows: list[SecFundamentalMetricRow] = []
    seen_keys: set[tuple[date, str, str, str]] = set()
    for row in rows:
        if row.metric_id not in _TSM_IR_SEC_METRIC_IDS:
            continue
        if not _tsm_ir_row_is_convertible(row, issues):
            continue
        if row.source_path is None:
            issues.append(
                TsmIrIssue(
                    severity=TsmIrIssueSeverity.ERROR,
                    code="tsm_ir_sec_metric_source_path_missing",
                    metric_id=row.metric_id,
                    source_url=row.source_url,
                    message=(
                        "TSMC IR row cannot be converted to SEC metric row without "
                        "source_path audit evidence."
                    ),
                )
            )
            continue

        key = (row.as_of, row.ticker.upper(), row.metric_id, row.period_type)
        if key in seen_keys:
            issues.append(
                TsmIrIssue(
                    severity=TsmIrIssueSeverity.ERROR,
                    code="tsm_ir_sec_metric_duplicate_key",
                    metric_id=row.metric_id,
                    source_url=row.source_url,
                    source_path=row.source_path,
                    message=(
                        "TSMC IR SEC conversion received duplicate "
                        "as_of/ticker/metric_id/period_type rows."
                    ),
                )
            )
            continue
        seen_keys.add(key)

        converted_rows.append(_tsm_ir_row_to_sec_metric_row(row, company))

    return TsmIrSecMetricConversionReport(rows=tuple(converted_rows), issues=tuple(issues))


def convert_tsm_ir_quarterly_metric_rows_to_sec_metric_rows(
    rows: tuple[TsmIrQuarterlyMetricRow, ...],
    tsm_company: SecCompaniesConfig | SecCompanyConfig,
) -> tuple[SecFundamentalMetricRow, ...]:
    report = build_tsm_ir_sec_metric_conversion_report(rows, tsm_company)
    if not report.passed:
        issue_summary = "; ".join(f"{issue.code}: {issue.message}" for issue in report.issues)
        raise ValueError(f"TSMC IR SEC metric conversion failed: {issue_summary}")
    return report.rows


def merge_tsm_ir_quarterly_rows_into_sec_metrics(
    existing_rows: tuple[SecFundamentalMetricRow, ...],
    tsm_rows: tuple[TsmIrQuarterlyMetricRow, ...],
    tsm_company: SecCompaniesConfig | SecCompanyConfig,
) -> tuple[SecFundamentalMetricRow, ...]:
    converted_rows = convert_tsm_ir_quarterly_metric_rows_to_sec_metric_rows(
        tsm_rows,
        tsm_company,
    )
    converted_by_key = {_sec_metric_merge_key(row): row for row in converted_rows}
    remaining_converted_keys = set(converted_by_key)
    merged_rows: list[SecFundamentalMetricRow] = []

    for row in existing_rows:
        key = _sec_metric_merge_key(row)
        if (
            row.ticker.upper() == _TSM_TICKER
            and row.period_type == "quarterly"
            and key in converted_by_key
        ):
            merged_rows.append(converted_by_key[key])
            remaining_converted_keys.remove(key)
            continue
        merged_rows.append(row)

    for row in converted_rows:
        key = _sec_metric_merge_key(row)
        if key in remaining_converted_keys:
            merged_rows.append(row)
            remaining_converted_keys.remove(key)

    return tuple(merged_rows)


def select_tsm_ir_quarterly_metric_rows_as_of(
    rows: tuple[TsmIrQuarterlyMetricRow, ...],
    as_of: date,
) -> tuple[TsmIrQuarterlyMetricRow, ...]:
    latest_by_metric: dict[tuple[str, str, str], TsmIrQuarterlyMetricRow] = {}
    for row in rows:
        if row.ticker.upper() != _TSM_TICKER:
            continue
        available_date = row.filed_date or row.captured_at.date()
        if available_date > as_of or row.end_date > as_of:
            continue
        key = (row.ticker.upper(), row.metric_id, row.period_type)
        current = latest_by_metric.get(key)
        if current is None or _tsm_ir_historical_selection_key(row) > (
            _tsm_ir_historical_selection_key(current)
        ):
            latest_by_metric[key] = row
    return tuple(
        row
        for _, row in sorted(
            latest_by_metric.items(),
            key=lambda item: (
                item[1].ticker,
                item[1].fiscal_year,
                item[1].fiscal_period,
                item[1].metric_id,
            ),
        )
    )


def merge_tsm_ir_quarterly_rows_into_sec_metrics_as_of(
    existing_rows: tuple[SecFundamentalMetricRow, ...],
    tsm_rows: tuple[TsmIrQuarterlyMetricRow, ...],
    tsm_company: SecCompaniesConfig | SecCompanyConfig,
    as_of: date,
) -> tuple[SecFundamentalMetricRow, ...]:
    selected_rows = select_tsm_ir_quarterly_metric_rows_as_of(tsm_rows, as_of)
    rows_as_of = tuple(replace(row, as_of=as_of) for row in selected_rows)
    return merge_tsm_ir_quarterly_rows_into_sec_metrics(
        existing_rows=existing_rows,
        tsm_rows=rows_as_of,
        tsm_company=tsm_company,
    )


def render_tsm_ir_quarterly_report(report: TsmIrQuarterlyReport) -> str:
    lines = [
        "# TSMC IR 季度基本面摘要",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- Ticker：{report.ticker}",
        f"- 财年/财期：{report.fiscal_year} {report.fiscal_period}",
        f"- 截止日：{report.end_date.isoformat()}",
        f"- Provider：{report.provider}",
        f"- Source ID：{report.source_id}",
        f"- Source URL：{report.source_url}",
        f"- Source Path：`{report.source_path or ''}`",
        f"- Filed Date：{_report_filed_date(report)}",
        f"- Captured At：{report.captured_at.isoformat()}",
        f"- checksum_sha256：{report.checksum_sha256}",
        f"- 指标行数：{report.row_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 指标摘要",
        "",
    ]

    if not report.rows:
        lines.append("未抽取到任何 TSMC IR 季度基本面指标。")
    else:
        lines.extend(
            [
                "| Ticker | 指标 | 财年 | 财期 | 截止日 | 单位 | 数值 | Source |",
                "|---|---|---:|---|---|---|---:|---|",
            ]
        )
        for row in report.rows:
            lines.append(
                "| "
                f"{row.ticker} | "
                f"{_escape_markdown_table(row.metric_name)} | "
                f"{row.fiscal_year} | "
                f"{row.fiscal_period} | "
                f"{row.end_date.isoformat()} | "
                f"{row.unit} | "
                f"{row.value:.2f} | "
                f"{_escape_markdown_table(row.source_id)} |"
            )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | 指标 | Source URL | Source Path | 说明 |",
                "|---|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.metric_id or ''} | "
                f"{_escape_markdown_table(issue.source_url or '')} | "
                f"{_escape_markdown_table(str(issue.source_path or ''))} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 本报告只解析 TSMC Investor Relations 官方季度资料中的 Management Report 文本。",
            "- 页面和资源 URL 必须来自 `investor.tsmc.com`；非官方域名会被标为错误。",
            "- CapEx 仅在来源文本明确披露时记录，缺失时只产生警告，不做推断。",
            "- 金额单位保留来源表格或 CapEx 段落的披露尺度，"
            "例如 `TWD_millions` 或 `USD_billions`。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_tsm_ir_quarterly_report(report: TsmIrQuarterlyReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_tsm_ir_quarterly_report(report), encoding="utf-8")
    return output_path


def write_tsm_ir_quarterly_batch_metrics_csv(
    report: TsmIrQuarterlyBatchImportReport,
    output_path: Path,
) -> Path:
    if not report.passed:
        raise ValueError("TSMC IR 批量导入报告未通过，不能写入季度指标 CSV。")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    replacement_keys = {
        (
            quarter_report.as_of.isoformat(),
            quarter_report.ticker.upper(),
            str(quarter_report.fiscal_year),
            quarter_report.fiscal_period.upper(),
        )
        for quarter_report in report.reports
    }
    new_rows = tuple(row for quarter_report in report.reports for row in quarter_report.rows)
    if not new_rows:
        raise ValueError("TSMC IR 批量导入报告没有可写入的季度指标行。")
    new_frame = pd.DataFrame([_row_record(row) for row in new_rows], columns=list(_CSV_COLUMNS))
    if output_path.exists():
        existing = pd.read_csv(output_path)
        missing_columns = sorted(set(_CSV_COLUMNS) - set(existing.columns))
        if missing_columns:
            raise ValueError(
                "existing TSM IR fundamentals file is missing columns: "
                f"{', '.join(missing_columns)}"
            )
        existing_keys = zip(
            existing["as_of"].astype(str),
            existing["ticker"].astype(str).str.upper(),
            existing["fiscal_year"].astype(str),
            existing["fiscal_period"].astype(str).str.upper(),
            strict=True,
        )
        keep_mask = [key not in replacement_keys for key in existing_keys]
        existing = existing.loc[keep_mask]
        new_frame = pd.concat([existing, new_frame], ignore_index=True)
    new_frame.to_csv(output_path, index=False)
    return output_path


def render_tsm_ir_quarterly_batch_import_report(
    report: TsmIrQuarterlyBatchImportReport,
) -> str:
    lines = [
        "# TSMC IR 批量季度基本面导入报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- Captured At：{report.captured_at.isoformat()}",
        f"- Manifest Path：`{report.manifest_path}`",
        f"- CSV Output Path：`{report.output_path or ''}`",
        f"- 季度条目数：{report.entry_count}",
        f"- 指标行数：{report.row_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 季度摘要",
        "",
    ]

    if not report.reports:
        lines.append("未解析任何 TSMC IR 季度条目。")
    else:
        lines.extend(
            [
                (
                    "| 财年 | 财期 | 状态 | Filed Date | Source URL | Source Path | "
                    "指标行数 | checksum_sha256 |"
                ),
                "|---:|---|---|---|---|---|---:|---|",
            ]
        )
        for quarter_report in report.reports:
            lines.append(
                "| "
                f"{quarter_report.fiscal_year} | "
                f"{quarter_report.fiscal_period} | "
                f"{quarter_report.status} | "
                f"{_report_filed_date(quarter_report)} | "
                f"{_escape_markdown_table(quarter_report.source_url)} | "
                f"`{_escape_markdown_table(str(quarter_report.source_path or ''))}` | "
                f"{quarter_report.row_count} | "
                f"{quarter_report.checksum_sha256} |"
            )

    lines.extend(["", "## 问题", ""])
    all_issues = report.all_issues
    if not all_issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | 指标 | Source URL | Source Path | 说明 |",
                "|---|---|---|---|---|---|",
            ]
        )
        for issue in all_issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.metric_id or ''} | "
                f"{_escape_markdown_table(issue.source_url or '')} | "
                f"`{_escape_markdown_table(str(issue.source_path or ''))}` | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- manifest 必须包含 `fiscal_year`、`fiscal_period`、`source_url` 和 `input_path`；"
            "`filed_date` 可选但历史回测建议填写，代表 Management Report 公开/披露日期。"
            "`input_path` 可为绝对路径，或相对 manifest 所在目录。",
            "- 每一行必须指向已抽取的官方 Management Report 文本；PDF 或二进制文件仍需先用 "
            "`aits fundamentals extract-tsm-ir-pdf-text` 生成可审计文本。",
            "- `source_url` 必须来自 `https://investor.tsmc.com/...`，非官方域名会失败。",
            "- 同一批次重复 `fiscal_year/fiscal_period` 会失败，避免后写入记录覆盖前一条证据。",
            "- 只有整批状态通过后才会写入 `data/processed/tsm_ir_quarterly_metrics.csv`；"
            "批量写入只替换同一 `as_of/ticker/fiscal_year/fiscal_period` 的旧行。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_tsm_ir_quarterly_batch_import_report(
    report: TsmIrQuarterlyBatchImportReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_tsm_ir_quarterly_batch_import_report(report),
        encoding="utf-8",
    )
    return output_path


def render_tsm_ir_pdf_text_extraction_report(report: TsmIrPdfTextExtractionReport) -> str:
    lines = [
        "# TSMC IR PDF 文本抽取报告",
        "",
        f"- 状态：{report.status}",
        f"- Provider：{report.provider}",
        f"- Source URL：{report.source_url}",
        f"- Input Path：`{report.input_path}`",
        f"- Output Path：`{report.output_path}`",
        f"- Extracted At：{report.extracted_at.isoformat()}",
        f"- Page Count：{report.page_count}",
        f"- Character Count：{report.character_count}",
        f"- Input checksum_sha256：{report.input_checksum_sha256}",
        f"- Text checksum_sha256：{report.text_checksum_sha256}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## 问题",
        "",
    ]
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Source URL | Source Path | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{_escape_markdown_table(issue.source_url or '')} | "
                f"{_escape_markdown_table(str(issue.source_path or ''))} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 仅允许 `https://investor.tsmc.com/...` 官方 Source URL。",
            "- `pypdf` 通过 `import_module(\"pypdf\")` 动态加载，是 optional dependency。",
            "- PDF 无可抽取文本时状态为 FAIL，需要 OCR 或人工抽取；本流程不生成伪文本。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_tsm_ir_pdf_text_extraction_report(
    report: TsmIrPdfTextExtractionReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_tsm_ir_pdf_text_extraction_report(report), encoding="utf-8")
    return output_path


def is_official_tsm_ir_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme == "https" and parsed.hostname == TSM_IR_OFFICIAL_HOST


def _binary_url_reason(url: str) -> str | None:
    path = urlparse(url).path.lower()
    binary_suffixes = {
        ".pdf": "URL path ends with .pdf",
        ".zip": "URL path ends with .zip",
        ".xls": "URL path ends with .xls",
        ".xlsx": "URL path ends with .xlsx",
        ".ppt": "URL path ends with .ppt",
        ".pptx": "URL path ends with .pptx",
        ".doc": "URL path ends with .doc",
        ".docx": "URL path ends with .docx",
    }
    for suffix, reason in binary_suffixes.items():
        if path.endswith(suffix):
            return reason
    return None


def _response_content_type(headers: object) -> str:
    headers_mapping = cast(Any, headers)
    value = (
        headers_mapping.get("Content-Type")
        or headers_mapping.get("content-type")
        or ""
    )
    return str(value).split(";", 1)[0].strip().lower()


def _binary_content_type_reason(content_type: str) -> str | None:
    if not content_type:
        return None
    if content_type in {"application/pdf", "application/octet-stream", "application/zip"}:
        return f"response Content-Type is {content_type}"
    if content_type.startswith(("image/", "audio/", "video/")):
        return f"response Content-Type is {content_type}"
    if content_type.startswith(
        (
            "application/vnd.ms-",
            "application/vnd.openxmlformats-officedocument.",
        )
    ):
        return f"response Content-Type is {content_type}"
    return None


def _is_text_content_type(content_type: str) -> bool:
    return content_type.startswith("text/") or content_type in {
        "application/json",
        "application/xml",
        "application/xhtml+xml",
    }


def _response_text(url: str, response: Any, content_type: str) -> str:
    content = getattr(response, "content", None)
    if isinstance(content, bytes):
        bytes_reason = _binary_bytes_reason(content)
        if bytes_reason is not None:
            raise ValueError(_extracted_text_required_message(url, bytes_reason))
        encoding = (
            getattr(response, "encoding", None)
            or getattr(response, "apparent_encoding", None)
            or "utf-8"
        )
        try:
            text = content.decode(str(encoding))
        except (LookupError, UnicodeDecodeError) as exc:
            raise ValueError(
                _extracted_text_required_message(
                    url,
                    f"response bytes could not be decoded as text ({content_type or 'unknown'})",
                )
            ) from exc
    else:
        response_text = getattr(response, "text", None)
        if not isinstance(response_text, str):
            raise TypeError("TSMC IR HTTP response did not expose text content")
        text = response_text

    text_reason = _binary_text_reason(text)
    if text_reason is not None:
        raise ValueError(_extracted_text_required_message(url, text_reason))
    if not text.strip():
        raise ValueError("TSMC IR HTTP response text was empty")
    return text


def _binary_bytes_reason(content: bytes) -> str | None:
    sample = content[:4096]
    if sample.lstrip().startswith(b"%PDF"):
        return "response bytes start with a PDF signature"
    if b"\x00" in sample:
        return "response bytes contain NUL bytes"
    return None


def _binary_text_reason(text: str) -> str | None:
    sample = text[:4096]
    if sample.lstrip().startswith("%PDF"):
        return "response text starts with a PDF signature"
    if "\x00" in sample:
        return "response text contains NUL bytes"
    return None


def _extracted_text_required_message(url: str, reason: str) -> str:
    return (
        f"TSMC IR resource at {url} appears to be PDF/binary or non-text "
        f"({reason}); current parser requires already extracted Management "
        "Report text and will not parse binary content."
    )


def _resolve_tsm_company(
    tsm_company: SecCompaniesConfig | SecCompanyConfig,
    issues: list[TsmIrIssue],
) -> SecCompanyConfig | None:
    if isinstance(tsm_company, SecCompanyConfig):
        company = tsm_company
    else:
        matches = [
            company
            for company in tsm_company.companies
            if company.ticker.upper() == _TSM_TICKER and company.active
        ]
        if not matches:
            issues.append(
                TsmIrIssue(
                    severity=TsmIrIssueSeverity.ERROR,
                    code="tsm_ir_sec_company_missing",
                    message="SEC companies config does not contain an active TSM company.",
                )
            )
            return None
        company = matches[0]

    if company.ticker.upper() != _TSM_TICKER:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_sec_company_ticker_mismatch",
                message=(
                    "TSMC IR SEC conversion requires SEC company config ticker TSM; "
                    f"received {company.ticker}."
                ),
            )
        )
        return None
    if not company.active:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_sec_company_inactive",
                message="TSMC IR SEC conversion requires an active TSM company config.",
            )
        )
        return None
    if "quarterly" not in company.sec_metric_periods:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_sec_company_quarterly_period_missing",
                message=(
                    "TSMC IR SEC conversion requires TSM sec_metric_periods to include quarterly."
                ),
            )
        )
        return None
    return company


def _tsm_ir_row_is_convertible(
    row: TsmIrQuarterlyMetricRow,
    issues: list[TsmIrIssue],
) -> bool:
    convertible = True
    period_type = str(row.period_type)
    if row.ticker.upper() != _TSM_TICKER:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_sec_metric_ticker_mismatch",
                metric_id=row.metric_id,
                source_url=row.source_url,
                source_path=row.source_path,
                message=(
                    "TSMC IR row cannot be converted to SEC metric row because ticker "
                    f"is {row.ticker}, not TSM."
                ),
            )
        )
        convertible = False
    if period_type != "quarterly":
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_sec_metric_period_type_mismatch",
                metric_id=row.metric_id,
                source_url=row.source_url,
                source_path=row.source_path,
                message=(
                    "TSMC IR row cannot be converted to SEC metric row because period_type "
                    f"is {period_type}, not quarterly."
                ),
            )
        )
        convertible = False
    if not re.fullmatch(r"Q[1-4]", row.fiscal_period.upper()):
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_sec_metric_fiscal_period_mismatch",
                metric_id=row.metric_id,
                source_url=row.source_url,
                source_path=row.source_path,
                message=(
                    "TSMC IR row cannot be converted to SEC metric row because fiscal_period "
                    f"is {row.fiscal_period}, not Q1-Q4."
                ),
            )
        )
        convertible = False
    return convertible


def _tsm_ir_row_to_sec_metric_row(
    row: TsmIrQuarterlyMetricRow,
    company: SecCompanyConfig,
) -> SecFundamentalMetricRow:
    if row.source_path is None:
        raise ValueError("source_path is required before converting TSM IR rows")
    return SecFundamentalMetricRow(
        as_of=row.as_of,
        ticker=_TSM_TICKER,
        cik=company.cik,
        company_name=company.company_name,
        metric_id=row.metric_id,
        metric_name=row.metric_name,
        period_type="quarterly",
        fiscal_year=row.fiscal_year,
        fiscal_period=row.fiscal_period.upper(),
        end_date=row.end_date,
        filed_date=row.filed_date or row.captured_at.date(),
        form="TSM-IR",
        taxonomy="tsm-ir",
        concept=f"management_report:{row.metric_id}",
        unit=row.unit,
        value=row.value,
        accession_number=_tsm_ir_accession_number(row),
        source_path=row.source_path,
    )


def _tsm_ir_accession_number(row: TsmIrQuarterlyMetricRow) -> str:
    checksum = row.checksum_sha256[:16] if row.checksum_sha256 else "no_checksum"
    period = f"FY{row.fiscal_year}{row.fiscal_period.upper()}"
    return f"{row.source_id}:{checksum}:{period}"


def _tsm_ir_historical_selection_key(
    row: TsmIrQuarterlyMetricRow,
) -> tuple[date, date, datetime, date]:
    return (
        row.end_date,
        row.filed_date or row.captured_at.date(),
        row.captured_at,
        row.as_of,
    )


def _sec_metric_merge_key(row: SecFundamentalMetricRow) -> tuple[date, str, str, str]:
    return (row.as_of, row.ticker.upper(), row.metric_id, row.period_type)


def _resources_from_page(
    source_url: str,
    provider: TsmIrQuarterlyProvider,
    issues: list[TsmIrIssue],
) -> list[TsmIrQuarterlyResource]:
    try:
        page_text = provider.download_text(source_url)
    except Exception as exc:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_source_download_failed",
                source_url=source_url,
                message=f"TSMC IR quarterly page could not be downloaded: {exc}",
            )
        )
        return []
    resources = _extract_source_links(page_text, source_url)
    if not resources:
        issues.append(
            TsmIrIssue(
                severity=TsmIrIssueSeverity.ERROR,
                code="tsm_ir_source_links_missing",
                source_url=source_url,
                message=(
                    "TSMC IR quarterly page did not expose Management Report or "
                    "Financial Statements links."
                ),
            )
        )
    return resources


def _extract_source_links(page_text: str, base_url: str) -> list[TsmIrQuarterlyResource]:
    resources: list[TsmIrQuarterlyResource] = []
    link_pattern = re.compile(
        r"""<a\b[^>]*href\s*=\s*["'](?P<href>[^"']+)["'][^>]*>(?P<label>.*?)</a>""",
        re.IGNORECASE | re.DOTALL,
    )
    for match in link_pattern.finditer(page_text):
        label = re.sub(r"<[^>]+>", " ", match.group("label"))
        label = _normalize_spaces(label)
        href = urljoin(base_url, match.group("href"))
        lower_label = label.lower()
        lower_href = href.lower()
        if "management report" in lower_label or "management-report" in lower_href:
            resources.append(TsmIrQuarterlyResource(href, "management_report_text"))
        elif "financial statements" in lower_label or "financial-statements" in lower_href:
            resources.append(TsmIrQuarterlyResource(href, "financial_statement_text"))
    return resources


def _select_management_resource(
    resources: tuple[TsmIrQuarterlyResource, ...],
    issues: list[TsmIrIssue],
) -> TsmIrQuarterlyResource | None:
    for resource in resources:
        if resource.resource_type == "management_report_text":
            return resource
    issues.append(
        TsmIrIssue(
            severity=TsmIrIssueSeverity.ERROR,
            code="tsm_ir_source_links_missing",
            message="No TSMC IR Management Report resource was provided or discovered.",
        )
    )
    return None


def _source_url_issues(url: str) -> list[TsmIrIssue]:
    if is_official_tsm_ir_url(url):
        return []
    return [
        TsmIrIssue(
            severity=TsmIrIssueSeverity.ERROR,
            code="tsm_ir_non_official_url",
            source_url=url,
            message="TSMC IR source URL must use the official investor.tsmc.com HTTPS domain.",
        )
    ]


def _parse_metric_value(
    text: str,
    spec: _MetricSpec,
    money_unit: str,
) -> tuple[float, str] | None:
    if spec.unit_kind == "capex":
        return _parse_capex(text)
    for label_pattern in spec.label_patterns:
        value = _first_numeric_after_label(text, label_pattern)
        if value is None:
            continue
        if spec.unit_kind == "percent":
            return value, "percent"
        return value, money_unit
    return None


def _first_numeric_after_label(text: str, label_pattern: str) -> float | None:
    line_pattern = re.compile(
        rf"^[^\S\r\n]*(?:[l•-]\s*)?{label_pattern}(?P<tail>.*)$",
        re.IGNORECASE,
    )
    for line in text.splitlines():
        match = line_pattern.match(line)
        if match is None:
            continue
        tail = match.group("tail")
        if _tail_starts_with_alternate_value_unit(tail):
            continue
        value = _first_number(tail)
        if value is not None:
            return value

    pattern = re.compile(
        rf"^[^\S\r\n]*(?:[l•-]\s*)?{label_pattern}(?P<tail>.{{0,220}})",
        re.IGNORECASE | re.DOTALL | re.MULTILINE,
    )
    for match in pattern.finditer(text):
        tail = match.group("tail")
        if _tail_starts_with_alternate_value_unit(tail):
            continue
        value = _first_number(tail)
        if value is not None:
            return value
    return None


def _parse_capex(text: str) -> tuple[float, str] | None:
    currency_candidates: list[tuple[float, str]] = []
    section_pattern = re.compile(
        r"Capital\s+Expenditures(?P<tail>.{0,260})",
        re.IGNORECASE | re.DOTALL,
    )
    for match in section_pattern.finditer(text):
        parsed_currency_amount = _first_currency_amount(match.group("tail"))
        if parsed_currency_amount is not None:
            currency_candidates.append(parsed_currency_amount)
    for value, unit in currency_candidates:
        if unit == "USD_billions":
            return value, unit
    if currency_candidates:
        return currency_candidates[0]

    table_pattern = re.compile(
        r"\(in\s+(?P<currency>US\$|NT\$)\s+(?P<scale>billions?|millions?)\)"
        r"(?P<tail>.{0,260}?^Capital\s+Expenditures\s+"
        r"(?P<number>[-+]?\(?\d[\d,]*(?:\.\d+)?\)?)\b)",
        re.IGNORECASE | re.DOTALL | re.MULTILINE,
    )
    for match in table_pattern.finditer(text):
        number = _numeric_match_value(match.group("number"))
        if number is None:
            continue
        currency = "USD" if match.group("currency").upper() == "US$" else "TWD"
        scale = _normalized_scale(match.group("scale"))
        return number, f"{currency}_{scale}"
    return None


def _tail_starts_with_alternate_value_unit(tail: str) -> bool:
    match = re.match(r"\s*\((?P<label>[^)]{0,80})\)", tail)
    if match is None:
        return False
    label = match.group("label")
    return bool(re.search(r"US\$|ADR|per\s+(?:common\s+)?share", label, re.IGNORECASE))


def _first_currency_amount(text: str) -> tuple[float, str] | None:
    match = re.search(
        r"(?P<currency>US\$|NT\$)\s*"
        r"(?P<number>[-+]?\(?\d[\d,]*(?:\.\d+)?\)?)"
        r"(?:\s*(?P<scale>billions?|millions?))?",
        text,
        re.IGNORECASE,
    )
    if match is None:
        return None
    value = _numeric_match_value(match.group("number"))
    if value is None:
        return None
    currency = "USD" if match.group("currency").upper() == "US$" else "TWD"
    scale = _normalized_scale(match.group("scale") or "amount")
    return value, f"{currency}_{scale}"


def _first_currency_number(text: str) -> float | None:
    match = re.search(r"(?:US\$|NT\$)\s*(?P<number>\d[\d,]*(?:\.\d+)?)", text, re.IGNORECASE)
    if match is None:
        return None
    return _numeric_match_value(match.group("number"))


def _first_number(text: str) -> float | None:
    match = re.search(r"[-+]?\(?\d[\d,]*(?:\.\d+)?\)?", text)
    if match is None:
        return None
    return _numeric_match_value(match.group(0))


def _numeric_match_value(value_text: str) -> float | None:
    normalized = value_text.strip().replace(",", "")
    if normalized.startswith("(") and normalized.endswith(")"):
        normalized = normalized[1:-1]
    try:
        value = float(normalized)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def _normalized_scale(scale: str) -> str:
    normalized = scale.strip().lower()
    if normalized in {"billion", "billions"}:
        return "billions"
    if normalized in {"million", "millions", "m", "mn"}:
        return "millions"
    return "amount"


def _money_unit(text: str) -> str:
    if re.search(r"NT\$\s*billions?|NTD\s*billions?", text, re.IGNORECASE):
        return "TWD_billions"
    if re.search(r"US\$\s*billions?|USD\s*billions?", text, re.IGNORECASE):
        return "USD_billions"
    if re.search(r"NT\$\s*M|NT\$\s*millions?|NTD\s*millions?", text, re.IGNORECASE):
        return "TWD_millions"
    if re.search(r"US\$\s*M|US\$\s*millions?|USD\s*millions?", text, re.IGNORECASE):
        return "USD_millions"
    return "reported_amount"


def _quarter_end_date(fiscal_year: int, fiscal_period: str) -> date:
    month_day_by_period = {
        "Q1": (3, 31),
        "Q2": (6, 30),
        "Q3": (9, 30),
        "Q4": (12, 31),
    }
    try:
        month, day = month_day_by_period[fiscal_period.upper()]
    except KeyError as exc:
        raise ValueError(
            f"unsupported fiscal_period for TSM IR quarterly metrics: {fiscal_period}"
        ) from exc
    return date(fiscal_year, month, day)


def _period_from_url(url: str) -> tuple[int | None, str | None]:
    match = re.search(r"/(?P<year>20\d{2})/(?P<period>q[1-4])(?:/|$)", url, re.IGNORECASE)
    if match is None:
        return None, None
    return int(match.group("year")), match.group("period").upper()


def _empty_report(
    as_of: date,
    fiscal_year: int,
    fiscal_period: str,
    source_url: str,
    source_path: Path | None,
    captured_at: datetime,
    checksum: str,
    issues: list[TsmIrIssue],
) -> TsmIrQuarterlyReport:
    normalized_period = fiscal_period.upper()
    end_date = (
        _quarter_end_date(fiscal_year, normalized_period)
        if fiscal_year and normalized_period
        else as_of
    )
    return TsmIrQuarterlyReport(
        as_of=as_of,
        ticker="TSM",
        fiscal_year=fiscal_year,
        fiscal_period=normalized_period,
        end_date=end_date,
        captured_at=captured_at,
        source_url=source_url,
        source_path=source_path,
        source_id=TSM_IR_SOURCE_ID,
        provider=TSM_IR_PROVIDER_NAME,
        checksum_sha256=checksum,
        rows=(),
        issues=tuple(issues),
    )


def _pdf_text_extraction_report(
    source_url: str,
    input_path: Path,
    output_path: Path,
    extracted_at: datetime,
    issues: list[TsmIrIssue],
    page_count: int = 0,
    character_count: int = 0,
    input_checksum_sha256: str = "",
    text_checksum_sha256: str = "",
) -> TsmIrPdfTextExtractionReport:
    return TsmIrPdfTextExtractionReport(
        provider=TSM_IR_PROVIDER_NAME,
        source_url=source_url,
        input_path=input_path,
        output_path=output_path,
        extracted_at=extracted_at,
        page_count=page_count,
        character_count=character_count,
        input_checksum_sha256=input_checksum_sha256,
        text_checksum_sha256=text_checksum_sha256,
        issues=tuple(issues),
    )


def _row_record(row: TsmIrQuarterlyMetricRow) -> dict[str, object]:
    return {
        "as_of": row.as_of.isoformat(),
        "ticker": row.ticker,
        "fiscal_year": row.fiscal_year,
        "fiscal_period": row.fiscal_period,
        "end_date": row.end_date.isoformat(),
        "filed_date": row.filed_date.isoformat() if row.filed_date else "",
        "captured_at": row.captured_at.isoformat(),
        "metric_id": row.metric_id,
        "metric_name": row.metric_name,
        "period_type": row.period_type,
        "unit": row.unit,
        "value": row.value,
        "source_url": row.source_url,
        "source_path": str(row.source_path or ""),
        "source_id": row.source_id,
        "checksum_sha256": row.checksum_sha256,
    }


def _tsm_ir_metric_row_from_record(
    record: dict[str, object],
) -> TsmIrQuarterlyMetricRow:
    period_type = _string_record_value(record, "period_type")
    if period_type != "quarterly":
        raise ValueError(f"TSMC IR 季度指标 CSV period_type 必须是 quarterly：{period_type}")
    value = _float_record_value(record, "value")
    if value is None:
        raise ValueError("TSMC IR 季度指标 CSV value 不是有效数值。")
    source_path_value = _string_record_value(record, "source_path")
    return TsmIrQuarterlyMetricRow(
        as_of=_required_date_record_value(record, "as_of"),
        ticker=_string_record_value(record, "ticker").upper(),
        fiscal_year=_required_int_record_value(record, "fiscal_year"),
        fiscal_period=_string_record_value(record, "fiscal_period").upper(),
        end_date=_required_date_record_value(record, "end_date"),
        filed_date=_date_record_value(record, "filed_date"),
        captured_at=_required_datetime_record_value(record, "captured_at"),
        metric_id=_string_record_value(record, "metric_id"),
        metric_name=_string_record_value(record, "metric_name"),
        period_type="quarterly",
        unit=_string_record_value(record, "unit"),
        value=value,
        source_url=_string_record_value(record, "source_url"),
        source_path=Path(source_path_value) if source_path_value else None,
        source_id=_string_record_value(record, "source_id"),
        checksum_sha256=_string_record_value(record, "checksum_sha256"),
    )


def _string_record_value(record: dict[str, object], key: str) -> str:
    value = record.get(key)
    if value is None or _is_missing_cell(value):
        return ""
    return str(value)


def _required_date_record_value(record: dict[str, object], key: str) -> date:
    parsed = _date_record_value(record, key)
    if parsed is None:
        raise ValueError(f"TSMC IR 季度指标 CSV 缺少有效日期字段：{key}")
    return parsed


def _date_record_value(record: dict[str, object], key: str) -> date | None:
    value = _string_record_value(record, key)
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"TSMC IR 季度指标 CSV 日期字段无效：{key}={value}") from exc


def _required_datetime_record_value(record: dict[str, object], key: str) -> datetime:
    value = _string_record_value(record, key)
    if not value:
        raise ValueError(f"TSMC IR 季度指标 CSV 缺少有效时间字段：{key}")
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"TSMC IR 季度指标 CSV 时间字段无效：{key}={value}") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _required_int_record_value(record: dict[str, object], key: str) -> int:
    value = record.get(key)
    if value is None or _is_missing_cell(value):
        raise ValueError(f"TSMC IR 季度指标 CSV 缺少有效整数字段：{key}")
    if not isinstance(value, bool) and isinstance(value, Real):
        numeric_value = float(value)
        if not math.isfinite(numeric_value):
            raise ValueError(f"TSMC IR 季度指标 CSV 整数字段无效：{key}={value}")
        return int(numeric_value)
    try:
        return int(float(str(value).strip()))
    except ValueError as exc:
        raise ValueError(f"TSMC IR 季度指标 CSV 整数字段无效：{key}={value}") from exc


def _float_record_value(record: dict[str, object], key: str) -> float | None:
    value = record.get(key)
    if value is None or _is_missing_cell(value):
        return None
    if not isinstance(value, bool) and isinstance(value, Real):
        numeric_value = float(value)
    else:
        try:
            numeric_value = float(str(value).strip())
        except ValueError:
            return None
    return numeric_value if math.isfinite(numeric_value) else None


def _manifest_row_is_empty(record: dict[str, object]) -> bool:
    return all(
        not _string_record_value(record, column)
        for column in _TSM_IR_IMPORT_MANIFEST_COLUMNS
    )


def _required_manifest_string_value(
    record: dict[str, object],
    key: str,
    row_number: int,
) -> str:
    value = _string_record_value(record, key).strip()
    if not value:
        raise ValueError(
            f"TSMC IR 批量导入 manifest 第 {row_number} 行缺少字段：{key}。"
        )
    return value


def _required_manifest_int_value(
    record: dict[str, object],
    key: str,
    row_number: int,
) -> int:
    value = record.get(key)
    if value is None or _is_missing_cell(value):
        raise ValueError(
            f"TSMC IR 批量导入 manifest 第 {row_number} 行缺少整数字段：{key}。"
        )
    if isinstance(value, bool):
        raise ValueError(
            f"TSMC IR 批量导入 manifest 第 {row_number} 行整数字段无效：{key}={value}。"
        )
    if isinstance(value, Real):
        numeric_value = float(value)
    else:
        try:
            numeric_value = float(str(value).strip())
        except ValueError as exc:
            raise ValueError(
                "TSMC IR 批量导入 manifest 第 "
                f"{row_number} 行整数字段无效：{key}={value}。"
            ) from exc
    if not math.isfinite(numeric_value) or not numeric_value.is_integer():
        raise ValueError(
            f"TSMC IR 批量导入 manifest 第 {row_number} 行整数字段无效：{key}={value}。"
        )
    return int(numeric_value)


def _manifest_optional_date_value(
    record: dict[str, object],
    key: str,
    row_number: int,
) -> date | None:
    if key not in record:
        return None
    value = _string_record_value(record, key).strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"TSMC IR 批量导入 manifest 第 {row_number} 行日期字段无效："
            f"{key}={value}。"
        ) from exc


def _manifest_input_path(manifest_path: Path, input_path_text: str) -> Path:
    input_path = Path(input_path_text)
    if not input_path.is_absolute():
        input_path = manifest_path.parent / input_path
    return input_path.resolve()


def _is_missing_cell(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, Real) and math.isnan(float(value))


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _normalize_spaces(value: str) -> str:
    return " ".join(value.split())


def _severity_label(severity: TsmIrIssueSeverity) -> str:
    if severity == TsmIrIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _report_filed_date(report: TsmIrQuarterlyReport) -> str:
    filed_dates = {row.filed_date for row in report.rows if row.filed_date is not None}
    if len(filed_dates) != 1:
        return ""
    return next(iter(filed_dates)).isoformat()


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
