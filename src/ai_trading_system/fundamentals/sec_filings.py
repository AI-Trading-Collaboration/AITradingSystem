from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from importlib import import_module
from pathlib import Path
from time import sleep
from typing import Any, Protocol, cast

import pandas as pd

from ai_trading_system.config import (
    PROJECT_ROOT,
    SecCompaniesConfig,
    SecCompanyConfig,
    dedupe_preserving_order,
)
from ai_trading_system.external_request_cache import (
    cached_requests_get,
    default_external_request_cache_dir,
)
from ai_trading_system.fundamentals.sec_metrics import (
    SecFundamentalMetricRow,
    load_sec_fundamental_metric_rows_csv,
)

DEFAULT_SEC_SUBMISSIONS_DIR = PROJECT_ROOT / "data" / "raw" / "sec_submissions"
DEFAULT_SEC_FILING_ARCHIVE_DIR = PROJECT_ROOT / "data" / "raw" / "sec_filings"


@dataclass(frozen=True)
class SecFilingRequest:
    ticker: str
    cik: str


@dataclass(frozen=True)
class SecFilingIndexRequest:
    ticker: str
    cik: str
    accession_number: str


class SecFilingArchiveProvider(Protocol):
    def download_submissions(self, request: SecFilingRequest) -> dict[str, Any]:
        """Download SEC submissions JSON for one CIK."""

    def download_filing_index(self, request: SecFilingIndexRequest) -> dict[str, Any]:
        """Download SEC archive accession directory index JSON."""


@dataclass(frozen=True)
class SecSubmissionsFile:
    ticker: str
    cik: str
    output_path: Path
    filing_count: int
    additional_file_count: int
    checksum_sha256: str


@dataclass(frozen=True)
class SecSubmissionsDownloadSummary:
    output_dir: Path
    manifest_path: Path
    files: tuple[SecSubmissionsFile, ...]

    @property
    def company_count(self) -> int:
        return len(self.files)

    @property
    def filing_count(self) -> int:
        return sum(file.filing_count for file in self.files)


@dataclass(frozen=True)
class SecFilingArchiveFile:
    ticker: str
    cik: str
    accession_number: str
    output_path: Path
    item_count: int
    checksum_sha256: str


@dataclass(frozen=True)
class SecFilingArchiveDownloadSummary:
    output_dir: Path
    manifest_path: Path
    files: tuple[SecFilingArchiveFile, ...]

    @property
    def accession_count(self) -> int:
        return len(self.files)


@dataclass(frozen=True)
class SecAccessionCoverageIssue:
    severity: str
    code: str
    message: str
    ticker: str | None = None
    accession_number: str | None = None


@dataclass(frozen=True)
class SecAccessionCoverageRow:
    ticker: str
    cik: str
    accession_number: str
    metric_ids: tuple[str, ...]
    forms: tuple[str, ...]
    metric_filed_dates: tuple[str, ...]
    submission_found: bool
    archive_index_found: bool
    submission_form: str
    filing_date: str
    report_date: str
    accepted_time: str
    primary_document: str
    archive_index_path: Path
    archive_index_sha256: str | None
    status: str


@dataclass(frozen=True)
class SecAccessionCoverageReport:
    as_of: date
    metrics_path: Path
    submissions_dir: Path
    filing_archive_dir: Path
    rows: tuple[SecAccessionCoverageRow, ...]
    issues: tuple[SecAccessionCoverageIssue, ...] = field(default_factory=tuple)
    production_effect: str = "none"

    @property
    def accession_count(self) -> int:
        return len(self.rows)

    @property
    def covered_count(self) -> int:
        return sum(1 for row in self.rows if row.status == "PASS")

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "ERROR")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "WARNING")

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


class SecEdgarFilingArchiveProvider:
    submissions_base_url = "https://data.sec.gov/submissions"
    archive_base_url = "https://www.sec.gov/Archives/edgar/data"

    def __init__(
        self,
        user_agent: str,
        *,
        requests_module: Any | None = None,
        request_cache_dir: Path | str | None = None,
    ) -> None:
        if not user_agent.strip():
            raise ValueError("SEC User-Agent must not be empty")
        self.user_agent = user_agent.strip()
        self._requests_module = requests_module
        self._request_cache_dir = request_cache_dir

    def download_submissions(self, request: SecFilingRequest) -> dict[str, Any]:
        return self._get_json(self.submissions_endpoint_for(request.cik))

    def download_filing_index(self, request: SecFilingIndexRequest) -> dict[str, Any]:
        return self._get_json(
            self.filing_index_endpoint_for(request.cik, request.accession_number)
        )

    def submissions_endpoint_for(self, cik: str) -> str:
        return f"{self.submissions_base_url}/CIK{cik}.json"

    def filing_index_endpoint_for(self, cik: str, accession_number: str) -> str:
        cik_int = int(cik)
        accession_directory = accession_number.replace("-", "")
        return f"{self.archive_base_url}/{cik_int}/{accession_directory}/index.json"

    def _get_json(self, url: str) -> dict[str, Any]:
        requests = self._requests_module or cast(Any, import_module("requests"))
        request_cache_dir = default_external_request_cache_dir(
            requests_module=self._requests_module,
            explicit_cache_dir=self._request_cache_dir,
        )
        response = cached_requests_get(
            provider="SEC EDGAR",
            api_family="filing_archive",
            url=url,
            headers={
                "User-Agent": self.user_agent,
                "Accept-Encoding": "gzip, deflate",
                "Accept": "application/json",
            },
            timeout=30,
            requests_module=requests,
            cache_dir=request_cache_dir,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise TypeError("SEC filing archive response was not a JSON object")
        return cast(dict[str, Any], data)


def download_sec_submissions(
    *,
    config: SecCompaniesConfig,
    output_dir: Path,
    provider: SecFilingArchiveProvider,
    tickers: list[str] | None = None,
) -> SecSubmissionsDownloadSummary:
    companies = _selected_companies(config, tickers)
    if not companies:
        raise ValueError("no active SEC companies selected for submissions download")

    output_dir.mkdir(parents=True, exist_ok=True)
    files: list[SecSubmissionsFile] = []
    manifest_records: list[dict[str, object]] = []
    for company in companies:
        request = SecFilingRequest(ticker=company.ticker, cik=company.cik)
        data = provider.download_submissions(request)
        output_path = output_dir / f"{company.ticker.lower()}_submissions.json"
        _write_json(data, output_path)
        checksum = _sha256_file(output_path)
        file_record = SecSubmissionsFile(
            ticker=company.ticker,
            cik=company.cik,
            output_path=output_path,
            filing_count=_recent_filing_count(data),
            additional_file_count=_additional_submission_file_count(data),
            checksum_sha256=checksum,
        )
        files.append(file_record)
        manifest_records.append(
            {
                "downloaded_at": datetime.now(tz=UTC).isoformat(),
                "source_id": "sec_submissions",
                "provider": "SEC EDGAR",
                "endpoint": _provider_submissions_endpoint(provider, company.cik),
                "request_parameters": json.dumps(
                    {"ticker": company.ticker, "cik": company.cik},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "ticker": company.ticker,
                "cik": company.cik,
                "output_path": str(output_path),
                "filing_count": file_record.filing_count,
                "additional_file_count": file_record.additional_file_count,
                "row_count": file_record.filing_count,
                "checksum_sha256": checksum,
            }
        )

    manifest_path = _write_manifest(
        output_dir,
        tuple(manifest_records),
        "sec_submissions_manifest.csv",
    )
    return SecSubmissionsDownloadSummary(
        output_dir=output_dir,
        manifest_path=manifest_path,
        files=tuple(files),
    )


def download_sec_filing_archive_indexes(
    *,
    metrics_path: Path,
    as_of: date,
    output_dir: Path,
    provider: SecFilingArchiveProvider,
    tickers: list[str] | None = None,
    request_delay_seconds: float = 0.2,
) -> SecFilingArchiveDownloadSummary:
    if request_delay_seconds < 0:
        raise ValueError("request_delay_seconds must be non-negative")
    accessions = _metric_accessions(metrics_path, as_of, tickers)
    if not accessions:
        raise ValueError(
            f"no SEC metric accessions found for as_of={as_of.isoformat()}"
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    files: list[SecFilingArchiveFile] = []
    manifest_records: list[dict[str, object]] = []
    for index, accession in enumerate(accessions):
        if index and request_delay_seconds:
            sleep(request_delay_seconds)
        request = SecFilingIndexRequest(
            ticker=accession["ticker"],
            cik=accession["cik"],
            accession_number=accession["accession_number"],
        )
        data = provider.download_filing_index(request)
        output_path = _filing_index_path(
            output_dir,
            request.ticker,
            request.accession_number,
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        _write_json(data, output_path)
        checksum = _sha256_file(output_path)
        file_record = SecFilingArchiveFile(
            ticker=request.ticker,
            cik=request.cik,
            accession_number=request.accession_number,
            output_path=output_path,
            item_count=_filing_index_item_count(data),
            checksum_sha256=checksum,
        )
        files.append(file_record)
        manifest_records.append(
            {
                "downloaded_at": datetime.now(tz=UTC).isoformat(),
                "source_id": "sec_filing_archive_index",
                "provider": "SEC EDGAR",
                "endpoint": _provider_filing_index_endpoint(
                    provider,
                    request.cik,
                    request.accession_number,
                ),
                "request_parameters": json.dumps(
                    {
                        "ticker": request.ticker,
                        "cik": request.cik,
                        "accession_number": request.accession_number,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "ticker": request.ticker,
                "cik": request.cik,
                "accession_number": request.accession_number,
                "output_path": str(output_path),
                "item_count": file_record.item_count,
                "row_count": file_record.item_count,
                "checksum_sha256": checksum,
            }
        )

    manifest_path = _write_manifest(
        output_dir,
        tuple(manifest_records),
        "sec_filing_archive_manifest.csv",
    )
    return SecFilingArchiveDownloadSummary(
        output_dir=output_dir,
        manifest_path=manifest_path,
        files=tuple(files),
    )


def build_sec_accession_coverage_report(
    *,
    metrics_path: Path,
    submissions_dir: Path,
    filing_archive_dir: Path,
    as_of: date,
) -> SecAccessionCoverageReport:
    rows: list[SecAccessionCoverageRow] = []
    issues: list[SecAccessionCoverageIssue] = []
    accessions = _metric_accessions(metrics_path, as_of, tickers=None)
    if not accessions:
        issues.append(
            SecAccessionCoverageIssue(
                severity="ERROR",
                code="sec_accession_metrics_missing",
                message=(
                    f"SEC 指标 CSV 不存在、没有 as_of={as_of.isoformat()} 的记录，"
                    "或没有 accession_number。"
                ),
            )
        )
    submissions_by_ticker: dict[str, dict[str, dict[str, str]]] = {}
    for accession in accessions:
        ticker = accession["ticker"]
        if ticker not in submissions_by_ticker:
            submissions_by_ticker[ticker] = _submission_records_by_accession(
                submissions_dir / f"{ticker.lower()}_submissions.json",
                ticker=ticker,
                issues=issues,
            )
        submission = submissions_by_ticker[ticker].get(accession["accession_number"])
        index_path = _filing_index_path(
            filing_archive_dir,
            ticker,
            accession["accession_number"],
        )
        archive_index_found = index_path.exists() and index_path.stat().st_size > 0
        status = "PASS"
        if submission is None:
            status = "MISSING_SUBMISSION_METADATA"
            issues.append(
                SecAccessionCoverageIssue(
                    severity="ERROR",
                    code="sec_accession_submission_metadata_missing",
                    ticker=ticker,
                    accession_number=accession["accession_number"],
                    message="SEC submissions JSON 中找不到该 accession。"
                )
            )
        elif not archive_index_found:
            status = "MISSING_ARCHIVE_INDEX"
            issues.append(
                SecAccessionCoverageIssue(
                    severity="ERROR",
                    code="sec_accession_archive_index_missing",
                    ticker=ticker,
                    accession_number=accession["accession_number"],
                    message="尚未归档该 accession directory 的 index.json。"
                )
            )
        rows.append(
            SecAccessionCoverageRow(
                ticker=ticker,
                cik=accession["cik"],
                accession_number=accession["accession_number"],
                metric_ids=tuple(accession["metric_ids"]),
                forms=tuple(accession["forms"]),
                metric_filed_dates=tuple(accession["filed_dates"]),
                submission_found=submission is not None,
                archive_index_found=archive_index_found,
                submission_form="" if submission is None else submission.get("form", ""),
                filing_date="" if submission is None else submission.get("filingDate", ""),
                report_date="" if submission is None else submission.get("reportDate", ""),
                accepted_time=(
                    "" if submission is None else submission.get("acceptanceDateTime", "")
                ),
                primary_document=(
                    "" if submission is None else submission.get("primaryDocument", "")
                ),
                archive_index_path=index_path,
                archive_index_sha256=_sha256_file(index_path)
                if archive_index_found
                else None,
                status=status,
            )
        )
    return SecAccessionCoverageReport(
        as_of=as_of,
        metrics_path=metrics_path,
        submissions_dir=submissions_dir,
        filing_archive_dir=filing_archive_dir,
        rows=tuple(rows),
        issues=tuple(issues),
    )


def render_sec_accession_coverage_report(report: SecAccessionCoverageReport) -> str:
    lines = [
        "# SEC Accession Archive 覆盖报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- SEC 指标 CSV：`{report.metrics_path}`",
        f"- Submissions 目录：`{report.submissions_dir}`",
        f"- Filing archive 目录：`{report.filing_archive_dir}`",
        f"- Accession 数：{report.accession_count}",
        f"- 已完整覆盖：{report.covered_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        f"- production_effect={report.production_effect}",
        "",
        "## 覆盖明细",
        "",
        "| Ticker | Accession | Metrics | Metric Filed Dates | Submission | "
        "Archive Index | Accepted Time | Primary Document | Status |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in sorted(report.rows, key=lambda item: (item.ticker, item.accession_number)):
        lines.append(
            "| "
            f"{row.ticker} | "
            f"{row.accession_number} | "
            f"{_escape_markdown_table(', '.join(row.metric_ids))} | "
            f"{_escape_markdown_table(', '.join(row.metric_filed_dates))} | "
            f"{'YES' if row.submission_found else 'NO'} | "
            f"{'YES' if row.archive_index_found else 'NO'} | "
            f"{row.accepted_time} | "
            f"{_escape_markdown_table(row.primary_document)} | "
            f"{row.status} |"
        )
    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(["| 级别 | Code | Ticker | Accession | 说明 |", "|---|---|---|---|---|"])
        for issue in report.issues:
            lines.append(
                "| "
                f"{issue.severity} | "
                f"{issue.code} | "
                f"{issue.ticker or ''} | "
                f"{issue.accession_number or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )
    lines.extend(
        [
            "",
            "## 方法边界",
            "",
            "- 本报告只验证 SEC 指标 CSV 中已使用 accession 的 submissions metadata 和 "
            "accession directory `index.json` 归档覆盖。",
            "- `filed_date <= as_of` 仍由 `validate-sec-metrics` 执行；本报告额外显示 "
            "`acceptanceDateTime` 供更严格 PIT 审计使用。",
            "- 第一版不默认下载全部 exhibit 或历史全量 filing；下载范围限制在当前指标实际用到的 "
            "accession number。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_sec_accession_coverage_report(
    report: SecAccessionCoverageReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_sec_accession_coverage_report(report), encoding="utf-8")
    return output_path


def default_sec_accession_coverage_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"sec_accession_coverage_{as_of.isoformat()}.md"


def _selected_companies(
    config: SecCompaniesConfig,
    tickers: list[str] | None,
) -> tuple[SecCompanyConfig, ...]:
    active_companies = [company for company in config.companies if company.active]
    if not tickers:
        return tuple(active_companies)
    requested = dedupe_preserving_order([ticker.upper() for ticker in tickers])
    by_ticker = {company.ticker: company for company in active_companies}
    missing = [ticker for ticker in requested if ticker not in by_ticker]
    if missing:
        raise ValueError(f"unknown or inactive SEC tickers: {', '.join(missing)}")
    return tuple(by_ticker[ticker] for ticker in requested)


def _metric_accessions(
    metrics_path: Path,
    as_of: date,
    tickers: list[str] | None,
) -> tuple[dict[str, object], ...]:
    rows = [
        row
        for row in load_sec_fundamental_metric_rows_csv(metrics_path)
        if row.as_of == as_of and row.accession_number
    ]
    if tickers:
        requested = set(dedupe_preserving_order([ticker.upper() for ticker in tickers]))
        rows = [row for row in rows if row.ticker in requested]
    by_key: dict[tuple[str, str], list[SecFundamentalMetricRow]] = {}
    for row in rows:
        by_key.setdefault((row.ticker, row.accession_number), []).append(row)

    records: list[dict[str, object]] = []
    for (ticker, accession_number), grouped_rows in sorted(by_key.items()):
        first = grouped_rows[0]
        records.append(
            {
                "ticker": ticker,
                "cik": first.cik,
                "accession_number": accession_number,
                "metric_ids": sorted({row.metric_id for row in grouped_rows}),
                "forms": sorted({row.form for row in grouped_rows if row.form}),
                "filed_dates": sorted(
                    {
                        row.filed_date.isoformat()
                        for row in grouped_rows
                        if row.filed_date is not None
                    }
                ),
            }
        )
    return tuple(records)


def _submission_records_by_accession(
    path: Path,
    *,
    ticker: str,
    issues: list[SecAccessionCoverageIssue],
) -> dict[str, dict[str, str]]:
    if not path.exists():
        issues.append(
            SecAccessionCoverageIssue(
                severity="ERROR",
                code="sec_submissions_file_missing",
                ticker=ticker,
                message=f"SEC submissions JSON 不存在：{path}",
            )
        )
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        issues.append(
            SecAccessionCoverageIssue(
                severity="ERROR",
                code="sec_submissions_file_unreadable",
                ticker=ticker,
                message=f"SEC submissions JSON 无法读取：{exc}",
            )
        )
        return {}
    return {
        record.get("accessionNumber", ""): record
        for record in _columnar_records(_recent_filings(data))
        if record.get("accessionNumber")
    }


def _recent_filings(data: dict[str, Any]) -> dict[str, Any]:
    filings = data.get("filings")
    if not isinstance(filings, dict):
        return {}
    recent = filings.get("recent")
    return recent if isinstance(recent, dict) else {}


def _columnar_records(columns: dict[str, Any]) -> tuple[dict[str, str], ...]:
    lengths = [len(value) for value in columns.values() if isinstance(value, list)]
    if not lengths:
        return ()
    row_count = max(lengths)
    records: list[dict[str, str]] = []
    for index in range(row_count):
        record: dict[str, str] = {}
        for key, values in columns.items():
            if isinstance(values, list) and index < len(values):
                record[key] = "" if values[index] is None else str(values[index])
        records.append(record)
    return tuple(records)


def _recent_filing_count(data: dict[str, Any]) -> int:
    return len(_columnar_records(_recent_filings(data)))


def _additional_submission_file_count(data: dict[str, Any]) -> int:
    filings = data.get("filings")
    if not isinstance(filings, dict):
        return 0
    files = filings.get("files")
    return len(files) if isinstance(files, list) else 0


def _filing_index_item_count(data: dict[str, Any]) -> int:
    directory = data.get("directory")
    if not isinstance(directory, dict):
        return 0
    items = directory.get("item")
    return len(items) if isinstance(items, list) else 0


def _filing_index_path(output_dir: Path, ticker: str, accession_number: str) -> Path:
    return output_dir / ticker.lower() / accession_number.replace("-", "") / "index.json"


def _provider_submissions_endpoint(provider: SecFilingArchiveProvider, cik: str) -> str:
    endpoint_for = getattr(provider, "submissions_endpoint_for", None)
    if callable(endpoint_for):
        return str(endpoint_for(cik))
    return f"provider:{provider.__class__.__name__}:submissions"


def _provider_filing_index_endpoint(
    provider: SecFilingArchiveProvider,
    cik: str,
    accession_number: str,
) -> str:
    endpoint_for = getattr(provider, "filing_index_endpoint_for", None)
    if callable(endpoint_for):
        return str(endpoint_for(cik, accession_number))
    return f"provider:{provider.__class__.__name__}:filing-index"


def _write_manifest(
    output_dir: Path,
    records: tuple[dict[str, object], ...],
    filename: str,
) -> Path:
    output_path = output_dir / filename
    new_frame = pd.DataFrame(records)
    if output_path.exists():
        existing = pd.read_csv(output_path)
        new_frame = pd.concat([existing, new_frame], ignore_index=True)
    new_frame.to_csv(output_path, index=False)
    return output_path


def _write_json(data: dict[str, Any], output_path: Path) -> None:
    output_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
