from __future__ import annotations

import csv
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from math import isfinite
from pathlib import Path
from typing import Any, Protocol, cast

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.valuation_sources import (
    FMP_ANALYST_ESTIMATE_PAGE,
    FMP_ANALYST_ESTIMATE_PERIOD,
    FMP_BASE_URL,
    FMP_SOURCE_NAME,
    FMP_SYMBOL_ALIASES,
    FmpHttpValuationProvider,
)

FMP_FORWARD_PIT_ENDPOINTS = (
    "analyst-estimates",
    "price-target-summary",
    "price-target-consensus",
    "grades",
    "grades-consensus",
    "ratings-snapshot",
    "earnings-calendar",
)
FMP_FORWARD_PIT_NORMALIZED_COLUMNS = (
    "normalized_id",
    "snapshot_id",
    "source_id",
    "source_name",
    "source_type",
    "endpoint",
    "endpoint_category",
    "canonical_ticker",
    "provider_symbol",
    "provider_symbol_alias",
    "as_of",
    "captured_at",
    "downloaded_at",
    "available_time",
    "vendor_date",
    "fiscal_period",
    "record_index",
    "normalized_values_json",
    "raw_payload_path",
    "raw_payload_sha256",
    "normalization_version",
    "point_in_time_class",
    "history_source_class",
    "backtest_use",
    "confidence_level",
    "confidence_reason",
)
FMP_FORWARD_PIT_SOURCE_ID = "fmp_valuation_expectations"
FMP_FORWARD_PIT_NORMALIZATION_VERSION = "fmp_forward_pit_v1"
DEFAULT_FMP_FORWARD_PIT_RAW_DIR = PROJECT_ROOT / "data" / "raw" / "fmp_forward_pit"
DEFAULT_FMP_FORWARD_PIT_NORMALIZED_DIR = (
    PROJECT_ROOT / "data" / "processed" / "pit_snapshots"
)


class FmpForwardPitIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class FmpForwardPitIssue:
    severity: FmpForwardPitIssueSeverity
    code: str
    message: str
    ticker: str | None = None
    endpoint: str | None = None


@dataclass(frozen=True)
class FmpForwardPitRawPayload:
    ticker: str
    as_of: date
    captured_at: date
    downloaded_at: datetime
    provider_symbol: str
    endpoint_records: dict[str, tuple[dict[str, Any], ...]]
    request_parameters_by_endpoint: dict[str, dict[str, object]]
    checksum_sha256: str
    source_path: Path | None = None

    @property
    def row_count(self) -> int:
        return sum(len(records) for records in self.endpoint_records.values())


@dataclass(frozen=True)
class FmpForwardPitNormalizedRow:
    normalized_id: str
    snapshot_id: str
    source_id: str
    source_name: str
    source_type: str
    endpoint: str
    endpoint_category: str
    canonical_ticker: str
    provider_symbol: str
    provider_symbol_alias: str
    as_of: str
    captured_at: str
    downloaded_at: str
    available_time: str
    vendor_date: str
    fiscal_period: str
    record_index: int
    normalized_values_json: str
    raw_payload_path: str
    raw_payload_sha256: str
    normalization_version: str
    point_in_time_class: str
    history_source_class: str
    backtest_use: str
    confidence_level: str
    confidence_reason: str


@dataclass(frozen=True)
class FmpForwardPitFetchReport:
    as_of: date
    captured_at: date
    downloaded_at: datetime
    requested_tickers: tuple[str, ...]
    provider_symbols: tuple[str, ...]
    analyst_estimate_limit: int
    earnings_calendar_from: date
    earnings_calendar_to: date
    raw_payloads: tuple[FmpForwardPitRawPayload, ...]
    normalized_rows: tuple[FmpForwardPitNormalizedRow, ...]
    row_count: int
    checksum_sha256: str
    issues: tuple[FmpForwardPitIssue, ...] = field(default_factory=tuple)
    source_name: str = FMP_SOURCE_NAME
    source_type: str = "paid_vendor"

    @property
    def raw_payload_count(self) -> int:
        return len(self.raw_payloads)

    @property
    def normalized_row_count(self) -> int:
        if self.normalized_rows:
            return len(self.normalized_rows)
        return self.row_count if self.raw_payloads else 0

    @property
    def error_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == FmpForwardPitIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == FmpForwardPitIssueSeverity.WARNING
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


class FmpForwardPitProvider(Protocol):
    def fetch_analyst_estimates(
        self,
        ticker: str,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Fetch analyst estimate records for one ticker."""

    def fetch_price_target_summary(self, ticker: str) -> list[dict[str, Any]]:
        """Fetch price target summary rows for one ticker."""

    def fetch_price_target_consensus(self, ticker: str) -> list[dict[str, Any]]:
        """Fetch price target consensus rows for one ticker."""

    def fetch_grades(self, ticker: str) -> list[dict[str, Any]]:
        """Fetch current analyst grade action rows for one ticker."""

    def fetch_grades_consensus(self, ticker: str) -> list[dict[str, Any]]:
        """Fetch analyst grades consensus rows for one ticker."""

    def fetch_ratings_snapshot(self, ticker: str) -> list[dict[str, Any]]:
        """Fetch ratings snapshot rows for one ticker."""

    def fetch_earnings_calendar(
        self,
        *,
        from_date: date,
        to_date: date,
    ) -> list[dict[str, Any]]:
        """Fetch earnings calendar rows for a date window."""


def fetch_fmp_forward_pit_snapshots(
    tickers: list[str] | tuple[str, ...],
    api_key: str,
    as_of: date,
    *,
    provider: FmpForwardPitProvider | None = None,
    captured_at: date | None = None,
    downloaded_at: datetime | None = None,
    analyst_estimate_limit: int = 10,
    earnings_calendar_lookback_days: int = 7,
    earnings_calendar_forward_days: int = 90,
    include_normalized_rows: bool = True,
) -> FmpForwardPitFetchReport:
    normalized_tickers = tuple(_normalize_tickers(tickers))
    if not normalized_tickers:
        raise ValueError("FMP forward PIT fetch requires at least one ticker")
    if not api_key.strip():
        raise ValueError("FMP API key must not be empty")
    if analyst_estimate_limit < 2:
        raise ValueError("analyst_estimate_limit must be at least 2")
    if earnings_calendar_lookback_days < 0:
        raise ValueError("earnings_calendar_lookback_days must be non-negative")
    if earnings_calendar_forward_days < 0:
        raise ValueError("earnings_calendar_forward_days must be non-negative")

    fetch_provider = provider or cast(FmpForwardPitProvider, FmpHttpValuationProvider(api_key))
    download_time = downloaded_at or datetime.now(tz=UTC)
    fetch_date = captured_at or download_time.date()
    calendar_from = as_of - timedelta(days=earnings_calendar_lookback_days)
    calendar_to = as_of + timedelta(days=earnings_calendar_forward_days)
    issues: list[FmpForwardPitIssue] = []
    raw_payloads: list[FmpForwardPitRawPayload] = []
    raw_payload_for_checksum: dict[str, dict[str, list[dict[str, Any]]]] = {}
    calendar_records = _fetch_earnings_calendar_records(
        fetch_provider,
        from_date=calendar_from,
        to_date=calendar_to,
        issues=issues,
    )

    for ticker in normalized_tickers:
        provider_symbol = _fmp_provider_symbol(ticker)
        try:
            endpoint_records = {
                "analyst-estimates": tuple(
                    fetch_provider.fetch_analyst_estimates(
                        provider_symbol,
                        period=FMP_ANALYST_ESTIMATE_PERIOD,
                        limit=analyst_estimate_limit,
                    )
                ),
                "price-target-summary": tuple(
                    fetch_provider.fetch_price_target_summary(provider_symbol)
                ),
                "price-target-consensus": tuple(
                    fetch_provider.fetch_price_target_consensus(provider_symbol)
                ),
                "grades": tuple(fetch_provider.fetch_grades(provider_symbol)),
                "grades-consensus": tuple(
                    fetch_provider.fetch_grades_consensus(provider_symbol)
                ),
                "ratings-snapshot": tuple(
                    fetch_provider.fetch_ratings_snapshot(provider_symbol)
                ),
                "earnings-calendar": tuple(
                    _calendar_records_for_symbol(calendar_records, provider_symbol)
                ),
            }
        except Exception as exc:
            issues.append(
                FmpForwardPitIssue(
                    severity=FmpForwardPitIssueSeverity.ERROR,
                    code="fmp_forward_pit_request_failed",
                    ticker=ticker,
                    message=f"FMP forward PIT 请求失败：{_sanitize_fmp_error_message(exc)}",
                )
            )
            continue

        raw_payload_for_checksum[ticker] = {
            endpoint: list(records) for endpoint, records in endpoint_records.items()
        }
        request_parameters = _request_parameters_by_endpoint(
            provider_symbol=provider_symbol,
            analyst_estimate_limit=analyst_estimate_limit,
            calendar_from=calendar_from,
            calendar_to=calendar_to,
        )
        raw_payloads.append(
            FmpForwardPitRawPayload(
                ticker=ticker,
                as_of=as_of,
                captured_at=fetch_date,
                downloaded_at=download_time,
                provider_symbol=provider_symbol,
                endpoint_records=endpoint_records,
                request_parameters_by_endpoint=request_parameters,
                checksum_sha256=_json_checksum(endpoint_records),
            )
        )

    normalized_rows = (
        tuple(_normalize_fmp_forward_pit_payloads(raw_payloads))
        if include_normalized_rows
        else tuple()
    )
    return FmpForwardPitFetchReport(
        as_of=as_of,
        captured_at=fetch_date,
        downloaded_at=download_time,
        requested_tickers=normalized_tickers,
        provider_symbols=tuple(_fmp_provider_symbol(ticker) for ticker in normalized_tickers),
        analyst_estimate_limit=analyst_estimate_limit,
        earnings_calendar_from=calendar_from,
        earnings_calendar_to=calendar_to,
        raw_payloads=tuple(raw_payloads),
        normalized_rows=normalized_rows,
        row_count=sum(payload.row_count for payload in raw_payloads),
        checksum_sha256=_json_checksum(raw_payload_for_checksum),
        issues=tuple(issues),
    )


def build_fmp_forward_pit_failure_report(
    tickers: list[str] | tuple[str, ...],
    as_of: date,
    *,
    code: str,
    message: str,
    captured_at: date | None = None,
    downloaded_at: datetime | None = None,
    analyst_estimate_limit: int = 10,
    earnings_calendar_lookback_days: int = 7,
    earnings_calendar_forward_days: int = 90,
) -> FmpForwardPitFetchReport:
    normalized_tickers = tuple(_normalize_tickers(tickers))
    download_time = downloaded_at or datetime.now(tz=UTC)
    fetch_date = captured_at or download_time.date()
    calendar_from = as_of - timedelta(days=earnings_calendar_lookback_days)
    calendar_to = as_of + timedelta(days=earnings_calendar_forward_days)
    issue = FmpForwardPitIssue(
        severity=FmpForwardPitIssueSeverity.ERROR,
        code=code,
        message=message,
    )
    return FmpForwardPitFetchReport(
        as_of=as_of,
        captured_at=fetch_date,
        downloaded_at=download_time,
        requested_tickers=normalized_tickers,
        provider_symbols=tuple(_fmp_provider_symbol(ticker) for ticker in normalized_tickers),
        analyst_estimate_limit=analyst_estimate_limit,
        earnings_calendar_from=calendar_from,
        earnings_calendar_to=calendar_to,
        raw_payloads=tuple(),
        normalized_rows=tuple(),
        row_count=0,
        checksum_sha256=_json_checksum({"issues": [issue.__dict__]}),
        issues=(issue,),
    )


def write_fmp_forward_pit_raw_payloads(
    payloads: tuple[FmpForwardPitRawPayload, ...] | list[FmpForwardPitRawPayload],
    output_dir: Path | str,
) -> tuple[Path, ...]:
    directory = Path(output_dir)
    written: list[Path] = []
    for payload in payloads:
        ticker_dir = directory / payload.ticker.lower()
        ticker_dir.mkdir(parents=True, exist_ok=True)
        output_path = ticker_dir / (
            f"fmp_forward_pit_{payload.ticker.lower()}_"
            f"{_timestamp_token(payload.downloaded_at)}.json"
        )
        _write_json_payload(output_path, _fmp_forward_pit_payload_to_raw(payload))
        written.append(output_path)
    return tuple(written)


def write_fmp_forward_pit_normalized_csv(
    rows: tuple[FmpForwardPitNormalizedRow, ...] | list[FmpForwardPitNormalizedRow],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(FMP_FORWARD_PIT_NORMALIZED_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow({column: getattr(row, column) for column in writer.fieldnames})
    return output_path


def write_fmp_forward_pit_normalized_csv_for_payloads(
    rows: tuple[FmpForwardPitNormalizedRow, ...] | list[FmpForwardPitNormalizedRow],
    payloads: tuple[FmpForwardPitRawPayload, ...] | list[FmpForwardPitRawPayload],
    output_path: Path,
) -> Path:
    payload_by_ticker = {payload.ticker: payload for payload in payloads}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(FMP_FORWARD_PIT_NORMALIZED_COLUMNS))
        writer.writeheader()
        for row in rows:
            payload = payload_by_ticker.get(row.canonical_ticker)
            writer.writerow(_normalized_row_csv_record(row, payload))
    return output_path


def write_fmp_forward_pit_normalized_csv_from_payloads(
    payloads: tuple[FmpForwardPitRawPayload, ...] | list[FmpForwardPitRawPayload],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(FMP_FORWARD_PIT_NORMALIZED_COLUMNS))
        writer.writeheader()
        for payload in payloads:
            for record in _normalized_csv_records_for_payload(payload):
                writer.writerow(record)
    return output_path


def normalize_fmp_forward_pit_payloads(
    payloads: tuple[FmpForwardPitRawPayload, ...] | list[FmpForwardPitRawPayload],
) -> tuple[FmpForwardPitNormalizedRow, ...]:
    return tuple(_normalize_fmp_forward_pit_payloads(payloads))


def retarget_fmp_forward_pit_normalized_rows(
    rows: tuple[FmpForwardPitNormalizedRow, ...] | list[FmpForwardPitNormalizedRow],
    payloads: tuple[FmpForwardPitRawPayload, ...] | list[FmpForwardPitRawPayload],
) -> tuple[FmpForwardPitNormalizedRow, ...]:
    payload_by_ticker = {payload.ticker: payload for payload in payloads}
    ordered_payloads: list[FmpForwardPitRawPayload] = []
    passthrough_rows: list[FmpForwardPitNormalizedRow] = []
    seen_payload_tickers: set[str] = set()
    for row in rows:
        payload = payload_by_ticker.get(row.canonical_ticker)
        if payload is None:
            passthrough_rows.append(row)
            continue
        if payload.ticker not in seen_payload_tickers:
            ordered_payloads.append(payload)
            seen_payload_tickers.add(payload.ticker)
    return (
        *tuple(_normalize_fmp_forward_pit_payloads(ordered_payloads)),
        *passthrough_rows,
    )


def _normalized_row_csv_record(
    row: FmpForwardPitNormalizedRow,
    payload: FmpForwardPitRawPayload | None,
) -> dict[str, object]:
    record = {column: getattr(row, column) for column in FMP_FORWARD_PIT_NORMALIZED_COLUMNS}
    if payload is None:
        return record
    raw_path = (
        payload.source_path.as_posix()
        if payload.source_path is not None
        else _default_raw_payload_path(payload).as_posix()
    )
    snapshot_id = _snapshot_id(payload)
    record["snapshot_id"] = snapshot_id
    record["normalized_id"] = _id_token(f"{snapshot_id}_{row.endpoint}_{row.record_index}")
    record["raw_payload_path"] = raw_path
    record["raw_payload_sha256"] = payload.checksum_sha256
    return record


def _normalized_csv_records_for_payload(
    payload: FmpForwardPitRawPayload,
) -> list[dict[str, object]]:
    records_for_payload: list[dict[str, object]] = []
    raw_path = (
        payload.source_path.as_posix()
        if payload.source_path is not None
        else _default_raw_payload_path(payload).as_posix()
    )
    snapshot_id = _snapshot_id(payload)
    provider_symbol_alias = _provider_symbol_alias(payload.ticker, payload.provider_symbol)
    as_of = payload.as_of.isoformat()
    captured_at = payload.captured_at.isoformat()
    downloaded_at = payload.downloaded_at.isoformat()
    for endpoint in FMP_FORWARD_PIT_ENDPOINTS:
        records = payload.endpoint_records.get(endpoint, ())
        for index in range(len(records)):
            record = records[index]
            if not isinstance(record, dict):
                continue
            records_for_payload.append(
                {
                    "normalized_id": _id_token(f"{snapshot_id}_{endpoint}_{index}"),
                    "snapshot_id": snapshot_id,
                    "source_id": FMP_FORWARD_PIT_SOURCE_ID,
                    "source_name": FMP_SOURCE_NAME,
                    "source_type": "paid_vendor",
                    "endpoint": f"{FMP_BASE_URL}/{endpoint}",
                    "endpoint_category": _endpoint_category(endpoint),
                    "canonical_ticker": payload.ticker,
                    "provider_symbol": payload.provider_symbol,
                    "provider_symbol_alias": provider_symbol_alias,
                    "as_of": as_of,
                    "captured_at": captured_at,
                    "downloaded_at": downloaded_at,
                    "available_time": downloaded_at,
                    "vendor_date": _vendor_date(record),
                    "fiscal_period": _fiscal_period(record),
                    "record_index": index,
                    "normalized_values_json": _json_dumps(_safe_scalar_values(record)),
                    "raw_payload_path": raw_path,
                    "raw_payload_sha256": payload.checksum_sha256,
                    "normalization_version": FMP_FORWARD_PIT_NORMALIZATION_VERSION,
                    "point_in_time_class": "captured_snapshot",
                    "history_source_class": "captured_snapshot_history",
                    "backtest_use": "captured_at_forward_only",
                    "confidence_level": "medium",
                    "confidence_reason": (
                        "FMP forward-only raw snapshot captured by local system; "
                        "available_time equals downloaded_at."
                    ),
                }
            )
    return records_for_payload


def attach_fmp_forward_pit_report_artifacts(
    report: FmpForwardPitFetchReport,
    *,
    raw_payloads: tuple[FmpForwardPitRawPayload, ...],
    normalized_rows: tuple[FmpForwardPitNormalizedRow, ...],
) -> FmpForwardPitFetchReport:
    return FmpForwardPitFetchReport(
        as_of=report.as_of,
        captured_at=report.captured_at,
        downloaded_at=report.downloaded_at,
        requested_tickers=report.requested_tickers,
        provider_symbols=report.provider_symbols,
        analyst_estimate_limit=report.analyst_estimate_limit,
        earnings_calendar_from=report.earnings_calendar_from,
        earnings_calendar_to=report.earnings_calendar_to,
        raw_payloads=raw_payloads,
        normalized_rows=normalized_rows,
        row_count=report.row_count,
        checksum_sha256=report.checksum_sha256,
        issues=report.issues,
        source_name=report.source_name,
        source_type=report.source_type,
    )


def render_fmp_forward_pit_fetch_report(report: FmpForwardPitFetchReport) -> str:
    alias_summary = _fmp_symbol_alias_summary(report.requested_tickers)
    lines = [
        "# FMP Forward-only PIT 快照抓取报告",
        "",
        f"- 状态：{report.status}",
        f"- 来源：{report.source_name}",
        f"- 来源类型：{report.source_type}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 采集日期：{report.captured_at.isoformat()}",
        f"- 下载时间：{report.downloaded_at.isoformat()}",
        f"- 请求标的：{', '.join(report.requested_tickers)}",
        f"- Provider symbols：{', '.join(report.provider_symbols)}",
        f"- Provider symbol aliases：{alias_summary or '无'}",
        f"- Endpoint：{', '.join(FMP_FORWARD_PIT_ENDPOINTS)}",
        (
            "- 请求参数：analyst-estimates period=annual, page=0, "
            f"limit={report.analyst_estimate_limit}; earnings-calendar "
            f"from={report.earnings_calendar_from.isoformat()}, "
            f"to={report.earnings_calendar_to.isoformat()}"
        ),
        f"- 原始 payload 数：{report.raw_payload_count}",
        f"- 原始记录数：{report.row_count}",
        f"- 标准化行数：{report.normalized_row_count}",
        f"- SHA256：`{report.checksum_sha256}`",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        "",
        "## Endpoint 覆盖",
        "",
        "| Ticker | Endpoint | Rows |",
        "|---|---|---:|",
    ]
    for payload in report.raw_payloads:
        for endpoint in FMP_FORWARD_PIT_ENDPOINTS:
            lines.append(
                "| "
                f"{payload.ticker} | "
                f"{endpoint} | "
                f"{len(payload.endpoint_records.get(endpoint, ()))} |"
            )

    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(["| 级别 | Code | Ticker | Endpoint | 说明 |",
                      "|---|---|---|---|---|"])
        for issue in report.issues:
            level = "错误" if issue.severity == FmpForwardPitIssueSeverity.ERROR else "警告"
            lines.append(
                "| "
                f"{level} | "
                f"{issue.code} | "
                f"{issue.ticker or ''} | "
                f"{issue.endpoint or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 本命令只建立 forward-only 自建 PIT 归档，不改变当前评分语义。",
            (
                "- `available_time` 等于本系统本次下载写入时间；缺跑日期不能"
                "事后补写成 strict PIT。"
            ),
            (
                "- 原始 payload 写入 `data/raw/fmp_forward_pit/`，标准化 as-of "
                "索引写入 `data/processed/pit_snapshots/`。"
            ),
            (
                "- 下游评分、回测或报告若使用这些数据，必须通过 "
                "`available_time <= decision_time` 查询。"
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def write_fmp_forward_pit_fetch_report(
    report: FmpForwardPitFetchReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_fmp_forward_pit_fetch_report(report), encoding="utf-8")
    return output_path


def default_fmp_forward_pit_fetch_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"fmp_forward_pit_fetch_{as_of.isoformat()}.md"


def default_fmp_forward_pit_normalized_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"fmp_forward_pit_{as_of.isoformat()}.csv"


def _fetch_earnings_calendar_records(
    provider: FmpForwardPitProvider,
    *,
    from_date: date,
    to_date: date,
    issues: list[FmpForwardPitIssue],
) -> tuple[dict[str, Any], ...]:
    try:
        return tuple(provider.fetch_earnings_calendar(from_date=from_date, to_date=to_date))
    except Exception as exc:
        issues.append(
            FmpForwardPitIssue(
                severity=FmpForwardPitIssueSeverity.ERROR,
                code="fmp_forward_pit_earnings_calendar_failed",
                endpoint="earnings-calendar",
                message=f"FMP earnings-calendar 请求失败：{_sanitize_fmp_error_message(exc)}",
            )
        )
        return tuple()


def _calendar_records_for_symbol(
    records: tuple[dict[str, Any], ...],
    provider_symbol: str,
) -> tuple[dict[str, Any], ...]:
    return tuple(
        record
        for record in records
        if str(record.get("symbol") or record.get("ticker") or "").upper()
        == provider_symbol.upper()
    )


def _request_parameters_by_endpoint(
    *,
    provider_symbol: str,
    analyst_estimate_limit: int,
    calendar_from: date,
    calendar_to: date,
) -> dict[str, dict[str, object]]:
    return {
        "analyst-estimates": {
            "symbol": provider_symbol,
            "period": FMP_ANALYST_ESTIMATE_PERIOD,
            "page": FMP_ANALYST_ESTIMATE_PAGE,
            "limit": analyst_estimate_limit,
        },
        "price-target-summary": {"symbol": provider_symbol},
        "price-target-consensus": {"symbol": provider_symbol},
        "grades": {"symbol": provider_symbol},
        "grades-consensus": {"symbol": provider_symbol},
        "ratings-snapshot": {"symbol": provider_symbol},
        "earnings-calendar": {
            "from": calendar_from.isoformat(),
            "to": calendar_to.isoformat(),
            "filtered_symbol": provider_symbol,
        },
    }


def _fmp_forward_pit_payload_to_raw(payload: FmpForwardPitRawPayload) -> dict[str, Any]:
    checksum = payload.checksum_sha256 or _json_checksum(payload.endpoint_records)
    return {
        "provider": FMP_SOURCE_NAME,
        "source_type": "paid_vendor",
        "ticker": payload.ticker,
        "provider_symbol": payload.provider_symbol,
        "as_of": payload.as_of.isoformat(),
        "captured_at": payload.captured_at.isoformat(),
        "downloaded_at": payload.downloaded_at.isoformat(),
        "endpoints": [f"{FMP_BASE_URL}/{endpoint}" for endpoint in payload.endpoint_records],
        "request_parameters_by_endpoint": payload.request_parameters_by_endpoint,
        "row_count": payload.row_count,
        "checksum_sha256": checksum,
        "records_by_endpoint": {
            endpoint: list(records)
            for endpoint, records in payload.endpoint_records.items()
        },
    }


def _write_json_payload(output_path: Path, payload: Mapping[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as file:
        _write_json_value(file, payload, indent_level=0)
        file.write("\n")


def _write_json_value(file: Any, value: Any, *, indent_level: int) -> None:
    if isinstance(value, Mapping):
        items = list(value.items())
        if not items:
            file.write("{}")
            return
        file.write("{\n")
        for index, (key, item_value) in enumerate(items):
            file.write(" " * (indent_level + 2))
            file.write(json.dumps(str(key), ensure_ascii=False))
            file.write(": ")
            _write_json_value(file, item_value, indent_level=indent_level + 2)
            file.write(",\n" if index < len(items) - 1 else "\n")
        file.write(" " * indent_level)
        file.write("}")
        return
    if isinstance(value, (list, tuple)):
        if not value:
            file.write("[]")
            return
        file.write("[\n")
        for index, item_value in enumerate(value):
            file.write(" " * (indent_level + 2))
            _write_json_value(file, item_value, indent_level=indent_level + 2)
            file.write(",\n" if index < len(value) - 1 else "\n")
        file.write(" " * indent_level)
        file.write("]")
        return
    if isinstance(value, str):
        file.write(json.dumps(value, ensure_ascii=False))
        return
    if value is None:
        file.write("null")
        return
    if isinstance(value, bool):
        file.write("true" if value else "false")
        return
    if isinstance(value, int):
        file.write(str(value))
        return
    if isinstance(value, float):
        file.write(str(value) if isfinite(value) else json.dumps(value))
        return
    file.write(json.dumps(str(value), ensure_ascii=False))


def _normalize_fmp_forward_pit_payloads(
    payloads: tuple[FmpForwardPitRawPayload, ...] | list[FmpForwardPitRawPayload],
) -> list[FmpForwardPitNormalizedRow]:
    rows: list[FmpForwardPitNormalizedRow] = []
    for payload in payloads:
        raw_path = (
            payload.source_path.as_posix()
            if payload.source_path is not None
            else _default_raw_payload_path(payload).as_posix()
        )
        snapshot_id = _snapshot_id(payload)
        for endpoint in FMP_FORWARD_PIT_ENDPOINTS:
            records = payload.endpoint_records.get(endpoint, ())
            for index in range(len(records)):
                record = records[index]
                if not isinstance(record, dict):
                    continue
                normalized_id = _id_token(f"{snapshot_id}_{endpoint}_{index}")
                rows.append(
                    FmpForwardPitNormalizedRow(
                        normalized_id=normalized_id,
                        snapshot_id=snapshot_id,
                        source_id=FMP_FORWARD_PIT_SOURCE_ID,
                        source_name=FMP_SOURCE_NAME,
                        source_type="paid_vendor",
                        endpoint=f"{FMP_BASE_URL}/{endpoint}",
                        endpoint_category=_endpoint_category(endpoint),
                        canonical_ticker=payload.ticker,
                        provider_symbol=payload.provider_symbol,
                        provider_symbol_alias=_provider_symbol_alias(
                            payload.ticker,
                            payload.provider_symbol,
                        ),
                        as_of=payload.as_of.isoformat(),
                        captured_at=payload.captured_at.isoformat(),
                        downloaded_at=payload.downloaded_at.isoformat(),
                        available_time=payload.downloaded_at.isoformat(),
                        vendor_date=_vendor_date(record),
                        fiscal_period=_fiscal_period(record),
                        record_index=index,
                        normalized_values_json=_json_dumps(_safe_scalar_values(record)),
                        raw_payload_path=raw_path,
                        raw_payload_sha256=payload.checksum_sha256,
                        normalization_version=FMP_FORWARD_PIT_NORMALIZATION_VERSION,
                        point_in_time_class="captured_snapshot",
                        history_source_class="captured_snapshot_history",
                        backtest_use="captured_at_forward_only",
                        confidence_level="medium",
                        confidence_reason=(
                            "FMP forward-only raw snapshot captured by local system; "
                            "available_time equals downloaded_at."
                        ),
                    )
                )
    return rows


def attach_fmp_forward_pit_raw_paths(
    payloads: tuple[FmpForwardPitRawPayload, ...],
    paths: tuple[Path, ...],
    *,
    project_root: Path = PROJECT_ROOT,
) -> tuple[FmpForwardPitRawPayload, ...]:
    path_by_ticker = {
        _ticker_from_forward_pit_path(path): path for path in paths
    }
    attached: list[FmpForwardPitRawPayload] = []
    for payload in payloads:
        path = path_by_ticker.get(payload.ticker)
        if path is None:
            attached.append(payload)
            continue
        attached.append(
            FmpForwardPitRawPayload(
                ticker=payload.ticker,
                as_of=payload.as_of,
                captured_at=payload.captured_at,
                downloaded_at=payload.downloaded_at,
                provider_symbol=payload.provider_symbol,
                endpoint_records=payload.endpoint_records,
                request_parameters_by_endpoint=payload.request_parameters_by_endpoint,
                checksum_sha256=sha256(path.read_bytes()).hexdigest(),
                source_path=_display_path(path, project_root),
            )
        )
    return tuple(attached)


def _ticker_from_forward_pit_path(path: Path) -> str:
    parts = path.stem.split("_")
    return parts[3].upper() if len(parts) >= 4 else path.parent.name.upper()


def _default_raw_payload_path(payload: FmpForwardPitRawPayload) -> Path:
    return (
        DEFAULT_FMP_FORWARD_PIT_RAW_DIR
        / payload.ticker.lower()
        / f"fmp_forward_pit_{payload.ticker.lower()}_{_timestamp_token(payload.downloaded_at)}.json"
    )


def _snapshot_id(payload: FmpForwardPitRawPayload) -> str:
    return _id_token(
        "fmp_forward_pit_"
        f"{payload.ticker}_{_timestamp_token(payload.downloaded_at)}_"
        f"{payload.checksum_sha256[:12]}"
    )


def _endpoint_category(endpoint: str) -> str:
    if endpoint == "analyst-estimates":
        return "analyst_estimates"
    if endpoint.startswith("price-target"):
        return "price_target"
    if endpoint in {"grades", "grades-consensus", "ratings-snapshot"}:
        return "ratings"
    if endpoint == "earnings-calendar":
        return "earnings_calendar"
    return endpoint.replace("-", "_")


def _safe_scalar_values(record: dict[str, Any]) -> dict[str, Any]:
    scalar_values: dict[str, Any] = {}
    for key, value in record.items():
        if type(key) is not str:
            continue
        if value is None or type(value) in {str, int, float, bool}:
            scalar_values[key] = value
    return {key: scalar_values[key] for key in sorted(scalar_values)}


def _vendor_date(record: dict[str, Any]) -> str:
    for key in (
        "date",
        "publishedDate",
        "calendarDate",
        "reportDate",
        "fillingDate",
        "acceptedDate",
    ):
        value = record.get(key)
        if value:
            return str(value)
    return "not_recorded"


def _fiscal_period(record: dict[str, Any]) -> str:
    for key in ("period", "fiscalPeriod", "fiscalQuarter"):
        value = record.get(key)
        if value:
            year = str(record.get("fiscalYear") or record.get("year") or "").strip()
            return f"{year}-{value}" if year else str(value)
    year = record.get("fiscalYear") or record.get("year")
    return str(year) if year else "not_recorded"


def _normalize_tickers(tickers: list[str] | tuple[str, ...]) -> list[str]:
    normalized: list[str] = []
    for ticker in tickers:
        value = ticker.strip().upper()
        if value and value not in normalized:
            normalized.append(value)
    return normalized


def _fmp_provider_symbol(ticker: str) -> str:
    normalized = ticker.strip().upper()
    return FMP_SYMBOL_ALIASES.get(normalized, normalized)


def _provider_symbol_alias(ticker: str, provider_symbol: str) -> str:
    return f"{ticker}->{provider_symbol}" if ticker != provider_symbol else "none"


def _fmp_symbol_alias_summary(tickers: tuple[str, ...]) -> str:
    aliases = [
        _provider_symbol_alias(ticker, _fmp_provider_symbol(ticker))
        for ticker in tickers
        if ticker != _fmp_provider_symbol(ticker)
    ]
    return ", ".join(aliases)


def _json_checksum(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)
    return sha256(payload.encode("utf-8")).hexdigest()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _timestamp_token(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")


def _id_token(value: str) -> str:
    raw_text = str(value)
    digest = sha256(raw_text.encode("utf-8", "backslashreplace")).hexdigest()[:12]
    ascii_text = raw_text.encode("ascii", "ignore").decode("ascii")
    token = re.sub(r"[^0-9A-Za-z]+", "_", ascii_text).strip("_").lower()
    token = re.sub(r"_+", "_", token)[:140].rstrip("_")
    if not token:
        return f"id_{digest}"
    return f"{token}_{digest}"


def _display_path(path: Path, project_root: Path) -> Path:
    try:
        return Path(path.resolve().relative_to(project_root.resolve()).as_posix())
    except ValueError:
        return path


def sanitize_fmp_forward_pit_error_message(exc: Exception | str) -> str:
    message = str(exc)
    return re.sub(
        r"(apikey=)[^&\s]+",
        r"\1<redacted>",
        message,
        flags=re.IGNORECASE,
    )


def _sanitize_fmp_error_message(exc: Exception) -> str:
    return sanitize_fmp_forward_pit_error_message(exc)


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
