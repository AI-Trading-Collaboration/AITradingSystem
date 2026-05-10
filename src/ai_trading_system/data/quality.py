from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pandas as pd

from ai_trading_system.config import (
    DataQualityConfig,
    KnownSplitEventConfig,
    PriceQualityConfig,
    RateQualityConfig,
)

PRICE_REQUIRED_COLUMNS = ("date", "ticker", "open", "high", "low", "close", "adj_close", "volume")
RATE_REQUIRED_COLUMNS = ("date", "series", "value")
MANIFEST_REQUIRED_COLUMNS = (
    "downloaded_at",
    "source_id",
    "provider",
    "endpoint",
    "request_parameters",
    "output_path",
    "row_count",
    "checksum_sha256",
)


class Severity(StrEnum):
    INFO = "INFO"
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class DataQualityIssue:
    severity: Severity
    code: str
    message: str
    rows: int | None = None
    sample: str | None = None
    source: str | None = None


@dataclass(frozen=True)
class DataFileSummary:
    path: Path
    exists: bool
    rows: int = 0
    sha256: str | None = None
    min_date: date | None = None
    max_date: date | None = None


@dataclass(frozen=True)
class MarketstackReconciliationRecord:
    date: str
    ticker: str
    severity: Severity
    classification: str
    rule_id: str
    evidence: str
    primary_close: float | None = None
    secondary_close: float | None = None
    close_diff_pct: float | None = None
    primary_adj_close: float | None = None
    secondary_adj_close: float | None = None
    adj_close_diff_pct: float | None = None


@dataclass(frozen=True)
class DataQualityReport:
    checked_at: datetime
    as_of: date
    price_summary: DataFileSummary
    rate_summary: DataFileSummary
    expected_price_tickers: tuple[str, ...]
    expected_rate_series: tuple[str, ...]
    secondary_price_summary: DataFileSummary | None = None
    manifest_summary: DataFileSummary | None = None
    price_consistency_start_date: date | None = None
    rate_consistency_start_date: date | None = None
    marketstack_reconciliation_records: tuple[MarketstackReconciliationRecord, ...] = ()
    issues: tuple[DataQualityIssue, ...] = field(default_factory=tuple)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == Severity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == Severity.WARNING)

    @property
    def info_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == Severity.INFO)

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


def validate_data_cache(
    prices_path: Path,
    rates_path: Path,
    expected_price_tickers: list[str],
    expected_rate_series: list[str],
    quality_config: DataQualityConfig,
    as_of: date,
    manifest_path: Path | None = None,
    secondary_prices_path: Path | None = None,
    require_secondary_prices: bool = False,
) -> DataQualityReport:
    issues: list[DataQualityIssue] = []
    marketstack_reconciliation_records: tuple[MarketstackReconciliationRecord, ...] = ()

    prices, price_summary = _read_csv(prices_path, issues, "prices")
    rates, rate_summary = _read_csv(rates_path, issues, "rates")
    secondary_prices, secondary_price_summary = _read_secondary_prices_csv(
        secondary_prices_path,
        issues,
        required=require_secondary_prices,
    )
    manifest_summary = (
        _validate_download_manifest(
            manifest_path,
            price_summary=price_summary,
            rate_summary=rate_summary,
            secondary_price_summary=secondary_price_summary,
            issues=issues,
        )
        if manifest_path is not None
        else None
    )

    if prices is not None:
        price_summary = _validate_prices(
            prices,
            price_summary,
            expected_price_tickers,
            quality_config,
            as_of,
            issues,
            source="价格主源",
        )

    if secondary_prices is not None and secondary_price_summary is not None:
        secondary_expected_tickers = _secondary_expected_price_tickers(
            expected_price_tickers,
            quality_config,
        )
        secondary_price_summary = _validate_prices(
            secondary_prices,
            secondary_price_summary,
            secondary_expected_tickers,
            quality_config,
            as_of,
            issues,
            source="第二行情源 Marketstack",
            error_severity=_secondary_price_self_check_error_severity(quality_config),
        )
        if prices is not None:
            marketstack_reconciliation_records = _check_secondary_price_reconciliation(
                primary_prices=prices,
                secondary_prices=secondary_prices,
                expected_tickers=secondary_expected_tickers,
                quality_config=quality_config,
                required=require_secondary_prices,
                issues=issues,
            )

    if rates is not None:
        rate_summary = _validate_rates(
            rates,
            rate_summary,
            expected_rate_series,
            quality_config,
            as_of,
            issues,
        )

    return DataQualityReport(
        checked_at=datetime.now(UTC),
        as_of=as_of,
        price_summary=price_summary,
        rate_summary=rate_summary,
        expected_price_tickers=tuple(expected_price_tickers),
        expected_rate_series=tuple(expected_rate_series),
        secondary_price_summary=secondary_price_summary,
        manifest_summary=manifest_summary,
        price_consistency_start_date=quality_config.prices.consistency_start_date,
        rate_consistency_start_date=quality_config.rates.consistency_start_date,
        marketstack_reconciliation_records=marketstack_reconciliation_records,
        issues=tuple(issues),
    )


def render_data_quality_report(report: DataQualityReport) -> str:
    lines = [
        "# 数据质量报告",
        "",
        f"- 状态：{report.status}",
        f"- 检查时间：{report.checked_at.isoformat()}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        f"- 信息数：{report.info_count}",
        "",
        "## 文件",
        "",
        _render_file_summary("价格数据", report.price_summary),
        *(
            [_render_file_summary("第二行情源 Marketstack", report.secondary_price_summary)]
            if report.secondary_price_summary is not None
            else []
        ),
        _render_file_summary("FRED 宏观序列", report.rate_summary),
        *(
            [_render_file_summary("下载审计清单", report.manifest_summary)]
            if report.manifest_summary is not None
            else []
        ),
        "",
        "## 预期覆盖范围",
        "",
        f"- 价格标的：{', '.join(report.expected_price_tickers)}",
        f"- FRED 宏观序列：{', '.join(report.expected_rate_series)}",
        f"- 价格一致性检查起点：{_consistency_start_label(report)}",
        f"- 宏观变化检查起点：{_rate_consistency_start_label(report)}",
        "",
        "## 问题",
        "",
    ]

    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | 来源 | Code | 行数 | 说明 | 样例 |",
                "|---|---|---|---:|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{_escape_markdown_table(_issue_source(issue))} | "
                f"{issue.code} | "
                f"{issue.rows if issue.rows is not None else ''} | "
                f"{_escape_markdown_table(issue.message)} | "
                f"{_escape_markdown_table(issue.sample or '')} |"
            )

    if report.marketstack_reconciliation_records:
        lines.extend(_marketstack_reconciliation_section(report))

    return "\n".join(lines) + "\n"


def write_data_quality_report(report: DataQualityReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_data_quality_report(report), encoding="utf-8")
    if report.marketstack_reconciliation_records:
        write_marketstack_reconciliation_csv(
            report,
            marketstack_reconciliation_path(output_path),
        )
    return output_path


def marketstack_reconciliation_path(report_path: Path) -> Path:
    return report_path.with_name(f"{report_path.stem}_marketstack_reconciliation.csv")


def write_marketstack_reconciliation_csv(
    report: DataQualityReport,
    output_path: Path,
) -> Path:
    rows = [
        {
            "as_of": report.as_of.isoformat(),
            "date": record.date,
            "ticker": record.ticker,
            "severity": record.severity.value,
            "classification": record.classification,
            "rule_id": record.rule_id,
            "evidence": record.evidence,
            "primary_close": record.primary_close,
            "secondary_close": record.secondary_close,
            "close_diff_pct": record.close_diff_pct,
            "primary_adj_close": record.primary_adj_close,
            "secondary_adj_close": record.secondary_adj_close,
            "adj_close_diff_pct": record.adj_close_diff_pct,
        }
        for record in report.marketstack_reconciliation_records
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(output_path, index=False)
    return output_path


def default_quality_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"data_quality_{as_of.isoformat()}.md"


def _read_csv(
    path: Path,
    issues: list[DataQualityIssue],
    label: str,
) -> tuple[pd.DataFrame | None, DataFileSummary]:
    if not path.exists():
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                f"{label}_file_missing",
                f"{_data_label(label)}文件不存在：{path}",
                source=_source_label(label),
            )
        )
        return None, DataFileSummary(path=path, exists=False)

    try:
        data = pd.read_csv(path)
    except Exception as exc:
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                f"{label}_file_unreadable",
                f"{_data_label(label)}文件无法按 CSV 读取：{exc}",
                source=_source_label(label),
            )
        )
        return None, DataFileSummary(path=path, exists=True, sha256=_file_sha256(path))

    return data, DataFileSummary(
        path=path,
        exists=True,
        rows=len(data),
        sha256=_file_sha256(path),
    )


def _read_secondary_prices_csv(
    path: Path | None,
    issues: list[DataQualityIssue],
    *,
    required: bool,
) -> tuple[pd.DataFrame | None, DataFileSummary | None]:
    if path is None:
        return None, None
    if not path.exists():
        issues.append(
            DataQualityIssue(
                Severity.ERROR if required else Severity.WARNING,
                "secondary_prices_file_missing",
                f"第二行情源 Marketstack 文件不存在：{path}",
                source="第二行情源 Marketstack",
            )
        )
        return None, DataFileSummary(path=path, exists=False)

    try:
        data = pd.read_csv(path)
    except Exception as exc:
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "secondary_prices_file_unreadable",
                f"第二行情源 Marketstack 文件无法按 CSV 读取：{exc}",
                source="第二行情源 Marketstack",
            )
        )
        return None, DataFileSummary(path=path, exists=True, sha256=_file_sha256(path))

    return data, DataFileSummary(
        path=path,
        exists=True,
        rows=len(data),
        sha256=_file_sha256(path),
    )


def _validate_download_manifest(
    path: Path,
    price_summary: DataFileSummary,
    rate_summary: DataFileSummary,
    secondary_price_summary: DataFileSummary | None,
    issues: list[DataQualityIssue],
) -> DataFileSummary:
    if not path.exists():
        issues.append(
            DataQualityIssue(
                Severity.WARNING,
                "download_manifest_missing",
                f"下载审计清单不存在：{path}。请重新执行 download-data 生成审计记录。",
                source="下载审计清单",
            )
        )
        return DataFileSummary(path=path, exists=False)

    try:
        manifest = pd.read_csv(path)
    except Exception as exc:
        issues.append(
            DataQualityIssue(
                Severity.WARNING,
                "download_manifest_unreadable",
                f"下载审计清单无法按 CSV 读取：{exc}",
                source="下载审计清单",
            )
        )
        return DataFileSummary(path=path, exists=True, sha256=_file_sha256(path))

    summary = DataFileSummary(
        path=path,
        exists=True,
        rows=len(manifest),
        sha256=_file_sha256(path),
    )
    missing_columns = [column for column in MANIFEST_REQUIRED_COLUMNS if column not in manifest]
    if missing_columns:
        issues.append(
            DataQualityIssue(
                Severity.WARNING,
                "manifest_missing_columns",
                f"下载审计清单缺少必需字段：{', '.join(missing_columns)}",
                source="下载审计清单",
            )
        )
        return summary

    _check_manifest_covers_file(manifest, price_summary, "prices", issues)
    _check_manifest_covers_file(manifest, rate_summary, "rates", issues)
    if secondary_price_summary is not None:
        _check_manifest_covers_file(
            manifest,
            secondary_price_summary,
            "secondary_prices",
            issues,
        )
    return summary


def _check_manifest_covers_file(
    manifest: pd.DataFrame,
    summary: DataFileSummary,
    label: str,
    issues: list[DataQualityIssue],
) -> None:
    if not summary.exists or summary.sha256 is None:
        return

    checksum_matches = manifest["checksum_sha256"].astype(str) == summary.sha256
    if checksum_matches.any():
        return

    issues.append(
        DataQualityIssue(
            Severity.WARNING,
            f"{label}_download_manifest_checksum_missing",
            (
                f"{_data_label(label)}当前文件 sha256 未出现在下载审计清单中；"
                "请确认缓存是否由 download-data 生成。"
            ),
            sample=str(summary.path),
            source="下载审计清单",
        )
    )


def _validate_prices(
    prices: pd.DataFrame,
    summary: DataFileSummary,
    expected_tickers: list[str],
    quality_config: DataQualityConfig,
    as_of: date,
    issues: list[DataQualityIssue],
    *,
    source: str = "价格主源",
    error_severity: Severity = Severity.ERROR,
) -> DataFileSummary:
    if prices.empty:
        issues.append(
            DataQualityIssue(error_severity, "prices_empty", "价格数据没有任何行", source=source)
        )
        return summary

    if not _check_required_columns(
        prices, PRICE_REQUIRED_COLUMNS, "prices", issues, source=source, severity=error_severity
    ):
        return summary

    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    invalid_dates = frame["_date"].isna()
    if invalid_dates.any():
        issues.append(
            DataQualityIssue(
                error_severity,
                "prices_invalid_date",
                "价格数据包含无法解析的日期",
                rows=int(invalid_dates.sum()),
                sample=_sample_rows(frame.loc[invalid_dates], ["date", "ticker"]),
                source=source,
            )
        )

    _check_duplicate_keys(
        frame, ["date", "ticker"], "prices", issues, source=source, severity=error_severity
    )
    _check_expected_values(
        frame,
        "ticker",
        expected_tickers,
        "prices",
        issues,
        source=source,
        severity=error_severity,
    )
    _check_price_numeric_rules(
        frame,
        quality_config,
        issues,
        source=source,
        error_severity=error_severity,
    )
    _check_price_staleness(
        frame,
        expected_tickers,
        quality_config,
        as_of,
        issues,
        source=source,
        error_severity=error_severity,
    )
    _check_price_moves(frame, quality_config, issues, source=source, error_severity=error_severity)

    valid_dates = frame.loc[frame["_date"].notna(), "_date"]
    return _summary_with_dates(summary, valid_dates)


def _validate_rates(
    rates: pd.DataFrame,
    summary: DataFileSummary,
    expected_series: list[str],
    quality_config: DataQualityConfig,
    as_of: date,
    issues: list[DataQualityIssue],
) -> DataFileSummary:
    if rates.empty:
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "rates_empty",
                "FRED 宏观序列没有任何行",
                source="FRED 宏观序列",
            )
        )
        return summary

    if not _check_required_columns(
        rates, RATE_REQUIRED_COLUMNS, "rates", issues, source="FRED 宏观序列"
    ):
        return summary

    frame = rates.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_value"] = pd.to_numeric(frame["value"], errors="coerce")

    invalid_dates = frame["_date"].isna()
    if invalid_dates.any():
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "rates_invalid_date",
                "FRED 宏观序列包含无法解析的日期",
                rows=int(invalid_dates.sum()),
                sample=_sample_rows(frame.loc[invalid_dates], ["date", "series"]),
                source="FRED 宏观序列",
            )
        )

    invalid_values = frame["_value"].isna()
    if invalid_values.any():
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "rates_invalid_value",
                "FRED 宏观序列包含缺失或非数值",
                rows=int(invalid_values.sum()),
                sample=_sample_rows(frame.loc[invalid_values], ["date", "series", "value"]),
                source="FRED 宏观序列",
            )
        )

    _check_duplicate_keys(frame, ["date", "series"], "rates", issues, source="FRED 宏观序列")
    _check_expected_values(
        frame, "series", expected_series, "rates", issues, source="FRED 宏观序列"
    )
    _check_rate_ranges(frame, quality_config, issues)
    _check_rate_staleness(frame, expected_series, quality_config, as_of, issues)
    _check_rate_moves(frame, quality_config, issues)

    valid_dates = frame.loc[frame["_date"].notna(), "_date"]
    return _summary_with_dates(summary, valid_dates)


def _check_required_columns(
    data: pd.DataFrame,
    required_columns: tuple[str, ...],
    label: str,
    issues: list[DataQualityIssue],
    *,
    source: str | None = None,
    severity: Severity = Severity.ERROR,
) -> bool:
    missing = [column for column in required_columns if column not in data.columns]
    if missing:
        issues.append(
            DataQualityIssue(
                severity,
                f"{label}_missing_columns",
                f"{_data_label(label)}缺少必需字段：{', '.join(missing)}",
                source=source or _source_label(label),
            )
        )
        return False
    return True


def _check_duplicate_keys(
    data: pd.DataFrame,
    key_columns: list[str],
    label: str,
    issues: list[DataQualityIssue],
    *,
    source: str | None = None,
    severity: Severity = Severity.ERROR,
) -> None:
    duplicates = data.duplicated(subset=key_columns, keep=False)
    if duplicates.any():
        issues.append(
            DataQualityIssue(
                severity,
                f"{label}_duplicate_keys",
                f"{_data_label(label)}存在重复主键：{', '.join(key_columns)}",
                rows=int(duplicates.sum()),
                sample=_sample_rows(data.loc[duplicates], key_columns),
                source=source or _source_label(label),
            )
        )


def _check_expected_values(
    data: pd.DataFrame,
    column: str,
    expected_values: list[str],
    label: str,
    issues: list[DataQualityIssue],
    *,
    source: str | None = None,
    severity: Severity = Severity.ERROR,
) -> None:
    present = set(str(value) for value in data[column].dropna().unique())
    missing = [value for value in expected_values if value not in present]
    if missing:
        issues.append(
            DataQualityIssue(
                severity,
                f"{label}_missing_expected_values",
                f"{_data_label(label)}缺少预期的 {column}：{', '.join(missing)}",
                source=source or _source_label(label),
            )
        )


def _check_price_numeric_rules(
    frame: pd.DataFrame,
    quality_config: DataQualityConfig,
    issues: list[DataQualityIssue],
    *,
    source: str,
    error_severity: Severity,
) -> None:
    numeric_columns = ["open", "high", "low", "close", "adj_close", "volume"]
    for column in numeric_columns:
        frame[f"_{column}"] = pd.to_numeric(frame[column], errors="coerce")

    required_numeric = ["open", "high", "low", "close", "adj_close"]
    for column in required_numeric:
        invalid = frame[f"_{column}"].isna()
        if invalid.any():
            issues.append(
                DataQualityIssue(
                    error_severity,
                    f"prices_invalid_{column}",
                    f"价格数据的 {column} 包含缺失或非数值",
                    rows=int(invalid.sum()),
                    sample=_sample_rows(frame.loc[invalid], ["date", "ticker", column]),
                    source=source,
                )
            )

        non_positive = frame[f"_{column}"] <= 0
        if non_positive.any():
            issues.append(
                DataQualityIssue(
                    error_severity,
                    f"prices_non_positive_{column}",
                    f"价格数据的 {column} 包含非正数",
                    rows=int(non_positive.sum()),
                    sample=_sample_rows(frame.loc[non_positive], ["date", "ticker", column]),
                    source=source,
                )
            )

    invalid_volume = frame["_volume"].notna() & (frame["_volume"] < 0)
    if invalid_volume.any():
        issues.append(
            DataQualityIssue(
                error_severity,
                "prices_negative_volume",
                "价格数据包含负成交量",
                rows=int(invalid_volume.sum()),
                sample=_sample_rows(frame.loc[invalid_volume], ["date", "ticker", "volume"]),
                source=source,
            )
        )

    volume_optional_tickers = set(quality_config.prices.volume_optional_tickers)
    missing_volume = frame["_volume"].isna()
    optional_missing_volume = missing_volume & frame["ticker"].isin(volume_optional_tickers)
    unexpected_missing_volume = missing_volume & ~frame["ticker"].isin(volume_optional_tickers)
    if optional_missing_volume.any():
        issues.append(
            DataQualityIssue(
                Severity.INFO,
                "prices_index_volume_not_applicable",
                (
                    "价格数据缺少成交量，但该 ticker 已配置为指数/非成交标的，"
                    "volume 不适用于质量阻断。"
                ),
                rows=int(optional_missing_volume.sum()),
                sample=_sample_rows(
                    frame.loc[optional_missing_volume],
                    ["date", "ticker", "volume"],
                ),
                source=source,
            )
        )
    if unexpected_missing_volume.any():
        issues.append(
            DataQualityIssue(
                _noncritical_price_issue_severity(error_severity),
                "prices_missing_volume",
                "价格数据的成交量包含缺失或非数值",
                rows=int(unexpected_missing_volume.sum()),
                sample=_sample_rows(
                    frame.loc[unexpected_missing_volume],
                    ["date", "ticker", "volume"],
                ),
                source=source,
            )
        )

    valid_ohlc = frame[["_open", "_high", "_low", "_close"]].notna().all(axis=1)
    high_invalid = valid_ohlc & (
        (frame["_high"] < frame["_open"])
        | (frame["_high"] < frame["_low"])
        | (frame["_high"] < frame["_close"])
    )
    low_invalid = valid_ohlc & (
        (frame["_low"] > frame["_open"])
        | (frame["_low"] > frame["_high"])
        | (frame["_low"] > frame["_close"])
    )
    ohlc_invalid = high_invalid | low_invalid
    if ohlc_invalid.any():
        issues.append(
            DataQualityIssue(
                error_severity,
                "prices_invalid_ohlc",
                "价格数据违反 OHLC 逻辑约束",
                rows=int(ohlc_invalid.sum()),
                sample=_sample_rows(
                    frame.loc[ohlc_invalid],
                    ["date", "ticker", "open", "high", "low", "close"],
                ),
                source=source,
            )
        )


def _check_price_staleness(
    frame: pd.DataFrame,
    expected_tickers: list[str],
    quality_config: DataQualityConfig,
    as_of: date,
    issues: list[DataQualityIssue],
    *,
    source: str,
    error_severity: Severity,
) -> None:
    latest_by_ticker = frame.loc[frame["_date"].notna()].groupby("ticker")["_date"].max()
    stale: list[str] = []
    future: list[str] = []
    for ticker in expected_tickers:
        if ticker not in latest_by_ticker:
            continue
        latest_date = latest_by_ticker[ticker].date()
        lag_days = (as_of - latest_date).days
        if lag_days < 0:
            future.append(f"{ticker}:{latest_date.isoformat()}")
        elif lag_days > quality_config.prices.max_stale_calendar_days:
            stale.append(f"{ticker}:{latest_date.isoformat()}({lag_days}d)")

    if future:
        issues.append(
            DataQualityIssue(
                error_severity,
                "prices_future_dates",
                "价格数据包含评估日期之后的数据",
                sample=", ".join(future[:10]),
                source=source,
            )
        )
    if stale:
        issues.append(
            DataQualityIssue(
                error_severity,
                "prices_stale",
                "价格数据最新日期过旧，不能用于评分",
                sample=", ".join(stale[:10]),
                source=source,
            )
        )


def _check_price_moves(
    frame: pd.DataFrame,
    quality_config: DataQualityConfig,
    issues: list[DataQualityIssue],
    *,
    source: str,
    error_severity: Severity,
) -> None:
    data = frame.loc[
        frame["_date"].notna() & frame["_adj_close"].notna() & (frame["_adj_close"] > 0)
    ].copy()
    data = _filter_price_consistency_window(data, quality_config)
    if data.empty:
        return

    data = data.sort_values(["ticker", "_date"])
    data["_return"] = data.groupby("ticker")["_adj_close"].pct_change()
    data["_suspicious_return_threshold"] = data["ticker"].map(
        lambda ticker: _suspicious_price_return_threshold(str(ticker), quality_config.prices)
    )
    data["_extreme_return_threshold"] = data["ticker"].map(
        lambda ticker: _extreme_price_return_threshold(str(ticker), quality_config.prices)
    )
    abs_return = data["_return"].abs()

    extreme = abs_return > data["_extreme_return_threshold"]
    suspicious = (abs_return > data["_suspicious_return_threshold"]) & ~extreme

    if extreme.any():
        issues.append(
            DataQualityIssue(
                error_severity,
                "prices_extreme_adj_close_move",
                "价格数据包含极端的调整收盘价单日波动",
                rows=int(extreme.sum()),
                sample=_sample_rows(
                    data.loc[extreme],
                    ["date", "ticker", "adj_close", "_return", "_extreme_return_threshold"],
                ),
                source=source,
            )
        )
    if suspicious.any():
        issues.append(
            DataQualityIssue(
                Severity.INFO,
                "prices_suspicious_adj_close_move",
                (
                    "价格数据包含较大调整收盘价单日波动；未达到极端错误阈值，"
                    "作为可审计市场波动信息记录。"
                ),
                rows=int(suspicious.sum()),
                sample=_sample_rows(
                    data.loc[suspicious],
                    [
                        "date",
                        "ticker",
                        "adj_close",
                        "_return",
                        "_suspicious_return_threshold",
                    ],
                ),
                source=source,
            )
        )

    data["_adjustment_ratio"] = data["_adj_close"] / data["_close"]
    data["_adjustment_ratio_change"] = data.groupby("ticker")["_adjustment_ratio"].pct_change()
    ratio_jump = (
        data["_adjustment_ratio_change"].abs()
        > quality_config.prices.suspicious_adjustment_ratio_change_abs
    )
    if ratio_jump.any():
        known_split = data.apply(
            lambda row: _matches_known_split_event(
                ticker=str(row["ticker"]),
                value_date=row["_date"].date(),
                adjustment_ratio_change=float(row["_adjustment_ratio_change"]),
                quality_config=quality_config,
            )
            is not None,
            axis=1,
        )
        split_ratio_jump = ratio_jump & known_split
        unresolved_ratio_jump = ratio_jump & ~known_split
        if split_ratio_jump.any():
            issues.append(
                DataQualityIssue(
                    Severity.INFO,
                    "prices_known_split_adjustment_ratio_jump",
                    "复权比例跳变匹配已配置 corporate action 拆股事件。",
                    rows=int(split_ratio_jump.sum()),
                    sample=_sample_rows(
                        data.loc[split_ratio_jump],
                        ["date", "ticker", "_adjustment_ratio", "_adjustment_ratio_change"],
                    ),
                    source=source,
                )
            )
    else:
        unresolved_ratio_jump = ratio_jump
    if unresolved_ratio_jump.any():
        issues.append(
            DataQualityIssue(
                _noncritical_price_issue_severity(error_severity),
                "prices_adjustment_ratio_jump",
                "价格数据的复权比例出现明显跳变",
                rows=int(unresolved_ratio_jump.sum()),
                sample=_sample_rows(
                    data.loc[unresolved_ratio_jump],
                    ["date", "ticker", "_adjustment_ratio", "_adjustment_ratio_change"],
                ),
                source=source,
            )
        )


def _secondary_expected_price_tickers(
    expected_tickers: list[str],
    quality_config: DataQualityConfig,
) -> list[str]:
    excluded = set(quality_config.prices.secondary_source_excluded_tickers)
    return [ticker for ticker in expected_tickers if ticker not in excluded]


def _secondary_price_self_check_error_severity(quality_config: DataQualityConfig) -> Severity:
    if quality_config.prices.secondary_source_self_check_fail_closed:
        return Severity.ERROR
    return Severity.INFO


def _noncritical_price_issue_severity(error_severity: Severity) -> Severity:
    if error_severity == Severity.INFO:
        return Severity.INFO
    return Severity.WARNING


def _matches_known_split_event(
    *,
    ticker: str,
    value_date: date,
    adjustment_ratio_change: float,
    quality_config: DataQualityConfig,
) -> KnownSplitEventConfig | None:
    events = quality_config.prices.known_split_events.get(ticker, [])
    if not events:
        return None
    observed_factor = 1.0 + adjustment_ratio_change
    tolerance = quality_config.prices.known_split_ratio_tolerance_abs
    window_days = quality_config.prices.known_split_match_window_days
    for event in events:
        if abs((value_date - event.effective_date).days) > window_days:
            continue
        expected_factors = (event.ratio, 1.0 / event.ratio)
        if any(abs(observed_factor - expected) <= tolerance for expected in expected_factors):
            return event
    return None


def _matches_known_split_close_basis(
    *,
    ticker: str,
    value_date: date,
    primary_close: float,
    secondary_close: float,
    quality_config: DataQualityConfig,
) -> KnownSplitEventConfig | None:
    events = quality_config.prices.known_split_events.get(ticker, [])
    if not events or primary_close <= 0 or secondary_close <= 0:
        return None
    observed_factor = max(primary_close, secondary_close) / min(primary_close, secondary_close)
    tolerance = quality_config.prices.known_split_ratio_tolerance_abs
    window_days = quality_config.prices.known_split_match_window_days
    for event in events:
        if abs((value_date - event.effective_date).days) > window_days:
            continue
        if abs(observed_factor - event.ratio) <= tolerance:
            return event
    return None


def _row_matches_known_split_close_basis(
    row: pd.Series,
    quality_config: DataQualityConfig,
) -> bool:
    value_timestamp = pd.to_datetime(row["date"], errors="coerce")
    if bool(pd.isna(cast(Any, value_timestamp))):
        return False
    return (
        _matches_known_split_close_basis(
            ticker=str(row["ticker"]),
            value_date=cast(pd.Timestamp, value_timestamp).date(),
            primary_close=float(row["_primary_close"]),
            secondary_close=float(row["_secondary_close"]),
            quality_config=quality_config,
        )
        is not None
    )


def _check_secondary_price_reconciliation(
    primary_prices: pd.DataFrame,
    secondary_prices: pd.DataFrame,
    expected_tickers: list[str],
    quality_config: DataQualityConfig,
    *,
    required: bool,
    issues: list[DataQualityIssue],
) -> tuple[MarketstackReconciliationRecord, ...]:
    if not expected_tickers:
        return ()
    required_columns = {"date", "ticker", "close", "adj_close"}
    if not required_columns.issubset(primary_prices.columns) or not required_columns.issubset(
        secondary_prices.columns
    ):
        return ()

    primary = primary_prices.loc[primary_prices["ticker"].isin(expected_tickers)].copy()
    secondary = secondary_prices.loc[secondary_prices["ticker"].isin(expected_tickers)].copy()
    records: list[MarketstackReconciliationRecord] = []
    if primary.empty or secondary.empty:
        issues.append(
            DataQualityIssue(
                Severity.ERROR if required else Severity.WARNING,
                "secondary_prices_no_reconciliation_overlap",
                "第二行情源与主价格缓存没有可核验的重叠 ticker/date。",
                source="跨源核验：主价格源 vs Marketstack",
            )
        )
        return ()

    primary["_date"] = pd.to_datetime(primary["date"], errors="coerce")
    secondary["_date"] = pd.to_datetime(secondary["date"], errors="coerce")
    primary = _filter_price_consistency_window(primary, quality_config)
    secondary = _filter_price_consistency_window(secondary, quality_config)
    primary["_primary_close"] = pd.to_numeric(primary["close"], errors="coerce")
    secondary["_secondary_close"] = pd.to_numeric(secondary["close"], errors="coerce")
    primary["_primary_adj_close"] = pd.to_numeric(primary["adj_close"], errors="coerce")
    secondary["_secondary_adj_close"] = pd.to_numeric(secondary["adj_close"], errors="coerce")
    for column in ("open", "high", "low"):
        secondary[f"_secondary_{column}"] = pd.to_numeric(secondary[column], errors="coerce")
    records.extend(_marketstack_bad_point_records(primary, secondary))

    primary = primary.loc[
        primary["_date"].notna()
        & primary["_primary_close"].notna()
        & (primary["_primary_close"] > 0)
        & primary["_primary_adj_close"].notna()
        & (primary["_primary_adj_close"] > 0),
        ["date", "ticker", "_primary_close", "_primary_adj_close"],
    ].drop_duplicates(subset=["date", "ticker"])
    secondary = secondary.loc[
        secondary["_date"].notna()
        & secondary["_secondary_close"].notna()
        & (secondary["_secondary_close"] > 0)
        & secondary["_secondary_adj_close"].notna()
        & (secondary["_secondary_adj_close"] > 0),
        ["date", "ticker", "_secondary_close", "_secondary_adj_close"],
    ].drop_duplicates(subset=["date", "ticker"])

    expected_pairs = len(primary)
    if expected_pairs == 0:
        return tuple(records)
    merged = primary.merge(secondary, on=["date", "ticker"], how="inner")
    overlap_ratio = len(merged) / expected_pairs
    if overlap_ratio < quality_config.prices.secondary_source_min_overlap_ratio:
        issues.append(
            DataQualityIssue(
                Severity.ERROR if required else Severity.WARNING,
                "secondary_prices_overlap_below_threshold",
                (
                    "第二行情源与主价格缓存重叠样本不足："
                    f"{len(merged)}/{expected_pairs}="
                    f"{overlap_ratio:.1%}，阈值 "
                    f"{quality_config.prices.secondary_source_min_overlap_ratio:.1%}。"
                ),
                source="跨源核验：主价格源 vs Marketstack",
            )
        )
    if merged.empty:
        return tuple(records)

    warning_threshold = quality_config.prices.secondary_source_adj_close_warning_pct
    error_threshold = quality_config.prices.secondary_source_adj_close_error_pct
    merged["_close_diff_pct"] = (
        (merged["_primary_close"] - merged["_secondary_close"]).abs() / merged["_primary_close"]
    )
    merged["_adj_close_diff_pct"] = (
        (merged["_primary_adj_close"] - merged["_secondary_adj_close"]).abs()
        / merged["_primary_adj_close"]
    )
    close_error_diff = merged["_close_diff_pct"] > error_threshold
    close_warning_diff = (merged["_close_diff_pct"] > warning_threshold) & ~close_error_diff
    close_reconciled = merged["_close_diff_pct"] <= warning_threshold
    known_split_close_basis_diff = (
        close_error_diff | close_warning_diff
    ) & merged.apply(
        lambda row: _row_matches_known_split_close_basis(row, quality_config),
        axis=1,
    )
    close_error_diff = close_error_diff & ~known_split_close_basis_diff
    close_warning_diff = close_warning_diff & ~known_split_close_basis_diff
    adj_error_diff = merged["_adj_close_diff_pct"] > error_threshold
    adj_warning_diff = (merged["_adj_close_diff_pct"] > warning_threshold) & ~adj_error_diff
    adj_adjustment_basis_diff = adj_error_diff & close_reconciled
    adj_unresolved_error_diff = (
        adj_error_diff & ~close_reconciled & ~known_split_close_basis_diff
    )
    adj_warning_basis_diff = adj_warning_diff & close_reconciled
    adj_unresolved_warning_diff = (
        adj_warning_diff & ~close_reconciled & ~known_split_close_basis_diff
    )
    records.extend(
        _marketstack_reconciliation_records_from_mask(
            merged,
            known_split_close_basis_diff,
            severity=Severity.INFO,
            classification="known_split_raw_close_basis_difference",
            rule_id="marketstack.known_split_close_basis.v1",
            evidence=(
                "raw close diff ratio matches a configured known split event within the "
                "corporate-action window; classified as provider split-date basis difference."
            ),
        )
    )
    records.extend(
        _marketstack_reconciliation_records_from_mask(
            merged,
            close_error_diff,
            severity=Severity.ERROR,
            classification="raw_close_unresolved_error",
            rule_id="marketstack.raw_close_reconciliation.v1",
            evidence="raw close diff exceeds error threshold; downstream remains blocked.",
        )
    )
    records.extend(
        _marketstack_reconciliation_records_from_mask(
            merged,
            close_warning_diff,
            severity=Severity.WARNING,
            classification="raw_close_unresolved_warning",
            rule_id="marketstack.raw_close_reconciliation.v1",
            evidence="raw close diff exceeds warning threshold; requires investigation.",
        )
    )
    records.extend(
        _marketstack_reconciliation_records_from_mask(
            merged,
            adj_adjustment_basis_diff | adj_warning_basis_diff,
            severity=Severity.INFO,
            classification="adjusted_close_dividend_basis_difference",
            rule_id="marketstack.adjusted_close_basis.v1",
            evidence=(
                "raw close reconciles within warning threshold while adjusted close differs; "
                "classified as dividend/adjusted-close basis difference, no price cache mutation."
            ),
        )
    )
    records.extend(
        _marketstack_reconciliation_records_from_mask(
            merged,
            adj_unresolved_error_diff,
            severity=Severity.ERROR,
            classification="adjusted_close_unresolved_error",
            rule_id="marketstack.adjusted_close_reconciliation.v1",
            evidence="adjusted close diff exceeds error threshold and raw close is not reconciled.",
        )
    )
    records.extend(
        _marketstack_reconciliation_records_from_mask(
            merged,
            adj_unresolved_warning_diff,
            severity=Severity.WARNING,
            classification="adjusted_close_unresolved_warning",
            rule_id="marketstack.adjusted_close_reconciliation.v1",
            evidence=(
                "adjusted close diff exceeds warning threshold and raw close is not reconciled."
            ),
        )
    )

    if close_error_diff.any():
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "secondary_prices_close_mismatch",
                "主价格缓存与 Marketstack 的未调整收盘价差异超过错误阈值。",
                rows=int(close_error_diff.sum()),
                sample=_sample_rows(
                    merged.loc[close_error_diff],
                    [
                        "date",
                        "ticker",
                        "_primary_close",
                        "_secondary_close",
                        "_close_diff_pct",
                    ],
                ),
                source="跨源核验：主价格源 vs Marketstack",
            )
        )
    if close_warning_diff.any():
        issues.append(
            DataQualityIssue(
                Severity.WARNING,
                "secondary_prices_close_warning",
                "主价格缓存与 Marketstack 的未调整收盘价差异超过警告阈值。",
                rows=int(close_warning_diff.sum()),
                sample=_sample_rows(
                    merged.loc[close_warning_diff],
                    [
                        "date",
                        "ticker",
                        "_primary_close",
                        "_secondary_close",
                        "_close_diff_pct",
                    ],
                ),
                source="跨源核验：主价格源 vs Marketstack",
            )
        )
    if known_split_close_basis_diff.any():
        issues.append(
            DataQualityIssue(
                Severity.INFO,
                "secondary_prices_known_split_close_basis",
                (
                    "主价格缓存与 Marketstack 的未调整收盘价差异匹配已配置拆股事件，"
                    "按 corporate-action window 日期口径差异记录，不改写价格缓存。"
                ),
                rows=int(known_split_close_basis_diff.sum()),
                sample=_sample_rows(
                    merged.loc[known_split_close_basis_diff],
                    [
                        "date",
                        "ticker",
                        "_primary_close",
                        "_secondary_close",
                        "_close_diff_pct",
                    ],
                ),
                source="跨源核验：主价格源 vs Marketstack",
            )
        )
    if adj_unresolved_error_diff.any():
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "secondary_prices_adj_close_mismatch",
                (
                    "主价格缓存与 Marketstack 的调整收盘价差异超过错误阈值，"
                    "且未调整收盘价也未通过警告阈值核验。"
                ),
                rows=int(adj_unresolved_error_diff.sum()),
                sample=_sample_rows(
                    merged.loc[adj_unresolved_error_diff],
                    [
                        "date",
                        "ticker",
                        "_primary_close",
                        "_secondary_close",
                        "_close_diff_pct",
                        "_primary_adj_close",
                        "_secondary_adj_close",
                        "_adj_close_diff_pct",
                    ],
                ),
                source="跨源核验：主价格源 vs Marketstack",
            )
        )
    if adj_adjustment_basis_diff.any():
        issues.append(
            DataQualityIssue(
                Severity.INFO,
                "secondary_prices_adjustment_basis_warning",
                (
                    "Marketstack 调整收盘价与主缓存差异超过错误阈值，"
                    "但未调整收盘价通过核验；这通常表示供应商 adjusted close "
                    "分红调整口径不同，需在报告中保留限制说明。"
                ),
                rows=int(adj_adjustment_basis_diff.sum()),
                sample=_sample_rows(
                    merged.loc[adj_adjustment_basis_diff],
                    [
                        "date",
                        "ticker",
                        "_primary_close",
                        "_secondary_close",
                        "_close_diff_pct",
                        "_primary_adj_close",
                        "_secondary_adj_close",
                        "_adj_close_diff_pct",
                    ],
                ),
                source="跨源核验：主价格源 vs Marketstack",
            )
        )
    if adj_unresolved_warning_diff.any():
        issues.append(
            DataQualityIssue(
                Severity.WARNING,
                "secondary_prices_adj_close_warning",
                "主价格缓存与 Marketstack 的调整收盘价差异超过警告阈值。",
                rows=int(adj_unresolved_warning_diff.sum()),
                sample=_sample_rows(
                    merged.loc[adj_unresolved_warning_diff],
                    [
                        "date",
                        "ticker",
                        "_primary_close",
                        "_secondary_close",
                        "_close_diff_pct",
                        "_primary_adj_close",
                        "_secondary_adj_close",
                        "_adj_close_diff_pct",
                    ],
                ),
                source="跨源核验：主价格源 vs Marketstack",
            )
        )
    if adj_warning_basis_diff.any():
        issues.append(
            DataQualityIssue(
                Severity.INFO,
                "secondary_prices_adjustment_basis_info",
                (
                    "Marketstack 调整收盘价与主缓存超过警告阈值，但未调整收盘价通过核验；"
                    "按分红复权口径差异记录。"
                ),
                rows=int(adj_warning_basis_diff.sum()),
                sample=_sample_rows(
                    merged.loc[adj_warning_basis_diff],
                    [
                        "date",
                        "ticker",
                        "_primary_close",
                        "_secondary_close",
                        "_close_diff_pct",
                        "_primary_adj_close",
                        "_secondary_adj_close",
                        "_adj_close_diff_pct",
                    ],
                ),
                source="跨源核验：主价格源 vs Marketstack",
            )
        )
    return tuple(records)


def _marketstack_bad_point_records(
    primary: pd.DataFrame,
    secondary: pd.DataFrame,
) -> list[MarketstackReconciliationRecord]:
    if secondary.empty:
        return []
    primary_rows: dict[tuple[str, str], pd.Series] = {}
    for _, primary_item in primary.iterrows():
        primary_rows[(str(primary_item["date"]), str(primary_item["ticker"]))] = primary_item
    valid_ohlc = secondary[
        ["_secondary_open", "_secondary_high", "_secondary_low", "_secondary_close"]
    ].notna().all(axis=1)
    invalid_ohlc = valid_ohlc & (
        (secondary["_secondary_high"] < secondary["_secondary_open"])
        | (secondary["_secondary_high"] < secondary["_secondary_low"])
        | (secondary["_secondary_high"] < secondary["_secondary_close"])
        | (secondary["_secondary_low"] > secondary["_secondary_open"])
        | (secondary["_secondary_low"] > secondary["_secondary_high"])
        | (secondary["_secondary_low"] > secondary["_secondary_close"])
    )
    bad_point = (
        secondary["_secondary_close"].isna()
        | secondary["_secondary_adj_close"].isna()
        | (secondary["_secondary_close"] <= 0)
        | (secondary["_secondary_adj_close"] <= 0)
        | invalid_ohlc
    )
    records: list[MarketstackReconciliationRecord] = []
    for _, row in secondary.loc[bad_point].iterrows():
        key = (str(row["date"]), str(row["ticker"]))
        matched_primary_row = primary_rows.get(key)
        primary_close = (
            _optional_float(matched_primary_row.get("_primary_close"))
            if matched_primary_row is not None
            else None
        )
        primary_adj_close = (
            _optional_float(matched_primary_row.get("_primary_adj_close"))
            if matched_primary_row is not None
            else None
        )
        records.append(
            MarketstackReconciliationRecord(
                date=str(row["date"]),
                ticker=str(row["ticker"]),
                severity=Severity.INFO,
                classification="marketstack_bad_point_primary_available"
                if primary_close is not None
                else "marketstack_bad_point_no_primary_evidence",
                rule_id="marketstack.secondary_self_check.v1",
                evidence=(
                    "Marketstack row violates close/adj_close/OHLC self-check; "
                    "primary source same ticker-date is recorded for audit when available."
                ),
                primary_close=primary_close,
                secondary_close=_optional_float(row["_secondary_close"]),
                primary_adj_close=primary_adj_close,
                secondary_adj_close=_optional_float(row["_secondary_adj_close"]),
            )
        )
    return records


def _marketstack_reconciliation_records_from_mask(
    merged: pd.DataFrame,
    mask: pd.Series,
    *,
    severity: Severity,
    classification: str,
    rule_id: str,
    evidence: str,
) -> list[MarketstackReconciliationRecord]:
    records: list[MarketstackReconciliationRecord] = []
    for _, row in merged.loc[mask].iterrows():
        records.append(
            MarketstackReconciliationRecord(
                date=str(row["date"]),
                ticker=str(row["ticker"]),
                severity=severity,
                classification=classification,
                rule_id=rule_id,
                evidence=evidence,
                primary_close=_optional_float(row["_primary_close"]),
                secondary_close=_optional_float(row["_secondary_close"]),
                close_diff_pct=_optional_float(row["_close_diff_pct"]),
                primary_adj_close=_optional_float(row["_primary_adj_close"]),
                secondary_adj_close=_optional_float(row["_secondary_adj_close"]),
                adj_close_diff_pct=_optional_float(row["_adj_close_diff_pct"]),
            )
        )
    return records


def _optional_float(value: object) -> float | None:
    try:
        if bool(pd.isna(cast(Any, value))):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return float(cast(Any, value))
    except (TypeError, ValueError):
        return None


def _suspicious_price_return_threshold(ticker: str, config: PriceQualityConfig) -> float:
    override = config.ticker_return_threshold_overrides.get(ticker)
    if override and override.suspicious_daily_return_abs is not None:
        return override.suspicious_daily_return_abs
    return config.suspicious_daily_return_abs


def _extreme_price_return_threshold(ticker: str, config: PriceQualityConfig) -> float:
    override = config.ticker_return_threshold_overrides.get(ticker)
    if override and override.extreme_daily_return_abs is not None:
        return override.extreme_daily_return_abs
    return config.extreme_daily_return_abs


def _filter_price_consistency_window(
    frame: pd.DataFrame,
    quality_config: DataQualityConfig,
) -> pd.DataFrame:
    consistency_start = quality_config.prices.consistency_start_date
    if consistency_start is None or "_date" not in frame:
        return frame
    return frame.loc[frame["_date"] >= pd.Timestamp(consistency_start)].copy()


def _filter_rate_consistency_window(
    frame: pd.DataFrame,
    quality_config: DataQualityConfig,
) -> pd.DataFrame:
    consistency_start = quality_config.rates.consistency_start_date
    if consistency_start is None or "_date" not in frame:
        return frame
    return frame.loc[frame["_date"] >= pd.Timestamp(consistency_start)].copy()


def _check_rate_ranges(
    frame: pd.DataFrame,
    quality_config: DataQualityConfig,
    issues: list[DataQualityIssue],
) -> None:
    min_values = frame["series"].map(
        lambda series: _rate_min_plausible_value(str(series), quality_config.rates)
    )
    max_values = frame["series"].map(
        lambda series: _rate_max_plausible_value(str(series), quality_config.rates)
    )
    invalid = frame["_value"].notna() & (
        (frame["_value"] < min_values) | (frame["_value"] > max_values)
    )
    if invalid.any():
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "rates_out_of_range",
                "FRED 宏观序列包含超出配置合理范围的数值",
                rows=int(invalid.sum()),
                sample=_sample_rows(frame.loc[invalid], ["date", "series", "value"]),
                source="FRED 宏观序列",
            )
        )


def _check_rate_staleness(
    frame: pd.DataFrame,
    expected_series: list[str],
    quality_config: DataQualityConfig,
    as_of: date,
    issues: list[DataQualityIssue],
) -> None:
    latest_by_series = frame.loc[frame["_date"].notna()].groupby("series")["_date"].max()
    stale: list[str] = []
    future: list[str] = []
    for series in expected_series:
        if series not in latest_by_series:
            continue
        latest_date = latest_by_series[series].date()
        lag_days = (as_of - latest_date).days
        max_stale_days = _rate_max_stale_calendar_days(series, quality_config.rates)
        if lag_days < 0:
            future.append(f"{series}:{latest_date.isoformat()}")
        elif lag_days > max_stale_days:
            stale.append(f"{series}:{latest_date.isoformat()}({lag_days}d)")

    if future:
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "rates_future_dates",
                "FRED 宏观序列包含评估日期之后的数据",
                sample=", ".join(future[:10]),
                source="FRED 宏观序列",
            )
        )
    if stale:
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "rates_stale",
                "FRED 宏观序列最新日期过旧，不能用于评分",
                sample=", ".join(stale[:10]),
                source="FRED 宏观序列",
            )
        )


def _check_rate_moves(
    frame: pd.DataFrame,
    quality_config: DataQualityConfig,
    issues: list[DataQualityIssue],
) -> None:
    data = frame.loc[frame["_date"].notna() & frame["_value"].notna()].copy()
    data = _filter_rate_consistency_window(data, quality_config)
    if data.empty:
        return

    data = data.sort_values(["series", "_date"])
    data["_change"] = data.groupby("series")["_value"].diff().abs()
    suspicious_thresholds = data["series"].map(
        lambda series: _rate_suspicious_daily_change_abs(str(series), quality_config.rates)
    )
    extreme_thresholds = data["series"].map(
        lambda series: _rate_extreme_daily_change_abs(str(series), quality_config.rates)
    )
    extreme = data["_change"] > extreme_thresholds
    suspicious = (data["_change"] > suspicious_thresholds) & ~extreme

    if extreme.any():
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "rates_extreme_daily_change",
                "FRED 宏观序列包含极端单日变化",
                rows=int(extreme.sum()),
                sample=_sample_rows(data.loc[extreme], ["date", "series", "value", "_change"]),
                source="FRED 宏观序列",
            )
        )
    if suspicious.any():
        issues.append(
            DataQualityIssue(
                Severity.WARNING,
                "rates_suspicious_daily_change",
                "FRED 宏观序列包含可疑单日变化",
                rows=int(suspicious.sum()),
                sample=_sample_rows(data.loc[suspicious], ["date", "series", "value", "_change"]),
                source="FRED 宏观序列",
            )
        )


def _summary_with_dates(summary: DataFileSummary, dates: pd.Series) -> DataFileSummary:
    if dates.empty:
        return summary
    return DataFileSummary(
        path=summary.path,
        exists=summary.exists,
        rows=summary.rows,
        sha256=summary.sha256,
        min_date=dates.min().date(),
        max_date=dates.max().date(),
    )


def _rate_min_plausible_value(series: str, config: RateQualityConfig) -> float:
    override = config.series_overrides.get(series)
    if override and override.min_plausible_value is not None:
        return override.min_plausible_value
    return config.min_plausible_value


def _rate_max_stale_calendar_days(series: str, config: RateQualityConfig) -> int:
    override = config.series_overrides.get(series)
    if override and override.max_stale_calendar_days is not None:
        return override.max_stale_calendar_days
    return config.max_stale_calendar_days


def _rate_max_plausible_value(series: str, config: RateQualityConfig) -> float:
    override = config.series_overrides.get(series)
    if override and override.max_plausible_value is not None:
        return override.max_plausible_value
    return config.max_plausible_value


def _rate_suspicious_daily_change_abs(series: str, config: RateQualityConfig) -> float:
    override = config.series_overrides.get(series)
    if override and override.suspicious_daily_change_abs is not None:
        return override.suspicious_daily_change_abs
    return config.suspicious_daily_change_abs


def _rate_extreme_daily_change_abs(series: str, config: RateQualityConfig) -> float:
    override = config.series_overrides.get(series)
    if override and override.extreme_daily_change_abs is not None:
        return override.extreme_daily_change_abs
    return config.extreme_daily_change_abs


def _sample_rows(data: pd.DataFrame, columns: list[str], max_rows: int = 3) -> str:
    available_columns = [column for column in columns if column in data.columns]
    if not available_columns:
        return ""
    records = data[available_columns].head(max_rows).to_dict(orient="records")
    return "; ".join(str(record) for record in records)


def _marketstack_reconciliation_section(report: DataQualityReport) -> list[str]:
    counts: dict[tuple[Severity, str], int] = {}
    for record in report.marketstack_reconciliation_records:
        key = (record.severity, record.classification)
        counts[key] = counts.get(key, 0) + 1

    lines = [
        "",
        "## Marketstack reconciliation",
        "",
        (
            "- 明细 CSV：与本 Markdown 同目录，文件名后缀为 "
            "`_marketstack_reconciliation.csv`。"
        ),
        "- 规则边界：只归因和记录，不改写主价格缓存、第二行情源缓存、评分或回测真值。",
        "",
        "| 级别 | 分类 | 行数 |",
        "|---|---|---:|",
    ]
    for (severity, classification), count in sorted(
        counts.items(),
        key=lambda item: (item[0][0].value, item[0][1]),
    ):
        lines.append(
            "| "
            f"{_severity_label(severity)} | "
            f"{_escape_markdown_table(classification)} | "
            f"{count} |"
        )

    lines.extend(
        [
            "",
            "### 样例",
            "",
            "| 日期 | Ticker | 级别 | 分类 | 规则 | 证据 |",
            "|---|---|---|---|---|---|",
        ]
    )
    for record in report.marketstack_reconciliation_records[:10]:
        lines.append(
            "| "
            f"{record.date} | "
            f"{record.ticker} | "
            f"{_severity_label(record.severity)} | "
            f"{_escape_markdown_table(record.classification)} | "
            f"{_escape_markdown_table(record.rule_id)} | "
            f"{_escape_markdown_table(record.evidence)} |"
        )
    return lines


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _render_file_summary(label: str, summary: DataFileSummary) -> str:
    if not summary.exists:
        return f"- {label}：缺失，路径 `{summary.path}`"
    min_date = summary.min_date.isoformat() if summary.min_date else "n/a"
    max_date = summary.max_date.isoformat() if summary.max_date else "n/a"
    checksum = summary.sha256 or "n/a"
    return (
        f"- {label}：`{summary.path}`，行数={summary.rows}，"
        f"日期范围={min_date} 至 {max_date}，sha256={checksum}"
    )


def _severity_label(severity: Severity) -> str:
    if severity == Severity.INFO:
        return "信息"
    if severity == Severity.ERROR:
        return "错误"
    return "警告"


def _consistency_start_label(report: DataQualityReport) -> str:
    if report.price_consistency_start_date is None:
        return "未限制，使用全部缓存历史"
    return (
        f"{report.price_consistency_start_date.isoformat()}"
        "（早于该日期的价格一致性差异不阻断默认日报）"
    )


def _rate_consistency_start_label(report: DataQualityReport) -> str:
    if report.rate_consistency_start_date is None:
        return "未限制，使用全部缓存历史"
    return (
        f"{report.rate_consistency_start_date.isoformat()}"
        "（早于该日期的宏观变化差异不提示默认日报）"
    )


def _issue_source(issue: DataQualityIssue) -> str:
    if issue.source:
        return issue.source
    if issue.code.startswith("secondary_prices_close") or issue.code.startswith(
        "secondary_prices_adj"
    ) or issue.code.startswith("secondary_prices_overlap") or issue.code.startswith(
        "secondary_prices_no_reconciliation"
    ):
        return "跨源核验：主价格源 vs Marketstack"
    if issue.code.startswith("secondary_prices_"):
        return "第二行情源 Marketstack"
    if issue.code.startswith("prices_"):
        return "价格主源"
    if issue.code.startswith("rates_"):
        return "FRED 宏观序列"
    if issue.code.startswith("download_manifest_") or issue.code.startswith("manifest_"):
        return "下载审计清单"
    return "未标注"


def _data_label(label: str) -> str:
    return {
        "prices": "价格数据",
        "secondary_prices": "第二行情源 Marketstack",
        "rates": "FRED 宏观序列",
        "manifest": "下载审计清单",
    }.get(label, label)


def _source_label(label: str) -> str:
    return {
        "prices": "价格主源",
        "secondary_prices": "第二行情源 Marketstack",
        "rates": "FRED 宏观序列",
        "manifest": "下载审计清单",
    }.get(label, label)


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
