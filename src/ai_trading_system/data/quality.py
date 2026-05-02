from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from hashlib import sha256
from pathlib import Path

import pandas as pd

from ai_trading_system.config import DataQualityConfig, PriceQualityConfig

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
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class DataQualityIssue:
    severity: Severity
    code: str
    message: str
    rows: int | None = None
    sample: str | None = None


@dataclass(frozen=True)
class DataFileSummary:
    path: Path
    exists: bool
    rows: int = 0
    sha256: str | None = None
    min_date: date | None = None
    max_date: date | None = None


@dataclass(frozen=True)
class DataQualityReport:
    checked_at: datetime
    as_of: date
    price_summary: DataFileSummary
    rate_summary: DataFileSummary
    expected_price_tickers: tuple[str, ...]
    expected_rate_series: tuple[str, ...]
    manifest_summary: DataFileSummary | None = None
    issues: tuple[DataQualityIssue, ...] = field(default_factory=tuple)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == Severity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == Severity.WARNING)

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
) -> DataQualityReport:
    issues: list[DataQualityIssue] = []

    prices, price_summary = _read_csv(prices_path, issues, "prices")
    rates, rate_summary = _read_csv(rates_path, issues, "rates")
    manifest_summary = (
        _validate_download_manifest(
            manifest_path,
            price_summary=price_summary,
            rate_summary=rate_summary,
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
        manifest_summary=manifest_summary,
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
        "",
        "## 文件",
        "",
        _render_file_summary("价格数据", report.price_summary),
        _render_file_summary("利率数据", report.rate_summary),
        *(
            [_render_file_summary("下载审计清单", report.manifest_summary)]
            if report.manifest_summary is not None
            else []
        ),
        "",
        "## 预期覆盖范围",
        "",
        f"- 价格标的：{', '.join(report.expected_price_tickers)}",
        f"- 利率序列：{', '.join(report.expected_rate_series)}",
        "",
        "## 问题",
        "",
    ]

    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | 行数 | 说明 | 样例 |",
                "|---|---|---:|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.rows if issue.rows is not None else ''} | "
                f"{_escape_markdown_table(issue.message)} | "
                f"{_escape_markdown_table(issue.sample or '')} |"
            )

    return "\n".join(lines) + "\n"


def write_data_quality_report(report: DataQualityReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_data_quality_report(report), encoding="utf-8")
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
    issues: list[DataQualityIssue],
) -> DataFileSummary:
    if not path.exists():
        issues.append(
            DataQualityIssue(
                Severity.WARNING,
                "download_manifest_missing",
                f"下载审计清单不存在：{path}。请重新执行 download-data 生成审计记录。",
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
            )
        )
        return summary

    _check_manifest_covers_file(manifest, price_summary, "prices", issues)
    _check_manifest_covers_file(manifest, rate_summary, "rates", issues)
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
        )
    )


def _validate_prices(
    prices: pd.DataFrame,
    summary: DataFileSummary,
    expected_tickers: list[str],
    quality_config: DataQualityConfig,
    as_of: date,
    issues: list[DataQualityIssue],
) -> DataFileSummary:
    if prices.empty:
        issues.append(DataQualityIssue(Severity.ERROR, "prices_empty", "价格数据没有任何行"))
        return summary

    if not _check_required_columns(prices, PRICE_REQUIRED_COLUMNS, "prices", issues):
        return summary

    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    invalid_dates = frame["_date"].isna()
    if invalid_dates.any():
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "prices_invalid_date",
                "价格数据包含无法解析的日期",
                rows=int(invalid_dates.sum()),
                sample=_sample_rows(frame.loc[invalid_dates], ["date", "ticker"]),
            )
        )

    _check_duplicate_keys(frame, ["date", "ticker"], "prices", issues)
    _check_expected_values(frame, "ticker", expected_tickers, "prices", issues)
    _check_price_numeric_rules(frame, issues)
    _check_price_staleness(frame, expected_tickers, quality_config, as_of, issues)
    _check_price_moves(frame, quality_config, issues)

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
        issues.append(DataQualityIssue(Severity.ERROR, "rates_empty", "利率数据没有任何行"))
        return summary

    if not _check_required_columns(rates, RATE_REQUIRED_COLUMNS, "rates", issues):
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
                "利率数据包含无法解析的日期",
                rows=int(invalid_dates.sum()),
                sample=_sample_rows(frame.loc[invalid_dates], ["date", "series"]),
            )
        )

    invalid_values = frame["_value"].isna()
    if invalid_values.any():
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "rates_invalid_value",
                "利率数据包含缺失或非数值",
                rows=int(invalid_values.sum()),
                sample=_sample_rows(frame.loc[invalid_values], ["date", "series", "value"]),
            )
        )

    _check_duplicate_keys(frame, ["date", "series"], "rates", issues)
    _check_expected_values(frame, "series", expected_series, "rates", issues)
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
) -> bool:
    missing = [column for column in required_columns if column not in data.columns]
    if missing:
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                f"{label}_missing_columns",
                f"{_data_label(label)}缺少必需字段：{', '.join(missing)}",
            )
        )
        return False
    return True


def _check_duplicate_keys(
    data: pd.DataFrame,
    key_columns: list[str],
    label: str,
    issues: list[DataQualityIssue],
) -> None:
    duplicates = data.duplicated(subset=key_columns, keep=False)
    if duplicates.any():
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                f"{label}_duplicate_keys",
                f"{_data_label(label)}存在重复主键：{', '.join(key_columns)}",
                rows=int(duplicates.sum()),
                sample=_sample_rows(data.loc[duplicates], key_columns),
            )
        )


def _check_expected_values(
    data: pd.DataFrame,
    column: str,
    expected_values: list[str],
    label: str,
    issues: list[DataQualityIssue],
) -> None:
    present = set(str(value) for value in data[column].dropna().unique())
    missing = [value for value in expected_values if value not in present]
    if missing:
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                f"{label}_missing_expected_values",
                f"{_data_label(label)}缺少预期的 {column}：{', '.join(missing)}",
            )
        )


def _check_price_numeric_rules(
    frame: pd.DataFrame,
    issues: list[DataQualityIssue],
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
                    Severity.ERROR,
                    f"prices_invalid_{column}",
                    f"价格数据的 {column} 包含缺失或非数值",
                    rows=int(invalid.sum()),
                    sample=_sample_rows(frame.loc[invalid], ["date", "ticker", column]),
                )
            )

        non_positive = frame[f"_{column}"] <= 0
        if non_positive.any():
            issues.append(
                DataQualityIssue(
                    Severity.ERROR,
                    f"prices_non_positive_{column}",
                    f"价格数据的 {column} 包含非正数",
                    rows=int(non_positive.sum()),
                    sample=_sample_rows(frame.loc[non_positive], ["date", "ticker", column]),
                )
            )

    invalid_volume = frame["_volume"].notna() & (frame["_volume"] < 0)
    if invalid_volume.any():
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "prices_negative_volume",
                "价格数据包含负成交量",
                rows=int(invalid_volume.sum()),
                sample=_sample_rows(frame.loc[invalid_volume], ["date", "ticker", "volume"]),
            )
        )

    missing_volume = frame["_volume"].isna()
    if missing_volume.any():
        issues.append(
            DataQualityIssue(
                Severity.WARNING,
                "prices_missing_volume",
                "价格数据的成交量包含缺失或非数值",
                rows=int(missing_volume.sum()),
                sample=_sample_rows(frame.loc[missing_volume], ["date", "ticker", "volume"]),
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
                Severity.ERROR,
                "prices_invalid_ohlc",
                "价格数据违反 OHLC 逻辑约束",
                rows=int(ohlc_invalid.sum()),
                sample=_sample_rows(
                    frame.loc[ohlc_invalid],
                    ["date", "ticker", "open", "high", "low", "close"],
                ),
            )
        )


def _check_price_staleness(
    frame: pd.DataFrame,
    expected_tickers: list[str],
    quality_config: DataQualityConfig,
    as_of: date,
    issues: list[DataQualityIssue],
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
                Severity.ERROR,
                "prices_future_dates",
                "价格数据包含评估日期之后的数据",
                sample=", ".join(future[:10]),
            )
        )
    if stale:
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "prices_stale",
                "价格数据最新日期过旧，不能用于评分",
                sample=", ".join(stale[:10]),
            )
        )


def _check_price_moves(
    frame: pd.DataFrame,
    quality_config: DataQualityConfig,
    issues: list[DataQualityIssue],
) -> None:
    data = frame.loc[
        frame["_date"].notna() & frame["_adj_close"].notna() & (frame["_adj_close"] > 0)
    ].copy()
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
                Severity.ERROR,
                "prices_extreme_adj_close_move",
                "价格数据包含极端的调整收盘价单日波动",
                rows=int(extreme.sum()),
                sample=_sample_rows(
                    data.loc[extreme],
                    ["date", "ticker", "adj_close", "_return", "_extreme_return_threshold"],
                ),
            )
        )
    if suspicious.any():
        issues.append(
            DataQualityIssue(
                Severity.WARNING,
                "prices_suspicious_adj_close_move",
                "价格数据包含可疑的调整收盘价单日波动",
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
            )
        )

    data["_adjustment_ratio"] = data["_adj_close"] / data["_close"]
    data["_adjustment_ratio_change"] = data.groupby("ticker")["_adjustment_ratio"].pct_change()
    ratio_jump = (
        data["_adjustment_ratio_change"].abs()
        > quality_config.prices.suspicious_adjustment_ratio_change_abs
    )
    if ratio_jump.any():
        issues.append(
            DataQualityIssue(
                Severity.WARNING,
                "prices_adjustment_ratio_jump",
                "价格数据的复权比例出现明显跳变",
                rows=int(ratio_jump.sum()),
                sample=_sample_rows(
                    data.loc[ratio_jump],
                    ["date", "ticker", "_adjustment_ratio", "_adjustment_ratio_change"],
                ),
            )
        )


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


def _check_rate_ranges(
    frame: pd.DataFrame,
    quality_config: DataQualityConfig,
    issues: list[DataQualityIssue],
) -> None:
    invalid = frame["_value"].notna() & (
        (frame["_value"] < quality_config.rates.min_plausible_value)
        | (frame["_value"] > quality_config.rates.max_plausible_value)
    )
    if invalid.any():
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "rates_out_of_range",
                "利率数据包含超出配置合理范围的数值",
                rows=int(invalid.sum()),
                sample=_sample_rows(frame.loc[invalid], ["date", "series", "value"]),
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
        if lag_days < 0:
            future.append(f"{series}:{latest_date.isoformat()}")
        elif lag_days > quality_config.rates.max_stale_calendar_days:
            stale.append(f"{series}:{latest_date.isoformat()}({lag_days}d)")

    if future:
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "rates_future_dates",
                "利率数据包含评估日期之后的数据",
                sample=", ".join(future[:10]),
            )
        )
    if stale:
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "rates_stale",
                "利率数据最新日期过旧，不能用于评分",
                sample=", ".join(stale[:10]),
            )
        )


def _check_rate_moves(
    frame: pd.DataFrame,
    quality_config: DataQualityConfig,
    issues: list[DataQualityIssue],
) -> None:
    data = frame.loc[frame["_date"].notna() & frame["_value"].notna()].copy()
    if data.empty:
        return

    data = data.sort_values(["series", "_date"])
    data["_change"] = data.groupby("series")["_value"].diff().abs()
    extreme = data["_change"] > quality_config.rates.extreme_daily_change_abs
    suspicious = (data["_change"] > quality_config.rates.suspicious_daily_change_abs) & ~extreme

    if extreme.any():
        issues.append(
            DataQualityIssue(
                Severity.ERROR,
                "rates_extreme_daily_change",
                "利率数据包含极端单日变化",
                rows=int(extreme.sum()),
                sample=_sample_rows(data.loc[extreme], ["date", "series", "value", "_change"]),
            )
        )
    if suspicious.any():
        issues.append(
            DataQualityIssue(
                Severity.WARNING,
                "rates_suspicious_daily_change",
                "利率数据包含可疑单日变化",
                rows=int(suspicious.sum()),
                sample=_sample_rows(data.loc[suspicious], ["date", "series", "value", "_change"]),
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


def _sample_rows(data: pd.DataFrame, columns: list[str], max_rows: int = 3) -> str:
    available_columns = [column for column in columns if column in data.columns]
    if not available_columns:
        return ""
    records = data[available_columns].head(max_rows).to_dict(orient="records")
    return "; ".join(str(record) for record in records)


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
    if severity == Severity.ERROR:
        return "错误"
    return "警告"


def _data_label(label: str) -> str:
    return {
        "prices": "价格数据",
        "rates": "利率数据",
        "manifest": "下载审计清单",
    }.get(label, label)


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
