from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import DataQualityConfig
from ai_trading_system.data.market_data import PriceDataProvider, PriceRequest
from ai_trading_system.data.quality import DataQualityReport

YAHOO_DIAGNOSTIC_SOURCE_ID = "yahoo_finance_daily_prices"
YAHOO_DIAGNOSTIC_PROVIDER = "Yahoo Finance via yfinance"
YAHOO_DIAGNOSTIC_ENDPOINT = "yfinance.download"
YAHOO_DIAGNOSTIC_PRODUCTION_EFFECT = "none"
MARKETSTACK_SELF_CHECK_SOURCE = "第二行情源 Marketstack"
MARKETSTACK_TARGETABLE_SELF_CHECK_CODES = frozenset(
    {
        "prices_invalid_date",
        "prices_duplicate_keys",
        "prices_invalid_open",
        "prices_invalid_high",
        "prices_invalid_low",
        "prices_invalid_close",
        "prices_invalid_adj_close",
        "prices_non_positive_open",
        "prices_non_positive_high",
        "prices_non_positive_low",
        "prices_non_positive_close",
        "prices_non_positive_adj_close",
        "prices_negative_volume",
        "prices_invalid_ohlc",
        "prices_extreme_adj_close_move",
        "prices_suspicious_adj_close_move",
        "prices_adjustment_ratio_jump",
    }
)
PRICE_COLUMNS = ("open", "high", "low", "close", "adj_close", "volume")
OHLC_COLUMNS = ("open", "high", "low", "close")


@dataclass(frozen=True)
class YahooDiagnosticTarget:
    ticker: str
    date: date
    issue_codes: tuple[str, ...]


@dataclass(frozen=True)
class PriceComparison:
    ticker: str
    date: date
    issue_codes: tuple[str, ...]
    primary_ohlc: dict[str, float | None]
    marketstack_ohlc: dict[str, float | None]
    yahoo_ohlc: dict[str, float | None]
    verdict: str


@dataclass(frozen=True)
class YahooPriceDiagnosticReport:
    checked_at: datetime
    as_of: date
    provider: str
    endpoint: str
    request_parameters: dict[str, Any]
    production_effect: str
    quality_report_status: str
    marketstack_self_check_issue_codes: tuple[str, ...]
    targets: tuple[YahooDiagnosticTarget, ...]
    row_count: int
    checksum_sha256: str
    comparisons: tuple[PriceComparison, ...]
    fetch_error: str | None = None
    notes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def status(self) -> str:
        if self.fetch_error:
            return "DIAGNOSTIC_FAILED"
        if not self.marketstack_self_check_issue_codes:
            return "NO_MARKETSTACK_SELF_CHECK_ISSUES"
        if not self.targets:
            return "NO_TARGETABLE_ROWS"
        risky_verdicts = [
            comparison
            for comparison in self.comparisons
            if comparison.verdict != "YAHOO_SUPPORTS_PRIMARY_MARKETSTACK_SELF_CHECK"
        ]
        if risky_verdicts:
            return "PASS_WITH_INVESTIGATION_ITEMS"
        return "PASS"


def build_yahoo_price_diagnostic_report(
    *,
    primary_prices_path: Path,
    marketstack_prices_path: Path,
    quality_report: DataQualityReport,
    quality_config: DataQualityConfig,
    yahoo_provider: PriceDataProvider,
    as_of: date,
    window_days: int = 3,
    max_targets: int = 20,
) -> YahooPriceDiagnosticReport:
    if window_days < 0:
        raise ValueError("window_days must be non-negative")
    if max_targets <= 0:
        raise ValueError("max_targets must be positive")

    primary_prices = (
        pd.read_csv(primary_prices_path) if primary_prices_path.exists() else pd.DataFrame()
    )
    marketstack_prices = (
        pd.read_csv(marketstack_prices_path) if marketstack_prices_path.exists() else pd.DataFrame()
    )
    issue_codes = _marketstack_self_check_issue_codes(quality_report)
    targets = _marketstack_diagnostic_targets(
        marketstack_prices,
        issue_codes=issue_codes,
        quality_config=quality_config,
        max_targets=max_targets,
    )
    request_parameters = _request_parameters_for_targets(targets, window_days=window_days)
    notes = (
        "diagnostic only / production_effect=none",
        "Yahoo 诊断不会写入 prices_daily.csv、prices_marketstack_daily.csv、"
        "评分、仓位闸门或回测真值。",
    )
    if not targets:
        return YahooPriceDiagnosticReport(
            checked_at=datetime.now(UTC),
            as_of=as_of,
            provider=YAHOO_DIAGNOSTIC_PROVIDER,
            endpoint=YAHOO_DIAGNOSTIC_ENDPOINT,
            request_parameters=request_parameters,
            production_effect=YAHOO_DIAGNOSTIC_PRODUCTION_EFFECT,
            quality_report_status=quality_report.status,
            marketstack_self_check_issue_codes=issue_codes,
            targets=targets,
            row_count=0,
            checksum_sha256=_records_checksum(()),
            comparisons=(),
            notes=notes,
        )

    try:
        yahoo_prices = _download_yahoo_target_windows(
            targets,
            yahoo_provider=yahoo_provider,
            window_days=window_days,
        )
        fetch_error = None
    except Exception as exc:  # pragma: no cover - exact provider errors vary by host.
        yahoo_prices = pd.DataFrame()
        fetch_error = f"{exc.__class__.__name__}: {exc}"

    comparisons = _build_comparisons(
        targets=targets,
        primary_prices=primary_prices,
        marketstack_prices=marketstack_prices,
        yahoo_prices=yahoo_prices,
        quality_config=quality_config,
    )
    return YahooPriceDiagnosticReport(
        checked_at=datetime.now(UTC),
        as_of=as_of,
        provider=YAHOO_DIAGNOSTIC_PROVIDER,
        endpoint=YAHOO_DIAGNOSTIC_ENDPOINT,
        request_parameters=request_parameters,
        production_effect=YAHOO_DIAGNOSTIC_PRODUCTION_EFFECT,
        quality_report_status=quality_report.status,
        marketstack_self_check_issue_codes=issue_codes,
        targets=targets,
        row_count=len(yahoo_prices),
        checksum_sha256=_dataframe_checksum(yahoo_prices),
        comparisons=comparisons,
        fetch_error=fetch_error,
        notes=notes,
    )


def render_yahoo_price_diagnostic_report(report: YahooPriceDiagnosticReport) -> str:
    lines = [
        "# Yahoo 价格诊断复查报告",
        "",
        f"- 状态：{report.status}",
        f"- 检查时间：{report.checked_at.isoformat()}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 质量报告状态：{report.quality_report_status}",
        f"- Provider：{report.provider}",
        f"- Endpoint：{report.endpoint}",
        f"- Production effect：{report.production_effect}",
        f"- Yahoo 返回行数：{report.row_count}",
        f"- Yahoo payload checksum：{report.checksum_sha256}",
        f"- Request parameters：`{_json_for_markdown(report.request_parameters)}`",
        "",
        "## 边界",
        "",
        *[f"- {note}" for note in report.notes],
        "- Yahoo 失败、空结果或与 FMP 不一致只进入调查报告，不阻断 score-daily，"
        "也不降低 FMP 主源权威性。",
        "",
        "## Marketstack 自检问题",
        "",
    ]
    if report.marketstack_self_check_issue_codes:
        lines.append("- Codes：" + ", ".join(report.marketstack_self_check_issue_codes))
    else:
        lines.append("未发现 Marketstack 第二源 self-check 目标问题；未请求 Yahoo。")

    if report.fetch_error:
        lines.extend(["", "## Yahoo 请求错误", "", f"- {report.fetch_error}"])

    lines.extend(["", "## 诊断目标", ""])
    if not report.targets:
        lines.append("没有可定位到 ticker/date 的 Marketstack 自检坏行。")
    else:
        lines.extend(["| Ticker | Date | Marketstack issue codes |", "|---|---|---|"])
        for target in report.targets:
            lines.append(
                "| "
                f"{target.ticker} | "
                f"{target.date.isoformat()} | "
                f"{', '.join(target.issue_codes)} |"
            )

    lines.extend(["", "## FMP / Marketstack / Yahoo raw OHLC 对比", ""])
    if not report.comparisons:
        lines.append("没有可比对样本。")
    else:
        lines.extend(
            [
                "| Ticker | Date | FMP close | Marketstack close | Yahoo close | Verdict |",
                "|---|---|---:|---:|---:|---|",
            ]
        )
        for comparison in report.comparisons:
            lines.append(
                "| "
                f"{comparison.ticker} | "
                f"{comparison.date.isoformat()} | "
                f"{_price_label(comparison.primary_ohlc.get('close'))} | "
                f"{_price_label(comparison.marketstack_ohlc.get('close'))} | "
                f"{_price_label(comparison.yahoo_ohlc.get('close'))} | "
                f"{comparison.verdict} |"
            )
    return "\n".join(lines) + "\n"


def write_yahoo_price_diagnostic_report(
    report: YahooPriceDiagnosticReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_yahoo_price_diagnostic_report(report), encoding="utf-8")
    return output_path


def default_yahoo_price_diagnostic_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"yahoo_price_diagnostic_{as_of.isoformat()}.md"


def _marketstack_self_check_issue_codes(report: DataQualityReport) -> tuple[str, ...]:
    codes = {
        issue.code
        for issue in report.issues
        if issue.source == MARKETSTACK_SELF_CHECK_SOURCE
        and issue.code in MARKETSTACK_TARGETABLE_SELF_CHECK_CODES
    }
    return tuple(sorted(codes))


def _marketstack_diagnostic_targets(
    marketstack_prices: pd.DataFrame,
    *,
    issue_codes: tuple[str, ...],
    quality_config: DataQualityConfig,
    max_targets: int,
) -> tuple[YahooDiagnosticTarget, ...]:
    if not issue_codes or marketstack_prices.empty:
        return ()
    if "date" not in marketstack_prices or "ticker" not in marketstack_prices:
        return ()

    frame = marketstack_prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    for column in PRICE_COLUMNS:
        if column in frame:
            frame[f"_{column}"] = pd.to_numeric(frame[column], errors="coerce")

    target_codes: dict[tuple[str, date], set[str]] = {}
    _mark_invalid_date_targets(frame, issue_codes, target_codes)
    _mark_duplicate_targets(frame, issue_codes, target_codes)
    _mark_numeric_targets(frame, issue_codes, target_codes)
    _mark_ohlc_targets(frame, issue_codes, target_codes)
    _mark_price_move_targets(frame, issue_codes, target_codes, quality_config)

    targets = [
        YahooDiagnosticTarget(ticker=ticker, date=target_date, issue_codes=tuple(sorted(codes)))
        for (ticker, target_date), codes in sorted(target_codes.items(), key=lambda item: item[0])
    ]
    return tuple(targets[:max_targets])


def _mark_invalid_date_targets(
    frame: pd.DataFrame,
    issue_codes: tuple[str, ...],
    target_codes: dict[tuple[str, date], set[str]],
) -> None:
    if "prices_invalid_date" not in issue_codes:
        return
    # Invalid dates cannot be fetched from Yahoo by exact signal date, so only duplicate rows with
    # parseable dates are targetable.
    valid_rows = frame.loc[frame["_date"].notna()]
    for _, row in valid_rows.iterrows():
        _add_target_code(target_codes, row, "prices_invalid_date")


def _mark_duplicate_targets(
    frame: pd.DataFrame,
    issue_codes: tuple[str, ...],
    target_codes: dict[tuple[str, date], set[str]],
) -> None:
    if "prices_duplicate_keys" not in issue_codes:
        return
    duplicates = frame.duplicated(subset=["date", "ticker"], keep=False)
    for _, row in frame.loc[duplicates].iterrows():
        _add_target_code(target_codes, row, "prices_duplicate_keys")


def _mark_numeric_targets(
    frame: pd.DataFrame,
    issue_codes: tuple[str, ...],
    target_codes: dict[tuple[str, date], set[str]],
) -> None:
    for column in ("open", "high", "low", "close", "adj_close"):
        if column not in frame:
            continue
        invalid_code = f"prices_invalid_{column}"
        non_positive_code = f"prices_non_positive_{column}"
        if invalid_code in issue_codes:
            invalid = frame[f"_{column}"].isna()
            for _, row in frame.loc[invalid].iterrows():
                _add_target_code(target_codes, row, invalid_code)
        if non_positive_code in issue_codes:
            non_positive = frame[f"_{column}"] <= 0
            for _, row in frame.loc[non_positive].iterrows():
                _add_target_code(target_codes, row, non_positive_code)
    if "prices_negative_volume" in issue_codes and "volume" in frame:
        negative_volume = frame["_volume"].notna() & (frame["_volume"] < 0)
        for _, row in frame.loc[negative_volume].iterrows():
            _add_target_code(target_codes, row, "prices_negative_volume")


def _mark_ohlc_targets(
    frame: pd.DataFrame,
    issue_codes: tuple[str, ...],
    target_codes: dict[tuple[str, date], set[str]],
) -> None:
    if "prices_invalid_ohlc" not in issue_codes:
        return
    if not all(column in frame for column in OHLC_COLUMNS):
        return
    valid_ohlc = frame[[f"_{column}" for column in OHLC_COLUMNS]].notna().all(axis=1)
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
    for _, row in frame.loc[high_invalid | low_invalid].iterrows():
        _add_target_code(target_codes, row, "prices_invalid_ohlc")


def _mark_price_move_targets(
    frame: pd.DataFrame,
    issue_codes: tuple[str, ...],
    target_codes: dict[tuple[str, date], set[str]],
    quality_config: DataQualityConfig,
) -> None:
    move_codes = {
        "prices_extreme_adj_close_move",
        "prices_suspicious_adj_close_move",
        "prices_adjustment_ratio_jump",
    }
    if not move_codes.intersection(issue_codes):
        return
    if not {"adj_close", "close"}.issubset(frame.columns):
        return
    data = frame.loc[frame["_date"].notna()].copy()
    consistency_start = quality_config.prices.consistency_start_date
    if consistency_start is not None:
        data = data.loc[data["_date"] >= pd.Timestamp(consistency_start)]
    data = data.loc[
        data["_adj_close"].notna() & (data["_adj_close"] > 0) & data["_close"].notna()
    ].copy()
    if data.empty:
        return
    data = data.sort_values(["ticker", "_date"])
    data["_return"] = data.groupby("ticker")["_adj_close"].pct_change()
    data["_adjustment_ratio"] = data["_adj_close"] / data["_close"]
    data["_adjustment_ratio_change"] = data.groupby("ticker")["_adjustment_ratio"].pct_change()
    for _, row in data.iterrows():
        ticker = str(row["ticker"])
        abs_return = abs(float(row["_return"])) if pd.notna(row["_return"]) else None
        if (
            "prices_extreme_adj_close_move" in issue_codes
            and abs_return is not None
            and abs_return > _extreme_price_return_threshold(ticker, quality_config)
        ):
            _add_target_code(target_codes, row, "prices_extreme_adj_close_move")
        if (
            "prices_suspicious_adj_close_move" in issue_codes
            and abs_return is not None
            and abs_return > _suspicious_price_return_threshold(ticker, quality_config)
            and abs_return <= _extreme_price_return_threshold(ticker, quality_config)
        ):
            _add_target_code(target_codes, row, "prices_suspicious_adj_close_move")
        ratio_change = row["_adjustment_ratio_change"]
        if (
            "prices_adjustment_ratio_jump" in issue_codes
            and pd.notna(ratio_change)
            and abs(float(ratio_change))
            > quality_config.prices.suspicious_adjustment_ratio_change_abs
        ):
            _add_target_code(target_codes, row, "prices_adjustment_ratio_jump")


def _add_target_code(
    target_codes: dict[tuple[str, date], set[str]],
    row: pd.Series,
    code: str,
) -> None:
    if pd.isna(row.get("_date")):
        return
    ticker = str(row.get("ticker") or "").strip()
    if not ticker:
        return
    target_date = row["_date"].date()
    target_codes.setdefault((ticker, target_date), set()).add(code)


def _download_yahoo_target_windows(
    targets: tuple[YahooDiagnosticTarget, ...],
    *,
    yahoo_provider: PriceDataProvider,
    window_days: int,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    tickers = sorted({target.ticker for target in targets})
    for ticker in tickers:
        ticker_dates = [target.date for target in targets if target.ticker == ticker]
        request = PriceRequest(
            tickers=[ticker],
            start=min(ticker_dates) - timedelta(days=window_days),
            end=max(ticker_dates) + timedelta(days=window_days),
            interval="1d",
        )
        prices = yahoo_provider.download_prices(request)
        if not prices.empty:
            frames.append(prices)
    if not frames:
        return pd.DataFrame(columns=["date", "ticker", *PRICE_COLUMNS])
    return pd.concat(frames, ignore_index=True)


def _build_comparisons(
    *,
    targets: tuple[YahooDiagnosticTarget, ...],
    primary_prices: pd.DataFrame,
    marketstack_prices: pd.DataFrame,
    yahoo_prices: pd.DataFrame,
    quality_config: DataQualityConfig,
) -> tuple[PriceComparison, ...]:
    comparisons: list[PriceComparison] = []
    for target in targets:
        primary_ohlc = _ohlc_for_date(primary_prices, target.ticker, target.date)
        marketstack_ohlc = _ohlc_for_date(marketstack_prices, target.ticker, target.date)
        yahoo_ohlc = _ohlc_for_date(yahoo_prices, target.ticker, target.date)
        comparisons.append(
            PriceComparison(
                ticker=target.ticker,
                date=target.date,
                issue_codes=target.issue_codes,
                primary_ohlc=primary_ohlc,
                marketstack_ohlc=marketstack_ohlc,
                yahoo_ohlc=yahoo_ohlc,
                verdict=_comparison_verdict(
                    primary_ohlc=primary_ohlc,
                    marketstack_ohlc=marketstack_ohlc,
                    yahoo_ohlc=yahoo_ohlc,
                    quality_config=quality_config,
                ),
            )
        )
    return tuple(comparisons)


def _ohlc_for_date(frame: pd.DataFrame, ticker: str, target_date: date) -> dict[str, float | None]:
    empty = {column: None for column in OHLC_COLUMNS}
    if frame.empty or not {"date", "ticker", *OHLC_COLUMNS}.issubset(frame.columns):
        return empty
    data = frame.copy()
    data["_date"] = pd.to_datetime(data["date"], errors="coerce")
    matched = data.loc[
        (data["ticker"].astype(str) == ticker) & (data["_date"] == pd.Timestamp(target_date))
    ]
    if matched.empty:
        return empty
    row = matched.iloc[0]
    return {column: _safe_float(row.get(column)) for column in OHLC_COLUMNS}


def _comparison_verdict(
    *,
    primary_ohlc: dict[str, float | None],
    marketstack_ohlc: dict[str, float | None],
    yahoo_ohlc: dict[str, float | None],
    quality_config: DataQualityConfig,
) -> str:
    primary_close = primary_ohlc.get("close")
    marketstack_close = marketstack_ohlc.get("close")
    yahoo_close = yahoo_ohlc.get("close")
    if yahoo_close is None or yahoo_close <= 0:
        return "YAHOO_MISSING_DIAGNOSTIC_ONLY"
    if primary_close is None or primary_close <= 0:
        return "PRIMARY_MISSING_MANUAL_INVESTIGATION"
    yahoo_primary_diff = _relative_diff(yahoo_close, primary_close)
    threshold = quality_config.prices.secondary_source_adj_close_warning_pct
    if marketstack_close is None or marketstack_close <= 0:
        if yahoo_primary_diff <= threshold:
            return "YAHOO_SUPPORTS_PRIMARY_MARKETSTACK_SELF_CHECK"
        return "YAHOO_DIFFERS_FROM_PRIMARY_MANUAL_INVESTIGATION"

    marketstack_primary_diff = _relative_diff(marketstack_close, primary_close)
    yahoo_marketstack_diff = _relative_diff(yahoo_close, marketstack_close)
    if yahoo_primary_diff <= threshold and marketstack_primary_diff > threshold:
        return "YAHOO_SUPPORTS_PRIMARY_MARKETSTACK_SELF_CHECK"
    if yahoo_marketstack_diff <= threshold and marketstack_primary_diff > threshold:
        return "YAHOO_SUPPORTS_MARKETSTACK_MANUAL_INVESTIGATION"
    if yahoo_primary_diff > threshold:
        return "YAHOO_DIFFERS_FROM_PRIMARY_MANUAL_INVESTIGATION"
    return "YAHOO_NEUTRAL_DIAGNOSTIC_ONLY"


def _request_parameters_for_targets(
    targets: tuple[YahooDiagnosticTarget, ...],
    *,
    window_days: int,
) -> dict[str, Any]:
    if not targets:
        return {"tickers": [], "target_dates": [], "window_days": window_days, "interval": "1d"}
    target_dates = [target.date for target in targets]
    return {
        "tickers": sorted({target.ticker for target in targets}),
        "target_dates": sorted({target.date.isoformat() for target in targets}),
        "window_days": window_days,
        "start": (min(target_dates) - timedelta(days=window_days)).isoformat(),
        "end": (max(target_dates) + timedelta(days=window_days)).isoformat(),
        "interval": "1d",
        "source_type": "public_convenience",
    }


def _dataframe_checksum(frame: pd.DataFrame) -> str:
    if frame.empty:
        return _records_checksum(())
    records = tuple(
        frame.sort_values([column for column in ("ticker", "date") if column in frame.columns])
        .to_dict(orient="records")
    )
    return _records_checksum(records)


def _records_checksum(records: tuple[Any, ...]) -> str:
    encoded = json.dumps(records, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _json_for_markdown(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True).replace("|", "\\|")


def _price_label(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.4f}"


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _relative_diff(value: float, base: float) -> float:
    if base == 0:
        return float("inf")
    return abs(value - base) / abs(base)


def _suspicious_price_return_threshold(ticker: str, config: DataQualityConfig) -> float:
    override = config.prices.ticker_return_threshold_overrides.get(ticker)
    if override and override.suspicious_daily_return_abs is not None:
        return override.suspicious_daily_return_abs
    return config.prices.suspicious_daily_return_abs


def _extreme_price_return_threshold(ticker: str, config: DataQualityConfig) -> float:
    override = config.prices.ticker_return_threshold_overrides.get(ticker)
    if override and override.extreme_daily_return_abs is not None:
        return override.extreme_daily_return_abs
    return config.prices.extreme_daily_return_abs
