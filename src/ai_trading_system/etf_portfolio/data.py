from __future__ import annotations

from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.data.market_data import PriceRequest, YFinancePriceProvider
from ai_trading_system.etf_portfolio.models import (
    ETFAssetsConfig,
    ETFQualityReport,
    ETFStrategyConfig,
    ETFValidationIssue,
)

PRICE_COLUMNS = (
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "source",
    "created_at",
)
MISSING_PRICE_METADATA_CREATED_AT = "price_metadata_missing_v1"


def read_price_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"ETF price file does not exist: {path}")
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def write_price_frame(prices: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = prices.copy()
    if path.suffix.lower() == ".parquet":
        frame.to_parquet(path, index=False)
    else:
        frame.to_csv(path, index=False)
    return path


def load_standard_prices(
    path: Path,
    assets: ETFAssetsConfig,
    strategy: ETFStrategyConfig | None = None,
    extra_symbols: set[str] | None = None,
) -> tuple[pd.DataFrame, ETFQualityReport]:
    raw = read_price_frame(path)
    prices, metadata_issues = standardize_price_frame(
        raw,
        assets=assets,
        source_name=str(path),
        extra_symbols=extra_symbols,
    )
    report = validate_price_data(
        prices,
        assets=assets,
        strategy=strategy,
        as_of=None,
        extra_issues=metadata_issues,
    )
    return prices, report


def standardize_price_frame(
    prices: pd.DataFrame,
    *,
    assets: ETFAssetsConfig,
    source_name: str,
    extra_symbols: set[str] | None = None,
) -> tuple[pd.DataFrame, tuple[ETFValidationIssue, ...]]:
    issues: list[ETFValidationIssue] = []
    frame = prices.copy()
    if "symbol" not in frame.columns and "ticker" in frame.columns:
        frame["symbol"] = frame["ticker"]
        issues.append(
            ETFValidationIssue(
                "INFO",
                "price_symbol_from_ticker",
                "价格缓存使用 ticker 字段，已标准化为 symbol。",
            )
        )
    if "adj_close" not in frame.columns and "close" in frame.columns:
        frame["adj_close"] = frame["close"]
        issues.append(
            ETFValidationIssue(
                "WARNING",
                "price_adj_close_from_close",
                "价格缓存缺少 adj_close，已用 close 标准化；收益口径需在报告披露。",
            )
        )
    if "source" not in frame.columns:
        frame["source"] = source_name
        issues.append(
            ETFValidationIssue(
                "INFO",
                "price_source_added",
                "价格缓存缺少 source 字段，已使用输入路径作为 source metadata。",
            )
        )
    if "created_at" not in frame.columns:
        if "updated_at" in frame.columns:
            frame["created_at"] = frame["updated_at"]
        else:
            frame["created_at"] = MISSING_PRICE_METADATA_CREATED_AT
        issues.append(
            ETFValidationIssue(
                "INFO",
                "price_created_at_added",
                (
                    "价格缓存缺少 created_at 字段，已使用 updated_at 或 deterministic metadata "
                    "placeholder 补齐。"
                ),
            )
        )
    frame["created_at"] = frame["created_at"].fillna("").astype(str)
    empty_created = frame["created_at"].str.strip() == ""
    if empty_created.any():
        frame.loc[empty_created, "created_at"] = MISSING_PRICE_METADATA_CREATED_AT
        issues.append(
            ETFValidationIssue(
                "INFO",
                "price_created_at_empty_filled",
                "部分 created_at 为空，已在标准化视图中补齐 deterministic metadata placeholder。",
                rows=int(empty_created.sum()),
            )
        )

    for column in ("open", "high", "low", "close", "adj_close", "volume"):
        if column not in frame.columns:
            frame[column] = pd.NA
    allowed_symbols = set(assets.assets) | set(extra_symbols or set())
    frame = frame.loc[frame.get("symbol").isin(sorted(allowed_symbols))].copy()
    frame = ensure_cash_prices(frame)
    ordered = [column for column in PRICE_COLUMNS if column in frame.columns]
    return frame[ordered].sort_values(["symbol", "date"]).reset_index(drop=True), tuple(issues)


def ensure_cash_prices(prices: pd.DataFrame) -> pd.DataFrame:
    if "date" not in prices.columns:
        return prices
    dates = sorted(str(value) for value in prices["date"].dropna().unique())
    if not dates:
        return prices
    existing_cash_dates: set[str] = set()
    if "symbol" in prices.columns:
        existing_cash_dates = set(
            str(value)
            for value in prices.loc[prices["symbol"] == "CASH", "date"].dropna().unique()
        )
    synthetic_created_at = _synthetic_cash_created_at(prices)
    cash_rows = [
        {
            "date": item,
            "symbol": "CASH",
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "adj_close": 1.0,
            "volume": 0.0,
            "source": "synthetic_cash",
            "created_at": synthetic_created_at,
        }
        for item in dates
        if item not in existing_cash_dates
    ]
    if not cash_rows:
        return prices
    return pd.concat([prices, pd.DataFrame(cash_rows)], ignore_index=True)


def _synthetic_cash_created_at(prices: pd.DataFrame) -> str:
    if "created_at" not in prices.columns:
        return MISSING_PRICE_METADATA_CREATED_AT
    values = sorted(
        str(value).strip()
        for value in prices["created_at"].dropna().unique()
        if str(value).strip()
    )
    return values[-1] if values else MISSING_PRICE_METADATA_CREATED_AT


def validate_price_data(
    prices: pd.DataFrame,
    *,
    assets: ETFAssetsConfig,
    strategy: ETFStrategyConfig | None,
    as_of: date | None,
    extra_issues: tuple[ETFValidationIssue, ...] = (),
) -> ETFQualityReport:
    issues: list[ETFValidationIssue] = list(extra_issues)
    if prices.empty:
        issues.append(ETFValidationIssue("ERROR", "prices_empty", "ETF 价格数据为空。"))
        return _quality_report(prices, issues, as_of)

    missing_columns = [column for column in PRICE_COLUMNS if column not in prices.columns]
    if missing_columns:
        issues.append(
            ETFValidationIssue(
                "ERROR",
                "prices_missing_columns",
                f"ETF 价格数据缺少必需字段：{', '.join(missing_columns)}。",
            )
        )
        return _quality_report(prices, issues, as_of)

    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    invalid_dates = frame["_date"].isna()
    if invalid_dates.any():
        issues.append(
            ETFValidationIssue(
                "ERROR",
                "prices_invalid_date",
                "ETF 价格数据包含无法解析的日期。",
                rows=int(invalid_dates.sum()),
                sample=_sample_rows(frame.loc[invalid_dates], ["date", "symbol"]),
            )
        )

    duplicate_keys = frame.duplicated(["date", "symbol"], keep=False)
    if duplicate_keys.any():
        issues.append(
            ETFValidationIssue(
                "ERROR",
                "prices_duplicate_date_symbol",
                "ETF 价格数据存在重复 date + symbol。",
                rows=int(duplicate_keys.sum()),
                sample=_sample_rows(frame.loc[duplicate_keys], ["date", "symbol"]),
            )
        )

    expected_symbols = set(assets.assets)
    missing_symbols = sorted(expected_symbols - set(frame["symbol"].astype(str)))
    if missing_symbols:
        issues.append(
            ETFValidationIssue(
                "ERROR",
                "prices_missing_required_symbols",
                f"ETF 价格数据缺少关键资产：{', '.join(missing_symbols)}。",
            )
        )

    numeric_columns = ["open", "high", "low", "close", "adj_close", "volume"]
    for column in numeric_columns:
        frame[f"_{column}"] = pd.to_numeric(frame[column], errors="coerce")

    price_columns = ["_open", "_high", "_low", "_close", "_adj_close"]
    invalid_prices = frame[price_columns].isna().any(axis=1) | (
        frame[price_columns] <= 0
    ).any(axis=1)
    if invalid_prices.any():
        issues.append(
            ETFValidationIssue(
                "ERROR",
                "prices_non_positive_or_missing",
                "ETF 价格数据包含缺失、非数值或非正 OHLC/adj_close。",
                rows=int(invalid_prices.sum()),
                sample=_sample_rows(frame.loc[invalid_prices], ["date", "symbol", "close"]),
            )
        )

    invalid_volume = frame["_volume"].notna() & (frame["_volume"] < 0)
    if invalid_volume.any():
        issues.append(
            ETFValidationIssue(
                "ERROR",
                "prices_negative_volume",
                "ETF 价格数据包含负 volume。",
                rows=int(invalid_volume.sum()),
                sample=_sample_rows(frame.loc[invalid_volume], ["date", "symbol", "volume"]),
            )
        )

    if as_of is not None and "_date" in frame.columns:
        future_rows = frame["_date"] > pd.Timestamp(as_of)
        if future_rows.any():
            issues.append(
                ETFValidationIssue(
                    "ERROR",
                    "prices_future_dates",
                    "ETF 价格数据包含评估日期之后的记录。",
                    rows=int(future_rows.sum()),
                    sample=_sample_rows(frame.loc[future_rows], ["date", "symbol"]),
                )
            )

    _append_return_issues(frame, strategy, issues)
    _append_start_date_info(frame, assets, issues)
    return _quality_report(frame, issues, as_of)


def write_quality_report(report: ETFQualityReport, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# ETF Portfolio Data Quality Report",
        "",
        f"- 状态：{report.status}",
        f"- 检查时间：{report.checked_at.isoformat()}",
        f"- 评估日期：{report.as_of.isoformat() if report.as_of else 'n/a'}",
        f"- 行数：{report.row_count}",
        f"- 标的：{', '.join(report.symbols)}",
        f"- 日期范围：{report.min_date.isoformat() if report.min_date else 'n/a'} 至 "
        f"{report.max_date.isoformat() if report.max_date else 'n/a'}",
        f"- checksum：{report.checksum or 'n/a'}",
        "",
        "## 问题",
        "",
    ]
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(["| 级别 | Code | 行数 | 说明 | 样例 |", "|---|---|---:|---|---|"])
        for issue in report.issues:
            lines.append(
                "| "
                f"{issue.severity} | "
                f"{issue.code} | "
                f"{issue.rows if issue.rows is not None else ''} | "
                f"{_escape(issue.message)} | "
                f"{_escape(issue.sample)} |"
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def ingest_yfinance_prices(
    *,
    symbols: list[str],
    start: date,
    end: date,
    output_path: Path,
) -> Path:
    provider = YFinancePriceProvider()
    raw = provider.download_prices(PriceRequest(tickers=symbols, start=start, end=end))
    frame = raw.rename(columns={"ticker": "symbol"}).copy()
    frame["source"] = "Yahoo Finance via yfinance"
    frame["created_at"] = datetime.now(UTC).isoformat()
    return write_price_frame(frame[list(PRICE_COLUMNS)], output_path)


def latest_price_date(prices: pd.DataFrame) -> date:
    if "date" not in prices.columns:
        raise ValueError("ETF price data missing date column")
    parsed = pd.to_datetime(prices["date"], errors="coerce").dropna()
    if parsed.empty:
        raise ValueError("ETF price data has no valid dates")
    return parsed.max().date()


def _append_return_issues(
    frame: pd.DataFrame,
    strategy: ETFStrategyConfig | None,
    issues: list[ETFValidationIssue],
) -> None:
    if strategy is None or "_adj_close" not in frame.columns:
        return
    data = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()].copy()
    if data.empty:
        return
    data = data.sort_values(["symbol", "_date"])
    data["_ret_1d"] = data.groupby("symbol")["_adj_close"].pct_change()
    abs_return = data["_ret_1d"].abs()
    extreme = abs_return > strategy.data_quality.extreme_daily_return_abs
    suspicious = (
        abs_return > strategy.data_quality.suspicious_daily_return_abs
    ) & ~extreme
    if extreme.any():
        issues.append(
            ETFValidationIssue(
                "ERROR",
                "prices_extreme_daily_return",
                "ETF 价格数据包含超过配置阈值的极端单日收益。",
                rows=int(extreme.sum()),
                sample=_sample_rows(data.loc[extreme], ["date", "symbol", "_ret_1d"]),
            )
        )
    if suspicious.any():
        issues.append(
            ETFValidationIssue(
                "WARNING",
                "prices_suspicious_daily_return",
                "ETF 价格数据包含可疑单日收益，需审计来源或 corporate action。",
                rows=int(suspicious.sum()),
                sample=_sample_rows(data.loc[suspicious], ["date", "symbol", "_ret_1d"]),
            )
        )


def _append_start_date_info(
    frame: pd.DataFrame,
    assets: ETFAssetsConfig,
    issues: list[ETFValidationIssue],
) -> None:
    if "_date" not in frame.columns:
        return
    for symbol in assets.tradeable_symbols:
        symbol_rows = frame.loc[frame["symbol"] == symbol]
        if symbol_rows.empty:
            continue
        first_date = symbol_rows["_date"].min()
        if pd.isna(first_date):
            continue
        issues.append(
            ETFValidationIssue(
                "INFO",
                "prices_symbol_start_date",
                f"{symbol} 有效价格起始日期：{pd.Timestamp(first_date).date().isoformat()}。",
            )
        )


def _quality_report(
    prices: pd.DataFrame,
    issues: list[ETFValidationIssue],
    as_of: date | None,
) -> ETFQualityReport:
    if "date" in prices.columns:
        parsed_dates = pd.to_datetime(prices["date"], errors="coerce").dropna()
    else:
        parsed_dates = pd.Series(dtype="datetime64[ns]")
    symbols = (
        tuple(sorted(str(value) for value in prices["symbol"].dropna().unique()))
        if "symbol" in prices.columns
        else ()
    )
    status = "FAIL" if any(issue.severity == "ERROR" for issue in issues) else (
        "PASS_WITH_WARNINGS" if any(issue.severity == "WARNING" for issue in issues) else "PASS"
    )
    return ETFQualityReport(
        checked_at=datetime.now(UTC),
        as_of=as_of,
        status=status,
        row_count=len(prices),
        symbols=symbols,
        min_date=parsed_dates.min().date() if not parsed_dates.empty else None,
        max_date=parsed_dates.max().date() if not parsed_dates.empty else None,
        checksum=_frame_checksum(prices) if not prices.empty else None,
        issues=tuple(issues),
    )


def _frame_checksum(frame: pd.DataFrame) -> str:
    records = frame.astype(str).sort_index(axis=1).to_csv(index=False).encode("utf-8")
    return sha256(records).hexdigest()


def _sample_rows(frame: pd.DataFrame, columns: list[str], max_rows: int = 3) -> str:
    available = [column for column in columns if column in frame.columns]
    if not available:
        return ""
    rows: list[dict[str, Any]] = frame[available].head(max_rows).to_dict(orient="records")
    return "; ".join(str(row) for row in rows)


def _escape(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
