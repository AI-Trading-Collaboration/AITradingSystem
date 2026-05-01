from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self, cast

import pandas as pd
import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from ai_trading_system.config import UniverseConfig, WatchlistConfig, configured_price_tickers
from ai_trading_system.data.quality import DataQualityReport

TradeDirection = Literal["long", "short"]


class TradeIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


class TradeRecord(BaseModel):
    trade_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.-]+$")
    ticker: str = Field(min_length=1)
    direction: TradeDirection
    opened_at: date
    closed_at: date | None = None
    thesis_id: str | None = None
    entry_price: float = Field(gt=0)
    exit_price: float | None = Field(default=None, gt=0)
    quantity: float | None = Field(default=None, gt=0)
    position_size_pct: float | None = Field(default=None, ge=0, le=1)
    fees: float = Field(default=0, ge=0)
    tags: list[str] = Field(default_factory=list)
    notes: str = ""

    @model_validator(mode="after")
    def validate_trade_dates_and_exit(self) -> Self:
        self.ticker = self.ticker.upper()
        if self.closed_at is not None and self.closed_at < self.opened_at:
            raise ValueError("closed_at must be on or after opened_at")
        if self.closed_at is not None and self.exit_price is None:
            raise ValueError("exit_price is required for closed trades")
        return self


@dataclass(frozen=True)
class LoadedTradeRecord:
    trade: TradeRecord
    path: Path


@dataclass(frozen=True)
class TradeLoadError:
    path: Path
    message: str


@dataclass(frozen=True)
class TradeRecordStore:
    input_path: Path
    loaded: tuple[LoadedTradeRecord, ...]
    load_errors: tuple[TradeLoadError, ...]


@dataclass(frozen=True)
class TradeIssue:
    severity: TradeIssueSeverity
    code: str
    message: str
    trade_id: str | None = None
    ticker: str | None = None
    path: Path | None = None


@dataclass(frozen=True)
class TradeValidationReport:
    as_of: date
    input_path: Path
    trades: tuple[LoadedTradeRecord, ...]
    issues: tuple[TradeIssue, ...] = field(default_factory=tuple)

    @property
    def trade_count(self) -> int:
        return len(self.trades)

    @property
    def closed_count(self) -> int:
        return sum(1 for loaded in self.trades if loaded.trade.closed_at is not None)

    @property
    def open_count(self) -> int:
        return self.trade_count - self.closed_count

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == TradeIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == TradeIssueSeverity.WARNING)

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
class TradeReviewItem:
    trade_id: str
    ticker: str
    direction: TradeDirection
    opened_at: date
    measured_until: date
    status: str
    trade_return: float
    benchmark_returns: dict[str, float]
    excess_vs_spy: float | None
    excess_vs_smh: float | None
    attribution_label: str


@dataclass(frozen=True)
class TradeReviewReport:
    as_of: date
    validation_report: TradeValidationReport
    data_quality_report: DataQualityReport
    benchmark_tickers: tuple[str, ...]
    items: tuple[TradeReviewItem, ...]

    @property
    def status(self) -> str:
        if not self.data_quality_report.passed or not self.validation_report.passed:
            return "FAIL"
        if self.validation_report.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


def load_trade_record_store(input_path: Path | str) -> TradeRecordStore:
    path = Path(input_path)
    loaded: list[LoadedTradeRecord] = []
    load_errors: list[TradeLoadError] = []

    for yaml_path in _trade_yaml_paths(path):
        try:
            raw = _load_yaml(yaml_path)
        except OSError as exc:
            load_errors.append(TradeLoadError(path=yaml_path, message=str(exc)))
            continue
        except yaml.YAMLError as exc:
            load_errors.append(TradeLoadError(path=yaml_path, message=f"YAML 解析失败：{exc}"))
            continue

        for raw_item in _raw_trade_items(raw):
            try:
                trade = TradeRecord.model_validate(raw_item)
            except ValidationError as exc:
                load_errors.append(
                    TradeLoadError(path=yaml_path, message=_compact_validation_error(exc))
                )
                continue
            loaded.append(LoadedTradeRecord(trade=trade, path=yaml_path))

    return TradeRecordStore(
        input_path=path,
        loaded=tuple(loaded),
        load_errors=tuple(load_errors),
    )


def validate_trade_record_store(
    store: TradeRecordStore,
    universe: UniverseConfig,
    watchlist: WatchlistConfig,
    as_of: date,
) -> TradeValidationReport:
    issues: list[TradeIssue] = []
    known_tickers = set(configured_price_tickers(universe, include_full_ai_chain=True))
    known_tickers.update(item.ticker for item in watchlist.items)

    for load_error in store.load_errors:
        issues.append(
            TradeIssue(
                severity=TradeIssueSeverity.ERROR,
                code="trade_load_error",
                path=load_error.path,
                message=load_error.message,
            )
        )

    if not store.input_path.exists():
        issues.append(
            TradeIssue(
                severity=TradeIssueSeverity.WARNING,
                code="trade_path_missing",
                path=store.input_path,
                message="交易记录目录或文件不存在；无法进行交易复盘归因。",
            )
        )
    elif not store.loaded and not store.load_errors:
        issues.append(
            TradeIssue(
                severity=TradeIssueSeverity.WARNING,
                code="no_trade_records",
                path=store.input_path,
                message="未发现交易记录 YAML 文件。",
            )
        )

    _check_duplicate_trade_ids(store.loaded, issues)
    for loaded in store.loaded:
        trade = loaded.trade
        if trade.ticker not in known_tickers:
            issues.append(
                TradeIssue(
                    severity=TradeIssueSeverity.ERROR,
                    code="unknown_trade_ticker",
                    trade_id=trade.trade_id,
                    ticker=trade.ticker,
                    path=loaded.path,
                    message="交易记录 ticker 未出现在数据 universe 或观察池中。",
                )
            )
        if trade.opened_at > as_of or (trade.closed_at is not None and trade.closed_at > as_of):
            issues.append(
                TradeIssue(
                    severity=TradeIssueSeverity.ERROR,
                    code="trade_date_in_future",
                    trade_id=trade.trade_id,
                    ticker=trade.ticker,
                    path=loaded.path,
                    message="交易日期晚于评估日期。",
                )
            )
        if trade.thesis_id is None:
            issues.append(
                TradeIssue(
                    severity=TradeIssueSeverity.WARNING,
                    code="trade_without_thesis",
                    trade_id=trade.trade_id,
                    ticker=trade.ticker,
                    path=loaded.path,
                    message="交易记录未关联 thesis，复盘不能判断原始假设是否成立。",
                )
            )

    return TradeValidationReport(
        as_of=as_of,
        input_path=store.input_path,
        trades=store.loaded,
        issues=tuple(issues),
    )


def build_trade_review_report(
    validation_report: TradeValidationReport,
    prices: pd.DataFrame,
    data_quality_report: DataQualityReport,
    benchmark_tickers: tuple[str, ...],
) -> TradeReviewReport:
    if not validation_report.passed:
        return TradeReviewReport(
            as_of=validation_report.as_of,
            validation_report=validation_report,
            data_quality_report=data_quality_report,
            benchmark_tickers=benchmark_tickers,
            items=(),
        )

    close_pivot = _prepare_adjusted_close_pivot(prices)
    required_tickers = tuple(
        dict.fromkeys(
            [loaded.trade.ticker for loaded in validation_report.trades] + list(benchmark_tickers)
        )
    )
    _check_required_tickers(close_pivot, required_tickers)

    items: list[TradeReviewItem] = []
    for loaded in validation_report.trades:
        trade = loaded.trade
        measured_until = trade.closed_at or validation_report.as_of
        trade_return = _trade_return(trade, close_pivot, measured_until)
        benchmark_returns = {
            ticker: _market_return(close_pivot, ticker, trade.opened_at, measured_until)
            for ticker in benchmark_tickers
        }
        excess_vs_spy = (
            trade_return - benchmark_returns["SPY"] if "SPY" in benchmark_returns else None
        )
        excess_vs_smh = (
            trade_return - benchmark_returns["SMH"] if "SMH" in benchmark_returns else None
        )

        items.append(
            TradeReviewItem(
                trade_id=trade.trade_id,
                ticker=trade.ticker,
                direction=trade.direction,
                opened_at=trade.opened_at,
                measured_until=measured_until,
                status="closed" if trade.closed_at else "open",
                trade_return=trade_return,
                benchmark_returns=benchmark_returns,
                excess_vs_spy=excess_vs_spy,
                excess_vs_smh=excess_vs_smh,
                attribution_label=_attribution_label(trade_return, excess_vs_spy, excess_vs_smh),
            )
        )

    return TradeReviewReport(
        as_of=validation_report.as_of,
        validation_report=validation_report,
        data_quality_report=data_quality_report,
        benchmark_tickers=benchmark_tickers,
        items=tuple(items),
    )


def render_trade_review_report(
    report: TradeReviewReport,
    data_quality_report_path: Path,
) -> str:
    validation = report.validation_report
    lines = [
        "# 交易复盘归因报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 交易记录路径：`{validation.input_path}`",
        f"- 数据质量状态：{report.data_quality_report.status}",
        f"- 数据质量报告：`{data_quality_report_path}`",
        f"- 交易数：{validation.trade_count}",
        f"- 已关闭：{validation.closed_count}",
        f"- 未关闭：{validation.open_count}",
        f"- 校验错误数：{validation.error_count}",
        f"- 校验警告数：{validation.warning_count}",
        "",
        "## 交易归因",
        "",
    ]
    if not report.items:
        lines.append("未发现可复核的交易记录，或交易记录校验未通过。")
    else:
        lines.extend(
            [
                "| Trade | Ticker | 状态 | 区间 | 交易收益 | SPY | QQQ | SMH | SOXX | "
                "超额 SPY | 超额 SMH | 归因提示 |",
                "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
            ]
        )
        for item in sorted(report.items, key=lambda value: value.trade_id):
            lines.append(
                "| "
                f"{item.trade_id} | "
                f"{item.ticker} | "
                f"{'已关闭' if item.status == 'closed' else '未关闭'} | "
                f"{item.opened_at.isoformat()} 至 {item.measured_until.isoformat()} | "
                f"{item.trade_return:.1%} | "
                f"{_benchmark_value(item, 'SPY')} | "
                f"{_benchmark_value(item, 'QQQ')} | "
                f"{_benchmark_value(item, 'SMH')} | "
                f"{_benchmark_value(item, 'SOXX')} | "
                f"{_optional_pct(item.excess_vs_spy)} | "
                f"{_optional_pct(item.excess_vs_smh)} | "
                f"{item.attribution_label} |"
            )

    lines.extend(["", "## 校验问题", ""])
    if not validation.issues:
        lines.append("未发现交易记录校验问题。")
    else:
        lines.extend(
            [
                "| 级别 | Code | Trade | Ticker | 说明 |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in validation.issues:
            lines.append(
                "| "
                f"{_severity_label(issue.severity)} | "
                f"{issue.code} | "
                f"{issue.trade_id or ''} | "
                f"{issue.ticker or ''} | "
                f"{_escape_markdown_table(issue.message)} |"
            )

    lines.extend(
        [
            "",
            "## 方法说明",
            "",
            "- 本报告使用交易记录的 entry_price / exit_price 或 as_of 标记价格计算交易收益。",
            "- 基准收益使用同区间调整收盘价计算，用于区分市场 Beta、主题 Beta 和个股表现。",
            "- 归因提示是规则化摘要，不等同于完整因子归因或投资建议。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_trade_review_report(
    report: TradeReviewReport,
    data_quality_report_path: Path,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_trade_review_report(report, data_quality_report_path=data_quality_report_path),
        encoding="utf-8",
    )
    return output_path


def default_trade_review_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"trade_review_{as_of.isoformat()}.md"


def _trade_yaml_paths(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted([*path.glob("*.yaml"), *path.glob("*.yml")])
    return []


def _load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def _raw_trade_items(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, dict) and "trades" in raw:
        trades = raw["trades"]
        if isinstance(trades, list):
            return trades
        return [trades]
    return [raw]


def _compact_validation_error(exc: ValidationError) -> str:
    first_error = exc.errors()[0] if exc.errors() else None
    if not first_error:
        return "trade record schema validation failed"
    location = ".".join(str(part) for part in first_error.get("loc", ()))
    message = str(first_error.get("msg", "schema validation failed"))
    return f"{location}: {message}" if location else message


def _check_duplicate_trade_ids(
    trades: tuple[LoadedTradeRecord, ...],
    issues: list[TradeIssue],
) -> None:
    paths_by_id: dict[str, list[Path]] = {}
    for loaded in trades:
        paths_by_id.setdefault(loaded.trade.trade_id, []).append(loaded.path)

    for trade_id, paths in sorted(paths_by_id.items()):
        if len(paths) <= 1:
            continue
        issues.append(
            TradeIssue(
                severity=TradeIssueSeverity.ERROR,
                code="duplicate_trade_id",
                trade_id=trade_id,
                path=paths[0],
                message="交易 trade_id 重复，无法可靠复盘归因。",
            )
        )


def _prepare_adjusted_close_pivot(prices: pd.DataFrame) -> pd.DataFrame:
    required_columns = {"date", "ticker", "adj_close"}
    missing = sorted(required_columns - set(prices.columns))
    if missing:
        raise ValueError(f"价格数据缺少必需字段：{', '.join(missing)}")

    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()].copy()
    return frame.pivot(index="_date", columns="ticker", values="_adj_close").sort_index()


def _check_required_tickers(close_pivot: pd.DataFrame, tickers: tuple[str, ...]) -> None:
    missing = [ticker for ticker in dict.fromkeys(tickers) if ticker not in close_pivot.columns]
    if missing:
        raise ValueError(f"交易复盘缺少价格标的：{', '.join(missing)}")


def _trade_return(
    trade: TradeRecord,
    close_pivot: pd.DataFrame,
    measured_until: date,
) -> float:
    exit_price = trade.exit_price
    if exit_price is None:
        exit_price = _price_on_or_before(close_pivot, trade.ticker, measured_until)
    raw_return = (exit_price / trade.entry_price) - 1.0
    if trade.direction == "short":
        return -raw_return
    return raw_return


def _market_return(
    close_pivot: pd.DataFrame,
    ticker: str,
    start: date,
    end: date,
) -> float:
    start_price = _price_on_or_before(close_pivot, ticker, start)
    end_price = _price_on_or_before(close_pivot, ticker, end)
    return (end_price / start_price) - 1.0


def _price_on_or_before(close_pivot: pd.DataFrame, ticker: str, target: date) -> float:
    history = close_pivot[ticker].dropna().sort_index()
    eligible = history.loc[history.index <= pd.Timestamp(target)]
    if eligible.empty:
        raise ValueError(f"{ticker} 在 {target.isoformat()} 或之前没有可用价格")
    return float(cast(Any, eligible.iloc[-1]))


def _attribution_label(
    trade_return: float,
    excess_vs_spy: float | None,
    excess_vs_smh: float | None,
) -> str:
    if trade_return < 0:
        if excess_vs_spy is not None and excess_vs_spy < 0:
            return "亏损且跑输市场，需要复盘方向、时机或止损纪律。"
        return "亏损但未明显跑输市场，需区分风险事件和仓位影响。"
    if excess_vs_smh is not None and excess_vs_smh > 0:
        return "跑赢半导体主题基准，可能包含个股 Alpha 或更好择时。"
    if excess_vs_spy is not None and excess_vs_spy > 0:
        return "跑赢市场但未跑赢主题，收益更可能来自 AI 主题 Beta。"
    return "正收益但未跑赢市场，不能把行情 Beta 误判为 Alpha。"


def _benchmark_value(item: TradeReviewItem, ticker: str) -> str:
    value = item.benchmark_returns.get(ticker)
    if value is None:
        return "n/a"
    return f"{value:.1%}"


def _optional_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1%}"


def _severity_label(severity: TradeIssueSeverity) -> str:
    if severity == TradeIssueSeverity.ERROR:
        return "错误"
    return "警告"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
