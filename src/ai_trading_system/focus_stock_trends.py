from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from ai_trading_system.config import WatchlistConfig
from ai_trading_system.features.market import MarketFeatureRow, MarketFeatureSet

EXPECTED_TREND_FEATURES = (
    "return_1d",
    "return_5d",
    "return_20d",
    "above_ma_20",
    "above_ma_50",
    "above_ma_100",
    "above_ma_200",
    "pct_vs_ma_50",
    "pct_vs_ma_200",
)


@dataclass(frozen=True)
class FocusStockTrendItem:
    ticker: str
    company_name: str
    source_date: date | None
    trend_status: str
    return_1d: float | None
    return_5d: float | None
    return_20d: float | None
    above_ma_20: bool | None
    above_ma_50: bool | None
    above_ma_100: bool | None
    above_ma_200: bool | None
    pct_vs_ma_50: float | None
    pct_vs_ma_200: float | None
    data_coverage: float
    missing_features: tuple[str, ...]
    explanation: str


@dataclass(frozen=True)
class FocusStockTrendReport:
    as_of: date
    items: tuple[FocusStockTrendItem, ...]
    production_effect: str = "none"

    @property
    def status(self) -> str:
        if not self.items or any(
            item.data_coverage < 1.0 or item.trend_status == "data_gap"
            for item in self.items
        ):
            return "PASS_WITH_WARNINGS"
        return "PASS"

    @property
    def ticker_count(self) -> int:
        return len(self.items)


def build_focus_stock_trend_report(
    *,
    feature_set: MarketFeatureSet,
    tickers: list[str] | tuple[str, ...],
    watchlist: WatchlistConfig | None = None,
) -> FocusStockTrendReport:
    ticker_list = tuple(dict.fromkeys(ticker.upper() for ticker in tickers if ticker))
    watchlist_names = _watchlist_name_map(watchlist)
    rows_by_ticker: dict[str, list[MarketFeatureRow]] = {}
    for row in feature_set.rows:
        rows_by_ticker.setdefault(row.subject.upper(), []).append(row)

    items = tuple(
        _build_item(
            ticker=ticker,
            company_name=watchlist_names.get(ticker, ticker),
            rows=tuple(rows_by_ticker.get(ticker, ())),
        )
        for ticker in ticker_list
    )
    return FocusStockTrendReport(as_of=feature_set.as_of, items=items)


def render_focus_stock_trend_section(report: FocusStockTrendReport) -> str:
    lines = [
        "## 关注股票趋势分析",
        "",
        f"- 状态：{report.status}",
        f"- 覆盖 ticker：{report.ticker_count}",
        f"- 生产影响：{report.production_effect}",
        "- 解释边界：本节只读使用已通过市场数据质量门禁后的价格/趋势特征，"
        "用于解释核心观察池个股走势；不改变评分、仓位闸门、执行建议或 prediction ledger。",
        f"- 汇总：{_trend_summary(report)}",
        "",
        "| Ticker | 公司 | 趋势状态 | Source Date | 1D | 5D | 20D | "
        "均线位置 | vs MA50 | vs MA200 | 数据覆盖 | 解读 |",
        "|---|---|---|---|---:|---:|---:|---|---:|---:|---:|---|",
    ]
    for item in report.items:
        lines.append(
            "| "
            f"{item.ticker} | "
            f"{_escape_markdown_table(item.company_name)} | "
            f"{_trend_status_label(item.trend_status)} | "
            f"{'' if item.source_date is None else item.source_date.isoformat()} | "
            f"{_format_pct(item.return_1d)} | "
            f"{_format_pct(item.return_5d)} | "
            f"{_format_pct(item.return_20d)} | "
            f"{_ma_position_summary(item)} | "
            f"{_format_pct(item.pct_vs_ma_50)} | "
            f"{_format_pct(item.pct_vs_ma_200)} | "
            f"{item.data_coverage:.0%} | "
            f"{_escape_markdown_table(item.explanation)} |"
        )
    return "\n".join(lines) + "\n"


def _build_item(
    *,
    ticker: str,
    company_name: str,
    rows: tuple[MarketFeatureRow, ...],
) -> FocusStockTrendItem:
    feature_map = {row.feature: row for row in rows}
    missing = tuple(
        feature for feature in EXPECTED_TREND_FEATURES if feature not in feature_map
    )
    coverage = (len(EXPECTED_TREND_FEATURES) - len(missing)) / len(
        EXPECTED_TREND_FEATURES
    )
    source_date = max((row.source_date for row in rows), default=None)
    item = FocusStockTrendItem(
        ticker=ticker,
        company_name=company_name,
        source_date=source_date,
        trend_status="data_gap",
        return_1d=_feature_value(feature_map, "return_1d"),
        return_5d=_feature_value(feature_map, "return_5d"),
        return_20d=_feature_value(feature_map, "return_20d"),
        above_ma_20=_feature_bool(feature_map, "above_ma_20"),
        above_ma_50=_feature_bool(feature_map, "above_ma_50"),
        above_ma_100=_feature_bool(feature_map, "above_ma_100"),
        above_ma_200=_feature_bool(feature_map, "above_ma_200"),
        pct_vs_ma_50=_feature_value(feature_map, "pct_vs_ma_50"),
        pct_vs_ma_200=_feature_value(feature_map, "pct_vs_ma_200"),
        data_coverage=coverage,
        missing_features=missing,
        explanation="",
    )
    status = _classify_item(item)
    return FocusStockTrendItem(
        ticker=item.ticker,
        company_name=item.company_name,
        source_date=item.source_date,
        trend_status=status,
        return_1d=item.return_1d,
        return_5d=item.return_5d,
        return_20d=item.return_20d,
        above_ma_20=item.above_ma_20,
        above_ma_50=item.above_ma_50,
        above_ma_100=item.above_ma_100,
        above_ma_200=item.above_ma_200,
        pct_vs_ma_50=item.pct_vs_ma_50,
        pct_vs_ma_200=item.pct_vs_ma_200,
        data_coverage=item.data_coverage,
        missing_features=item.missing_features,
        explanation=_item_explanation(item, status),
    )


def _classify_item(item: FocusStockTrendItem) -> str:
    if item.data_coverage < 0.50:
        return "data_gap"

    ma_values = (
        item.above_ma_20,
        item.above_ma_50,
        item.above_ma_100,
        item.above_ma_200,
    )
    available_ma = [value for value in ma_values if value is not None]
    above_count = sum(1 for value in available_ma if value)
    return_20d = item.return_20d
    return_5d = item.return_5d

    if (
        item.above_ma_200 is False
        and return_20d is not None
        and return_20d < 0
        and above_count <= 1
    ):
        return "downtrend"
    if item.above_ma_50 is False or (return_20d is not None and return_20d <= -0.05):
        return "weakening"
    if above_count >= 3 and return_20d is not None and return_20d > 0:
        if return_5d is not None and return_5d < -0.03:
            return "uptrend_pullback"
        return "uptrend"
    if above_count >= 2:
        return "range_constructive"
    return "mixed"


def _item_explanation(item: FocusStockTrendItem, status: str) -> str:
    if status == "data_gap":
        missing = ", ".join(item.missing_features[:4])
        suffix = " 等" if len(item.missing_features) > 4 else ""
        return f"趋势特征覆盖不足，缺少 {missing}{suffix}，只可作为数据缺口提示。"

    ma_summary = _ma_count_summary(item)
    return_20d = "缺少 20 日收益" if item.return_20d is None else (
        f"20 日收益 {_format_pct(item.return_20d)}"
    )
    if status == "uptrend":
        return f"{ma_summary}，{return_20d}，短中期趋势仍在延续。"
    if status == "uptrend_pullback":
        return f"{ma_summary}，{return_20d}，但 5 日收益转负，属于上升趋势内回撤。"
    if status == "range_constructive":
        return f"{ma_summary}，{return_20d}，趋势偏建设性但还不是全面强势。"
    if status == "weakening":
        return f"{ma_summary}，{return_20d}，跌破中期均线或 20 日表现偏弱。"
    if status == "downtrend":
        return f"{ma_summary}，{return_20d}，价格弱于长期均线且动量为负。"
    return f"{ma_summary}，{return_20d}，信号分歧，保持中性观察。"


def _trend_summary(report: FocusStockTrendReport) -> str:
    if not report.items:
        return "未配置关注 ticker。"
    strong = [
        item.ticker
        for item in report.items
        if item.trend_status in {"uptrend", "uptrend_pullback", "range_constructive"}
    ]
    weak = [
        item.ticker
        for item in report.items
        if item.trend_status in {"weakening", "downtrend"}
    ]
    gaps = [item.ticker for item in report.items if item.trend_status == "data_gap"]
    return (
        f"偏强/建设性 {len(strong)} 个"
        f"（{', '.join(strong) or '无'}）；"
        f"转弱/下行 {len(weak)} 个"
        f"（{', '.join(weak) or '无'}）；"
        f"数据缺口 {len(gaps)} 个"
        f"（{', '.join(gaps) or '无'}）。"
    )


def _ma_count_summary(item: FocusStockTrendItem) -> str:
    values = (
        item.above_ma_20,
        item.above_ma_50,
        item.above_ma_100,
        item.above_ma_200,
    )
    available = [value for value in values if value is not None]
    if not available:
        return "缺少均线位置"
    return f"站上 {sum(1 for value in available if value)}/{len(available)} 条均线"


def _ma_position_summary(item: FocusStockTrendItem) -> str:
    values = (
        ("20", item.above_ma_20),
        ("50", item.above_ma_50),
        ("100", item.above_ma_100),
        ("200", item.above_ma_200),
    )
    return " / ".join(f"{window}:{_above_label(value)}" for window, value in values)


def _above_label(value: bool | None) -> str:
    if value is None:
        return "缺"
    return "上" if value else "下"


def _trend_status_label(status: str) -> str:
    labels = {
        "uptrend": "多头延续",
        "uptrend_pullback": "上升回撤",
        "range_constructive": "建设性震荡",
        "mixed": "信号分歧",
        "weakening": "转弱",
        "downtrend": "下行",
        "data_gap": "数据缺口",
    }
    return labels.get(status, status)


def _feature_value(
    feature_map: dict[str, MarketFeatureRow],
    feature: str,
) -> float | None:
    row = feature_map.get(feature)
    return None if row is None else row.value


def _feature_bool(
    feature_map: dict[str, MarketFeatureRow],
    feature: str,
) -> bool | None:
    value = _feature_value(feature_map, feature)
    if value is None:
        return None
    return value > 0.5


def _watchlist_name_map(watchlist: WatchlistConfig | None) -> dict[str, str]:
    if watchlist is None:
        return {}
    return {item.ticker.upper(): item.company_name for item in watchlist.items}


def _format_pct(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:+.1%}"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
