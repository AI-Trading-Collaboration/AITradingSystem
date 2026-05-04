from __future__ import annotations

from dataclasses import dataclass

from ai_trading_system.config import IndustryChainConfig, WatchlistConfig
from ai_trading_system.features.market import MarketFeatureSet


@dataclass(frozen=True)
class IndustryNodeHeatItem:
    node_id: str
    node_name: str
    heat_level: str
    heat_score: float | None
    coverage: float
    covered_tickers: tuple[str, ...]
    missing_tickers: tuple[str, ...]
    concentration: float | None
    main_contributors: tuple[str, ...]
    explanation: str


@dataclass(frozen=True)
class IndustryNodeHeatReport:
    as_of: object
    items: tuple[IndustryNodeHeatItem, ...]
    production_effect: str = "none"

    @property
    def status(self) -> str:
        if any(item.coverage == 0 for item in self.items):
            return "PASS_WITH_WARNINGS"
        return "PASS"

    @property
    def node_count(self) -> int:
        return len(self.items)


def build_industry_node_heat_report(
    *,
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
    feature_set: MarketFeatureSet,
) -> IndustryNodeHeatReport:
    ticker_scores = _ticker_heat_scores(feature_set)
    watchlist_tickers_by_node = _watchlist_tickers_by_node(watchlist)
    items: list[IndustryNodeHeatItem] = []
    for node in industry_chain.nodes:
        tickers = tuple(
            dict.fromkeys(
                [
                    *node.related_tickers,
                    *watchlist_tickers_by_node.get(node.node_id, ()),
                ]
            )
        )
        covered = tuple(ticker for ticker in tickers if ticker in ticker_scores)
        missing = tuple(ticker for ticker in tickers if ticker not in ticker_scores)
        coverage = 0.0 if not tickers else len(covered) / len(tickers)
        covered_scores = {ticker: ticker_scores[ticker] for ticker in covered}
        heat_score = (
            None if not covered_scores else sum(covered_scores.values()) / len(covered_scores)
        )
        concentration = _node_concentration(covered_scores)
        contributors = tuple(
            ticker
            for ticker, _score in sorted(
                covered_scores.items(),
                key=lambda item: item[1],
                reverse=True,
            )[:3]
        )
        heat_level = _heat_level(heat_score, coverage)
        items.append(
            IndustryNodeHeatItem(
                node_id=node.node_id,
                node_name=node.name,
                heat_level=heat_level,
                heat_score=heat_score,
                coverage=coverage,
                covered_tickers=covered,
                missing_tickers=missing,
                concentration=concentration,
                main_contributors=contributors,
                explanation=_node_heat_explanation(
                    heat_level=heat_level,
                    heat_score=heat_score,
                    coverage=coverage,
                    concentration=concentration,
                    contributors=contributors,
                    missing=missing,
                ),
            )
        )
    return IndustryNodeHeatReport(as_of=feature_set.as_of, items=tuple(items))


def render_industry_node_heat_section(report: IndustryNodeHeatReport) -> str:
    lines = [
        "## 产业链节点热度",
        "",
        f"- 状态：{report.status}",
        f"- 节点数量：{report.node_count}",
        f"- 生产影响：{report.production_effect}",
        "- 解释边界：本节只使用已通过市场数据门禁的价格/趋势特征，"
        "用于解释市场正在交易哪些产业链节点；不直接改变评分、仓位闸门或执行建议。",
        "- 节点健康度：第一阶段尚未接入基本面、估值、风险事件和 thesis 健康度，"
        "不能把热度视为基本面确认。",
        "",
        "| 节点 | 热度 | 分数 | 覆盖率 | 集中度 | 主要贡献 ticker | 解释 |",
        "|---|---|---:|---:|---:|---|---|",
    ]
    for item in report.items:
        lines.append(
            "| "
            f"{_escape_markdown_table(item.node_name)}（`{item.node_id}`） | "
            f"{_heat_level_label(item.heat_level)} | "
            f"{'' if item.heat_score is None else f'{item.heat_score:.0%}'} | "
            f"{item.coverage:.0%} | "
            f"{'' if item.concentration is None else f'{item.concentration:.0%}'} | "
            f"{_escape_markdown_table(', '.join(item.main_contributors) or '无')} | "
            f"{_escape_markdown_table(item.explanation)} |"
        )
    return "\n".join(lines) + "\n"


def _ticker_heat_scores(feature_set: MarketFeatureSet) -> dict[str, float]:
    by_ticker: dict[str, dict[str, float]] = {}
    for row in feature_set.rows:
        if row.category != "trend":
            continue
        values = by_ticker.setdefault(row.subject, {})
        values[row.feature] = row.value

    scores: dict[str, float] = {}
    for ticker, values in by_ticker.items():
        return_20d = values.get("return_20d")
        if return_20d is None:
            continue
        score = _return_heat_score(return_20d)
        above_ma_50 = values.get("above_ma_50")
        if above_ma_50 is not None:
            score += 0.10 if above_ma_50 >= 1 else -0.10
        scores[ticker] = _clamp(score, 0.0, 1.0)
    return scores


def _watchlist_tickers_by_node(watchlist: WatchlistConfig) -> dict[str, tuple[str, ...]]:
    tickers_by_node: dict[str, list[str]] = {}
    for item in watchlist.items:
        if not item.active:
            continue
        for node_id in item.ai_chain_nodes:
            tickers_by_node.setdefault(node_id, []).append(item.ticker)
    return {
        node_id: tuple(dict.fromkeys(tickers))
        for node_id, tickers in tickers_by_node.items()
    }


def _return_heat_score(return_20d: float) -> float:
    if return_20d >= 0.10:
        return 0.90
    if return_20d >= 0.03:
        return 0.70
    if return_20d >= 0:
        return 0.55
    if return_20d >= -0.05:
        return 0.35
    return 0.15


def _node_concentration(scores: dict[str, float]) -> float | None:
    total = sum(max(score, 0.0) for score in scores.values())
    if total <= 0:
        return None
    return max(scores.values()) / total


def _heat_level(heat_score: float | None, coverage: float) -> str:
    if heat_score is None:
        return "insufficient_data"
    if coverage <= 0.50:
        return "low_coverage"
    if heat_score >= 0.75:
        return "hot"
    if heat_score >= 0.55:
        return "warm"
    if heat_score >= 0.35:
        return "neutral"
    return "cold"


def _heat_level_label(level: str) -> str:
    return {
        "hot": "高热度",
        "warm": "中等热度",
        "neutral": "中性",
        "cold": "低热度",
        "low_coverage": "覆盖不足",
        "insufficient_data": "无可用数据",
    }.get(level, level)


def _node_heat_explanation(
    *,
    heat_level: str,
    heat_score: float | None,
    coverage: float,
    concentration: float | None,
    contributors: tuple[str, ...],
    missing: tuple[str, ...],
) -> str:
    if heat_score is None:
        return "该节点相关 ticker 没有可用 20 日价格热度特征。"
    concentration_text = (
        "集中度未知"
        if concentration is None
        else "单一贡献偏高"
        if concentration >= 0.55
        else "贡献较分散"
    )
    missing_text = "" if not missing else f"；缺少 {', '.join(missing[:4])}"
    return (
        f"{_heat_level_label(heat_level)}，覆盖率 {coverage:.0%}，"
        f"{concentration_text}；主要贡献 {', '.join(contributors) or '无'}"
        f"{missing_text}。"
    )


def _clamp(value: float, lower: float, upper: float) -> float:
    return min(max(value, lower), upper)


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
