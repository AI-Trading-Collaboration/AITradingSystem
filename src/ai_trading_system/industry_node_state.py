from __future__ import annotations

from dataclasses import dataclass, field

from ai_trading_system.config import IndustryChainConfig, WatchlistConfig
from ai_trading_system.features.market import MarketFeatureSet
from ai_trading_system.fundamentals.sec_features import SecFundamentalFeaturesReport
from ai_trading_system.risk_events import RiskEventOccurrenceReviewReport
from ai_trading_system.thesis import ThesisReviewReport
from ai_trading_system.valuation import ValuationReviewReport


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
    health_level: str = "not_assessed"
    health_coverage: float = 0.0
    fundamental_coverage: float = 0.0
    valuation_coverage: float = 0.0
    risk_event_coverage: float = 0.0
    thesis_coverage: float = 0.0
    support_items: tuple[str, ...] = field(default_factory=tuple)
    risk_items: tuple[str, ...] = field(default_factory=tuple)
    data_gaps: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class IndustryNodeHeatReport:
    as_of: object
    items: tuple[IndustryNodeHeatItem, ...]
    production_effect: str = "none"

    @property
    def status(self) -> str:
        if any(
            item.coverage == 0
            or item.health_level in {"insufficient_data", "price_only", "risk_limited"}
            or item.data_gaps
            for item in self.items
        ):
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
    fundamental_feature_report: SecFundamentalFeaturesReport | None = None,
    valuation_review_report: ValuationReviewReport | None = None,
    risk_event_occurrence_review_report: RiskEventOccurrenceReviewReport | None = None,
    thesis_review_report: ThesisReviewReport | None = None,
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
        health = _node_health_assessment(
            node_id=node.node_id,
            tickers=tickers,
            heat_score=heat_score,
            fundamental_feature_report=fundamental_feature_report,
            valuation_review_report=valuation_review_report,
            risk_event_occurrence_review_report=risk_event_occurrence_review_report,
            thesis_review_report=thesis_review_report,
        )
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
                health_level=health.health_level,
                health_coverage=health.health_coverage,
                fundamental_coverage=health.fundamental_coverage,
                valuation_coverage=health.valuation_coverage,
                risk_event_coverage=health.risk_event_coverage,
                thesis_coverage=health.thesis_coverage,
                support_items=health.support_items,
                risk_items=health.risk_items,
                data_gaps=health.data_gaps,
            )
        )
    return IndustryNodeHeatReport(as_of=feature_set.as_of, items=tuple(items))


def render_industry_node_heat_section(report: IndustryNodeHeatReport) -> str:
    lines = [
        "## 产业链节点热度与健康度",
        "",
        f"- 状态：{report.status}",
        f"- 节点数量：{report.node_count}",
        f"- 生产影响：{report.production_effect}",
        "- 解释边界：本节只使用已通过门禁或复核的价格/趋势、SEC/TSM 基本面、估值、"
        "风险事件和 thesis 输入，用于解释市场正在交易哪些产业链节点及其证据覆盖；"
        "不直接改变评分、仓位闸门或执行建议。",
        "- 健康度边界：不能把热度视为基本面确认；估值拥挤和风险事件只作为健康度风险"
        "或限制说明，不写成基本面证伪。",
        "",
        "| 节点 | 热度 | 分数 | 市场覆盖 | 集中度 | 健康度 | 健康覆盖 | "
        "支持项 | 风险/限制 | 数据缺口 | 主要贡献 ticker | 解释 |",
        "|---|---|---:|---:|---:|---|---:|---|---|---|---|---|",
    ]
    for item in report.items:
        lines.append(
            "| "
            f"{_escape_markdown_table(item.node_name)}（`{item.node_id}`） | "
            f"{_heat_level_label(item.heat_level)} | "
            f"{'' if item.heat_score is None else f'{item.heat_score:.0%}'} | "
            f"{item.coverage:.0%} | "
            f"{'' if item.concentration is None else f'{item.concentration:.0%}'} | "
            f"{_health_level_label(item.health_level)} | "
            f"{item.health_coverage:.0%} | "
            f"{_escape_markdown_table(_join_items(item.support_items))} | "
            f"{_escape_markdown_table(_join_items(item.risk_items))} | "
            f"{_escape_markdown_table(_join_items(item.data_gaps))} | "
            f"{_escape_markdown_table(', '.join(item.main_contributors) or '无')} | "
            f"{_escape_markdown_table(item.explanation)} |"
        )
    return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class _NodeHealthAssessment:
    health_level: str
    health_coverage: float
    fundamental_coverage: float
    valuation_coverage: float
    risk_event_coverage: float
    thesis_coverage: float
    support_items: tuple[str, ...]
    risk_items: tuple[str, ...]
    data_gaps: tuple[str, ...]


def _node_health_assessment(
    *,
    node_id: str,
    tickers: tuple[str, ...],
    heat_score: float | None,
    fundamental_feature_report: SecFundamentalFeaturesReport | None,
    valuation_review_report: ValuationReviewReport | None,
    risk_event_occurrence_review_report: RiskEventOccurrenceReviewReport | None,
    thesis_review_report: ThesisReviewReport | None,
) -> _NodeHealthAssessment:
    ticker_set = set(_normalized_tickers(tickers))
    support_items: list[str] = []
    risk_items: list[str] = []
    data_gaps: list[str] = []
    if not ticker_set:
        data_gaps.append("产业链节点未配置相关 ticker，无法计算健康度")

    fundamental_coverage = _fundamental_coverage(
        ticker_set=ticker_set,
        report=fundamental_feature_report,
        support_items=support_items,
        data_gaps=data_gaps,
    )
    valuation_coverage, has_valuation_limit = _valuation_coverage(
        ticker_set=ticker_set,
        report=valuation_review_report,
        support_items=support_items,
        risk_items=risk_items,
        data_gaps=data_gaps,
    )
    risk_event_coverage, has_node_risk_event, has_severe_risk_event = (
        _risk_event_coverage(
            node_id=node_id,
            ticker_set=ticker_set,
            report=risk_event_occurrence_review_report,
            support_items=support_items,
            risk_items=risk_items,
            data_gaps=data_gaps,
        )
    )
    thesis_coverage, has_thesis_pressure, has_invalidated_thesis = _thesis_coverage(
        node_id=node_id,
        ticker_set=ticker_set,
        report=thesis_review_report,
        support_items=support_items,
        risk_items=risk_items,
        data_gaps=data_gaps,
    )

    health_coverage = (
        fundamental_coverage
        + valuation_coverage
        + risk_event_coverage
        + thesis_coverage
    ) / 4
    has_any_risk = (
        has_valuation_limit
        or has_node_risk_event
        or has_thesis_pressure
        or has_invalidated_thesis
    )
    health_level = _health_level(
        heat_score=heat_score,
        health_coverage=health_coverage,
        fundamental_coverage=fundamental_coverage,
        valuation_coverage=valuation_coverage,
        thesis_coverage=thesis_coverage,
        has_any_risk=has_any_risk,
        has_severe_risk=has_severe_risk_event or has_invalidated_thesis,
    )

    return _NodeHealthAssessment(
        health_level=health_level,
        health_coverage=health_coverage,
        fundamental_coverage=fundamental_coverage,
        valuation_coverage=valuation_coverage,
        risk_event_coverage=risk_event_coverage,
        thesis_coverage=thesis_coverage,
        support_items=tuple(support_items),
        risk_items=tuple(risk_items),
        data_gaps=tuple(data_gaps),
    )


def _fundamental_coverage(
    *,
    ticker_set: set[str],
    report: SecFundamentalFeaturesReport | None,
    support_items: list[str],
    data_gaps: list[str],
) -> float:
    if report is None:
        data_gaps.append("SEC/TSM 基本面特征未接入")
        return 0.0
    if not report.passed:
        data_gaps.append(f"SEC/TSM 基本面特征状态 {report.status}，不用于健康度")
        return 0.0

    covered = {
        row.ticker.upper()
        for row in report.rows
        if row.ticker.upper() in ticker_set
    }
    coverage = _coverage(covered, ticker_set)
    if covered:
        support_items.append(
            "SEC/TSM 基本面覆盖 "
            f"{len(covered)}/{len(ticker_set)}：{_format_tickers(covered)}"
        )
    elif ticker_set:
        data_gaps.append(f"SEC/TSM 基本面覆盖 0/{len(ticker_set)}")
    return coverage


def _valuation_coverage(
    *,
    ticker_set: set[str],
    report: ValuationReviewReport | None,
    support_items: list[str],
    risk_items: list[str],
    data_gaps: list[str],
) -> tuple[float, bool]:
    if report is None:
        data_gaps.append("估值快照未接入")
        return 0.0, False
    if not report.validation_report.passed:
        data_gaps.append(
            f"估值快照校验状态 {report.validation_report.status}，不用于健康度"
        )
        return 0.0, False

    items = [item for item in report.items if item.ticker.upper() in ticker_set]
    covered = {item.ticker.upper() for item in items}
    coverage = _coverage(covered, ticker_set)
    if not items:
        if ticker_set:
            data_gaps.append(f"估值快照覆盖 0/{len(ticker_set)}")
        return coverage, False

    stale = [item for item in items if item.health == "STALE"]
    limiting = [item for item in items if _valuation_item_is_limiting(item.health)]
    if stale:
        data_gaps.append(f"估值快照过期：{_format_tickers(item.ticker for item in stale)}")
    if limiting:
        risk_items.append(
            "估值/拥挤限制 "
            f"{_format_tickers(item.ticker for item in limiting)}；不等同基本面证伪"
        )
    else:
        support_items.append(
            "估值快照覆盖 "
            f"{len(covered)}/{len(ticker_set)}，未显示偏贵或极端拥挤"
        )
    return coverage, bool(limiting)


def _risk_event_coverage(
    *,
    node_id: str,
    ticker_set: set[str],
    report: RiskEventOccurrenceReviewReport | None,
    support_items: list[str],
    risk_items: list[str],
    data_gaps: list[str],
) -> tuple[float, bool, bool]:
    if report is None:
        data_gaps.append("风险事件发生记录未接入")
        return 0.0, False, False
    if not report.validation_report.passed:
        data_gaps.append(
            f"风险事件发生记录状态 {report.validation_report.status}，不用于健康度"
        )
        return 0.0, False, False

    node_items = _node_risk_event_items(
        node_id=node_id,
        ticker_set=ticker_set,
        report=report,
    )
    if node_items:
        severe = any(
            item.level in {"L2", "L3"} or item.position_gate_eligible
            for item in node_items
        )
        risk_items.append(
            "风险事件 active/watch："
            + ", ".join(
                f"{item.event_id}({item.level}/{item.status})"
                for item in node_items[:3]
            )
        )
        return 1.0, True, severe

    if report.has_current_review_attestation:
        support_items.append("风险事件复核未显示本节点 active/watch 记录")
        return 1.0, False, False

    data_gaps.append("缺少当前风险事件复核声明，空发生记录不能证明本节点无风险")
    return 0.0, False, False


def _thesis_coverage(
    *,
    node_id: str,
    ticker_set: set[str],
    report: ThesisReviewReport | None,
    support_items: list[str],
    risk_items: list[str],
    data_gaps: list[str],
) -> tuple[float, bool, bool]:
    if report is None:
        data_gaps.append("节点级 thesis 复核未接入")
        return 0.0, False, False
    if not report.validation_report.passed:
        data_gaps.append(f"Thesis 复核状态 {report.status}，不用于节点健康度")
        return 0.0, False, False

    relevant = _node_thesis_items(
        node_id=node_id,
        ticker_set=ticker_set,
        report=report,
    )
    if not relevant:
        data_gaps.append("缺少节点级 thesis 覆盖")
        return 0.0, False, False

    intact = [item for item in relevant if item.health == "INTACT"]
    pressured = [
        item
        for item in relevant
        if item.health in {"WATCH", "CHALLENGED", "INVALIDATED"}
    ]
    inactive = [item for item in relevant if item.health == "INACTIVE"]
    if intact:
        support_items.append(f"Thesis intact {len(intact)} 个")
    if pressured:
        risk_items.append(
            "Thesis 承压："
            + ", ".join(f"{item.thesis_id}({item.health})" for item in pressured[:3])
        )
    if inactive and not intact and not pressured:
        data_gaps.append("相关 thesis 当前非活跃，不能确认节点健康度")

    coverage = min(len(relevant) / max(len(ticker_set), 1), 1.0)
    invalidated = any(item.health == "INVALIDATED" for item in pressured)
    return coverage, bool(pressured), invalidated


def _node_risk_event_items(
    *,
    node_id: str,
    ticker_set: set[str],
    report: RiskEventOccurrenceReviewReport,
):
    rules_by_event = {
        rule.event_id: rule for rule in report.validation_report.config.event_rules
    }
    matched = []
    for item in report.active_items:
        rule = rules_by_event.get(item.event_id)
        if rule is None:
            continue
        related_tickers = {ticker.upper() for ticker in rule.related_tickers}
        if node_id in rule.affected_nodes or ticker_set.intersection(related_tickers):
            matched.append(item)
    return tuple(matched)


def _node_thesis_items(
    *,
    node_id: str,
    ticker_set: set[str],
    report: ThesisReviewReport,
):
    review_items = {item.thesis_id: item for item in report.items}
    matched = []
    for loaded in report.validation_report.theses:
        thesis = loaded.thesis
        if node_id not in thesis.ai_chain_nodes and thesis.ticker.upper() not in ticker_set:
            continue
        item = review_items.get(thesis.thesis_id)
        if item is not None:
            matched.append(item)
    return tuple(matched)


def _health_level(
    *,
    heat_score: float | None,
    health_coverage: float,
    fundamental_coverage: float,
    valuation_coverage: float,
    thesis_coverage: float,
    has_any_risk: bool,
    has_severe_risk: bool,
) -> str:
    if health_coverage <= 0:
        return "price_only" if heat_score is not None else "insufficient_data"
    if has_severe_risk:
        return "risk_limited"
    if has_any_risk:
        return "mixed"
    if fundamental_coverage >= 0.50 and thesis_coverage > 0:
        return "supported"
    if fundamental_coverage > 0 or valuation_coverage > 0 or thesis_coverage > 0:
        return "partial"
    return "price_only" if heat_score is not None else "insufficient_data"


def _valuation_item_is_limiting(health: str) -> bool:
    return health in {"EXPENSIVE_OR_CROWDED", "EXTREME_OVERHEATED"}


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


def _health_level_label(level: str) -> str:
    return {
        "supported": "基本面支持",
        "partial": "部分覆盖",
        "mixed": "支持和风险并存",
        "risk_limited": "风险限制",
        "price_only": "仅价格热度",
        "insufficient_data": "覆盖不足",
        "not_assessed": "未评估",
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


def _coverage(covered: set[str], universe: set[str]) -> float:
    return 0.0 if not universe else len(covered) / len(universe)


def _normalized_tickers(tickers) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(ticker).upper() for ticker in tickers if ticker))


def _format_tickers(tickers, limit: int = 4) -> str:
    values = tuple(sorted(_normalized_tickers(tickers)))
    if not values:
        return "无"
    if len(values) <= limit:
        return ", ".join(values)
    shown = ", ".join(values[:limit])
    return f"{shown} 等 {len(values)} 个"


def _join_items(items: tuple[str, ...], limit: int = 2) -> str:
    if not items:
        return "无"
    if len(items) <= limit:
        return "；".join(items)
    return "；".join((*items[:limit], f"另 {len(items) - limit} 项"))


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
