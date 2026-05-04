from __future__ import annotations

from collections.abc import Mapping

from ai_trading_system.config import PositionGateRulesConfig, RiskBudgetConfig
from ai_trading_system.features.market import MarketFeatureSet
from ai_trading_system.portfolio_exposure import PortfolioExposureReport
from ai_trading_system.risk_events import RiskEventOccurrenceReviewReport
from ai_trading_system.scoring.position_model import PositionBand, PositionGate
from ai_trading_system.valuation import ValuationReviewReport

_EPSILON = 1e-9


def build_position_gates(
    *,
    score_band: PositionBand,
    total_risk_asset_max: float,
    max_total_ai_exposure: float | None,
    gate_rules: PositionGateRulesConfig,
    data_quality_status: str,
    component_source_types: Mapping[str, str],
    thesis_status: str | None = None,
    thesis_error_count: int = 0,
    thesis_warning_count: int = 0,
    risk_budget: RiskBudgetConfig | None = None,
    feature_set: MarketFeatureSet | None = None,
    portfolio_exposure_report: PortfolioExposureReport | None = None,
    valuation_review_report: ValuationReviewReport | None = None,
    risk_event_occurrence_review_report: RiskEventOccurrenceReviewReport | None = None,
) -> tuple[PositionGate, ...]:
    gates = [
        _portfolio_limit_gate(
            score_band=score_band,
            total_risk_asset_max=total_risk_asset_max,
            max_total_ai_exposure=max_total_ai_exposure,
        ),
    ]
    if risk_budget is not None:
        gates.append(
            _risk_budget_gate(
                score_band=score_band,
                risk_budget=risk_budget,
                feature_set=feature_set,
                portfolio_exposure_report=portfolio_exposure_report,
            )
    )
    gates.extend(
        [
            _risk_event_gate(
                score_band=score_band,
                risk_event_occurrence_review_report=risk_event_occurrence_review_report,
            ),
            _valuation_gate(
                score_band=score_band,
                valuation_review_report=valuation_review_report,
                gate_rules=gate_rules,
            ),
            _thesis_gate(
                score_band=score_band,
                gate_rules=gate_rules,
                thesis_status=thesis_status,
                thesis_error_count=thesis_error_count,
                thesis_warning_count=thesis_warning_count,
            ),
            _data_confidence_gate(
                score_band=score_band,
                gate_rules=gate_rules,
                data_quality_status=data_quality_status,
                component_source_types=component_source_types,
            ),
        ]
    )
    return tuple(gates)


def _portfolio_limit_gate(
    *,
    score_band: PositionBand,
    total_risk_asset_max: float,
    max_total_ai_exposure: float | None,
) -> PositionGate:
    if max_total_ai_exposure is None:
        max_position = 1.0
        reason = "未传入组合总资产 AI 仓位上限，本 gate 不额外限制。"
    elif total_risk_asset_max <= 0:
        max_position = 0.0
        reason = "总风险资产预算上限不大于 0，AI 风险资产仓位必须为 0。"
    else:
        max_position = _clamp(max_total_ai_exposure / total_risk_asset_max)
        reason = (
            "组合上限要求 AI 总资产仓位不超过 "
            f"{max_total_ai_exposure:.0%}；按风险资产预算上限 "
            f"{total_risk_asset_max:.0%} 换算，风险资产内 AI 仓位上限为 "
            f"{max_position:.0%}。"
        )
    return _gate(
        gate_id="portfolio_limits",
        label="组合限制",
        source="config/portfolio.yaml:position_limits.max_total_ai_exposure",
        max_position=max_position,
        score_band=score_band,
        reason=reason,
    )


def _risk_event_gate(
    *,
    score_band: PositionBand,
    risk_event_occurrence_review_report: RiskEventOccurrenceReviewReport | None,
) -> PositionGate:
    if risk_event_occurrence_review_report is None:
        max_position = 1.0
        reason = "未传入已校验的风险事件发生记录，本 gate 不额外限制。"
    else:
        eligible_items = (
            risk_event_occurrence_review_report.position_gate_eligible_active_items
        )
        if not eligible_items:
            max_position = 1.0
            reason = "没有可触发仓位闸门的 active 风险事件发生记录，本 gate 不额外限制。"
        else:
            minimum_multiplier = min(
                item.target_ai_exposure_multiplier for item in eligible_items
            )
            max_position = _clamp(score_band.max_position * minimum_multiplier)
            event_summary = "；".join(
                (
                    f"{item.occurrence_id}:{item.level}:{item.status}:"
                    f"{item.target_ai_exposure_multiplier:.0%}"
                )
                for item in eligible_items
            )
            reason = (
                "按可触发仓位闸门的 active 风险事件最低 AI 仓位乘数 "
                f"{minimum_multiplier:.0%} 约束评分仓位上限；事件："
                f"{event_summary}。"
            )
    return _gate(
        gate_id="risk_events",
        label="风险事件",
        source="data/external/risk_event_occurrences",
        max_position=max_position,
        score_band=score_band,
        reason=reason,
    )


def _risk_budget_gate(
    *,
    score_band: PositionBand,
    risk_budget: RiskBudgetConfig,
    feature_set: MarketFeatureSet | None,
    portfolio_exposure_report: PortfolioExposureReport | None,
) -> PositionGate:
    if not risk_budget.enabled:
        return _gate(
            gate_id="risk_budget",
            label="风险预算",
            source="config/portfolio.yaml:risk_budget",
            max_position=1.0,
            score_band=score_band,
            reason="risk_budget 未启用，本 gate 不额外限制。",
        )

    max_position = 1.0
    reasons: list[str] = []
    max_position = min(
        max_position,
        _market_stress_cap(
            risk_budget=risk_budget,
            feature_set=feature_set,
            reasons=reasons,
        ),
    )
    max_position = min(
        max_position,
        _portfolio_concentration_cap(
            risk_budget=risk_budget,
            portfolio_exposure_report=portfolio_exposure_report,
            reasons=reasons,
        ),
    )

    reason = " ".join(reasons) if reasons else "风险预算未触发额外仓位限制。"
    return _gate(
        gate_id="risk_budget",
        label="风险预算",
        source="config/portfolio.yaml:risk_budget",
        max_position=max_position,
        score_band=score_band,
        reason=reason,
    )


def _market_stress_cap(
    *,
    risk_budget: RiskBudgetConfig,
    feature_set: MarketFeatureSet | None,
    reasons: list[str],
) -> float:
    if feature_set is None:
        reasons.append("未传入市场特征，市场压力约束不生效。")
        return 1.0

    config = risk_budget.market_stress
    vix_current = _feature_value(feature_set, "risk_sentiment", "^VIX", "vix_current")
    vix_percentile = _feature_value(
        feature_set,
        "risk_sentiment",
        "^VIX",
        "vix_percentile_252",
    )
    stress_triggered = (
        vix_current is not None and vix_current >= config.stress_vix_current
    ) or (
        vix_percentile is not None
        and vix_percentile >= config.stress_vix_percentile
    )
    if stress_triggered:
        reasons.append(
            "市场压力达到 stress 阈值，风险预算上限 "
            f"{config.stress_max_position:.0%}"
            f"（VIX={_format_optional(vix_current)}，"
            f"VIX percentile={_format_optional(vix_percentile)}）。"
        )
        return config.stress_max_position

    elevated_triggered = (
        vix_current is not None and vix_current >= config.elevated_vix_current
    ) or (
        vix_percentile is not None
        and vix_percentile >= config.elevated_vix_percentile
    )
    if elevated_triggered:
        reasons.append(
            "市场压力达到 elevated 阈值，风险预算上限 "
            f"{config.elevated_max_position:.0%}"
            f"（VIX={_format_optional(vix_current)}，"
            f"VIX percentile={_format_optional(vix_percentile)}）。"
        )
        return config.elevated_max_position

    return 1.0


def _portfolio_concentration_cap(
    *,
    risk_budget: RiskBudgetConfig,
    portfolio_exposure_report: PortfolioExposureReport | None,
    reasons: list[str],
) -> float:
    if portfolio_exposure_report is None:
        reasons.append("未传入真实组合暴露，组合集中度约束不生效。")
        return 1.0
    if portfolio_exposure_report.status == "NOT_CONNECTED":
        reasons.append(
            "真实持仓未接入，不能用观察池或模型建议仓位替代组合集中度约束。"
        )
        return 1.0
    if not portfolio_exposure_report.passed:
        reasons.append("组合暴露报告未通过校验，组合集中度约束不生效。")
        return 1.0
    if portfolio_exposure_report.ai_market_value <= 0:
        reasons.append("真实组合没有 AI 名义暴露，集中度约束不额外限制。")
        return 1.0

    config = risk_budget.concentration
    cap = 1.0
    if (
        portfolio_exposure_report.max_single_ticker_share_of_ai
        > config.max_single_ticker_share_of_ai
    ):
        cap = min(cap, config.concentration_max_position)
        reasons.append(
            "单票 AI 暴露集中度 "
            f"{portfolio_exposure_report.max_single_ticker_share_of_ai:.0%} "
            f"超过上限 {config.max_single_ticker_share_of_ai:.0%}。"
        )

    max_node_share = _max_bucket_share(portfolio_exposure_report.node_exposures)
    if max_node_share > config.max_industry_node_share_of_ai:
        cap = min(cap, config.concentration_max_position)
        reasons.append(
            f"产业链节点 AI 暴露集中度 {max_node_share:.0%} "
            f"超过上限 {config.max_industry_node_share_of_ai:.0%}。"
        )

    max_cluster_share = _max_bucket_share(
        portfolio_exposure_report.correlation_cluster_exposures
    )
    if max_cluster_share > config.max_correlation_cluster_share_of_ai:
        cap = min(cap, config.concentration_max_position)
        reasons.append(
            f"相关性簇 AI 暴露集中度 {max_cluster_share:.0%} "
            f"超过上限 {config.max_correlation_cluster_share_of_ai:.0%}。"
        )

    if portfolio_exposure_report.etf_beta_coverage < config.min_etf_beta_coverage:
        cap = min(cap, config.missing_etf_beta_max_position)
        reasons.append(
            "ETF beta 覆盖率 "
            f"{portfolio_exposure_report.etf_beta_coverage:.0%} "
            f"低于要求 {config.min_etf_beta_coverage:.0%}。"
        )
    return cap


def _valuation_gate(
    *,
    score_band: PositionBand,
    valuation_review_report: ValuationReviewReport | None,
    gate_rules: PositionGateRulesConfig,
) -> PositionGate:
    if valuation_review_report is None:
        max_position = 1.0
        reason = "未传入估值复核报告，本 gate 不额外限制。"
    else:
        usable_items = tuple(
            item
            for item in valuation_review_report.items
            if item.source_type != "public_convenience" and item.health != "STALE"
        )
        if any(item.health == "EXTREME_OVERHEATED" for item in usable_items):
            max_position = gate_rules.valuation.extreme_overheated_max_position
            reason = (
                "存在 EXTREME_OVERHEATED 估值或拥挤度信号，限制新增 AI 仓位上限。"
            )
        elif any(item.health == "EXPENSIVE_OR_CROWDED" for item in usable_items):
            max_position = gate_rules.valuation.expensive_or_crowded_max_position
            reason = (
                "存在 EXPENSIVE_OR_CROWDED 估值或拥挤度信号，限制新增 AI 仓位上限。"
            )
        else:
            max_position = 1.0
            reason = "估值复核未发现可用的高估或拥挤信号，本 gate 不额外限制。"
    return _gate(
        gate_id="valuation",
        label="估值拥挤",
        source="data/external/valuation_snapshots",
        max_position=max_position,
        score_band=score_band,
        reason=reason,
    )


def _thesis_gate(
    *,
    score_band: PositionBand,
    gate_rules: PositionGateRulesConfig,
    thesis_status: str | None,
    thesis_error_count: int,
    thesis_warning_count: int,
) -> PositionGate:
    if thesis_status is None:
        max_position = 1.0
        reason = "未传入交易 thesis 复核摘要，本 gate 不额外限制。"
    elif thesis_status == "FAIL" or thesis_error_count:
        max_position = gate_rules.thesis.failure_max_position
        reason = (
            "交易 thesis 复核失败或存在错误，不能把当前评分作为完整加仓依据。"
        )
    elif "WARNING" in thesis_status or thesis_warning_count:
        max_position = gate_rules.thesis.warning_max_position
        reason = "交易 thesis 复核存在警告，仓位结论需要人工确认。"
    else:
        max_position = 1.0
        reason = "交易 thesis 复核未发现阻断性问题，本 gate 不额外限制。"
    return _gate(
        gate_id="thesis",
        label="交易 thesis",
        source="data/external/trade_theses",
        max_position=max_position,
        score_band=score_band,
        reason=reason,
    )


def _data_confidence_gate(
    *,
    score_band: PositionBand,
    gate_rules: PositionGateRulesConfig,
    data_quality_status: str,
    component_source_types: Mapping[str, str],
) -> PositionGate:
    max_position = 1.0
    reasons: list[str] = []

    if data_quality_status == "FAIL":
        max_position = 0.0
        reasons.append("市场数据质量门禁失败。")
    elif "WARNING" in data_quality_status:
        max_position = min(
            max_position,
            gate_rules.data_confidence.data_quality_warning_max_position,
        )
        reasons.append("市场数据质量门禁存在警告。")

    insufficient_components = _components_by_source_type(
        component_source_types,
        "insufficient_data",
    )
    if insufficient_components:
        max_position = min(
            max_position,
            gate_rules.data_confidence.insufficient_data_max_position,
        )
        reasons.append(
            "存在数据不足评分模块："
            f"{', '.join(insufficient_components)}。"
        )

    placeholder_components = _components_by_source_type(
        component_source_types,
        "placeholder",
    )
    if placeholder_components:
        max_position = min(
            max_position,
            gate_rules.data_confidence.placeholder_max_position,
        )
        reasons.append(
            "存在占位评分模块："
            f"{', '.join(placeholder_components)}。"
        )

    reason = " ".join(reasons) if reasons else "评分输入置信度未触发额外仓位限制。"
    return _gate(
        gate_id="data_confidence",
        label="数据置信度",
        source="data quality gate and score component coverage",
        max_position=max_position,
        score_band=score_band,
        reason=reason,
    )


def _components_by_source_type(
    component_source_types: Mapping[str, str],
    source_type: str,
) -> list[str]:
    return [
        component
        for component, component_source_type in sorted(component_source_types.items())
        if component_source_type == source_type
    ]


def _feature_value(
    feature_set: MarketFeatureSet,
    category: str,
    subject: str,
    feature: str,
) -> float | None:
    matches = [
        row.value
        for row in feature_set.rows
        if row.category == category and row.subject == subject and row.feature == feature
    ]
    if not matches:
        return None
    return matches[-1]


def _max_bucket_share(buckets) -> float:  # type: ignore[no-untyped-def]
    if not buckets:
        return 0.0
    return max(bucket.share_of_ai for bucket in buckets)


def _format_optional(value: float | None) -> str:
    if value is None:
        return "NA"
    return f"{value:.2f}"


def _gate(
    *,
    gate_id: str,
    label: str,
    source: str,
    max_position: float,
    score_band: PositionBand,
    reason: str,
) -> PositionGate:
    capped_max_position = _clamp(max_position)
    return PositionGate(
        gate_id=gate_id,
        label=label,
        source=source,
        max_position=capped_max_position,
        triggered=capped_max_position < score_band.max_position - _EPSILON,
        reason=reason,
    )


def _clamp(value: float) -> float:
    return min(max(value, 0.0), 1.0)
