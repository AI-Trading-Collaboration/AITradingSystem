from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from statistics import median

import pandas as pd

from ai_trading_system.conclusion_boundary import (
    classify_conclusion_boundary,
    render_conclusion_boundary_section,
)
from ai_trading_system.config import (
    MacroRiskAssetBudgetConfig,
    RiskBudgetConfig,
    ScoreModuleRuleConfig,
    ScoreSignalConfig,
    ScoringRulesConfig,
)
from ai_trading_system.data.quality import DataQualityReport
from ai_trading_system.features.market import MarketFeatureSet
from ai_trading_system.fundamentals.sec_features import SecFundamentalFeaturesReport
from ai_trading_system.portfolio_exposure import PortfolioExposureReport
from ai_trading_system.risk_events import RiskEventOccurrenceReviewReport
from ai_trading_system.scoring.macro_budget import (
    MacroRiskAssetBudgetAdjustment,
    build_macro_risk_asset_budget_adjustment,
)
from ai_trading_system.scoring.position_gates import build_position_gates
from ai_trading_system.scoring.position_model import (
    ModuleScore,
    PositionBand,
    PositionRecommendation,
    WeightedScoreModel,
)
from ai_trading_system.valuation import ValuationReviewReport

COMPONENT_LABELS = {
    "trend": "趋势",
    "fundamentals": "基本面",
    "macro_liquidity": "宏观流动性",
    "risk_sentiment": "风险情绪",
    "valuation": "估值",
    "policy_geopolitics": "政策/地缘",
    "overall": "综合",
}

SOURCE_TYPE_LABELS = {
    "hard_data": "硬数据",
    "partial_hard_data": "部分硬数据",
    "insufficient_data": "数据不足",
    "placeholder": "占位输入",
    "manual_input": "手工/审计输入",
    "partial_manual_input": "部分手工/审计输入",
    "derived": "派生结果",
}


@dataclass(frozen=True)
class SignalScore:
    subject: str
    feature: str
    value: float | None
    points: float
    earned_points: float
    available: bool
    reason: str


@dataclass(frozen=True)
class DailyScoreComponent:
    name: str
    score: float
    weight: float
    source_type: str
    coverage: float
    confidence: float
    reason: str
    signals: tuple[SignalScore, ...]

    def to_module_score(self) -> ModuleScore:
        return ModuleScore(
            name=self.name,
            score=self.score,
            weight=self.weight,
            reason=self.reason,
        )


@dataclass(frozen=True)
class DailyManualReviewStatus:
    name: str
    status: str
    summary: str
    error_count: int = 0
    warning_count: int = 0
    source_path: Path | None = None


@dataclass(frozen=True)
class DailyReviewSummary:
    thesis: DailyManualReviewStatus | None = None
    risk_events: DailyManualReviewStatus | None = None
    valuation: DailyManualReviewStatus | None = None
    trades: DailyManualReviewStatus | None = None

    @property
    def items(self) -> tuple[DailyManualReviewStatus, ...]:
        return tuple(
            item
            for item in (self.thesis, self.risk_events, self.valuation, self.trades)
            if item is not None
        )

    @property
    def has_failures(self) -> bool:
        return any(item.status == "FAIL" or item.error_count for item in self.items)

    @property
    def has_warnings(self) -> bool:
        return any("WARNING" in item.status or item.warning_count for item in self.items)


@dataclass(frozen=True)
class DailyConfidenceAssessment:
    score: float
    level: str
    reasons: tuple[str, ...]
    adjusted_risk_asset_ai_band: PositionBand


@dataclass(frozen=True)
class PreviousDailyScoreSnapshot:
    as_of: date
    overall_score: float | None
    confidence_score: float | None
    confidence_level: str | None
    component_scores: dict[str, float]
    component_confidence_scores: dict[str, float]
    model_risk_asset_ai_min: float | None
    model_risk_asset_ai_max: float | None
    final_risk_asset_ai_min: float | None
    final_risk_asset_ai_max: float | None
    confidence_adjusted_risk_asset_ai_min: float | None
    confidence_adjusted_risk_asset_ai_max: float | None
    total_asset_ai_min: float | None
    total_asset_ai_max: float | None
    triggered_position_gates: str | None


@dataclass(frozen=True)
class DailyScoreReport:
    as_of: date
    components: tuple[DailyScoreComponent, ...]
    recommendation: PositionRecommendation
    macro_risk_asset_budget: MacroRiskAssetBudgetAdjustment
    data_quality_report: DataQualityReport
    feature_set: MarketFeatureSet
    minimum_action_delta: float
    fundamental_feature_report: SecFundamentalFeaturesReport | None = None
    valuation_review_report: ValuationReviewReport | None = None
    risk_event_occurrence_review_report: RiskEventOccurrenceReviewReport | None = None
    review_summary: DailyReviewSummary | None = None

    @property
    def status(self) -> str:
        if self.fundamental_feature_report and not self.fundamental_feature_report.passed:
            return "FAIL"
        if self.review_summary and self.review_summary.has_failures:
            return "PASS_WITH_LIMITATIONS"
        if any(component.source_type != "hard_data" for component in self.components):
            return "PASS_WITH_LIMITATIONS"
        if self.feature_set.warnings or (
            self.review_summary and self.review_summary.has_warnings
        ) or (
            self.fundamental_feature_report
            and self.fundamental_feature_report.warning_count
        ):
            return "PASS_WITH_WARNINGS"
        return "PASS"

    @property
    def confidence_assessment(self) -> DailyConfidenceAssessment:
        return _confidence_assessment(self)


def build_daily_score_report(
    feature_set: MarketFeatureSet,
    data_quality_report: DataQualityReport,
    rules: ScoringRulesConfig,
    total_risk_asset_min: float,
    total_risk_asset_max: float,
    max_total_ai_exposure: float | None = None,
    macro_risk_asset_budget: MacroRiskAssetBudgetConfig | None = None,
    risk_budget: RiskBudgetConfig | None = None,
    portfolio_exposure_report: PortfolioExposureReport | None = None,
    review_summary: DailyReviewSummary | None = None,
    fundamental_feature_report: SecFundamentalFeaturesReport | None = None,
    valuation_review_report: ValuationReviewReport | None = None,
    risk_event_occurrence_review_report: RiskEventOccurrenceReviewReport | None = None,
) -> DailyScoreReport:
    if fundamental_feature_report is not None and not fundamental_feature_report.passed:
        raise ValueError("SEC 基本面特征报告未通过，不能进入每日评分")
    if (
        valuation_review_report is not None
        and not valuation_review_report.validation_report.passed
    ):
        raise ValueError("估值快照校验未通过，不能进入每日评分")
    if (
        risk_event_occurrence_review_report is not None
        and not risk_event_occurrence_review_report.validation_report.passed
    ):
        raise ValueError("风险事件发生记录校验未通过，不能进入每日评分")

    components = [
        _score_hard_data_module("trend", rules.weights["trend"], rules.trend, feature_set, rules),
        _score_fundamental_module(
            rules=rules,
            fundamental_feature_report=fundamental_feature_report,
        ),
        _score_hard_data_module(
            "macro_liquidity",
            rules.weights["macro_liquidity"],
            rules.macro_liquidity,
            feature_set,
            rules,
        ),
        _score_hard_data_module(
            "risk_sentiment",
            rules.weights["risk_sentiment"],
            rules.risk_sentiment,
            feature_set,
            rules,
        ),
        _score_valuation_module(
            rules=rules,
            valuation_review_report=valuation_review_report,
        ),
        _score_policy_geopolitics_module(
            rules=rules,
            risk_event_occurrence_review_report=risk_event_occurrence_review_report,
        ),
    ]

    score_model = WeightedScoreModel()
    module_scores = [component.to_module_score() for component in components]
    macro_budget_adjustment = build_macro_risk_asset_budget_adjustment(
        static_total_risk_asset_min=total_risk_asset_min,
        static_total_risk_asset_max=total_risk_asset_max,
        feature_set=feature_set,
        config=macro_risk_asset_budget,
    )
    adjusted_total_risk_asset_min = (
        macro_budget_adjustment.adjusted_total_risk_asset_band.min_position
    )
    adjusted_total_risk_asset_max = (
        macro_budget_adjustment.adjusted_total_risk_asset_band.max_position
    )
    model_recommendation = score_model.recommend(
        module_scores,
        total_risk_asset_min=adjusted_total_risk_asset_min,
        total_risk_asset_max=adjusted_total_risk_asset_max,
    )
    thesis_review_status = review_summary.thesis if review_summary is not None else None
    position_gates = build_position_gates(
        score_band=model_recommendation.model_risk_asset_ai_band,
        total_risk_asset_max=adjusted_total_risk_asset_max,
        max_total_ai_exposure=max_total_ai_exposure,
        gate_rules=rules.position_gates,
        data_quality_status=data_quality_report.status,
        component_source_types={
            component.name: component.source_type for component in components
        },
        thesis_status=None if thesis_review_status is None else thesis_review_status.status,
        thesis_error_count=0 if thesis_review_status is None else thesis_review_status.error_count,
        thesis_warning_count=(
            0 if thesis_review_status is None else thesis_review_status.warning_count
        ),
        risk_budget=risk_budget,
        feature_set=feature_set,
        portfolio_exposure_report=portfolio_exposure_report,
        valuation_review_report=valuation_review_report,
        risk_event_occurrence_review_report=risk_event_occurrence_review_report,
    )
    recommendation = score_model.recommend(
        module_scores,
        total_risk_asset_min=adjusted_total_risk_asset_min,
        total_risk_asset_max=adjusted_total_risk_asset_max,
        position_gates=position_gates,
    )
    return DailyScoreReport(
        as_of=feature_set.as_of,
        components=tuple(components),
        recommendation=recommendation,
        macro_risk_asset_budget=macro_budget_adjustment,
        data_quality_report=data_quality_report,
        feature_set=feature_set,
        minimum_action_delta=rules.position_change.minimum_action_delta,
        fundamental_feature_report=fundamental_feature_report,
        valuation_review_report=valuation_review_report,
        risk_event_occurrence_review_report=risk_event_occurrence_review_report,
        review_summary=review_summary,
    )


def write_scores_csv(report: DailyScoreReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_frame = pd.DataFrame(
        [_score_component_record(report.as_of, item) for item in report.components]
    )
    overall = pd.DataFrame(
        [
            {
                "as_of": report.as_of.isoformat(),
                "component": "overall",
                "score": report.recommendation.total_score,
                "weight": 100.0,
                "source_type": "derived",
                "coverage": "",
                "confidence": report.confidence_assessment.score,
                "confidence_level": report.confidence_assessment.level,
                "confidence_reasons": "；".join(report.confidence_assessment.reasons),
                "model_risk_asset_ai_min": (
                    report.recommendation.model_risk_asset_ai_band.min_position
                ),
                "model_risk_asset_ai_max": (
                    report.recommendation.model_risk_asset_ai_band.max_position
                ),
                "final_risk_asset_ai_min": (
                    report.recommendation.risk_asset_ai_band.min_position
                ),
                "final_risk_asset_ai_max": (
                    report.recommendation.risk_asset_ai_band.max_position
                ),
                "confidence_adjusted_risk_asset_ai_min": (
                    report.confidence_assessment.adjusted_risk_asset_ai_band.min_position
                ),
                "confidence_adjusted_risk_asset_ai_max": (
                    report.confidence_assessment.adjusted_risk_asset_ai_band.max_position
                ),
                "total_asset_ai_min": report.recommendation.total_asset_ai_band.min_position,
                "total_asset_ai_max": report.recommendation.total_asset_ai_band.max_position,
                "static_total_risk_asset_min": (
                    report.macro_risk_asset_budget.static_total_risk_asset_band.min_position
                ),
                "static_total_risk_asset_max": (
                    report.macro_risk_asset_budget.static_total_risk_asset_band.max_position
                ),
                "final_total_risk_asset_min": (
                    report.recommendation.total_risk_asset_band.min_position
                ),
                "final_total_risk_asset_max": (
                    report.recommendation.total_risk_asset_band.max_position
                ),
                "macro_risk_asset_budget_level": (
                    report.macro_risk_asset_budget.level
                ),
                "macro_risk_asset_budget_triggered": (
                    report.macro_risk_asset_budget.triggered
                ),
                "macro_risk_asset_budget_reasons": "；".join(
                    report.macro_risk_asset_budget.reasons
                ),
                "triggered_position_gates": _triggered_position_gate_summary(report),
                "reason": (
                    f"最终仓位区间：{report.recommendation.label}；"
                    f"触发仓位闸门：{_triggered_position_gate_summary(report)}"
                ),
            }
        ]
    )
    new_frame = pd.concat([new_frame, overall], ignore_index=True)

    if output_path.exists():
        existing = pd.read_csv(output_path)
        if "as_of" not in existing.columns:
            raise ValueError(f"existing score file is missing as_of column: {output_path}")
        existing = existing.loc[existing["as_of"] != report.as_of.isoformat()]
        new_frame = pd.concat([existing, new_frame], ignore_index=True)

    new_frame = new_frame.sort_values(["as_of", "component"]).reset_index(drop=True)
    new_frame.to_csv(output_path, index=False)
    return output_path


def load_previous_daily_score_snapshot(
    scores_path: Path,
    as_of: date,
) -> PreviousDailyScoreSnapshot | None:
    if not scores_path.exists():
        return None
    frame = pd.read_csv(scores_path)
    if "as_of" not in frame.columns:
        raise ValueError(f"existing score file is missing as_of column: {scores_path}")
    if "component" not in frame.columns:
        return None

    frame["_as_of_date"] = pd.to_datetime(frame["as_of"], errors="coerce")
    overall = frame.loc[frame["component"] == "overall"].copy()
    if overall.empty:
        return None
    overall = overall.loc[
        overall["_as_of_date"].notna()
        & (overall["_as_of_date"] < pd.Timestamp(as_of))
    ].copy()
    if overall.empty:
        return None
    latest = overall.sort_values("_as_of_date").iloc[-1]
    latest_timestamp = latest["_as_of_date"]
    latest_rows = frame.loc[frame["_as_of_date"] == latest_timestamp]
    return PreviousDailyScoreSnapshot(
        as_of=latest["_as_of_date"].date(),
        overall_score=_row_float_or_none(latest, "score"),
        confidence_score=_row_float_or_none(latest, "confidence"),
        confidence_level=_row_str_or_none(latest, "confidence_level"),
        component_scores=_component_value_map(latest_rows, "score"),
        component_confidence_scores=_component_value_map(latest_rows, "confidence"),
        model_risk_asset_ai_min=_row_float_or_none(latest, "model_risk_asset_ai_min"),
        model_risk_asset_ai_max=_row_float_or_none(latest, "model_risk_asset_ai_max"),
        final_risk_asset_ai_min=_row_float_or_none(latest, "final_risk_asset_ai_min"),
        final_risk_asset_ai_max=_row_float_or_none(latest, "final_risk_asset_ai_max"),
        confidence_adjusted_risk_asset_ai_min=_row_float_or_none(
            latest,
            "confidence_adjusted_risk_asset_ai_min",
        ),
        confidence_adjusted_risk_asset_ai_max=_row_float_or_none(
            latest,
            "confidence_adjusted_risk_asset_ai_max",
        ),
        total_asset_ai_min=_row_float_or_none(latest, "total_asset_ai_min"),
        total_asset_ai_max=_row_float_or_none(latest, "total_asset_ai_max"),
        triggered_position_gates=_row_str_or_none(latest, "triggered_position_gates"),
    )


def render_daily_score_report(
    report: DailyScoreReport,
    data_quality_report_path: Path,
    feature_report_path: Path,
    features_path: Path,
    scores_path: Path,
    sec_metrics_validation_report_path: Path | None = None,
    sec_fundamental_feature_report_path: Path | None = None,
    sec_fundamental_features_path: Path | None = None,
    risk_event_occurrence_report_path: Path | None = None,
    previous_score_snapshot: PreviousDailyScoreSnapshot | None = None,
    belief_state_section: str | None = None,
    execution_action_label: str | None = None,
    execution_action_id: str | None = None,
    focus_stock_trend_section: str | None = None,
    industry_node_heat_section: str | None = None,
    execution_advisory_section: str | None = None,
    portfolio_exposure_section: str | None = None,
    alert_summary_section: str | None = None,
    feature_availability_section: str | None = None,
    risk_event_openai_precheck_section: str | None = None,
    traceability_section: str | None = None,
) -> str:
    recommendation = report.recommendation
    confidence = report.confidence_assessment
    lines = [
        "# AI 产业链每日评分",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- AI 产业链评分：{recommendation.total_score:.1f}",
        f"- 判断置信度：{confidence.score:.1f}（{_confidence_level_label(confidence.level)}）",
        f"- 仓位状态：{recommendation.label}",
        (
            "- 评分模型 AI 仓位（硬闸门前，股票风险资产内）："
            f"{recommendation.model_risk_asset_ai_band.min_position:.0%}-"
            f"{recommendation.model_risk_asset_ai_band.max_position:.0%}"
        ),
        (
            "- 置信度调整后建议仓位（股票风险资产内）："
            f"{confidence.adjusted_risk_asset_ai_band.min_position:.0%}-"
            f"{confidence.adjusted_risk_asset_ai_band.max_position:.0%}"
        ),
        (
            "- 最终 AI 仓位（股票风险资产内）："
            f"{recommendation.risk_asset_ai_band.min_position:.0%}-"
            f"{recommendation.risk_asset_ai_band.max_position:.0%}"
        ),
        f"- 置信度主要限制：{_confidence_reason_summary(confidence)}",
        (
            "- 股票/风险资产预算（总资产内）："
            f"{recommendation.total_risk_asset_band.min_position:.0%}-"
            f"{recommendation.total_risk_asset_band.max_position:.0%}"
        ),
        f"- 宏观风险资产预算状态：{_macro_budget_summary(report)}",
        (
            "- AI 仓位（总资产内）："
            f"{recommendation.total_asset_ai_band.min_position:.0%}-"
            f"{recommendation.total_asset_ai_band.max_position:.0%}"
        ),
        f"- 最小操作变化阈值：{report.minimum_action_delta:.0%}",
        "",
        render_daily_conclusion_card(
            report,
            execution_action_label=execution_action_label,
            execution_action_id=execution_action_id,
        ).rstrip(),
        "",
        render_daily_conclusion_boundary(report).rstrip(),
        "",
        render_daily_change_explanation(
            report,
            previous_score_snapshot=previous_score_snapshot,
        ).rstrip(),
    ]
    if focus_stock_trend_section is not None:
        lines.extend(["", focus_stock_trend_section.rstrip()])
    if industry_node_heat_section is not None:
        lines.extend(["", industry_node_heat_section.rstrip()])
    if belief_state_section is not None:
        lines.extend(["", belief_state_section.rstrip()])
    if execution_advisory_section is not None:
        lines.extend(["", execution_advisory_section.rstrip()])
    if portfolio_exposure_section is not None:
        lines.extend(["", portfolio_exposure_section.rstrip()])
    if alert_summary_section is not None:
        lines.extend(["", alert_summary_section.rstrip()])
    lines.extend(["", render_macro_risk_asset_budget_section(report).rstrip()])
    lines.extend(
        [
            "",
            "## 数据门禁",
            "",
            f"- 数据质量状态：{report.data_quality_report.status}",
            f"- 数据质量报告：`{data_quality_report_path}`",
            f"- 特征状态：{report.feature_set.status}",
            f"- 特征警告数：{len(report.feature_set.warnings)}",
            f"- 特征报告：`{feature_report_path}`",
            f"- 特征数据：`{features_path}`",
            f"- 评分数据：`{scores_path}`",
        ]
    )
    if report.fundamental_feature_report is not None:
        lines.append(
            f"- SEC 指标 CSV 校验状态：{report.fundamental_feature_report.validation_report.status}"
        )
        if sec_metrics_validation_report_path is not None:
            lines.append(f"- SEC 指标 CSV 校验报告：`{sec_metrics_validation_report_path}`")
        lines.append(f"- SEC 基本面特征状态：{report.fundamental_feature_report.status}")
        if sec_fundamental_feature_report_path is not None:
            lines.append(f"- SEC 基本面特征报告：`{sec_fundamental_feature_report_path}`")
        if sec_fundamental_features_path is not None:
            lines.append(f"- SEC 基本面特征数据：`{sec_fundamental_features_path}`")
    if report.valuation_review_report is not None:
        valuation_validation = report.valuation_review_report.validation_report
        lines.append(f"- 估值快照校验状态：{valuation_validation.status}")
        lines.append(f"- 估值快照数量：{valuation_validation.snapshot_count}")
        lines.append(f"- 当前估值复核快照数：{len(report.valuation_review_report.items)}")
        lines.append(f"- 估值覆盖标的数：{valuation_validation.ticker_count}")
        lines.append(
            "- 估值历史指标覆盖："
            f"valuation_percentile "
            f"{_valuation_percentile_count(report.valuation_review_report)}/"
            f"{len(report.valuation_review_report.items)}；"
            f"eps_revision_90d_pct "
            f"{_eps_revision_count(report.valuation_review_report)}/"
            f"{len(report.valuation_review_report.items)}"
        )
        lines.append(
            "- 估值 PIT 可信度："
            f"{_valuation_confidence_summary(report.valuation_review_report)}"
        )
    if report.risk_event_occurrence_review_report is not None:
        occurrence_validation = report.risk_event_occurrence_review_report.validation_report
        lines.append(f"- 风险事件发生记录状态：{report.risk_event_occurrence_review_report.status}")
        lines.append(f"- 风险事件发生记录校验状态：{occurrence_validation.status}")
        lines.append(f"- 风险事件发生记录数：{occurrence_validation.occurrence_count}")
        lines.append(f"- 风险事件复核声明数：{occurrence_validation.review_attestation_count}")
        lines.append(
            "- 当前有效风险事件复核声明数："
            f"{occurrence_validation.current_review_attestation_count}"
        )
        lines.append(
            "- 可进入普通评分的活跃风险事件数："
            f"{len(report.risk_event_occurrence_review_report.score_eligible_active_items)}"
        )
        lines.append(
            "- 可触发仓位闸门的活跃风险事件数："
            f"{len(report.risk_event_occurrence_review_report.position_gate_eligible_active_items)}"
        )
        if risk_event_occurrence_report_path is not None:
            lines.append(f"- 风险事件发生记录报告：`{risk_event_occurrence_report_path}`")
    if feature_availability_section is not None:
        lines.extend(["", feature_availability_section.rstrip()])
    if risk_event_openai_precheck_section is not None:
        lines.extend(["", risk_event_openai_precheck_section.rstrip()])

    lines.extend(
        [
            "",
            "## 仓位闸门",
            "",
            "| Gate | 来源 | 上限 | 触发 | 说明 |",
            "|---|---|---:|---|---|",
        ]
    )
    for gate in recommendation.position_gates:
        lines.append(
            "| "
            f"{_escape_markdown_table(gate.label)}（{gate.gate_id}） | "
            f"{_escape_markdown_table(gate.source)} | "
            f"{gate.max_position:.0%} | "
            f"{'是' if gate.triggered else '否'} | "
            f"{_escape_markdown_table(gate.reason)} |"
        )

    lines.extend(
        [
            "",
            "## 模块评分",
            "",
            "| 模块 | 分数 | 权重 | 来源 | 覆盖率 | 置信度 | 说明 |",
            "|---|---:|---:|---|---:|---:|---|",
        ]
    )

    for component in report.components:
        lines.append(
            "| "
            f"{_component_label(component.name)} | "
            f"{component.score:.1f} | "
            f"{component.weight:.1f} | "
            f"{_source_type_label(component.source_type)} | "
            f"{component.coverage:.0%} | "
            f"{component.confidence:.0%} | "
            f"{_escape_markdown_table(component.reason)} |"
        )

    lines.extend(["", "## 硬数据信号", ""])
    hard_signals = [signal for component in report.components for signal in component.signals]
    if not hard_signals:
        lines.append("没有评估任何硬数据信号。")
    else:
        lines.extend(
            [
                "| 标的 | 特征 | 数值 | 满分 | 得分 | 可用 | 说明 |",
                "|---|---|---:|---:|---:|---|---|",
            ]
        )
        for signal in hard_signals:
            value = "" if signal.value is None else f"{signal.value:.4f}"
            lines.append(
                "| "
                f"{signal.subject} | "
                f"{signal.feature} | "
                f"{value} | "
                f"{signal.points:.1f} | "
                f"{signal.earned_points:.1f} | "
                f"{'是' if signal.available else '否'} | "
                f"{_escape_markdown_table(signal.reason)} |"
            )

    lines.extend(["", "## 人工复核摘要", ""])
    if report.review_summary is None or not report.review_summary.items:
        lines.append("未接入交易 thesis、风险事件、估值或交易复盘摘要。")
    else:
        lines.extend(
            [
                "| 模块 | 状态 | 错误 | 警告 | 输入 | 摘要 |",
                "|---|---|---:|---:|---|---|",
            ]
        )
        for item in report.review_summary.items:
            source_path = "" if item.source_path is None else f"`{item.source_path}`"
            lines.append(
                "| "
                f"{_escape_markdown_table(item.name)} | "
                f"{item.status} | "
                f"{item.error_count} | "
                f"{item.warning_count} | "
                f"{_escape_markdown_table(source_path)} | "
                f"{_escape_markdown_table(item.summary)} |"
            )

    if traceability_section is not None:
        lines.append(traceability_section)

    lines.extend(["", "## 限制说明", ""])
    limitations = [
        component
        for component in report.components
        if component.source_type in {"placeholder", "insufficient_data"}
    ]
    has_review_issues = bool(
        report.review_summary
        and (report.review_summary.has_failures or report.review_summary.has_warnings)
    )
    if not limitations and not report.feature_set.warnings and not has_review_issues:
        lines.append("未发现限制。")
    else:
        for component in limitations:
            lines.append(f"- {_component_label(component.name)}：{component.reason}")
        if report.feature_set.warnings:
            lines.append(
                f"- 存在特征警告：{len(report.feature_set.warnings)} 条。"
                "请查看特征摘要，确认是否有历史窗口不足或输入不可用。"
            )
        if report.review_summary and report.review_summary.has_failures:
            lines.append(
                "- 人工复核摘要存在错误，日报结论不能视为完整交易结论；"
                "请先修复对应输入或配置。"
            )
        elif report.review_summary and report.review_summary.has_warnings:
            lines.append(
                "- 人工复核摘要存在警告，日报结论需要结合 thesis、风险、估值或复盘记录人工确认。"
            )

    return "\n".join(lines) + "\n"


def write_daily_score_report(
    report: DailyScoreReport,
    data_quality_report_path: Path,
    feature_report_path: Path,
    features_path: Path,
    scores_path: Path,
    output_path: Path,
    sec_metrics_validation_report_path: Path | None = None,
    sec_fundamental_feature_report_path: Path | None = None,
    sec_fundamental_features_path: Path | None = None,
    risk_event_occurrence_report_path: Path | None = None,
    previous_score_snapshot: PreviousDailyScoreSnapshot | None = None,
    belief_state_section: str | None = None,
    execution_action_label: str | None = None,
    execution_action_id: str | None = None,
    focus_stock_trend_section: str | None = None,
    industry_node_heat_section: str | None = None,
    execution_advisory_section: str | None = None,
    portfolio_exposure_section: str | None = None,
    alert_summary_section: str | None = None,
    feature_availability_section: str | None = None,
    risk_event_openai_precheck_section: str | None = None,
    traceability_section: str | None = None,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_daily_score_report(
            report,
            data_quality_report_path=data_quality_report_path,
            feature_report_path=feature_report_path,
            features_path=features_path,
            scores_path=scores_path,
            sec_metrics_validation_report_path=sec_metrics_validation_report_path,
            sec_fundamental_feature_report_path=sec_fundamental_feature_report_path,
            sec_fundamental_features_path=sec_fundamental_features_path,
            risk_event_occurrence_report_path=risk_event_occurrence_report_path,
            previous_score_snapshot=previous_score_snapshot,
            belief_state_section=belief_state_section,
            execution_action_label=execution_action_label,
            execution_action_id=execution_action_id,
            focus_stock_trend_section=focus_stock_trend_section,
            industry_node_heat_section=industry_node_heat_section,
            execution_advisory_section=execution_advisory_section,
            portfolio_exposure_section=portfolio_exposure_section,
            alert_summary_section=alert_summary_section,
            feature_availability_section=feature_availability_section,
            risk_event_openai_precheck_section=risk_event_openai_precheck_section,
            traceability_section=traceability_section,
        ),
        encoding="utf-8",
    )
    return output_path


def default_daily_score_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"daily_score_{as_of.isoformat()}.md"


def render_daily_conclusion_card(
    report: DailyScoreReport,
    *,
    execution_action_label: str | None = None,
    execution_action_id: str | None = None,
) -> str:
    action = _execution_action_summary(execution_action_label, execution_action_id)
    posture = _daily_posture_label(report)
    lines = [
        "## 今日结论卡",
        "",
        "| 项目 | 结论 |",
        "|---|---|",
        f"| 状态标签 | {_escape_markdown_table(posture)} |",
        f"| 市场吸引力 | {_escape_markdown_table(_market_attractiveness_summary(report))} |",
        (
            "| 判断置信度 | "
            f"{_escape_markdown_table(_confidence_card_summary(report))} |"
        ),
        (
            "| 评分映射仓位 | "
            f"{report.recommendation.model_risk_asset_ai_band.min_position:.0%}-"
            f"{report.recommendation.model_risk_asset_ai_band.max_position:.0%} |"
        ),
        (
            "| 风险闸门后最终仓位 | "
            f"{report.recommendation.risk_asset_ai_band.min_position:.0%}-"
            f"{report.recommendation.risk_asset_ai_band.max_position:.0%} |"
        ),
        f"| 总风险资产预算 | {_escape_markdown_table(_macro_budget_summary(report))} |",
        f"| 执行动作 | {_escape_markdown_table(action)} |",
        "",
        "### 一句话主结论",
        "",
        f"- {_daily_main_conclusion(report, posture)}",
        "",
        "### 三个核心原因",
        "",
    ]
    lines.extend(f"- {reason}" for reason in _daily_core_reasons(report))
    lines.extend(
        [
            "",
            "### 最大限制",
            "",
            f"- {_daily_largest_limitation(report)}",
            "",
            "### 下一步触发条件",
            "",
            f"- {_daily_next_trigger(report)}",
        ]
    )
    return "\n".join(lines) + "\n"


def render_daily_conclusion_boundary(report: DailyScoreReport) -> str:
    boundary = classify_conclusion_boundary(
        report_status=report.status,
        data_quality_status=report.data_quality_report.status,
        posture_label=_daily_posture_label(report),
        confidence_level=report.confidence_assessment.level,
        has_review_failures=bool(
            report.review_summary and report.review_summary.has_failures
        ),
        has_review_warnings=bool(
            report.review_summary and report.review_summary.has_warnings
        ),
        has_source_limitations=any(
            component.source_type in {"placeholder", "insufficient_data"}
            for component in report.components
        ),
        decision_scope="trend_judgment",
        evidence_refs=(
            f"quality:data_cache:{report.as_of.isoformat()}",
            f"daily_score:{report.as_of.isoformat()}:overall_position",
            f"daily_score:{report.as_of.isoformat()}:confidence",
        ),
    )
    return render_conclusion_boundary_section(boundary)


def render_macro_risk_asset_budget_section(report: DailyScoreReport) -> str:
    adjustment = report.macro_risk_asset_budget
    static_band = adjustment.static_total_risk_asset_band
    adjusted_band = adjustment.adjusted_total_risk_asset_band
    reason = "；".join(adjustment.reasons)
    lines = [
        "## 宏观风险资产预算",
        "",
        "| 项目 | 内容 |",
        "|---|---|",
        (
            "| 静态总风险资产预算 | "
            f"{static_band.min_position:.0%}-{static_band.max_position:.0%} |"
        ),
        (
            "| 宏观调整后总风险资产预算 | "
            f"{adjusted_band.min_position:.0%}-{adjusted_band.max_position:.0%} |"
        ),
        f"| 状态 | {adjustment.level} |",
        f"| 触发下调 | {'是' if adjustment.triggered else '否'} |",
        f"| 来源 | `{adjustment.source}` |",
        f"| 说明 | {_escape_markdown_table(reason)} |",
    ]
    return "\n".join(lines) + "\n"


def render_daily_change_explanation(
    report: DailyScoreReport,
    previous_score_snapshot: PreviousDailyScoreSnapshot | None = None,
) -> str:
    lines = [
        "## 变化原因树",
        "",
        f"- 判断类型：{_judgement_type(report)}",
        f"- 本期仓位变化：{_position_change_summary(report, previous_score_snapshot)}",
        f"- 模块变化来源：{_module_change_source_summary(report, previous_score_snapshot)}",
        f"- Thesis 状态：{_review_item_summary(report, 'thesis')}",
        f"- 风险事件状态：{_risk_event_change_summary(report)}",
        f"- 估值状态：{_valuation_change_summary(report)}",
        f"- 最终动作约束：{_action_constraint_summary(report)}",
        "",
        "### 什么情况会改变判断",
        "",
        f"- 转为加仓：{_add_condition_summary(report)}",
        f"- 转为减仓：{_reduce_condition_summary(report)}",
        f"- 保持观察：{_watch_condition_summary(report)}",
    ]
    return "\n".join(lines) + "\n"


def _score_hard_data_module(
    name: str,
    weight: float,
    module_rules: ScoreModuleRuleConfig,
    feature_set: MarketFeatureSet,
    rules: ScoringRulesConfig,
) -> DailyScoreComponent:
    return _score_signal_module(
        name=name,
        weight=weight,
        module_rules=module_rules,
        feature_index=_feature_index(feature_set),
        rules=rules,
        source_description="市场硬数据",
    )


def _score_fundamental_module(
    rules: ScoringRulesConfig,
    fundamental_feature_report: SecFundamentalFeaturesReport | None,
) -> DailyScoreComponent:
    placeholder = rules.placeholders["fundamentals"]
    if fundamental_feature_report is None or rules.fundamentals is None:
        return DailyScoreComponent(
            name="fundamentals",
            score=placeholder.score,
            weight=rules.weights["fundamentals"],
            source_type="placeholder",
            coverage=0.0,
            confidence=_source_type_confidence("placeholder", 0.0),
            reason=placeholder.reason,
            signals=(),
        )

    return _score_signal_module(
        name="fundamentals",
        weight=rules.weights["fundamentals"],
        module_rules=rules.fundamentals,
        feature_index=_fundamental_feature_index(fundamental_feature_report),
        rules=rules,
        source_description="SEC 基本面硬数据",
    )


def _score_valuation_module(
    rules: ScoringRulesConfig,
    valuation_review_report: ValuationReviewReport | None,
) -> DailyScoreComponent:
    placeholder = rules.placeholders["valuation"]
    if valuation_review_report is None or rules.valuation is None:
        return DailyScoreComponent(
            name="valuation",
            score=placeholder.score,
            weight=rules.weights["valuation"],
            source_type="placeholder",
            coverage=0.0,
            confidence=_source_type_confidence("placeholder", 0.0),
            reason=placeholder.reason,
            signals=(),
        )

    component = _score_signal_module(
        name="valuation",
        weight=rules.weights["valuation"],
        module_rules=rules.valuation,
        feature_index=_valuation_feature_index(valuation_review_report),
        rules=rules,
        source_description="估值与拥挤度快照",
    )
    source_type = component.source_type
    if source_type == "hard_data":
        source_type = "manual_input"
    elif source_type == "partial_hard_data":
        source_type = "partial_manual_input"
    return DailyScoreComponent(
        name=component.name,
        score=component.score,
        weight=component.weight,
        source_type=source_type,
        coverage=component.coverage,
        confidence=_source_type_confidence(source_type, component.coverage),
        reason=component.reason,
        signals=component.signals,
    )


def _score_policy_geopolitics_module(
    rules: ScoringRulesConfig,
    risk_event_occurrence_review_report: RiskEventOccurrenceReviewReport | None,
) -> DailyScoreComponent:
    placeholder = rules.placeholders["policy_geopolitics"]
    if (
        risk_event_occurrence_review_report is None
        or rules.policy_geopolitics is None
    ):
        return DailyScoreComponent(
            name="policy_geopolitics",
            score=placeholder.score,
            weight=rules.weights["policy_geopolitics"],
            source_type="placeholder",
            coverage=0.0,
            confidence=_source_type_confidence("placeholder", 0.0),
            reason=placeholder.reason,
            signals=(),
        )

    eligible_active_items = (
        risk_event_occurrence_review_report.score_eligible_active_items
    )
    has_current_review_attestation = (
        risk_event_occurrence_review_report.has_current_review_attestation
    )
    component = _score_signal_module(
        name="policy_geopolitics",
        weight=rules.weights["policy_geopolitics"],
        module_rules=rules.policy_geopolitics,
        feature_index=_risk_event_occurrence_feature_index(
            risk_event_occurrence_review_report
        ),
        rules=rules,
        source_description="政策/地缘风险事件发生记录",
    )
    source_type = component.source_type
    reason = component.reason
    if not eligible_active_items:
        if has_current_review_attestation:
            source_type = "manual_input"
            reason = (
                "已完成覆盖评估日且未过期的风险事件复核声明；未发现可进入评分的 "
                "active 政策或地缘风险事件发生记录。该声明只代表已检查的来源范围，"
                "不是自动风险消除证明。"
            )
        else:
            source_type = "insufficient_data"
            reason = (
                "未发现可进入评分的 active 政策或地缘风险事件发生记录；"
                "watch 记录只进入报告和人工复核。为避免把空记录当作无风险证明，"
                "本模块使用中性分。"
            )
    elif source_type == "hard_data":
        source_type = "manual_input"
        reason = (
            f"已评估 {len(eligible_active_items)} 个经审计且可评分的 active 风险事件发生记录。"
        )
    elif source_type == "partial_hard_data":
        source_type = "partial_manual_input"
        reason = (
            f"已部分评估 {len(eligible_active_items)} 个经审计且可评分的 active 风险事件发生记录。"
        )
    return DailyScoreComponent(
        name=component.name,
        score=component.score,
        weight=component.weight,
        source_type=source_type,
        coverage=component.coverage,
        confidence=_source_type_confidence(source_type, component.coverage),
        reason=reason,
        signals=component.signals,
    )


def _score_signal_module(
    name: str,
    weight: float,
    module_rules: ScoreModuleRuleConfig,
    feature_index: dict[tuple[str, str], float],
    rules: ScoringRulesConfig,
    source_description: str,
) -> DailyScoreComponent:
    signals = tuple(_score_signal(signal, feature_index) for signal in module_rules.signals)
    total_points = sum(signal.points for signal in signals)
    available_points = sum(signal.points for signal in signals if signal.available)
    coverage = available_points / total_points if total_points else 0.0

    if coverage < rules.minimum_signal_coverage:
        score = module_rules.neutral_score
        source_type = "insufficient_data"
        reason = (
            f"{source_description}信号覆盖率不足（{coverage:.0%}），"
            f"使用中性分 {module_rules.neutral_score:.1f}。"
        )
    else:
        earned_points = sum(signal.earned_points for signal in signals if signal.available)
        missing_points = total_points - available_points
        neutral_points = missing_points * (module_rules.neutral_score / 100.0)
        score = ((earned_points + neutral_points) / total_points) * 100.0
        source_type = "hard_data" if coverage == 1.0 else "partial_hard_data"
        reason = f"已按{source_description}评估配置权重的 {coverage:.0%}。"

    return DailyScoreComponent(
        name=name,
        score=_clamp(score, 0.0, 100.0),
        weight=weight,
        source_type=source_type,
        coverage=coverage,
        confidence=_source_type_confidence(source_type, coverage),
        reason=reason,
        signals=signals,
    )


def _score_signal(
    signal: ScoreSignalConfig,
    feature_index: dict[tuple[str, str], float],
) -> SignalScore:
    key = (signal.subject, signal.feature)
    value = feature_index.get(key)
    if value is None:
        return SignalScore(
            subject=signal.subject,
            feature=signal.feature,
            value=None,
            points=signal.points,
            earned_points=0.0,
            available=False,
            reason="缺少特征",
        )

    normalized = _normalize_signal_value(value, signal)
    return SignalScore(
        subject=signal.subject,
        feature=signal.feature,
        value=value,
        points=signal.points,
        earned_points=signal.points * normalized,
        available=True,
        reason=f"归一化得分={normalized:.2f}",
    )


def _normalize_signal_value(value: float, signal: ScoreSignalConfig) -> float:
    if signal.scale_min is not None and signal.scale_max is not None:
        if signal.scale_max == signal.scale_min:
            raise ValueError(f"scale_max equals scale_min for {signal.subject} {signal.feature}")
        return _clamp((value - signal.scale_min) / (signal.scale_max - signal.scale_min), 0.0, 1.0)

    if signal.bullish_below is not None and signal.bearish_above is not None:
        if value <= signal.bullish_below:
            return 1.0
        if value >= signal.bearish_above:
            return 0.0
        return _clamp(
            1.0 - ((value - signal.bullish_below) / (signal.bearish_above - signal.bullish_below)),
            0.0,
            1.0,
        )

    if signal.bullish_above is not None and signal.bearish_below is not None:
        if value >= signal.bullish_above:
            return 1.0
        if value <= signal.bearish_below:
            return 0.0
        return _clamp(
            (value - signal.bearish_below) / (signal.bullish_above - signal.bearish_below),
            0.0,
            1.0,
        )

    if signal.bullish_above is not None:
        return 1.0 if value > signal.bullish_above else 0.0
    if signal.bullish_below is not None:
        return 1.0 if value < signal.bullish_below else 0.0
    if signal.bearish_above is not None:
        return 0.0 if value > signal.bearish_above else 1.0
    if signal.bearish_below is not None:
        return 0.0 if value < signal.bearish_below else 1.0

    raise ValueError(f"signal has no scoring rule: {signal.subject} {signal.feature}")


def _feature_index(feature_set: MarketFeatureSet) -> dict[tuple[str, str], float]:
    return {(row.subject, row.feature): row.value for row in feature_set.rows}


def _fundamental_feature_index(
    report: SecFundamentalFeaturesReport,
) -> dict[tuple[str, str], float]:
    index: dict[tuple[str, str], float] = {}
    aggregate_values: dict[str, list[float]] = {}
    for row in report.rows:
        feature_name = f"{row.feature_id}_{row.period_type}"
        index[(row.ticker, feature_name)] = row.value
        aggregate_values.setdefault(feature_name, []).append(row.value)

    for feature_name, values in aggregate_values.items():
        index[("AI_CORE_MEDIAN", f"{feature_name}_median")] = float(median(values))
    return index


def _valuation_feature_index(report: ValuationReviewReport) -> dict[tuple[str, str], float]:
    usable_items = [
        item
        for item in report.items
        if item.source_type != "public_convenience"
        and item.health != "STALE"
        and item.valuation_percentile is not None
    ]
    if not usable_items:
        return {}

    valuation_percentiles = [
        item.valuation_percentile
        for item in usable_items
        if item.valuation_percentile is not None
    ]
    overheated_count = sum(
        item.health in {"EXPENSIVE_OR_CROWDED", "EXTREME_OVERHEATED"}
        for item in usable_items
    )
    return {
        ("AI_CORE_MEDIAN", "valuation_percentile"): float(median(valuation_percentiles)),
        ("AI_CORE", "overheated_snapshot_ratio"): overheated_count / len(usable_items),
    }


def _valuation_percentile_count(report: ValuationReviewReport) -> int:
    return sum(1 for item in report.items if item.valuation_percentile is not None)


def _eps_revision_count(report: ValuationReviewReport) -> int:
    snapshot_by_id = {
        loaded.snapshot.snapshot_id: loaded.snapshot
        for loaded in report.validation_report.snapshots
    }
    return sum(
        1
        for item in report.items
        if (snapshot := snapshot_by_id.get(item.snapshot_id)) is not None
        and any(
            metric.metric_id == "eps_revision_90d_pct"
            for metric in snapshot.expectation_metrics
        )
    )


def _execution_action_summary(
    execution_action_label: str | None,
    execution_action_id: str | None,
) -> str:
    if execution_action_label is None:
        return "未接入执行政策"
    if execution_action_id is None:
        return execution_action_label
    return f"{execution_action_label}（`{execution_action_id}`）"


def _daily_posture_label(report: DailyScoreReport) -> str:
    non_score_gates = [
        gate
        for gate in report.recommendation.triggered_position_gates
        if gate.gate_id != "score_model"
    ]
    if report.review_summary and report.review_summary.has_failures:
        return "人工复核"
    if report.confidence_assessment.level == "low":
        return "人工复核"
    if report.recommendation.total_score < 45:
        return "防守降仓"
    if non_score_gates and report.recommendation.total_score >= 55:
        return "中高配但受限"
    if (
        report.recommendation.total_score >= 65
        and report.confidence_assessment.level == "high"
        and not non_score_gates
    ):
        return "积极进攻"
    if report.recommendation.total_score >= 55:
        return "中高配"
    return "中性观察"


def _market_attractiveness_summary(report: DailyScoreReport) -> str:
    score = report.recommendation.total_score
    if score >= 65:
        label = "较强"
    elif score >= 55:
        label = "中等偏强"
    elif score >= 45:
        label = "中性"
    else:
        label = "偏弱"
    return f"{label}，AI 产业链评分 {score:.1f}/100。"


def _confidence_card_summary(report: DailyScoreReport) -> str:
    confidence = report.confidence_assessment
    return (
        f"{_confidence_level_label(confidence.level)}，"
        f"{confidence.score:.1f}/100；{_confidence_reason_summary(confidence)}"
    )


def _macro_budget_summary(report: DailyScoreReport) -> str:
    adjustment = report.macro_risk_asset_budget
    static_band = adjustment.static_total_risk_asset_band
    adjusted_band = adjustment.adjusted_total_risk_asset_band
    if adjustment.triggered:
        return (
            f"{static_band.min_position:.0%}-{static_band.max_position:.0%} -> "
            f"{adjusted_band.min_position:.0%}-{adjusted_band.max_position:.0%}"
            f"（{adjustment.level}）"
        )
    return (
        f"{adjusted_band.min_position:.0%}-{adjusted_band.max_position:.0%}"
        f"（{adjustment.level}）"
    )


def _daily_main_conclusion(report: DailyScoreReport, posture: str) -> str:
    final_band = _format_position_range(
        report.recommendation.risk_asset_ai_band.min_position,
        report.recommendation.risk_asset_ai_band.max_position,
    )
    return (
        f"{posture}：市场吸引力为 {report.recommendation.total_score:.1f} 分，"
        f"判断置信度为 {_confidence_level_label(report.confidence_assessment.level)}，"
        f"风险闸门后最终 AI 仓位为 {final_band}；"
        f"{_action_constraint_summary(report)}"
    )


def _daily_core_reasons(report: DailyScoreReport) -> tuple[str, str, str]:
    support = _primary_support_component(report)
    support_text = (
        "核心支撑："
        f"{_component_label(support.name)} {support.score:.1f} 分，"
        f"{support.reason}"
    )
    return (
        f"市场吸引力：{_market_attractiveness_summary(report)}",
        support_text,
        f"主要约束：{_action_constraint_summary(report)}",
    )


def _primary_support_component(report: DailyScoreReport) -> DailyScoreComponent:
    return max(
        report.components,
        key=lambda component: (
            component.score * component.weight,
            component.confidence,
        ),
    )


def _daily_largest_limitation(report: DailyScoreReport) -> str:
    if report.review_summary and report.review_summary.has_failures:
        return "人工复核摘要存在错误，日报结论不能作为完整仓位复核依据。"
    if report.review_summary and report.review_summary.has_warnings:
        return "人工复核摘要存在警告，thesis、风险、估值或交易复盘需要人工确认。"
    triggered = [
        gate
        for gate in report.recommendation.triggered_position_gates
        if gate.gate_id != "score_model"
    ]
    if triggered:
        gate = min(triggered, key=lambda item: item.max_position)
        return f"{gate.label} 将最终仓位上限压到 {gate.max_position:.0%}：{gate.reason}"
    if report.confidence_assessment.level != "high":
        return _confidence_reason_summary(report.confidence_assessment)
    limited_components = [
        component
        for component in report.components
        if component.source_type in {"placeholder", "insufficient_data"}
    ]
    if limited_components:
        labels = "、".join(_component_label(component.name) for component in limited_components)
        return f"仍有模块缺少硬数据或覆盖不足：{labels}。"
    return "未发现超出最小操作变化阈值外的主要限制。"


def _daily_next_trigger(report: DailyScoreReport) -> str:
    if report.confidence_assessment.level != "high" or (
        report.review_summary
        and (report.review_summary.has_failures or report.review_summary.has_warnings)
    ):
        return _add_condition_summary(report)
    if report.recommendation.total_score < 55:
        return _add_condition_summary(report)
    return _watch_condition_summary(report)


def _valuation_confidence_summary(report: ValuationReviewReport) -> str:
    if not report.items:
        return "无当前可见估值快照"
    counts: dict[str, int] = {}
    for item in report.items:
        key = f"{item.confidence_level}/{item.point_in_time_class}/{item.backtest_use}"
        counts[key] = counts.get(key, 0) + 1
    return "；".join(f"{key}={count}" for key, count in sorted(counts.items()))


def _position_change_summary(
    report: DailyScoreReport,
    previous: PreviousDailyScoreSnapshot | None,
) -> str:
    current_band = _format_position_range(
        report.recommendation.risk_asset_ai_band.min_position,
        report.recommendation.risk_asset_ai_band.max_position,
    )
    if previous is None:
        return (
            f"未找到早于 {report.as_of.isoformat()} 的结构化 overall 评分记录；"
            f"本期最终 AI 仓位（股票风险资产内）为 {current_band}。"
        )
    if previous.final_risk_asset_ai_min is None or previous.final_risk_asset_ai_max is None:
        return (
            f"上一条评分记录为 {previous.as_of.isoformat()}，但缺少结构化仓位列；"
            f"本期最终 AI 仓位（股票风险资产内）为 {current_band}。"
        )

    previous_band = _format_position_range(
        previous.final_risk_asset_ai_min,
        previous.final_risk_asset_ai_max,
    )
    score_delta = _format_number_delta(
        report.recommendation.total_score,
        previous.overall_score,
        "分",
    )
    confidence_delta = _format_number_delta(
        report.confidence_assessment.score,
        previous.confidence_score,
        "分",
    )
    band_delta = _format_band_delta(
        report.recommendation.risk_asset_ai_band.min_position,
        report.recommendation.risk_asset_ai_band.max_position,
        previous.final_risk_asset_ai_min,
        previous.final_risk_asset_ai_max,
    )
    return (
        f"{previous.as_of.isoformat()} {previous_band} -> "
        f"{report.as_of.isoformat()} {current_band}；"
        f"总分变化 {score_delta}，置信度变化 {confidence_delta}，"
        f"仓位边界变化 {band_delta}。"
    )


def _module_change_source_summary(
    report: DailyScoreReport,
    previous: PreviousDailyScoreSnapshot | None,
) -> str:
    supports = sorted(
        (
            component
            for component in report.components
            if component.score >= 55 and component.confidence >= 0.60
        ),
        key=lambda item: item.score,
        reverse=True,
    )
    pressures = sorted(
        (
            component
            for component in report.components
            if component.score < 50
            or component.confidence < 0.60
            or component.source_type in {"placeholder", "insufficient_data"}
        ),
        key=lambda item: (item.score, item.confidence),
    )
    support_text = _component_pressure_list(supports[:3], fallback="无明显高分支撑模块")
    pressure_text = _component_pressure_list(pressures[:4], fallback="无明显低分或低置信度模块")
    change_text = _component_change_list(report, previous)
    return f"分模块变化：{change_text}；支撑：{support_text}；压力：{pressure_text}。"


def _component_change_list(
    report: DailyScoreReport,
    previous: PreviousDailyScoreSnapshot | None,
) -> str:
    if previous is None:
        return "无上期模块评分对比，以下仅解释当前状态"
    parts: list[str] = []
    for component in report.components:
        previous_score = previous.component_scores.get(component.name)
        if previous_score is None:
            parts.append(f"{_component_label(component.name)} 本期{component.score:.1f}分/无上期")
        else:
            parts.append(
                f"{_component_label(component.name)} {component.score - previous_score:+.1f}分"
            )
    return "、".join(parts)


def _component_pressure_list(
    components: list[DailyScoreComponent],
    *,
    fallback: str,
) -> str:
    if not components:
        return fallback
    return "、".join(
        (
            f"{_component_label(component.name)} {component.score:.1f}分/"
            f"{_source_type_label(component.source_type)}/置信度{component.confidence:.0%}"
        )
        for component in components
    )


def _review_item_summary(report: DailyScoreReport, item_name: str) -> str:
    if report.review_summary is None:
        return "未接入人工复核摘要。"
    item = getattr(report.review_summary, item_name)
    if item is None:
        return "未接入人工复核摘要。"
    return (
        f"{item.status}；错误 {item.error_count}，警告 {item.warning_count}；"
        f"{item.summary}"
    )


def _risk_event_change_summary(report: DailyScoreReport) -> str:
    review = report.risk_event_occurrence_review_report
    if review is None:
        return "未接入风险事件发生记录，政策/地缘模块不能把空记录视为无风险证明。"
    active_items = [item for item in review.items if item.status == "active"]
    watch_items = [item for item in review.items if item.status == "watch"]
    eligible_count = len(review.score_eligible_active_items)
    gate_count = len(review.position_gate_eligible_active_items)
    highest = _highest_risk_level([item.level for item in active_items])
    return (
        f"{review.status}；active {len(active_items)}，watch {len(watch_items)}，"
        f"可进入评分 active {eligible_count}，可触发仓位闸门 active {gate_count}，"
        f"当前有效复核声明 {review.validation_report.current_review_attestation_count}，"
        f"最高 active 等级 {highest}。"
    )


def _valuation_change_summary(report: DailyScoreReport) -> str:
    review = report.valuation_review_report
    if review is None:
        return "未接入估值快照，估值模块使用占位/数据不足状态。"
    crowded = [
        item
        for item in review.items
        if item.health in {"EXPENSIVE_OR_CROWDED", "EXTREME_OVERHEATED"}
    ]
    confidence = _valuation_confidence_summary(review)
    return (
        f"{review.status}；当前快照 {len(review.items)} 个，"
        f"昂贵/拥挤 {len(crowded)} 个；PIT 可信度 {confidence}。"
    )


def _judgement_type(report: DailyScoreReport) -> str:
    thesis = report.review_summary.thesis if report.review_summary else None
    if thesis and (thesis.status == "FAIL" or thesis.error_count):
        return "thesis 证伪或人工复核失败"
    if thesis and ("WARNING" in thesis.status or thesis.warning_count):
        return "thesis 承压"

    risk_review = report.risk_event_occurrence_review_report
    if risk_review and any(
        item.level == "L3" for item in risk_review.position_gate_eligible_active_items
    ):
        return "风险事件主导的仓位限制"

    fundamentals = _component_by_name(report, "fundamentals")
    valuation = _component_by_name(report, "valuation")
    if fundamentals and fundamentals.source_type == "hard_data" and fundamentals.score < 45:
        return "基本面恶化"
    if (
        fundamentals
        and valuation
        and fundamentals.source_type == "hard_data"
        and fundamentals.score >= 50
        and valuation.score < 50
    ):
        return "估值过高但基本面未坏"

    trend = _component_by_name(report, "trend")
    risk_sentiment = _component_by_name(report, "risk_sentiment")
    if (trend and trend.score < 50) or (risk_sentiment and risk_sentiment.score < 50):
        return "市场短期波动或风险情绪扰动"
    return "评分模型常规再平衡"


def _action_constraint_summary(report: DailyScoreReport) -> str:
    if report.review_summary and report.review_summary.has_failures:
        return "存在人工复核失败项；日报结论仅可作为研究输入，需先修复复核问题。"
    triggered = [
        gate
        for gate in report.recommendation.triggered_position_gates
        if gate.gate_id != "score_model"
    ]
    if triggered:
        gate_text = "、".join(f"{gate.label} 上限 {gate.max_position:.0%}" for gate in triggered)
        return f"不主动加仓；最终仓位不得超过已触发闸门中的最严格上限（{gate_text}）。"
    if report.confidence_assessment.level == "low":
        return "判断置信度为低；即使总分支持当前区间，也应等待数据或人工复核补强。"
    return (
        "未触发额外硬闸门；任何主动调整仍需超过"
        f"最小操作变化阈值 {report.minimum_action_delta:.0%}。"
    )


def _add_condition_summary(report: DailyScoreReport) -> str:
    conditions: list[str] = []
    triggered = [
        gate.label
        for gate in report.recommendation.triggered_position_gates
        if gate.gate_id != "score_model"
    ]
    if triggered:
        conditions.append(f"已触发仓位闸门解除或上限不再低于模型区间（{', '.join(triggered)}）")
    low_components = [
        _component_label(component.name)
        for component in report.components
        if component.score < 50
        or component.source_type in {"placeholder", "insufficient_data"}
    ]
    if low_components:
        conditions.append(f"低分/缺数模块回到 50 分以上并补齐来源（{', '.join(low_components)}）")
    if report.confidence_assessment.level != "high":
        conditions.append("判断置信度回到高，且低置信度原因被数据或人工复核解除")
    if not conditions:
        conditions.append("综合分进入更高仓位区间，且未新增 thesis、风险事件或估值拥挤限制")
    return "；".join(conditions) + "。"


def _reduce_condition_summary(report: DailyScoreReport) -> str:
    conditions = [
        "综合分跌破当前仓位区间下沿，或趋势/风险情绪任一核心模块降至 45 分以下",
        "新增 L3 或可评分 active 风险事件，导致 position_gate 上限低于当前最终区间",
        "thesis 进入 challenged/invalidated 或人工复核失败",
        "基本面硬数据恶化，同时估值仍处高位或数据置信度下降",
    ]
    return "；".join(conditions) + "。"


def _watch_condition_summary(report: DailyScoreReport) -> str:
    if report.confidence_assessment.level == "low":
        return "总分未明显恶化但置信度不足，优先观察数据补齐、人工复核和风险事件状态。"
    if report.recommendation.triggered_position_gates:
        return "总分维持当前区间但闸门未解除，保持按最终上限复核，不把模型区间当作可执行区间。"
    return "总分、置信度和闸门均未越过触发条件时，维持当前区间并复核新增证据。"


def _format_position_range(min_position: float, max_position: float) -> str:
    return f"{min_position:.0%}-{max_position:.0%}"


def _format_band_delta(
    current_min: float,
    current_max: float,
    previous_min: float,
    previous_max: float,
) -> str:
    min_delta = (current_min - previous_min) * 100
    max_delta = (current_max - previous_max) * 100
    return f"下限{min_delta:+.0f}pp/上限{max_delta:+.0f}pp"


def _format_number_delta(
    current: float,
    previous: float | None,
    unit: str,
) -> str:
    if previous is None:
        return "无上期可比值"
    return f"{current - previous:+.1f}{unit}"


def _highest_risk_level(levels: list[str]) -> str:
    if not levels:
        return "无"
    order = {"L3": 3, "L2": 2, "L1": 1}
    return max(levels, key=lambda level: order.get(level, 0))


def _component_by_name(
    report: DailyScoreReport,
    component_name: str,
) -> DailyScoreComponent | None:
    return next(
        (component for component in report.components if component.name == component_name),
        None,
    )


def _row_float_or_none(row: pd.Series, column: str) -> float | None:
    if column not in row.index:
        return None
    value = row[column]
    if pd.isna(value):
        return None
    return float(value)


def _row_str_or_none(row: pd.Series, column: str) -> str | None:
    if column not in row.index:
        return None
    value = row[column]
    if pd.isna(value):
        return None
    text = str(value)
    return text if text else None


def _component_value_map(frame: pd.DataFrame, column: str) -> dict[str, float]:
    if column not in frame.columns:
        return {}
    values: dict[str, float] = {}
    for _, row in frame.iterrows():
        component = _row_str_or_none(row, "component")
        value = _row_float_or_none(row, column)
        if component is None or component == "overall" or value is None:
            continue
        values[component] = value
    return values


def _risk_event_occurrence_feature_index(
    report: RiskEventOccurrenceReviewReport,
) -> dict[tuple[str, str], float]:
    active_items = report.score_eligible_active_items
    if not active_items:
        if report.has_current_review_attestation:
            return {
                ("POLICY_GEOPOLITICS", "active_or_watch_l3_count"): 0.0,
                ("POLICY_GEOPOLITICS", "active_or_watch_l2_count"): 0.0,
                ("POLICY_GEOPOLITICS", "minimum_exposure_multiplier"): 1.0,
            }
        return {}

    return {
        ("POLICY_GEOPOLITICS", "active_or_watch_l3_count"): float(
            sum(item.level == "L3" for item in active_items)
        ),
        ("POLICY_GEOPOLITICS", "active_or_watch_l2_count"): float(
            sum(item.level == "L2" for item in active_items)
        ),
        ("POLICY_GEOPOLITICS", "minimum_exposure_multiplier"): min(
            item.target_ai_exposure_multiplier for item in active_items
        ),
    }


def _score_component_record(as_of: date, component: DailyScoreComponent) -> dict[str, object]:
    return {
        "as_of": as_of.isoformat(),
        "component": component.name,
        "score": component.score,
        "weight": component.weight,
        "source_type": component.source_type,
        "coverage": component.coverage,
        "confidence": component.confidence,
        "confidence_level": _confidence_level(component.confidence),
        "confidence_reasons": "",
        "reason": component.reason,
    }


def _triggered_position_gate_summary(report: DailyScoreReport) -> str:
    triggered = [
        gate.label
        for gate in report.recommendation.triggered_position_gates
        if gate.gate_id != "score_model"
    ]
    if not triggered:
        return "无"
    return "、".join(triggered)


def _component_label(name: str) -> str:
    label = COMPONENT_LABELS.get(name)
    if label is None:
        return name
    return f"{label}（{name}）"


def _source_type_label(source_type: str) -> str:
    return SOURCE_TYPE_LABELS.get(source_type, source_type)


def _source_type_confidence(source_type: str, coverage: float) -> float:
    coverage = _clamp(coverage, 0.0, 1.0)
    if source_type == "hard_data":
        return coverage
    if source_type == "partial_hard_data":
        return _clamp(0.55 + coverage * 0.35, 0.0, 0.90)
    if source_type == "manual_input":
        return _clamp(coverage * 0.75, 0.0, 0.75)
    if source_type == "partial_manual_input":
        return _clamp(coverage * 0.60, 0.0, 0.60)
    if source_type == "insufficient_data":
        return 0.35
    if source_type == "placeholder":
        return 0.25
    if source_type == "derived":
        return 1.0
    return _clamp(coverage * 0.50, 0.0, 0.50)


def _confidence_assessment(report: DailyScoreReport) -> DailyConfidenceAssessment:
    total_weight = sum(component.weight for component in report.components)
    if total_weight <= 0:
        base_score = 0.0
    else:
        base_score = (
            sum(component.confidence * component.weight for component in report.components)
            / total_weight
            * 100.0
        )

    reasons: list[str] = []
    penalty = 0.0
    if report.data_quality_report.status == "FAIL":
        penalty += 100.0
        reasons.append("市场数据质量门禁失败")
    elif "WARNING" in report.data_quality_report.status:
        penalty += 15.0
        reasons.append("市场数据质量门禁存在警告")

    low_confidence_components = [
        _component_label(component.name)
        for component in report.components
        if component.confidence < 0.60
    ]
    if low_confidence_components:
        reasons.append(f"低置信度模块：{', '.join(low_confidence_components)}")

    if report.feature_set.warnings:
        penalty += 5.0
        reasons.append(f"市场特征存在 {len(report.feature_set.warnings)} 条警告")
    if (
        report.fundamental_feature_report is not None
        and report.fundamental_feature_report.warning_count
    ):
        penalty += 5.0
        reasons.append(
            f"SEC 基本面特征存在 {report.fundamental_feature_report.warning_count} 条警告"
        )
    if report.review_summary and report.review_summary.has_failures:
        penalty += 30.0
        reasons.append("人工复核摘要存在失败项")
    elif report.review_summary and report.review_summary.has_warnings:
        penalty += 10.0
        reasons.append("人工复核摘要存在警告项")

    score = _clamp(base_score - penalty, 0.0, 100.0)
    level = _confidence_level(score / 100.0)
    adjusted_band = _confidence_adjusted_band(
        report.recommendation.risk_asset_ai_band,
        score,
    )
    if not reasons:
        reasons.append("核心输入覆盖和质量状态未触发额外置信度扣减")
    return DailyConfidenceAssessment(
        score=score,
        level=level,
        reasons=tuple(reasons),
        adjusted_risk_asset_ai_band=adjusted_band,
    )


def _confidence_adjusted_band(band: PositionBand, confidence_score: float) -> PositionBand:
    if confidence_score >= 75:
        cap_multiplier = 1.0
    elif confidence_score >= 60:
        cap_multiplier = 0.85
    elif confidence_score >= 45:
        cap_multiplier = 0.70
    else:
        cap_multiplier = 0.50
    adjusted_max = min(band.max_position, band.max_position * cap_multiplier)
    adjusted_min = min(band.min_position, adjusted_max)
    label = band.label if adjusted_max >= band.max_position else f"{band.label}/置信度受限"
    return PositionBand(
        min_position=adjusted_min,
        max_position=adjusted_max,
        label=label,
    )


def _confidence_level(confidence: float) -> str:
    if confidence >= 0.75:
        return "high"
    if confidence >= 0.60:
        return "medium"
    return "low"


def _confidence_level_label(level: str) -> str:
    return {
        "high": "高",
        "medium": "中",
        "low": "低",
    }.get(level, level)


def _confidence_reason_summary(confidence: DailyConfidenceAssessment) -> str:
    return "；".join(confidence.reasons)


def _clamp(value: float, lower: float, upper: float) -> float:
    return min(max(value, lower), upper)


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
