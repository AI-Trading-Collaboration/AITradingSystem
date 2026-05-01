from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.config import (
    ScoreModuleRuleConfig,
    ScoreSignalConfig,
    ScoringRulesConfig,
)
from ai_trading_system.data.quality import DataQualityReport
from ai_trading_system.features.market import MarketFeatureSet
from ai_trading_system.scoring.position_model import (
    ModuleScore,
    PositionRecommendation,
    WeightedScoreModel,
)

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
class DailyScoreReport:
    as_of: date
    components: tuple[DailyScoreComponent, ...]
    recommendation: PositionRecommendation
    data_quality_report: DataQualityReport
    feature_set: MarketFeatureSet
    minimum_action_delta: float

    @property
    def status(self) -> str:
        if any(component.source_type != "hard_data" for component in self.components):
            return "PASS_WITH_LIMITATIONS"
        if self.feature_set.warnings:
            return "PASS_WITH_WARNINGS"
        return "PASS"


def build_daily_score_report(
    feature_set: MarketFeatureSet,
    data_quality_report: DataQualityReport,
    rules: ScoringRulesConfig,
    total_risk_asset_min: float,
    total_risk_asset_max: float,
) -> DailyScoreReport:
    components = [
        _score_hard_data_module("trend", rules.weights["trend"], rules.trend, feature_set, rules),
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
    ]
    for name in ["fundamentals", "valuation", "policy_geopolitics"]:
        placeholder = rules.placeholders[name]
        components.append(
            DailyScoreComponent(
                name=name,
                score=placeholder.score,
                weight=rules.weights[name],
                source_type="placeholder",
                coverage=0.0,
                reason=placeholder.reason,
                signals=(),
            )
        )

    recommendation = WeightedScoreModel().recommend(
        [component.to_module_score() for component in components],
        total_risk_asset_min=total_risk_asset_min,
        total_risk_asset_max=total_risk_asset_max,
    )
    return DailyScoreReport(
        as_of=feature_set.as_of,
        components=tuple(components),
        recommendation=recommendation,
        data_quality_report=data_quality_report,
        feature_set=feature_set,
        minimum_action_delta=rules.position_change.minimum_action_delta,
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
                "reason": f"仓位区间：{report.recommendation.label}",
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


def render_daily_score_report(
    report: DailyScoreReport,
    data_quality_report_path: Path,
    feature_report_path: Path,
    features_path: Path,
    scores_path: Path,
) -> str:
    recommendation = report.recommendation
    lines = [
        "# AI 产业链每日评分",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 总分：{recommendation.total_score:.1f}",
        f"- 仓位状态：{recommendation.label}",
        (
            "- AI 仓位（股票风险资产内）："
            f"{recommendation.risk_asset_ai_band.min_position:.0%}-"
            f"{recommendation.risk_asset_ai_band.max_position:.0%}"
        ),
        (
            "- 股票/风险资产预算（总资产内）："
            f"{recommendation.total_risk_asset_band.min_position:.0%}-"
            f"{recommendation.total_risk_asset_band.max_position:.0%}"
        ),
        (
            "- AI 仓位（总资产内）："
            f"{recommendation.total_asset_ai_band.min_position:.0%}-"
            f"{recommendation.total_asset_ai_band.max_position:.0%}"
        ),
        f"- 最小操作变化阈值：{report.minimum_action_delta:.0%}",
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
        "",
        "## 模块评分",
        "",
        "| 模块 | 分数 | 权重 | 来源 | 覆盖率 | 说明 |",
        "|---|---:|---:|---|---:|---|",
    ]

    for component in report.components:
        lines.append(
            "| "
            f"{_component_label(component.name)} | "
            f"{component.score:.1f} | "
            f"{component.weight:.1f} | "
            f"{_source_type_label(component.source_type)} | "
            f"{component.coverage:.0%} | "
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

    lines.extend(["", "## 限制说明", ""])
    limitations = [
        component
        for component in report.components
        if component.source_type in {"placeholder", "insufficient_data"}
    ]
    if not limitations and not report.feature_set.warnings:
        lines.append("未发现限制。")
    else:
        for component in limitations:
            lines.append(f"- {_component_label(component.name)}：{component.reason}")
        if report.feature_set.warnings:
            lines.append(
                f"- 存在特征警告：{len(report.feature_set.warnings)} 条。"
                "请查看特征摘要，确认是否有历史窗口不足或输入不可用。"
            )

    return "\n".join(lines) + "\n"


def write_daily_score_report(
    report: DailyScoreReport,
    data_quality_report_path: Path,
    feature_report_path: Path,
    features_path: Path,
    scores_path: Path,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_daily_score_report(
            report,
            data_quality_report_path=data_quality_report_path,
            feature_report_path=feature_report_path,
            features_path=features_path,
            scores_path=scores_path,
        ),
        encoding="utf-8",
    )
    return output_path


def default_daily_score_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"daily_score_{as_of.isoformat()}.md"


def _score_hard_data_module(
    name: str,
    weight: float,
    module_rules: ScoreModuleRuleConfig,
    feature_set: MarketFeatureSet,
    rules: ScoringRulesConfig,
) -> DailyScoreComponent:
    feature_index = _feature_index(feature_set)
    signals = tuple(_score_signal(signal, feature_index) for signal in module_rules.signals)
    total_points = sum(signal.points for signal in signals)
    available_points = sum(signal.points for signal in signals if signal.available)
    coverage = available_points / total_points if total_points else 0.0

    if coverage < rules.minimum_signal_coverage:
        score = module_rules.neutral_score
        source_type = "insufficient_data"
        reason = (
            f"硬数据信号覆盖率不足（{coverage:.0%}），"
            f"使用中性分 {module_rules.neutral_score:.1f}。"
        )
    else:
        earned_points = sum(signal.earned_points for signal in signals if signal.available)
        missing_points = total_points - available_points
        neutral_points = missing_points * (module_rules.neutral_score / 100.0)
        score = ((earned_points + neutral_points) / total_points) * 100.0
        source_type = "hard_data" if coverage == 1.0 else "partial_hard_data"
        reason = f"已评估配置硬数据信号权重的 {coverage:.0%}。"

    return DailyScoreComponent(
        name=name,
        score=_clamp(score, 0.0, 100.0),
        weight=weight,
        source_type=source_type,
        coverage=coverage,
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


def _score_component_record(as_of: date, component: DailyScoreComponent) -> dict[str, object]:
    return {
        "as_of": as_of.isoformat(),
        "component": component.name,
        "score": component.score,
        "weight": component.weight,
        "source_type": component.source_type,
        "coverage": component.coverage,
        "reason": component.reason,
    }


def _component_label(name: str) -> str:
    label = COMPONENT_LABELS.get(name)
    if label is None:
        return name
    return f"{label}（{name}）"


def _source_type_label(source_type: str) -> str:
    return SOURCE_TYPE_LABELS.get(source_type, source_type)


def _clamp(value: float, lower: float, upper: float) -> float:
    return min(max(value, lower), upper)


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
