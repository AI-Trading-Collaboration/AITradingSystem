from __future__ import annotations

from collections.abc import Mapping

from ai_trading_system.config import PositionGateRulesConfig
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
    valuation_review_report: ValuationReviewReport | None = None,
    risk_event_occurrence_review_report: RiskEventOccurrenceReviewReport | None = None,
) -> tuple[PositionGate, ...]:
    return (
        _portfolio_limit_gate(
            score_band=score_band,
            total_risk_asset_max=total_risk_asset_max,
            max_total_ai_exposure=max_total_ai_exposure,
        ),
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
    )


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
