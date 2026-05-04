from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ConclusionUsageLevel = Literal[
    "actionable",
    "trend_only",
    "review_required",
    "research_only",
    "data_limited",
    "backtest_limited",
]

DecisionScope = Literal["position_review", "trend_judgment"]


@dataclass(frozen=True)
class ConclusionBoundary:
    usage_level: ConclusionUsageLevel
    usage_label: str
    decision_scope: DecisionScope
    scope_label: str
    posture_label: str | None
    reasons: tuple[str, ...]
    release_conditions: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    production_effect: str = "none"


def classify_conclusion_boundary(
    *,
    report_status: str,
    data_quality_status: str,
    posture_label: str | None = None,
    confidence_level: str | None = None,
    has_review_failures: bool = False,
    has_review_warnings: bool = False,
    has_source_limitations: bool = False,
    has_backtest_limitations: bool = False,
    decision_scope: DecisionScope = "position_review",
    evidence_refs: tuple[str, ...] = (),
) -> ConclusionBoundary:
    reasons: list[str] = []
    if data_quality_status == "FAIL":
        return _boundary(
            "data_limited",
            posture_label,
            decision_scope=decision_scope,
            reasons=("数据质量门禁失败，不能把结论用于仓位复核。",),
            evidence_refs=evidence_refs,
        )
    if has_backtest_limitations:
        reasons.append("回测输入覆盖不足或审计存在限制，历史结论只能降级解释。")
        return _boundary(
            "backtest_limited",
            posture_label,
            decision_scope=decision_scope,
            reasons=tuple(reasons),
            evidence_refs=evidence_refs,
        )
    if has_review_failures:
        reasons.append("人工复核摘要存在失败项。")
        return _boundary(
            "review_required",
            posture_label,
            decision_scope=decision_scope,
            reasons=tuple(reasons),
            evidence_refs=evidence_refs,
        )
    if confidence_level == "low":
        reasons.append("判断置信度为低。")
        return _boundary(
            "review_required",
            posture_label,
            decision_scope=decision_scope,
            reasons=tuple(reasons),
            evidence_refs=evidence_refs,
        )
    if has_source_limitations or report_status != "PASS":
        if has_source_limitations:
            reasons.append("存在占位、覆盖不足或来源限制。")
        if report_status != "PASS":
            reasons.append(f"报告状态为 {report_status}。")
        return _boundary(
            "data_limited",
            posture_label,
            decision_scope=decision_scope,
            reasons=tuple(reasons),
            evidence_refs=evidence_refs,
        )
    if has_review_warnings:
        reasons.append("人工复核摘要存在警告。")
        return _boundary(
            "review_required",
            posture_label,
            decision_scope=decision_scope,
            reasons=tuple(reasons),
            evidence_refs=evidence_refs,
        )
    if decision_scope == "trend_judgment":
        return _boundary(
            "trend_only",
            posture_label,
            decision_scope=decision_scope,
            reasons=("当前项目范围限定为趋势判断和投研复核辅助，不触发交易或账户调仓。",),
            evidence_refs=evidence_refs,
        )
    return _boundary(
        "actionable",
        posture_label,
        decision_scope=decision_scope,
        reasons=("数据质量、置信度、来源覆盖和人工复核未触发降级条件。",),
        evidence_refs=evidence_refs,
    )


def render_conclusion_boundary_section(boundary: ConclusionBoundary) -> str:
    lines = [
        "## 结论使用等级",
        "",
        f"- 结论等级：{boundary.usage_label}（`{boundary.usage_level}`）",
        f"- 适用范围：{boundary.scope_label}（`{boundary.decision_scope}`）",
        f"- 投资姿态标签：{boundary.posture_label or '未记录'}",
        f"- 生产影响：{boundary.production_effect}",
        "- 边界说明：结论等级回答“这个结论能用于什么范围”，"
        "投资姿态回答“当前 AI 产业链处于什么状态”；两者不能互相替代。",
        "",
        "### 降级原因",
        "",
    ]
    lines.extend(f"- {reason}" for reason in boundary.reasons)
    lines.extend(["", "### 解除条件", ""])
    lines.extend(f"- {condition}" for condition in boundary.release_conditions)
    lines.extend(["", "### 可追溯证据", ""])
    if boundary.evidence_refs:
        lines.extend(f"- `{ref}`" for ref in boundary.evidence_refs)
    else:
        lines.append("- 未记录额外证据引用。")
    return "\n".join(lines) + "\n"


def _boundary(
    usage_level: ConclusionUsageLevel,
    posture_label: str | None,
    *,
    decision_scope: DecisionScope,
    reasons: tuple[str, ...],
    evidence_refs: tuple[str, ...],
) -> ConclusionBoundary:
    return ConclusionBoundary(
        usage_level=usage_level,
        usage_label=_usage_label(usage_level),
        decision_scope=decision_scope,
        scope_label=_scope_label(decision_scope),
        posture_label=posture_label,
        reasons=reasons,
        release_conditions=_release_conditions(usage_level),
        evidence_refs=evidence_refs,
    )


def _usage_label(level: ConclusionUsageLevel) -> str:
    return {
        "actionable": "可作为仓位复核依据",
        "trend_only": "趋势判断，不触发交易",
        "review_required": "必须人工复核",
        "research_only": "仅研究观察",
        "data_limited": "数据不足，结论降级",
        "backtest_limited": "回测覆盖不足，结论降级",
    }[level]


def _scope_label(scope: DecisionScope) -> str:
    return {
        "position_review": "仓位复核",
        "trend_judgment": "趋势判断/投研辅助，不触发交易",
    }[scope]


def _release_conditions(level: ConclusionUsageLevel) -> tuple[str, ...]:
    return {
        "actionable": ("保持数据质量门禁通过，并持续复核新增证据和风险事件。",),
        "trend_only": (
            "保持数据质量门禁通过，并持续复核新增证据、风险事件和趋势变化。",
            "如未来需要仓位复核或交易执行，必须重新提高范围并补齐账户、执行和审批要求。",
        ),
        "review_required": (
            "完成人工复核并解除失败/警告项。",
            "置信度回到中或高，且关键 gate 不再要求人工确认。",
        ),
        "research_only": ("补齐使结论可进入日报或回测复核的结构化输入。",),
        "data_limited": (
            "修复数据质量、来源覆盖或模块占位问题。",
            "报告状态回到 PASS，或明确说明剩余限制不影响本结论。",
        ),
        "backtest_limited": (
            "补齐 point-in-time 基本面、估值和风险事件历史切片。",
            "输入审计不再提示关键覆盖缺口。",
        ),
    }[level]
