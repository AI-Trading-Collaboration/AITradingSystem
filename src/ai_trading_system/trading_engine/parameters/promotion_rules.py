from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ai_trading_system.trading_engine.parameters.parameter_diff import ParameterChange
from ai_trading_system.trading_engine.parameters.parameter_schema import (
    DataQualityStatus,
    PromotionRulesConfig,
    PromotionStatus,
)


@dataclass(frozen=True)
class PromotionDecision:
    status: PromotionStatus
    reason: str
    hard_rejections: tuple[str, ...]
    manual_review_items: tuple[str, ...]
    criteria_results: dict[str, bool]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "reason": self.reason,
            "hard_rejections": list(self.hard_rejections),
            "manual_review_items": list(self.manual_review_items),
            "criteria_results": dict(self.criteria_results),
        }


def evaluate_promotion_decision(
    *,
    rules: PromotionRulesConfig,
    baseline_result: dict[str, float],
    candidate_result: dict[str, float],
    walk_forward_windows: tuple[dict[str, Any], ...],
    parameter_changes: tuple[ParameterChange, ...],
    data_quality_status: DataQualityStatus,
    missing_required_input_artifacts: bool,
) -> PromotionDecision:
    hard_rejections = _hard_rejections(
        baseline_result=baseline_result,
        candidate_result=candidate_result,
        walk_forward_windows=walk_forward_windows,
        parameter_changes=parameter_changes,
        data_quality_status=data_quality_status,
        missing_required_input_artifacts=missing_required_input_artifacts,
    )
    criteria = _criteria_results(
        rules=rules,
        baseline_result=baseline_result,
        candidate_result=candidate_result,
        walk_forward_windows=walk_forward_windows,
        parameter_changes=parameter_changes,
    )
    manual_review_items = _manual_review_items(criteria, parameter_changes)
    if hard_rejections:
        return PromotionDecision(
            status="rejected",
            reason="Hard rejection rule triggered: " + ", ".join(hard_rejections),
            hard_rejections=tuple(hard_rejections),
            manual_review_items=tuple(manual_review_items),
            criteria_results=criteria,
        )
    if all(criteria.values()) and parameter_changes:
        return PromotionDecision(
            status="candidate",
            reason=(
                "Candidate passed all configured promotion criteria but still requires "
                "manual review."
            ),
            hard_rejections=(),
            manual_review_items=tuple(manual_review_items),
            criteria_results=criteria,
        )
    if _has_positive_evidence(baseline_result, candidate_result):
        return PromotionDecision(
            status="manual_review_required" if manual_review_items else "watch",
            reason="Candidate has partial improvement but does not pass all conservative criteria.",
            hard_rejections=(),
            manual_review_items=tuple(manual_review_items),
            criteria_results=criteria,
        )
    return PromotionDecision(
        status="watch",
        reason="Candidate does not show enough improvement over baseline.",
        hard_rejections=(),
        manual_review_items=tuple(manual_review_items),
        criteria_results=criteria,
    )


def _hard_rejections(
    *,
    baseline_result: dict[str, float],
    candidate_result: dict[str, float],
    walk_forward_windows: tuple[dict[str, Any], ...],
    parameter_changes: tuple[ParameterChange, ...],
    data_quality_status: DataQualityStatus,
    missing_required_input_artifacts: bool,
) -> list[str]:
    rejections: list[str] = []
    baseline_mdd = baseline_result.get("max_drawdown", 0.0)
    candidate_mdd = candidate_result.get("max_drawdown", 0.0)
    if candidate_mdd < baseline_mdd - 0.05:
        rejections.append("max_drawdown_worse_than_baseline_by_more_than_5_percent")
    baseline_turnover = baseline_result.get("turnover", 0.0)
    candidate_turnover = candidate_result.get("turnover", 0.0)
    if candidate_turnover > baseline_turnover * 1.5 + 1e-12 and candidate_turnover > 0.0:
        rejections.append("turnover_more_than_50_percent_above_baseline")
    passing_windows = [
        window for window in walk_forward_windows if str(window.get("status")) == "PASS"
    ]
    if len(walk_forward_windows) > 1 and len(passing_windows) <= 1:
        rejections.append("performance_only_improves_in_one_window")
    if any(not change.reason.strip() for change in parameter_changes):
        rejections.append("parameter_change_without_explanation")
    if missing_required_input_artifacts:
        rejections.append("missing_required_input_artifacts")
    if data_quality_status == "INSUFFICIENT_DATA":
        rejections.append("insufficient_data")
    if data_quality_status == "FAILED":
        rejections.append("data_quality_status_red")
    return rejections


def _criteria_results(
    *,
    rules: PromotionRulesConfig,
    baseline_result: dict[str, float],
    candidate_result: dict[str, float],
    walk_forward_windows: tuple[dict[str, Any], ...],
    parameter_changes: tuple[ParameterChange, ...],
) -> dict[str, bool]:
    criteria = rules.promotion_criteria
    annualized_delta = candidate_result.get("annualized_return", 0.0) - baseline_result.get(
        "annualized_return", 0.0
    )
    sharpe_delta = candidate_result.get("sharpe_ratio", 0.0) - baseline_result.get(
        "sharpe_ratio", 0.0
    )
    baseline_turnover = baseline_result.get("turnover", 0.0)
    candidate_turnover = candidate_result.get("turnover", 0.0)
    turnover_increase = _relative_increase(candidate_turnover, baseline_turnover)
    recent_delta = _recent_period_delta(walk_forward_windows)
    passing_ratio = _passing_ratio(walk_forward_windows)
    return {
        "max_drawdown": candidate_result.get("max_drawdown", 0.0)
        >= baseline_result.get("max_drawdown", 0.0),
        "annualized_return": annualized_delta
        >= float(criteria["annualized_return"]["min_relative_improvement"]),
        "sharpe_ratio": sharpe_delta >= float(criteria["sharpe_ratio"]["min_relative_improvement"]),
        "turnover": turnover_increase <= float(criteria["turnover"]["max_relative_increase"]),
        "recent_period": recent_delta
        >= -float(criteria["recent_period"]["must_not_underperform_baseline_by_more_than"]),
        "stability": passing_ratio >= float(criteria["stability"]["min_passing_windows_ratio"]),
        "explainability": bool(parameter_changes)
        and all(change.reason.strip() for change in parameter_changes),
    }


def _manual_review_items(
    criteria: dict[str, bool],
    parameter_changes: tuple[ParameterChange, ...],
) -> list[str]:
    items = [f"criterion_failed:{key}" for key, passed in criteria.items() if not passed]
    for change in parameter_changes:
        if abs(change.delta) >= 0.05:
            items.append(f"review_parameter_change:{change.name}")
    return items


def _has_positive_evidence(
    baseline_result: dict[str, float],
    candidate_result: dict[str, float],
) -> bool:
    return (
        candidate_result.get("annualized_return", 0.0)
        > baseline_result.get("annualized_return", 0.0)
        or candidate_result.get("sharpe_ratio", 0.0) > baseline_result.get("sharpe_ratio", 0.0)
        or candidate_result.get("max_drawdown", 0.0) > baseline_result.get("max_drawdown", 0.0)
    )


def _relative_increase(candidate: float, baseline: float) -> float:
    if baseline <= 0.0:
        return 0.0 if candidate <= 0.0 else float("inf")
    return (candidate - baseline) / baseline


def _passing_ratio(windows: tuple[dict[str, Any], ...]) -> float:
    if not windows:
        return 0.0
    return sum(1 for window in windows if str(window.get("status")) == "PASS") / len(windows)


def _recent_period_delta(windows: tuple[dict[str, Any], ...]) -> float:
    if not windows:
        return 0.0
    latest = windows[-1]
    baseline = latest.get("baseline_metrics")
    candidate = latest.get("candidate_metrics")
    if not isinstance(baseline, dict) or not isinstance(candidate, dict):
        return 0.0
    return float(candidate.get("annualized_return", 0.0)) - float(
        baseline.get("annualized_return", 0.0)
    )
