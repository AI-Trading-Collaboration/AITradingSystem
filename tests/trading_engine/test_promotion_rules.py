from __future__ import annotations

from ai_trading_system.trading_engine.parameters.parameter_diff import ParameterChange
from ai_trading_system.trading_engine.parameters.parameter_schema import PromotionRulesConfig
from ai_trading_system.trading_engine.parameters.promotion_rules import (
    evaluate_promotion_decision,
)


def test_promotion_rejects_high_turnover_even_when_return_improves() -> None:
    decision = evaluate_promotion_decision(
        rules=_promotion_rules(),
        baseline_result=_metrics(annualized_return=0.10, max_drawdown=-0.10, sharpe_ratio=1.0),
        candidate_result=_metrics(
            annualized_return=0.14,
            max_drawdown=-0.09,
            sharpe_ratio=1.1,
            turnover=1.60,
        ),
        walk_forward_windows=_passing_windows(3),
        parameter_changes=(_change(),),
        data_quality_status="OK",
        missing_required_input_artifacts=False,
    )

    assert decision.status == "rejected"
    assert "turnover_more_than_50_percent_above_baseline" in decision.hard_rejections


def test_promotion_rejects_insufficient_data() -> None:
    decision = evaluate_promotion_decision(
        rules=_promotion_rules(),
        baseline_result=_metrics(),
        candidate_result=_metrics(annualized_return=0.20, max_drawdown=-0.08, sharpe_ratio=1.2),
        walk_forward_windows=_passing_windows(3),
        parameter_changes=(_change(),),
        data_quality_status="INSUFFICIENT_DATA",
        missing_required_input_artifacts=False,
    )

    assert decision.status == "rejected"
    assert "insufficient_data" in decision.hard_rejections


def test_promotion_allows_candidate_only_after_stable_multi_window_pass() -> None:
    decision = evaluate_promotion_decision(
        rules=_promotion_rules(),
        baseline_result=_metrics(annualized_return=0.10, max_drawdown=-0.10, sharpe_ratio=1.0),
        candidate_result=_metrics(
            annualized_return=0.13,
            max_drawdown=-0.08,
            sharpe_ratio=1.06,
            turnover=1.10,
        ),
        walk_forward_windows=_passing_windows(3),
        parameter_changes=(_change(),),
        data_quality_status="OK",
        missing_required_input_artifacts=False,
    )

    assert decision.status == "candidate"
    assert decision.hard_rejections == ()
    assert all(decision.criteria_results.values())


def _promotion_rules() -> PromotionRulesConfig:
    return PromotionRulesConfig.model_validate(
        {
            "version": "test",
            "owner": "tests",
            "status": "pilot",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "rationale": "test conservative promotion rules",
            "intended_effect": "test",
            "validation_evidence": "unit tests",
            "review_condition": "test review",
            "promotion_status": [
                "rejected",
                "watch",
                "candidate",
                "manual_review_required",
            ],
            "promotion_criteria": {
                "max_drawdown": {"must_be_less_or_equal_to_baseline": True},
                "annualized_return": {"min_relative_improvement": 0.02},
                "sharpe_ratio": {"min_relative_improvement": 0.05},
                "turnover": {"max_relative_increase": 0.20},
                "recent_period": {
                    "must_not_underperform_baseline_by_more_than": 0.03,
                },
                "stability": {"min_passing_windows_ratio": 0.60},
                "explainability": {"all_major_changes_require_reason": True},
            },
            "hard_rejection_rules": [
                "max_drawdown_worse_than_baseline_by_more_than_5_percent",
                "turnover_more_than_50_percent_above_baseline",
                "performance_only_improves_in_one_window",
                "parameter_change_without_explanation",
                "missing_required_input_artifacts",
                "insufficient_data",
                "data_quality_status_red",
            ],
        }
    )


def _metrics(
    *,
    annualized_return: float = 0.10,
    max_drawdown: float = -0.10,
    sharpe_ratio: float = 1.0,
    turnover: float = 1.0,
) -> dict[str, float]:
    return {
        "annualized_return": annualized_return,
        "max_drawdown": max_drawdown,
        "sharpe_ratio": sharpe_ratio,
        "turnover": turnover,
    }


def _passing_windows(count: int) -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "window_id": f"wf-{index:03d}",
            "status": "PASS",
            "baseline_metrics": {"annualized_return": 0.10},
            "candidate_metrics": {"annualized_return": 0.13},
        }
        for index in range(1, count + 1)
    )


def _change() -> ParameterChange:
    return ParameterChange(
        name="trend_momentum",
        baseline=0.25,
        candidate=0.30,
        delta=0.05,
        reason="Improved validation participation across multiple windows.",
        risk="May underperform in choppy markets.",
        source_windows=("wf-001", "wf-002", "wf-003"),
        improved_metrics=("annualized_return", "sharpe_ratio"),
        worsened_metrics=(),
    )
