from __future__ import annotations

from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    CONFIRMATION_CANDIDATE_ID,
    build_confirmation_only_validation_scorecard,
    classify_confirmation_alignment,
)


def _record(direction: str):
    return {
        "scope_narrowed_candidate_id": CONFIRMATION_CANDIDATE_ID,
        "usage_role": "confirmation_only",
        "signal_direction": direction,
        "scope_active": True,
        "target_asset": "QQQ",
        "horizon": "5d",
    }


def _actual(**extra: object):
    return {
        "validation_eligible": True,
        "forward_return": 0.03,
        "max_drawdown_during_horizon": -0.01,
        "stress_event": False,
        "realized_volatility": 0.1,
        **extra,
    }


def _outcome(score: float, *, inactive: bool = False, error_type: str = "no_error"):
    return {
        "scope_narrowed_candidate_id": CONFIRMATION_CANDIDATE_ID,
        "usage_role": "confirmation_only",
        "target_asset": "QQQ",
        "horizon": "5d",
        "confirmation_direction": "positive_confirmation",
        "validation_eligible": True,
        "alignment_score": score,
        "error_type": error_type,
        "actual_forward_return": 0.03 if score >= 0 else -0.04,
        "actual_max_drawdown": -0.01 if score >= 0 else -0.05,
        "actual_realized_volatility": 0.1,
        "scope_active": not inactive,
        "broker_action": "none",
    }


def test_positive_confirmation_alignment() -> None:
    alignment = classify_confirmation_alignment(_record("trend_confirming"), _actual())

    assert alignment["alignment_label"] == "confirmation_aligned"
    assert alignment["alignment_score"] == 1.0


def test_negative_confirmation_alignment() -> None:
    alignment = classify_confirmation_alignment(
        _record("trend_weakening"),
        _actual(forward_return=-0.02, max_drawdown_during_horizon=-0.04, stress_event=True),
    )

    assert alignment["alignment_label"] == "warning_aligned"
    assert alignment["alignment_score"] == 1.0


def test_false_confirmation_and_false_warning_costs() -> None:
    false_confirmation = classify_confirmation_alignment(
        _record("trend_confirming"),
        _actual(forward_return=-0.03, max_drawdown_during_horizon=-0.05),
    )
    false_warning = classify_confirmation_alignment(_record("risk_off"), _actual())

    scorecard = build_confirmation_only_validation_scorecard(
        [
            _outcome(-1.0, error_type=false_confirmation["error_type"]),
            _outcome(-1.0, error_type=false_warning["error_type"]),
        ],
        [_outcome(0.0, inactive=True)],
    )[0]

    assert false_confirmation["error_type"] == "false_confirmation"
    assert false_warning["error_type"] == "false_warning"
    assert scorecard["false_confirmation_count"] == 1
    assert scorecard["false_warning_count"] == 1
    assert scorecard["broker_action"] == "none"


def test_active_vs_inactive_confirmation_delta() -> None:
    scorecard = build_confirmation_only_validation_scorecard(
        [_outcome(1.0), _outcome(0.5)],
        [_outcome(0.0, inactive=True), _outcome(-1.0, inactive=True)],
    )[0]

    assert scorecard["active_vs_inactive_alignment_delta"] == 1.25
