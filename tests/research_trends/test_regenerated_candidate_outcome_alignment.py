from __future__ import annotations

from ai_trading_system.regenerated_candidate_actual_path_validation import (
    classify_outcome_alignment,
)


def _record(direction: str, confidence: float = 0.7):
    return {
        "signal_direction": direction,
        "signal_value": 0.5,
        "signal_confidence": confidence,
    }


def _actual(
    *,
    forward_return: float,
    max_drawdown: float,
    realized_volatility: float = 0.12,
    stress_event: bool = False,
    rolling_volatility_baseline: float = 0.2,
):
    return {
        "validation_eligible": True,
        "forward_return": forward_return,
        "max_drawdown_during_horizon": max_drawdown,
        "realized_volatility": realized_volatility,
        "stress_event": stress_event,
        "rolling_volatility_baseline": rolling_volatility_baseline,
    }


def test_risk_on_positive_path_alignment() -> None:
    alignment = classify_outcome_alignment(
        _record("risk_on"),
        _actual(forward_return=0.03, max_drawdown=-0.01),
    )

    assert alignment["alignment_label"] == "positive_alignment"
    assert alignment["alignment_score"] == 1.0


def test_risk_on_stress_path_false_risk_on() -> None:
    alignment = classify_outcome_alignment(
        _record("risk_on"),
        _actual(forward_return=-0.02, max_drawdown=-0.05, stress_event=True),
    )

    assert alignment["error_type"] == "false_risk_on"


def test_risk_off_drawdown_path_positive_alignment() -> None:
    alignment = classify_outcome_alignment(
        _record("risk_off"),
        _actual(forward_return=-0.01, max_drawdown=-0.04),
    )

    assert alignment["alignment_label"] == "positive_alignment"


def test_risk_off_strong_upside_mild_drawdown_false_risk_off() -> None:
    alignment = classify_outcome_alignment(
        _record("trend_weakening"),
        _actual(forward_return=0.04, max_drawdown=-0.005),
    )

    assert alignment["error_type"] == "false_risk_off"


def test_neutral_quiet_path_positive_alignment() -> None:
    alignment = classify_outcome_alignment(
        _record("neutral"),
        _actual(forward_return=0.002, max_drawdown=-0.004),
    )

    assert alignment["alignment_label"] == "positive_alignment"


def test_volatility_expansion_high_realized_vol_positive_alignment() -> None:
    alignment = classify_outcome_alignment(
        _record("volatility_expansion"),
        _actual(forward_return=0.0, max_drawdown=-0.01, realized_volatility=0.3),
    )

    assert alignment["alignment_label"] == "positive_alignment"


def test_volatility_compression_low_realized_vol_positive_alignment() -> None:
    alignment = classify_outcome_alignment(
        _record("volatility_compression"),
        _actual(forward_return=0.0, max_drawdown=-0.01, realized_volatility=0.1),
    )

    assert alignment["alignment_label"] == "positive_alignment"
