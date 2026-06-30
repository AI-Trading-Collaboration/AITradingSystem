from __future__ import annotations

from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    RISK_CAP_CANDIDATE_ID,
    build_risk_cap_only_validation_scorecard,
    classify_risk_cap_alignment,
    scope_narrowed_prediction_outcome_row,
)


def _record(direction: str = "volatility_expansion"):
    return {
        "scope_narrowed_candidate_id": RISK_CAP_CANDIDATE_ID,
        "usage_role": "risk_cap_only",
        "signal_direction": direction,
        "scope_active": True,
        "target_asset": "SPY",
        "horizon": "5d",
    }


def _actual(**extra: object):
    return {
        "validation_eligible": True,
        "actual_path_status": "complete",
        "forward_return": -0.01,
        "max_drawdown_during_horizon": -0.04,
        "max_runup_during_horizon": 0.01,
        "realized_volatility": 0.3,
        "downside_tail_event": True,
        "upside_breakout_event": False,
        "stress_event": True,
        "data_quality_warning": False,
        **extra,
    }


def _outcome(score: float, *, error_type: str = "no_error"):
    return {
        "scope_narrowed_candidate_id": RISK_CAP_CANDIDATE_ID,
        "usage_role": "risk_cap_only",
        "target_asset": "SPY",
        "horizon": "5d",
        "signal_direction": "volatility_expansion",
        "validation_eligible": True,
        "alignment_label": "risk_cap_aligned" if score > 0 else "false_risk_cap",
        "alignment_score": score,
        "error_type": error_type,
        "actual_forward_return": -0.01 if score > 0 else 0.04,
        "actual_max_drawdown": -0.04 if score > 0 else -0.01,
        "actual_realized_volatility": 0.3 if score > 0 else 0.1,
        "stress_event": score > 0,
        "downside_tail_event": score > 0,
    }


def test_risk_cap_capture_stress_and_tail_alignment() -> None:
    alignment = classify_risk_cap_alignment(_record(), _actual())

    assert alignment["alignment_label"] == "risk_cap_aligned"
    assert alignment["alignment_score"] == 1.0


def test_false_risk_cap_and_missed_upside_cost() -> None:
    alignment = classify_risk_cap_alignment(
        _record(),
        _actual(
            forward_return=0.04,
            max_drawdown_during_horizon=-0.01,
            realized_volatility=0.1,
            downside_tail_event=False,
            stress_event=False,
        ),
    )
    scorecard = build_risk_cap_only_validation_scorecard(
        [_outcome(-1.0, error_type=alignment["error_type"])],
        [],
    )[0]

    assert alignment["error_type"] == "false_risk_cap"
    assert scorecard["false_risk_cap_count"] == 1
    assert scorecard["missed_upside_cost"] == 0.04
    assert scorecard["broker_action"] == "none"


def test_scorecard_capture_rates() -> None:
    scorecard = build_risk_cap_only_validation_scorecard(
        [_outcome(1.0), _outcome(1.0), _outcome(-1.0, error_type="false_risk_cap")],
        [],
    )[0]

    assert scorecard["risk_cap_capture_rate"] == 0.666667
    assert scorecard["stress_event_capture_rate"] == 0.666667
    assert scorecard["downside_tail_capture_rate"] == 0.666667


def test_risk_on_records_do_not_become_active_risk_cap_validation() -> None:
    outcome = scope_narrowed_prediction_outcome_row(
        _record("risk_on"),
        _actual(),
        {
            "alignment_label": "risk_cap_inactive_reference",
            "alignment_score": 0.0,
            "error_type": "no_error",
        },
    )

    assert outcome["scope_active"] is True
    assert outcome["active_validation_record"] is False
