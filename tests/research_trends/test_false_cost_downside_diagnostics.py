from __future__ import annotations

from ai_trading_system.exposure_cap_diagnostics_review import (
    build_downside_protection_diagnostics,
    build_false_cost_missed_upside_diagnostics,
)


def test_false_cost_and_missed_upside_diagnostics_marks_blocking() -> None:
    diagnostics = build_false_cost_missed_upside_diagnostics(
        false_cost_report={
            "false_risk_cap_count": 12,
            "false_risk_cap_days": 6,
            "false_risk_cap_cost_proxy": 0.40,
            "missed_upside_cost_proxy": 0.40,
            "strong_upside_after_cap_count": 12,
            "mild_drawdown_after_cap_count": 2,
            "false_risk_cap_cost_label": "FALSE_COST_INCONCLUSIVE",
        },
        missed_upside_report={"missed_upside_cost_proxy": 0.40},
        data_quality_status="PASS",
    )

    assert diagnostics["false_risk_cap_count"] == 12
    assert diagnostics["false_cost_label"] == "FALSE_COST_BLOCKING"
    assert diagnostics["missed_upside_label"] == "MISSED_UPSIDE_BLOCKING"


def test_downside_protection_diagnostics_marks_positive_and_negative() -> None:
    positive = build_downside_protection_diagnostics(
        downside_report={
            "risk_cap_trigger_count": 3,
            "post_trigger_drawdown_capture_count": 2,
            "post_trigger_stress_capture_count": 2,
            "downside_tail_capture_count": 2,
            "downside_protection_proxy": 0.10,
            "drawdown_reduction_proxy": 0.10,
            "stress_window_exposure_reduction": 0.30,
        },
        data_quality_status="PASS",
    )
    negative = build_downside_protection_diagnostics(
        downside_report={"downside_protection_proxy": -0.02},
        data_quality_status="PASS",
    )

    assert positive["downside_protection_label"] == "DOWNSIDE_PROTECTION_POSITIVE_PROXY"
    assert negative["downside_protection_label"] == "DOWNSIDE_PROTECTION_NEGATIVE_PROXY"
