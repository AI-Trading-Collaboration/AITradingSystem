from __future__ import annotations

from ai_trading_system.exposure_cap_diagnostics_review import (
    build_turnover_cooldown_diagnostics,
)


def test_turnover_cooldown_diagnostics_marks_high_and_costly() -> None:
    diagnostics = build_turnover_cooldown_diagnostics(
        turnover_report={
            "turnover_proxy_total": 2.0,
            "average_daily_turnover_proxy": 0.5,
            "turnover_spike_days": ["2023-01-04"],
            "turnover_proxy_from_cap_entry": 1.1,
            "turnover_proxy_from_cap_exit": 0.8,
            "turnover_proxy_from_cooldown": 0.1,
            "turnover_impact_label": "TURNOVER_IMPACT_INCONCLUSIVE",
        },
        cooldown_report={
            "cooldown_trigger_count": 2,
            "cooldown_active_days": 5,
            "average_cooldown_length": 2.5,
            "cooldown_prevented_reentry_days": 3,
            "cooldown_return_proxy_delta": -0.04,
            "cooldown_false_cost_proxy": 0.06,
            "cooldown_impact_label": "COOLDOWN_COSTLY_PROXY",
        },
        data_quality_status="PASS",
    )

    assert diagnostics["turnover_proxy_total"] == 2.0
    assert diagnostics["turnover_proxy_average"] == 0.5
    assert diagnostics["turnover_spike_days"] == ["2023-01-04"]
    assert diagnostics["cooldown_return_proxy_delta"] == -0.04
    assert diagnostics["turnover_cooldown_label"] == "TURNOVER_HIGH_AND_COOLDOWN_COSTLY"


def test_turnover_cooldown_diagnostics_marks_neutral() -> None:
    diagnostics = build_turnover_cooldown_diagnostics(
        turnover_report={"turnover_proxy_total": 0.0},
        cooldown_report={"cooldown_return_proxy_delta": 0.0},
        data_quality_status="PASS",
    )

    assert diagnostics["turnover_cooldown_label"] == "COOLDOWN_NEUTRAL"
