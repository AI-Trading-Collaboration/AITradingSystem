from __future__ import annotations

from ai_trading_system.dynamic_target_exposure_cap_dry_run import (
    build_dynamic_target_strategy_overlap_report,
)


def test_strategy_overlap_labels_incremental_binding() -> None:
    rows = [
        {
            "risk_cap_triggered": True,
            "dynamic_strategy_already_de_risked": False,
            "risk_cap_incremental_binding": True,
            "simulated_cap_binding_active": True,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        {
            "risk_cap_triggered": True,
            "dynamic_strategy_already_de_risked": False,
            "risk_cap_incremental_binding": True,
            "simulated_cap_binding_active": True,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
    ]

    report = build_dynamic_target_strategy_overlap_report(rows)

    assert report["risk_cap_incremental_binding_count"] == 2
    assert report["risk_cap_redundant_binding_count"] == 0
    assert report["overlap_label"] == "RISK_CAP_BINDING_WHEN_DYNAMIC_MISSES_RISK"


def test_strategy_overlap_labels_dynamic_already_handles_risk() -> None:
    rows = [
        {
            "risk_cap_triggered": True,
            "dynamic_strategy_already_de_risked": True,
            "risk_cap_incremental_binding": False,
            "simulated_cap_binding_active": False,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    ]

    report = build_dynamic_target_strategy_overlap_report(rows)

    assert report["dynamic_strategy_derisked_count"] == 1
    assert report["risk_cap_redundant_binding_count"] == 1
    assert report["overlap_label"] == "DYNAMIC_STRATEGY_ALREADY_HANDLES_RISK"
