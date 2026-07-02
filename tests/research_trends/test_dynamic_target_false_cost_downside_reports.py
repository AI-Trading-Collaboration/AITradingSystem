from __future__ import annotations

from ai_trading_system.dynamic_target_exposure_cap_dry_run import (
    build_dynamic_target_downside_protection_proxy_report,
    build_dynamic_target_false_risk_cap_cost_report,
    build_dynamic_target_missed_upside_cost_report,
)


def test_false_cost_and_downside_reports() -> None:
    rows = [
        {
            "date": "2023-01-09",
            "simulated_cap_binding_active": True,
            "risk_cap_incremental_binding": True,
            "risk_cap_triggered": True,
            "asset_return": 0.1,
            "dynamic_no_cap_return_contribution_proxy": 0.08,
            "dynamic_capped_return_contribution_proxy": 0.05,
            "simulated_exposure_delta": 0.3,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        {
            "date": "2023-01-10",
            "simulated_cap_binding_active": True,
            "risk_cap_incremental_binding": True,
            "risk_cap_triggered": True,
            "asset_return": -0.1,
            "dynamic_no_cap_return_contribution_proxy": -0.08,
            "dynamic_capped_return_contribution_proxy": -0.05,
            "simulated_exposure_delta": 0.3,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
    ]

    false_cost = build_dynamic_target_false_risk_cap_cost_report(rows)
    missed = build_dynamic_target_missed_upside_cost_report(rows)
    downside = build_dynamic_target_downside_protection_proxy_report(rows)

    assert false_cost["false_risk_cap_cost_proxy"] == 0.03
    assert false_cost["false_risk_cap_cost_label"] == "FALSE_COST_INCONCLUSIVE"
    assert missed["missed_upside_after_incremental_cap_count"] == 1
    assert downside["downside_protection_proxy"] == 0.03
    assert downside["downside_protection_label"] == "DOWNSIDE_PROTECTION_POSITIVE_PROXY"
