from __future__ import annotations

from ai_trading_system.dynamic_exposure_cap_diagnostics_review import (
    build_dynamic_downside_protection_diagnostics,
    build_dynamic_false_cost_missed_upside_diagnostics,
)


def test_false_cost_and_missed_upside_labels_are_computed() -> None:
    payload = build_dynamic_false_cost_missed_upside_diagnostics(
        false_cost_report={
            "false_risk_cap_count": 5,
            "false_risk_cap_days": 4,
            "false_risk_cap_cost_proxy": 0.3,
            "strong_upside_after_cap_count": 5,
        },
        missed_upside_report={
            "missed_upside_cost_proxy": 0.2,
            "missed_upside_after_incremental_cap_count": 2,
            "missed_upside_after_redundant_cap_count": 3,
        },
        data_quality_status="PASS",
    )

    assert payload["false_cost_label"] == "FALSE_COST_BLOCKING"
    assert payload["missed_upside_label"] == "MISSED_UPSIDE_HIGH"


def test_downside_protection_weak_and_positive_labels_are_computed() -> None:
    weak = build_dynamic_downside_protection_diagnostics(
        downside_report={
            "downside_protection_proxy": 0.02,
            "incremental_downside_protection_proxy": 0.0,
        },
        data_quality_status="PASS",
    )
    positive = build_dynamic_downside_protection_diagnostics(
        downside_report={
            "downside_protection_proxy": 0.02,
            "incremental_downside_protection_proxy": 0.01,
        },
        data_quality_status="PASS",
    )

    assert weak["downside_protection_label"] == "DOWNSIDE_PROTECTION_WEAK_PROXY"
    assert positive["downside_protection_label"] == "DOWNSIDE_PROTECTION_POSITIVE_PROXY"
