from __future__ import annotations

from ai_trading_system.dynamic_exposure_cap_diagnostics_review import (
    build_static_vs_dynamic_exposure_cap_evidence_comparison,
)


def test_static_vs_dynamic_comparison_deltas_and_reduces_overbinding_label() -> None:
    payload = build_static_vs_dynamic_exposure_cap_evidence_comparison(
        static_diagnostics={
            "cap_binding": {"cap_binding_rate": 0.4},
            "return_drawdown": {"return_proxy_delta": -0.1, "drawdown_proxy_delta": 0.03},
            "false_cost": {"false_risk_cap_cost_proxy": 0.1},
            "downside": {"downside_protection_proxy": 0.05},
        },
        static_dry_run={},
        dynamic_cap_binding={"cap_binding_rate": 0.2},
        dynamic_return_drawdown={"return_proxy_delta": -0.08, "drawdown_proxy_delta": 0.04},
        dynamic_false_cost={"false_risk_cap_cost_proxy": 0.05},
        dynamic_downside={"downside_protection_proxy": 0.06},
        dynamic_strategy_overlap={"overlap_label": "RISK_CAP_INCREMENTAL_TO_DYNAMIC_STRATEGY"},
        data_quality_status="PASS",
    )

    assert payload["cap_binding_rate_delta"] == -0.2
    assert payload["return_cost_delta"] == 0.02
    assert payload["drawdown_protection_delta"] == 0.01
    assert payload["comparison_label"] == "DYNAMIC_BASELINE_REDUCES_OVERBINDING"


def test_static_vs_dynamic_comparison_false_cost_worse_label() -> None:
    payload = build_static_vs_dynamic_exposure_cap_evidence_comparison(
        static_diagnostics={
            "cap_binding": {"cap_binding_rate": 0.3},
            "return_drawdown": {"return_proxy_delta": -0.1, "drawdown_proxy_delta": 0.06},
            "false_cost": {"false_risk_cap_cost_proxy": 0.05},
            "downside": {"downside_protection_proxy": 0.05},
        },
        static_dry_run={},
        dynamic_cap_binding={"cap_binding_rate": 0.45},
        dynamic_return_drawdown={"return_proxy_delta": -0.2, "drawdown_proxy_delta": 0.03},
        dynamic_false_cost={"false_risk_cap_cost_proxy": 0.2},
        dynamic_downside={"downside_protection_proxy": 0.04},
        dynamic_strategy_overlap={"overlap_label": "RISK_CAP_INCREMENTAL_TO_DYNAMIC_STRATEGY"},
        data_quality_status="PASS",
    )

    assert payload["false_cost_delta"] == 0.15
    assert payload["comparison_label"] == "DYNAMIC_BASELINE_SHOWS_FALSE_COST_WORSE"
