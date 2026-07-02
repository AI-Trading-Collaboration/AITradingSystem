from __future__ import annotations

from ai_trading_system.dynamic_target_exposure_cap_dry_run import (
    build_dynamic_target_cap_binding_day_matrix,
    build_dynamic_target_cap_vs_no_cap_comparison,
)


def _row(day: str, *, binding: bool, no_cap: float, capped: float) -> dict:
    return {
        "date": day,
        "target_asset": "QQQ",
        "simulated_cap_binding_active": binding,
        "simulated_exposure_delta": 0.2 if binding else 0.0,
        "simulated_turnover_proxy": 0.2 if binding else 0.0,
        "dynamic_no_cap_return_contribution_proxy": no_cap,
        "dynamic_capped_return_contribution_proxy": capped,
        "asset_return": 0.1 if no_cap > 0 else -0.1,
        "manual_review_required": binding,
        "risk_cap_triggered": binding,
        "risk_cap_intensity": "high" if binding else "none",
        "dynamic_risk_asset_exposure": 0.8,
        "simulated_final_exposure_after_cap": 0.6,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def test_dynamic_cap_vs_no_cap_comparison_metrics() -> None:
    rows = [
        _row("2023-01-09", binding=True, no_cap=0.08, capped=0.05),
        _row("2023-01-10", binding=False, no_cap=-0.04, capped=-0.04),
    ]

    comparison = build_dynamic_target_cap_vs_no_cap_comparison(
        dry_run_rows=rows,
        data_quality_status="PASS",
    )
    binding = build_dynamic_target_cap_binding_day_matrix(rows)

    assert comparison["cap_binding_days"] == 1
    assert comparison["cap_binding_rate"] == 0.5
    assert comparison["return_proxy_delta"] == -0.03
    assert comparison["false_risk_cap_cost_proxy"] == 0.03
    assert binding[0]["cap_binding_active_any_asset"] is True
    assert binding[0]["cap_binding_asset_count"] == 1
