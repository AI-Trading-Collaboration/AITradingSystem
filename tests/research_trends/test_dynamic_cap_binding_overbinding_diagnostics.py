from __future__ import annotations

from ai_trading_system.dynamic_exposure_cap_diagnostics_review import (
    build_dynamic_cap_binding_diagnostics,
    build_dynamic_overbinding_diagnostics,
)


def test_cap_binding_rate_label_and_clusters_are_computed() -> None:
    dry_rows = [
        {"date": "2023-01-03", "target_asset": "QQQ", "simulated_cap_binding_active": True},
        {"date": "2023-01-04", "target_asset": "QQQ", "simulated_cap_binding_active": True},
        {"date": "2023-01-06", "target_asset": "SPY", "simulated_cap_binding_active": True},
    ]
    binding_rows = [
        {"date": "2023-01-03", "cap_binding_active_any_asset": True, "cap_binding_assets": ["QQQ"]},
        {"date": "2023-01-04", "cap_binding_active_any_asset": True, "cap_binding_assets": ["QQQ"]},
        {"date": "2023-01-05", "cap_binding_active_any_asset": False, "cap_binding_assets": []},
        {"date": "2023-01-06", "cap_binding_active_any_asset": True, "cap_binding_assets": ["SPY"]},
    ]

    payload = build_dynamic_cap_binding_diagnostics(
        dry_run_rows=dry_rows,
        binding_rows=binding_rows,
        comparison={"record_count": 6, "cap_binding_days": 3, "cap_binding_rate": 0.5},
        data_quality_status="PASS_WITH_WARNINGS",
    )

    assert payload["cap_binding_rate"] == 0.5
    assert payload["cap_binding_frequency_label"] == "EXCESSIVE_BINDING_FREQUENCY"
    assert payload["cap_binding_cluster_count"] == 2
    assert payload["max_cap_binding_cluster_length"] == 2


def test_overbinding_high_and_blocking_labels_are_computed() -> None:
    base = {
        "cap_binding": {"cap_binding_rate": 0.45},
        "false_cost": {"false_cost_label": "FALSE_COST_HIGH", "false_risk_cap_cost_proxy": 0.1},
        "downside": {"downside_protection_proxy": 0.02},
        "strategy_overlap": {
            "incremental_binding_rate": 0.2,
            "redundant_binding_rate": 0.6,
            "dynamic_strategy_derisked_count": 4,
            "risk_cap_incremental_binding_count": 2,
            "risk_cap_redundant_binding_count": 6,
        },
    }

    high = build_dynamic_overbinding_diagnostics(
        dry_run_rows=[],
        return_drawdown={"return_proxy_delta": -0.08, "drawdown_proxy_delta": 0.08},
        data_quality_status="PASS",
        **base,
    )
    blocking = build_dynamic_overbinding_diagnostics(
        dry_run_rows=[],
        return_drawdown={"return_proxy_delta": -0.08, "drawdown_proxy_delta": 0.02},
        data_quality_status="PASS",
        **base,
    )

    assert high["overbinding_label"] == "OVERBINDING_HIGH"
    assert blocking["overbinding_label"] == "OVERBINDING_BLOCKING"
