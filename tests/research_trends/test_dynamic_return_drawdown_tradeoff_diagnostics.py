from __future__ import annotations

from ai_trading_system.dynamic_exposure_cap_diagnostics_review import (
    build_dynamic_return_drawdown_tradeoff_diagnostics,
)


def test_return_drawdown_diagnostics_reads_deltas_and_costly_label() -> None:
    payload = build_dynamic_return_drawdown_tradeoff_diagnostics(
        comparison={
            "dynamic_no_cap_return_proxy": 0.2,
            "dynamic_capped_return_proxy": 0.1,
            "return_proxy_delta": -0.1,
            "dynamic_no_cap_max_drawdown_proxy": -0.2,
            "dynamic_capped_max_drawdown_proxy": -0.16,
            "drawdown_proxy_delta": 0.04,
        },
        return_drawdown_report={},
        data_quality_status="PASS",
    )

    assert payload["return_proxy_delta"] == -0.1
    assert payload["drawdown_proxy_delta"] == 0.04
    assert payload["return_drawdown_tradeoff_label"] == (
        "DRAWDOWN_PROTECTION_WEAK_RETURN_COST_HIGH"
    )


def test_return_drawdown_diagnostics_handles_no_material_difference() -> None:
    payload = build_dynamic_return_drawdown_tradeoff_diagnostics(
        comparison={"return_proxy_delta": 0.0, "drawdown_proxy_delta": 0.0},
        return_drawdown_report={},
        data_quality_status="PASS",
    )

    assert payload["return_drawdown_tradeoff_label"] == "NO_MATERIAL_DIFFERENCE"
