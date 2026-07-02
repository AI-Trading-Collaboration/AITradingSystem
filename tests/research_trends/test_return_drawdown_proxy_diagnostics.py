from __future__ import annotations

from ai_trading_system.exposure_cap_diagnostics_review import (
    build_return_drawdown_proxy_diagnostics,
)


def test_return_drawdown_tradeoff_marks_costly_drawdown_improvement() -> None:
    diagnostics = build_return_drawdown_proxy_diagnostics(
        comparison={
            "no_cap_return_proxy": 0.20,
            "capped_return_proxy": 0.10,
            "return_proxy_delta": -0.10,
            "no_cap_max_drawdown_proxy": -0.20,
            "capped_max_drawdown_proxy": -0.10,
            "drawdown_proxy_delta": 0.10,
        },
        data_quality_status="PASS",
    )

    assert diagnostics["return_proxy_delta"] == -0.10
    assert diagnostics["drawdown_proxy_delta"] == 0.10
    assert diagnostics["return_proxy_materiality_label"] == "RETURN_PROXY_COSTLY"
    assert diagnostics["drawdown_proxy_materiality_label"] == "DRAWDOWN_PROXY_IMPROVED"
    assert diagnostics["return_drawdown_tradeoff_label"] == (
        "DRAWDOWN_IMPROVED_RETURN_COSTLY"
    )


def test_return_drawdown_tradeoff_marks_no_material_difference() -> None:
    diagnostics = build_return_drawdown_proxy_diagnostics(
        comparison={
            "return_proxy_delta": 0.001,
            "drawdown_proxy_delta": 0.001,
        },
        data_quality_status="PASS",
    )

    assert diagnostics["return_drawdown_tradeoff_label"] == "NO_MATERIAL_DIFFERENCE"


def test_return_drawdown_tradeoff_marks_costly_without_drawdown_improvement() -> None:
    diagnostics = build_return_drawdown_proxy_diagnostics(
        comparison={
            "return_proxy_delta": -0.05,
            "drawdown_proxy_delta": -0.02,
        },
        data_quality_status="PASS",
    )

    assert diagnostics["return_drawdown_tradeoff_label"] == (
        "DRAWDOWN_NOT_IMPROVED_RETURN_COSTLY"
    )
