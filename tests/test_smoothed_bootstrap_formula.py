from __future__ import annotations

from datetime import date

import pandas as pd

from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_bootstrap as bootstrap,
)


def test_smoothed_forward_portfolio_math_preserves_cash_and_missing_values() -> None:
    pivot = pd.DataFrame(
        {"QQQ": [100.0, 120.0, 90.0]},
        index=pd.to_datetime(["2026-01-05", "2026-01-06", "2026-01-07"]),
    )
    weights = {"CASH": 0.5, "QQQ": 0.5}

    assert bootstrap._portfolio_return(
        weights, pivot, date(2026, 1, 5), date(2026, 1, 7)
    ) == -0.05
    assert bootstrap._portfolio_drawdown(
        weights, pivot, date(2026, 1, 5), date(2026, 1, 7)
    ) == -0.125
    assert (
        bootstrap._portfolio_return(weights, pivot, date(2026, 1, 5), date(2026, 1, 8))
        is None
    )
    assert (
        bootstrap._portfolio_drawdown(
            {"SMH": 1.0}, pivot, date(2026, 1, 5), date(2026, 1, 7)
        )
        is None
    )


def test_smoothed_forward_classification_uses_dynamic_methods_and_named_regimes() -> None:
    classes, confidence = bootstrap._classes(
        {
            "regime_context": "unknown",
            "candidate_method": "candidate_x",
            "baseline_method": "baseline_y",
            "method_returns": {"candidate_x": 0.03, "baseline_y": 0.01},
        }
    )
    assert classes == ["strong_recovery"]
    assert confidence == "LOW"

    classes, confidence = bootstrap._classes(
        {
            "regime_context": "tech_drawdown",
            "candidate_method": "candidate_x",
            "baseline_method": "baseline_y",
            "method_returns": {},
        }
    )
    assert classes == ["fast_regime_change"]
    assert confidence == "MEDIUM"

    assert bootstrap._classes(
        {
            "regime_context": "unknown",
            "candidate_method": "candidate_x",
            "baseline_method": "baseline_y",
            "method_returns": {"candidate_x": None, "baseline_y": 0.0},
        }
    ) == (["unknown"], "LOW")
