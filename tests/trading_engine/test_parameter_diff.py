from __future__ import annotations

from pydantic import ValidationError

from ai_trading_system.trading_engine.parameters.parameter_diff import diff_parameters
from ai_trading_system.trading_engine.parameters.parameter_schema import ProductionParameters


def test_production_parameter_schema_validates_weight_sum() -> None:
    payload = _production_payload(weights={"macro_liquidity": 0.7, "trend_momentum": 0.2})

    try:
        ProductionParameters.model_validate(payload)
    except ValidationError as exc:
        assert "weights must sum to 1.0" in str(exc)
    else:  # pragma: no cover - assertion clarity
        raise AssertionError("expected invalid weight sum to fail")


def test_parameter_diff_records_direction_reason_and_risk() -> None:
    baseline = ProductionParameters.model_validate(_production_payload())

    changes = diff_parameters(
        baseline,
        {
            "macro_liquidity": 0.20,
            "trend_momentum": 0.30,
            "sector_strength": 0.15,
            "earnings_quality": 0.15,
            "valuation_risk": 0.10,
            "event_risk": 0.10,
        },
        reasons={"trend_momentum": "Improved validation trend participation."},
        source_windows={"trend_momentum": ("wf-001", "wf-002")},
        improved_metrics={"trend_momentum": ("annualized_return", "sharpe_ratio")},
        worsened_metrics={"sector_strength": ("turnover",)},
    )

    assert [change.name for change in changes] == ["sector_strength", "trend_momentum"]
    trend = next(change for change in changes if change.name == "trend_momentum")
    assert trend.baseline == 0.25
    assert trend.candidate == 0.30
    assert round(trend.delta, 2) == 0.05
    assert trend.reason == "Improved validation trend participation."
    assert trend.source_windows == ("wf-001", "wf-002")
    assert trend.improved_metrics == ("annualized_return", "sharpe_ratio")
    assert "crowded momentum" in trend.risk


def _production_payload(
    *,
    weights: dict[str, float] | None = None,
) -> dict[str, object]:
    return {
        "version": "production-test",
        "created_at": "2026-05-29T00:00:00+09:00",
        "asset_universe": {"core": ["QQQ", "NVDA", "CASH"]},
        "decision_frequency": "daily",
        "rebalance_frequency": "weekly",
        "risk_profile": "balanced_growth",
        "weights": weights
        or {
            "macro_liquidity": 0.20,
            "trend_momentum": 0.25,
            "sector_strength": 0.20,
            "earnings_quality": 0.15,
            "valuation_risk": 0.10,
            "event_risk": 0.10,
        },
        "hard_gates": {},
        "position_limits": {
            "max_single_asset_weight": 0.30,
            "max_sector_weight": 0.60,
            "min_cash_weight": 0.05,
        },
    }
