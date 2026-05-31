from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.parameters.promotion_rules import PromotionDecision
from ai_trading_system.trading_engine.parameters.weight_stability import (
    calculate_weight_stability,
    estimate_turnover_prefilter,
    load_weight_stability_config,
    write_weight_stability_summary,
)
from ai_trading_system.trading_engine.parameters.weight_tuning import (
    _objective_breakdown,
    generate_restricted_grid_candidate_diagnostics,
)
from trading_engine.weight_stability_helpers import sample_weight_stability_payload
from trading_engine.weight_tuning_helpers import BASELINE_WEIGHTS


def test_loads_stability_config_and_preserves_safety() -> None:
    config = load_weight_stability_config()

    assert config["metadata"]["production_effect"] == "none"
    assert config["metadata"]["manual_review_required"] is True
    assert config["metadata"]["auto_promotion"] is False
    assert config["safety"]["forbid_free_fallback_weight_tuning"] is True
    assert config["guardrails"]["turnover_relative_increase_limit"] == pytest.approx(0.30)


def test_calculates_l1_distance_and_rejects_single_signal_delta() -> None:
    config = load_weight_stability_config()
    weights = {**BASELINE_WEIGHTS, "trend_momentum": 0.40, "macro_liquidity": 0.05}

    stability = calculate_weight_stability(weights, BASELINE_WEIGHTS, config)

    assert stability["l1_distance_from_baseline"] == pytest.approx(0.30)
    assert stability["max_single_signal_delta"] == pytest.approx(0.15)
    assert stability["stability_status"] == "FAIL"
    assert "single_signal_delta_too_high" in stability["rejection_reasons"]


def test_rejects_candidate_by_total_l1_and_trend_sector_combined_weight() -> None:
    config = load_weight_stability_config()
    weights = {
        **BASELINE_WEIGHTS,
        "trend_momentum": 0.45,
        "sector_strength": 0.30,
        "macro_liquidity": 0.05,
    }

    stability = calculate_weight_stability(weights, BASELINE_WEIGHTS, config)

    assert stability["l1_distance_from_baseline"] > 0.25
    assert stability["trend_sector_combined_weight"] > 0.65
    assert "l1_distance_too_high" in stability["rejection_reasons"]
    assert "trend_sector_combined_weight_too_high" in stability["rejection_reasons"]


def test_turnover_prefilter_uses_baseline_distance_proxy() -> None:
    config = load_weight_stability_config()
    weights = {
        **BASELINE_WEIGHTS,
        "trend_momentum": 0.45,
        "sector_strength": 0.30,
        "macro_liquidity": 0.05,
    }

    prefilter = estimate_turnover_prefilter(weights, BASELINE_WEIGHTS, config)

    assert prefilter["estimated_turnover_relative_increase"] > 0.25
    assert prefilter["status"] == "FAIL"
    assert prefilter["rejection_reasons"] == ["estimated_turnover_too_high"]


def test_objective_includes_turnover_and_cost_drag_penalties() -> None:
    baseline = SimpleNamespace(
        metrics={
            "sharpe_ratio": 1.0,
            "max_drawdown": -0.10,
            "annualized_return": 0.10,
            "turnover": 1.0,
        },
        transaction_cost_drag=0.01,
        actual=pd.DataFrame(),
    )
    candidate = SimpleNamespace(
        metrics={
            "sharpe_ratio": 1.1,
            "max_drawdown": -0.10,
            "annualized_return": 0.12,
            "turnover": 1.2,
        },
        transaction_cost_drag=0.03,
        actual=pd.DataFrame(),
    )

    breakdown = _objective_breakdown(
        baseline,
        candidate,
        {
            "sharpe_improvement": 0.30,
            "max_drawdown_improvement": 0.25,
            "annualized_return_improvement": 0.20,
            "turnover_penalty": 0.15,
            "cost_drag_penalty": 0.10,
        },
    )

    assert breakdown["turnover_penalty_score"] == pytest.approx(0.20)
    assert breakdown["cost_drag_penalty_score"] == pytest.approx(0.02)
    assert breakdown["objective_score"] == pytest.approx(0.002)


def test_stable_candidate_generation_reports_stability_and_prefilter_counts() -> None:
    config = load_weight_stability_config()

    generation = generate_restricted_grid_candidate_diagnostics(config, BASELINE_WEIGHTS)

    assert generation["candidates_generated"] > 0
    assert generation["candidates_rejected_by_stability"] > 0
    assert generation["candidates_rejected_by_turnover_prefilter"] >= 0
    first = generation["candidate_diagnostics"][0]
    assert "stability" in first
    assert "turnover_prefilter" in first


def test_shadow_backtest_supporting_artifacts_reference_weight_stability(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    as_of = date(2026, 5, 28)
    monkeypatch.setattr(shadow_backtest, "PROJECT_ROOT", tmp_path)
    artifact_dir = tmp_path / "artifacts" / "weight_stability" / as_of.isoformat()
    write_weight_stability_summary(
        sample_weight_stability_payload(as_of=as_of),
        artifact_dir / "weight_stability_summary.json",
        artifact_dir / "weight_stability_summary.md",
    )
    decision = PromotionDecision(
        status="rejected",
        reason="signal quality limited",
        hard_rejections=("signal_quality_limited",),
        manual_review_items=(),
        criteria_results={},
    )

    payload = shadow_backtest._promotion_decision_payload(
        decision,
        as_of=as_of,
        backtest_mode="full_signal_backtest_limited",
    )

    assert payload["status"] == "rejected"
    assert "weight_stability" in payload["supporting_artifacts"]
    assert payload["weight_stability_status"] == "LIMITED"
