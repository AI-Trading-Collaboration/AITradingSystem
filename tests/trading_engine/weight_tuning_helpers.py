from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from ai_trading_system.trading_engine.parameters.weight_tuning import (
    WEIGHT_TUNING_REPORT_TYPE,
    WEIGHT_TUNING_SCHEMA_VERSION,
)

BASELINE_WEIGHTS: dict[str, float] = {
    "macro_liquidity": 0.20,
    "trend_momentum": 0.25,
    "sector_strength": 0.20,
    "earnings_quality": 0.15,
    "valuation_risk": 0.10,
    "event_risk": 0.10,
}

RESTRICTED_SHADOW_WEIGHTS: dict[str, float] = {
    "macro_liquidity": 0.15,
    "trend_momentum": 0.45,
    "sector_strength": 0.25,
    "earnings_quality": 0.05,
    "valuation_risk": 0.05,
    "event_risk": 0.05,
}


def sample_weight_tuning_payload(
    *,
    as_of: date = date(2026, 5, 28),
    status: str = "NO_CANDIDATE",
    candidate_status: str = "rejected",
) -> dict[str, Any]:
    guardrail_status = "PASS" if candidate_status in {"watch", "shadow_candidate_only"} else "FAIL"
    weights = (
        RESTRICTED_SHADOW_WEIGHTS
        if candidate_status in {"watch", "shadow_candidate_only"}
        else BASELINE_WEIGHTS
    )
    reason = (
        "restricted candidate passed shadow-only guardrails"
        if guardrail_status == "PASS"
        else "no candidate passed restricted guardrails"
    )
    return {
        "schema_version": WEIGHT_TUNING_SCHEMA_VERSION,
        "report_type": WEIGHT_TUNING_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "metadata": {
            "run_id": f"weight-tuning-{as_of.isoformat()}",
            "generated_at": datetime(2026, 5, 30, tzinfo=UTC).isoformat(),
            "status": status,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": False,
            "market_regime": "ai_after_chatgpt",
            "requested_date_range": {
                "start": "2022-12-01",
                "end": as_of.isoformat(),
            },
            "policy_version": "weight-tuning-v0.1",
        },
        "inputs": {
            "baseline_parameters": "config/parameters/production/current.yaml",
            "signal_snapshot": "artifacts/signal_snapshots/2026-05-28/signal_snapshot.json",
            "backtest_input_manifest": (
                "artifacts/backtest_inputs/2026-05-28/backtest_input_manifest.json"
            ),
            "data_quality_report": "artifacts/reports/data_quality_2026-05-28.md",
            "market_data_freshness": (
                "artifacts/data_freshness/2026-05-28/market_data_freshness_summary.json"
            ),
            "portfolio_profile": "lower_rebalance_threshold_2pct",
        },
        "input_artifacts": {},
        "output_artifacts": {
            "weight_tuning_summary_json": (
                "artifacts/weight_tuning/2026-05-28/weight_tuning_summary.json"
            ),
            "weight_tuning_summary_md": (
                "artifacts/weight_tuning/2026-05-28/weight_tuning_summary.md"
            ),
            "recommended_shadow_weights": (
                "artifacts/weight_tuning/2026-05-28/recommended_shadow_weights.yaml"
            ),
            "weight_tuning_candidates": (
                "artifacts/weight_tuning/2026-05-28/weight_tuning_candidates.json"
            ),
        },
        "data_quality": {
            "status": "OK",
            "data_gate_status": "OK",
            "quality_report_path": "artifacts/reports/data_quality_2026-05-28.md",
        },
        "freshness": {"status": "OK"},
        "signal_quality": {
            "status": "LIMITED",
            "real_signals": ["trend_momentum", "sector_strength"],
            "proxy_signals": ["macro_liquidity"],
            "fallback_signals": ["earnings_quality", "event_risk"],
        },
        "tuning_scope": {
            "tunable_weights": ["macro_liquidity", "trend_momentum", "sector_strength"],
            "capped_weights": ["valuation_risk"],
            "fixed_weights": ["earnings_quality", "event_risk"],
        },
        "constraints": {
            "total_weight_sum": 1.0,
            "forbid_free_fallback_weight_tuning": True,
        },
        "objective": {
            "ranking_weights": {
                "sharpe_improvement": 0.35,
                "max_drawdown_improvement": 0.25,
            },
        },
        "guardrail_policy": {"min_validation_windows": 3},
        "baseline": {
            "weights": BASELINE_WEIGHTS,
            "metrics": {
                "annualized_return": 0.16,
                "sharpe_ratio": 1.10,
                "max_drawdown": -0.12,
                "turnover": 0.40,
            },
        },
        "search": {
            "method": "restricted_grid_search",
            "candidates_evaluated": 240,
            "candidates_rejected_by_constraints": 390,
            "candidates_rejected_by_guardrails": 240,
            "candidates_generated": 240,
            "selected_signals": ["macro_liquidity", "trend_momentum", "sector_strength"],
        },
        "candidate_ranking": [
            {
                "rank": 1,
                "candidate_id": "wt-0001",
                "status": candidate_status,
                "guardrail_status": guardrail_status,
                "objective_score": -0.01 if guardrail_status == "FAIL" else 0.05,
                "relative_metrics": {
                    "sharpe_delta": -0.03 if guardrail_status == "FAIL" else 0.08,
                },
                "weights": weights,
                "reason": reason,
            }
        ],
        "recommended_candidate": {
            "candidate_id": "wt-0001",
            "status": candidate_status,
            "weights": weights,
            "metrics": {
                "annualized_return": 0.165,
                "sharpe_ratio": 1.02 if guardrail_status == "FAIL" else 1.20,
                "max_drawdown": -0.12,
                "turnover": 0.42,
            },
            "relative_metrics": {
                "annualized_return_delta": 0.005,
                "sharpe_delta": -0.08 if guardrail_status == "FAIL" else 0.10,
                "max_drawdown_delta": 0.0,
                "turnover_relative_increase": 0.05,
                "non_worse_walk_forward_ratio": 1.0,
            },
            "objective_breakdown": {
                "sharpe_improvement_score": -0.08 if guardrail_status == "FAIL" else 0.10,
                "drawdown_improvement_score": 0.0,
                "return_improvement_score": 0.005,
                "signal_transmission_score": 0.02,
                "turnover_penalty_score": -0.005,
                "objective_score": -0.01 if guardrail_status == "FAIL" else 0.05,
            },
            "guardrail_status": guardrail_status,
            "guardrails": {
                "status": guardrail_status,
                "hard_rejections": [] if guardrail_status == "PASS" else ["objective_score"],
            },
            "reason": reason,
            "walk_forward_windows": [],
        },
        "walk_forward": {"windows": [], "non_worse_window_ratio": 1.0},
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": "shadow-only advisory candidate; production parameters unchanged",
        },
        "warnings": [],
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "fallback_signals_free_tuned": False,
            "production_write_allowed": False,
            "production_config_modified": False,
            "candidate_promotion_triggered": False,
            "broker_action": False,
            "trading_action": False,
        },
    }
