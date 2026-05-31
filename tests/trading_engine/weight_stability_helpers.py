from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from trading_engine.weight_tuning_helpers import BASELINE_WEIGHTS, RESTRICTED_SHADOW_WEIGHTS


def sample_weight_stability_payload(
    *,
    as_of: date = date(2026, 5, 28),
    status: str = "LIMITED",
    candidate_status: str = "no_candidate",
) -> dict[str, Any]:
    candidate_found = candidate_status in {"watch", "shadow_candidate_only"}
    weights = RESTRICTED_SHADOW_WEIGHTS if candidate_found else {}
    return {
        "schema_version": 1,
        "report_type": "weight_stability",
        "as_of": as_of.isoformat(),
        "metadata": {
            "run_id": f"weight-stability-{as_of.isoformat()}",
            "generated_at": datetime(2026, 5, 31, tzinfo=UTC).isoformat(),
            "status": status,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "dry_run": False,
            "market_regime": "ai_after_chatgpt",
            "market_regime_anchor": "2022-11-30",
            "requested_date_range": {
                "start": "2022-12-01",
                "end": as_of.isoformat(),
            },
            "source_task": "TRADING-061",
            "policy_version": "weight-tuning-v0.2-stability",
        },
        "inputs": {
            "baseline_parameters": "config/parameters/production/current.yaml",
            "weight_tuning_config": "config/parameters/weight_tuning_v0_2_stability.yaml",
        },
        "input_artifacts": {},
        "input_context": {
            "previous_failure_root_cause": "weight_search_too_aggressive",
            "previous_top_failure_reason": "cost_drag_too_high",
            "previous_failed_by_turnover": 240,
            "source_artifact": (
                "artifacts/portfolio_turnover_attribution/2026-05-28/"
                "portfolio_turnover_attribution_summary.json"
            ),
        },
        "output_artifacts": {
            "weight_stability_summary_json": (
                "artifacts/weight_stability/2026-05-28/weight_stability_summary.json"
            ),
            "weight_stability_summary_md": (
                "artifacts/weight_stability/2026-05-28/weight_stability_summary.md"
            ),
            "stable_weight_candidates": (
                "artifacts/weight_stability/2026-05-28/stable_weight_candidates.json"
            ),
            "recommended_stable_shadow_weights": (
                "artifacts/weight_stability/2026-05-28/"
                "recommended_stable_shadow_weights.yaml"
                if candidate_found
                else ""
            ),
        },
        "data_quality": {"status": "OK", "data_gate_status": "OK"},
        "freshness": {"status": "OK"},
        "signal_quality": {
            "status": "LIMITED",
            "real_signals": ["trend_momentum", "sector_strength"],
            "proxy_signals": ["macro_liquidity"],
            "fallback_signals": ["earnings_quality", "event_risk"],
        },
        "baseline": {"weights": BASELINE_WEIGHTS, "metrics": {"turnover": 1.7}},
        "stability_constraints": {
            "max_single_signal_delta_from_baseline": 0.10,
            "max_total_l1_distance_from_baseline": 0.25,
            "max_combined_trend_sector_weight": 0.65,
        },
        "turnover_controls": {
            "prefilter_candidates_by_estimated_turnover": True,
            "max_estimated_turnover_relative_increase": 0.25,
        },
        "objective": {"ranking_weights": {"turnover_penalty": 0.15, "cost_drag_penalty": 0.10}},
        "guardrail_policy": {"turnover_relative_increase_limit": 0.30},
        "search_summary": {
            "method": "stable_restricted_grid_search",
            "candidates_generated": 120,
            "candidates_rejected_by_stability": 60,
            "candidates_rejected_by_turnover_prefilter": 20,
            "candidates_backtested": 40,
            "candidates_rejected_by_guardrails": 40 if not candidate_found else 39,
            "candidates_passed_guardrails": 0 if not candidate_found else 1,
        },
        "candidate_ranking": [],
        "recommended_candidate": {
            "candidate_id": "wts-0001" if candidate_found else "",
            "status": candidate_status,
            "weights": weights,
            "guardrail_status": "PASS" if candidate_found else "FAIL",
            "stability": {"stability_status": "PASS"},
            "turnover_prefilter": {"status": "PASS"},
            "reason": (
                "stable shadow candidate for manual review"
                if candidate_found
                else "Stable weight tuning did not find a guardrail-passing candidate."
            ),
        },
        "comparison_to_trading_059": {
            "turnover_failures_reduced": True,
            "cost_drag_improved": True,
            "candidate_found": candidate_found,
        },
        "promotion_impact": {
            "can_support_candidate_promotion": False,
            "reason": "Stable weight tuning remains shadow-only because signal quality is LIMITED.",
        },
        "reader_brief": "Stable weight tuning reduced aggressive candidates.",
        "warnings": [],
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_write_allowed": False,
            "production_config_modified": False,
            "turnover_guardrail_modified": False,
            "cost_model_modified": False,
            "fallback_signals_free_tuned": False,
            "candidate_promotion_triggered": False,
            "broker_action": False,
            "trading_action": False,
        },
    }
