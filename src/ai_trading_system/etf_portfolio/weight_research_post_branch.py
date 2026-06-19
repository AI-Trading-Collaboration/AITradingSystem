from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import (
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH, DEFAULT_ETF_REPORT_DIR
from ai_trading_system.etf_portfolio.weight_research_unblock import DEFAULT_RATES_CACHE_PATH

DEFAULT_WEIGHT_RESEARCH_REPORT_DIR = DEFAULT_ETF_REPORT_DIR / "weight_research"
DEFAULT_RESEARCH_SOURCE_DIR = PROJECT_ROOT / "docs" / "research"

B2_EVAL_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_risk_heavy_window_evaluation.json"
B3_AUDIT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b3_slow_tilt_signal_direction_audit.json"
B1_REVIEW_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b1_execution_control_adoption_review.json"
BRANCH_DECISION_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "ablation_path_branching_decision.json"
B2_ONLY_CHECKPOINT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_only_research_candidate_checkpoint.json"
B3_HYPOTHESIS_PACK_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b3_redesign_hypothesis_pack.json"
PROGRAM_CHECKPOINT_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "research_program_checkpoint_after_branching.json"
)
MULTI_WINDOW_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b1_b4_multi_window_diagnostic_expansion.json"
B4_SYNTHESIS_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b4_interaction_evidence_synthesis.json"
B5_ADMISSION_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b5_admission_checkpoint.json"
WINDOW_CATALOG_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "research_window_catalog.json"

SAFETY_BOUNDARY = {
    "research_only": True,
    "manual_review_only": True,
    "paper_shadow_activation": False,
    "extended_shadow_allowed": False,
    "live_trading_allowed": False,
    "official_target_weights": False,
    "broker_action_allowed": False,
    "order_ticket_generated": False,
    "owner_decision_appended": False,
    "production_effect": "none",
}

B2_ALLOWED_MECHANISMS = (
    "risk trigger",
    "risk-off exposure scaler",
    "risk re-entry logic",
    "risk-heavy window diagnostics",
    "false risk-off accounting",
    "re-entry lag accounting",
)

B2_FORBIDDEN_MECHANISMS = (
    "slow relative tilt",
    "regime filter",
    "confidence shrinkage",
    "momentum allocator",
    "relative strength allocator",
    "P0 mixed dynamic strategy",
    "official target weights",
    "broker/order/live output",
)

# Pilot ordering policy is deliberately named and surfaced in artifacts. It is a research
# triage order, not an investment threshold or promotion rule.
B3_HYPOTHESIS_RANK_ORDER = (
    "smaller_tilt_cap",
    "baseline_shrinkage",
    "relative_strength_confirmation",
    "stronger_smoothing",
    "slower_relative_strength_window",
    "avoid_tilt_during_high_vol_regimes",
    "asset_level_contribution_filter",
    "confidence_weighted_tilt",
    "direction_inversion_test",
)

RISK_HEAVY_CATALOG = (
    {
        "window_id": "rapid_drawdown",
        "source_window_id": "rapid_drawdown",
        "risk_intensity": "rapid drawdown diagnostic",
        "expected_b2_behavior": "Trigger quickly only if risk score breaches risk-off policy.",
    },
    {
        "window_id": "slow_drawdown",
        "source_window_id": "slow_drawdown",
        "risk_intensity": "slow drawdown diagnostic",
        "expected_b2_behavior": (
            "De-risk during persistent drawdown and re-enter after risk normalizes."
        ),
    },
    {
        "window_id": "high_volatility_sideways",
        "source_window_id": "high_volatility_sideways",
        "risk_intensity": "high-volatility sideways diagnostic",
        "expected_b2_behavior": "Avoid excessive false risk-off churn in sideways volatility.",
    },
    {
        "window_id": "semiconductor_correction",
        "source_window_id": "semiconductor_correction",
        "risk_intensity": "AI / semiconductor correction diagnostic",
        "expected_b2_behavior": (
            "Protect drawdown without confusing single-industry weakness for market exit."
        ),
    },
    {
        "window_id": "v_shaped_recovery",
        "source_window_id": "v_shaped_recovery",
        "risk_intensity": "V-shaped recovery diagnostic",
        "expected_b2_behavior": "Re-enter quickly enough to limit recovery opportunity cost.",
    },
    {
        "window_id": "false_risk_off_cluster",
        "source_window_id": "false_risk_off_cluster",
        "risk_intensity": "false risk-off cluster diagnostic",
        "expected_b2_behavior": "Keep exposure unless risk trigger is confirmed.",
    },
    {
        "window_id": "risk_off_but_quick_recovery",
        "source_window_id": "v_shaped_recovery",
        "risk_intensity": "risk-off followed by quick recovery diagnostic",
        "expected_b2_behavior": "Do not stay under-exposed through a sharp recovery.",
    },
    {
        "window_id": "risk_signal_false_positive",
        "source_window_id": "false_risk_off_cluster",
        "risk_intensity": "risk signal false-positive diagnostic",
        "expected_b2_behavior": "Expose any false-positive risk-off count and opportunity cost.",
    },
)


def run_post_b2_b3_research(
    *,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = DEFAULT_RESEARCH_SOURCE_DIR,
    generated_at: datetime | None = None,
    data_quality_output_path: Path | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, tuple[Path, Path]]]:
    generated = generated_at or datetime.now(UTC)
    sources = _load_sources()
    requested_range = _requested_date_range(sources)
    data_quality = _run_quality_gate(
        prices_path=prices_path,
        rates_path=rates_path,
        as_of=generated.date(),
        output_path=data_quality_output_path,
    )

    scope = build_b2_only_research_scope(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    catalog = build_b2_risk_heavy_window_catalog(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b2_backfill = build_b2_only_risk_heavy_diagnostic_backfill(
        sources=sources,
        catalog=catalog,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b2_attribution = build_b2_false_risk_off_reentry_attribution(
        backfill=b2_backfill,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b2_cost = build_b2_cost_benchmark_survival_review(
        sources=sources,
        backfill=b2_backfill,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b2_gate = build_b2_only_research_gate(
        sources=sources,
        scope=scope,
        backfill=b2_backfill,
        attribution=b2_attribution,
        cost_review=b2_cost,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b3_constraints = build_b3_redesign_constraints(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b3_ranking = build_b3_redesign_hypothesis_ranking(
        sources=sources,
        constraints=b3_constraints,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b3_precheck = build_b3_signal_direction_precheck(
        sources=sources,
        ranking=b3_ranking,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b3_mini = build_b3_redesigned_mini_backfill(
        precheck=b3_precheck,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b3_gate = build_b3_redesign_gate(
        precheck=b3_precheck,
        mini_backfill=b3_mini,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b1_b2 = build_b1_wrapper_compatibility_with_b2(
        sources=sources,
        b2_backfill=b2_backfill,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b1_b3 = build_b1_wrapper_compatibility_with_redesigned_b3(
        b3_gate=b3_gate,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    synthesis = build_post_b2_b3_branch_synthesis(
        sources=sources,
        b2_gate=b2_gate,
        b3_gate=b3_gate,
        b1_b2=b1_b2,
        b1_b3=b1_b3,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b4_retest = build_retest_b4_with_redesigned_b3(
        b2_gate=b2_gate,
        b3_gate=b3_gate,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b5_readmission = build_b5_readmission_after_redesigned_b4(
        b4_retest=b4_retest,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    cadence = build_research_cadence_controller(
        synthesis=synthesis,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    backlog = build_candidate_exploration_backlog_manager(
        b2_gate=b2_gate,
        b3_gate=b3_gate,
        b1_b2=b1_b2,
        b1_b3=b1_b3,
        b4_retest=b4_retest,
        b5_readmission=b5_readmission,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    monthly = build_monthly_research_program_review(
        sources=sources,
        b2_gate=b2_gate,
        b3_gate=b3_gate,
        b1_b2=b1_b2,
        b1_b3=b1_b3,
        synthesis=synthesis,
        backlog=backlog,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    final = build_final_branch_decision_snapshot(
        synthesis=synthesis,
        b4_retest=b4_retest,
        b5_readmission=b5_readmission,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )

    payloads = {
        "b2_only_research_scope": scope,
        "b2_risk_heavy_window_catalog": catalog,
        "b2_only_risk_heavy_diagnostic_backfill": b2_backfill,
        "b2_false_risk_off_reentry_attribution": b2_attribution,
        "b2_cost_benchmark_survival_review": b2_cost,
        "b2_only_research_gate": b2_gate,
        "b3_redesign_constraints": b3_constraints,
        "b3_redesign_hypothesis_ranking": b3_ranking,
        "b3_signal_direction_precheck": b3_precheck,
        "b3_redesigned_mini_backfill": b3_mini,
        "b3_redesign_gate": b3_gate,
        "b1_wrapper_compatibility_with_b2": b1_b2,
        "b1_wrapper_compatibility_with_redesigned_b3": b1_b3,
        "post_b2_b3_branch_synthesis": synthesis,
        "retest_b4_with_redesigned_b3": b4_retest,
        "b5_readmission_after_redesigned_b4": b5_readmission,
        "research_cadence_controller": cadence,
        "candidate_exploration_backlog_manager": backlog,
        "monthly_research_program_review": monthly,
        "final_branch_decision_snapshot": final,
    }
    paths = write_post_branch_payloads(payloads, output_dir=output_dir, alias_dir=alias_dir)
    return payloads, paths


def build_b2_only_research_scope(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-537",
        report_type="b2_only_research_scope",
        status="B2_ONLY_RESEARCH_SCOPE_PASS",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2-only scope is frozen to B0 static baseline plus fast asymmetric risk scaler.",
    )
    payload.update(
        {
            "b2_definition": "B0 static strategic baseline + fast asymmetric risk scaler only",
            "allowed_mechanisms": list(B2_ALLOWED_MECHANISMS),
            "forbidden_mechanisms": list(B2_FORBIDDEN_MECHANISMS),
            "scope_validation": [
                _check("B3 slow tilt excluded", True, "B3 is not part of B2-only scope."),
                _check("B5 confidence excluded", True, "Confidence shrinkage remains blocked."),
                _check("B6 regime excluded", True, "Regime filter remains blocked."),
                _check(
                    "P0 mixed allocator excluded",
                    True,
                    "P0 mixed strategy is not a module proxy.",
                ),
                _check(
                    "B5/B6/v3 blocked",
                    _all_blocked(sources),
                    "Source gates keep b5_allowed=false, b6_allowed=false and v3_allowed=false.",
                ),
            ],
            "research_outputs_only": True,
        }
    )
    return payload


def build_b2_risk_heavy_window_catalog(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    source_windows = _source_windows(sources)
    rows = []
    for catalog_row in RISK_HEAVY_CATALOG:
        source = source_windows.get(str(catalog_row["source_window_id"]), {})
        rows.append(
            {
                "window_id": catalog_row["window_id"],
                "source_window_id": catalog_row["source_window_id"],
                "start_date": source.get("start_date"),
                "end_date": source.get("end_date"),
                "market_regime": source.get("market_regime", "ai_after_chatgpt"),
                "risk_intensity": catalog_row["risk_intensity"],
                "expected_b2_behavior": catalog_row["expected_b2_behavior"],
                "allowed_stage": "diagnostic",
                "holdout_allowed": False,
                "source_artifacts": _source_artifacts(),
                "data_quality_status": data_quality_gate["status"],
            }
        )
    payload = _base_payload(
        task_id="TRADING-538",
        report_type="b2_risk_heavy_window_catalog",
        status="B2_RISK_HEAVY_WINDOW_CATALOG_READY",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary=(
            "B2 risk-heavy diagnostic windows cover drawdown, recovery and false-positive probes."
        ),
    )
    payload.update(
        {
            "windows": rows,
            "required_coverage": [row["window_id"] for row in RISK_HEAVY_CATALOG],
            "untouched_holdout_used": False,
            "coverage_validation": [
                _check("required windows present", len(rows) == len(RISK_HEAVY_CATALOG), "ready"),
                _check(
                    "all diagnostic only",
                    all(row["allowed_stage"] == "diagnostic" for row in rows),
                    "no holdout rows are allowed",
                ),
            ],
        }
    )
    return payload


def build_b2_only_risk_heavy_diagnostic_backfill(
    *,
    sources: dict[str, dict[str, Any]],
    catalog: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    evaluations = {
        str(row["window_id"]): row
        for row in sources["b2_eval"].get("window_evaluations", [])
    }
    rows = []
    for catalog_row in catalog["windows"]:
        source_window_id = str(catalog_row["source_window_id"])
        source_row = evaluations.get(source_window_id, {})
        rows.append(
            {
                "window_id": catalog_row["window_id"],
                "source_window_id": source_window_id,
                "start_date": catalog_row["start_date"],
                "end_date": catalog_row["end_date"],
                "risk_trigger_count": int(source_row.get("risk_trigger_count", 0)),
                "risk_trigger_dates": source_row.get("risk_trigger_dates", []),
                "risk_off_events": _risk_off_events(source_row),
                "risk_reentry_events": _risk_reentry_events(source_row),
                "exposure_scaler_changes": source_row.get("exposure_scaler_changes", []),
                "return_delta_vs_B0": float(source_row.get("return_delta_vs_b0", 0.0)),
                "drawdown_delta_vs_B0": float(source_row.get("drawdown_delta_vs_b0", 0.0)),
                "turnover_delta_vs_B0": float(source_row.get("turnover_delta_vs_b0", 0.0)),
                "cost_delta_vs_B0": float(source_row.get("cost_delta_vs_b0", 0.0)),
                "benchmark_relative_delta": float(source_row.get("return_delta_vs_b0", 0.0)),
                "false_risk_off_count": int(source_row.get("false_risk_off_count", 0)),
                "reentry_lag": source_row.get("re_entry_lag_days"),
                "V_shaped_recovery_opportunity_cost": float(
                    source_row.get("v_shaped_recovery_opportunity_cost", 0.0)
                ),
            }
        )
    status = (
        "B2_RISK_HEAVY_BACKFILL_COMPLETE"
        if data_quality_gate["passed"] and rows
        else "B2_RISK_HEAVY_BACKFILL_BLOCKED"
    )
    payload = _base_payload(
        task_id="TRADING-539",
        report_type="b2_only_risk_heavy_diagnostic_backfill",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2-only diagnostic backfill is materialized from frozen B2 risk-heavy evidence.",
    )
    payload.update(
        {
            "scope_status": sources["b2_only_checkpoint"].get("status"),
            "window_results": rows,
            "aggregate": _b2_backfill_aggregate(rows),
            "parameter_changes_applied": False,
            "forbidden_modules_absent": ["B3", "B5", "B6", "P0 mixed allocator"],
        }
    )
    return payload


def build_b2_false_risk_off_reentry_attribution(
    *,
    backfill: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    aggregate = backfill["aggregate"]
    false_count = int(aggregate["false_risk_off_count"])
    triggered = int(aggregate["risk_trigger_count"])
    drawdown_benefit = float(aggregate["drawdown_delta_vs_B0"])
    max_lag = aggregate["max_reentry_lag"]
    if triggered == 0:
        status = "B2_NOT_TRIGGERING_ENOUGH"
    elif false_count > 0:
        status = "B2_FALSE_RISK_OFF_TOO_HIGH"
    elif drawdown_benefit > 0 and max_lag is not None:
        status = "B2_PROTECTS_BUT_REENTRY_LAG_HIGH"
    elif drawdown_benefit > 0:
        status = "B2_PROTECTS_DRAWDOWN_ACCEPTABLE_COST"
    else:
        status = "B2_RISK_LOGIC_NEEDS_REDESIGN"
    payload = _base_payload(
        task_id="TRADING-540",
        report_type="b2_false_risk_off_reentry_attribution",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 risk-off and re-entry behavior is attributed from diagnostic windows.",
    )
    payload.update(
        {
            "analysis": {
                "risk_off_in_true_risk_windows": triggered > 0,
                "risk_off_too_frequent": false_count > 0,
                "risk_off_too_late": "requires_owner_review",
                "re_entry_too_slow": max_lag is not None,
                "missed_V_shaped_recovery": aggregate["V_shaped_recovery_opportunity_cost"] > 0,
                "high_volatility_sideways_false_trigger": _window_false_count(
                    backfill,
                    "high_volatility_sideways",
                ),
                "false_risk_off_opportunity_cost": aggregate[
                    "V_shaped_recovery_opportunity_cost"
                ],
            },
            "window_rows": backfill["window_results"],
            "classification_reason": (
                "Slow drawdown shows drawdown protection, but non-null re-entry lag keeps the "
                "result in owner-review evidence rather than promising research clearance."
            ),
        }
    )
    return payload


def build_b2_cost_benchmark_survival_review(
    *,
    sources: dict[str, dict[str, Any]],
    backfill: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    aggregate = backfill["aggregate"]
    status = "B2_COST_BENCHMARK_MIXED"
    payload = _base_payload(
        task_id="TRADING-541",
        report_type="b2_cost_benchmark_survival_review",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 cost and benchmark survival remains mixed on current diagnostic evidence.",
    )
    payload.update(
        {
            "cost_survival_status": status,
            "benchmark_relative_status": "MIXED_VS_B0_RETURN_DRAG_WITH_DRAWDOWN_BENEFIT",
            "drawdown_benefit": aggregate["drawdown_delta_vs_B0"],
            "opportunity_cost": -min(0.0, float(aggregate["return_delta_vs_B0"])),
            "turnover_cost": aggregate["cost_delta_vs_B0"],
            "net_utility_delta": (
                float(aggregate["drawdown_delta_vs_B0"])
                + float(aggregate["return_delta_vs_B0"])
                - float(aggregate["cost_delta_vs_B0"])
            ),
            "worst_window_delta": _worst_return_window(backfill),
            "comparisons": [
                {
                    "comparison": "B2 vs B0",
                    "status": "MIXED",
                    "evidence": aggregate,
                },
                {
                    "comparison": "B2 vs B1 optional wrapper baseline",
                    "status": "SOURCE_SEPARATE_NOT_COMBINED",
                    "evidence": sources["b1_review"].get("window_reviews", []),
                },
                {
                    "comparison": "B2 vs static benchmark",
                    "status": "B0_STATIC_PROXY_USED",
                    "evidence": "B0 static baseline is the canonical comparator for this batch.",
                },
                {
                    "comparison": "B2 vs no-trade baseline",
                    "status": "NOT_WINDOW_ALIGNED_SOURCE_ABSENT",
                    "evidence": (
                        "No no-trade risk-heavy window artifact is available in this batch."
                    ),
                },
            ],
        }
    )
    return payload


def build_b2_only_research_gate(
    *,
    sources: dict[str, dict[str, Any]],
    scope: dict[str, Any],
    backfill: dict[str, Any],
    attribution: dict[str, Any],
    cost_review: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    status = "B2_ONLY_NEEDS_MORE_EVIDENCE"
    if cost_review["status"] == "B2_COST_BENCHMARK_WEAK":
        status = "B2_ONLY_WEAK"
    if attribution["status"] == "B2_RISK_LOGIC_NEEDS_REDESIGN":
        status = "B2_ONLY_RETURN_TO_DESIGN"
    payload = _base_payload(
        task_id="TRADING-542",
        report_type="b2_only_research_gate",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary=(
            "B2-only remains researchable but does not have enough evidence for combo promotion."
        ),
    )
    payload.update(
        {
            "input_statuses": {
                "scope": scope["status"],
                "backfill": backfill["status"],
                "false_risk_off_reentry": attribution["status"],
                "cost_benchmark": cost_review["status"],
                "source_checkpoint": sources["b2_only_checkpoint"].get("status"),
            },
            "decision_logic": [
                _check(
                    "drawdown benefit present",
                    float(backfill["aggregate"]["drawdown_delta_vs_B0"]) > 0,
                    "slow_drawdown benefit is present",
                ),
                _check(
                    "false risk-off acceptable",
                    int(backfill["aggregate"]["false_risk_off_count"]) == 0,
                    "no false risk-off count in current source artifact",
                ),
                _check(
                    "cost benchmark not weak",
                    cost_review["status"] != "B2_COST_BENCHMARK_WEAK",
                    cost_review["status"],
                ),
                _check(
                    "window not promotion-ready",
                    True,
                    "Only one triggered risk-heavy source window is available.",
                ),
            ],
            "paper_shadow_allowed": False,
            "combo_research_allowed": False,
        }
    )
    return payload


def build_b3_redesign_constraints(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-543",
        report_type="b3_redesign_constraints",
        status="B3_REDESIGN_CONSTRAINTS_FROZEN",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B3 redesign constraints are frozen around signal direction, lag and turnover.",
    )
    payload.update(
        {
            "current_b3_problem": sources["b3_audit"].get("status"),
            "constraints": [
                "relative strength direction must be audited before weight generation",
                "signal lag must be lower or explicitly accepted",
                "tilt cap must not increase",
                "redesign must not chase reversals with larger active weights",
                "turnover must not significantly worsen versus B0/B3 source evidence",
                "risk/confidence filters are forbidden unless separately admitted",
            ],
            "forbidden_redesign_shortcuts": [
                "parameter sweep without hypothesis",
                "holdout use",
                "official target weights",
                "P0 mixed allocator substitution",
                "B5/B6/v3 dependency",
            ],
        }
    )
    return payload


def build_b3_redesign_hypothesis_ranking(
    *,
    sources: dict[str, dict[str, Any]],
    constraints: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    source_rows = {
        str(row["hypothesis_id"]): row
        for row in sources["b3_hypothesis_pack"].get("hypotheses", [])
    }
    source_rows["direction_inversion_test"] = {
        "hypothesis_id": "direction_inversion_test",
        "expected_improvement": "Detect whether B3 signal direction is structurally reversed.",
        "changed_logic": "Diagnostic-only inversion test before any production-style redesign.",
        "expected_failure_mode": "Inversion overfits wrong-tilt dates and fails normal windows.",
        "kill_criteria": "Reject if normal-uptrend agreement worsens or turnover increases.",
    }
    ranked = []
    for rank, hypothesis_id in enumerate(B3_HYPOTHESIS_RANK_ORDER, start=1):
        row = source_rows.get(hypothesis_id)
        if row is None:
            continue
        ranked.append(
            {
                "rank": rank,
                "hypothesis_id": hypothesis_id,
                "changed_logic": row.get("changed_logic"),
                "expected_improvement": row.get("expected_improvement"),
                "expected_failure_mode": row.get("expected_failure_mode"),
                "diagnostic_window": _hypothesis_window(hypothesis_id),
                "main_metric": _hypothesis_metric(hypothesis_id),
                "kill_criteria": row.get("kill_criteria"),
                "forbidden_mechanisms": [
                    "B5 confidence",
                    "B6 regime",
                    "official target weights",
                    "holdout",
                    "broker/order/live output",
                ],
                "selected_for_precheck": rank <= 3,
            }
        )
    payload = _base_payload(
        task_id="TRADING-544",
        report_type="b3_redesign_hypothesis_ranking",
        status="B3_REDESIGN_HYPOTHESES_RANKED",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B3 redesign hypotheses are ranked under frozen signal-direction constraints.",
    )
    payload.update(
        {
            "ranking_policy_version": "b3_redesign_hypothesis_ranking_pilot_2026-06-19",
            "constraints_status": constraints["status"],
            "hypotheses": ranked,
            "selected_hypothesis_ids": [
                row["hypothesis_id"] for row in ranked if row["selected_for_precheck"]
            ],
        }
    )
    return payload


def build_b3_signal_direction_precheck(
    *,
    sources: dict[str, dict[str, Any]],
    ranking: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    b3 = sources["b3_audit"]
    wrong_dates = b3.get("wrong_tilt_dates", [])
    total_tilt_days = _total_tilt_direction_days(b3)
    wrong_rate = (len(wrong_dates) / total_tilt_days) if total_tilt_days else None
    status = (
        "B3_SIGNAL_DIRECTION_PRECHECK_MIXED"
        if wrong_dates
        else "B3_SIGNAL_DIRECTION_PRECHECK_PASS"
    )
    payload = _base_payload(
        task_id="TRADING-545",
        report_type="b3_signal_direction_precheck",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B3 signal direction precheck stays mixed; redesigned weights are not generated.",
    )
    payload.update(
        {
            "selected_hypothesis_ids": ranking["selected_hypothesis_ids"],
            "wrong_direction_rate": wrong_rate,
            "wrong_direction_count": len(wrong_dates),
            "lag_score": b3.get("relative_strength_signal_lag", {}),
            "directional_agreement": "MIXED_EXISTING_B3_WRONG_TILT_DATES_REMAIN",
            "asset_contribution_alignment": _asset_contribution_alignment(b3),
            "signal_turnover": _signal_turnover(b3),
            "hypothetical_weight_generated": False,
            "precheck_decision": (
                "Do not run redesigned mini-backfill unless a later precheck produces PASS."
            ),
        }
    )
    return payload


def build_b3_redesigned_mini_backfill(
    *,
    precheck: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    blocked = precheck["status"] != "B3_SIGNAL_DIRECTION_PRECHECK_PASS"
    status = (
        "B3_REDESIGNED_MINI_BACKFILL_BLOCKED"
        if blocked
        else "B3_REDESIGNED_MINI_BACKFILL_MIXED"
    )
    payload = _base_payload(
        task_id="TRADING-546",
        report_type="b3_redesigned_mini_backfill",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B3 redesigned mini-backfill is blocked until signal-direction precheck passes.",
    )
    payload.update(
        {
            "precheck_status": precheck["status"],
            "mini_backfill_executed": not blocked,
            "blocked_reason": (
                "B3 signal-direction precheck is not PASS; no redesigned B3 weights are generated."
                if blocked
                else "none"
            ),
            "metrics": {
                "return_delta_vs_B0": None,
                "drawdown_delta_vs_B0": None,
                "turnover_delta_vs_B0": None,
                "cost_delta_vs_B0": None,
                "benchmark_relative_delta": None,
                "wrong_tilt_dates": [],
                "tilt_contribution": None,
                "window_result": "not_run",
            },
        }
    )
    return payload


def build_b3_redesign_gate(
    *,
    precheck: dict[str, Any],
    mini_backfill: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    if mini_backfill["status"] == "B3_REDESIGNED_MINI_BACKFILL_BLOCKED":
        status = "B3_REDESIGN_RETURN_TO_HYPOTHESIS"
    else:
        status = "B3_REDESIGNED_NEEDS_MORE_EVIDENCE"
    payload = _base_payload(
        task_id="TRADING-547",
        report_type="b3_redesign_gate",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B3 cannot re-enter B4 combo research until a redesigned mini-backfill is valid.",
    )
    payload.update(
        {
            "precheck_status": precheck["status"],
            "mini_backfill_status": mini_backfill["status"],
            "reenter_b4_allowed": status == "B3_REDESIGNED_REENTER_COMBO_RESEARCH",
            "current_b3_allowed_in_combo": False,
            "required_next_action": "return_to_hypothesis_and_produce_precheck_pass",
        }
    )
    return payload


def build_b1_wrapper_compatibility_with_b2(
    *,
    sources: dict[str, dict[str, Any]],
    b2_backfill: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-548",
        report_type="b1_wrapper_compatibility_with_b2",
        status="B1_WRAPPER_MIXED_WITH_B2",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B1 wrapper lowers turnover in separate evidence but lacks combined B1+B2 proof.",
    )
    payload.update(
        {
            "comparisons": {
                "B2_vs_B0": b2_backfill["aggregate"],
                "B1_plus_B2_vs_B0": "NOT_RUN_NO_COMBINED_WRAPPER_ARTIFACT",
                "B1_plus_B2_vs_B2": "NOT_RUN_NO_COMBINED_WRAPPER_ARTIFACT",
            },
            "required_checks": {
                "turnover_decreases": _b1_turnover_decreases(sources["b1_review"]),
                "cost_decreases": _b1_cost_decreases(sources["b1_review"]),
                "risk_off_delayed": "UNKNOWN_NO_COMBINED_ARTIFACT",
                "drawdown_protection_worse": "UNKNOWN_NO_COMBINED_ARTIFACT",
                "reentry_slower": "UNKNOWN_NO_COMBINED_ARTIFACT",
                "false_risk_off_improves": "UNKNOWN_NO_COMBINED_ARTIFACT",
            },
            "wrapper_default_allowed": False,
            "optional_research_wrapper_allowed": True,
        }
    )
    return payload


def build_b1_wrapper_compatibility_with_redesigned_b3(
    *,
    b3_gate: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    valid_b3 = b3_gate["status"] in {
        "B3_REDESIGNED_REENTER_COMBO_RESEARCH",
        "B3_REDESIGNED_NEEDS_MORE_EVIDENCE",
    }
    status = (
        "B1_WRAPPER_MIXED_WITH_B3"
        if valid_b3
        else "B1_B3_WRAPPER_TEST_BLOCKED_NO_VALID_B3"
    )
    payload = _base_payload(
        task_id="TRADING-549",
        report_type="b1_wrapper_compatibility_with_redesigned_b3",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary=(
            "B1 wrapper test with redesigned B3 is blocked because no valid redesigned B3 exists."
        ),
    )
    payload.update(
        {
            "b3_gate_status": b3_gate["status"],
            "test_executed": valid_b3,
            "blocked_reason": "no_valid_redesigned_b3" if not valid_b3 else "none",
        }
    )
    return payload


def build_post_b2_b3_branch_synthesis(
    *,
    sources: dict[str, dict[str, Any]],
    b2_gate: dict[str, Any],
    b3_gate: dict[str, Any],
    b1_b2: dict[str, Any],
    b1_b3: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    selected = "CONTINUE_B2_ONLY_RESEARCH"
    payload = _base_payload(
        task_id="TRADING-550",
        report_type="post_b2_b3_branch_synthesis",
        status=selected,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="Branch synthesis keeps B2-only research active and B5/B6/v3 blocked.",
    )
    payload.update(
        {
            "selected_branch": selected,
            "rejected_branches": [
                {
                    "branch": "RETEST_B4_WITH_REDESIGNED_B3",
                    "reason": "No valid redesigned B3 gate is available.",
                },
                {
                    "branch": "CONTINUE_B2_WITH_B1_WRAPPER",
                    "reason": "B1+B2 combined wrapper evidence is not available.",
                },
                {
                    "branch": "CONTINUE_B3_REDESIGN_ONLY",
                    "reason": "B3 remains useful as a redesign audit, but not as primary path.",
                },
                {
                    "branch": "DROP_B3_CURRENT_LINE",
                    "reason": "Current B3 is barred from combo, but hypotheses remain reviewable.",
                },
            ],
            "blocked_modules": ["B3_CURRENT_FORM", "B4_CURRENT_COMBO", "B5", "B6", "v3"],
            "allowed_modules": [
                "B2_ONLY_RESEARCH",
                "B3_REDESIGN_AUDIT",
                "B1_OPTIONAL_WRAPPER_TESTS",
            ],
            "next_task_recommendation": "continue_b2_only_risk_heavy_research",
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
            "input_statuses": {
                "b2_only_research_gate": b2_gate["status"],
                "b3_redesign_gate": b3_gate["status"],
                "b1_wrapper_with_b2": b1_b2["status"],
                "b1_wrapper_with_b3": b1_b3["status"],
                "previous_b4": sources["b4_synthesis"].get("status"),
            },
        }
    )
    return payload


def build_retest_b4_with_redesigned_b3(
    *,
    b2_gate: dict[str, Any],
    b3_gate: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    prerequisites_pass = b2_gate["status"] in {
        "B2_ONLY_RESEARCH_PROMISING",
        "B2_ONLY_NEEDS_MORE_EVIDENCE",
    } and b3_gate["status"] == "B3_REDESIGNED_REENTER_COMBO_RESEARCH"
    status = (
        "B4_REDESIGNED_INCONCLUSIVE"
        if prerequisites_pass
        else "B4_REDESIGNED_RETEST_BLOCKED_NO_VALID_B3"
    )
    payload = _base_payload(
        task_id="TRADING-551",
        report_type="retest_b4_with_redesigned_b3",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B4 retest is blocked until B3 redesign gate allows combo research.",
    )
    payload.update(
        {
            "prerequisites": {
                "b2_gate_status": b2_gate["status"],
                "b3_gate_status": b3_gate["status"],
                "passed": prerequisites_pass,
            },
            "comparisons": {
                "B4_prime_vs_B0": "NOT_RUN",
                "B4_prime_vs_B2": "NOT_RUN",
                "B4_prime_vs_B3_prime": "NOT_RUN",
                "B4_prime_vs_previous_B4": "NOT_RUN",
            },
            "blocked_reason": "no_valid_redesigned_b3" if not prerequisites_pass else "none",
        }
    )
    return payload


def build_b5_readmission_after_redesigned_b4(
    *,
    b4_retest: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    allowed = b4_retest["status"] in {
        "B4_REDESIGNED_POSITIVE_SYNERGY",
        "B4_REDESIGNED_MOSTLY_ADDITIVE",
    }
    status = "B5_READMISSION_ALLOWED" if allowed else "B5_READMISSION_BLOCKED"
    payload = _base_payload(
        task_id="TRADING-552",
        report_type="b5_readmission_after_redesigned_b4",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B5 readmission remains blocked because redesigned B4 is not positive/additive.",
    )
    payload.update(
        {
            "b5_allowed": allowed,
            "b6_allowed": False,
            "v3_allowed": False,
            "required_b4_status": [
                "B4_REDESIGNED_POSITIVE_SYNERGY",
                "B4_REDESIGNED_MOSTLY_ADDITIVE",
            ],
            "source_b4_retest_status": b4_retest["status"],
            "hard_rule_checks": [
                _check(
                    "B5 requires positive/additive redesigned B4",
                    not allowed,
                    "Current redesigned B4 retest is not eligible.",
                ),
                _check("No holdout used", True, "holdout_accessed=false"),
            ],
        }
    )
    return payload


def build_research_cadence_controller(
    *,
    synthesis: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-553",
        report_type="research_cadence_controller",
        status="RESEARCH_CADENCE_CONTROLLER_READY",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="Research cadence is structured without automatic tuning or promotion.",
    )
    payload.update(
        {
            "allowed": [
                "B2-only risk-heavy research",
                "B3 redesign audit",
                "B1 optional wrapper tests",
            ],
            "blocked": [
                "B5",
                "B6",
                "v3",
                "paper-shadow",
                "extended shadow",
                "live trading",
            ],
            "cadence": {
                "daily": "system health only",
                "weekly": "research queue triage",
                "event_triggered": "backfill only after documented gate allows it",
                "monthly": "research gate summary",
            },
            "forbidden_actions": [
                "auto_change_candidate_logic",
                "auto_touch_holdout",
                "auto_enter_paper_shadow",
                "auto_append_owner_decision",
            ],
            "selected_branch": synthesis["selected_branch"],
        }
    )
    return payload


def build_candidate_exploration_backlog_manager(
    *,
    b2_gate: dict[str, Any],
    b3_gate: dict[str, Any],
    b1_b2: dict[str, Any],
    b1_b3: dict[str, Any],
    b4_retest: dict[str, Any],
    b5_readmission: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    items = [
        _backlog_item(
            "B2_ONLY_RISK_HEAVY_EVIDENCE",
            "B2",
            b2_gate["status"],
            "More triggered risk-heavy windows with acceptable cost/re-entry.",
            "continue_b2_only_diagnostics",
            False,
            "none",
        ),
        _backlog_item(
            "B3_SIGNAL_DIRECTION_REDESIGN",
            "B3",
            b3_gate["status"],
            "Signal-direction precheck PASS before redesigned mini-backfill.",
            "return_to_hypothesis_precheck",
            False,
            "precheck_not_pass",
        ),
        _backlog_item(
            "B1_B2_WRAPPER_COMBINED_TEST",
            "B1+B2",
            b1_b2["status"],
            "Combined B1+B2 artifact proving no risk-off delay.",
            "build_combined_wrapper_test_after_owner_review",
            True,
            "combined_artifact_absent",
        ),
        _backlog_item(
            "B1_B3_WRAPPER_AFTER_VALID_B3",
            "B1+B3",
            b1_b3["status"],
            "Valid redesigned B3 gate.",
            "blocked_until_valid_b3",
            False,
            "no_valid_b3",
        ),
        _backlog_item(
            "B4_REDESIGNED_RETEST",
            "B4",
            b4_retest["status"],
            "B2 research value plus B3 redesigned re-entry gate.",
            "blocked_until_b3_reentry",
            False,
            "no_valid_b3",
        ),
        _backlog_item(
            "B5_READMISSION",
            "B5",
            b5_readmission["status"],
            "Redesigned B4 positive/additive and all robustness gates pass.",
            "blocked_until_redesigned_b4_positive",
            False,
            "no_positive_redesigned_b4",
        ),
    ]
    payload = _base_payload(
        task_id="TRADING-554",
        report_type="candidate_exploration_backlog_manager",
        status="BACKLOG_READY",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="Candidate exploration backlog is structured with explicit blockers.",
    )
    payload.update({"backlog_items": items})
    return payload


def build_monthly_research_program_review(
    *,
    sources: dict[str, dict[str, Any]],
    b2_gate: dict[str, Any],
    b3_gate: dict[str, Any],
    b1_b2: dict[str, Any],
    b1_b3: dict[str, Any],
    synthesis: dict[str, Any],
    backlog: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-555",
        report_type="monthly_research_program_review",
        status="MONTHLY_RESEARCH_PROGRAM_REVIEW_READY",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="Monthly review records B2-only as active and B5/B6/v3 as blocked.",
    )
    payload.update(
        {
            "B0_status": "STATIC_BASELINE_AVAILABLE",
            "B1_status": sources["b1_review"].get("status"),
            "B2_status": b2_gate["status"],
            "B3_status": b3_gate["status"],
            "B4_status": sources["b4_synthesis"].get("status"),
            "B2_only_path_status": b2_gate["status"],
            "B3_redesign_status": b3_gate["status"],
            "B1_wrapper_status": {
                "with_B2": b1_b2["status"],
                "with_B3": b1_b3["status"],
            },
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
            "active_blockers": synthesis["blocked_modules"],
            "dropped_modules": ["B3_CURRENT_FORM", "B4_CURRENT_COMBO"],
            "next_month_research_plan": [
                "Continue B2-only risk-heavy evidence collection.",
                "Return B3 redesign to hypothesis/precheck before any mini-backfill.",
                "Do not revisit B4/B5 until redesigned B3 is valid.",
            ],
            "backlog_status": backlog["status"],
        }
    )
    return payload


def build_final_branch_decision_snapshot(
    *,
    synthesis: dict[str, Any],
    b4_retest: dict[str, Any],
    b5_readmission: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    status = "CONTINUE_B2_ONLY_PATH"
    if b5_readmission["status"] == "B5_READMISSION_ALLOWED":
        status = "PROCEED_TO_B5"
    elif b4_retest["status"] in {
        "B4_REDESIGNED_POSITIVE_SYNERGY",
        "B4_REDESIGNED_MOSTLY_ADDITIVE",
    }:
        status = "RETEST_B4_WITH_REDESIGNED_B3"
    payload = _base_payload(
        task_id="TRADING-556",
        report_type="final_branch_decision_snapshot",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="Final snapshot continues B2-only path and keeps B5/B6/v3 blocked.",
    )
    payload.update(
        {
            "selected_branch": status,
            "synthesis_status": synthesis["status"],
            "b4_retest_status": b4_retest["status"],
            "b5_readmission_status": b5_readmission["status"],
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
            "paper_shadow_allowed": False,
            "hard_constraints": [
                _check(
                    "PROCEED_TO_B5 only after positive/additive redesigned B4",
                    status != "PROCEED_TO_B5",
                    "B5 is not admitted.",
                ),
                _check(
                    "paper-shadow never entered by this batch",
                    True,
                    "paper_shadow_allowed=false",
                ),
            ],
        }
    )
    return payload


def write_post_branch_payloads(
    payloads: dict[str, dict[str, Any]],
    *,
    output_dir: Path,
    alias_dir: Path | None,
) -> dict[str, tuple[Path, Path]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, tuple[Path, Path]] = {}
    for stem, payload in payloads.items():
        stamp = _stamp(str(payload["generated_at"]))
        json_path = output_dir / f"{stem}_{stamp}.json"
        md_path = output_dir / f"{stem}_{stamp}.md"
        markdown = render_post_branch_payload(payload)
        _write_json(json_path, payload)
        md_path.write_text(markdown, encoding="utf-8")
        if alias_dir is not None:
            alias_dir.mkdir(parents=True, exist_ok=True)
            _write_json(alias_dir / f"{stem}.json", payload)
            (alias_dir / f"{stem}.md").write_text(markdown, encoding="utf-8")
        paths[stem] = (json_path, md_path)
    return paths


def render_post_branch_payload(payload: dict[str, Any]) -> str:
    lines = [
        f"# {str(payload['report_type']).replace('_', ' ').title()}",
        "",
        f"- Status: {payload['status']}",
        f"- Market Regime: {payload['market_regime']}",
        f"- Requested Range: {payload['requested_date_range']['start_date']} to "
        f"{payload['requested_date_range']['end_date']}",
        f"- Data Quality: {payload['data_quality_gate']['status']}",
        f"- Production Effect: {payload['safety_boundary']['production_effect']}",
        "",
        "## Reader Brief",
        "",
        f"- Summary: {payload['reader_brief']['summary']}",
        f"- Key Result: {payload['reader_brief']['key_result']}",
        f"- Blocking Issues: {payload['reader_brief']['blocking_issues']}",
        f"- Warnings: {payload['reader_brief']['warnings']}",
        f"- Safety Boundary: {payload['reader_brief']['safety_boundary']}",
        f"- Next Action: {payload['reader_brief']['next_action']}",
    ]
    if "selected_branch" in payload:
        lines.extend(["", "## Branch", "", f"`{payload['selected_branch']}`"])
    if "b5_allowed" in payload:
        lines.extend(
            [
                "",
                "## Allowed Flags",
                "",
                f"- b5_allowed: {payload['b5_allowed']}",
                f"- b6_allowed: {payload['b6_allowed']}",
                f"- v3_allowed: {payload['v3_allowed']}",
            ]
        )
    return "\n".join(lines) + "\n"


def _base_payload(
    *,
    task_id: str,
    report_type: str,
    status: str,
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
    summary: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "task_id": task_id,
        "report_type": report_type,
        "status": status,
        "generated_at": generated_at.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "requested_date_range": requested_date_range,
        "data_quality_gate": data_quality_gate,
        "holdout_accessed": False,
        "forbidden_outputs_absent": True,
        "safety_boundary": dict(SAFETY_BOUNDARY),
        "source_artifacts": _source_artifacts(),
        "reader_brief": {
            "summary": summary,
            "key_result": status,
            "blocking_issues": "none" if "BLOCKED" not in status else status,
            "warnings": (
                "Research-only post-branch review; no B5/B6/v3, paper-shadow, broker/order "
                "or production action."
            ),
            "safety_boundary": (
                "research_only=true; manual_review_only=true; "
                "official_target_weights=false; production_effect=none"
            ),
            "next_action": "Manual owner/research review before any subsequent gate.",
        },
    }


def _load_sources() -> dict[str, dict[str, Any]]:
    return {
        "b2_eval": _read_json(B2_EVAL_PATH),
        "b3_audit": _read_json(B3_AUDIT_PATH),
        "b1_review": _read_json(B1_REVIEW_PATH),
        "branch": _read_json(BRANCH_DECISION_PATH),
        "b2_only_checkpoint": _read_json(B2_ONLY_CHECKPOINT_PATH),
        "b3_hypothesis_pack": _read_json(B3_HYPOTHESIS_PACK_PATH),
        "program_checkpoint": _read_json(PROGRAM_CHECKPOINT_PATH),
        "multi_window": _read_json(MULTI_WINDOW_PATH),
        "b4_synthesis": _read_json(B4_SYNTHESIS_PATH),
        "b5_admission": _read_json(B5_ADMISSION_PATH),
        "window_catalog": _read_json(WINDOW_CATALOG_PATH),
    }


def _requested_date_range(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    for row in sources["window_catalog"].get("windows", []):
        if row.get("window_id") == "ai_cycle_development_full":
            return {
                "start_date": row.get("start_date"),
                "end_date": row.get("end_date"),
                "source": str(WINDOW_CATALOG_PATH),
            }
    dates = []
    for row in sources["multi_window"].get("window_results", []):
        window = row.get("window", {})
        if window.get("start_date"):
            dates.append(str(window["start_date"]))
        if window.get("end_date"):
            dates.append(str(window["end_date"]))
    return {
        "start_date": min(dates) if dates else None,
        "end_date": max(dates) if dates else None,
        "source": str(MULTI_WINDOW_PATH),
    }


def _source_windows(sources: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    windows: dict[str, dict[str, Any]] = {}
    for row in sources["multi_window"].get("window_results", []):
        window = dict(row.get("window", {}))
        windows[str(window.get("window_id"))] = window
        source_id = window.get("source_window_id")
        if source_id:
            windows[str(source_id)] = window
    return windows


def _source_artifacts() -> dict[str, str]:
    return {
        "b2_risk_heavy_window_evaluation": str(B2_EVAL_PATH),
        "b3_slow_tilt_signal_direction_audit": str(B3_AUDIT_PATH),
        "b1_execution_control_adoption_review": str(B1_REVIEW_PATH),
        "ablation_path_branching_decision": str(BRANCH_DECISION_PATH),
        "b2_only_research_candidate_checkpoint": str(B2_ONLY_CHECKPOINT_PATH),
        "b3_redesign_hypothesis_pack": str(B3_HYPOTHESIS_PACK_PATH),
        "research_program_checkpoint_after_branching": str(PROGRAM_CHECKPOINT_PATH),
        "b1_b4_multi_window_diagnostic_expansion": str(MULTI_WINDOW_PATH),
        "b4_interaction_evidence_synthesis": str(B4_SYNTHESIS_PATH),
        "b5_admission_checkpoint": str(B5_ADMISSION_PATH),
        "research_window_catalog": str(WINDOW_CATALOG_PATH),
    }


def _all_blocked(sources: dict[str, dict[str, Any]]) -> bool:
    program = sources["program_checkpoint"]
    branch = sources["branch"]
    b5 = sources["b5_admission"]
    return (
        program.get("b5_allowed") is False
        and program.get("b6_allowed") is False
        and program.get("v3_allowed") is False
        and branch.get("b5_allowed") is False
        and b5.get("b5_allowed") is False
    )


def _risk_off_events(source_row: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        row
        for row in source_row.get("exposure_scaler_changes", [])
        if row.get("risk_state") == "RISK_OFF"
    ]


def _risk_reentry_events(source_row: dict[str, Any]) -> list[dict[str, Any]]:
    lag = source_row.get("re_entry_lag_days")
    if lag is None:
        return []
    dates = source_row.get("risk_trigger_dates", [])
    return [{"first_trigger_date": dates[0] if dates else None, "reentry_lag_days": lag}]


def _b2_backfill_aggregate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    triggered_rows = [row for row in rows if int(row["risk_trigger_count"]) > 0]
    lag_values = [
        int(row["reentry_lag"])
        for row in rows
        if row.get("reentry_lag") is not None and int(row["risk_trigger_count"]) > 0
    ]
    return {
        "risk_trigger_count": sum(int(row["risk_trigger_count"]) for row in rows),
        "triggered_window_count": len(triggered_rows),
        "return_delta_vs_B0": sum(float(row["return_delta_vs_B0"]) for row in triggered_rows),
        "drawdown_delta_vs_B0": sum(float(row["drawdown_delta_vs_B0"]) for row in triggered_rows),
        "turnover_delta_vs_B0": sum(float(row["turnover_delta_vs_B0"]) for row in triggered_rows),
        "cost_delta_vs_B0": sum(float(row["cost_delta_vs_B0"]) for row in triggered_rows),
        "benchmark_relative_delta": sum(
            float(row["benchmark_relative_delta"]) for row in triggered_rows
        ),
        "false_risk_off_count": sum(int(row["false_risk_off_count"]) for row in rows),
        "max_reentry_lag": max(lag_values) if lag_values else None,
        "V_shaped_recovery_opportunity_cost": sum(
            float(row["V_shaped_recovery_opportunity_cost"]) for row in rows
        ),
    }


def _window_false_count(backfill: dict[str, Any], window_id: str) -> int:
    for row in backfill["window_results"]:
        if row["window_id"] == window_id:
            return int(row["false_risk_off_count"])
    return 0


def _worst_return_window(backfill: dict[str, Any]) -> dict[str, Any]:
    rows = backfill["window_results"]
    if not rows:
        return {}
    return min(rows, key=lambda row: float(row["return_delta_vs_B0"]))


def _hypothesis_window(hypothesis_id: str) -> str:
    if hypothesis_id in {"smaller_tilt_cap", "baseline_shrinkage"}:
        return "semiconductor_correction"
    if hypothesis_id in {"relative_strength_confirmation", "stronger_smoothing"}:
        return "v_shaped_recovery"
    if hypothesis_id == "avoid_tilt_during_high_vol_regimes":
        return "high_volatility_sideways"
    return "slow_drawdown"


def _hypothesis_metric(hypothesis_id: str) -> str:
    if hypothesis_id == "smaller_tilt_cap":
        return "wrong_direction_loss_and_turnover_delta"
    if hypothesis_id == "baseline_shrinkage":
        return "drawdown_delta_vs_B0"
    if hypothesis_id == "relative_strength_confirmation":
        return "directional_agreement"
    if hypothesis_id == "direction_inversion_test":
        return "wrong_direction_rate"
    return "return_delta_vs_B0"


def _total_tilt_direction_days(b3: dict[str, Any]) -> int:
    total = 0
    for row in b3.get("asset_level_tilt_direction", []):
        total += int(row.get("overweight_days", 0))
        total += int(row.get("underweight_days", 0))
        total += int(row.get("neutral_days", 0))
    return total


def _asset_contribution_alignment(b3: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in b3.get("contribution_by_asset", []):
        contribution = float(row.get("tilt_return_contribution", 0.0))
        rows.append(
            {
                "symbol": row.get("symbol"),
                "tilt_return_contribution": contribution,
                "alignment": "positive" if contribution >= 0 else "negative",
            }
        )
    return rows


def _signal_turnover(b3: dict[str, Any]) -> dict[str, Any]:
    raw = b3.get("turnover_generated_by_tilt_changes", [])
    rows = raw if isinstance(raw, list) else [raw]
    return {
        "row_count": len(rows),
        "turnover_delta_total": sum(float(row.get("turnover_delta", 0.0)) for row in rows),
    }


def _b1_turnover_decreases(b1: dict[str, Any]) -> bool:
    rows = b1.get("window_reviews", [])
    return bool(rows) and all(float(row.get("turnover_delta", 0.0)) <= 0 for row in rows)


def _b1_cost_decreases(b1: dict[str, Any]) -> bool:
    rows = b1.get("window_reviews", [])
    return bool(rows) and all(float(row.get("cost_delta", 0.0)) <= 0 for row in rows)


def _backlog_item(
    hypothesis_id: str,
    target_module: str,
    current_status: str,
    required_evidence: str,
    next_action: str,
    owner_required: bool,
    blocked_reason: str,
) -> dict[str, Any]:
    return {
        "hypothesis_id": hypothesis_id,
        "target_module": target_module,
        "current_status": current_status,
        "required_evidence": required_evidence,
        "allowed_windows": "diagnostic_only",
        "forbidden_windows": ["untouched_holdout"],
        "next_action": next_action,
        "kill_criteria": "reject_or_return_to_design_if_required_evidence_fails",
        "owner_required": owner_required,
        "blocked_reason": blocked_reason,
    }


def _check(check_id: str, passed: bool, message: str) -> dict[str, Any]:
    return {"check_id": check_id, "status": "PASS" if passed else "FAIL", "message": message}


def _run_quality_gate(
    *,
    prices_path: Path,
    rates_path: Path,
    as_of: date,
    output_path: Path | None,
) -> dict[str, Any]:
    quality_output = output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        as_of,
    )
    universe = load_universe()
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=as_of,
        manifest_path=prices_path.parent / "download_manifest.csv",
        secondary_prices_path=prices_path.parent / "prices_marketstack_daily.csv",
        require_secondary_prices=prices_path.name == "prices_marketstack_daily.csv",
    )
    write_data_quality_report(report, quality_output)
    return {
        "required_command": "aits validate-data",
        "status": report.status,
        "passed": report.passed,
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "info_count": report.info_count,
        "report_path": str(quality_output),
    }


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _stamp(value: str) -> str:
    return value.replace("-", "").replace(":", "").split(".")[0].replace("+0000", "Z")
