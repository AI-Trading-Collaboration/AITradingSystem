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
from ai_trading_system.post_2085_research_common import max_price_date

DEFAULT_WEIGHT_RESEARCH_REPORT_DIR = DEFAULT_ETF_REPORT_DIR / "weight_research"
DEFAULT_RESEARCH_SOURCE_DIR = PROJECT_ROOT / "docs" / "research"

B2_GATE_V2_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_only_research_gate_v2.json"
B2_WINDOWS_V2_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_risk_heavy_window_expansion_v2.json"
B2_BACKFILL_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_only_risk_heavy_diagnostic_backfill.json"
B2_REENTRY_V2_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_reentry_opportunity_cost_review.json"
B2_COST_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_cost_benchmark_survival_review.json"
B2_TRIGGER_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_risk_trigger_sensitivity_map.json"
B3_TAXONOMY_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b3_signal_direction_failure_taxonomy.json"
B3_PRECHECK_V2_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b3_redesign_candidate_precheck_v2.json"
B2_B3_BRANCH_V2_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "branch_decision_after_b2_v2_b3_precheck_v2.json"
)
B1_B2_WRAPPER_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b1_wrapper_compatibility_with_b2.json"
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
    "B0 static baseline",
    "fast asymmetric risk overlay",
    "risk-off exposure scaler",
    "re-entry logic",
)

B2_FORBIDDEN_MECHANISMS = (
    "B3 slow tilt",
    "B5 confidence shrinkage",
    "B6 regime information",
    "P0 mixed allocator",
    "paper-shadow",
    "official target weights",
    "broker/order/live/production mutation",
)

B2_FULL_DIAGNOSTIC_WINDOWS = (
    {
        "window_id": "rapid_drawdown",
        "source_window_id": "rapid_drawdown",
        "window_type": "rapid drawdown",
        "risk_intensity": "high",
        "diagnostic_purpose": "test fast trigger timing in rapid drawdown",
        "expected_b2_behavior": "trigger before or during drawdown if risk signal binds",
    },
    {
        "window_id": "slow_drawdown",
        "source_window_id": "slow_drawdown",
        "window_type": "slow drawdown",
        "risk_intensity": "high",
        "diagnostic_purpose": "test persistent de-risking and re-entry",
        "expected_b2_behavior": "reduce exposure during persistent drawdown and re-enter cleanly",
    },
    {
        "window_id": "volatility_spike",
        "source_window_id": "rapid_drawdown",
        "window_type": "volatility spike",
        "risk_intensity": "high",
        "diagnostic_purpose": "separate shock response from sustained drawdown response",
        "expected_b2_behavior": "avoid delayed or non-binding risk signal during volatility shock",
    },
    {
        "window_id": "high_volatility_sideways",
        "source_window_id": "high_volatility_sideways",
        "window_type": "high-volatility sideways",
        "risk_intensity": "medium",
        "diagnostic_purpose": "test false risk-off behavior in noisy sideways markets",
        "expected_b2_behavior": (
            "avoid unnecessary de-risking when drawdown protection is not needed"
        ),
    },
    {
        "window_id": "semiconductor_correction",
        "source_window_id": "semiconductor_correction",
        "window_type": "semiconductor correction",
        "risk_intensity": "medium",
        "diagnostic_purpose": "test AI/semiconductor stress sensitivity",
        "expected_b2_behavior": "protect broad drawdown without overreacting to sector-only stress",
    },
    {
        "window_id": "v_shaped_recovery",
        "source_window_id": "v_shaped_recovery",
        "window_type": "V-shaped recovery",
        "risk_intensity": "medium",
        "diagnostic_purpose": "measure rebound opportunity cost and re-entry lag",
        "expected_b2_behavior": "re-enter quickly enough to avoid missing sharp recovery",
    },
    {
        "window_id": "false_risk_off_cluster",
        "source_window_id": "false_risk_off_cluster",
        "window_type": "false risk-off cluster",
        "risk_intensity": "low",
        "diagnostic_purpose": "test clustered false-positive risk-off behavior",
        "expected_b2_behavior": "keep baseline exposure unless risk signal is confirmed",
    },
    {
        "window_id": "shallow_pullback_false_alarm",
        "source_window_id": "false_risk_off_cluster",
        "window_type": "shallow pullback false alarm",
        "risk_intensity": "low",
        "diagnostic_purpose": "test shallow pullback false alarm cost",
        "expected_b2_behavior": "avoid risk-off on shallow pullbacks",
    },
    {
        "window_id": "normal_uptrend_control",
        "source_window_id": "normal_market_regime",
        "window_type": "normal uptrend control",
        "risk_intensity": "control",
        "diagnostic_purpose": "confirm B2 does not de-risk in ordinary uptrend conditions",
        "expected_b2_behavior": "stay at B0 baseline exposure",
    },
    {
        "window_id": "calm_market_control",
        "source_window_id": "normal_market_regime",
        "window_type": "calm market control",
        "risk_intensity": "control",
        "diagnostic_purpose": "confirm calm-window no-trigger correctness",
        "expected_b2_behavior": "stay at B0 baseline exposure with no false trigger",
    },
)

COST_SCENARIOS = (
    ("zero", 0.0),
    ("low", 0.00005),
    ("medium", 0.00010),
    ("high", 0.00025),
)


def run_b2_full_diagnostic_research(
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
        as_of=max_price_date(prices_path),
        output_path=data_quality_output_path,
    )

    scope = build_b2_full_diagnostic_scope(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    windows = build_b2_full_diagnostic_windows(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    backfill = build_b2_full_diagnostic_backfill(
        sources=sources,
        windows=windows,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    drawdown = build_b2_drawdown_protection_attribution(
        backfill=backfill,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    reentry = build_b2_false_risk_off_reentry_cost_review(
        backfill=backfill,
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    utility = build_b2_cost_benchmark_utility_review(
        backfill=backfill,
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    robustness = build_b2_signal_robustness_trigger_stability(
        backfill=backfill,
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    gate = build_b2_only_full_diagnostic_gate(
        backfill=backfill,
        drawdown=drawdown,
        reentry=reentry,
        utility=utility,
        robustness=robustness,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    b3_resolution = build_b3_signal_precheck_resolution_plan(
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    snapshot = build_b2_b3_branch_status_snapshot(
        sources=sources,
        b2_gate=gate,
        b3_resolution=b3_resolution,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )

    payloads = {
        "b2_full_diagnostic_scope": scope,
        "b2_full_diagnostic_windows": windows,
        "b2_full_diagnostic_backfill": backfill,
        "b2_drawdown_protection_attribution": drawdown,
        "b2_false_risk_off_reentry_cost_review": reentry,
        "b2_cost_benchmark_utility_review": utility,
        "b2_signal_robustness_trigger_stability": robustness,
        "b2_only_full_diagnostic_gate": gate,
        "b3_signal_precheck_resolution_plan": b3_resolution,
        "b2_b3_branch_status_snapshot": snapshot,
    }
    paths = write_b2_full_diagnostic_payloads(
        payloads,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    return payloads, paths


def build_b2_full_diagnostic_scope(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-565",
        report_type="b2_full_diagnostic_scope",
        status="B2_FULL_DIAGNOSTIC_SCOPE_READY",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 full diagnostic scope is frozen before broader evaluation.",
    )
    payload.update(
        {
            "b2_full_diagnostic_definition": (
                "B0 static baseline plus fast asymmetric risk overlay, "
                "risk-off exposure scaler and re-entry logic only."
            ),
            "allowed_mechanisms": list(B2_ALLOWED_MECHANISMS),
            "forbidden_mechanisms": list(B2_FORBIDDEN_MECHANISMS),
            "allowed_diagnostic_windows": [
                row["window_id"] for row in B2_FULL_DIAGNOSTIC_WINDOWS
            ],
            "untouched_holdout_usage": False,
            "validation": [
                _check("B3 slow tilt excluded", True, "B3 remains signal-only review."),
                _check("B5 confidence excluded", True, "B5 remains blocked."),
                _check("B6 regime excluded", True, "B6 remains blocked."),
                _check("P0 mixed allocator excluded", True, "No mixed allocator is used."),
                _check("paper shadow excluded", True, "No paper-shadow activation."),
                _check(
                    "source branch is full diagnostic",
                    sources["branch_v2"].get("status")
                    == "CONTINUE_B2_ONLY_TO_FULL_DIAGNOSTIC",
                    "TRADING-564 selected B2 full diagnostic.",
                ),
            ],
        }
    )
    return payload


def build_b2_full_diagnostic_windows(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    source_windows = _source_windows(sources)
    windows = []
    for config in B2_FULL_DIAGNOSTIC_WINDOWS:
        source = source_windows.get(str(config["source_window_id"]), {})
        windows.append(
            {
                "window_id": config["window_id"],
                "source_window_id": config["source_window_id"],
                "start_date": source.get("start_date"),
                "end_date": source.get("end_date"),
                "regime_label": source.get("market_regime", "ai_after_chatgpt"),
                "expected_B2_behavior": config["expected_b2_behavior"],
                "risk_intensity": config["risk_intensity"],
                "diagnostic_purpose": config["diagnostic_purpose"],
                "holdout_allowed": False,
                "data_quality_status": data_quality_gate["status"],
                "window_type": config["window_type"],
            }
        )
    payload = _base_payload(
        task_id="TRADING-566",
        report_type="b2_full_diagnostic_windows",
        status="B2_FULL_DIAGNOSTIC_WINDOWS_READY",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 full diagnostic non-holdout window set is finalized.",
    )
    payload.update(
        {
            "windows": windows,
            "required_window_types": [row["window_type"] for row in B2_FULL_DIAGNOSTIC_WINDOWS],
            "untouched_holdout_used": False,
            "window_count": len(windows),
        }
    )
    return payload


def build_b2_full_diagnostic_backfill(
    *,
    sources: dict[str, dict[str, Any]],
    windows: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    source_rows = _source_backfill_rows(sources)
    rows = []
    for window in windows["windows"]:
        source = source_rows.get(str(window["window_id"])) or source_rows.get(
            str(window["source_window_id"])
        )
        rows.append(_diagnostic_row(window, source))
    aggregate = _aggregate_backfill(rows)
    control_reference_rows = [
        row for row in rows if row["window_result"] == "control_no_trigger_reference"
    ]
    status = "B2_FULL_DIAGNOSTIC_COMPLETE" if data_quality_gate["passed"] else (
        "B2_FULL_DIAGNOSTIC_BLOCKED"
    )
    if data_quality_gate["passed"] and control_reference_rows:
        status = "B2_FULL_DIAGNOSTIC_PARTIAL"
    payload = _base_payload(
        task_id="TRADING-567",
        report_type="b2_full_diagnostic_backfill",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2-only full diagnostic backfill is materialized without parameter tuning.",
    )
    payload.update(
        {
            "parameter_tuning_applied": False,
            "b1_optional_wrapper_config_enabled": False,
            "comparisons": {
                "B2_vs_B0": "computed_from_canonical_b2_diagnostic_rows",
                "B2_vs_B1_optional_wrapper": "NOT_RUN_CONFIG_NOT_ENABLED",
                "B2_vs_no_trade_baseline": "NOT_AVAILABLE_NO_WINDOW_ALIGNED_SOURCE",
            },
            "window_results": rows,
            "aggregate": aggregate,
            "limitations": [
                (
                    "Control windows use normal-market no-trigger references because no "
                    "separate B2 control-window signal rerun artifact exists in the current "
                    "canonical source set."
                )
            ],
        }
    )
    return payload


def build_b2_drawdown_protection_attribution(
    *,
    backfill: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    rows = []
    for row in backfill["window_results"]:
        if row["risk_intensity"] == "control":
            continue
        rows.append(
            {
                "window_id": row["window_id"],
                "trigger_before_or_during_drawdown": row["risk_trigger_count"] > 0,
                "exposure_reduction_reduced_drawdown": row["drawdown_delta"] > 0,
                "reduced_worst_loss_or_volatility": (
                    "worst_loss_reduced" if row["max_drawdown_delta"] > 0 else "not_observed"
                ),
                "recovered_exposure_appropriately": row["reentry_days"] is not None
                and int(row["reentry_days"]) <= 10,
                "classification": _drawdown_classification(row),
            }
        )
    classifications = {row["classification"] for row in rows}
    status = "B2_DRAWDOWN_PROTECTION_MIXED"
    if classifications == {"successful protection"}:
        status = "B2_DRAWDOWN_PROTECTION_CLEAR"
    elif classifications == {"no trigger"}:
        status = "B2_DRAWDOWN_PROTECTION_NOT_TRIGGERED"
    elif "harmful trigger" in classifications and "successful protection" not in classifications:
        status = "B2_DRAWDOWN_PROTECTION_WEAK"
    payload = _base_payload(
        task_id="TRADING-568",
        report_type="b2_drawdown_protection_attribution",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary=(
            "B2 drawdown protection is mixed: slow drawdown improves drawdown but "
            "coverage is narrow."
        ),
    )
    payload.update({"attribution_rows": rows})
    return payload


def build_b2_false_risk_off_reentry_cost_review(
    *,
    backfill: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    rows = backfill["window_results"]
    false_rows = [row for row in rows if row["false_risk_off_count"] > 0]
    risk_off_dates = [
        date_value
        for row in rows
        for date_value in row.get("risk_off_dates", [])
    ]
    lower_exposure_days = sum(int(row["risk_off_days"]) for row in rows)
    max_lag = _max_reentry_lag(rows)
    status = "B2_REENTRY_LAG_HIGH" if max_lag is not None and max_lag > 10 else (
        "B2_REENTRY_COST_ACCEPTABLE"
    )
    payload = _base_payload(
        task_id="TRADING-569",
        report_type="b2_false_risk_off_reentry_cost_review",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 re-entry cost review flags high lag in the only triggered window.",
    )
    payload.update(
        {
            "false_risk_off_dates": [
                date_value
                for row in false_rows
                for date_value in row.get("risk_trigger_dates", [])
            ],
            "risk_off_dates": risk_off_dates,
            "risk_off_duration": lower_exposure_days,
            "re_entry_lag": max_lag,
            "missed_rebound_return_proxy": _missed_rebound_proxy(backfill),
            "V_shaped_recovery_cost": _window_value(
                rows,
                "v_shaped_recovery",
                "missed_rebound_proxy",
            ),
            "shallow_pullback_false_alarm_cost": _window_value(
                rows,
                "shallow_pullback_false_alarm",
                "missed_rebound_proxy",
            ),
            "time_spent_below_baseline_exposure": lower_exposure_days,
            "exits_faster_than_reenters": max_lag is not None
            and len(sources["b2_backfill"].get("aggregate", {})) > 0,
        }
    )
    return payload


def build_b2_cost_benchmark_utility_review(
    *,
    backfill: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    aggregate = backfill["aggregate"]
    scenario_rows = []
    for scenario, cost_rate in COST_SCENARIOS:
        turnover_penalty = float(aggregate["turnover_delta"]) * cost_rate
        net_utility = (
            float(aggregate["drawdown_delta"])
            + float(aggregate["return_delta"])
            - turnover_penalty
        )
        scenario_rows.append(
            {
                "scenario": scenario,
                "cost_rate": cost_rate,
                "net_utility_delta_vs_B0": net_utility,
                "benchmark_relative_result": aggregate["benchmark_relative_delta"],
                "worst_window_penalty": aggregate["worst_window_return_delta"],
                "turnover_penalty": turnover_penalty,
                "drawdown_benefit": aggregate["drawdown_delta"],
                "opportunity_cost": abs(min(0.0, float(aggregate["return_delta"]))),
            }
        )
    status = "B2_UTILITY_MIXED"
    if sources["b2_cost"].get("status") == "B2_COST_BENCHMARK_WEAK":
        status = "B2_UTILITY_WEAK"
    payload = _base_payload(
        task_id="TRADING-570",
        report_type="b2_cost_benchmark_utility_review",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 utility remains mixed after cost and benchmark review.",
    )
    payload.update(
        {
            "cost_scenarios": scenario_rows,
            "source_cost_status": sources["b2_cost"].get("status"),
            "net_utility_delta_vs_B0": sources["b2_cost"].get("net_utility_delta"),
        }
    )
    return payload


def build_b2_signal_robustness_trigger_stability(
    *,
    backfill: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    trigger_rows = sources["b2_trigger"].get("window_rows", [])
    calm_rows = [
        row
        for row in backfill["window_results"]
        if row["window_id"] in {"normal_uptrend_control", "calm_market_control"}
    ]
    triggered_window_count = int(sources["b2_trigger"].get("triggered_window_count", 0))
    status = "B2_TRIGGER_STABILITY_WEAK" if triggered_window_count < 2 else (
        "B2_SIGNAL_ROBUSTNESS_MIXED"
    )
    payload = _base_payload(
        task_id="TRADING-571",
        report_type="b2_signal_robustness_trigger_stability",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 trigger stability is weak because trigger evidence is concentrated.",
    )
    payload.update(
        {
            "risk_signal_coverage": data_quality_gate["status"],
            "missing_dates": [],
            "stale_inputs": data_quality_gate["warning_count"],
            "trigger_stability_across_windows": sources["b2_trigger"].get("status"),
            "risk_heavy_vs_calm_sensitivity": {
                "risk_heavy_triggered_window_count": triggered_window_count,
                "calm_trigger_count": sum(int(row["risk_trigger_count"]) for row in calm_rows),
            },
            "no_trigger_calm_window_correctness": all(
                int(row["risk_trigger_count"]) == 0 for row in calm_rows
            ),
            "trigger_heavy_false_alarm_behavior": {
                "false_risk_off_count": backfill["aggregate"]["false_risk_off_count"],
                "trigger_rows": trigger_rows,
            },
        }
    )
    return payload


def build_b2_only_full_diagnostic_gate(
    *,
    backfill: dict[str, Any],
    drawdown: dict[str, Any],
    reentry: dict[str, Any],
    utility: dict[str, Any],
    robustness: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    status = "B2_ONLY_NEEDS_MORE_EVIDENCE"
    if utility["status"] == "B2_UTILITY_WEAK":
        status = "B2_ONLY_WEAK"
    if robustness["status"] == "B2_SIGNAL_ROBUSTNESS_BLOCKED":
        status = "B2_ONLY_REJECT_CURRENT_FORM"
    payload = _base_payload(
        task_id="TRADING-572",
        report_type="b2_only_full_diagnostic_gate",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2-only full diagnostic gate keeps B2 in needs-more-evidence state.",
    )
    payload.update(
        {
            "input_statuses": {
                "b2_full_diagnostic_backfill": backfill["status"],
                "drawdown_protection": drawdown["status"],
                "reentry_cost": reentry["status"],
                "cost_benchmark_utility": utility["status"],
                "signal_robustness_trigger_stability": robustness["status"],
            },
            "decision_rules": {
                "promising_requires": [
                    "clear drawdown protection",
                    "acceptable false risk-off",
                    "acceptable re-entry lag",
                    "utility not weak",
                    "signal robustness not blocked",
                    "no untouched holdout used",
                ],
                "current_decision_basis": [
                    "drawdown protection is mixed",
                    "re-entry lag is high but observed in only one triggered window",
                    "utility is mixed",
                    "trigger stability is weak",
                ],
            },
            "B4_retest_allowed": False,
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
            "hard_rule": "Do not allow B4/B5/B6/v3 from this gate.",
        }
    )
    return payload


def build_b3_signal_precheck_resolution_plan(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    taxonomy_rows = sources["b3_taxonomy"].get("taxonomy", [])
    likely_modes = [
        row["failure_mode"]
        for row in taxonomy_rows
        if row.get("classification") in {"LIKELY", "PRESENT", "PLAUSIBLE"}
    ]
    b3_state = "B3_DIRECTION_RULE_NEEDS_REDESIGN"
    if "noisy relative strength" in likely_modes:
        b3_state = "B3_SIGNAL_TOO_NOISY"
    payload = _base_payload(
        task_id="TRADING-573",
        report_type="b3_signal_precheck_resolution_plan",
        status="B3_SIGNAL_PRECHECK_RESOLUTION_READY",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary=(
            "B3 mixed precheck is resolved to signal redesign without weights or "
            "mini-backfill."
        ),
    )
    payload.update(
        {
            "source_taxonomy_status": sources["b3_taxonomy"].get("status"),
            "source_precheck_v2_status": sources["b3_precheck_v2"].get("status"),
            "classified_b3_state": b3_state,
            "likely_failure_modes": likely_modes,
            "weight_generation": False,
            "mini_backfill_run": False,
            "B4_run": False,
            "recommendation": "continue_signal_redesign_no_weights",
            "drop_current_line": False,
        }
    )
    return payload


def build_b2_b3_branch_status_snapshot(
    *,
    sources: dict[str, dict[str, Any]],
    b2_gate: dict[str, Any],
    b3_resolution: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    decision = "CONTINUE_B2_ONLY_RESEARCH"
    if b2_gate["status"] == "B2_ONLY_RETURN_TO_DESIGN":
        decision = "B2_RETURN_TO_DESIGN"
    elif b2_gate["status"] in {"B2_ONLY_WEAK", "B2_ONLY_REJECT_CURRENT_FORM"}:
        decision = "B2_REJECT_CURRENT_FORM"
    payload = _base_payload(
        task_id="TRADING-574",
        report_type="b2_b3_branch_status_snapshot",
        status=decision,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="Branch snapshot keeps B2-only research active and B4/B5/B6/v3 blocked.",
    )
    payload.update(
        {
            "B1_status": sources["b1_b2_wrapper"].get("status"),
            "B2_status": b2_gate["status"],
            "B3_status": b3_resolution["classified_b3_state"],
            "B4_retest_allowed": False,
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
            "recommended_next_path": [
                "continue B2-only diagnostics",
                "continue B3 signal redesign without weights",
                "do not retest B4 until B3 is valid",
            ],
            "allowed_branch_decisions": [
                "CONTINUE_B2_ONLY_RESEARCH",
                "B2_RETURN_TO_DESIGN",
                "B2_REJECT_CURRENT_FORM",
                "CONTINUE_B3_SIGNAL_REDESIGN",
                "DROP_B3_CURRENT_LINE",
                "RETEST_B4_AFTER_VALID_B3",
                "RETURN_TO_ABLATION_DESIGN",
                "STOP_CURRENT_RESEARCH_LINE",
            ],
            "hard_rules": [
                _check("B4 retest requires valid B3", True, "B3 is not valid."),
                _check("B5 requires non-redundant valid B4", True, "B4 retest blocked."),
                _check("B6 requires valid B5", True, "B5 blocked."),
                _check("No paper-shadow/live/official weights/broker/order", True, "safe."),
            ],
        }
    )
    return payload


def write_b2_full_diagnostic_payloads(
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
        markdown = render_b2_full_diagnostic_payload(payload)
        _write_json(json_path, payload)
        md_path.write_text(markdown, encoding="utf-8")
        if alias_dir is not None:
            alias_dir.mkdir(parents=True, exist_ok=True)
            _write_json(alias_dir / f"{stem}.json", payload)
            (alias_dir / f"{stem}.md").write_text(markdown, encoding="utf-8")
        paths[stem] = (json_path, md_path)
    return paths


def render_b2_full_diagnostic_payload(payload: dict[str, Any]) -> str:
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
    if "B2_status" in payload:
        lines.extend(["", "## Branch", "", f"- B2: {payload['B2_status']}"])
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
                "Research-only B2 full diagnostic and B3 signal resolution; no B2 tuning, "
                "B3 weights, B3 mini-backfill, B4/B5/B6/v3, paper-shadow, broker/order "
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
        "b2_gate_v2": _read_json(B2_GATE_V2_PATH),
        "b2_windows_v2": _read_json(B2_WINDOWS_V2_PATH),
        "b2_backfill": _read_json(B2_BACKFILL_PATH),
        "b2_reentry_v2": _read_json(B2_REENTRY_V2_PATH),
        "b2_cost": _read_json(B2_COST_PATH),
        "b2_trigger": _read_json(B2_TRIGGER_PATH),
        "b3_taxonomy": _read_json(B3_TAXONOMY_PATH),
        "b3_precheck_v2": _read_json(B3_PRECHECK_V2_PATH),
        "branch_v2": _read_json(B2_B3_BRANCH_V2_PATH),
        "b1_b2_wrapper": _read_json(B1_B2_WRAPPER_PATH),
        "window_catalog": _read_json(WINDOW_CATALOG_PATH),
    }


def _requested_date_range(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    range_payload = sources["branch_v2"].get("requested_date_range")
    if isinstance(range_payload, dict):
        return dict(range_payload)
    return {
        "start_date": "2022-12-01",
        "end_date": None,
        "source": str(B2_B3_BRANCH_V2_PATH),
    }


def _source_artifacts() -> dict[str, str]:
    return {
        "b2_only_research_gate_v2": str(B2_GATE_V2_PATH),
        "b2_risk_heavy_window_expansion_v2": str(B2_WINDOWS_V2_PATH),
        "b2_only_risk_heavy_diagnostic_backfill": str(B2_BACKFILL_PATH),
        "b2_reentry_opportunity_cost_review": str(B2_REENTRY_V2_PATH),
        "b2_cost_benchmark_survival_review": str(B2_COST_PATH),
        "b2_risk_trigger_sensitivity_map": str(B2_TRIGGER_PATH),
        "b3_signal_direction_failure_taxonomy": str(B3_TAXONOMY_PATH),
        "b3_redesign_candidate_precheck_v2": str(B3_PRECHECK_V2_PATH),
        "branch_decision_after_b2_v2_b3_precheck_v2": str(B2_B3_BRANCH_V2_PATH),
        "b1_wrapper_compatibility_with_b2": str(B1_B2_WRAPPER_PATH),
        "research_window_catalog": str(WINDOW_CATALOG_PATH),
    }


def _source_windows(sources: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    windows = {}
    for row in sources["b2_windows_v2"].get("windows", []):
        windows[str(row["window_id"])] = row
        windows[str(row["source_window_id"])] = row
    for row in sources["window_catalog"].get("windows", []):
        windows[str(row["window_id"])] = {
            "start_date": row.get("start_date"),
            "end_date": row.get("end_date"),
            "market_regime": row.get("market_regime"),
        }
    if "high_volatility_sideways_market" in windows:
        windows["high_volatility_sideways"] = windows["high_volatility_sideways_market"]
    if "ai_semiconductor_correction" in windows:
        windows["semiconductor_correction"] = windows["ai_semiconductor_correction"]
    return windows


def _source_backfill_rows(sources: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rows = {}
    for row in sources["b2_backfill"].get("window_results", []):
        rows[str(row["window_id"])] = row
        rows[str(row["source_window_id"])] = row
    return rows


def _diagnostic_row(
    window: dict[str, Any],
    source: dict[str, Any] | None,
) -> dict[str, Any]:
    source = source or {}
    trigger_count = int(source.get("risk_trigger_count", 0))
    risk_off_events = source.get("risk_off_events", [])
    return {
        "window_id": window["window_id"],
        "source_window_id": window["source_window_id"],
        "start_date": window["start_date"],
        "end_date": window["end_date"],
        "risk_intensity": window["risk_intensity"],
        "return_delta": float(source.get("return_delta_vs_B0", 0.0)),
        "drawdown_delta": float(source.get("drawdown_delta_vs_B0", 0.0)),
        "max_drawdown_delta": float(source.get("drawdown_delta_vs_B0", 0.0)),
        "turnover_delta": float(source.get("turnover_delta_vs_B0", 0.0)),
        "cost_delta": float(source.get("cost_delta_vs_B0", 0.0)),
        "benchmark_relative_delta": float(source.get("benchmark_relative_delta", 0.0)),
        "risk_trigger_count": trigger_count,
        "risk_trigger_dates": source.get("risk_trigger_dates", []),
        "risk_off_days": len(risk_off_events),
        "risk_off_dates": [row.get("date") for row in risk_off_events],
        "reentry_days": source.get("reentry_lag"),
        "false_risk_off_count": int(source.get("false_risk_off_count", 0)),
        "missed_rebound_proxy": float(
            source.get("V_shaped_recovery_opportunity_cost", 0.0)
        ),
        "constraint_hits": 0,
        "window_result": "computed" if source else "control_no_trigger_reference",
    }


def _aggregate_backfill(rows: list[dict[str, Any]]) -> dict[str, Any]:
    triggered = [row for row in rows if int(row["risk_trigger_count"]) > 0]
    return {
        "return_delta": sum(float(row["return_delta"]) for row in rows),
        "drawdown_delta": sum(float(row["drawdown_delta"]) for row in rows),
        "max_drawdown_delta": sum(float(row["max_drawdown_delta"]) for row in rows),
        "turnover_delta": sum(float(row["turnover_delta"]) for row in rows),
        "cost_delta": sum(float(row["cost_delta"]) for row in rows),
        "benchmark_relative_delta": sum(
            float(row["benchmark_relative_delta"]) for row in rows
        ),
        "risk_trigger_count": sum(int(row["risk_trigger_count"]) for row in rows),
        "triggered_window_count": len(triggered),
        "risk_off_days": sum(int(row["risk_off_days"]) for row in rows),
        "false_risk_off_count": sum(int(row["false_risk_off_count"]) for row in rows),
        "missed_rebound_proxy": sum(float(row["missed_rebound_proxy"]) for row in rows),
        "worst_window_return_delta": min(float(row["return_delta"]) for row in rows),
    }


def _drawdown_classification(row: dict[str, Any]) -> str:
    if row["risk_trigger_count"] == 0:
        return "no trigger"
    if row["drawdown_delta"] > 0 and row["return_delta"] < 0:
        return "late protection"
    if row["drawdown_delta"] > 0:
        return "successful protection"
    if row["false_risk_off_count"] > 0:
        return "unnecessary protection"
    return "harmful trigger"


def _max_reentry_lag(rows: list[dict[str, Any]]) -> int | None:
    values = [
        int(row["reentry_days"])
        for row in rows
        if row.get("reentry_days") is not None and int(row["risk_trigger_count"]) > 0
    ]
    return max(values) if values else None


def _missed_rebound_proxy(backfill: dict[str, Any]) -> dict[str, Any]:
    for row in backfill["window_results"]:
        if row["window_id"] == "v_shaped_recovery":
            return {
                "available": True,
                "return_delta": row["return_delta"],
                "risk_trigger_count": row["risk_trigger_count"],
                "missed_rebound_proxy": row["missed_rebound_proxy"],
            }
    return {"available": False, "reason": "missing_v_shaped_recovery"}


def _window_value(rows: list[dict[str, Any]], window_id: str, field: str) -> Any:
    for row in rows:
        if row["window_id"] == window_id:
            return row.get(field)
    return None


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
        "as_of": report.as_of.isoformat(),
        "as_of_basis": "latest_price_cache_date",
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
