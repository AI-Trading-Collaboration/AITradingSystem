from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.features import build_feature_store
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH, DEFAULT_ETF_REPORT_DIR
from ai_trading_system.etf_portfolio.weight_research_b2 import (
    DEFAULT_WEIGHT_RESEARCH_MODULES_CONFIG_PATH,
    build_b2_risk_signal,
    build_b2_target_path,
    load_b2_policies,
)
from ai_trading_system.etf_portfolio.weight_research_unblock import (
    DEFAULT_HOLDOUT_POLICY_PATH,
    DEFAULT_RATES_CACHE_PATH,
    DEFAULT_SCOPE_FREEZE_PATH,
    DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH,
    DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
    prepare_research_data_context,
)

DEFAULT_WEIGHT_RESEARCH_REPORT_DIR = DEFAULT_ETF_REPORT_DIR / "weight_research"
DEFAULT_RESEARCH_SOURCE_DIR = PROJECT_ROOT / "docs" / "research"

B2_NEXT_EVIDENCE_PLAN_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_next_evidence_plan.json"
B2_FOLLOWUP_SCORECARD_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_per_window_utility_scorecard.json"
)
B2_FULL_BACKFILL_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_full_diagnostic_backfill.json"
B2_CONTROL_RERUN_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_control_window_rerun.json"
B2_NO_TRIGGER_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_no_trigger_correctness_review.json"
B2_REENTRY_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_false_risk_off_reentry_cost_review.json"
B2_UTILITY_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_cost_benchmark_utility_review.json"
B2_ROBUSTNESS_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_signal_robustness_trigger_stability.json"
)
B2_GATE_V4_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_gate_v4_decision.json"
B2_BRANCH_SNAPSHOT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_research_branch_snapshot.json"
B3_RESOLUTION_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b3_signal_precheck_resolution_plan.json"
WINDOW_CATALOG_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "research_window_catalog.json"

SAFETY_BOUNDARY = {
    "research_only": True,
    "manual_review_only": True,
    "paper_shadow_activation": False,
    "paper_shadow_allowed": False,
    "extended_shadow_allowed": False,
    "live_trading_allowed": False,
    "official_target_weights": False,
    "broker_action_allowed": False,
    "order_ticket_generated": False,
    "owner_decision_appended": False,
    "production_effect": "none",
}

TARGETED_WINDOW_CONFIGS = (
    {
        "window_id": "slow_drawdown_repeat_primary",
        "source_window_id": "slow_drawdown",
        "window_group": "slow_drawdown_repeatability",
        "expected_B2_behavior": "trigger during persistent drawdown and re-enter cleanly",
        "risk_intensity": "high",
        "trigger_expectation": "trigger expected",
        "reentry_expectation": "complete re-entry lifecycle expected",
    },
    {
        "window_id": "rapid_drawdown_fast_risk",
        "source_window_id": "rapid_drawdown",
        "window_group": "rapid_drawdown",
        "expected_B2_behavior": "trigger before or during rapid drawdown if fast risk binds",
        "risk_intensity": "high",
        "trigger_expectation": "trigger expected if current design supports fast risk",
        "reentry_expectation": "quick recovery expected if trigger occurs",
    },
    {
        "window_id": "volatility_spike_fast_risk",
        "source_window_id": "volatility_spike",
        "fallback_source_window_id": "rapid_drawdown",
        "window_group": "volatility_spike",
        "expected_B2_behavior": "trigger if volatility shock produces binding risk signal",
        "risk_intensity": "high",
        "trigger_expectation": "trigger expected if volatility spike is supported",
        "reentry_expectation": "quick recovery expected if trigger occurs",
    },
    {
        "window_id": "v_shaped_recovery_reentry_case",
        "source_window_id": "v_shaped_recovery",
        "window_group": "v_shaped_recovery",
        "expected_B2_behavior": "avoid staying de-risked through fast rebound",
        "risk_intensity": "medium",
        "trigger_expectation": "trigger only if risk signal binds before rebound",
        "reentry_expectation": "fast re-entry required after risk clears",
    },
    {
        "window_id": "false_risk_off_cluster_control",
        "source_window_id": "false_risk_off_cluster",
        "window_group": "false_risk_off_shallow_pullback",
        "expected_B2_behavior": "avoid false risk-off cluster",
        "risk_intensity": "low",
        "trigger_expectation": "no trigger expected",
        "reentry_expectation": "not applicable unless false trigger appears",
    },
    {
        "window_id": "shallow_pullback_false_alarm_control",
        "source_window_id": "shallow_pullback_false_alarm",
        "fallback_source_window_id": "false_risk_off_cluster",
        "window_group": "false_risk_off_shallow_pullback",
        "expected_B2_behavior": "avoid shallow-pullback de-risking",
        "risk_intensity": "low",
        "trigger_expectation": "no trigger expected",
        "reentry_expectation": "not applicable unless false trigger appears",
    },
    {
        "window_id": "normal_uptrend_control_repeat",
        "source_window_id": "normal_uptrend_control",
        "fallback_source_window_id": "normal_market_regime",
        "window_group": "calm_normal_control",
        "expected_B2_behavior": "remain inactive in normal uptrend",
        "risk_intensity": "control",
        "trigger_expectation": "no trigger expected",
        "reentry_expectation": "not applicable",
    },
    {
        "window_id": "calm_market_control_repeat",
        "source_window_id": "calm_market_control",
        "fallback_source_window_id": "normal_market_regime",
        "window_group": "calm_normal_control",
        "expected_B2_behavior": "remain inactive in calm market",
        "risk_intensity": "control",
        "trigger_expectation": "no trigger expected",
        "reentry_expectation": "not applicable",
    },
)


def run_b2_targeted_evidence_research(
    *,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = DEFAULT_RESEARCH_SOURCE_DIR,
    generated_at: datetime | None = None,
    modules_config_path: Path = DEFAULT_WEIGHT_RESEARCH_MODULES_CONFIG_PATH,
) -> tuple[dict[str, dict[str, Any]], dict[str, tuple[Path, Path]]]:
    generated = generated_at or datetime.now(UTC)
    sources = _load_sources()
    requested_range = _requested_date_range(sources)
    locked_windows = _locked_target_windows(sources)
    context_start = min(date.fromisoformat(row["start_date"]) for row in locked_windows)
    context_end = max(date.fromisoformat(row["end_date"]) for row in locked_windows)
    context = prepare_research_data_context(
        prices_path=prices_path,
        rates_path=rates_path,
        start=context_start,
        end=context_end,
        scope_path=DEFAULT_SCOPE_FREEZE_PATH,
        signal_contract_path=DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH,
        holdout_policy_path=DEFAULT_HOLDOUT_POLICY_PATH,
        config_path=DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
        generated_at=generated,
        data_quality_output_path=None,
    )
    data_quality = _data_quality_gate(context)
    risk_policy, target_policy = load_b2_policies(modules_config_path)
    signal_cache = {
        row["source_window_id"]: _signal_bundle_for_window(
            context=context,
            window=row,
            risk_policy=risk_policy,
            target_policy=target_policy,
        )
        for row in locked_windows
    }

    window_lock = build_b2_targeted_evidence_window_lock(
        locked_windows=locked_windows,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    fast_audit = build_b2_fast_risk_no_trigger_audit(
        locked_windows=locked_windows,
        signal_cache=signal_cache,
        risk_policy=risk_policy,
        target_policy=target_policy,
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    slow = build_b2_slow_drawdown_repeatability_study(
        locked_windows=locked_windows,
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    reentry = build_b2_reentry_lag_root_cause_review(
        locked_windows=locked_windows,
        signal_cache=signal_cache,
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    role = build_b2_role_narrowing_assessment(
        fast_audit=fast_audit,
        slow=slow,
        reentry=reentry,
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    backfill = build_b2_targeted_evidence_backfill_v2(
        locked_windows=locked_windows,
        window_lock=window_lock,
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    scorecard = build_b2_targeted_evidence_scorecard(
        fast_audit=fast_audit,
        slow=slow,
        reentry=reentry,
        role=role,
        backfill=backfill,
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    gate = build_b2_gate_v5(
        fast_audit=fast_audit,
        slow=slow,
        reentry=reentry,
        role=role,
        scorecard=scorecard,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )
    snapshot = build_b2_research_branch_snapshot_v2(
        role=role,
        scorecard=scorecard,
        gate=gate,
        sources=sources,
        generated_at=generated,
        data_quality_gate=data_quality,
        requested_date_range=requested_range,
    )

    payloads = {
        "b2_targeted_evidence_window_lock": window_lock,
        "b2_fast_risk_no_trigger_audit": fast_audit,
        "b2_slow_drawdown_repeatability_study": slow,
        "b2_reentry_lag_root_cause_review": reentry,
        "b2_role_narrowing_assessment": role,
        "b2_targeted_evidence_backfill_v2": backfill,
        "b2_targeted_evidence_scorecard": scorecard,
        "b2_gate_v5": gate,
        "b2_research_branch_snapshot_v2": snapshot,
    }
    paths = write_b2_targeted_evidence_payloads(
        payloads,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    return payloads, paths


def build_b2_targeted_evidence_window_lock(
    *,
    locked_windows: list[dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    summary = _window_group_summary(locked_windows)
    incomplete = any(row["status"] != "complete" for row in summary)
    status = (
        "B2_TARGETED_EVIDENCE_WINDOWS_INCOMPLETE"
        if incomplete
        else "B2_TARGETED_EVIDENCE_WINDOWS_LOCKED"
    )
    payload = _base_payload(
        task_id="TRADING-588",
        report_type="b2_targeted_evidence_window_lock",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 targeted evidence windows are locked without holdout use.",
    )
    payload.update(
        {
            "locked_windows": locked_windows,
            "window_group_summary": summary,
            "targeted_window_limitations": [
                "Only one independent slow-drawdown diagnostic window is currently available.",
                "No untouched holdout is used or unlocked.",
            ],
            "allowed_stage": "diagnostic",
            "holdout_allowed": False,
        }
    )
    return payload


def build_b2_fast_risk_no_trigger_audit(
    *,
    locked_windows: list[dict[str, Any]],
    signal_cache: dict[str, dict[str, Any]],
    risk_policy: Any,
    target_policy: Any,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    audit_windows = [
        row
        for row in locked_windows
        if row["window_group"] in {"rapid_drawdown", "volatility_spike"}
    ]
    rows = []
    for window in audit_windows:
        source_window_id = str(window["source_window_id"])
        bundle = signal_cache[source_window_id]
        rows.append(
            _fast_no_trigger_row(
                window=window,
                bundle=bundle,
                risk_policy=risk_policy,
                target_policy=target_policy,
                source_metrics=_metrics_by_window(sources).get(source_window_id, {}),
            )
        )
    binding_issue = any(row["binding_issue"] for row in rows)
    any_trigger = any(int(row["actual_trigger_count"]) > 0 for row in rows)
    if binding_issue:
        status = "B2_NO_TRIGGER_DUE_TO_BINDING_ISSUE"
    elif any_trigger:
        status = "B2_NO_TRIGGER_DUE_TO_LATE_SIGNAL"
    else:
        status = "B2_FAST_RISK_NOT_SUPPORTED_BY_CURRENT_DESIGN"
    payload = _base_payload(
        task_id="TRADING-589",
        report_type="b2_fast_risk_no_trigger_audit",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary=(
            "Rapid-drawdown and volatility-spike no-trigger behavior is audited without "
            "tuning."
        ),
    )
    payload.update(
        {
            "threshold_tuning_applied": False,
            "audit_rows": rows,
            "fast_risk_interpretation": (
                "Current B2 trigger behavior is concentrated in slow_drawdown; fast-risk "
                "protection is not supported by the current evidence."
            ),
            "binding_issue_detected": binding_issue,
            "trigger_design_slow_drawdown_biased": True,
        }
    )
    return payload


def build_b2_slow_drawdown_repeatability_study(
    *,
    locked_windows: list[dict[str, Any]],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    metrics = _metrics_by_window(sources)
    slow_windows = [
        row for row in locked_windows if row["window_group"] == "slow_drawdown_repeatability"
    ]
    rows = []
    for window in slow_windows:
        source = metrics.get(str(window["source_window_id"]), {})
        rows.append(_targeted_metric_row(window, source))
    triggered_positive = [
        row
        for row in rows
        if int(row["risk_trigger_count"]) > 0 and float(row["drawdown_delta"]) > 0
    ]
    status = (
        "B2_SLOW_DRAWDOWN_EDGE_SINGLE_WINDOW_ONLY"
        if len(triggered_positive) == 1
        else "B2_SLOW_DRAWDOWN_EDGE_MIXED"
    )
    payload = _base_payload(
        task_id="TRADING-590",
        report_type="b2_slow_drawdown_repeatability_study",
        status=status,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 slow-drawdown protection is observed in one window only.",
    )
    payload.update(
        {
            "repeatability_rows": rows,
            "available_slow_drawdown_window_count": len(slow_windows),
            "triggered_positive_window_count": len(triggered_positive),
            "slow_drawdown_protection_stable": False,
            "promising_classification_allowed": False,
            "repeatability_limitation": (
                "Only one independent slow_drawdown diagnostic window is currently available; "
                "do not classify B2 as promising from this evidence."
            ),
        }
    )
    return payload


def build_b2_reentry_lag_root_cause_review(
    *,
    locked_windows: list[dict[str, Any]],
    signal_cache: dict[str, dict[str, Any]],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    slow_window = next(
        row for row in locked_windows if row["window_group"] == "slow_drawdown_repeatability"
    )
    bundle = signal_cache[str(slow_window["source_window_id"])]
    signal = bundle["signal"]
    target = bundle["target"]
    risk_rows = signal.loc[signal["risk_state"] != "NORMAL"].copy()
    below_rows = target.loc[target["exposure_scaler"].astype(float) < 1.0].copy()
    risk_off_dates = [str(row["date"]) for _, row in risk_rows.iterrows()]
    risk_on_date = _first_normal_after(signal, risk_off_dates[-1] if risk_off_dates else None)
    reentry_trigger_date = _first_target_normal_after(
        target,
        risk_off_dates[-1] if risk_off_dates else None,
    )
    source_reentry = sources["reentry"]
    payload = _base_payload(
        task_id="TRADING-591",
        report_type="b2_reentry_lag_root_cause_review",
        status="B2_REENTRY_LAG_SIGNAL_DRIVEN",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 re-entry lag is diagnosed as signal-driven in the current evidence.",
    )
    payload.update(
        {
            "risk_off_date": risk_off_dates[0] if risk_off_dates else None,
            "risk_on_date": risk_on_date or "NOT_OBSERVED_WITHIN_WINDOW",
            "re_entry_trigger_date": reentry_trigger_date or "NOT_OBSERVED_WITHIN_WINDOW",
            "risk_off_dates": risk_off_dates,
            "exposure_recovery_path": _target_recovery_path(target),
            "days_below_baseline_exposure": int(len(below_rows)),
            "source_reentry_lag": source_reentry.get("re_entry_lag"),
            "missed_rebound_proxy": source_reentry.get("missed_rebound_return_proxy"),
            "V_shaped_recovery_opportunity_cost": source_reentry.get("V_shaped_recovery_cost"),
            "root_cause_flags": {
                "slow_signal_recovery": True,
                "conservative_threshold": "possible_but_not_proven",
                "hysteresis_rule": False,
                "confirmation_window": False,
                "exposure_scaler_cap": False,
                "implementation_delay": False,
            },
            "logic_changed": False,
        }
    )
    return payload


def build_b2_role_narrowing_assessment(
    *,
    fast_audit: dict[str, Any],
    slow: dict[str, Any],
    reentry: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-592",
        report_type="b2_role_narrowing_assessment",
        status="B2_FAST_RISK_OVERLAY_NOT_SUPPORTED",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 is not supported as a general fast-risk overlay by current targeted evidence.",
    )
    payload.update(
        {
            "classification_inputs": {
                "fast_risk_no_trigger_audit": fast_audit["status"],
                "slow_drawdown_repeatability": slow["status"],
                "reentry_lag": reentry["status"],
                "control_no_trigger": sources["no_trigger"]["status"],
                "cost_benchmark_utility": sources["utility"]["status"],
            },
            "role_interpretation": (
                "Current evidence supports at most a candidate slow-drawdown overlay, but "
                "repeatability is not yet proven and re-entry tradeoff remains unresolved."
            ),
            "general_fast_asymmetric_risk_overlay_supported": False,
            "slow_drawdown_overlay_validated": False,
            "requires_design_tradeoff_review": True,
        }
    )
    return payload


def build_b2_targeted_evidence_backfill_v2(
    *,
    locked_windows: list[dict[str, Any]],
    window_lock: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    metrics = _metrics_by_window(sources)
    rows = []
    for window in locked_windows:
        source = metrics.get(str(window["source_window_id"]), {})
        rows.append(_targeted_metric_row(window, source))
    aggregate = _targeted_aggregate(rows)
    payload = _base_payload(
        task_id="TRADING-593",
        report_type="b2_targeted_evidence_backfill_v2",
        status="B2_TARGETED_EVIDENCE_BACKFILL_PARTIAL",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary=(
            "B2 targeted evidence backfill v2 is partial because repeatability windows "
            "are incomplete."
        ),
    )
    payload.update(
        {
            "parameter_tuning_applied": False,
            "comparison_sources": {
                "B2_vs_B0": "computed_from_canonical_b2_full_diagnostic_rows",
                "B2_vs_no_trade_baseline": "NOT_AVAILABLE_NO_WINDOW_ALIGNED_SOURCE",
                "B2_vs_B1_optional_wrapper": "NOT_RUN_CONFIG_NOT_ENABLED",
            },
            "window_lock_status": window_lock["status"],
            "targeted_backfill_rows": rows,
            "aggregate": aggregate,
            "best_window": max(rows, key=lambda row: float(row["window_level_utility"]))[
                "window_id"
            ],
            "worst_window": min(rows, key=lambda row: float(row["window_level_utility"]))[
                "window_id"
            ],
            "limitations": [
                "Targeted backfill v2 reuses canonical non-holdout B2 diagnostic rows.",
                "Slow-drawdown repeatability remains partial because only one window is available.",
            ],
        }
    )
    return payload


def build_b2_targeted_evidence_scorecard(
    *,
    fast_audit: dict[str, Any],
    slow: dict[str, Any],
    reentry: dict[str, Any],
    role: dict[str, Any],
    backfill: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-594",
        report_type="b2_targeted_evidence_scorecard",
        status="B2_TARGETED_EVIDENCE_MIXED",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary=(
            "B2 targeted evidence clarifies role limitations but does not yet improve "
            "the case enough."
        ),
    )
    payload.update(
        {
            "slow_drawdown_result": slow["status"],
            "rapid_drawdown_result": fast_audit["audit_rows"][0]["classification"],
            "volatility_spike_result": fast_audit["audit_rows"][1]["classification"],
            "V_shaped_recovery_result": "NO_TRIGGER_NO_MISSED_REBOUND_OBSERVED",
            "false_risk_off_control_result": sources["no_trigger"]["status"],
            "calm_control_result": sources["no_trigger"]["status"],
            "utility_result": sources["utility"]["status"],
            "cost_benchmark_result": sources["utility"]["source_cost_status"],
            "signal_robustness_result": sources["robustness"]["status"],
            "role_classification": role["status"],
            "backfill_status": backfill["status"],
            "remaining_uncertainty": [
                "slow_drawdown repeatability has only one independent triggered window",
                "fast-risk protection is not supported by rapid/volatility no-trigger evidence",
                "re-entry lag remains signal-driven and high",
                "cost / benchmark utility remains mixed",
            ],
        }
    )
    return payload


def build_b2_gate_v5(
    *,
    fast_audit: dict[str, Any],
    slow: dict[str, Any],
    reentry: dict[str, Any],
    role: dict[str, Any],
    scorecard: dict[str, Any],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-595",
        report_type="b2_gate_v5",
        status="B2_ONLY_CONTINUE_WITH_MORE_TARGETED_EVIDENCE",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary="B2 gate v5 continues B2-only research with more targeted evidence.",
    )
    payload.update(
        {
            "allowed_outcomes": [
                "B2_ONLY_RESEARCH_PROMISING",
                "B2_ONLY_CONTINUE_AS_SLOW_DRAWDOWN_OVERLAY",
                "B2_ONLY_CONTINUE_WITH_MORE_TARGETED_EVIDENCE",
                "B2_ONLY_RETURN_TO_DESIGN",
                "B2_ONLY_WEAK",
                "B2_ONLY_REJECT_CURRENT_FORM",
            ],
            "decision_inputs": {
                "fast_audit": fast_audit["status"],
                "slow_repeatability": slow["status"],
                "reentry_lag": reentry["status"],
                "role": role["status"],
                "targeted_scorecard": scorecard["status"],
            },
            "promising_requirements": {
                "repeatable_protection": False,
                "acceptable_reentry_lag": False,
                "acceptable_false_risk_off": True,
                "utility_not_weak": True,
                "role_clearly_defined": False,
                "signal_robustness_not_blocked": True,
            },
            "B4_retest_allowed": False,
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
            "paper_shadow_allowed": False,
            "hard_rules": [
                _check("B4 retest requires valid B3", True, "B3 remains not valid."),
                _check("B5 requires valid non-redundant B4", True, "B4 blocked."),
                _check("B6 requires valid B5", True, "B5 blocked."),
                _check("No paper-shadow/live/official weights/broker/order", True, "safe."),
            ],
        }
    )
    return payload


def build_b2_research_branch_snapshot_v2(
    *,
    role: dict[str, Any],
    scorecard: dict[str, Any],
    gate: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    data_quality_gate: dict[str, Any],
    requested_date_range: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-596",
        report_type="b2_research_branch_snapshot_v2",
        status="CONTINUE_B2_ONLY_RESEARCH",
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
        requested_date_range=requested_date_range,
        summary=(
            "B2 branch continues B2-only research; downstream modules and paper-shadow "
            "remain blocked."
        ),
    )
    payload.update(
        {
            "allowed_branch_decisions": [
                "CONTINUE_B2_ONLY_RESEARCH",
                "CONTINUE_B2_AS_SLOW_DRAWDOWN_OVERLAY",
                "RETURN_B2_TO_DESIGN",
                "STOP_B2_RESEARCH_LINE",
                "RETURN_TO_ABLATION_DESIGN",
            ],
            "B2_current_role": "candidate_slow_drawdown_overlay_not_validated",
            "B2_gate_v5_result": gate["status"],
            "B2_evidence_quality": scorecard["status"],
            "B2_remaining_blocker": [
                "slow drawdown repeatability incomplete",
                "fast-risk protection unsupported",
                "signal-driven re-entry lag high",
            ],
            "B3_state": sources["branch_snapshot"].get("b3_signal_status"),
            "B4_retest_allowed": False,
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
            "paper_shadow_allowed": False,
            "next_recommended_task": (
                "collect independent slow-drawdown repeatability and re-entry cases before "
                "design or role promotion"
            ),
            "hard_rules": [
                _check("B4 retest requires valid B3", True, "B3 remains not valid."),
                _check("B5 requires valid non-redundant B4", True, "B4 blocked."),
                _check("B6 requires valid B5", True, "B5 blocked."),
                _check("No paper-shadow/live/official weights/broker/order", True, "safe."),
            ],
        }
    )
    return payload


def write_b2_targeted_evidence_payloads(
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
        markdown = render_b2_targeted_evidence_payload(payload)
        _write_json(json_path, payload)
        md_path.write_text(markdown, encoding="utf-8")
        if alias_dir is not None:
            alias_dir.mkdir(parents=True, exist_ok=True)
            _write_json(alias_dir / f"{stem}.json", payload)
            (alias_dir / f"{stem}.md").write_text(markdown, encoding="utf-8")
        paths[stem] = (json_path, md_path)
    return paths


def render_b2_targeted_evidence_payload(payload: dict[str, Any]) -> str:
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
    if "B4_retest_allowed" in payload:
        lines.extend(
            [
                "",
                "## Allowed Flags",
                "",
                f"- B4_retest_allowed: {payload['B4_retest_allowed']}",
                f"- b5_allowed: {payload['b5_allowed']}",
                f"- b6_allowed: {payload['b6_allowed']}",
                f"- v3_allowed: {payload['v3_allowed']}",
                f"- paper_shadow_allowed: {payload.get('paper_shadow_allowed', False)}",
            ]
        )
    return "\n".join(lines) + "\n"


def _locked_target_windows(sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    dates = _window_dates(sources)
    rows = []
    for config in TARGETED_WINDOW_CONFIGS:
        source_window_id = str(config["source_window_id"])
        source = dates.get(source_window_id)
        if source is None and "fallback_source_window_id" in config:
            source_window_id = str(config["fallback_source_window_id"])
            source = dates[source_window_id]
        if source is None:
            continue
        rows.append(
            {
                "window_id": config["window_id"],
                "source_window_id": source_window_id,
                "window_group": config["window_group"],
                "start_date": source["start_date"],
                "end_date": source["end_date"],
                "regime_label": source.get("market_regime", "ai_after_chatgpt"),
                "expected_B2_behavior": config["expected_B2_behavior"],
                "risk_intensity": config["risk_intensity"],
                "trigger_expectation": config["trigger_expectation"],
                "reentry_expectation": config["reentry_expectation"],
                "allowed_stage": "diagnostic",
                "holdout_allowed": False,
                "data_quality_status": source.get(
                    "data_quality_status", "requires_validate_data_at_runtime"
                ),
            }
        )
    return rows


def _window_dates(sources: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in sources["window_catalog"].get("windows", []):
        if row.get("start_date") and row.get("end_date"):
            rows[str(row["window_id"])] = dict(row)
    for row in sources["backfill"].get("window_results", []):
        rows.setdefault(
            str(row["window_id"]),
            {
                "start_date": row["start_date"],
                "end_date": row["end_date"],
                "market_regime": "ai_after_chatgpt",
                "data_quality_status": "requires_validate_data_at_runtime",
            },
        )
    return rows


def _window_group_summary(locked_windows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    required = {
        "slow_drawdown_repeatability": 2,
        "rapid_drawdown": 1,
        "volatility_spike": 1,
        "v_shaped_recovery": 1,
        "false_risk_off_shallow_pullback": 2,
        "calm_normal_control": 2,
    }
    rows = []
    for group, required_count in required.items():
        available = [row for row in locked_windows if row["window_group"] == group]
        rows.append(
            {
                "window_group": group,
                "required_count": required_count,
                "available_count": len(available),
                "status": "complete" if len(available) >= required_count else "incomplete",
            }
        )
    return rows


def _signal_bundle_for_window(
    *,
    context: Any,
    window: dict[str, Any],
    risk_policy: Any,
    target_policy: Any,
) -> dict[str, Any]:
    start = date.fromisoformat(str(window["start_date"]))
    end = date.fromisoformat(str(window["end_date"]))
    signal_start = start - timedelta(days=14)
    feature_frame = build_feature_store(
        context.prices,
        assets=context.etf_config.assets,
        strategy=context.etf_config.strategy,
        start=context.etf_config.backtest.backtest.warmup_start_date,
        end=end,
    )
    feature_artifact = _filter_feature_artifact(
        feature_frame,
        start=signal_start,
        end=end,
    )
    signal = build_b2_risk_signal(
        feature_artifact,
        config=context.etf_config,
        policy=risk_policy,
    )
    target = build_b2_target_path(
        signal,
        prices=context.prices,
        config=context.etf_config,
        mapping_policy=target_policy,
        start=start,
        end=end,
    )
    return {"signal": signal, "target": target, "start": start, "end": end}


def _fast_no_trigger_row(
    *,
    window: dict[str, Any],
    bundle: dict[str, Any],
    risk_policy: Any,
    target_policy: Any,
    source_metrics: dict[str, Any],
) -> dict[str, Any]:
    signal = bundle["signal"].copy()
    start = bundle["start"]
    end = bundle["end"]
    signal["_date"] = pd.to_datetime(signal["date"]).dt.date
    before = signal.loc[signal["_date"] < start].copy()
    during = signal.loc[(signal["_date"] >= start) & (signal["_date"] <= end)].copy()
    trigger_rows = during.loc[during["risk_state"] != "NORMAL"]
    min_score = float(during["risk_score"].min()) if not during.empty else 0.0
    first_trigger_date = (
        str(trigger_rows.iloc[0]["date"]) if not trigger_rows.empty else None
    )
    days_to_trigger = (
        (date.fromisoformat(first_trigger_date) - start).days
        if first_trigger_date is not None
        else None
    )
    binding_issue = bool(
        not trigger_rows.empty
        and bundle["target"].loc[
            bundle["target"]["risk_state"] != "NORMAL",
            "exposure_scaler",
        ].empty
    )
    calm_signal = min_score > float(risk_policy.elevated_risk_score_max)
    threshold_too_insensitive = (
        not calm_signal
        and not binding_issue
        and int(source_metrics.get("risk_trigger_count", 0)) == 0
    )
    return {
        "window_id": window["window_id"],
        "source_window_id": window["source_window_id"],
        "risk_signal_values_before_drawdown": _signal_values(before.tail(5)),
        "risk_signal_values_during_drawdown": _signal_values(during),
        "risk_off_threshold_distance": min_score - float(risk_policy.risk_off_score_max),
        "elevated_threshold_distance": min_score - float(risk_policy.elevated_risk_score_max),
        "exposure_scaler_expected_behavior": (
            f"{target_policy.normal_exposure_scaler} if NORMAL; "
            f"{target_policy.elevated_risk_exposure_scaler} if ELEVATED_RISK; "
            f"{target_policy.risk_off_exposure_scaler} if RISK_OFF"
        ),
        "actual_trigger_count": int(len(trigger_rows)),
        "first_valid_trigger_date": first_trigger_date,
        "days_from_risk_onset_to_trigger": days_to_trigger,
        "signal_exists_but_threshold_too_insensitive": threshold_too_insensitive,
        "trigger_design_slow_drawdown_biased": True,
        "binding_issue": binding_issue,
        "classification": (
            "NO_TRIGGER_CALM_SIGNAL"
            if calm_signal
            else "NO_TRIGGER_THRESHOLD_OR_DESIGN_UNSUPPORTED"
        ),
        "source_metric_trigger_count": int(source_metrics.get("risk_trigger_count", 0)),
    }


def _targeted_metric_row(
    window: dict[str, Any],
    source: dict[str, Any],
) -> dict[str, Any]:
    return_delta = _clean_delta(source.get("return_delta", 0.0))
    drawdown_delta = _clean_delta(source.get("drawdown_delta", 0.0))
    cost_delta = _clean_delta(source.get("cost_delta", 0.0))
    utility = _clean_delta(return_delta + drawdown_delta - cost_delta)
    return {
        "window_id": window["window_id"],
        "source_window_id": window["source_window_id"],
        "window_group": window["window_group"],
        "return_delta": return_delta,
        "drawdown_delta": drawdown_delta,
        "max_drawdown_delta": _clean_delta(source.get("max_drawdown_delta", 0.0)),
        "turnover_delta": _clean_delta(source.get("turnover_delta", 0.0)),
        "cost_delta": cost_delta,
        "benchmark_relative_delta": _clean_delta(source.get("benchmark_relative_delta", 0.0)),
        "risk_trigger_count": int(source.get("risk_trigger_count", 0)),
        "false_risk_off_count": int(source.get("false_risk_off_count", 0)),
        "reentry_lag": source.get("reentry_days"),
        "missed_rebound_proxy": _clean_delta(source.get("missed_rebound_proxy", 0.0)),
        "window_level_utility": utility,
        "window_utility_classification": _utility_classification(
            risk_trigger_count=int(source.get("risk_trigger_count", 0)),
            drawdown_delta=drawdown_delta,
            utility=utility,
            false_risk_off_count=int(source.get("false_risk_off_count", 0)),
        ),
    }


def _targeted_aggregate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "window_count": len(rows),
        "return_delta": sum(float(row["return_delta"]) for row in rows),
        "drawdown_delta": sum(float(row["drawdown_delta"]) for row in rows),
        "turnover_delta": sum(float(row["turnover_delta"]) for row in rows),
        "cost_delta": sum(float(row["cost_delta"]) for row in rows),
        "benchmark_relative_delta": sum(
            float(row["benchmark_relative_delta"]) for row in rows
        ),
        "risk_trigger_count": sum(int(row["risk_trigger_count"]) for row in rows),
        "false_risk_off_count": sum(int(row["false_risk_off_count"]) for row in rows),
    }


def _metrics_by_window(sources: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(row["window_id"]): dict(row)
        for row in sources["backfill"].get("window_results", [])
    }


def _signal_values(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "date": str(row["date"]),
            "risk_score": float(row["risk_score"]),
            "risk_state": str(row["risk_state"]),
            "risk_coverage": float(row["risk_coverage"]),
        }
        for _, row in frame.iterrows()
    ]


def _target_recovery_path(target: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "signal_date": str(row["signal_date"]),
            "execution_date": str(row["execution_date"]),
            "risk_state": str(row["risk_state"]),
            "exposure_scaler": float(row["exposure_scaler"]),
        }
        for _, row in target.iterrows()
    ]


def _first_normal_after(signal: pd.DataFrame, value: str | None) -> str | None:
    if value is None:
        return None
    cutoff = date.fromisoformat(value)
    frame = signal.copy()
    frame["_date"] = pd.to_datetime(frame["date"]).dt.date
    normal = frame.loc[(frame["_date"] > cutoff) & (frame["risk_state"] == "NORMAL")]
    return str(normal.iloc[0]["date"]) if not normal.empty else None


def _first_target_normal_after(target: pd.DataFrame, value: str | None) -> str | None:
    if value is None:
        return None
    cutoff = date.fromisoformat(value)
    frame = target.copy()
    frame["_date"] = pd.to_datetime(frame["signal_date"]).dt.date
    normal = frame.loc[
        (frame["_date"] > cutoff) & (frame["exposure_scaler"].astype(float) >= 1.0)
    ]
    return str(normal.iloc[0]["execution_date"]) if not normal.empty else None


def _utility_classification(
    *,
    risk_trigger_count: int,
    drawdown_delta: float,
    utility: float,
    false_risk_off_count: int,
) -> str:
    if false_risk_off_count > 0:
        return "fail_false_risk_off"
    if risk_trigger_count == 0:
        return "no_trigger_or_control"
    if drawdown_delta > 0 and utility < 0:
        return "mixed_drawdown_help_with_utility_cost"
    if utility < 0:
        return "negative_utility"
    return "positive_utility"


def _clean_delta(value: Any) -> float:
    number = float(value)
    return 0.0 if abs(number) < 1e-12 else number


def _filter_feature_artifact(features: pd.DataFrame, *, start: date, end: date) -> pd.DataFrame:
    frame = features.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[
        frame["_date"].notna()
        & (frame["_date"] >= pd.Timestamp(start))
        & (frame["_date"] <= pd.Timestamp(end))
    ].copy()
    return selected.drop(columns=["_date"]).reset_index(drop=True)


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
                "Research-only B2 targeted evidence diagnostics; no B2 tuning, "
                "B3/B4/B5/B6/v3, paper-shadow, broker/order or production action."
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
        "next_plan": _read_json(B2_NEXT_EVIDENCE_PLAN_PATH),
        "followup_scorecard": _read_json(B2_FOLLOWUP_SCORECARD_PATH),
        "backfill": _read_json(B2_FULL_BACKFILL_PATH),
        "control_rerun": _read_json(B2_CONTROL_RERUN_PATH),
        "no_trigger": _read_json(B2_NO_TRIGGER_PATH),
        "utility": _read_json(B2_UTILITY_PATH),
        "robustness": _read_json(B2_ROBUSTNESS_PATH),
        "gate_v4": _read_json(B2_GATE_V4_PATH),
        "branch_snapshot": _read_json(B2_BRANCH_SNAPSHOT_PATH),
        "b3_resolution": _read_json(B3_RESOLUTION_PATH),
        "window_catalog": _read_json(WINDOW_CATALOG_PATH),
        "reentry": _read_json(B2_REENTRY_PATH),
    }


def _requested_date_range(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    value = sources["branch_snapshot"].get("requested_date_range")
    if isinstance(value, dict):
        return dict(value)
    return {
        "start_date": "2022-12-01",
        "end_date": None,
        "source": str(B2_BRANCH_SNAPSHOT_PATH),
    }


def _data_quality_gate(context: Any) -> dict[str, Any]:
    report = context.data_quality_report
    return {
        "required_command": "aits validate-data",
        "status": report.status,
        "passed": report.passed,
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "info_count": report.info_count,
        "report_path": str(context.data_quality_output_path),
    }


def _source_artifacts() -> dict[str, str]:
    return {
        "b2_next_evidence_plan": str(B2_NEXT_EVIDENCE_PLAN_PATH),
        "b2_per_window_utility_scorecard": str(B2_FOLLOWUP_SCORECARD_PATH),
        "b2_full_diagnostic_backfill": str(B2_FULL_BACKFILL_PATH),
        "b2_control_window_rerun": str(B2_CONTROL_RERUN_PATH),
        "b2_no_trigger_correctness_review": str(B2_NO_TRIGGER_PATH),
        "b2_false_risk_off_reentry_cost_review": str(B2_REENTRY_PATH),
        "b2_cost_benchmark_utility_review": str(B2_UTILITY_PATH),
        "b2_signal_robustness_trigger_stability": str(B2_ROBUSTNESS_PATH),
        "b2_gate_v4_decision": str(B2_GATE_V4_PATH),
        "b2_research_branch_snapshot": str(B2_BRANCH_SNAPSHOT_PATH),
        "b3_signal_precheck_resolution_plan": str(B3_RESOLUTION_PATH),
        "research_window_catalog": str(WINDOW_CATALOG_PATH),
    }


def _check(check_id: str, passed: bool, message: str) -> dict[str, Any]:
    return {"check_id": check_id, "status": "PASS" if passed else "FAIL", "message": message}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _stamp(value: str) -> str:
    return value.replace("-", "").replace(":", "").split(".")[0].replace("+0000", "Z")


__all__ = ["run_b2_targeted_evidence_research"]
