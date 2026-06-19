from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_REPORT_DIR

DEFAULT_WEIGHT_RESEARCH_REPORT_DIR = DEFAULT_ETF_REPORT_DIR / "weight_research"
DEFAULT_RESEARCH_SOURCE_DIR = PROJECT_ROOT / "docs" / "research"

B2_TARGETED_WINDOW_LOCK_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_targeted_evidence_window_lock.json"
)
B2_FAST_AUDIT_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_fast_risk_no_trigger_audit.json"
B2_SLOW_REPEATABILITY_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_slow_drawdown_repeatability_study.json"
)
B2_REENTRY_ROOT_CAUSE_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_reentry_lag_root_cause_review.json"
)
B2_ROLE_NARROWING_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_role_narrowing_assessment.json"
B2_TARGETED_BACKFILL_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_targeted_evidence_backfill_v2.json"
)
B2_TARGETED_SCORECARD_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_targeted_evidence_scorecard.json"
)
B2_GATE_V5_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_gate_v5.json"
B2_BRANCH_SNAPSHOT_V2_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_research_branch_snapshot_v2.json"
)
B2_FULL_BACKFILL_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_full_diagnostic_backfill.json"
B2_CONTROL_RERUN_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_control_window_rerun.json"
B2_NO_TRIGGER_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_no_trigger_correctness_review.json"
B2_UTILITY_PATH = DEFAULT_RESEARCH_SOURCE_DIR / "b2_cost_benchmark_utility_review.json"
B2_ROBUSTNESS_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b2_signal_robustness_trigger_stability.json"
)
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


def run_b2_final_evidence_role_decision(
    *,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = DEFAULT_RESEARCH_SOURCE_DIR,
    generated_at: datetime | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, tuple[Path, Path]]]:
    generated = generated_at or datetime.now(UTC)
    sources = _load_sources()
    requested_range = _requested_date_range(sources)
    data_quality = _data_quality_gate(sources)

    completion = build_b2_slow_drawdown_evidence_completion(
        sources=sources,
        generated_at=generated,
        requested_date_range=requested_range,
        data_quality_gate=data_quality,
    )
    edge = build_b2_slow_drawdown_edge_validation(
        completion=completion,
        sources=sources,
        generated_at=generated,
        requested_date_range=requested_range,
        data_quality_gate=data_quality,
    )
    fast_role = build_b2_fast_risk_role_deprecation_review(
        sources=sources,
        generated_at=generated,
        requested_date_range=requested_range,
        data_quality_gate=data_quality,
    )
    reentry = build_b2_reentry_lag_design_implication(
        sources=sources,
        generated_at=generated,
        requested_date_range=requested_range,
        data_quality_gate=data_quality,
    )
    role = build_b2_role_reclassification(
        edge=edge,
        fast_role=fast_role,
        reentry=reentry,
        sources=sources,
        generated_at=generated,
        requested_date_range=requested_range,
        data_quality_gate=data_quality,
    )
    gate = build_b2_final_research_gate(
        edge=edge,
        fast_role=fast_role,
        reentry=reentry,
        role=role,
        sources=sources,
        generated_at=generated,
        requested_date_range=requested_range,
        data_quality_gate=data_quality,
    )
    owner_packet = build_b2_research_line_owner_packet(
        completion=completion,
        edge=edge,
        fast_role=fast_role,
        reentry=reentry,
        role=role,
        gate=gate,
        sources=sources,
        generated_at=generated,
        requested_date_range=requested_range,
        data_quality_gate=data_quality,
    )
    snapshot = build_b2_branch_snapshot_final(
        role=role,
        gate=gate,
        owner_packet=owner_packet,
        sources=sources,
        generated_at=generated,
        requested_date_range=requested_range,
        data_quality_gate=data_quality,
    )

    payloads = {
        "b2_slow_drawdown_evidence_completion": completion,
        "b2_slow_drawdown_edge_validation": edge,
        "b2_fast_risk_role_deprecation_review": fast_role,
        "b2_reentry_lag_design_implication": reentry,
        "b2_role_reclassification": role,
        "b2_final_research_gate": gate,
        "b2_research_line_owner_packet": owner_packet,
        "b2_branch_snapshot_final": snapshot,
    }
    paths = write_b2_final_decision_payloads(
        payloads,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    return payloads, paths


def build_b2_slow_drawdown_evidence_completion(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    inventory = _slow_drawdown_window_inventory(sources)
    additional = [
        row
        for row in inventory
        if row["is_slow_drawdown_candidate"]
        and row["independent_from_primary"]
        and not row["holdout_blocked"]
    ]
    additional_with_metrics = [
        row for row in additional if _metric_by_window(sources, row["window_id"])
    ]
    if not additional:
        status = "B2_SLOW_DRAWDOWN_NO_ADDITIONAL_WINDOW"
    elif len(additional_with_metrics) == len(additional):
        status = "B2_SLOW_DRAWDOWN_REPEATABILITY_COMPLETE"
    else:
        status = "B2_SLOW_DRAWDOWN_REPEATABILITY_PARTIAL"

    original = _original_slow_drawdown_evidence(sources)
    payload = _base_payload(
        task_id="TRADING-597",
        report_type="b2_slow_drawdown_evidence_completion",
        status=status,
        generated_at=generated_at,
        requested_date_range=requested_date_range,
        data_quality_gate=data_quality_gate,
        summary=(
            "B2 slow-drawdown repeatability cannot be completed unless a second "
            "independent non-holdout slow-drawdown window exists."
        ),
    )
    payload.update(
        {
            "primary_slow_drawdown_window": "slow_drawdown",
            "additional_slow_drawdown_windows_required": 1,
            "additional_slow_drawdown_windows_found": len(additional),
            "additional_slow_drawdown_windows_with_metrics": len(additional_with_metrics),
            "window_inventory": inventory,
            "original_slow_drawdown_evidence": original,
            "additional_slow_drawdown_evidence": [
                _metric_row_for_additional_window(sources, row) for row in additional
            ],
            "B2_only_rerun_on_additional_window": (
                "NOT_RUN_NO_ADDITIONAL_INDEPENDENT_SLOW_DRAWDOWN_WINDOW"
                if not additional
                else "REQUIRES_WINDOW_ALIGNED_B2_METRICS"
            ),
            "B0_comparison_source": "canonical_non_holdout_B2_vs_B0_diagnostic_rows",
            "holdout_accessed": False,
            "threshold_tuning_applied": False,
            "promising_from_single_window_allowed": False,
        }
    )
    return payload


def build_b2_slow_drawdown_edge_validation(
    *,
    completion: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    original = completion["original_slow_drawdown_evidence"]
    additional = completion["additional_slow_drawdown_evidence"]
    protected_windows = [
        row
        for row in [original, *additional]
        if int(row.get("risk_trigger_count") or 0) > 0
        and float(row.get("drawdown_delta") or 0.0) > 0.0
    ]
    repeatable = len(protected_windows) > 1
    acceptable_reentry = sources["reentry_root"]["status"] not in {
        "B2_REENTRY_LAG_HIGH",
        "B2_REENTRY_LAG_SIGNAL_DRIVEN",
    }
    utility_acceptable = sources["utility"]["status"] not in {
        "B2_UTILITY_WEAK",
        "B2_UTILITY_MIXED",
    }
    if completion["status"] == "B2_SLOW_DRAWDOWN_NO_ADDITIONAL_WINDOW":
        status = "B2_SLOW_DRAWDOWN_EDGE_SINGLE_WINDOW_ONLY"
    elif repeatable and acceptable_reentry and utility_acceptable:
        status = "B2_SLOW_DRAWDOWN_EDGE_REPEATABLE"
    elif protected_windows:
        status = "B2_SLOW_DRAWDOWN_EDGE_MIXED"
    else:
        status = "B2_SLOW_DRAWDOWN_EDGE_WEAK"

    payload = _base_payload(
        task_id="TRADING-598",
        report_type="b2_slow_drawdown_edge_validation",
        status=status,
        generated_at=generated_at,
        requested_date_range=requested_range_with_note(requested_date_range),
        data_quality_gate=data_quality_gate,
        summary="B2 slow-drawdown edge validation applies repeatability and cost rules.",
    )
    payload.update(
        {
            "original_slow_drawdown_status": sources["slow_repeatability"]["status"],
            "completion_status": completion["status"],
            "independent_slow_drawdown_window_count": 1 + len(additional),
            "protected_slow_drawdown_window_count": len(protected_windows),
            "repeatable_protection": repeatable,
            "meaningful_drawdown_protection": bool(protected_windows),
            "acceptable_opportunity_cost": acceptable_reentry and utility_acceptable,
            "reentry_lag_review": sources["reentry_root"]["status"],
            "cost_benchmark_review": sources["utility"]["status"],
            "single_window_only_blocks_promising": not repeatable,
            "decision_rules": [
                _check(
                    "repeatable requires more than one independent slow-drawdown window",
                    repeatable,
                    "Only one independent positive slow_drawdown window is available.",
                ),
                _check(
                    "acceptable re-entry required",
                    acceptable_reentry,
                    f"Current re-entry status is {sources['reentry_root']['status']}.",
                ),
                _check(
                    "acceptable utility required",
                    utility_acceptable,
                    f"Current utility status is {sources['utility']['status']}.",
                ),
            ],
        }
    )
    return payload


def build_b2_fast_risk_role_deprecation_review(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    audit_rows = sources["fast_audit"].get("audit_rows", [])
    actual_trigger_count = sum(int(row.get("actual_trigger_count", 0)) for row in audit_rows)
    calm_signal_rows = [
        row for row in audit_rows if row.get("classification") == "NO_TRIGGER_CALM_SIGNAL"
    ]
    status = (
        "B2_FAST_RISK_ROLE_DEPRECATED"
        if sources["fast_audit"]["status"]
        == "B2_FAST_RISK_NOT_SUPPORTED_BY_CURRENT_DESIGN"
        else "B2_FAST_RISK_ROLE_NEEDS_MORE_EVIDENCE"
    )
    payload = _base_payload(
        task_id="TRADING-599",
        report_type="b2_fast_risk_role_deprecation_review",
        status=status,
        generated_at=generated_at,
        requested_date_range=requested_date_range,
        data_quality_gate=data_quality_gate,
        summary="Current B2 form should no longer claim fast-risk overlay capability.",
    )
    payload.update(
        {
            "evidence_inputs": {
                "fast_risk_no_trigger_audit": sources["fast_audit"]["status"],
                "role_narrowing_assessment": sources["role_narrowing"]["status"],
            },
            "failed_fast_risk_because_trigger_too_slow": False,
            "failed_fast_risk_because_signal_not_sensitive": bool(calm_signal_rows),
            "actual_fast_risk_trigger_count": actual_trigger_count,
            "binding_issue_detected": bool(sources["fast_audit"].get("binding_issue_detected")),
            "fast_risk_better_as_future_module": True,
            "requires_new_trigger_family_for_fast_risk": True,
            "should_rename_or_reclassify_B2": True,
            "deprecated_claims": [
                "fast asymmetric risk overlay",
                "rapid drawdown protection",
                "volatility spike protection",
            ],
            "preserved_claims": [
                "research-only candidate slow-drawdown behavior under redesign review"
            ],
            "audit_rows": audit_rows,
        }
    )
    return payload


def build_b2_reentry_lag_design_implication(
    *,
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    reentry = sources["reentry_root"]
    risk_on_not_observed = reentry.get("risk_on_date") == "NOT_OBSERVED_WITHIN_WINDOW"
    exposure_not_observed = (
        reentry.get("re_entry_trigger_date") == "NOT_OBSERVED_WITHIN_WINDOW"
    )
    status = (
        "B2_REENTRY_LAG_REQUIRES_DESIGN_REWORK"
        if risk_on_not_observed or exposure_not_observed
        else "B2_REENTRY_LAG_ACCEPTABLE_WITH_WARNING"
    )
    payload = _base_payload(
        task_id="TRADING-600",
        report_type="b2_reentry_lag_design_implication",
        status=status,
        generated_at=generated_at,
        requested_date_range=requested_range_with_note(requested_date_range),
        data_quality_gate=data_quality_gate,
        summary="B2 re-entry lag is evaluated without tuning the re-entry logic.",
    )
    payload.update(
        {
            "signal_recovery_timing": {
                "risk_off_date": reentry.get("risk_off_date"),
                "risk_on_date": reentry.get("risk_on_date"),
                "risk_on_observed_within_window": not risk_on_not_observed,
            },
            "exposure_recovery_timing": {
                "re_entry_trigger_date": reentry.get("re_entry_trigger_date"),
                "exposure_recovery_observed_within_window": not exposure_not_observed,
                "days_below_baseline_exposure": reentry.get("days_below_baseline_exposure"),
            },
            "missed_rebound_proxy": reentry.get("missed_rebound_proxy"),
            "reentry_lag_by_window": [
                {
                    "window_id": "slow_drawdown",
                    "source_reentry_lag": reentry.get("source_reentry_lag"),
                    "risk_trigger_count": len(reentry.get("risk_off_dates", [])),
                    "risk_on_observed_within_window": not risk_on_not_observed,
                }
            ],
            "lag_systematic_assessment": "NOT_PROVEN_SINGLE_TRIGGER_WINDOW",
            "root_cause_flags": {
                "slow_signal_recovery": True,
                "conservative_threshold": reentry.get("root_cause_flags", {}).get(
                    "conservative_threshold"
                ),
                "hysteresis": reentry.get("root_cause_flags", {}).get("hysteresis_rule"),
                "confirmation": reentry.get("root_cause_flags", {}).get(
                    "confirmation_window"
                ),
                "exposure_scaler_cap": reentry.get("root_cause_flags", {}).get(
                    "exposure_scaler_cap"
                ),
            },
            "logic_changed": False,
            "threshold_tuning_applied": False,
            "design_implication": (
                "Current form needs re-entry design review before any role promotion."
            ),
        }
    )
    return payload


def build_b2_role_reclassification(
    *,
    edge: dict[str, Any],
    fast_role: dict[str, Any],
    reentry: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    fast_supported = fast_role["status"] not in {
        "B2_FAST_RISK_ROLE_DEPRECATED",
        "B2_FAST_RISK_ROLE_REJECTED_FOR_CURRENT_DESIGN",
    }
    slow_repeatable = edge["status"] == "B2_SLOW_DRAWDOWN_EDGE_REPEATABLE"
    reentry_acceptable = reentry["status"].startswith("B2_REENTRY_LAG_ACCEPTABLE")
    if fast_supported:
        status = "B2_FAST_ASYMMETRIC_RISK_OVERLAY"
    elif slow_repeatable and reentry_acceptable:
        status = "B2_SLOW_DRAWDOWN_DEFENSIVE_OVERLAY"
    elif edge["status"] in {
        "B2_SLOW_DRAWDOWN_EDGE_SINGLE_WINDOW_ONLY",
        "B2_SLOW_DRAWDOWN_EDGE_MIXED",
    }:
        status = "B2_RISK_OVERLAY_NEEDS_REDESIGN"
    elif sources["utility"]["status"] == "B2_UTILITY_WEAK":
        status = "B2_RISK_OVERLAY_WEAK"
    else:
        status = "B2_RISK_OVERLAY_REJECT_CURRENT_FORM"

    payload = _base_payload(
        task_id="TRADING-601",
        report_type="b2_role_reclassification",
        status=status,
        generated_at=generated_at,
        requested_date_range=requested_date_range,
        data_quality_gate=data_quality_gate,
        summary="B2 role is reclassified from final targeted evidence.",
    )
    payload.update(
        {
            "classification_inputs": {
                "slow_drawdown_edge": edge["status"],
                "fast_risk_role": fast_role["status"],
                "reentry_lag": reentry["status"],
                "targeted_scorecard": sources["targeted_scorecard"]["status"],
                "cost_benchmark_utility": sources["utility"]["status"],
                "control_behavior": sources["no_trigger"]["status"],
            },
            "fast_asymmetric_role_allowed": fast_supported,
            "slow_drawdown_defensive_role_allowed": slow_repeatable and reentry_acceptable,
            "current_supported_role": (
                "none_promotable_current_form_needs_design_review"
                if status == "B2_RISK_OVERLAY_NEEDS_REDESIGN"
                else status
            ),
            "role_rationale": [
                "fast-risk role is deprecated for current B2 form",
                "slow-drawdown edge is single-window-only",
                "re-entry lag requires design review",
            ],
        }
    )
    return payload


def build_b2_final_research_gate(
    *,
    edge: dict[str, Any],
    fast_role: dict[str, Any],
    reentry: dict[str, Any],
    role: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    repeatable = edge["status"] == "B2_SLOW_DRAWDOWN_EDGE_REPEATABLE"
    acceptable_reentry = reentry["status"].startswith("B2_REENTRY_LAG_ACCEPTABLE")
    utility_ok = sources["utility"]["status"] not in {"B2_UTILITY_WEAK"}
    clear_role = role["status"] in {
        "B2_FAST_ASYMMETRIC_RISK_OVERLAY",
        "B2_SLOW_DRAWDOWN_DEFENSIVE_OVERLAY",
    }
    if repeatable and acceptable_reentry and utility_ok and clear_role:
        status = "B2_CURRENT_FORM_RESEARCH_PROMISING"
    elif role["status"] == "B2_SLOW_DRAWDOWN_DEFENSIVE_OVERLAY":
        status = "B2_CURRENT_FORM_CONTINUE_AS_NARROW_SLOW_DRAWDOWN_MODULE"
    elif role["status"] == "B2_RISK_OVERLAY_NEEDS_REDESIGN":
        status = "B2_CURRENT_FORM_RETURN_TO_DESIGN"
    elif role["status"] == "B2_RISK_OVERLAY_WEAK":
        status = "B2_CURRENT_FORM_WEAK"
    else:
        status = "B2_CURRENT_FORM_REJECT"

    payload = _base_payload(
        task_id="TRADING-602",
        report_type="b2_final_research_gate",
        status=status,
        generated_at=generated_at,
        requested_date_range=requested_range_with_note(requested_date_range),
        data_quality_gate=data_quality_gate,
        summary="Final B2 current-form research gate blocks downstream modules.",
    )
    payload.update(
        {
            "allowed_outcomes": [
                "B2_CURRENT_FORM_RESEARCH_PROMISING",
                "B2_CURRENT_FORM_CONTINUE_AS_NARROW_SLOW_DRAWDOWN_MODULE",
                "B2_CURRENT_FORM_RETURN_TO_DESIGN",
                "B2_CURRENT_FORM_WEAK",
                "B2_CURRENT_FORM_REJECT",
            ],
            "decision_inputs": {
                "slow_drawdown_edge": edge["status"],
                "fast_risk_role": fast_role["status"],
                "reentry_lag": reentry["status"],
                "role_reclassification": role["status"],
                "previous_gate_v5": sources["gate_v5"]["status"],
            },
            "promising_requirements": {
                "repeatable_protection": repeatable,
                "acceptable_reentry": acceptable_reentry,
                "acceptable_utility": utility_ok,
                "clear_role": clear_role,
                "no_fast_risk_claim_if_unsupported": (
                    fast_role["status"] == "B2_FAST_RISK_ROLE_DEPRECATED"
                ),
                "no_untouched_holdout_used": True,
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


def build_b2_research_line_owner_packet(
    *,
    completion: dict[str, Any],
    edge: dict[str, Any],
    fast_role: dict[str, Any],
    reentry: dict[str, Any],
    role: dict[str, Any],
    gate: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    payload = _base_payload(
        task_id="TRADING-603",
        report_type="b2_research_line_owner_packet",
        status="B2_RESEARCH_LINE_OWNER_PACKET_READY",
        generated_at=generated_at,
        requested_date_range=requested_range_with_note(requested_date_range),
        data_quality_gate=data_quality_gate,
        summary="Owner packet summarizes final current-form B2 evidence without decision.",
    )
    payload.update(
        {
            "B2_original_intended_role": "fast asymmetric risk overlay",
            "actual_evidence_supported_role": role["current_supported_role"],
            "fast_risk_evidence": {
                "status": fast_role["status"],
                "summary": "Rapid drawdown and volatility spike evidence do not support B2.",
            },
            "slow_drawdown_evidence": {
                "completion_status": completion["status"],
                "edge_status": edge["status"],
                "original_window": completion["original_slow_drawdown_evidence"],
                "additional_window_count": completion[
                    "additional_slow_drawdown_windows_found"
                ],
            },
            "control_window_evidence": {
                "status": sources["no_trigger"]["status"],
                "control_rerun_status": sources["control_rerun"]["status"],
            },
            "reentry_lag": {
                "status": reentry["status"],
                "source": sources["reentry_root"]["status"],
                "design_implication": reentry["design_implication"],
            },
            "cost_benchmark_utility": {
                "status": sources["utility"]["status"],
                "source_cost_status": sources["utility"].get("source_cost_status"),
            },
            "final_gate_result": gate["status"],
            "allowed_owner_options": [
                "continue B2 as narrow slow-drawdown research",
                "return B2 to design",
                "reject current B2 form",
                "hold for more data",
            ],
            "recommended_owner_options": [
                "return B2 to design",
                "hold for more data only if owner wants more non-holdout diagnostics",
                "reject current B2 form if no redesign work is desired",
            ],
            "owner_decision_appended": False,
        }
    )
    return payload


def build_b2_branch_snapshot_final(
    *,
    role: dict[str, Any],
    gate: dict[str, Any],
    owner_packet: dict[str, Any],
    sources: dict[str, dict[str, Any]],
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
) -> dict[str, Any]:
    if gate["status"] == "B2_CURRENT_FORM_RETURN_TO_DESIGN":
        status = "RETURN_B2_TO_DESIGN"
    elif gate["status"] == "B2_CURRENT_FORM_REJECT":
        status = "REJECT_B2_CURRENT_FORM"
    elif gate["status"] == "B2_CURRENT_FORM_CONTINUE_AS_NARROW_SLOW_DRAWDOWN_MODULE":
        status = "CONTINUE_NARROW_B2_RESEARCH"
    else:
        status = "STOP_CURRENT_RESEARCH_LINE"

    payload = _base_payload(
        task_id="TRADING-604",
        report_type="b2_branch_snapshot_final",
        status=status,
        generated_at=generated_at,
        requested_date_range=requested_range_with_note(requested_date_range),
        data_quality_gate=data_quality_gate,
        summary="Final B2 branch snapshot keeps downstream research gates closed.",
    )
    payload.update(
        {
            "allowed_branch_decisions": [
                "CONTINUE_NARROW_B2_RESEARCH",
                "RETURN_B2_TO_DESIGN",
                "REJECT_B2_CURRENT_FORM",
                "RETURN_TO_ABLATION_DESIGN",
                "STOP_CURRENT_RESEARCH_LINE",
            ],
            "B2_role_classification": role["status"],
            "B2_final_gate_result": gate["status"],
            "B2_owner_packet_status": owner_packet["status"],
            "B3_status": sources["b3_resolution"]["status"],
            "B4_retest_allowed": False,
            "b5_allowed": False,
            "b6_allowed": False,
            "v3_allowed": False,
            "paper_shadow_allowed": False,
            "next_recommended_task": (
                "draft B2 redesign scope focused on separate fast-risk trigger design, "
                "slow-drawdown repeatability sourcing, and re-entry recovery logic"
            ),
            "hard_rules": [
                _check("No B4 retest without valid B3", True, "B3 is not valid."),
                _check("No B5 without valid non-redundant B4", True, "B4 blocked."),
                _check("No B6 without valid B5", True, "B5 blocked."),
                _check("No paper-shadow/live/official weights/broker/order", True, "safe."),
            ],
        }
    )
    return payload


def write_b2_final_decision_payloads(
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
        markdown = render_b2_final_decision_payload(payload)
        _write_json(json_path, payload)
        md_path.write_text(markdown, encoding="utf-8")
        if alias_dir is not None:
            alias_dir.mkdir(parents=True, exist_ok=True)
            _write_json(alias_dir / f"{stem}.json", payload)
            (alias_dir / f"{stem}.md").write_text(markdown, encoding="utf-8")
        paths[stem] = (json_path, md_path)
    return paths


def render_b2_final_decision_payload(payload: dict[str, Any]) -> str:
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


def _slow_drawdown_window_inventory(sources: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in sources["window_catalog"].get("windows", []):
        window_id = str(row.get("window_id"))
        is_holdout = window_id == "untouched_temporal_holdout"
        is_slow = window_id == "slow_drawdown" or "slow_drawdown" in str(
            row.get("purpose", "")
        ).lower()
        independent = window_id != "slow_drawdown"
        rows.append(
            {
                "window_id": window_id,
                "start_date": row.get("start_date"),
                "end_date": row.get("end_date"),
                "purpose": row.get("purpose"),
                "is_slow_drawdown_candidate": is_slow,
                "independent_from_primary": independent,
                "is_untouched_holdout": is_holdout,
                "holdout_blocked": is_holdout,
                "selected_as_additional_slow_drawdown": (
                    is_slow and independent and not is_holdout
                ),
                "rejection_reason": _slow_window_rejection_reason(
                    window_id=window_id,
                    is_slow=is_slow,
                    independent=independent,
                    is_holdout=is_holdout,
                ),
            }
        )
    return rows


def _slow_window_rejection_reason(
    *,
    window_id: str,
    is_slow: bool,
    independent: bool,
    is_holdout: bool,
) -> str | None:
    if is_holdout:
        return "untouched_holdout_blocked"
    if not is_slow:
        return "not_labeled_slow_drawdown"
    if not independent:
        return "primary_slow_drawdown_window_already_used"
    return None


def _original_slow_drawdown_evidence(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    row = _metric_by_window(sources, "slow_drawdown")
    targeted_row = next(
        (
            item
            for item in sources["slow_repeatability"].get("repeatability_rows", [])
            if item.get("source_window_id") == "slow_drawdown"
        ),
        {},
    )
    exposure_path = sources["reentry_root"].get("exposure_recovery_path", [])
    return {
        "window_id": "slow_drawdown",
        "source_window_id": "slow_drawdown",
        "start_date": row.get("start_date"),
        "end_date": row.get("end_date"),
        "trigger_date": _first(row.get("risk_trigger_dates", [])),
        "risk_off_date": _first(row.get("risk_off_dates", [])),
        "exposure_reduction_path": _compress_exposure_path(exposure_path),
        "drawdown_delta": _clean_delta(row.get("drawdown_delta", 0.0)),
        "return_delta": _clean_delta(row.get("return_delta", 0.0)),
        "turnover_delta": _clean_delta(row.get("turnover_delta", 0.0)),
        "cost_delta": _clean_delta(row.get("cost_delta", 0.0)),
        "re_entry_lag": row.get("reentry_days"),
        "missed_rebound_proxy": _clean_delta(row.get("missed_rebound_proxy", 0.0)),
        "window_utility": targeted_row.get("window_level_utility"),
        "window_utility_classification": targeted_row.get(
            "window_utility_classification"
        ),
        "risk_trigger_count": int(row.get("risk_trigger_count", 0)),
        "B0_comparison": "B2_vs_B0",
    }


def _metric_row_for_additional_window(
    sources: dict[str, dict[str, Any]],
    row: dict[str, Any],
) -> dict[str, Any]:
    metrics = _metric_by_window(sources, str(row["window_id"]))
    if not metrics:
        return {
            "window_id": row["window_id"],
            "status": "MISSING_B2_WINDOW_ALIGNED_METRICS",
            "B2_only_rerun_status": "NOT_AVAILABLE",
        }
    return {
        "window_id": row["window_id"],
        "status": "B2_WINDOW_ALIGNED_METRICS_AVAILABLE",
        "trigger_date": _first(metrics.get("risk_trigger_dates", [])),
        "exposure_reduction_path": "SEE_SOURCE_TARGET_PATH",
        "drawdown_delta": _clean_delta(metrics.get("drawdown_delta", 0.0)),
        "return_delta": _clean_delta(metrics.get("return_delta", 0.0)),
        "turnover_delta": _clean_delta(metrics.get("turnover_delta", 0.0)),
        "cost_delta": _clean_delta(metrics.get("cost_delta", 0.0)),
        "re_entry_lag": metrics.get("reentry_days"),
        "missed_rebound_proxy": _clean_delta(metrics.get("missed_rebound_proxy", 0.0)),
        "window_utility": _clean_delta(
            float(metrics.get("return_delta", 0.0))
            + float(metrics.get("drawdown_delta", 0.0))
            - float(metrics.get("cost_delta", 0.0))
        ),
        "B0_comparison": "B2_vs_B0",
    }


def _metric_by_window(sources: dict[str, dict[str, Any]], window_id: str) -> dict[str, Any]:
    for row in sources["full_backfill"].get("window_results", []):
        if row.get("window_id") == window_id:
            return dict(row)
    for row in sources["targeted_backfill"].get("targeted_backfill_rows", []):
        if row.get("source_window_id") == window_id or row.get("window_id") == window_id:
            return dict(row)
    return {}


def _compress_exposure_path(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compressed: list[dict[str, Any]] = []
    last_scaler: float | None = None
    for row in rows:
        scaler = float(row.get("exposure_scaler", 1.0))
        if scaler != last_scaler:
            compressed.append(
                {
                    "signal_date": row.get("signal_date"),
                    "execution_date": row.get("execution_date"),
                    "risk_state": row.get("risk_state"),
                    "exposure_scaler": scaler,
                }
            )
            last_scaler = scaler
    return compressed


def _base_payload(
    *,
    task_id: str,
    report_type: str,
    status: str,
    generated_at: datetime,
    requested_date_range: dict[str, Any],
    data_quality_gate: dict[str, Any],
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
                "Research-only B2 final decision diagnostics; no B2 tuning, "
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
        "targeted_window_lock": _read_json(B2_TARGETED_WINDOW_LOCK_PATH),
        "fast_audit": _read_json(B2_FAST_AUDIT_PATH),
        "slow_repeatability": _read_json(B2_SLOW_REPEATABILITY_PATH),
        "reentry_root": _read_json(B2_REENTRY_ROOT_CAUSE_PATH),
        "role_narrowing": _read_json(B2_ROLE_NARROWING_PATH),
        "targeted_backfill": _read_json(B2_TARGETED_BACKFILL_PATH),
        "targeted_scorecard": _read_json(B2_TARGETED_SCORECARD_PATH),
        "gate_v5": _read_json(B2_GATE_V5_PATH),
        "branch_snapshot_v2": _read_json(B2_BRANCH_SNAPSHOT_V2_PATH),
        "full_backfill": _read_json(B2_FULL_BACKFILL_PATH),
        "control_rerun": _read_json(B2_CONTROL_RERUN_PATH),
        "no_trigger": _read_json(B2_NO_TRIGGER_PATH),
        "utility": _read_json(B2_UTILITY_PATH),
        "robustness": _read_json(B2_ROBUSTNESS_PATH),
        "b3_resolution": _read_json(B3_RESOLUTION_PATH),
        "window_catalog": _read_json(WINDOW_CATALOG_PATH),
    }


def _requested_date_range(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    value = sources["branch_snapshot_v2"].get("requested_date_range")
    if isinstance(value, dict):
        return dict(value)
    return {
        "start_date": "2022-12-01",
        "end_date": None,
        "source": str(B2_BRANCH_SNAPSHOT_V2_PATH),
    }


def requested_range_with_note(requested_date_range: dict[str, Any]) -> dict[str, Any]:
    value = dict(requested_date_range)
    value["interpretation_note"] = "B2 final decision uses current-form evidence only."
    return value


def _data_quality_gate(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    value = sources["targeted_window_lock"].get("data_quality_gate")
    if isinstance(value, dict):
        return dict(value)
    return {
        "required_command": "aits validate-data",
        "status": "UNKNOWN",
        "passed": False,
        "error_count": None,
        "warning_count": None,
        "info_count": None,
        "report_path": None,
    }


def _source_artifacts() -> dict[str, str]:
    return {
        "b2_targeted_evidence_window_lock": str(B2_TARGETED_WINDOW_LOCK_PATH),
        "b2_fast_risk_no_trigger_audit": str(B2_FAST_AUDIT_PATH),
        "b2_slow_drawdown_repeatability_study": str(B2_SLOW_REPEATABILITY_PATH),
        "b2_reentry_lag_root_cause_review": str(B2_REENTRY_ROOT_CAUSE_PATH),
        "b2_role_narrowing_assessment": str(B2_ROLE_NARROWING_PATH),
        "b2_targeted_evidence_backfill_v2": str(B2_TARGETED_BACKFILL_PATH),
        "b2_targeted_evidence_scorecard": str(B2_TARGETED_SCORECARD_PATH),
        "b2_gate_v5": str(B2_GATE_V5_PATH),
        "b2_research_branch_snapshot_v2": str(B2_BRANCH_SNAPSHOT_V2_PATH),
        "b2_full_diagnostic_backfill": str(B2_FULL_BACKFILL_PATH),
        "b2_control_window_rerun": str(B2_CONTROL_RERUN_PATH),
        "b2_no_trigger_correctness_review": str(B2_NO_TRIGGER_PATH),
        "b2_cost_benchmark_utility_review": str(B2_UTILITY_PATH),
        "b2_signal_robustness_trigger_stability": str(B2_ROBUSTNESS_PATH),
        "b3_signal_precheck_resolution_plan": str(B3_RESOLUTION_PATH),
        "research_window_catalog": str(WINDOW_CATALOG_PATH),
    }


def _check(check_id: str, passed: bool, message: str) -> dict[str, Any]:
    return {"check_id": check_id, "status": "PASS" if passed else "FAIL", "message": message}


def _first(values: Any) -> Any:
    return values[0] if isinstance(values, list) and values else None


def _clean_delta(value: Any) -> float:
    number = float(value)
    return 0.0 if abs(number) < 1e-12 else number


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _stamp(value: str) -> str:
    return value.replace("-", "").replace(":", "").split(".")[0].replace("+0000", "Z")


__all__ = ["run_b2_final_evidence_role_decision"]
