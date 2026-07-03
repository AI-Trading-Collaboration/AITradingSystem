from __future__ import annotations

from pathlib import Path

from high_intensity_forward_observe_plan_fixtures import (
    build_high_intensity_forward_observe_plan_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_forward_observe_plan import (
    run_high_intensity_risk_cap_forward_observe_plan,
)

__all__ = [
    "build_high_intensity_threshold_selection_fixture",
    "read_json",
    "sample_candidate_rows",
    "sample_selected_candidate",
    "write_json",
]


def build_high_intensity_threshold_selection_fixture(tmp_path: Path) -> dict[str, Path]:
    fixture = build_high_intensity_forward_observe_plan_fixture(tmp_path)
    forward_observe_plan_dir = tmp_path / "forward_observe_plan"
    run_high_intensity_risk_cap_forward_observe_plan(
        dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
        dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        readiness_dir=fixture["readiness_dir"],
        timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
        simulation_policy_dir=fixture["simulation_policy_dir"],
        output_dir=forward_observe_plan_dir,
        docs_root=tmp_path / "forward_docs",
    )
    return {
        **fixture,
        "forward_observe_plan_dir": forward_observe_plan_dir,
    }


def sample_candidate_rows() -> list[dict[str, object]]:
    return [
        {
            "threshold_id": "P90_RISK_CAP_SCORE",
            "threshold_type": "INTENSITY_PERCENTILE_THRESHOLD",
            "threshold_value": "P90",
            "numeric_threshold_value": 0.90,
            "trigger_count_estimate": 36,
            "trigger_density_estimate": 0.11,
            "expected_observe_event_frequency": "36 historical candidate events",
            "historical_binding_overlap_count": 36,
            "historical_false_cost_context": "FALSE_COST_BLOCKING",
            "historical_downside_context": "DOWNSIDE_PROTECTION_POSITIVE_PROXY",
            "historical_missed_upside_context": "MISSED_UPSIDE_BLOCKING",
            "overbinding_risk": "HIGH_FROM_2333_BROAD_MECHANICS_CONTEXT",
            "missed_stress_risk": "MEDIUM",
            "interpretability_score": 0.8,
            "implementation_complexity": "LOW",
            "recommended_status": "TOO_BROAD_OVERBINDING_RISK",
        },
        {
            "threshold_id": "P95_RISK_CAP_SCORE",
            "threshold_type": "INTENSITY_PERCENTILE_THRESHOLD",
            "threshold_value": "P95",
            "numeric_threshold_value": 0.95,
            "trigger_count_estimate": 5,
            "trigger_density_estimate": 0.015,
            "expected_observe_event_frequency": "5 historical candidate events",
            "historical_binding_overlap_count": 5,
            "historical_false_cost_context": "FALSE_COST_BLOCKING",
            "historical_downside_context": "DOWNSIDE_PROTECTION_POSITIVE_PROXY",
            "historical_missed_upside_context": "MISSED_UPSIDE_BLOCKING",
            "overbinding_risk": "MEDIUM",
            "missed_stress_risk": "HIGH_IF_SCORE_SATURATION_DROPS_STRESS_EVENTS",
            "interpretability_score": 0.8,
            "implementation_complexity": "LOW",
            "recommended_status": "TOO_NARROW_MISSED_STRESS_RISK",
        },
        sample_selected_candidate(),
    ]


def sample_selected_candidate() -> dict[str, object]:
    return {
        "threshold_id": "COMPOSITE_HIGH_INTENSITY_RULE",
        "threshold_type": "COMPOSITE_HIGH_INTENSITY_RULE",
        "threshold_value": (
            "risk_cap_triggered AND scope_active AND "
            "risk_cap_score >= 1.0 AND signal_direction != none"
        ),
        "numeric_threshold_value": 1.0,
        "trigger_count_estimate": 24,
        "trigger_density_estimate": 0.06,
        "expected_observe_event_frequency": "24 historical candidate events",
        "historical_binding_overlap_count": 24,
        "historical_false_cost_context": "FALSE_COST_BLOCKING",
        "historical_downside_context": "DOWNSIDE_PROTECTION_POSITIVE_PROXY",
        "historical_missed_upside_context": "MISSED_UPSIDE_BLOCKING",
        "overbinding_risk": "LOWER_THAN_SIMPLE_PERCENTILE",
        "missed_stress_risk": "MEDIUM",
        "false_warning_risk": "MODERATE",
        "interpretability_score": 0.7,
        "implementation_complexity": "MEDIUM",
        "selection_score": 0.9,
        "selection_label": "SELECTED",
        "source_recommended_status": "CANDIDATE_FOR_2335_SELECTION",
        "recommended_status": "CANDIDATE_FOR_2335_SELECTION",
    }
