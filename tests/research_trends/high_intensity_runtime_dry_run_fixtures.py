from __future__ import annotations

from pathlib import Path
from typing import Any

from high_intensity_runtime_integration_plan_fixtures import (
    build_high_intensity_runtime_integration_plan_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_observe_only_runtime_integration_plan import (
    run_high_intensity_risk_cap_observe_only_runtime_integration_plan,
)

__all__ = [
    "build_high_intensity_runtime_dry_run_fixture",
    "read_json",
    "sample_detection_rows",
    "sample_selected_rule",
    "sample_trigger_source_rows",
    "write_json",
]


def build_high_intensity_runtime_dry_run_fixture(tmp_path: Path) -> dict[str, Path]:
    fixture = build_high_intensity_runtime_integration_plan_fixture(tmp_path)
    runtime_plan_dir = tmp_path / "runtime_plan"
    runtime_docs_root = tmp_path / "runtime_plan_docs"
    dynamic_dry_run_dir = tmp_path / "dynamic_dry_run"
    _write_runtime_dynamic_dry_run(dynamic_dry_run_dir)
    run_high_intensity_risk_cap_observe_only_runtime_integration_plan(
        continue_decision_dir=fixture["continue_decision_dir"],
        forward_outcome_review_dir=fixture["forward_outcome_review_dir"],
        partial_readiness_dir=fixture["partial_readiness_dir"],
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        output_dir=runtime_plan_dir,
        docs_root=runtime_docs_root,
    )
    return {
        **fixture,
        "runtime_integration_plan_dir": runtime_plan_dir,
        "runtime_plan_docs_root": runtime_docs_root,
        "dynamic_dry_run_dir": dynamic_dry_run_dir,
    }


def _write_runtime_dynamic_dry_run(root: Path) -> None:
    safety = {
        "research_only": True,
        "manual_review_only": True,
        "runtime_scheduler_enabled": False,
        "target_weight_action_allowed": False,
        "rebalance_instruction_allowed": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "portfolio_effect": "none",
        "production_effect": "none",
    }
    payloads: dict[str, dict[str, Any]] = {
        "dynamic_target_exposure_cap_dry_run_summary.json": {
            "task_id": (
                "TRADING-2332_SOURCE_BOUND_EXPOSURE_CAP_DRY_RUN_WITH_DYNAMIC_TARGET_BASELINE"
            ),
            "record_count": 2,
            "data_quality_status": "PASS_WITH_WARNINGS",
            "data_quality_gate_executed": True,
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            "pit_policy": "PIT_APPROXIMATION_READY",
            **safety,
        },
        "dynamic_target_risk_cap_trigger_alignment_matrix.json": {
            "rows": sample_trigger_source_rows(),
            "data_quality_status": "PASS_WITH_WARNINGS",
            **safety,
        },
        "dynamic_target_exposure_cap_dry_run_result.json": {
            "rows": [],
            **safety,
        },
        "dynamic_target_data_quality_report.json": {
            "data_quality_status": "PASS_WITH_WARNINGS",
            "error_count": 0,
            **safety,
        },
        "dynamic_target_pit_caveat_interpretation_boundary.json": {
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            **safety,
        },
    }
    for filename, payload in payloads.items():
        write_json(root / filename, payload)


def sample_selected_rule(threshold: float = 1.0) -> dict[str, object]:
    return {
        "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
        "selected_rule_version": "v1",
        "trigger_rule": {
            "threshold_value": threshold,
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            "pit_policy": "PIT_APPROXIMATION_READY",
        },
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def sample_trigger_source_rows() -> list[dict[str, object]]:
    return [
        {
            "date": "2026-06-30",
            "target_asset": "QQQ",
            "risk_cap_triggered": True,
            "risk_cap_intensity": "high",
            "risk_cap_score": 1.25,
            "scope_active": True,
            "signal_direction": "portfolio_level_risk_cap",
            "decision_timestamp": "2026-06-30T00:00:00Z",
            "risk_cap_decision_timestamp": "2026-06-30T09:00:00+09:00",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        {
            "date": "2026-07-01",
            "target_asset": "QQQ",
            "risk_cap_triggered": False,
            "risk_cap_intensity": "none",
            "risk_cap_score": 0.0,
            "scope_active": True,
            "signal_direction": "portfolio_level_risk_cap",
            "decision_timestamp": "2026-07-01T00:00:00Z",
            "risk_cap_decision_timestamp": "2026-07-01T09:00:00+09:00",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
    ]


def sample_detection_rows() -> list[dict[str, object]]:
    return [
        {
            "detection_record_id": "det_1",
            "date": "2026-06-30",
            "target_asset": "QQQ",
            "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
            "selected_rule_hash": "hash",
            "risk_cap_triggered": True,
            "risk_cap_intensity": "high",
            "risk_cap_score": 1.25,
            "scope_active": True,
            "signal_direction": "portfolio_level_risk_cap",
            "as_of_timestamp": "2026-06-30T09:00:00+09:00",
            "decision_timestamp": "2026-06-30T00:00:00Z",
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            "pit_policy": "PIT_APPROXIMATION_READY",
            "high_intensity_triggered": True,
            "high_intensity_reason": "COMPOSITE_HIGH_INTENSITY_RULE",
            "detection_status": "DETECTED",
            "blocked_reason": "",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
    ]
