from __future__ import annotations

from pathlib import Path
from typing import Any

from high_intensity_partial_outcome_readiness_fixtures import (
    build_high_intensity_partial_outcome_readiness_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_partial_outcome_readiness_review import (
    run_high_intensity_risk_cap_partial_outcome_readiness_review,
)

__all__ = [
    "build_high_intensity_forward_outcome_review_fixture",
    "read_json",
    "sample_cluster_review_row",
    "write_json",
]


def build_high_intensity_forward_outcome_review_fixture(tmp_path: Path) -> dict[str, Path]:
    fixture = build_high_intensity_partial_outcome_readiness_fixture(tmp_path)
    partial_readiness_dir = tmp_path / "partial_readiness"
    partial_docs_root = tmp_path / "partial_docs"
    run_high_intensity_risk_cap_partial_outcome_readiness_review(
        outcome_binder_dir=fixture["outcome_binder_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        output_dir=partial_readiness_dir,
        docs_root=partial_docs_root,
    )
    return {
        **fixture,
        "partial_readiness_dir": partial_readiness_dir,
        "partial_docs_root": partial_docs_root,
    }


def sample_cluster_review_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "event_cluster_id": "hicl_sample",
        "target_asset": "QQQ",
        "cluster_start_date": "2026-04-01",
        "cluster_end_date": "2026-04-01",
        "cluster_active_days": 1,
        "trigger_day_count": 1,
        "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
        "horizon_1d_status": "OUTCOME_BOUND",
        "horizon_5d_status": "OUTCOME_BOUND",
        "horizon_10d_status": "OUTCOME_BOUND",
        "horizon_20d_status": "OUTCOME_BOUND",
        "bound_horizon_count": 4,
        "not_due_horizon_count": 0,
        "cluster_forward_return_1d": 0.0,
        "cluster_forward_return_5d": 0.0,
        "cluster_forward_return_10d": 0.0,
        "cluster_forward_return_20d": 0.0,
        "cluster_max_drawdown_1d": -0.002,
        "cluster_max_drawdown_5d": -0.002,
        "cluster_max_drawdown_10d": -0.002,
        "cluster_max_drawdown_20d": -0.002,
        "stress_detected_any_horizon": False,
        "rebound_detected_any_horizon": False,
        "false_warning_candidate_any_horizon": False,
        "missed_upside_candidate_any_horizon": False,
        "downside_capture_candidate_any_horizon": False,
        "manual_review_would_have_helped_candidate": False,
        "cluster_evidence_label": "NO_MATERIAL_OUTCOME",
        "cluster_review_weight": 1.0,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    row.update(overrides)
    return row
