from __future__ import annotations

from pathlib import Path

from high_intensity_threshold_selection_fixtures import (
    build_high_intensity_threshold_selection_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_threshold_selection import (
    run_high_intensity_risk_cap_threshold_selection,
)

__all__ = [
    "build_high_intensity_event_logger_fixture",
    "read_json",
    "sample_selected_rule",
    "sample_trigger_source_rows",
    "write_json",
]


def build_high_intensity_event_logger_fixture(tmp_path: Path) -> dict[str, Path]:
    fixture = build_high_intensity_threshold_selection_fixture(tmp_path)
    threshold_selection_dir = tmp_path / "threshold_selection"
    run_high_intensity_risk_cap_threshold_selection(
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        dynamic_diagnostics_dir=fixture["dynamic_diagnostics_dir"],
        dynamic_dry_run_dir=fixture["dynamic_dry_run_dir"],
        readiness_dir=fixture["readiness_dir"],
        timestamp_remediation_dir=fixture["timestamp_remediation_dir"],
        output_dir=threshold_selection_dir,
        docs_root=tmp_path / "threshold_docs",
    )
    return {
        **fixture,
        "threshold_selection_dir": threshold_selection_dir,
    }


def sample_selected_rule(threshold: float = 1.0) -> dict[str, object]:
    return {
        "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
        "selected_rule_version": "v1",
        "source_signal_family": "volatility_regime_scope_narrowed_risk_cap",
        "trigger_rule": {
            "threshold_type": "COMPOSITE_HIGH_INTENSITY_RULE",
            "threshold_value": threshold,
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            "latency_policy": "NEXT_TRADING_DAY_DECISION",
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
            "date": "2023-01-03",
            "target_asset": "QQQ",
            "risk_cap_triggered": True,
            "risk_cap_intensity": "high",
            "risk_cap_score": 1.05,
            "scope_active": True,
            "signal_direction": "portfolio_level_risk_cap",
            "decision_timestamp": "2023-01-03T00:00:00Z",
            "risk_cap_decision_timestamp": "2023-01-03T00:00:00+00:00",
            "trigger_source_hash": "fixture_hash",
        },
        {
            "date": "2023-01-04",
            "target_asset": "QQQ",
            "risk_cap_triggered": True,
            "risk_cap_intensity": "medium",
            "risk_cap_score": 0.95,
            "scope_active": True,
            "signal_direction": "portfolio_level_risk_cap",
            "decision_timestamp": "2023-01-04T00:00:00Z",
            "risk_cap_decision_timestamp": "2023-01-04T00:00:00+00:00",
            "trigger_source_hash": "fixture_hash",
        },
    ]
