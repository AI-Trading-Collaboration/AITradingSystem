from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_confirmation_cycle as cycle

CONFIRMATION_PLAN_ID = "808e55a74ca6951f"


def write_confirmation_plan_fixture(tmp_path: Path) -> dict[str, Any]:
    plan_dir = tmp_path / "forward_confirmation_plan" / CONFIRMATION_PLAN_ID
    plan_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_forward_confirmation_plan_manifest",
        "confirmation_plan_id": CONFIRMATION_PLAN_ID,
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "auto_apply": False,
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
    }
    targets = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_confirmation_targets",
        "targets": [
            {
                "target_id": "limited_adjustment_vs_no_trade",
                "priority": "HIGH",
                "windows": [1, 5, 10, 20],
                "required_forward_events": 10,
                "success_criteria": {
                    "win_rate_vs_no_trade_min": 0.55,
                    "avg_relative_return_min": 0.0,
                    "drawdown_delta_max": 0.0,
                },
                "current_status": "IN_PROGRESS",
            },
            {
                "target_id": "defensive_limited_adjustment_drawdown",
                "priority": "HIGH",
                "windows": [5, 10, 20],
                "required_pressure_regime_events": 5,
                "success_criteria": {
                    "drawdown_delta_vs_no_trade_max": 0.0,
                    "win_rate_vs_no_trade_min": 0.50,
                },
                "current_status": "IN_PROGRESS",
            },
            {
                "target_id": "consensus_target_risk",
                "priority": "MEDIUM",
                "windows": [5, 10, 20],
                "required_forward_events": 10,
                "success_criteria": {
                    "drawdown_delta_vs_limited_adjustment_max": 0.0,
                    "turnover_delta_max": 0.0,
                },
                "current_status": "WATCH_ONLY",
            },
        ],
        "broker_action_allowed": False,
        "production_effect": "none",
    }
    failures = {
        "schema_version": 1,
        "report_type": "etf_dynamic_v3_confirmation_failure_conditions",
        "failure_conditions": [
            {
                "target": "limited_adjustment_vs_no_trade",
                "condition": "underperforms_no_trade",
                "action": "tighten_or_disable_limited_adjustment_proposal",
            },
            {
                "target": "limited_adjustment_vs_no_trade",
                "condition": "drawdown_worsening_persists",
                "action": "do_not_loosen_rules",
            },
            {
                "target": "defensive_limited_adjustment_drawdown",
                "condition": "fails_to_reduce_drawdown_in_pressure_regime",
                "action": "rename_or_remove_defensive_label",
            },
            {
                "target": "consensus_target_risk",
                "condition": "excess_drawdown_persists",
                "action": "keep_consensus_target_as_reference_only",
            },
        ],
        "broker_action_allowed": False,
        "production_effect": "none",
    }
    _write_json(plan_dir / "confirmation_plan_manifest.json", manifest)
    _write_json(plan_dir / "confirmation_targets.json", targets)
    _write_json(plan_dir / "failure_conditions.json", failures)
    (plan_dir / "forward_confirmation_plan_report.md").write_text("plan\n", encoding="utf-8")
    (plan_dir / "reader_brief_section.md").write_text("brief\n", encoding="utf-8")
    position_config_path = tmp_path / "position_advisory_v1.yaml"
    position_config_path.write_text("schema_version: 1\n", encoding="utf-8")
    return {
        "confirmation_plan_id": CONFIRMATION_PLAN_ID,
        "confirmation_plan_root": tmp_path / "forward_confirmation_plan",
        "registry_dir": tmp_path / "forward_confirmation_registry",
        "registry_yaml_path": tmp_path / "registry" / "targets.yaml",
        "progress_dir": tmp_path / "confirmation_progress",
        "evaluation_dir": tmp_path / "confirmation_evaluation",
        "cycle_dir": tmp_path / "rule_review_cycle",
        "journal_path": tmp_path / "rule_owner_decision" / "rule_owner_decision_journal.jsonl",
        "limited_dir": tmp_path / "limited_vs_notrade",
        "consensus_dir": tmp_path / "consensus_risk",
        "position_config_path": position_config_path,
    }


def register_targets_fixture(tmp_path: Path) -> dict[str, Any]:
    paths = write_confirmation_plan_fixture(tmp_path)
    registry = cycle.register_confirmation_targets(
        confirmation_plan_id=paths["confirmation_plan_id"],
        confirmation_plan_dir=paths["confirmation_plan_root"],
        output_dir=paths["registry_dir"],
        registry_yaml_path=paths["registry_yaml_path"],
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )
    return {**paths, "registry": registry}


def write_progress_sources(paths: dict[str, Any]) -> None:
    limited_dir = paths["limited_dir"] / "limited-focus"
    limited_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        limited_dir / "limited_vs_notrade_manifest.json",
        {
            "schema_version": 1,
            "focus_id": "limited-focus",
            "status": "PASS_WITH_WARNINGS",
            "available_count": 2,
            "auto_policy_apply": False,
            "broker_action_allowed": False,
            "broker_action_taken": False,
            "production_effect": "none",
        },
    )
    _write_jsonl(
        limited_dir / "sample_inventory.jsonl",
        [
            {
                "sample_id": "sample-1",
                "window_days": 1,
                "relative_return": 0.00049,
                "sample_status": "AVAILABLE",
            },
            {
                "sample_id": "sample-2",
                "window_days": 1,
                "relative_return": 0.00049,
                "sample_status": "AVAILABLE",
            },
        ],
    )
    _write_json(
        limited_dir / "window_comparison_metrics.json",
        {
            "schema_version": 1,
            "by_window": [
                {
                    "window_days": 1,
                    "available_count": 2,
                    "win_rate": 1.0,
                    "avg_relative_return": 0.00049,
                    "avg_drawdown_delta": None,
                    "confidence": "LOW",
                },
                {
                    "window_days": 5,
                    "available_count": 0,
                    "win_rate": 0.0,
                    "avg_relative_return": 0.0,
                    "avg_drawdown_delta": None,
                    "confidence": "INSUFFICIENT_DATA",
                },
                {
                    "window_days": 10,
                    "available_count": 0,
                    "win_rate": 0.0,
                    "avg_relative_return": 0.0,
                    "avg_drawdown_delta": None,
                    "confidence": "INSUFFICIENT_DATA",
                },
                {
                    "window_days": 20,
                    "available_count": 0,
                    "win_rate": 0.0,
                    "avg_relative_return": 0.0,
                    "avg_drawdown_delta": None,
                    "confidence": "INSUFFICIENT_DATA",
                },
            ],
            "overall_recommendation": "continue_tracking",
            "policy_mutated": False,
            "production_effect": "none",
        },
    )
    (limited_dir / "limited_vs_notrade_report.md").write_text("limited\n", encoding="utf-8")

    consensus_dir = paths["consensus_dir"] / "consensus-risk"
    consensus_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        consensus_dir / "consensus_risk_manifest.json",
        {
            "schema_version": 1,
            "risk_id": "consensus-risk",
            "status": "INSUFFICIENT_DATA",
            "consensus_target_risk": "INSUFFICIENT_DATA",
            "sample_count": 0,
            "consensus_target_default_execution_recommended": False,
            "auto_policy_apply": False,
            "broker_action_allowed": False,
            "broker_action_taken": False,
            "production_effect": "none",
        },
    )
    _write_json(
        consensus_dir / "consensus_exposure_summary.json",
        {"schema_version": 1, "sample_count": 0},
    )
    _write_json(
        consensus_dir / "consensus_drawdown_risk.json",
        {
            "schema_version": 1,
            "window_results": [
                {
                    "window_days": window,
                    "available_count": 0,
                    "drawdown_delta_vs_no_trade": 0.0,
                    "risk_status": "INSUFFICIENT_DATA",
                }
                for window in [5, 10, 20]
            ],
        },
    )
    _write_json(
        consensus_dir / "consensus_turnover_risk.json",
        {"schema_version": 1, "avg_turnover": 0.0, "turnover_status": "INSUFFICIENT_DATA"},
    )
    (consensus_dir / "consensus_risk_report.md").write_text("consensus\n", encoding="utf-8")


def progress_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = register_targets_fixture(tmp_path)
    write_progress_sources(fixture)
    progress = cycle.update_confirmation_progress(
        registry_id=fixture["registry"]["registry_id"],
        registry_dir=fixture["registry_dir"],
        output_dir=fixture["progress_dir"],
        limited_vs_notrade_dir=fixture["limited_dir"],
        consensus_risk_dir=fixture["consensus_dir"],
        generated_at=datetime(2026, 6, 10, 1, tzinfo=UTC),
    )
    return {**fixture, "progress": progress}


def evaluation_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = progress_fixture(tmp_path)
    evaluation = cycle.run_confirmation_evaluation(
        progress_id=fixture["progress"]["progress_id"],
        progress_dir=fixture["progress_dir"],
        output_dir=fixture["evaluation_dir"],
        generated_at=datetime(2026, 6, 10, 2, tzinfo=UTC),
    )
    return {**fixture, "evaluation": evaluation}


def cycle_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = evaluation_fixture(tmp_path)
    review_cycle = cycle.run_rule_review_cycle(
        registry_id=fixture["registry"]["registry_id"],
        progress_id=fixture["progress"]["progress_id"],
        evaluation_id=fixture["evaluation"]["evaluation_id"],
        registry_dir=fixture["registry_dir"],
        progress_dir=fixture["progress_dir"],
        evaluation_dir=fixture["evaluation_dir"],
        output_dir=fixture["cycle_dir"],
        generated_at=datetime(2026, 6, 10, 3, tzinfo=UTC),
    )
    return {**fixture, "cycle": review_cycle}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
