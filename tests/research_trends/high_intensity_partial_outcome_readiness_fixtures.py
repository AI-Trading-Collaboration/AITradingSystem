from __future__ import annotations

import json
from pathlib import Path
from typing import Any

__all__ = [
    "build_high_intensity_partial_outcome_readiness_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_partial_outcome_readiness_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    outcome_binder_dir = tmp_path / "outcome_binder"
    event_logger_dir = tmp_path / "event_logger"
    threshold_selection_dir = tmp_path / "threshold_selection"
    forward_observe_plan_dir = tmp_path / "forward_observe_plan"
    for directory in (
        outcome_binder_dir,
        event_logger_dir,
        threshold_selection_dir,
        forward_observe_plan_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    cluster_rows, event_rows, event_logger_clusters = _sample_outcome_rows()
    _write_outcome_binder_artifacts(outcome_binder_dir, cluster_rows, event_rows)
    _write_event_logger_artifacts(event_logger_dir, event_logger_clusters, event_rows)
    _write_threshold_selection_artifacts(threshold_selection_dir)
    _write_forward_observe_plan_artifacts(forward_observe_plan_dir)
    return {
        "outcome_binder_dir": outcome_binder_dir,
        "event_logger_dir": event_logger_dir,
        "threshold_selection_dir": threshold_selection_dir,
        "forward_observe_plan_dir": forward_observe_plan_dir,
    }


def _sample_outcome_rows() -> tuple[
    list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]
]:
    cluster_rows: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    logger_clusters: list[dict[str, Any]] = []
    assets = ["QQQ", "SGOV", "TQQQ"]
    for index in range(60):
        asset = assets[index % len(assets)]
        event_id = f"hievt_{index:03d}"
        cluster_id = f"hicl_{index:03d}"
        if index < 54:
            event_date = "2026-04-01"
            cluster_active_days = 1
            not_due_horizons: set[str] = set()
        elif index < 57:
            event_date = "2026-06-05"
            cluster_active_days = 5
            not_due_horizons = {"20d"}
        else:
            event_date = "2026-06-15"
            cluster_active_days = 1
            not_due_horizons = {"10d", "20d"}
        logger_clusters.append(
            {
                "event_cluster_id": cluster_id,
                "primary_event_id": event_id,
                "target_asset": asset,
                "cluster_start_date": event_date,
                "cluster_end_date": event_date,
                "cluster_active_days": cluster_active_days,
                "monthly_event_count": 6 if index >= 54 else 1,
                "cluster_warning": "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"
                if index >= 54
                else "",
                "promotion_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            }
        )
        for horizon in ("1d", "5d", "10d", "20d"):
            due_date = {
                "1d": "2026-06-16",
                "5d": "2026-06-22",
                "10d": "2026-06-29",
                "20d": "2026-07-13",
            }[horizon]
            status = "OUTCOME_NOT_DUE" if horizon in not_due_horizons else "OUTCOME_BOUND"
            quality = "OUTCOME_PENDING_NOT_DUE" if status == "OUTCOME_NOT_DUE" else "PASS"
            downside = index < 21 and status == "OUTCOME_BOUND"
            false_warning = 21 <= index < 44 and status == "OUTCOME_BOUND"
            missed_upside = 44 <= index < 54 and status == "OUTCOME_BOUND"
            rebound = (false_warning or missed_upside) and status == "OUTCOME_BOUND"
            if downside:
                forward_return = -0.04
                max_drawdown = -0.06
            elif false_warning:
                forward_return = 0.025
                max_drawdown = -0.004
            elif missed_upside:
                forward_return = 0.08
                max_drawdown = -0.003
            else:
                forward_return = 0.0
                max_drawdown = -0.002
            event_rows.append(
                {
                    "event_id": event_id,
                    "event_cluster_id": cluster_id,
                    "target_asset": asset,
                    "event_date": event_date,
                    "horizon": horizon,
                    "outcome_due_date": due_date,
                    "outcome_due_status": "NOT_DUE"
                    if status == "OUTCOME_NOT_DUE"
                    else "HISTORICAL_DUE",
                    "outcome_binding_status": status,
                    "outcome_quality_status": quality,
                    "pending_outcome_id": f"hiout_{index:03d}_{horizon}",
                    "forward_return": forward_return,
                    "forward_max_drawdown": max_drawdown,
                    "promotion_allowed": False,
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                }
            )
            cluster_rows.append(
                {
                    "event_cluster_id": cluster_id,
                    "primary_event_id": event_id,
                    "target_asset": asset,
                    "cluster_start_date": event_date,
                    "cluster_end_date": event_date,
                    "cluster_active_days": cluster_active_days,
                    "horizon": horizon,
                    "outcome_due_date": due_date,
                    "outcome_due_status": "NOT_DUE"
                    if status == "OUTCOME_NOT_DUE"
                    else "HISTORICAL_DUE",
                    "cluster_outcome_binding_status": status,
                    "cluster_outcome_quality_status": quality,
                    "cluster_forward_return": forward_return,
                    "cluster_forward_max_drawdown": max_drawdown,
                    "cluster_forward_max_return": max(forward_return, 0.0),
                    "cluster_forward_min_return": min(forward_return, max_drawdown),
                    "cluster_stress_detected": downside,
                    "cluster_rebound_detected": rebound,
                    "cluster_false_warning_candidate": false_warning,
                    "cluster_missed_upside_candidate": missed_upside,
                    "cluster_downside_capture_candidate": downside,
                    "cluster_manual_review_would_have_helped_candidate": downside
                    or false_warning,
                    "cluster_realized_volatility": 0.25 if downside else 0.1,
                    "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
                    "trigger_day_count": 1,
                    "promotion_allowed": False,
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                }
            )
    return cluster_rows, event_rows, logger_clusters


def _write_outcome_binder_artifacts(
    directory: Path,
    cluster_rows: list[dict[str, Any]],
    event_rows: list[dict[str, Any]],
) -> None:
    source_safety = _source_safety(outcome_binding_executed=True)
    summary = {
        "status": "PARTIAL_OUTCOME_BINDING_WITH_NOT_DUE_HORIZONS",
        "event_count": 60,
        "cluster_count": 60,
        "pending_outcome_count": 240,
        "event_actual_path_outcome_count": 240,
        "cluster_actual_path_outcome_count": 240,
        "coverage_status": "PARTIAL_COVERAGE_WITH_NOT_DUE_HORIZONS",
        "validate_data_executed": True,
        "validate_data_as_of": "2026-06-29",
        "validate_data_status": "PASS_WITH_WARNINGS",
        "validate_data_error_count": 0,
        "next_task": "TRADING-2339_High_Intensity_Risk_Cap_Partial_Outcome_Readiness_Review",
        **source_safety,
    }
    write_json(directory / "high_intensity_outcome_binder_summary.json", summary)
    write_json(
        directory / "high_intensity_event_actual_path_outcome_matrix.json",
        {"rows": event_rows, **source_safety},
    )
    write_json(
        directory / "high_intensity_cluster_actual_path_outcome_matrix.json",
        {"rows": cluster_rows, **source_safety},
    )
    write_json(
        directory / "high_intensity_trigger_day_actual_path_context.json",
        {"rows": [], **source_safety},
    )
    write_json(
        directory / "high_intensity_outcome_coverage_report.json",
        {
            "pending_outcome_count": 240,
            "outcome_bound_count": 231,
            "outcome_not_due_count": 9,
            "outcome_blocked_count": 0,
            "cluster_count": 60,
            "coverage_status": "PARTIAL_COVERAGE_WITH_NOT_DUE_HORIZONS",
            **source_safety,
        },
    )
    write_json(
        directory / "high_intensity_horizon_outcome_quality_report.json",
        {"rows": [], **source_safety},
    )
    write_json(
        directory / "high_intensity_rebound_stress_classification_matrix.json",
        {"rows": [], **source_safety},
    )
    for filename in (
        "high_intensity_false_warning_classification_report.json",
        "high_intensity_missed_upside_classification_report.json",
        "high_intensity_downside_capture_classification_report.json",
        "high_intensity_manual_review_usefulness_proxy_report.json",
        "high_intensity_outcome_binder_interpretation_boundary.json",
        "high_intensity_2339_readiness_checklist.json",
        "high_intensity_outcome_binder_safety_boundary.json",
    ):
        write_json(directory / filename, source_safety)
    write_json(
        directory / "high_intensity_cluster_weighting_policy.json",
        {
            "primary_analysis_level": "cluster",
            "secondary_analysis_level": "event",
            "trigger_day_level_usage": "context_only",
            "cluster_weighting_rule": "one_cluster_horizon_one_primary_sample",
            **source_safety,
        },
    )
    write_json(
        directory / "high_intensity_actual_path_data_quality_report.json",
        {
            "data_quality_status": "PASS_WITH_WARNINGS",
            "validate_data_executed": True,
            "validate_data_as_of": "2026-06-29",
            "validate_data_status": "PASS_WITH_WARNINGS",
            "validate_data_error_count": 0,
            **source_safety,
        },
    )
    write_json(
        directory / "high_intensity_2339_task_route.json",
        {
            "next_task": "TRADING-2339_High_Intensity_Risk_Cap_Partial_Outcome_Readiness_Review",
            **source_safety,
        },
    )


def _write_event_logger_artifacts(
    directory: Path,
    logger_clusters: list[dict[str, Any]],
    event_rows: list[dict[str, Any]],
) -> None:
    safety = _source_safety(outcome_binding_executed=False)
    events_by_id = {
        row["event_id"]: {
            "event_id": row["event_id"],
            "event_cluster_id": row["event_cluster_id"],
            "event_date": row["event_date"],
            "target_asset": row["target_asset"],
            **safety,
        }
        for row in event_rows
    }
    write_json(
        directory / "high_intensity_event_logger_summary.json",
        {
            "trigger_day_count": 168,
            "event_count_after_dedup": len(events_by_id),
            "cluster_count": len(logger_clusters),
            **safety,
        },
    )
    write_json(
        directory / "high_intensity_observe_event_log.json",
        {"rows": list(events_by_id.values()), **safety},
    )
    write_json(
        directory / "high_intensity_observe_trigger_day_log.json",
        {"rows": list(events_by_id.values()), **safety},
    )
    write_json(
        directory / "high_intensity_observe_event_cluster_registry.json",
        {"rows": logger_clusters, **safety},
    )
    write_json(
        directory / "high_intensity_pending_outcome_registry.json",
        {"rows": event_rows, **safety},
    )
    write_json(
        directory / "high_intensity_outcome_collection_schedule.json",
        {"rows": event_rows, **safety},
    )
    write_json(
        directory / "high_intensity_monthly_concentration_report.json",
        {
            "monthly_concentration_status": "PASS_WITH_WARNINGS",
            "monthly_concentration_warnings": ["MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL"],
            "monthly_event_counts": {"2026-04": 54, "2026-06": 6},
            "monthly_event_guardrail": 3,
            **safety,
        },
    )
    write_json(
        directory / "high_intensity_manual_review_event_queue.json",
        {"rows": list(events_by_id.values()), **safety},
    )
    write_json(directory / "high_intensity_event_logger_interpretation_boundary.json", safety)
    write_json(directory / "high_intensity_event_logger_safety_boundary.json", safety)


def _write_threshold_selection_artifacts(directory: Path) -> None:
    safety = _source_safety(outcome_binding_executed=False)
    write_json(
        directory / "high_intensity_selected_trigger_rule.json",
        {"selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE", **safety},
    )
    write_json(
        directory / "high_intensity_selected_trigger_contract.json",
        {
            "contract_id": "HIGH_INTENSITY_SELECTED_TRIGGER_CONTRACT_V1",
            "contract_version": "v1",
            "selected_rule_hash": "fixture-selected-rule-hash",
            "required_input_fields": [
                "date",
                "target_asset",
                "risk_cap_triggered",
                "risk_cap_intensity",
                "risk_cap_score",
                "scope_active",
                "signal_direction",
                "as_of_timestamp",
                "decision_timestamp",
                "known_at_policy",
                "pit_policy",
            ],
            **safety,
        },
    )
    write_json(directory / "high_intensity_event_logger_input_contract.json", safety)
    write_json(directory / "high_intensity_threshold_selection_caveat_report.json", safety)
    write_json(
        directory / "high_intensity_selected_rule_backtest_context.json",
        {
            "selected_threshold_id": "COMPOSITE_HIGH_INTENSITY_RULE",
            "trigger_density_estimate": 0.06747,
            **safety,
        },
    )
    write_json(directory / "high_intensity_selected_rule_manual_review_boundary.json", safety)
    write_json(directory / "high_intensity_threshold_selection_safety_boundary.json", safety)


def _write_forward_observe_plan_artifacts(directory: Path) -> None:
    safety = _source_safety(outcome_binding_executed=False)
    write_json(
        directory / "high_intensity_forward_observe_plan_summary.json",
        {
            "status": "HIGH_INTENSITY_FORWARD_OBSERVE_PLAN_READY",
            "selected_rule_id": "COMPOSITE_HIGH_INTENSITY_RULE",
            "next_task": "TRADING-2335_High_Intensity_Risk_Cap_Threshold_Selection",
            **safety,
        },
    )
    for filename in (
        "high_intensity_forward_observe_event_schema.json",
        "high_intensity_forward_observe_evidence_contract.json",
        "high_intensity_actual_path_outcome_contract.json",
        "high_intensity_false_warning_missed_stress_framework.json",
        "high_intensity_stop_continue_archive_rules.json",
        "high_intensity_manual_review_boundary.json",
        "high_intensity_forward_observe_safety_boundary.json",
    ):
        write_json(directory / filename, safety)


def _source_safety(*, outcome_binding_executed: bool) -> dict[str, Any]:
    return {
        "research_only": True,
        "outcome_binding_executed": outcome_binding_executed,
        "original_event_log_mutated": False,
        "runtime_scheduler_enabled": False,
        "target_weight_action_allowed": False,
        "rebalance_instruction_allowed": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "portfolio_effect": "none",
        "production_effect": "none",
        "manual_review_only": True,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
