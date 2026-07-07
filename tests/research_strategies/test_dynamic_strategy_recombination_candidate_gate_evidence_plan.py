from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_recombination_candidate_gate_evidence_plan as plan
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_recombination_candidate_gate_evidence_plan_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "plan"
    docs_root = tmp_path / "docs" / "research"

    payload = plan.run_dynamic_strategy_recombination_candidate_gate_evidence_plan(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 7),
    )

    assert payload["status"] == plan.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["source_ready_for_gate_evidence_plan"] is True
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == plan.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["backtest_run"] is False
    assert payload["new_signal_generated"] is False
    assert payload["scoring_run"] is False
    assert payload["candidate_under_review"] == plan.BEST_RECOMBINATION_CANDIDATE
    assert payload["decision_from_2396"] == plan.EXPECTED_DECISION_FROM_2396
    assert payload["owner_decision_from_2397"] == plan.OWNER_DECISION_FROM_2397
    assert payload["gate_evidence_gap_summary_ready"] is True
    assert payload["targeted_improvement_plan_ready"] is True
    assert payload["retest_plan_2399_ready"] is True
    assert len(payload["planned_targeted_variants"]) == len(plan.TARGETED_VARIANTS)
    assert payload["recommended_next_research_task"] == plan.NEXT_ROUTE

    gap_summary = payload["gate_evidence_gap_summary"]
    gaps = gap_summary["gap_areas"]
    assert gap_summary["record_ready"] is True
    assert gaps["time_slice_evidence_gap"]["status"] == "GAP_REMAINS"
    assert gaps["regime_expectation_gap"]["status"] == "GAP_REMAINS"
    assert gaps["turnover_cost_evidence_gap"]["status"] == "GAP_REMAINS"
    assert gaps["valid_until_stale_signal_gap"]["status"] == "PASS"
    assert gaps["return_retention_gap"]["status"] == "ADEQUATE_BUT_MONITOR"

    targeted_plan = payload["targeted_improvement_plan"]
    assert targeted_plan["record_ready"] is True
    assert targeted_plan["variant_count"] == len(plan.TARGETED_VARIANTS)
    assert targeted_plan["shared_constraints"]["production_effect"] == "none"
    assert (
        targeted_plan["shared_constraints"][
            "monthly_rebalance_allowed_for_primary_decision"
        ]
        is False
    )

    retest_plan = payload["retest_plan_2399"]
    assert retest_plan["record_ready"] is True
    assert retest_plan["primary_execution_cadence"] == "valid_until_window"
    assert retest_plan["monthly_rebalance"]["allowed_for_primary_decision"] is False
    assert (
        plan.BEST_RECOMBINATION_CANDIDATE
        in retest_plan["required_2399_candidates"]["reference"]
    )
    assert retest_plan["recommended_next_research_task"] == plan.NEXT_ROUTE

    for field in plan.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    for key in (
        "json_path",
        "gate_evidence_gap_summary_json",
        "targeted_improvement_plan_json",
        "retest_plan_2399_json",
        "next_route_json",
        "markdown_path",
        "gate_evidence_gap_summary_markdown",
        "targeted_improvement_plan_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_recombination_candidate_gate_evidence_plan_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "plan_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-recombination-candidate-gate-evidence-plan",
            *_source_args(source_paths),
            "--as-of",
            "2026-07-07",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert plan.READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "gate_evidence_plan_result.json").exists()
    assert (output_root / "gate_evidence_gap_summary.json").exists()
    assert (output_root / "targeted_improvement_plan.json").exists()
    assert (output_root / "retest_plan_2399.json").exists()
    assert (output_root / "next_route.json").exists()


def test_dynamic_strategy_recombination_candidate_gate_evidence_plan_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_recombination_candidate_gate_evidence_plan"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-recombination-candidate-gate-evidence-plan"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("gate_evidence_plan_result.json" in item for item in entry["artifact_globs"])
    assert any("targeted_improvement_plan.json" in item for item in entry["artifact_globs"])
    assert any("retest_plan_2399.json" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_recombination_candidate_gate_evidence_plan" in catalog
    assert "dynamic-strategy-recombination-candidate-gate-evidence-plan" in system_flow
    assert plan.TASK_REGISTER_ID in task_register


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "owner_review_decision_2397": root / "owner_review_decision.json",
        "gate_evidence_gap_summary_2397": root / "gate_evidence_gap_summary.json",
        "observation_non_approval_record_2397": (
            root / "observation_non_approval_record.json"
        ),
        "next_route_2397": root / "next_route_2397.json",
        "recombination_retest_result_2396": root / "recombination_retest_result.json",
        "recombination_candidate_ranking_2396": (
            root / "recombination_candidate_ranking.json"
        ),
        "component_evidence_matrix_2396": root / "component_evidence_matrix.json",
        "decision_update_2396": root / "decision_update.json",
        "recombination_candidate_plan_2395": root / "recombination_candidate_plan.json",
        "candidate_definitions_2395": root / "recombination_candidate_definitions.json",
        "ablation_retest_result_2393": root / "ablation_retest_result.json",
        "component_attribution_matrix_2393": root / "component_attribution_matrix.json",
    }
    best = plan.BEST_RECOMBINATION_CANDIDATE
    ranking_row = {
        "rank": 1,
        "candidate_id": best,
        "decision": plan.EXPECTED_DECISION_FROM_2396,
        "annualized_return": 0.208832,
        "cost_adjusted_return": 0.208832,
        "max_drawdown": -0.160679,
        "turnover": 1.982428,
        "time_slice_pass_rate": 0.0,
        "regime_expectation_score": 0.325498,
        "return_retention_vs_raw_growth_tilt": 0.976494,
        "turnover_reduction_vs_raw_growth_tilt": -0.009088,
        "cost_stress_survival": "harsh",
        "valid_until_window_preserved": True,
        "no_stale_signal_carry_forward": True,
        "stale_signal_execution_count": 0,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
    }
    evidence_row = {
        "candidate_id": best,
        "components": [
            "growth_tilt_engine",
            "lower_turnover_guardrail",
            "guarded_turnover_transfer",
            "valid_until_window",
            "no_stale_signal_carry_forward",
        ],
        "recombination_quality": {
            "candidate_decision": plan.EXPECTED_DECISION_FROM_2396,
            "time_slice_pass_rate": 0.0,
            "regime_expectation_score": 0.325498,
            "return_per_drawdown_penalty": 1.299684,
        },
        "return_engine_metrics": {
            "return_retention_vs_raw_growth_tilt": 0.976494,
            "return_gap_vs_raw_growth_tilt": 0.005026,
            "upside_capture": 0.725894,
            "upside_capture_gap": 0.274106,
        },
        "guardrail_metrics": {
            "turnover_reduction_vs_raw_growth_tilt": -0.009088,
            "cost_drag_reduction": -0.005027,
            "drawdown_gap_vs_static": -0.020611,
            "realistic_cost_passed": True,
            "conservative_cost_passed": True,
            "harsh_cost_passed": True,
        },
        "valid_until_metrics": {
            "valid_until_window_preserved": True,
            "near_expiry_signal_behavior": "NO_STALE_EXECUTION",
            "no_stale_signal_carry_forward": True,
            "signal_to_execution_lag_days": 1.0,
            "stale_signal_execution_count": 0,
        },
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
    }
    _write_json(
        paths["owner_review_decision_2397"],
        {
            **_safe_doc(plan.m2397.READY_STATUS),
            "owner_decision": plan.OWNER_DECISION_FROM_2397,
            "best_recombination_candidate": best,
            "best_recombination_decision_from_2396": (
                plan.EXPECTED_DECISION_FROM_2396
            ),
            "observation_preview_candidates_count": 0,
            "owner_review_required_retained": True,
            "gate_evidence_gap_summary_ready": True,
            "recommended_next_research_task": plan.m2397.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["gate_evidence_gap_summary_2397"],
        {
            **_safe_doc(plan.m2397.READY_STATUS),
            "gate_evidence_gap_summary_ready": True,
            "gate_evidence_gap_summary": {
                "record_ready": True,
                "best_recombination_candidate": best,
                "gate_evidence_gaps": {
                    "time_slice_evidence": {"status": "GAP_REMAINS"},
                    "regime_evidence": {"status": "GAP_REMAINS"},
                    "turnover_guardrail": {"status": "GAP_REMAINS"},
                    "valid_until_guardrail": {"status": "PASS"},
                    "cost_stress": {"status": "PASS"},
                },
            },
        },
    )
    _write_json(
        paths["observation_non_approval_record_2397"],
        {
            **_safe_doc(plan.m2397.READY_STATUS),
            "observation_non_approval_record": {
                "record_ready": True,
                "owner_decision": plan.OWNER_DECISION_FROM_2397,
                "research_only_observation_approved": False,
                "paper_shadow_enabled": False,
                "production_enabled": False,
                "broker_action_enabled": False,
            },
        },
    )
    _write_json(
        paths["next_route_2397"],
        {
            **_safe_doc(plan.m2397.READY_STATUS),
            "recommended_next_research_task": plan.m2397.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["recombination_retest_result_2396"],
        {
            **_safe_doc(plan.m2396.READY_STATUS),
            "best_recombination_candidate": best,
            "best_recombination_decision": plan.EXPECTED_DECISION_FROM_2396,
            "recommended_next_research_task": plan.m2396.NEXT_ROUTE,
            "recombination_retest_ready": True,
            "candidate_ranking_ready": True,
            "component_evidence_matrix_ready": True,
            "decision_update_ready": True,
            "data_quality_gate_executed": True,
            "data_quality_status": "PASS",
        },
    )
    _write_json(
        paths["recombination_candidate_ranking_2396"],
        {
            **_safe_doc(plan.m2396.READY_STATUS),
            "best_recombination_candidate": best,
            "best_recombination_decision": plan.EXPECTED_DECISION_FROM_2396,
            "recombination_candidate_ranking": [ranking_row],
        },
    )
    _write_json(
        paths["component_evidence_matrix_2396"],
        {
            **_safe_doc(plan.m2396.READY_STATUS),
            "component_evidence_matrix": [evidence_row],
        },
    )
    _write_json(
        paths["decision_update_2396"],
        {
            **_safe_doc(plan.m2396.READY_STATUS),
            "decision_update": {
                "best_recombination_candidate": best,
                "best_recombination_decision": plan.EXPECTED_DECISION_FROM_2396,
                "research_only_observation_preview_exists": False,
                "observation_preview_candidates": [],
                "research_only_observation_approved": False,
                "recommended_next_research_task": plan.m2396.NEXT_ROUTE,
            },
        },
    )
    _write_json(
        paths["recombination_candidate_plan_2395"],
        {
            **_safe_doc(plan.m2395.READY_STATUS),
            "recommended_next_research_task": plan.m2395.NEXT_ROUTE,
            "recombination_candidate_plan_ready": True,
            "candidate_definitions_ready": True,
            "planned_recombination_candidates": [best],
        },
    )
    _write_json(
        paths["candidate_definitions_2395"],
        {
            **_safe_doc(plan.m2395.READY_STATUS),
            "recombination_candidate_definitions": [
                {
                    "candidate_id": best,
                    "owner_review_required": True,
                    "components": [
                        "growth_tilt_engine",
                        "lower_turnover_guardrail",
                        "guarded_turnover_transfer",
                        "valid_until_window",
                    ],
                }
            ],
        },
    )
    _write_json(
        paths["ablation_retest_result_2393"],
        {
            **_safe_doc(plan.m2393.READY_STATUS),
            "recommended_next_research_task": plan.m2393.NEXT_ROUTE,
            "best_reusable_component": plan.m2395.RETURN_ENGINE_COMPONENT,
            "research_only_observation_approved": False,
            "data_quality_gate_executed": True,
        },
    )
    _write_json(
        paths["component_attribution_matrix_2393"],
        {
            **_safe_doc(plan.m2393.READY_STATUS),
            "component_attribution_matrix": [
                {
                    "component_name": plan.m2395.RETURN_ENGINE_COMPONENT,
                    "recommended_component_decision": (
                        plan.m2393.COMPONENT_DECISION_REUSABLE
                    ),
                },
                {
                    "component_name": plan.m2395.LOWER_TURNOVER_GUARDRAIL,
                    "recommended_component_decision": (
                        plan.m2393.COMPONENT_DECISION_GUARDRAIL
                    ),
                },
                {
                    "component_name": plan.m2395.GUARDED_TURNOVER_TRANSFER,
                    "recommended_component_decision": (
                        plan.m2393.COMPONENT_DECISION_OWNER_REVIEW
                    ),
                },
            ],
        },
    )
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_owner_review_decision_2397_path": paths[
            "owner_review_decision_2397"
        ],
        "source_gate_evidence_gap_summary_2397_path": paths[
            "gate_evidence_gap_summary_2397"
        ],
        "source_observation_non_approval_record_2397_path": paths[
            "observation_non_approval_record_2397"
        ],
        "source_next_route_2397_path": paths["next_route_2397"],
        "source_recombination_retest_result_2396_path": paths[
            "recombination_retest_result_2396"
        ],
        "source_recombination_candidate_ranking_2396_path": paths[
            "recombination_candidate_ranking_2396"
        ],
        "source_component_evidence_matrix_2396_path": paths[
            "component_evidence_matrix_2396"
        ],
        "source_decision_update_2396_path": paths["decision_update_2396"],
        "source_recombination_candidate_plan_2395_path": paths[
            "recombination_candidate_plan_2395"
        ],
        "source_candidate_definitions_2395_path": paths["candidate_definitions_2395"],
        "source_ablation_retest_result_2393_path": paths[
            "ablation_retest_result_2393"
        ],
        "source_component_attribution_matrix_2393_path": paths[
            "component_attribution_matrix_2393"
        ],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-owner-review-decision-2397",
        str(paths["owner_review_decision_2397"]),
        "--source-gate-evidence-gap-summary-2397",
        str(paths["gate_evidence_gap_summary_2397"]),
        "--source-observation-non-approval-record-2397",
        str(paths["observation_non_approval_record_2397"]),
        "--source-next-route-2397",
        str(paths["next_route_2397"]),
        "--source-recombination-retest-result-2396",
        str(paths["recombination_retest_result_2396"]),
        "--source-recombination-candidate-ranking-2396",
        str(paths["recombination_candidate_ranking_2396"]),
        "--source-component-evidence-matrix-2396",
        str(paths["component_evidence_matrix_2396"]),
        "--source-decision-update-2396",
        str(paths["decision_update_2396"]),
        "--source-recombination-candidate-plan-2395",
        str(paths["recombination_candidate_plan_2395"]),
        "--source-candidate-definitions-2395",
        str(paths["candidate_definitions_2395"]),
        "--source-ablation-retest-result-2393",
        str(paths["ablation_retest_result_2393"]),
        "--source-component-attribution-matrix-2393",
        str(paths["component_attribution_matrix_2393"]),
    ]


def _safe_doc(status: str) -> dict[str, object]:
    return {
        "status": status,
        "candidate_auto_accept_approved": False,
        "research_only_observation_approved": False,
        "observation_approved": False,
        "current_best_candidate_observation_approved": False,
        "paper_shadow_enabled": False,
        "paper_shadow_approved": False,
        "paper_shadow_allowed": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "event_append_approved": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_binding_approved": False,
        "outcome_store_mutated": False,
        "production_enabled": False,
        "production_approved": False,
        "production_allowed": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "daily_report_generated": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
