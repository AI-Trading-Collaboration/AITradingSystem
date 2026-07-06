from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_recombination_candidate_owner_review_decision as decision
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "candidate_auto_accept_approved",
    "research_only_observation_approved",
    "paper_shadow_enabled",
    "paper_shadow_approved",
    "paper_trade_created",
    "shadow_position_created",
    "scheduler_enabled",
    "event_append_enabled",
    "event_append_approved",
    "outcome_binding_enabled",
    "outcome_binding_approved",
    "production_enabled",
    "broker_action_enabled",
    "daily_report_generated",
)


def test_dynamic_strategy_recombination_candidate_owner_review_decision_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "decision"
    docs_root = tmp_path / "docs" / "research"

    payload = decision.run_dynamic_strategy_recombination_candidate_owner_review_decision(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 7),
    )

    assert payload["status"] == decision.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["source_ready_for_owner_review_decision"] is True
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == decision.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["backtest_run"] is False
    assert payload["new_signal_generated"] is False
    assert payload["scoring_run"] is False
    assert payload["owner_review_decision_recorded"] is True
    assert payload["owner_decision"] == decision.OWNER_DECISION
    assert (
        payload["best_recombination_candidate"]
        == decision.BEST_RECOMBINATION_CANDIDATE
    )
    assert (
        payload["best_recombination_decision_from_2396"]
        == decision.EXPECTED_DECISION_FROM_2396
    )
    assert payload["owner_review_required_retained"] is True
    assert payload["observation_preview_candidates_count"] == 0
    assert payload["research_only_observation_approved"] is False
    assert payload["gate_evidence_gap_summary_ready"] is True
    assert payload["recommended_next_research_task"] == decision.NEXT_ROUTE

    gap_summary = payload["gate_evidence_gap_summary"]
    gaps = gap_summary["gate_evidence_gaps"]
    assert gap_summary["record_ready"] is True
    assert gaps["time_slice_evidence"]["status"] == "GAP_REMAINS"
    assert gaps["regime_evidence"]["status"] == "GAP_REMAINS"
    assert gaps["turnover_guardrail"]["status"] == "GAP_REMAINS"
    assert gaps["valid_until_guardrail"]["status"] == "PASS"
    assert gaps["cost_stress"]["status"] == "PASS"
    assert payload["observation_non_approval_record"]["record_ready"] is True
    assert payload["observation_non_approval_reason"] == list(
        decision.OBSERVATION_NON_APPROVAL_REASONS
    )

    for field in SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    for key in (
        "json_path",
        "observation_non_approval_record_json",
        "gate_evidence_gap_summary_json",
        "next_route_json",
        "markdown_path",
        "observation_non_approval_markdown",
        "gate_evidence_gap_summary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_recombination_candidate_owner_review_decision_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "decision_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-recombination-candidate-owner-review-decision",
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
    assert decision.READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "owner_review_decision.json").exists()
    assert (output_root / "observation_non_approval_record.json").exists()
    assert (output_root / "gate_evidence_gap_summary.json").exists()
    assert (output_root / "next_route.json").exists()


def test_dynamic_strategy_recombination_candidate_owner_review_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_recombination_candidate_owner_review_decision"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-recombination-candidate-owner-review-decision"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("owner_review_decision.json" in item for item in entry["artifact_globs"])
    assert any(
        "gate_evidence_gap_summary.json" in item
        for item in entry["artifact_globs"]
    )

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_recombination_candidate_owner_review_decision" in catalog
    assert (
        "dynamic-strategy-recombination-candidate-owner-review-decision"
        in system_flow
    )
    assert (
        "TRADING-2397_DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_OWNER_REVIEW_AND_"
        "OBSERVATION_DECISION"
    ) in task_register


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "recombination_retest_result_2396": root / "recombination_retest_result.json",
        "recombination_candidate_ranking_2396": root / "recombination_candidate_ranking.json",
        "component_evidence_matrix_2396": root / "component_evidence_matrix.json",
        "decision_update_2396": root / "decision_update.json",
        "recombination_candidate_plan_2395": root / "recombination_candidate_plan.json",
        "candidate_definitions_2395": root / "recombination_candidate_definitions.json",
        "owner_review_decision_2394": root / "owner_review_decision_2394.json",
        "component_recombination_decision_2394": (
            root / "component_recombination_decision.json"
        ),
        "ablation_retest_result_2393": root / "ablation_retest_result.json",
        "component_attribution_matrix_2393": root / "component_attribution_matrix.json",
    }
    best = decision.BEST_RECOMBINATION_CANDIDATE
    ranking_row = {
        "rank": 1,
        "candidate_id": best,
        "decision": decision.EXPECTED_DECISION_FROM_2396,
        "time_slice_pass_rate": 0.0,
        "regime_expectation_score": 0.32,
        "return_retention_vs_raw_growth_tilt": 0.97,
        "turnover_reduction_vs_raw_growth_tilt": -0.01,
        "max_drawdown": -0.16,
        "cost_stress_survival": "harsh",
        "stale_signal_execution_count": 0,
        "no_stale_signal_carry_forward": True,
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
        ],
        "recombination_quality": {
            "candidate_decision": decision.EXPECTED_DECISION_FROM_2396,
            "owner_review_required": True,
            "time_slice_pass_rate": 0.0,
            "regime_expectation_score": 0.32,
        },
        "valid_until_metrics": {
            "no_stale_signal_carry_forward": True,
            "stale_signal_execution_count": 0,
        },
        "guardrail_metrics": {
            "turnover_reduction_vs_raw_growth_tilt": -0.01,
            "harsh_cost_passed": True,
        },
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
    }
    _write_json(
        paths["recombination_retest_result_2396"],
        {
            **_safe_doc(decision.m2396.READY_STATUS),
            "best_recombination_candidate": best,
            "best_recombination_decision": decision.EXPECTED_DECISION_FROM_2396,
            "recommended_next_research_task": decision.m2396.NEXT_ROUTE,
            "recombination_retest_ready": True,
            "candidate_ranking_ready": True,
            "component_evidence_matrix_ready": True,
            "decision_update_ready": True,
        },
    )
    _write_json(
        paths["recombination_candidate_ranking_2396"],
        {
            **_safe_doc(decision.m2396.READY_STATUS),
            "best_recombination_candidate": best,
            "best_recombination_decision": decision.EXPECTED_DECISION_FROM_2396,
            "recombination_candidate_ranking": [ranking_row],
        },
    )
    _write_json(
        paths["component_evidence_matrix_2396"],
        {
            **_safe_doc(decision.m2396.READY_STATUS),
            "component_evidence_matrix": [evidence_row],
        },
    )
    _write_json(
        paths["decision_update_2396"],
        {
            **_safe_doc(decision.m2396.READY_STATUS),
            "decision_update": {
                "best_recombination_candidate": best,
                "best_recombination_decision": decision.EXPECTED_DECISION_FROM_2396,
                "research_only_observation_preview_exists": False,
                "observation_preview_candidates": [],
                "research_only_observation_approved": False,
                "recommended_next_research_task": decision.m2396.NEXT_ROUTE,
            },
        },
    )
    _write_json(
        paths["recombination_candidate_plan_2395"],
        {
            **_safe_doc(decision.m2395.READY_STATUS),
            "recommended_next_research_task": decision.m2395.NEXT_ROUTE,
            "recombination_candidate_plan_ready": True,
            "candidate_definitions_ready": True,
            "planned_recombination_candidates": [best],
            "return_engine_component": decision.m2395.RETURN_ENGINE_COMPONENT,
            "owner_review_components": [decision.m2395.GUARDED_TURNOVER_TRANSFER],
            "guardrail_components": [
                decision.m2395.LOWER_TURNOVER_GUARDRAIL,
                decision.m2395.VALID_UNTIL_WINDOW,
                decision.m2395.NO_STALE_SIGNAL_CARRY_FORWARD,
            ],
        },
    )
    _write_json(
        paths["candidate_definitions_2395"],
        {
            **_safe_doc(decision.m2395.READY_STATUS),
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
        paths["owner_review_decision_2394"],
        {
            **_safe_doc(decision.m2394.READY_STATUS),
            "owner_decision": decision.m2394.OWNER_DECISION,
            "recombination_plan_approved": True,
            "best_reusable_component": decision.m2394.BEST_REUSABLE_COMPONENT,
            "recommended_next_research_task": decision.m2394.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["component_recombination_decision_2394"],
        {
            **_safe_doc(decision.m2394.READY_STATUS),
            "component_recombination_decision": {
                "record_ready": True,
                "owner_decision": decision.m2394.OWNER_DECISION,
                "recombination_plan_approved": True,
            },
        },
    )
    _write_json(
        paths["ablation_retest_result_2393"],
        {
            **_safe_doc(decision.m2393.READY_STATUS),
            "recommended_next_research_task": decision.m2393.NEXT_ROUTE,
            "best_reusable_component": decision.m2394.BEST_REUSABLE_COMPONENT,
            "data_quality_gate_executed": True,
        },
    )
    _write_json(
        paths["component_attribution_matrix_2393"],
        {
            **_safe_doc(decision.m2393.READY_STATUS),
            "component_attribution_matrix": [
                {
                    "component_name": decision.m2395.RETURN_ENGINE_COMPONENT,
                    "recommended_component_decision": (
                        decision.m2393.COMPONENT_DECISION_REUSABLE
                    ),
                },
                {
                    "component_name": decision.m2395.LOWER_TURNOVER_GUARDRAIL,
                    "recommended_component_decision": (
                        decision.m2393.COMPONENT_DECISION_GUARDRAIL
                    ),
                },
                {
                    "component_name": decision.m2395.GUARDED_TURNOVER_TRANSFER,
                    "recommended_component_decision": (
                        decision.m2393.COMPONENT_DECISION_OWNER_REVIEW
                    ),
                },
            ],
        },
    )
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
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
        "source_owner_review_decision_2394_path": paths["owner_review_decision_2394"],
        "source_component_recombination_decision_2394_path": paths[
            "component_recombination_decision_2394"
        ],
        "source_ablation_retest_result_2393_path": paths[
            "ablation_retest_result_2393"
        ],
        "source_component_attribution_matrix_2393_path": paths[
            "component_attribution_matrix_2393"
        ],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
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
        "--source-owner-review-decision-2394",
        str(paths["owner_review_decision_2394"]),
        "--source-component-recombination-decision-2394",
        str(paths["component_recombination_decision_2394"]),
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
        "paper_shadow_enabled": False,
        "paper_shadow_approved": False,
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
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
