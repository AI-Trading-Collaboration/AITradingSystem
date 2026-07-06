from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_component_recombination_candidate_plan as plan
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
    "outcome_store_mutated",
    "production_enabled",
    "broker_action_enabled",
    "daily_report_generated",
    "new_signal_generated",
    "scoring_run",
)


def test_dynamic_strategy_component_recombination_candidate_plan_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "recombine"
    docs_root = tmp_path / "docs" / "research"

    payload = plan.run_dynamic_strategy_component_recombination_candidate_plan(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 7),
    )

    assert payload["status"] == plan.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["source_ready_for_recombination_candidate_plan"] is True
    assert payload["data_quality_gate_executed"] is False
    assert payload["fresh_market_data_read"] is False
    assert payload["backtest_run"] is False
    assert payload["new_signal_generated"] is False
    assert payload["owner_decision_from_2394"] == plan.m2394.OWNER_DECISION
    assert payload["recombination_candidate_plan_ready"] is True
    assert payload["recombination_candidate_definitions_ready"] is True
    assert payload["retest_plan_2396_ready"] is True
    assert payload["acceptance_criteria_ready"] is True
    assert payload["return_engine_component"] == plan.RETURN_ENGINE_COMPONENT
    assert plan.LOWER_TURNOVER_GUARDRAIL in payload["guardrail_components"]
    assert plan.VALID_UNTIL_WINDOW in payload["guardrail_components"]
    assert plan.NO_STALE_SIGNAL_CARRY_FORWARD in payload["guardrail_components"]
    assert payload["owner_review_components"] == [plan.GUARDED_TURNOVER_TRANSFER]
    assert payload["planned_recombination_candidates"] == list(
        plan.PLANNED_RECOMBINATION_CANDIDATES
    )
    assert payload["recommended_next_research_task"] == plan.NEXT_ROUTE

    assert payload["candidate_auto_accept_approved"] is False
    assert payload["research_only_observation_approved"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["event_append_enabled"] is False
    assert payload["outcome_binding_enabled"] is False
    assert payload["scheduler_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_action_enabled"] is False
    assert payload["daily_report_generated"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    for key in (
        "json_path",
        "candidate_definitions_json",
        "retest_plan_2396_json",
        "acceptance_criteria_json",
        "markdown_path",
        "candidate_definitions_markdown",
        "retest_plan_2396_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_component_recombination_candidate_plan_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "recombine_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-component-recombination-candidate-plan",
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
    assert (output_root / "recombination_candidate_plan.json").exists()
    assert (output_root / "recombination_candidate_definitions.json").exists()
    assert (output_root / "retest_plan_2396.json").exists()
    assert (output_root / "recombination_acceptance_criteria.json").exists()


def test_dynamic_strategy_component_recombination_candidate_plan_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_component_recombination_candidate_plan"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-component-recombination-candidate-plan"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "recombination_candidate_plan.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("retest_plan_2396.json" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_component_recombination_candidate_plan" in catalog
    assert "dynamic-strategy-component-recombination-candidate-plan" in system_flow
    assert "TRADING-2395_DYNAMIC_STRATEGY_COMPONENT_RECOMBINATION" in task_register


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_reclassification_result_2390_path": paths[
            "reclassification_result_2390"
        ],
        "source_owner_review_decision_2391_path": paths[
            "owner_review_decision_2391"
        ],
        "source_component_attribution_plan_2392_path": paths[
            "component_attribution_plan_2392"
        ],
        "source_ablation_retest_result_2393_path": paths[
            "ablation_retest_result_2393"
        ],
        "source_component_attribution_matrix_2393_path": paths[
            "component_attribution_matrix_2393"
        ],
        "source_reusable_component_decision_2393_path": paths[
            "reusable_component_decision_2393"
        ],
        "source_owner_review_decision_2394_path": paths[
            "owner_review_decision_2394"
        ],
        "source_component_recombination_decision_2394_path": paths[
            "component_recombination_decision_2394"
        ],
        "source_recombination_principles_2394_path": paths[
            "recombination_principles_2394"
        ],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "reclassification_result_2390": "--source-reclassification-result-2390",
        "owner_review_decision_2391": "--source-owner-review-decision-2391",
        "component_attribution_plan_2392": "--source-component-attribution-plan-2392",
        "ablation_retest_result_2393": "--source-ablation-retest-result-2393",
        "component_attribution_matrix_2393": (
            "--source-component-attribution-matrix-2393"
        ),
        "reusable_component_decision_2393": (
            "--source-reusable-component-decision-2393"
        ),
        "owner_review_decision_2394": "--source-owner-review-decision-2394",
        "component_recombination_decision_2394": (
            "--source-component-recombination-decision-2394"
        ),
        "recombination_principles_2394": "--source-recombination-principles-2394",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "source"
    root.mkdir(parents=True, exist_ok=True)
    component_decisions = {
        plan.RETURN_ENGINE_COMPONENT: plan.m2393.COMPONENT_DECISION_REUSABLE,
        plan.GUARDED_TURNOVER_TRANSFER: plan.m2393.COMPONENT_DECISION_OWNER_REVIEW,
        plan.LOWER_TURNOVER_GUARDRAIL: plan.m2393.COMPONENT_DECISION_GUARDRAIL,
        "turnover_budgeting": "CONTINUE_COMPONENT_RESEARCH",
        "valid_until_strictness": "CONTINUE_COMPONENT_RESEARCH",
    }
    reusable_decision = {
        "schema_version": "dynamic_strategy_reusable_component_decision.v1",
        "best_reusable_component": plan.RETURN_ENGINE_COMPONENT,
        "component_decisions": component_decisions,
    }
    recombination_principles = {
        "return_engine": {
            "primary": plan.RETURN_ENGINE_COMPONENT,
            "source": plan.RANKING_TOP_CANDIDATE,
        }
    }
    payloads = {
        "reclassification_result_2390": _source(
            plan.m2390.READY_STATUS,
            owner_review_recommendation_ready=True,
            recommended_next_research_task=plan.m2390.NEXT_ROUTE,
        ),
        "owner_review_decision_2391": _source(
            plan.m2391.READY_STATUS,
            owner_decision=plan.m2391.OWNER_DECISION,
            component_attribution_continue_recommended=True,
            recommended_next_research_task=plan.m2391.NEXT_ROUTE,
        ),
        "component_attribution_plan_2392": _source(
            plan.m2392.READY_STATUS,
            component_value_candidates=list(plan.m2392.COMPONENT_VALUE_CANDIDATES),
            components_to_attribute=list(plan.m2392.COMPONENTS_TO_ATTRIBUTE),
            targeted_ablation_retest_plan_ready=True,
            recommended_next_research_task=plan.m2392.NEXT_ROUTE,
        ),
        "ablation_retest_result_2393": _source(
            plan.m2393.READY_STATUS,
            ablation_retest_ready=True,
            data_quality_gate_executed=True,
            data_quality_status="PASS_WITH_WARNINGS",
            best_reusable_component=plan.RETURN_ENGINE_COMPONENT,
            component_decisions=component_decisions,
            recommended_next_research_task=plan.m2393.NEXT_ROUTE,
        ),
        "component_attribution_matrix_2393": _source(
            plan.m2393.READY_STATUS,
            component_attribution_matrix=[
                {
                    "component_name": component_name,
                    "recommended_component_decision": decision,
                }
                for component_name, decision in component_decisions.items()
            ],
        ),
        "reusable_component_decision_2393": _source(
            plan.m2393.READY_STATUS,
            reusable_component_decision=reusable_decision,
        ),
        "owner_review_decision_2394": _source(
            plan.m2394.READY_STATUS,
            owner_decision=plan.m2394.OWNER_DECISION,
            recombination_plan_approved=True,
            growth_tilt_engine_adopted_as_return_engine=True,
            lower_turnover_guardrail_adopted_as_guardrail_only=True,
            guarded_turnover_transfer_requires_further_review=True,
            recommended_next_research_task=plan.m2394.NEXT_ROUTE,
        ),
        "component_recombination_decision_2394": _source(
            plan.m2394.READY_STATUS,
            component_recombination_decision={
                "record_ready": True,
                "owner_decision": plan.m2394.OWNER_DECISION,
                "research_only_observation_approved": False,
            },
        ),
        "recombination_principles_2394": _source(
            plan.m2394.READY_STATUS,
            recombination_principles=recombination_principles,
        ),
    }
    paths: dict[str, Path] = {}
    for name, payload in payloads.items():
        path = root / f"{name}.json"
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        paths[name] = path
    return paths


def _source(status: str, **updates: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "status": status,
        "production_effect": "none",
        "broker_action": "none",
        **{field: False for field in SAFETY_FALSE_FIELDS},
    }
    payload.update(updates)
    return payload
