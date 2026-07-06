from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_component_ablation_owner_review_decision as review
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
)


def test_dynamic_strategy_component_ablation_owner_review_decision_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_review"
    docs_root = tmp_path / "docs" / "research"

    payload = review.run_dynamic_strategy_component_ablation_owner_review_decision(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 7),
    )

    assert payload["status"] == review.READY_STATUS
    assert payload["source_tasks"] == list(review.SOURCE_TASKS)
    assert payload["source_validation_errors"] == []
    assert payload["source_ready_for_owner_review_decision"] is True
    assert payload["data_quality_gate_executed"] is False
    assert payload["fresh_market_data_read"] is False
    assert payload["backtest_run"] is False
    assert payload["new_signal_generated"] is False
    assert payload["owner_review_decision_recorded"] is True
    assert payload["owner_decision"] == review.OWNER_DECISION
    assert payload["best_reusable_component"] == review.BEST_REUSABLE_COMPONENT
    assert payload["growth_tilt_engine_adopted_as_return_engine"] is True
    assert payload["growth_tilt_engine_decision"] == review.COMPONENT_DECISION_REUSABLE
    assert payload["lower_turnover_guardrail_decision"] == (
        review.COMPONENT_DECISION_GUARDRAIL
    )
    assert payload["lower_turnover_guardrail_adopted_as_guardrail_only"] is True
    assert payload["guarded_turnover_transfer_decision"] == (
        review.COMPONENT_DECISION_OWNER_REVIEW
    )
    assert payload["guarded_turnover_transfer_requires_further_review"] is True
    assert payload["recombination_plan_approved"] is True
    assert payload["recommended_next_research_task"] == review.NEXT_ROUTE
    assert payload["component_recombination_decision"]["record_ready"] is True
    assert payload["component_recombination_decision"][
        "recombination_plan_approved"
    ] is True
    assert payload["recombination_principles"]["return_engine"]["primary"] == (
        review.BEST_REUSABLE_COMPONENT
    )

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
        "component_recombination_decision_json",
        "recombination_principles_json",
        "next_route_json",
        "markdown_path",
        "component_recombination_decision_markdown",
        "recombination_principles_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_component_ablation_owner_review_decision_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-component-ablation-owner-review-decision",
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
    assert review.READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "owner_review_decision.json").exists()
    assert (output_root / "component_recombination_decision.json").exists()
    assert (output_root / "recombination_principles.json").exists()
    assert (output_root / "next_route.json").exists()


def test_dynamic_strategy_component_ablation_owner_review_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_component_ablation_owner_review_decision"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-component-ablation-owner-review-decision"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("owner_review_decision.json" in item for item in entry["artifact_globs"])
    assert any(
        "component_recombination_decision.json" in item
        for item in entry["artifact_globs"]
    )

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_component_ablation_owner_review_decision" in catalog
    assert "dynamic-strategy-component-ablation-owner-review-decision" in system_flow
    assert (
        "TRADING-2394_DYNAMIC_STRATEGY_COMPONENT_ABLATION_OWNER_REVIEW"
        in task_register
    )


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
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
        "source_decision_update_2393_path": paths["decision_update_2393"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "owner_review_decision_2391": "--source-owner-review-decision-2391",
        "component_attribution_plan_2392": "--source-component-attribution-plan-2392",
        "ablation_retest_result_2393": "--source-ablation-retest-result-2393",
        "component_attribution_matrix_2393": (
            "--source-component-attribution-matrix-2393"
        ),
        "reusable_component_decision_2393": (
            "--source-reusable-component-decision-2393"
        ),
        "decision_update_2393": "--source-decision-update-2393",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "source"
    root.mkdir(parents=True, exist_ok=True)
    component_decisions = {
        "combined_turnover_budgeting_and_valid_until": "CONTINUE_COMPONENT_RESEARCH",
        review.BEST_REUSABLE_COMPONENT: review.COMPONENT_DECISION_REUSABLE,
        review.GUARDED_TURNOVER_TRANSFER: review.COMPONENT_DECISION_OWNER_REVIEW,
        review.LOWER_TURNOVER_GUARDRAIL: review.COMPONENT_DECISION_GUARDRAIL,
        "turnover_budgeting": "CONTINUE_COMPONENT_RESEARCH",
        "valid_until_strictness": "CONTINUE_COMPONENT_RESEARCH",
    }
    reusable_decision = {
        "schema_version": "dynamic_strategy_reusable_component_decision.v1",
        "reusable_component_decision_ready": True,
        "best_reusable_component": review.BEST_REUSABLE_COMPONENT,
        "best_reusable_component_decision": review.COMPONENT_DECISION_REUSABLE,
        "component_decisions": component_decisions,
        "recombination_candidate_direction": (
            "RECOMBINE_GROWTH_TILT_WITH_TURNOVER_AND_VALID_UNTIL_GUARDRAILS"
        ),
    }
    payloads = {
        "owner_review_decision_2391": _source(
            review.SOURCE_2391_READY_STATUS,
            owner_decision=review.SOURCE_2391_OWNER_DECISION,
            component_attribution_continue_recommended=True,
            research_only_observation_approved=False,
            recommended_next_research_task=review.SOURCE_2391_EXPECTED_ROUTE,
        ),
        "component_attribution_plan_2392": _source(
            review.SOURCE_2392_READY_STATUS,
            component_value_candidates=list(review.COMPONENT_VALUE_CANDIDATES),
            components_to_attribute=list(review.COMPONENTS_TO_ATTRIBUTE),
            targeted_ablation_retest_plan_ready=True,
            recommended_next_research_task=review.SOURCE_2392_EXPECTED_ROUTE,
        ),
        "ablation_retest_result_2393": _source(
            review.SOURCE_2393_READY_STATUS,
            ablation_retest_ready=True,
            component_attribution_matrix_ready=True,
            reusable_component_decision_ready=True,
            data_quality_gate_executed=True,
            data_quality_status="PASS_WITH_WARNINGS",
            best_reusable_component=review.BEST_REUSABLE_COMPONENT,
            component_decisions=component_decisions,
            reusable_component_decision=reusable_decision,
            recommended_next_research_task=review.SOURCE_2393_EXPECTED_ROUTE,
        ),
        "component_attribution_matrix_2393": _source(
            review.SOURCE_2393_READY_STATUS,
            component_attribution_matrix=[
                {
                    "component_name": component_name,
                    "recommended_component_decision": decision,
                }
                for component_name, decision in component_decisions.items()
            ],
        ),
        "reusable_component_decision_2393": _source(
            review.SOURCE_2393_READY_STATUS,
            reusable_component_decision=reusable_decision,
        ),
        "decision_update_2393": _source(
            review.SOURCE_2393_READY_STATUS,
            decision_update={
                "decision_update_ready": True,
                "recommended_next_research_task": review.SOURCE_2393_EXPECTED_ROUTE,
            },
            recommended_next_research_task=review.SOURCE_2393_EXPECTED_ROUTE,
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
