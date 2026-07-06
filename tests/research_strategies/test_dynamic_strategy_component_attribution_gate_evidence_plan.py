from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_component_attribution_gate_evidence_plan as plan
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_component_attribution_gate_evidence_plan_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "component_plan"
    docs_root = tmp_path / "docs" / "research"

    payload = plan.run_dynamic_strategy_component_attribution_gate_evidence_plan(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 7),
    )

    assert payload["status"] == plan.READY_STATUS
    assert payload["source_tasks"] == list(plan.SOURCE_TASKS)
    assert payload["source_validation_errors"] == []
    assert payload["owner_decision_from_2391"] == plan.SOURCE_2391_OWNER_DECISION
    assert payload["component_attribution_plan_ready"] is True
    assert payload["component_value_matrix_ready"] is True
    assert payload["gate_evidence_plan_ready"] is True
    assert payload["targeted_ablation_retest_plan_ready"] is True
    assert set(payload["component_value_candidates"]) == set(
        plan.COMPONENT_VALUE_CANDIDATES
    )
    assert "dynamic_turnover_budgeted_growth_tilt_v1" in payload[
        "component_value_candidates"
    ]
    assert "dynamic_valid_until_expiry_strict_v1" in payload[
        "component_value_candidates"
    ]
    assert set(plan.COMPONENTS_TO_ATTRIBUTE).issubset(
        set(payload["components_to_attribute"])
    )
    assert "turnover_budgeting" in payload["components_to_attribute"]
    assert "valid_until_strictness" in payload["components_to_attribute"]
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
    assert payload["recommended_next_research_task"] == plan.NEXT_ROUTE
    assert payload["component_attribution_plan"]["plan_ready"] is True
    assert payload["targeted_ablation_retest_plan"]["plan_ready"] is True
    assert len(payload["targeted_ablation_retest_plan"]["ablation_test_candidates"]) == 6

    matrix = {row["component_name"]: row for row in payload["component_value_matrix"]}
    assert matrix["turnover_budgeting"]["component_class"] == "EXECUTION_GUARDRAIL"
    assert matrix["growth_tilt_engine"]["component_class"] == "RETURN_ENGINE"
    assert matrix["lower_turnover_guardrail"]["reuse_mode"] == "GUARDRAIL_ONLY"

    for key in (
        "json_path",
        "component_value_matrix_json",
        "gate_evidence_plan_json",
        "targeted_ablation_retest_plan_json",
        "markdown_path",
        "component_value_matrix_markdown",
        "gate_evidence_plan_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_component_attribution_gate_evidence_plan_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "component_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-component-attribution-gate-evidence-plan",
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
    assert (output_root / "component_attribution_plan.json").exists()
    assert (output_root / "component_value_matrix.json").exists()
    assert (output_root / "gate_evidence_plan.json").exists()
    assert (output_root / "targeted_ablation_retest_plan.json").exists()


def test_dynamic_strategy_component_attribution_gate_evidence_plan_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_component_attribution_gate_evidence_plan"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-component-attribution-gate-evidence-plan"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "component_attribution_plan.json" in item for item in entry["artifact_globs"]
    )
    assert any("gate_evidence_plan.json" in item for item in entry["artifact_globs"])

    assert "dynamic_strategy_component_attribution_gate_evidence_plan" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-component-attribution-gate-evidence-plan" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    assert "TRADING-2392_DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION" in Path(
        "docs/task_register.md"
    ).read_text(encoding="utf-8")


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_candidate_ranking_2365_path": paths["candidate_ranking_2365"],
        "source_sensitivity_result_2366_path": paths["sensitivity_result_2366"],
        "source_expanded_candidate_retest_2386_path": paths[
            "expanded_candidate_retest_2386"
        ],
        "source_expanded_candidate_ranking_2386_path": paths[
            "expanded_candidate_ranking_2386"
        ],
        "source_reclassification_result_2390_path": paths[
            "reclassification_result_2390"
        ],
        "source_component_attribution_review_2390_path": paths[
            "component_attribution_review_2390"
        ],
        "source_candidate_reclassification_preview_2390_path": paths[
            "candidate_reclassification_preview_2390"
        ],
        "source_owner_review_decision_2391_path": paths[
            "owner_review_decision_2391"
        ],
        "source_candidate_owner_review_record_2391_path": paths[
            "candidate_owner_review_record_2391"
        ],
        "source_observation_non_approval_record_2391_path": paths[
            "observation_non_approval_record_2391"
        ],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "candidate_ranking_2365": "--source-candidate-ranking-2365",
        "sensitivity_result_2366": "--source-sensitivity-result-2366",
        "expanded_candidate_retest_2386": "--source-expanded-candidate-retest-2386",
        "expanded_candidate_ranking_2386": "--source-expanded-candidate-ranking-2386",
        "reclassification_result_2390": "--source-reclassification-result-2390",
        "component_attribution_review_2390": (
            "--source-component-attribution-review-2390"
        ),
        "candidate_reclassification_preview_2390": (
            "--source-candidate-reclassification-preview-2390"
        ),
        "owner_review_decision_2391": "--source-owner-review-decision-2391",
        "candidate_owner_review_record_2391": (
            "--source-candidate-owner-review-record-2391"
        ),
        "observation_non_approval_record_2391": (
            "--source-observation-non-approval-record-2391"
        ),
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "source"
    root.mkdir(parents=True, exist_ok=True)
    ranking_rows = _ranking_rows()
    component_review = _component_review()
    payloads = {
        "candidate_ranking_2365": _source(
            plan.SOURCE_2365_READY_STATUS,
            candidate_ranking=[
                {
                    "candidate_id": plan.RANKING_TOP_CANDIDATE,
                    "decision": "OWNER_REVIEW_REQUIRED",
                }
            ],
        ),
        "sensitivity_result_2366": _source(
            plan.SOURCE_2366_READY_STATUS,
            top_candidate_from_2365=plan.RANKING_TOP_CANDIDATE,
        ),
        "expanded_candidate_retest_2386": _source(
            plan.SOURCE_2386_READY_STATUS,
            best_candidate_after_expanded_screening=plan.RANKING_TOP_CANDIDATE,
            best_candidate_decision=plan.CURRENT_BEST_PREVIOUS_DECISION,
            candidate_ready_for_research_only_observation=False,
            expanded_candidate_ranking=ranking_rows,
        ),
        "expanded_candidate_ranking_2386": _source(
            plan.SOURCE_2386_READY_STATUS,
            expanded_candidate_ranking=ranking_rows,
        ),
        "reclassification_result_2390": _source(
            plan.SOURCE_2390_READY_STATUS,
            current_best_candidate=plan.RANKING_TOP_CANDIDATE,
            current_best_candidate_previous_decision=(
                plan.CURRENT_BEST_PREVIOUS_DECISION
            ),
            current_best_candidate_preview_decision=(
                plan.CURRENT_BEST_PREVIEW_DECISION
            ),
            component_value_candidates=list(plan.COMPONENT_VALUE_CANDIDATES),
            candidate_auto_accept_approved=False,
            research_only_observation_approved=False,
            component_attribution_review=component_review,
        ),
        "component_attribution_review_2390": _source(
            plan.SOURCE_2390_READY_STATUS,
            component_value_candidates=list(plan.COMPONENT_VALUE_CANDIDATES),
            component_attribution_review=component_review,
        ),
        "candidate_reclassification_preview_2390": _source(
            plan.SOURCE_2390_READY_STATUS,
            candidate_reclassification_preview=[
                {
                    "candidate_id": plan.RANKING_TOP_CANDIDATE,
                    "previous_decision": plan.CURRENT_BEST_PREVIOUS_DECISION,
                    "preview_decision": plan.CURRENT_BEST_PREVIEW_DECISION,
                }
            ],
        ),
        "owner_review_decision_2391": _source(
            plan.SOURCE_2391_READY_STATUS,
            owner_decision=plan.SOURCE_2391_OWNER_DECISION,
            recommended_next_research_task=plan.SOURCE_2391_EXPECTED_ROUTE,
            component_attribution_continue_recommended=True,
            candidate_auto_accept_approved=False,
            research_only_observation_approved=False,
        ),
        "candidate_owner_review_record_2391": _source(
            plan.SOURCE_2391_READY_STATUS,
            candidate_owner_review_record={
                "owner_review_required_retained": True,
            },
        ),
        "observation_non_approval_record_2391": _source(
            plan.SOURCE_2391_READY_STATUS,
            observation_non_approval_record={
                "research_only_observation_approved": False,
            },
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
        **{field: False for field in plan.SAFETY_FALSE_FIELDS},
    }
    payload.update(updates)
    return payload


def _ranking_rows() -> list[dict[str, object]]:
    candidates = [
        plan.RANKING_TOP_CANDIDATE,
        "dynamic_turnover_budgeted_growth_tilt_v1",
        "dynamic_valid_until_expiry_strict_v1",
        "dynamic_regime_overlay_v0_4_lower_turnover",
        "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
        "equal_risk_growth_tilt_guarded_turnover_v1",
    ]
    return [
        {
            "candidate_id": candidate_id,
            "decision": plan.CURRENT_BEST_PREVIOUS_DECISION,
            "dynamic_vs_static_gap": 0.02 if index == 0 else 0.006,
            "cost_adjusted_dynamic_vs_static_gap": 0.02 if index == 0 else 0.006,
            "turnover": 1.0 + index,
            "turnover_budget_passed": True,
            "valid_until_window_preserved": True,
            "no_stale_signal_carry_forward": True,
            "time_slice_pass_rate": 0.0 if index == 0 else 0.42,
            "regime_slice_pass_rate": 0.0,
            "drawdown_gap_vs_static": 0.04 if index == 0 else -0.001,
            "candidate_vs_ranking_top_gap": 0.0 if index == 0 else -0.01,
            "return_advantage_retained": 1.0 if index == 0 else 0.3,
            "decision_reasons": ["fixture_reason"],
        }
        for index, candidate_id in enumerate(candidates)
    ]


def _component_review() -> list[dict[str, object]]:
    return [
        {
            "component_name": component_name,
            "source_candidates": source_candidates,
            "component_value_hypothesis": ["fixture_value"],
            "supporting_metrics": [],
            "failure_metrics": [],
            "reusable_in_future_candidate": True,
            "recommended_followup": "fixture_followup",
        }
        for component_name, source_candidates in {
            "turnover_budgeting": ["dynamic_turnover_budgeted_growth_tilt_v1"],
            "valid_until_strictness": ["dynamic_valid_until_expiry_strict_v1"],
            "growth_tilt_engine": [plan.RANKING_TOP_CANDIDATE],
            "lower_turnover_guardrail": [
                "dynamic_regime_overlay_v0_4_lower_turnover",
                "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
            ],
            "guarded_turnover_transfer": ["equal_risk_growth_tilt_guarded_turnover_v1"],
        }.items()
    ]
