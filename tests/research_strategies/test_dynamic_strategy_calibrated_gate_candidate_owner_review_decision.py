from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_calibrated_gate_candidate_owner_review_decision as review
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_calibrated_gate_candidate_owner_review_decision_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_decision"
    docs_root = tmp_path / "docs" / "research"

    payload = (
        review.run_dynamic_strategy_calibrated_gate_candidate_owner_review_decision(
            **_source_kwargs(source_paths),
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=date(2026, 7, 7),
        )
    )

    assert payload["status"] == review.READY_STATUS
    assert payload["source_tasks"] == list(review.SOURCE_TASKS)
    assert payload["source_validation_errors"] == []
    assert payload["current_best_candidate"] == review.RANKING_TOP_CANDIDATE
    assert payload["previous_decision"] == review.CURRENT_BEST_PREVIOUS_DECISION
    assert payload["calibrated_preview_decision"] == (
        review.CURRENT_BEST_PREVIEW_DECISION
    )
    assert payload["owner_review_decision_recorded"] is True
    assert payload["owner_decision"] == review.OWNER_DECISION
    assert payload["owner_decision_option"] == review.DEFAULT_OWNER_DECISION_OPTION
    assert payload["candidate_auto_accept_approved"] is False
    assert payload["research_only_observation_approved"] is False
    assert payload["owner_review_required_retained"] is True
    assert payload["component_attribution_continue_recommended"] is True
    assert set(payload["component_value_candidates"]) == set(
        review.COMPONENT_VALUE_CANDIDATES
    )
    assert "dynamic_turnover_budgeted_growth_tilt_v1" in payload[
        "component_value_candidates"
    ]
    assert "dynamic_valid_until_expiry_strict_v1" in payload[
        "component_value_candidates"
    ]
    assert payload["paper_shadow_enabled"] is False
    assert payload["paper_trade_created"] is False
    assert payload["shadow_position_created"] is False
    assert payload["event_append_enabled"] is False
    assert payload["outcome_binding_enabled"] is False
    assert payload["scheduler_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_action_enabled"] is False
    assert payload["daily_report_generated"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["recommended_next_research_task"] == review.NEXT_ROUTE
    assert payload["candidate_owner_review_record"]["record_ready"] is True
    assert payload["observation_non_approval_record"]["record_ready"] is True
    assert payload["next_route"]["recommended_next_research_task"] == review.NEXT_ROUTE

    for key in (
        "json_path",
        "candidate_owner_review_record_json",
        "observation_non_approval_record_json",
        "next_route_json",
        "markdown_path",
        "candidate_owner_review_markdown",
        "observation_non_approval_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_calibrated_gate_candidate_owner_review_decision_cli(
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
            "dynamic-strategy-calibrated-gate-candidate-owner-review-decision",
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
    assert (output_root / "candidate_owner_review_record.json").exists()
    assert (output_root / "observation_non_approval_record.json").exists()
    assert (output_root / "next_route.json").exists()


def test_dynamic_strategy_calibrated_gate_candidate_owner_review_decision_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_calibrated_gate_candidate_owner_review_decision"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-calibrated-gate-candidate-owner-review-decision"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("owner_review_decision.json" in item for item in entry["artifact_globs"])
    assert any(
        "observation_non_approval_record.json" in item
        for item in entry["artifact_globs"]
    )

    assert (
        "dynamic_strategy_calibrated_gate_candidate_owner_review_decision"
        in Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    )
    assert (
        "dynamic-strategy-calibrated-gate-candidate-owner-review-decision"
        in Path("docs/system_flow.md").read_text(encoding="utf-8")
    )
    assert (
        "TRADING-2391_DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER"
        in Path("docs/task_register.md").read_text(encoding="utf-8")
    )


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_expanded_candidate_retest_2386_path": paths[
            "expanded_candidate_retest_2386"
        ],
        "source_threshold_methodology_review_2388_path": paths[
            "threshold_methodology_review_2388"
        ],
        "source_owner_review_decision_2389_path": paths[
            "owner_review_decision_2389"
        ],
        "source_reclassification_result_2390_path": paths[
            "reclassification_result_2390"
        ],
        "source_candidate_reclassification_preview_2390_path": paths[
            "candidate_reclassification_preview_2390"
        ],
        "source_component_attribution_review_2390_path": paths[
            "component_attribution_review_2390"
        ],
        "source_owner_review_recommendation_2390_path": paths[
            "owner_review_recommendation_2390"
        ],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "expanded_candidate_retest_2386": "--source-expanded-candidate-retest-2386",
        "threshold_methodology_review_2388": (
            "--source-threshold-methodology-review-2388"
        ),
        "owner_review_decision_2389": "--source-owner-review-decision-2389",
        "reclassification_result_2390": "--source-reclassification-result-2390",
        "candidate_reclassification_preview_2390": (
            "--source-candidate-reclassification-preview-2390"
        ),
        "component_attribution_review_2390": (
            "--source-component-attribution-review-2390"
        ),
        "owner_review_recommendation_2390": (
            "--source-owner-review-recommendation-2390"
        ),
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "source"
    root.mkdir(parents=True, exist_ok=True)
    current_best_preview = {
        "candidate_id": review.RANKING_TOP_CANDIDATE,
        "previous_decision": review.CURRENT_BEST_PREVIOUS_DECISION,
        "preview_decision": review.CURRENT_BEST_PREVIEW_DECISION,
        "actual_approval_in_this_task": False,
        "supporting_metrics": {
            "dynamic_vs_static_gap": 0.021302,
            "turnover_budget_passed": True,
        },
        "failure_metrics": {
            "time_slice_pass_rate": 0.0,
            "regime_slice_pass_rate": 0.0,
            "drawdown_not_materially_worse": False,
        },
    }
    payloads = {
        "expanded_candidate_retest_2386": _source(
            review.SOURCE_2386_READY_STATUS,
            best_candidate_after_expanded_screening=review.RANKING_TOP_CANDIDATE,
            best_candidate_decision=review.CURRENT_BEST_PREVIOUS_DECISION,
            candidate_ready_for_research_only_observation=False,
        ),
        "threshold_methodology_review_2388": _source(
            review.SOURCE_2388_READY_STATUS,
            threshold_methodology_review_ready=True,
            research_only_vs_paper_shadow_gate_separated=True,
            reference_candidate_policy_recommendation=(
                review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
            ),
        ),
        "owner_review_decision_2389": _source(
            review.SOURCE_2389_READY_STATUS,
            owner_decision=review.SOURCE_2389_OWNER_DECISION,
            reference_candidate_policy_adopted=(
                review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
            ),
            threshold_methodology_adopted=True,
            research_only_vs_paper_shadow_gate_separated=True,
            calibrated_reclassification_preview_approved=True,
            component_attribution_review_required=True,
            candidate_auto_accept_approved=False,
            current_best_candidate_observation_approved=False,
        ),
        "reclassification_result_2390": _source(
            review.SOURCE_2390_READY_STATUS,
            current_best_candidate=review.RANKING_TOP_CANDIDATE,
            current_best_candidate_previous_decision=(
                review.CURRENT_BEST_PREVIOUS_DECISION
            ),
            current_best_candidate_preview_decision=(
                review.CURRENT_BEST_PREVIEW_DECISION
            ),
            reference_candidate_policy=(
                review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
            ),
            candidate_auto_accept_approved=False,
            research_only_observation_approved=False,
            component_value_candidates=list(review.COMPONENT_VALUE_CANDIDATES),
            recommended_next_research_task=review.SOURCE_2390_EXPECTED_ROUTE,
            candidate_reclassification_preview=[current_best_preview],
        ),
        "candidate_reclassification_preview_2390": _source(
            review.SOURCE_2390_READY_STATUS,
            current_best_candidate=review.RANKING_TOP_CANDIDATE,
            current_best_candidate_preview_decision=(
                review.CURRENT_BEST_PREVIEW_DECISION
            ),
            candidate_reclassification_preview=[current_best_preview],
        ),
        "component_attribution_review_2390": _source(
            review.SOURCE_2390_READY_STATUS,
            component_value_candidates=list(review.COMPONENT_VALUE_CANDIDATES),
            component_attribution_review=[],
        ),
        "owner_review_recommendation_2390": _source(
            review.SOURCE_2390_READY_STATUS,
            owner_review_recommendation={
                "enter_owner_review_decision": True,
                "recommendation": (
                    "PROCEED_TO_2391_OWNER_REVIEW_DECISION_WITH_NO_OBSERVATION_APPROVAL_IN_2390"
                ),
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
        **{field: False for field in review.SAFETY_FALSE_FIELDS},
    }
    payload.update(updates)
    return payload
