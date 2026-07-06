from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_calibrated_gate_owner_review_decision as review
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_calibrated_gate_owner_review_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_review"
    docs_root = tmp_path / "docs" / "research"

    payload = review.run_dynamic_strategy_calibrated_gate_owner_review_decision(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 6),
    )

    assert payload["status"] == review.READY_STATUS
    assert payload["source_tasks"] == list(review.SOURCE_TASKS)
    assert payload["source_validation_errors"] == []
    assert payload["source_ready_for_calibrated_gate_owner_review"] is True
    assert payload["owner_decision_recorded"] is True
    assert payload["owner_decision"] == review.OWNER_DECISION
    assert payload["threshold_methodology_adopted"] is True
    assert payload["research_only_vs_paper_shadow_gate_separated"] is True
    assert payload["reference_candidate_policy_adopted"] == (
        review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
    )
    assert payload["candidate_auto_accept_approved"] is False
    assert payload["current_best_candidate_observation_approved"] is False
    assert payload["calibrated_reclassification_preview_approved"] is True
    assert payload["component_attribution_review_required"] is True
    assert payload["future_statistical_threshold_calibration_required"] is True
    assert payload["recommended_next_research_task"] == review.NEXT_ROUTE

    adoption = payload["calibrated_gate_adoption_record"]
    assert adoption["threshold_methodology_adopted"] is True
    assert adoption["reference_candidate_policy"]["adopted_policy"] == (
        review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
    )
    assert adoption["policy_update_applied"] is False
    assert adoption["rules_mutated"] is False

    non_approval = payload["non_approval_record"]
    assert non_approval["candidate_auto_accept_approved"] is False
    assert non_approval["current_best_candidate_observation_approved"] is False
    assert non_approval["paper_shadow_approved"] is False
    assert "research_only_observation_for_candidate" in (
        non_approval["explicit_non_approval_list"]
    )

    route = payload["next_reclassification_route"]
    assert route["recommended_next_research_task"] == review.NEXT_ROUTE
    assert {
        item["candidate_id"] for item in route["candidate_reclassification_targets"]
    } == set(review.CANDIDATE_RECLASSIFICATION_TARGETS)
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == review.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["backtest_run"] is False
    assert payload["new_signal_generated"] is False
    assert payload["scoring_run"] is False
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
        "calibrated_gate_adoption_record_json",
        "non_approval_record_json",
        "next_reclassification_route_json",
        "markdown_path",
        "calibrated_gate_adoption_record_markdown",
        "non_approval_record_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_calibrated_gate_owner_review_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_review_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-calibrated-gate-owner-review-decision",
            *_source_args(source_paths),
            "--as-of",
            "2026-07-06",
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
    assert (output_root / "calibrated_gate_adoption_record.json").exists()
    assert (output_root / "non_approval_record.json").exists()
    assert (output_root / "next_reclassification_route.json").exists()


def test_dynamic_strategy_calibrated_gate_owner_review_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_calibrated_gate_owner_review_decision"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-calibrated-gate-owner-review-decision"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("owner_review_decision.json" in item for item in entry["artifact_globs"])
    assert any(
        "next_reclassification_route.json" in item
        for item in entry["artifact_globs"]
    )

    assert "dynamic_strategy_calibrated_gate_owner_review_decision" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-calibrated-gate-owner-review-decision" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    assert "TRADING-2389_DYNAMIC_STRATEGY_CALIBRATED_GATE" in Path(
        "docs/task_register.md"
    ).read_text(encoding="utf-8")


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_expanded_candidate_retest_path": paths["expanded_candidate_retest"],
        "source_expanded_candidate_ranking_path": paths[
            "expanded_candidate_ranking"
        ],
        "source_expanded_decision_update_path": paths["expanded_decision_update"],
        "source_gate_calibration_review_path": paths["gate_calibration_review"],
        "source_gate_policy_review_path": paths["gate_policy_review"],
        "source_candidate_reclassification_preview_path": paths[
            "candidate_reclassification_preview"
        ],
        "source_recommended_policy_update_path": paths["recommended_policy_update"],
        "source_threshold_methodology_review_path": paths[
            "threshold_methodology_review"
        ],
        "source_threshold_inventory_path": paths["threshold_inventory"],
        "source_gate_taxonomy_path": paths["gate_taxonomy"],
        "source_candidate_threshold_outcome_matrix_path": paths[
            "candidate_threshold_outcome_matrix"
        ],
        "source_recommended_gate_policy_proposal_path": paths[
            "recommended_gate_policy_proposal"
        ],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "expanded_candidate_retest": "--source-expanded-candidate-retest",
        "expanded_candidate_ranking": "--source-expanded-candidate-ranking",
        "expanded_decision_update": "--source-expanded-decision-update",
        "gate_calibration_review": "--source-gate-calibration-review",
        "gate_policy_review": "--source-gate-policy-review",
        "candidate_reclassification_preview": (
            "--source-candidate-reclassification-preview"
        ),
        "recommended_policy_update": "--source-recommended-policy-update",
        "threshold_methodology_review": "--source-threshold-methodology-review",
        "threshold_inventory": "--source-threshold-inventory",
        "gate_taxonomy": "--source-gate-taxonomy",
        "candidate_threshold_outcome_matrix": (
            "--source-candidate-threshold-outcome-matrix"
        ),
        "recommended_gate_policy_proposal": (
            "--source-recommended-gate-policy-proposal"
        ),
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "source"
    root.mkdir(parents=True, exist_ok=True)
    proposal = _recommended_gate_policy_proposal()
    candidate_matrix = _candidate_matrix()
    payloads = {
        "expanded_candidate_retest": _source(
            review.SOURCE_2386_READY_STATUS,
            best_candidate_after_expanded_screening=review.RANKING_TOP_CANDIDATE,
            best_candidate_decision="CONTINUE_OPTIMIZATION",
            candidate_ready_for_research_only_observation=False,
        ),
        "expanded_candidate_ranking": _source(
            review.SOURCE_2386_READY_STATUS,
            expanded_candidate_ranking=candidate_matrix,
        ),
        "expanded_decision_update": _source(
            review.SOURCE_2386_READY_STATUS,
            decision_update={
                "best_candidate_after_expanded_screening": (
                    review.RANKING_TOP_CANDIDATE
                ),
                "best_candidate_decision": "CONTINUE_OPTIMIZATION",
                "candidate_ready_for_research_only_observation": False,
            },
        ),
        "gate_calibration_review": _source(
            review.SOURCE_2387_READY_STATUS,
            reference_candidate_policy_recommendation=(
                review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
            ),
            observation_approved=False,
            policy_update_applied=False,
            research_only_gate_may_be_too_strict=True,
        ),
        "gate_policy_review": _source(review.SOURCE_2387_READY_STATUS),
        "candidate_reclassification_preview": _source(
            review.SOURCE_2387_READY_STATUS,
            candidate_reclassification_preview={
                review.RANKING_TOP_CANDIDATE: {
                    "preview_decision_under_calibrated_gate": (
                        "OWNER_REVIEW_REQUIRED"
                    ),
                    "auto_accept_allowed": False,
                    "owner_review_allowed": True,
                }
            },
        ),
        "recommended_policy_update": _source(
            review.SOURCE_2387_READY_STATUS,
            policy_update_applied=False,
            rules_mutated=False,
        ),
        "threshold_methodology_review": _source(
            review.SOURCE_2388_READY_STATUS,
            recommended_next_research_task=review.SOURCE_2388_EXPECTED_ROUTE,
            threshold_methodology_review_ready=True,
            research_only_vs_paper_shadow_gate_separated=True,
            reference_candidate_policy_recommendation=(
                review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
            ),
            current_gate_may_be_too_strict_for_research_only_observation=True,
            thresholds_requiring_statistical_calibration=[
                "time_slice_pass_rate",
                "regime_expectation_score",
                "drawdown_materiality",
                "return_per_drawdown_penalty",
                "owner_review_required_vs_continue_optimization_boundary",
            ],
            candidate_threshold_outcome_matrix=candidate_matrix,
            recommended_gate_policy_proposal=proposal,
        ),
        "threshold_inventory": _source(
            review.SOURCE_2388_READY_STATUS,
            threshold_inventory={"slice_stability_thresholds": {}},
        ),
        "gate_taxonomy": _source(
            review.SOURCE_2388_READY_STATUS,
            gate_taxonomy={"research_only_observation_gate": {"side_effect": "none"}},
        ),
        "candidate_threshold_outcome_matrix": _source(
            review.SOURCE_2388_READY_STATUS,
            candidate_threshold_outcome_matrix=candidate_matrix,
        ),
        "recommended_gate_policy_proposal": _source(
            review.SOURCE_2388_READY_STATUS,
            recommended_gate_policy_proposal=proposal,
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


def _recommended_gate_policy_proposal() -> dict[str, object]:
    return {
        "policy_update_applied": False,
        "rules_mutated": False,
        "reference_candidate_policy": {
            "current": "HARD_BLOCK_ACCEPTANCE",
            "recommended": review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION,
        },
    }


def _candidate_matrix() -> list[dict[str, object]]:
    return [
        {
            "candidate_id": candidate_id,
            "latest_decision": "CONTINUE_OPTIMIZATION",
            "candidate_value_type": "candidate",
            "likely_reclassification_under_calibrated_gate": (
                "OWNER_REVIEW_REQUIRED"
                if candidate_id == review.RANKING_TOP_CANDIDATE
                else "CONTINUE_OPTIMIZATION"
            ),
        }
        for candidate_id in review.CANDIDATE_RECLASSIFICATION_TARGETS
    ]
