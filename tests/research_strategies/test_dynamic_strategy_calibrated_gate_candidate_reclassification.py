from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_calibrated_gate_candidate_reclassification as review
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_calibrated_gate_candidate_reclassification_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "reclassification"
    docs_root = tmp_path / "docs" / "research"

    payload = (
        review.run_dynamic_strategy_calibrated_gate_candidate_reclassification(
            **_source_kwargs(source_paths),
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=date(2026, 7, 6),
        )
    )

    assert payload["status"] == review.READY_STATUS
    assert payload["source_tasks"] == list(review.SOURCE_TASKS)
    assert payload["source_validation_errors"] == []
    assert payload["calibrated_gate_policy_source"] == "TRADING-2389"
    assert payload["reference_candidate_policy"] == (
        review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
    )
    assert payload["candidate_reclassification_ready"] is True
    assert payload["component_attribution_ready"] is True
    assert payload["owner_review_recommendation_ready"] is True
    assert payload["current_best_candidate"] == review.RANKING_TOP_CANDIDATE
    assert payload["current_best_candidate_previous_decision"] == (
        review.CURRENT_BEST_PREVIOUS_DECISION
    )
    assert payload["current_best_candidate_preview_decision"] == (
        review.CURRENT_BEST_PREVIEW_DECISION
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
    assert payload["recommended_next_research_task"] == review.NEXT_ROUTE

    preview_by_candidate = {
        item["candidate_id"]: item for item in payload["candidate_reclassification_preview"]
    }
    assert preview_by_candidate[review.RANKING_TOP_CANDIDATE]["preview_decision"] == (
        "OWNER_REVIEW_REQUIRED"
    )
    assert preview_by_candidate["dynamic_turnover_budgeted_growth_tilt_v1"][
        "preview_decision"
    ] == "COMPONENT_VALUE_ONLY"
    assert preview_by_candidate["dynamic_valid_until_expiry_strict_v1"][
        "preview_decision"
    ] == "COMPONENT_VALUE_ONLY"
    assert set(payload["component_value_candidates"]) == set(
        review.COMPONENT_VALUE_CANDIDATES
    )
    assert len(payload["component_attribution_review"]) >= 5
    assert payload["owner_review_recommendation"]["enter_owner_review_decision"] is True

    for key in (
        "json_path",
        "candidate_reclassification_preview_json",
        "component_attribution_review_json",
        "owner_review_recommendation_json",
        "markdown_path",
        "candidate_reclassification_preview_markdown",
        "component_attribution_review_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_calibrated_gate_candidate_reclassification_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "reclassification_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-calibrated-gate-candidate-reclassification",
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
    assert (output_root / "reclassification_result.json").exists()
    assert (output_root / "candidate_reclassification_preview.json").exists()
    assert (output_root / "component_attribution_review.json").exists()
    assert (output_root / "owner_review_recommendation.json").exists()


def test_dynamic_strategy_calibrated_gate_candidate_reclassification_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_calibrated_gate_candidate_reclassification"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-calibrated-gate-candidate-reclassification"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("reclassification_result.json" in item for item in entry["artifact_globs"])
    assert any(
        "component_attribution_review.json" in item
        for item in entry["artifact_globs"]
    )

    assert "dynamic_strategy_calibrated_gate_candidate_reclassification" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-calibrated-gate-candidate-reclassification" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    assert "TRADING-2390_DYNAMIC_STRATEGY_CALIBRATED_GATE" in Path(
        "docs/task_register.md"
    ).read_text(encoding="utf-8")


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_candidate_ranking_2365_path": paths["candidate_ranking_2365"],
        "source_sensitivity_result_2366_path": paths["sensitivity_result_2366"],
        "source_expanded_candidate_retest_path": paths["expanded_candidate_retest"],
        "source_expanded_candidate_ranking_path": paths[
            "expanded_candidate_ranking"
        ],
        "source_expanded_decision_update_path": paths["expanded_decision_update"],
        "source_threshold_methodology_review_path": paths[
            "threshold_methodology_review"
        ],
        "source_candidate_threshold_outcome_matrix_path": paths[
            "candidate_threshold_outcome_matrix"
        ],
        "source_recommended_gate_policy_proposal_path": paths[
            "recommended_gate_policy_proposal"
        ],
        "source_owner_review_decision_path": paths["owner_review_decision"],
        "source_calibrated_gate_adoption_record_path": paths[
            "calibrated_gate_adoption_record"
        ],
        "source_non_approval_record_path": paths["non_approval_record"],
        "source_next_reclassification_route_path": paths[
            "next_reclassification_route"
        ],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "candidate_ranking_2365": "--source-candidate-ranking-2365",
        "sensitivity_result_2366": "--source-sensitivity-result-2366",
        "expanded_candidate_retest": "--source-expanded-candidate-retest",
        "expanded_candidate_ranking": "--source-expanded-candidate-ranking",
        "expanded_decision_update": "--source-expanded-decision-update",
        "threshold_methodology_review": "--source-threshold-methodology-review",
        "candidate_threshold_outcome_matrix": (
            "--source-candidate-threshold-outcome-matrix"
        ),
        "recommended_gate_policy_proposal": (
            "--source-recommended-gate-policy-proposal"
        ),
        "owner_review_decision": "--source-owner-review-decision",
        "calibrated_gate_adoption_record": (
            "--source-calibrated-gate-adoption-record"
        ),
        "non_approval_record": "--source-non-approval-record",
        "next_reclassification_route": "--source-next-reclassification-route",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "source"
    root.mkdir(parents=True, exist_ok=True)
    candidate_rows = _candidate_rows()
    threshold_rows = _threshold_rows()
    payloads = {
        "candidate_ranking_2365": _source(
            review.SOURCE_2365_READY_STATUS,
            candidate_ranking=[
                {
                    "candidate_id": review.RANKING_TOP_CANDIDATE,
                    "decision": "OWNER_REVIEW_REQUIRED",
                }
            ],
        ),
        "sensitivity_result_2366": _source(
            review.SOURCE_2366_READY_STATUS,
            top_candidate_from_2365=review.RANKING_TOP_CANDIDATE,
        ),
        "expanded_candidate_retest": _source(
            review.SOURCE_2386_READY_STATUS,
            best_candidate_after_expanded_screening=review.RANKING_TOP_CANDIDATE,
            best_candidate_decision=review.CURRENT_BEST_PREVIOUS_DECISION,
            candidate_ready_for_research_only_observation=False,
            expanded_candidate_ranking=candidate_rows,
        ),
        "expanded_candidate_ranking": _source(
            review.SOURCE_2386_READY_STATUS,
            expanded_candidate_ranking=candidate_rows,
        ),
        "expanded_decision_update": _source(
            review.SOURCE_2386_READY_STATUS,
            decision_update={
                "best_candidate_after_expanded_screening": (
                    review.RANKING_TOP_CANDIDATE
                ),
                "best_candidate_decision": review.CURRENT_BEST_PREVIOUS_DECISION,
            },
        ),
        "threshold_methodology_review": _source(
            review.SOURCE_2388_READY_STATUS,
            threshold_methodology_review_ready=True,
            research_only_vs_paper_shadow_gate_separated=True,
            reference_candidate_policy_recommendation=(
                review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
            ),
            candidate_threshold_outcome_matrix=threshold_rows,
        ),
        "candidate_threshold_outcome_matrix": _source(
            review.SOURCE_2388_READY_STATUS,
            candidate_threshold_outcome_matrix=threshold_rows,
        ),
        "recommended_gate_policy_proposal": _source(
            review.SOURCE_2388_READY_STATUS,
            recommended_gate_policy_proposal={
                "reference_candidate_policy": {
                    "recommended": review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
                },
                "policy_update_applied": False,
                "rules_mutated": False,
            },
        ),
        "owner_review_decision": _source(
            review.SOURCE_2389_READY_STATUS,
            owner_decision=review.OWNER_DECISION,
            recommended_next_research_task=review.SOURCE_2389_EXPECTED_ROUTE,
            reference_candidate_policy_adopted=(
                review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
            ),
            threshold_methodology_adopted=True,
            research_only_vs_paper_shadow_gate_separated=True,
            calibrated_reclassification_preview_approved=True,
            component_attribution_review_required=True,
            future_statistical_threshold_calibration_required=True,
            candidate_auto_accept_approved=False,
            current_best_candidate_observation_approved=False,
        ),
        "calibrated_gate_adoption_record": _source(
            review.SOURCE_2389_READY_STATUS,
            calibrated_gate_adoption_record={
                "owner_decision": review.OWNER_DECISION,
                "threshold_methodology_adopted": True,
                "reference_candidate_policy": {
                    "adopted_policy": (
                        review.REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
                    )
                },
                "policy_update_applied": False,
                "rules_mutated": False,
            },
        ),
        "non_approval_record": _source(
            review.SOURCE_2389_READY_STATUS,
            non_approval_record={
                "current_best_candidate_observation_approved": False,
                "candidate_auto_accept_approved": False,
            },
        ),
        "next_reclassification_route": _source(
            review.SOURCE_2389_READY_STATUS,
            next_reclassification_route={
                "recommended_next_research_task": review.SOURCE_2389_EXPECTED_ROUTE
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


def _candidate_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = [
        _candidate(review.RANKING_TOP_CANDIDATE, 1, "reference_candidate", 0.021302),
        _candidate(
            "dynamic_turnover_budgeted_growth_tilt_v1",
            2,
            "new_candidate",
            0.006941,
            time_slice_pass_rate=0.428571,
            drawdown_gap_vs_static=-0.000389,
        ),
        _candidate(
            "dynamic_valid_until_expiry_strict_v1",
            3,
            "new_candidate",
            0.007195,
            time_slice_pass_rate=0.428571,
            drawdown_gap_vs_static=-0.005479,
        ),
        _candidate(
            "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
            4,
            "reference_candidate",
            0.010275,
        ),
        _candidate(
            "equal_risk_growth_tilt_guarded_turnover_v1",
            5,
            "reference_candidate",
            0.02062,
        ),
    ]
    return rows


def _candidate(
    candidate_id: str,
    rank: int,
    candidate_type: str,
    dynamic_vs_static_gap: float,
    *,
    time_slice_pass_rate: float = 0.0,
    drawdown_gap_vs_static: float = 0.043574,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "candidate_type": candidate_type,
        "rank": rank,
        "decision": review.CURRENT_BEST_PREVIOUS_DECISION,
        "dynamic_vs_static_gap": dynamic_vs_static_gap,
        "cost_adjusted_dynamic_vs_static_gap": dynamic_vs_static_gap,
        "realistic_cost_passed": True,
        "conservative_cost_passed": True,
        "harsh_cost_passed": True,
        "turnover_budget_passed": True,
        "time_slice_pass_rate": time_slice_pass_rate,
        "regime_slice_pass_rate": 0.0,
        "return_advantage_retained": 1.0 if rank == 1 else 0.3,
        "drawdown_gap_vs_static": drawdown_gap_vs_static,
        "candidate_vs_ranking_top_gap": 0.0 if rank == 1 else -0.01,
        "candidate_vs_guarded_ranking_top_gap": 0.0 if rank == 5 else -0.01,
        "candidate_vs_lower_turnover_gap": 0.01,
        "turnover": 2.0,
        "max_monthly_turnover": 0.5,
        "no_stale_signal_carry_forward": True,
        "valid_until_window_preserved": True,
        "signal_family": "fixture_family",
        "decision_reasons": ["fixture_reason"],
    }


def _threshold_rows() -> list[dict[str, object]]:
    rows = []
    for candidate_id in review.REQUIRED_FOCUS_CANDIDATES:
        rows.append(
            {
                "candidate_id": candidate_id,
                "latest_decision": review.CURRENT_BEST_PREVIOUS_DECISION,
                "likely_reclassification_under_calibrated_gate": (
                    "OWNER_REVIEW_REQUIRED"
                    if candidate_id == review.RANKING_TOP_CANDIDATE
                    else "CONTINUE_OPTIMIZATION"
                ),
                "current_gate_blockers": ["fixture_blocker"],
                "drawdown_not_materially_worse": candidate_id
                in review.COMPONENT_VALUE_CANDIDATES,
            }
        )
    return rows
