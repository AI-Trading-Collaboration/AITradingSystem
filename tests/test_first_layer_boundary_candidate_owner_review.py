from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.first_layer_boundary_candidate_owner_review import (
    run_first_layer_boundary_candidate_owner_review_pack,
)


def test_first_layer_boundary_owner_review_decisions_and_metrics(tmp_path: Path) -> None:
    payload = run_first_layer_boundary_candidate_owner_review_pack(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )

    rows = {row["candidate_id"]: row for row in payload["boundary_candidate_rows"]}

    assert payload["status"] == "FIRST_LAYER_BOUNDARY_OWNER_REVIEW_READY_PROMOTION_BLOCKED"
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"

    wf_504d = rows["wf_504d_baseline"]
    assert wf_504d["candidate_state"] == "OWNER_REVIEW_REQUIRED"
    assert wf_504d["owner_review_decision"] == "expand_neighborhood"
    assert wf_504d["primary_risk_flag"] == "2023_plus_dependency"
    assert wf_504d["utility_tradeoff_acceptable"] == "inconclusive"
    assert wf_504d["metrics"]["actual_path_utility"] == 0.070283
    assert wf_504d["metrics"]["dependency_2023_plus"]["risk_flag_present"] is True
    assert wf_504d["metrics"]["defensive_probe_result"]["no_major_regression"] is True
    assert wf_504d["promotion_ready"] is False

    wf_378d = rows["wf_378d_initial"]
    assert wf_378d["candidate_state"] == "RESEARCH_ACCEPTED"
    assert wf_378d["owner_review_decision"] == "continue_research"
    assert wf_378d["utility_tradeoff_acceptable"] is True
    assert wf_378d["metrics"]["actual_path_utility"] == 0.041538
    assert wf_378d["metrics"]["stress_2022_slice"]["covered_2022"] is True
    assert wf_378d["metrics"]["defensive_probe_result"]["no_major_regression"] is True
    assert wf_378d["promotion_ready"] is False

    assert wf_504d["metrics"]["false_risk_on_status"] == (
        "unavailable_for_frozen_actual_path_policy_rows"
    )
    assert wf_378d["metrics"]["beta_attribution_status"] == (
        "unavailable_candidate_level_beta_tqqq_dependency_not_run"
    )


def test_first_layer_boundary_owner_review_summaries_and_artifacts(tmp_path: Path) -> None:
    payload = run_first_layer_boundary_candidate_owner_review_pack(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )

    assert payload["offline_validation_ready_candidate_summary"]["candidate_count"] == 4
    assert payload["blocked_candidate_failure_reason_summary"]["candidate_count"] == 4
    assert (
        payload["blocked_candidate_failure_reason_summary"]["misclassification_check"]
        == "no_obvious_misclassification_detected"
    )
    blocked_rows = {
        row["candidate_id"]: row
        for row in payload["blocked_candidate_failure_reason_summary"]["blocked_candidate_rows"]
    }
    assert blocked_rows["wf_252d_initial"]["defensive_probe_result"] is False
    assert (
        blocked_rows["wf_expanding_initial"]["misclassification_risk"]
        == "blocked_due_strong_defensive_regression_reasonable"
    )
    assert payload["summary"]["wf_504d_owner_review_decision"] == "expand_neighborhood"
    assert payload["summary"]["wf_378d_owner_review_decision"] == "continue_research"

    for key in (
        "first_layer_boundary_candidate_owner_review",
        "boundary_candidate_comparison_matrix",
        "owner_review_candidate_tradeoff_summary",
        "offline_validation_ready_candidate_summary",
        "blocked_candidate_failure_reason_summary",
        "recommended_next_experiment_plan",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_first_layer_boundary_owner_review_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-boundary-owner-review" in result.output
