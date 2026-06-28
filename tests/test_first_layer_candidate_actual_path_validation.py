from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.first_layer_candidate_actual_path_validation import (
    run_first_layer_candidate_actual_path_validation_pack,
)


def test_candidate_actual_path_validation_reclassifies_missing_offline_rows(
    tmp_path: Path,
) -> None:
    payload = run_first_layer_candidate_actual_path_validation_pack(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )

    rows = {row["candidate_id"]: row for row in payload["candidate_rows"]}

    assert (
        payload["status"] == "FIRST_LAYER_CANDIDATE_ACTUAL_PATH_VALIDATION_READY_PROMOTION_BLOCKED"
    )
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"
    assert payload["summary"]["covered_candidate_count"] == 6
    assert payload["summary"]["candidate_level_actual_path_available_count"] == 2
    assert payload["summary"]["missing_candidate_signal_artifact_count"] == 4

    wf_504d = rows["wf_504d_baseline"]
    assert wf_504d["previous_state"] == "OWNER_REVIEW_REQUIRED"
    assert wf_504d["updated_state"] == "OWNER_REVIEW_REQUIRED"
    assert wf_504d["utility_rank"] == 1
    assert wf_504d["metrics"]["actual_path_utility"] == 0.070283
    assert wf_504d["primary_risk_flag"] == "2023_plus_dependency"
    assert wf_504d["expand_neighborhood"] is True
    assert wf_504d["promotion_ready"] is False

    wf_378d = rows["wf_378d_initial"]
    assert wf_378d["previous_state"] == "RESEARCH_ACCEPTED"
    assert wf_378d["updated_state"] == "RESEARCH_ACCEPTED"
    assert wf_378d["utility_rank"] == 2
    assert wf_378d["metrics"]["actual_path_utility"] == 0.041538
    assert wf_378d["continue_research"] is True
    assert wf_378d["promotion_ready"] is False

    for candidate_id in (
        "baseline",
        "baseline_plus_trend_structure",
        "risk_appetite",
        "volatility_regime",
    ):
        row = rows[candidate_id]
        assert row["previous_state"] == "OFFLINE_VALIDATION_READY"
        assert row["updated_state"] == "INCONCLUSIVE"
        assert row["validation_status"] == "missing_candidate_signal_artifact"
        assert row["metrics"]["actual_path_utility"] is None
        assert row["continue_research"] is False
        assert row["promotion_ready"] is False


def test_candidate_actual_path_validation_queues_and_artifacts(tmp_path: Path) -> None:
    payload = run_first_layer_candidate_actual_path_validation_pack(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )

    assert payload["updated_research_candidate_queue"]["candidate_count"] == 1
    assert payload["updated_owner_review_queue"]["candidate_count"] == 1
    assert payload["updated_offline_validation_queue"]["candidate_count"] == 0
    assert (
        payload["updated_offline_validation_queue"][
            "reclassified_from_offline_validation_ready_count"
        ]
        == 4
    )
    assert payload["summary"]["updated_state_counts"] == {
        "INCONCLUSIVE": 4,
        "OWNER_REVIEW_REQUIRED": 1,
        "RESEARCH_ACCEPTED": 1,
    }

    risk_rows = {row["candidate_id"]: row for row in payload["candidate_risk_attribution_rows"]}
    assert risk_rows["wf_504d_baseline"]["beta_attribution_status"] == (
        "unavailable_candidate_level_beta_tqqq_dependency_not_run"
    )
    assert risk_rows["baseline"]["validation_blockers"] == [
        "candidate_signal_artifact_missing",
        "candidate_actual_path_backtest_not_run",
    ]

    for key in (
        "candidate_actual_path_validation_report",
        "candidate_actual_path_matrix",
        "candidate_risk_attribution_matrix",
        "candidate_state_reclassification_report",
        "updated_research_candidate_queue",
        "updated_owner_review_queue",
        "updated_offline_validation_queue",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_candidate_actual_path_validation_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-candidate-actual-path-validation" in result.output
