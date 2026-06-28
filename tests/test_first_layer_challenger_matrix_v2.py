from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import first_layer_challenger_matrix_v2
from ai_trading_system.cli_commands.research_trends import trends_app


def test_first_layer_challenger_matrix_v2_preserves_policy_v2_boundaries(
    tmp_path: Path,
) -> None:
    payload = first_layer_challenger_matrix_v2.run_first_layer_challenger_matrix_v2_pack(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )

    rows = {row["candidate_id"]: row for row in payload["candidate_rows"]}
    summary = payload["summary"]

    assert payload["status"] == "FIRST_LAYER_CHALLENGER_MATRIX_V2_READY_PROMOTION_BLOCKED"
    assert payload["gate_policy_v2_applied"] is True
    assert payload["active_selection_policy_v2_applied"] is True
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"

    assert summary["research_accepted_count"] == 1
    assert summary["offline_validation_ready_count"] == 4
    assert summary["owner_review_required_count"] == 1
    assert summary["blocked_count"] == 4
    assert summary["promotion_ready_count"] == 0
    assert summary["best_research_candidate"]["candidate_id"] == "wf_378d_initial"
    assert summary["best_owner_review_candidate"]["candidate_id"] == "wf_504d_baseline"

    assert rows["wf_504d_baseline"]["v1_active_selection_state"] == "BLOCKED"
    assert rows["wf_504d_baseline"]["selection_policy_v2_state"] == "OWNER_REVIEW_REQUIRED"
    assert rows["wf_504d_baseline"]["candidate_state_transition_from_v1"] == (
        "BLOCKED -> OWNER_REVIEW_REQUIRED"
    )
    assert rows["wf_504d_baseline"]["promotion_ready"] is False
    assert rows["wf_504d_baseline"]["promotion_allowed"] is False

    assert rows["wf_378d_initial"]["v1_active_selection_state"] == "BLOCKED"
    assert rows["wf_378d_initial"]["selection_policy_v2_state"] == "RESEARCH_ACCEPTED"
    assert rows["wf_378d_initial"]["candidate_state_transition_from_v1"] == (
        "BLOCKED -> RESEARCH_ACCEPTED"
    )
    assert rows["wf_378d_initial"]["promotion_ready"] is False
    assert rows["wf_378d_initial"]["promotion_allowed"] is False

    assert payload["promotion_boundary_check"]["passed"] is True
    assert all(row["passed"] for row in payload["promotion_boundary_check"]["checks"])


def test_first_layer_challenger_matrix_v2_writes_required_artifacts(
    tmp_path: Path,
) -> None:
    payload = first_layer_challenger_matrix_v2.run_first_layer_challenger_matrix_v2_pack(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )

    for key in (
        "first_layer_challenger_matrix_v2",
        "first_layer_challenger_report_v2",
        "research_candidate_queue_v2",
        "owner_review_queue_v2",
        "blocked_candidate_queue_v2",
        "promotion_boundary_check_v2",
    ):
        assert Path(payload["artifact_paths"][key]).exists()

    research_queue = payload["research_candidate_queue_v2"]["candidate_rows"]
    owner_review_queue = payload["owner_review_queue_v2"]["candidate_rows"]
    blocked_queue = payload["blocked_candidate_queue_v2"]["candidate_rows"]

    assert {row["candidate_id"] for row in research_queue} >= {"wf_378d_initial"}
    assert {row["candidate_id"] for row in owner_review_queue} == {"wf_504d_baseline"}
    assert blocked_queue
    assert all(row["promotion_allowed"] is False for row in research_queue)
    assert all(row["broker_action"] == "none" for row in blocked_queue)


def test_first_layer_challenger_matrix_v2_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-challenger-matrix-v2" in result.output
