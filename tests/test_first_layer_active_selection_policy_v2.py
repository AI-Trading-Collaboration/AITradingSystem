from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import first_layer_active_selection_policy_v2
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_first_layer_active_selection_policy_v2_preserves_owner_review_state(
    tmp_path: Path,
) -> None:
    payload = (
        first_layer_active_selection_policy_v2.run_first_layer_active_selection_policy_v2_pack(
            output_root=tmp_path / "outputs",
            docs_root=tmp_path / "docs",
        )
    )

    rows = {row["candidate_id"]: row for row in payload["updated_challenger_selection_matrix"]}
    owner_queue_rows = {
        row["candidate_id"]: row for row in payload["owner_review_queue"]["owner_review_queue"]
    }
    research_queue_rows = {
        row["candidate_id"]: row
        for row in payload["research_candidate_queue"]["ranked_review_queue"]
    }
    ranked_review_queue = payload["research_candidate_queue"]["ranked_review_queue"]

    assert payload["status"] == ("FIRST_LAYER_ACTIVE_SELECTION_POLICY_V2_READY_PROMOTION_BLOCKED")
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"
    assert payload["promotion_boundary"]["active_selection_can_set_promotion_ready"] is False
    assert payload["promotion_boundary"]["promotion_ready_count"] == 0

    assert rows["wf_504d_baseline"]["gate_policy_v2_state"] == "OWNER_REVIEW_REQUIRED"
    assert rows["wf_504d_baseline"]["selection_state"] == "OWNER_REVIEW_REQUIRED"
    assert rows["wf_504d_baseline"]["selection_state"] != "BLOCKED"
    assert rows["wf_504d_baseline"]["promotion_allowed"] is False
    assert owner_queue_rows["wf_504d_baseline"]["selection_state"] == ("OWNER_REVIEW_REQUIRED")
    assert "2023_plus_dependency" in owner_queue_rows["wf_504d_baseline"]["risk_flags"]
    assert owner_queue_rows["wf_504d_baseline"]["rank_features"]["utility"] == 0.070283
    assert ranked_review_queue[0]["candidate_id"] == "wf_504d_baseline"

    assert rows["wf_378d_initial"]["gate_policy_v2_state"] == "ACCEPTED"
    assert rows["wf_378d_initial"]["selection_state"] == "RESEARCH_ACCEPTED"
    assert rows["wf_378d_initial"]["promotion_allowed"] is False
    assert research_queue_rows["wf_378d_initial"]["selection_state"] == ("RESEARCH_ACCEPTED")
    assert research_queue_rows["wf_378d_initial"]["rank_features"]["utility"] == 0.041538

    boundary = {row["candidate_id"]: row for row in payload["boundary_candidate_rows"]}
    assert boundary["wf_504d_baseline"]["passes_expected_state"] is True
    assert boundary["wf_378d_initial"]["passes_expected_state"] is True


def test_first_layer_active_selection_policy_v2_writes_artifacts(tmp_path: Path) -> None:
    payload = (
        first_layer_active_selection_policy_v2.run_first_layer_active_selection_policy_v2_pack(
            output_root=tmp_path / "outputs",
            docs_root=tmp_path / "docs",
        )
    )
    artifact_paths = payload["artifact_paths"]

    for key in (
        "active_selection_policy_v2_markdown",
        "active_selection_policy_v2_yaml",
        "research_candidate_queue",
        "owner_review_queue",
        "promotion_boundary_report",
        "updated_challenger_selection_matrix",
    ):
        assert Path(artifact_paths[key]).exists()

    policy = safe_load_yaml_path(Path(artifact_paths["active_selection_policy_v2_yaml"]))
    assert isinstance(policy, dict)
    assert "OWNER_REVIEW_REQUIRED" in policy["selection_states"]
    assert (
        policy["transition_rules"]["gate_policy_v2_state_OWNER_REVIEW_REQUIRED"][
            "may_be_rewritten_to_BLOCKED_by_active_selection"
        ]
        is False
    )
    assert policy["promotion_boundary"]["active_selection_decides_promotion"] is False
    assert policy["promotion_boundary"]["promotion_allowed"] is False
    assert policy["promotion_boundary"]["paper_shadow_allowed"] is False
    assert policy["promotion_boundary"]["production_allowed"] is False
    assert policy["promotion_boundary"]["broker_action"] == "none"


def test_first_layer_active_selection_policy_v2_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-active-selection-policy-v2" in result.output
