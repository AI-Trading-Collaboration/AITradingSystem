from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import first_layer_active_selection_rule_audit
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_first_layer_active_selection_rule_audit_outputs_required_artifacts(
    tmp_path: Path,
) -> None:
    run_pack = (
        first_layer_active_selection_rule_audit.run_first_layer_active_selection_rule_audit_pack
    )
    payload = run_pack(output_root=tmp_path / "outputs", docs_root=tmp_path / "docs")

    mode_rows = {row["mode"]: row for row in payload["mode_rows"]}
    conclusion = payload["conclusion"]
    artifact_paths = payload["artifact_paths"]

    assert payload["status"] == ("FIRST_LAYER_ACTIVE_SELECTION_RULE_AUDIT_READY_PROMOTION_BLOCKED")
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"
    assert set(mode_rows) == {
        "no_active_selection",
        "relaxed_active_selection",
        "current_active_selection",
        "strict_active_selection",
    }
    assert mode_rows["current_active_selection"]["accepted_candidate_count"] == 0
    assert mode_rows["no_active_selection"]["owner_review_required_count"] >= 1
    assert mode_rows["relaxed_active_selection"]["accepted_candidate_count"] >= 1
    assert mode_rows["current_active_selection"]["blocked_candidate_count"] == 4
    assert mode_rows["current_active_selection"]["best_rejected_candidate_utility"] == 0.070283

    assert conclusion["active_selection_marginal_utility"] == "negative"
    assert conclusion["active_selection_blocks_best_candidate"] is True
    assert conclusion["active_selection_conflicts_with_gate_policy_v2"] is True
    assert conclusion["owner_review_candidates_suppressed_by_selection"] == 1
    assert conclusion["recommended_action"] == "split_selection_and_promotion"

    current_decisions = {
        row["policy_id"]: row
        for row in mode_rows["current_active_selection"]["candidate_decisions"]
    }
    assert current_decisions["wf_504d_baseline"]["gate_policy_v2_state"] == (
        "OWNER_REVIEW_REQUIRED"
    )
    assert current_decisions["wf_504d_baseline"]["selection_state"] == "BLOCKED"
    assert current_decisions["wf_504d_baseline"]["owner_review_suppressed_by_selection"] is True

    assert (
        payload["source_generation"]["recommended_gate_policy_v2_source"]
        == "regenerated_from_trading_2274_and_2275_code_paths"
    )
    assert payload["source_generation"]["ignored_outputs_not_required_as_source_of_truth"] is True

    for key in (
        "active_selection_rule_audit_report",
        "active_selection_ablation_matrix",
        "active_selection_counterfactual_report",
        "active_selection_threshold_sensitivity",
        "active_selection_recommended_policy",
    ):
        assert Path(artifact_paths[key]).exists()

    recommended = safe_load_yaml_path(Path(artifact_paths["active_selection_recommended_policy"]))
    assert isinstance(recommended, dict)
    assert recommended["recommended_action"] == "split_selection_and_promotion"
    assert recommended["recommended_semantics"]["owner_review_required_is_not_blocked"]
    assert recommended["recommended_semantics"]["promotion_allowed"] is False
    assert recommended["recommended_semantics"]["paper_shadow_allowed"] is False
    assert recommended["recommended_semantics"]["production_allowed"] is False
    assert recommended["recommended_semantics"]["broker_action"] == "none"


def test_first_layer_active_selection_rule_audit_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-active-selection-rule-audit" in result.output
