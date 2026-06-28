from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import first_layer_performance_gate_audit
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_first_layer_performance_gate_audit_outputs_required_artifacts(
    tmp_path: Path,
) -> None:
    payload = first_layer_performance_gate_audit.run_first_layer_performance_gate_audit_pack(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
    )

    gate_rows = {row["gate_id"]: row for row in payload["gate_rows"]}
    artifact_paths = payload["artifact_paths"]
    mandatory_gate_ids = {
        "actual_path_improved_probe_count_min",
        "no_major_regression_in_defensive_probe",
        "2022_slice_not_worse_than_flat_reference",
        "net_of_cost_not_worse",
        "not_2023_plus_only",
        "not_beta_dependency",
        "not_tqqq_dependency",
        "probability_threshold_0_55",
        "probability_threshold_0_60",
        "all_slices_not_worse",
        "no_slice_regression",
    }
    allowed_actions = {
        "keep_as_hard_gate",
        "keep_as_performance_gate",
        "relax_threshold",
        "tighten_threshold",
        "convert_to_owner_review",
        "convert_to_score_penalty",
        "remove_gate",
    }

    assert payload["status"] == ("FIRST_LAYER_PERFORMANCE_GATE_AUDIT_READY_PROMOTION_BLOCKED")
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"
    assert set(gate_rows) == mandatory_gate_ids
    assert payload["summary"]["candidate_count_after_current_performance_gates"] == 1
    assert payload["summary"]["active_selection_rule_current_accept_count"] == 0
    assert payload["summary"]["offline_validation_ready_challenger_count"] == 4
    assert payload["summary"]["challenger_actual_path_available_count"] == 0
    assert gate_rows["not_2023_plus_only"]["gate_marginal_utility"] == "negative"
    assert gate_rows["not_2023_plus_only"]["recommended_action"] == "remove_gate"
    assert (
        gate_rows["not_2023_plus_only"]["owner_decision_override"]["owner_instruction"]
        == "do_not_continue_gate"
    )
    assert (
        gate_rows["no_major_regression_in_defensive_probe"]["gate_marginal_utility"] == "positive"
    )
    assert (
        gate_rows["no_major_regression_in_defensive_probe"]["recommended_action"]
        == "keep_as_hard_gate"
    )
    assert gate_rows["not_tqqq_dependency"]["gate_marginal_utility"] == "inconclusive"
    assert (
        gate_rows["probability_threshold_0_60"]["threshold_stability"]
        == "insufficient_candidate_level_evidence"
    )
    assert all(row["recommended_action"] in allowed_actions for row in gate_rows.values())

    for key in (
        "gate_ablation_matrix",
        "threshold_sensitivity_report",
        "rejected_candidate_counterfactual_report",
        "recommended_gate_policy",
        "gate_acceptance_audit_report",
    ):
        assert Path(artifact_paths[key]).exists()

    recommended = safe_load_yaml_path(Path(artifact_paths["recommended_gate_policy"]))
    assert isinstance(recommended, dict)
    assert recommended["summary"]["active_policy_change_allowed"] is False
    assert recommended["summary"]["promotion_allowed"] is False
    assert recommended["summary"]["paper_shadow_allowed"] is False
    assert recommended["summary"]["production_allowed"] is False
    assert recommended["safety_boundary"]["broker_action"] == "none"
    assert set(recommended["allowed_recommended_actions"]) == allowed_actions
    assert any(
        row["actual_path_status"] == "unavailable_actual_path_not_run"
        for row in payload["challenger_rows"]
    )


def test_first_layer_performance_gate_audit_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-performance-gate-audit" in result.output
