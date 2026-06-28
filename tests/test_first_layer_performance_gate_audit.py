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

    assert payload["status"] == (
        "FIRST_LAYER_PERFORMANCE_GATE_AUDIT_READY_PROMOTION_BLOCKED"
    )
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["broker_action"] == "none"
    assert payload["summary"]["current_gate_accept_count"] == 0
    assert gate_rows["coverage_pass_rule"]["gate_marginal_utility"] == "negative"
    assert (
        gate_rows["coverage_pass_rule"]["recommended_action"]
        == "convert_to_owner_review_evidence_gate"
    )
    assert (
        gate_rows["no_major_regression_in_defensive_probe"]["gate_marginal_utility"]
        == "positive"
    )
    assert (
        gate_rows["same_risk_comparison_reported"]["recommended_action"]
        == "retain_as_audit_completeness_gate_not_performance_gate"
    )

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
    assert recommended["summary"]["paper_shadow_allowed"] is False
    assert recommended["safety_boundary"]["broker_action"] == "none"


def test_first_layer_performance_gate_audit_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-performance-gate-audit" in result.output
