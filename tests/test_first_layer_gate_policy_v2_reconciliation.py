from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import first_layer_gate_policy_v2_reconciliation
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_first_layer_gate_policy_v2_reconciliation_outputs_required_artifacts(
    tmp_path: Path,
) -> None:
    run_pack = (
        first_layer_gate_policy_v2_reconciliation.run_first_layer_gate_policy_v2_reconciliation_pack
    )
    payload = run_pack(output_root=tmp_path / "outputs", docs_root=tmp_path / "docs")

    artifact_paths = payload["artifact_paths"]
    gate_rows = {row["gate_id"]: row for row in payload["gate_policy_v2_rows"]}
    hard_gate_ids = {row["gate_id"] for row in payload["hard_research_gates"]}

    assert payload["status"] == (
        "FIRST_LAYER_GATE_POLICY_V2_RECONCILIATION_READY_PROMOTION_BLOCKED"
    )
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"
    assert hard_gate_ids == {
        "pit_no_lookahead",
        "data_quality",
        "actual_path_only",
        "no_broker_action",
        "owner_approval",
        "production_boundary",
    }
    assert all(
        row["return_improvement_waiver_allowed"] is False for row in payload["hard_research_gates"]
    )

    defensive_gate = gate_rows["no_major_regression_in_defensive_probe"]
    assert defensive_gate["gate_layer"] == "strong_performance_gate"
    assert defensive_gate["failure_action"] == "BLOCKED"
    assert defensive_gate["v2_recommended_action"] == "keep_as_strong_performance_gate"
    assert defensive_gate["return_improvement_waiver_allowed"] is False

    market_window_gate = gate_rows["not_2023_plus_only"]
    assert market_window_gate["gate_layer"] == "owner_review_risk_flag"
    assert market_window_gate["failure_action"] == "OWNER_REVIEW_REQUIRED"
    assert market_window_gate["binary_block_allowed"] is False
    assert market_window_gate["offline_review_allowed_after_failure"] is True
    assert market_window_gate["risk_flag_retained"] is True

    beta_gate = gate_rows["not_beta_dependency"]
    assert beta_gate["gate_layer"] == "inconclusive_diagnostic_gate"
    assert beta_gate["failure_action"] == "DIAGNOSTIC_ONLY"
    assert beta_gate["exposure_attribution_required"] is True
    assert "tqqq_beta_share" in beta_gate["exposure_attribution_fields"]
    assert gate_rows["not_tqqq_dependency"]["binary_block_allowed"] is False

    threshold_gate = gate_rows["probability_threshold_0_55"]
    assert threshold_gate["gate_layer"] == "threshold_sensitivity_gate"
    assert threshold_gate["hard_threshold_allowed"] is False
    assert payload["threshold_sensitivity_artifact"]["probability_thresholds"] == [
        "0.55",
        "0.60",
    ]

    slice_gate = gate_rows["all_slices_not_worse"]
    assert slice_gate["gate_layer"] == "slice_review_gate"
    assert slice_gate["severe_slice_regression_action"] == "BLOCKED"
    assert slice_gate["minor_slice_regression_action"] == "OWNER_REVIEW_REQUIRED"
    assert slice_gate["slice_tradeoff_summary_required"] is True

    active_selection_plan = payload["active_selection_rule_audit_plan"]
    assert active_selection_plan["current_active_selection_accept_count"] == 0
    assert active_selection_plan["gate_policy_v2_auto_promotion_allowed"] is False
    assert active_selection_plan["ablation_modes"] == [
        "no_active_selection",
        "relaxed_active_selection",
        "current_active_selection",
        "strict_active_selection",
    ]
    assert "best_rejected_candidate_utility" in active_selection_plan["comparison_metrics"]

    for key in (
        "recommended_gate_policy_v2",
        "gate_policy_v2_reconciliation_report",
        "owner_review_gate_semantics",
        "active_selection_rule_audit_plan",
    ):
        assert Path(artifact_paths[key]).exists()

    recommended = safe_load_yaml_path(Path(artifact_paths["recommended_gate_policy_v2"]))
    assert isinstance(recommended, dict)
    assert recommended["safety_boundary"]["promotion_allowed"] is False
    assert recommended["safety_boundary"]["paper_shadow_allowed"] is False
    assert recommended["safety_boundary"]["production_allowed"] is False
    assert recommended["safety_boundary"]["broker_action"] == "none"


def test_first_layer_gate_policy_v2_reconciliation_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-gate-policy-v2-reconciliation" in result.output
