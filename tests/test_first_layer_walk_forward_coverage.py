from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.second_layer_probe_library_freeze import DEFAULT_PROBE_REGISTRY_V2_PATH
from ai_trading_system.yaml_loader import safe_load_yaml_path

COVERAGE_POLICY_PATH = Path("config/research/first_layer_walk_forward_coverage_policy_v2.yaml")
OPTIONALIZATION_POLICY_PATH = Path(
    "config/research/first_layer_feature_optionalization_policy.yaml"
)
SELECTION_RULE_PATH = Path("config/research/first_layer_v2_coverage_aware_selection_rule.yaml")
SIMULATION_PATH = Path(
    "inputs/research_reviews/first_layer_walk_forward_coverage_simulation_matrix.yaml"
)
EARLY_FEATURE_PATH = Path(
    "inputs/research_reviews/first_layer_v2_early_feature_coverage_audit.yaml"
)
ACTUAL_PATH = Path(
    "inputs/research_reviews/first_layer_v2_coverage_policy_actual_path_matrix.yaml"
)
SLICE_PATH = Path("inputs/research_reviews/first_layer_v2_2022_slice_matrix.yaml")
FAILURE_PATH = Path(
    "inputs/research_reviews/first_layer_v2_coverage_rebuild_failure_attribution.yaml"
)
FINAL_PATH = Path(
    "inputs/research_reviews/first_layer_v2_walk_forward_coverage_rebuild_final_matrix.yaml"
)

EXPECTED_VARIANTS = {
    "wf_504d_baseline",
    "wf_378d_initial",
    "wf_252d_initial",
    "wf_expanding_initial",
    "wf_warm_start_diagnostic",
}


def test_coverage_policy_variants_are_registered() -> None:
    policy = _load_yaml(COVERAGE_POLICY_PATH)
    result = CliRunner().invoke(
        app,
        ["research", "trends", "--help"],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert "first-layer-coverage-rebuild" in result.output
    assert set(policy["variants"]) == EXPECTED_VARIANTS
    assert policy["coverage_pass_rule"]["required_prediction_effective_start_on_or_before"] == (
        "2022-03-01"
    )
    assert policy["safety_boundary"]["broker_action"] == "none"


def test_coverage_simulation_reports_first_prediction_date() -> None:
    simulation = _load_yaml(SIMULATION_PATH)
    rows = _rows_by_policy(simulation)

    assert simulation["summary"]["coverage_pass_count"] == 2
    assert rows["wf_504d_baseline"]["first_prediction_date"] == "2023-02-22"
    assert rows["wf_378d_initial"]["first_prediction_date"] == "2022-08-22"
    assert rows["wf_252d_initial"]["first_prediction_date"] == "2022-02-18"
    assert rows["wf_expanding_initial"]["first_prediction_date"] == "2022-02-18"
    assert rows["wf_warm_start_diagnostic"]["first_prediction_date"] == "2021-02-22"
    assert rows["wf_252d_initial"]["covered_2022"] is True
    assert rows["wf_expanding_initial"]["covered_2022_risk_off_window"] is True


def test_coverage_pass_rule_blocks_2023_only_predictions() -> None:
    rows = _rows_by_policy(_load_yaml(SIMULATION_PATH))

    assert rows["wf_504d_baseline"]["does_coverage_pass_rule"] is False
    assert (
        rows["wf_504d_baseline"]["coverage_block_reason"]
        == "FIRST_PREDICTION_AFTER_REQUIRED_2022_03_01"
    )
    assert rows["wf_378d_initial"]["does_coverage_pass_rule"] is False
    assert rows["wf_252d_initial"]["does_coverage_pass_rule"] is True
    assert rows["wf_expanding_initial"]["does_coverage_pass_rule"] is True
    assert rows["wf_warm_start_diagnostic"]["coverage_block_reason"] == (
        "DIAGNOSTIC_ONLY_WARM_START"
    )


def test_second_layer_registry_remains_frozen_during_coverage_rebuild() -> None:
    registry = _load_yaml(Path(DEFAULT_PROBE_REGISTRY_V2_PATH))
    final = _load_yaml(FINAL_PATH)
    actual = _load_yaml(ACTUAL_PATH)

    assert final["final_decision"]["second_layer_registry"] == (
        "dynamic_second_layer_probe_registry_v2"
    )
    assert final["research_audit_metadata"]["frozen_second_layer_version"] == (
        "dynamic_second_layer_probe_registry_v2"
    )
    assert len(registry["probes"]) == 8
    assert {row["probe_count"] for row in actual["policy_rows"]} == {8}
    assert actual["summary"]["target_path_metrics_used_for_pass"] is False


def test_early_feature_optionalization_does_not_use_future_data() -> None:
    policy = _load_yaml(OPTIONALIZATION_POLICY_PATH)
    audit = _load_yaml(EARLY_FEATURE_PATH)

    assert policy["pit_rules"]["future_data_fill_allowed"] is False
    assert audit["summary"]["future_data_fill_allowed"] is False
    assert audit["summary"]["blocking_core_feature_count"] == 0
    assert {row["future_data_fill_allowed"] for row in audit["feature_rows"]} == {False}
    assert all(
        row["optionalization_allowed"] is False
        for row in audit["feature_rows"]
        if row["feature_role"] == "core"
    )


def test_2022_slice_required_for_owner_escalation() -> None:
    selection_rule = _load_yaml(SELECTION_RULE_PATH)
    actual = _load_yaml(ACTUAL_PATH)
    slice_review = _load_yaml(SLICE_PATH)
    failure = _load_yaml(FAILURE_PATH)
    final = _load_yaml(FINAL_PATH)
    actual_rows = _rows_by_policy(actual)
    slice_rows = _rows_by_policy(slice_review)

    assert (
        selection_rule["selection_conditions"]["2022_slice_not_worse_than_flat_reference"]
        is True
    )
    assert final["summary"]["coverage_pass_variant_count"] == 2
    assert final["summary"]["coverage_aware_selected_policy"] == ""
    assert final["final_decision"]["owner_review_allowed"] is False
    assert failure["summary"]["primary_reason"] == "DEFENSIVE_PROBE_REGRESSION"
    for policy_id in ("wf_252d_initial", "wf_expanding_initial"):
        assert actual_rows[policy_id]["does_coverage_pass_rule"] is True
        assert actual_rows[policy_id]["2022_slice_not_worse_than_flat_reference"] is True
        assert slice_rows[policy_id]["2022_slice_not_worse_than_flat_reference"] is True
        assert actual_rows[policy_id]["no_major_regression_in_defensive_probe"] is False
        assert actual_rows[policy_id]["coverage_aware_selection_pass"] is False


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw


def _rows_by_policy(payload: dict[str, object]) -> dict[str, dict[str, object]]:
    rows = payload["policy_rows"]
    assert isinstance(rows, list)
    return {str(row["policy_id"]): row for row in rows}
