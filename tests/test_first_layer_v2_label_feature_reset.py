from __future__ import annotations

from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.upper_state_label_feature_reset import (
    DEFAULT_ALTERNATING_PROTOCOL_PATH,
    DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH,
    DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH,
    DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH,
    first_layer_predictions_contain_weights,
    target_path_metrics_can_pass_first_layer_gate,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

CONTRACT_PATH = Path("inputs/research_reviews/first_layer_v2_frozen_probe_contract.yaml")
COVERAGE_PATH = Path("inputs/research_reviews/first_layer_v2_effective_coverage_audit.yaml")
LABEL_SUMMARY_PATH = Path("inputs/research_reviews/upper_state_label_v2_summary.yaml")
FINAL_MATRIX_PATH = (
    Path("inputs/research_reviews") / "first_layer_v2_label_feature_model_final_matrix.yaml"
)
ACTUAL_PATH_MATRIX_PATH = (
    Path("inputs/research_reviews") / "first_layer_v2_frozen_probe_actual_path_matrix.yaml"
)


def test_first_layer_v2_uses_frozen_probe_registry_v2() -> None:
    contract = _load_yaml(CONTRACT_PATH)
    result = CliRunner().invoke(app, ["research", "trends", "first-layer-v2-reset", "--help"])

    assert result.exit_code == 0
    assert "first-layer-v2-reset" in result.output
    assert contract["summary"]["frozen_second_layer"] == "dynamic_second_layer_probe_registry_v2"
    assert contract["summary"]["registry_frozen"] is True
    assert contract["summary"]["second_layer_weight_changes_allowed"] is False
    assert contract["summary"]["probe_count"] == 8


def test_first_layer_v2_artifacts_have_primary_window_id() -> None:
    for path in (
        CONTRACT_PATH,
        COVERAGE_PATH,
        LABEL_SUMMARY_PATH,
        Path("inputs/research_reviews/first_layer_feature_pit_audit_v3.yaml"),
        Path("inputs/research_reviews/first_layer_walk_forward_matrix_v3.yaml"),
        ACTUAL_PATH_MATRIX_PATH,
        Path("inputs/research_reviews/first_layer_v2_failure_attribution.yaml"),
        FINAL_MATRIX_PATH,
    ):
        artifact = _load_yaml(path)
        assert artifact["research_window_id"] == "exact_three_asset_validated"
        assert artifact["requested_start"] == "2021-02-22"
        assert artifact["research_audit_metadata"]["modified_layer"] == "first_layer"
        assert (
            artifact["research_audit_metadata"]["frozen_second_layer_version"]
            == "dynamic_second_layer_probe_registry_v2"
        )
        assert artifact["promotion_allowed"] is False
        assert artifact["broker_action"] == "none"


def test_effective_coverage_audit_flags_late_prediction_start() -> None:
    audit = _load_yaml(COVERAGE_PATH)

    assert audit["status"] == "PRIMARY_WINDOW_COVERAGE_INCOMPLETE"
    assert audit["summary"]["requested_research_window_start"] == "2021-02-22"
    assert audit["summary"]["actual_label_start"] == "2021-02-22"
    assert audit["summary"]["actual_feature_start"] == "2021-02-22"
    assert audit["summary"]["actual_prediction_start"] > "2022-01-01"
    assert audit["summary"]["primary_window_coverage_incomplete"] is True
    assert audit["summary"]["covers_2021_predictions"] is False
    assert audit["summary"]["covers_2022_predictions"] is False


def test_do_not_de_risk_label_is_generated() -> None:
    summary = _load_yaml(LABEL_SUMMARY_PATH)
    rows = {row["label_id"]: row for row in summary["label_rows"]}

    assert rows["do_not_de_risk"]["positive_count"] > 0
    assert rows["do_not_de_risk"]["positive_share"] > 0
    assert rows["do_not_de_risk"]["sample_status"] == "PASS"


def test_add_risk_label_is_distinct_from_stay_constructive() -> None:
    summary = _load_yaml(LABEL_SUMMARY_PATH)
    rows = {row["label_id"]: row for row in summary["label_rows"]}

    assert rows["add_risk"]["positive_count"] != rows["stay_constructive"]["positive_count"]
    assert rows["add_risk"]["positive_share"] != rows["stay_constructive"]["positive_share"]


def test_high_confidence_risk_on_is_research_only() -> None:
    summary = _load_yaml(LABEL_SUMMARY_PATH)
    final = _load_yaml(FINAL_MATRIX_PATH)
    rows = {row["label_id"]: row for row in summary["label_rows"]}

    assert rows["high_confidence_risk_on"]["research_only"] is True
    assert final["final_decision"]["high_confidence_risk_on"] == "diagnostic_only"
    assert final["paper_shadow_allowed"] is False
    assert final["production_allowed"] is False


def test_first_layer_model_does_not_output_weights() -> None:
    composer = _load_yaml(DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH)
    allowed_columns = composer["output_contract"]["allowed_columns"]
    forbidden_columns = set(composer["output_contract"]["forbidden_columns"])
    predictions = pd.DataFrame(columns=allowed_columns)

    assert first_layer_predictions_contain_weights(predictions) is False
    assert {"trend_state", "confidence", "validity_days", "decay_profile"} <= set(
        predictions.columns
    )
    assert {"QQQ", "SGOV", "TQQQ", "weight", "target_weight", "actual_weight"} <= forbidden_columns


def test_target_path_metrics_cannot_pass_first_layer_v2_gate() -> None:
    protocol = _load_yaml(DEFAULT_ALTERNATING_PROTOCOL_PATH)
    taxonomy = _load_yaml(DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH)
    threshold_policy = _load_yaml(DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH)
    final = _load_yaml(FINAL_MATRIX_PATH)
    actual_path = _load_yaml(ACTUAL_PATH_MATRIX_PATH)

    assert (
        target_path_metrics_can_pass_first_layer_gate(protocol, taxonomy, threshold_policy)
        is False
    )
    assert final["final_decision"]["target_path_metrics_can_pass"] is False
    assert actual_path["summary"]["target_path_metrics_used_for_pass"] is False


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
