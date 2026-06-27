from __future__ import annotations

from pathlib import Path

from ai_trading_system.research_audit_metadata import (
    load_research_audit_metadata_schema,
    load_window_aware_selection_rule_templates,
    post_1665_artifact_requires_research_audit_metadata,
    validate_research_audit_metadata,
    validate_window_aware_selection_rule_templates,
    window_extension_reveals_legacy_overfit_blocks_promotion,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_post_1665_artifact_requires_research_audit_metadata() -> None:
    schema = load_research_audit_metadata_schema()
    artifact = _artifact()

    validation = validate_research_audit_metadata(artifact, schema)

    assert validation["status"] == "PASS"
    assert post_1665_artifact_requires_research_audit_metadata(artifact, schema) is True


def test_missing_research_audit_metadata_fails() -> None:
    schema = load_research_audit_metadata_schema()
    artifact = _artifact()
    artifact.pop("research_audit_metadata")

    validation = validate_research_audit_metadata(artifact, schema)

    assert validation["status"] == "FAIL"
    assert validation["issues"][0]["code"] == "research_audit_metadata_missing"


def test_invalid_modified_layer_fails() -> None:
    schema = load_research_audit_metadata_schema()
    artifact = _artifact()
    artifact["research_audit_metadata"]["modified_layer"] = "both_layers"

    validation = validate_research_audit_metadata(artifact, schema)

    assert validation["status"] == "FAIL"
    assert any(issue["code"] == "modified_layer_invalid" for issue in validation["issues"])


def test_window_aware_selection_rule_templates_are_complete() -> None:
    templates = load_window_aware_selection_rule_templates()

    validation = validate_window_aware_selection_rule_templates(templates)

    assert validation["status"] == "PASS"
    assert templates["templates"]["first_layer_calibration"]["primary_window_id"] == (
        "exact_three_asset_validated"
    )
    assert "target_path_return" in templates["templates"]["first_layer_calibration"][
        "forbidden_posthoc_metrics"
    ]


def test_adoption_closeout_artifact_has_audit_metadata() -> None:
    schema = load_research_audit_metadata_schema()
    artifact = _load_yaml(
        Path("inputs/research_reviews/research_window_extension_adoption_closeout.yaml")
    )

    assert validate_research_audit_metadata(artifact, schema)["status"] == "PASS"
    assert artifact["summary"]["adoption_status"] == "PRIMARY_WINDOW_ADOPTED_LEGACY_OVERFIT_BLOCKED"
    assert artifact["promotion_allowed"] is False


def test_final_matrix_records_legacy_overfit_promotion_blocker() -> None:
    schema = load_research_audit_metadata_schema()
    artifact = _load_yaml(
        Path("inputs/research_reviews/post_window_extension_research_discipline_final_matrix.yaml")
    )

    assert validate_research_audit_metadata(artifact, schema)["status"] == "PASS"
    assert artifact["status"] == "PRIMARY_WINDOW_ADOPTED_LEGACY_OVERFIT_BLOCKED"
    assert artifact["summary"]["legacy_overfit_blocker"] is True
    assert window_extension_reveals_legacy_overfit_blocks_promotion(
        {
            "status": "WINDOW_EXTENSION_REVEALS_LEGACY_OVERFIT_PROMOTION_BLOCKED",
            "promotion_allowed": False,
        }
    )


def test_second_layer_probe_freeze_artifacts_have_audit_metadata() -> None:
    schema = load_research_audit_metadata_schema()
    paths = [
        Path("inputs/research_reviews/second_layer_probe_exposure_matrix_v2.yaml"),
        Path("inputs/research_reviews/second_layer_probe_actual_path_matrix_v2.yaml"),
        Path("inputs/research_reviews/second_layer_probe_same_risk_frontier_matrix_v2.yaml"),
        Path("inputs/research_reviews/second_layer_probe_tqqq_stress_matrix_v2.yaml"),
        Path("inputs/research_reviews/second_layer_action_value_probe_readiness_v2.yaml"),
        Path("inputs/research_reviews/second_layer_probe_library_freeze_final_matrix.yaml"),
    ]

    for path in paths:
        artifact = _load_yaml(path)
        metadata = artifact["research_audit_metadata"]

        assert validate_research_audit_metadata(artifact, schema)["status"] == "PASS"
        assert metadata["modified_layer"] == "second_layer"
        assert metadata["probe_registry_version"] == "dynamic_second_layer_probe_registry_v2"
        assert artifact["research_window_id"] == "exact_three_asset_validated"
        assert artifact["promotion_allowed"] is False


def test_second_layer_probe_freeze_final_matrix_counts() -> None:
    artifact = _load_yaml(
        Path("inputs/research_reviews/second_layer_probe_library_freeze_final_matrix.yaml")
    )

    assert artifact["status"] == "SECOND_LAYER_RETURN_SEEKING_PROBES_DIAGNOSTIC_ONLY"
    assert artifact["summary"]["probe_count"] == 8
    assert artifact["summary"]["approved_action_value_probe_count"] == 7
    assert artifact["summary"]["diagnostic_only_probe_count"] == 1
    assert artifact["summary"]["tqqq_stress_blocked_count"] == 0


def test_first_layer_v2_reset_artifacts_have_audit_metadata() -> None:
    schema = load_research_audit_metadata_schema()
    paths = [
        Path("inputs/research_reviews/first_layer_v2_frozen_probe_contract.yaml"),
        Path("inputs/research_reviews/first_layer_v2_effective_coverage_audit.yaml"),
        Path("inputs/research_reviews/upper_state_label_v2_summary.yaml"),
        Path("inputs/research_reviews/first_layer_feature_pit_audit_v3.yaml"),
        Path("inputs/research_reviews/first_layer_walk_forward_matrix_v3.yaml"),
        Path("inputs/research_reviews/first_layer_v2_frozen_probe_actual_path_matrix.yaml"),
        Path("inputs/research_reviews/first_layer_v2_failure_attribution.yaml"),
        Path("inputs/research_reviews/first_layer_v2_label_feature_model_final_matrix.yaml"),
    ]

    for path in paths:
        artifact = _load_yaml(path)
        metadata = artifact["research_audit_metadata"]

        assert validate_research_audit_metadata(artifact, schema)["status"] == "PASS"
        assert metadata["modified_layer"] == "first_layer"
        assert metadata["frozen_second_layer_version"] == "dynamic_second_layer_probe_registry_v2"
        assert metadata["probe_registry_version"] == "dynamic_second_layer_probe_registry_v2"
        assert artifact["research_window_id"] == "exact_three_asset_validated"
        assert artifact["promotion_allowed"] is False


def test_first_layer_v2_final_matrix_records_coverage_blocker() -> None:
    artifact = _load_yaml(
        Path("inputs/research_reviews/first_layer_v2_label_feature_model_final_matrix.yaml")
    )

    assert artifact["status"] == "WINDOW_COVERAGE_INCOMPLETE"
    assert artifact["summary"]["actual_prediction_start"] == "2023-02-22"
    assert artifact["summary"]["primary_failure_reason"] == "WINDOW_COVERAGE_INCOMPLETE"
    assert (
        artifact["final_decision"]["next_action"]
        == "REBUILD_WALK_FORWARD_COVERAGE_BEFORE_OWNER_ESCALATION"
    )


def _artifact() -> dict[str, object]:
    return {
        "research_window_id": "exact_three_asset_validated",
        "requested_start": "2021-02-22",
        "actual_portfolio_start": "2021-02-22",
        "window_role": "primary_validated",
        "data_quality_contract": "secondary_cross_checked",
        "research_audit_metadata": {
            "modified_layer": "validation_only",
            "frozen_first_layer_version": "frozen_or_not_applicable",
            "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v1",
            "research_window_id": "exact_three_asset_validated",
            "label_version": "labels_v1",
            "feature_set_version": "features_v1",
            "model_version": "model_v1",
            "threshold_policy": "primary_research_window_policy_v1",
            "probe_registry_version": "dynamic_second_layer_probe_registry_v1",
            "candidate_count": 0,
            "pre_registered_selection_rule": "unit_test_rule",
        },
    }


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
