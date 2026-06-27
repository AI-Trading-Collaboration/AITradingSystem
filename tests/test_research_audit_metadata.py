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
