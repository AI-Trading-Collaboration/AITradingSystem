from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.research_audit_metadata import (
    load_research_audit_metadata_schema,
    validate_research_audit_metadata,
)
from ai_trading_system.second_layer_probe_library_freeze import DEFAULT_PROBE_REGISTRY_V2_PATH
from ai_trading_system.yaml_loader import safe_load_yaml_path

RECLASSIFICATION_PATH = Path(
    "inputs/research_reviews/first_layer_v2_coverage_rebuild_reclassification.yaml"
)
INVENTORY_PATH = Path(
    "inputs/research_reviews/first_layer_v2_defensive_probe_regression_inventory.yaml"
)
ROLE_GROUP_PATH = Path("inputs/research_reviews/first_layer_v2_probe_role_group_matrix.yaml")
SIGNAL_ATTRIBUTION_PATH = Path(
    "inputs/research_reviews/first_layer_v2_signal_error_attribution.yaml"
)
RETURN_SEEKING_PATH = Path(
    "inputs/research_reviews/first_layer_v2_return_seeking_diagnostic_reclassification.yaml"
)
RISK_OFF_ONLY_PATH = Path(
    "inputs/research_reviews/first_layer_v2_risk_off_only_fallback_assessment.yaml"
)
DIAGNOSIS_PATH = Path(
    "inputs/research_reviews/first_layer_v2_defensive_regression_diagnosis_matrix.yaml"
)
FINAL_PATH = Path(
    "inputs/research_reviews/first_layer_v2_defensive_regression_diagnosis_final_matrix.yaml"
)

EXPECTED_COVERAGE_PASS_POLICIES = {"wf_252d_initial", "wf_expanding_initial"}


def test_coverage_pass_variants_are_required_for_selection() -> None:
    reclassification = _load_yaml(RECLASSIFICATION_PATH)
    final = _load_yaml(FINAL_PATH)
    result = CliRunner().invoke(
        app,
        ["research", "trends", "--help"],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert "first-layer-defensive-regression-diagnosis" in result.output
    assert set(reclassification["summary"]["coverage_pass_policy_ids"]) == (
        EXPECTED_COVERAGE_PASS_POLICIES
    )
    assert reclassification["summary"]["coverage_pass_selection"] is False
    assert final["summary"]["coverage_pass_policy_ids"] == [
        "wf_252d_initial",
        "wf_expanding_initial",
    ]


def test_late_window_improvement_cannot_enable_owner_review() -> None:
    reclassification = _load_yaml(RECLASSIFICATION_PATH)
    final = _load_yaml(FINAL_PATH)

    assert reclassification["summary"]["positive_late_window_evidence"] is True
    assert reclassification["summary"]["positive_late_window_policy_ids"] == [
        "wf_504d_baseline",
        "wf_378d_initial",
    ]
    assert reclassification["summary"]["owner_review_allowed"] is False
    assert final["final_decision"]["owner_review_allowed"] is False
    assert final["summary"]["promotion_status"] == "blocked"


def test_defensive_probe_regression_blocks_first_layer_candidate() -> None:
    inventory = _load_yaml(INVENTORY_PATH)
    role_group = _load_yaml(ROLE_GROUP_PATH)
    risk_off_only = _load_yaml(RISK_OFF_ONLY_PATH)

    regressed = {
        (row["policy_id"], row["probe_id"])
        for row in inventory["probe_rows"]
        if row["coverage_pass"] and not row["improved_vs_flat"]
    }

    assert ("wf_252d_initial", "defensive_overlay_probe") in regressed
    assert ("wf_252d_initial", "drawdown_control_probe") in regressed
    assert ("wf_expanding_initial", "defensive_overlay_probe") in regressed
    assert ("wf_expanding_initial", "drawdown_control_probe") in regressed
    assert role_group["summary"]["defensive_or_drawdown_regressed_count"] == 5
    assert risk_off_only["status"] == "RISK_OFF_ONLY_FALLBACK_NOT_SUPPORTED"


def test_return_seeking_diagnostic_cannot_enable_promotion() -> None:
    role_group = _load_yaml(ROLE_GROUP_PATH)
    return_seeking = _load_yaml(RETURN_SEEKING_PATH)
    final = _load_yaml(FINAL_PATH)

    assert role_group["summary"]["pure_return_seeking_improved_count"] == (
        role_group["summary"]["pure_return_seeking_probe_count"]
    )
    assert return_seeking["status"] == "RETURN_SEEKING_ONLY_BUT_DEFENSIVE_BLOCKED"
    assert return_seeking["summary"]["owner_review_allowed"] is False
    assert return_seeking["promotion_allowed"] is False
    assert final["status"] == "FIRST_LAYER_V2_RETURN_SEEKING_DIAGNOSTIC_ONLY"
    assert final["promotion_allowed"] is False
    assert final["paper_shadow_allowed"] is False
    assert final["production_allowed"] is False
    assert final["broker_action"] == "none"


def test_regression_diagnosis_matrix_has_final_diagnosis() -> None:
    diagnosis = _load_yaml(DIAGNOSIS_PATH)
    signal = _load_yaml(SIGNAL_ATTRIBUTION_PATH)
    final = _load_yaml(FINAL_PATH)

    assert diagnosis["summary"]["final_diagnosis"] == "RETURN_SEEKING_ONLY_DIAGNOSTIC"
    assert signal["summary"]["primary_signal_error_diagnosis"] in {
        "DEFENSIVE_REGRESSION_DUE_TO_FALSE_ADD_RISK",
        "DEFENSIVE_REGRESSION_DUE_TO_FALSE_DO_NOT_DERISK",
        "DEFENSIVE_REGRESSION_DUE_TO_EARLY_RE_RISK",
    }
    assert final["summary"]["final_diagnosis"] == "RETURN_SEEKING_ONLY_DIAGNOSTIC"
    assert "DEFENSIVE_PROBE_REGRESSION" in final["summary"]["remaining_blockers"]


def test_second_layer_registry_remains_frozen() -> None:
    schema = load_research_audit_metadata_schema()
    registry = _load_yaml(Path(DEFAULT_PROBE_REGISTRY_V2_PATH))
    final = _load_yaml(FINAL_PATH)
    diagnosis = _load_yaml(DIAGNOSIS_PATH)

    assert registry["policy_id"] == "dynamic_second_layer_probe_registry_v2"
    assert registry["status"] == "frozen_research_baseline"
    assert len(registry["probes"]) == 8
    for artifact in (final, diagnosis):
        metadata = artifact["research_audit_metadata"]
        assert validate_research_audit_metadata(artifact, schema)["status"] == "PASS"
        assert metadata["modified_layer"] == "first_layer"
        assert metadata["frozen_second_layer_version"] == "dynamic_second_layer_probe_registry_v2"
        assert metadata["probe_registry_version"] == "dynamic_second_layer_probe_registry_v2"
        assert artifact["promotion_allowed"] is False
        assert artifact["broker_action"] == "none"


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
