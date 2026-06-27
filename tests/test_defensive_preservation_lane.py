from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_PATH = Path("config/research/defensive_preservation_lane_policy.yaml")
LABEL_TAXONOMY_PATH = Path("config/research/defensive_lane_label_taxonomy.yaml")
ACTION_VALUE_POLICY_PATH = Path("config/research/defensive_lane_action_value_policy.yaml")
FEATURE_AUDIT_PATH = Path("inputs/research_reviews/defensive_lane_feature_pit_audit.yaml")
MODEL_MATRIX_PATH = Path("inputs/research_reviews/defensive_lane_model_matrix.yaml")
ACTUAL_PATH = Path("inputs/research_reviews/defensive_lane_actual_path_matrix.yaml")
FINAL_MATRIX_PATH = Path("inputs/research_reviews/defensive_preservation_lane_final_matrix.yaml")


def test_defensive_lane_cli_is_registered() -> None:
    result = CliRunner().invoke(app, ["research", "trends", "--help"])

    assert result.exit_code == 0
    assert "defensive-lane" in result.output


def test_defensive_lane_policy_blocks_add_risk_and_tqqq() -> None:
    policy = _load_yaml(POLICY_PATH)
    label_taxonomy = _load_yaml(LABEL_TAXONOMY_PATH)
    action_policy = _load_yaml(ACTION_VALUE_POLICY_PATH)

    assert "add_risk" in policy["disallowed_outputs"]
    assert "high_confidence_risk_on" in policy["disallowed_outputs"]
    assert "tqqq_related_signal" in policy["blocked_features"]
    assert "add_risk" in label_taxonomy["blocked_labels"]
    assert "high_confidence_risk_on" in label_taxonomy["blocked_labels"]
    assert "add_risk_reward" in action_policy["excluded_components"]
    assert "tqqq_beta_reward" in action_policy["excluded_components"]
    assert policy["safety_boundary"]["promotion_allowed"] is False
    assert policy["safety_boundary"]["paper_shadow_allowed"] is False
    assert policy["safety_boundary"]["production_allowed"] is False
    assert policy["safety_boundary"]["broker_action"] == "none"


def test_defensive_lane_feature_matrix_is_pit_and_defensive_only() -> None:
    feature_audit = _load_yaml(FEATURE_AUDIT_PATH)
    columns = set(feature_audit["feature_matrix_columns"])

    required_columns = {
        "known_at",
        "available_at",
        "decision_at",
        "feature_cutoff_passed",
        "pit_status",
    }
    assert required_columns.issubset(columns)
    assert feature_audit["summary"]["feature_row_count"] > 0
    assert feature_audit["summary"]["feature_cutoff_passed"] is True
    assert _contains_no_banned_columns(columns)
    assert feature_audit["summary"]["approved_feature_count"] > 0
    assert feature_audit["summary"]["blocked_feature_count"] > 0
    assert feature_audit["summary"]["tqqq_feature_count"] == 0
    assert feature_audit["summary"]["add_risk_feature_count"] == 0


def test_defensive_lane_labels_do_not_include_add_risk_reward() -> None:
    model = _load_yaml(MODEL_MATRIX_PATH)
    columns = set(model["label_columns"])
    contract = model["label_contract"]

    expected_labels = {
        "risk_off_needed",
        "defensive_hold_needed",
        "do_not_de_risk",
        "re_risk_allowed_but_not_add_risk",
    }
    assert expected_labels.issubset(columns)
    assert model["summary"]["label_count"] > 0
    assert contract["add_risk_label_allowed_any"] is False
    assert contract["high_confidence_risk_on_label_allowed_any"] is False
    assert "add_risk" not in columns
    assert "high_confidence_risk_on" not in columns


def test_defensive_lane_predictions_are_defensive_only() -> None:
    model = _load_yaml(MODEL_MATRIX_PATH)
    columns = set(model["prediction_columns"])
    contract = model["prediction_contract"]

    assert set(contract["observed_trend_states"]).issubset({"risk_off", "defensive", "neutral"})
    assert contract["add_risk_probability_max"] == 0.0
    assert contract["high_confidence_risk_on_probability_max"] == 0.0
    assert contract["tqqq_signal_allowed_any"] is False
    assert _contains_no_banned_columns(
        column
        for column in columns
        if column
        not in {
            "add_risk_probability",
            "high_confidence_risk_on_probability",
            "tqqq_signal_allowed",
        }
    )


def test_defensive_lane_actual_path_only_uses_defensive_probes_and_reference() -> None:
    actual_path = _load_yaml(ACTUAL_PATH)
    rows = actual_path["probe_rows"]
    modeled = [row for row in rows if row["probe_id"] != "limited_adjustment_reference"]

    assert {row["probe_id"] for row in modeled} == {
        "defensive_overlay_probe",
        "drawdown_control_probe",
    }
    assert all(row["lane_role"] == "defensive_preservation" for row in modeled)
    assert all(row["tqqq_max_weight"] == 0.0 for row in modeled)
    assert actual_path["summary"]["add_risk_used"] is False
    assert actual_path["summary"]["risk_on_used"] is False
    assert actual_path["summary"]["tqqq_signal_used"] is False
    assert actual_path["promotion_allowed"] is False
    assert actual_path["paper_shadow_allowed"] is False
    assert actual_path["production_allowed"] is False
    assert actual_path["broker_action"] == "none"


def test_defensive_lane_final_matrix_blocks_gated_integration() -> None:
    final = _load_yaml(FINAL_MATRIX_PATH)
    allowed_statuses = {
        "DEFENSIVE_LANE_IMPROVES",
        "DEFENSIVE_LANE_NO_MATERIAL_IMPROVEMENT",
        "DEFENSIVE_LANE_RISK_OFF_ONLY_RETAINED",
        "DEFENSIVE_LANE_ARCHIVED",
    }

    assert final["status"] in allowed_statuses
    assert final["summary"]["gated_integration_allowed"] is False
    assert final["summary"]["add_risk_disabled"] is True
    assert final["summary"]["high_confidence_risk_on_disabled"] is True
    assert final["summary"]["tqqq_signal_disabled"] is True
    assert final["promotion_allowed"] is False
    assert final["paper_shadow_allowed"] is False
    assert final["production_allowed"] is False
    assert final["broker_action"] == "none"


def _contains_no_banned_columns(columns: object) -> bool:
    names = {str(column).lower() for column in columns}
    return not any("tqqq" in name or "add_risk" in name or "risk_on" in name for name in names)


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
