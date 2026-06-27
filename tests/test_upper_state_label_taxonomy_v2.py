from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.upper_state_label_feature_reset import (
    DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH,
    DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH,
    DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH,
    DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH,
    build_upper_state_labels_v2,
    first_layer_predictions_contain_weights,
    target_path_metrics_can_pass_first_layer_gate,
    upper_state_label_rows_have_window_metadata,
    validate_upper_state_label_taxonomy_v2,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_upper_state_taxonomy_v2_contract_passes() -> None:
    taxonomy = _load_yaml(DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH)

    validation = validate_upper_state_label_taxonomy_v2(taxonomy)

    assert validation["status"] == "PASS"
    assert taxonomy["label_definitions"]["high_confidence_risk_on"]["label_role"] == (
        "research_only_diagnostic"
    )
    assert taxonomy["target_path_metrics_can_pass_gate"] is False


def test_do_not_de_risk_label_is_generated() -> None:
    labels = build_upper_state_labels_v2(
        action_value=_synthetic_action_value(do_not_de_risk=0.004),
        taxonomy=_load_yaml(DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH),
        score_policy=_load_yaml(DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH),
    )

    horizon20 = labels.loc[labels["horizon_days"] == 20]
    assert horizon20["do_not_de_risk_label"].any()
    assert upper_state_label_rows_have_window_metadata(labels) is True


def test_add_risk_is_distinct_from_stay_constructive() -> None:
    labels = build_upper_state_labels_v2(
        action_value=_synthetic_action_value(stay_constructive=0.0002, add_risk=0.006),
        taxonomy=_load_yaml(DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH),
        score_policy=_load_yaml(DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH),
    )
    row = labels.loc[labels["horizon_days"] == 20].iloc[0]

    assert bool(row["stay_constructive_label"]) is False
    assert bool(row["add_risk_label"]) is True
    assert bool(row["add_risk_distinct_from_stay_constructive"]) is True


def test_high_confidence_risk_on_is_research_only() -> None:
    labels = build_upper_state_labels_v2(
        action_value=_synthetic_action_value(risk_on=0.007),
        taxonomy=_load_yaml(DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH),
        score_policy=_load_yaml(DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH),
    )

    assert set(labels["high_confidence_risk_on_usage"]) == {"research_only_diagnostic"}
    assert labels["promotion_allowed"].eq(False).all()
    assert labels["paper_shadow_allowed"].eq(False).all()
    assert labels["production_allowed"].eq(False).all()
    assert labels["broker_action"].eq("none").all()


def test_first_layer_model_output_contract_has_no_weights() -> None:
    composer = _load_yaml(DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH)
    predictions = pd.DataFrame(
        [
            {
                "date": "2021-02-22",
                "model_id": "first_layer_composer_v2",
                "trend_state": "constructive",
                "confidence": 0.7,
                "validity_days": 15,
                "decay_profile": "medium",
            }
        ]
    )

    assert first_layer_predictions_contain_weights(predictions) is False
    assert "weight" in composer["output_contract"]["forbidden_columns"]
    assert "QQQ" in composer["output_contract"]["forbidden_columns"]


def test_upper_state_label_requires_window_metadata() -> None:
    labels = build_upper_state_labels_v2(
        action_value=_synthetic_action_value(),
        taxonomy=_load_yaml(DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH),
        score_policy=_load_yaml(DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH),
    )
    missing_window = labels.drop(columns=["research_window_id"])

    assert upper_state_label_rows_have_window_metadata(labels) is True
    assert upper_state_label_rows_have_window_metadata(missing_window) is False


def test_target_path_metrics_cannot_pass_first_layer_gate() -> None:
    taxonomy = _load_yaml(DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH)
    threshold_policy = _load_yaml(DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH)

    assert (
        target_path_metrics_can_pass_first_layer_gate({}, taxonomy, threshold_policy)
        is False
    )
    unsafe = dict(taxonomy)
    unsafe["target_path_metrics_can_pass_gate"] = True
    assert target_path_metrics_can_pass_first_layer_gate({}, unsafe, threshold_policy) is True


def _synthetic_action_value(
    *,
    do_not_de_risk: float = 0.004,
    stay_constructive: float = 0.003,
    add_risk: float = 0.004,
    risk_on: float = 0.001,
) -> pd.DataFrame:
    rows = []
    for probe_id in ("balanced_dynamic_probe", "drawdown_control_probe", "risk_on_probe"):
        rows.append(
            {
                "research_window_id": "exact_three_asset_validated",
                "requested_start": "2021-02-22",
                "actual_start": "2021-02-22",
                "actual_portfolio_start": "2021-02-22",
                "end": "latest",
                "window_role": "primary_validated",
                "data_quality_contract": "secondary_cross_checked",
                "exact_or_proxy": "exact",
                "date": "2024-01-02",
                "horizon_days": 20,
                "probe_id": probe_id,
                "do_not_de_risk_score": do_not_de_risk,
                "stay_constructive_score": stay_constructive,
                "add_risk_score": add_risk,
                "risk_on_diagnostic_score": risk_on,
            }
        )
    return pd.DataFrame(rows)


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
