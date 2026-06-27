from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.first_layer_up_state_learning import (
    DEFAULT_HIERARCHICAL_CONFIG_PATH,
    DEFAULT_THRESHOLD_POLICY_PATH,
    UP_STATE_FEATURE_COLUMNS,
    build_hierarchical_trend_labels,
    run_hierarchical_walk_forward,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_upper_state_labels_are_generated() -> None:
    labels = build_hierarchical_trend_labels(_synthetic_consensus_labels())

    assert labels["upper_state_binary_label"].sum() > 0
    assert labels["risk_off_binary_label"].sum() > 0
    assert {"downside_zone", "middle_zone", "upside_zone"} <= set(labels["three_zone_label"])
    assert set(labels.loc[labels["consensus_state"] == "risk_on", "risk_on_severity_label"]) == {2}


def test_upper_state_detector_predicts_nonzero_when_validation_has_upper_state() -> None:
    result = _run_synthetic_hierarchical_model()

    assert result["metrics"]["true_upper_state_count"] > 0
    assert result["metrics"]["predicted_upper_state_count"] > 0
    assert result["metrics"]["upper_state_collapse_flag"] is False


def test_hierarchical_model_does_not_collapse_to_only_risk_off_defensive() -> None:
    result = _run_synthetic_hierarchical_model()
    predictions = result["predictions"]

    assert set(predictions["trend_state"]) - {"risk_off", "defensive"}
    assert result["metrics"]["predicted_constructive_count"] > 0


def test_constructive_and_risk_on_are_not_silently_absent() -> None:
    result = _run_synthetic_hierarchical_model()

    assert result["metrics"]["predicted_constructive_count"] > 0
    assert result["metrics"]["predicted_risk_on_count"] > 0
    assert result["metrics"]["risk_on_severity_status"] == "RISK_ON_SEVERITY_SCALER_READY"


def test_probe_roles_include_return_seeking_metadata() -> None:
    registry = _load_yaml(Path("config/research/dynamic_second_layer_probe_registry.yaml"))
    return_seeking = [probe for probe in registry["probes"] if bool(probe.get("return_seeking"))]

    assert {probe["probe_id"] for probe in return_seeking} == {
        "balanced_dynamic_probe",
        "drawdown_control_probe",
        "risk_on_diagnostic_probe",
    }
    assert all("return_seeking" in probe.get("role_tags", []) for probe in return_seeking)
    assert all(probe["research_only"] is True for probe in return_seeking)
    assert all(probe["promotion_enabled"] is False for probe in return_seeking)
    assert all(probe["broker_enabled"] is False for probe in return_seeking)


def _run_synthetic_hierarchical_model() -> dict[str, object]:
    labels = build_hierarchical_trend_labels(_synthetic_consensus_labels())
    features = _synthetic_features(labels)
    scope = {
        "walk_forward": {
            "train_window_days": 60,
            "validation_window_days": 20,
            "step_days": 20,
            "min_train_samples": 40,
            "label_horizon_days": 20,
        }
    }
    threshold_policy = _load_yaml(DEFAULT_THRESHOLD_POLICY_PATH)
    threshold_policy["risk_on_severity"]["min_upper_state_samples"] = 5
    hierarchical_config = _load_yaml(DEFAULT_HIERARCHICAL_CONFIG_PATH)
    return run_hierarchical_walk_forward(
        feature_matrix=features,
        hierarchical_labels=labels,
        scope_config=scope,
        threshold_policy=threshold_policy,
        hierarchical_config=hierarchical_config,
    )


def _synthetic_consensus_labels() -> pd.DataFrame:
    dates = pd.bdate_range("2022-12-01", periods=150)
    rows = []
    states = [
        "risk_off",
        "neutral",
        "constructive",
        "risk_on",
        "neutral",
        "constructive",
        "risk_off",
        "neutral",
        "risk_on",
        "constructive",
    ]
    for idx, date_value in enumerate(dates):
        state = states[idx % len(states)]
        rows.append(
            {
                "date": date_value.date().isoformat(),
                "horizon_days": 20,
                "consensus_state": state,
                "consensus_confidence": 0.8,
                "probe_votes": "{}",
                "disagreement_score": 0.0,
                "score_margin": 0.2,
                "train_usable": True,
                "allowed_training_usage": "[]",
            }
        )
    return pd.DataFrame(rows)


def _synthetic_features(labels: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, label in labels.iterrows():
        state = str(label["consensus_state"])
        if state == "risk_on":
            signal = 1.5
            risk = 0.0
        elif state == "constructive":
            signal = 0.8
            risk = 0.0
        elif state == "risk_off":
            signal = -1.2
            risk = 1.2
        else:
            signal = 0.0
            risk = 0.0
        row = {
            "date": label["date"],
            "known_at": label["date"],
            "available_at": label["date"],
            "decision_at": label["date"],
            "feature_cutoff_passed": True,
            "pit_status": "PIT_APPROVED",
        }
        for column in UP_STATE_FEATURE_COLUMNS:
            row[column] = risk if "drawdown" in column or "downside" in column else signal
        rows.append(row)
    return pd.DataFrame(rows)


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
