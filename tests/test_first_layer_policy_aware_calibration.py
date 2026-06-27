from __future__ import annotations

from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.first_layer_policy_calibration import (
    DEFAULT_PROBE_REGISTRY_PATH,
    DEFAULT_SCORECARD_CONFIG_PATH,
    build_action_value_matrix,
    build_pit_feature_matrix,
    build_scorecard_predictions,
    first_layer_predictions_contain_weights,
    probe_can_generate_action_value_labels,
    validate_probe_registry,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_second_layer_probe_must_be_trend_sensitive() -> None:
    registry = _load_yaml(DEFAULT_PROBE_REGISTRY_PATH)

    validation = validate_probe_registry(registry)

    assert validation["status"] == "PROBE_REGISTRY_VALIDATED"
    assert validation["summary"]["probe_count"] >= 3
    assert validation["summary"]["trend_sensitive_count"] == validation["summary"]["probe_count"]


def test_static_baseline_cannot_generate_action_value_labels() -> None:
    static_probe = {
        "probe_id": "static_60_40",
        "role": "static_baseline",
        "weights_by_trend_state": {
            state: {"QQQ": 0.6, "SGOV": 0.4, "TQQQ": 0.0}
            for state in ["risk_off", "defensive", "neutral", "constructive", "risk_on"]
        },
    }
    registry = {
        "trend_states": ["risk_off", "defensive", "neutral", "constructive", "risk_on"],
        "forbidden_probe_roles": ["static_baseline", "static_frontier"],
        "probes": [static_probe],
    }

    validation = validate_probe_registry(registry)

    assert probe_can_generate_action_value_labels(static_probe) is False
    row = validation["rows"][0]
    assert row["status"] == "BLOCKED"
    assert "static_baseline_role_forbidden" in row["issues"]
    assert "probe_not_trend_sensitive" in row["issues"]


def test_action_value_matrix_uses_future_only_for_labels_not_features() -> None:
    registry = _load_yaml(DEFAULT_PROBE_REGISTRY_PATH)
    policy = {
        "horizons": [5],
        "full_allocation_score": {"lambda_dd": 0.8, "lambda_worst5": 0.5},
        "tqqq_risk_penalty": {"penalty_per_weight": 0.2},
    }
    prices = _prices()

    matrix = build_action_value_matrix(
        prices=prices,
        probe_registry=registry,
        score_policy=policy,
    )

    assert not matrix.empty
    assert matrix["label_uses_future_outcome"].all()
    assert not matrix["feature_cutoff_used"].any()
    assert {"date", "assumed_trend_state", "future_return", "action_value_score"} <= set(
        matrix.columns
    )


def test_feature_matrix_blocks_non_pit_features() -> None:
    feature_matrix, report = build_pit_feature_matrix(prices=_prices(), rates=_rates())

    assert report["status"] == "PIT_WARNING"
    assert report["blocked_feature_count"] == 0
    assert report["excluded_non_pit_or_unavailable_features"]
    assert feature_matrix["feature_cutoff_passed"].all()
    assert not any("future" in column for column in feature_matrix.columns)


def test_first_layer_model_does_not_output_weights() -> None:
    scorecard = _load_yaml(DEFAULT_SCORECARD_CONFIG_PATH)
    feature_matrix, _ = build_pit_feature_matrix(prices=_prices(), rates=_rates())

    predictions = build_scorecard_predictions(
        feature_matrix=feature_matrix,
        scorecard_config=scorecard,
    )

    assert not predictions.empty
    assert first_layer_predictions_contain_weights(predictions) is False
    assert {"trend_state", "confidence", "validity_days", "decay_profile"} <= set(
        predictions.columns
    )


def test_calibrated_first_layer_does_not_enable_promotion() -> None:
    scorecard = _load_yaml(DEFAULT_SCORECARD_CONFIG_PATH)
    feature_matrix, _ = build_pit_feature_matrix(prices=_prices(), rates=_rates())

    predictions = build_scorecard_predictions(
        feature_matrix=feature_matrix,
        scorecard_config=scorecard,
    )

    assert predictions["promotion_allowed"].eq(False).all()
    assert predictions["paper_shadow_allowed"].eq(False).all()
    assert predictions["production_allowed"].eq(False).all()
    assert predictions["broker_action"].eq("none").all()


def test_tqqq_risk_on_probe_remains_research_only() -> None:
    registry = _load_yaml(DEFAULT_PROBE_REGISTRY_PATH)
    validation = validate_probe_registry(registry)
    rows = {row["probe_id"]: row for row in validation["rows"]}

    risk_on = rows["risk_on_diagnostic_probe"]
    assert risk_on["tqqq_used"] is True
    assert risk_on["research_only"] is True
    assert risk_on["promotion_enabled"] is False
    assert risk_on["broker_enabled"] is False


def test_research_trends_cli_is_registered() -> None:
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["research", "trends", "--help"],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert "full-pack" in result.output


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw


def _prices() -> pd.DataFrame:
    dates = pd.bdate_range("2022-12-01", periods=90)
    rows = []
    levels = {"QQQ": 280.0, "SGOV": 100.0, "TQQQ": 22.0}
    for idx, date_value in enumerate(dates):
        qqq_return = 0.001 + (0.002 if idx % 17 == 0 else 0.0) - (0.004 if idx % 29 == 0 else 0.0)
        levels["QQQ"] *= 1.0 + qqq_return
        levels["SGOV"] *= 1.0 + 0.00015
        levels["TQQQ"] *= 1.0 + qqq_return * 2.6
        rows.append({"date": date_value, **levels})
    return pd.DataFrame(rows).set_index("date")


def _rates() -> pd.DataFrame:
    dates = pd.bdate_range("2022-12-01", periods=90)
    return pd.DataFrame(
        {
            "DGS2": [4.5 for _ in dates],
            "DGS10": [4.1 for _ in dates],
            "DTWEXBGS": [120.0 for _ in dates],
        },
        index=dates,
    )
