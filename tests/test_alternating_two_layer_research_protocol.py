from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.upper_state_label_feature_reset import (
    DEFAULT_ALTERNATING_PROTOCOL_PATH,
    validate_alternating_two_layer_protocol,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_alternating_two_layer_protocol_passes() -> None:
    protocol = _load_yaml(DEFAULT_ALTERNATING_PROTOCOL_PATH)

    validation = validate_alternating_two_layer_protocol(protocol)

    assert validation["status"] == "PASS"
    assert protocol["promotion_rules"]["dynamic_promotion_status"] == "BLOCKED"
    assert protocol["promotion_rules"]["target_path_metrics_can_pass_first_layer_gate"] is False


def test_only_one_layer_is_modified_per_round() -> None:
    protocol = _load_yaml(DEFAULT_ALTERNATING_PROTOCOL_PATH)

    rounds = protocol["round_types"]
    assert rounds["first_layer_calibration"]["modified_layer"] == "first_layer"
    assert rounds["first_layer_calibration"]["frozen_layer"] == "second_layer"
    assert rounds["second_layer_mapping"]["modified_layer"] == "second_layer"
    assert rounds["second_layer_mapping"]["frozen_layer"] == "first_layer"
    assert rounds["validation"]["modified_layer"] == "none"
    assert rounds["validation"]["frozen_layer"] == "both"


def test_protocol_blocks_cross_layer_mutation() -> None:
    protocol = _load_yaml(DEFAULT_ALTERNATING_PROTOCOL_PATH)
    broken = dict(protocol)
    rounds = dict(protocol["round_types"])
    first = dict(rounds["first_layer_calibration"])
    first["forbidden_changes"] = [
        change
        for change in first["forbidden_changes"]
        if change != "second_layer_probe_weight_change"
    ]
    rounds["first_layer_calibration"] = first
    broken["round_types"] = rounds

    validation = validate_alternating_two_layer_protocol(broken)

    assert validation["status"] == "FAIL"
    assert any(
        issue["code"] == "first_layer_calibration_can_modify_second_layer_weights"
        for issue in validation["issues"]
    )


def test_upper_state_reset_cli_is_registered() -> None:
    runner = CliRunner()

    result = runner.invoke(
        app,
        ["research", "trends", "--help"],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert "upper-state-reset" in result.output


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
