from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.yaml_loader import safe_load_yaml_path

BOUNDARY_PATH = Path("config/research/two_layer_strategy_boundary_contract.yaml")
CHANNEL_POLICY_PATH = Path("config/research/first_layer_channel_policy.yaml")
USAGE_MATRIX_PATH = Path("inputs/research_reviews/first_layer_signal_usage_matrix_v2.yaml")
BASE_OVERLAY_PATH = Path("config/research/base_overlay_veto_policy_schema.yaml")
FORWARD_LOG_PATH = Path("config/research/return_seeking_diagnostic_forward_log.yaml")


def test_first_layer_cannot_emit_weights() -> None:
    contract = _load_yaml(BOUNDARY_PATH)
    matrix = _load_yaml(USAGE_MATRIX_PATH)

    assert contract["first_layer_contract"]["can_emit_weights"] is False
    for channel in contract["first_layer_contract"]["channels"].values():
        assert channel["can_emit_weights"] is False
    for signal in matrix["signals"]:
        assert signal["can_emit_weights"] is False


def test_add_risk_blocked_from_defensive_overlay() -> None:
    matrix = _signals_by_name(_load_yaml(USAGE_MATRIX_PATH))
    policy = _load_yaml(CHANNEL_POLICY_PATH)

    add_risk = matrix["add_risk"]
    defensive = policy["channels"]["defensive_channel"]

    assert "defensive_overlay" in add_risk["blocked_usage"]
    assert "growth_overlay" in add_risk["blocked_usage"]
    assert "add_risk" in defensive["cannot_emit"]
    assert defensive["can_drive_growth_overlay"] is False


def test_return_seeking_signal_is_diagnostic_only() -> None:
    matrix = _signals_by_name(_load_yaml(USAGE_MATRIX_PATH))
    policy = _load_yaml(CHANNEL_POLICY_PATH)

    for name in ("stay_constructive", "add_risk", "risk_on_diagnostic"):
        assert matrix[name]["diagnostic_only"] is True
        assert "promotion" in matrix[name]["blocked_usage"]
        assert "broker" in matrix[name]["blocked_usage"]
    channel = policy["channels"]["return_seeking_diagnostic_channel"]
    assert channel["diagnostic_only"] is True
    assert channel["can_emit_weights"] is False
    assert channel["can_enable_promotion"] is False


def test_risk_veto_blocks_growth_overlay() -> None:
    policy = _load_yaml(CHANNEL_POLICY_PATH)
    schema = _load_yaml(BASE_OVERLAY_PATH)
    veto = policy["channels"]["risk_veto_channel"]

    assert veto["priority"] == "highest"
    assert veto["veto_effect"]["growth_overlay"] == "blocked"
    assert schema["veto_rules"]["risk_off_veto_blocks_growth"] is True
    assert schema["veto_rules"]["active_veto_sets_growth_overlay_to_zero"] is True


def test_second_layer_cannot_use_raw_indicators() -> None:
    contract = _load_yaml(BOUNDARY_PATH)
    schema = _load_yaml(BASE_OVERLAY_PATH)

    assert contract["second_layer_contract"]["raw_indicators_allowed"] is False
    assert "QQQ_momentum" in contract["second_layer_contract"]["blocked_raw_indicator_inputs"]
    assert schema["second_layer_input_contract"]["raw_indicators_allowed"] is False


def test_diagnostic_signal_cannot_enable_promotion() -> None:
    matrix = _load_yaml(USAGE_MATRIX_PATH)
    forward_log = _load_yaml(FORWARD_LOG_PATH)

    assert matrix["matrix_rules"]["diagnostic_signal_cannot_enable_promotion"] is True
    assert matrix["promotion_allowed"] is False
    assert "target_weights" in forward_log["blocked_fields"]
    assert forward_log["can_emit_weights"] is False
    assert forward_log["safety_boundary"]["broker_action"] == "none"


def test_indicator_family_ablation_cli_writes_diagnostic_matrix(tmp_path: Path) -> None:
    matrix_path = tmp_path / "indicator_family_ablation_matrix.yaml"
    review_path = tmp_path / "indicator_family_ablation_review.md"
    output_root = tmp_path / "outputs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "indicator-family-ablation",
            "--matrix-path",
            str(matrix_path),
            "--review-path",
            str(review_path),
            "--output-root",
            str(output_root),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    payload = _load_yaml(matrix_path)
    assert payload["status"] == "INDICATOR_FAMILY_ABLATION_EVIDENCE_READY"
    assert payload["summary"]["diagnostic_only"] is True
    assert payload["summary"]["allocation_candidate_count"] == 0
    assert payload["promotion_allowed"] is False
    assert review_path.exists()


def _signals_by_name(matrix: dict[str, object]) -> dict[str, dict[str, object]]:
    signals = matrix["signals"]
    assert isinstance(signals, list)
    return {str(signal["signal_name"]): signal for signal in signals if isinstance(signal, dict)}


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
