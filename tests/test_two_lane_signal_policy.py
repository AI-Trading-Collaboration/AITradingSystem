from __future__ import annotations

from pathlib import Path

from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_PATH = Path("config/research/two_lane_signal_policy.yaml")
UNIVERSAL_CLOSEOUT_PATH = Path(
    "inputs/research_reviews/first_layer_v2_universal_layer_closeout.yaml"
)
SIGNAL_USAGE_PATH = Path("inputs/research_reviews/first_layer_signal_usage_matrix.yaml")
FINAL_MATRIX_PATH = Path("inputs/research_reviews/two_layer_lane_separation_final_matrix.yaml")


def test_add_risk_cannot_drive_defensive_overlay() -> None:
    policy = _load_yaml(POLICY_PATH)
    usage = _signals_by_name(_load_yaml(SIGNAL_USAGE_PATH))
    defensive_channel = policy["channels"]["defensive_channel"]
    add_risk = usage["add_risk"]

    assert defensive_channel["can_add_risk"] is False
    assert "add_risk" in defensive_channel["disallowed_outputs"]
    assert "high_confidence_risk_on" in defensive_channel["disallowed_outputs"]
    assert "defensive_overlay" in add_risk["blocked_usage"]
    assert policy["lane_rules"]["add_risk_can_drive_defensive_overlay"] is False


def test_risk_off_veto_blocks_growth_overlay() -> None:
    policy = _load_yaml(POLICY_PATH)
    veto = policy["risk_off_veto"]
    gated_channel = policy["channels"]["gated_integration_channel"]
    growth_overlay = gated_channel["growth_overlay"]

    assert veto["enabled"] is True
    assert veto["priority"] == "risk_off_has_veto_power_over_add_risk"
    assert "risk_off" in veto["veto_states"]
    assert "gated_growth_overlay" in veto["vetoed_actions"]
    assert growth_overlay["requires_risk_off_veto_clear"] is True
    assert growth_overlay["allowed_only_when_defensive_overlay_inactive"] is True
    assert policy["lane_rules"]["risk_off_veto_blocks_growth_overlay"] is True


def test_return_seeking_diagnostic_cannot_enable_promotion() -> None:
    policy = _load_yaml(POLICY_PATH)
    usage = _signals_by_name(_load_yaml(SIGNAL_USAGE_PATH))
    final = _load_yaml(FINAL_MATRIX_PATH)
    return_channel = policy["channels"]["return_seeking_channel"]

    assert return_channel["status"] == "DIAGNOSTIC_ONLY"
    assert return_channel["promotion_allowed"] is False
    assert "promotion" in return_channel["blocked_downstream_usage"]
    assert "return_seeking_diagnostic" in usage["add_risk"]["allowed_usage"]
    assert "promotion" in usage["add_risk"]["blocked_usage"]
    assert final["summary"]["return_seeking_diagnostic_can_enable_promotion"] is False
    assert final["promotion_allowed"] is False
    assert final["broker_action"] == "none"


def test_universal_first_layer_v2_is_rejected() -> None:
    closeout = _load_yaml(UNIVERSAL_CLOSEOUT_PATH)
    final = _load_yaml(FINAL_MATRIX_PATH)

    assert closeout["status"] == "UNIVERSAL_FIRST_LAYER_REJECTED"
    assert closeout["summary"]["universal_layer_status"] == "UNIVERSAL_FIRST_LAYER_REJECTED"
    assert "RETURN_SEEKING_DIAGNOSTIC_ONLY" in closeout["summary"]["allowed_states"]
    assert "DEFENSIVE_USAGE_BLOCKED" in closeout["summary"]["allowed_states"]
    assert closeout["closeout_decision"]["universal_first_layer_rejected"] is True
    assert final["summary"]["universal_first_layer_status"] == "UNIVERSAL_FIRST_LAYER_REJECTED"
    assert final["summary"]["defensive_usage_status"] == "DEFENSIVE_USAGE_BLOCKED"


def test_signal_usage_matrix_requires_allowed_and_blocked_usage() -> None:
    matrix = _load_yaml(SIGNAL_USAGE_PATH)
    signals = matrix["signals"]

    assert matrix["summary"]["signal_count"] == len(signals)
    assert matrix["matrix_rules"]["every_signal_requires_allowed_usage"] is True
    assert matrix["matrix_rules"]["every_signal_requires_blocked_usage"] is True
    assert matrix["matrix_rules"]["every_signal_requires_required_gate"] is True
    for signal in signals:
        assert signal["signal_name"]
        assert signal["source_model"]
        assert signal["allowed_usage"]
        assert signal["blocked_usage"]
        assert signal["required_gate"]
    assert matrix["promotion_allowed"] is False
    assert matrix["paper_shadow_allowed"] is False
    assert matrix["production_allowed"] is False
    assert matrix["broker_action"] == "none"


def _signals_by_name(matrix: dict[str, object]) -> dict[str, dict[str, object]]:
    signals = matrix["signals"]
    assert isinstance(signals, list)
    return {str(signal["signal_name"]): signal for signal in signals if isinstance(signal, dict)}


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
