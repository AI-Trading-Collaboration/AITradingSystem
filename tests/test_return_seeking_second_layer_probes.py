from __future__ import annotations

from pathlib import Path

from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_return_seeking_probe_metadata_and_safety_boundary() -> None:
    registry = _load_yaml(Path("config/research/dynamic_second_layer_probe_registry.yaml"))
    probes = registry["probes"]
    return_seeking = [probe for probe in probes if bool(probe.get("return_seeking"))]

    assert len(return_seeking) >= 3
    for probe in return_seeking:
        assert "return_seeking" in probe.get("role_tags", [])
        assert probe["research_only"] is True
        assert probe["promotion_enabled"] is False
        assert probe["broker_enabled"] is False
        assert "action_value_labels" in probe.get("allowed_usage", [])


def test_return_seeking_probes_increase_upper_state_exposure() -> None:
    registry = _load_yaml(Path("config/research/dynamic_second_layer_probe_registry.yaml"))
    return_seeking = [probe for probe in registry["probes"] if bool(probe.get("return_seeking"))]

    for probe in return_seeking:
        weights = probe["weights_by_trend_state"]
        neutral = _qqq_equivalent(weights["neutral"])
        constructive = _qqq_equivalent(weights["constructive"])
        risk_on = _qqq_equivalent(weights["risk_on"])
        assert constructive >= neutral
        assert risk_on >= constructive


def _qqq_equivalent(weights: dict[str, float]) -> float:
    return float(weights["QQQ"]) + 3.0 * float(weights["TQQQ"])


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
