from __future__ import annotations

import math
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.second_layer_probe_library_freeze import (
    DEFAULT_PROBE_REGISTRY_V2_PATH,
    validate_dynamic_second_layer_probe_registry_v2,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

REQUIRED_PROBES = {
    "defensive_overlay_probe",
    "balanced_dynamic_probe",
    "drawdown_control_probe",
    "no_tqqq_return_seeking_probe",
    "low_tqqq_balanced_growth_probe",
    "qqq_heavy_growth_probe",
    "capped_risk_on_diagnostic_probe",
    "asymmetric_risk_on_slow_confirm_probe",
}
STATE_ORDER = ["risk_off", "defensive", "neutral", "constructive", "risk_on"]


def test_probe_registry_contains_required_probe_families() -> None:
    registry = _load_registry()
    probes = {probe["probe_id"]: probe for probe in registry["probes"]}

    assert REQUIRED_PROBES <= set(probes)
    validation = validate_dynamic_second_layer_probe_registry_v2(registry)
    assert validation["status"] == "DYNAMIC_SECOND_LAYER_PROBE_REGISTRY_V2_READY_PROMOTION_BLOCKED"


def test_probes_are_trend_sensitive() -> None:
    for probe in _load_registry()["probes"]:
        exposures = [
            _qqq_equivalent(probe["weights_by_trend_state"][state]) for state in STATE_ORDER
        ]

        assert len({round(value, 6) for value in exposures}) > 1


def test_probe_weights_sum_to_one() -> None:
    for probe in _load_registry()["probes"]:
        for state in STATE_ORDER:
            weights = probe["weights_by_trend_state"][state]

            weight_sum = sum(float(weights[asset]) for asset in ("QQQ", "SGOV", "TQQQ"))
            assert math.isclose(weight_sum, 1.0)


def test_probe_weights_are_long_only() -> None:
    for probe in _load_registry()["probes"]:
        for state in STATE_ORDER:
            weights = probe["weights_by_trend_state"][state]

            assert all(float(weights[asset]) >= 0.0 for asset in ("QQQ", "SGOV", "TQQQ"))


def test_no_tqqq_probe_has_zero_tqqq() -> None:
    registry = _load_registry()
    no_tqqq = [
        probe
        for probe in registry["probes"]
        if "no_tqqq" in probe.get("role_tags", [])
    ]

    assert no_tqqq
    for probe in no_tqqq:
        assert all(
            float(probe["weights_by_trend_state"][state]["TQQQ"]) == 0.0
            for state in STATE_ORDER
        )


def test_low_tqqq_probe_respects_tqqq_cap() -> None:
    registry = _load_registry()
    cap = float(registry["freeze_readiness_policy"]["low_tqqq_cap"])
    low_tqqq = [
        probe
        for probe in registry["probes"]
        if "low_tqqq" in probe.get("role_tags", [])
    ]

    assert low_tqqq
    for probe in low_tqqq:
        assert max(
            float(probe["weights_by_trend_state"][state]["TQQQ"]) for state in STATE_ORDER
        ) <= cap


def test_diagnostic_probe_is_research_only() -> None:
    registry = _load_registry()
    diagnostic = next(
        probe
        for probe in registry["probes"]
        if probe["probe_id"] == "capped_risk_on_diagnostic_probe"
    )

    assert diagnostic["research_only"] is True
    assert diagnostic["watch_candidate_allowed"] is False
    assert diagnostic["promotion_enabled"] is False
    assert diagnostic["broker_enabled"] is False
    assert "diagnostic_only" in diagnostic["role_tags"]
    assert "first_layer_action_value_approval" in diagnostic["blocked_usage"]


def test_probe_promotion_and_broker_are_disabled() -> None:
    for probe in _load_registry()["probes"]:
        assert probe["research_only"] is True
        assert probe["promotion_enabled"] is False
        assert probe["broker_enabled"] is False


def test_second_layer_probe_freeze_cli_is_registered() -> None:
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["research", "trends", "--help"],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert "second-layer-probe-freeze" in result.output


def _qqq_equivalent(weights: dict[str, object]) -> float:
    return float(weights["QQQ"]) + 3.0 * float(weights["TQQQ"])


def _load_registry() -> dict[str, object]:
    raw = safe_load_yaml_path(Path(DEFAULT_PROBE_REGISTRY_V2_PATH))
    assert isinstance(raw, dict)
    return raw
