from __future__ import annotations

from copy import deepcopy

import pytest
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle
from ai_trading_system.etf_portfolio.weight_calibration import (
    DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    ETFWeightSearchRegistry,
    WeightCalibrationError,
    load_weight_search_definition,
    load_weight_search_registry,
    validate_weight_search_registry,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_weight_search_config_loads_default() -> None:
    registry = load_weight_search_registry()
    search = load_weight_search_definition("etf_initial_weight_search_v1")

    assert registry.schema_version == "etf_weight_search_v1"
    assert registry.config_hash
    assert search.search_id == "etf_initial_weight_search_v1"
    assert search.universe == ["SPY", "QQQ", "SMH", "SOXX", "CASH"]
    assert search.grid_step == 0.05
    assert search.max_candidate_count == 1000
    assert search.safety.production_effect == "none"
    assert search.safety.broker_action == "none"


def test_weight_search_constraints_are_valid() -> None:
    search = load_weight_search_definition("etf_initial_weight_search_v1")

    assert set(search.weight_constraints) == set(search.universe)
    for symbol, constraint in search.weight_constraints.items():
        assert 0.0 <= constraint.min <= constraint.max <= 1.0, symbol
    assert search.sleeve_constraints.semiconductor_total_max == 0.35
    assert search.sleeve_constraints.cash_min_when_risk_off == 0.30


def test_weight_search_universe_symbols_exist() -> None:
    raw = _raw_registry()
    search = raw["weight_searches"]["etf_initial_weight_search_v1"]
    search["universe"].append("MISSING")
    search["weight_constraints"]["MISSING"] = {"min": 0.0, "max": 0.1}
    registry = ETFWeightSearchRegistry.model_validate(raw)

    with pytest.raises(WeightCalibrationError, match="unknown symbols"):
        validate_weight_search_registry(registry, etf_config=load_etf_config_bundle())


def test_weight_search_benchmark_set_exists() -> None:
    raw = _raw_registry()
    raw["weight_searches"]["etf_initial_weight_search_v1"]["benchmark_set"] = "missing"

    with pytest.raises(ValueError, match="unknown benchmark_set"):
        ETFWeightSearchRegistry.model_validate(raw)


def test_weight_search_benchmark_ids_exist_in_etf_backtest_config() -> None:
    raw = _raw_registry()
    raw["benchmark_sets"]["standard_etf"]["benchmark_ids"].append("B999")
    registry = ETFWeightSearchRegistry.model_validate(raw)

    with pytest.raises(WeightCalibrationError, match="unknown ETF benchmark ids"):
        validate_weight_search_registry(registry, etf_config=load_etf_config_bundle())


def test_weight_search_safety_fields_are_required() -> None:
    raw = _raw_registry()
    del raw["weight_searches"]["etf_initial_weight_search_v1"]["safety"][
        "manual_review_required"
    ]

    with pytest.raises(ValueError, match="manual_review_required"):
        ETFWeightSearchRegistry.model_validate(raw)


def test_weight_search_unsafe_production_effect_fails() -> None:
    raw = _raw_registry()
    raw["weight_searches"]["etf_initial_weight_search_v1"]["safety"][
        "production_effect"
    ] = "apply_weights"

    with pytest.raises(ValueError, match="production_effect"):
        ETFWeightSearchRegistry.model_validate(raw)


def test_weight_search_invalid_grid_step_fails() -> None:
    raw = _raw_registry()
    raw["weight_searches"]["etf_initial_weight_search_v1"]["grid_step"] = 0.03

    with pytest.raises(ValueError, match="grid_step"):
        ETFWeightSearchRegistry.model_validate(raw)


def test_weight_search_invalid_constraint_order_fails() -> None:
    raw = _raw_registry()
    raw["weight_searches"]["etf_initial_weight_search_v1"]["weight_constraints"]["SPY"] = {
        "min": 0.70,
        "max": 0.60,
    }

    with pytest.raises(ValueError, match="min must be <= max"):
        ETFWeightSearchRegistry.model_validate(raw)


def test_weight_search_config_cli_validate_smoke() -> None:
    result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "validate-config",
            "--search",
            "etf_initial_weight_search_v1",
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "search_id=etf_initial_weight_search_v1" in result.output
    assert "objective_policy=robust_risk_adjusted_v1" in result.output
    assert "observe_only=true" in result.output
    assert "candidate_only=true" in result.output
    assert "production_effect=none" in result.output
    assert "broker_action=none" in result.output
    assert "manual_review_required=true" in result.output


def _raw_registry() -> dict[str, object]:
    raw = safe_load_yaml_path(DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH)
    assert isinstance(raw, dict)
    return deepcopy(raw)
