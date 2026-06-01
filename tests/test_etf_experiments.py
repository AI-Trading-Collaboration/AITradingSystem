from __future__ import annotations

from copy import deepcopy

import pytest

from ai_trading_system.etf_portfolio.experiments import (
    ETFExperimentRegistry,
    load_experiment_registry,
)


def test_etf_experiment_registry_loads_default_config() -> None:
    registry = load_experiment_registry()

    assert registry.policy_metadata.version == "etf_experiment_registry_v0_1"
    assert len(registry.experiments) == 16
    assert "base_balanced_growth" in registry.experiments
    assert registry.config_hash


def test_etf_experiment_ids_are_unique_and_match_keys() -> None:
    registry = load_experiment_registry()
    ids = [experiment.experiment_id for experiment in registry.experiments.values()]

    assert len(ids) == len(set(ids))
    for key, experiment in registry.experiments.items():
        assert experiment.experiment_id == key


def test_etf_experiment_base_weights_sum_to_one() -> None:
    registry = load_experiment_registry()

    for experiment in registry.experiments.values():
        weights = experiment.overrides.get("base_weights")
        if weights is None:
            continue
        assert round(sum(float(value) for value in weights.values()), 10) == 1.0


def test_etf_experiment_safety_fields_are_required() -> None:
    registry = load_experiment_registry()

    for experiment in registry.experiments.values():
        assert experiment.observe_only is True
        assert experiment.production_effect == "none"
        assert experiment.broker_action == "none"
        assert experiment.manual_review_required is True


def test_etf_experiment_registry_rejects_unknown_base_config_ref() -> None:
    raw = _registry_raw()
    raw["experiments"]["base_balanced_growth"]["base_config_ref"] = "missing"

    with pytest.raises(ValueError, match="unknown base_config_ref"):
        ETFExperimentRegistry.model_validate(raw)


def test_etf_experiment_registry_rejects_invalid_override_key() -> None:
    raw = _registry_raw()
    raw["experiments"]["base_balanced_growth"]["overrides"]["unknown_weight_knob"] = 0.1

    with pytest.raises(ValueError, match="unsupported keys"):
        ETFExperimentRegistry.model_validate(raw)


def test_etf_experiment_registry_rejects_unsafe_production_effect() -> None:
    raw = _registry_raw()
    raw["experiments"]["base_balanced_growth"]["production_effect"] = "target_weights"

    with pytest.raises(ValueError, match="production_effect=none"):
        ETFExperimentRegistry.model_validate(raw)


def test_etf_experiment_registry_rejects_bad_base_weight_sum() -> None:
    raw = _registry_raw()
    raw["experiments"]["base_balanced_growth"]["overrides"]["base_weights"]["CASH"] = 0.20

    with pytest.raises(ValueError, match="base_weights must sum"):
        ETFExperimentRegistry.model_validate(raw)


def _registry_raw() -> dict[str, object]:
    return deepcopy(load_experiment_registry().model_dump(mode="json"))
