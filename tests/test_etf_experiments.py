from __future__ import annotations

from copy import deepcopy

import pytest

from ai_trading_system.etf_portfolio.experiments import (
    ETFExperimentPackRegistry,
    ETFExperimentRegistry,
    load_experiment_pack_registry,
    load_experiment_registry,
    validate_experiment_pack_registry,
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


def test_etf_experiment_pack_loads_default_pack() -> None:
    pack_registry = load_experiment_pack_registry()
    pack = pack_registry.experiment_packs["etf_calibration_v1"]

    assert pack.pack_id == "etf_calibration_v1"
    assert pack.created_for_task == "TRADING-064"
    assert len(pack.experiment_ids) == 16
    assert pack.ranking_policy == "risk_adjusted_v1"
    assert pack.promotion_policy == "shadow_only_manual_review"
    assert pack_registry.config_hash


def test_etf_experiment_pack_references_existing_experiments() -> None:
    registry = load_experiment_registry()
    pack_registry = load_experiment_pack_registry(experiment_registry=registry)
    pack = pack_registry.experiment_packs["etf_calibration_v1"]

    assert set(pack.experiment_ids).issubset(set(registry.experiments))


def test_etf_experiment_pack_rejects_duplicate_experiments() -> None:
    raw = _pack_raw()
    ids = raw["experiment_packs"]["etf_calibration_v1"]["experiment_ids"]
    ids.append(ids[0])

    with pytest.raises(ValueError, match="duplicate experiments"):
        ETFExperimentPackRegistry.model_validate(raw)


def test_etf_experiment_pack_rejects_unknown_experiment_reference() -> None:
    raw = _pack_raw()
    raw["experiment_packs"]["etf_calibration_v1"]["experiment_ids"].append("missing")
    pack_registry = ETFExperimentPackRegistry.model_validate(raw)

    with pytest.raises(ValueError, match="unknown experiment_id"):
        validate_experiment_pack_registry(
            pack_registry,
            experiment_registry=load_experiment_registry(),
        )


def test_etf_experiment_pack_rejects_unsafe_experiment() -> None:
    registry = load_experiment_registry()
    registry.experiments["regime_mild"].production_effect = "target_weights"
    pack_registry = ETFExperimentPackRegistry.model_validate(_pack_raw())

    with pytest.raises(ValueError, match="unsafe experiment"):
        validate_experiment_pack_registry(pack_registry, experiment_registry=registry)


def test_etf_experiment_pack_rejects_missing_ranking_policy() -> None:
    raw = _pack_raw()
    raw["experiment_packs"]["etf_calibration_v1"]["ranking_policy"] = "missing"

    with pytest.raises(ValueError, match="unknown ranking_policy"):
        ETFExperimentPackRegistry.model_validate(raw)


def test_etf_experiment_pack_rejects_missing_promotion_policy() -> None:
    raw = _pack_raw()
    raw["experiment_packs"]["etf_calibration_v1"]["promotion_policy"] = "missing"

    with pytest.raises(ValueError, match="unknown promotion_policy"):
        ETFExperimentPackRegistry.model_validate(raw)


def _registry_raw() -> dict[str, object]:
    return deepcopy(load_experiment_registry().model_dump(mode="json"))


def _pack_raw() -> dict[str, object]:
    return deepcopy(load_experiment_pack_registry().model_dump(mode="json"))
