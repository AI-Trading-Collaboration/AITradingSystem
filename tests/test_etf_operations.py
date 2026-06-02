from __future__ import annotations

from copy import deepcopy

import pytest

from ai_trading_system.etf_portfolio.operations import (
    DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH,
    OPERATIONS_SCHEDULE_SCHEMA_VERSION,
    ETFOperationsScheduleConfig,
    load_operations_schedule_config,
    operations_schedule_required_step_ids,
    operations_schedule_step_ids,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_operations_schedule_config_loads_default() -> None:
    config = load_operations_schedule_config()

    assert config.schema_version == OPERATIONS_SCHEDULE_SCHEMA_VERSION
    assert config.policy_metadata.version == "etf_operations_schedule_v0_1"
    assert config.safety.observe_only is True
    assert config.safety.candidate_only is True
    assert config.safety.production_effect == "none"
    assert config.safety.broker_action == "none"
    assert config.safety.manual_review_required is True


def test_operations_schedule_includes_required_cadence_sections() -> None:
    config = load_operations_schedule_config()

    assert config.daily_pipeline
    assert config.weekly_pipeline
    assert config.biweekly_pipeline
    assert config.monthly_pipeline
    assert config.manual_review_steps
    assert all(step.cadence == "daily" for step in config.daily_pipeline)
    assert all(step.cadence == "weekly" for step in config.weekly_pipeline)
    assert all(step.cadence == "biweekly" for step in config.biweekly_pipeline)
    assert all(step.cadence == "monthly" for step in config.monthly_pipeline)


def test_operations_schedule_daily_required_nodes_exist() -> None:
    config = load_operations_schedule_config()

    daily_ids = {step.step_id for step in config.daily_pipeline}

    assert {
        "data_freshness_check",
        "etf_daily_run",
        "forward_update",
        "ai_confirmation_run",
        "satellite_replacement_run",
        "ai_attribution_update",
        "satellite_attribution_update",
        "reader_brief_generate",
        "report_registry_update",
        "operations_health_check",
    }.issubset(daily_ids)


def test_operations_schedule_step_ids_are_unique() -> None:
    config = load_operations_schedule_config()
    step_ids = operations_schedule_step_ids(config)

    assert len(step_ids) == len(set(step_ids))


def test_operations_schedule_commands_are_non_empty() -> None:
    config = load_operations_schedule_config()

    assert all(step.command.strip() for step in config.steps())


def test_operations_schedule_dependencies_reference_valid_step_ids() -> None:
    config = load_operations_schedule_config()
    step_ids = set(operations_schedule_step_ids(config))

    for step in config.steps():
        assert set(step.dependencies).issubset(step_ids), step.step_id


def test_operations_schedule_required_steps_have_expected_outputs() -> None:
    config = load_operations_schedule_config()

    required_ids = operations_schedule_required_step_ids(config)
    assert "data_freshness_check" in required_ids
    for step in config.steps():
        if step.required:
            assert step.expected_outputs, step.step_id


def test_operations_schedule_rejects_duplicate_step_ids() -> None:
    raw = _raw_schedule()
    raw["daily_pipeline"][1]["step_id"] = "data_freshness_check"

    with pytest.raises(ValueError, match="step IDs must be unique"):
        ETFOperationsScheduleConfig.model_validate(raw)


def test_operations_schedule_rejects_missing_dependency() -> None:
    raw = _raw_schedule()
    raw["weekly_pipeline"][0]["dependencies"] = ["missing_daily_gate"]

    with pytest.raises(ValueError, match="dependencies reference unknown steps"):
        ETFOperationsScheduleConfig.model_validate(raw)


def test_operations_schedule_rejects_required_step_without_outputs() -> None:
    raw = _raw_schedule()
    raw["daily_pipeline"][0]["expected_outputs"] = []

    with pytest.raises(ValueError, match="required step must declare expected_outputs"):
        ETFOperationsScheduleConfig.model_validate(raw)


def test_operations_schedule_safety_fields_are_required() -> None:
    raw = _raw_schedule()
    del raw["safety"]["manual_review_required"]

    with pytest.raises(ValueError, match="manual_review_required"):
        ETFOperationsScheduleConfig.model_validate(raw)


def test_operations_schedule_unsafe_production_effect_fails() -> None:
    raw = _raw_schedule()
    raw["safety"]["production_effect"] = "apply_weights"

    with pytest.raises(ValueError, match="production_effect"):
        ETFOperationsScheduleConfig.model_validate(raw)


def test_operations_schedule_rejects_empty_command() -> None:
    raw = _raw_schedule()
    raw["daily_pipeline"][0]["command"] = ""

    with pytest.raises(ValueError, match="command"):
        ETFOperationsScheduleConfig.model_validate(raw)


def test_operations_schedule_weight_search_is_not_daily() -> None:
    config = load_operations_schedule_config()

    daily_ids = {step.step_id for step in config.daily_pipeline}
    monthly_ids = {step.step_id for step in config.monthly_pipeline}
    assert "weight_calibration_search" not in daily_ids
    assert "weight_calibration_search" in monthly_ids


def _raw_schedule() -> dict[str, object]:
    raw = safe_load_yaml_path(DEFAULT_ETF_OPERATIONS_SCHEDULE_CONFIG_PATH)
    assert isinstance(raw, dict)
    return deepcopy(raw)
