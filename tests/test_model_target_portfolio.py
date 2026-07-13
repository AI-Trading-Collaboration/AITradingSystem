from __future__ import annotations

from datetime import UTC, datetime

import pytest
from dynamic_v3_system_target_helpers import (
    TARGET_AS_OF,
    build_model_target_fixture,
    write_model_target_config,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_model_target_config_and_generation_are_research_only(tmp_path) -> None:
    config_path = write_model_target_config(tmp_path)

    validation = system_target.validate_model_target_config(config_path)

    assert validation["status"] == "PASS"
    assert validation["broker_action_allowed"] is False
    fixture = build_model_target_fixture(tmp_path)
    manifest = fixture["manifest"]
    weights = fixture["model_target_weights"]["method_weights"]

    assert manifest["status"] == "PASS"
    assert manifest["research_target_only"] is True
    assert manifest["not_official_target_weights"] is True
    assert manifest["broker_action_allowed"] is False
    assert manifest["generated_methods"] == list(system_target.TARGET_METHODS)
    assert weights["consensus_target"] != weights["static_baseline"]
    assert weights["limited_adjustment"] != weights["consensus_target"]
    assert weights["defensive_limited_adjustment"]["CASH"] > weights["limited_adjustment"]["CASH"]
    assert fixture["target_constraint_checks"]["overall_status"] == "PASS"

    artifact_validation = system_target.validate_model_target_artifact(
        target_id=fixture["target_id"],
        output_dir=tmp_path / "model_target",
    )
    assert artifact_validation["status"] == "PASS"


def test_model_target_fails_closed_without_daily_source(tmp_path) -> None:
    config_path = write_model_target_config(tmp_path)

    with pytest.raises(ValueError, match="no daily advisory source"):
        system_target.generate_model_target(
            config_path=config_path,
            as_of=TARGET_AS_OF,
            output_dir=tmp_path / "model_target",
            position_advisory_daily_dir=tmp_path / "missing_daily",
            shadow_monitor_dir=tmp_path / "missing_monitor",
            shadow_shortlist_dir=tmp_path / "missing_shortlist",
            consensus_drift_dir=tmp_path / "missing_drift",
            generated_at=datetime(2026, 1, 5, tzinfo=UTC),
        )


def test_model_target_validation_detects_source_drift(tmp_path) -> None:
    fixture = build_model_target_fixture(tmp_path)
    source_path = (
        fixture["position_advisory_daily_dir"]
        / "daily-1"
        / "daily_advisory_actions.json"
    )
    source_path.write_text('{"daily_advisory_id":"tampered"}\n', encoding="utf-8")

    validation = system_target.validate_model_target_artifact(
        target_id=fixture["target_id"],
        output_dir=tmp_path / "model_target",
    )
    assert validation["status"] == "FAIL"
