from __future__ import annotations

from dynamic_v3_system_target_helpers import build_model_target_fixture, write_model_target_config

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
