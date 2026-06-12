from __future__ import annotations

import yaml

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_risk_capped_limited_config_validation_passes() -> None:
    validation = system_target.validate_risk_capped_limited_config(
        system_target.DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH
    )

    assert validation["status"] == "PASS"
    assert validation["broker_action_allowed"] is False
    assert validation["production_effect"] == "none"


def test_risk_capped_limited_config_rejects_wider_semiconductor_cap(tmp_path) -> None:
    payload = yaml.safe_load(
        system_target.DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH.read_text(encoding="utf-8")
    )
    payload["caps"]["max_semiconductor_weight"] = 0.50
    config_path = tmp_path / "risk_capped_limited_adjustment_v1.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    validation = system_target.validate_risk_capped_limited_config(config_path)

    assert validation["status"] == "FAIL"
    failed = {row["check_id"] for row in validation["checks"] if row["passed"] is False}
    assert "max_semiconductor_not_wider_than_model_target" in failed


def test_risk_capped_limited_config_report_writes_normalized_artifact(tmp_path) -> None:
    result = system_target.build_risk_capped_limited_config_report(output_dir=tmp_path)

    assert result["manifest"]["status"] == "PASS"
    assert (result["config_dir"] / "risk_capped_config_manifest.json").exists()
    assert (result["config_dir"] / "normalized_risk_capped_config.yaml").exists()
    assert (result["config_dir"] / "risk_capped_config_report.md").exists()
