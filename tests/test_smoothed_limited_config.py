from __future__ import annotations

import yaml

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_limited_config_validation_passes() -> None:
    validation = system_target.validate_smoothed_limited_config(
        system_target.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH
    )

    assert validation["status"] == "PASS"
    assert validation["broker_action_allowed"] is False
    assert validation["production_effect"] == "none"


def test_smoothed_limited_config_rejects_invalid_window(tmp_path) -> None:
    payload = yaml.safe_load(
        system_target.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH.read_text(encoding="utf-8")
    )
    payload["variants"]["smooth_weights_3d"]["smoothing_window_days"] = 0
    config_path = tmp_path / "smoothed_limited_adjustment_v1.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    validation = system_target.validate_smoothed_limited_config(config_path)

    assert validation["status"] == "FAIL"
    failed = {row["check_id"] for row in validation["checks"] if row["passed"] is False}
    assert "variant_smoothing_windows_positive" in failed


def test_smoothed_limited_config_rejects_alpha_out_of_range(tmp_path) -> None:
    payload = yaml.safe_load(
        system_target.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH.read_text(encoding="utf-8")
    )
    payload["variants"]["smooth_weights_5d"]["alpha"] = 1.5
    config_path = tmp_path / "smoothed_limited_adjustment_v1.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    validation = system_target.validate_smoothed_limited_config(config_path)

    assert validation["status"] == "FAIL"
    failed = {row["check_id"] for row in validation["checks"] if row["passed"] is False}
    assert "variant_alpha_within_bounds" in failed


def test_smoothed_limited_config_report_writes_normalized_artifact(tmp_path) -> None:
    result = system_target.build_smoothed_limited_config_report(output_dir=tmp_path)

    assert result["manifest"]["status"] == "PASS"
    assert (result["config_dir"] / "smoothed_limited_config_manifest.json").exists()
    assert (result["config_dir"] / "normalized_smoothed_limited_config.yaml").exists()
    assert (result["config_dir"] / "smoothed_limited_config_report.md").exists()
