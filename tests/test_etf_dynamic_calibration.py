from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.dynamic_allocation import (
    load_dynamic_allocation_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_calibration import (
    DEFAULT_DYNAMIC_CALIBRATION_POLICY_CONFIG_PATH,
    DYNAMIC_CALIBRATION_SAFETY,
    DynamicCalibrationError,
    build_dynamic_calibration_batch_report,
    build_dynamic_calibration_validation_report,
    load_dynamic_calibration_policy_config,
    write_dynamic_calibration_report,
)
from ai_trading_system.reports import reader_brief


def test_dynamic_calibration_policy_loads_and_rejects_unsafe(tmp_path: Path) -> None:
    policy = load_dynamic_calibration_policy_config()

    assert policy.safety.model_dump(mode="json") == DYNAMIC_CALIBRATION_SAFETY
    assert policy.market_regime.regime_id == "ai_after_chatgpt"
    assert policy.ranking_policy.risk_adjusted_return_weight == pytest.approx(0.25)

    raw = yaml.safe_load(
        DEFAULT_DYNAMIC_CALIBRATION_POLICY_CONFIG_PATH.read_text(encoding="utf-8")
    )
    raw["safety"]["official_target_weights_mutated"] = True
    unsafe_path = tmp_path / "unsafe_dynamic_calibration.yaml"
    unsafe_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DynamicCalibrationError):
        load_dynamic_calibration_policy_config(unsafe_path)

    raw = yaml.safe_load(
        DEFAULT_DYNAMIC_CALIBRATION_POLICY_CONFIG_PATH.read_text(encoding="utf-8")
    )
    raw["ranking_policy"]["overfit_risk_weight"] = 0.50
    invalid_path = tmp_path / "invalid_dynamic_calibration.yaml"
    invalid_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DynamicCalibrationError):
        load_dynamic_calibration_policy_config(invalid_path)


def test_dynamic_calibration_report_uses_cache_and_candidate_ranking(tmp_path: Path) -> None:
    policy = load_dynamic_calibration_policy_config()
    dynamic_policy = load_dynamic_allocation_policy_config()

    first = build_dynamic_calibration_batch_report(
        policy=policy,
        dynamic_policy=dynamic_policy,
        trend_report=None,
        cache_mode="read-write",
        cache_root=tmp_path / "cache",
        workers=1,
        top_n=3,
    )
    second = build_dynamic_calibration_batch_report(
        policy=policy,
        dynamic_policy=dynamic_policy,
        trend_report=None,
        cache_mode="read-write",
        cache_root=tmp_path / "cache",
        workers=1,
        top_n=3,
    )

    assert first["status"] == "PASS"
    assert first["candidate_pack_count"] > 0
    assert first["production_effect"] == "none"
    assert first["official_target_weights_mutated"] is False
    assert first["summary"]["full_robustness_backtest_required"] is True
    assert first["top_candidate_packs"][0]["ranking"]["return_only_ranking_blocked"] is True
    assert "ranking_components" in first["top_candidate_packs"][0]["ranking"]
    assert {"trend_score", "allocation_path", "dynamic_backtest"}.issubset(
        set(first["cache_summary"]["layers"])
    )
    assert second["cache_summary"]["cache_hit_count"] > 0
    assert second["cache_summary"]["cache_hit_rate"] > 0


def test_dynamic_calibration_cli_validate_run_and_reader_brief(tmp_path: Path) -> None:
    runner = CliRunner()
    validate_result = runner.invoke(
        etf_app,
        [
            "dynamic-calibration",
            "validate",
            "--output-dir",
            str(tmp_path / "validation"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert validate_result.exit_code == 0, validate_result.output
    assert "status=PASS" in validate_result.output

    run_result = runner.invoke(
        etf_app,
        [
            "dynamic-calibration",
            "run",
            "--pack",
            "dynamic_etf_test",
            "--no-latest-trend-report",
            "--cache",
            "read-write",
            "--cache-root",
            str(tmp_path / "cache"),
            "--workers",
            "1",
            "--top",
            "3",
            "--candidate-output-dir",
            str(tmp_path / "candidates"),
            "--report-output-dir",
            str(tmp_path / "reports"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert run_result.exit_code == 0, run_result.output
    assert "candidate_pack_count=" in run_result.output
    assert "full_robustness_backtest_required=true" in run_result.output
    assert "official_target_weights_mutated=false" in run_result.output

    report_path = next((tmp_path / "reports").glob("dynamic-calibration-report_*.json"))
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["candidate_pack_count"] > 0

    summary = reader_brief._etf_dynamic_calibration_summary(
        {"reports": [_report_record("etf_dynamic_calibration_report", report_path)]}
    )
    assert summary["availability"] == "AVAILABLE"
    assert summary["top_candidate"] == payload["summary"]["top_dynamic_candidate_pack_id"]
    assert summary["safety_status"].startswith("observe_only=true")
    assert summary["full_robustness_backtest_required"] is True

    missing = reader_brief._etf_dynamic_calibration_summary({"reports": []})
    assert missing["availability"] == "MISSING"


def test_dynamic_calibration_validation_report_passes() -> None:
    validation = build_dynamic_calibration_validation_report()

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
    assert validation["automatic_candidate_promotion_blocked"] is True


def test_dynamic_calibration_report_writer(tmp_path: Path) -> None:
    report = build_dynamic_calibration_batch_report(
        policy=load_dynamic_calibration_policy_config(),
        dynamic_policy=load_dynamic_allocation_policy_config(),
        trend_report=None,
        cache_mode="disabled",
        workers=1,
        top_n=2,
    )
    paths = write_dynamic_calibration_report(report, output_dir=tmp_path)

    assert paths["json"].exists()
    assert paths["markdown"].exists()
    assert "Full dynamic strategy robustness" in paths["markdown"].read_text(encoding="utf-8")


def _report_record(report_id: str, path: Path) -> dict[str, object]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "latest_artifact_name": path.name,
        "artifact_date": "2026-06-05",
        "freshness_status": "FRESH",
        "artifact_status": "PASS",
        "exists": True,
        "age_days": 0,
    }
