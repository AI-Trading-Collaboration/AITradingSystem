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
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    DYNAMIC_ROBUSTNESS_SAFETY,
    DynamicRobustnessError,
    _synthetic_validation_prices,
    build_dynamic_robustness_report,
    build_dynamic_robustness_validation_report,
    load_dynamic_robustness_policy_config,
    write_dynamic_robustness_report,
)
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle
from ai_trading_system.reports import reader_brief


def test_dynamic_robustness_policy_loads_and_rejects_unsafe(tmp_path: Path) -> None:
    policy = load_dynamic_robustness_policy_config()

    assert policy.safety.model_dump(mode="json") == DYNAMIC_ROBUSTNESS_SAFETY
    assert policy.market_regime.regime_id == "unified_primary_2021"
    assert policy.price_backtest.warmup_days >= 30

    raw = yaml.safe_load(DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["safety"]["automatic_candidate_promotion"] = True
    unsafe_path = tmp_path / "unsafe_dynamic_robustness.yaml"
    unsafe_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DynamicRobustnessError):
        load_dynamic_robustness_policy_config(unsafe_path)

    raw = yaml.safe_load(DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["score_model"]["event_risk_negative_trend_weight"] = 0.50
    invalid_path = tmp_path / "invalid_dynamic_robustness.yaml"
    invalid_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DynamicRobustnessError):
        load_dynamic_robustness_policy_config(invalid_path)


def test_dynamic_robustness_report_compares_dynamic_static_and_false_signals() -> None:
    policy = load_dynamic_robustness_policy_config()
    report = build_dynamic_robustness_report(
        prices=_synthetic_validation_prices(policy),
        etf_config=load_etf_config_bundle(),
        policy=policy,
        dynamic_policy=load_dynamic_allocation_policy_config(),
        candidate_id="dynamic-candidate-test",
        data_quality_status="VALIDATION_SAMPLE",
        data_quality_report="validation_sample",
    )

    comparison_ids = {row["comparison_id"] for row in report["comparison_table"]}
    assert {
        "dynamic_candidate",
        "static_base_candidate",
        "current_etf_baseline",
        "QQQ_buy_and_hold",
        "SPY_buy_and_hold",
        "SMH_buy_and_hold",
        "best_static_historical_candidate",
    }.issubset(comparison_ids)
    assert report["summary"]["dynamic_candidate_id"] == "dynamic-candidate-test"
    assert report["summary"]["data_quality_status"] == "VALIDATION_SAMPLE"
    assert report["validation_context"]["no_lookahead_timing"] == (
        "signal_date < execution_date < return_date"
    )
    assert report["false_signal_diagnostics"]["false_risk_off"]["event_count"] >= 0
    assert report["turnover_sensitivity"]["variant_count"] >= 4
    assert report["overfit_diagnostics"]["status"] in {"PASS", "REVIEW_REQUIRED"}
    assert report["shadow_enrollment_allowed"] is False
    assert report["official_target_weights_mutated"] is False


def test_dynamic_robustness_writer_cli_latest_and_reader_brief(tmp_path: Path) -> None:
    policy = load_dynamic_robustness_policy_config()
    report = build_dynamic_robustness_report(
        prices=_synthetic_validation_prices(policy),
        etf_config=load_etf_config_bundle(),
        policy=policy,
        dynamic_policy=load_dynamic_allocation_policy_config(),
        candidate_id="dynamic-candidate-reader",
        data_quality_status="VALIDATION_SAMPLE",
        data_quality_report="validation_sample",
    )
    paths = write_dynamic_robustness_report(report, output_dir=tmp_path / "reports")

    assert paths["json"].exists()
    assert paths["markdown"].exists()
    assert "False Signal Diagnostics" in paths["markdown"].read_text(encoding="utf-8")

    summary = reader_brief._etf_dynamic_robustness_summary(
        {"reports": [_report_record("etf_dynamic_robustness_report", paths["json"])]}
    )
    assert summary["availability"] == "AVAILABLE"
    assert summary["candidate"] == "dynamic-candidate-reader"
    assert summary["safety_status"].startswith("observe_only=true")
    assert summary["shadow_enrollment_allowed"] is False

    missing = reader_brief._etf_dynamic_robustness_summary({"reports": []})
    assert missing["availability"] == "MISSING"

    result = CliRunner().invoke(
        etf_app,
        [
            "dynamic-robustness",
            "report",
            "--latest",
            "--report-output-dir",
            str(tmp_path / "reports"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert result.exit_code == 0, result.output
    assert "status=" in result.output
    assert "official_target_weights_mutated=false" in result.output


def test_dynamic_robustness_validation_report_and_cli_pass(tmp_path: Path) -> None:
    validation = build_dynamic_robustness_validation_report()

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
    assert validation["automatic_candidate_promotion_blocked"] is True

    result = CliRunner().invoke(
        etf_app,
        [
            "dynamic-robustness",
            "validate",
            "--output-dir",
            str(tmp_path / "validation"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert result.exit_code == 0, result.output
    assert "status=PASS" in result.output


def _report_record(report_id: str, path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "latest_artifact_name": path.name,
        "artifact_date": "2026-06-05",
        "freshness_status": "FRESH",
        "artifact_status": payload.get("status", "PASS"),
        "exists": True,
        "age_days": 0,
    }
