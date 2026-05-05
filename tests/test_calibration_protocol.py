from __future__ import annotations

from datetime import date
from pathlib import Path

import yaml
from typer.testing import CliRunner

from ai_trading_system.calibration_protocol import (
    load_calibration_protocol_manifest,
    render_calibration_protocol_report,
    validate_calibration_protocol_manifest,
)
from ai_trading_system.cli import app


def test_calibration_protocol_passes_locked_nested_manifest(tmp_path: Path) -> None:
    manifest_path = tmp_path / "calibration_protocol.yaml"
    manifest_path.write_text(yaml.safe_dump(_valid_manifest()), encoding="utf-8")
    manifest = load_calibration_protocol_manifest(manifest_path)

    report = validate_calibration_protocol_manifest(
        manifest,
        manifest_path=manifest_path,
        as_of=date(2026, 5, 6),
    )
    markdown = render_calibration_protocol_report(report)

    assert report.status == "PASS"
    assert report.error_count == 0
    assert "production_effect：none" in markdown
    assert "nested_walk_forward" in markdown


def test_calibration_protocol_rejects_global_sharpe_only_manifest(tmp_path: Path) -> None:
    manifest = _valid_manifest()
    manifest["parameter_family_scope"] = "global"
    manifest["objective_version"] = "sharpe_only_v1"
    manifest["purge_days"] = 0
    manifest["embargo_days"] = 0

    report = validate_calibration_protocol_manifest(
        manifest,
        manifest_path=tmp_path / "bad.yaml",
        as_of=date(2026, 5, 6),
    )
    codes = {issue.code for issue in report.issues}

    assert report.status == "FAIL"
    assert "disallowed_global_parameter_scope" in codes
    assert "single_metric_objective" in codes
    assert "missing_purge_or_embargo" in codes


def test_calibration_protocol_cli_writes_report(tmp_path: Path) -> None:
    manifest_path = tmp_path / "calibration_protocol.yaml"
    output_path = tmp_path / "calibration_protocol.md"
    manifest_path.write_text(yaml.safe_dump(_valid_manifest()), encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "validate-calibration-protocol",
            "--manifest-path",
            str(manifest_path),
            "--output-path",
            str(output_path),
            "--as-of",
            "2026-05-06",
        ],
    )

    assert result.exit_code == 0
    assert "调权协议校验状态：PASS" in result.output
    assert output_path.exists()
    assert "不批准 overlay" in output_path.read_text(encoding="utf-8")


def _valid_manifest() -> dict[str, object]:
    return {
        "protocol_id": "calibration_protocol_v1",
        "experiment_id": "calibration_2026_05_walk_forward",
        "git_commit": "abcdef0",
        "feature_version": "market_features_v1",
        "prompt_version": "risk_event_prereview_v1",
        "model_version": "score_model_current",
        "data_snapshot_hash": "sha256:data",
        "yaml_hash": "sha256:yaml",
        "cost_model_version": "cost_v1",
        "execution_assumption_version": "exec_v1",
        "market_regime": "ai_after_chatgpt",
        "date_range": {"start": "2022-12-01", "end": "2026-05-06"},
        "label_horizon": "20D",
        "train_validation_test_scheme": "nested_walk_forward",
        "purge_days": 20,
        "embargo_days": 5,
        "objective_version": "multi_objective_drawdown_turnover_tail_stability_v1",
        "benchmark_set": [
            "current_production_model",
            "buy_and_hold",
            "trend_only_model",
            "no_gate_model",
        ],
        "parameter_family_scope": "signal_weights",
        "parameter_search_space_hash": "sha256:search",
        "number_of_trials": 12,
        "trial_registry_ref": "outputs/reports/trial_registry.json",
        "multiple_testing_adjustment": "deflated_sharpe_and_pbo",
        "approval_owner": "system_review",
        "production_effect": "none",
    }
