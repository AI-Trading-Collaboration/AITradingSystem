from __future__ import annotations

from datetime import UTC, datetime

import pytest
import yaml
from dynamic_v3_system_target_helpers import (
    run_batch_experiment_fixture,
    run_top_variant_interpretation_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def _check_status(validation: dict[str, object], name: str) -> str:
    checks = validation.get("checks")
    assert isinstance(checks, list)
    return next(
        "PASS" if check["passed"] is True else "FAIL"
        for check in checks
        if isinstance(check, dict) and check.get("check_id") == name
    )


def test_invalid_matrix_policy_fails_before_any_formal_artifact(tmp_path) -> None:
    payload = yaml.safe_load(
        system_target.DEFAULT_WEIGHT_EXPERIMENT_MATRIX_CONFIG_PATH.read_text(encoding="utf-8")
    )
    payload["variants"][0]["transforms"][0]["type"] = "unknown_transform"
    config_path = tmp_path / "invalid_matrix.yaml"
    config_path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )
    output_dir = tmp_path / "experiment_matrix"

    with pytest.raises(system_target.DynamicV3SystemTargetError):
        system_target.build_experiment_matrix(
            config_path=config_path,
            output_dir=output_dir,
            generated_at=datetime(2026, 1, 6, 1, tzinfo=UTC),
        )

    assert not output_dir.exists()


def test_backlog_validation_detects_live_config_drift(tmp_path) -> None:
    payload = yaml.safe_load(
        system_target.DEFAULT_WEIGHT_OPTIMIZATION_HYPOTHESIS_CONFIG_PATH.read_text(encoding="utf-8")
    )
    config_path = tmp_path / "hypothesis.yaml"
    config_path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )
    backlog = system_target.build_hypothesis_backlog(
        config_path=config_path,
        output_dir=tmp_path / "hypothesis_backlog",
        generated_at=datetime(2026, 1, 6, tzinfo=UTC),
    )

    payload["policy_metadata"]["rationale"] += " drift"
    config_path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )
    validation = system_target.validate_hypothesis_backlog_artifact(
        backlog_id=backlog["backlog_id"], output_dir=tmp_path / "hypothesis_backlog"
    )

    assert validation["status"] == "FAIL"
    assert _check_status(validation, "snapshot_and_live_config") == "FAIL"
    assert _check_status(validation, "content_derived_views") == "PASS"


def test_batch_uses_real_return_window_and_nulls_empty_regimes(tmp_path) -> None:
    fixture = run_batch_experiment_fixture(tmp_path)
    batch = fixture["batch"]
    manifest = batch["manifest"]
    insufficient = [
        row
        for row in batch["variant_regime_metrics"]
        if row["regime_status"] == "INSUFFICIENT_DATA"
    ]
    no_effect = [
        row
        for row in batch["variant_performance_metrics"]
        if row["transform_effective_rebalance_count"] == 0
    ]

    assert manifest["date_start"] > manifest["requested_start_date"]
    assert min(row["date"] for row in batch["variant_weight_paths"]) == manifest["date_start"]
    assert insufficient
    assert no_effect
    assert all(row["max_transform_weight_delta"] == 0.0 for row in no_effect)
    assert all(row["sample_count"] == 0 for row in insufficient)
    assert all(
        row[field] is None
        for row in insufficient
        for field in (
            "relative_to_limited_adjustment",
            "relative_to_static_baseline",
            "drawdown_delta_vs_limited",
            "turnover_delta_vs_limited",
        )
    )


def test_batch_tamper_blocks_triage_before_output(tmp_path) -> None:
    fixture = run_batch_experiment_fixture(tmp_path)
    metrics_path = fixture["batch"]["batch_dir"] / "variant_performance_metrics.jsonl"
    metrics_path.write_text(metrics_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    validation = system_target.validate_batch_experiment_artifact(
        batch_id=fixture["batch"]["batch_id"], output_dir=tmp_path / "batch_experiment"
    )
    output_dir = tmp_path / "experiment_triage"

    assert validation["status"] == "FAIL"
    with pytest.raises(system_target.DynamicV3SystemTargetError):
        system_target.run_experiment_triage(
            batch_id=fixture["batch"]["batch_id"],
            batch_dir=tmp_path / "batch_experiment",
            matrix_dir=tmp_path / "experiment_matrix",
            output_dir=output_dir,
            generated_at=datetime(2026, 1, 6, 3, tzinfo=UTC),
        )
    assert not output_dir.exists()


def test_promotion_plan_rejects_mixed_triage_interpretation_lineage(tmp_path) -> None:
    fixture = run_top_variant_interpretation_fixture(tmp_path)
    second_triage = system_target.run_experiment_triage(
        batch_id=fixture["batch"]["batch_id"],
        batch_dir=tmp_path / "batch_experiment",
        matrix_dir=tmp_path / "experiment_matrix",
        output_dir=tmp_path / "experiment_triage",
        generated_at=datetime(2026, 1, 6, 3, 30, tzinfo=UTC),
    )
    second_interpretation = system_target.run_top_variant_interpretation(
        triage_id=second_triage["triage_id"],
        triage_dir=tmp_path / "experiment_triage",
        matrix_dir=tmp_path / "experiment_matrix",
        output_dir=tmp_path / "top_variant_interpretation",
        generated_at=datetime(2026, 1, 6, 4, 30, tzinfo=UTC),
    )
    output_dir = tmp_path / "method_promotion_plan"

    with pytest.raises(system_target.DynamicV3SystemTargetError):
        system_target.run_method_promotion_plan(
            triage_id=fixture["triage"]["triage_id"],
            interpretation_id=second_interpretation["interpretation_id"],
            triage_dir=tmp_path / "experiment_triage",
            interpretation_dir=tmp_path / "top_variant_interpretation",
            output_dir=output_dir,
            generated_at=datetime(2026, 1, 6, 5, tzinfo=UTC),
        )
    assert not output_dir.exists()
