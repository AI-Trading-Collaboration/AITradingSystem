from __future__ import annotations

import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from dynamic_v3_system_target_helpers import (
    build_model_target_fixture,
    write_long_market_cache,
    write_paper_shadow_backfill_config,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_risk_capped as risk_capped,
)


def _copy_risk_policy(destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(system_target.DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH, destination)
    return destination


def _rewrite_yaml(path: Path, field: str, value: float) -> None:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload["evaluation_policy"][field] = value
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def test_risk_capped_config_report_rejects_live_policy_and_output_drift(tmp_path) -> None:
    policy = _copy_risk_policy(tmp_path / "risk_policy.yaml")
    result = system_target.build_risk_capped_limited_config_report(
        config_path=policy,
        output_dir=tmp_path / "config_report",
    )
    artifact_id = result["config_validation_id"]
    assert risk_capped.validate_risk_capped_limited_config_report_artifact(
        config_validation_id=artifact_id,
        output_dir=tmp_path / "config_report",
    )["status"] == "PASS"

    original = policy.read_bytes()
    _rewrite_yaml(policy, "acceptable_return_delta_floor", -0.04)
    assert risk_capped.validate_risk_capped_limited_config_report_artifact(
        config_validation_id=artifact_id,
        output_dir=tmp_path / "config_report",
    )["status"] == "FAIL"
    policy.write_bytes(original)

    report = result["config_dir"] / "risk_capped_config_report.md"
    report.write_text(report.read_text(encoding="utf-8") + "\ntampered\n", encoding="utf-8")
    assert risk_capped.validate_risk_capped_limited_config_report_artifact(
        config_validation_id=artifact_id,
        output_dir=tmp_path / "config_report",
    )["status"] == "FAIL"


def test_risk_capped_target_rejects_source_policy_and_output_drift(tmp_path) -> None:
    fixture = build_model_target_fixture(tmp_path)
    target_generated = datetime.fromisoformat(fixture["manifest"]["generated_at"])
    policy = _copy_risk_policy(tmp_path / "risk_policy.yaml")
    result = system_target.generate_risk_capped_limited_target(
        target_id=fixture["target_id"],
        model_target_dir=tmp_path / "model_target",
        config_path=policy,
        output_dir=tmp_path / "risk_target",
        generated_at=target_generated + timedelta(seconds=1),
    )
    artifact_id = result["risk_capped_id"]
    assert system_target.validate_risk_capped_limited_artifact(
        risk_capped_id=artifact_id,
        output_dir=tmp_path / "risk_target",
    )["status"] == "PASS"

    original_policy = policy.read_bytes()
    _rewrite_yaml(policy, "exposure_change_tolerance", 0.002)
    assert system_target.validate_risk_capped_limited_artifact(
        risk_capped_id=artifact_id,
        output_dir=tmp_path / "risk_target",
    )["status"] == "FAIL"
    policy.write_bytes(original_policy)

    summary = result["risk_capped_dir"] / "cap_reason_summary.json"
    original_summary = summary.read_bytes()
    summary.write_text("{}\n", encoding="utf-8")
    assert system_target.validate_risk_capped_limited_artifact(
        risk_capped_id=artifact_id,
        output_dir=tmp_path / "risk_target",
    )["status"] == "FAIL"
    summary.write_bytes(original_summary)

    model_weights = fixture["target_dir"] / "model_target_weights.json"
    original_weights = model_weights.read_bytes()
    model_weights.write_text("{}\n", encoding="utf-8")
    assert system_target.validate_risk_capped_limited_artifact(
        risk_capped_id=artifact_id,
        output_dir=tmp_path / "risk_target",
    )["status"] == "FAIL"
    model_weights.write_bytes(original_weights)


def test_risk_capped_chain_enforces_lineage_and_content_rebuild(tmp_path) -> None:
    prices_path, rates_path = write_long_market_cache(tmp_path / "market_cache")
    config = write_paper_shadow_backfill_config(tmp_path, prices_path=prices_path)
    generated = datetime.now(UTC)
    first = system_target.run_risk_capped_backfill(
        config_path=config["config_path"],
        output_dir=tmp_path / "risk_backfill",
        paper_shadow_backfill_dir=tmp_path / "paper_backfill",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
        generated_at=generated,
    )
    second = system_target.run_risk_capped_backfill(
        config_path=config["config_path"],
        output_dir=tmp_path / "risk_backfill",
        paper_shadow_backfill_dir=tmp_path / "paper_backfill",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
        generated_at=generated + timedelta(seconds=1),
    )

    with pytest.raises(system_target.DynamicV3SystemTargetError, match="lineage mismatch"):
        system_target.run_risk_capped_comparison(
            risk_capped_backfill_id=first["risk_capped_backfill_id"],
            baseline_backfill_id=second["source_paper_shadow_backfill"]["backfill_id"],
            risk_capped_backfill_dir=tmp_path / "risk_backfill",
            baseline_backfill_dir=tmp_path / "paper_backfill",
            output_dir=tmp_path / "comparison",
            generated_at=generated + timedelta(seconds=2),
        )

    comparison = system_target.run_risk_capped_comparison(
        risk_capped_backfill_id=first["risk_capped_backfill_id"],
        baseline_backfill_id=first["source_paper_shadow_backfill"]["backfill_id"],
        risk_capped_backfill_dir=tmp_path / "risk_backfill",
        baseline_backfill_dir=tmp_path / "paper_backfill",
        output_dir=tmp_path / "comparison",
        generated_at=generated + timedelta(seconds=2),
    )
    tech_drawdown = next(
        row
        for row in comparison["risk_capped_regime_comparison"]["regimes"]
        if row["regime"] == "tech_drawdown"
    )
    assert tech_drawdown["sample_count"] == 0
    assert tech_drawdown["return_delta_vs_limited"] is None
    assert tech_drawdown["drawdown_delta_vs_limited"] is None
    assert tech_drawdown["win_rate_vs_limited"] is None
    review = system_target.build_risk_capped_review_pack(
        comparison_id=comparison["comparison_id"],
        risk_capped_backfill_id=first["risk_capped_backfill_id"],
        comparison_dir=tmp_path / "comparison",
        risk_capped_backfill_dir=tmp_path / "risk_backfill",
        output_dir=tmp_path / "review",
        generated_at=generated + timedelta(seconds=3),
    )
    assert system_target.validate_risk_capped_backfill_artifact(
        backfill_id=first["risk_capped_backfill_id"],
        output_dir=tmp_path / "risk_backfill",
    )["status"] == "PASS"
    assert system_target.validate_risk_capped_comparison_artifact(
        comparison_id=comparison["comparison_id"],
        output_dir=tmp_path / "comparison",
    )["status"] == "PASS"
    assert system_target.validate_risk_capped_review_artifact(
        review_id=review["review_id"],
        output_dir=tmp_path / "review",
    )["status"] == "PASS"

    risk_states = first["risk_capped_backfill_dir"] / "risk_capped_method_states.jsonl"
    original_states = risk_states.read_bytes()
    risk_states.write_text("{}\n", encoding="utf-8")
    assert system_target.validate_risk_capped_backfill_artifact(
        backfill_id=first["risk_capped_backfill_id"],
        output_dir=tmp_path / "risk_backfill",
    )["status"] == "FAIL"
    assert system_target.validate_risk_capped_comparison_artifact(
        comparison_id=comparison["comparison_id"],
        output_dir=tmp_path / "comparison",
    )["status"] == "FAIL"
    risk_states.write_bytes(original_states)

    metrics = comparison["comparison_dir"] / "risk_capped_vs_limited_metrics.json"
    original_metrics = metrics.read_bytes()
    metrics.write_text("{}\n", encoding="utf-8")
    assert system_target.validate_risk_capped_comparison_artifact(
        comparison_id=comparison["comparison_id"],
        output_dir=tmp_path / "comparison",
    )["status"] == "FAIL"
    assert system_target.validate_risk_capped_review_artifact(
        review_id=review["review_id"],
        output_dir=tmp_path / "review",
    )["status"] == "FAIL"
    metrics.write_bytes(original_metrics)

    report = review["review_dir"] / "risk_capped_review_report.md"
    report.write_text(report.read_text(encoding="utf-8") + "\ntampered\n", encoding="utf-8")
    assert system_target.validate_risk_capped_review_artifact(
        review_id=review["review_id"],
        output_dir=tmp_path / "review",
    )["status"] == "FAIL"
