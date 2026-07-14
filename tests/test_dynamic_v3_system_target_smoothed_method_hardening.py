from __future__ import annotations

import json

import pytest
import yaml
from dynamic_v3_system_target_helpers import (
    build_model_target_fixture,
    run_smoothed_review_chain_fixture,
    write_long_market_cache,
    write_paper_shadow_backfill_config,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_method as smoothed_method,
)


def _rewrite_json(path, payload) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _rewrite_jsonl(path, rows) -> None:
    path.write_text(
        "".join(
            json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )


def test_smoothed_target_rejects_output_tamper_and_live_policy_drift(tmp_path) -> None:
    fixture = build_model_target_fixture(tmp_path)
    config = yaml.safe_load(
        system_target.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH.read_text(encoding="utf-8")
    )
    config_path = tmp_path / "smoothed_limited_adjustment_v1.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    result = system_target.generate_smoothed_limited_target(
        target_id=fixture["target_id"],
        model_target_dir=tmp_path / "model_target",
        config_path=config_path,
        output_dir=tmp_path / "smoothed_limited",
    )
    validation = system_target.validate_smoothed_limited_artifact(
        smoothed_id=result["smoothed_id"], output_dir=tmp_path / "smoothed_limited"
    )
    assert validation["status"] == "PASS"

    weights_path = result["smoothed_dir"] / "smoothed_target_weights.jsonl"
    original_weights = weights_path.read_text(encoding="utf-8")
    rows = [json.loads(line) for line in original_weights.splitlines() if line]
    rows[0]["alpha"] = 0.6
    _rewrite_jsonl(weights_path, rows)
    tampered = system_target.validate_smoothed_limited_artifact(
        smoothed_id=result["smoothed_id"], output_dir=tmp_path / "smoothed_limited"
    )
    assert tampered["status"] == "FAIL"
    assert any(
        row["check_id"] == "content_derived_views" and row["passed"] is False
        for row in tampered["checks"]
    )
    weights_path.write_text(original_weights, encoding="utf-8")

    config["variants"]["smooth_weights_3d"]["alpha"] = 0.45
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    drifted = system_target.validate_smoothed_limited_artifact(
        smoothed_id=result["smoothed_id"], output_dir=tmp_path / "smoothed_limited"
    )
    assert drifted["status"] == "FAIL"
    assert any(
        row["check_id"] == "snapshot_and_live_inputs" and row["passed"] is False
        for row in drifted["checks"]
    )


def test_smoothed_chain_rebuilds_every_materialized_view(tmp_path) -> None:
    fixture = run_smoothed_review_chain_fixture(tmp_path)
    checks = (
        (
            fixture["smoothed"]["smoothed_backfill_dir"] / "smoothed_backfill_summary.json",
            system_target.validate_smoothed_backfill_artifact,
            {
                "backfill_id": fixture["smoothed"]["smoothed_backfill_id"],
                "output_dir": tmp_path / "smoothed_backfill",
            },
        ),
        (
            fixture["comparison"]["comparison_dir"] / "smoothed_regime_comparison.json",
            system_target.validate_smoothed_comparison_artifact,
            {
                "comparison_id": fixture["comparison"]["comparison_id"],
                "output_dir": tmp_path / "smoothed_comparison",
            },
        ),
        (
            fixture["review"]["review_dir"] / "smoothed_decision.json",
            system_target.validate_smoothed_review_artifact,
            {
                "review_id": fixture["review"]["review_id"],
                "output_dir": tmp_path / "smoothed_review",
            },
        ),
    )
    for path, validator, kwargs in checks:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["production_effect"] = "tampered"
        _rewrite_json(path, payload)
        validation = validator(**kwargs)
        assert validation["status"] == "FAIL"
        assert any(
            row["check_id"] == "content_derived_views" and row["passed"] is False
            for row in validation["checks"]
        )


def test_smoothed_comparison_rejects_cross_backfill_lineage(tmp_path) -> None:
    first = run_smoothed_review_chain_fixture(tmp_path / "first")
    prices_path, rates_path = write_long_market_cache(tmp_path / "second" / "market_cache")
    config = write_paper_shadow_backfill_config(tmp_path / "second", prices_path=prices_path)
    second = system_target.run_smoothed_backfill(
        config_path=config["config_path"],
        output_dir=tmp_path / "second" / "smoothed_backfill",
        paper_shadow_backfill_dir=tmp_path / "second" / "paper_shadow_backfill",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
    )

    with pytest.raises(
        system_target.DynamicV3SystemTargetError,
        match="backfill lineage mismatch",
    ):
        system_target.run_smoothed_comparison(
            smoothed_backfill_id=first["smoothed"]["smoothed_backfill_id"],
            baseline_backfill_id=first["smoothed"]["source_paper_shadow_backfill"][
                "backfill_id"
            ],
            risk_capped_backfill_id=second["source_risk_capped_backfill"][
                "risk_capped_backfill_id"
            ],
            smoothed_backfill_dir=tmp_path / "first" / "smoothed_backfill",
            baseline_backfill_dir=tmp_path / "first" / "paper_shadow_backfill",
            risk_capped_backfill_dir=tmp_path / "second" / "risk_capped_backfill",
            output_dir=tmp_path / "cross_lineage_comparison",
        )


def test_smoothed_missing_samples_remain_null_and_cannot_select_method() -> None:
    config = smoothed_method.load_smoothed_limited_config()
    policy = smoothed_method._evaluation_policy(config)
    metrics = smoothed_method._comparison_metrics([], [], [], policy)
    assert metrics["comparisons"]
    assert all(row["evidence_status"] == "INSUFFICIENT_DATA" for row in metrics["comparisons"])
    assert all(row["total_return_delta"] is None for row in metrics["comparisons"])
    assert all(row["conclusion"] == "INSUFFICIENT_DATA" for row in metrics["comparisons"])

    decision = smoothed_method._review_decision(
        {
            "smoothed_vs_limited_metrics": metrics,
            "smoothed_rolling_comparison": {"methods": []},
            "smoothed_stability_comparison": {"methods": []},
            "smoothing_lag_cost_analysis": {"methods": []},
        },
        {"smoothed_backfill_summary": {"data_quality": "PASS_WITH_WARNINGS"}},
        policy,
    )
    assert decision["decision"] == "DEFER"
    assert decision["recommended_method"] is None
    assert decision["secondary_method"] is None
