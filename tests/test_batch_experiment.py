from __future__ import annotations

from dynamic_v3_system_target_helpers import run_batch_experiment_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_batch_experiment_generates_weight_paths_and_scorecard_metrics(tmp_path) -> None:
    fixture = run_batch_experiment_fixture(tmp_path)
    batch = fixture["batch"]
    manifest = batch["manifest"]

    performance = batch["variant_performance_metrics"]
    regime = batch["variant_regime_metrics"]
    stability = batch["variant_stability_metrics"]
    variants = {row["variant_id"] for row in performance}

    assert manifest["status"] == "PASS"
    assert manifest["market_regime"] == "ai_after_chatgpt"
    assert manifest["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert manifest["data_quality_as_of"] >= manifest["date_end"]
    assert manifest["variants_completed"] == manifest["variants_total"]
    assert len(variants) >= 15
    assert all("relative_to_limited_adjustment" in row for row in performance)
    assert all("regime_status" in row for row in regime)
    assert all("rolling_consistency_delta" in row for row in stability)
    assert manifest["broker_action_allowed"] is False
    assert manifest["production_effect"] == "none"

    validation = system_target.validate_batch_experiment_artifact(
        batch_id=batch["batch_id"],
        output_dir=tmp_path / "batch_experiment",
    )
    assert validation["status"] == "PASS"
