from __future__ import annotations

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_experiment_matrix_contains_required_first_batch_variants(tmp_path) -> None:
    result = system_target.build_experiment_matrix(
        output_dir=tmp_path / "experiment_matrix",
    )

    manifest = result["manifest"]
    variants = result["variant_specs"]
    variant_ids = {row["variant_id"] for row in variants}

    assert manifest["status"] == "PASS"
    assert manifest["variant_count"] >= 15
    assert {
        "sideways_choppy_hold_previous",
        "sideways_choppy_reduce_tilt_50",
        "sideways_choppy_cooldown_5d",
        "sideways_choppy_signal_persistence_3d",
        "tech_drawdown_block_risk_increase",
        "semiconductor_pullback_block_smh_increase",
        "risk_off_only_allow_risk_reduction",
        "smooth_weights_3d",
        "smooth_weights_5d",
        "rebalance_only_if_delta_gt_3pct",
        "top_5_candidate_consensus",
        "cluster_representative_consensus",
        "median_target_weights",
        "trimmed_mean_target_weights",
        "cash_buffer_15",
    }.issubset(variant_ids)
    assert {
        "regime_gating",
        "cooldown",
        "weight_smoothing",
        "rebalance_threshold",
        "candidate_ensemble",
    }.issubset(set(manifest["families_covered"]))
    assert all(row["target_failure_modes"] for row in variants)
    assert all(row["experiment_only"] is True for row in variants)
    assert all(row["not_formal_research_method"] is True for row in variants)
    assert all(row["broker_action_allowed"] is False for row in variants)

    validation = system_target.validate_experiment_matrix_artifact(
        matrix_id=result["matrix_id"],
        output_dir=tmp_path / "experiment_matrix",
    )
    assert validation["status"] == "PASS"
