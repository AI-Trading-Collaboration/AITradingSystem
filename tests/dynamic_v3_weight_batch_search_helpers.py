from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from dynamic_v3_system_target_helpers import run_backfill_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_micro_search_foundation as micro
from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_diagnostics as diagnostics
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_targeted as targeted
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


def write_weight_search_space_config(
    tmp_path: Path,
    *,
    source_backfill_id: str,
) -> Path:
    payload = yaml.safe_load(
        weight_search.DEFAULT_WEIGHT_SEARCH_SPACE_CONFIG_PATH.read_text(encoding="utf-8")
    )
    payload["search"]["source_backfill_id"] = source_backfill_id
    config_path = tmp_path / "weight_search_space_v2.yaml"
    config_path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    return config_path


def run_weight_search_space_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_backfill_fixture(tmp_path)
    config_path = write_weight_search_space_config(
        tmp_path,
        source_backfill_id=fixture["backfill"]["backfill_id"],
    )
    search_space = weight_search.run_weight_search_space_validation(
        config_path=config_path,
        output_dir=tmp_path / "weight_search_space",
        generated_at=datetime(2024, 3, 2, tzinfo=UTC),
    )
    return {**fixture, "weight_search_config_path": config_path, "search_space": search_space}


def run_weight_experiment_batch2_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_weight_search_space_fixture(tmp_path)
    matrix = weight_search.build_weight_experiment_batch2(
        search_space_id=fixture["search_space"]["search_space_id"],
        source_backfill_id=fixture["backfill"]["backfill_id"],
        search_space_dir=tmp_path / "weight_search_space",
        output_dir=tmp_path / "weight_experiment_batch2",
        generated_at=datetime(2024, 3, 2, 1, tzinfo=UTC),
    )
    return {**fixture, "matrix": matrix}


def run_weight_batch_backfill_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_weight_experiment_batch2_fixture(tmp_path)
    backfill = weight_search.run_weight_batch_backfill(
        matrix_id=fixture["matrix"]["matrix_id"],
        matrix_dir=tmp_path / "weight_experiment_batch2",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "weight_batch_backfill",
        price_cache_path=fixture["prices_path"],
        rates_cache_path=fixture["rates_path"],
        generated_at=datetime(2024, 3, 3, tzinfo=UTC),
    )
    return {**fixture, "weight_backfill": backfill}


def run_weight_scorecard_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_weight_batch_backfill_fixture(tmp_path)
    scorecard = weight_search.run_weight_scorecard(
        backfill_id=fixture["weight_backfill"]["batch_backfill_id"],
        backfill_dir=tmp_path / "weight_batch_backfill",
        matrix_dir=tmp_path / "weight_experiment_batch2",
        output_dir=tmp_path / "weight_scorecard",
        generated_at=datetime(2024, 3, 4, tzinfo=UTC),
    )
    return {**fixture, "scorecard": scorecard}


def run_weight_robustness_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_weight_scorecard_fixture(tmp_path)
    robustness = weight_search.run_weight_robustness_review(
        scorecard_id=fixture["scorecard"]["scorecard_id"],
        scorecard_dir=tmp_path / "weight_scorecard",
        backfill_dir=tmp_path / "weight_batch_backfill",
        output_dir=tmp_path / "weight_robustness_review",
        generated_at=datetime(2024, 3, 5, tzinfo=UTC),
    )
    return {**fixture, "robustness": robustness}


def run_weight_adaptive_branch_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_weight_robustness_fixture(tmp_path)
    branch = weight_search.run_weight_adaptive_branch(
        scorecard_id=fixture["scorecard"]["scorecard_id"],
        robustness_id=fixture["robustness"]["robustness_id"],
        scorecard_dir=tmp_path / "weight_scorecard",
        robustness_dir=tmp_path / "weight_robustness_review",
        output_dir=tmp_path / "weight_adaptive_branch",
        generated_at=datetime(2024, 3, 6, tzinfo=UTC),
    )
    return {**fixture, "branch": branch}


def run_weight_candidate_cluster_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_weight_adaptive_branch_fixture(tmp_path)
    cluster = weight_search.run_weight_candidate_cluster(
        scorecard_id=fixture["scorecard"]["scorecard_id"],
        robustness_id=fixture["robustness"]["robustness_id"],
        scorecard_dir=tmp_path / "weight_scorecard",
        robustness_dir=tmp_path / "weight_robustness_review",
        output_dir=tmp_path / "weight_candidate_cluster",
        generated_at=datetime(2024, 3, 7, tzinfo=UTC),
    )
    return {**fixture, "cluster": cluster}


def run_weight_top_candidate_interpretation_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_weight_candidate_cluster_fixture(tmp_path)
    interpretation = weight_search.run_weight_top_candidate_interpretation(
        cluster_id=fixture["cluster"]["cluster_id"],
        cluster_dir=tmp_path / "weight_candidate_cluster",
        output_dir=tmp_path / "weight_top_candidate_interpretation",
        generated_at=datetime(2024, 3, 8, tzinfo=UTC),
    )
    return {**fixture, "interpretation": interpretation}


def run_weight_method_promotion_gate_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_weight_top_candidate_interpretation_fixture(tmp_path)
    gate = weight_search.run_weight_method_promotion_gate(
        interpretation_id=fixture["interpretation"]["interpretation_id"],
        interpretation_dir=tmp_path / "weight_top_candidate_interpretation",
        output_dir=tmp_path / "weight_method_promotion_gate",
        generated_at=datetime(2024, 3, 9, tzinfo=UTC),
    )
    return {**fixture, "promotion_gate": gate}


def run_formal_method_auto_plan_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_weight_method_promotion_gate_fixture(tmp_path)
    plan = weight_search.run_formal_method_auto_plan(
        promotion_gate_id=fixture["promotion_gate"]["promotion_gate_id"],
        promotion_gate_dir=tmp_path / "weight_method_promotion_gate",
        output_dir=tmp_path / "formal_method_auto_plan",
        generated_at=datetime(2024, 3, 10, tzinfo=UTC),
    )
    return {**fixture, "formal_plan": plan}


def run_weight_search_dashboard_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_formal_method_auto_plan_fixture(tmp_path)
    dashboard = weight_search.build_weight_search_dashboard(
        scorecard_id=fixture["scorecard"]["scorecard_id"],
        branch_id=fixture["branch"]["branch_id"],
        promotion_gate_id=fixture["promotion_gate"]["promotion_gate_id"],
        scorecard_dir=tmp_path / "weight_scorecard",
        branch_dir=tmp_path / "weight_adaptive_branch",
        promotion_gate_dir=tmp_path / "weight_method_promotion_gate",
        output_dir=tmp_path / "weight_search_dashboard",
        generated_at=datetime(2024, 3, 11, tzinfo=UTC),
    )
    return {**fixture, "dashboard": dashboard}


def run_owner_research_decision_pack_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_weight_search_dashboard_fixture(tmp_path)
    owner_pack = weight_search.build_owner_research_decision_pack(
        dashboard_id=fixture["dashboard"]["dashboard_id"],
        dashboard_dir=tmp_path / "weight_search_dashboard",
        output_dir=tmp_path / "owner_research_decision_pack",
        generated_at=datetime(2024, 3, 12, tzinfo=UTC),
    )
    return {**fixture, "owner_pack": owner_pack}


def run_no_promotion_review_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_weight_scorecard_fixture(tmp_path)
    review = weight_search.run_no_promotion_review(
        scorecard_id=fixture["scorecard"]["scorecard_id"],
        scorecard_dir=tmp_path / "weight_scorecard",
        output_dir=tmp_path / "no_promotion_review",
        generated_at=datetime(2024, 3, 13, tzinfo=UTC),
    )
    return {**fixture, "no_promotion_review": review}


def run_near_miss_candidates_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_no_promotion_review_fixture(tmp_path)
    near_miss = weight_search.extract_near_miss_candidates(
        scorecard_id=fixture["scorecard"]["scorecard_id"],
        no_promotion_review_id=fixture["no_promotion_review"]["review_id"],
        scorecard_dir=tmp_path / "weight_scorecard",
        review_dir=tmp_path / "no_promotion_review",
        output_dir=tmp_path / "near_miss_candidates",
        generated_at=datetime(2024, 3, 14, tzinfo=UTC),
    )
    return {**fixture, "near_miss": near_miss}


def run_cash_buffer_attribution_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_near_miss_candidates_fixture(tmp_path)
    attribution = weight_search.run_cash_buffer_attribution(
        scorecard_id=fixture["scorecard"]["scorecard_id"],
        near_miss_id=fixture["near_miss"]["near_miss_id"],
        variant_id="cash_buffer_10",
        scorecard_dir=tmp_path / "weight_scorecard",
        near_miss_dir=tmp_path / "near_miss_candidates",
        output_dir=tmp_path / "cash_buffer_attribution",
        generated_at=datetime(2024, 3, 15, tzinfo=UTC),
    )
    return {**fixture, "cash_buffer_attribution": attribution}


def run_search_coverage_gap_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_cash_buffer_attribution_fixture(tmp_path)
    coverage_gap = weight_search.run_search_coverage_gap(
        search_space_id=fixture["search_space"]["search_space_id"],
        near_miss_id=fixture["near_miss"]["near_miss_id"],
        cash_buffer_attribution_id=fixture["cash_buffer_attribution"]["attribution_id"],
        search_space_dir=tmp_path / "weight_search_space",
        near_miss_dir=tmp_path / "near_miss_candidates",
        attribution_dir=tmp_path / "cash_buffer_attribution",
        output_dir=tmp_path / "search_coverage_gap",
        generated_at=datetime(2024, 3, 16, tzinfo=UTC),
    )
    return {**fixture, "coverage_gap": coverage_gap}


@with_artifact_validation_session
def run_targeted_search_v3_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_search_coverage_gap_fixture(tmp_path)
    targeted_v3 = weight_search.build_targeted_search_v3(
        coverage_gap_id=fixture["coverage_gap"]["coverage_gap_id"],
        coverage_gap_dir=tmp_path / "search_coverage_gap",
        near_miss_dir=tmp_path / "near_miss_candidates",
        output_dir=tmp_path / "targeted_search_v3",
        generated_at=datetime(2024, 3, 17, tzinfo=UTC),
    )
    return {**fixture, "targeted_v3": targeted_v3}


@with_artifact_validation_session
def run_targeted_v3_backfill_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_targeted_search_v3_fixture(tmp_path)
    targeted_v3_backfill = weight_search.run_targeted_v3_backfill(
        v3_matrix_id=fixture["targeted_v3"]["v3_matrix_id"],
        v3_matrix_dir=tmp_path / "targeted_search_v3",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "targeted_v3_backfill",
        price_cache_path=fixture["prices_path"],
        rates_cache_path=fixture["rates_path"],
        generated_at=datetime(2026, 3, 18, 2, tzinfo=UTC),
    )
    return {**fixture, "targeted_v3_backfill": targeted_v3_backfill}


@with_artifact_validation_session
def run_near_miss_ab_comparison_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_targeted_v3_backfill_fixture(tmp_path)
    ab = weight_search.run_near_miss_ab_comparison(
        v3_backfill_id=fixture["targeted_v3_backfill"]["v3_backfill_id"],
        near_miss_id=fixture["near_miss"]["near_miss_id"],
        v3_backfill_dir=tmp_path / "targeted_v3_backfill",
        v3_matrix_dir=tmp_path / "targeted_search_v3",
        near_miss_dir=tmp_path / "near_miss_candidates",
        scorecard_dir=tmp_path / "weight_scorecard",
        output_dir=tmp_path / "near_miss_ab_comparison",
        generated_at=datetime(2026, 3, 19, tzinfo=UTC),
    )
    return {**fixture, "near_miss_ab": ab}


@with_artifact_validation_session
def run_promotion_threshold_sensitivity_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_near_miss_ab_comparison_fixture(tmp_path)
    sensitivity = weight_search.run_promotion_threshold_sensitivity(
        v3_backfill_id=fixture["targeted_v3_backfill"]["v3_backfill_id"],
        ab_id=fixture["near_miss_ab"]["ab_id"],
        v3_backfill_dir=tmp_path / "targeted_v3_backfill",
        v3_matrix_dir=tmp_path / "targeted_search_v3",
        ab_dir=tmp_path / "near_miss_ab_comparison",
        output_dir=tmp_path / "promotion_threshold_sensitivity",
        generated_at=datetime(2026, 3, 20, tzinfo=UTC),
    )
    return {**fixture, "sensitivity": sensitivity}


@with_artifact_validation_session
def run_candidate_promotion_v2_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_promotion_threshold_sensitivity_fixture(tmp_path)
    promotion_v2 = weight_search.run_candidate_promotion_v2(
        v3_backfill_id=fixture["targeted_v3_backfill"]["v3_backfill_id"],
        ab_id=fixture["near_miss_ab"]["ab_id"],
        sensitivity_id=fixture["sensitivity"]["sensitivity_id"],
        v3_backfill_dir=tmp_path / "targeted_v3_backfill",
        v3_matrix_dir=tmp_path / "targeted_search_v3",
        ab_dir=tmp_path / "near_miss_ab_comparison",
        sensitivity_dir=tmp_path / "promotion_threshold_sensitivity",
        output_dir=tmp_path / "candidate_promotion_v2",
        generated_at=datetime(2026, 3, 21, tzinfo=UTC),
    )
    return {**fixture, "promotion_v2": promotion_v2}


@with_artifact_validation_session
def run_next_formal_or_search_plan_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_candidate_promotion_v2_fixture(tmp_path)
    next_plan = weight_search.run_next_formal_or_search_plan(
        promotion_v2_id=fixture["promotion_v2"]["promotion_v2_id"],
        promotion_v2_dir=tmp_path / "candidate_promotion_v2",
        output_dir=tmp_path / "next_formal_or_search_plan",
        generated_at=datetime(2026, 3, 22, tzinfo=UTC),
    )
    return {**fixture, "next_plan": next_plan}


def run_gate_calibration_review_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_promotion_threshold_sensitivity_fixture(tmp_path)
    gate_calibration = weight_search.run_gate_calibration_review(
        no_promotion_review_id=fixture["no_promotion_review"]["review_id"],
        threshold_sensitivity_id=fixture["sensitivity"]["sensitivity_id"],
        review_dir=tmp_path / "no_promotion_review",
        sensitivity_dir=tmp_path / "promotion_threshold_sensitivity",
        output_dir=tmp_path / "gate_calibration_review",
        generated_at=datetime(2026, 3, 23, tzinfo=UTC),
    )
    return {**fixture, "gate_calibration": gate_calibration}


def run_scorecard_attribution_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_targeted_v3_backfill_fixture(tmp_path)
    scorecard_attribution = weight_search.run_scorecard_attribution(
        scorecard_id=fixture["scorecard"]["scorecard_id"],
        v3_backfill_id=fixture["targeted_v3_backfill"]["v3_backfill_id"],
        scorecard_dir=tmp_path / "weight_scorecard",
        v3_backfill_dir=tmp_path / "targeted_v3_backfill",
        v3_matrix_dir=tmp_path / "targeted_search_v3",
        output_dir=tmp_path / "scorecard_attribution",
        generated_at=datetime(2026, 3, 23, 1, tzinfo=UTC),
    )
    return {**fixture, "scorecard_attribution": scorecard_attribution}


def run_signal_instability_diagnosis_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_scorecard_attribution_fixture(tmp_path)
    signal_diagnosis = weight_search.run_signal_instability_diagnosis(
        scorecard_attribution_id=fixture["scorecard_attribution"][
            "scorecard_attribution_id"
        ],
        attribution_dir=tmp_path / "scorecard_attribution",
        output_dir=tmp_path / "signal_instability_diagnosis",
        generated_at=datetime(2026, 3, 24, tzinfo=UTC),
    )
    return {**fixture, "signal_diagnosis": signal_diagnosis}


def run_consensus_quality_review_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_signal_instability_diagnosis_fixture(tmp_path)
    consensus_review = weight_search.run_consensus_quality_review(
        signal_diagnosis_id=fixture["signal_diagnosis"]["signal_diagnosis_id"],
        signal_dir=tmp_path / "signal_instability_diagnosis",
        attribution_dir=tmp_path / "scorecard_attribution",
        output_dir=tmp_path / "consensus_quality_review",
        generated_at=datetime(2026, 3, 24, 1, tzinfo=UTC),
    )
    return {**fixture, "consensus_review": consensus_review}


@with_artifact_validation_session
def run_signal_diagnosis_foundation_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_weight_scorecard_fixture(tmp_path)
    diagnostics_policy_path = tmp_path / "weight_search_diagnostics_test_v1.yaml"
    diagnostics_policy = yaml.safe_load(
        diagnostics.DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_POLICY_PATH.read_text(encoding="utf-8")
    )
    diagnostics_policy["policy_metadata"][
        "policy_version"
    ] = "weight_search_diagnostics_test_v1"
    diagnostics_policy["coverage"]["maximum_targeted_v3_variants"] = 12
    compact_ranges = {
        "cash_buffer": [0.10],
        "smoothing_window": [3],
        "rebalance_threshold": [0.03],
        "top_k": [3],
    }
    for parameter_gap in diagnostics_policy["coverage"]["parameter_gap_templates"]:
        parameter = parameter_gap["parameter"]
        parameter_gap["recommended_values"] = compact_ranges[parameter]
    diagnostics_policy_path.write_text(
        yaml.safe_dump(diagnostics_policy, sort_keys=False), encoding="utf-8"
    )
    no_promotion_review = weight_search.run_no_promotion_review(
        scorecard_id=fixture["scorecard"]["scorecard_id"],
        scorecard_dir=tmp_path / "weight_scorecard",
        output_dir=tmp_path / "no_promotion_review",
        generated_at=datetime(2024, 3, 13, tzinfo=UTC),
        policy_path=diagnostics_policy_path,
    )
    near_miss = weight_search.extract_near_miss_candidates(
        scorecard_id=fixture["scorecard"]["scorecard_id"],
        no_promotion_review_id=no_promotion_review["review_id"],
        scorecard_dir=tmp_path / "weight_scorecard",
        review_dir=tmp_path / "no_promotion_review",
        output_dir=tmp_path / "near_miss_candidates",
        generated_at=datetime(2024, 3, 14, tzinfo=UTC),
        policy_path=diagnostics_policy_path,
    )
    cash_buffer_attribution = weight_search.run_cash_buffer_attribution(
        scorecard_id=fixture["scorecard"]["scorecard_id"],
        near_miss_id=near_miss["near_miss_id"],
        variant_id="cash_buffer_10",
        scorecard_dir=tmp_path / "weight_scorecard",
        near_miss_dir=tmp_path / "near_miss_candidates",
        output_dir=tmp_path / "cash_buffer_attribution",
        generated_at=datetime(2024, 3, 15, tzinfo=UTC),
        policy_path=diagnostics_policy_path,
    )
    coverage_gap = weight_search.run_search_coverage_gap(
        search_space_id=fixture["search_space"]["search_space_id"],
        near_miss_id=near_miss["near_miss_id"],
        cash_buffer_attribution_id=cash_buffer_attribution["attribution_id"],
        search_space_dir=tmp_path / "weight_search_space",
        near_miss_dir=tmp_path / "near_miss_candidates",
        attribution_dir=tmp_path / "cash_buffer_attribution",
        output_dir=tmp_path / "search_coverage_gap",
        generated_at=datetime(2024, 3, 16, tzinfo=UTC),
        policy_path=diagnostics_policy_path,
    )
    policy_path = tmp_path / "weight_search_targeted_test_v1.yaml"
    policy = yaml.safe_load(
        targeted.DEFAULT_WEIGHT_SEARCH_TARGETED_POLICY_PATH.read_text(encoding="utf-8")
    )
    policy["policy_metadata"]["policy_version"] = "weight_search_targeted_test_v1"
    policy["matrix"]["minimum_variants"] = 6
    policy["matrix"]["maximum_variants"] = 12
    policy_path.write_text(yaml.safe_dump(policy, sort_keys=False), encoding="utf-8")
    targeted_v3 = weight_search.build_targeted_search_v3(
        coverage_gap_id=coverage_gap["coverage_gap_id"],
        coverage_gap_dir=tmp_path / "search_coverage_gap",
        near_miss_dir=tmp_path / "near_miss_candidates",
        output_dir=tmp_path / "targeted_search_v3",
        generated_at=datetime(2024, 3, 17, tzinfo=UTC),
        policy_path=policy_path,
    )
    targeted_v3_backfill = weight_search.run_targeted_v3_backfill(
        v3_matrix_id=targeted_v3["v3_matrix_id"],
        v3_matrix_dir=tmp_path / "targeted_search_v3",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "targeted_v3_backfill",
        price_cache_path=fixture["prices_path"],
        rates_cache_path=fixture["rates_path"],
        generated_at=datetime(2026, 3, 18, 2, tzinfo=UTC),
        policy_path=policy_path,
    )
    near_miss_ab = weight_search.run_near_miss_ab_comparison(
        v3_backfill_id=targeted_v3_backfill["v3_backfill_id"],
        near_miss_id=near_miss["near_miss_id"],
        v3_backfill_dir=tmp_path / "targeted_v3_backfill",
        v3_matrix_dir=tmp_path / "targeted_search_v3",
        near_miss_dir=tmp_path / "near_miss_candidates",
        scorecard_dir=tmp_path / "weight_scorecard",
        output_dir=tmp_path / "near_miss_ab_comparison",
        generated_at=datetime(2026, 3, 19, tzinfo=UTC),
        policy_path=policy_path,
    )
    sensitivity = weight_search.run_promotion_threshold_sensitivity(
        v3_backfill_id=targeted_v3_backfill["v3_backfill_id"],
        ab_id=near_miss_ab["ab_id"],
        v3_backfill_dir=tmp_path / "targeted_v3_backfill",
        v3_matrix_dir=tmp_path / "targeted_search_v3",
        ab_dir=tmp_path / "near_miss_ab_comparison",
        output_dir=tmp_path / "promotion_threshold_sensitivity",
        generated_at=datetime(2026, 3, 20, tzinfo=UTC),
    )
    gate_calibration = weight_search.run_gate_calibration_review(
        no_promotion_review_id=no_promotion_review["review_id"],
        threshold_sensitivity_id=sensitivity["sensitivity_id"],
        review_dir=tmp_path / "no_promotion_review",
        sensitivity_dir=tmp_path / "promotion_threshold_sensitivity",
        output_dir=tmp_path / "gate_calibration_review",
        generated_at=datetime(2026, 3, 23, tzinfo=UTC),
    )
    scorecard_attribution = weight_search.run_scorecard_attribution(
        scorecard_id=fixture["scorecard"]["scorecard_id"],
        v3_backfill_id=targeted_v3_backfill["v3_backfill_id"],
        scorecard_dir=tmp_path / "weight_scorecard",
        v3_backfill_dir=tmp_path / "targeted_v3_backfill",
        v3_matrix_dir=tmp_path / "targeted_search_v3",
        output_dir=tmp_path / "scorecard_attribution",
        generated_at=datetime(2026, 3, 23, 1, tzinfo=UTC),
    )
    signal_diagnosis = weight_search.run_signal_instability_diagnosis(
        scorecard_attribution_id=scorecard_attribution["scorecard_attribution_id"],
        attribution_dir=tmp_path / "scorecard_attribution",
        output_dir=tmp_path / "signal_instability_diagnosis",
        generated_at=datetime(2026, 3, 24, tzinfo=UTC),
    )
    consensus_review = weight_search.run_consensus_quality_review(
        signal_diagnosis_id=signal_diagnosis["signal_diagnosis_id"],
        signal_dir=tmp_path / "signal_instability_diagnosis",
        attribution_dir=tmp_path / "scorecard_attribution",
        output_dir=tmp_path / "consensus_quality_review",
        generated_at=datetime(2026, 3, 24, 1, tzinfo=UTC),
    )
    return {
        **fixture,
        "no_promotion_review": no_promotion_review,
        "near_miss": near_miss,
        "cash_buffer_attribution": cash_buffer_attribution,
        "coverage_gap": coverage_gap,
        "targeted_v3": targeted_v3,
        "targeted_v3_backfill": targeted_v3_backfill,
        "near_miss_ab": near_miss_ab,
        "sensitivity": sensitivity,
        "gate_calibration": gate_calibration,
        "scorecard_attribution": scorecard_attribution,
        "signal_diagnosis": signal_diagnosis,
        "consensus_review": consensus_review,
    }


def run_micro_search_v4_design_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_signal_diagnosis_foundation_fixture(tmp_path)
    v4_design = micro.run_micro_search_v4_design(
        gate_calibration_id=fixture["gate_calibration"]["gate_calibration_id"],
        scorecard_attribution_id=fixture["scorecard_attribution"][
            "scorecard_attribution_id"
        ],
        signal_diagnosis_id=fixture["signal_diagnosis"]["signal_diagnosis_id"],
        consensus_review_id=fixture["consensus_review"]["consensus_review_id"],
        gate_calibration_dir=tmp_path / "gate_calibration_review",
        attribution_dir=tmp_path / "scorecard_attribution",
        signal_dir=tmp_path / "signal_instability_diagnosis",
        consensus_dir=tmp_path / "consensus_quality_review",
        output_dir=tmp_path / "micro_search_v4_design",
        generated_at=datetime(2026, 3, 25, tzinfo=UTC),
    )
    return {**fixture, "v4_design": v4_design}


def _write_micro_search_current_cache(
    tmp_path: Path,
    *,
    historical_prices_path: Path,
    historical_rates_path: Path,
) -> tuple[Path, Path]:
    """Build a current-quality cache without mutating the frozen historical source."""
    root = tmp_path / "micro_search_v4_market_cache"
    root.mkdir(parents=True, exist_ok=False)
    prices_path = root / "prices_daily.csv"
    rates_path = root / "rates_daily.csv"

    price_lines = historical_prices_path.read_text(encoding="utf-8").splitlines()
    last_price_by_ticker: dict[str, list[str]] = {}
    for line in price_lines[1:]:
        columns = line.split(",")
        if len(columns) >= 8:
            last_price_by_ticker[columns[1]] = columns
    for ticker in sorted(last_price_by_ticker):
        current = list(last_price_by_ticker[ticker])
        current[0] = "2026-03-25"
        price_lines.append(",".join(current))
    prices_path.write_text("\n".join(price_lines) + "\n", encoding="utf-8")

    rate_lines = historical_rates_path.read_text(encoding="utf-8").splitlines()
    latest_rate = rate_lines[-1].split(",")
    latest_rate[0] = "2026-03-25"
    rate_lines.append(",".join(latest_rate))
    rates_path.write_text("\n".join(rate_lines) + "\n", encoding="utf-8")
    return prices_path, rates_path


def run_micro_search_v4_backfill_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_micro_search_v4_design_fixture(tmp_path)
    current_prices_path, current_rates_path = _write_micro_search_current_cache(
        tmp_path,
        historical_prices_path=fixture["prices_path"],
        historical_rates_path=fixture["rates_path"],
    )
    v4_backfill = micro.run_micro_search_v4_backfill(
        v4_design_id=fixture["v4_design"]["v4_design_id"],
        v4_design_dir=tmp_path / "micro_search_v4_design",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "micro_search_v4_backfill",
        price_cache_path=current_prices_path,
        rates_cache_path=current_rates_path,
        generated_at=datetime(2026, 3, 25, 2, tzinfo=UTC),
    )
    return {
        **fixture,
        "v4_prices_path": current_prices_path,
        "v4_rates_path": current_rates_path,
        "v4_backfill": v4_backfill,
    }


def run_gate_calibrated_review_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_micro_search_v4_backfill_fixture(tmp_path)
    gate_review = micro.run_gate_calibrated_review(
        v4_backfill_id=fixture["v4_backfill"]["v4_backfill_id"],
        gate_calibration_id=fixture["gate_calibration"]["gate_calibration_id"],
        v4_backfill_dir=tmp_path / "micro_search_v4_backfill",
        v4_design_dir=tmp_path / "micro_search_v4_design",
        gate_calibration_dir=tmp_path / "gate_calibration_review",
        output_dir=tmp_path / "gate_calibrated_review",
        generated_at=datetime(2026, 3, 26, tzinfo=UTC),
    )
    return {**fixture, "gate_review": gate_review}


@with_artifact_validation_session
def run_signal_vs_parameter_attribution_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_gate_calibrated_review_fixture(tmp_path)
    signal_vs_parameter = micro.run_signal_vs_parameter_attribution(
        signal_diagnosis_id=fixture["signal_diagnosis"]["signal_diagnosis_id"],
        consensus_review_id=fixture["consensus_review"]["consensus_review_id"],
        gate_review_id=fixture["gate_review"]["gate_review_id"],
        signal_dir=tmp_path / "signal_instability_diagnosis",
        consensus_dir=tmp_path / "consensus_quality_review",
        gate_review_dir=tmp_path / "gate_calibrated_review",
        output_dir=tmp_path / "signal_vs_parameter_attribution",
        generated_at=datetime(2026, 3, 27, tzinfo=UTC),
    )
    return {**fixture, "signal_vs_parameter": signal_vs_parameter}


def run_micro_search_foundation_fixture(tmp_path: Path) -> dict[str, Any]:
    """Build the four CX2 artifacts once inside one validation session."""
    return run_signal_vs_parameter_attribution_fixture(tmp_path)


def run_next_research_direction_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_signal_vs_parameter_attribution_fixture(tmp_path)
    next_direction = weight_search.run_next_research_direction(
        attribution_id=fixture["signal_vs_parameter"]["signal_vs_parameter_id"],
        attribution_dir=tmp_path / "signal_vs_parameter_attribution",
        output_dir=tmp_path / "next_research_direction",
        generated_at=datetime(2026, 3, 28, tzinfo=UTC),
    )
    return {**fixture, "next_direction": next_direction}


def run_owner_research_roadmap_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_next_research_direction_fixture(tmp_path)
    owner_roadmap = weight_search.update_owner_research_roadmap(
        direction_id=fixture["next_direction"]["direction_id"],
        direction_dir=tmp_path / "next_research_direction",
        output_dir=tmp_path / "owner_research_roadmap",
        generated_at=datetime(2026, 3, 29, tzinfo=UTC),
    )
    return {**fixture, "owner_roadmap": owner_roadmap}


def run_signal_failure_taxonomy_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_micro_search_v4_backfill_fixture(tmp_path)
    taxonomy = weight_search.run_signal_failure_taxonomy_validation(
        config_path=weight_search.DEFAULT_SIGNAL_FAILURE_TAXONOMY_CONFIG_PATH,
        output_dir=tmp_path / "signal_failure_taxonomy",
        generated_at=datetime(2026, 3, 30, tzinfo=UTC),
    )
    return {**fixture, "signal_failure_taxonomy": taxonomy}


def run_candidate_signal_ledger_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_signal_failure_taxonomy_fixture(tmp_path)
    ledger = weight_search.build_candidate_signal_ledger(
        taxonomy_id=fixture["signal_failure_taxonomy"]["taxonomy_id"],
        source_backfill_id=fixture["v4_backfill"]["v4_backfill_id"],
        taxonomy_dir=tmp_path / "signal_failure_taxonomy",
        source_backfill_dir=tmp_path / "micro_search_v4_backfill",
        v4_design_dir=tmp_path / "micro_search_v4_design",
        signal_dir=tmp_path / "signal_instability_diagnosis",
        consensus_dir=tmp_path / "consensus_quality_review",
        output_dir=tmp_path / "candidate_signal_ledger",
        generated_at=datetime(2026, 3, 31, tzinfo=UTC),
    )
    return {**fixture, "candidate_signal_ledger": ledger}


def run_signal_churn_root_cause_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_candidate_signal_ledger_fixture(tmp_path)
    root_cause = weight_search.run_signal_churn_root_cause_review(
        ledger_id=fixture["candidate_signal_ledger"]["ledger_id"],
        ledger_dir=tmp_path / "candidate_signal_ledger",
        output_dir=tmp_path / "signal_churn_root_cause",
        generated_at=datetime(2026, 4, 1, tzinfo=UTC),
    )
    return {**fixture, "signal_churn_root_cause": root_cause}


def run_regime_mismatch_attribution_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_signal_churn_root_cause_fixture(tmp_path)
    mismatch = weight_search.run_regime_mismatch_attribution(
        ledger_id=fixture["candidate_signal_ledger"]["ledger_id"],
        ledger_dir=tmp_path / "candidate_signal_ledger",
        output_dir=tmp_path / "regime_mismatch_attribution",
        generated_at=datetime(2026, 4, 2, tzinfo=UTC),
    )
    return {**fixture, "regime_mismatch_attribution": mismatch}


def run_candidate_quality_filter_design_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_regime_mismatch_attribution_fixture(tmp_path)
    filter_design = weight_search.run_candidate_quality_filter_design(
        root_cause_id=fixture["signal_churn_root_cause"]["root_cause_id"],
        mismatch_id=fixture["regime_mismatch_attribution"]["mismatch_id"],
        root_cause_dir=tmp_path / "signal_churn_root_cause",
        mismatch_dir=tmp_path / "regime_mismatch_attribution",
        output_dir=tmp_path / "candidate_quality_filter_design",
        generated_at=datetime(2026, 4, 3, tzinfo=UTC),
    )
    return {**fixture, "candidate_quality_filter_design": filter_design}


def run_filtered_candidate_backfill_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_candidate_quality_filter_design_fixture(tmp_path)
    filtered_backfill = weight_search.run_filtered_candidate_backfill(
        filter_design_id=fixture["candidate_quality_filter_design"]["filter_design_id"],
        filter_design_dir=tmp_path / "candidate_quality_filter_design",
        ledger_dir=tmp_path / "candidate_signal_ledger",
        output_dir=tmp_path / "filtered_candidate_backfill",
        generated_at=datetime(2026, 4, 4, tzinfo=UTC),
    )
    return {**fixture, "filtered_candidate_backfill": filtered_backfill}


def run_filtered_vs_original_comparison_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_filtered_candidate_backfill_fixture(tmp_path)
    comparison = weight_search.run_filtered_vs_original_comparison(
        filtered_backfill_id=fixture["filtered_candidate_backfill"]["filtered_backfill_id"],
        filtered_backfill_dir=tmp_path / "filtered_candidate_backfill",
        output_dir=tmp_path / "filtered_vs_original_comparison",
        generated_at=datetime(2026, 4, 5, tzinfo=UTC),
    )
    return {**fixture, "filtered_vs_original_comparison": comparison}


def run_signal_gate_experiment_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_filtered_vs_original_comparison_fixture(tmp_path)
    gate_experiment = weight_search.run_signal_gate_experiment(
        filter_design_id=fixture["candidate_quality_filter_design"]["filter_design_id"],
        filter_design_dir=tmp_path / "candidate_quality_filter_design",
        ledger_dir=tmp_path / "candidate_signal_ledger",
        output_dir=tmp_path / "signal_gate_experiment",
        generated_at=datetime(2026, 4, 6, tzinfo=UTC),
    )
    return {**fixture, "signal_gate_experiment": gate_experiment}


def run_filtered_candidate_promotion_review_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_signal_gate_experiment_fixture(tmp_path)
    review = weight_search.run_filtered_candidate_promotion_review(
        comparison_id=fixture["filtered_vs_original_comparison"]["comparison_id"],
        signal_gate_experiment_id=fixture["signal_gate_experiment"][
            "signal_gate_experiment_id"
        ],
        comparison_dir=tmp_path / "filtered_vs_original_comparison",
        experiment_dir=tmp_path / "signal_gate_experiment",
        output_dir=tmp_path / "filtered_candidate_promotion_review",
        generated_at=datetime(2026, 4, 7, tzinfo=UTC),
    )
    return {**fixture, "filtered_candidate_promotion_review": review}


def run_owner_signal_roadmap_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_filtered_candidate_promotion_review_fixture(tmp_path)
    roadmap = weight_search.build_owner_signal_roadmap(
        filtered_review_id=fixture["filtered_candidate_promotion_review"]["filtered_review_id"],
        review_dir=tmp_path / "filtered_candidate_promotion_review",
        output_dir=tmp_path / "owner_signal_roadmap",
        generated_at=datetime(2026, 4, 8, tzinfo=UTC),
    )
    return {**fixture, "owner_signal_roadmap": roadmap}
