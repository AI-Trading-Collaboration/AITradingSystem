from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from dynamic_v3_system_target_helpers import run_backfill_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


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
