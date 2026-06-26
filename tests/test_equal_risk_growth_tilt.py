from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.equal_risk_growth_tilt import (
    DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    run_balanced_core_definition_lock,
    run_balanced_core_first_observation_write,
    run_balanced_core_forward_aging_dry_run,
    run_balanced_core_idempotency_duplicate_guard,
    run_balanced_core_maturity_scoreboard_safety_gate,
    run_balanced_core_owner_launch_pack,
    run_balanced_core_watchlist_activation_contract,
    run_best_growth_tilt_candidate_deep_dive,
    run_beta_adjusted_edge_methodology_audit,
    run_dual_forward_aging_comparator_panel,
    run_dual_forward_aging_master_review,
    run_dual_forward_aging_reader_brief_safe_preview,
    run_equal_risk_cap_floor_tilt_search,
    run_equal_risk_growth_tilt_objective_contract,
    run_equal_risk_growth_tilt_ranking_tiering,
    run_equal_risk_growth_tilt_registry_review,
    run_equal_risk_growth_tilt_tradeoff_frontier,
    run_equal_risk_missed_upside_compensation_search,
    run_equal_risk_risk_budget_tilt_search,
    run_equal_risk_small_tqqq_overlay_search,
    run_equal_risk_trend_on_qqq_boost_search,
    run_equal_risk_vol_target_growth_tilt_search,
    run_growth_exploration_master_review,
    run_growth_research_framing_correction,
    run_growth_tilt_balanced_core_role_review,
    run_growth_tilt_beta_adjusted_edge_review,
    run_growth_tilt_beta_risk_budget_attribution,
    run_growth_tilt_candidate_result_summary,
    run_growth_tilt_cost_turnover_sensitivity,
    run_growth_tilt_definition_lock_versioning,
    run_growth_tilt_focused_diagnosis_master_review,
    run_growth_tilt_forward_aging_readiness_gate,
    run_growth_tilt_forward_aging_watchlist_review,
    run_growth_tilt_owner_decision_pack,
    run_growth_tilt_owner_decision_pack_real_run,
    run_growth_tilt_owner_diagnosis_pack,
    run_growth_tilt_parameter_neighbor_finalist_review,
    run_growth_tilt_period_drawdown_cost_triage,
    run_growth_tilt_period_drawdown_replay,
    run_growth_tilt_reader_brief_safety_preview,
    run_growth_tilt_real_cli_suite,
    run_growth_tilt_real_result_master_review,
    run_growth_tilt_risk_return_frontier_review,
    run_growth_tilt_tier_validation,
    run_growth_tilt_vs_equal_risk_and_qqq_final_gate,
    run_growth_tilt_vs_equal_risk_missed_upside_review,
    run_growth_tilt_watchlist_reconsideration_gate,
    run_roadmap_update_after_growth_tilt_review,
    run_vol_target_growth_tilt_local_sensitivity,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

GROWTH_TILT_REPORT_IDS = {
    "growth_research_framing_correction",
    "equal_risk_growth_tilt_objective_contract",
    "equal_risk_growth_tilt_registry_review",
    "equal_risk_cap_floor_tilt_search",
    "equal_risk_risk_budget_tilt_search",
    "equal_risk_trend_on_qqq_boost_search",
    "equal_risk_missed_upside_compensation_search",
    "equal_risk_small_tqqq_overlay_search",
    "equal_risk_vol_target_growth_tilt_search",
    "equal_risk_growth_tilt_ranking_tiering",
    "growth_tilt_beta_risk_budget_attribution",
    "growth_tilt_period_drawdown_replay",
    "growth_tilt_cost_turnover_sensitivity",
    "equal_risk_growth_tilt_tradeoff_frontier",
    "growth_tilt_definition_lock_versioning",
    "growth_tilt_forward_aging_readiness_gate",
    "growth_tilt_owner_decision_pack",
    "growth_exploration_master_review",
    "roadmap_update_after_growth_tilt_review",
    "growth_tilt_reader_brief_safety_preview",
    "growth_tilt_real_cli_suite_summary",
    "growth_tilt_candidate_result_summary",
    "growth_tilt_tier_validation",
    "growth_tilt_beta_adjusted_edge_review",
    "growth_tilt_risk_return_frontier_review",
    "growth_tilt_period_drawdown_cost_triage",
    "growth_tilt_final_gate",
    "growth_tilt_forward_aging_watchlist_review",
    "growth_tilt_owner_decision_pack_real_run",
    "growth_tilt_real_result_master_review",
    "best_growth_tilt_candidate_deep_dive",
    "vol_target_growth_tilt_local_sensitivity",
    "beta_adjusted_edge_methodology_audit",
    "growth_tilt_balanced_core_role_review",
    "growth_tilt_vs_equal_risk_missed_upside_review",
    "growth_tilt_parameter_neighbor_finalist_review",
    "growth_tilt_watchlist_reconsideration_gate",
    "growth_tilt_owner_diagnosis_pack",
    "growth_tilt_focused_diagnosis_master_review",
    "balanced_core_watchlist_activation_contract",
    "balanced_core_definition_lock",
    "balanced_core_forward_aging_dry_run",
    "balanced_core_first_observation_write",
    "balanced_core_idempotency_duplicate_guard",
    "balanced_core_maturity_scoreboard_safety_gate",
    "dual_forward_aging_comparator_panel",
    "dual_forward_aging_reader_brief_safe_preview",
    "balanced_core_owner_launch_pack",
    "dual_forward_aging_master_review",
    "balanced_core_launch_preflight",
    "balanced_core_first_observation_write_after_validation",
    "balanced_core_observation_idempotency_proof",
    "dual_forward_aging_comparator_panel_after_launch",
    "dual_forward_aging_scoreboard_safety_review",
    "dual_forward_aging_reader_brief_safe_preview_after_launch",
    "balanced_core_launch_owner_report",
    "external_validation_balanced_core_launch_master_review",
    "dual_forward_aging_monthly_monitor_contract",
}


def test_equal_risk_growth_tilt_builders_preserve_research_only_boundary(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_growth_caches(tmp_path)
    config_path = _write_small_growth_config(tmp_path)
    growth_root = tmp_path / "outputs" / "research_strategies" / "growth_components"
    roadmap_root = tmp_path / "outputs" / "research_strategies" / "roadmap"
    docs_root = tmp_path / "docs" / "research"
    owner_docs_path = docs_root / "growth_tilt_owner_decision_pack.md"
    master_docs_path = docs_root / "growth_exploration_master_review.md"
    data_kwargs = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "config_path": config_path,
        "output_root": growth_root,
        "as_of_date": as_of,
    }

    payloads = [
        run_growth_research_framing_correction(output_root=growth_root),
        run_equal_risk_growth_tilt_objective_contract(
            config_path=config_path,
            output_root=growth_root,
        ),
        run_equal_risk_growth_tilt_registry_review(
            config_path=config_path,
            output_root=growth_root,
        ),
    ]
    payloads.extend(
        builder(**data_kwargs)
        for builder in (
            run_equal_risk_cap_floor_tilt_search,
            run_equal_risk_risk_budget_tilt_search,
            run_equal_risk_trend_on_qqq_boost_search,
            run_equal_risk_missed_upside_compensation_search,
            run_equal_risk_small_tqqq_overlay_search,
            run_equal_risk_vol_target_growth_tilt_search,
            run_equal_risk_growth_tilt_ranking_tiering,
            run_growth_tilt_beta_risk_budget_attribution,
            run_growth_tilt_period_drawdown_replay,
            run_growth_tilt_cost_turnover_sensitivity,
            run_equal_risk_growth_tilt_tradeoff_frontier,
            run_growth_tilt_definition_lock_versioning,
            run_growth_tilt_forward_aging_readiness_gate,
            run_growth_tilt_reader_brief_safety_preview,
        )
    )
    payloads.extend(
        [
            run_growth_tilt_owner_decision_pack(
                **data_kwargs,
                docs_path=owner_docs_path,
            ),
            run_growth_exploration_master_review(
                **data_kwargs,
                docs_path=master_docs_path,
                owner_docs_path=owner_docs_path,
            ),
            run_roadmap_update_after_growth_tilt_review(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_path,
                rates_path=rates_path,
                config_path=config_path,
                growth_output_root=growth_root,
                output_root=roadmap_root,
                growth_master_docs_path=master_docs_path,
                growth_owner_docs_path=owner_docs_path,
                as_of_date=as_of,
            ),
        ]
    )

    payloads_by_type = {payload["report_type"]: payload for payload in payloads}
    ranking = payloads_by_type["equal_risk_growth_tilt_ranking_tiering"]
    cap_floor = payloads_by_type["equal_risk_cap_floor_tilt_search"]
    gate = payloads_by_type["growth_tilt_forward_aging_readiness_gate"]
    owner = payloads_by_type["growth_tilt_owner_decision_pack"]
    master = payloads_by_type["growth_exploration_master_review"]
    reader_preview = payloads_by_type["growth_tilt_reader_brief_safety_preview"]

    assert cap_floor["summary"]["candidate_count"] >= 1
    assert cap_floor["data_quality"]["passed"] is True
    assert str(cap_floor["requested_date_range"]).startswith("2022-12-01..")
    assert ranking["status"] in {
        "GROWTH_TILT_CANDIDATES_RANKED",
        "NO_GROWTH_TILT_EDGE",
    }
    assert ranking["summary"]["candidate_count"] >= 1
    assert gate["forward_aging_watchlist_allowed"] is False
    assert gate["status"] in {
        "GROWTH_TILT_FORWARD_AGING_REVIEWABLE",
        "GROWTH_TILT_RESEARCH_ONLY",
        "NO_GROWTH_TILT_CANDIDATE",
    }
    assert owner["owner_recommendation"] in {
        "OWNER_REVIEW_GROWTH_TILT_FORWARD_AGING_CANDIDATE",
        "KEEP_GROWTH_TILT_RESEARCH_ONLY",
        "NEED_MORE_HISTORY",
        "NO_USEFUL_GROWTH_TILT",
        "BLOCKED",
    }
    assert owner_docs_path.exists()
    assert master["status"] in {
        "GROWTH_TILT_FOUND",
        "BALANCED_CORE_CANDIDATE_FOUND",
        "CONTINUE_STRUCTURED_GROWTH_EXPLORATION",
    }
    assert master_docs_path.exists()
    assert reader_preview["prohibited_phrase_hits"] == []

    for payload in payloads:
        _assert_research_only_payload(payload)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "equal-risk-cap-floor-tilt-search",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--config",
            str(config_path),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(growth_root),
        ],
    )

    assert result.exit_code == 0, result.output
    written = json.loads(
        (growth_root / "equal_risk_cap_floor_tilt_search.json").read_text(
            encoding="utf-8"
        )
    )
    assert written["summary"]["broker_action"] == "none"


def test_growth_tilt_real_result_convergence_builders_and_cli(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_growth_caches(tmp_path)
    config_path = _write_small_growth_config(tmp_path)
    growth_root = tmp_path / "outputs" / "research_strategies" / "growth_components"
    roadmap_root = tmp_path / "outputs" / "research_strategies" / "roadmap"
    docs_root = tmp_path / "docs" / "research"
    source_owner_docs_path = docs_root / "growth_tilt_owner_decision_pack.md"
    source_master_docs_path = docs_root / "growth_exploration_master_review.md"
    real_owner_docs_path = docs_root / "growth_tilt_owner_decision_pack_real_run.md"
    real_master_docs_path = docs_root / "growth_tilt_real_result_master_review.md"
    common = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "config_path": config_path,
        "output_root": growth_root,
        "as_of_date": as_of,
    }

    suite = run_growth_tilt_real_cli_suite(
        **common,
        roadmap_output_root=roadmap_root,
        owner_docs_path=source_owner_docs_path,
        master_docs_path=source_master_docs_path,
    )
    candidate_summary = run_growth_tilt_candidate_result_summary(**common)
    tier = run_growth_tilt_tier_validation(
        **common,
        _candidate_summary_payload=candidate_summary,
    )
    beta = run_growth_tilt_beta_adjusted_edge_review(
        **common,
        _candidate_summary_payload=candidate_summary,
    )
    frontier = run_growth_tilt_risk_return_frontier_review(
        **common,
        _candidate_summary_payload=candidate_summary,
    )
    triage = run_growth_tilt_period_drawdown_cost_triage(
        **common,
        _candidate_summary_payload=candidate_summary,
    )
    final_gate = run_growth_tilt_vs_equal_risk_and_qqq_final_gate(
        **common,
        _candidate_summary_payload=candidate_summary,
        _tier_payload=tier,
        _beta_payload=beta,
        _triage_payload=triage,
    )
    watchlist = run_growth_tilt_forward_aging_watchlist_review(
        **common,
        _final_gate_payload=final_gate,
    )
    owner = run_growth_tilt_owner_decision_pack_real_run(
        **common,
        docs_path=real_owner_docs_path,
        _candidate_summary_payload=candidate_summary,
        _tier_payload=tier,
        _beta_payload=beta,
        _triage_payload=triage,
        _final_gate_payload=final_gate,
        _watchlist_payload=watchlist,
    )
    master = run_growth_tilt_real_result_master_review(
        **common,
        roadmap_output_root=roadmap_root,
        docs_path=real_master_docs_path,
        owner_docs_path=real_owner_docs_path,
        source_owner_docs_path=source_owner_docs_path,
        source_master_docs_path=source_master_docs_path,
        _suite_payload=suite,
        _candidate_summary_payload=candidate_summary,
        _tier_payload=tier,
        _beta_payload=beta,
        _triage_payload=triage,
        _final_gate_payload=final_gate,
        _watchlist_payload=watchlist,
        _owner_payload=owner,
    )

    assert suite["status"] in {
        "GROWTH_TILT_REAL_RUN_PASS",
        "GROWTH_TILT_REAL_RUN_WARN",
        "GROWTH_TILT_REAL_RUN_BLOCKED",
    }
    assert suite["summary"]["source_command_count"] == 20
    assert candidate_summary["candidate_results"]
    assert candidate_summary["candidate_by_family"]
    assert tier["status"] in {
        "GROWTH_TILT_TIER_VALIDATED",
        "GROWTH_TILT_TIER_INCONCLUSIVE",
        "NO_TIER_1_CANDIDATE",
    }
    assert beta["status"] in {
        "BETA_ADJUSTED_EDGE_MATERIAL",
        "BETA_ADJUSTED_EDGE_PRESENT",
        "BETA_EXPLAINS_EDGE",
        "EDGE_WEAK_AFTER_PENALTY",
        "BETA_ADJUSTED_EDGE_BLOCKED",
    }
    assert len(beta["benchmark_comparisons"]) >= 4
    assert frontier["non_dominated_candidate_list"]
    assert triage["triage_rows"]
    assert final_gate["broker_action"] == "none"
    assert watchlist["manual_review_required"] is True
    assert owner["owner_recommendation"] in {
        "ADD_GROWTH_TILT_TO_FORWARD_AGING",
        "KEEP_GROWTH_TILT_RESEARCH_ONLY",
        "NO_USEFUL_GROWTH_TILT",
        "NEED_MORE_RESEARCH",
        "BLOCKED",
    }
    assert master["status"] in {
        "GROWTH_TILT_FORWARD_AGING_REVIEWABLE",
        "GROWTH_TILT_RESEARCH_ONLY",
        "NO_USEFUL_GROWTH_TILT",
        "GROWTH_TILT_NEEDS_MORE_RESEARCH",
        "GROWTH_TILT_REAL_RESULT_BLOCKED",
    }
    assert real_owner_docs_path.exists()
    assert real_master_docs_path.exists()

    for payload in (
        suite,
        candidate_summary,
        tier,
        beta,
        frontier,
        triage,
        final_gate,
        watchlist,
        owner,
        master,
    ):
        _assert_research_only_payload(payload)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-candidate-result-summary",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--config",
            str(config_path),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(growth_root),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (growth_root / "growth_tilt_candidate_result_summary.json").exists()


def test_growth_tilt_focused_diagnosis_builders_and_cli(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_growth_caches(tmp_path)
    config_path = _write_small_growth_config(tmp_path)
    growth_root = tmp_path / "outputs" / "research_strategies" / "growth_components"
    docs_root = tmp_path / "docs" / "research"
    owner_docs_path = docs_root / "growth_tilt_owner_diagnosis_pack.md"
    master_docs_path = docs_root / "growth_tilt_focused_diagnosis_master_review.md"
    common = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "config_path": config_path,
        "output_root": growth_root,
        "as_of_date": as_of,
    }

    deep = run_best_growth_tilt_candidate_deep_dive(**common)
    sensitivity = run_vol_target_growth_tilt_local_sensitivity(**common)
    beta_method = run_beta_adjusted_edge_methodology_audit(
        **common,
        _deep_dive_payload=deep,
    )
    role = run_growth_tilt_balanced_core_role_review(
        **common,
        _deep_dive_payload=deep,
        _beta_method_payload=beta_method,
    )
    missed = run_growth_tilt_vs_equal_risk_missed_upside_review(
        **common,
        _deep_dive_payload=deep,
    )
    finalist = run_growth_tilt_parameter_neighbor_finalist_review(
        **common,
        _sensitivity_payload=sensitivity,
        _deep_dive_payload=deep,
        _beta_method_payload=beta_method,
        _role_payload=role,
    )
    watchlist = run_growth_tilt_watchlist_reconsideration_gate(
        **common,
        _deep_dive_payload=deep,
        _sensitivity_payload=sensitivity,
        _beta_method_payload=beta_method,
        _role_payload=role,
        _missed_payload=missed,
        _finalist_payload=finalist,
    )
    owner = run_growth_tilt_owner_diagnosis_pack(
        **common,
        docs_path=owner_docs_path,
        _deep_dive_payload=deep,
        _sensitivity_payload=sensitivity,
        _beta_method_payload=beta_method,
        _role_payload=role,
        _missed_payload=missed,
        _finalist_payload=finalist,
        _watchlist_payload=watchlist,
    )
    master = run_growth_tilt_focused_diagnosis_master_review(
        **common,
        docs_path=master_docs_path,
        owner_docs_path=owner_docs_path,
        _deep_dive_payload=deep,
        _sensitivity_payload=sensitivity,
        _beta_method_payload=beta_method,
        _role_payload=role,
        _missed_payload=missed,
        _finalist_payload=finalist,
        _watchlist_payload=watchlist,
        _owner_payload=owner,
    )

    assert deep["status"] in {
        "BEST_GROWTH_TILT_DEEP_DIVE_READY",
        "BEST_GROWTH_TILT_DEEP_DIVE_WARN",
        "BEST_GROWTH_TILT_DEEP_DIVE_BLOCKED",
    }
    assert deep["candidate_strategy_id"]
    assert deep["qqq_weight_path_summary"]["max_weight"] >= 0.0
    assert sensitivity["status"] in {
        "LOCAL_SENSITIVITY_STABLE",
        "LOCAL_VARIANT_IMPROVES_EDGE",
        "LOCAL_SENSITIVITY_FRAGILE",
        "LOCAL_SENSITIVITY_BLOCKED",
    }
    assert sensitivity["sensitivity_rows"]
    assert {
        "variant_strategy_id",
        "parameter_delta",
        "local_robustness_score",
    } <= set(sensitivity["sensitivity_rows"][0])
    assert beta_method["status"] in {
        "BETA_METHOD_CONFIRMS_WEAK_EDGE",
        "BETA_METHOD_SHOWS_TIMING_EDGE",
        "BETA_METHOD_INCONCLUSIVE",
        "BETA_METHOD_BLOCKED",
    }
    assert len(beta_method["method_rows"]) >= 6
    assert role["status"] in {
        "BALANCED_CORE_REVIEWABLE",
        "GROWTH_COMPONENT_NOT_SUPPORTED",
        "DEFENSIVE_ONLY_BETTER",
        "ROLE_INCONCLUSIVE",
        "ROLE_REVIEW_BLOCKED",
    }
    assert missed["period_rows"]
    assert finalist["status"] in {
        "BASE_CANDIDATE_REMAINS_BEST",
        "NEIGHBOR_CANDIDATE_BETTER",
        "NO_STABLE_FINALIST",
        "FINALIST_REVIEW_BLOCKED",
    }
    assert watchlist["broker_action"] == "none"
    assert owner["owner_recommendation"] in {
        "ADD_AS_GROWTH_TILT_FORWARD_AGING_CANDIDATE",
        "ADD_AS_BALANCED_CORE_FORWARD_AGING_CANDIDATE",
        "KEEP_GROWTH_TILT_RESEARCH_ONLY",
        "NO_STABLE_GROWTH_TILT_CANDIDATE",
        "NEED_MORE_HISTORY",
        "BLOCKED",
    }
    assert master["status"] in {
        "GROWTH_TILT_FORWARD_AGING_REVIEWABLE",
        "BALANCED_CORE_FORWARD_AGING_REVIEWABLE",
        "KEEP_GROWTH_TILT_RESEARCH_ONLY",
        "NO_STABLE_GROWTH_TILT_CANDIDATE",
        "GROWTH_TILT_DIAGNOSIS_BLOCKED",
    }
    assert owner_docs_path.exists()
    assert master_docs_path.exists()

    for payload in (
        deep,
        sensitivity,
        beta_method,
        role,
        missed,
        finalist,
        watchlist,
        owner,
        master,
    ):
        _assert_research_only_payload(payload)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-focused-diagnosis-master-review",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--config",
            str(config_path),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(growth_root),
            "--docs-path",
            str(master_docs_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (growth_root / "growth_tilt_focused_diagnosis_master_review.json").exists()


def test_balanced_core_forward_aging_launch_builders_and_cli(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_growth_caches(tmp_path)
    config_path = _write_small_growth_config(tmp_path)
    growth_root = tmp_path / "outputs" / "research_strategies" / "growth_components"
    roadmap_root = tmp_path / "outputs" / "research_strategies" / "roadmap"
    docs_root = tmp_path / "docs" / "research"
    common = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "config_path": config_path,
        "output_root": growth_root,
        "as_of_date": as_of,
    }
    activation_sources = _balanced_core_ready_activation_sources()

    activation = run_balanced_core_watchlist_activation_contract(
        **common,
        _master_payload=activation_sources["master"],
        _owner_payload=activation_sources["owner"],
        _watchlist_payload=activation_sources["watchlist"],
        _role_payload=activation_sources["role"],
        _finalist_payload=activation_sources["finalist"],
    )
    candidate_summary = run_growth_tilt_candidate_result_summary(**common)
    definition_lock = run_balanced_core_definition_lock(
        **common,
        _candidate_summary_payload=candidate_summary,
    )
    dry_run = run_balanced_core_forward_aging_dry_run(
        **common,
        _activation_payload=activation,
        _definition_lock_payload=definition_lock,
    )
    observation = run_balanced_core_first_observation_write(
        **common,
        _activation_payload=activation,
        _definition_lock_payload=definition_lock,
        _dry_run_payload=dry_run,
    )
    duplicate = run_balanced_core_first_observation_write(
        **common,
        decision_date=as_of,
        _dry_run_payload=dry_run,
    )
    idempotency = run_balanced_core_idempotency_duplicate_guard(
        **common,
        decision_date=as_of,
    )
    maturity = run_balanced_core_maturity_scoreboard_safety_gate(**common)
    panel = run_dual_forward_aging_comparator_panel(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        config_path=config_path,
        growth_output_root=growth_root,
        output_root=roadmap_root,
        as_of_date=as_of,
    )
    reader_preview = run_dual_forward_aging_reader_brief_safe_preview(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        config_path=config_path,
        growth_output_root=growth_root,
        output_root=roadmap_root,
        as_of_date=as_of,
        _maturity_payload=maturity,
        _panel_payload=panel,
    )
    owner = run_balanced_core_owner_launch_pack(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        config_path=config_path,
        growth_output_root=growth_root,
        output_root=roadmap_root,
        docs_path=docs_root / "balanced_core_owner_launch_pack.md",
        as_of_date=as_of,
        _activation_payload=activation,
        _definition_lock_payload=definition_lock,
        _observation_payload=observation,
        _idempotency_payload=idempotency,
        _maturity_payload=maturity,
        _panel_payload=panel,
        _reader_preview_payload=reader_preview,
    )
    master = run_dual_forward_aging_master_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        config_path=config_path,
        growth_output_root=growth_root,
        output_root=roadmap_root,
        docs_path=docs_root / "dual_forward_aging_master_review.md",
        owner_docs_path=docs_root / "balanced_core_owner_launch_pack.md",
        as_of_date=as_of,
        _owner_launch_payload=owner,
    )

    assert activation["status"] == "BALANCED_CORE_WATCHLIST_CONTRACT_READY"
    assert definition_lock["status"] == "BALANCED_CORE_DEFINITION_LOCKED"
    assert definition_lock["strategy_id"] == "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
    assert dry_run["status"] == "BALANCED_CORE_FORWARD_DRY_RUN_PASS"
    assert dry_run["observation_written"] is False
    assert dry_run["target_weight_qqq"] > 0.0
    assert dry_run["comparator_equal_risk_weights"]["QQQ"] > 0.0
    assert dry_run["comparator_100_qqq_weights"]["QQQ"] == 1.0
    assert observation["status"] == "BALANCED_CORE_FIRST_OBSERVATION_WRITTEN"
    assert observation["observation_written"] is True
    assert duplicate["status"] == "BALANCED_CORE_OBSERVATION_ALREADY_EXISTS"
    assert idempotency["status"] == "BALANCED_CORE_IDEMPOTENCY_PASS"
    assert maturity["status"] == "BALANCED_CORE_SCOREBOARD_INSUFFICIENT"
    assert maturity["scoreboard_status"] == "INSUFFICIENT"
    assert maturity["matured_20d_count"] == 0
    assert panel["status"] == "DUAL_FORWARD_PANEL_PENDING"
    assert len(panel["panel_rows"]) == 5
    assert reader_preview["status"] == "DUAL_READER_BRIEF_SAFE"
    assert reader_preview["prohibited_phrase_hits"] == []
    assert owner["owner_recommendation"] == "BALANCED_CORE_FORWARD_AGING_LAUNCHED"
    assert master["status"] == "DUAL_FORWARD_AGING_ACTIVE_RESEARCH_ONLY"
    assert (docs_root / "balanced_core_owner_launch_pack.md").exists()
    assert (docs_root / "dual_forward_aging_master_review.md").exists()
    assert (
        growth_root
        / "forward_aging_observations"
        / f"balanced_core_forward_aging_observation_{as_of.isoformat()}.json"
    ).exists()

    for payload in (
        activation,
        definition_lock,
        dry_run,
        observation,
        idempotency,
        maturity,
        panel,
        reader_preview,
        owner,
        master,
    ):
        _assert_research_only_payload(payload)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "balanced-core-maturity-scoreboard-safety-gate",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--config",
            str(config_path),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(growth_root),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (growth_root / "balanced_core_maturity_scoreboard_safety_gate.json").exists()


def test_equal_risk_growth_tilt_reports_and_registry_contracts() -> None:
    config = yaml.safe_load(DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH.read_text())
    safety = config["safety_boundary"]
    balanced_core_policy = config["research_policy"]["balanced_core_forward_aging"]
    excluded_paths = set(config["excluded_paths"])
    families = config["candidate_families"]
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}

    assert safety["production_effect"] == "none"
    assert safety["broker_action"] == "none"
    assert safety["paper_shadow_allowed"] is False
    assert safety["production_allowed"] is False
    assert safety["manual_review_required"] is True
    assert config["market_regime"]["regime_id"] == "ai_after_chatgpt"
    assert str(config["market_regime"]["default_backtest_start"]) == "2022-12-01"
    assert balanced_core_policy["policy_id"] == "balanced_core_forward_aging_safety_v1"
    assert balanced_core_policy["observation_windows"] == [5, 10, 20, 60, 120]
    assert balanced_core_policy["minimum_required_20d"] == 20
    assert balanced_core_policy["minimum_required_60d"] == 20
    assert balanced_core_policy["minimum_required_120d"] == 20
    assert "modify_original_equal_risk_qqq_sgov" in excluded_paths
    assert "tail_risk_fallback" in excluded_paths
    assert "LEAPS" in excluded_paths
    assert "Wheel" in excluded_paths
    assert {item["candidate_family"] for item in families} == {
        "cap_floor_tilt",
        "risk_budget_tilt",
        "trend_on_qqq_boost",
        "missed_upside_compensation",
        "small_tqqq_overlay",
        "vol_target_growth_tilt",
    }
    assert all(item["forward_aging_allowed"] is False for item in families)
    assert all(item["paper_shadow_allowed"] is False for item in families)
    assert all(item["production_allowed"] is False for item in families)
    assert all(item["broker_action"] == "none" for item in families)

    assert GROWTH_TILT_REPORT_IDS <= set(entries)
    for report_id in GROWTH_TILT_REPORT_IDS:
        entry = entries[report_id]
        assert entry["artifact_selection_policy"] == "latest_available"
        assert entry["required_for_daily_reading"] is False
        assert entry["production_effect"] == "none"
        assert entry["broker_action"] == "none"
        assert entry["command"].startswith("aits research strategies ")
        assert entry["artifact_globs"]


def _assert_research_only_payload(payload: dict[str, object]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["manual_review_required"] is True
    assert payload["market_regime"] == "ai_after_chatgpt"
    assert Path(payload["artifact_paths"]["json_path"]).exists()
    assert Path(payload["artifact_paths"]["markdown_path"]).exists()


def _write_small_growth_config(tmp_path: Path) -> Path:
    config = yaml.safe_load(DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH.read_text())
    policy = config["research_policy"]
    grids = policy["search_grids"]
    grids["cap_floor_tilt"] = {
        "qqq_max_weight": [0.70],
        "sgov_min_weight": [0.30],
        "rebalance": ["monthly"],
    }
    grids["risk_budget_tilt"] = {
        "qqq_risk_budget": [0.65],
        "sgov_risk_budget": [0.35],
        "vol_lookback": [60],
        "rebalance": ["monthly"],
    }
    grids["trend_on_qqq_boost"] = {
        "boost_amount": [0.10],
        "rebalance": ["monthly"],
    }
    policy["missed_upside_policy"]["thresholds"] = [0.05]
    policy["missed_upside_policy"]["compensation_amounts"] = [0.10]
    policy["missed_upside_policy"]["ramp_days"] = [10]
    grids["small_tqqq_overlay"] = {
        "max_tqqq_weight": [0.05],
        "rebalance": ["monthly"],
    }
    grids["vol_target_growth_tilt"] = {
        "target_vol_absolute": [0.15],
        "target_vol_additive_pp": [0.04],
        "vol_lookback": [120],
        "qqq_max_weight": [0.70],
        "sgov_min_weight": [0.10],
    }
    config_path = tmp_path / "equal_risk_growth_tilt_candidate_registry.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_path


def _balanced_core_ready_activation_sources() -> dict[str, dict[str, object]]:
    safety = {
        "production_effect": "none",
        "broker_action": "none",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "manual_review_required": True,
        "market_regime": "ai_after_chatgpt",
        "artifact_paths": {"json_path": "fixture.json", "markdown_path": "fixture.md"},
    }
    candidate_id = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
    return {
        "master": {
            **safety,
            "status": "BALANCED_CORE_FORWARD_AGING_REVIEWABLE",
            "summary": {"candidate_strategy_id": candidate_id},
        },
        "owner": {
            **safety,
            "status": "GROWTH_TILT_OWNER_DIAGNOSIS_PACK_READY",
            "owner_recommendation": "ADD_AS_BALANCED_CORE_FORWARD_AGING_CANDIDATE",
        },
        "watchlist": {
            **safety,
            "status": "BALANCED_CORE_WATCHLIST_REVIEWABLE",
            "candidate_strategy_id": candidate_id,
            "warning_reasons": [],
        },
        "role": {
            **safety,
            "status": "BALANCED_CORE_REVIEWABLE",
        },
        "finalist": {
            **safety,
            "status": "BASE_CANDIDATE_REMAINS_BEST",
        },
    }


def _write_growth_caches(tmp_path: Path) -> tuple[Path, Path, Path, date]:
    dates = _business_dates(date(2022, 12, 1), 760)
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    levels = {"QQQ": 280.0, "TQQQ": 22.0, "SGOV": 100.0}

    for day_index, row_date in enumerate(dates):
        qqq_return = 0.00065 + 0.0018 * math.sin(day_index / 19.0)
        if 90 <= day_index <= 125:
            qqq_return -= 0.006
        if 126 <= day_index <= 185:
            qqq_return += 0.004
        if 430 <= day_index <= 470:
            qqq_return -= 0.004
        levels["QQQ"] *= 1.0 + qqq_return
        levels["TQQQ"] *= 1.0 + qqq_return * 3.0 - 0.00025
        levels["SGOV"] *= 1.0 + 0.00016
        for ticker in ("QQQ", "TQQQ", "SGOV"):
            close = levels[ticker]
            row = (
                f"{row_date.isoformat()},{ticker},{close * 0.999:.4f},"
                f"{close * 1.002:.4f},{close * 0.998:.4f},{close:.4f},"
                f"{close:.4f},{1000000 + day_index}\n"
            )
            price_rows.append(row)
            secondary_rows.append(row)

    rate_rows = ["date,series,value\n"]
    for day_index, row_date in enumerate(dates):
        rate_rows.append(f"{row_date.isoformat()},DGS2,{4.0 + day_index * 0.0004:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DGS10,{4.2 + day_index * 0.0003:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DTWEXBGS,{120.0 + day_index * 0.01:.4f}\n")

    prices_path.write_text("".join(price_rows), encoding="utf-8")
    marketstack_path.write_text("".join(secondary_rows), encoding="utf-8")
    rates_path.write_text("".join(rate_rows), encoding="utf-8")
    return prices_path, marketstack_path, rates_path, dates[-1]


def _business_dates(start: date, count: int) -> list[date]:
    result = []
    current = start
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return result
