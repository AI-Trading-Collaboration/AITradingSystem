from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.cli_commands.research_execution_semantics import (
    register_execution_semantics_strategy_commands,
)
from ai_trading_system.cli_commands.research_expanded_universe import (
    register_expanded_universe_strategy_commands,
)
from ai_trading_system.cli_commands.research_external_validation import (
    register_external_validation_strategy_commands,
)
from ai_trading_system.cli_commands.research_growth_tilt import (
    register_growth_tilt_strategy_commands,
)
from ai_trading_system.controlled_growth_component_research import (
    DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    DEFAULT_GROWTH_COMPONENT_OWNER_DECISION_DOC_PATH,
    DEFAULT_GROWTH_COMPONENT_ROADMAP_DOC_PATH,
    run_beta_adjusted_growth_edge_contract,
    run_controlled_growth_component_registry_v2_review,
    run_drawdown_guarded_growth_component_search,
    run_equal_risk_and_growth_dual_track_roadmap,
    run_growth_component_beta_exposure_attribution,
    run_growth_component_cost_turnover_sensitivity,
    run_growth_component_owner_decision_pack,
    run_growth_component_period_drawdown_validation,
    run_growth_component_readiness_gate,
    run_layer2_growth_component_restart_contract,
    run_low_turnover_controlled_growth_search,
    run_research_roadmap_v2_master_review,
    run_volatility_targeted_growth_component_search,
)
from ai_trading_system.layer1_meta_policy_readiness import (
    DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    run_layer1_dataset_lineage_leakage_audit,
    run_layer1_historical_research_readiness_gate,
    run_layer1_meta_policy_master_review,
    run_layer1_naive_selector_baselines,
    run_layer1_objective_outcome_contract,
    run_layer1_policy_combiner_contract,
    run_layer1_purged_walk_forward_split_contract,
    run_layer1_reader_brief_safety_preview,
    run_layer1_research_dataset_builder,
    run_layer1_research_owner_decision_pack,
    run_layer1_selector_cost_adjusted_evaluation,
    run_layer1_selector_failure_case_review,
    run_layer1_selector_regime_period_validation,
    run_layer1_simple_rule_selector_search,
    run_layer2_best_component_label_builder,
)
from ai_trading_system.layer1_simple_rule_meta_policy import (
    DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    run_layer1_combined_simple_rule_selector_search,
    run_layer1_drawdown_rule_selector_backtest,
    run_layer1_selector_buffered_200dma_variants,
    run_layer1_selector_cost_latency_stress,
    run_layer1_selector_drawdown_episode_review,
    run_layer1_selector_forward_aging_dry_run,
    run_layer1_selector_forward_aging_watchlist_final_review,
    run_layer1_selector_forward_aging_watchlist_gate,
    run_layer1_selector_history_coverage_gap_audit,
    run_layer1_selector_hysteresis_review,
    run_layer1_selector_low_turnover_finalist_ranking,
    run_layer1_selector_low_turnover_owner_decision_pack,
    run_layer1_selector_low_turnover_ranking,
    run_layer1_selector_min_holding_cooldown_review,
    run_layer1_selector_minimum_holding_period_review,
    run_layer1_selector_monthly_only_review,
    run_layer1_selector_overfit_sensitivity_review,
    run_layer1_selector_owner_decision_pack,
    run_layer1_selector_owner_watchlist_review,
    run_layer1_selector_pause_or_continue_owner_pack,
    run_layer1_selector_period_split_validation,
    run_layer1_selector_reader_brief_preview,
    run_layer1_selector_real_result_summary,
    run_layer1_selector_recent_regime_risk_disclosure,
    run_layer1_selector_regret_attribution,
    run_layer1_selector_result_review_master,
    run_layer1_selector_soft_blend_constrained_search,
    run_layer1_selector_soft_blend_review,
    run_layer1_selector_switch_count_threshold_contract,
    run_layer1_selector_switch_quality_attribution,
    run_layer1_selector_turnover_source_diagnosis,
    run_layer1_selector_vs_component_baseline_ranking,
    run_layer1_selector_vs_simple_components_final_gate,
    run_layer1_selector_watchlist_blocker_report,
    run_layer1_simple_rule_selector_master_review,
    run_layer1_simple_rule_selector_registry_review,
    run_layer1_trend_rule_selector_backtest,
    run_layer1_volatility_rule_selector_backtest,
)
from ai_trading_system.layer2_strategy_component_readiness import (
    DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
    DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    run_layer2_anti_leakage_time_boundary_audit,
    run_layer2_common_robustness_validation,
    run_layer2_component_data_quality_check,
    run_layer2_component_definition_lock,
    run_layer2_component_distinctiveness_review,
    run_layer2_component_pool_freeze,
    run_layer2_component_readiness_matrix,
    run_layer2_component_readiness_reconciliation,
    run_layer2_forward_outcome_cube_build,
    run_layer2_historical_weight_path_build,
    run_layer2_return_cost_exposure_panel,
    run_layer2_selector_headroom_oracle_review,
    run_layer2_switching_constraint_contract,
    run_layer2_transition_cost_latency_review,
)
from ai_trading_system.qqq_plus_growth_challenger import (
    DEFAULT_QQQ_OUTPERFORMANCE_OWNER_DECISION_DOC_PATH,
    DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    run_controlled_tqqq_overlay_search,
    run_drawdown_guarded_growth_policy_search,
    run_growth_candidate_forward_aging_watchlist,
    run_growth_edge_significance_review,
    run_growth_vs_defensive_role_allocation_review,
    run_qqq_outperformance_drawdown_replay,
    run_qqq_outperformance_objective_contract,
    run_qqq_outperformance_owner_decision_pack,
    run_qqq_outperformance_period_split_validation,
    run_qqq_outperformance_ranking_report,
    run_qqq_plus_growth_candidate_registry,
    run_qqq_plus_risk_budget_review,
    run_trend_gated_leverage_policy_search,
    run_volatility_targeted_growth_policy_search,
)
from ai_trading_system.research_governance import ResearchGovernanceError
from ai_trading_system.research_roadmap_stabilization import (
    DEFAULT_LAYER1_SELECTOR_DRY_RUN_ARCHIVE_DOC_PATH,
    DEFAULT_RESEARCH_ROADMAP_MASTER_DOC_PATH,
    DEFAULT_RESEARCH_ROADMAP_OUTPUT_ROOT,
    run_equal_risk_first_maturity_monitor,
    run_equal_risk_forward_aging_daily_run_health_check,
    run_equal_risk_forward_aging_maturity_update_check,
    run_equal_risk_forward_aging_scheduler_integration,
    run_equal_risk_forward_aging_scoreboard_first_window_review,
    run_equal_risk_forward_aging_scoreboard_safety_gate,
    run_equal_risk_observation_continuity_check,
    run_equal_risk_reader_brief_live_summary,
    run_layer1_selector_dry_run_archive_report,
    run_layer1_selector_restart_condition_contract,
    run_layer2_growth_component_gap_review,
    run_research_roadmap_master_review,
)
from ai_trading_system.roadmap_v2_real_result_convergence import (
    DEFAULT_DUAL_TRACK_OWNER_DECISION_DOC_PATH,
    DEFAULT_ROADMAP_V2_REAL_RESULT_MASTER_REVIEW_DOC_PATH,
    DEFAULT_ROADMAP_V2_REAL_RESULT_OUTPUT_ROOT,
    run_controlled_growth_beta_adjusted_edge_review,
    run_controlled_growth_component_final_gate,
    run_controlled_growth_period_drawdown_cost_triage,
    run_controlled_growth_v2_candidate_summary,
    run_dual_track_owner_decision_pack,
    run_equal_risk_forward_aging_live_health_summary,
    run_equal_risk_growth_v2_real_cli_suite,
    run_roadmap_v2_real_result_master_review,
)
from ai_trading_system.simple_baseline_candidate_validation import (
    DEFAULT_SIMPLE_BASELINE_WATCHLIST_OWNER_DECISION_DOC_PATH,
    run_dynamic_vs_static_edge_significance_review,
    run_equal_risk_qqq_sgov_deep_dive,
    run_simple_baseline_drawdown_episode_review,
    run_simple_baseline_period_split_validation,
    run_simple_baseline_watchlist_owner_decision,
    run_tqqq_heavy_pause_rationale_report,
)
from ai_trading_system.simple_baseline_data_repair import (
    DEFAULT_DOWNLOAD_MANIFEST_PATH,
    run_data_repair_owner_decision_pack,
    run_data_repair_reproducibility_proof,
    run_equal_risk_result_recompute_after_data_repair,
    run_first_forward_aging_observation_dry_run,
    run_forward_aging_unblock_readiness_review,
    run_market_data_repair_manifest_audit,
    run_marketstack_ssl_failure_triage,
    run_reader_brief_forward_aging_safe_preview,
    run_sgov_total_return_data_contract,
    run_sgov_total_return_proxy_quality_review,
    run_simple_baseline_data_source_inventory,
    run_simple_baseline_post_data_repair_real_run,
    run_simple_baseline_validate_data_hardening,
    run_tqqq_cache_rebuild_validation,
    run_tqqq_challenger_revalidation_after_cache_fix,
)
from ai_trading_system.simple_baseline_forward_aging import (
    DEFAULT_FORWARD_AGING_MASTER_REVIEW_DOC_PATH,
    DEFAULT_FORWARD_AGING_OWNER_REVIEW_DOC_PATH,
    run_daily_reader_forward_aging_summary,
    run_equal_risk_qqq_sgov_policy_definition_lock,
    run_first_forward_aging_observation_write,
    run_forward_aging_idempotency_and_duplicate_guard,
    run_forward_aging_owner_launch_pack,
    run_forward_aging_scheduler_dry_run,
    run_paper_shadow_blocker_status_report,
    run_simple_baseline_absolute_return_gap_review,
    run_simple_baseline_candidate_role_assignment,
    run_simple_baseline_comparator_definition_lock,
    run_simple_baseline_forward_aging_automation_readiness,
    run_simple_baseline_forward_aging_candidate_freeze,
    run_simple_baseline_forward_aging_contract,
    run_simple_baseline_forward_aging_data_quality_gate,
    run_simple_baseline_forward_aging_master_review,
    run_simple_baseline_forward_aging_owner_review_pack,
    run_simple_baseline_forward_aging_scoreboard,
    run_simple_baseline_forward_aging_update_maturity,
    run_simple_baseline_forward_aging_write_observation,
    run_simple_baseline_paper_shadow_threshold_contract,
    run_simple_baseline_real_result_reconciliation,
    run_simple_baseline_risk_budget_review,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_MARKETSTACK_PRICES_PATH as DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_PRICES_PATH as DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_RATES_PATH as DEFAULT_SIMPLE_BASELINE_RATES_PATH,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_SIMPLE_BASELINE_MASTER_REVIEW_DOC_PATH,
    DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    run_options_next_stage_gate,
    run_qqq_sgov_baseline_backtest,
    run_simple_baseline_cost_sensitivity,
    run_simple_baseline_daily_reader_safety_summary,
    run_simple_baseline_dominance_ranking,
    run_simple_baseline_forward_aging_tracker,
    run_simple_baseline_master_review,
    run_simple_baseline_paper_shadow_readiness,
    run_simple_baseline_pit_boundary_audit,
    run_simple_baseline_portfolio_dry_run_mapper,
    run_simple_baseline_regime_review,
    run_simple_baseline_registry_review,
    run_tqqq_sgov_risk_controlled_baseline,
    run_trend_vol_allocation_policy_search,
)

console = Console()


def register_simple_baseline_strategy_commands(strategies_app: typer.Typer) -> None:
    for command_name, command in _SIMPLE_BASELINE_STRATEGY_COMMANDS:
        strategies_app.command(command_name)(command)
    register_growth_tilt_strategy_commands(strategies_app)
    register_external_validation_strategy_commands(strategies_app)
    register_execution_semantics_strategy_commands(strategies_app)
    register_expanded_universe_strategy_commands(strategies_app)


def strategies_simple_baseline_registry_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_registry_review(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline registry review", payload)


def strategies_qqq_sgov_baseline_backtest_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_qqq_sgov_baseline_backtest(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("QQQ / SGOV baseline backtest", payload)


def strategies_tqqq_sgov_risk_controlled_baseline_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tqqq_sgov_risk_controlled_baseline(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("TQQQ / SGOV risk-controlled baseline", payload)


def strategies_trend_vol_allocation_policy_search_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_trend_vol_allocation_policy_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Trend / volatility allocation policy search", payload)


def strategies_simple_baseline_dominance_ranking_command(
    qqq_sgov: Annotated[
        Path | None,
        typer.Option("--qqq-sgov", help="QQQ / SGOV baseline JSON。"),
    ] = None,
    tqqq_sgov: Annotated[
        Path | None,
        typer.Option("--tqqq-sgov", help="TQQQ / SGOV baseline JSON。"),
    ] = None,
    policy_search: Annotated[
        Path | None,
        typer.Option("--policy-search", help="Trend-vol policy search JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_dominance_ranking(
            qqq_sgov_path=qqq_sgov,
            tqqq_sgov_path=tqqq_sgov,
            policy_search_path=policy_search,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline dominance ranking", payload)


def strategies_simple_baseline_pit_boundary_audit_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_pit_boundary_audit(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline PIT boundary audit", payload)


def strategies_simple_baseline_cost_sensitivity_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_cost_sensitivity(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Simple baseline cost sensitivity", payload)


def strategies_simple_baseline_regime_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_regime_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Simple baseline regime review", payload)


def strategies_simple_baseline_forward_aging_tracker_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_tracker(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging tracker", payload)


def strategies_simple_baseline_paper_shadow_readiness_command(
    pit_audit: Annotated[
        Path | None,
        typer.Option("--pit-audit", help="PIT boundary audit JSON。"),
    ] = None,
    ranking: Annotated[
        Path | None,
        typer.Option("--ranking", help="Dominance ranking JSON。"),
    ] = None,
    regime_review: Annotated[
        Path | None,
        typer.Option("--regime-review", help="Regime review JSON。"),
    ] = None,
    cost_sensitivity: Annotated[
        Path | None,
        typer.Option("--cost-sensitivity", help="Cost sensitivity JSON。"),
    ] = None,
    forward_aging: Annotated[
        Path | None,
        typer.Option("--forward-aging", help="Forward aging JSON。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_paper_shadow_readiness(
            pit_audit_path=pit_audit,
            ranking_path=ranking,
            regime_review_path=regime_review,
            cost_sensitivity_path=cost_sensitivity,
            forward_aging_path=forward_aging,
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline paper-shadow readiness", payload)


def strategies_daily_reader_portfolio_control_safety_summary_command(
    ranking: Annotated[
        Path | None,
        typer.Option("--ranking", help="Dominance ranking JSON。"),
    ] = None,
    readiness: Annotated[
        Path | None,
        typer.Option("--readiness", help="Paper-shadow readiness JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_daily_reader_safety_summary(
            ranking_path=ranking,
            readiness_path=readiness,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Daily reader portfolio control safety summary", payload)


def strategies_simple_baseline_portfolio_dry_run_mapper_command(
    strategy_id: Annotated[
        str,
        typer.Option("--strategy-id", help="Simple baseline strategy id。"),
    ] = "qqq_80_sgov_20",
    hypothetical_portfolio_value: Annotated[
        float | None,
        typer.Option("--hypothetical-portfolio-value", help="假设组合金额。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_portfolio_dry_run_mapper(
            strategy_id=strategy_id,
            hypothetical_portfolio_value=hypothetical_portfolio_value,
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline portfolio dry-run mapper", payload)


def strategies_simple_baseline_master_review_command(
    ranking: Annotated[
        Path | None,
        typer.Option("--ranking", help="Dominance ranking JSON。"),
    ] = None,
    cost_sensitivity: Annotated[
        Path | None,
        typer.Option("--cost-sensitivity", help="Cost sensitivity JSON。"),
    ] = None,
    regime_review: Annotated[
        Path | None,
        typer.Option("--regime-review", help="Regime review JSON。"),
    ] = None,
    forward_aging: Annotated[
        Path | None,
        typer.Option("--forward-aging", help="Forward aging JSON。"),
    ] = None,
    readiness: Annotated[
        Path | None,
        typer.Option("--readiness", help="Paper-shadow readiness JSON。"),
    ] = None,
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path", help="Master review Markdown path。"),
    ] = DEFAULT_SIMPLE_BASELINE_MASTER_REVIEW_DOC_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_master_review(
            ranking_path=ranking,
            cost_sensitivity_path=cost_sensitivity,
            regime_review_path=regime_review,
            forward_aging_path=forward_aging,
            readiness_path=readiness,
            output_root=output_root,
            master_doc_path=docs_path,
        )
    )
    _print_simple_baseline_payload("Simple baseline master review", payload)


def strategies_options_next_stage_gate_command(
    master_review: Annotated[
        Path | None,
        typer.Option("--master-review", help="Simple baseline master review JSON。"),
    ] = None,
    forward_aging: Annotated[
        Path | None,
        typer.Option("--forward-aging", help="Forward aging JSON。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_options_next_stage_gate(
            master_review_path=master_review,
            forward_aging_path=forward_aging,
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Options next-stage gate", payload)


def strategies_equal_risk_qqq_sgov_deep_dive_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="分析开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选分析结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_qqq_sgov_deep_dive(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Equal-risk QQQ / SGOV deep dive", payload)


def strategies_simple_baseline_period_split_validation_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_period_split_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline period split validation", payload)


def strategies_simple_baseline_drawdown_episode_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_drawdown_episode_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline drawdown episode review", payload)


def strategies_dynamic_vs_static_edge_significance_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_dynamic_vs_static_edge_significance_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Dynamic vs static edge significance review", payload)


def strategies_tqqq_heavy_pause_rationale_report_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tqqq_heavy_pause_rationale_report(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("TQQQ-heavy pause rationale report", payload)


def strategies_simple_baseline_watchlist_owner_decision_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path", help="Owner decision Markdown path。"),
    ] = DEFAULT_SIMPLE_BASELINE_WATCHLIST_OWNER_DECISION_DOC_PATH,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_watchlist_owner_decision(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            docs_path=docs_path,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline watchlist owner decision", payload)


def strategies_simple_baseline_real_result_reconciliation_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_real_result_reconciliation(output_root=output_root)
    )
    _print_simple_baseline_payload("Simple baseline real result reconciliation", payload)


def strategies_simple_baseline_forward_aging_candidate_freeze_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_candidate_freeze(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging candidate freeze", payload)


def strategies_simple_baseline_forward_aging_contract_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_contract(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging contract", payload)


def strategies_simple_baseline_forward_aging_write_observation_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    decision_date: Annotated[
        str | None,
        typer.Option("--decision-date", help="可选 observation decision date。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_write_observation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            decision_date=_parse_optional_date(decision_date),
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging observation", payload)


def strategies_simple_baseline_forward_aging_update_maturity_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_update_maturity(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging maturity update", payload)


def strategies_simple_baseline_forward_aging_scoreboard_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_scoreboard(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging scoreboard", payload)


def strategies_equal_risk_qqq_sgov_policy_definition_lock_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_qqq_sgov_policy_definition_lock(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Equal-risk policy definition lock", payload)


def strategies_simple_baseline_comparator_definition_lock_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_comparator_definition_lock(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline comparator definition lock", payload)


def strategies_simple_baseline_forward_aging_data_quality_gate_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_data_quality_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging data quality gate", payload)


def strategies_simple_baseline_paper_shadow_threshold_contract_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_paper_shadow_threshold_contract(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline paper-shadow threshold contract", payload)


def strategies_daily_reader_forward_aging_summary_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_daily_reader_forward_aging_summary(output_root=output_root)
    )
    _print_simple_baseline_payload("Daily reader forward aging summary", payload)


def strategies_simple_baseline_risk_budget_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_risk_budget_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline risk budget review", payload)


def strategies_simple_baseline_absolute_return_gap_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_absolute_return_gap_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline absolute return gap review", payload)


def strategies_simple_baseline_candidate_role_assignment_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_candidate_role_assignment(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline candidate role assignment", payload)


def strategies_simple_baseline_forward_aging_owner_review_pack_command(
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path", help="Owner review Markdown path。"),
    ] = DEFAULT_FORWARD_AGING_OWNER_REVIEW_DOC_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_owner_review_pack(
            output_root=output_root,
            docs_path=docs_path,
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging owner review pack", payload)


def strategies_simple_baseline_forward_aging_automation_readiness_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_automation_readiness(output_root=output_root)
    )
    _print_simple_baseline_payload("Simple baseline forward aging automation readiness", payload)


def strategies_simple_baseline_forward_aging_master_review_command(
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path", help="Master review Markdown path。"),
    ] = DEFAULT_FORWARD_AGING_MASTER_REVIEW_DOC_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_master_review(
            output_root=output_root,
            docs_path=docs_path,
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging master review", payload)


def strategies_simple_baseline_data_source_inventory_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_data_source_inventory(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            manifest_path=manifest_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline data source inventory", payload)


def strategies_tqqq_cache_rebuild_and_validation_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    execute_repair: Annotated[
        bool,
        typer.Option(
            "--execute-repair/--no-execute-repair",
            help="是否调用既有 FMP repair 路径补 TQQQ。",
        ),
    ] = True,
) -> None:
    payload = _build_research_payload(
        lambda: run_tqqq_cache_rebuild_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            manifest_path=manifest_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            execute_repair=execute_repair,
        )
    )
    _print_simple_baseline_payload("TQQQ cache rebuild validation", payload)


def strategies_sgov_total_return_data_contract_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_sgov_total_return_data_contract(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            manifest_path=manifest_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("SGOV total-return data contract", payload)


def strategies_market_data_repair_manifest_audit_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_market_data_repair_manifest_audit(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            manifest_path=manifest_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Market data repair manifest audit", payload)


def strategies_simple_baseline_validate_data_hardening_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_validate_data_hardening(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            manifest_path=manifest_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline validate-data hardening", payload)


def strategies_rerun_simple_baseline_real_cli_after_data_repair_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_post_data_repair_real_run(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            manifest_path=manifest_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline post-data-repair real run", payload)


def strategies_equal_risk_result_recompute_after_data_repair_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_result_recompute_after_data_repair(
            prices_path=prices_path,
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Equal-risk result recompute after data repair", payload)


def strategies_tqqq_challenger_revalidation_after_cache_fix_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tqqq_challenger_revalidation_after_cache_fix(
            prices_path=prices_path,
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("TQQQ challenger revalidation", payload)


def strategies_forward_aging_unblock_readiness_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_forward_aging_unblock_readiness_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            manifest_path=manifest_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Forward-aging unblock readiness review", payload)


def strategies_first_forward_aging_observation_dry_run_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    decision_date: Annotated[
        str | None,
        typer.Option("--decision-date", help="Dry-run observation decision date。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_first_forward_aging_observation_dry_run(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            manifest_path=manifest_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            decision_date=_parse_optional_date(decision_date),
        )
    )
    _print_simple_baseline_payload("First forward-aging observation dry-run", payload)


def strategies_reader_brief_forward_aging_safe_preview_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_reader_brief_forward_aging_safe_preview(output_root=output_root)
    )
    _print_simple_baseline_payload("Reader Brief forward-aging safe preview", payload)


def strategies_data_repair_owner_decision_pack_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_data_repair_owner_decision_pack(output_root=output_root)
    )
    _print_simple_baseline_payload("Data repair owner decision pack", payload)


def strategies_data_repair_reproducibility_proof_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    expected_tqqq_rows: Annotated[
        int,
        typer.Option("--expected-tqqq-rows", help="TQQQ repaired expected row count。"),
    ] = 1008,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_data_repair_reproducibility_proof(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            manifest_path=manifest_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            expected_tqqq_rows=expected_tqqq_rows,
        )
    )
    _print_simple_baseline_payload("Data repair reproducibility proof", payload)


def strategies_marketstack_ssl_failure_triage_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="Marketstack failed request start date。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="Marketstack failed request end date。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_marketstack_ssl_failure_triage(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            manifest_path=manifest_path,
            output_root=output_root,
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Marketstack SSL failure triage", payload)


def strategies_sgov_total_return_proxy_quality_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_sgov_total_return_proxy_quality_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            manifest_path=manifest_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("SGOV total-return proxy quality review", payload)


def strategies_first_forward_aging_observation_write_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    decision_date: Annotated[
        str | None,
        typer.Option("--decision-date", help="Formal observation decision date。"),
    ] = None,
    owner_approved: Annotated[
        bool,
        typer.Option(
            "--owner-approved/--no-owner-approved",
            help="Owner approved research-only observation。",
        ),
    ] = True,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_first_forward_aging_observation_write(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            decision_date=_parse_optional_date(decision_date),
            owner_approved=owner_approved,
        )
    )
    _print_simple_baseline_payload("First forward-aging observation write", payload)


def strategies_forward_aging_idempotency_and_duplicate_guard_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    decision_date: Annotated[
        str | None,
        typer.Option("--decision-date", help="Observation decision date to duplicate-check。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_forward_aging_idempotency_and_duplicate_guard(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            decision_date=_parse_optional_date(decision_date),
        )
    )
    _print_simple_baseline_payload("Forward aging idempotency guard", payload)


def strategies_forward_aging_scheduler_dry_run_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="Scheduler dry-run data-quality as-of date。"),
    ] = None,
    decision_date: Annotated[
        str | None,
        typer.Option("--decision-date", help="Scheduler dry-run observation decision date。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_forward_aging_scheduler_dry_run(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            decision_date=_parse_optional_date(decision_date),
        )
    )
    _print_simple_baseline_payload("Forward aging scheduler dry-run", payload)


def strategies_paper_shadow_blocker_status_report_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_paper_shadow_blocker_status_report(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Paper-shadow blocker status report", payload)


def strategies_forward_aging_owner_launch_pack_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_forward_aging_owner_launch_pack(output_root=output_root)
    )
    _print_simple_baseline_payload("Forward aging owner launch pack", payload)


def strategies_qqq_outperformance_objective_contract_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="QQQ-plus growth candidate registry YAML。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_qqq_outperformance_objective_contract(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("QQQ outperformance objective contract", payload)


def strategies_qqq_plus_growth_candidate_registry_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="QQQ-plus growth candidate registry YAML。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_qqq_plus_growth_candidate_registry(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("QQQ-plus growth candidate registry", payload)


def strategies_controlled_tqqq_overlay_search_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="QQQ-plus growth candidate registry YAML。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_controlled_tqqq_overlay_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Controlled TQQQ overlay search", payload)


def strategies_trend_gated_leverage_policy_search_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="QQQ-plus growth candidate registry YAML。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_trend_gated_leverage_policy_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Trend-gated leverage policy search", payload)


def strategies_volatility_targeted_growth_policy_search_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="QQQ-plus growth candidate registry YAML。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_volatility_targeted_growth_policy_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Volatility-targeted growth policy search", payload)


def strategies_drawdown_guarded_growth_policy_search_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="QQQ-plus growth candidate registry YAML。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_drawdown_guarded_growth_policy_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Drawdown-guarded growth policy search", payload)


def strategies_qqq_outperformance_ranking_report_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="QQQ-plus growth candidate registry YAML。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_qqq_outperformance_ranking_report(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("QQQ outperformance ranking report", payload)


def strategies_qqq_outperformance_period_split_validation_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="QQQ-plus growth candidate registry YAML。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_qqq_outperformance_period_split_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("QQQ outperformance period split validation", payload)


def strategies_qqq_outperformance_drawdown_replay_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="QQQ-plus growth candidate registry YAML。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_qqq_outperformance_drawdown_replay(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("QQQ outperformance drawdown replay", payload)


def strategies_growth_edge_significance_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="QQQ-plus growth candidate registry YAML。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_growth_edge_significance_review(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Growth edge significance review", payload)


def strategies_growth_candidate_forward_aging_watchlist_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="QQQ-plus growth candidate registry YAML。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_growth_candidate_forward_aging_watchlist(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Growth candidate forward-aging watchlist", payload)


def strategies_qqq_plus_risk_budget_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="QQQ-plus growth candidate registry YAML。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_qqq_plus_risk_budget_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("QQQ-plus risk budget review", payload)


def strategies_growth_vs_defensive_role_allocation_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="QQQ-plus growth candidate registry YAML。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_growth_vs_defensive_role_allocation_review(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Growth vs defensive role allocation review", payload)


def strategies_qqq_outperformance_owner_decision_pack_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path", help="Owner decision Markdown 输出路径。"),
    ] = DEFAULT_QQQ_OUTPERFORMANCE_OWNER_DECISION_DOC_PATH,
) -> None:
    payload = _build_research_payload(
        lambda: run_qqq_outperformance_owner_decision_pack(
            output_root=output_root,
            docs_path=docs_path,
        )
    )
    _print_simple_baseline_payload("QQQ outperformance owner decision pack", payload)


def strategies_layer2_component_readiness_reconciliation_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool YAML。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_output_root: Annotated[
        Path,
        typer.Option("--simple-output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    growth_output_root: Annotated[
        Path,
        typer.Option("--growth-output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component readiness 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_component_readiness_reconciliation(
            config_path=config_path,
            simple_output_root=simple_output_root,
            growth_output_root=growth_output_root,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 component readiness reconciliation", payload)


def strategies_layer2_component_pool_freeze_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool YAML。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    growth_output_root: Annotated[
        Path,
        typer.Option("--growth-output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component readiness 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_component_pool_freeze(
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 component pool freeze", payload)


def strategies_layer2_component_definition_lock_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool YAML。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_output_root: Annotated[
        Path,
        typer.Option("--growth-output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component readiness 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_component_definition_lock(
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 component definition lock", payload)


def strategies_layer2_component_data_quality_check_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool YAML。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component readiness 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_component_data_quality_check(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            as_of_date=_parse_optional_date(as_of),
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 component data quality check", payload)


def strategies_layer2_component_readiness_matrix_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool YAML。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    simple_output_root: Annotated[
        Path,
        typer.Option("--simple-output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    growth_output_root: Annotated[
        Path,
        typer.Option("--growth-output-root", help="QQQ-plus growth 输出目录。"),
    ] = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component readiness 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_component_readiness_matrix(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            simple_output_root=simple_output_root,
            growth_output_root=growth_output_root,
            as_of_date=_parse_optional_date(as_of),
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 component readiness matrix", payload)


def strategies_layer2_historical_weight_path_build_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="PIT weight path 开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 PIT weight path 结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool YAML。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component readiness 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_historical_weight_path_build(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 historical weight path", payload)


def strategies_layer2_return_cost_exposure_panel_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="return/cost/exposure panel 开始日期。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 return/cost/exposure panel 结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool YAML。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component readiness 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_return_cost_exposure_panel(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 return/cost/exposure panel", payload)


def strategies_layer2_forward_outcome_cube_build_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="forward outcome cube 开始日期。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 forward outcome cube 结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool YAML。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component readiness 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_forward_outcome_cube_build(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 forward outcome cube", payload)


def strategies_layer2_anti_leakage_time_boundary_audit_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="anti-leakage audit 开始日期。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 anti-leakage audit 结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool YAML。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component readiness 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_anti_leakage_time_boundary_audit(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 anti-leakage/time-boundary audit", payload)


def strategies_layer2_common_robustness_validation_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="robustness validation 开始日期。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 robustness validation 结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool YAML。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component readiness 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_common_robustness_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 common robustness validation", payload)


def strategies_layer2_transition_cost_latency_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="transition review 开始日期。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 transition review 结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool YAML。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component readiness 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_transition_cost_latency_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 transition cost/latency review", payload)


def strategies_layer2_component_distinctiveness_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="distinctiveness review 开始日期。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 distinctiveness review 结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool YAML。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component readiness 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_component_distinctiveness_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 component distinctiveness review", payload)


def strategies_layer2_selector_headroom_oracle_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="selector headroom oracle 开始日期。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 selector headroom oracle 结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool YAML。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component readiness 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_selector_headroom_oracle_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 selector headroom oracle review", payload)


def strategies_layer2_switching_constraint_contract_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="switching contract 开始日期。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 switching contract 结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool YAML。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component readiness 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_switching_constraint_contract(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 switching constraint contract", payload)


def strategies_layer2_best_component_label_builder_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_best_component_label_builder(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 best component label builder", payload)


def strategies_layer1_policy_combiner_contract_command(
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_policy_combiner_contract(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-1 policy combiner contract", payload)


def strategies_layer1_objective_outcome_contract_command(
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_objective_outcome_contract(output_root=output_root)
    )
    _print_simple_baseline_payload("Layer-1 objective/outcome contract", payload)


def strategies_layer1_purged_walk_forward_split_contract_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_purged_walk_forward_split_contract(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        )
    )
    _print_simple_baseline_payload("Layer-1 purged walk-forward split contract", payload)


def strategies_layer1_research_dataset_builder_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_research_dataset_builder(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        )
    )
    _print_simple_baseline_payload("Layer-1 research dataset builder", payload)


def strategies_layer1_dataset_lineage_leakage_audit_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_dataset_lineage_leakage_audit(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        )
    )
    _print_simple_baseline_payload("Layer-1 dataset lineage/leakage audit", payload)


def strategies_layer1_naive_selector_baselines_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_naive_selector_baselines(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        )
    )
    _print_simple_baseline_payload("Layer-1 naive selector baselines", payload)


def strategies_layer1_simple_rule_selector_search_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_simple_rule_selector_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        )
    )
    _print_simple_baseline_payload("Layer-1 simple rule selector search", payload)


def strategies_layer1_selector_cost_adjusted_evaluation_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_selector_cost_adjusted_evaluation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        )
    )
    _print_simple_baseline_payload("Layer-1 selector cost-adjusted evaluation", payload)


def strategies_layer1_selector_regime_period_validation_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_selector_regime_period_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        )
    )
    _print_simple_baseline_payload("Layer-1 selector regime/period validation", payload)


def strategies_layer1_selector_failure_case_review_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_selector_failure_case_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        )
    )
    _print_simple_baseline_payload("Layer-1 selector failure case review", payload)


def strategies_layer1_historical_research_readiness_gate_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_historical_research_readiness_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        )
    )
    _print_simple_baseline_payload("Layer-1 historical research readiness gate", payload)


def strategies_layer1_research_owner_decision_pack_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_research_owner_decision_pack(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        )
    )
    _print_simple_baseline_payload("Layer-1 research owner decision pack", payload)


def strategies_layer1_reader_brief_safety_preview_command(
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_reader_brief_safety_preview(output_root=output_root)
    )
    _print_simple_baseline_payload("Layer-1 Reader Brief safety preview", payload)


def strategies_layer1_meta_policy_master_review_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_meta_policy_master_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        )
    )
    _print_simple_baseline_payload("Layer-1 meta-policy master review", payload)


def strategies_layer1_simple_rule_selector_registry_review_command(
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_simple_rule_selector_registry_review(
            registry_config_path=registry_config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-1 simple-rule selector registry review", payload)


def strategies_layer1_trend_rule_selector_backtest_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_trend_rule_selector_backtest(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 trend rule selector backtest",
    )
    _print_simple_baseline_payload("Layer-1 trend rule selector backtest", payload)


def strategies_layer1_volatility_rule_selector_backtest_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_volatility_rule_selector_backtest(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 volatility rule selector backtest",
    )
    _print_simple_baseline_payload("Layer-1 volatility rule selector backtest", payload)


def strategies_layer1_drawdown_rule_selector_backtest_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_drawdown_rule_selector_backtest(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 drawdown rule selector backtest",
    )
    _print_simple_baseline_payload("Layer-1 drawdown rule selector backtest", payload)


def strategies_layer1_combined_simple_rule_selector_search_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_combined_simple_rule_selector_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 combined simple-rule selector search",
    )
    _print_simple_baseline_payload("Layer-1 combined simple-rule selector search", payload)


def strategies_layer1_selector_cost_latency_stress_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_cost_latency_stress(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector cost/latency stress",
    )
    _print_simple_baseline_payload("Layer-1 selector cost/latency stress", payload)


def strategies_layer1_selector_period_split_validation_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_period_split_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector period split validation",
    )
    _print_simple_baseline_payload("Layer-1 selector period split validation", payload)


def strategies_layer1_selector_drawdown_episode_review_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_drawdown_episode_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector drawdown episode review",
    )
    _print_simple_baseline_payload("Layer-1 selector drawdown episode review", payload)


def strategies_layer1_selector_regret_attribution_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_regret_attribution(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector regret attribution",
    )
    _print_simple_baseline_payload("Layer-1 selector regret attribution", payload)


def strategies_layer1_selector_vs_component_baseline_ranking_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_vs_component_baseline_ranking(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector vs component baseline ranking",
    )
    _print_simple_baseline_payload("Layer-1 selector vs component baseline ranking", payload)


def strategies_layer1_selector_overfit_sensitivity_review_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_overfit_sensitivity_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector overfit sensitivity review",
    )
    _print_simple_baseline_payload("Layer-1 selector overfit sensitivity review", payload)


def strategies_layer1_selector_minimum_holding_period_review_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_minimum_holding_period_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector minimum holding period review",
    )
    _print_simple_baseline_payload("Layer-1 selector minimum holding period review", payload)


def strategies_layer1_selector_forward_aging_watchlist_gate_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_forward_aging_watchlist_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector forward-aging watchlist gate",
    )
    _print_simple_baseline_payload("Layer-1 selector forward-aging watchlist gate", payload)


def strategies_layer1_selector_owner_decision_pack_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_owner_decision_pack(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector owner decision pack",
    )
    _print_simple_baseline_payload("Layer-1 selector owner decision pack", payload)


def strategies_layer1_simple_rule_selector_master_review_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_simple_rule_selector_master_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 simple-rule selector master review",
    )
    _print_simple_baseline_payload("Layer-1 simple-rule selector master review", payload)


def strategies_layer1_selector_real_result_summary_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_real_result_summary(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector real result summary",
    )
    _print_simple_baseline_payload("Layer-1 selector real result summary", payload)


def strategies_layer1_selector_history_coverage_gap_audit_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_history_coverage_gap_audit(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector history coverage gap audit",
    )
    _print_simple_baseline_payload("Layer-1 selector history coverage gap audit", payload)


def strategies_layer1_selector_recent_regime_risk_disclosure_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_recent_regime_risk_disclosure(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector recent-regime risk disclosure",
    )
    _print_simple_baseline_payload("Layer-1 selector recent-regime risk disclosure", payload)


def strategies_layer1_selector_owner_watchlist_review_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_owner_watchlist_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector owner watchlist review",
    )
    _print_simple_baseline_payload("Layer-1 selector owner watchlist review", payload)


def strategies_layer1_selector_forward_aging_dry_run_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_forward_aging_dry_run(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector forward-aging dry-run",
    )
    _print_simple_baseline_payload("Layer-1 selector forward-aging dry-run", payload)


def strategies_layer1_selector_watchlist_blocker_report_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_watchlist_blocker_report(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector watchlist blocker report",
    )
    _print_simple_baseline_payload("Layer-1 selector watchlist blocker report", payload)


def strategies_layer1_selector_reader_brief_preview_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_reader_brief_preview(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector Reader Brief preview",
    )
    _print_simple_baseline_payload("Layer-1 selector Reader Brief preview", payload)


def strategies_layer1_selector_result_review_master_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_result_review_master(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector result review master",
    )
    _print_simple_baseline_payload("Layer-1 selector result review master", payload)


def strategies_layer1_selector_turnover_source_diagnosis_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_turnover_source_diagnosis(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector turnover source diagnosis",
    )
    _print_simple_baseline_payload("Layer-1 selector turnover source diagnosis", payload)


def strategies_layer1_selector_buffered_200dma_variants_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_buffered_200dma_variants(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector buffered 200DMA variants",
    )
    _print_simple_baseline_payload("Layer-1 selector buffered 200DMA variants", payload)


def strategies_layer1_selector_min_holding_cooldown_review_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_min_holding_cooldown_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector minimum holding and cooldown review",
    )
    _print_simple_baseline_payload(
        "Layer-1 selector minimum holding and cooldown review",
        payload,
    )


def strategies_layer1_selector_soft_blend_review_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_soft_blend_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector soft blend review",
    )
    _print_simple_baseline_payload("Layer-1 selector soft blend review", payload)


def strategies_layer1_selector_low_turnover_ranking_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_low_turnover_ranking(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector low-turnover ranking",
    )
    _print_simple_baseline_payload("Layer-1 selector low-turnover ranking", payload)


def strategies_layer1_selector_low_turnover_owner_decision_pack_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_low_turnover_owner_decision_pack(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector low-turnover owner decision pack",
    )
    _print_simple_baseline_payload(
        "Layer-1 selector low-turnover owner decision pack",
        payload,
    )


def strategies_layer1_selector_switch_count_threshold_contract_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_switch_count_threshold_contract(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector switch count threshold contract",
    )
    _print_simple_baseline_payload("Layer-1 selector switch count threshold contract", payload)


def strategies_layer1_selector_soft_blend_constrained_search_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_soft_blend_constrained_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector soft blend constrained search",
    )
    _print_simple_baseline_payload("Layer-1 selector soft blend constrained search", payload)


def strategies_layer1_selector_monthly_only_review_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_monthly_only_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector monthly-only review",
    )
    _print_simple_baseline_payload("Layer-1 selector monthly-only review", payload)


def strategies_layer1_selector_hysteresis_review_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_hysteresis_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector hysteresis review",
    )
    _print_simple_baseline_payload("Layer-1 selector hysteresis review", payload)


def strategies_layer1_selector_switch_quality_attribution_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_switch_quality_attribution(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector switch quality attribution",
    )
    _print_simple_baseline_payload("Layer-1 selector switch quality attribution", payload)


def strategies_layer1_selector_low_turnover_finalist_ranking_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_low_turnover_finalist_ranking(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector low-turnover finalist ranking",
    )
    _print_simple_baseline_payload("Layer-1 selector low-turnover finalist ranking", payload)


def strategies_layer1_selector_vs_simple_components_final_gate_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_vs_simple_components_final_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector vs simple components final gate",
    )
    _print_simple_baseline_payload("Layer-1 selector vs simple components final gate", payload)


def strategies_layer1_selector_forward_aging_watchlist_final_review_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_forward_aging_watchlist_final_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector forward-aging watchlist final review",
    )
    _print_simple_baseline_payload(
        "Layer-1 selector forward-aging watchlist final review",
        payload,
    )


def strategies_layer1_selector_pause_or_continue_owner_pack_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="主价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="rates cache for validate-data gate。")
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="validate-data as-of date。")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date", help="开始日期。")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date", help="结束日期。")] = None,
    config_path: Annotated[
        Path, typer.Option("--config", help="Layer-2 component pool YAML。")
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-registry-config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path, typer.Option("--registry-config", help="Layer-1 selector registry YAML。")
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。")
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path, typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。")
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _layer1_selector_research_payload(
        lambda: run_layer1_selector_pause_or_continue_owner_pack(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
        ),
        "Layer-1 selector pause or continue owner pack",
    )
    _print_simple_baseline_payload("Layer-1 selector pause or continue owner pack", payload)


def strategies_layer1_selector_dry_run_archive_report_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="研究开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选研究结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--layer2-config", help="Layer-2 component pool config。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-config", help="Simple baseline registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    registry_config_path: Annotated[
        Path,
        typer.Option("--selector-config", help="Layer-1 selector registry YAML。"),
    ] = DEFAULT_LAYER1_SELECTOR_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。"),
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path,
        typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
    archive_doc_path: Annotated[
        Path,
        typer.Option("--archive-doc-path", help="归档 Markdown 文档路径。"),
    ] = DEFAULT_LAYER1_SELECTOR_DRY_RUN_ARCHIVE_DOC_PATH,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_selector_dry_run_archive_report(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_registry_config_path=simple_registry_config_path,
            registry_config_path=registry_config_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date),
            end_date=_parse_optional_date(end_date),
            output_root=output_root,
            layer2_output_root=layer2_output_root,
            archive_doc_path=archive_doc_path,
        )
    )
    _print_simple_baseline_payload("Layer-1 selector dry-run archive report", payload)


def strategies_layer1_selector_restart_condition_contract_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-1 meta-policy 输出目录。"),
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    simple_output_root: Annotated[
        Path,
        typer.Option("--simple-output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    layer2_config_path: Annotated[
        Path,
        typer.Option("--layer2-config", help="Layer-2 component pool config。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Annotated[
        Path,
        typer.Option("--simple-config", help="Simple baseline registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer1_selector_restart_condition_contract(
            output_root=output_root,
            simple_output_root=simple_output_root,
            layer2_config_path=layer2_config_path,
            simple_registry_config_path=simple_registry_config_path,
        )
    )
    _print_simple_baseline_payload("Layer-1 selector restart condition contract", payload)


def strategies_equal_risk_forward_aging_daily_run_health_check_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_forward_aging_daily_run_health_check(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Equal-risk forward-aging daily run health check", payload)


def strategies_equal_risk_forward_aging_maturity_update_check_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_forward_aging_maturity_update_check(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Equal-risk forward-aging maturity update check", payload)


def strategies_equal_risk_forward_aging_scoreboard_first_window_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_forward_aging_scoreboard_first_window_review(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload(
        "Equal-risk forward-aging scoreboard first-window review",
        payload,
    )


def strategies_layer2_growth_component_gap_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Layer-2 component pool config。"),
    ] = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Layer-2 component 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_growth_component_gap_review(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 growth component gap review", payload)


def strategies_research_roadmap_master_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    layer1_output_root: Annotated[
        Path,
        typer.Option("--layer1-output-root", help="Layer-1 meta-policy 输出目录。"),
    ] = DEFAULT_LAYER1_META_POLICY_OUTPUT_ROOT,
    simple_output_root: Annotated[
        Path,
        typer.Option("--simple-output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    layer2_output_root: Annotated[
        Path,
        typer.Option("--layer2-output-root", help="Layer-2 component 输出目录。"),
    ] = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research roadmap 输出目录。"),
    ] = DEFAULT_RESEARCH_ROADMAP_OUTPUT_ROOT,
    roadmap_doc_path: Annotated[
        Path,
        typer.Option("--roadmap-doc-path", help="Master review Markdown 文档路径。"),
    ] = DEFAULT_RESEARCH_ROADMAP_MASTER_DOC_PATH,
) -> None:
    payload = _build_research_payload(
        lambda: run_research_roadmap_master_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            layer1_output_root=layer1_output_root,
            simple_output_root=simple_output_root,
            layer2_output_root=layer2_output_root,
            output_root=output_root,
            roadmap_doc_path=roadmap_doc_path,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Research roadmap master review", payload)


def strategies_equal_risk_forward_aging_scheduler_integration_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    decision_date: Annotated[
        str | None,
        typer.Option("--decision-date", help="可选 observation decision date。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_forward_aging_scheduler_integration(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            decision_date=_parse_optional_date(decision_date),
        )
    )
    _print_simple_baseline_payload("Equal-risk forward-aging scheduler integration", payload)


def strategies_equal_risk_observation_continuity_check_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_observation_continuity_check(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Equal-risk observation continuity check", payload)


def strategies_equal_risk_first_maturity_monitor_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_first_maturity_monitor(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Equal-risk first maturity monitor", payload)


def strategies_equal_risk_forward_aging_scoreboard_safety_gate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_forward_aging_scoreboard_safety_gate(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Equal-risk scoreboard safety gate", payload)


def strategies_equal_risk_reader_brief_live_summary_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_reader_brief_live_summary(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Equal-risk Reader Brief live summary", payload)


def strategies_layer2_growth_component_restart_contract_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Controlled growth component v2 registry YAML。"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Growth component 输出目录。"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_layer2_growth_component_restart_contract(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Layer-2 growth component restart contract", payload)


def strategies_controlled_growth_component_registry_v2_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Controlled growth component v2 registry YAML。"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Growth component 输出目录。"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_controlled_growth_component_registry_v2_review(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Controlled growth component registry v2 review", payload)


def strategies_beta_adjusted_growth_edge_contract_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Controlled growth component v2 registry YAML。"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Growth component 输出目录。"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_beta_adjusted_growth_edge_contract(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Beta-adjusted growth edge contract", payload)


def strategies_low_turnover_controlled_growth_search_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_low_turnover_controlled_growth_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Low-turnover controlled growth search", payload)


def strategies_volatility_targeted_growth_component_search_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_volatility_targeted_growth_component_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Volatility-targeted growth component search", payload)


def strategies_drawdown_guarded_growth_component_search_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_drawdown_guarded_growth_component_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Drawdown-guarded growth component search", payload)


def strategies_growth_component_beta_exposure_attribution_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_growth_component_beta_exposure_attribution(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Growth component beta exposure attribution", payload)


def strategies_growth_component_period_drawdown_validation_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_growth_component_period_drawdown_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Growth component period drawdown validation", payload)


def strategies_growth_component_cost_turnover_sensitivity_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_growth_component_cost_turnover_sensitivity(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Growth component cost turnover sensitivity", payload)


def strategies_growth_component_readiness_gate_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_growth_component_readiness_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Growth component readiness gate", payload)


def strategies_growth_component_owner_decision_pack_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path"),
    ] = DEFAULT_GROWTH_COMPONENT_OWNER_DECISION_DOC_PATH,
) -> None:
    payload = _build_research_payload(
        lambda: run_growth_component_owner_decision_pack(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            docs_path=docs_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Growth component owner decision pack", payload)


def strategies_equal_risk_and_growth_dual_track_roadmap_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Roadmap 输出目录。"),
    ] = DEFAULT_RESEARCH_ROADMAP_OUTPUT_ROOT / "roadmap",
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_and_growth_dual_track_roadmap(output_root=output_root)
    )
    _print_simple_baseline_payload("Equal-risk and growth dual-track roadmap", payload)


def strategies_research_roadmap_v2_master_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    simple_output_root: Annotated[
        Path,
        typer.Option("--simple-output-root"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    growth_output_root: Annotated[
        Path,
        typer.Option("--growth-output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_RESEARCH_ROADMAP_OUTPUT_ROOT / "roadmap",
    owner_docs_path: Annotated[
        Path,
        typer.Option("--owner-docs-path"),
    ] = DEFAULT_GROWTH_COMPONENT_OWNER_DECISION_DOC_PATH,
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path"),
    ] = DEFAULT_GROWTH_COMPONENT_ROADMAP_DOC_PATH,
) -> None:
    payload = _build_research_payload(
        lambda: run_research_roadmap_v2_master_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            simple_output_root=simple_output_root,
            growth_output_root=growth_output_root,
            output_root=output_root,
            owner_docs_path=owner_docs_path,
            docs_path=docs_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Research roadmap v2 master review", payload)


def strategies_equal_risk_growth_v2_real_cli_suite_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    decision_date: Annotated[str | None, typer.Option("--decision-date")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    simple_config_path: Annotated[
        Path,
        typer.Option("--simple-config"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Annotated[
        Path,
        typer.Option("--growth-config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    simple_output_root: Annotated[
        Path,
        typer.Option("--simple-output-root"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    growth_output_root: Annotated[
        Path,
        typer.Option("--growth-output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_ROADMAP_V2_REAL_RESULT_OUTPUT_ROOT,
    growth_owner_docs_path: Annotated[
        Path,
        typer.Option("--growth-owner-docs-path"),
    ] = DEFAULT_GROWTH_COMPONENT_OWNER_DECISION_DOC_PATH,
    growth_roadmap_docs_path: Annotated[
        Path,
        typer.Option("--growth-roadmap-docs-path"),
    ] = DEFAULT_GROWTH_COMPONENT_ROADMAP_DOC_PATH,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_growth_v2_real_cli_suite(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            simple_output_root=simple_output_root,
            growth_output_root=growth_output_root,
            output_root=output_root,
            growth_owner_docs_path=growth_owner_docs_path,
            growth_roadmap_docs_path=growth_roadmap_docs_path,
            as_of_date=_parse_optional_date(as_of),
            decision_date=_parse_optional_date(decision_date),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Equal-risk growth v2 real CLI suite", payload)


def strategies_equal_risk_forward_aging_live_health_summary_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_forward_aging_live_health_summary(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Equal-risk forward-aging live health summary", payload)


def strategies_controlled_growth_v2_candidate_summary_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_controlled_growth_v2_candidate_summary(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Controlled growth v2 candidate summary", payload)


def strategies_controlled_growth_beta_adjusted_edge_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_controlled_growth_beta_adjusted_edge_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Controlled growth beta-adjusted edge review", payload)


def strategies_controlled_growth_period_drawdown_cost_triage_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_controlled_growth_period_drawdown_cost_triage(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Controlled growth period drawdown cost triage", payload)


def strategies_controlled_growth_component_final_gate_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_controlled_growth_component_final_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Controlled growth component final gate", payload)


def strategies_dual_track_owner_decision_pack_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    simple_config_path: Annotated[
        Path,
        typer.Option("--simple-config"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Annotated[
        Path,
        typer.Option("--growth-config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    simple_output_root: Annotated[
        Path,
        typer.Option("--simple-output-root"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    growth_output_root: Annotated[
        Path,
        typer.Option("--growth-output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_ROADMAP_V2_REAL_RESULT_OUTPUT_ROOT,
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path"),
    ] = DEFAULT_DUAL_TRACK_OWNER_DECISION_DOC_PATH,
) -> None:
    payload = _build_research_payload(
        lambda: run_dual_track_owner_decision_pack(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            simple_output_root=simple_output_root,
            growth_output_root=growth_output_root,
            output_root=output_root,
            docs_path=docs_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Dual-track owner decision pack", payload)


def strategies_roadmap_v2_real_result_master_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    decision_date: Annotated[str | None, typer.Option("--decision-date")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    simple_config_path: Annotated[
        Path,
        typer.Option("--simple-config"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Annotated[
        Path,
        typer.Option("--growth-config"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    simple_output_root: Annotated[
        Path,
        typer.Option("--simple-output-root"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    growth_output_root: Annotated[
        Path,
        typer.Option("--growth-output-root"),
    ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_ROADMAP_V2_REAL_RESULT_OUTPUT_ROOT,
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path"),
    ] = DEFAULT_ROADMAP_V2_REAL_RESULT_MASTER_REVIEW_DOC_PATH,
    growth_owner_docs_path: Annotated[
        Path,
        typer.Option("--growth-owner-docs-path"),
    ] = DEFAULT_GROWTH_COMPONENT_OWNER_DECISION_DOC_PATH,
    growth_roadmap_docs_path: Annotated[
        Path,
        typer.Option("--growth-roadmap-docs-path"),
    ] = DEFAULT_GROWTH_COMPONENT_ROADMAP_DOC_PATH,
    dual_track_docs_path: Annotated[
        Path,
        typer.Option("--dual-track-docs-path"),
    ] = DEFAULT_DUAL_TRACK_OWNER_DECISION_DOC_PATH,
) -> None:
    payload = _build_research_payload(
        lambda: run_roadmap_v2_real_result_master_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            simple_output_root=simple_output_root,
            growth_output_root=growth_output_root,
            output_root=output_root,
            docs_path=docs_path,
            growth_owner_docs_path=growth_owner_docs_path,
            growth_roadmap_docs_path=growth_roadmap_docs_path,
            dual_track_docs_path=dual_track_docs_path,
            as_of_date=_parse_optional_date(as_of),
            decision_date=_parse_optional_date(decision_date),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Roadmap v2 real result master review", payload)


_SIMPLE_BASELINE_STRATEGY_COMMANDS = (
    ("simple-baseline-registry-review", strategies_simple_baseline_registry_review_command),
    ("qqq-sgov-baseline-backtest", strategies_qqq_sgov_baseline_backtest_command),
    ("tqqq-sgov-risk-controlled-baseline", strategies_tqqq_sgov_risk_controlled_baseline_command),
    ("trend-vol-allocation-policy-search", strategies_trend_vol_allocation_policy_search_command),
    ("simple-baseline-dominance-ranking", strategies_simple_baseline_dominance_ranking_command),
    ("simple-baseline-pit-boundary-audit", strategies_simple_baseline_pit_boundary_audit_command),
    ("simple-baseline-cost-sensitivity", strategies_simple_baseline_cost_sensitivity_command),
    ("simple-baseline-regime-review", strategies_simple_baseline_regime_review_command),
    (
        "simple-baseline-forward-aging-tracker",
        strategies_simple_baseline_forward_aging_tracker_command,
    ),
    (
        "simple-baseline-paper-shadow-readiness",
        strategies_simple_baseline_paper_shadow_readiness_command,
    ),
    (
        "daily-reader-portfolio-control-safety-summary",
        strategies_daily_reader_portfolio_control_safety_summary_command,
    ),
    (
        "simple-baseline-portfolio-dry-run-mapper",
        strategies_simple_baseline_portfolio_dry_run_mapper_command,
    ),
    ("simple-baseline-master-review", strategies_simple_baseline_master_review_command),
    ("options-next-stage-gate", strategies_options_next_stage_gate_command),
    ("equal-risk-qqq-sgov-deep-dive", strategies_equal_risk_qqq_sgov_deep_dive_command),
    (
        "simple-baseline-period-split-validation",
        strategies_simple_baseline_period_split_validation_command,
    ),
    (
        "simple-baseline-drawdown-episode-review",
        strategies_simple_baseline_drawdown_episode_review_command,
    ),
    (
        "dynamic-vs-static-edge-significance-review",
        strategies_dynamic_vs_static_edge_significance_review_command,
    ),
    ("tqqq-heavy-pause-rationale-report", strategies_tqqq_heavy_pause_rationale_report_command),
    (
        "simple-baseline-watchlist-owner-decision",
        strategies_simple_baseline_watchlist_owner_decision_command,
    ),
    (
        "simple-baseline-real-result-reconciliation",
        strategies_simple_baseline_real_result_reconciliation_command,
    ),
    (
        "simple-baseline-forward-aging-candidate-freeze",
        strategies_simple_baseline_forward_aging_candidate_freeze_command,
    ),
    (
        "simple-baseline-forward-aging-contract",
        strategies_simple_baseline_forward_aging_contract_command,
    ),
    (
        "simple-baseline-forward-aging-write-observation",
        strategies_simple_baseline_forward_aging_write_observation_command,
    ),
    (
        "simple-baseline-forward-aging-update-maturity",
        strategies_simple_baseline_forward_aging_update_maturity_command,
    ),
    (
        "simple-baseline-forward-aging-scoreboard",
        strategies_simple_baseline_forward_aging_scoreboard_command,
    ),
    (
        "equal-risk-qqq-sgov-policy-definition-lock",
        strategies_equal_risk_qqq_sgov_policy_definition_lock_command,
    ),
    (
        "simple-baseline-comparator-definition-lock",
        strategies_simple_baseline_comparator_definition_lock_command,
    ),
    (
        "simple-baseline-forward-aging-data-quality-gate",
        strategies_simple_baseline_forward_aging_data_quality_gate_command,
    ),
    (
        "simple-baseline-paper-shadow-threshold-contract",
        strategies_simple_baseline_paper_shadow_threshold_contract_command,
    ),
    ("daily-reader-forward-aging-summary", strategies_daily_reader_forward_aging_summary_command),
    ("simple-baseline-risk-budget-review", strategies_simple_baseline_risk_budget_review_command),
    (
        "simple-baseline-absolute-return-gap-review",
        strategies_simple_baseline_absolute_return_gap_review_command,
    ),
    (
        "simple-baseline-candidate-role-assignment",
        strategies_simple_baseline_candidate_role_assignment_command,
    ),
    (
        "simple-baseline-forward-aging-owner-review-pack",
        strategies_simple_baseline_forward_aging_owner_review_pack_command,
    ),
    (
        "simple-baseline-forward-aging-automation-readiness",
        strategies_simple_baseline_forward_aging_automation_readiness_command,
    ),
    (
        "simple-baseline-forward-aging-master-review",
        strategies_simple_baseline_forward_aging_master_review_command,
    ),
    (
        "simple-baseline-data-source-inventory",
        strategies_simple_baseline_data_source_inventory_command,
    ),
    (
        "tqqq-cache-rebuild-and-validation",
        strategies_tqqq_cache_rebuild_and_validation_command,
    ),
    (
        "sgov-total-return-data-contract",
        strategies_sgov_total_return_data_contract_command,
    ),
    (
        "market-data-repair-manifest-audit",
        strategies_market_data_repair_manifest_audit_command,
    ),
    (
        "simple-baseline-validate-data-hardening",
        strategies_simple_baseline_validate_data_hardening_command,
    ),
    (
        "rerun-simple-baseline-real-cli-after-data-repair",
        strategies_rerun_simple_baseline_real_cli_after_data_repair_command,
    ),
    (
        "equal-risk-result-recompute-after-data-repair",
        strategies_equal_risk_result_recompute_after_data_repair_command,
    ),
    (
        "tqqq-challenger-revalidation-after-cache-fix",
        strategies_tqqq_challenger_revalidation_after_cache_fix_command,
    ),
    (
        "forward-aging-unblock-readiness-review",
        strategies_forward_aging_unblock_readiness_review_command,
    ),
    (
        "first-forward-aging-observation-dry-run",
        strategies_first_forward_aging_observation_dry_run_command,
    ),
    (
        "reader-brief-forward-aging-safe-preview",
        strategies_reader_brief_forward_aging_safe_preview_command,
    ),
    (
        "data-repair-owner-decision-pack",
        strategies_data_repair_owner_decision_pack_command,
    ),
    (
        "data-repair-reproducibility-proof",
        strategies_data_repair_reproducibility_proof_command,
    ),
    (
        "marketstack-ssl-failure-triage",
        strategies_marketstack_ssl_failure_triage_command,
    ),
    (
        "sgov-total-return-proxy-quality-review",
        strategies_sgov_total_return_proxy_quality_review_command,
    ),
    (
        "first-forward-aging-observation-write",
        strategies_first_forward_aging_observation_write_command,
    ),
    (
        "forward-aging-idempotency-and-duplicate-guard",
        strategies_forward_aging_idempotency_and_duplicate_guard_command,
    ),
    (
        "forward-aging-scheduler-dry-run",
        strategies_forward_aging_scheduler_dry_run_command,
    ),
    (
        "paper-shadow-blocker-status-report",
        strategies_paper_shadow_blocker_status_report_command,
    ),
    (
        "forward-aging-owner-launch-pack",
        strategies_forward_aging_owner_launch_pack_command,
    ),
    (
        "qqq-outperformance-objective-contract",
        strategies_qqq_outperformance_objective_contract_command,
    ),
    (
        "qqq-plus-growth-candidate-registry",
        strategies_qqq_plus_growth_candidate_registry_command,
    ),
    (
        "controlled-tqqq-overlay-search",
        strategies_controlled_tqqq_overlay_search_command,
    ),
    (
        "trend-gated-leverage-policy-search",
        strategies_trend_gated_leverage_policy_search_command,
    ),
    (
        "volatility-targeted-growth-policy-search",
        strategies_volatility_targeted_growth_policy_search_command,
    ),
    (
        "drawdown-guarded-growth-policy-search",
        strategies_drawdown_guarded_growth_policy_search_command,
    ),
    (
        "qqq-outperformance-ranking-report",
        strategies_qqq_outperformance_ranking_report_command,
    ),
    (
        "qqq-outperformance-period-split-validation",
        strategies_qqq_outperformance_period_split_validation_command,
    ),
    (
        "qqq-outperformance-drawdown-replay",
        strategies_qqq_outperformance_drawdown_replay_command,
    ),
    (
        "growth-edge-significance-review",
        strategies_growth_edge_significance_review_command,
    ),
    (
        "growth-candidate-forward-aging-watchlist",
        strategies_growth_candidate_forward_aging_watchlist_command,
    ),
    (
        "qqq-plus-risk-budget-review",
        strategies_qqq_plus_risk_budget_review_command,
    ),
    (
        "growth-vs-defensive-role-allocation-review",
        strategies_growth_vs_defensive_role_allocation_review_command,
    ),
    (
        "qqq-outperformance-owner-decision-pack",
        strategies_qqq_outperformance_owner_decision_pack_command,
    ),
    (
        "layer2-component-readiness-reconciliation",
        strategies_layer2_component_readiness_reconciliation_command,
    ),
    (
        "layer2-component-pool-freeze",
        strategies_layer2_component_pool_freeze_command,
    ),
    (
        "layer2-component-definition-lock",
        strategies_layer2_component_definition_lock_command,
    ),
    (
        "layer2-component-data-quality-check",
        strategies_layer2_component_data_quality_check_command,
    ),
    (
        "layer2-component-readiness-matrix",
        strategies_layer2_component_readiness_matrix_command,
    ),
    (
        "layer2-historical-weight-path-build",
        strategies_layer2_historical_weight_path_build_command,
    ),
    (
        "layer2-return-cost-exposure-panel",
        strategies_layer2_return_cost_exposure_panel_command,
    ),
    (
        "layer2-forward-outcome-cube-build",
        strategies_layer2_forward_outcome_cube_build_command,
    ),
    (
        "layer2-anti-leakage-time-boundary-audit",
        strategies_layer2_anti_leakage_time_boundary_audit_command,
    ),
    (
        "layer2-common-robustness-validation",
        strategies_layer2_common_robustness_validation_command,
    ),
    (
        "layer2-transition-cost-latency-review",
        strategies_layer2_transition_cost_latency_review_command,
    ),
    (
        "layer2-component-distinctiveness-review",
        strategies_layer2_component_distinctiveness_review_command,
    ),
    (
        "layer2-selector-headroom-oracle-review",
        strategies_layer2_selector_headroom_oracle_review_command,
    ),
    (
        "layer2-switching-constraint-contract",
        strategies_layer2_switching_constraint_contract_command,
    ),
    (
        "layer2-best-component-label-builder",
        strategies_layer2_best_component_label_builder_command,
    ),
    (
        "layer1-policy-combiner-contract",
        strategies_layer1_policy_combiner_contract_command,
    ),
    (
        "layer1-objective-outcome-contract",
        strategies_layer1_objective_outcome_contract_command,
    ),
    (
        "layer1-purged-walk-forward-split-contract",
        strategies_layer1_purged_walk_forward_split_contract_command,
    ),
    (
        "layer1-research-dataset-builder",
        strategies_layer1_research_dataset_builder_command,
    ),
    (
        "layer1-dataset-lineage-leakage-audit",
        strategies_layer1_dataset_lineage_leakage_audit_command,
    ),
    (
        "layer1-naive-selector-baselines",
        strategies_layer1_naive_selector_baselines_command,
    ),
    (
        "layer1-simple-rule-selector-search",
        strategies_layer1_simple_rule_selector_search_command,
    ),
    (
        "layer1-selector-cost-adjusted-evaluation",
        strategies_layer1_selector_cost_adjusted_evaluation_command,
    ),
    (
        "layer1-selector-regime-period-validation",
        strategies_layer1_selector_regime_period_validation_command,
    ),
    (
        "layer1-selector-failure-case-review",
        strategies_layer1_selector_failure_case_review_command,
    ),
    (
        "layer1-historical-research-readiness-gate",
        strategies_layer1_historical_research_readiness_gate_command,
    ),
    (
        "layer1-research-owner-decision-pack",
        strategies_layer1_research_owner_decision_pack_command,
    ),
    (
        "layer1-reader-brief-safety-preview",
        strategies_layer1_reader_brief_safety_preview_command,
    ),
    (
        "layer1-meta-policy-master-review",
        strategies_layer1_meta_policy_master_review_command,
    ),
    (
        "layer1-simple-rule-selector-registry-review",
        strategies_layer1_simple_rule_selector_registry_review_command,
    ),
    (
        "layer1-trend-rule-selector-backtest",
        strategies_layer1_trend_rule_selector_backtest_command,
    ),
    (
        "layer1-volatility-rule-selector-backtest",
        strategies_layer1_volatility_rule_selector_backtest_command,
    ),
    (
        "layer1-drawdown-rule-selector-backtest",
        strategies_layer1_drawdown_rule_selector_backtest_command,
    ),
    (
        "layer1-combined-simple-rule-selector-search",
        strategies_layer1_combined_simple_rule_selector_search_command,
    ),
    (
        "layer1-selector-cost-latency-stress",
        strategies_layer1_selector_cost_latency_stress_command,
    ),
    (
        "layer1-selector-period-split-validation",
        strategies_layer1_selector_period_split_validation_command,
    ),
    (
        "layer1-selector-drawdown-episode-review",
        strategies_layer1_selector_drawdown_episode_review_command,
    ),
    (
        "layer1-selector-regret-attribution",
        strategies_layer1_selector_regret_attribution_command,
    ),
    (
        "layer1-selector-vs-component-baseline-ranking",
        strategies_layer1_selector_vs_component_baseline_ranking_command,
    ),
    (
        "layer1-selector-overfit-sensitivity-review",
        strategies_layer1_selector_overfit_sensitivity_review_command,
    ),
    (
        "layer1-selector-minimum-holding-period-review",
        strategies_layer1_selector_minimum_holding_period_review_command,
    ),
    (
        "layer1-selector-forward-aging-watchlist-gate",
        strategies_layer1_selector_forward_aging_watchlist_gate_command,
    ),
    (
        "layer1-selector-owner-decision-pack",
        strategies_layer1_selector_owner_decision_pack_command,
    ),
    (
        "layer1-simple-rule-selector-master-review",
        strategies_layer1_simple_rule_selector_master_review_command,
    ),
    (
        "layer1-selector-real-result-summary",
        strategies_layer1_selector_real_result_summary_command,
    ),
    (
        "layer1-selector-history-coverage-gap-audit",
        strategies_layer1_selector_history_coverage_gap_audit_command,
    ),
    (
        "layer1-selector-recent-regime-risk-disclosure",
        strategies_layer1_selector_recent_regime_risk_disclosure_command,
    ),
    (
        "layer1-selector-owner-watchlist-review",
        strategies_layer1_selector_owner_watchlist_review_command,
    ),
    (
        "layer1-selector-forward-aging-dry-run",
        strategies_layer1_selector_forward_aging_dry_run_command,
    ),
    (
        "layer1-selector-watchlist-blocker-report",
        strategies_layer1_selector_watchlist_blocker_report_command,
    ),
    (
        "layer1-selector-reader-brief-preview",
        strategies_layer1_selector_reader_brief_preview_command,
    ),
    (
        "layer1-selector-result-review-master",
        strategies_layer1_selector_result_review_master_command,
    ),
    (
        "layer1-selector-turnover-source-diagnosis",
        strategies_layer1_selector_turnover_source_diagnosis_command,
    ),
    (
        "layer1-selector-buffered-200dma-variants",
        strategies_layer1_selector_buffered_200dma_variants_command,
    ),
    (
        "layer1-selector-min-holding-cooldown-review",
        strategies_layer1_selector_min_holding_cooldown_review_command,
    ),
    (
        "layer1-selector-soft-blend-review",
        strategies_layer1_selector_soft_blend_review_command,
    ),
    (
        "layer1-selector-low-turnover-ranking",
        strategies_layer1_selector_low_turnover_ranking_command,
    ),
    (
        "layer1-selector-low-turnover-owner-decision-pack",
        strategies_layer1_selector_low_turnover_owner_decision_pack_command,
    ),
    (
        "layer1-selector-switch-count-threshold-contract",
        strategies_layer1_selector_switch_count_threshold_contract_command,
    ),
    (
        "layer1-selector-soft-blend-constrained-search",
        strategies_layer1_selector_soft_blend_constrained_search_command,
    ),
    (
        "layer1-selector-monthly-only-review",
        strategies_layer1_selector_monthly_only_review_command,
    ),
    (
        "layer1-selector-hysteresis-review",
        strategies_layer1_selector_hysteresis_review_command,
    ),
    (
        "layer1-selector-switch-quality-attribution",
        strategies_layer1_selector_switch_quality_attribution_command,
    ),
    (
        "layer1-selector-low-turnover-finalist-ranking",
        strategies_layer1_selector_low_turnover_finalist_ranking_command,
    ),
    (
        "layer1-selector-vs-simple-components-final-gate",
        strategies_layer1_selector_vs_simple_components_final_gate_command,
    ),
    (
        "layer1-selector-forward-aging-watchlist-final-review",
        strategies_layer1_selector_forward_aging_watchlist_final_review_command,
    ),
    (
        "layer1-selector-pause-or-continue-owner-pack",
        strategies_layer1_selector_pause_or_continue_owner_pack_command,
    ),
    (
        "layer1-selector-dry-run-archive-report",
        strategies_layer1_selector_dry_run_archive_report_command,
    ),
    (
        "layer1-selector-restart-condition-contract",
        strategies_layer1_selector_restart_condition_contract_command,
    ),
    (
        "equal-risk-forward-aging-daily-run-health-check",
        strategies_equal_risk_forward_aging_daily_run_health_check_command,
    ),
    (
        "equal-risk-forward-aging-maturity-update-check",
        strategies_equal_risk_forward_aging_maturity_update_check_command,
    ),
    (
        "equal-risk-forward-aging-scoreboard-first-window-review",
        strategies_equal_risk_forward_aging_scoreboard_first_window_review_command,
    ),
    (
        "layer2-growth-component-gap-review",
        strategies_layer2_growth_component_gap_review_command,
    ),
    (
        "research-roadmap-master-review",
        strategies_research_roadmap_master_review_command,
    ),
    (
        "equal-risk-forward-aging-scheduler-integration",
        strategies_equal_risk_forward_aging_scheduler_integration_command,
    ),
    (
        "equal-risk-observation-continuity-check",
        strategies_equal_risk_observation_continuity_check_command,
    ),
    (
        "equal-risk-first-maturity-monitor",
        strategies_equal_risk_first_maturity_monitor_command,
    ),
    (
        "equal-risk-forward-aging-scoreboard-safety-gate",
        strategies_equal_risk_forward_aging_scoreboard_safety_gate_command,
    ),
    (
        "equal-risk-reader-brief-live-summary",
        strategies_equal_risk_reader_brief_live_summary_command,
    ),
    (
        "layer2-growth-component-restart-contract",
        strategies_layer2_growth_component_restart_contract_command,
    ),
    (
        "controlled-growth-component-registry-v2-review",
        strategies_controlled_growth_component_registry_v2_review_command,
    ),
    (
        "beta-adjusted-growth-edge-contract",
        strategies_beta_adjusted_growth_edge_contract_command,
    ),
    (
        "low-turnover-controlled-growth-search",
        strategies_low_turnover_controlled_growth_search_command,
    ),
    (
        "volatility-targeted-growth-component-search",
        strategies_volatility_targeted_growth_component_search_command,
    ),
    (
        "drawdown-guarded-growth-component-search",
        strategies_drawdown_guarded_growth_component_search_command,
    ),
    (
        "growth-component-beta-exposure-attribution",
        strategies_growth_component_beta_exposure_attribution_command,
    ),
    (
        "growth-component-period-drawdown-validation",
        strategies_growth_component_period_drawdown_validation_command,
    ),
    (
        "growth-component-cost-turnover-sensitivity",
        strategies_growth_component_cost_turnover_sensitivity_command,
    ),
    (
        "growth-component-readiness-gate",
        strategies_growth_component_readiness_gate_command,
    ),
    (
        "growth-component-owner-decision-pack",
        strategies_growth_component_owner_decision_pack_command,
    ),
    (
        "equal-risk-and-growth-dual-track-roadmap",
        strategies_equal_risk_and_growth_dual_track_roadmap_command,
    ),
    (
        "research-roadmap-v2-master-review",
        strategies_research_roadmap_v2_master_review_command,
    ),
    (
        "equal-risk-growth-v2-real-cli-suite",
        strategies_equal_risk_growth_v2_real_cli_suite_command,
    ),
    (
        "equal-risk-forward-aging-live-health-summary",
        strategies_equal_risk_forward_aging_live_health_summary_command,
    ),
    (
        "controlled-growth-v2-candidate-summary",
        strategies_controlled_growth_v2_candidate_summary_command,
    ),
    (
        "controlled-growth-beta-adjusted-edge-review",
        strategies_controlled_growth_beta_adjusted_edge_review_command,
    ),
    (
        "controlled-growth-period-drawdown-cost-triage",
        strategies_controlled_growth_period_drawdown_cost_triage_command,
    ),
    (
        "controlled-growth-component-final-gate",
        strategies_controlled_growth_component_final_gate_command,
    ),
    (
        "dual-track-owner-decision-pack",
        strategies_dual_track_owner_decision_pack_command,
    ),
    (
        "roadmap-v2-real-result-master-review",
        strategies_roadmap_v2_real_result_master_review_command,
    ),
)


def _build_research_payload(builder: Callable[[], dict[str, object]]) -> dict[str, object]:
    try:
        return builder()
    except (ResearchGovernanceError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc


def _layer1_selector_research_payload(
    builder: Callable[[], dict[str, object]],
    _label: str,
) -> dict[str, object]:
    return _build_research_payload(builder)


def _print_simple_baseline_payload(label: str, payload: dict[str, object]) -> None:
    _print_status(label, str(payload["status"]))
    console.print(f"status={payload.get('status')}")
    _print_summary(payload)
    paths = payload.get("artifact_paths")
    if isinstance(paths, dict):
        console.print(f"JSON：{paths.get('json_path')}")
        console.print(f"Markdown：{paths.get('markdown_path')}")
    for safety_field, expected in (
        ("research_only", True),
        ("observe_only", True),
        ("promotion_allowed", False),
        ("paper_shadow_allowed", False),
        ("production_allowed", False),
        ("broker_action", "none"),
        ("production_effect", "none"),
    ):
        console.print(f"{safety_field}={payload.get(safety_field, expected)}")
    if str(payload["status"]) == "FAIL":
        raise typer.Exit(code=1)


def _parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _print_summary(payload: dict[str, object]) -> None:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return
    compact = "; ".join(f"{key}={value}" for key, value in list(summary.items())[:6])
    if compact:
        console.print(compact)


def _print_status(label: str, status: str) -> None:
    style = "green" if status in {"PASS", "PROMISING"} else "yellow"
    if status in {"FAIL", "BLOCKED", "REJECTED"}:
        style = "red"
    console.print(f"[{style}]{label}：{status}[/{style}]")
