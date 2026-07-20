from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.cli_commands.research_foundation import (
    register_research_foundation_commands,
)
from ai_trading_system.cli_commands.research_restart import (
    register_research_restart_commands,
)
from ai_trading_system.cli_commands.research_simple_baselines import (
    register_simple_baseline_strategy_commands,
)
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.controlled_strategy_batch import (
    DEFAULT_AI_REGIME_ATTRIBUTION_REVIEW_PATH,
    DEFAULT_BENCHMARK_FALLBACK_DRAWDOWN_GUARD_PROTOTYPE_PATH,
    DEFAULT_BENCHMARK_FIRST_TAIL_RISK_POLICY_CONTRACT_PATH,
    DEFAULT_CONSERVATIVE_HORIZON_RISK_FILTER_PATH,
    DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH,
    DEFAULT_CONTROLLED_STRATEGY_BATCH_REVIEW_OUTPUT_ROOT,
    DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    DEFAULT_COST_AWARE_HORIZON_HYSTERESIS_PATH,
    DEFAULT_COST_TURNOVER_AWARE_VALUE_SURFACE_PATH,
    DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    DEFAULT_FORWARD_EVIDENCE_CONTINUITY_EXTENSION_PATH,
    DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT,
    DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
    DEFAULT_GBDT_ACTION_UTILITY_PATH,
    DEFAULT_GBDT_PIVOT_REVIEW_PATH,
    DEFAULT_GBDT_PIVOT_SELECTION_PATH,
    DEFAULT_GBDT_RESIDUAL_HYPOTHESIS_TRIAGE_PATH,
    DEFAULT_GBDT_RESIDUAL_REGIME_CONDITIONING_PATH,
    DEFAULT_GBDT_VALUE_SURFACE_RESIDUAL_DIAGNOSTIC_PATH,
    DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    DEFAULT_HORIZON_SELECTOR_CONTROLLED_PROTOTYPE_PATH,
    DEFAULT_HORIZON_SELECTOR_HOLDOUT_REVIEW_PATH,
    DEFAULT_HORIZON_SELECTOR_PROBLEM_CONTRACT_PATH,
    DEFAULT_LONG_HORIZON_QUARANTINE_FALLBACK_REVIEW_PATH,
    DEFAULT_LONG_HORIZON_QUARANTINE_REVIEW_PATH,
    DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH,
    DEFAULT_REGIME_CONDITIONED_WALK_FORWARD_HOLDOUT_PATH,
    DEFAULT_REGIME_HORIZON_LOSS_ATTRIBUTION_MATRIX_PATH,
    DEFAULT_REGRET_ACTIVATION_INPUTS_PATH,
    DEFAULT_REGRET_CASEBOOK_EXPANSION_GATE_PATH,
    DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT,
    DEFAULT_REGRET_STATE_MACHINE_PATH,
    DEFAULT_SIMPLE_ENSEMBLE_OUTPUT_ROOT,
    DEFAULT_SIMPLE_STRATEGY_SELECTOR_PATH,
    DEFAULT_STATE_TRANSITION_CASEBOOK_PATH,
    DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    DEFAULT_TAIL_LOSS_GUARDRAIL_FALLBACK_POLICY_PATH,
    DEFAULT_TAIL_RISK_ANTI_LEAKAGE_AUDIT_PATH,
    DEFAULT_TAIL_RISK_ARTIFACT_DETERMINISM_CHECK_PATH,
    DEFAULT_TAIL_RISK_AUDIT_UNIVERSE_RECONCILIATION_PATH,
    DEFAULT_TAIL_RISK_BASELINE_DOMINANCE_GATE_PATH,
    DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    DEFAULT_TAIL_RISK_COUNTERFACTUAL_BASELINE_RESULT_REVIEW_PATH,
    DEFAULT_TAIL_RISK_DAILY_READING_SAFETY_SUMMARY_PATH,
    DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH,
    DEFAULT_TAIL_RISK_EVIDENCE_MATURITY_GATE_PATH,
    DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH,
    DEFAULT_TAIL_RISK_FALLBACK_ERROR_COST_LEDGER_PATH,
    DEFAULT_TAIL_RISK_FALLBACK_TRIGGER_PRECISION_RECALL_PATH,
    DEFAULT_TAIL_RISK_FORWARD_AGING_TRACKER_PATH,
    DEFAULT_TAIL_RISK_FORWARD_EVIDENCE_INTEGRATION_PATH,
    DEFAULT_TAIL_RISK_FORWARD_MATURITY_SCOREBOARD_PATH,
    DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH,
    DEFAULT_TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_PATH,
    DEFAULT_TAIL_RISK_HARD_BLOCK_MUTATION_TESTS_PATH,
    DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_RESULT_REVIEW_PATH,
    DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    DEFAULT_TAIL_RISK_INDEPENDENT_TRIGGER_V2_BUILDER_PATH,
    DEFAULT_TAIL_RISK_LEAKAGE_STRESS_SUITE_PATH,
    DEFAULT_TAIL_RISK_NEXT_DECISION_DOC_PATH,
    DEFAULT_TAIL_RISK_NEXT_DECISION_PATH,
    DEFAULT_TAIL_RISK_OPPORTUNITY_COST_UPSIDE_CAPTURE_PATH,
    DEFAULT_TAIL_RISK_POLICY_CONTROLLED_REVIEW_BOARD_PATH,
    DEFAULT_TAIL_RISK_POST_MERGE_EVIDENCE_REVIEW_PATH,
    DEFAULT_TAIL_RISK_PROMOTION_READINESS_GATE_PATH,
    DEFAULT_TAIL_RISK_REAL_DATA_VALIDATION_AUDIT_PATH,
    DEFAULT_TAIL_RISK_REGIME_SEGMENTED_ROBUSTNESS_PATH,
    DEFAULT_TAIL_RISK_REGIME_STRATIFIED_FORWARD_OUTCOME_REVIEW_PATH,
    DEFAULT_TAIL_RISK_REPORT_REGISTRY_INTEGRITY_REVIEW_PATH,
    DEFAULT_TAIL_RISK_RESEARCH_MASTER_REVIEW_PATH,
    DEFAULT_TAIL_RISK_RESEARCH_READINESS_SCORE_PATH,
    DEFAULT_TAIL_RISK_STATUS_MATRIX_PATH,
    DEFAULT_TAIL_RISK_TAINTED_METRIC_QUARANTINE_PATH,
    DEFAULT_TAIL_RISK_TASK_COVERAGE_MAP_DOC_PATH,
    DEFAULT_TAIL_RISK_TASK_COVERAGE_MAP_PATH,
    DEFAULT_TAIL_RISK_THRESHOLD_SENSITIVITY_PATH,
    DEFAULT_TAIL_RISK_THRESHOLD_SENSITIVITY_REVIEW_PATH,
    DEFAULT_TAIL_RISK_TRIGGER_FEATURE_AVAILABILITY_CATALOG_PATH,
    DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    DEFAULT_TAIL_RISK_TRIGGER_V2_INPUT_QUALITY_REVIEW_PATH,
    DEFAULT_UTILITY_BOUNDARY_AUDIT_PATH,
    DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT,
    DEFAULT_VALUE_SURFACE_DIRECTION_REVIEW_PATH,
    DEFAULT_VALUE_SURFACE_EXPANSION_OUTPUT_ROOT,
    DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    DEFAULT_VALUE_SURFACE_OUTPUT_ROOT,
    DEFAULT_VALUE_SURFACE_PATH,
    DEFAULT_VALUE_SURFACE_POLICY_KILL_DIAGNOSTIC_DOWNGRADE_PATH,
    DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
    DEFAULT_VALUE_SURFACE_UTILITY_PARETO_RANKING_PATH,
    DEFAULT_VALUE_SURFACE_V2_CONTROLLED_REVIEW_PATH,
    DEFAULT_VALUE_SURFACE_WALK_FORWARD_PATH,
    DEFAULT_VALUE_SURFACE_WARNING_TRIAGE_PATH,
    run_ai_after_chatgpt_full_regime_attribution_review,
    run_benchmark_fallback_drawdown_guard_controlled_prototype,
    run_benchmark_first_tail_risk_policy_contract,
    run_conservative_horizon_risk_filter,
    run_cost_aware_horizon_hysteresis,
    run_cost_turnover_aware_regime_conditioned_value_surface,
    run_gbdt_pivot_direction_selection,
    run_gbdt_pivot_review,
    run_gbdt_residual_hypothesis_regime_conditioning,
    run_gbdt_residual_hypothesis_triage,
    run_gbdt_value_surface_residual_diagnostic_prototype,
    run_horizon_cliff_utility_ranking_stabilization_review,
    run_horizon_selector_controlled_prototype,
    run_horizon_selector_holdout_review,
    run_horizon_selector_problem_contract,
    run_long_horizon_quarantine_fallback_review,
    run_long_horizon_quarantine_selection_review,
    run_regime_conditioned_value_surface_controlled_review,
    run_regime_conditioned_value_surface_design,
    run_regime_conditioned_walk_forward_holdout,
    run_regime_horizon_loss_attribution_matrix,
    run_regret_activation_inputs_from_value_surface_failures,
    run_regret_casebook_activation_recheck,
    run_regret_casebook_expansion_gate,
    run_regret_state_machine_controlled_prototype,
    run_simple_strategy_selector_pilot,
    run_tail_loss_avoidance_classifier_prototype,
    run_tail_loss_guardrail_fallback_policy,
    run_tail_risk_artifact_determinism_check,
    run_tail_risk_baseline_dominance_gate,
    run_tail_risk_benchmark_fallback_robustness_expansion,
    run_tail_risk_counterfactual_baseline_result_review,
    run_tail_risk_daily_reading_safety_summary,
    run_tail_risk_decision_time_boundary_audit,
    run_tail_risk_evidence_maturity_gate,
    run_tail_risk_fallback_anti_leakage_audit,
    run_tail_risk_fallback_audit_universe_reconciliation,
    run_tail_risk_fallback_blocker_diagnostic,
    run_tail_risk_fallback_counterfactual_validation,
    run_tail_risk_fallback_error_cost_ledger,
    run_tail_risk_fallback_forward_maturity_scoreboard,
    run_tail_risk_fallback_regime_segmented_robustness,
    run_tail_risk_fallback_threshold_sensitivity,
    run_tail_risk_fallback_trigger_precision_recall_audit,
    run_tail_risk_forward_aging_tracker,
    run_tail_risk_forward_evidence_integration,
    run_tail_risk_forward_outcome_contract_audit,
    run_tail_risk_governance_artifact_snapshot,
    run_tail_risk_hard_block_mutation_tests,
    run_tail_risk_independent_forward_outcome_result_review,
    run_tail_risk_independent_forward_outcome_validation,
    run_tail_risk_independent_trigger_v2_builder,
    run_tail_risk_independent_trigger_v2_input_quality_review,
    run_tail_risk_leakage_stress_suite,
    run_tail_risk_next_decision_document,
    run_tail_risk_opportunity_cost_upside_capture_review,
    run_tail_risk_policy_controlled_review_board,
    run_tail_risk_policy_family_controlled_review,
    run_tail_risk_post_merge_evidence_review,
    run_tail_risk_promotion_readiness_gate,
    run_tail_risk_real_data_validation_audit,
    run_tail_risk_regime_stratified_forward_outcome_review,
    run_tail_risk_report_registry_integrity_review,
    run_tail_risk_research_master_review,
    run_tail_risk_research_readiness_score,
    run_tail_risk_status_matrix,
    run_tail_risk_tainted_metric_quarantine,
    run_tail_risk_task_coverage_map,
    run_tail_risk_threshold_sensitivity_review,
    run_tail_risk_trigger_feature_availability_catalog,
    run_tail_risk_trigger_label_independence_audit,
    run_utility_boundary_ranking_policy_audit,
    run_utility_ranking_robustness_pareto_audit,
    run_value_surface_controlled_expansion,
    run_value_surface_controlled_prototype,
    run_value_surface_controlled_walk_forward_expansion,
    run_value_surface_direction_review,
    run_value_surface_failure_attribution,
    run_value_surface_policy_kill_diagnostic_downgrade,
    run_value_surface_utility_pareto_ranking_review,
    run_value_surface_v2_controlled_review,
    run_value_surface_warning_triage_review,
)
from ai_trading_system.controlled_strategy_batch import (
    run_controlled_strategy_batch_review as run_controlled_strategy_candidate_batch_review,
)
from ai_trading_system.controlled_strategy_batch import (
    run_gbdt_action_utility_baseline as run_controlled_gbdt_action_utility_baseline,
)
from ai_trading_system.current_subscription_qualification import (
    DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    DEFAULT_CONTROLLED_BENCHMARK_BATCH_OUTPUT_ROOT,
    DEFAULT_CONTROLLED_BENCHMARK_BATCH_REPORT_PATH,
    DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
    DEFAULT_CONTROLLED_RESEARCH_REVIEW_OUTPUT_ROOT,
    DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
    DEFAULT_FMP_DELISTED_VALIDATION_REPORT_PATH,
    DEFAULT_FMP_OWNER_REVIEW_PACKAGE_PATH,
    DEFAULT_FMP_WATCHLIST_CLOSURE_REPORT_PATH,
    DEFAULT_FORWARD_DRY_RUN_ARCHIVE_PATH,
    DEFAULT_MARKETSTACK_COVERAGE_EXPANSION_REPORT_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_REGRET_CASEBOOK_CONTROLLED_PILOT_PATH,
    DEFAULT_REGRET_CASEBOOK_OUTPUT_ROOT,
    DEFAULT_REVERSE_DIAGNOSTICS_ACTIVATION_GATE_PATH,
    DEFAULT_REVERSE_DIAGNOSTICS_CONTROLLED_PILOT_PATH,
    DEFAULT_REVERSE_DIAGNOSTICS_OUTPUT_ROOT,
    build_strategy_research_readiness_board,
    run_benchmark_controls_real_data_batch,
    run_controlled_benchmark_batch,
    run_controlled_benchmark_execution_expansion,
    run_controlled_research_batch_review,
    run_gbdt_action_utility_baseline,
    run_horizon_conditioned_value_surface_prototype,
    run_pilot_batch_review,
    run_regret_casebook_controlled_pilot,
    run_regret_casebook_failure_taxonomy_pilot,
    run_regret_driven_state_machine_prototype,
    run_reverse_diagnostics_activation_gate,
    run_reverse_diagnostics_controlled_pilot,
    run_simple_strategy_ensemble_selector_prototype,
    run_strategy_pair_reverse_diagnostics_pilot,
)
from ai_trading_system.feature_availability import DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH
from ai_trading_system.indicator_research import (
    DEFAULT_INDICATOR_OUTPUT_ROOT,
    DEFAULT_INDICATOR_REGISTRY_PATH,
    DEFAULT_MASKING_ABLATION_CAP_RATIO,
    DEFAULT_MASKING_OUTCOME_TICKER,
    DEFAULT_PIT_FEATURE_CONTRACT_REGISTRY_PATH,
    DEFAULT_THRESHOLD_REGISTRY_PATH,
    IndicatorResearchError,
    build_backtest_trace_bridge,
    build_component_level_historical_trace,
    build_coverage_audit,
    build_daily_indicator_coverage_gap_report,
    build_daily_indicator_inventory,
    build_dependency_graph,
    build_dynamic_trend_bridge_consistency_audit,
    build_dynamic_trend_full_advisory_expansion_report,
    build_dynamic_trend_threshold_calibration_prep_report,
    build_dynamic_trend_threshold_sensitivity_review,
    build_gate_availability_audit,
    build_historical_multi_stage_weight_trace_validation,
    build_indicator_diagnostics,
    build_indicator_research_gate,
    build_indicator_research_validation_rollup,
    build_lineage_manifest_repair_report,
    build_long_horizon_evidence_floor_calibration_audit,
    build_mapping_plan,
    build_masking_audit,
    build_masking_casebook,
    build_multi_stage_weight_trace_contract,
    build_ontology_payload,
    build_pit_source_readiness_audit,
    build_threshold_calibration_followup_plan,
    build_threshold_calibration_report,
    build_threshold_prioritization_report,
    build_threshold_registry_audit,
    build_valuation_crowding_ablation_validation,
    build_valuation_crowding_masking_effectiveness_review,
    build_valuation_crowding_masking_robustness_review,
    build_valuation_crowding_outcome_availability_audit,
    build_valuation_crowding_pilot_audit,
    build_valuation_crowding_pilot_validation_report,
    write_indicator_artifact_pair,
    write_indicator_framework_validation_pack,
    write_indicator_validation_pack_stability_report,
)
from ai_trading_system.portfolio_decision import (
    DEFAULT_PORTFOLIO_DECISION_CONTRACT_PATH,
    DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
    build_action_outcome_dataset,
    build_advanced_policy_compare,
    build_advanced_policy_register,
    build_advanced_policy_run,
    build_cohort_prepare,
    build_cohort_status,
    build_strategy_compare,
    build_strategy_evaluation,
    build_value_surface_evaluate,
    build_value_surface_fit,
    build_value_surface_report,
    show_portfolio_decision_contract,
    validate_portfolio_decision_contract,
)
from ai_trading_system.research_acceleration import (
    build_batch_plan,
    build_batch_rollup,
    build_batch_run,
    build_benchmark_run,
    build_control_audit,
    build_dashboard,
    build_experiment_pack,
    build_falsification_run,
    build_hypothesis_compile,
    build_pivot_review,
    build_portfolio_status,
    build_preflight,
    build_queue,
    build_queue_status,
    build_regret_casebook,
    build_review_board,
    build_strategy_pair_diagnosis,
    record_negative_result,
)
from ai_trading_system.research_campaign import (
    DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    DEFAULT_CAMPAIGN_ROOT,
    DEFAULT_GATE_POLICY_PATH,
    DEFAULT_MIGRATION_PATH,
    DEFAULT_MODULE_REGISTRY_PATH,
    DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    DEFAULT_WINDOW_POLICY_PATH,
    ResearchCampaignError,
    archive_campaign,
    build_campaign_validation_payload,
    build_case_specific_runner_deprecation_plan,
    build_owner_packet,
    build_status_payload,
    campaign_plan,
    diagnose_campaign,
    evaluate_gate,
    initialize_campaign,
    load_campaign_bundle,
    load_campaign_spec,
    run_campaign_stage,
    validate_stage_adapter_contracts,
    write_campaign_control_plane_v1_validation_artifacts,
)
from ai_trading_system.research_governance import (
    DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
    DEFAULT_RESEARCH_GOVERNANCE_POLICY_PATH,
    DEFAULT_RESEARCH_OPS_OUTPUT_ROOT,
    DEFAULT_RESEARCH_PROTOCOL_DIR,
    ResearchGovernanceError,
    build_decision_record,
    build_direction_review_status,
    build_evidence_audit,
    build_promotion_readiness,
    build_protocol_show,
    build_protocol_validation,
    build_research_rollup,
    build_sample_quality_audit,
    build_state_evaluation,
    build_threshold_dependency_audit,
    build_watchlist,
    ingest_evidence_ledger,
    write_research_artifact_pair,
)

console = Console()
research_app = typer.Typer(help="研究 Campaign 控制面。", no_args_is_help=True)
campaign_app = typer.Typer(
    help="Research Campaign spec、状态机、证据和 owner packet。",
    no_args_is_help=True,
)
indicators_app = typer.Typer(
    help="日报指标、约束、mapping、遮蔽和研究 gate 控制面。",
    no_args_is_help=True,
)
governance_app = typer.Typer(
    help="Research governance protocol/evidence/state 控制面。", no_args_is_help=True
)
acceleration_app = typer.Typer(
    help="Research acceleration diagnostics、controls、preflight。", no_args_is_help=True
)
portfolio_decision_app = typer.Typer(
    help="Portfolio decision problem contract and datasets。", no_args_is_help=True
)
strategy_app = typer.Typer(help="统一 strategy adapter evaluation harness。", no_args_is_help=True)
strategies_app = typer.Typer(
    help="TRADING-770 controlled strategy candidate prototypes。", no_args_is_help=True
)
advanced_policy_app = typer.Typer(help="Advanced policy sandbox。", no_args_is_help=True)
strategy_pilot_app = typer.Typer(
    help="Controlled strategy research pilot board and diagnostics。", no_args_is_help=True
)
controlled_pilot_app = typer.Typer(
    help="TRADING-760 controlled benchmark and control batch。", no_args_is_help=True
)
research_ops_app = typer.Typer(help="Research workstream ops and dashboard。", no_args_is_help=True)
paper_shadow_app = typer.Typer(
    help="Research paper-shadow cohort readiness。", no_args_is_help=True
)
research_app.add_typer(campaign_app, name="campaign")
research_app.add_typer(indicators_app, name="indicators")
research_app.add_typer(governance_app, name="governance")
research_app.add_typer(acceleration_app, name="acceleration")
research_app.add_typer(portfolio_decision_app, name="portfolio-decision")
research_app.add_typer(strategy_app, name="strategy")
research_app.add_typer(strategies_app, name="strategies")
research_app.add_typer(advanced_policy_app, name="advanced-policy")
research_app.add_typer(strategy_pilot_app, name="strategy-pilot")
research_app.add_typer(controlled_pilot_app, name="controlled-pilot")
research_app.add_typer(research_ops_app, name="ops")
research_app.add_typer(paper_shadow_app, name="paper-shadow")
research_app.add_typer(trends_app, name="trends")
register_research_foundation_commands(research_app)
register_simple_baseline_strategy_commands(strategies_app)
register_research_restart_commands(research_ops_app)


@controlled_pilot_app.command("benchmark-batch")
def controlled_pilot_benchmark_batch_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="FMP 主价格缓存 CSV。"),
    ] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="Marketstack 第二源价格缓存 CSV。"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-760 controlled benchmark 输出目录。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_BATCH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_controlled_benchmark_batch(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            as_of_date=_parse_optional_date(as_of),
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Controlled benchmark batch", payload)


@controlled_pilot_app.command("benchmark-expansion")
def controlled_pilot_benchmark_expansion_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="FMP 主价格缓存 CSV。"),
    ] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="Marketstack 第二源价格缓存 CSV。"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-765 benchmark expansion 输出目录。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_BATCH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_controlled_benchmark_execution_expansion(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            as_of_date=_parse_optional_date(as_of),
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Controlled benchmark execution expansion", payload)


@strategies_app.command("value-surface-controlled-prototype")
def strategies_value_surface_controlled_prototype_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-770 controlled strategy candidate config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="FMP 主价格缓存 CSV。"),
    ] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="Marketstack 第二源价格缓存 CSV。"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    benchmark_expansion: Annotated[
        Path,
        typer.Option("--benchmark-expansion", help="TRADING-765 benchmark expansion JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
    control_audit: Annotated[
        Path,
        typer.Option("--control-audit", help="TRADING-765 control audit JSON。"),
    ] = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-770 value surface 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_value_surface_controlled_prototype(
            config_path=config_path,
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            benchmark_expansion_path=benchmark_expansion,
            control_audit_path=control_audit,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_strategy_pilot_payload("Value surface controlled prototype", payload)


@strategies_app.command("regret-state-machine-controlled-prototype")
def strategies_regret_state_machine_controlled_prototype_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-771 controlled strategy candidate config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="FMP 主价格缓存 CSV。"),
    ] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="Marketstack 第二源价格缓存 CSV。"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    benchmark_expansion: Annotated[
        Path,
        typer.Option("--benchmark-expansion", help="TRADING-765 benchmark expansion JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
    control_audit: Annotated[
        Path,
        typer.Option("--control-audit", help="TRADING-765 control audit JSON。"),
    ] = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-771 state machine 输出目录。"),
    ] = DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_regret_state_machine_controlled_prototype(
            config_path=config_path,
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            benchmark_expansion_path=benchmark_expansion,
            control_audit_path=control_audit,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_strategy_pilot_payload("Regret state machine controlled prototype", payload)


@strategies_app.command("simple-strategy-selector-pilot")
def strategies_simple_strategy_selector_pilot_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-772 controlled strategy candidate config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="FMP 主价格缓存 CSV。"),
    ] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="Marketstack 第二源价格缓存 CSV。"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    benchmark_expansion: Annotated[
        Path,
        typer.Option("--benchmark-expansion", help="TRADING-765 benchmark expansion JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
    control_audit: Annotated[
        Path,
        typer.Option("--control-audit", help="TRADING-765 control audit JSON。"),
    ] = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-772 simple ensemble 输出目录。"),
    ] = DEFAULT_SIMPLE_ENSEMBLE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_strategy_selector_pilot(
            config_path=config_path,
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            benchmark_expansion_path=benchmark_expansion,
            control_audit_path=control_audit,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_strategy_pilot_payload("Simple strategy selector pilot", payload)


@strategies_app.command("gbdt-action-utility-baseline")
def strategies_gbdt_action_utility_baseline_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-773 controlled strategy candidate config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_BATCH_CONFIG_PATH,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="FMP 主价格缓存 CSV。"),
    ] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="Marketstack 第二源价格缓存 CSV。"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    benchmark_expansion: Annotated[
        Path,
        typer.Option("--benchmark-expansion", help="TRADING-765 benchmark expansion JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
    control_audit: Annotated[
        Path,
        typer.Option("--control-audit", help="TRADING-765 control audit JSON。"),
    ] = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-773 GBDT action utility 输出目录。"),
    ] = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_controlled_gbdt_action_utility_baseline(
            config_path=config_path,
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            benchmark_expansion_path=benchmark_expansion,
            control_audit_path=control_audit,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_strategy_pilot_payload("GBDT action utility baseline", payload)


@strategies_app.command("value-surface-controlled-expansion")
def strategies_value_surface_controlled_expansion_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-775 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="FMP 主价格缓存 CSV。"),
    ] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="Marketstack 第二源价格缓存 CSV。"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    benchmark_expansion: Annotated[
        Path,
        typer.Option("--benchmark-expansion", help="TRADING-765 benchmark expansion JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
    control_audit: Annotated[
        Path,
        typer.Option("--control-audit", help="TRADING-765 control audit JSON。"),
    ] = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-775 value surface expansion 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_value_surface_controlled_expansion(
            config_path=config_path,
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            benchmark_expansion_path=benchmark_expansion,
            control_audit_path=control_audit,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_strategy_pilot_payload("Value surface controlled expansion", payload)


@strategies_app.command("utility-boundary-ranking-policy-audit")
def strategies_utility_boundary_ranking_policy_audit_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-776 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-776 utility boundary audit 输出目录。"),
    ] = DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_utility_boundary_ranking_policy_audit(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Utility boundary ranking policy audit", payload)


@strategies_app.command("value-surface-warning-triage-review")
def strategies_value_surface_warning_triage_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-780 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    utility_boundary_audit: Annotated[
        Path,
        typer.Option("--utility-boundary-audit", help="TRADING-776 utility audit JSON。"),
    ] = DEFAULT_UTILITY_BOUNDARY_AUDIT_PATH,
    forward_maturity: Annotated[
        Path,
        typer.Option("--forward-maturity", help="TRADING-777 forward maturity JSON。"),
    ] = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT
    / "forward_evidence_maturity_tracker.json",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-780 value surface review 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_value_surface_warning_triage_review(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            utility_boundary_audit_path=utility_boundary_audit,
            forward_maturity_path=forward_maturity,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Value surface warning triage review", payload)


@strategies_app.command("value-surface-controlled-walk-forward-expansion")
def strategies_value_surface_controlled_walk_forward_expansion_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-785 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="FMP 主价格缓存 CSV。"),
    ] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="Marketstack 第二源价格缓存 CSV。"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    warning_triage: Annotated[
        Path,
        typer.Option("--warning-triage", help="TRADING-780 warning triage JSON。"),
    ] = DEFAULT_VALUE_SURFACE_WARNING_TRIAGE_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-785 walk-forward review 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_value_surface_controlled_walk_forward_expansion(
            config_path=config_path,
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            value_surface_expansion_path=value_surface_expansion,
            warning_triage_path=warning_triage,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_strategy_pilot_payload("Value surface controlled walk-forward expansion", payload)


@strategies_app.command("value-surface-failure-attribution")
def strategies_value_surface_failure_attribution_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-790 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="FMP 主价格缓存 CSV。"),
    ] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="Marketstack 第二源价格缓存 CSV。"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    walk_forward: Annotated[
        Path,
        typer.Option("--walk-forward", help="TRADING-785 walk-forward review JSON。"),
    ] = DEFAULT_VALUE_SURFACE_WALK_FORWARD_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-790 failure attribution 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_value_surface_failure_attribution(
            config_path=config_path,
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            value_surface_expansion_path=value_surface_expansion,
            walk_forward_path=walk_forward,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_strategy_pilot_payload("Value surface failure attribution", payload)


@strategies_app.command("utility-ranking-robustness-pareto-audit")
def strategies_utility_ranking_robustness_pareto_audit_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-781 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    utility_boundary_audit: Annotated[
        Path,
        typer.Option("--utility-boundary-audit", help="TRADING-776 utility audit JSON。"),
    ] = DEFAULT_UTILITY_BOUNDARY_AUDIT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-781 robustness audit 输出目录。"),
    ] = DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_utility_ranking_robustness_pareto_audit(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            utility_boundary_audit_path=utility_boundary_audit,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Utility ranking robustness Pareto audit", payload)


@strategies_app.command("value-surface-utility-pareto-ranking-review")
def strategies_value_surface_utility_pareto_ranking_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-786 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    utility_boundary_audit: Annotated[
        Path,
        typer.Option("--utility-boundary-audit", help="TRADING-776 utility audit JSON。"),
    ] = DEFAULT_UTILITY_BOUNDARY_AUDIT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-786 utility/Pareto review 输出目录。"),
    ] = DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_value_surface_utility_pareto_ranking_review(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            utility_boundary_audit_path=utility_boundary_audit,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Value surface utility Pareto ranking review", payload)


@strategies_app.command("horizon-cliff-utility-ranking-stabilization-review")
def strategies_horizon_cliff_utility_ranking_stabilization_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-791 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    utility_pareto_ranking: Annotated[
        Path,
        typer.Option("--utility-pareto-ranking", help="TRADING-786 utility/Pareto JSON。"),
    ] = DEFAULT_VALUE_SURFACE_UTILITY_PARETO_RANKING_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-791 stabilization review 输出目录。"),
    ] = DEFAULT_UTILITY_BOUNDARY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_horizon_cliff_utility_ranking_stabilization_review(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            utility_pareto_ranking_path=utility_pareto_ranking,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Horizon cliff utility ranking stabilization review", payload)


@strategies_app.command("gbdt-pivot-review")
def strategies_gbdt_pivot_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-778 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    gbdt_action_utility: Annotated[
        Path,
        typer.Option("--gbdt-action-utility", help="TRADING-773 GBDT baseline JSON。"),
    ] = DEFAULT_GBDT_ACTION_UTILITY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-778 GBDT pivot review 输出目录。"),
    ] = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_gbdt_pivot_review(
            config_path=config_path,
            gbdt_action_utility_path=gbdt_action_utility,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("GBDT pivot review", payload)


@strategies_app.command("gbdt-pivot-direction-selection")
def strategies_gbdt_pivot_direction_selection_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-783 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    gbdt_pivot_review: Annotated[
        Path,
        typer.Option("--gbdt-pivot-review", help="TRADING-778 GBDT pivot review JSON。"),
    ] = DEFAULT_GBDT_PIVOT_REVIEW_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-783 pivot selection 输出目录。"),
    ] = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_gbdt_pivot_direction_selection(
            config_path=config_path,
            gbdt_pivot_review_path=gbdt_pivot_review,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("GBDT pivot direction selection", payload)


@strategies_app.command("gbdt-value-surface-residual-diagnostic-prototype")
def strategies_gbdt_value_surface_residual_diagnostic_prototype_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-788 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    gbdt_pivot_selection: Annotated[
        Path,
        typer.Option("--gbdt-pivot-selection", help="TRADING-783 pivot selection JSON。"),
    ] = DEFAULT_GBDT_PIVOT_SELECTION_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-788 residual diagnostic 输出目录。"),
    ] = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_gbdt_value_surface_residual_diagnostic_prototype(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            gbdt_pivot_selection_path=gbdt_pivot_selection,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("GBDT value surface residual diagnostic prototype", payload)


@strategies_app.command("gbdt-residual-hypothesis-triage")
def strategies_gbdt_residual_hypothesis_triage_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-792 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    residual_diagnostic: Annotated[
        Path,
        typer.Option("--residual-diagnostic", help="TRADING-788 residual diagnostic JSON。"),
    ] = DEFAULT_GBDT_VALUE_SURFACE_RESIDUAL_DIAGNOSTIC_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-792 residual hypothesis triage 输出目录。"),
    ] = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_gbdt_residual_hypothesis_triage(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            residual_diagnostic_path=residual_diagnostic,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("GBDT residual hypothesis triage", payload)


@strategies_app.command("regret-casebook-expansion-gate")
def strategies_regret_casebook_expansion_gate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-779 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    regret_state_machine: Annotated[
        Path,
        typer.Option("--regret-state-machine", help="TRADING-771 state machine JSON。"),
    ] = DEFAULT_REGRET_STATE_MACHINE_PATH,
    state_transition_casebook: Annotated[
        Path,
        typer.Option("--state-transition-casebook", help="TRADING-771 state transition casebook。"),
    ] = DEFAULT_STATE_TRANSITION_CASEBOOK_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-779 regret gate 输出目录。"),
    ] = DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_regret_casebook_expansion_gate(
            config_path=config_path,
            regret_state_machine_path=regret_state_machine,
            state_transition_casebook_path=state_transition_casebook,
            value_surface_expansion_path=value_surface_expansion,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Regret casebook expansion gate", payload)


@strategies_app.command("regret-activation-inputs-from-value-surface-failures")
def strategies_regret_activation_inputs_from_value_surface_failures_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-784 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    regret_casebook_expansion_gate: Annotated[
        Path,
        typer.Option("--regret-casebook-expansion-gate", help="TRADING-779 gate JSON。"),
    ] = DEFAULT_REGRET_CASEBOOK_EXPANSION_GATE_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-784 activation inputs 输出目录。"),
    ] = DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_regret_activation_inputs_from_value_surface_failures(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            regret_casebook_expansion_gate_path=regret_casebook_expansion_gate,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Regret activation inputs", payload)


@strategies_app.command("regret-casebook-activation-recheck")
def strategies_regret_casebook_activation_recheck_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-789 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    regret_activation_inputs: Annotated[
        Path,
        typer.Option("--regret-activation-inputs", help="TRADING-784 activation inputs JSON。"),
    ] = DEFAULT_REGRET_ACTIVATION_INPUTS_PATH,
    regret_casebook_expansion_gate: Annotated[
        Path,
        typer.Option("--regret-casebook-expansion-gate", help="TRADING-779 gate JSON。"),
    ] = DEFAULT_REGRET_CASEBOOK_EXPANSION_GATE_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-789 activation recheck 输出目录。"),
    ] = DEFAULT_REGRET_STATE_MACHINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_regret_casebook_activation_recheck(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            regret_activation_inputs_path=regret_activation_inputs,
            regret_casebook_expansion_gate_path=regret_casebook_expansion_gate,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Regret casebook activation recheck", payload)


@strategies_app.command("value-surface-direction-review")
def strategies_value_surface_direction_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-794 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    failure_attribution: Annotated[
        Path,
        typer.Option("--failure-attribution", help="TRADING-790 failure attribution JSON。"),
    ] = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization: Annotated[
        Path,
        typer.Option("--horizon-stabilization", help="TRADING-791 stabilization JSON。"),
    ] = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    residual_triage: Annotated[
        Path,
        typer.Option("--residual-triage", help="TRADING-792 residual triage JSON。"),
    ] = DEFAULT_GBDT_RESIDUAL_HYPOTHESIS_TRIAGE_PATH,
    forward_continuity_extension: Annotated[
        Path,
        typer.Option("--forward-continuity-extension", help="TRADING-793 continuity JSON。"),
    ] = DEFAULT_FORWARD_EVIDENCE_CONTINUITY_EXTENSION_PATH,
    walk_forward: Annotated[
        Path,
        typer.Option("--walk-forward", help="TRADING-785 walk-forward review JSON。"),
    ] = DEFAULT_VALUE_SURFACE_WALK_FORWARD_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-794 direction review 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_value_surface_direction_review(
            config_path=config_path,
            failure_attribution_path=failure_attribution,
            horizon_stabilization_path=horizon_stabilization,
            residual_triage_path=residual_triage,
            forward_continuity_extension_path=forward_continuity_extension,
            walk_forward_path=walk_forward,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Value surface direction review", payload)


@strategies_app.command("regime-conditioned-value-surface-design")
def strategies_regime_conditioned_value_surface_design_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-795 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    failure_attribution: Annotated[
        Path,
        typer.Option("--failure-attribution", help="TRADING-790 failure attribution JSON。"),
    ] = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization: Annotated[
        Path,
        typer.Option("--horizon-stabilization", help="TRADING-791 stabilization JSON。"),
    ] = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    residual_triage: Annotated[
        Path,
        typer.Option("--residual-triage", help="TRADING-792 residual triage JSON。"),
    ] = DEFAULT_GBDT_RESIDUAL_HYPOTHESIS_TRIAGE_PATH,
    direction_review: Annotated[
        Path,
        typer.Option("--direction-review", help="TRADING-794 direction review JSON。"),
    ] = DEFAULT_VALUE_SURFACE_DIRECTION_REVIEW_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-795 regime-conditioned design 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_regime_conditioned_value_surface_design(
            config_path=config_path,
            failure_attribution_path=failure_attribution,
            horizon_stabilization_path=horizon_stabilization,
            residual_triage_path=residual_triage,
            direction_review_path=direction_review,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Regime-conditioned value surface design", payload)


@strategies_app.command("tail-loss-guardrail-fallback-policy")
def strategies_tail_loss_guardrail_fallback_policy_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-796 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution: Annotated[
        Path,
        typer.Option("--failure-attribution", help="TRADING-790 failure attribution JSON。"),
    ] = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization: Annotated[
        Path,
        typer.Option("--horizon-stabilization", help="TRADING-791 stabilization JSON。"),
    ] = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    design: Annotated[
        Path,
        typer.Option("--design", help="TRADING-795 regime-conditioned design JSON。"),
    ] = DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-796 guardrail/fallback 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_loss_guardrail_fallback_policy(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            failure_attribution_path=failure_attribution,
            horizon_stabilization_path=horizon_stabilization,
            design_path=design,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-loss guardrail fallback policy", payload)


@strategies_app.command("regime-horizon-loss-attribution-matrix")
def strategies_regime_horizon_loss_attribution_matrix_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-797 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution: Annotated[
        Path,
        typer.Option("--failure-attribution", help="TRADING-790 failure attribution JSON。"),
    ] = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-797 loss attribution matrix 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_regime_horizon_loss_attribution_matrix(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            failure_attribution_path=failure_attribution,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Regime horizon loss attribution matrix", payload)


@strategies_app.command("gbdt-residual-hypothesis-regime-conditioning")
def strategies_gbdt_residual_hypothesis_regime_conditioning_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-798 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    residual_triage: Annotated[
        Path,
        typer.Option("--residual-triage", help="TRADING-792 residual triage JSON。"),
    ] = DEFAULT_GBDT_RESIDUAL_HYPOTHESIS_TRIAGE_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-798 residual regime hypotheses 输出目录。"),
    ] = DEFAULT_GBDT_ACTION_UTILITY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_gbdt_residual_hypothesis_regime_conditioning(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            residual_triage_path=residual_triage,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("GBDT residual regime conditioning", payload)


@strategies_app.command("regime-conditioned-value-surface-controlled-review")
def strategies_regime_conditioned_value_surface_controlled_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-799 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    design: Annotated[
        Path,
        typer.Option("--design", help="TRADING-795 design JSON。"),
    ] = DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH,
    guardrail_policy: Annotated[
        Path,
        typer.Option("--guardrail-policy", help="TRADING-796 guardrail/fallback JSON。"),
    ] = DEFAULT_TAIL_LOSS_GUARDRAIL_FALLBACK_POLICY_PATH,
    loss_matrix: Annotated[
        Path,
        typer.Option("--loss-matrix", help="TRADING-797 loss matrix JSON。"),
    ] = DEFAULT_REGIME_HORIZON_LOSS_ATTRIBUTION_MATRIX_PATH,
    residual_regime: Annotated[
        Path,
        typer.Option("--residual-regime", help="TRADING-798 residual regime JSON。"),
    ] = DEFAULT_GBDT_RESIDUAL_REGIME_CONDITIONING_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-799 controlled review 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_regime_conditioned_value_surface_controlled_review(
            config_path=config_path,
            design_path=design,
            guardrail_policy_path=guardrail_policy,
            loss_matrix_path=loss_matrix,
            residual_regime_path=residual_regime,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Regime-conditioned value surface controlled review", payload)


@strategies_app.command("cost-turnover-aware-regime-conditioned-value-surface")
def strategies_cost_turnover_aware_regime_conditioned_value_surface_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-800 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution: Annotated[
        Path,
        typer.Option("--failure-attribution", help="TRADING-790 failure attribution JSON。"),
    ] = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization: Annotated[
        Path,
        typer.Option("--horizon-stabilization", help="TRADING-791 stabilization JSON。"),
    ] = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    design: Annotated[
        Path,
        typer.Option("--design", help="TRADING-795 regime-conditioned design JSON。"),
    ] = DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH,
    guardrail_policy: Annotated[
        Path,
        typer.Option("--guardrail-policy", help="TRADING-796 guardrail/fallback JSON。"),
    ] = DEFAULT_TAIL_LOSS_GUARDRAIL_FALLBACK_POLICY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-800 cost/turnover-aware 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_cost_turnover_aware_regime_conditioned_value_surface(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            failure_attribution_path=failure_attribution,
            horizon_stabilization_path=horizon_stabilization,
            design_path=design,
            guardrail_policy_path=guardrail_policy,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Cost/turnover-aware regime-conditioned value surface", payload)


@strategies_app.command("long-horizon-quarantine-selection-review")
def strategies_long_horizon_quarantine_selection_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-801 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution: Annotated[
        Path,
        typer.Option("--failure-attribution", help="TRADING-790 failure attribution JSON。"),
    ] = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization: Annotated[
        Path,
        typer.Option("--horizon-stabilization", help="TRADING-791 stabilization JSON。"),
    ] = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    cost_turnover: Annotated[
        Path,
        typer.Option("--cost-turnover", help="TRADING-800 cost/turnover-aware JSON。"),
    ] = DEFAULT_COST_TURNOVER_AWARE_VALUE_SURFACE_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-801 long-horizon review 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_long_horizon_quarantine_selection_review(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            failure_attribution_path=failure_attribution,
            horizon_stabilization_path=horizon_stabilization,
            cost_turnover_path=cost_turnover,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Long-horizon quarantine selection review", payload)


@strategies_app.command("ai-after-chatgpt-full-regime-attribution-review")
def strategies_ai_after_chatgpt_full_regime_attribution_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-802 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution: Annotated[
        Path,
        typer.Option("--failure-attribution", help="TRADING-790 failure attribution JSON。"),
    ] = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    loss_matrix: Annotated[
        Path,
        typer.Option("--loss-matrix", help="TRADING-797 loss matrix JSON。"),
    ] = DEFAULT_REGIME_HORIZON_LOSS_ATTRIBUTION_MATRIX_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-802 regime attribution 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_ai_after_chatgpt_full_regime_attribution_review(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            failure_attribution_path=failure_attribution,
            loss_matrix_path=loss_matrix,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("ai_after_chatgpt_full regime attribution review", payload)


@strategies_app.command("regime-conditioned-walk-forward-holdout")
def strategies_regime_conditioned_walk_forward_holdout_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-803 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    failure_attribution: Annotated[
        Path,
        typer.Option("--failure-attribution", help="TRADING-790 failure attribution JSON。"),
    ] = DEFAULT_VALUE_SURFACE_FAILURE_ATTRIBUTION_PATH,
    horizon_stabilization: Annotated[
        Path,
        typer.Option("--horizon-stabilization", help="TRADING-791 stabilization JSON。"),
    ] = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    design: Annotated[
        Path,
        typer.Option("--design", help="TRADING-795 design JSON。"),
    ] = DEFAULT_REGIME_CONDITIONED_VALUE_SURFACE_DESIGN_PATH,
    cost_turnover: Annotated[
        Path,
        typer.Option("--cost-turnover", help="TRADING-800 cost/turnover-aware JSON。"),
    ] = DEFAULT_COST_TURNOVER_AWARE_VALUE_SURFACE_PATH,
    horizon_quarantine: Annotated[
        Path,
        typer.Option("--horizon-quarantine", help="TRADING-801 long-horizon JSON。"),
    ] = DEFAULT_LONG_HORIZON_QUARANTINE_REVIEW_PATH,
    regime_attribution: Annotated[
        Path,
        typer.Option("--regime-attribution", help="TRADING-802 regime attribution JSON。"),
    ] = DEFAULT_AI_REGIME_ATTRIBUTION_REVIEW_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-803 holdout 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_regime_conditioned_walk_forward_holdout(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            failure_attribution_path=failure_attribution,
            horizon_stabilization_path=horizon_stabilization,
            design_path=design,
            cost_turnover_path=cost_turnover,
            horizon_quarantine_path=horizon_quarantine,
            regime_attribution_path=regime_attribution,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Regime-conditioned walk-forward holdout", payload)


@strategies_app.command("value-surface-v2-controlled-review")
def strategies_value_surface_v2_controlled_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-804 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    cost_turnover: Annotated[
        Path,
        typer.Option("--cost-turnover", help="TRADING-800 cost/turnover-aware JSON。"),
    ] = DEFAULT_COST_TURNOVER_AWARE_VALUE_SURFACE_PATH,
    horizon_quarantine: Annotated[
        Path,
        typer.Option("--horizon-quarantine", help="TRADING-801 long-horizon JSON。"),
    ] = DEFAULT_LONG_HORIZON_QUARANTINE_REVIEW_PATH,
    regime_attribution: Annotated[
        Path,
        typer.Option("--regime-attribution", help="TRADING-802 regime attribution JSON。"),
    ] = DEFAULT_AI_REGIME_ATTRIBUTION_REVIEW_PATH,
    holdout: Annotated[
        Path,
        typer.Option("--holdout", help="TRADING-803 holdout JSON。"),
    ] = DEFAULT_REGIME_CONDITIONED_WALK_FORWARD_HOLDOUT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-804 v2 controlled review 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_value_surface_v2_controlled_review(
            config_path=config_path,
            cost_turnover_path=cost_turnover,
            horizon_quarantine_path=horizon_quarantine,
            regime_attribution_path=regime_attribution,
            holdout_path=holdout,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Value surface v2 controlled review", payload)


@strategies_app.command("horizon-selector-problem-contract")
def strategies_horizon_selector_problem_contract_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-805 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    v2_review: Annotated[
        Path,
        typer.Option("--v2-review", help="TRADING-804 v2 controlled review JSON。"),
    ] = DEFAULT_VALUE_SURFACE_V2_CONTROLLED_REVIEW_PATH,
    long_horizon_review: Annotated[
        Path,
        typer.Option("--long-horizon-review", help="TRADING-801 long-horizon review JSON。"),
    ] = DEFAULT_LONG_HORIZON_QUARANTINE_REVIEW_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-805 horizon selector contract 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_horizon_selector_problem_contract(
            config_path=config_path,
            v2_review_path=v2_review,
            long_horizon_review_path=long_horizon_review,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Horizon selector problem contract", payload)


@strategies_app.command("long-horizon-quarantine-fallback-review")
def strategies_long_horizon_quarantine_fallback_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-806 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    contract: Annotated[
        Path,
        typer.Option("--contract", help="TRADING-805 horizon selector contract JSON。"),
    ] = DEFAULT_HORIZON_SELECTOR_PROBLEM_CONTRACT_PATH,
    v2_review: Annotated[
        Path,
        typer.Option("--v2-review", help="TRADING-804 v2 controlled review JSON。"),
    ] = DEFAULT_VALUE_SURFACE_V2_CONTROLLED_REVIEW_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-806 long-horizon fallback 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_long_horizon_quarantine_fallback_review(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            contract_path=contract,
            v2_review_path=v2_review,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Long-horizon quarantine fallback review", payload)


@strategies_app.command("horizon-selector-controlled-prototype")
def strategies_horizon_selector_controlled_prototype_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-807 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    contract: Annotated[
        Path,
        typer.Option("--contract", help="TRADING-805 horizon selector contract JSON。"),
    ] = DEFAULT_HORIZON_SELECTOR_PROBLEM_CONTRACT_PATH,
    fallback_review: Annotated[
        Path,
        typer.Option("--fallback-review", help="TRADING-806 long-horizon fallback JSON。"),
    ] = DEFAULT_LONG_HORIZON_QUARANTINE_FALLBACK_REVIEW_PATH,
    horizon_stabilization: Annotated[
        Path,
        typer.Option("--horizon-stabilization", help="TRADING-791 stabilization JSON。"),
    ] = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-807 selector prototype 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_horizon_selector_controlled_prototype(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            contract_path=contract,
            fallback_review_path=fallback_review,
            horizon_stabilization_path=horizon_stabilization,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Horizon selector controlled prototype", payload)


@strategies_app.command("cost-aware-horizon-hysteresis")
def strategies_cost_aware_horizon_hysteresis_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-808 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    contract: Annotated[
        Path,
        typer.Option("--contract", help="TRADING-805 horizon selector contract JSON。"),
    ] = DEFAULT_HORIZON_SELECTOR_PROBLEM_CONTRACT_PATH,
    prototype: Annotated[
        Path,
        typer.Option("--prototype", help="TRADING-807 selector prototype JSON。"),
    ] = DEFAULT_HORIZON_SELECTOR_CONTROLLED_PROTOTYPE_PATH,
    horizon_stabilization: Annotated[
        Path,
        typer.Option("--horizon-stabilization", help="TRADING-791 stabilization JSON。"),
    ] = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-808 horizon hysteresis 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_cost_aware_horizon_hysteresis(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            contract_path=contract,
            prototype_path=prototype,
            horizon_stabilization_path=horizon_stabilization,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Cost-aware horizon hysteresis", payload)


@strategies_app.command("horizon-selector-holdout-review")
def strategies_horizon_selector_holdout_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-809 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    contract: Annotated[
        Path,
        typer.Option("--contract", help="TRADING-805 horizon selector contract JSON。"),
    ] = DEFAULT_HORIZON_SELECTOR_PROBLEM_CONTRACT_PATH,
    fallback_review: Annotated[
        Path,
        typer.Option("--fallback-review", help="TRADING-806 long-horizon fallback JSON。"),
    ] = DEFAULT_LONG_HORIZON_QUARANTINE_FALLBACK_REVIEW_PATH,
    prototype: Annotated[
        Path,
        typer.Option("--prototype", help="TRADING-807 selector prototype JSON。"),
    ] = DEFAULT_HORIZON_SELECTOR_CONTROLLED_PROTOTYPE_PATH,
    hysteresis: Annotated[
        Path,
        typer.Option("--hysteresis", help="TRADING-808 cost-aware hysteresis JSON。"),
    ] = DEFAULT_COST_AWARE_HORIZON_HYSTERESIS_PATH,
    horizon_stabilization: Annotated[
        Path,
        typer.Option("--horizon-stabilization", help="TRADING-791 stabilization JSON。"),
    ] = DEFAULT_HORIZON_CLIFF_STABILIZATION_REVIEW_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-809 selector holdout 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_horizon_selector_holdout_review(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            contract_path=contract,
            fallback_review_path=fallback_review,
            prototype_path=prototype,
            hysteresis_path=hysteresis,
            horizon_stabilization_path=horizon_stabilization,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Horizon selector holdout review", payload)


@strategies_app.command("value-surface-policy-kill-diagnostic-downgrade")
def strategies_value_surface_policy_kill_diagnostic_downgrade_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-810 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    horizon_selector_holdout: Annotated[
        Path,
        typer.Option("--horizon-selector-holdout", help="TRADING-809 holdout review JSON。"),
    ] = DEFAULT_HORIZON_SELECTOR_HOLDOUT_REVIEW_PATH,
    v2_review: Annotated[
        Path,
        typer.Option("--v2-review", help="TRADING-804 value surface v2 review JSON。"),
    ] = DEFAULT_VALUE_SURFACE_V2_CONTROLLED_REVIEW_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-810 policy kill 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_value_surface_policy_kill_diagnostic_downgrade(
            config_path=config_path,
            horizon_selector_holdout_path=horizon_selector_holdout,
            v2_review_path=v2_review,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Value surface policy kill", payload)


@strategies_app.command("benchmark-first-tail-risk-policy-contract")
def strategies_benchmark_first_tail_risk_policy_contract_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-811 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    policy_kill: Annotated[
        Path,
        typer.Option("--policy-kill", help="TRADING-810 value surface policy kill JSON。"),
    ] = DEFAULT_VALUE_SURFACE_POLICY_KILL_DIAGNOSTIC_DOWNGRADE_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-811 policy contract 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_benchmark_first_tail_risk_policy_contract(
            config_path=config_path,
            policy_kill_path=policy_kill,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Benchmark-first tail-risk policy contract", payload)


@strategies_app.command("tail-loss-avoidance-classifier-prototype")
def strategies_tail_loss_avoidance_classifier_prototype_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-812 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    policy_kill: Annotated[
        Path,
        typer.Option("--policy-kill", help="TRADING-810 value surface policy kill JSON。"),
    ] = DEFAULT_VALUE_SURFACE_POLICY_KILL_DIAGNOSTIC_DOWNGRADE_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-812 classifier 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_loss_avoidance_classifier_prototype(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            policy_kill_path=policy_kill,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-loss avoidance classifier prototype", payload)


@strategies_app.command("conservative-horizon-risk-filter")
def strategies_conservative_horizon_risk_filter_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-813 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier: Annotated[
        Path,
        typer.Option("--classifier", help="TRADING-812 classifier prototype JSON。"),
    ] = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    contract: Annotated[
        Path,
        typer.Option("--contract", help="TRADING-811 policy contract JSON。"),
    ] = DEFAULT_BENCHMARK_FIRST_TAIL_RISK_POLICY_CONTRACT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-813 horizon risk filter 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_conservative_horizon_risk_filter(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            classifier_path=classifier,
            contract_path=contract,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Conservative horizon risk filter", payload)


@strategies_app.command("benchmark-fallback-drawdown-guard-prototype")
def strategies_benchmark_fallback_drawdown_guard_prototype_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-814 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier: Annotated[
        Path,
        typer.Option("--classifier", help="TRADING-812 classifier prototype JSON。"),
    ] = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    horizon_filter: Annotated[
        Path,
        typer.Option("--horizon-filter", help="TRADING-813 horizon risk filter JSON。"),
    ] = DEFAULT_CONSERVATIVE_HORIZON_RISK_FILTER_PATH,
    contract: Annotated[
        Path,
        typer.Option("--contract", help="TRADING-811 policy contract JSON。"),
    ] = DEFAULT_BENCHMARK_FIRST_TAIL_RISK_POLICY_CONTRACT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-814 fallback/drawdown 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_benchmark_fallback_drawdown_guard_controlled_prototype(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            classifier_path=classifier,
            horizon_filter_path=horizon_filter,
            contract_path=contract,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Benchmark fallback drawdown guard prototype", payload)


@strategies_app.command("tail-risk-policy-family-controlled-review")
def strategies_tail_risk_policy_family_controlled_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-815 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    policy_kill: Annotated[
        Path,
        typer.Option("--policy-kill", help="TRADING-810 value surface policy kill JSON。"),
    ] = DEFAULT_VALUE_SURFACE_POLICY_KILL_DIAGNOSTIC_DOWNGRADE_PATH,
    contract: Annotated[
        Path,
        typer.Option("--contract", help="TRADING-811 policy contract JSON。"),
    ] = DEFAULT_BENCHMARK_FIRST_TAIL_RISK_POLICY_CONTRACT_PATH,
    classifier: Annotated[
        Path,
        typer.Option("--classifier", help="TRADING-812 classifier prototype JSON。"),
    ] = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    horizon_filter: Annotated[
        Path,
        typer.Option("--horizon-filter", help="TRADING-813 horizon risk filter JSON。"),
    ] = DEFAULT_CONSERVATIVE_HORIZON_RISK_FILTER_PATH,
    fallback: Annotated[
        Path,
        typer.Option("--fallback", help="TRADING-814 fallback/drawdown JSON。"),
    ] = DEFAULT_BENCHMARK_FALLBACK_DRAWDOWN_GUARD_PROTOTYPE_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-815 controlled review 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_policy_family_controlled_review(
            config_path=config_path,
            policy_kill_path=policy_kill,
            contract_path=contract,
            classifier_path=classifier,
            horizon_filter_path=horizon_filter,
            fallback_path=fallback,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk policy family controlled review", payload)


@strategies_app.command("tail-risk-benchmark-fallback-robustness-expansion")
def strategies_tail_risk_benchmark_fallback_robustness_expansion_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-816 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier: Annotated[
        Path,
        typer.Option("--classifier", help="TRADING-812 classifier prototype JSON。"),
    ] = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    fallback: Annotated[
        Path,
        typer.Option("--fallback", help="TRADING-814 fallback/drawdown JSON。"),
    ] = DEFAULT_BENCHMARK_FALLBACK_DRAWDOWN_GUARD_PROTOTYPE_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-816 robustness 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_benchmark_fallback_robustness_expansion(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            classifier_path=classifier,
            fallback_path=fallback,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk benchmark fallback robustness", payload)


@strategies_app.command("tail-risk-fallback-trigger-precision-recall-audit")
def strategies_tail_risk_fallback_trigger_precision_recall_audit_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-817 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier: Annotated[
        Path,
        typer.Option("--classifier", help="TRADING-812 classifier prototype JSON。"),
    ] = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness: Annotated[
        Path,
        typer.Option("--robustness", help="TRADING-816 robustness JSON。"),
    ] = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-817 precision/recall 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_fallback_trigger_precision_recall_audit(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            classifier_path=classifier,
            robustness_path=robustness,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk fallback trigger precision/recall", payload)


@strategies_app.command("tail-risk-opportunity-cost-upside-capture-review")
def strategies_tail_risk_opportunity_cost_upside_capture_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-818 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier: Annotated[
        Path,
        typer.Option("--classifier", help="TRADING-812 classifier prototype JSON。"),
    ] = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness: Annotated[
        Path,
        typer.Option("--robustness", help="TRADING-816 robustness JSON。"),
    ] = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-818 opportunity cost 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_opportunity_cost_upside_capture_review(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            classifier_path=classifier,
            robustness_path=robustness,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk opportunity cost/upside capture", payload)


@strategies_app.command("tail-risk-forward-evidence-integration")
def strategies_tail_risk_forward_evidence_integration_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-819 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier: Annotated[
        Path,
        typer.Option("--classifier", help="TRADING-812 classifier prototype JSON。"),
    ] = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness: Annotated[
        Path,
        typer.Option("--robustness", help="TRADING-816 robustness JSON。"),
    ] = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    ledger_path: Annotated[
        Path,
        typer.Option("--ledger-path", help="TRADING-819 forward evidence ledger JSONL。"),
    ] = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-819 forward integration 输出目录。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_EVIDENCE_INTEGRATION_PATH.parent,
    as_of_date: Annotated[
        str | None,
        typer.Option("--as-of-date", help="TRADING-819 forward dry-run as-of date。"),
    ] = None,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_forward_evidence_integration(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            classifier_path=classifier,
            robustness_path=robustness,
            ledger_path=ledger_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of_date),
        )
    )
    _print_strategy_pilot_payload("Tail-risk forward evidence integration", payload)


@strategies_app.command("tail-risk-fallback-audit-universe-reconciliation")
def strategies_tail_risk_fallback_audit_universe_reconciliation_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-821 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    robustness: Annotated[
        Path,
        typer.Option("--robustness", help="TRADING-816 robustness JSON。"),
    ] = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    precision_recall: Annotated[
        Path,
        typer.Option("--precision-recall", help="TRADING-817 precision/recall JSON。"),
    ] = DEFAULT_TAIL_RISK_FALLBACK_TRIGGER_PRECISION_RECALL_PATH,
    opportunity_cost: Annotated[
        Path,
        typer.Option("--opportunity-cost", help="TRADING-818 opportunity cost JSON。"),
    ] = DEFAULT_TAIL_RISK_OPPORTUNITY_COST_UPSIDE_CAPTURE_PATH,
    forward_integration: Annotated[
        Path,
        typer.Option("--forward-integration", help="TRADING-819 forward integration JSON。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_EVIDENCE_INTEGRATION_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-821 reconciliation 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_fallback_audit_universe_reconciliation(
            config_path=config_path,
            robustness_path=robustness,
            precision_recall_path=precision_recall,
            opportunity_cost_path=opportunity_cost,
            forward_integration_path=forward_integration,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk fallback universe reconciliation", payload)


@strategies_app.command("tail-risk-fallback-anti-leakage-audit")
def strategies_tail_risk_fallback_anti_leakage_audit_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-822 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier: Annotated[
        Path,
        typer.Option("--classifier", help="TRADING-812 classifier prototype JSON。"),
    ] = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness: Annotated[
        Path,
        typer.Option("--robustness", help="TRADING-816 robustness JSON。"),
    ] = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-822 anti-leakage 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_fallback_anti_leakage_audit(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            classifier_path=classifier,
            robustness_path=robustness,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk fallback anti-leakage audit", payload)


@strategies_app.command("tail-risk-fallback-threshold-sensitivity")
def strategies_tail_risk_fallback_threshold_sensitivity_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-823 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier: Annotated[
        Path,
        typer.Option("--classifier", help="TRADING-812 classifier prototype JSON。"),
    ] = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness: Annotated[
        Path,
        typer.Option("--robustness", help="TRADING-816 robustness JSON。"),
    ] = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-823 sensitivity 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_fallback_threshold_sensitivity(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            classifier_path=classifier,
            robustness_path=robustness,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk fallback threshold sensitivity", payload)


@strategies_app.command("tail-risk-fallback-regime-segmented-robustness")
def strategies_tail_risk_fallback_regime_segmented_robustness_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-824 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier: Annotated[
        Path,
        typer.Option("--classifier", help="TRADING-812 classifier prototype JSON。"),
    ] = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness: Annotated[
        Path,
        typer.Option("--robustness", help="TRADING-816 robustness JSON。"),
    ] = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-824 regime segmented 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_fallback_regime_segmented_robustness(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            classifier_path=classifier,
            robustness_path=robustness,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk fallback regime-segmented robustness", payload)


@strategies_app.command("tail-risk-fallback-forward-maturity-scoreboard")
def strategies_tail_risk_fallback_forward_maturity_scoreboard_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-825 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    forward_integration: Annotated[
        Path,
        typer.Option("--forward-integration", help="TRADING-819 forward integration JSON。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_EVIDENCE_INTEGRATION_PATH,
    as_of_date: Annotated[
        str | None,
        typer.Option("--as-of-date", help="TRADING-825 maturity scoreboard as-of date。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-825 forward maturity 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_fallback_forward_maturity_scoreboard(
            config_path=config_path,
            forward_integration_path=forward_integration,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of_date),
        )
    )
    _print_strategy_pilot_payload("Tail-risk fallback forward maturity scoreboard", payload)


@strategies_app.command("tail-risk-policy-controlled-review-board")
def strategies_tail_risk_policy_controlled_review_board_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-820 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    robustness: Annotated[
        Path,
        typer.Option("--robustness", help="TRADING-816 robustness JSON。"),
    ] = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    precision_recall: Annotated[
        Path,
        typer.Option("--precision-recall", help="TRADING-817 precision/recall JSON。"),
    ] = DEFAULT_TAIL_RISK_FALLBACK_TRIGGER_PRECISION_RECALL_PATH,
    opportunity_cost: Annotated[
        Path,
        typer.Option("--opportunity-cost", help="TRADING-818 opportunity cost JSON。"),
    ] = DEFAULT_TAIL_RISK_OPPORTUNITY_COST_UPSIDE_CAPTURE_PATH,
    forward_integration: Annotated[
        Path,
        typer.Option("--forward-integration", help="TRADING-819 forward integration JSON。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_EVIDENCE_INTEGRATION_PATH,
    audit_universe_reconciliation: Annotated[
        Path,
        typer.Option(
            "--audit-universe-reconciliation", help="TRADING-821 universe reconciliation JSON。"
        ),
    ] = DEFAULT_TAIL_RISK_AUDIT_UNIVERSE_RECONCILIATION_PATH,
    anti_leakage: Annotated[
        Path,
        typer.Option("--anti-leakage", help="TRADING-822 anti-leakage JSON。"),
    ] = DEFAULT_TAIL_RISK_ANTI_LEAKAGE_AUDIT_PATH,
    sensitivity: Annotated[
        Path,
        typer.Option("--sensitivity", help="TRADING-823 sensitivity JSON。"),
    ] = DEFAULT_TAIL_RISK_THRESHOLD_SENSITIVITY_PATH,
    regime_segmented: Annotated[
        Path,
        typer.Option("--regime-segmented", help="TRADING-824 regime segmented JSON。"),
    ] = DEFAULT_TAIL_RISK_REGIME_SEGMENTED_ROBUSTNESS_PATH,
    forward_maturity_scoreboard: Annotated[
        Path,
        typer.Option("--forward-maturity-scoreboard", help="TRADING-825 scoreboard JSON。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_MATURITY_SCOREBOARD_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-820 controlled review board 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_policy_controlled_review_board(
            config_path=config_path,
            robustness_path=robustness,
            precision_recall_path=precision_recall,
            opportunity_cost_path=opportunity_cost,
            forward_integration_path=forward_integration,
            audit_universe_reconciliation_path=audit_universe_reconciliation,
            anti_leakage_path=anti_leakage,
            sensitivity_path=sensitivity,
            regime_segmented_path=regime_segmented,
            forward_maturity_scoreboard_path=forward_maturity_scoreboard,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk policy controlled review board", payload)


@strategies_app.command("tail-risk-fallback-blocker-diagnostic")
def strategies_tail_risk_fallback_blocker_diagnostic_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-826 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    review_board: Annotated[
        Path,
        typer.Option("--review-board", help="Latest tail-risk controlled review board JSON。"),
    ] = DEFAULT_TAIL_RISK_POLICY_CONTROLLED_REVIEW_BOARD_PATH,
    audit_universe_reconciliation: Annotated[
        Path | None,
        typer.Option(
            "--audit-universe-reconciliation",
            help="Optional TRADING-821 universe reconciliation JSON override。",
        ),
    ] = None,
    anti_leakage: Annotated[
        Path | None,
        typer.Option("--anti-leakage", help="Optional TRADING-822 anti-leakage JSON override。"),
    ] = None,
    sensitivity: Annotated[
        Path | None,
        typer.Option("--sensitivity", help="Optional TRADING-823 sensitivity JSON override。"),
    ] = None,
    regime_segmented: Annotated[
        Path | None,
        typer.Option("--regime-segmented", help="Optional TRADING-824 regime JSON override。"),
    ] = None,
    forward_maturity_scoreboard: Annotated[
        Path | None,
        typer.Option("--forward-maturity-scoreboard", help="Optional TRADING-825 JSON override。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-826 blocker diagnostic 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_fallback_blocker_diagnostic(
            config_path=config_path,
            review_board_path=review_board,
            audit_universe_reconciliation_path=audit_universe_reconciliation,
            anti_leakage_path=anti_leakage,
            sensitivity_path=sensitivity,
            regime_segmented_path=regime_segmented,
            forward_maturity_scoreboard_path=forward_maturity_scoreboard,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk fallback blocker diagnostic", payload)


@strategies_app.command("tail-risk-trigger-label-independence-audit")
def strategies_tail_risk_trigger_label_independence_audit_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-827 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    classifier: Annotated[
        Path,
        typer.Option("--classifier", help="Tail-loss classifier JSON。"),
    ] = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness: Annotated[
        Path,
        typer.Option("--robustness", help="TRADING-816 robustness JSON。"),
    ] = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    precision_recall: Annotated[
        Path,
        typer.Option("--precision-recall", help="TRADING-817 precision/recall JSON。"),
    ] = DEFAULT_TAIL_RISK_FALLBACK_TRIGGER_PRECISION_RECALL_PATH,
    anti_leakage: Annotated[
        Path,
        typer.Option("--anti-leakage", help="TRADING-822 anti-leakage JSON。"),
    ] = DEFAULT_TAIL_RISK_ANTI_LEAKAGE_AUDIT_PATH,
    forward_integration: Annotated[
        Path,
        typer.Option("--forward-integration", help="TRADING-819 forward integration JSON。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_EVIDENCE_INTEGRATION_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-827 independence audit 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_trigger_label_independence_audit(
            config_path=config_path,
            classifier_path=classifier,
            robustness_path=robustness,
            precision_recall_path=precision_recall,
            anti_leakage_path=anti_leakage,
            forward_integration_path=forward_integration,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk trigger/label independence audit", payload)


@strategies_app.command("tail-risk-independent-forward-outcome-validation")
def strategies_tail_risk_independent_forward_outcome_validation_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-828 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option("--value-surface-expansion", help="Value surface expansion JSON。"),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    classifier: Annotated[
        Path,
        typer.Option("--classifier", help="Tail-loss classifier JSON。"),
    ] = DEFAULT_TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPE_PATH,
    robustness: Annotated[
        Path,
        typer.Option("--robustness", help="TRADING-816 robustness JSON。"),
    ] = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    trigger_label_audit: Annotated[
        Path,
        typer.Option("--trigger-label-audit", help="TRADING-827 trigger/label audit JSON。"),
    ] = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-828 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_independent_forward_outcome_validation(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            classifier_path=classifier,
            robustness_path=robustness,
            trigger_label_audit_path=trigger_label_audit,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk independent forward outcome validation", payload)


@strategies_app.command("tail-risk-forward-outcome-contract-audit")
def strategies_tail_risk_forward_outcome_contract_audit_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-829 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    trigger_label_audit: Annotated[
        Path,
        typer.Option("--trigger-label-audit", help="TRADING-827 trigger/label audit JSON。"),
    ] = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    independent_forward: Annotated[
        Path,
        typer.Option("--independent-forward", help="TRADING-828 independent forward JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-829 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_forward_outcome_contract_audit(
            config_path=config_path,
            trigger_label_audit_path=trigger_label_audit,
            independent_forward_path=independent_forward,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk forward outcome contract audit", payload)


@strategies_app.command("tail-risk-decision-time-boundary-audit")
def strategies_tail_risk_decision_time_boundary_audit_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-830 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    trigger_label_audit: Annotated[
        Path,
        typer.Option("--trigger-label-audit", help="TRADING-827 trigger/label audit JSON。"),
    ] = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    contract_audit: Annotated[
        Path,
        typer.Option("--contract-audit", help="TRADING-829 contract audit JSON。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-830 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_decision_time_boundary_audit(
            config_path=config_path,
            trigger_label_audit_path=trigger_label_audit,
            contract_audit_path=contract_audit,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk decision-time boundary audit", payload)


@strategies_app.command("tail-risk-tainted-metric-quarantine")
def strategies_tail_risk_tainted_metric_quarantine_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-831 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    trigger_label_audit: Annotated[
        Path,
        typer.Option("--trigger-label-audit", help="TRADING-827 trigger/label audit JSON。"),
    ] = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    precision_recall: Annotated[
        Path,
        typer.Option("--precision-recall", help="TRADING-817 precision/recall JSON。"),
    ] = DEFAULT_TAIL_RISK_FALLBACK_TRIGGER_PRECISION_RECALL_PATH,
    robustness: Annotated[
        Path,
        typer.Option("--robustness", help="TRADING-816 robustness JSON。"),
    ] = DEFAULT_TAIL_RISK_BENCHMARK_FALLBACK_ROBUSTNESS_PATH,
    opportunity_cost: Annotated[
        Path,
        typer.Option("--opportunity-cost", help="TRADING-818 opportunity cost JSON。"),
    ] = DEFAULT_TAIL_RISK_OPPORTUNITY_COST_UPSIDE_CAPTURE_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-831 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_tainted_metric_quarantine(
            config_path=config_path,
            trigger_label_audit_path=trigger_label_audit,
            precision_recall_path=precision_recall,
            robustness_path=robustness,
            opportunity_cost_path=opportunity_cost,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk tainted metric quarantine", payload)


@strategies_app.command("tail-risk-fallback-counterfactual-validation")
def strategies_tail_risk_fallback_counterfactual_validation_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-832 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    independent_forward: Annotated[
        Path,
        typer.Option("--independent-forward", help="TRADING-828 independent forward JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-832 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_fallback_counterfactual_validation(
            config_path=config_path,
            independent_forward_path=independent_forward,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk fallback counterfactual validation", payload)


@strategies_app.command("tail-risk-regime-stratified-forward-outcome-review")
def strategies_tail_risk_regime_stratified_forward_outcome_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-833 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    independent_forward: Annotated[
        Path,
        typer.Option("--independent-forward", help="TRADING-828 independent forward JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    counterfactual: Annotated[
        Path,
        typer.Option("--counterfactual", help="TRADING-832 counterfactual JSON。"),
    ] = DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-833 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_regime_stratified_forward_outcome_review(
            config_path=config_path,
            independent_forward_path=independent_forward,
            counterfactual_path=counterfactual,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk regime-stratified forward outcome review", payload)


@strategies_app.command("tail-risk-threshold-sensitivity-review")
def strategies_tail_risk_threshold_sensitivity_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-834 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    independent_forward: Annotated[
        Path,
        typer.Option("--independent-forward", help="TRADING-828 independent forward JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    counterfactual: Annotated[
        Path,
        typer.Option("--counterfactual", help="TRADING-832 counterfactual JSON。"),
    ] = DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-834 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_threshold_sensitivity_review(
            config_path=config_path,
            independent_forward_path=independent_forward,
            counterfactual_path=counterfactual,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk threshold sensitivity review", payload)


@strategies_app.command("tail-risk-fallback-error-cost-ledger")
def strategies_tail_risk_fallback_error_cost_ledger_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-835 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    independent_forward: Annotated[
        Path,
        typer.Option("--independent-forward", help="TRADING-828 independent forward JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-835 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_fallback_error_cost_ledger(
            config_path=config_path,
            independent_forward_path=independent_forward,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk fallback error cost ledger", payload)


@strategies_app.command("tail-risk-evidence-maturity-gate")
def strategies_tail_risk_evidence_maturity_gate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-836 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    independent_forward: Annotated[
        Path,
        typer.Option("--independent-forward", help="TRADING-828 independent forward JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    regime_review: Annotated[
        Path,
        typer.Option("--regime-review", help="TRADING-833 regime review JSON。"),
    ] = DEFAULT_TAIL_RISK_REGIME_STRATIFIED_FORWARD_OUTCOME_REVIEW_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-836 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_evidence_maturity_gate(
            config_path=config_path,
            independent_forward_path=independent_forward,
            regime_review_path=regime_review,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk evidence maturity gate", payload)


@strategies_app.command("tail-risk-forward-aging-tracker")
def strategies_tail_risk_forward_aging_tracker_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-837 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    forward_integration: Annotated[
        Path,
        typer.Option("--forward-integration", help="TRADING-819 forward integration JSON。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_EVIDENCE_INTEGRATION_PATH,
    independent_forward: Annotated[
        Path,
        typer.Option("--independent-forward", help="TRADING-828 independent forward JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    as_of_date: Annotated[
        str | None,
        typer.Option("--as-of-date", help="TRADING-837 aging tracker as-of date。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-837 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_forward_aging_tracker(
            config_path=config_path,
            forward_integration_path=forward_integration,
            independent_forward_path=independent_forward,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of_date),
        )
    )
    _print_strategy_pilot_payload("Tail-risk forward aging tracker", payload)


@strategies_app.command("tail-risk-leakage-stress-suite")
def strategies_tail_risk_leakage_stress_suite_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-838 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    trigger_label_audit: Annotated[
        Path,
        typer.Option("--trigger-label-audit", help="TRADING-827 trigger/label audit JSON。"),
    ] = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    independent_forward: Annotated[
        Path,
        typer.Option("--independent-forward", help="TRADING-828 independent forward JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    contract_audit: Annotated[
        Path,
        typer.Option("--contract-audit", help="TRADING-829 contract audit JSON。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH,
    boundary_audit: Annotated[
        Path,
        typer.Option("--boundary-audit", help="TRADING-830 boundary audit JSON。"),
    ] = DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-838 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_leakage_stress_suite(
            config_path=config_path,
            trigger_label_audit_path=trigger_label_audit,
            independent_forward_path=independent_forward,
            contract_audit_path=contract_audit,
            boundary_audit_path=boundary_audit,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk leakage stress suite", payload)


@strategies_app.command("tail-risk-promotion-readiness-gate")
def strategies_tail_risk_promotion_readiness_gate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-839 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    trigger_label_audit: Annotated[
        Path,
        typer.Option("--trigger-label-audit", help="TRADING-827 trigger/label audit JSON。"),
    ] = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    independent_forward: Annotated[
        Path,
        typer.Option("--independent-forward", help="TRADING-828 independent forward JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    contract_audit: Annotated[
        Path,
        typer.Option("--contract-audit", help="TRADING-829 contract audit JSON。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH,
    boundary_audit: Annotated[
        Path,
        typer.Option("--boundary-audit", help="TRADING-830 boundary audit JSON。"),
    ] = DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH,
    leakage_stress: Annotated[
        Path,
        typer.Option("--leakage-stress", help="TRADING-838 leakage stress JSON。"),
    ] = DEFAULT_TAIL_RISK_LEAKAGE_STRESS_SUITE_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-839 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_promotion_readiness_gate(
            config_path=config_path,
            trigger_label_audit_path=trigger_label_audit,
            independent_forward_path=independent_forward,
            contract_audit_path=contract_audit,
            boundary_audit_path=boundary_audit,
            leakage_stress_path=leakage_stress,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk promotion readiness gate", payload)


@strategies_app.command("tail-risk-independent-trigger-v2-builder")
def strategies_tail_risk_independent_trigger_v2_builder_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-840 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option("--value-surface-expansion", help="Value surface expansion JSON。"),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    boundary_audit: Annotated[
        Path,
        typer.Option("--boundary-audit", help="TRADING-830 boundary audit JSON。"),
    ] = DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-840 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_independent_trigger_v2_builder(
            config_path=config_path,
            value_surface_expansion_path=value_surface_expansion,
            boundary_audit_path=boundary_audit,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk independent trigger v2 builder", payload)


@strategies_app.command("tail-risk-trigger-feature-availability-catalog")
def strategies_tail_risk_trigger_feature_availability_catalog_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-841 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    trigger_v2: Annotated[
        Path,
        typer.Option("--trigger-v2", help="TRADING-840 trigger v2 builder JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_TRIGGER_V2_BUILDER_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-841 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_trigger_feature_availability_catalog(
            config_path=config_path,
            trigger_v2_path=trigger_v2,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk trigger feature availability catalog", payload)


@strategies_app.command("tail-risk-research-master-review")
def strategies_tail_risk_research_master_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-842 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    trigger_label_audit: Annotated[
        Path,
        typer.Option("--trigger-label-audit", help="TRADING-827 trigger/label audit JSON。"),
    ] = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    independent_forward: Annotated[
        Path,
        typer.Option("--independent-forward", help="TRADING-828 independent forward JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    contract_audit: Annotated[
        Path,
        typer.Option("--contract-audit", help="TRADING-829 contract audit JSON。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH,
    boundary_audit: Annotated[
        Path,
        typer.Option("--boundary-audit", help="TRADING-830 boundary audit JSON。"),
    ] = DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH,
    quarantine: Annotated[
        Path,
        typer.Option("--quarantine", help="TRADING-831 quarantine JSON。"),
    ] = DEFAULT_TAIL_RISK_TAINTED_METRIC_QUARANTINE_PATH,
    counterfactual: Annotated[
        Path,
        typer.Option("--counterfactual", help="TRADING-832 counterfactual JSON。"),
    ] = DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH,
    regime_review: Annotated[
        Path,
        typer.Option("--regime-review", help="TRADING-833 regime review JSON。"),
    ] = DEFAULT_TAIL_RISK_REGIME_STRATIFIED_FORWARD_OUTCOME_REVIEW_PATH,
    sensitivity_review: Annotated[
        Path,
        typer.Option("--sensitivity-review", help="TRADING-834 sensitivity JSON。"),
    ] = DEFAULT_TAIL_RISK_THRESHOLD_SENSITIVITY_REVIEW_PATH,
    error_cost: Annotated[
        Path,
        typer.Option("--error-cost", help="TRADING-835 error cost JSON。"),
    ] = DEFAULT_TAIL_RISK_FALLBACK_ERROR_COST_LEDGER_PATH,
    evidence_gate: Annotated[
        Path,
        typer.Option("--evidence-gate", help="TRADING-836 evidence gate JSON。"),
    ] = DEFAULT_TAIL_RISK_EVIDENCE_MATURITY_GATE_PATH,
    aging_tracker: Annotated[
        Path,
        typer.Option("--aging-tracker", help="TRADING-837 aging tracker JSON。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_AGING_TRACKER_PATH,
    leakage_stress: Annotated[
        Path,
        typer.Option("--leakage-stress", help="TRADING-838 leakage stress JSON。"),
    ] = DEFAULT_TAIL_RISK_LEAKAGE_STRESS_SUITE_PATH,
    promotion_gate: Annotated[
        Path,
        typer.Option("--promotion-gate", help="TRADING-839 promotion gate JSON。"),
    ] = DEFAULT_TAIL_RISK_PROMOTION_READINESS_GATE_PATH,
    trigger_v2: Annotated[
        Path,
        typer.Option("--trigger-v2", help="TRADING-840 trigger v2 JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_TRIGGER_V2_BUILDER_PATH,
    feature_catalog: Annotated[
        Path,
        typer.Option("--feature-catalog", help="TRADING-841 feature catalog JSON。"),
    ] = DEFAULT_TAIL_RISK_TRIGGER_FEATURE_AVAILABILITY_CATALOG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-842 输出目录。"),
    ] = DEFAULT_VALUE_SURFACE_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_research_master_review(
            config_path=config_path,
            trigger_label_audit_path=trigger_label_audit,
            independent_forward_path=independent_forward,
            contract_audit_path=contract_audit,
            boundary_audit_path=boundary_audit,
            quarantine_path=quarantine,
            counterfactual_path=counterfactual,
            regime_review_path=regime_review,
            sensitivity_review_path=sensitivity_review,
            error_cost_path=error_cost,
            evidence_gate_path=evidence_gate,
            aging_tracker_path=aging_tracker,
            leakage_stress_path=leakage_stress,
            promotion_gate_path=promotion_gate,
            trigger_v2_path=trigger_v2,
            feature_catalog_path=feature_catalog,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk research master review", payload)


@strategies_app.command("tail-risk-post-merge-evidence-review")
def strategies_tail_risk_post_merge_evidence_review_command(
    trigger_label_audit: Annotated[
        Path,
        typer.Option("--trigger-label-audit", help="TRADING-827 trigger/label audit JSON。"),
    ] = DEFAULT_TAIL_RISK_TRIGGER_LABEL_INDEPENDENCE_AUDIT_PATH,
    independent_forward: Annotated[
        Path,
        typer.Option("--independent-forward", help="TRADING-828 independent forward JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    contract_audit: Annotated[
        Path,
        typer.Option("--contract-audit", help="TRADING-829 contract audit JSON。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH,
    boundary_audit: Annotated[
        Path,
        typer.Option("--boundary-audit", help="TRADING-830 boundary audit JSON。"),
    ] = DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH,
    quarantine: Annotated[
        Path,
        typer.Option("--quarantine", help="TRADING-831 quarantine JSON。"),
    ] = DEFAULT_TAIL_RISK_TAINTED_METRIC_QUARANTINE_PATH,
    counterfactual: Annotated[
        Path,
        typer.Option("--counterfactual", help="TRADING-832 counterfactual JSON。"),
    ] = DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH,
    regime_review: Annotated[
        Path,
        typer.Option("--regime-review", help="TRADING-833 regime review JSON。"),
    ] = DEFAULT_TAIL_RISK_REGIME_STRATIFIED_FORWARD_OUTCOME_REVIEW_PATH,
    sensitivity_review: Annotated[
        Path,
        typer.Option("--sensitivity-review", help="TRADING-834 sensitivity JSON。"),
    ] = DEFAULT_TAIL_RISK_THRESHOLD_SENSITIVITY_REVIEW_PATH,
    error_cost: Annotated[
        Path,
        typer.Option("--error-cost", help="TRADING-835 error cost JSON。"),
    ] = DEFAULT_TAIL_RISK_FALLBACK_ERROR_COST_LEDGER_PATH,
    evidence_gate: Annotated[
        Path,
        typer.Option("--evidence-gate", help="TRADING-836 evidence gate JSON。"),
    ] = DEFAULT_TAIL_RISK_EVIDENCE_MATURITY_GATE_PATH,
    aging_tracker: Annotated[
        Path,
        typer.Option("--aging-tracker", help="TRADING-837 aging tracker JSON。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_AGING_TRACKER_PATH,
    leakage_stress: Annotated[
        Path,
        typer.Option("--leakage-stress", help="TRADING-838 leakage stress JSON。"),
    ] = DEFAULT_TAIL_RISK_LEAKAGE_STRESS_SUITE_PATH,
    promotion_gate: Annotated[
        Path,
        typer.Option("--promotion-gate", help="TRADING-839 promotion gate JSON。"),
    ] = DEFAULT_TAIL_RISK_PROMOTION_READINESS_GATE_PATH,
    trigger_v2: Annotated[
        Path,
        typer.Option("--trigger-v2", help="TRADING-840 trigger v2 JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_TRIGGER_V2_BUILDER_PATH,
    feature_catalog: Annotated[
        Path,
        typer.Option("--feature-catalog", help="TRADING-841 feature catalog JSON。"),
    ] = DEFAULT_TAIL_RISK_TRIGGER_FEATURE_AVAILABILITY_CATALOG_PATH,
    master_review: Annotated[
        Path,
        typer.Option("--master-review", help="TRADING-842 master review JSON。"),
    ] = DEFAULT_TAIL_RISK_RESEARCH_MASTER_REVIEW_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Post-merge evidence review 输出目录。"),
    ] = DEFAULT_TAIL_RISK_POST_MERGE_EVIDENCE_REVIEW_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_post_merge_evidence_review(
            trigger_label_audit_path=trigger_label_audit,
            independent_forward_path=independent_forward,
            contract_audit_path=contract_audit,
            boundary_audit_path=boundary_audit,
            quarantine_path=quarantine,
            counterfactual_path=counterfactual,
            regime_review_path=regime_review,
            sensitivity_review_path=sensitivity_review,
            error_cost_path=error_cost,
            evidence_gate_path=evidence_gate,
            aging_tracker_path=aging_tracker,
            leakage_stress_path=leakage_stress,
            promotion_gate_path=promotion_gate,
            trigger_v2_path=trigger_v2,
            feature_catalog_path=feature_catalog,
            master_review_path=master_review,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk post-merge evidence review", payload)


@strategies_app.command("tail-risk-governance-artifact-snapshot")
def strategies_tail_risk_governance_artifact_snapshot_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-843 输出目录。"),
    ] = DEFAULT_TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_governance_artifact_snapshot(output_root=output_root)
    )
    _print_strategy_pilot_payload("Tail-risk governance artifact snapshot", payload)


@strategies_app.command("tail-risk-status-matrix")
def strategies_tail_risk_status_matrix_command(
    snapshot: Annotated[
        Path,
        typer.Option("--snapshot", help="TRADING-843 snapshot JSON。"),
    ] = DEFAULT_TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-844 输出目录。"),
    ] = DEFAULT_TAIL_RISK_STATUS_MATRIX_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_status_matrix(snapshot_path=snapshot, output_root=output_root)
    )
    _print_strategy_pilot_payload("Tail-risk status matrix", payload)


@strategies_app.command("tail-risk-real-data-validation-audit")
def strategies_tail_risk_real_data_validation_audit_command(
    snapshot: Annotated[
        Path,
        typer.Option("--snapshot", help="TRADING-843 snapshot JSON。"),
    ] = DEFAULT_TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-845 输出目录。"),
    ] = DEFAULT_TAIL_RISK_REAL_DATA_VALIDATION_AUDIT_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_real_data_validation_audit(
            snapshot_path=snapshot,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk real data validation audit", payload)


@strategies_app.command("tail-risk-independent-forward-outcome-result-review")
def strategies_tail_risk_independent_forward_outcome_result_review_command(
    independent_forward: Annotated[
        Path,
        typer.Option("--independent-forward", help="TRADING-828 independent forward JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    contract_audit: Annotated[
        Path,
        typer.Option("--contract-audit", help="TRADING-829 contract audit JSON。"),
    ] = DEFAULT_TAIL_RISK_FORWARD_OUTCOME_CONTRACT_AUDIT_PATH,
    boundary_audit: Annotated[
        Path,
        typer.Option("--boundary-audit", help="TRADING-830 boundary audit JSON。"),
    ] = DEFAULT_TAIL_RISK_DECISION_TIME_BOUNDARY_AUDIT_PATH,
    counterfactual: Annotated[
        Path,
        typer.Option("--counterfactual", help="TRADING-832 counterfactual JSON。"),
    ] = DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-846 输出目录。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_RESULT_REVIEW_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_independent_forward_outcome_result_review(
            independent_forward_path=independent_forward,
            contract_audit_path=contract_audit,
            boundary_audit_path=boundary_audit,
            counterfactual_path=counterfactual,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk independent forward outcome result review", payload)


@strategies_app.command("tail-risk-counterfactual-baseline-result-review")
def strategies_tail_risk_counterfactual_baseline_result_review_command(
    counterfactual: Annotated[
        Path,
        typer.Option("--counterfactual", help="TRADING-832 counterfactual JSON。"),
    ] = DEFAULT_TAIL_RISK_FALLBACK_COUNTERFACTUAL_VALIDATION_PATH,
    independent_forward: Annotated[
        Path,
        typer.Option("--independent-forward", help="TRADING-828 independent forward JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_VALIDATION_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-847 输出目录。"),
    ] = DEFAULT_TAIL_RISK_COUNTERFACTUAL_BASELINE_RESULT_REVIEW_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_counterfactual_baseline_result_review(
            counterfactual_path=counterfactual,
            independent_forward_path=independent_forward,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk counterfactual baseline result review", payload)


@strategies_app.command("tail-risk-artifact-determinism-check")
def strategies_tail_risk_artifact_determinism_check_command(
    snapshot: Annotated[
        Path,
        typer.Option("--snapshot", help="TRADING-843 snapshot JSON。"),
    ] = DEFAULT_TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-848 输出目录。"),
    ] = DEFAULT_TAIL_RISK_ARTIFACT_DETERMINISM_CHECK_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_artifact_determinism_check(
            snapshot_path=snapshot,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk artifact determinism check", payload)


@strategies_app.command("tail-risk-task-coverage-map")
def strategies_tail_risk_task_coverage_map_command(
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path", help="TRADING-850 coverage map Markdown path。"),
    ] = DEFAULT_TAIL_RISK_TASK_COVERAGE_MAP_DOC_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-850 输出目录。"),
    ] = DEFAULT_TAIL_RISK_TASK_COVERAGE_MAP_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_task_coverage_map(
            output_root=output_root,
            docs_path=docs_path,
        )
    )
    _print_strategy_pilot_payload("Tail-risk task coverage map", payload)


@strategies_app.command("tail-risk-hard-block-mutation-tests")
def strategies_tail_risk_hard_block_mutation_tests_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-851 输出目录。"),
    ] = DEFAULT_TAIL_RISK_HARD_BLOCK_MUTATION_TESTS_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_hard_block_mutation_tests(output_root=output_root)
    )
    _print_strategy_pilot_payload("Tail-risk hard-block mutation tests", payload)


@strategies_app.command("tail-risk-report-registry-integrity-review")
def strategies_tail_risk_report_registry_integrity_review_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-852 输出目录。"),
    ] = DEFAULT_TAIL_RISK_REPORT_REGISTRY_INTEGRITY_REVIEW_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_report_registry_integrity_review(output_root=output_root)
    )
    _print_strategy_pilot_payload("Tail-risk report registry integrity review", payload)


@strategies_app.command("tail-risk-daily-reading-safety-summary")
def strategies_tail_risk_daily_reading_safety_summary_command(
    status_matrix: Annotated[
        Path,
        typer.Option("--status-matrix", help="TRADING-844 status matrix JSON。"),
    ] = DEFAULT_TAIL_RISK_STATUS_MATRIX_PATH,
    master_review: Annotated[
        Path,
        typer.Option("--master-review", help="TRADING-842 master review JSON。"),
    ] = DEFAULT_TAIL_RISK_RESEARCH_MASTER_REVIEW_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-853 输出目录。"),
    ] = DEFAULT_TAIL_RISK_DAILY_READING_SAFETY_SUMMARY_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_daily_reading_safety_summary(
            status_matrix_path=status_matrix,
            master_review_path=master_review,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk daily reading safety summary", payload)


@strategies_app.command("tail-risk-independent-trigger-v2-input-quality-review")
def strategies_tail_risk_independent_trigger_v2_input_quality_review_command(
    feature_catalog: Annotated[
        Path,
        typer.Option("--feature-catalog", help="TRADING-841 feature catalog JSON。"),
    ] = DEFAULT_TAIL_RISK_TRIGGER_FEATURE_AVAILABILITY_CATALOG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-854 输出目录。"),
    ] = DEFAULT_TAIL_RISK_TRIGGER_V2_INPUT_QUALITY_REVIEW_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_independent_trigger_v2_input_quality_review(
            feature_catalog_path=feature_catalog,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk trigger v2 input quality review", payload)


@strategies_app.command("tail-risk-baseline-dominance-gate")
def strategies_tail_risk_baseline_dominance_gate_command(
    baseline_review: Annotated[
        Path,
        typer.Option("--baseline-review", help="TRADING-847 baseline review JSON。"),
    ] = DEFAULT_TAIL_RISK_COUNTERFACTUAL_BASELINE_RESULT_REVIEW_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-856 输出目录。"),
    ] = DEFAULT_TAIL_RISK_BASELINE_DOMINANCE_GATE_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_baseline_dominance_gate(
            baseline_review_path=baseline_review,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk baseline dominance gate", payload)


@strategies_app.command("tail-risk-research-readiness-score")
def strategies_tail_risk_research_readiness_score_command(
    status_matrix: Annotated[
        Path,
        typer.Option("--status-matrix", help="TRADING-844 status matrix JSON。"),
    ] = DEFAULT_TAIL_RISK_STATUS_MATRIX_PATH,
    forward_review: Annotated[
        Path,
        typer.Option("--forward-review", help="TRADING-846 forward review JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_RESULT_REVIEW_PATH,
    baseline_gate: Annotated[
        Path,
        typer.Option("--baseline-gate", help="TRADING-856 baseline gate JSON。"),
    ] = DEFAULT_TAIL_RISK_BASELINE_DOMINANCE_GATE_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-857 输出目录。"),
    ] = DEFAULT_TAIL_RISK_RESEARCH_READINESS_SCORE_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_research_readiness_score(
            status_matrix_path=status_matrix,
            forward_review_path=forward_review,
            baseline_gate_path=baseline_gate,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Tail-risk research readiness score", payload)


@strategies_app.command("tail-risk-next-decision-document")
def strategies_tail_risk_next_decision_document_command(
    status_matrix: Annotated[
        Path,
        typer.Option("--status-matrix", help="TRADING-844 status matrix JSON。"),
    ] = DEFAULT_TAIL_RISK_STATUS_MATRIX_PATH,
    forward_review: Annotated[
        Path,
        typer.Option("--forward-review", help="TRADING-846 forward review JSON。"),
    ] = DEFAULT_TAIL_RISK_INDEPENDENT_FORWARD_OUTCOME_RESULT_REVIEW_PATH,
    baseline_review: Annotated[
        Path,
        typer.Option("--baseline-review", help="TRADING-847 baseline review JSON。"),
    ] = DEFAULT_TAIL_RISK_COUNTERFACTUAL_BASELINE_RESULT_REVIEW_PATH,
    baseline_gate: Annotated[
        Path,
        typer.Option("--baseline-gate", help="TRADING-856 baseline gate JSON。"),
    ] = DEFAULT_TAIL_RISK_BASELINE_DOMINANCE_GATE_PATH,
    readiness: Annotated[
        Path,
        typer.Option("--readiness", help="TRADING-857 readiness JSON。"),
    ] = DEFAULT_TAIL_RISK_RESEARCH_READINESS_SCORE_PATH,
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path", help="TRADING-858 decision Markdown path。"),
    ] = DEFAULT_TAIL_RISK_NEXT_DECISION_DOC_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-858 输出目录。"),
    ] = DEFAULT_TAIL_RISK_NEXT_DECISION_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_tail_risk_next_decision_document(
            status_matrix_path=status_matrix,
            forward_review_path=forward_review,
            baseline_review_path=baseline_review,
            baseline_gate_path=baseline_gate,
            readiness_path=readiness,
            output_root=output_root,
            docs_path=docs_path,
        )
    )
    _print_strategy_pilot_payload("Tail-risk next decision document", payload)


@strategy_pilot_app.command("readiness-board")
def strategy_pilot_readiness_board_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-749 strategy readiness board 输出目录。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: build_strategy_research_readiness_board(output_root=output_root)
    )
    _print_strategy_pilot_payload("Strategy research readiness board", payload)


@strategy_pilot_app.command("benchmark-controls")
def strategy_pilot_benchmark_controls_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-750 benchmark/control batch 输出目录。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_benchmark_controls_real_data_batch(output_root=output_root)
    )
    _print_strategy_pilot_payload("Benchmark controls batch", payload)


@strategy_pilot_app.command("reverse-diagnostics")
def strategy_pilot_reverse_diagnostics_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-751 reverse diagnostics 输出目录。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_strategy_pair_reverse_diagnostics_pilot(output_root=output_root)
    )
    _print_strategy_pilot_payload("Strategy pair reverse diagnostics pilot", payload)


@strategy_pilot_app.command("regret-casebook")
def strategy_pilot_regret_casebook_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-752 regret casebook 输出目录。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_regret_casebook_failure_taxonomy_pilot(output_root=output_root)
    )
    _print_strategy_pilot_payload("Regret casebook pilot", payload)


@strategy_pilot_app.command("value-surface")
def strategy_pilot_value_surface_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-753 value surface 输出目录。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_horizon_conditioned_value_surface_prototype(output_root=output_root)
    )
    _print_strategy_pilot_payload("Horizon-conditioned value surface", payload)


@strategy_pilot_app.command("state-machine")
def strategy_pilot_state_machine_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-754 state machine 输出目录。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_regret_driven_state_machine_prototype(output_root=output_root)
    )
    _print_strategy_pilot_payload("Regret-driven state machine", payload)


@strategy_pilot_app.command("ensemble-selector")
def strategy_pilot_ensemble_selector_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-755 ensemble selector 输出目录。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_strategy_ensemble_selector_prototype(output_root=output_root)
    )
    _print_strategy_pilot_payload("Simple strategy ensemble selector", payload)


@strategy_pilot_app.command("gbdt-action-utility")
def strategy_pilot_gbdt_action_utility_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-756 GBDT action utility 输出目录。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_gbdt_action_utility_baseline(output_root=output_root)
    )
    _print_strategy_pilot_payload("GBDT action utility baseline", payload)


@strategy_pilot_app.command("batch-review")
def strategy_pilot_batch_review_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-757 pilot batch review 输出目录。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: run_pilot_batch_review(output_root=output_root))
    _print_strategy_pilot_payload("Pilot batch review", payload)


@governance_app.command("protocol-validate")
def governance_protocol_validate_command(
    protocol_dir: Annotated[
        Path,
        typer.Option("--protocol-dir", help="Research protocol 目录。"),
    ] = DEFAULT_RESEARCH_PROTOCOL_DIR,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research governance 输出目录。"),
    ] = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> None:
    """验证 research protocol registry 和 core schema。"""
    payload = _build_research_payload(lambda: build_protocol_validation(protocol_dir=protocol_dir))
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / "governance",
        artifact_id="research_protocol_validation",
    )
    _print_research_artifact("Research protocol validation", payload, paths)


@governance_app.command("protocol-show")
def governance_protocol_show_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    protocol_dir: Annotated[
        Path,
        typer.Option("--protocol-dir", help="Research protocol 目录。"),
    ] = DEFAULT_RESEARCH_PROTOCOL_DIR,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research governance 输出目录。"),
    ] = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> None:
    """输出单条 research protocol。"""
    payload = _build_research_payload(
        lambda: build_protocol_show(research_id, protocol_dir=protocol_dir)
    )
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / research_id / "governance",
        artifact_id="research_protocol",
    )
    _print_research_artifact("Research protocol", payload, paths)


@governance_app.command("evidence-ingest")
def governance_evidence_ingest_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research governance 输出目录。"),
    ] = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
    policy_path: Annotated[
        Path,
        typer.Option("--policy", help="Research governance policy。"),
    ] = DEFAULT_RESEARCH_GOVERNANCE_POLICY_PATH,
) -> None:
    """按 source policy 生成 evidence ledger。"""
    payload = _build_research_payload(
        lambda: ingest_evidence_ledger(
            research_id,
            output_root=output_root,
            policy_path=policy_path,
        )
    )
    console.print(f"ledger={payload['ledger_path']}")
    _print_status("Evidence ingest", str(payload["status"]))


@governance_app.command("evidence-audit")
def governance_evidence_audit_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research governance 输出目录。"),
    ] = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
    policy_path: Annotated[
        Path,
        typer.Option("--policy", help="Research governance policy。"),
    ] = DEFAULT_RESEARCH_GOVERNANCE_POLICY_PATH,
) -> None:
    """审计 evidence ledger source class 和 allowed uses。"""
    payload = _build_research_payload(
        lambda: build_evidence_audit(
            research_id,
            output_root=output_root,
            policy_path=policy_path,
        )
    )
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / research_id / "evidence",
        artifact_id="evidence_audit",
    )
    _print_research_artifact("Evidence audit", payload, paths)


@governance_app.command("state-evaluate")
def governance_state_evaluate_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research governance 输出目录。"),
    ] = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> None:
    """输出 research 多轴状态和 blocker taxonomy。"""
    payload = _build_research_payload(
        lambda: build_state_evaluation(research_id, output_root=output_root)
    )
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / research_id / "governance",
        artifact_id="research_state_evaluation",
    )
    _print_research_artifact("Research state evaluation", payload, paths)


@governance_app.command("sample-quality-audit")
def governance_sample_quality_audit_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research governance 输出目录。"),
    ] = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> None:
    """输出 effective evidence sample quality audit。"""
    payload = _build_research_payload(
        lambda: build_sample_quality_audit(research_id, output_root=output_root)
    )
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / research_id / "governance",
        artifact_id="sample_quality_audit",
    )
    _print_research_artifact("Sample quality audit", payload, paths)


@governance_app.command("threshold-dependency-audit")
def governance_threshold_dependency_audit_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research governance 输出目录。"),
    ] = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> None:
    """审计 research protocol threshold dependency。"""
    payload = _build_research_payload(
        lambda: build_threshold_dependency_audit(
            research_id,
            threshold_registry_path=threshold_registry_path,
        )
    )
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / research_id / "governance",
        artifact_id="threshold_dependency_audit",
    )
    _print_research_artifact("Threshold dependency audit", payload, paths)


@governance_app.command("promotion-readiness")
def governance_promotion_readiness_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research governance 输出目录。"),
    ] = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> None:
    """输出 promotion readiness single source of truth。"""
    payload = _build_research_payload(
        lambda: build_promotion_readiness(research_id, output_root=output_root)
    )
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / research_id / "governance",
        artifact_id="promotion_readiness",
    )
    _print_research_artifact("Promotion readiness", payload, paths)


@governance_app.command("decision-record")
def governance_decision_record_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    decision: Annotated[
        str,
        typer.Option("--decision", help="Decision label。"),
    ] = "WATCHLIST",
    reason: Annotated[
        str,
        typer.Option("--reason", help="Decision reason。"),
    ] = "validation-only baseline decision record",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research governance 输出目录。"),
    ] = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> None:
    """追加 research decision ledger record。"""
    payload = _build_research_payload(
        lambda: build_decision_record(
            research_id,
            decision=decision,
            reason=reason,
            output_root=output_root,
        )
    )
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / research_id / "governance",
        artifact_id="decision_record",
    )
    _print_research_artifact("Decision record", payload, paths)


@governance_app.command("rollup")
def governance_rollup_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research governance 输出目录。"),
    ] = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> None:
    """生成单条 research primary rollup。"""
    payload = _build_research_payload(
        lambda: build_research_rollup(research_id, output_root=output_root)
    )
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / research_id / "rollup",
        artifact_id="research_rollup",
    )
    _print_research_artifact("Research rollup", payload, paths)


@governance_app.command("watchlist")
def governance_watchlist_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research governance 输出目录。"),
    ] = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> None:
    """输出 research watchlist。"""
    payload = _build_research_payload(lambda: build_watchlist(output_root=output_root))
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / "governance",
        artifact_id="research_watchlist",
    )
    _print_research_artifact("Research watchlist", payload, paths)


@governance_app.command("direction-review-status")
def governance_direction_review_status_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research governance 输出目录。"),
    ] = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> None:
    """输出 research direction review status。"""
    payload = _build_research_payload(
        lambda: build_direction_review_status(research_id, output_root=output_root)
    )
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / research_id / "governance",
        artifact_id="direction_review_status",
    )
    _print_research_artifact("Direction review status", payload, paths)


@acceleration_app.command("strategy-pair-diagnose")
def acceleration_strategy_pair_diagnose_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    baseline: Annotated[str, typer.Option("--baseline", help="Baseline strategy id。")],
    teacher: Annotated[str, typer.Option("--teacher", help="Teacher strategy id。")],
) -> None:
    """运行 validation-only strategy pair reverse diagnostics。"""
    payload = _build_research_payload(
        lambda: build_strategy_pair_diagnosis(
            research_id,
            baseline=baseline,
            teacher=teacher,
        )
    )
    _print_status("Strategy pair diagnostics", str(payload["status"]))
    _print_summary(payload)


@acceleration_app.command("regret-casebook")
def acceleration_regret_casebook_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
) -> None:
    payload = _build_research_payload(lambda: build_regret_casebook(research_id))
    _print_status("Regret casebook", str(payload["status"]))
    _print_summary(payload)


@acceleration_app.command("reverse-diagnostics-controlled-pilot")
def acceleration_reverse_diagnostics_controlled_pilot_command(
    benchmark_report: Annotated[
        Path,
        typer.Option("--benchmark-report", help="TRADING-760 benchmark report JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_BATCH_REPORT_PATH,
    control_audit: Annotated[
        Path,
        typer.Option("--control-audit", help="TRADING-760 control audit JSON。"),
    ] = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-763 reverse diagnostics 输出目录。"),
    ] = DEFAULT_REVERSE_DIAGNOSTICS_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_reverse_diagnostics_controlled_pilot(
            benchmark_report_path=benchmark_report,
            control_audit_path=control_audit,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Reverse diagnostics controlled pilot", payload)


@acceleration_app.command("reverse-diagnostics-activation-gate")
def acceleration_reverse_diagnostics_activation_gate_command(
    benchmark_expansion: Annotated[
        Path,
        typer.Option("--benchmark-expansion", help="TRADING-765 benchmark expansion JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
    fmp_closure: Annotated[
        Path,
        typer.Option("--fmp-closure", help="TRADING-767 FMP closure JSON。"),
    ] = DEFAULT_FMP_WATCHLIST_CLOSURE_REPORT_PATH,
    controlled_review: Annotated[
        Path,
        typer.Option("--controlled-review", help="TRADING-764 review board JSON。"),
    ] = DEFAULT_CONTROLLED_RESEARCH_REVIEW_OUTPUT_ROOT
    / "controlled_research_batch_review.json",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-769 activation gate 输出目录。"),
    ] = DEFAULT_REVERSE_DIAGNOSTICS_ACTIVATION_GATE_PATH.parent,
) -> None:
    payload = _build_research_payload(
        lambda: run_reverse_diagnostics_activation_gate(
            benchmark_expansion_path=benchmark_expansion,
            fmp_closure_path=fmp_closure,
            controlled_review_path=controlled_review,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Reverse diagnostics activation gate", payload)


@acceleration_app.command("regret-casebook-controlled-pilot")
def acceleration_regret_casebook_controlled_pilot_command(
    reverse_diagnostics: Annotated[
        Path,
        typer.Option("--reverse-diagnostics", help="TRADING-763 reverse diagnostics JSON。"),
    ] = DEFAULT_REVERSE_DIAGNOSTICS_CONTROLLED_PILOT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-763 regret casebook 输出目录。"),
    ] = DEFAULT_REGRET_CASEBOOK_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_regret_casebook_controlled_pilot(
            reverse_diagnostics_path=reverse_diagnostics,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Regret casebook controlled pilot", payload)


@acceleration_app.command("negative-result-record")
def acceleration_negative_result_record_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    result: Annotated[
        str, typer.Option("--result", help="Negative result label。")
    ] = "evidence_required",
) -> None:
    payload = _build_research_payload(lambda: record_negative_result(research_id, result=result))
    _print_status("Negative result record", str(payload["status"]))
    _print_summary(payload)


@acceleration_app.command("benchmark-run")
def acceleration_benchmark_run_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
) -> None:
    payload = _build_research_payload(lambda: build_benchmark_run(research_id))
    _print_status("Benchmark run", str(payload["status"]))
    _print_summary(payload)


@acceleration_app.command("control-audit")
def acceleration_control_audit_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
) -> None:
    payload = _build_research_payload(lambda: build_control_audit(research_id))
    _print_status("Control audit", str(payload["status"]))
    _print_summary(payload)


@acceleration_app.command("falsification-run")
def acceleration_falsification_run_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
) -> None:
    payload = _build_research_payload(lambda: build_falsification_run(research_id))
    _print_status("Falsification run", str(payload["status"]))
    _print_summary(payload)


@acceleration_app.command("preflight")
def acceleration_preflight_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
) -> None:
    payload = _build_research_payload(lambda: build_preflight(research_id))
    _print_status("Research preflight", str(payload["status"]))
    _print_summary(payload)


@acceleration_app.command("portfolio-status")
def acceleration_portfolio_status_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research ops 输出目录。"),
    ] = DEFAULT_RESEARCH_OPS_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(build_portfolio_status)
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / "acceleration",
        artifact_id="portfolio_status",
    )
    _print_research_artifact("Research portfolio status", payload, paths)


@acceleration_app.command("pivot-review")
def acceleration_pivot_review_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research ops 输出目录。"),
    ] = DEFAULT_RESEARCH_OPS_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: build_pivot_review(research_id))
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / "acceleration",
        artifact_id=f"pivot_review_{research_id}",
    )
    _print_research_artifact("Pivot review", payload, paths)


@acceleration_app.command("hypothesis-compile")
def acceleration_hypothesis_compile_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research ops 输出目录。"),
    ] = DEFAULT_RESEARCH_OPS_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: build_hypothesis_compile(research_id))
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / "acceleration",
        artifact_id=f"hypothesis_compile_{research_id}",
    )
    _print_research_artifact("Hypothesis compile", payload, paths)


@acceleration_app.command("mutation-generate")
def acceleration_mutation_generate_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research ops 输出目录。"),
    ] = DEFAULT_RESEARCH_OPS_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: build_hypothesis_compile(research_id, artifact_id="mutation_generate")
    )
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / "acceleration",
        artifact_id=f"mutation_generate_{research_id}",
    )
    _print_research_artifact("Mutation generate", payload, paths)


@acceleration_app.command("direction-generate")
def acceleration_direction_generate_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research ops 输出目录。"),
    ] = DEFAULT_RESEARCH_OPS_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: build_hypothesis_compile(research_id, artifact_id="direction_generate")
    )
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / "acceleration",
        artifact_id=f"direction_generate_{research_id}",
    )
    _print_research_artifact("Direction generate", payload, paths)


@portfolio_decision_app.command("validate-contract")
def portfolio_decision_validate_contract_command(
    contract_path: Annotated[
        Path,
        typer.Option("--contract", help="Portfolio decision contract path。"),
    ] = DEFAULT_PORTFOLIO_DECISION_CONTRACT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Portfolio decision 输出目录。"),
    ] = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: validate_portfolio_decision_contract(contract_path=contract_path)
    )
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="portfolio_decision_contract_validation",
    )
    _print_research_artifact("Portfolio decision contract validation", payload, paths)


@portfolio_decision_app.command("show-contract")
def portfolio_decision_show_contract_command(
    contract_path: Annotated[
        Path,
        typer.Option("--contract", help="Portfolio decision contract path。"),
    ] = DEFAULT_PORTFOLIO_DECISION_CONTRACT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Portfolio decision 输出目录。"),
    ] = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: show_portfolio_decision_contract(contract_path=contract_path)
    )
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="portfolio_decision_contract",
    )
    _print_research_artifact("Portfolio decision contract", payload, paths)


@portfolio_decision_app.command("build-action-outcome-dataset")
def portfolio_decision_build_dataset_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Portfolio decision 输出目录。"),
    ] = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: build_action_outcome_dataset(research_id, output_root=output_root)
    )
    _print_status("PIT action-outcome dataset", str(payload["status"]))
    _print_summary(payload)


@portfolio_decision_app.command("value-surface-fit")
def portfolio_decision_value_surface_fit_command() -> None:
    payload = _build_research_payload(build_value_surface_fit)
    _print_status("Value surface fit", str(payload["status"]))
    _print_summary(payload)


@portfolio_decision_app.command("value-surface-evaluate")
def portfolio_decision_value_surface_evaluate_command() -> None:
    payload = _build_research_payload(build_value_surface_evaluate)
    _print_status("Value surface evaluate", str(payload["status"]))
    _print_summary(payload)


@portfolio_decision_app.command("value-surface-report")
def portfolio_decision_value_surface_report_command() -> None:
    payload = _build_research_payload(build_value_surface_report)
    _print_status("Value surface report", str(payload["status"]))
    _print_summary(payload)


@strategy_app.command("evaluate")
def strategy_evaluate_command(
    strategy_id: Annotated[str, typer.Option("--strategy", help="Strategy id。")],
    stage: Annotated[str, typer.Option("--stage", help="Evaluation stage。")],
) -> None:
    payload = _build_research_payload(
        lambda: build_strategy_evaluation(strategy_id=strategy_id, stage=stage)
    )
    _print_status("Strategy evaluation", str(payload["status"]))
    _print_summary(payload)


@strategy_app.command("compare")
def strategy_compare_command(
    run_id: Annotated[str, typer.Option("--run-id", help="Run id。")],
) -> None:
    payload = _build_research_payload(lambda: build_strategy_compare(run_id=run_id))
    _print_status("Strategy compare", str(payload["status"]))
    _print_summary(payload)


@advanced_policy_app.command("register")
def advanced_policy_register_command(
    policy_id: Annotated[
        str, typer.Option("--policy-id", help="Policy id。")
    ] = "advanced_policy_candidate",
    method: Annotated[str, typer.Option("--method", help="Advanced policy method。")] = "tree",
) -> None:
    payload = _build_research_payload(
        lambda: build_advanced_policy_register(policy_id=policy_id, method=method)
    )
    _print_status("Advanced policy register", str(payload["status"]))
    _print_summary(payload)


@advanced_policy_app.command("run")
def advanced_policy_run_command(
    policy_id: Annotated[
        str, typer.Option("--policy-id", help="Policy id。")
    ] = "advanced_policy_candidate",
) -> None:
    payload = _build_research_payload(lambda: build_advanced_policy_run(policy_id=policy_id))
    _print_status("Advanced policy run", str(payload["status"]))
    _print_summary(payload)


@advanced_policy_app.command("compare")
def advanced_policy_compare_command(
    policy_id: Annotated[
        str, typer.Option("--policy-id", help="Policy id。")
    ] = "advanced_policy_candidate",
) -> None:
    payload = _build_research_payload(lambda: build_advanced_policy_compare(policy_id=policy_id))
    _print_status("Advanced policy compare", str(payload["status"]))
    _print_summary(payload)


@research_ops_app.command("queue-build")
def research_ops_queue_build_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research ops 输出目录。"),
    ] = DEFAULT_RESEARCH_OPS_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: build_queue(output_root=output_root))
    _print_status("Research queue build", str(payload["status"]))
    _print_summary(payload)


@research_ops_app.command("queue-status")
def research_ops_queue_status_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research ops 输出目录。"),
    ] = DEFAULT_RESEARCH_OPS_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: build_queue_status(output_root=output_root))
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="queue_status",
    )
    _print_research_artifact("Research queue status", payload, paths)


@research_ops_app.command("batch-plan")
def research_ops_batch_plan_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research ops 输出目录。"),
    ] = DEFAULT_RESEARCH_OPS_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: build_batch_plan(output_root=output_root))
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="batch_plan",
    )
    _print_research_artifact("Research batch plan", payload, paths)


@research_ops_app.command("batch-run")
def research_ops_batch_run_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research ops 输出目录。"),
    ] = DEFAULT_RESEARCH_OPS_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: build_batch_run(output_root=output_root))
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="batch_run",
    )
    _print_research_artifact("Research batch run", payload, paths)


@research_ops_app.command("batch-rollup")
def research_ops_batch_rollup_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research ops 输出目录。"),
    ] = DEFAULT_RESEARCH_OPS_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: build_batch_rollup(output_root=output_root))
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="batch_rollup",
    )
    _print_research_artifact("Research batch rollup", payload, paths)


@research_ops_app.command("experiment-pack-build")
def research_ops_experiment_pack_build_command(
    research_id: Annotated[
        str,
        typer.Option("--research-id", help="Research id。"),
    ] = "portfolio_decision_problem_v1",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research ops 输出目录。"),
    ] = DEFAULT_RESEARCH_OPS_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: build_experiment_pack(research_id, output_root=output_root)
    )
    _print_status("Experiment pack build", str(payload["status"]))
    _print_summary(payload)


@research_ops_app.command("review-board")
def research_ops_review_board_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research ops 输出目录。"),
    ] = DEFAULT_RESEARCH_OPS_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: build_review_board(output_root=output_root))
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="review_board",
    )
    _print_research_artifact("Research review board", payload, paths)


@research_ops_app.command("controlled-batch-review")
def research_ops_controlled_batch_review_command(
    benchmark_report: Annotated[
        Path,
        typer.Option("--benchmark-report", help="TRADING-760 benchmark report JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_BATCH_REPORT_PATH,
    control_audit: Annotated[
        Path,
        typer.Option("--control-audit", help="TRADING-760 control audit JSON。"),
    ] = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    forward_archive: Annotated[
        Path,
        typer.Option("--forward-archive", help="TRADING-760 forward dry-run archive JSON。"),
    ] = DEFAULT_FORWARD_DRY_RUN_ARCHIVE_PATH,
    marketstack_report: Annotated[
        Path,
        typer.Option("--marketstack-report", help="TRADING-761 Marketstack expansion JSON。"),
    ] = DEFAULT_MARKETSTACK_COVERAGE_EXPANSION_REPORT_PATH,
    fmp_owner_review: Annotated[
        Path,
        typer.Option("--fmp-owner-review", help="TRADING-762 FMP owner review JSON。"),
    ] = DEFAULT_FMP_OWNER_REVIEW_PACKAGE_PATH,
    fmp_delisted_report: Annotated[
        Path,
        typer.Option("--fmp-delisted-report", help="TRADING-762 FMP delisted report JSON。"),
    ] = DEFAULT_FMP_DELISTED_VALIDATION_REPORT_PATH,
    reverse_diagnostics: Annotated[
        Path,
        typer.Option("--reverse-diagnostics", help="TRADING-763 reverse diagnostics JSON。"),
    ] = DEFAULT_REVERSE_DIAGNOSTICS_CONTROLLED_PILOT_PATH,
    regret_casebook: Annotated[
        Path,
        typer.Option("--regret-casebook", help="TRADING-763 regret casebook JSON。"),
    ] = DEFAULT_REGRET_CASEBOOK_CONTROLLED_PILOT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-764 review board 输出目录。"),
    ] = DEFAULT_CONTROLLED_RESEARCH_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_controlled_research_batch_review(
            benchmark_report_path=benchmark_report,
            control_audit_path=control_audit,
            forward_archive_path=forward_archive,
            marketstack_report_path=marketstack_report,
            fmp_owner_review_path=fmp_owner_review,
            fmp_delisted_report_path=fmp_delisted_report,
            reverse_diagnostics_path=reverse_diagnostics,
            regret_casebook_path=regret_casebook,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Controlled research batch review", payload)


@research_ops_app.command("controlled-strategy-batch-review")
def research_ops_controlled_strategy_batch_review_command(
    value_surface: Annotated[
        Path,
        typer.Option("--value-surface", help="TRADING-770 value surface JSON。"),
    ] = DEFAULT_VALUE_SURFACE_PATH,
    regret_state_machine: Annotated[
        Path,
        typer.Option("--regret-state-machine", help="TRADING-771 state machine JSON。"),
    ] = DEFAULT_REGRET_STATE_MACHINE_PATH,
    simple_selector: Annotated[
        Path,
        typer.Option("--simple-selector", help="TRADING-772 simple selector JSON。"),
    ] = DEFAULT_SIMPLE_STRATEGY_SELECTOR_PATH,
    gbdt_action_utility: Annotated[
        Path,
        typer.Option("--gbdt-action-utility", help="TRADING-773 GBDT utility JSON。"),
    ] = DEFAULT_GBDT_ACTION_UTILITY_PATH,
    benchmark_expansion: Annotated[
        Path,
        typer.Option("--benchmark-expansion", help="TRADING-765 benchmark expansion JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
    forward_archive: Annotated[
        Path,
        typer.Option("--forward-archive", help="Forward evidence dry-run archive JSON。"),
    ] = DEFAULT_FORWARD_DRY_RUN_ARCHIVE_PATH,
    fmp_closure: Annotated[
        Path,
        typer.Option("--fmp-closure", help="TRADING-767 FMP closure JSON。"),
    ] = DEFAULT_FMP_WATCHLIST_CLOSURE_REPORT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-774 review board 输出目录。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_BATCH_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_controlled_strategy_candidate_batch_review(
            value_surface_path=value_surface,
            regret_state_machine_path=regret_state_machine,
            simple_selector_path=simple_selector,
            gbdt_action_utility_path=gbdt_action_utility,
            benchmark_expansion_path=benchmark_expansion,
            forward_archive_path=forward_archive,
            fmp_closure_path=fmp_closure,
            output_root=output_root,
        )
    )
    _print_strategy_pilot_payload("Controlled strategy batch review", payload)


@research_ops_app.command("dashboard")
def research_ops_dashboard_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research ops 输出目录。"),
    ] = DEFAULT_RESEARCH_OPS_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: build_dashboard(output_root=output_root))
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="research_ops_dashboard",
    )
    _print_research_artifact("Research ops dashboard", payload, paths)


@paper_shadow_app.command("cohort-prepare")
def paper_shadow_cohort_prepare_command(
    candidate_id: Annotated[
        str,
        typer.Option("--candidate-id", help="Candidate id。"),
    ] = "candidate_requires_human_review",
    strategy_id: Annotated[
        str,
        typer.Option("--strategy-id", help="Strategy id。"),
    ] = "strategy_requires_review",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Portfolio decision 输出目录。"),
    ] = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: build_cohort_prepare(
            candidate_id=candidate_id,
            strategy_id=strategy_id,
            output_root=output_root,
        )
    )
    _print_status("Paper-shadow cohort prepare", str(payload["status"]))
    _print_summary(payload)


@paper_shadow_app.command("cohort-status")
def paper_shadow_cohort_status_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Portfolio decision 输出目录。"),
    ] = DEFAULT_PORTFOLIO_DECISION_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: build_cohort_status(output_root=output_root))
    paths = write_research_artifact_pair(
        payload,
        output_root=output_root / "paper_shadow",
        artifact_id="paper_shadow_cohort_status",
    )
    _print_research_artifact("Paper-shadow cohort status", payload, paths)


@indicators_app.command("ontology")
def indicator_ontology_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """验证并输出 indicator / signal / constraint ontology。"""
    payload = _build_indicator_payload(lambda: build_ontology_payload(registry_path=registry_path))
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="indicator_research_ontology",
    )
    _print_indicator_artifact("Indicator ontology", payload, paths)


@indicators_app.command("inventory")
def indicator_inventory_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """扫描当前日报指标、约束和 heuristic registry，输出全量 inventory。"""
    payload = _build_indicator_payload(
        lambda: build_daily_indicator_inventory(registry_path=registry_path)
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="daily_indicator_inventory",
    )
    _print_indicator_artifact("Daily indicator inventory", payload, paths)


@indicators_app.command("coverage")
def indicator_coverage_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 research coverage 分类和高影响未验证缺口。"""
    payload = _build_indicator_payload(lambda: build_coverage_audit(registry_path=registry_path))
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="indicator_research_coverage_audit",
    )
    _print_indicator_artifact("Indicator coverage audit", payload, paths)


@indicators_app.command("coverage-gap")
def indicator_coverage_gap_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选日报 multi-stage weight trace JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出日报指标 coverage gap report。"""
    payload = _build_indicator_payload(
        lambda: build_daily_indicator_coverage_gap_report(
            registry_path=registry_path,
            trace_path=trace_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="daily_indicator_coverage_gap_report",
    )
    _print_indicator_artifact("Daily indicator coverage gap", payload, paths)


@indicators_app.command("threshold-audit")
def indicator_threshold_audit_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-693 threshold registry audit。"""
    payload = _build_indicator_payload(
        lambda: build_threshold_registry_audit(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="threshold_registry_audit",
    )
    _print_indicator_artifact("Threshold registry audit", payload, paths)


@indicators_app.command("threshold-prioritization")
def indicator_threshold_prioritization_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-693 high-impact threshold prioritization report。"""
    payload = _build_indicator_payload(
        lambda: build_threshold_prioritization_report(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="threshold_prioritization_report",
    )
    _print_indicator_artifact("Threshold prioritization report", payload, paths)


@indicators_app.command("threshold-calibration")
def indicator_threshold_calibration_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage / historical trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 realized outcome prices CSV。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit root。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option(
            "--bridge-artifact-root", help="可选 backtest/advisory bridge artifact root。"
        ),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="默认 outcome ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="Capped masking counterfactual ratio。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 trace/filter start date。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 trace/filter end date。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选 event window start。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选 event window end。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="逗号分隔 asset universe。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-695 indicator research threshold calibration sensitivity report。"""
    payload = _build_indicator_payload(
        lambda: build_threshold_calibration_report(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="threshold_calibration_report",
    )
    _print_indicator_artifact("Threshold calibration report", payload, paths)


@indicators_app.command("threshold-calibration-followup")
def indicator_threshold_calibration_followup_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    calibration_report_path: Annotated[
        Path | None,
        typer.Option("--calibration-report", help="可选 threshold_calibration_report JSON。"),
    ] = None,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="无 calibration report 时可选 trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="无 calibration report 时可选 prices CSV。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="无 calibration report 时可选 gate audit root。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option(
            "--bridge-artifact-root",
            help="无 calibration report 时可选 bridge artifact root。",
        ),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="默认 outcome ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="Capped masking counterfactual ratio。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 trace/filter start date。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 trace/filter end date。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选 event window start。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选 event window end。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="逗号分隔 asset universe。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-696 threshold calibration follow-up plan。"""
    payload = _build_indicator_payload(
        lambda: build_threshold_calibration_followup_plan(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
            calibration_report_path=calibration_report_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="threshold_calibration_followup_plan",
    )
    _print_indicator_artifact("Threshold calibration follow-up plan", payload, paths)


@indicators_app.command("dynamic-trend-threshold-calibration-prep")
def indicator_dynamic_trend_threshold_calibration_prep_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-697 dynamic allocation / trend threshold calibration prep report。"""
    payload = _build_indicator_payload(
        lambda: build_dynamic_trend_threshold_calibration_prep_report(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="dynamic_trend_threshold_calibration_prep_report",
    )
    _print_indicator_artifact("Dynamic/trend threshold calibration prep report", payload, paths)


@indicators_app.command("dynamic-trend-threshold-sensitivity-review")
def indicator_dynamic_trend_threshold_sensitivity_review_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage / historical trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 realized outcome prices CSV。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit root。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option(
            "--bridge-artifact-root", help="可选 backtest/advisory bridge artifact root。"
        ),
    ] = None,
    coverage_extension_root: Annotated[
        Path | None,
        typer.Option(
            "--coverage-extension-root",
            help="可选 TRADING-699 coverage extension root，例如 outputs/research_campaigns。",
        ),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="默认 outcome ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 trace/filter start date。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 trace/filter end date。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选 event window start。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选 event window end。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="逗号分隔 asset universe。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-698 dynamic allocation / trend threshold sensitivity review。"""
    payload = _build_indicator_payload(
        lambda: build_dynamic_trend_threshold_sensitivity_review(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            coverage_extension_root=coverage_extension_root,
            outcome_ticker=outcome_ticker,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="dynamic_trend_threshold_sensitivity_review",
    )
    _print_indicator_artifact(
        "Dynamic/trend threshold sensitivity review",
        payload,
        paths,
    )


@indicators_app.command("dynamic-trend-bridge-consistency-audit")
def indicator_dynamic_trend_bridge_consistency_audit_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    sensitivity_review_path: Annotated[
        Path | None,
        typer.Option(
            "--sensitivity-review",
            help="可选 TRADING-699 dynamic_trend_threshold_sensitivity_review JSON。",
        ),
    ] = None,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage / historical trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 realized outcome prices CSV。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit root。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option(
            "--bridge-artifact-root", help="可选 backtest/advisory bridge artifact root。"
        ),
    ] = None,
    coverage_extension_root: Annotated[
        Path | None,
        typer.Option(
            "--coverage-extension-root",
            help="可选 TRADING-699 coverage extension root，例如 outputs/research_campaigns。",
        ),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="默认 outcome ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 trace/filter start date。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 trace/filter end date。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选 event window start。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选 event window end。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="逗号分隔 asset universe。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-700 dynamic/trend bridge consistency audit。"""
    payload = _build_indicator_payload(
        lambda: build_dynamic_trend_bridge_consistency_audit(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
            sensitivity_review_path=sensitivity_review_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            coverage_extension_root=coverage_extension_root,
            outcome_ticker=outcome_ticker,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="dynamic_trend_bridge_consistency_audit",
    )
    _print_indicator_artifact(
        "Dynamic/trend bridge consistency audit",
        payload,
        paths,
    )


@indicators_app.command("dynamic-trend-full-advisory-expansion")
def indicator_dynamic_trend_full_advisory_expansion_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="TRADING-699/700 expanded historical trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 realized outcome prices CSV。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="historical gate audit root。"),
    ] = None,
    coverage_extension_root: Annotated[
        Path | None,
        typer.Option(
            "--coverage-extension-root",
            help="可选 TRADING-699 coverage extension root，例如 outputs/research_campaigns。",
        ),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="默认 outcome ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 trace/filter start date。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 trace/filter end date。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选 event window start。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选 event window end。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="逗号分隔 asset universe。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-701 dynamic/trend full-advisory expansion report。"""
    payload = _build_indicator_payload(
        lambda: build_dynamic_trend_full_advisory_expansion_report(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            coverage_extension_root=coverage_extension_root,
            expanded_trace_output_path=(
                output_root / "dynamic_trend_full_advisory_expanded_trace.json"
            ),
            outcome_ticker=outcome_ticker,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="dynamic_trend_full_advisory_expansion_report",
    )
    _print_indicator_artifact(
        "Dynamic/trend full-advisory expansion report",
        payload,
        paths,
    )


@indicators_app.command("pit-source-readiness-audit")
def indicator_pit_source_readiness_audit_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    feature_availability_config_path: Annotated[
        Path,
        typer.Option("--feature-availability-config", help="PIT feature availability catalog。"),
    ] = DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
    pit_contract_registry_path: Annotated[
        Path,
        typer.Option(
            "--pit-contract-registry", help="PIT feature availability contract registry。"
        ),
    ] = DEFAULT_PIT_FEATURE_CONTRACT_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 historical/replay multi-stage trace JSON。"),
    ] = None,
    blocked_dates_source_path: Annotated[
        Path | None,
        typer.Option("--blocked-dates-source", help="可选 blocked-date source artifact JSON。"),
    ] = None,
    date_range: Annotated[
        str | None,
        typer.Option("--date-range", help="可选日期区间 START:END 或 START..END。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="historical gate audit 输出根目录。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    feature_family: Annotated[
        str | None,
        typer.Option("--feature-family", help="可选 PIT feature family 过滤。"),
    ] = None,
    include_sec_edgar: Annotated[
        bool,
        typer.Option(
            "--include-sec-edgar/--exclude-sec-edgar", help="是否包含 SEC/EDGAR PIT features。"
        ),
    ] = True,
    include_fundamental: Annotated[
        bool,
        typer.Option(
            "--include-fundamental/--exclude-fundamental",
            help="是否包含 fundamental / valuation features。",
        ),
    ] = True,
    include_macro: Annotated[
        bool,
        typer.Option(
            "--include-macro/--exclude-macro", help="是否包含 macro / calendar features。"
        ),
    ] = True,
    include_price: Annotated[
        bool,
        typer.Option(
            "--include-price/--exclude-price",
            help="是否包含 price / volume / volatility features。",
        ),
    ] = True,
    include_trend_risk: Annotated[
        bool,
        typer.Option(
            "--include-trend-risk/--exclude-trend-risk", help="是否包含 trend / risk features。"
        ),
    ] = True,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-702 PIT data source readiness audit。"""
    payload = _build_indicator_payload(
        lambda: build_pit_source_readiness_audit(
            registry_path=registry_path,
            feature_availability_config_path=feature_availability_config_path,
            pit_contract_registry_path=pit_contract_registry_path,
            gate_audit_root=gate_audit_root,
            trace_path=trace_path,
            blocked_dates_source_path=blocked_dates_source_path,
            date_range=date_range,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
            feature_family=feature_family,
            include_sec_edgar=include_sec_edgar,
            include_fundamental=include_fundamental,
            include_macro=include_macro,
            include_price=include_price,
            include_trend_risk=include_trend_risk,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="pit_source_readiness_audit",
    )
    _print_indicator_artifact("PIT source readiness audit", payload, paths)


@indicators_app.command("graph")
def indicator_graph_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 Feature -> Indicator -> Mapping -> Constraint -> Research Weight 依赖图。"""
    payload = _build_indicator_payload(
        lambda: build_dependency_graph(registry_path=registry_path, trace_path=trace_path)
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="indicator_dependency_graph",
    )
    _print_indicator_artifact("Indicator dependency graph", payload, paths)


@indicators_app.command("trace-contract")
def indicator_trace_contract_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 multi-stage research weight trace contract。"""
    payload = _build_indicator_payload(
        lambda: build_multi_stage_weight_trace_contract(registry_path=registry_path)
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="multi_stage_weight_trace_contract",
    )
    _print_indicator_artifact("Multi-stage weight trace contract", payload, paths)


@indicators_app.command("diagnose")
def indicator_diagnose_command(
    indicator_id: Annotated[str, typer.Option("--id", help="Indicator id。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 mapping-free diagnostics，不生成 research weight。"""
    payload = _build_indicator_payload(
        lambda: build_indicator_diagnostics(
            indicator_id=indicator_id,
            registry_path=registry_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id=f"mapping_free_indicator_diagnostics_{indicator_id}",
    )
    _print_indicator_artifact("Indicator diagnostics", payload, paths)


@indicators_app.command("mapping-plan")
def indicator_mapping_plan_command(
    indicator_id: Annotated[str, typer.Option("--id", help="Indicator id。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """生成 mapping candidate hypothesis cards，不运行 backfill。"""
    payload = _build_indicator_payload(
        lambda: build_mapping_plan(indicator_id=indicator_id, registry_path=registry_path)
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id=f"indicator_mapping_candidate_plan_{indicator_id}",
    )
    _print_indicator_artifact("Indicator mapping plan", payload, paths)


@indicators_app.command("masking")
def indicator_masking_command(
    indicator_id: Annotated[str, typer.Option("--id", help="Indicator id。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """计算或声明 masking ratio / dominance audit。"""
    payload = _build_indicator_payload(
        lambda: build_masking_audit(
            indicator_id=indicator_id,
            registry_path=registry_path,
            trace_path=trace_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id=f"indicator_masking_and_dominance_audit_{indicator_id}",
    )
    _print_indicator_artifact("Indicator masking audit", payload, paths)


@indicators_app.command("gate")
def indicator_gate_command(
    indicator_id: Annotated[str, typer.Option("--id", help="Indicator id。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 indicator-to-signal research gate。"""
    payload = _build_indicator_payload(
        lambda: build_indicator_research_gate(
            indicator_id=indicator_id,
            registry_path=registry_path,
            trace_path=trace_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id=f"indicator_to_signal_research_gate_{indicator_id}",
    )
    _print_indicator_artifact("Indicator research gate", payload, paths)


@indicators_app.command("valuation-crowding-pilot")
def valuation_crowding_pilot_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 valuation/crowding 高影响覆盖审计 pilot。"""
    payload = _build_indicator_payload(
        lambda: build_valuation_crowding_pilot_audit(
            registry_path=registry_path,
            trace_path=trace_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="valuation_crowding_pilot_audit",
    )
    _print_indicator_artifact("Valuation/crowding pilot", payload, paths)


@indicators_app.command("valuation-crowding-pilot-validation")
def valuation_crowding_pilot_validation_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 valuation/crowding high-impact pilot validation report。"""
    payload = _build_indicator_payload(
        lambda: build_valuation_crowding_pilot_validation_report(
            registry_path=registry_path,
            trace_path=trace_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="valuation_crowding_pilot_validation_report",
    )
    _print_indicator_artifact("Valuation/crowding pilot validation", payload, paths)


@indicators_app.command("masking-casebook")
def indicator_masking_casebook_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 forward outcome。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="casebook outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 valuation/crowding -> trend masking casebook。"""
    payload = _build_indicator_payload(
        lambda: build_masking_casebook(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            outcome_ticker=outcome_ticker,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="indicator_masking_casebook_valuation_crowding_trend",
    )
    _print_indicator_artifact("Indicator masking casebook", payload, paths)


@indicators_app.command("ablation-validation")
def indicator_ablation_validation_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 forward outcome。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="ablation outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 valuation/crowding masking 只读 counterfactual/ablation validation。"""
    payload = _build_indicator_payload(
        lambda: build_valuation_crowding_ablation_validation(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="valuation_crowding_ablation_validation",
    )
    _print_indicator_artifact("Valuation/crowding ablation validation", payload, paths)


@indicators_app.command("masking-effectiveness-review")
def indicator_masking_effectiveness_review_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 forward outcome。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="effectiveness outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 valuation/crowding masking effectiveness review。"""
    payload = _build_indicator_payload(
        lambda: build_valuation_crowding_masking_effectiveness_review(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="valuation_crowding_masking_effectiveness_review",
    )
    _print_indicator_artifact("Masking effectiveness review", payload, paths)


@indicators_app.command("masking-robustness-review")
def indicator_masking_robustness_review_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 forward outcome。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="robustness outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 valuation/crowding masking robustness review。"""
    payload = _build_indicator_payload(
        lambda: build_valuation_crowding_masking_robustness_review(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="valuation_crowding_masking_robustness_review",
    )
    _print_indicator_artifact("Masking robustness review", payload, paths)


@indicators_app.command("validation-rollup")
def indicator_validation_rollup_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 forward outcome。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="rollup outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-665～691 indicator research validation rollup。"""
    payload = _build_indicator_payload(
        lambda: build_indicator_research_validation_rollup(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="indicator_research_validation_rollup",
    )
    _print_indicator_artifact("Indicator research validation rollup", payload, paths)


@indicators_app.command("long-horizon-floor-calibration-audit")
def indicator_long_horizon_floor_calibration_audit_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 forward outcome。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="calibration audit outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 long-horizon evidence floor calibration audit。"""
    payload = _build_indicator_payload(
        lambda: build_long_horizon_evidence_floor_calibration_audit(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="long_horizon_evidence_floor_calibration_audit",
    )
    _print_indicator_artifact("Long-horizon floor calibration audit", payload, paths)


@indicators_app.command("outcome-availability-audit")
def indicator_outcome_availability_audit_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 realized outcome。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 valuation/crowding realized outcome availability audit。"""
    payload = _build_indicator_payload(
        lambda: build_valuation_crowding_outcome_availability_audit(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="valuation_crowding_outcome_availability_audit",
    )
    _print_indicator_artifact("Outcome availability audit", payload, paths)


@indicators_app.command("historical-trace-validation")
def indicator_historical_trace_validation_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 historical/replay multi-stage trace JSON。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 historical replay/backtest multi-stage weight trace validation。"""
    payload = _build_indicator_payload(
        lambda: build_historical_multi_stage_weight_trace_validation(
            registry_path=registry_path,
            trace_path=trace_path,
            gate_audit_root=gate_audit_root,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="historical_multi_stage_weight_trace_validation",
    )
    _print_indicator_artifact("Historical trace validation", payload, paths)


@indicators_app.command("gate-availability-audit")
def indicator_gate_availability_audit_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 historical/replay multi-stage trace JSON。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="historical gate audit 输出根目录。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 historical trace gate availability audit。"""
    payload = _build_indicator_payload(
        lambda: build_gate_availability_audit(
            registry_path=registry_path,
            gate_audit_root=gate_audit_root,
            trace_path=trace_path,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="historical_trace_gate_availability_audit",
    )
    _print_indicator_artifact("Historical gate availability audit", payload, paths)


@indicators_app.command("lineage-manifest-repair")
def indicator_lineage_manifest_repair_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 historical/replay multi-stage trace JSON。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="historical gate audit 输出根目录。"),
    ] = None,
    root_cause_audit_path: Annotated[
        Path | None,
        typer.Option(
            "--root-cause-audit-path",
            help="可选修复前 gate availability audit JSON，用于锁定 affected artifacts。",
        ),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 historical replay lineage manifest repair report。"""
    payload = _build_indicator_payload(
        lambda: build_lineage_manifest_repair_report(
            registry_path=registry_path,
            trace_path=trace_path,
            gate_audit_root=gate_audit_root,
            root_cause_audit_path=root_cause_audit_path,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="lineage_manifest_repair_report",
    )
    _print_indicator_artifact("Lineage manifest repair", payload, paths)


@indicators_app.command("component-historical-trace")
def indicator_component_historical_trace_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 historical/replay multi-stage trace JSON。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 component-level historical trace，供 masking diagnostic 使用。"""
    payload = _build_indicator_payload(
        lambda: build_component_level_historical_trace(
            registry_path=registry_path,
            trace_path=trace_path,
            gate_audit_root=gate_audit_root,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="component_level_historical_trace",
    )
    _print_indicator_artifact("Component-level historical trace", payload, paths)


@indicators_app.command("backtest-trace-bridge")
def indicator_backtest_trace_bridge_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 historical/replay multi-stage trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 forward outcome。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="bridge outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """从 backtest/historical simulation/advisory outcome artifact 导出 trace bridge。"""
    payload = _build_indicator_payload(
        lambda: build_backtest_trace_bridge(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="backtest_trace_bridge",
    )
    _print_indicator_artifact("Backtest trace bridge", payload, paths)


@indicators_app.command("validation-pack")
def indicator_validation_pack_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option(
            "--prices-path",
            help="可选 prices_daily.csv，用于 casebook/ablation outcome。",
        ),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    coverage_extension_root: Annotated[
        Path | None,
        typer.Option(
            "--coverage-extension-root",
            help="可选 TRADING-699 coverage extension root，例如 outputs/research_campaigns。",
        ),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="casebook/ablation outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """写出 TRADING-665～685 indicator framework v1 validation pack。"""
    payload = _build_indicator_payload(
        lambda: write_indicator_framework_validation_pack(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
            output_root=output_root,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            coverage_extension_root=coverage_extension_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    _print_status("Indicator validation pack", payload["status"])
    console.print(
        f"artifacts={len(payload['artifacts'])}；production_effect={payload['production_effect']}"
    )


@indicators_app.command("validation-pack-stability")
def indicator_validation_pack_stability_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option(
            "--prices-path",
            help="可选 prices_daily.csv，用于 casebook/ablation outcome。",
        ),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    coverage_extension_root: Annotated[
        Path | None,
        typer.Option(
            "--coverage-extension-root",
            help="可选 TRADING-699 coverage extension root，例如 outputs/research_campaigns。",
        ),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="casebook/ablation outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """对 validation-pack 做两次 rerun 稳定性检查。"""
    payload = _build_indicator_payload(
        lambda: write_indicator_validation_pack_stability_report(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
            output_root=output_root,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            coverage_extension_root=coverage_extension_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    _print_status("Indicator validation pack stability", payload["status"])
    console.print(
        f"stable={payload['summary']['stable']}；"
        f"artifact_count={payload['summary']['artifact_count']}；"
        f"production_effect={payload['production_effect']}"
    )


@campaign_app.command("init")
def init_campaign_command(
    spec: Annotated[Path, typer.Option("--spec", help="Campaign YAML/JSON spec 路径。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    module_registry_path: Annotated[
        Path,
        typer.Option("--module-registry", help="Module capability registry 路径。"),
    ] = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Annotated[
        Path,
        typer.Option("--window-policy", help="Window/holdout policy 路径。"),
    ] = DEFAULT_WINDOW_POLICY_PATH,
    migration_path: Annotated[
        Path,
        typer.Option("--migration-config", help="历史 evidence 迁移配置路径。"),
    ] = DEFAULT_MIGRATION_PATH,
    force: Annotated[bool, typer.Option(help="覆盖已有 campaign 状态。")] = False,
) -> None:
    """创建 Campaign state、evidence store、transition audit 和 reproducibility manifest。"""
    try:
        payload = initialize_campaign(
            spec_path=spec,
            campaign_root=campaign_root,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
            migration_path=migration_path,
            force=force,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    style = "green" if payload["validation_status"] != "FAIL" else "red"
    console.print(f"[{style}]Campaign 初始化：{payload['validation_status']}[/{style}]")
    console.print(f"Campaign：{payload['campaign_id']}")
    console.print(f"Stage：{payload['current_stage']}；Outcome：{payload['current_outcome']}")
    console.print(f"Evidence records：{payload['evidence_record_count']}")
    console.print(f"目录：{payload['campaign_dir']}")


@campaign_app.command("validate")
def validate_campaign_command(
    spec: Annotated[
        Path | None,
        typer.Option("--spec", help="Campaign YAML/JSON spec 路径。"),
    ] = None,
    campaign_id: Annotated[
        str | None,
        typer.Option("--id", help="已初始化 campaign id。"),
    ] = None,
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    module_registry_path: Annotated[
        Path,
        typer.Option("--module-registry", help="Module capability registry 路径。"),
    ] = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Annotated[
        Path,
        typer.Option("--window-policy", help="Window/holdout policy 路径。"),
    ] = DEFAULT_WINDOW_POLICY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """验证 Campaign spec、module boundary、holdout policy、gate policy 和 safety metadata。"""
    if bool(spec) == bool(campaign_id):
        raise typer.BadParameter("--spec 和 --id 必须且只能指定一个")
    try:
        if spec is not None:
            campaign_spec = load_campaign_spec(spec)
        else:
            campaign_spec, _, _ = load_campaign_bundle(campaign_id or "", campaign_root)
        payload = build_campaign_validation_payload(
            spec=campaign_spec,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign validation", payload["validation_status"])
    console.print(
        f"issues={payload['summary']['issue_count']}；"
        f"errors={payload['summary']['error_count']}；"
        f"warnings={payload['summary']['warning_count']}；"
        f"production_effect={payload['safety_boundary']['production_effect']}"
    )
    for issue in payload["issues"][:10]:
        console.print(f"{issue['severity']}: {issue['issue_id']}: {issue['message']}")
    if payload["validation_status"] == "FAIL":
        raise typer.Exit(code=1)


@campaign_app.command("validate-adapters")
def validate_campaign_adapters_command(
    adapter_registry_path: Annotated[
        Path,
        typer.Option("--adapter-registry", help="Campaign stage adapter registry 路径。"),
    ] = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    module_registry_path: Annotated[
        Path,
        typer.Option("--module-registry", help="Module capability registry 路径。"),
    ] = DEFAULT_MODULE_REGISTRY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """验证 Campaign stage adapter contract、输入 artifact 和 safety metadata。"""
    try:
        payload = validate_stage_adapter_contracts(
            adapter_registry_path=adapter_registry_path,
            module_registry_path=module_registry_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign adapter contract validation", payload["validation_status"])
    console.print(
        f"adapters={payload['adapter_count']}；"
        f"issues={len(payload['issues'])}；"
        f"production_effect={payload['production_effect']}"
    )
    for issue in payload["issues"][:10]:
        console.print(f"{issue['severity']}: {issue['issue_id']}: {issue['message']}")
    if payload["validation_status"] == "FAIL":
        raise typer.Exit(code=1)


@campaign_app.command("validation-pack")
def campaign_validation_pack_command(
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Campaign 输出 artifact 根目录。"),
    ] = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    adapter_registry_path: Annotated[
        Path,
        typer.Option("--adapter-registry", help="Campaign stage adapter registry 路径。"),
    ] = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """写出 Control Plane v1 rc5 adapter/parity/budget/next-action validation pack。"""
    try:
        payload = write_campaign_control_plane_v1_validation_artifacts(
            campaign_root=campaign_root,
            output_root=output_root,
            adapter_registry_path=adapter_registry_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign validation pack", payload["status"])
    console.print(
        f"artifacts={len(payload['artifacts'])}；production_effect={payload['production_effect']}"
    )


@campaign_app.command("plan")
def plan_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出当前 stage/outcome、预算使用、允许动作、阻断动作和推荐下一阶段。"""
    try:
        payload = campaign_plan(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            gate_policy_path=gate_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_campaign_plan(payload)


@campaign_app.command("allowed-actions")
def allowed_actions_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出当前 Campaign 允许的下一步动作。"""
    try:
        payload = campaign_plan(campaign_id=campaign_id, campaign_root=campaign_root)
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    result = {
        "campaign_id": campaign_id,
        "allowed_next_actions": payload["allowed_next_actions"],
        "adapter_run_mode": payload["adapter_run_mode"],
        "production_effect": "none",
    }
    _write_json_if_requested(json_output_path, result)
    _print_action_list("Allowed actions", result["allowed_next_actions"])


@campaign_app.command("blocked-actions")
def blocked_actions_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出当前 Campaign 被阻断的动作。"""
    try:
        payload = campaign_plan(campaign_id=campaign_id, campaign_root=campaign_root)
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    result = {
        "campaign_id": campaign_id,
        "blocked_actions": payload["blocked_actions"],
        "required_owner_actions": payload["required_owner_actions"],
        "adapter_run_mode": payload["adapter_run_mode"],
        "production_effect": "none",
    }
    _write_json_if_requested(json_output_path, result)
    _print_action_list("Blocked actions", result["blocked_actions"])
    _print_action_list("Owner required", result["required_owner_actions"])


@campaign_app.command("budget")
def budget_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出 evidence budget used/remaining。"""
    try:
        payload = campaign_plan(campaign_id=campaign_id, campaign_root=campaign_root)
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    result = {
        "campaign_id": campaign_id,
        "budget_status": payload["budget_status"],
        "evidence_budget_used": payload["evidence_budget_used"],
        "evidence_budget_remaining": payload["evidence_budget_remaining"],
        "production_effect": "none",
    }
    _write_json_if_requested(json_output_path, result)
    _print_status("Campaign budget", result["budget_status"])
    console.print(f"used={result['evidence_budget_used']}")
    console.print(f"remaining={result['evidence_budget_remaining']}")


@campaign_app.command("source-artifacts")
def source_artifacts_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出 Campaign source artifact lineage。"""
    try:
        payload = build_status_payload(
            campaign_id=campaign_id,
            detailed=True,
            campaign_root=campaign_root,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    result = {
        "campaign_id": campaign_id,
        "source_artifacts": payload["source_artifacts"],
        "adapter_runtime": payload["adapter_runtime"],
        "production_effect": "none",
    }
    _write_json_if_requested(json_output_path, result)
    _print_action_list("Source artifacts", result["source_artifacts"])


@campaign_app.command("run")
def run_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    stage: Annotated[
        str,
        typer.Option("--stage", help="要运行的 stage，默认 next。"),
    ] = "next",
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    module_registry_path: Annotated[
        Path,
        typer.Option("--module-registry", help="Module capability registry 路径。"),
    ] = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Annotated[
        Path,
        typer.Option("--window-policy", help="Window/holdout policy 路径。"),
    ] = DEFAULT_WINDOW_POLICY_PATH,
    adapter_registry_path: Annotated[
        Path,
        typer.Option("--adapter-registry", help="Campaign stage adapter registry 路径。"),
    ] = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Campaign 输出 artifact 根目录。"),
    ] = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """按状态机运行允许的下一阶段；缺少计算 adapter 时 fail-closed。"""
    try:
        payload = run_campaign_stage(
            campaign_id=campaign_id,
            requested_stage=stage,
            campaign_root=campaign_root,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
            adapter_registry_path=adapter_registry_path,
            output_root=output_root,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign stage run", payload["outcome"])
    console.print(f"run_id={payload['run_id']}；stage={payload['stage']}")
    console.print(
        f"adapter={payload.get('adapter_id') or 'none'}；status={payload.get('adapter_status')}"
    )
    if payload["outcome"] == "BLOCKED":
        raise typer.Exit(code=1)


@campaign_app.command("diagnose")
def diagnose_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """聚合 campaign-level evidence matrix，并披露 best/worst/missing evidence。"""
    try:
        payload = diagnose_campaign(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            gate_policy_path=gate_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign diagnosis", payload["current_outcome"])
    console.print(
        f"positive={len(payload['positive_evidence'])}；"
        f"negative={len(payload['negative_evidence'])}；"
        f"missing={len(payload['missing_required_evidence_categories'])}"
    )


@campaign_app.command("gate")
def gate_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """用配置化 gate policy 计算当前 Campaign gate 结果。"""
    try:
        spec, state, evidence = load_campaign_bundle(campaign_id, campaign_root)
        payload = evaluate_gate(
            spec=spec,
            state=state,
            evidence=evidence,
            gate_policy_path=gate_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign gate", payload["decision_outcome"])
    console.print(
        f"scorecard={payload['scorecard_policy']}；"
        f"missing={len(payload['missing_required_evidence_categories'])}；"
        f"blockers={len(payload['blocking_evidence_ids'])}"
    )


@campaign_app.command("status")
def status_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    view: Annotated[
        str,
        typer.Option("--view", help="concise 或 detailed。"),
    ] = "concise",
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出 canonical Campaign status。"""
    if view not in {"concise", "detailed"}:
        raise typer.BadParameter("--view must be concise or detailed")
    try:
        payload = build_status_payload(
            campaign_id=campaign_id,
            detailed=view == "detailed",
            campaign_root=campaign_root,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign status", payload["current_outcome"])
    console.print(f"stage={payload['current_stage']}；evidence={payload['evidence_record_count']}")
    console.print(f"budget={payload['budget_status']}；run_mode={payload['adapter_run_mode']}")
    console.print(f"allowed={', '.join(payload['allowed_next_actions']) or 'none'}")
    console.print(f"blocked={', '.join(payload['blocked_actions']) or 'none'}")


@campaign_app.command("packet")
def packet_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Owner packet 输出根目录。"),
    ] = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
) -> None:
    """生成 Campaign Reader Brief / owner packet，不写 owner decision。"""
    try:
        payload = build_owner_packet(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            output_root=output_root,
            gate_policy_path=gate_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _print_status("Campaign owner packet", payload["decision"])
    console.print(f"JSON：{payload['json_path']}")
    console.print(f"Markdown：{payload['markdown_path']}")
    console.print("owner_decision_appended=false；production_effect=none")


@campaign_app.command("archive")
def archive_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    reason: Annotated[
        str,
        typer.Option("--reason", help="Archive reason。"),
    ] = "manual_archive",
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
) -> None:
    """归档 Campaign，不写 owner decision、不触发 production effect。"""
    try:
        payload = archive_campaign(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            reason=reason,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _print_status("Campaign archive", payload["current_outcome"])
    console.print(f"stage={payload['current_stage']}；reason={payload['archive_reason']}")


@campaign_app.command("deprecation-plan")
def deprecation_plan_campaign_command(
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出旧 B2/B3 task-specific runner 的 Campaign 替代边界。"""
    payload = build_case_specific_runner_deprecation_plan()
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign deprecation plan", payload["status"])
    for runner in payload["old_runners"]:
        console.print(
            f"{runner['old_command']} -> {runner['replacement_campaign_command']}；"
            f"parity={runner['parity_status']}；status={runner['deprecation_status']}"
        )


def _print_campaign_plan(payload: dict[str, object]) -> None:
    _print_status("Campaign plan", str(payload["current_outcome"]))
    console.print(f"stage={payload['current_stage']}；next={payload['next_recommended_stage']}")
    console.print(f"budget={payload['budget_status']}")
    console.print(
        f"adapter={payload.get('adapter_id') or 'none'}；"
        f"run_mode={payload.get('adapter_run_mode') or 'none'}"
    )
    console.print(f"allowed={', '.join(payload['allowed_next_actions']) or 'none'}")
    console.print(f"blocked={', '.join(payload['blocked_actions']) or 'none'}")
    console.print(f"owner_required={', '.join(payload['required_owner_actions']) or 'none'}")


def _print_status(label: str, status: str) -> None:
    style = "green" if status in {"PASS", "PROMISING"} else "yellow"
    if status in {"FAIL", "BLOCKED", "REJECTED"}:
        style = "red"
    console.print(f"[{style}]{label}：{status}[/{style}]")


def _write_json_if_requested(path: Path | None, payload: dict[str, object]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _build_indicator_payload(builder):  # type: ignore[no-untyped-def]
    try:
        return builder()
    except IndicatorResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _build_research_payload(builder):  # type: ignore[no-untyped-def]
    try:
        return builder()
    except (ResearchGovernanceError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc


def _print_indicator_artifact(
    label: str,
    payload: dict[str, object],
    paths: dict[str, str],
) -> None:
    _print_status(label, str(payload["status"]))
    summary = payload.get("summary")
    if isinstance(summary, dict):
        compact = "; ".join(f"{key}={value}" for key, value in list(summary.items())[:4])
        if compact:
            console.print(compact)
    console.print(f"JSON：{paths['json_path']}")
    console.print(f"Markdown：{paths['markdown_path']}")
    console.print("research_only=true；production_effect=none")
    if str(payload["status"]) == "FAIL":
        raise typer.Exit(code=1)


def _print_research_artifact(
    label: str,
    payload: dict[str, object],
    paths: dict[str, str],
) -> None:
    _print_status(label, str(payload["status"]))
    _print_summary(payload)
    console.print(f"JSON：{paths['json_path']}")
    console.print(f"Markdown：{paths['markdown_path']}")
    console.print("research_only=true；production_effect=none")
    if str(payload["status"]) == "FAIL":
        raise typer.Exit(code=1)


def _print_strategy_pilot_payload(label: str, payload: dict[str, object]) -> None:
    _print_status(label, str(payload["status"]))
    console.print(f"task_id={payload.get('task_id', 'N/A')}")
    console.print(f"status={payload.get('status')}")
    _print_summary(payload)
    paths = payload.get("artifact_paths")
    if isinstance(paths, dict):
        console.print(f"artifact_path={paths.get('json_path')}")
        console.print(f"JSON：{paths.get('json_path')}")
        console.print(f"Markdown：{paths.get('markdown_path')}")
    blockers = payload.get("blockers") or payload.get("remaining_blockers") or []
    warnings = payload.get("warnings") or []
    console.print(f"blockers={len(blockers) if isinstance(blockers, list) else 0}")
    console.print(f"warnings={len(warnings) if isinstance(warnings, list) else 0}")
    console.print(
        f"next_recommended_action={payload.get('next_recommended_action', 'manual_review')}"
    )
    for safety_field in (
        "controlled_only=true",
        "promotion_gate_allowed=false",
        "paper_shadow_change_allowed=false",
        "production_weight_change_allowed=false",
        "broker_action=none",
        "production_effect=none",
    ):
        console.print(safety_field)
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


def _print_action_list(label: str, values: object) -> None:
    console.print(f"{label}:")
    if not values:
        console.print("- none")
        return
    for value in values:
        console.print(f"- {value}")
