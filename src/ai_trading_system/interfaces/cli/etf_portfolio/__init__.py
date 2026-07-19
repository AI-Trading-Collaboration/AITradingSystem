from ai_trading_system.interfaces.cli.etf_portfolio import ai_attribution as ai_attribution_commands
from ai_trading_system.interfaces.cli.etf_portfolio import (
    ai_confirmation as ai_confirmation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import backtest as backtest_commands
from ai_trading_system.interfaces.cli.etf_portfolio import (
    baseline_review as baseline_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import data as data_commands
from ai_trading_system.interfaces.cli.etf_portfolio import data_quality as data_quality_commands
from ai_trading_system.interfaces.cli.etf_portfolio import (
    decision_journal as decision_journal_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_allocation as dynamic_allocation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_calibration as dynamic_calibration_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import dynamic_rescue as dynamic_rescue_commands
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_robustness as dynamic_robustness_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import dynamic_shadow as dynamic_shadow_commands
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v2_review as dynamic_v2_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_advisory_outcome as dynamic_v3_advisory_outcome_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_advisory_proposal_review as dynamic_v3_advisory_proposal_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_backfill_repair as dynamic_v3_backfill_repair_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_backfilled_outcome as dynamic_v3_backfilled_outcome_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_backtest_sim_calibration as dynamic_v3_backtest_sim_calibration_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_backtest_sim_events as dynamic_v3_backtest_sim_events_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_backtest_sim_forward_bridge as dynamic_v3_backtest_sim_forward_bridge_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_backtest_sim_outcome as dynamic_v3_backtest_sim_outcome_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_backtest_sim_paper as dynamic_v3_backtest_sim_paper_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_backtest_sim_regime as dynamic_v3_backtest_sim_regime_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_backtest_sim_sensitivity as dynamic_v3_backtest_sim_sensitivity_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_backtest_sim_variants as dynamic_v3_backtest_sim_variants_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_candidate_evidence as dynamic_v3_candidate_evidence_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_candidate_observation as dynamic_v3_candidate_observation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_confirmation_evaluation as dynamic_v3_confirmation_evaluation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_confirmation_operations as dynamic_v3_confirmation_operations_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_confirmation_progress as dynamic_v3_confirmation_progress_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_confirmation_targets as dynamic_v3_confirmation_targets_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_consensus_drift as dynamic_v3_consensus_drift_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_consensus_risk as dynamic_v3_consensus_risk_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_data_audit as dynamic_v3_data_audit_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_data_provenance as dynamic_v3_data_provenance_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_defensive_research as dynamic_v3_defensive_research_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_evidence_governance as dynamic_v3_evidence_governance_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_evidence_materialization as dynamic_v3_evidence_materialization_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_evidence_readiness as dynamic_v3_evidence_readiness_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_evidence_trend as dynamic_v3_evidence_trend_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_failure_attribution as dynamic_v3_failure_attribution_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_filtered_candidate_pipeline as dynamic_v3_filtered_candidate_pipeline_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_filtered_candidate_readiness as dynamic_v3_filtered_candidate_readiness_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_forward_confirmation_plan as dynamic_v3_forward_confirmation_plan_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_forward_outcome_decision as dynamic_v3_forward_outcome_decision_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_forward_pressure as dynamic_v3_forward_pressure_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_historical_paper_sim as dynamic_v3_historical_paper_sim_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_historical_replay as dynamic_v3_historical_replay_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_injection_audit as dynamic_v3_injection_audit_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_legacy_validation as dynamic_v3_legacy_validation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_limited_vs_notrade as dynamic_v3_limited_vs_notrade_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_manual_execution_review as dynamic_v3_manual_execution_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_micro_search_foundation as dynamic_v3_micro_search_foundation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_observation_lifecycle as dynamic_v3_observation_lifecycle_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_outcome_dashboard as dynamic_v3_outcome_dashboard_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_outcome_due as dynamic_v3_outcome_due_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_outcome_update as dynamic_v3_outcome_update_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_outcome_update_review as dynamic_v3_outcome_update_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_owner_attribution as dynamic_v3_owner_attribution_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_owner_review_journal as dynamic_v3_owner_review_journal_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_paper_portfolio as dynamic_v3_paper_portfolio_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_paper_shadow_operations as dynamic_v3_paper_shadow_operations_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_portfolio_intake as dynamic_v3_portfolio_intake_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_portfolio_risk_controls as dynamic_v3_portfolio_risk_controls_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_position_advisory as dynamic_v3_position_advisory_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_position_advisory_daily as dynamic_v3_position_advisory_daily_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_pressure_validation as dynamic_v3_pressure_validation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_real_evaluation as dynamic_v3_real_evaluation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_real_execution_owner_review as dynamic_v3_real_execution_owner_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_real_snapshot_dry_run as dynamic_v3_real_snapshot_dry_run_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_real_snapshot_intake as dynamic_v3_real_snapshot_intake_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_real_snapshot_paper_action as dynamic_v3_real_snapshot_paper_action_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_replay_diagnosis as dynamic_v3_replay_diagnosis_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_replay_forward_bridge as dynamic_v3_replay_forward_bridge_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_replay_inventory as dynamic_v3_replay_inventory_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_replay_performance_review as dynamic_v3_replay_performance_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_replay_sample_expansion as dynamic_v3_replay_sample_expansion_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_rescue as dynamic_v3_rescue_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_research_contract_ledger as dynamic_v3_research_contract_ledger_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_research_control as dynamic_v3_research_control_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_research_direction_foundation as dynamic_v3_research_direction_foundation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_rolling_evidence_refresh as dynamic_v3_rolling_evidence_refresh_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_rule_calibration as dynamic_v3_rule_calibration_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_rule_owner_decision as dynamic_v3_rule_owner_decision_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_rule_review_cycle as dynamic_v3_rule_review_cycle_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_shadow_aging as dynamic_v3_shadow_aging_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_shadow_decision_support as dynamic_v3_shadow_decision_support_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_shadow_health_control as dynamic_v3_shadow_health_control_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_shadow_registry as dynamic_v3_shadow_registry_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_signal_diagnosis_foundation as dynamic_v3_signal_diagnosis_foundation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_signal_filter_foundation as dynamic_v3_signal_filter_foundation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_signal_input_readiness as dynamic_v3_signal_input_readiness_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_sim_defensive_validation as dynamic_v3_sim_defensive_validation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_sim_interpretation as dynamic_v3_sim_interpretation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_sim_risk_return as dynamic_v3_sim_risk_return_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_sweep_config as dynamic_v3_sweep_config_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_sweep_runtime as dynamic_v3_sweep_runtime_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_system_target_experiment_factory as dynamic_v3_experiment_factory_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_system_target_hardening as dynamic_v3_system_target_hardening_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_system_target_history as dynamic_v3_system_target_history_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_system_target_portfolio as dynamic_v3_system_target_portfolio_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_system_target_refinement as dynamic_v3_system_target_refinement_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_system_target_risk_capped as dynamic_v3_system_target_risk_capped_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_system_target_smoothed_bootstrap as dynamic_v3_smoothed_bootstrap_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_system_target_smoothed_evidence as dynamic_v3_smoothed_evidence_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_system_target_smoothed_freshness as dynamic_v3_smoothed_freshness_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_system_target_smoothed_method as dynamic_v3_system_target_smoothed_method_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_system_target_smoothed_operations as dynamic_v3_smoothed_operations_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_system_target_smoothed_promotion as dynamic_v3_smoothed_promotion_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_system_target_smoothed_readiness as dynamic_v3_smoothed_readiness_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_system_target_smoothed_refresh as dynamic_v3_smoothed_refresh_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_validation_evidence as dynamic_v3_validation_evidence_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_variant_comparison as dynamic_v3_variant_comparison_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_weekly_advisory_review as dynamic_v3_weekly_advisory_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_weekly_real_snapshot_review as dynamic_v3_weekly_real_snapshot_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_weight_path as dynamic_v3_weight_path_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_weight_search_decision as dynamic_v3_weight_search_decision_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_weight_search_diagnostics as dynamic_v3_weight_search_diagnostics_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_weight_search_evaluation as dynamic_v3_weight_search_evaluation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_weight_search_followup as dynamic_v3_weight_search_followup_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_weight_search_foundation as dynamic_v3_weight_search_foundation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_weight_search_targeted as dynamic_v3_weight_search_targeted_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_window_audit as dynamic_v3_window_audit_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import experiments as experiments_commands
from ai_trading_system.interfaces.cli.etf_portfolio import forward as forward_commands
from ai_trading_system.interfaces.cli.etf_portfolio import operations as operations_commands
from ai_trading_system.interfaces.cli.etf_portfolio import p1 as p1_commands
from ai_trading_system.interfaces.cli.etf_portfolio import p2 as p2_commands
from ai_trading_system.interfaces.cli.etf_portfolio import (
    parameter_review as parameter_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import reporting as reporting_commands
from ai_trading_system.interfaces.cli.etf_portfolio import satellite as satellite_commands
from ai_trading_system.interfaces.cli.etf_portfolio import (
    satellite_attribution as satellite_attribution_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import shadow_review as shadow_review_commands
from ai_trading_system.interfaces.cli.etf_portfolio import simulation as simulation_commands
from ai_trading_system.interfaces.cli.etf_portfolio import (
    trend_calibration as trend_calibration_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import weekly_review as weekly_review_commands
from ai_trading_system.interfaces.cli.etf_portfolio import (
    weight_calibration as weight_calibration_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    weight_research as weight_research_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import workflow as workflow_commands
from ai_trading_system.interfaces.cli.etf_portfolio.registration import etf_app

__all__ = [
    "baseline_review_commands",
    "data_commands",
    "data_quality_commands",
    "dynamic_allocation_commands",
    "dynamic_calibration_commands",
    "dynamic_robustness_commands",
    "dynamic_rescue_commands",
    "dynamic_v2_review_commands",
    "dynamic_v3_rescue_commands",
    "dynamic_v3_advisory_outcome_commands",
    "dynamic_v3_advisory_proposal_review_commands",
    "dynamic_v3_confirmation_evaluation_commands",
    "dynamic_v3_confirmation_operations_commands",
    "dynamic_v3_confirmation_progress_commands",
    "dynamic_v3_confirmation_targets_commands",
    "dynamic_v3_rule_review_cycle_commands",
    "dynamic_v3_rule_owner_decision_commands",
    "dynamic_v3_forward_confirmation_plan_commands",
    "dynamic_v3_real_evaluation_commands",
    "dynamic_v3_real_execution_owner_review_commands",
    "dynamic_v3_real_snapshot_intake_commands",
    "dynamic_v3_real_snapshot_dry_run_commands",
    "dynamic_v3_research_control_commands",
    "dynamic_v3_real_snapshot_paper_action_commands",
    "dynamic_v3_failure_attribution_commands",
    "dynamic_v3_forward_outcome_decision_commands",
    "dynamic_v3_forward_pressure_commands",
    "dynamic_v3_evidence_readiness_commands",
    "dynamic_v3_evidence_governance_commands",
    "dynamic_v3_evidence_trend_commands",
    "dynamic_v3_injection_audit_commands",
    "dynamic_v3_legacy_validation_commands",
    "dynamic_v3_limited_vs_notrade_commands",
    "dynamic_v3_consensus_risk_commands",
    "dynamic_v3_manual_execution_review_commands",
    "dynamic_v3_observation_lifecycle_commands",
    "dynamic_v3_outcome_dashboard_commands",
    "dynamic_v3_outcome_due_commands",
    "dynamic_v3_outcome_update_commands",
    "dynamic_v3_outcome_update_review_commands",
    "dynamic_v3_owner_review_journal_commands",
    "dynamic_v3_owner_attribution_commands",
    "dynamic_v3_paper_portfolio_commands",
    "dynamic_v3_portfolio_intake_commands",
    "dynamic_v3_portfolio_risk_controls_commands",
    "dynamic_v3_position_advisory_commands",
    "dynamic_v3_position_advisory_daily_commands",
    "dynamic_v3_pressure_validation_commands",
    "dynamic_v3_replay_inventory_commands",
    "dynamic_v3_replay_sample_expansion_commands",
    "dynamic_v3_replay_diagnosis_commands",
    "dynamic_v3_replay_forward_bridge_commands",
    "dynamic_v3_replay_performance_review_commands",
    "dynamic_v3_historical_replay_commands",
    "dynamic_v3_backfilled_outcome_commands",
    "dynamic_v3_backfill_repair_commands",
    "dynamic_v3_backtest_sim_calibration_commands",
    "dynamic_v3_backtest_sim_events_commands",
    "dynamic_v3_backtest_sim_forward_bridge_commands",
    "dynamic_v3_backtest_sim_outcome_commands",
    "dynamic_v3_backtest_sim_paper_commands",
    "dynamic_v3_backtest_sim_regime_commands",
    "dynamic_v3_backtest_sim_sensitivity_commands",
    "dynamic_v3_backtest_sim_variants_commands",
    "dynamic_v3_historical_paper_sim_commands",
    "dynamic_v3_shadow_registry_commands",
    "dynamic_v3_micro_search_foundation_commands",
    "dynamic_v3_research_direction_foundation_commands",
    "dynamic_v3_research_contract_ledger_commands",
    "dynamic_v3_signal_diagnosis_foundation_commands",
    "dynamic_v3_signal_filter_foundation_commands",
    "dynamic_v3_signal_input_readiness_commands",
    "dynamic_v3_sim_interpretation_commands",
    "dynamic_v3_sim_defensive_validation_commands",
    "dynamic_v3_sim_risk_return_commands",
    "dynamic_v3_shadow_aging_commands",
    "dynamic_v3_shadow_decision_support_commands",
    "dynamic_v3_shadow_health_control_commands",
    "dynamic_v3_sweep_config_commands",
    "dynamic_v3_sweep_runtime_commands",
    "dynamic_v3_system_target_portfolio_commands",
    "dynamic_v3_system_target_history_commands",
    "dynamic_v3_system_target_hardening_commands",
    "dynamic_v3_system_target_refinement_commands",
    "dynamic_v3_system_target_risk_capped_commands",
    "dynamic_v3_system_target_smoothed_method_commands",
    "dynamic_v3_smoothed_bootstrap_commands",
    "dynamic_v3_smoothed_evidence_commands",
    "dynamic_v3_smoothed_freshness_commands",
    "dynamic_v3_smoothed_refresh_commands",
    "dynamic_v3_smoothed_operations_commands",
    "dynamic_v3_smoothed_promotion_commands",
    "dynamic_v3_smoothed_readiness_commands",
    "dynamic_v3_experiment_factory_commands",
    "dynamic_v3_evidence_materialization_commands",
    "dynamic_v3_paper_shadow_operations_commands",
    "dynamic_v3_filtered_candidate_pipeline_commands",
    "dynamic_v3_filtered_candidate_readiness_commands",
    "dynamic_v3_data_audit_commands",
    "dynamic_v3_data_provenance_commands",
    "dynamic_v3_defensive_research_commands",
    "dynamic_v3_candidate_evidence_commands",
    "dynamic_v3_candidate_observation_commands",
    "dynamic_v3_consensus_drift_commands",
    "dynamic_v3_window_audit_commands",
    "dynamic_v3_weekly_real_snapshot_review_commands",
    "dynamic_v3_weekly_advisory_review_commands",
    "dynamic_v3_weight_path_commands",
    "dynamic_v3_weight_search_foundation_commands",
    "dynamic_v3_weight_search_evaluation_commands",
    "dynamic_v3_weight_search_decision_commands",
    "dynamic_v3_weight_search_diagnostics_commands",
    "dynamic_v3_weight_search_followup_commands",
    "dynamic_v3_weight_search_targeted_commands",
    "dynamic_v3_validation_evidence_commands",
    "dynamic_v3_variant_comparison_commands",
    "dynamic_v3_rule_calibration_commands",
    "dynamic_v3_rolling_evidence_refresh_commands",
    "etf_app",
    "dynamic_shadow_commands",
    "experiments_commands",
    "p2_commands",
    "satellite_commands",
    "simulation_commands",
    "ai_attribution_commands",
    "ai_confirmation_commands",
    "backtest_commands",
    "decision_journal_commands",
    "forward_commands",
    "p1_commands",
    "workflow_commands",
    "operations_commands",
    "parameter_review_commands",
    "reporting_commands",
    "shadow_review_commands",
    "satellite_attribution_commands",
    "weekly_review_commands",
    "weight_calibration_commands",
    "weight_research_commands",
    "trend_calibration_commands",
]
