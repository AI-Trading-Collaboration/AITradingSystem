from __future__ import annotations

import typer

etf_app = typer.Typer(help="ETF 主仓组合配置、信号、回测和模拟舱。", no_args_is_help=True)
data_app = typer.Typer(help="ETF price ingest and validation。", no_args_is_help=True)
features_app = typer.Typer(help="ETF feature store。", no_args_is_help=True)
signals_app = typer.Typer(help="ETF signal engine。", no_args_is_help=True)
regime_app = typer.Typer(help="ETF market regime engine。", no_args_is_help=True)
portfolio_app = typer.Typer(help="ETF portfolio allocation。", no_args_is_help=True)
backtest_app = typer.Typer(help="ETF portfolio backtest。", no_args_is_help=True)
simulation_app = typer.Typer(help="ETF simulation ledger。", no_args_is_help=True)
report_app = typer.Typer(help="ETF portfolio reports。", no_args_is_help=True)
run_app = typer.Typer(help="ETF portfolio full workflow。", no_args_is_help=True)
relative_strength_app = typer.Typer(help="ETF P1 relative strength。", no_args_is_help=True)
confirmation_app = typer.Typer(help="ETF P1 confirmation scores。", no_args_is_help=True)
satellite_app = typer.Typer(help="ETF P1 satellite candidates。", no_args_is_help=True)
satellite_attribution_app = typer.Typer(
    help="ETF satellite replacement forward attribution review。",
    no_args_is_help=True,
)
attribution_app = typer.Typer(help="ETF P1 attribution。", no_args_is_help=True)
experiments_app = typer.Typer(help="ETF P1 experiment registry。", no_args_is_help=True)
forward_app = typer.Typer(help="ETF forward shadow simulation review。", no_args_is_help=True)
ai_confirmation_app = typer.Typer(
    help="ETF AI confirmation overlay calibration。",
    no_args_is_help=True,
)
ai_attribution_app = typer.Typer(
    help="ETF AI confirmation forward attribution review。",
    no_args_is_help=True,
)
weekly_review_app = typer.Typer(help="ETF weekly portfolio review package。", no_args_is_help=True)
decision_journal_app = typer.Typer(
    help="ETF portfolio decision journal and human review notes。",
    no_args_is_help=True,
)
parameter_review_app = typer.Typer(
    help="ETF allocation parameter review from forward evidence。",
    no_args_is_help=True,
)
weight_calibration_app = typer.Typer(
    help="ETF dual-track weight calibration。",
    no_args_is_help=True,
)
weight_research_app = typer.Typer(
    help="ETF weight research unblock and ablation runner workflow。",
    no_args_is_help=True,
)
ops_app = typer.Typer(help="ETF operations workflow planning。", no_args_is_help=True)
data_quality_app = typer.Typer(
    help="ETF data quality and staleness governance。",
    no_args_is_help=True,
)
evidence_dashboard_app = typer.Typer(
    help="ETF strategy evidence dashboard。",
    no_args_is_help=True,
)
baseline_review_app = typer.Typer(
    help="ETF baseline candidate review playbook。",
    no_args_is_help=True,
)
shadow_review_app = typer.Typer(
    help="ETF shadow-ready candidate review and enrollment playbook。",
    no_args_is_help=True,
)
trend_calibration_app = typer.Typer(
    help="ETF trend signal weight calibration workflow。",
    no_args_is_help=True,
)
dynamic_allocation_app = typer.Typer(
    help="ETF candidate-only dynamic allocation policy workflow。",
    no_args_is_help=True,
)
dynamic_calibration_app = typer.Typer(
    help="ETF two-layer dynamic candidate batch/cache workflow。",
    no_args_is_help=True,
)
dynamic_robustness_app = typer.Typer(
    help="ETF dynamic strategy robustness review workflow。",
    no_args_is_help=True,
)
dynamic_rescue_app = typer.Typer(
    help="ETF dynamic failure diagnostics and rescue candidate workflow。",
    no_args_is_help=True,
)
dynamic_v2_review_app = typer.Typer(
    help="ETF dynamic v0.2 review-only robustness and shadow review package。",
    no_args_is_help=True,
)
dynamic_v3_rescue_app = typer.Typer(
    help="ETF dynamic v0.3 constraint-aware rescue candidate workflow。",
    no_args_is_help=True,
)
dynamic_v3_sweep_config_app = typer.Typer(
    help="Dynamic v3 rescue parameter sweep config workflow。",
    no_args_is_help=True,
)
dynamic_v3_sweep_app = typer.Typer(
    help="Dynamic v3 rescue batch parameter sweep workflow。",
    no_args_is_help=True,
)
dynamic_v3_data_audit_app = typer.Typer(
    help="Dynamic v3 rescue research data audit workflow。",
    no_args_is_help=True,
)
dynamic_v3_data_provenance_app = typer.Typer(
    help="Dynamic v3 rescue price cache provenance workflow。",
    no_args_is_help=True,
)
dynamic_v3_window_audit_app = typer.Typer(
    help="Dynamic v3 rescue backtest window audit workflow。",
    no_args_is_help=True,
)
dynamic_v3_weight_path_app = typer.Typer(
    help="Dynamic v3 rescue real evaluator weight path workflow。",
    no_args_is_help=True,
)
dynamic_v3_injection_audit_app = typer.Typer(
    help="Dynamic v3 rescue parameter injection audit workflow。",
    no_args_is_help=True,
)
dynamic_v3_candidate_app = typer.Typer(
    help="Dynamic v3 rescue candidate report workflow。",
    no_args_is_help=True,
)
dynamic_v3_walk_forward_app = typer.Typer(
    help="Dynamic v3 rescue walk-forward validation workflow。",
    no_args_is_help=True,
)
dynamic_v3_robustness_app = typer.Typer(
    help="Dynamic v3 rescue robustness diagnostics workflow。",
    no_args_is_help=True,
)
dynamic_v3_overfit_app = typer.Typer(
    help="Dynamic v3 rescue overfit risk workflow。",
    no_args_is_help=True,
)
dynamic_v3_shadow_app = typer.Typer(
    help="Dynamic v3 rescue observe-only shadow registry workflow。",
    no_args_is_help=True,
)
dynamic_v3_artifacts_app = typer.Typer(
    help="Dynamic v3 rescue artifact latest/validation workflow。",
    no_args_is_help=True,
)
dynamic_v3_schedule_app = typer.Typer(
    help="Dynamic v3 rescue scheduled observation gate。",
    no_args_is_help=True,
)
dynamic_v3_evidence_summary_app = typer.Typer(
    help="Dynamic v3 rescue candidate evidence summary workflow。",
    no_args_is_help=True,
)
dynamic_v3_medium_real_app = typer.Typer(
    help="Dynamic v3 rescue medium real candidate discovery report。",
    no_args_is_help=True,
)
dynamic_v3_regime_coverage_app = typer.Typer(
    help="Dynamic v3 rescue tech/semiconductor regime coverage audit。",
    no_args_is_help=True,
)
dynamic_v3_promotion_app = typer.Typer(
    help="Dynamic v3 rescue promotion review pack workflow。",
    no_args_is_help=True,
)
dynamic_v3_observe_pool_app = typer.Typer(
    help="Dynamic v3 rescue observe-only candidate pool workflow。",
    no_args_is_help=True,
)
dynamic_v3_overnight_readiness_app = typer.Typer(
    help="Dynamic v3 rescue overnight real readiness workflow。",
    no_args_is_help=True,
)
dynamic_v3_governance_app = typer.Typer(
    help="Dynamic v3 rescue parameter governance workflow。",
    no_args_is_help=True,
)
dynamic_v3_research_app = typer.Typer(
    help="Dynamic v3 rescue research index/query workflow。",
    no_args_is_help=True,
)
dynamic_v3_research_decision_app = typer.Typer(
    help="Dynamic v3 rescue research decision pack workflow。",
    no_args_is_help=True,
)
dynamic_v3_evidence_diagnosis_app = typer.Typer(
    help="Dynamic v3 rescue evidence blocking diagnosis workflow。",
    no_args_is_help=True,
)
dynamic_v3_gate_impact_app = typer.Typer(
    help="Dynamic v3 rescue gate impact simulation workflow。",
    no_args_is_help=True,
)
dynamic_v3_gate_policy_app = typer.Typer(
    help="Dynamic v3 rescue evidence gate policy workflow。",
    no_args_is_help=True,
)
dynamic_v3_candidate_recovery_app = typer.Typer(
    help="Dynamic v3 rescue candidate recovery workflow。",
    no_args_is_help=True,
)
dynamic_v3_shortlist_app = typer.Typer(
    help="Dynamic v3 rescue shadow shortlist workflow。",
    no_args_is_help=True,
)
dynamic_v3_candidate_cluster_app = typer.Typer(
    help="Dynamic v3 rescue candidate clustering workflow。",
    no_args_is_help=True,
)
dynamic_v3_shadow_shortlist_app = typer.Typer(
    help="Dynamic v3 rescue shadow shortlist monitoring pack workflow。",
    no_args_is_help=True,
)
dynamic_v3_shadow_monitor_run_app = typer.Typer(
    help="Dynamic v3 rescue shadow shortlist daily/weekly monitor workflow。",
    no_args_is_help=True,
)
dynamic_v3_portfolio_snapshot_app = typer.Typer(
    help="Dynamic v3 rescue manual portfolio snapshot workflow。",
    no_args_is_help=True,
)
dynamic_v3_manual_portfolio_app = typer.Typer(
    help="Dynamic v3 rescue hardened manual portfolio snapshot workflow。",
    no_args_is_help=True,
)
dynamic_v3_portfolio_exposure_app = typer.Typer(
    help="Dynamic v3 rescue portfolio exposure validation workflow。",
    no_args_is_help=True,
)
dynamic_v3_position_drift_app = typer.Typer(
    help="Dynamic v3 rescue current-vs-target position drift workflow。",
    no_args_is_help=True,
)
dynamic_v3_execution_guardrails_app = typer.Typer(
    help="Dynamic v3 rescue execution guardrail review workflow。",
    no_args_is_help=True,
)
dynamic_v3_manual_execution_review_app = typer.Typer(
    help="Dynamic v3 rescue manual execution review pack workflow。",
    no_args_is_help=True,
)
dynamic_v3_real_snapshot_app = typer.Typer(
    help="Dynamic v3 rescue real manual snapshot intake workflow。",
    no_args_is_help=True,
)
dynamic_v3_real_snapshot_dry_run_app = typer.Typer(
    help="Dynamic v3 rescue real snapshot advisory dry-run workflow。",
    no_args_is_help=True,
)
dynamic_v3_real_execution_owner_review_app = typer.Typer(
    help="Dynamic v3 rescue real execution owner decision workflow。",
    no_args_is_help=True,
)
dynamic_v3_real_snapshot_paper_action_app = typer.Typer(
    help="Dynamic v3 rescue real snapshot paper action workflow。",
    no_args_is_help=True,
)
dynamic_v3_weekly_real_snapshot_review_app = typer.Typer(
    help="Dynamic v3 rescue weekly real snapshot advisory review workflow。",
    no_args_is_help=True,
)
dynamic_v3_position_advisory_app = typer.Typer(
    help="Dynamic v3 rescue position advisory workflow。",
    no_args_is_help=True,
)
dynamic_v3_consensus_drift_app = typer.Typer(
    help="Dynamic v3 rescue candidate consensus drift workflow。",
    no_args_is_help=True,
)
dynamic_v3_owner_review_app = typer.Typer(
    help="Dynamic v3 rescue owner review journal workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_portfolio_app = typer.Typer(
    help="Dynamic v3 rescue paper portfolio workflow。",
    no_args_is_help=True,
)
dynamic_v3_model_target_app = typer.Typer(
    help="Dynamic v3 rescue research model target workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_shadow_app = typer.Typer(
    help="Dynamic v3 rescue paper shadow account workflow。",
    no_args_is_help=True,
)
dynamic_v3_model_rebalance_app = typer.Typer(
    help="Dynamic v3 rescue model target paper rebalance workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_shadow_performance_app = typer.Typer(
    help="Dynamic v3 rescue paper shadow performance workflow。",
    no_args_is_help=True,
)
dynamic_v3_system_target_review_app = typer.Typer(
    help="Dynamic v3 rescue system target portfolio review workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_shadow_backfill_app = typer.Typer(
    help="Dynamic v3 rescue paper shadow historical backfill workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_shadow_rolling_eval_app = typer.Typer(
    help="Dynamic v3 rescue paper shadow rolling evaluation workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_shadow_regime_review_app = typer.Typer(
    help="Dynamic v3 rescue paper shadow regime review workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_shadow_stability_app = typer.Typer(
    help="Dynamic v3 rescue paper shadow stability diagnostics workflow。",
    no_args_is_help=True,
)
dynamic_v3_system_target_selection_review_app = typer.Typer(
    help="Dynamic v3 rescue system target method selection review workflow。",
    no_args_is_help=True,
)
dynamic_v3_selection_attribution_app = typer.Typer(
    help="Dynamic v3 rescue selection attribution workflow。",
    no_args_is_help=True,
)
dynamic_v3_limited_long_risk_app = typer.Typer(
    help="Dynamic v3 rescue limited adjustment long-window risk workflow。",
    no_args_is_help=True,
)
dynamic_v3_limited_consistency_app = typer.Typer(
    help="Dynamic v3 rescue limited adjustment consistency workflow。",
    no_args_is_help=True,
)
dynamic_v3_data_warning_impact_app = typer.Typer(
    help="Dynamic v3 rescue data warning impact workflow。",
    no_args_is_help=True,
)
dynamic_v3_research_method_hardening_app = typer.Typer(
    help="Dynamic v3 rescue research method hardening workflow。",
    no_args_is_help=True,
)
dynamic_v3_limited_instability_app = typer.Typer(
    help="Dynamic v3 rescue limited adjustment instability diagnosis workflow。",
    no_args_is_help=True,
)
dynamic_v3_limited_risk_attribution_app = typer.Typer(
    help="Dynamic v3 rescue limited adjustment risk attribution workflow。",
    no_args_is_help=True,
)
dynamic_v3_data_warning_repair_plan_app = typer.Typer(
    help="Dynamic v3 rescue data warning repair plan workflow。",
    no_args_is_help=True,
)
dynamic_v3_alternative_method_review_app = typer.Typer(
    help="Dynamic v3 rescue alternative method review workflow。",
    no_args_is_help=True,
)
dynamic_v3_refined_method_proposal_app = typer.Typer(
    help="Dynamic v3 rescue refined research method proposal workflow。",
    no_args_is_help=True,
)
dynamic_v3_risk_capped_limited_app = typer.Typer(
    help="Dynamic v3 rescue risk-capped limited adjustment target workflow。",
    no_args_is_help=True,
)
dynamic_v3_risk_capped_backfill_app = typer.Typer(
    help="Dynamic v3 rescue risk-capped paper shadow backfill workflow。",
    no_args_is_help=True,
)
dynamic_v3_risk_capped_comparison_app = typer.Typer(
    help="Dynamic v3 rescue risk-capped method comparison workflow。",
    no_args_is_help=True,
)
dynamic_v3_risk_capped_review_app = typer.Typer(
    help="Dynamic v3 rescue risk-capped research method review workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_limited_app = typer.Typer(
    help="Dynamic v3 rescue smoothed limited adjustment target workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_backfill_app = typer.Typer(
    help="Dynamic v3 rescue smoothed paper shadow backfill workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_comparison_app = typer.Typer(
    help="Dynamic v3 rescue smoothed method comparison workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_review_app = typer.Typer(
    help="Dynamic v3 rescue smoothed research method review workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_review_attribution_app = typer.Typer(
    help="Dynamic v3 rescue smoothed review attribution workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothing_benefit_lag_app = typer.Typer(
    help="Dynamic v3 rescue smoothing benefit vs lag drilldown workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_regime_validation_app = typer.Typer(
    help="Dynamic v3 rescue smoothed regime validation workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_confirmation_app = typer.Typer(
    help="Dynamic v3 rescue smoothed forward confirmation workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_watch_pack_app = typer.Typer(
    help="Dynamic v3 rescue smoothed operational watch pack workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_evidence_gap_app = typer.Typer(
    help="Dynamic v3 rescue smoothed evidence gap workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_churn_backfill_app = typer.Typer(
    help="Dynamic v3 rescue smoothed churn metric backfill workflow。",
    no_args_is_help=True,
)
dynamic_v3_sideways_mixed_attribution_app = typer.Typer(
    help="Dynamic v3 rescue sideways mixed attribution workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_readiness_scorecard_app = typer.Typer(
    help="Dynamic v3 rescue smoothed readiness scorecard workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_owner_review_update_app = typer.Typer(
    help="Dynamic v3 rescue smoothed owner review update workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_promotion_review_app = typer.Typer(
    help="Dynamic v3 rescue smoothed promotion review workflow。",
    no_args_is_help=True,
)
dynamic_v3_primary_research_candidate_gate_app = typer.Typer(
    help="Dynamic v3 rescue paper shadow primary research candidate gate。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_forward_binding_app = typer.Typer(
    help="Dynamic v3 rescue smoothed forward confirmation binding workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_shadow_primary_switch_app = typer.Typer(
    help="Dynamic v3 rescue paper shadow primary candidate switch plan workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_owner_promotion_app = typer.Typer(
    help="Dynamic v3 rescue smoothed owner promotion decision journal。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_forward_progress_app = typer.Typer(
    help="Dynamic v3 rescue smoothed forward progress tracker。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_weekly_dashboard_app = typer.Typer(
    help="Dynamic v3 rescue smoothed weekly evidence dashboard。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_event_monitor_app = typer.Typer(
    help="Dynamic v3 rescue smoothed sideways/recovery event monitor。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_switch_readiness_app = typer.Typer(
    help="Dynamic v3 rescue smoothed primary switch readiness recheck。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_owner_renewal_app = typer.Typer(
    help="Dynamic v3 rescue smoothed owner decision renewal pack。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_daily_emission_app = typer.Typer(
    help="Dynamic v3 rescue smoothed daily target emission workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_outcome_due_app = typer.Typer(
    help="Dynamic v3 rescue smoothed forward outcome due scanner。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_outcome_update_app = typer.Typer(
    help="Dynamic v3 rescue smoothed forward outcome updater。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_forward_classify_app = typer.Typer(
    help="Dynamic v3 rescue smoothed sideways/recovery classifier。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_forward_weekly_run_app = typer.Typer(
    help="Dynamic v3 rescue smoothed forward evidence weekly runner。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_data_preflight_app = typer.Typer(
    help="Dynamic v3 rescue smoothed data freshness preflight。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_latest_emission_app = typer.Typer(
    help="Dynamic v3 rescue smoothed latest-available daily emission。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_blocked_explain_app = typer.Typer(
    help="Dynamic v3 rescue smoothed blocked run explanation pack。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_refresh_plan_app = typer.Typer(
    help="Dynamic v3 rescue smoothed source refresh and rerun plan。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_bootstrap_retry_app = typer.Typer(
    help="Dynamic v3 rescue smoothed bootstrap retry runner。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_source_refresh_app = typer.Typer(
    help="Dynamic v3 rescue smoothed source refresh execution。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_post_refresh_validate_app = typer.Typer(
    help="Dynamic v3 rescue smoothed post-refresh validation gate。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_retry_resume_app = typer.Typer(
    help="Dynamic v3 rescue smoothed retry resume workflow。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_sample_growth_app = typer.Typer(
    help="Dynamic v3 rescue smoothed sample growth dashboard。",
    no_args_is_help=True,
)
dynamic_v3_smoothed_data_readiness_app = typer.Typer(
    help="Dynamic v3 rescue smoothed owner data readiness pack。",
    no_args_is_help=True,
)
dynamic_v3_weight_search_space_app = typer.Typer(
    help="Dynamic v3 rescue weight search-space workflow。",
    no_args_is_help=True,
)
dynamic_v3_weight_experiment_batch2_app = typer.Typer(
    help="Dynamic v3 rescue weight optimization Batch-2 matrix workflow。",
    no_args_is_help=True,
)
dynamic_v3_weight_batch_backfill_app = typer.Typer(
    help="Dynamic v3 rescue weight Batch-2 historical backfill workflow。",
    no_args_is_help=True,
)
dynamic_v3_weight_scorecard_app = typer.Typer(
    help="Dynamic v3 rescue weight Batch-2 multi-objective scorecard。",
    no_args_is_help=True,
)
dynamic_v3_weight_robustness_review_app = typer.Typer(
    help="Dynamic v3 rescue weight Batch-2 robustness review。",
    no_args_is_help=True,
)
dynamic_v3_weight_adaptive_branch_app = typer.Typer(
    help="Dynamic v3 rescue weight Batch-2 adaptive branch controller。",
    no_args_is_help=True,
)
dynamic_v3_weight_expanded_search_app = typer.Typer(
    help="Dynamic v3 rescue weight expanded search workflow。",
    no_args_is_help=True,
)
dynamic_v3_weight_candidate_cluster_app = typer.Typer(
    help="Dynamic v3 rescue weight candidate clustering workflow。",
    no_args_is_help=True,
)
dynamic_v3_weight_top_candidate_interpretation_app = typer.Typer(
    help="Dynamic v3 rescue weight top candidate interpretation workflow。",
    no_args_is_help=True,
)
dynamic_v3_weight_method_promotion_gate_app = typer.Typer(
    help="Dynamic v3 rescue weight method promotion gate workflow。",
    no_args_is_help=True,
)
dynamic_v3_formal_method_auto_plan_app = typer.Typer(
    help="Dynamic v3 rescue formal method auto-plan workflow。",
    no_args_is_help=True,
)
dynamic_v3_weight_search_dashboard_app = typer.Typer(
    help="Dynamic v3 rescue weight search dashboard workflow。",
    no_args_is_help=True,
)
dynamic_v3_owner_research_decision_pack_app = typer.Typer(
    help="Dynamic v3 rescue owner research decision pack workflow。",
    no_args_is_help=True,
)
dynamic_v3_no_promotion_review_app = typer.Typer(
    help="Dynamic v3 rescue no-promotion diagnostics workflow。",
    no_args_is_help=True,
)
dynamic_v3_near_miss_candidates_app = typer.Typer(
    help="Dynamic v3 rescue near-miss candidate extraction workflow。",
    no_args_is_help=True,
)
dynamic_v3_cash_buffer_attribution_app = typer.Typer(
    help="Dynamic v3 rescue cash buffer attribution workflow。",
    no_args_is_help=True,
)
dynamic_v3_search_coverage_gap_app = typer.Typer(
    help="Dynamic v3 rescue search coverage gap analysis workflow。",
    no_args_is_help=True,
)
dynamic_v3_targeted_search_v3_app = typer.Typer(
    help="Dynamic v3 rescue targeted search v3 matrix workflow。",
    no_args_is_help=True,
)
dynamic_v3_targeted_v3_backfill_app = typer.Typer(
    help="Dynamic v3 rescue targeted v3 historical backfill workflow。",
    no_args_is_help=True,
)
dynamic_v3_near_miss_ab_comparison_app = typer.Typer(
    help="Dynamic v3 rescue near-miss A/B comparison workflow。",
    no_args_is_help=True,
)
dynamic_v3_promotion_threshold_sensitivity_app = typer.Typer(
    help="Dynamic v3 rescue promotion threshold sensitivity workflow。",
    no_args_is_help=True,
)
dynamic_v3_candidate_promotion_v2_app = typer.Typer(
    help="Dynamic v3 rescue candidate promotion decision v2 workflow。",
    no_args_is_help=True,
)
dynamic_v3_next_formal_or_search_plan_app = typer.Typer(
    help="Dynamic v3 rescue next formal method or search plan workflow。",
    no_args_is_help=True,
)
dynamic_v3_gate_calibration_review_app = typer.Typer(
    help="Dynamic v3 rescue promotion gate calibration review workflow。",
    no_args_is_help=True,
)
dynamic_v3_scorecard_attribution_app = typer.Typer(
    help="Dynamic v3 rescue scorecard component attribution workflow。",
    no_args_is_help=True,
)
dynamic_v3_signal_instability_diagnosis_app = typer.Typer(
    help="Dynamic v3 rescue signal-level instability diagnosis workflow。",
    no_args_is_help=True,
)
dynamic_v3_consensus_quality_review_app = typer.Typer(
    help="Dynamic v3 rescue candidate consensus quality review workflow。",
    no_args_is_help=True,
)
dynamic_v3_micro_search_v4_design_app = typer.Typer(
    help="Dynamic v3 rescue micro search v4 design workflow。",
    no_args_is_help=True,
)
dynamic_v3_micro_search_v4_backfill_app = typer.Typer(
    help="Dynamic v3 rescue micro search v4 backfill workflow。",
    no_args_is_help=True,
)
dynamic_v3_gate_calibrated_review_app = typer.Typer(
    help="Dynamic v3 rescue gate-calibrated candidate review workflow。",
    no_args_is_help=True,
)
dynamic_v3_signal_vs_parameter_attribution_app = typer.Typer(
    help="Dynamic v3 rescue signal-vs-parameter attribution workflow。",
    no_args_is_help=True,
)
dynamic_v3_next_research_direction_app = typer.Typer(
    help="Dynamic v3 rescue next research direction workflow。",
    no_args_is_help=True,
)
dynamic_v3_owner_research_roadmap_app = typer.Typer(
    help="Dynamic v3 rescue owner research roadmap workflow。",
    no_args_is_help=True,
)
dynamic_v3_signal_failure_taxonomy_app = typer.Typer(
    help="Dynamic v3 rescue signal feature failure taxonomy workflow。",
    no_args_is_help=True,
)
dynamic_v3_candidate_signal_ledger_app = typer.Typer(
    help="Dynamic v3 rescue candidate signal ledger workflow。",
    no_args_is_help=True,
)
dynamic_v3_signal_churn_root_cause_app = typer.Typer(
    help="Dynamic v3 rescue signal churn root-cause workflow。",
    no_args_is_help=True,
)
dynamic_v3_regime_mismatch_attribution_app = typer.Typer(
    help="Dynamic v3 rescue regime mismatch attribution workflow。",
    no_args_is_help=True,
)
dynamic_v3_candidate_quality_filter_design_app = typer.Typer(
    help="Dynamic v3 rescue candidate quality filter design workflow。",
    no_args_is_help=True,
)
dynamic_v3_filtered_candidate_backfill_app = typer.Typer(
    help="Dynamic v3 rescue filtered candidate backfill workflow。",
    no_args_is_help=True,
)
dynamic_v3_filtered_vs_original_comparison_app = typer.Typer(
    help="Dynamic v3 rescue filtered versus original comparison workflow。",
    no_args_is_help=True,
)
dynamic_v3_signal_gate_experiment_app = typer.Typer(
    help="Dynamic v3 rescue signal gate experiment workflow。",
    no_args_is_help=True,
)
dynamic_v3_filtered_candidate_promotion_review_app = typer.Typer(
    help="Dynamic v3 rescue filtered candidate promotion review workflow。",
    no_args_is_help=True,
)
dynamic_v3_owner_signal_roadmap_app = typer.Typer(
    help="Dynamic v3 rescue owner signal roadmap workflow。",
    no_args_is_help=True,
)
dynamic_v3_filtered_candidate_evidence_app = typer.Typer(
    help="Dynamic v3 rescue filtered candidate evidence drilldown workflow。",
    no_args_is_help=True,
)
dynamic_v3_median_regime_filter_spec_app = typer.Typer(
    help="Dynamic v3 rescue median regime filter specification workflow。",
    no_args_is_help=True,
)
dynamic_v3_filtered_candidate_stress_backfill_app = typer.Typer(
    help="Dynamic v3 rescue filtered candidate stress backfill workflow。",
    no_args_is_help=True,
)
dynamic_v3_drawdown_mismatch_reduction_app = typer.Typer(
    help="Dynamic v3 rescue drawdown mismatch reduction workflow。",
    no_args_is_help=True,
)
dynamic_v3_flip_rotation_reduction_app = typer.Typer(
    help="Dynamic v3 rescue direction flip and rotation reduction workflow。",
    no_args_is_help=True,
)
dynamic_v3_filtered_candidate_ab_review_app = typer.Typer(
    help="Dynamic v3 rescue filtered candidate A/B review workflow。",
    no_args_is_help=True,
)
dynamic_v3_signal_gate_confirmation_app = typer.Typer(
    help="Dynamic v3 rescue signal gate forward confirmation workflow。",
    no_args_is_help=True,
)
dynamic_v3_filtered_formalization_readiness_app = typer.Typer(
    help="Dynamic v3 rescue filtered formalization readiness workflow。",
    no_args_is_help=True,
)
dynamic_v3_owner_filtered_candidate_review_app = typer.Typer(
    help="Dynamic v3 rescue owner filtered candidate review workflow。",
    no_args_is_help=True,
)
dynamic_v3_filtered_next_decision_app = typer.Typer(
    help="Dynamic v3 rescue filtered next decision workflow。",
    no_args_is_help=True,
)
dynamic_v3_formal_research_method_contract_app = typer.Typer(
    help="Dynamic v3 rescue formal research method contract workflow。",
    no_args_is_help=True,
)
dynamic_v3_promotion_gate_threshold_calibration_app = typer.Typer(
    help="Dynamic v3 rescue promotion gate threshold calibration workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_shadow_protocol_app = typer.Typer(
    help="Dynamic v3 rescue paper-shadow protocol workflow。",
    no_args_is_help=True,
)
dynamic_v3_signal_input_completeness_app = typer.Typer(
    help="Dynamic v3 rescue signal input completeness workflow。",
    no_args_is_help=True,
)
dynamic_v3_signal_input_completeness_recovery_app = typer.Typer(
    help="Dynamic v3 rescue signal input completeness recovery workflow。",
    no_args_is_help=True,
)
dynamic_v3_signal_input_recovery_app = typer.Typer(
    help="Dynamic v3 rescue signal input recovery root-cause workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_shadow_daily_app = typer.Typer(
    help="Dynamic v3 rescue paper-shadow daily observation workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_shadow_drift_monitor_app = typer.Typer(
    help="Dynamic v3 rescue paper-shadow drift monitor workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_shadow_weekly_review_app = typer.Typer(
    help="Dynamic v3 rescue paper-shadow weekly review workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_shadow_health_app = typer.Typer(
    help="Dynamic v3 rescue canonical paper-shadow health workflow。",
    no_args_is_help=True,
)
dynamic_v3_readiness_health_recovery_app = typer.Typer(
    help="Dynamic v3 rescue readiness/health recovery chain workflow。",
    no_args_is_help=True,
)
dynamic_v3_normal_paper_shadow_resumption_gate_app = typer.Typer(
    help="Dynamic v3 rescue normal paper-shadow resumption gate workflow。",
    no_args_is_help=True,
)
dynamic_v3_paper_shadow_outcome_attribution_app = typer.Typer(
    help="Dynamic v3 rescue paper-shadow outcome attribution workflow。",
    no_args_is_help=True,
)
dynamic_v3_shadow_decision_comparison_app = typer.Typer(
    help="Dynamic v3 rescue shadow decision comparison workflow。",
    no_args_is_help=True,
)
dynamic_v3_cost_sensitivity_review_app = typer.Typer(
    help="Dynamic v3 rescue cost-sensitivity review workflow。",
    no_args_is_help=True,
)
dynamic_v3_cost_sensitivity_metrics_materialization_app = typer.Typer(
    help="Dynamic v3 rescue cost-sensitivity metrics materialization workflow。",
    no_args_is_help=True,
)
dynamic_v3_metric_source_map_app = typer.Typer(
    help="Dynamic v3 rescue cost/benchmark metric source map workflow。",
    no_args_is_help=True,
)
dynamic_v3_benchmark_baseline_control_app = typer.Typer(
    help="Dynamic v3 rescue benchmark baseline control workflow。",
    no_args_is_help=True,
)
dynamic_v3_benchmark_baseline_metrics_materialization_app = typer.Typer(
    help="Dynamic v3 rescue benchmark baseline metrics materialization workflow。",
    no_args_is_help=True,
)
dynamic_v3_candidate_regression_replay_app = typer.Typer(
    help="Dynamic v3 rescue candidate regression replay workflow。",
    no_args_is_help=True,
)
dynamic_v3_candidate_decision_ledger_app = typer.Typer(
    help="Dynamic v3 rescue candidate decision ledger workflow。",
    no_args_is_help=True,
)
dynamic_v3_evidence_staleness_monitor_app = typer.Typer(
    help="Dynamic v3 rescue evidence staleness monitor workflow。",
    no_args_is_help=True,
)
dynamic_v3_shadow_continuation_readiness_app = typer.Typer(
    help="Dynamic v3 rescue shadow continuation readiness workflow。",
    no_args_is_help=True,
)
dynamic_v3_stress_scenario_library_app = typer.Typer(
    help="Dynamic v3 rescue stress scenario library workflow。",
    no_args_is_help=True,
)
dynamic_v3_drawdown_event_casebook_app = typer.Typer(
    help="Dynamic v3 rescue drawdown event casebook workflow。",
    no_args_is_help=True,
)
dynamic_v3_flip_rotation_event_casebook_app = typer.Typer(
    help="Dynamic v3 rescue flip/rotation event casebook workflow。",
    no_args_is_help=True,
)
dynamic_v3_hypothesis_backlog_app = typer.Typer(
    help="Dynamic v3 rescue weight optimization hypothesis backlog workflow。",
    no_args_is_help=True,
)
dynamic_v3_variant_transform_app = typer.Typer(
    help="Dynamic v3 rescue lightweight weight transform spec workflow。",
    no_args_is_help=True,
)
dynamic_v3_experiment_matrix_app = typer.Typer(
    help="Dynamic v3 rescue lightweight experiment matrix workflow。",
    no_args_is_help=True,
)
dynamic_v3_batch_experiment_app = typer.Typer(
    help="Dynamic v3 rescue batch lightweight backfill experiment workflow。",
    no_args_is_help=True,
)
dynamic_v3_experiment_triage_app = typer.Typer(
    help="Dynamic v3 rescue experiment triage and promotion screening workflow。",
    no_args_is_help=True,
)
dynamic_v3_top_variant_interpretation_app = typer.Typer(
    help="Dynamic v3 rescue top variant interpretation workflow。",
    no_args_is_help=True,
)
dynamic_v3_method_promotion_plan_app = typer.Typer(
    help="Dynamic v3 rescue formal research method promotion plan workflow。",
    no_args_is_help=True,
)
dynamic_v3_advisory_outcome_app = typer.Typer(
    help="Dynamic v3 rescue advisory outcome workflow。",
    no_args_is_help=True,
)
dynamic_v3_owner_attribution_app = typer.Typer(
    help="Dynamic v3 rescue owner attribution workflow。",
    no_args_is_help=True,
)
dynamic_v3_shadow_aging_app = typer.Typer(
    help="Dynamic v3 rescue shadow aging workflow。",
    no_args_is_help=True,
)
dynamic_v3_weekly_advisory_review_app = typer.Typer(
    help="Dynamic v3 rescue weekly advisory review workflow。",
    no_args_is_help=True,
)
dynamic_v3_replay_inventory_app = typer.Typer(
    help="Dynamic v3 rescue historical replay inventory workflow。",
    no_args_is_help=True,
)
dynamic_v3_historical_replay_app = typer.Typer(
    help="Dynamic v3 rescue historical advisory replay workflow。",
    no_args_is_help=True,
)
dynamic_v3_backfill_outcome_app = typer.Typer(
    help="Dynamic v3 rescue backfilled outcome workflow。",
    no_args_is_help=True,
)
dynamic_v3_historical_paper_sim_app = typer.Typer(
    help="Dynamic v3 rescue historical paper portfolio simulation workflow。",
    no_args_is_help=True,
)
dynamic_v3_replay_performance_review_app = typer.Typer(
    help="Dynamic v3 rescue replay performance review workflow。",
    no_args_is_help=True,
)
dynamic_v3_replay_diagnosis_app = typer.Typer(
    help="Dynamic v3 rescue replay result diagnosis workflow。",
    no_args_is_help=True,
)
dynamic_v3_backfill_repair_app = typer.Typer(
    help="Dynamic v3 rescue backfilled outcome repair workflow。",
    no_args_is_help=True,
)
dynamic_v3_variant_comparison_app = typer.Typer(
    help="Dynamic v3 rescue replay variant comparison workflow。",
    no_args_is_help=True,
)
dynamic_v3_rule_calibration_app = typer.Typer(
    help="Dynamic v3 rescue advisory rule calibration workflow。",
    no_args_is_help=True,
)
dynamic_v3_replay_forward_bridge_app = typer.Typer(
    help="Dynamic v3 rescue replay-to-forward bridge workflow。",
    no_args_is_help=True,
)
dynamic_v3_outcome_due_app = typer.Typer(
    help="Dynamic v3 rescue forward outcome due-window workflow。",
    no_args_is_help=True,
)
dynamic_v3_replay_sample_expansion_app = typer.Typer(
    help="Dynamic v3 rescue replay sample expansion workflow。",
    no_args_is_help=True,
)
dynamic_v3_outcome_dashboard_app = typer.Typer(
    help="Dynamic v3 rescue outcome availability dashboard workflow。",
    no_args_is_help=True,
)
dynamic_v3_limited_vs_notrade_app = typer.Typer(
    help="Dynamic v3 rescue limited adjustment versus no-trade workflow。",
    no_args_is_help=True,
)
dynamic_v3_consensus_risk_app = typer.Typer(
    help="Dynamic v3 rescue consensus target risk review workflow。",
    no_args_is_help=True,
)
dynamic_v3_outcome_update_review_app = typer.Typer(
    help="Dynamic v3 rescue outcome update-ready review workflow。",
    no_args_is_help=True,
)
dynamic_v3_outcome_update_app = typer.Typer(
    help="Dynamic v3 rescue safe outcome update workflow。",
    no_args_is_help=True,
)
dynamic_v3_rolling_evidence_refresh_app = typer.Typer(
    help="Dynamic v3 rescue rolling evidence refresh workflow。",
    no_args_is_help=True,
)
dynamic_v3_evidence_trend_app = typer.Typer(
    help="Dynamic v3 rescue advisory evidence trend workflow。",
    no_args_is_help=True,
)
dynamic_v3_forward_outcome_decision_app = typer.Typer(
    help="Dynamic v3 rescue weekly forward outcome decision workflow。",
    no_args_is_help=True,
)
dynamic_v3_backtest_sim_app = typer.Typer(
    help="Dynamic v3 rescue backtest simulation advisory workflow。",
    no_args_is_help=True,
)
dynamic_v3_sim_interpretation_app = typer.Typer(
    help="Dynamic v3 simulation result interpretation pack。",
    no_args_is_help=True,
)
dynamic_v3_sim_risk_return_app = typer.Typer(
    help="Dynamic v3 simulation risk-return tradeoff review。",
    no_args_is_help=True,
)
dynamic_v3_sim_defensive_validation_app = typer.Typer(
    help="Dynamic v3 simulation defensive validation review。",
    no_args_is_help=True,
)
dynamic_v3_advisory_proposal_review_app = typer.Typer(
    help="Dynamic v3 advisory proposal review pack。",
    no_args_is_help=True,
)
dynamic_v3_forward_confirmation_plan_app = typer.Typer(
    help="Dynamic v3 forward confirmation plan update。",
    no_args_is_help=True,
)
dynamic_v3_confirmation_targets_app = typer.Typer(
    help="Dynamic v3 forward confirmation target registry。",
    no_args_is_help=True,
)
dynamic_v3_confirmation_progress_app = typer.Typer(
    help="Dynamic v3 confirmation target progress tracking。",
    no_args_is_help=True,
)
dynamic_v3_confirmation_evaluate_app = typer.Typer(
    help="Dynamic v3 confirmation success/failure evaluation。",
    no_args_is_help=True,
)
dynamic_v3_rule_review_cycle_app = typer.Typer(
    help="Dynamic v3 rule review cycle report。",
    no_args_is_help=True,
)
dynamic_v3_rule_owner_decision_app = typer.Typer(
    help="Dynamic v3 rule owner decision journal。",
    no_args_is_help=True,
)
dynamic_v3_confirmation_cycle_app = typer.Typer(
    help="Dynamic v3 weekly confirmation cycle operations。",
    no_args_is_help=True,
)
dynamic_v3_pressure_regime_tag_app = typer.Typer(
    help="Dynamic v3 pressure regime outcome tagging workflow。",
    no_args_is_help=True,
)
dynamic_v3_pressure_tag_diagnosis_app = typer.Typer(
    help="Dynamic v3 pressure tag threshold and mapping diagnosis。",
    no_args_is_help=True,
)
dynamic_v3_pressure_outcome_backfill_app = typer.Typer(
    help="Dynamic v3 pressure outcome backfill workflow。",
    no_args_is_help=True,
)
dynamic_v3_defensive_pressure_compare_app = typer.Typer(
    help="Dynamic v3 defensive pressure-window comparison。",
    no_args_is_help=True,
)
dynamic_v3_defensive_rule_review_app = typer.Typer(
    help="Dynamic v3 defensive rule status review。",
    no_args_is_help=True,
)
dynamic_v3_weekly_ops_decision_update_app = typer.Typer(
    help="Dynamic v3 weekly operations decision update。",
    no_args_is_help=True,
)
dynamic_v3_defensive_hypothesis_deep_dive_app = typer.Typer(
    help="Dynamic v3 defensive hypothesis deep-dive workflow。",
    no_args_is_help=True,
)
dynamic_v3_defensive_label_review_app = typer.Typer(
    help="Dynamic v3 defensive label review workflow。",
    no_args_is_help=True,
)
dynamic_v3_defensive_failure_study_app = typer.Typer(
    help="Dynamic v3 defensive failure study workflow。",
    no_args_is_help=True,
)
dynamic_v3_defensive_research_note_app = typer.Typer(
    help="Dynamic v3 defensive research note workflow。",
    no_args_is_help=True,
)
dynamic_v3_defensive_owner_pack_app = typer.Typer(
    help="Dynamic v3 defensive owner decision pack workflow。",
    no_args_is_help=True,
)
dynamic_v3_forward_pressure_capture_app = typer.Typer(
    help="Dynamic v3 forward pressure evidence capture planning。",
    no_args_is_help=True,
)
dynamic_v3_pressure_trigger_app = typer.Typer(
    help="Dynamic v3 daily pressure trigger scanner。",
    no_args_is_help=True,
)
dynamic_v3_pressure_capture_app = typer.Typer(
    help="Dynamic v3 event-driven pressure capture workflow。",
    no_args_is_help=True,
)
dynamic_v3_pressure_sample_ledger_app = typer.Typer(
    help="Dynamic v3 forward/PIT pressure sample ledger。",
    no_args_is_help=True,
)
dynamic_v3_weekly_defensive_evidence_app = typer.Typer(
    help="Dynamic v3 weekly defensive evidence update。",
    no_args_is_help=True,
)
dynamic_v3_confirmation_dashboard_app = typer.Typer(
    help="Dynamic v3 confirmation evidence dashboard。",
    no_args_is_help=True,
)
dynamic_v3_rule_review_queue_app = typer.Typer(
    help="Dynamic v3 owner rule review queue workflow。",
    no_args_is_help=True,
)
dynamic_v3_position_review_app = typer.Typer(
    help="Dynamic v3 rescue position review workflow。",
    no_args_is_help=True,
)
dynamic_shadow_app = typer.Typer(
    help="ETF owner-approved dynamic candidate forward shadow workflow。",
    no_args_is_help=True,
)
governance_app = typer.Typer(help="ETF P1 weight governance。", no_args_is_help=True)
events_app = typer.Typer(help="ETF P1 event risk flags。", no_args_is_help=True)
p2_app = typer.Typer(help="ETF P2 observe-only contracts。", no_args_is_help=True)
credibility_app = typer.Typer(help="ETF credibility validation gate。", no_args_is_help=True)

etf_app.add_typer(data_app, name="data")
etf_app.add_typer(features_app, name="features")
etf_app.add_typer(signals_app, name="signals")
etf_app.add_typer(regime_app, name="regime")
etf_app.add_typer(portfolio_app, name="portfolio")
etf_app.add_typer(backtest_app, name="backtest")
etf_app.add_typer(simulation_app, name="simulation")
etf_app.add_typer(report_app, name="report")
etf_app.add_typer(run_app, name="run")
etf_app.add_typer(relative_strength_app, name="relative-strength")
etf_app.add_typer(confirmation_app, name="confirmation")
etf_app.add_typer(satellite_app, name="satellite")
etf_app.add_typer(satellite_attribution_app, name="satellite-attribution")
etf_app.add_typer(attribution_app, name="attribution")
etf_app.add_typer(experiments_app, name="experiments")
etf_app.add_typer(forward_app, name="forward")
etf_app.add_typer(ai_confirmation_app, name="ai-confirmation")
etf_app.add_typer(ai_attribution_app, name="ai-attribution")
etf_app.add_typer(weekly_review_app, name="weekly-review")
etf_app.add_typer(decision_journal_app, name="decision-journal")
etf_app.add_typer(parameter_review_app, name="parameter-review")
etf_app.add_typer(weight_calibration_app, name="weight-calibration")
etf_app.add_typer(weight_research_app, name="weight-research")
etf_app.add_typer(ops_app, name="ops")
etf_app.add_typer(data_quality_app, name="data-quality")
etf_app.add_typer(evidence_dashboard_app, name="evidence-dashboard")
etf_app.add_typer(baseline_review_app, name="baseline-review")
etf_app.add_typer(shadow_review_app, name="shadow-review")
etf_app.add_typer(trend_calibration_app, name="trend-calibration")
etf_app.add_typer(dynamic_allocation_app, name="dynamic-allocation")
etf_app.add_typer(dynamic_calibration_app, name="dynamic-calibration")
etf_app.add_typer(dynamic_robustness_app, name="dynamic-robustness")
etf_app.add_typer(dynamic_rescue_app, name="dynamic-rescue")
etf_app.add_typer(dynamic_v2_review_app, name="dynamic-v2-review")
dynamic_v3_rescue_app.add_typer(dynamic_v3_sweep_config_app, name="sweep-config")
dynamic_v3_rescue_app.add_typer(dynamic_v3_sweep_app, name="sweep")
dynamic_v3_rescue_app.add_typer(dynamic_v3_data_audit_app, name="data-audit")
dynamic_v3_rescue_app.add_typer(dynamic_v3_data_provenance_app, name="data-provenance")
dynamic_v3_rescue_app.add_typer(dynamic_v3_window_audit_app, name="window-audit")
dynamic_v3_rescue_app.add_typer(dynamic_v3_weight_path_app, name="weight-path")
dynamic_v3_rescue_app.add_typer(dynamic_v3_injection_audit_app, name="injection-audit")
dynamic_v3_rescue_app.add_typer(dynamic_v3_candidate_app, name="candidate")
dynamic_v3_rescue_app.add_typer(dynamic_v3_walk_forward_app, name="walk-forward")
dynamic_v3_rescue_app.add_typer(dynamic_v3_robustness_app, name="robustness")
dynamic_v3_rescue_app.add_typer(dynamic_v3_overfit_app, name="overfit")
dynamic_v3_rescue_app.add_typer(dynamic_v3_shadow_app, name="shadow")
dynamic_v3_rescue_app.add_typer(dynamic_v3_artifacts_app, name="artifacts")
dynamic_v3_rescue_app.add_typer(dynamic_v3_schedule_app, name="schedule")
dynamic_v3_rescue_app.add_typer(dynamic_v3_evidence_summary_app, name="evidence-summary")
dynamic_v3_rescue_app.add_typer(dynamic_v3_medium_real_app, name="medium-real")
dynamic_v3_rescue_app.add_typer(dynamic_v3_regime_coverage_app, name="regime-coverage")
dynamic_v3_rescue_app.add_typer(dynamic_v3_promotion_app, name="promotion")
dynamic_v3_rescue_app.add_typer(dynamic_v3_observe_pool_app, name="observe-pool")
dynamic_v3_rescue_app.add_typer(dynamic_v3_overnight_readiness_app, name="overnight-readiness")
dynamic_v3_rescue_app.add_typer(dynamic_v3_governance_app, name="governance")
dynamic_v3_rescue_app.add_typer(dynamic_v3_research_app, name="research")
dynamic_v3_rescue_app.add_typer(dynamic_v3_research_decision_app, name="research-decision")
dynamic_v3_rescue_app.add_typer(dynamic_v3_evidence_diagnosis_app, name="evidence-diagnosis")
dynamic_v3_rescue_app.add_typer(dynamic_v3_gate_impact_app, name="gate-impact")
dynamic_v3_rescue_app.add_typer(dynamic_v3_gate_policy_app, name="gate-policy")
dynamic_v3_rescue_app.add_typer(dynamic_v3_candidate_recovery_app, name="candidate-recovery")
dynamic_v3_rescue_app.add_typer(dynamic_v3_shortlist_app, name="shortlist")
dynamic_v3_rescue_app.add_typer(dynamic_v3_candidate_cluster_app, name="candidate-cluster")
dynamic_v3_rescue_app.add_typer(dynamic_v3_shadow_shortlist_app, name="shadow-shortlist")
dynamic_v3_rescue_app.add_typer(dynamic_v3_shadow_monitor_run_app, name="shadow-monitor")
dynamic_v3_rescue_app.add_typer(dynamic_v3_portfolio_snapshot_app, name="portfolio-snapshot")
dynamic_v3_rescue_app.add_typer(dynamic_v3_manual_portfolio_app, name="manual-portfolio")
dynamic_v3_rescue_app.add_typer(dynamic_v3_portfolio_exposure_app, name="portfolio-exposure")
dynamic_v3_rescue_app.add_typer(dynamic_v3_position_drift_app, name="position-drift")
dynamic_v3_rescue_app.add_typer(dynamic_v3_execution_guardrails_app, name="execution-guardrails")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_manual_execution_review_app,
    name="manual-execution-review",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_real_snapshot_app, name="real-snapshot")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_real_snapshot_dry_run_app,
    name="real-snapshot-dry-run",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_real_execution_owner_review_app,
    name="real-execution-owner-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_real_snapshot_paper_action_app,
    name="real-snapshot-paper-action",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_weekly_real_snapshot_review_app,
    name="weekly-real-snapshot-review",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_position_advisory_app, name="position-advisory")
dynamic_v3_rescue_app.add_typer(dynamic_v3_consensus_drift_app, name="consensus-drift")
dynamic_v3_rescue_app.add_typer(dynamic_v3_owner_review_app, name="owner-review")
dynamic_v3_rescue_app.add_typer(dynamic_v3_paper_portfolio_app, name="paper-portfolio")
dynamic_v3_rescue_app.add_typer(dynamic_v3_model_target_app, name="model-target")
dynamic_v3_rescue_app.add_typer(dynamic_v3_paper_shadow_app, name="paper-shadow")
dynamic_v3_rescue_app.add_typer(dynamic_v3_model_rebalance_app, name="model-rebalance")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_paper_shadow_performance_app,
    name="paper-shadow-performance",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_system_target_review_app,
    name="system-target-review",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_paper_shadow_backfill_app, name="paper-shadow-backfill")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_paper_shadow_rolling_eval_app,
    name="paper-shadow-rolling-eval",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_paper_shadow_regime_review_app,
    name="paper-shadow-regime-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_paper_shadow_stability_app,
    name="paper-shadow-stability",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_system_target_selection_review_app,
    name="system-target-selection-review",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_selection_attribution_app, name="selection-attribution")
dynamic_v3_rescue_app.add_typer(dynamic_v3_limited_long_risk_app, name="limited-long-risk")
dynamic_v3_rescue_app.add_typer(dynamic_v3_limited_consistency_app, name="limited-consistency")
dynamic_v3_rescue_app.add_typer(dynamic_v3_data_warning_impact_app, name="data-warning-impact")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_research_method_hardening_app,
    name="research-method-hardening",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_limited_instability_app, name="limited-instability")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_limited_risk_attribution_app,
    name="limited-risk-attribution",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_data_warning_repair_plan_app,
    name="data-warning-repair-plan",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_alternative_method_review_app,
    name="alternative-method-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_refined_method_proposal_app,
    name="refined-method-proposal",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_risk_capped_limited_app, name="risk-capped-limited")
dynamic_v3_rescue_app.add_typer(dynamic_v3_risk_capped_backfill_app, name="risk-capped-backfill")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_risk_capped_comparison_app,
    name="risk-capped-comparison",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_risk_capped_review_app, name="risk-capped-review")
dynamic_v3_rescue_app.add_typer(dynamic_v3_smoothed_limited_app, name="smoothed-limited")
dynamic_v3_rescue_app.add_typer(dynamic_v3_smoothed_backfill_app, name="smoothed-backfill")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_comparison_app,
    name="smoothed-comparison",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_smoothed_review_app, name="smoothed-review")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_review_attribution_app,
    name="smoothed-review-attribution",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothing_benefit_lag_app,
    name="smoothing-benefit-lag",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_regime_validation_app,
    name="smoothed-regime-validation",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_confirmation_app,
    name="smoothed-confirmation",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_smoothed_watch_pack_app, name="smoothed-watch-pack")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_evidence_gap_app,
    name="smoothed-evidence-gap",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_churn_backfill_app,
    name="smoothed-churn-backfill",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_sideways_mixed_attribution_app,
    name="sideways-mixed-attribution",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_readiness_scorecard_app,
    name="smoothed-readiness-scorecard",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_owner_review_update_app,
    name="smoothed-owner-review-update",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_promotion_review_app,
    name="smoothed-promotion-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_primary_research_candidate_gate_app,
    name="primary-research-candidate-gate",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_forward_binding_app,
    name="smoothed-forward-binding",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_paper_shadow_primary_switch_app,
    name="paper-shadow-primary-switch",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_owner_promotion_app,
    name="smoothed-owner-promotion",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_forward_progress_app,
    name="smoothed-forward-progress",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_weekly_dashboard_app,
    name="smoothed-weekly-dashboard",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_event_monitor_app,
    name="smoothed-event-monitor",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_switch_readiness_app,
    name="smoothed-switch-readiness",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_owner_renewal_app,
    name="smoothed-owner-renewal",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_daily_emission_app,
    name="smoothed-daily-emission",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_outcome_due_app,
    name="smoothed-outcome-due",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_outcome_update_app,
    name="smoothed-outcome-update",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_forward_classify_app,
    name="smoothed-forward-classify",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_forward_weekly_run_app,
    name="smoothed-forward-weekly-run",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_data_preflight_app,
    name="smoothed-data-preflight",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_latest_emission_app,
    name="smoothed-latest-emission",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_blocked_explain_app,
    name="smoothed-blocked-explain",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_refresh_plan_app,
    name="smoothed-refresh-plan",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_bootstrap_retry_app,
    name="smoothed-bootstrap-retry",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_source_refresh_app,
    name="smoothed-source-refresh",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_post_refresh_validate_app,
    name="smoothed-post-refresh-validate",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_retry_resume_app,
    name="smoothed-retry-resume",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_sample_growth_app,
    name="smoothed-sample-growth",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_smoothed_data_readiness_app,
    name="smoothed-data-readiness",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_weight_search_space_app, name="weight-search-space")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_weight_experiment_batch2_app,
    name="weight-experiment-batch2",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_weight_batch_backfill_app,
    name="weight-batch-backfill",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_weight_scorecard_app, name="weight-scorecard")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_weight_robustness_review_app,
    name="weight-robustness-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_weight_adaptive_branch_app,
    name="weight-adaptive-branch",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_weight_expanded_search_app,
    name="weight-expanded-search",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_weight_candidate_cluster_app,
    name="weight-candidate-cluster",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_weight_top_candidate_interpretation_app,
    name="weight-top-candidate-interpretation",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_weight_method_promotion_gate_app,
    name="weight-method-promotion-gate",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_formal_method_auto_plan_app,
    name="formal-method-auto-plan",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_weight_search_dashboard_app,
    name="weight-search-dashboard",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_owner_research_decision_pack_app,
    name="owner-research-decision-pack",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_no_promotion_review_app, name="no-promotion-review")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_near_miss_candidates_app,
    name="near-miss-candidates",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_cash_buffer_attribution_app,
    name="cash-buffer-attribution",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_search_coverage_gap_app, name="search-coverage-gap")
dynamic_v3_rescue_app.add_typer(dynamic_v3_targeted_search_v3_app, name="targeted-search-v3")
dynamic_v3_rescue_app.add_typer(dynamic_v3_targeted_v3_backfill_app, name="targeted-v3-backfill")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_near_miss_ab_comparison_app,
    name="near-miss-ab-comparison",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_promotion_threshold_sensitivity_app,
    name="promotion-threshold-sensitivity",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_candidate_promotion_v2_app,
    name="candidate-promotion-v2",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_next_formal_or_search_plan_app,
    name="next-formal-or-search-plan",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_gate_calibration_review_app,
    name="gate-calibration-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_scorecard_attribution_app,
    name="scorecard-attribution",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_signal_instability_diagnosis_app,
    name="signal-instability-diagnosis",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_consensus_quality_review_app,
    name="consensus-quality-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_micro_search_v4_design_app,
    name="micro-search-v4-design",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_micro_search_v4_backfill_app,
    name="micro-search-v4-backfill",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_gate_calibrated_review_app,
    name="gate-calibrated-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_signal_vs_parameter_attribution_app,
    name="signal-vs-parameter-attribution",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_next_research_direction_app,
    name="next-research-direction",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_owner_research_roadmap_app,
    name="owner-research-roadmap",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_signal_failure_taxonomy_app,
    name="signal-failure-taxonomy",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_candidate_signal_ledger_app,
    name="candidate-signal-ledger",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_signal_churn_root_cause_app,
    name="signal-churn-root-cause",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_regime_mismatch_attribution_app,
    name="regime-mismatch-attribution",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_candidate_quality_filter_design_app,
    name="candidate-quality-filter-design",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_filtered_candidate_backfill_app,
    name="filtered-candidate-backfill",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_filtered_vs_original_comparison_app,
    name="filtered-vs-original-comparison",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_signal_gate_experiment_app,
    name="signal-gate-experiment",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_filtered_candidate_promotion_review_app,
    name="filtered-candidate-promotion-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_owner_signal_roadmap_app,
    name="owner-signal-roadmap",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_filtered_candidate_evidence_app,
    name="filtered-candidate-evidence",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_median_regime_filter_spec_app,
    name="median-regime-filter-spec",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_filtered_candidate_stress_backfill_app,
    name="filtered-candidate-stress-backfill",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_drawdown_mismatch_reduction_app,
    name="drawdown-mismatch-reduction",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_flip_rotation_reduction_app,
    name="flip-rotation-reduction",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_filtered_candidate_ab_review_app,
    name="filtered-candidate-ab-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_signal_gate_confirmation_app,
    name="signal-gate-confirmation",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_filtered_formalization_readiness_app,
    name="filtered-formalization-readiness",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_owner_filtered_candidate_review_app,
    name="owner-filtered-candidate-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_filtered_next_decision_app,
    name="filtered-next-decision",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_formal_research_method_contract_app,
    name="research-method-contract",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_promotion_gate_threshold_calibration_app,
    name="promotion-gate-threshold-calibration",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_paper_shadow_protocol_app,
    name="paper-shadow-protocol",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_signal_input_completeness_app,
    name="signal-input-completeness",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_signal_input_completeness_recovery_app,
    name="signal-input-completeness-recovery",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_signal_input_recovery_app,
    name="signal-input-recovery",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_paper_shadow_daily_app,
    name="paper-shadow-daily",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_paper_shadow_drift_monitor_app,
    name="paper-shadow-drift-monitor",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_paper_shadow_weekly_review_app,
    name="paper-shadow-weekly-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_paper_shadow_health_app,
    name="paper-shadow-health",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_readiness_health_recovery_app,
    name="readiness-health-recovery",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_normal_paper_shadow_resumption_gate_app,
    name="normal-paper-shadow-resumption-gate",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_paper_shadow_outcome_attribution_app,
    name="paper-shadow-outcome-attribution",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_shadow_decision_comparison_app,
    name="shadow-decision-comparison",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_cost_sensitivity_review_app,
    name="cost-sensitivity-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_cost_sensitivity_metrics_materialization_app,
    name="cost-sensitivity-metrics-materialization",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_metric_source_map_app,
    name="metric-source-map",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_benchmark_baseline_control_app,
    name="benchmark-baseline-control",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_benchmark_baseline_metrics_materialization_app,
    name="benchmark-baseline-metrics-materialization",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_candidate_regression_replay_app,
    name="candidate-regression-replay",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_candidate_decision_ledger_app,
    name="candidate-decision-ledger",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_evidence_staleness_monitor_app,
    name="evidence-staleness-monitor",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_shadow_continuation_readiness_app,
    name="shadow-continuation-readiness",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_stress_scenario_library_app,
    name="stress-scenario-library",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_drawdown_event_casebook_app,
    name="drawdown-event-casebook",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_flip_rotation_event_casebook_app,
    name="flip-rotation-event-casebook",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_hypothesis_backlog_app, name="hypothesis-backlog")
dynamic_v3_rescue_app.add_typer(dynamic_v3_variant_transform_app, name="variant-transform")
dynamic_v3_rescue_app.add_typer(dynamic_v3_experiment_matrix_app, name="experiment-matrix")
dynamic_v3_rescue_app.add_typer(dynamic_v3_batch_experiment_app, name="batch-experiment")
dynamic_v3_rescue_app.add_typer(dynamic_v3_experiment_triage_app, name="experiment-triage")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_top_variant_interpretation_app,
    name="top-variant-interpretation",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_method_promotion_plan_app,
    name="method-promotion-plan",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_advisory_outcome_app, name="advisory-outcome")
dynamic_v3_rescue_app.add_typer(dynamic_v3_owner_attribution_app, name="owner-attribution")
dynamic_v3_rescue_app.add_typer(dynamic_v3_shadow_aging_app, name="shadow-aging")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_weekly_advisory_review_app,
    name="weekly-advisory-review",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_replay_inventory_app, name="replay-inventory")
dynamic_v3_rescue_app.add_typer(dynamic_v3_historical_replay_app, name="historical-replay")
dynamic_v3_rescue_app.add_typer(dynamic_v3_backfill_outcome_app, name="backfill-outcome")
dynamic_v3_rescue_app.add_typer(dynamic_v3_historical_paper_sim_app, name="historical-paper-sim")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_replay_performance_review_app,
    name="replay-performance-review",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_replay_diagnosis_app, name="replay-diagnosis")
dynamic_v3_rescue_app.add_typer(dynamic_v3_backfill_repair_app, name="backfill-repair")
dynamic_v3_rescue_app.add_typer(dynamic_v3_variant_comparison_app, name="variant-comparison")
dynamic_v3_rescue_app.add_typer(dynamic_v3_rule_calibration_app, name="rule-calibration")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_replay_forward_bridge_app,
    name="replay-forward-bridge",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_outcome_due_app, name="outcome-due")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_replay_sample_expansion_app,
    name="replay-sample-expansion",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_outcome_dashboard_app, name="outcome-dashboard")
dynamic_v3_rescue_app.add_typer(dynamic_v3_limited_vs_notrade_app, name="limited-vs-notrade")
dynamic_v3_rescue_app.add_typer(dynamic_v3_consensus_risk_app, name="consensus-risk")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_outcome_update_review_app,
    name="outcome-update-review",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_outcome_update_app, name="outcome-update")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_rolling_evidence_refresh_app,
    name="rolling-evidence-refresh",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_evidence_trend_app, name="evidence-trend")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_forward_outcome_decision_app,
    name="forward-outcome-decision",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_backtest_sim_app, name="backtest-sim")
dynamic_v3_rescue_app.add_typer(dynamic_v3_sim_interpretation_app, name="sim-interpretation")
dynamic_v3_rescue_app.add_typer(dynamic_v3_sim_risk_return_app, name="sim-risk-return")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_sim_defensive_validation_app,
    name="sim-defensive-validation",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_advisory_proposal_review_app,
    name="advisory-proposal-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_forward_confirmation_plan_app,
    name="forward-confirmation-plan",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_confirmation_targets_app,
    name="confirmation-targets",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_confirmation_progress_app,
    name="confirmation-progress",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_confirmation_evaluate_app,
    name="confirmation-evaluate",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_rule_review_cycle_app, name="rule-review-cycle")
dynamic_v3_rescue_app.add_typer(dynamic_v3_rule_owner_decision_app, name="rule-owner-decision")
dynamic_v3_rescue_app.add_typer(dynamic_v3_confirmation_cycle_app, name="confirmation-cycle")
dynamic_v3_rescue_app.add_typer(dynamic_v3_pressure_regime_tag_app, name="pressure-regime-tag")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_pressure_tag_diagnosis_app,
    name="pressure-tag-diagnosis",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_pressure_outcome_backfill_app,
    name="pressure-outcome-backfill",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_defensive_pressure_compare_app,
    name="defensive-pressure-compare",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_defensive_rule_review_app,
    name="defensive-rule-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_weekly_ops_decision_update_app,
    name="weekly-ops-decision-update",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_defensive_hypothesis_deep_dive_app,
    name="defensive-hypothesis-deep-dive",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_defensive_label_review_app,
    name="defensive-label-review",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_defensive_failure_study_app,
    name="defensive-failure-study",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_defensive_research_note_app,
    name="defensive-research-note",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_defensive_owner_pack_app,
    name="defensive-owner-pack",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_forward_pressure_capture_app,
    name="forward-pressure-capture",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_pressure_trigger_app, name="pressure-trigger")
dynamic_v3_rescue_app.add_typer(dynamic_v3_pressure_capture_app, name="pressure-capture")
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_pressure_sample_ledger_app,
    name="pressure-sample-ledger",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_weekly_defensive_evidence_app,
    name="weekly-defensive-evidence",
)
dynamic_v3_rescue_app.add_typer(
    dynamic_v3_confirmation_dashboard_app,
    name="confirmation-dashboard",
)
dynamic_v3_rescue_app.add_typer(dynamic_v3_rule_review_queue_app, name="rule-review-queue")
dynamic_v3_rescue_app.add_typer(dynamic_v3_position_review_app, name="position-review")
etf_app.add_typer(dynamic_v3_rescue_app, name="dynamic-v3-rescue")
etf_app.add_typer(dynamic_shadow_app, name="dynamic-shadow")
etf_app.add_typer(governance_app, name="governance")
etf_app.add_typer(events_app, name="events")
etf_app.add_typer(p2_app, name="p2")
etf_app.add_typer(credibility_app, name="credibility")
