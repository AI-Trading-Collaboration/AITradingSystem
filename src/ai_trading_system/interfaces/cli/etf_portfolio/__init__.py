from ai_trading_system.interfaces.cli.etf_portfolio import (
    baseline_review as baseline_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import data as data_commands
from ai_trading_system.interfaces.cli.etf_portfolio import data_quality as data_quality_commands
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
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v2_review as dynamic_v2_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_advisory_outcome as dynamic_v3_advisory_outcome_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_backfilled_outcome as dynamic_v3_backfilled_outcome_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_candidate_evidence as dynamic_v3_candidate_evidence_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_candidate_observation as dynamic_v3_candidate_observation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_consensus_drift as dynamic_v3_consensus_drift_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_data_audit as dynamic_v3_data_audit_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_data_provenance as dynamic_v3_data_provenance_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_evidence_governance as dynamic_v3_evidence_governance_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_evidence_readiness as dynamic_v3_evidence_readiness_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_failure_attribution as dynamic_v3_failure_attribution_commands,
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
    dynamic_v3_manual_execution_review as dynamic_v3_manual_execution_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_observation_lifecycle as dynamic_v3_observation_lifecycle_commands,
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
    dynamic_v3_replay_inventory as dynamic_v3_replay_inventory_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_replay_performance_review as dynamic_v3_replay_performance_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_rescue as dynamic_v3_rescue_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_research_control as dynamic_v3_research_control_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_shadow_aging as dynamic_v3_shadow_aging_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_shadow_registry as dynamic_v3_shadow_registry_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_sweep_config as dynamic_v3_sweep_config_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_sweep_runtime as dynamic_v3_sweep_runtime_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_validation_evidence as dynamic_v3_validation_evidence_commands,
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
    dynamic_v3_window_audit as dynamic_v3_window_audit_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import operations as operations_commands
from ai_trading_system.interfaces.cli.etf_portfolio import (
    parameter_review as parameter_review_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import reporting as reporting_commands
from ai_trading_system.interfaces.cli.etf_portfolio import (
    satellite_attribution as satellite_attribution_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import shadow_review as shadow_review_commands
from ai_trading_system.interfaces.cli.etf_portfolio import (
    trend_calibration as trend_calibration_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import weekly_review as weekly_review_commands
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
    "dynamic_v3_real_evaluation_commands",
    "dynamic_v3_real_execution_owner_review_commands",
    "dynamic_v3_real_snapshot_intake_commands",
    "dynamic_v3_real_snapshot_dry_run_commands",
    "dynamic_v3_research_control_commands",
    "dynamic_v3_real_snapshot_paper_action_commands",
    "dynamic_v3_failure_attribution_commands",
    "dynamic_v3_evidence_readiness_commands",
    "dynamic_v3_evidence_governance_commands",
    "dynamic_v3_injection_audit_commands",
    "dynamic_v3_legacy_validation_commands",
    "dynamic_v3_manual_execution_review_commands",
    "dynamic_v3_observation_lifecycle_commands",
    "dynamic_v3_owner_review_journal_commands",
    "dynamic_v3_owner_attribution_commands",
    "dynamic_v3_paper_portfolio_commands",
    "dynamic_v3_portfolio_intake_commands",
    "dynamic_v3_portfolio_risk_controls_commands",
    "dynamic_v3_position_advisory_commands",
    "dynamic_v3_position_advisory_daily_commands",
    "dynamic_v3_replay_inventory_commands",
    "dynamic_v3_replay_diagnosis_commands",
    "dynamic_v3_replay_performance_review_commands",
    "dynamic_v3_historical_replay_commands",
    "dynamic_v3_backfilled_outcome_commands",
    "dynamic_v3_historical_paper_sim_commands",
    "dynamic_v3_shadow_registry_commands",
    "dynamic_v3_shadow_aging_commands",
    "dynamic_v3_sweep_config_commands",
    "dynamic_v3_sweep_runtime_commands",
    "dynamic_v3_data_audit_commands",
    "dynamic_v3_data_provenance_commands",
    "dynamic_v3_candidate_evidence_commands",
    "dynamic_v3_candidate_observation_commands",
    "dynamic_v3_consensus_drift_commands",
    "dynamic_v3_window_audit_commands",
    "dynamic_v3_weekly_real_snapshot_review_commands",
    "dynamic_v3_weekly_advisory_review_commands",
    "dynamic_v3_weight_path_commands",
    "dynamic_v3_validation_evidence_commands",
    "etf_app",
    "operations_commands",
    "parameter_review_commands",
    "reporting_commands",
    "shadow_review_commands",
    "satellite_attribution_commands",
    "weekly_review_commands",
    "trend_calibration_commands",
]
