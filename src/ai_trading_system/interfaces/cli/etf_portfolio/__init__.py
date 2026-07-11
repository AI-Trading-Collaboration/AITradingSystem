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
    dynamic_v3_candidate_evidence as dynamic_v3_candidate_evidence_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_data_audit as dynamic_v3_data_audit_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_data_provenance as dynamic_v3_data_provenance_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_evidence_readiness as dynamic_v3_evidence_readiness_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_failure_attribution as dynamic_v3_failure_attribution_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_injection_audit as dynamic_v3_injection_audit_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_legacy_validation as dynamic_v3_legacy_validation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_observation_lifecycle as dynamic_v3_observation_lifecycle_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_real_evaluation as dynamic_v3_real_evaluation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_rescue as dynamic_v3_rescue_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_research_control as dynamic_v3_research_control_commands,
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
    "dynamic_v3_real_evaluation_commands",
    "dynamic_v3_research_control_commands",
    "dynamic_v3_failure_attribution_commands",
    "dynamic_v3_evidence_readiness_commands",
    "dynamic_v3_injection_audit_commands",
    "dynamic_v3_legacy_validation_commands",
    "dynamic_v3_observation_lifecycle_commands",
    "dynamic_v3_shadow_registry_commands",
    "dynamic_v3_sweep_config_commands",
    "dynamic_v3_sweep_runtime_commands",
    "dynamic_v3_data_audit_commands",
    "dynamic_v3_data_provenance_commands",
    "dynamic_v3_candidate_evidence_commands",
    "dynamic_v3_window_audit_commands",
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
