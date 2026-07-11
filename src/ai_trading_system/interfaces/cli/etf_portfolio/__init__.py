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
    dynamic_v3_failure_attribution as dynamic_v3_failure_attribution_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_real_evaluation as dynamic_v3_real_evaluation_commands,
)
from ai_trading_system.interfaces.cli.etf_portfolio import (
    dynamic_v3_rescue as dynamic_v3_rescue_commands,
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
    "dynamic_v3_failure_attribution_commands",
    "etf_app",
    "operations_commands",
    "parameter_review_commands",
    "reporting_commands",
    "shadow_review_commands",
    "satellite_attribution_commands",
    "weekly_review_commands",
    "trend_calibration_commands",
]
