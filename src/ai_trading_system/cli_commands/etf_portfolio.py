from __future__ import annotations

from ai_trading_system.interfaces.cli.etf_portfolio.data_quality import (
    DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH as DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH,
)
from ai_trading_system.interfaces.cli.etf_portfolio.data_quality import (
    DEFAULT_ETF_DATA_QUALITY_REPORT_DIR as DEFAULT_ETF_DATA_QUALITY_REPORT_DIR,
)
from ai_trading_system.interfaces.cli.etf_portfolio.data_quality import (
    DEFAULT_ETF_DATA_QUALITY_VALIDATION_DIR as DEFAULT_ETF_DATA_QUALITY_VALIDATION_DIR,
)
from ai_trading_system.interfaces.cli.etf_portfolio.data_quality import (
    DEFAULT_REPORT_REGISTRY_PATH as DEFAULT_REPORT_REGISTRY_PATH,
)
from ai_trading_system.interfaces.cli.etf_portfolio.experiments import (
    experiments_compare_command as experiments_compare_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.experiments import (
    experiments_register_command as experiments_register_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.experiments import (
    experiments_run_command as experiments_run_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.forward import (
    DEFAULT_ETF_FORWARD_CONFIG_PATH as DEFAULT_ETF_FORWARD_CONFIG_PATH,
)
from ai_trading_system.interfaces.cli.etf_portfolio.forward import (
    DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH as DEFAULT_ETF_FORWARD_DECISION_LEDGER_PATH,
)
from ai_trading_system.interfaces.cli.etf_portfolio.forward import (
    DEFAULT_ETF_FORWARD_REPORT_DIR as DEFAULT_ETF_FORWARD_REPORT_DIR,
)
from ai_trading_system.interfaces.cli.etf_portfolio.forward import (
    DEFAULT_ETF_PRICE_PATH as DEFAULT_ETF_PRICE_PATH,
)
from ai_trading_system.interfaces.cli.etf_portfolio.forward import (
    DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH as DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
)
from ai_trading_system.interfaces.cli.etf_portfolio.forward import (
    PROJECT_ROOT as PROJECT_ROOT,
)
from ai_trading_system.interfaces.cli.etf_portfolio.forward import (
    forward_dashboard_command as forward_dashboard_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.forward import (
    forward_update_command as forward_update_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.forward import (
    forward_watchlist_command as forward_watchlist_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    ai_attribution_app as ai_attribution_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    ai_confirmation_app as ai_confirmation_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    attribution_app as attribution_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    backtest_app as backtest_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    confirmation_app as confirmation_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    credibility_app as credibility_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    decision_journal_app as decision_journal_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_shadow_app as dynamic_shadow_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    etf_app as etf_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    events_app as events_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    experiments_app as experiments_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    forward_app as forward_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    governance_app as governance_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    p2_app as p2_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    portfolio_app as portfolio_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    regime_app as regime_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    relative_strength_app as relative_strength_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    report_app as report_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    run_app as run_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    satellite_app as satellite_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    signals_app as signals_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    simulation_app as simulation_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    weight_calibration_app as weight_calibration_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    weight_research_app as weight_research_app,
)
from ai_trading_system.interfaces.cli.etf_portfolio.simulation import (
    simulation_evaluate_command as simulation_evaluate_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.simulation import (
    simulation_record_command as simulation_record_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.simulation import (
    simulation_report_command as simulation_report_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.workflow import (
    portfolio_allocate_command as portfolio_allocate_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.workflow import (
    regime_generate_command as regime_generate_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.workflow import (
    report_daily_command as report_daily_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.workflow import (
    run_daily_command as run_daily_command,
)
from ai_trading_system.interfaces.cli.etf_portfolio.workflow import (
    signals_generate_command as signals_generate_command,
)
