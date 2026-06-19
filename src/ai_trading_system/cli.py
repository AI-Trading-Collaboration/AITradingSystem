from __future__ import annotations

import typer

from ai_trading_system.cli_commands.backtest import register_backtest_commands
from ai_trading_system.cli_commands.catalysts import catalysts_app
from ai_trading_system.cli_commands.data import data_app
from ai_trading_system.cli_commands.data_cache import register_data_cache_commands
from ai_trading_system.cli_commands.data_sources import data_sources_app
from ai_trading_system.cli_commands.docs import docs_app
from ai_trading_system.cli_commands.etf_compat import (
    experiments_app,
    features_app,
    regime_app,
    register_etf_compatibility_aliases,
    report_app,
    run_app,
    simulation_app,
)
from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.cli_commands.evidence import evidence_app
from ai_trading_system.cli_commands.execution import execution_app
from ai_trading_system.cli_commands.feedback import feedback_app
from ai_trading_system.cli_commands.fundamentals import fundamentals_app
from ai_trading_system.cli_commands.industry_chain import industry_chain_app
from ai_trading_system.cli_commands.llm import llm_app
from ai_trading_system.cli_commands.market_features import register_market_feature_commands
from ai_trading_system.cli_commands.ops import ops_app
from ai_trading_system.cli_commands.parameters import parameters_app
from ai_trading_system.cli_commands.pit_snapshots import pit_snapshots_app
from ai_trading_system.cli_commands.portfolio import portfolio_app
from ai_trading_system.cli_commands.reports import reports_app
from ai_trading_system.cli_commands.risk_events import risk_events_app
from ai_trading_system.cli_commands.root_utils import register_root_utility_commands
from ai_trading_system.cli_commands.scenarios import scenarios_app
from ai_trading_system.cli_commands.score_daily import score_daily_app
from ai_trading_system.cli_commands.sec_pit import sec_pit_app
from ai_trading_system.cli_commands.security import security_app
from ai_trading_system.cli_commands.signals import signals_app
from ai_trading_system.cli_commands.system import system_app
from ai_trading_system.cli_commands.thesis import thesis_app
from ai_trading_system.cli_commands.trace import trace_app
from ai_trading_system.cli_commands.trade_review import register_trade_review_commands
from ai_trading_system.cli_commands.valuation import valuation_app
from ai_trading_system.cli_commands.watchlist import watchlist_app

app = typer.Typer(help="AI 产业链趋势分析和仓位管理工具。", no_args_is_help=True)
app.add_typer(watchlist_app, name="watchlist")
app.add_typer(industry_chain_app, name="industry-chain")
app.add_typer(thesis_app, name="thesis")
app.add_typer(risk_events_app, name="risk-events")
app.add_typer(valuation_app, name="valuation")
app.add_typer(data_app, name="data")
app.add_typer(features_app, name="features")
app.add_typer(data_sources_app, name="data-sources")
app.add_typer(fundamentals_app, name="fundamentals")
app.add_typer(trace_app, name="trace")
app.add_typer(evidence_app, name="evidence")
app.add_typer(feedback_app, name="feedback")
app.add_typer(scenarios_app, name="scenarios")
app.add_typer(catalysts_app, name="catalysts")
app.add_typer(execution_app, name="execution")
app.add_typer(portfolio_app, name="portfolio")
app.add_typer(parameters_app, name="parameters")
app.add_typer(signals_app, name="signals")
app.add_typer(regime_app, name="regime")
app.add_typer(reports_app, name="reports")
app.add_typer(simulation_app, name="simulation")
app.add_typer(report_app, name="report")
app.add_typer(run_app, name="run")
app.add_typer(experiments_app, name="experiments")
app.add_typer(ops_app, name="ops")
app.add_typer(system_app, name="system")
app.add_typer(security_app, name="security")
app.add_typer(llm_app, name="llm")
app.add_typer(pit_snapshots_app, name="pit-snapshots")
app.add_typer(score_daily_app, name="score-daily")
app.add_typer(docs_app, name="docs")
app.add_typer(sec_pit_app, name="sec-pit")
app.add_typer(etf_app, name="etf")
register_data_cache_commands(app)
register_root_utility_commands(app)
register_trade_review_commands(app)
register_market_feature_commands(app)
register_backtest_commands(app)


@app.callback()
def main() -> None:
    """AI 产业链趋势分析和仓位管理工具。"""


register_etf_compatibility_aliases(
    data_app=data_app,
    portfolio_app=portfolio_app,
    signals_app=signals_app,
)
if __name__ == "__main__":
    app()
