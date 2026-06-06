from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Annotated, Literal

import pandas as pd
import typer
from rich.console import Console

from ai_trading_system.alerts import (
    build_daily_alert_report,
    default_alert_report_path,
    render_alert_summary_section,
    write_alert_report,
)
from ai_trading_system.backtest.daily import (
    DEFAULT_BENCHMARK_TICKERS,
    BacktestRegimeContext,
)
from ai_trading_system.belief_state import (
    DEFAULT_BELIEF_STATE_DIR,
    DEFAULT_BELIEF_STATE_HISTORY_PATH,
    append_belief_state_history,
    build_belief_state,
    default_belief_state_path,
    render_belief_state_summary,
    write_belief_state,
)
from ai_trading_system.catalyst_calendar import (
    load_catalyst_calendar,
    validate_catalyst_calendar,
)
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
from ai_trading_system.cli_commands.risk_event_artifacts import (
    DEFAULT_OPENAI_REQUEST_CACHE_PATH,
    DEFAULT_RISK_EVENT_DAILY_PREREVIEW_PROFILE,
    DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH,
    _coalesce_profile_value,
    _load_llm_request_profile,
)
from ai_trading_system.cli_commands.risk_events import risk_events_app
from ai_trading_system.cli_commands.root_utils import register_root_utility_commands
from ai_trading_system.cli_commands.scenarios import scenarios_app
from ai_trading_system.cli_commands.sec_pit import sec_pit_app
from ai_trading_system.cli_commands.security import security_app
from ai_trading_system.cli_commands.signals import signals_app
from ai_trading_system.cli_commands.thesis import thesis_app
from ai_trading_system.cli_commands.trace import trace_app
from ai_trading_system.cli_commands.trace_config import daily_trace_config_paths
from ai_trading_system.cli_commands.trade_review import register_trade_review_commands
from ai_trading_system.cli_commands.valuation import valuation_app
from ai_trading_system.cli_commands.watchlist import watchlist_app
from ai_trading_system.config import (
    DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
    DEFAULT_DATA_SOURCES_CONFIG_PATH,
    DEFAULT_EXECUTION_POLICY_CONFIG_PATH,
    DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    DEFAULT_RISK_EVENTS_CONFIG_PATH,
    DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    PROJECT_ROOT,
    IndustryChainConfig,
    SecCompaniesConfig,
    UniverseConfig,
    WatchlistConfig,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_data_sources,
    load_features,
    load_fundamental_features,
    load_fundamental_metrics,
    load_industry_chain,
    load_market_regimes,
    load_portfolio,
    load_risk_events,
    load_scoring_rules,
    load_sec_companies,
    load_universe,
    load_watchlist,
    market_regime_by_id,
)
from ai_trading_system.data.quality import (
    DataQualityReport,
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.decision_snapshots import (
    DEFAULT_DECISION_SNAPSHOT_DIR,
    build_decision_snapshot,
    default_decision_snapshot_path,
    write_decision_snapshot,
)
from ai_trading_system.execution_policy import (
    build_execution_advisory,
    default_execution_policy_report_path,
    load_execution_policy,
    render_execution_advisory_section,
    validate_execution_policy,
    write_execution_policy_report,
)
from ai_trading_system.feature_availability import (
    DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
    FeatureAvailabilitySourceCheck,
    build_feature_availability_report,
    build_feature_source_check,
    default_feature_availability_report_path,
    feature_availability_summary_record,
    render_feature_availability_section,
    write_feature_availability_report,
)
from ai_trading_system.features.market import (
    build_market_features,
    default_feature_report_path,
    write_feature_summary,
    write_features_csv,
)
from ai_trading_system.focus_stock_trends import (
    build_focus_stock_trend_report,
    render_focus_stock_trend_section,
)
from ai_trading_system.fundamentals.sec_features import (
    SecFundamentalFeaturesReport,
    build_sec_fundamental_features_report,
    default_sec_fundamental_features_csv_path,
    default_sec_fundamental_features_report_path,
    write_sec_fundamental_features_csv,
    write_sec_fundamental_features_report,
)
from ai_trading_system.fundamentals.sec_metrics import (
    default_sec_fundamental_metrics_csv_path,
    default_sec_fundamental_metrics_validation_report_path,
    load_sec_fundamental_metric_rows_csv,
    validate_sec_fundamental_metrics_csv,
    write_sec_fundamental_metric_rows_csv,
    write_sec_fundamental_metrics_validation_report,
)
from ai_trading_system.fundamentals.sec_pit_backfill import DEFAULT_SEC_EDGAR_PROCESSED_DIR
from ai_trading_system.fundamentals.sec_pit_panel import (
    sec_pit_feature_panel_to_feature_reports,
)
from ai_trading_system.fundamentals.tsm_ir import (
    load_tsm_ir_quarterly_metric_rows_csv,
    merge_tsm_ir_quarterly_rows_into_sec_metrics_as_of,
    select_tsm_ir_quarterly_metric_rows_as_of,
)
from ai_trading_system.industry_node_state import (
    build_industry_node_heat_report,
    render_industry_node_heat_section,
)
from ai_trading_system.llm_request_profiles import (
    DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
)
from ai_trading_system.official_policy_sources import (
    DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
    DEFAULT_OFFICIAL_POLICY_RAW_DIR,
    default_official_policy_candidates_path,
    default_official_policy_fetch_report_path,
    fetch_official_policy_sources,
    write_official_policy_fetch_report,
)
from ai_trading_system.ops_daily import (
    resolve_daily_ops_default_as_of,
)
from ai_trading_system.portfolio_exposure import (
    build_portfolio_exposure_report,
    default_portfolio_exposure_report_path,
    render_portfolio_exposure_section,
    write_portfolio_exposure_report,
)
from ai_trading_system.prediction_ledger import (
    DEFAULT_PREDICTION_LEDGER_PATH,
    append_prediction_records,
    build_prediction_record_from_decision_snapshot,
)
from ai_trading_system.report_traceability import (
    build_daily_score_trace_bundle,
    default_report_trace_bundle_path,
    render_traceability_section,
    write_trace_bundle,
)
from ai_trading_system.reports.calculation_explainers import (
    build_calculation_explainers_payload,
    default_calculation_explainers_path,
    write_calculation_explainers_json,
)
from ai_trading_system.risk_event_llm_formal import (
    build_llm_formal_assessment_report,
    default_llm_formal_assessment_report_path,
    write_llm_formal_assessment_outputs,
    write_llm_formal_assessment_report,
)
from ai_trading_system.risk_event_prereview import (
    default_risk_event_openai_prereview_report_path,
    run_openai_risk_event_prereview_for_official_candidates,
    write_risk_event_prereview_import_report,
    write_risk_event_prereview_queue,
)
from ai_trading_system.risk_events import (
    RiskEventOccurrenceReviewReport,
    RiskEventsValidationReport,
    build_risk_event_occurrence_review_report,
    default_risk_event_occurrence_report_path,
    load_risk_event_occurrence_store,
    validate_risk_event_occurrence_store,
    validate_risk_events_config,
    write_risk_event_occurrence_review_report,
)
from ai_trading_system.rule_governance import (
    DEFAULT_RULE_CARDS_PATH,
    build_rule_version_manifest,
    load_rule_card_store,
    validate_rule_card_store,
)
from ai_trading_system.scoring.baseline_score_backfill import (
    BASELINE_SCORE_BACKFILL_MODE,
    run_baseline_score_backfill,
)
from ai_trading_system.scoring.daily import (
    DailyManualReviewStatus,
    DailyReviewSummary,
    build_daily_score_report,
    build_weight_calibration_context,
    default_daily_score_report_path,
    load_previous_daily_score_snapshot,
    write_daily_score_report,
    write_scores_csv,
)
from ai_trading_system.thesis import (
    ThesisReviewReport,
    build_thesis_review_report,
    load_trade_thesis_store,
    validate_trade_thesis_store,
)
from ai_trading_system.trade_review import (
    build_trade_review_report,
    load_trade_record_store,
    validate_trade_record_store,
)
from ai_trading_system.valuation import (
    ValuationReviewReport,
    build_valuation_review_report,
    load_valuation_snapshot_store,
    validate_valuation_snapshot_store,
)
from ai_trading_system.weight_calibration import (
    DEFAULT_CURRENT_CONTEXT_PATH,
    DEFAULT_EFFECTIVE_WEIGHTS_PATH,
    resolve_calibration_application,
    write_calibration_context,
    write_effective_weights,
)

app = typer.Typer(help="AI 产业链趋势分析和仓位管理工具。", no_args_is_help=True)
score_daily_app = typer.Typer(help="每日评分和 research baseline backfill。", no_args_is_help=False)
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
console = Console()
DEFAULT_PORTFOLIO_POSITIONS_PATH = (
    PROJECT_ROOT / "data" / "external" / "portfolio_positions" / "current_positions.csv"
)


@score_daily_app.command(
    "backfill-baseline",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def score_daily_backfill_baseline_command(
    ctx: typer.Context,
    start: Annotated[
        str,
        typer.Option(help="Backfill 开始日期 YYYY-MM-DD。"),
    ],
    end: Annotated[
        str,
        typer.Option(help="Backfill 结束日期 YYYY-MM-DD。"),
    ],
    tickers: Annotated[
        list[str] | None,
        typer.Option(
            "--tickers",
            help="Backfill ticker；可重复。若使用 `--tickers A B C`，B/C 会作为额外 ticker 读取。",
        ),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Baseline score CSV 输出路径；默认写入 data/processed/research/ "
                "research-only 路径。"
            )
        ),
    ] = None,
    mode: Annotated[
        str,
        typer.Option(help="Backfill 模式；当前只允许 research_backfill。"),
    ] = BASELINE_SCORE_BACKFILL_MODE,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告路径。"),
    ] = None,
    overwrite: Annotated[
        bool,
        typer.Option("--overwrite", help="显式允许覆盖既有 output path。"),
    ] = False,
    overwrite_production_path: Annotated[
        bool,
        typer.Option(
            "--overwrite-production-path",
            help=(
                "允许把 research backfill 写入 production scores_daily.csv 路径；"
                "仍需在文件存在时同时传 --overwrite。"
            ),
        ),
    ] = False,
) -> None:
    """生成 research-only historical baseline score rows。"""
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    resolved_output_path = (
        _default_baseline_backfill_output_path(start_date, end_date)
        if output_path is None
        else _project_relative_path(output_path)
    )
    if _is_production_scores_daily_path(resolved_output_path) and not overwrite_production_path:
        raise typer.BadParameter(
            "research backfill 默认不得写入 data/processed/scores_daily.csv；"
            "如确需覆盖 production score path，请同时传 --overwrite-production-path。"
        )
    requested_tickers = [*(tickers or []), *[str(item) for item in ctx.args]]
    requested_tickers = [ticker for ticker in requested_tickers if ticker.strip()]
    if not requested_tickers:
        raise typer.BadParameter("至少需要一个 --tickers ticker。")

    universe = load_universe()
    expected_price_tickers = list(
        dict.fromkeys([*configured_price_tickers(universe), *requested_tickers])
    )
    expected_rate_series = configured_rate_series(universe)
    data_quality_config = load_data_quality()
    quality_output = quality_report_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"baseline_score_backfill_data_quality_{end_date.isoformat()}.md"
    )
    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_price_tickers,
        expected_rate_series=expected_rate_series,
        quality_config=data_quality_config,
        as_of=end_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止 baseline research backfill。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    result = run_baseline_score_backfill(
        start=start_date,
        end=end_date,
        tickers=requested_tickers,
        prices_path=prices_path,
        rates_path=rates_path,
        output_path=resolved_output_path,
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio=load_portfolio(),
        data_quality_status=data_quality_report.status,
        data_quality_report_path=quality_output,
        mode=mode,
        overwrite=overwrite,
    )
    console.print("[green]Baseline research backfill 完成。[/green]")
    console.print(f"输出：{result.output_path}")
    console.print(
        f"行数：{result.row_count}；ticker 数：{result.ticker_count}；日期数：{result.date_count}"
    )
    console.print(f"research_backfill=true；production_effect={result.production_effect}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")


def _default_baseline_backfill_output_path(start_date: date, end_date: date) -> Path:
    return (
        PROJECT_ROOT
        / "data"
        / "processed"
        / "research"
        / ("scores_daily_backfill_sec_pit_" f"{start_date:%Y%m%d}_{end_date:%Y%m%d}.csv")
    )


def _project_relative_path(path: Path) -> Path:
    return path if path.is_absolute() else PROJECT_ROOT / path


def _is_production_scores_daily_path(path: Path) -> bool:
    production_path = PROJECT_ROOT / "data" / "processed" / "scores_daily.csv"
    return path.resolve() == production_path.resolve()


@score_daily_app.callback(invoke_without_command=True)
def score_daily(
    ctx: typer.Context = None,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="评分日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    run_id: Annotated[
        str | None,
        typer.Option(help="可选 run id，用于 evidence bundle 和日报结论卡。"),
    ] = None,
    features_path: Annotated[
        Path,
        typer.Option(help="特征 CSV 输出路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "features_daily.csv",
    scores_path: Annotated[
        Path,
        typer.Option(help="评分 CSV 输出路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "scores_daily.csv",
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 每日评分报告输出路径。"),
    ] = None,
    alert_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 投资与数据告警报告输出路径。"),
    ] = None,
    portfolio_positions_path: Annotated[
        Path,
        typer.Option(help="真实持仓 CSV 路径，用于日报只读组合暴露分解。"),
    ] = DEFAULT_PORTFOLIO_POSITIONS_PATH,
    portfolio_exposure_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 组合暴露报告输出路径。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="JSON evidence bundle 输出路径。"),
    ] = None,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="JSON 决策快照输出路径。"),
    ] = None,
    calculation_explainers_path: Annotated[
        Path | None,
        typer.Option(help="JSON 计算解释输出路径。"),
    ] = None,
    belief_state_path: Annotated[
        Path | None,
        typer.Option(help="JSON 只读认知状态输出路径。"),
    ] = None,
    belief_state_history_path: Annotated[
        Path | None,
        typer.Option(help="CSV 只读认知状态历史索引输出路径。"),
    ] = None,
    feature_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 特征摘要输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
    feature_availability_path: Annotated[
        Path,
        typer.Option(help="PIT feature availability catalog YAML 路径。"),
    ] = DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
    feature_availability_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown PIT 特征可见时间报告输出路径。"),
    ] = None,
    prediction_ledger_path: Annotated[
        Path,
        typer.Option(help="append-only prediction/shadow ledger CSV 输出路径。"),
    ] = DEFAULT_PREDICTION_LEDGER_PATH,
    prediction_candidate_id: Annotated[
        str,
        typer.Option(help="写入 prediction ledger 的模型候选 ID。"),
    ] = "production",
    prediction_production_effect: Annotated[
        str,
        typer.Option(help="prediction ledger 生产影响标记：production 或 none。"),
    ] = "production",
    execution_policy_path: Annotated[
        Path,
        typer.Option(help="execution policy YAML 路径，用于日报执行建议。"),
    ] = DEFAULT_EXECUTION_POLICY_CONFIG_PATH,
    execution_policy_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown execution policy 校验报告输出路径。"),
    ] = None,
    rule_cards_path: Annotated[
        Path,
        typer.Option(help="rule card registry YAML 路径，用于记录本次日报规则版本。"),
    ] = DEFAULT_RULE_CARDS_PATH,
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径，用于日报基本面评分。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    sec_metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径，用于日报基本面评分。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    fundamental_feature_config_path: Annotated[
        Path,
        typer.Option(help="SEC 基本面特征公式配置文件路径，用于日报基本面评分。"),
    ] = DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    sec_fundamentals_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输入路径，用于日报基本面评分。"),
    ] = None,
    tsm_ir_quarterly_metrics_path: Annotated[
        Path,
        typer.Option(
            help=("TSMC IR 季度指标 CSV；存在时日报会按 as-of 合并到 " "SEC-style 指标后再校验。")
        ),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "tsm_ir_quarterly_metrics.csv",
    tsm_ir_merge: Annotated[
        bool,
        typer.Option(
            "--tsm-ir-merge/--skip-tsm-ir-merge",
            help="是否在日报基本面校验前合并 TSMC IR 季度指标。",
        ),
    ] = True,
    sec_fundamental_features_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面特征 CSV 输出路径，用于日报基本面评分。"),
    ] = None,
    sec_fundamental_feature_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面特征报告输出路径。"),
    ] = None,
    sec_metrics_validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面指标 CSV 校验报告输出路径。"),
    ] = None,
    sec_fundamental_source: Annotated[
        Literal["legacy_companyfacts", "sec_pit_feature_panel"],
        typer.Option(
            "--sec-fundamental-source",
            help=(
                "日报基本面来源；legacy_companyfacts 使用 SEC-style 指标 CSV，"
                "sec_pit_feature_panel 使用 TRADING-039 reconstructed PIT panel。"
            ),
        ),
    ] = "legacy_companyfacts",
    sec_pit_feature_panel_path: Annotated[
        Path,
        typer.Option(help="SEC reconstructed PIT feature panel CSV 路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "sec_pit_feature_panel.csv",
    thesis_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径，用于写入日报复核摘要。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "trade_theses",
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于写入日报复核摘要。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    risk_event_occurrences_path: Annotated[
        Path,
        typer.Option(help="风险事件发生记录 YAML 文件或目录路径，用于政策/地缘评分。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    risk_event_occurrence_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件发生记录报告输出路径。"),
    ] = None,
    risk_event_openai_precheck: Annotated[
        bool,
        typer.Option(
            "--risk-event-openai-precheck/--skip-risk-event-openai-precheck",
            help=(
                "默认在日报评分前抓取官方政策/地缘来源并调用 OpenAI 风险事件预审；"
                "输出只进入 llm_extracted / pending_review 队列。"
            ),
        ),
    ] = True,
    risk_event_prereview_queue_path: Annotated[
        Path,
        typer.Option(help="风险事件 OpenAI 预审待复核队列 JSON 输出路径。"),
    ] = DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH,
    risk_event_openai_precheck_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件 OpenAI 自动预审报告输出路径。"),
    ] = None,
    risk_event_openai_precheck_visibility_cutoff: Annotated[
        str | None,
        typer.Option(
            "--risk-event-openai-precheck-visibility-cutoff",
            help=(
                "生产日报 OpenAI request timestamp 可见时间上限，ISO datetime；"
                "仅允许用于最新已完成美股交易日。"
            ),
        ),
    ] = None,
    llm_request_profiles_path: Annotated[
        Path,
        typer.Option(help="LLM request profile 配置路径。"),
    ] = DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
    llm_request_profile: Annotated[
        str,
        typer.Option(help="日报前 OpenAI 风险事件预审使用的 profile_id。"),
    ] = DEFAULT_RISK_EVENT_DAILY_PREREVIEW_PROFILE,
    risk_event_llm_formal_assessment: Annotated[
        bool | None,
        typer.Option(
            "--risk-event-llm-formal-assessment/--skip-risk-event-llm-formal-assessment",
            help=(
                "OpenAI 风险事件预审成功后，是否自动写入 LLM formal "
                "occurrence/attestation 作为政策/地缘正式评估输入。"
            ),
        ),
    ] = None,
    risk_event_llm_formal_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件 LLM formal assessment 报告输出路径。"),
    ] = None,
    risk_event_llm_formal_min_confidence: Annotated[
        float | None,
        typer.Option(
            help="覆盖 profile 中低于该 confidence 的 LLM 预审记录不写入正式 occurrence。"
        ),
    ] = None,
    risk_event_llm_formal_next_review_days: Annotated[
        int | None,
        typer.Option(help="覆盖 profile 中 LLM formal assessment 的下次复核间隔天数。"),
    ] = None,
    risk_event_llm_formal_overwrite: Annotated[
        bool | None,
        typer.Option(
            "--risk-event-llm-formal-overwrite/--no-risk-event-llm-formal-overwrite",
            help="覆盖 profile 中日报重复运行同一 as-of 时是否覆盖同名 LLM formal YAML。",
        ),
    ] = None,
    official_policy_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 官方政策/地缘来源自动抓取报告输出路径。"),
    ] = None,
    official_policy_source_ids: Annotated[
        str | None,
        typer.Option(help="日报前官方来源抓取 source_id 白名单，逗号分隔；为空抓取全部。"),
    ] = None,
    official_policy_limit: Annotated[
        int | None,
        typer.Option(help="覆盖 profile 中日报前官方来源抓取每个可分页来源最多请求的记录数。"),
    ] = None,
    risk_event_openai_precheck_max_candidates: Annotated[
        int | None,
        typer.Option(help="覆盖 profile 中日报前 OpenAI 风险事件预审最多处理的官方候选数。"),
    ] = None,
    openai_api_key_env: Annotated[
        str,
        typer.Option(help="日报前风险事件 OpenAI 预审读取 API key 的环境变量名。"),
    ] = "OPENAI_API_KEY",
    openai_model: Annotated[
        str | None,
        typer.Option(help="覆盖 profile 中日报前风险事件 OpenAI 预审使用的 Responses API 模型。"),
    ] = None,
    openai_reasoning_effort: Annotated[
        str | None,
        typer.Option(help="覆盖 profile 中日报前风险事件 OpenAI 预审 reasoning.effort。"),
    ] = None,
    openai_timeout_seconds: Annotated[
        float | None,
        typer.Option(
            help="覆盖 profile 中日报前风险事件 OpenAI 预审 Responses API 请求读超时秒数。"
        ),
    ] = None,
    openai_http_client: Annotated[
        str | None,
        typer.Option(
            help="覆盖 profile 中日报前风险事件 OpenAI 预审 HTTP 客户端：requests 或 urllib。"
        ),
    ] = None,
    openai_cache_dir: Annotated[
        Path,
        typer.Option(help="日报前 OpenAI 请求/响应本地缓存与审计归档目录。"),
    ] = DEFAULT_OPENAI_REQUEST_CACHE_PATH,
    openai_cache_ttl_hours: Annotated[
        float | None,
        typer.Option(help="覆盖 profile 中完全相同日报前 OpenAI 请求的本地缓存复用时长。"),
    ] = None,
    catalyst_calendar_path: Annotated[
        Path,
        typer.Option(help="未来催化剂日历 YAML 路径，用于日报告警摘要。"),
    ] = DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
    valuation_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径，用于写入日报复核摘要。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "valuation_snapshots",
    trades_path: Annotated[
        Path,
        typer.Option(help="交易记录 YAML 文件或目录路径，用于写入日报复盘摘要。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "trades",
    review_benchmarks: Annotated[
        str,
        typer.Option(help="逗号分隔的日报交易复盘归因基准标的。"),
    ] = ",".join(DEFAULT_BENCHMARK_TICKERS),
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验并评分配置中的完整 AI 产业链标的。",
        ),
    ] = False,
) -> None:
    """构建特征并生成每日市场评分报告。"""
    if ctx is not None and ctx.invoked_subcommand is not None:
        return
    universe = load_universe()
    industry_chain = load_industry_chain()
    watchlist = load_watchlist()
    data_quality_config = load_data_quality()
    feature_config = load_features()
    scoring_rules = load_scoring_rules()
    portfolio = load_portfolio()
    market_regimes = load_market_regimes(DEFAULT_MARKET_REGIMES_CONFIG_PATH)
    default_market_regime = market_regime_by_id(
        market_regimes,
        market_regimes.default_backtest_regime,
    )
    score_started_at = datetime.now(tz=UTC)
    score_date = _parse_date(as_of) if as_of else resolve_daily_ops_default_as_of(score_started_at)
    production_score_date = resolve_daily_ops_default_as_of(score_started_at)
    explicit_risk_event_precheck_visibility_cutoff = (
        _parse_datetime(risk_event_openai_precheck_visibility_cutoff)
        if risk_event_openai_precheck_visibility_cutoff
        else None
    )
    if (
        explicit_risk_event_precheck_visibility_cutoff is not None
        and score_date != production_score_date
    ):
        raise typer.BadParameter(
            "risk_event_openai_precheck_visibility_cutoff 只能用于最新已完成美股交易日；"
            "历史 as-of 的 OpenAI request timestamp 仍按 as_of 当日 UTC 末尾 fail closed。"
        )
    benchmark_tickers = tuple(_parse_csv_items(review_benchmarks))
    if not benchmark_tickers:
        raise typer.BadParameter("日报交易复盘至少需要一个归因基准标的。")
    if prediction_production_effect not in {"production", "none"}:
        raise typer.BadParameter("prediction_production_effect 只能是 production 或 none。")
    if prediction_candidate_id != "production" and prediction_production_effect == "production":
        raise typer.BadParameter(
            "非 production 候选模型必须使用 prediction_production_effect=none。"
        )
    rule_governance_report = validate_rule_card_store(
        load_rule_card_store(rule_cards_path),
        as_of=date.today(),
    )
    if not rule_governance_report.passed:
        console.print("[red]规则治理校验失败，已停止日报评分。[/red]")
        console.print(
            f"错误数：{rule_governance_report.error_count}；"
            f"警告数：{rule_governance_report.warning_count}"
        )
        raise typer.Exit(code=1)
    daily_rule_version_manifest = build_rule_version_manifest(
        rule_governance_report,
        applies_to="score-daily",
    )
    if daily_rule_version_manifest["production_rule_count"] == 0:
        console.print(
            "[red]未找到适用于 score-daily 的 production rule card，已停止日报评分。[/red]"
        )
        raise typer.Exit(code=1)
    expected_price_tickers = configured_price_tickers(
        universe,
        include_full_ai_chain=full_universe,
    )
    expected_price_tickers = list(dict.fromkeys([*expected_price_tickers, *benchmark_tickers]))
    expected_rate_series = configured_rate_series(universe)
    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        score_date,
    )
    feature_report_output = feature_report_path or default_feature_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        score_date,
    )
    feature_availability_output = (
        feature_availability_report_path
        or default_feature_availability_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    score_report_output = report_path or default_daily_score_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        score_date,
    )
    alert_report_output = alert_report_path or default_alert_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        score_date,
    )
    portfolio_exposure_output = (
        portfolio_exposure_report_path
        or default_portfolio_exposure_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    daily_trace_output = trace_bundle_path or default_report_trace_bundle_path(score_report_output)
    decision_snapshot_output = decision_snapshot_path or default_decision_snapshot_path(
        DEFAULT_DECISION_SNAPSHOT_DIR,
        score_date,
    )
    calculation_explainers_output = (
        calculation_explainers_path
        or default_calculation_explainers_path(
            score_report_output.parent,
            score_date,
        )
    )
    belief_state_output = belief_state_path or default_belief_state_path(
        DEFAULT_BELIEF_STATE_DIR,
        score_date,
    )
    belief_state_history_output = belief_state_history_path or DEFAULT_BELIEF_STATE_HISTORY_PATH
    sec_fundamentals_input = sec_fundamentals_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        score_date,
    )
    sec_metrics_validation_output = (
        sec_metrics_validation_report_path
        or default_sec_fundamental_metrics_validation_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    sec_fundamental_features_output = (
        sec_fundamental_features_path
        or default_sec_fundamental_features_csv_path(
            PROJECT_ROOT / "data" / "processed",
            score_date,
        )
    )
    sec_fundamental_feature_report_output = (
        sec_fundamental_feature_report_path
        or default_sec_fundamental_features_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    risk_event_occurrence_report_output = (
        risk_event_occurrence_report_path
        or default_risk_event_occurrence_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    execution_policy_report_output = (
        execution_policy_report_path
        or default_execution_policy_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    official_policy_report_output = (
        official_policy_report_path
        or default_official_policy_fetch_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    risk_event_openai_precheck_report_output = (
        risk_event_openai_precheck_report_path
        or default_risk_event_openai_prereview_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )
    risk_event_llm_formal_report_output = (
        risk_event_llm_formal_report_path
        or default_llm_formal_assessment_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            score_date,
        )
    )

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_price_tickers,
        expected_rate_series=expected_rate_series,
        quality_config=data_quality_config,
        as_of=score_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止每日评分。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    execution_policy_report = validate_execution_policy(
        load_execution_policy(execution_policy_path),
        as_of=score_date,
    )
    write_execution_policy_report(
        execution_policy_report,
        execution_policy_report_output,
    )
    if not execution_policy_report.passed:
        console.print("[red]执行政策校验失败，已停止每日评分。[/red]")
        console.print(f"执行政策报告：{execution_policy_report_output}")
        console.print(
            f"错误数：{execution_policy_report.error_count}；"
            f"警告数：{execution_policy_report.warning_count}"
        )
        raise typer.Exit(code=1)
    if execution_policy_report.store.policy is None:
        console.print("[red]执行政策为空，已停止每日评分。[/red]")
        raise typer.Exit(code=1)

    prices_frame = pd.read_csv(prices_path)
    rates_frame = pd.read_csv(rates_path)
    fundamental_source_id = (
        "sec_edgar_reconstructed_pit_features"
        if sec_fundamental_source == "sec_pit_feature_panel"
        else "sec_fundamental_features"
    )
    feature_source_checks = list(
        _market_feature_source_checks(
            prices_frame=prices_frame,
            rates_frame=rates_frame,
            prices_path=prices_path,
            rates_path=rates_path,
            decision_time=score_date,
        )
    )
    if sec_fundamental_source == "sec_pit_feature_panel":
        feature_source_checks.append(
            _sec_pit_feature_source_check(
                feature_panel_path=sec_pit_feature_panel_path,
                decision_time=score_date,
            )
        )
    feature_set = build_market_features(
        prices=prices_frame,
        rates=rates_frame,
        config=feature_config,
        as_of=score_date,
        core_watchlist=universe.ai_chain.get("core_watchlist", []),
    )
    feature_availability_report = build_feature_availability_report(
        input_path=feature_availability_path,
        as_of=score_date,
        observed_sources=tuple(sorted({row.source for row in feature_set.rows})),
        required_sources=(
            "prices_daily",
            "rates_daily",
            fundamental_source_id,
            "valuation_snapshots",
            "risk_event_occurrences",
        ),
        source_checks=tuple(feature_source_checks),
    )
    write_feature_availability_report(
        feature_availability_report,
        feature_availability_output,
    )
    if not feature_availability_report.passed:
        console.print("[red]PIT 特征可见时间校验失败，已停止每日评分。[/red]")
        console.print(f"PIT 特征可见时间报告：{feature_availability_output}")
        console.print(
            f"错误数：{feature_availability_report.error_count}；"
            f"警告数：{feature_availability_report.warning_count}"
        )
        raise typer.Exit(code=1)
    feature_availability_section = render_feature_availability_section(
        feature_availability_report,
        feature_availability_output,
    )
    features_output = write_features_csv(feature_set, features_path)
    feature_summary_output = write_feature_summary(
        feature_set,
        data_quality_report=data_quality_report,
        data_quality_report_path=quality_output,
        features_path=features_output,
        output_path=feature_report_output,
        feature_availability_section=feature_availability_section,
    )
    portfolio_exposure_report = build_portfolio_exposure_report(
        input_path=portfolio_positions_path,
        as_of=score_date,
        industry_chain=industry_chain,
        watchlist=watchlist,
    )
    write_portfolio_exposure_report(
        portfolio_exposure_report,
        portfolio_exposure_output,
    )
    if not portfolio_exposure_report.passed:
        console.print("[red]组合持仓输入校验失败，已停止每日评分。[/red]")
        console.print(f"组合暴露报告：{portfolio_exposure_output}")
        console.print(
            f"错误数：{portfolio_exposure_report.error_count}；"
            f"警告数：{portfolio_exposure_report.warning_count}"
        )
        raise typer.Exit(code=1)
    sec_companies = load_sec_companies(sec_companies_path)
    sec_metrics = load_fundamental_metrics(sec_metrics_path)
    if sec_fundamental_source == "sec_pit_feature_panel":
        sec_fundamental_feature_report = _build_score_daily_sec_pit_feature_report(
            score_date=score_date,
            sec_companies=sec_companies,
            sec_pit_feature_panel_path=sec_pit_feature_panel_path,
        )
        sec_metrics_validation_reference = sec_pit_feature_panel_path
    else:
        if tsm_ir_merge:
            _merge_tsm_ir_for_daily_score(
                sec_fundamentals_path=sec_fundamentals_input,
                tsm_ir_path=tsm_ir_quarterly_metrics_path,
                sec_companies=sec_companies,
                as_of=score_date,
            )
        sec_metrics_validation_report = validate_sec_fundamental_metrics_csv(
            companies=sec_companies,
            metrics=sec_metrics,
            input_path=sec_fundamentals_input,
            as_of=score_date,
        )
        write_sec_fundamental_metrics_validation_report(
            sec_metrics_validation_report,
            sec_metrics_validation_output,
        )
        if not sec_metrics_validation_report.passed:
            console.print("[red]SEC 基本面指标 CSV 校验失败，已停止每日评分。[/red]")
            console.print(f"SEC 指标 CSV 校验报告：{sec_metrics_validation_output}")
            console.print(f"SEC 指标 CSV：{sec_fundamentals_input}")
            console.print(
                f"错误数：{sec_metrics_validation_report.error_count}；"
                f"警告数：{sec_metrics_validation_report.warning_count}"
            )
            raise typer.Exit(code=1)

        sec_fundamental_feature_report = build_sec_fundamental_features_report(
            companies=sec_companies,
            feature_config=load_fundamental_features(fundamental_feature_config_path),
            input_path=sec_fundamentals_input,
            as_of=score_date,
            validation_report=sec_metrics_validation_report,
        )
        sec_metrics_validation_reference = sec_metrics_validation_output
    if not sec_fundamental_feature_report.passed:
        write_sec_fundamental_features_report(
            report=sec_fundamental_feature_report,
            validation_report_path=sec_metrics_validation_reference,
            output_csv_path=sec_fundamental_features_output,
            output_path=sec_fundamental_feature_report_output,
        )
        console.print("[red]SEC 基本面特征构建失败，已停止每日评分。[/red]")
        console.print(f"SEC 基本面特征报告：{sec_fundamental_feature_report_output}")
        console.print(f"SEC 指标 CSV 校验报告：{sec_metrics_validation_output}")
        console.print(
            f"错误数：{sec_fundamental_feature_report.error_count}；"
            f"警告数：{sec_fundamental_feature_report.warning_count}"
        )
        raise typer.Exit(code=1)

    sec_fundamental_features_output = write_sec_fundamental_features_csv(
        sec_fundamental_feature_report,
        sec_fundamental_features_output,
    )
    sec_fundamental_feature_report_output = write_sec_fundamental_features_report(
        report=sec_fundamental_feature_report,
        validation_report_path=sec_metrics_validation_reference,
        output_csv_path=sec_fundamental_features_output,
        output_path=sec_fundamental_feature_report_output,
    )
    valuation_validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(valuation_path),
        universe=universe,
        watchlist=watchlist,
        as_of=score_date,
    )
    valuation_review_report = build_valuation_review_report(valuation_validation_report)
    if not valuation_validation_report.passed:
        console.print("[red]估值快照校验失败，已停止每日评分。[/red]")
        console.print(
            f"错误数：{valuation_validation_report.error_count}；"
            f"警告数：{valuation_validation_report.warning_count}"
        )
        raise typer.Exit(code=1)
    risk_events_config = load_risk_events(risk_events_path)
    risk_events_validation_report = validate_risk_events_config(
        risk_events=risk_events_config,
        industry_chain=industry_chain,
        watchlist=watchlist,
        universe=universe,
        as_of=score_date,
    )
    if not risk_events_validation_report.passed:
        console.print("[red]风险事件规则校验失败，已停止每日评分。[/red]")
        console.print(
            f"错误数：{risk_events_validation_report.error_count}；"
            f"警告数：{risk_events_validation_report.warning_count}"
        )
        raise typer.Exit(code=1)
    official_policy_fetch_report = None
    risk_event_prereview_report = None
    llm_formal_report = None
    if risk_event_openai_precheck:
        llm_profile = _load_llm_request_profile(
            llm_request_profiles_path,
            llm_request_profile,
        )
        effective_openai_model = _coalesce_profile_value(openai_model, llm_profile.model)
        effective_openai_reasoning_effort = _coalesce_profile_value(
            openai_reasoning_effort,
            llm_profile.reasoning_effort,
        )
        effective_openai_timeout_seconds = _coalesce_profile_value(
            openai_timeout_seconds,
            llm_profile.timeout_seconds,
        )
        effective_openai_http_client = _coalesce_profile_value(
            openai_http_client,
            llm_profile.http_client,
        )
        effective_openai_cache_ttl_hours = _coalesce_profile_value(
            openai_cache_ttl_hours,
            llm_profile.cache_ttl_hours,
        )
        effective_risk_event_openai_precheck_max_candidates = _coalesce_profile_value(
            risk_event_openai_precheck_max_candidates,
            llm_profile.max_candidates,
        )
        effective_official_policy_limit = _coalesce_profile_value(
            official_policy_limit,
            llm_profile.official_policy_limit,
        )
        effective_risk_event_llm_formal_assessment = _coalesce_profile_value(
            risk_event_llm_formal_assessment,
            llm_profile.formal_assessment.enabled,
        )
        effective_risk_event_llm_formal_min_confidence = _coalesce_profile_value(
            risk_event_llm_formal_min_confidence,
            llm_profile.formal_assessment.min_confidence,
        )
        effective_risk_event_llm_formal_next_review_days = _coalesce_profile_value(
            risk_event_llm_formal_next_review_days,
            llm_profile.formal_assessment.next_review_days,
        )
        effective_risk_event_llm_formal_overwrite = _coalesce_profile_value(
            risk_event_llm_formal_overwrite,
            llm_profile.formal_assessment.overwrite,
        )
        if effective_risk_event_openai_precheck_max_candidates is None:
            raise typer.BadParameter("LLM request profile 必须设置 max_candidates，或显式传入。")
        if effective_official_policy_limit is None:
            raise typer.BadParameter(
                "LLM request profile 必须设置 official_policy_limit，或显式传入。"
            )
        if effective_risk_event_openai_precheck_max_candidates < 0:
            raise typer.BadParameter("OpenAI 风险事件预审候选上限不能为负数。")
        if effective_openai_timeout_seconds <= 0:
            raise typer.BadParameter("OpenAI 风险事件预审超时秒数必须为正数。")
        if effective_openai_cache_ttl_hours <= 0:
            raise typer.BadParameter("OpenAI 请求缓存 TTL 小时数必须为正数。")
        if (
            effective_risk_event_llm_formal_min_confidence < 0
            or effective_risk_event_llm_formal_min_confidence > 1
        ):
            raise typer.BadParameter("LLM formal min confidence 必须在 0 到 1 之间。")
        if effective_risk_event_llm_formal_next_review_days < 0:
            raise typer.BadParameter("LLM formal next review days 不能为负数。")
        if not os.getenv(openai_api_key_env, ""):
            console.print("[red]缺少 OpenAI API key，已停止日报前风险事件预审。[/red]")
            console.print(f"需要环境变量：{openai_api_key_env}")
            raise typer.Exit(code=1)
        selected_official_source_ids = (
            _parse_csv_items(official_policy_source_ids) if official_policy_source_ids else None
        )
        official_policy_fetch_report = fetch_official_policy_sources(
            as_of=score_date,
            since=None,
            raw_dir=DEFAULT_OFFICIAL_POLICY_RAW_DIR,
            processed_dir=DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
            api_keys={
                "CONGRESS_API_KEY": os.getenv("CONGRESS_API_KEY", ""),
                "GOVINFO_API_KEY": os.getenv("GOVINFO_API_KEY", ""),
            },
            selected_source_ids=selected_official_source_ids,
            limit=effective_official_policy_limit,
            download_manifest_path=PROJECT_ROOT / "data" / "raw" / "download_manifest.csv",
        )
        write_official_policy_fetch_report(
            official_policy_fetch_report,
            official_policy_report_output,
        )
        if not official_policy_fetch_report.passed:
            console.print("[red]官方政策/地缘来源抓取失败，已停止每日评分。[/red]")
            console.print(f"官方来源抓取报告：{official_policy_report_output}")
            console.print(
                f"错误数：{official_policy_fetch_report.error_count}；"
                f"警告数：{official_policy_fetch_report.warning_count}"
            )
            raise typer.Exit(code=1)
        official_candidates_path = default_official_policy_candidates_path(
            DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
            score_date,
        )
        risk_event_prereview_report = run_openai_risk_event_prereview_for_official_candidates(
            official_policy_fetch_report.candidates,
            api_key=os.getenv(openai_api_key_env, ""),
            data_sources=load_data_sources(DEFAULT_DATA_SOURCES_CONFIG_PATH),
            risk_events=risk_events_config,
            input_path=official_candidates_path,
            as_of=score_date,
            model=effective_openai_model,
            reasoning_effort=effective_openai_reasoning_effort,
            endpoint=llm_profile.endpoint,
            timeout_seconds=effective_openai_timeout_seconds,
            http_client=effective_openai_http_client,
            openai_cache_dir=openai_cache_dir,
            openai_cache_ttl_seconds=effective_openai_cache_ttl_hours * 3600,
            max_retries=llm_profile.max_retries,
            generated_at=explicit_risk_event_precheck_visibility_cutoff,
            request_visibility_cutoff=explicit_risk_event_precheck_visibility_cutoff,
            max_candidates=effective_risk_event_openai_precheck_max_candidates,
        )
        write_risk_event_prereview_import_report(
            risk_event_prereview_report,
            risk_event_openai_precheck_report_output,
        )
        if not risk_event_prereview_report.passed:
            console.print("[red]风险事件 OpenAI 自动预审失败，已停止每日评分。[/red]")
            console.print(f"OpenAI 预审报告：{risk_event_openai_precheck_report_output}")
            console.print(
                f"错误数：{risk_event_prereview_report.error_count}；"
                f"警告数：{risk_event_prereview_report.warning_count}"
            )
            raise typer.Exit(code=1)
        write_risk_event_prereview_queue(
            risk_event_prereview_report,
            risk_event_prereview_queue_path,
        )
        console.print(
            "[green]日报前风险事件 OpenAI 自动预审完成。[/green] "
            f"官方候选 {official_policy_fetch_report.candidate_count}；"
            f"待复核队列 {risk_event_prereview_report.record_count}；"
            f"L2/L3 候选 {risk_event_prereview_report.high_level_candidate_count}"
        )
        console.print(f"官方来源抓取报告：{official_policy_report_output}")
        console.print(f"OpenAI 预审报告：{risk_event_openai_precheck_report_output}")
        console.print(f"预审待复核队列：{risk_event_prereview_queue_path}")
        console.print(
            f"LLM request profile：{llm_profile.profile_id}；"
            f"model={effective_openai_model}；"
            f"reasoning={effective_openai_reasoning_effort}；"
            f"max_candidates={effective_risk_event_openai_precheck_max_candidates}"
        )
        if effective_risk_event_llm_formal_assessment:
            try:
                llm_formal_report = build_llm_formal_assessment_report(
                    risk_event_prereview_queue_path,
                    as_of=score_date,
                    risk_events=risk_events_config,
                    include_attestation=True,
                    next_review_days=effective_risk_event_llm_formal_next_review_days,
                    min_confidence=effective_risk_event_llm_formal_min_confidence,
                )
                write_llm_formal_assessment_report(
                    llm_formal_report,
                    risk_event_llm_formal_report_output,
                )
                if not llm_formal_report.passed:
                    console.print(
                        "[red]风险事件 LLM formal assessment 失败，已停止每日评分。[/red]"
                    )
                    console.print(f"LLM formal 报告：{risk_event_llm_formal_report_output}")
                    console.print(
                        f"错误数：{llm_formal_report.error_count}；"
                        f"警告数：{llm_formal_report.warning_count}"
                    )
                    raise typer.Exit(code=1)
                written_llm_formal_paths = write_llm_formal_assessment_outputs(
                    llm_formal_report,
                    risk_event_occurrences_path,
                    overwrite=effective_risk_event_llm_formal_overwrite,
                )
            except (OSError, ValueError, FileExistsError) as exc:
                console.print(f"[red]风险事件 LLM formal assessment 写入失败：{exc}[/red]")
                raise typer.Exit(code=1) from exc
            console.print(
                "[green]风险事件 LLM formal assessment 已写入。[/green] "
                f"occurrence={llm_formal_report.occurrence_count}；"
                f"attestation={'是' if llm_formal_report.attestation else '否'}；"
                f"YAML={len(written_llm_formal_paths)}"
            )
            console.print(f"LLM formal 报告：{risk_event_llm_formal_report_output}")
    risk_event_occurrence_validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(risk_event_occurrences_path),
        risk_events=risk_events_config,
        as_of=score_date,
    )
    risk_event_occurrence_review_report = build_risk_event_occurrence_review_report(
        risk_event_occurrence_validation_report
    )
    write_risk_event_occurrence_review_report(
        risk_event_occurrence_review_report,
        risk_event_occurrence_report_output,
    )
    if not risk_event_occurrence_validation_report.passed:
        console.print("[red]风险事件发生记录校验失败，已停止每日评分。[/red]")
        console.print(f"风险事件发生记录报告：{risk_event_occurrence_report_output}")
        console.print(
            f"错误数：{risk_event_occurrence_validation_report.error_count}；"
            f"警告数：{risk_event_occurrence_validation_report.warning_count}"
        )
        raise typer.Exit(code=1)
    catalyst_calendar_report = validate_catalyst_calendar(
        load_catalyst_calendar(catalyst_calendar_path),
        as_of=score_date,
        industry_chain=industry_chain,
        watchlist=watchlist,
        risk_events=risk_events_config,
    )
    thesis_review_report = _build_daily_thesis_review_report(
        input_path=thesis_path,
        watchlist=watchlist,
        industry_chain=industry_chain,
        score_date=score_date,
    )

    review_summary = _build_daily_review_summary(
        thesis_path=thesis_path,
        thesis_review_report=thesis_review_report,
        risk_events_path=risk_events_path,
        risk_event_occurrences_path=risk_event_occurrences_path,
        risk_events_validation_report=risk_events_validation_report,
        risk_event_occurrence_review_report=risk_event_occurrence_review_report,
        valuation_path=valuation_path,
        valuation_review_report=valuation_review_report,
        trades_path=trades_path,
        score_date=score_date,
        universe=universe,
        industry_chain=industry_chain,
        watchlist=watchlist,
        prices=prices_frame,
        data_quality_report=data_quality_report,
        benchmark_tickers=benchmark_tickers,
    )
    core_watchlist_tickers = tuple(universe.ai_chain.get("core_watchlist", []))
    focus_stock_trend_report = build_focus_stock_trend_report(
        feature_set=feature_set,
        tickers=core_watchlist_tickers,
        watchlist=watchlist,
    )
    focus_stock_trend_section = render_focus_stock_trend_section(focus_stock_trend_report)
    industry_node_heat_report = build_industry_node_heat_report(
        industry_chain=industry_chain,
        watchlist=watchlist,
        feature_set=feature_set,
        fundamental_feature_report=sec_fundamental_feature_report,
        valuation_review_report=valuation_review_report,
        risk_event_occurrence_review_report=risk_event_occurrence_review_report,
        thesis_review_report=thesis_review_report,
    )
    industry_node_heat_section = render_industry_node_heat_section(industry_node_heat_report)
    weight_calibration_context = build_weight_calibration_context(
        feature_set=feature_set,
        data_quality_report=data_quality_report,
        fundamental_feature_report=sec_fundamental_feature_report,
        valuation_review_report=valuation_review_report,
        risk_event_occurrence_review_report=risk_event_occurrence_review_report,
        run_type="score_daily",
        market_regime_id=default_market_regime.regime_id,
    )
    weight_calibration_application = resolve_calibration_application(
        context=weight_calibration_context,
        as_of=score_date,
    )
    current_context_output: Path | None = None
    current_effective_weights_output: Path | None = None
    if report_path is None:
        current_context_output = write_calibration_context(
            weight_calibration_context,
            DEFAULT_CURRENT_CONTEXT_PATH,
        )
        current_effective_weights_output = write_effective_weights(
            weight_calibration_application,
            DEFAULT_EFFECTIVE_WEIGHTS_PATH,
        )
    score_report = build_daily_score_report(
        feature_set=feature_set,
        data_quality_report=data_quality_report,
        rules=scoring_rules,
        total_risk_asset_min=portfolio.portfolio.total_risk_asset_min,
        total_risk_asset_max=portfolio.portfolio.total_risk_asset_max,
        max_total_ai_exposure=portfolio.position_limits.max_total_ai_exposure,
        macro_risk_asset_budget=portfolio.macro_risk_asset_budget,
        risk_budget=portfolio.risk_budget,
        portfolio_exposure_report=portfolio_exposure_report,
        review_summary=review_summary,
        fundamental_feature_report=sec_fundamental_feature_report,
        valuation_review_report=valuation_review_report,
        risk_event_occurrence_review_report=risk_event_occurrence_review_report,
        weight_calibration=weight_calibration_application,
    )
    previous_score_snapshot = load_previous_daily_score_snapshot(scores_path, score_date)
    daily_alert_report = build_daily_alert_report(
        score_report,
        previous_score_snapshot=previous_score_snapshot,
        catalyst_calendar_report=catalyst_calendar_report,
        data_quality_report_path=quality_output,
        risk_event_occurrence_report_path=risk_event_occurrence_report_output,
        valuation_report_path=valuation_path,
        catalyst_calendar_path=catalyst_calendar_path,
    )
    daily_alert_output = write_alert_report(daily_alert_report, alert_report_output)
    previous_execution_band = None
    if (
        previous_score_snapshot is not None
        and previous_score_snapshot.final_risk_asset_ai_min is not None
        and previous_score_snapshot.final_risk_asset_ai_max is not None
    ):
        previous_execution_band = (
            previous_score_snapshot.final_risk_asset_ai_min,
            previous_score_snapshot.final_risk_asset_ai_max,
        )
    execution_advisory = build_execution_advisory(
        policy=execution_policy_report.store.policy,
        current_band=(
            score_report.recommendation.risk_asset_ai_band.min_position,
            score_report.recommendation.risk_asset_ai_band.max_position,
        ),
        previous_band=previous_execution_band,
        confidence_level=score_report.confidence_assessment.level,
        triggered_gate_ids=tuple(
            gate.gate_id for gate in score_report.recommendation.triggered_position_gates
        ),
        report_status=score_report.status,
    )
    execution_advisory_section = render_execution_advisory_section(
        execution_advisory,
        validation_status=execution_policy_report.status,
        validation_report_path=execution_policy_report_output,
    )
    scores_output = write_scores_csv(score_report, scores_path)
    daily_market_regime = BacktestRegimeContext(
        regime_id=default_market_regime.regime_id,
        name=default_market_regime.name,
        start_date=default_market_regime.start_date,
        anchor_date=default_market_regime.anchor_date,
        anchor_event=default_market_regime.anchor_event,
        description=default_market_regime.description,
    )
    daily_config_paths = daily_trace_config_paths(
        sec_companies_path=sec_companies_path,
        sec_metrics_path=sec_metrics_path,
        fundamental_feature_config_path=fundamental_feature_config_path,
        risk_events_path=risk_events_path,
        execution_policy_path=execution_policy_path,
        rule_cards_path=rule_cards_path,
        feature_availability_path=feature_availability_path,
    )
    daily_trace_bundle = build_daily_score_trace_bundle(
        report=score_report,
        report_path=score_report_output,
        data_quality_report_path=quality_output,
        feature_report_path=feature_summary_output,
        features_path=features_output,
        scores_path=scores_output,
        market_regime=daily_market_regime,
        config_paths=daily_config_paths,
        rule_version_manifest=daily_rule_version_manifest,
        sec_metrics_validation_report_path=sec_metrics_validation_output,
        sec_fundamental_feature_report_path=sec_fundamental_feature_report_output,
        sec_fundamental_features_path=sec_fundamental_features_output,
        risk_event_occurrence_report_path=risk_event_occurrence_report_output,
        belief_state_path=belief_state_output,
        feature_availability_report_path=feature_availability_output,
        feature_availability_summary=feature_availability_summary_record(
            feature_availability_report,
            feature_availability_output,
        ),
        focus_stock_trend_tickers=core_watchlist_tickers,
        run_id=run_id,
    )
    daily_trace_output = write_trace_bundle(daily_trace_bundle, daily_trace_output)
    belief_state = build_belief_state(
        report=score_report,
        trace_bundle_path=daily_trace_output,
        decision_snapshot_path=decision_snapshot_output,
        market_regime=daily_market_regime,
        config_paths=daily_config_paths,
    )
    belief_state_output = write_belief_state(belief_state, belief_state_output)
    belief_state_history_output = append_belief_state_history(
        belief_state,
        belief_state_output,
        belief_state_history_output,
    )
    decision_snapshot = build_decision_snapshot(
        report=score_report,
        trace_bundle_path=daily_trace_output,
        market_regime=daily_market_regime,
        config_paths=daily_config_paths,
        belief_state_path=belief_state_output,
        rule_version_manifest=daily_rule_version_manifest,
    )
    daily_decision_snapshot_output = write_decision_snapshot(
        decision_snapshot,
        decision_snapshot_output,
    )
    calculation_explainers_payload = build_calculation_explainers_payload(
        as_of=score_date,
        decision_snapshot_path=daily_decision_snapshot_output,
        scores_daily_path=scores_output,
    )
    calculation_explainers_output = write_calculation_explainers_json(
        calculation_explainers_payload,
        calculation_explainers_output,
    )
    prediction_ledger_output = append_prediction_records(
        (
            build_prediction_record_from_decision_snapshot(
                snapshot=decision_snapshot,
                trace_bundle=daily_trace_bundle.to_dict(),
                trace_bundle_path=daily_trace_output,
                features_path=features_output,
                data_quality_report_path=quality_output,
                candidate_id=prediction_candidate_id,
                production_effect=prediction_production_effect,
            ),
        ),
        prediction_ledger_path,
    )
    daily_report_output = write_daily_score_report(
        score_report,
        data_quality_report_path=quality_output,
        feature_report_path=feature_summary_output,
        features_path=features_output,
        scores_path=scores_output,
        output_path=score_report_output,
        sec_metrics_validation_report_path=sec_metrics_validation_output,
        sec_fundamental_feature_report_path=sec_fundamental_feature_report_output,
        sec_fundamental_features_path=sec_fundamental_features_output,
        risk_event_occurrence_report_path=risk_event_occurrence_report_output,
        feature_availability_section=feature_availability_section,
        previous_score_snapshot=previous_score_snapshot,
        belief_state_section=render_belief_state_summary(
            belief_state,
            belief_state_output,
        ),
        execution_action_label=execution_advisory.label,
        execution_action_id=execution_advisory.action_id,
        focus_stock_trend_section=focus_stock_trend_section,
        industry_node_heat_section=industry_node_heat_section,
        execution_advisory_section=execution_advisory_section,
        portfolio_exposure_section=(
            render_portfolio_exposure_section(portfolio_exposure_report).rstrip()
            + f"\n- 独立报告：`{portfolio_exposure_output}`"
        ),
        alert_summary_section=render_alert_summary_section(
            daily_alert_report,
            report_path=daily_alert_output,
        ),
        risk_event_openai_precheck_section=(
            _risk_event_openai_precheck_daily_section(
                official_policy_fetch_report=official_policy_fetch_report,
                risk_event_prereview_report=risk_event_prereview_report,
                official_policy_report_output=official_policy_report_output,
                risk_event_openai_precheck_report_output=(risk_event_openai_precheck_report_output),
                risk_event_prereview_queue_path=risk_event_prereview_queue_path,
                llm_formal_report=llm_formal_report,
                risk_event_llm_formal_report_output=(risk_event_llm_formal_report_output),
                llm_profile_id=llm_profile.profile_id,
                llm_formal_enabled=effective_risk_event_llm_formal_assessment,
                model=effective_openai_model,
                reasoning_effort=effective_openai_reasoning_effort,
                timeout_seconds=effective_openai_timeout_seconds,
                http_client=effective_openai_http_client,
                cache_dir=openai_cache_dir,
                cache_ttl_hours=effective_openai_cache_ttl_hours,
                max_candidates=effective_risk_event_openai_precheck_max_candidates,
                request_visibility_cutoff=explicit_risk_event_precheck_visibility_cutoff,
            )
            if risk_event_openai_precheck
            and official_policy_fetch_report is not None
            and risk_event_prereview_report is not None
            else None
        ),
        traceability_section=render_traceability_section(
            daily_trace_bundle,
            daily_trace_output,
        ),
        run_id=run_id or daily_trace_bundle.run_manifest["run_id"],
        trace_bundle_path=daily_trace_output,
    )

    status_style = "green" if score_report.status == "PASS" else "yellow"
    console.print(f"[{status_style}]每日评分状态：{score_report.status}[/{status_style}]")
    console.print(f"AI 产业链评分：{score_report.recommendation.total_score:.1f}")
    console.print(
        "判断置信度："
        f"{score_report.confidence_assessment.score:.1f}"
        f"（{score_report.confidence_assessment.level}）"
    )
    console.print(f"仓位状态：{score_report.recommendation.label}")
    console.print(f"执行建议：{execution_advisory.label}（{execution_advisory.action_id}）")
    console.print(
        f"产业链节点热度与健康度：{industry_node_heat_report.status}"
        f"（{industry_node_heat_report.node_count} 个节点）"
    )
    console.print(
        f"关注股票趋势分析：{focus_stock_trend_report.status}"
        f"（{focus_stock_trend_report.ticker_count} 个 ticker）"
    )
    console.print(
        f"组合暴露：{portfolio_exposure_report.status}"
        f"（AI 占比 {portfolio_exposure_report.ai_exposure_pct_total:.1%}）"
    )
    console.print(
        f"告警状态：{daily_alert_report.status}" f"（{len(daily_alert_report.alerts)} 条）"
    )
    console.print(f"每日评分报告：{daily_report_output}")
    console.print(f"告警报告：{daily_alert_output}")
    console.print(f"Evidence bundle：{daily_trace_output}")
    console.print(f"Decision snapshot：{daily_decision_snapshot_output}")
    console.print(f"Calculation explainers：{calculation_explainers_output}")
    if current_context_output is not None and current_effective_weights_output is not None:
        console.print(f"Current context：{current_context_output}")
        console.print(f"Current effective weights：{current_effective_weights_output}")
    console.print(f"Prediction ledger：{prediction_ledger_output}")
    console.print(f"Belief state：{belief_state_output}")
    console.print(f"Belief state history：{belief_state_history_output}")
    console.print(f"评分数据：{scores_output}")
    console.print(f"特征摘要：{feature_summary_output}")
    console.print(
        f"PIT 特征可见时间报告：{feature_availability_output}"
        f"（{feature_availability_report.status}）"
    )
    console.print(
        f"SEC 基本面特征：{sec_fundamental_features_output}"
        f"（{sec_fundamental_feature_report.status}）"
    )
    console.print(
        f"风险事件发生记录：{risk_event_occurrence_report_output}"
        f"（{risk_event_occurrence_review_report.status}）"
    )
    if risk_event_openai_precheck:
        console.print(f"官方政策/地缘抓取报告：{official_policy_report_output}")
        console.print(f"风险事件 OpenAI 预审报告：{risk_event_openai_precheck_report_output}")
        console.print(f"风险事件预审队列：{risk_event_prereview_queue_path}")
        console.print(f"LLM request profile：{llm_profile.profile_id}")
        if llm_formal_report is not None:
            console.print(f"风险事件 LLM formal 报告：{risk_event_llm_formal_report_output}")
    console.print(
        f"组合暴露报告：{portfolio_exposure_output}" f"（{portfolio_exposure_report.status}）"
    )
    console.print(
        f"执行政策报告：{execution_policy_report_output}" f"（{execution_policy_report.status}）"
    )
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")
    console.print(
        "规则版本："
        f"{daily_rule_version_manifest['production_rule_count']} 个 production rule cards"
        f"（{rule_governance_report.status}）"
    )


def _merge_tsm_ir_for_daily_score(
    *,
    sec_fundamentals_path: Path,
    tsm_ir_path: Path,
    sec_companies: object,
    as_of: date,
) -> None:
    tsm_ir_required = any(
        company.active
        and company.ticker.upper() == "TSM"
        and "quarterly" in company.sec_metric_periods
        for company in sec_companies.companies
    )
    if not tsm_ir_required or not tsm_ir_path.exists():
        return

    all_tsm_rows = load_tsm_ir_quarterly_metric_rows_csv(tsm_ir_path)
    selected_tsm_rows = select_tsm_ir_quarterly_metric_rows_as_of(all_tsm_rows, as_of)
    if not selected_tsm_rows:
        return

    try:
        existing_rows = load_sec_fundamental_metric_rows_csv(sec_fundamentals_path)
        merged_rows = merge_tsm_ir_quarterly_rows_into_sec_metrics_as_of(
            existing_rows=existing_rows,
            tsm_rows=all_tsm_rows,
            tsm_company=sec_companies,
            as_of=as_of,
        )
    except ValueError as exc:
        console.print("[red]TSMC IR 日报基本面合并失败，已停止每日评分。[/red]")
        console.print(str(exc))
        raise typer.Exit(code=1) from exc

    write_sec_fundamental_metric_rows_csv(merged_rows, sec_fundamentals_path)
    console.print(
        "TSMC IR 已合并到 SEC-style 基本面指标："
        f"{sec_fundamentals_path}（TSM 行数 {len(selected_tsm_rows)}）"
    )


def _risk_event_openai_precheck_daily_section(
    *,
    official_policy_fetch_report,
    risk_event_prereview_report,
    official_policy_report_output: Path,
    risk_event_openai_precheck_report_output: Path,
    risk_event_prereview_queue_path: Path,
    llm_formal_report,
    risk_event_llm_formal_report_output: Path,
    llm_profile_id: str,
    llm_formal_enabled: bool,
    model: str,
    reasoning_effort: str,
    timeout_seconds: float,
    http_client: str,
    cache_dir: Path,
    cache_ttl_hours: float,
    max_candidates: int,
    request_visibility_cutoff: datetime | None,
) -> str:
    as_of_text = (
        risk_event_prereview_report.as_of.isoformat()
        if risk_event_prereview_report.as_of is not None
        else "未记录"
    )
    visibility_cutoff_text = (
        request_visibility_cutoff.astimezone(UTC).isoformat()
        if request_visibility_cutoff is not None
        else "未显式传入，request_timestamp 按 as_of 当日 UTC 末尾 fail closed"
    )
    lines = [
        "## 日报前 OpenAI 风险事件预审",
        "",
        f"- 官方来源抓取状态：{official_policy_fetch_report.status}",
        f"- 官方 payload 数：{official_policy_fetch_report.payload_count}",
        f"- 官方候选数：{official_policy_fetch_report.candidate_count}",
        f"- OpenAI 预审状态：{risk_event_prereview_report.status}",
        f"- LLM request profile：{llm_profile_id}",
        f"- OpenAI 模型：{model}",
        f"- reasoning.effort：{reasoning_effort}",
        f"- 请求读超时：{timeout_seconds:g} 秒",
        f"- HTTP client：{http_client}",
        f"- OpenAI 请求缓存目录：`{cache_dir}`",
        f"- OpenAI 请求缓存 TTL：{cache_ttl_hours:g} 小时",
        f"- OpenAI 请求缓存命中：HIT={risk_event_prereview_report.openai_cache_hit_count} / "
        f"MISS={risk_event_prereview_report.openai_cache_miss_count} / "
        f"EXPIRED={risk_event_prereview_report.openai_cache_expired_count} / "
        f"DISABLED={risk_event_prereview_report.openai_cache_disabled_count}",
        (
            "- request timestamp 可见性："
            f"as_of={as_of_text}；visibility_cutoff={visibility_cutoff_text}"
        ),
        f"- 本次候选上限：{max_candidates}",
        f"- LLM claim 数：{risk_event_prereview_report.row_count}",
        f"- 待人工复核队列记录数：{risk_event_prereview_report.record_count}",
        f"- L2/L3 候选数：{risk_event_prereview_report.high_level_candidate_count}",
        f"- active 候选数：{risk_event_prereview_report.active_candidate_count}",
        f"- LLM formal 自动写入：{'是' if llm_formal_enabled else '否'}",
    ]
    if llm_formal_report is not None:
        lines.extend(
            [
                f"- LLM formal 状态：{llm_formal_report.status}",
                f"- LLM formal occurrence 数：{llm_formal_report.occurrence_count}",
                f"- LLM formal active/watch：{llm_formal_report.active_occurrence_count}/"
                f"{llm_formal_report.watch_occurrence_count}",
                f"- LLM formal attestation：{'是' if llm_formal_report.attestation else '否'}",
                f"- LLM formal 报告：`{risk_event_llm_formal_report_output}`",
            ]
        )
    if llm_formal_enabled:
        mode_line = (
            "- 复核模式：LLM formal trusted by owner；不伪装成人工复核，"
            "不进入 `execution_policy.manual_review_gate_ids`，不会单独把执行动作改成 "
            "`wait_manual_review`。"
        )
        boundary_line = (
            "- 边界：LLM formal 是正式评估输入，但不是人工复核；"
            "LLM formal evidence 默认最高 B 级，可进入普通评分但不能单独触发 "
            "position gate 或 thesis 状态。"
        )
    else:
        mode_line = (
            "- 复核模式：backlog-only；未确认 OpenAI 候选不进入 "
            "`execution_policy.manual_review_gate_ids`，不会单独把执行动作改成 "
            "`wait_manual_review`。"
        )
        boundary_line = (
            "- 边界：预审输出只作为 `llm_extracted / pending_review` 线索，"
            "不会写入 occurrence、复核声明、评分、仓位闸门或 thesis 状态；"
            "无人复核时保留为未确认 backlog。"
        )
    lines.extend(
        [
            mode_line,
            f"- 官方来源抓取报告：`{official_policy_report_output}`",
            f"- OpenAI 预审报告：`{risk_event_openai_precheck_report_output}`",
            f"- 预审待复核队列：`{risk_event_prereview_queue_path}`",
            boundary_line,
        ]
    )
    return "\n".join(lines)


def _build_daily_review_summary(
    thesis_path: Path,
    thesis_review_report: ThesisReviewReport,
    risk_events_path: Path,
    risk_event_occurrences_path: Path,
    risk_events_validation_report: RiskEventsValidationReport,
    risk_event_occurrence_review_report: RiskEventOccurrenceReviewReport,
    valuation_path: Path,
    valuation_review_report: ValuationReviewReport,
    trades_path: Path,
    score_date: date,
    universe: UniverseConfig,
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
    prices: pd.DataFrame,
    data_quality_report: DataQualityReport,
    benchmark_tickers: tuple[str, ...],
) -> DailyReviewSummary:
    return DailyReviewSummary(
        thesis=_build_daily_thesis_status(
            input_path=thesis_path,
            review_report=thesis_review_report,
        ),
        risk_events=_build_daily_risk_events_status(
            rules_path=risk_events_path,
            occurrences_path=risk_event_occurrences_path,
            rules_validation_report=risk_events_validation_report,
            occurrence_review_report=risk_event_occurrence_review_report,
        ),
        valuation=_build_daily_valuation_status(
            input_path=valuation_path,
            review_report=valuation_review_report,
        ),
        trades=_build_daily_trade_review_status(
            input_path=trades_path,
            universe=universe,
            watchlist=watchlist,
            score_date=score_date,
            prices=prices,
            data_quality_report=data_quality_report,
            benchmark_tickers=benchmark_tickers,
        ),
    )


def _build_daily_thesis_review_report(
    input_path: Path,
    watchlist: WatchlistConfig,
    industry_chain: IndustryChainConfig,
    score_date: date,
) -> ThesisReviewReport:
    validation_report = validate_trade_thesis_store(
        store=load_trade_thesis_store(input_path),
        watchlist=watchlist,
        industry_chain=industry_chain,
        as_of=score_date,
    )
    return build_thesis_review_report(validation_report)


def _build_daily_thesis_status(
    input_path: Path,
    review_report: ThesisReviewReport,
) -> DailyManualReviewStatus:
    validation_report = review_report.validation_report
    watch_count = sum(item.health == "WATCH" for item in review_report.items)
    challenged_count = sum(item.health == "CHALLENGED" for item in review_report.items)
    invalidated_count = sum(item.health == "INVALIDATED" for item in review_report.items)
    summary = (
        f"Thesis {validation_report.thesis_count} 个，活跃 "
        f"{validation_report.active_count} 个；需关注 {watch_count} 个，"
        f"受挑战 {challenged_count} 个，已证伪 {invalidated_count} 个。"
    )
    status = "FAIL" if invalidated_count else review_report.status
    error_count = validation_report.error_count + (1 if invalidated_count else 0)
    return DailyManualReviewStatus(
        name="交易 thesis",
        status=status,
        summary=summary,
        error_count=error_count,
        warning_count=validation_report.warning_count,
        source_path=input_path,
    )


def _build_daily_risk_events_status(
    rules_path: Path,
    occurrences_path: Path,
    rules_validation_report: RiskEventsValidationReport,
    occurrence_review_report: RiskEventOccurrenceReviewReport,
) -> DailyManualReviewStatus:
    active_l2_l3_count = sum(
        1
        for rule in rules_validation_report.config.event_rules
        if rule.active and rule.level in {"L2", "L3"}
    )
    occurrence_validation = occurrence_review_report.validation_report
    error_count = rules_validation_report.error_count + occurrence_validation.error_count
    warning_count = rules_validation_report.warning_count + occurrence_validation.warning_count
    status = "PASS"
    if error_count:
        status = "FAIL"
    elif warning_count or occurrence_review_report.status == "PASS_WITH_WARNINGS":
        status = "PASS_WITH_WARNINGS"
    summary = (
        f"风险规则 {len(rules_validation_report.config.event_rules)} 条，活跃 "
        f"{rules_validation_report.active_rule_count} 条；活跃 L2/L3 规则 "
        f"{active_l2_l3_count} 条。发生记录 {occurrence_validation.occurrence_count} 条，"
        f"活跃/观察 {occurrence_validation.active_occurrence_count} 条，可评分 "
        f"{len(occurrence_review_report.score_eligible_active_items)} 条，可触发仓位闸门 "
        f"{len(occurrence_review_report.position_gate_eligible_active_items)} 条。"
    )
    return DailyManualReviewStatus(
        name="风险事件",
        status=status,
        summary=summary,
        error_count=error_count,
        warning_count=warning_count,
        source_path=occurrences_path if occurrences_path.exists() else rules_path,
    )


def _build_daily_valuation_status(
    input_path: Path,
    review_report: ValuationReviewReport,
) -> DailyManualReviewStatus:
    validation_report = review_report.validation_report
    overheated_count = sum(
        item.health in {"EXPENSIVE_OR_CROWDED", "EXTREME_OVERHEATED"}
        for item in review_report.items
    )
    summary = (
        f"估值快照 {validation_report.snapshot_count} 个，覆盖 "
        f"{validation_report.ticker_count} 个标的；偏贵或拥挤 "
        f"{overheated_count} 个。"
    )
    return DailyManualReviewStatus(
        name="估值与拥挤度",
        status=review_report.status,
        summary=summary,
        error_count=validation_report.error_count,
        warning_count=validation_report.warning_count,
        source_path=input_path,
    )


def _build_daily_trade_review_status(
    input_path: Path,
    universe: UniverseConfig,
    watchlist: WatchlistConfig,
    score_date: date,
    prices: pd.DataFrame,
    data_quality_report: DataQualityReport,
    benchmark_tickers: tuple[str, ...],
) -> DailyManualReviewStatus:
    try:
        validation_report = validate_trade_record_store(
            store=load_trade_record_store(input_path),
            universe=universe,
            watchlist=watchlist,
            as_of=score_date,
        )
        review_report = build_trade_review_report(
            validation_report=validation_report,
            prices=prices,
            data_quality_report=data_quality_report,
            benchmark_tickers=benchmark_tickers,
        )
    except ValueError as exc:
        return _daily_review_exception_status("交易复盘归因", input_path, exc)

    summary = (
        f"交易记录 {validation_report.trade_count} 笔，已关闭 "
        f"{validation_report.closed_count} 笔；生成基准归因 "
        f"{len(review_report.items)} 条。"
    )
    return DailyManualReviewStatus(
        name="交易复盘归因",
        status=review_report.status,
        summary=summary,
        error_count=validation_report.error_count,
        warning_count=validation_report.warning_count,
        source_path=input_path,
    )


def _daily_review_exception_status(
    name: str,
    source_path: Path,
    exc: Exception,
) -> DailyManualReviewStatus:
    return DailyManualReviewStatus(
        name=name,
        status="FAIL",
        summary=f"模块加载或复核失败：{exc}",
        error_count=1,
        warning_count=0,
        source_path=source_path,
    )


def _download_manifest_path(prices_path: Path) -> Path:
    return prices_path.parent / "download_manifest.csv"


def _marketstack_prices_path(prices_path: Path) -> Path:
    return prices_path.parent / "prices_marketstack_daily.csv"


def _requires_marketstack_prices(prices_path: Path) -> bool:
    default_prices_path = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
    try:
        return prices_path.resolve() == default_prices_path.resolve()
    except OSError:
        return prices_path == default_prices_path


def _market_feature_source_checks(
    *,
    prices_frame: pd.DataFrame,
    rates_frame: pd.DataFrame,
    prices_path: Path,
    rates_path: Path,
    decision_time: date,
) -> tuple[FeatureAvailabilitySourceCheck, ...]:
    return (
        build_feature_source_check(
            source="prices_daily",
            frame=prices_frame,
            decision_time=decision_time,
            input_path=prices_path,
            event_time_columns=("date",),
            available_time_columns=(
                "available_time",
                "vendor_available_at",
                "ingested_at",
                "downloaded_at",
            ),
            fallback_policy="download_manifest.downloaded_at；若缺失则按下一交易日可用保守处理",
        ),
        build_feature_source_check(
            source="rates_daily",
            frame=rates_frame,
            decision_time=decision_time,
            input_path=rates_path,
            event_time_columns=("date",),
            available_time_columns=(
                "available_time",
                "vendor_available_at",
                "ingested_at",
                "downloaded_at",
            ),
            fallback_policy="download_manifest.downloaded_at；若缺失则按下一交易日可用保守处理",
        ),
    )


def _sec_pit_feature_source_check(
    *,
    feature_panel_path: Path,
    decision_time: date,
) -> FeatureAvailabilitySourceCheck:
    frame = pd.read_csv(feature_panel_path) if feature_panel_path.exists() else pd.DataFrame()
    return build_feature_source_check(
        source="sec_edgar_reconstructed_pit_features",
        frame=frame,
        decision_time=decision_time,
        input_path=feature_panel_path,
        event_time_columns=("period_end", "decision_date"),
        available_time_columns=("max_input_available_time_utc",),
        fallback_policy="",
        notes="TRADING-039 SEC reconstructed filing-time PIT feature panel.",
    )


def _path_from_snapshot_trace(snapshot: dict[str, object]) -> Path | None:
    trace = snapshot.get("trace")
    if not isinstance(trace, dict):
        return None
    path_text = trace.get("trace_bundle_path")
    return Path(str(path_text)) if path_text else None


def _trace_dataset_path(trace_bundle: dict[str, object], dataset_type: str) -> Path | None:
    dataset_refs = trace_bundle.get("dataset_refs")
    if not isinstance(dataset_refs, list):
        return None
    for dataset in dataset_refs:
        if not isinstance(dataset, dict):
            continue
        if dataset.get("dataset_type") == dataset_type and dataset.get("path"):
            return Path(str(dataset["path"]))
    return None


def _trace_quality_report_path(trace_bundle: dict[str, object]) -> Path | None:
    quality_refs = trace_bundle.get("quality_refs")
    if not isinstance(quality_refs, list):
        return None
    for quality in quality_refs:
        if not isinstance(quality, dict):
            continue
        if quality.get("report_path"):
            return Path(str(quality["report_path"]))
    return None


def _build_score_daily_sec_pit_feature_report(
    *,
    score_date: date,
    sec_companies: SecCompaniesConfig,
    sec_pit_feature_panel_path: Path,
) -> SecFundamentalFeaturesReport:
    if not sec_pit_feature_panel_path.exists():
        console.print("[red]SEC PIT feature panel 不存在，已停止每日评分。[/red]")
        console.print(f"SEC PIT feature panel：{sec_pit_feature_panel_path}")
        raise typer.Exit(code=1)
    tickers = [company.ticker for company in sec_companies.companies if company.active]
    try:
        reports = sec_pit_feature_panel_to_feature_reports(
            sec_pit_feature_panel_path,
            (score_date,),
            tickers,
        )
    except ValueError as exc:
        console.print("[red]SEC PIT feature panel 可见时间校验失败，已停止每日评分。[/red]")
        console.print(f"SEC PIT feature panel：{sec_pit_feature_panel_path}")
        console.print(str(exc))
        raise typer.Exit(code=1) from exc
    return reports[score_date]


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _latest_decision_snapshot_path(snapshot_dir: Path) -> Path:
    candidates: list[tuple[date, Path]] = []
    for path in snapshot_dir.glob("decision_snapshot_*.json"):
        if not path.is_file():
            continue
        try:
            candidates.append((_decision_snapshot_date(path), path))
        except typer.BadParameter:
            continue
    if not candidates:
        raise typer.BadParameter(f"未找到可用 decision_snapshot：{snapshot_dir}")
    return max(candidates, key=lambda item: (item[0], item[1].name))[1]


def _decision_snapshot_date(path: Path) -> date:
    raw_date = path.stem.removeprefix("decision_snapshot_")
    try:
        return date.fromisoformat(raw_date)
    except ValueError:
        pass
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise typer.BadParameter(f"无法读取 decision_snapshot 日期：{path}") from exc
    if isinstance(payload, dict):
        signal_date = payload.get("signal_date") or payload.get("as_of")
        if signal_date:
            return _parse_date(str(signal_date))
    raise typer.BadParameter(f"decision_snapshot 文件名或内容缺少 YYYY-MM-DD 日期：{path}")


def _parse_datetime(value: str) -> datetime:
    normalized = value.strip()
    if not normalized:
        raise typer.BadParameter("时间不能为空。")
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        if "T" not in normalized and " " not in normalized:
            parsed_date = date.fromisoformat(normalized)
            return datetime.combine(parsed_date, time.min, tzinfo=UTC)
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise typer.BadParameter("时间必须使用 ISO datetime 或 YYYY-MM-DD 格式。") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _parse_csv_items(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_positive_int_csv(value: str, label: str) -> list[int]:
    items = _parse_csv_items(value)
    if not items:
        raise typer.BadParameter(f"{label}不能为空。")
    parsed: list[int] = []
    for item in items:
        try:
            integer = int(item)
        except ValueError as exc:
            raise typer.BadParameter(f"{label}必须是逗号分隔的正整数。") from exc
        if integer <= 0:
            raise typer.BadParameter(f"{label}必须是正整数。")
        parsed.append(integer)
    return parsed


if __name__ == "__main__":
    app()
