from __future__ import annotations

import os
from dataclasses import replace
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from ai_trading_system.backtest.audit import (
    build_backtest_audit_report,
    default_backtest_audit_report_path,
    write_backtest_audit_report,
)
from ai_trading_system.backtest.daily import (
    DEFAULT_BENCHMARK_TICKERS,
    BacktestRegimeContext,
    default_backtest_daily_path,
    default_backtest_input_coverage_path,
    default_backtest_report_path,
    run_daily_score_backtest,
    write_backtest_daily_csv,
    write_backtest_input_coverage_csv,
    write_backtest_report,
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
from ai_trading_system.benchmark_policy import (
    DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
    default_benchmark_policy_report_path,
    load_benchmark_policy,
    lookup_benchmark_policy_entry,
    render_benchmark_policy_lookup,
    validate_benchmark_policy,
    write_benchmark_policy_report,
)
from ai_trading_system.catalyst_calendar import (
    default_catalyst_calendar_report_path,
    load_catalyst_calendar,
    lookup_catalyst,
    render_catalyst_lookup,
    validate_catalyst_calendar,
    write_catalyst_calendar_report,
)
from ai_trading_system.config import (
    DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DATA_QUALITY_CONFIG_PATH,
    DEFAULT_DATA_SOURCES_CONFIG_PATH,
    DEFAULT_EXECUTION_POLICY_CONFIG_PATH,
    DEFAULT_FEATURE_CONFIG_PATH,
    DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    DEFAULT_PORTFOLIO_CONFIG_PATH,
    DEFAULT_RISK_EVENTS_CONFIG_PATH,
    DEFAULT_SCENARIO_LIBRARY_CONFIG_PATH,
    DEFAULT_SCORING_RULES_CONFIG_PATH,
    DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    DEFAULT_WATCHLIST_CONFIG_PATH,
    PROJECT_ROOT,
    IndustryChainConfig,
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
from ai_trading_system.data.download import download_daily_data
from ai_trading_system.data.quality import (
    DataQualityReport,
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.data_sources import (
    build_data_source_health_report,
    default_data_source_health_report_path,
    default_data_sources_report_path,
    validate_data_sources_config,
    write_data_source_health_report,
    write_data_sources_validation_report,
)
from ai_trading_system.decision_causal_chains import (
    DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    build_decision_causal_chain_ledger,
    default_decision_causal_chain_report_path,
    load_decision_causal_chain_ledger,
    load_decision_outcomes_frame,
    lookup_decision_causal_chain,
    render_decision_causal_chain_lookup,
    write_decision_causal_chain_ledger,
    write_decision_causal_chain_report,
)
from ai_trading_system.decision_learning_queue import (
    DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    build_decision_learning_queue,
    default_decision_learning_queue_report_path,
    load_decision_learning_queue,
    lookup_decision_learning_item,
    render_decision_learning_item_lookup,
    write_decision_learning_queue,
    write_decision_learning_queue_report,
)
from ai_trading_system.decision_outcomes import (
    DEFAULT_DECISION_OUTCOMES_PATH,
    DEFAULT_OUTCOME_HORIZONS,
    build_decision_outcomes,
    default_decision_calibration_report_path,
    load_decision_snapshots,
    write_decision_calibration_report,
    write_decision_outcomes_csv,
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
    lookup_execution_action,
    render_execution_action_lookup,
    render_execution_advisory_section,
    validate_execution_policy,
    write_execution_policy_report,
)
from ai_trading_system.features.market import (
    build_market_features,
    default_feature_report_path,
    write_feature_summary,
    write_features_csv,
)
from ai_trading_system.feedback_loop_review import (
    build_feedback_loop_review_report,
    default_feedback_loop_review_report_path,
    write_feedback_loop_review_report,
)
from ai_trading_system.fundamentals.sec_companyfacts import (
    SecEdgarCompanyFactsProvider,
    download_sec_companyfacts,
)
from ai_trading_system.fundamentals.sec_features import (
    SecFundamentalFeaturesReport,
    build_sec_fundamental_features_report,
    build_sec_fundamental_features_report_from_metric_rows,
    default_sec_fundamental_features_csv_path,
    default_sec_fundamental_features_report_path,
    write_sec_fundamental_features_csv,
    write_sec_fundamental_features_report,
)
from ai_trading_system.fundamentals.sec_metrics import (
    build_sec_fundamental_metrics_report,
    default_sec_fundamental_metrics_csv_path,
    default_sec_fundamental_metrics_report_path,
    default_sec_fundamental_metrics_validation_report_path,
    load_sec_fundamental_metric_rows_csv,
    validate_sec_fundamental_metric_rows,
    validate_sec_fundamental_metrics_csv,
    write_sec_fundamental_metric_rows_csv,
    write_sec_fundamental_metrics_csv,
    write_sec_fundamental_metrics_report,
    write_sec_fundamental_metrics_validation_report,
)
from ai_trading_system.fundamentals.sec_validation import (
    default_sec_companyfacts_validation_report_path,
    validate_sec_companyfacts_cache,
    write_sec_companyfacts_validation_report,
)
from ai_trading_system.fundamentals.tsm_ir import (
    TsmIrHttpProvider,
    TsmIrQuarterlyMetricRow,
    build_tsm_ir_quarterly_batch_import_report,
    build_tsm_ir_sec_metric_conversion_report,
    extract_tsm_ir_pdf_text,
    load_tsm_ir_quarterly_metric_rows_csv,
    merge_tsm_ir_quarterly_rows_into_sec_metrics,
    merge_tsm_ir_quarterly_rows_into_sec_metrics_as_of,
    parse_tsm_ir_management_report_text,
    select_tsm_ir_management_report_resource,
    select_tsm_ir_quarterly_metric_rows_as_of,
    write_tsm_ir_pdf_text_extraction_report,
    write_tsm_ir_quarterly_batch_import_report,
    write_tsm_ir_quarterly_batch_metrics_csv,
    write_tsm_ir_quarterly_metrics_csv,
    write_tsm_ir_quarterly_report,
)
from ai_trading_system.historical_inputs import (
    build_historical_risk_event_occurrence_review_report,
    build_historical_valuation_review_report,
)
from ai_trading_system.industry_chain import (
    default_industry_chain_report_path,
    validate_industry_chain_config,
    write_industry_chain_validation_report,
)
from ai_trading_system.market_evidence import (
    default_market_evidence_report_path,
    import_market_evidence_csv,
    load_market_evidence_store,
    validate_market_evidence_store,
    write_market_evidence_import_report,
    write_market_evidence_validation_report,
    write_market_evidence_yaml,
)
from ai_trading_system.report_traceability import (
    build_backtest_trace_bundle,
    build_daily_score_trace_bundle,
    default_report_trace_bundle_path,
    lookup_trace_record,
    render_trace_lookup,
    render_traceability_section,
    write_trace_bundle,
)
from ai_trading_system.reports.daily import render_recommendation_markdown
from ai_trading_system.risk_event_prereview import (
    default_risk_event_prereview_report_path,
    import_risk_event_prereview_csv,
    write_risk_event_prereview_import_report,
    write_risk_event_prereview_queue,
)
from ai_trading_system.risk_event_sources import (
    import_risk_event_occurrences_csv,
    write_risk_event_occurrence_import_report,
    write_risk_event_occurrences_yaml,
)
from ai_trading_system.risk_events import (
    RiskEventOccurrenceReviewReport,
    RiskEventsValidationReport,
    build_risk_event_occurrence_review_report,
    default_risk_event_occurrence_report_path,
    default_risk_events_report_path,
    load_risk_event_occurrence_store,
    validate_risk_event_occurrence_store,
    validate_risk_events_config,
    write_risk_event_occurrence_review_report,
    write_risk_events_validation_report,
)
from ai_trading_system.rule_experiments import (
    DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    build_rule_experiment_ledger,
    default_rule_experiment_report_path,
    lookup_rule_experiment,
    render_rule_experiment_lookup,
    write_rule_experiment_ledger,
    write_rule_experiment_report,
)
from ai_trading_system.rule_governance import (
    DEFAULT_RULE_CARDS_PATH,
    default_rule_governance_report_path,
    load_rule_card_store,
    lookup_rule_card,
    render_rule_card_lookup,
    validate_rule_card_store,
    write_rule_governance_report,
)
from ai_trading_system.scenario_library import (
    default_scenario_library_report_path,
    load_scenario_library,
    lookup_scenario,
    render_scenario_lookup,
    validate_scenario_library,
    write_scenario_library_report,
)
from ai_trading_system.scoring.daily import (
    DailyManualReviewStatus,
    DailyReviewSummary,
    build_daily_score_report,
    default_daily_score_report_path,
    load_previous_daily_score_snapshot,
    write_daily_score_report,
    write_scores_csv,
)
from ai_trading_system.scoring.position_model import ModuleScore, WeightedScoreModel
from ai_trading_system.thesis import (
    build_thesis_review_report,
    default_thesis_review_report_path,
    default_thesis_validation_report_path,
    load_trade_thesis_store,
    validate_trade_thesis_store,
    write_thesis_review_report,
    write_thesis_validation_report,
)
from ai_trading_system.trade_review import (
    build_trade_review_report,
    default_trade_review_report_path,
    load_trade_record_store,
    validate_trade_record_store,
    write_trade_review_report,
)
from ai_trading_system.valuation import (
    ValuationReviewReport,
    build_valuation_review_report,
    default_valuation_review_report_path,
    default_valuation_validation_report_path,
    load_valuation_snapshot_store,
    validate_valuation_snapshot_store,
    write_valuation_review_report,
    write_valuation_validation_report,
)
from ai_trading_system.valuation_sources import (
    default_fmp_analyst_estimate_history_dir,
    default_fmp_analyst_history_validation_report_path,
    default_fmp_historical_valuation_fetch_report_path,
    default_fmp_historical_valuation_raw_dir,
    default_fmp_valuation_fetch_report_path,
    fetch_fmp_historical_valuation_snapshots,
    fetch_fmp_valuation_snapshots,
    import_valuation_snapshots_from_csv,
    validate_fmp_analyst_estimate_history,
    write_fmp_analyst_estimate_history_snapshots,
    write_fmp_analyst_history_validation_report,
    write_fmp_historical_valuation_fetch_report,
    write_fmp_historical_valuation_raw_payloads,
    write_fmp_valuation_fetch_report,
    write_valuation_csv_import_report,
    write_valuation_snapshots_as_yaml,
)
from ai_trading_system.watchlist import (
    default_watchlist_report_path,
    validate_watchlist_config,
    write_watchlist_validation_report,
)
from ai_trading_system.watchlist_lifecycle import (
    DEFAULT_WATCHLIST_LIFECYCLE_PATH,
    default_watchlist_lifecycle_report_path,
    load_watchlist_lifecycle,
    validate_watchlist_lifecycle,
    write_watchlist_lifecycle_report,
)

app = typer.Typer(help="AI 产业链趋势分析和仓位管理工具。", no_args_is_help=True)
watchlist_app = typer.Typer(help="观察池和能力圈管理。", no_args_is_help=True)
industry_chain_app = typer.Typer(help="产业链节点和因果图管理。", no_args_is_help=True)
thesis_app = typer.Typer(help="交易 thesis 和假设验证管理。", no_args_is_help=True)
risk_events_app = typer.Typer(help="风险事件分级和动作规则管理。", no_args_is_help=True)
valuation_app = typer.Typer(help="估值、预期和拥挤度快照管理。", no_args_is_help=True)
data_sources_app = typer.Typer(help="数据源目录和审计规则管理。", no_args_is_help=True)
fundamentals_app = typer.Typer(help="基本面数据源下载和审计。", no_args_is_help=True)
trace_app = typer.Typer(help="报告 evidence bundle 反查。", no_args_is_help=True)
evidence_app = typer.Typer(help="新市场信息 evidence 账本。", no_args_is_help=True)
feedback_app = typer.Typer(help="决策结果观察、校准和因果链查询。", no_args_is_help=True)
scenarios_app = typer.Typer(help="AI 产业链情景压力测试库。", no_args_is_help=True)
catalysts_app = typer.Typer(help="未来催化剂日历和事件前复核。", no_args_is_help=True)
execution_app = typer.Typer(help="Advisory execution policy 和执行纪律。", no_args_is_help=True)
app.add_typer(watchlist_app, name="watchlist")
app.add_typer(industry_chain_app, name="industry-chain")
app.add_typer(thesis_app, name="thesis")
app.add_typer(risk_events_app, name="risk-events")
app.add_typer(valuation_app, name="valuation")
app.add_typer(data_sources_app, name="data-sources")
app.add_typer(fundamentals_app, name="fundamentals")
app.add_typer(trace_app, name="trace")
app.add_typer(evidence_app, name="evidence")
app.add_typer(feedback_app, name="feedback")
app.add_typer(scenarios_app, name="scenarios")
app.add_typer(catalysts_app, name="catalysts")
app.add_typer(execution_app, name="execution")
console = Console()
DEFAULT_RISK_EVENT_OCCURRENCES_PATH = (
    PROJECT_ROOT / "data" / "external" / "risk_event_occurrences"
)
DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH = (
    PROJECT_ROOT / "data" / "processed" / "risk_event_prereview_queue.json"
)
DEFAULT_MARKET_EVIDENCE_PATH = PROJECT_ROOT / "data" / "external" / "market_evidence"
DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR = default_fmp_analyst_estimate_history_dir(
    PROJECT_ROOT / "data" / "raw"
)
DEFAULT_FMP_HISTORICAL_VALUATION_RAW_DIR = default_fmp_historical_valuation_raw_dir(
    PROJECT_ROOT / "data" / "raw"
)


@trace_app.command("lookup")
def trace_lookup(
    bundle_path: Annotated[
        Path,
        typer.Option(help="evidence bundle JSON 路径。"),
    ],
    object_id: Annotated[
        str,
        typer.Option("--id", help="claim/evidence/dataset/quality/run id。"),
    ],
) -> None:
    """按 ID 反查报告 evidence bundle 中的上下文。"""
    try:
        record_type, record = lookup_trace_record(bundle_path, object_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"evidence bundle 不存在：{bundle_path}") from exc
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_trace_lookup(record_type, record))


@evidence_app.command("validate")
def validate_market_evidence(
    input_path: Annotated[
        Path,
        typer.Option(help="market_evidence YAML 文件或目录路径。"),
    ] = DEFAULT_MARKET_EVIDENCE_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown evidence 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验新市场信息 evidence 账本。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_market_evidence_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_market_evidence_store(
        load_market_evidence_store(input_path),
        as_of=validation_date,
    )
    write_market_evidence_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]Market evidence 状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"证据数：{report.evidence_count}；待复核：{report.pending_review_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@evidence_app.command("import-csv")
def import_market_evidence_command(
    input_path: Annotated[
        Path,
        typer.Option(help="人工复核或 LLM 分类后的 market_evidence CSV 路径。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(help="写入 market_evidence YAML 的目录。"),
    ] = DEFAULT_MARKET_EVIDENCE_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 导入报告输出路径。"),
    ] = None,
) -> None:
    """从 CSV 导入 market_evidence YAML。"""
    import_report = import_market_evidence_csv(input_path)
    import_report_output = report_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"market_evidence_import_{date.today()}.md"
    )
    write_market_evidence_import_report(import_report, import_report_output)
    if not import_report.passed:
        console.print("[red]Market evidence CSV 导入失败，未写入 YAML。[/red]")
        console.print(f"导入报告：{import_report_output}")
        raise typer.Exit(code=1)

    written_paths = write_market_evidence_yaml(import_report.evidence, output_dir)
    console.print("[green]Market evidence 已导入。[/green]")
    console.print(f"导入报告：{import_report_output}")
    console.print(f"写入证据数：{len(written_paths)}")
    console.print(f"输出目录：{output_dir}")


@feedback_app.command("calibrate")
def calibrate_decision_outcomes(
    decision_snapshot_path: Annotated[
        Path,
        typer.Option(help="decision_snapshot JSON 文件或目录路径。"),
    ] = DEFAULT_DECISION_SNAPSHOT_DIR,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径，用于复用数据质量门禁。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="校准截止日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    horizons: Annotated[
        str,
        typer.Option(help="逗号分隔的 outcome 观察窗口，单位为交易日。"),
    ] = ",".join(str(item) for item in DEFAULT_OUTCOME_HORIZONS),
    strategy_ticker: Annotated[
        str,
        typer.Option(help="AI proxy 或策略代理标的。"),
    ] = "SMH",
    benchmarks: Annotated[
        str,
        typer.Option(help="逗号分隔的对比基准 ticker。"),
    ] = ",".join(DEFAULT_BENCHMARK_TICKERS),
    benchmark_policy_path: Annotated[
        Path,
        typer.Option(help="benchmark policy YAML 路径，用于审计 AI proxy / benchmark 解释口径。"),
    ] = DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
    benchmark_policy_report_path: Annotated[
        Path | None,
        typer.Option(help="可选：Markdown benchmark policy 校验报告输出路径。"),
    ] = None,
    outcomes_path: Annotated[
        Path,
        typer.Option(help="decision_outcomes CSV 输出路径。"),
    ] = DEFAULT_DECISION_OUTCOMES_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 校准报告输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
) -> None:
    """从历史 decision_snapshot 生成 outcome 和评分校准报告。"""
    calibration_date = _parse_date(as_of) if as_of else date.today()
    horizon_values = _parse_positive_int_csv(horizons, "outcome 观察窗口")
    benchmark_tickers = tuple(_parse_csv_items(benchmarks))
    if not benchmark_tickers:
        raise typer.BadParameter("至少需要一个对比基准 ticker。")
    benchmark_policy_report = validate_benchmark_policy(
        load_benchmark_policy(benchmark_policy_path),
        as_of=calibration_date,
        selected_strategy_ticker=strategy_ticker,
        selected_benchmark_tickers=benchmark_tickers,
    )
    if benchmark_policy_report_path is not None:
        write_benchmark_policy_report(benchmark_policy_report, benchmark_policy_report_path)
    if not benchmark_policy_report.passed:
        console.print("[red]基准政策校验失败，已停止决策校准。[/red]")
        console.print(
            f"错误数：{benchmark_policy_report.error_count}；"
            f"警告数：{benchmark_policy_report.warning_count}"
        )
        if benchmark_policy_report_path is not None:
            console.print(f"基准政策报告：{benchmark_policy_report_path}")
        raise typer.Exit(code=1)
    tickers = list(dict.fromkeys([strategy_ticker, *benchmark_tickers]))
    universe = load_universe()
    data_quality_config = load_data_quality()
    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        calibration_date,
    )
    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=tickers,
        expected_rate_series=configured_rate_series(universe),
        quality_config=data_quality_config,
        as_of=calibration_date,
        manifest_path=_download_manifest_path(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止决策校准。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    snapshots = load_decision_snapshots(decision_snapshot_path)
    if not snapshots:
        raise typer.BadParameter(f"未找到 decision_snapshot：{decision_snapshot_path}")
    prices_frame = pd.read_csv(prices_path)
    market_regimes = load_market_regimes(DEFAULT_MARKET_REGIMES_CONFIG_PATH)
    default_market_regime = market_regime_by_id(
        market_regimes,
        market_regimes.default_backtest_regime,
    )
    market_regime = BacktestRegimeContext(
        regime_id=default_market_regime.regime_id,
        name=default_market_regime.name,
        start_date=default_market_regime.start_date,
        anchor_date=default_market_regime.anchor_date,
        anchor_event=default_market_regime.anchor_event,
        description=default_market_regime.description,
    )
    result = build_decision_outcomes(
        snapshots=snapshots,
        prices=prices_frame,
        as_of=calibration_date,
        horizons=tuple(horizon_values),
        strategy_ticker=strategy_ticker,
        benchmark_tickers=benchmark_tickers,
        market_regime=market_regime,
        data_quality_report=data_quality_report,
        benchmark_policy_report=benchmark_policy_report,
    )
    outcomes_output = write_decision_outcomes_csv(result, outcomes_path)
    calibration_report_output = report_path or default_decision_calibration_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        calibration_date,
    )
    calibration_report_output = write_decision_calibration_report(
        result,
        outcomes_path=outcomes_output,
        data_quality_report_path=quality_output,
        output_path=calibration_report_output,
    )

    status_style = "green" if len(result.available_rows) >= 30 else "yellow"
    console.print(
        f"[{status_style}]决策校准完成。可用 outcome："
        f"{len(result.available_rows)}[/{status_style}]"
    )
    console.print(f"校准报告：{calibration_report_output}")
    console.print(f"Outcome CSV：{outcomes_output}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")
    console.print(f"基准政策状态：{benchmark_policy_report.status}")
    if benchmark_policy_report_path is not None:
        console.print(f"基准政策报告：{benchmark_policy_report_path}")


@feedback_app.command("build-causal-chain")
def build_decision_causal_chains_command(
    decision_snapshot_path: Annotated[
        Path,
        typer.Option(help="decision_snapshot JSON 文件或目录路径。"),
    ] = DEFAULT_DECISION_SNAPSHOT_DIR,
    outcomes_path: Annotated[
        Path,
        typer.Option(help="decision_outcomes CSV 路径；不存在时只生成 signal-time 链条。"),
    ] = DEFAULT_DECISION_OUTCOMES_PATH,
    output_path: Annotated[
        Path,
        typer.Option(help="decision_causal_chain ledger JSON 输出路径。"),
    ] = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 因果链报告输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
) -> None:
    """构建 decision causal chain ledger。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    snapshots = load_decision_snapshots(decision_snapshot_path)
    if not snapshots:
        raise typer.BadParameter(f"未找到 decision_snapshot：{decision_snapshot_path}")
    outcomes = load_decision_outcomes_frame(outcomes_path)
    ledger = build_decision_causal_chain_ledger(
        snapshots=snapshots,
        outcomes=outcomes,
    )
    ledger_output = write_decision_causal_chain_ledger(ledger, output_path)
    report_output = report_path or default_decision_causal_chain_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    report_output = write_decision_causal_chain_report(
        ledger,
        ledger_path=ledger_output,
        output_path=report_output,
    )

    console.print("[green]决策因果链已生成。[/green]")
    console.print(f"链条数：{ledger.chain_count}")
    console.print(f"Ledger：{ledger_output}")
    console.print(f"报告：{report_output}")


@feedback_app.command("lookup-chain")
def lookup_decision_causal_chain_command(
    chain_id: Annotated[
        str,
        typer.Option("--id", help="decision causal chain id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="decision_causal_chain ledger JSON 路径。"),
    ] = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
) -> None:
    """按 chain_id 反查 decision causal chain。"""
    try:
        chain = lookup_decision_causal_chain(input_path, chain_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"decision causal chain ledger 不存在：{input_path}") from exc
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_decision_causal_chain_lookup(chain))


@feedback_app.command("build-learning-queue")
def build_decision_learning_queue_command(
    causal_chain_path: Annotated[
        Path,
        typer.Option(help="decision_causal_chain ledger JSON 输入路径。"),
    ] = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    output_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 输出路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 学习队列报告输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    min_available_windows: Annotated[
        int,
        typer.Option(help="形成非 sample_limited 归因所需的最少可用 outcome 窗口。"),
    ] = 1,
) -> None:
    """从 decision causal chain 生成学习复核队列。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        causal_ledger = load_decision_causal_chain_ledger(causal_chain_path)
    except FileNotFoundError as exc:
        raise typer.BadParameter(
            f"decision causal chain ledger 不存在：{causal_chain_path}"
        ) from exc
    ledger = build_decision_learning_queue(
        chains=tuple(causal_ledger.get("chains", [])),
        min_available_windows=min_available_windows,
    )
    queue_output = write_decision_learning_queue(ledger, output_path)
    report_output = report_path or default_decision_learning_queue_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    report_output = write_decision_learning_queue_report(
        ledger,
        ledger_path=queue_output,
        output_path=report_output,
    )

    console.print("[green]决策学习队列已生成。[/green]")
    console.print(f"复核项数：{ledger.item_count}")
    console.print(f"Queue：{queue_output}")
    console.print(f"报告：{report_output}")


@feedback_app.command("lookup-learning")
def lookup_decision_learning_queue_command(
    review_id: Annotated[
        str,
        typer.Option("--id", help="learning review id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
) -> None:
    """按 review_id 反查 learning queue 项。"""
    try:
        item = lookup_decision_learning_item(input_path, review_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"decision learning queue 不存在：{input_path}") from exc
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_decision_learning_item_lookup(item))


@feedback_app.command("build-rule-experiments")
def build_rule_experiments_command(
    learning_queue_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 输入路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    output_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 输出路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 规则实验台账报告输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    replay_start: Annotated[
        str,
        typer.Option(help="历史 replay 计划起点，格式为 YYYY-MM-DD。"),
    ] = "2022-12-01",
    replay_end: Annotated[
        str | None,
        typer.Option(help="历史 replay 计划终点，格式为 YYYY-MM-DD，默认 as-of。"),
    ] = None,
    shadow_start: Annotated[
        str | None,
        typer.Option(help="前向 shadow 计划起点，格式为 YYYY-MM-DD，默认 as-of。"),
    ] = None,
    shadow_days: Annotated[
        int,
        typer.Option(help="前向 shadow 最少观察天数。"),
    ] = 20,
) -> None:
    """从 learning queue 生成候选规则实验台账。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        learning_ledger = load_decision_learning_queue(learning_queue_path)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"decision learning queue 不存在：{learning_queue_path}") from exc
    replay_start_date = _parse_date(replay_start)
    replay_end_date = _parse_date(replay_end) if replay_end else report_date
    shadow_start_date = _parse_date(shadow_start) if shadow_start else report_date
    ledger = build_rule_experiment_ledger(
        learning_items=tuple(learning_ledger.get("items", [])),
        replay_start=replay_start_date,
        replay_end=replay_end_date,
        shadow_start=shadow_start_date,
        shadow_days=shadow_days,
    )
    ledger_output = write_rule_experiment_ledger(ledger, output_path)
    report_output = report_path or default_rule_experiment_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    report_output = write_rule_experiment_report(
        ledger,
        ledger_path=ledger_output,
        output_path=report_output,
    )

    console.print("[green]候选规则实验台账已生成。[/green]")
    console.print(f"候选规则数：{ledger.candidate_count}")
    console.print(f"Ledger：{ledger_output}")
    console.print(f"报告：{report_output}")


@feedback_app.command("lookup-rule-experiment")
def lookup_rule_experiment_command(
    candidate_id: Annotated[
        str,
        typer.Option("--id", help="rule experiment candidate id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
) -> None:
    """按 candidate_id 反查候选规则实验。"""
    try:
        candidate = lookup_rule_experiment(input_path, candidate_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"rule experiment ledger 不存在：{input_path}") from exc
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_rule_experiment_lookup(candidate))


@feedback_app.command("validate-rule-cards")
def validate_rule_cards_command(
    input_path: Annotated[
        Path,
        typer.Option(help="rule cards YAML 路径。"),
    ] = DEFAULT_RULE_CARDS_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 规则治理校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 production / candidate / retired rule card registry。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report = validate_rule_card_store(
        load_rule_card_store(input_path),
        as_of=validation_date,
    )
    report_path = output_path or default_rule_governance_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_rule_governance_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]规则治理状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"Rule cards：{report.card_count}；"
        f"Production：{report.production_count}；Candidate：{report.candidate_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@feedback_app.command("lookup-rule-card")
def lookup_rule_card_command(
    rule_id: Annotated[
        str,
        typer.Option("--id", help="rule card id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="rule cards YAML 路径。"),
    ] = DEFAULT_RULE_CARDS_PATH,
) -> None:
    """按 rule_id 反查 rule card。"""
    try:
        card = lookup_rule_card(input_path, rule_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"rule card registry 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_rule_card_lookup(card))


@feedback_app.command("validate-benchmark-policy")
def validate_benchmark_policy_command(
    input_path: Annotated[
        Path,
        typer.Option(help="benchmark policy YAML 路径。"),
    ] = DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    strategy_ticker: Annotated[
        str | None,
        typer.Option(help="可选：本次回测或校准使用的 AI proxy / strategy ticker。"),
    ] = None,
    benchmarks: Annotated[
        str | None,
        typer.Option(help="可选：逗号分隔的本次对比基准 ticker。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown benchmark policy 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 AI proxy 与 benchmark policy registry。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    benchmark_tickers = (
        tuple(_parse_csv_items(benchmarks)) if benchmarks is not None else None
    )
    report = validate_benchmark_policy(
        load_benchmark_policy(input_path),
        as_of=validation_date,
        selected_strategy_ticker=strategy_ticker,
        selected_benchmark_tickers=benchmark_tickers,
    )
    report_path = output_path or default_benchmark_policy_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_benchmark_policy_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]基准政策状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"Benchmark：{report.instrument_count}；"
        f"Custom AI basket：{report.custom_basket_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@feedback_app.command("lookup-benchmark-policy")
def lookup_benchmark_policy_command(
    entry_id: Annotated[
        str,
        typer.Option("--id", help="benchmark id、ticker 或 custom basket id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="benchmark policy YAML 路径。"),
    ] = DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
) -> None:
    """按 ticker、benchmark id 或 basket id 反查 benchmark policy 条目。"""
    try:
        entry = lookup_benchmark_policy_entry(input_path, entry_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"benchmark policy 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_benchmark_policy_lookup(entry))


@feedback_app.command("loop-review")
def feedback_loop_review_command(
    evidence_path: Annotated[
        Path,
        typer.Option(help="market evidence YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "market_evidence",
    decision_snapshot_path: Annotated[
        Path,
        typer.Option(help="decision_snapshot JSON 文件或目录路径。"),
    ] = DEFAULT_DECISION_SNAPSHOT_DIR,
    outcomes_path: Annotated[
        Path,
        typer.Option(help="decision_outcomes CSV 路径。"),
    ] = DEFAULT_DECISION_OUTCOMES_PATH,
    causal_chain_path: Annotated[
        Path,
        typer.Option(help="decision_causal_chain ledger JSON 路径。"),
    ] = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    learning_queue_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    rule_experiment_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    task_register_path: Annotated[
        Path,
        typer.Option(help="任务登记 Markdown 路径。"),
    ] = PROJECT_ROOT / "docs" / "task_register.md",
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(help="复核窗口起始日期，格式为 YYYY-MM-DD，默认 as_of 前 7 天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 闭环复核报告输出路径。"),
    ] = None,
) -> None:
    """生成反馈闭环周期复核报告。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    since_date = _parse_date(since) if since else None
    report = build_feedback_loop_review_report(
        as_of=review_date,
        since=since_date,
        evidence_path=evidence_path,
        decision_snapshot_path=decision_snapshot_path,
        outcomes_path=outcomes_path,
        causal_chain_path=causal_chain_path,
        learning_queue_path=learning_queue_path,
        rule_experiment_path=rule_experiment_path,
        task_register_path=task_register_path,
    )
    report_path = output_path or default_feedback_loop_review_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    write_feedback_loop_review_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{status_style}]反馈闭环复核状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"警告数：{report.warning_count}")


@app.callback()
def main() -> None:
    """AI 产业链趋势分析和仓位管理工具。"""


@app.command("score-example")
def score_example() -> None:
    """输出一份示例仓位建议。"""
    model = WeightedScoreModel()
    portfolio = load_portfolio()
    recommendation = model.recommend(
        [
            ModuleScore(
                "trend",
                score=72,
                weight=25,
                reason="SMH 和 QQQ 仍在长期均线上方",
            ),
            ModuleScore("fundamentals", score=60, weight=25, reason="MVP 阶段中性占位"),
            ModuleScore("macro_liquidity", score=55, weight=15, reason="利率环境不算友好"),
            ModuleScore("risk_sentiment", score=66, weight=15, reason="VIX 仍可控"),
            ModuleScore("valuation", score=50, weight=10, reason="MVP 阶段中性占位"),
            ModuleScore(
                "policy_geopolitics",
                score=60,
                weight=10,
                reason="MVP 阶段中性占位",
            ),
        ],
        total_risk_asset_min=portfolio.portfolio.total_risk_asset_min,
        total_risk_asset_max=portfolio.portfolio.total_risk_asset_max,
    )
    console.print(render_recommendation_markdown(recommendation))


@app.command("download-data")
def download_data(
    start: Annotated[
        str,
        typer.Option(help="开始日期，包含当天，格式为 YYYY-MM-DD。"),
    ] = "2018-01-01",
    end: Annotated[
        str | None,
        typer.Option(help="结束日期，包含当天，格式为 YYYY-MM-DD。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="输出缓存目录。"),
    ] = PROJECT_ROOT / "data" / "raw",
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="包含配置中的完整 AI 产业链标的，而不只下载核心观察池。",
        ),
    ] = False,
) -> None:
    """下载市场日线价格和 FRED 利率数据到本地 CSV 缓存。"""
    universe = load_universe()
    start_date = _parse_date(start)
    end_date = _parse_date(end) if end else date.today()

    summary = download_daily_data(
        universe,
        start=start_date,
        end=end_date,
        output_dir=output_dir,
        include_full_ai_chain=full_universe,
    )

    console.print("[green]数据缓存已更新。[/green]")
    console.print(f"价格数据：{summary.prices_path}（{summary.price_rows} 行）")
    console.print(f"利率数据：{summary.rates_path}（{summary.rate_rows} 行）")
    console.print(f"下载审计清单：{summary.manifest_path}")
    console.print(f"价格标的：{', '.join(summary.price_tickers)}")
    console.print(f"利率序列：{', '.join(summary.rate_series)}")


@app.command("validate-data")
def validate_data(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 报告输出路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验配置中的完整 AI 产业链标的，而不只校验核心观察池。",
        ),
    ] = False,
) -> None:
    """校验缓存数据并写入 Markdown 质量报告。"""
    universe = load_universe()
    quality_config = load_data_quality()
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(
            universe,
            include_full_ai_chain=full_universe,
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=quality_config,
        as_of=validation_date,
        manifest_path=_download_manifest_path(prices_path),
    )
    write_data_quality_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]数据质量状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@app.command("backtest")
def backtest(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    start: Annotated[
        str | None,
        typer.Option(
            "--from",
            help="回测开始日期，格式为 YYYY-MM-DD；未提供时使用所选市场阶段起点。",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--to", help="回测结束日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    strategy_ticker: Annotated[
        str,
        typer.Option(help="用于承载 AI 仓位的策略代理标的。"),
    ] = "SMH",
    benchmarks: Annotated[
        str,
        typer.Option(help="逗号分隔的买入持有基准标的。"),
    ] = ",".join(DEFAULT_BENCHMARK_TICKERS),
    benchmark_policy_path: Annotated[
        Path,
        typer.Option(help="benchmark policy YAML 路径，用于审计 AI proxy / benchmark 解释口径。"),
    ] = DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
    benchmark_policy_report_path: Annotated[
        Path | None,
        typer.Option(help="可选：Markdown benchmark policy 校验报告输出路径。"),
    ] = None,
    cost_bps: Annotated[
        float,
        typer.Option(help="单边交易成本，单位 bps。"),
    ] = 5.0,
    slippage_bps: Annotated[
        float,
        typer.Option(help="线性滑点或盘口冲击估算，单位 bps；默认不额外扣除。"),
    ] = 0.0,
    regime: Annotated[
        str | None,
        typer.Option(
            "--regime",
            help="市场阶段 ID，默认使用 config/market_regimes.yaml 的 default_backtest_regime。",
        ),
    ] = None,
    regimes_path: Annotated[
        Path,
        typer.Option(help="市场阶段配置文件路径。"),
    ] = DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    daily_output_path: Annotated[
        Path | None,
        typer.Option(help="每日回测明细 CSV 输出路径。"),
    ] = None,
    input_coverage_output_path: Annotated[
        Path | None,
        typer.Option(help="机器可读历史输入覆盖诊断 CSV 输出路径。"),
    ] = None,
    audit_output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 回测输入审计报告输出路径。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="JSON evidence bundle 输出路径。"),
    ] = None,
    minimum_component_coverage: Annotated[
        float,
        typer.Option(help="审计用评分模块最低平均覆盖率阈值，范围 0-1。"),
    ] = 0.9,
    fail_on_audit_warning: Annotated[
        bool,
        typer.Option(
            "--fail-on-audit-warning",
            help="输入审计状态不是 PASS 时返回非零退出码，适合严格本地门禁。",
        ),
    ] = False,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 回测报告输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径，用于 point-in-time 基本面回测。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    sec_metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径，用于 point-in-time 基本面回测。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    fundamental_feature_config_path: Annotated[
        Path,
        typer.Option(help="SEC 基本面特征公式配置文件路径，用于 point-in-time 基本面回测。"),
    ] = DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    sec_companyfacts_dir: Annotated[
        Path,
        typer.Option(help="SEC companyfacts 原始 JSON 缓存目录。"),
    ] = PROJECT_ROOT / "data" / "raw" / "sec_companyfacts",
    tsm_ir_input_path: Annotated[
        Path,
        typer.Option(help="TSMC IR 季度指标 CSV，用于补齐 TSM point-in-time 回测基本面。"),
    ] = PROJECT_ROOT / "data" / "processed" / "tsm_ir_quarterly_metrics.csv",
    sec_companyfacts_validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC companyfacts 缓存校验报告输出路径。"),
    ] = None,
    valuation_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径，用于 point-in-time 回测评分。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于 point-in-time 回测评分。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    risk_event_occurrences_path: Annotated[
        Path,
        typer.Option(help="风险事件发生记录 YAML 文件或目录路径，用于 point-in-time 回测评分。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    watchlist_lifecycle_path: Annotated[
        Path,
        typer.Option(help="观察池 point-in-time lifecycle 配置路径，用于回测按 signal_date 过滤。"),
    ] = DEFAULT_WATCHLIST_LIFECYCLE_PATH,
    watchlist_lifecycle_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 观察池 lifecycle 校验报告输出路径。"),
    ] = None,
    quality_as_of: Annotated[
        str | None,
        typer.Option(help="数据质量校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验配置中的完整 AI 产业链标的，而不只校验核心观察池。",
        ),
    ] = False,
) -> None:
    """基于每日评分规则运行历史回测。"""
    universe = load_universe()
    industry_chain = load_industry_chain()
    watchlist = load_watchlist()
    watchlist_lifecycle = load_watchlist_lifecycle(watchlist_lifecycle_path)
    data_quality_config = load_data_quality()
    feature_config = load_features()
    scoring_rules = load_scoring_rules()
    portfolio = load_portfolio()
    market_regimes = load_market_regimes(regimes_path)
    selected_regime_id = regime or market_regimes.default_backtest_regime
    try:
        selected_regime = market_regime_by_id(market_regimes, selected_regime_id)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    start_date = _parse_date(start) if start else selected_regime.start_date
    end_date = _parse_date(end) if end else date.today()
    quality_date = _parse_date(quality_as_of) if quality_as_of else date.today()
    benchmark_tickers = _parse_csv_items(benchmarks)
    if not benchmark_tickers:
        raise typer.BadParameter("至少需要一个基准标的。")
    if not 0.0 <= minimum_component_coverage <= 1.0:
        raise typer.BadParameter("审计覆盖率阈值必须在 0 到 1 之间。")
    benchmark_policy_report = validate_benchmark_policy(
        load_benchmark_policy(benchmark_policy_path),
        as_of=quality_date,
        selected_strategy_ticker=strategy_ticker,
        selected_benchmark_tickers=tuple(benchmark_tickers),
    )
    if benchmark_policy_report_path is not None:
        write_benchmark_policy_report(benchmark_policy_report, benchmark_policy_report_path)
    if not benchmark_policy_report.passed:
        console.print("[red]基准政策校验失败，已停止回测。[/red]")
        console.print(
            f"错误数：{benchmark_policy_report.error_count}；"
            f"警告数：{benchmark_policy_report.warning_count}"
        )
        if benchmark_policy_report_path is not None:
            console.print(f"基准政策报告：{benchmark_policy_report_path}")
        raise typer.Exit(code=1)

    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        quality_date,
    )
    backtest_daily_output = daily_output_path or default_backtest_daily_path(
        PROJECT_ROOT / "outputs" / "backtests",
        start_date,
        end_date,
    )
    backtest_input_coverage_output = (
        input_coverage_output_path
        or default_backtest_input_coverage_path(
            PROJECT_ROOT / "outputs" / "backtests",
            start_date,
            end_date,
        )
    )
    backtest_report_output = report_path or default_backtest_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        start_date,
        end_date,
    )
    backtest_audit_output = audit_output_path or default_backtest_audit_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        start_date,
        end_date,
    )
    backtest_trace_output = trace_bundle_path or default_report_trace_bundle_path(
        backtest_report_output
    )
    sec_companyfacts_validation_output = (
        sec_companyfacts_validation_report_path
        or default_sec_companyfacts_validation_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            quality_date,
        )
    )
    watchlist_lifecycle_report_output = (
        watchlist_lifecycle_report_path
        or default_watchlist_lifecycle_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            quality_date,
        )
    )

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=list(
            dict.fromkeys(
                [
                    *configured_price_tickers(
                        universe,
                        include_full_ai_chain=full_universe,
                    ),
                    strategy_ticker,
                    *benchmark_tickers,
                ]
            )
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=data_quality_config,
        as_of=quality_date,
        manifest_path=_download_manifest_path(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止回测。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    watchlist_lifecycle_report = validate_watchlist_lifecycle(
        lifecycle=watchlist_lifecycle,
        input_path=watchlist_lifecycle_path,
        watchlist=watchlist,
        universe=universe,
        as_of=quality_date,
    )
    write_watchlist_lifecycle_report(
        watchlist_lifecycle_report,
        watchlist_lifecycle_report_output,
    )
    if not watchlist_lifecycle_report.passed:
        console.print("[red]观察池 lifecycle 校验失败，已停止回测。[/red]")
        console.print(f"观察池 lifecycle 报告：{watchlist_lifecycle_report_output}")
        console.print(
            f"错误数：{watchlist_lifecycle_report.error_count}；"
            f"警告数：{watchlist_lifecycle_report.warning_count}"
        )
        raise typer.Exit(code=1)

    prices_frame = pd.read_csv(prices_path)
    rates_frame = pd.read_csv(rates_path)
    signal_dates = _backtest_signal_dates(
        prices=prices_frame,
        strategy_ticker=strategy_ticker,
        start=start_date,
        end=end_date,
    )
    sec_fundamental_feature_reports = _build_backtest_sec_fundamental_feature_reports(
        signal_dates=signal_dates,
        sec_companies_path=sec_companies_path,
        sec_metrics_path=sec_metrics_path,
        fundamental_feature_config_path=fundamental_feature_config_path,
        sec_companyfacts_dir=sec_companyfacts_dir,
        tsm_ir_input_path=tsm_ir_input_path,
        validation_as_of=quality_date,
        validation_report_output=sec_companyfacts_validation_output,
    )
    valuation_review_reports = _build_backtest_valuation_review_reports(
        signal_dates=signal_dates,
        valuation_path=valuation_path,
        universe=universe,
        watchlist=watchlist,
    )
    risk_event_occurrence_review_reports = (
        _build_backtest_risk_event_occurrence_review_reports(
            signal_dates=signal_dates,
            risk_events_path=risk_events_path,
            risk_event_occurrences_path=risk_event_occurrences_path,
            universe=universe,
            industry_chain=industry_chain,
            watchlist=watchlist,
            validation_as_of=quality_date,
        )
    )

    result = run_daily_score_backtest(
        prices=prices_frame,
        rates=rates_frame,
        feature_config=feature_config,
        scoring_rules=scoring_rules,
        portfolio_config=portfolio,
        data_quality_report=data_quality_report,
        core_watchlist=universe.ai_chain.get("core_watchlist", []),
        start=start_date,
        end=end_date,
        strategy_ticker=strategy_ticker,
        benchmark_tickers=tuple(benchmark_tickers),
        cost_bps=cost_bps,
        slippage_bps=slippage_bps,
        fundamental_feature_reports=sec_fundamental_feature_reports,
        valuation_review_reports=valuation_review_reports,
        risk_event_occurrence_review_reports=risk_event_occurrence_review_reports,
        watchlist_lifecycle=watchlist_lifecycle,
        benchmark_policy_report=benchmark_policy_report,
        market_regime=BacktestRegimeContext(
            regime_id=selected_regime.regime_id,
            name=selected_regime.name,
            start_date=selected_regime.start_date,
            anchor_date=selected_regime.anchor_date,
            anchor_event=selected_regime.anchor_event,
            description=selected_regime.description,
        ),
    )
    daily_output = write_backtest_daily_csv(result, backtest_daily_output)
    input_coverage_output = write_backtest_input_coverage_csv(
        result,
        backtest_input_coverage_output,
    )
    audit_report = build_backtest_audit_report(
        result=result,
        data_quality_report_path=quality_output,
        backtest_report_path=backtest_report_output,
        daily_output_path=daily_output,
        input_coverage_output_path=input_coverage_output,
        sec_companyfacts_validation_report_path=sec_companyfacts_validation_output,
        minimum_component_coverage=minimum_component_coverage,
    )
    audit_output = write_backtest_audit_report(audit_report, backtest_audit_output)
    backtest_trace_bundle = build_backtest_trace_bundle(
        result=result,
        audit_report=audit_report,
        report_path=backtest_report_output,
        data_quality_report_path=quality_output,
        daily_output_path=daily_output,
        input_coverage_output_path=input_coverage_output,
        audit_report_path=audit_output,
        config_paths=_backtest_trace_config_paths(
            regimes_path=regimes_path,
            benchmark_policy_path=benchmark_policy_path,
            sec_companies_path=sec_companies_path,
            sec_metrics_path=sec_metrics_path,
            fundamental_feature_config_path=fundamental_feature_config_path,
            risk_events_path=risk_events_path,
            watchlist_lifecycle_path=watchlist_lifecycle_path,
        ),
        sec_companyfacts_validation_report_path=sec_companyfacts_validation_output,
    )
    backtest_trace_output = write_trace_bundle(
        backtest_trace_bundle,
        backtest_trace_output,
    )
    report_output = write_backtest_report(
        result,
        data_quality_report_path=quality_output,
        daily_output_path=daily_output,
        output_path=backtest_report_output,
        sec_companyfacts_validation_report_path=sec_companyfacts_validation_output,
        input_coverage_output_path=input_coverage_output,
        audit_report_path=audit_output,
        traceability_section=render_traceability_section(
            backtest_trace_bundle,
            backtest_trace_output,
        ),
    )

    console.print(f"[yellow]回测状态：{result.status}[/yellow]")
    audit_style = "green" if audit_report.status == "PASS" else "yellow"
    console.print(f"[{audit_style}]输入审计状态：{audit_report.status}[/{audit_style}]")
    if result.market_regime is not None:
        console.print(
            f"市场阶段：{result.market_regime.name}（{result.market_regime.regime_id}）"
        )
    console.print(f"策略总收益：{result.strategy_metrics.total_return:.1%}")
    console.print(f"策略 CAGR：{result.strategy_metrics.cagr:.1%}")
    console.print(f"策略最大回撤：{result.strategy_metrics.max_drawdown:.1%}")
    console.print(f"回测报告：{report_output}")
    console.print(f"观察池 lifecycle 报告：{watchlist_lifecycle_report_output}")
    console.print(f"Evidence bundle：{backtest_trace_output}")
    console.print(f"输入审计报告：{audit_output}")
    console.print(f"每日明细：{daily_output}")
    console.print(f"历史输入覆盖诊断：{input_coverage_output}")
    console.print(
        f"SEC 基本面切片：{result.fundamental_feature_report_count} 个 signal_date"
    )
    console.print(
        f"估值快照切片：{result.valuation_review_report_count} 个 signal_date"
    )
    console.print(
        "风险事件发生记录切片："
        f"{result.risk_event_occurrence_review_report_count} 个 signal_date"
    )
    console.print(
        f"SEC companyfacts 校验报告：{sec_companyfacts_validation_output}"
    )
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")
    console.print(f"基准政策状态：{benchmark_policy_report.status}")
    if benchmark_policy_report_path is not None:
        console.print(f"基准政策报告：{benchmark_policy_report_path}")
    if fail_on_audit_warning and audit_report.status != "PASS":
        console.print(
            "[red]输入审计未达到 PASS，严格审计门禁已返回失败。[/red]"
        )
        raise typer.Exit(code=1)


@watchlist_app.command("list")
def list_watchlist(
    config_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    active_only: Annotated[
        bool,
        typer.Option("--active-only/--all", help="只显示活跃标的，或显示全部配置标的。"),
    ] = True,
) -> None:
    """列出观察池和能力圈配置。"""
    watchlist = load_watchlist(config_path)
    items = [item for item in watchlist.items if item.active or not active_only]

    table = Table(title="观察池与能力圈")
    table.add_column("Ticker")
    table.add_column("公司")
    table.add_column("类型")
    table.add_column("能力圈")
    table.add_column("风险")
    table.add_column("Thesis")
    table.add_column("产业链节点")

    for item in sorted(items, key=lambda value: value.ticker):
        table.add_row(
            item.ticker,
            item.company_name,
            item.instrument_type,
            f"{item.competence_score:.0f}",
            _risk_level_label(item.default_risk_level),
            "需要" if item.thesis_required else "不需要",
            ", ".join(item.ai_chain_nodes),
        )

    console.print(table)


@watchlist_app.command("validate")
def validate_watchlist(
    config_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 观察池校验报告输出路径。"),
    ] = None,
) -> None:
    """校验观察池覆盖、能力圈和 thesis 约束。"""
    universe = load_universe()
    watchlist = load_watchlist(config_path)
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_watchlist_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )

    report = validate_watchlist_config(
        watchlist=watchlist,
        universe=universe,
        as_of=validation_date,
    )
    write_watchlist_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]观察池校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"活跃标的数：{report.active_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@watchlist_app.command("validate-lifecycle")
def validate_watchlist_lifecycle_command(
    input_path: Annotated[
        Path,
        typer.Option(help="观察池 lifecycle YAML 配置路径。"),
    ] = DEFAULT_WATCHLIST_LIFECYCLE_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="当前观察池配置路径，用于一致性校验。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown lifecycle 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验观察池 point-in-time 生命周期。"""
    universe = load_universe()
    watchlist = load_watchlist(watchlist_path)
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_watchlist_lifecycle_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_watchlist_lifecycle(
        lifecycle=load_watchlist_lifecycle(input_path),
        input_path=input_path,
        watchlist=watchlist,
        universe=universe,
        as_of=validation_date,
    )
    write_watchlist_lifecycle_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(
        f"[{status_style}]观察池 lifecycle 校验状态：{report.status}[/{status_style}]"
    )
    console.print(f"报告：{report_path}")
    console.print(f"生命周期记录数：{report.entry_count}；当前活跃：{report.active_entry_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@industry_chain_app.command("list")
def list_industry_chain(
    config_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
) -> None:
    """列出产业链节点和因果关系。"""
    industry_chain = load_industry_chain(config_path)

    table = Table(title="产业链因果图")
    table.add_column("节点")
    table.add_column("名称")
    table.add_column("父节点")
    table.add_column("周期")
    table.add_column("现金流")
    table.add_column("情绪")
    table.add_column("相关标的")

    for node in sorted(industry_chain.nodes, key=lambda value: value.node_id):
        table.add_row(
            node.node_id,
            node.name,
            ", ".join(node.parent_node_ids) or "无",
            _horizon_label(node.impact_horizon),
            _relevance_label(node.cash_flow_relevance),
            _relevance_label(node.sentiment_relevance),
            ", ".join(node.related_tickers),
        )

    console.print(table)


@industry_chain_app.command("validate")
def validate_industry_chain(
    config_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径，用于校验节点引用。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 产业链校验报告输出路径。"),
    ] = None,
) -> None:
    """校验产业链节点、父子关系和观察池引用。"""
    industry_chain = load_industry_chain(config_path)
    watchlist = load_watchlist(watchlist_path)
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_industry_chain_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )

    report = validate_industry_chain_config(
        industry_chain=industry_chain,
        watchlist=watchlist,
        as_of=validation_date,
    )
    write_industry_chain_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]产业链校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"节点数：{len(report.nodes)}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@scenarios_app.command("validate")
def validate_scenarios_command(
    input_path: Annotated[
        Path,
        typer.Option(help="scenario library YAML 路径。"),
    ] = DEFAULT_SCENARIO_LIBRARY_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径，用于校验 affected_nodes。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径，用于校验 affected_tickers。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于校验 linked_risk_event_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 情景库校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 AI 产业链情景压力测试库。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report = validate_scenario_library(
        load_scenario_library(input_path),
        as_of=validation_date,
        industry_chain=load_industry_chain(industry_chain_path),
        watchlist=load_watchlist(watchlist_path),
        risk_events=load_risk_events(risk_events_path),
    )
    report_path = output_path or default_scenario_library_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_scenario_library_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]情景库状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"情景数：{report.scenario_count}；Active：{report.active_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@scenarios_app.command("lookup")
def lookup_scenario_command(
    scenario_id: Annotated[
        str,
        typer.Option("--id", help="scenario id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="scenario library YAML 路径。"),
    ] = DEFAULT_SCENARIO_LIBRARY_CONFIG_PATH,
) -> None:
    """按 scenario_id 反查情景定义。"""
    try:
        scenario = lookup_scenario(input_path, scenario_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"scenario library 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_scenario_lookup(scenario))


@catalysts_app.command("validate")
def validate_catalysts_command(
    input_path: Annotated[
        Path,
        typer.Option(help="catalyst calendar YAML 路径。"),
    ] = DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径，用于校验 related_nodes。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径，用于校验 related_tickers。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于校验 linked_risk_event_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    windows: Annotated[
        str,
        typer.Option(help="逗号分隔的 upcoming catalyst 窗口，单位为自然日。"),
    ] = "5,20,60",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 催化剂日历报告输出路径。"),
    ] = None,
) -> None:
    """校验未来催化剂日历并输出 5/20/60 天窗口。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    window_values = tuple(_parse_positive_int_csv(windows, "催化剂窗口"))
    report = validate_catalyst_calendar(
        load_catalyst_calendar(input_path),
        as_of=validation_date,
        industry_chain=load_industry_chain(industry_chain_path),
        watchlist=load_watchlist(watchlist_path),
        risk_events=load_risk_events(risk_events_path),
        windows=window_values,
    )
    report_path = output_path or default_catalyst_calendar_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_catalyst_calendar_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]催化剂日历状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"事件数：{report.event_count}；"
        f"未来 {max(report.windows)} 天：{report.upcoming_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@catalysts_app.command("upcoming")
def upcoming_catalysts_command(
    input_path: Annotated[
        Path,
        typer.Option(help="catalyst calendar YAML 路径。"),
    ] = DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    windows: Annotated[
        str,
        typer.Option(help="逗号分隔的 upcoming catalyst 窗口，单位为自然日。"),
    ] = "5,20,60",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown upcoming catalyst 报告输出路径。"),
    ] = None,
) -> None:
    """输出 upcoming catalyst 分桶报告。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    window_values = tuple(_parse_positive_int_csv(windows, "催化剂窗口"))
    report = validate_catalyst_calendar(
        load_catalyst_calendar(input_path),
        as_of=validation_date,
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        risk_events=load_risk_events(),
        windows=window_values,
    )
    report_path = output_path or default_catalyst_calendar_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_catalyst_calendar_report(report, report_path)
    console.print(f"Upcoming catalyst 报告：{report_path}")
    console.print(f"未来 {max(report.windows)} 天事件数：{report.upcoming_count}")
    if not report.passed:
        console.print("[red]催化剂日历校验失败，upcoming 报告仅供排查。[/red]")
        raise typer.Exit(code=1)


@catalysts_app.command("lookup")
def lookup_catalyst_command(
    catalyst_id: Annotated[
        str,
        typer.Option("--id", help="catalyst id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="catalyst calendar YAML 路径。"),
    ] = DEFAULT_CATALYST_CALENDAR_CONFIG_PATH,
) -> None:
    """按 catalyst_id 反查催化剂事件。"""
    try:
        event = lookup_catalyst(input_path, catalyst_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"catalyst calendar 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_catalyst_lookup(event))


@execution_app.command("validate")
def validate_execution_policy_command(
    input_path: Annotated[
        Path,
        typer.Option(help="execution policy YAML 路径。"),
    ] = DEFAULT_EXECUTION_POLICY_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown execution policy 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 advisory execution policy 和固定动作词表。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report = validate_execution_policy(
        load_execution_policy(input_path),
        as_of=validation_date,
    )
    report_path = output_path or default_execution_policy_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_execution_policy_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]执行政策状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"动作数：{report.action_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@execution_app.command("lookup")
def lookup_execution_action_command(
    action_id: Annotated[
        str,
        typer.Option("--id", help="execution action id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="execution policy YAML 路径。"),
    ] = DEFAULT_EXECUTION_POLICY_CONFIG_PATH,
) -> None:
    """按 execution action id 反查动作定义。"""
    try:
        action = lookup_execution_action(input_path, action_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"execution policy 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_execution_action_lookup(action))


@thesis_app.command("list")
def list_theses(
    input_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "trade_theses",
) -> None:
    """列出本地交易 thesis。"""
    store = load_trade_thesis_store(input_path)

    table = Table(title="交易 Thesis")
    table.add_column("Thesis")
    table.add_column("Ticker")
    table.add_column("方向")
    table.add_column("状态")
    table.add_column("创建日期")
    table.add_column("复核频率")
    table.add_column("文件")

    for loaded in sorted(store.loaded, key=lambda item: item.thesis.thesis_id):
        thesis = loaded.thesis
        table.add_row(
            thesis.thesis_id,
            thesis.ticker,
            _thesis_direction_label(thesis.direction),
            _thesis_status_label(thesis.status),
            thesis.created_at.isoformat(),
            _thesis_review_frequency_label(thesis.review_frequency),
            str(loaded.path),
        )

    console.print(table)
    if not store.loaded:
        console.print("未发现可读取的交易 thesis。")
    if store.load_errors:
        console.print(
            f"[red]存在 {len(store.load_errors)} 个加载错误，请运行 validate 查看。[/red]"
        )


@thesis_app.command("validate")
def validate_theses(
    input_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "trade_theses",
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown thesis 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验交易 thesis 的结构、引用和复核约束。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_thesis_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    store = load_trade_thesis_store(input_path)
    report = validate_trade_thesis_store(
        store=store,
        watchlist=load_watchlist(watchlist_path),
        industry_chain=load_industry_chain(industry_chain_path),
        as_of=validation_date,
    )
    write_thesis_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]交易 thesis 校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"Thesis 数量：{report.thesis_count}；活跃：{report.active_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@thesis_app.command("review")
def review_theses(
    input_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "trade_theses",
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown thesis 复核报告输出路径。"),
    ] = None,
) -> None:
    """复核交易 thesis 的验证指标、证伪条件和风险事件状态。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_thesis_review_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    store = load_trade_thesis_store(input_path)
    validation_report = validate_trade_thesis_store(
        store=store,
        watchlist=load_watchlist(watchlist_path),
        industry_chain=load_industry_chain(industry_chain_path),
        as_of=review_date,
    )
    review_report = build_thesis_review_report(validation_report)
    write_thesis_review_report(review_report, report_path)

    status_style = "green" if review_report.status == "PASS" else (
        "yellow" if validation_report.passed else "red"
    )
    console.print(f"[{status_style}]交易 thesis 复核状态：{review_report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"Thesis 数量：{validation_report.thesis_count}；"
        f"活跃：{validation_report.active_count}"
    )
    console.print(
        f"校验错误数：{validation_report.error_count}；"
        f"校验警告数：{validation_report.warning_count}"
    )

    if not validation_report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("list")
def list_risk_events(
    config_path: Annotated[
        Path,
        typer.Option(help="风险事件配置文件路径。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    active_only: Annotated[
        bool,
        typer.Option("--active-only/--all", help="只显示活跃规则，或显示全部规则。"),
    ] = True,
) -> None:
    """列出风险事件分级规则。"""
    risk_events = load_risk_events(config_path)

    levels_table = Table(title="风险等级")
    levels_table.add_column("等级")
    levels_table.add_column("名称")
    levels_table.add_column("AI 仓位乘数")
    levels_table.add_column("人工复核")
    levels_table.add_column("默认动作")
    for level in sorted(risk_events.levels, key=lambda item: item.level):
        levels_table.add_row(
            level.level,
            level.name,
            f"{level.target_ai_exposure_multiplier:.0%}",
            "需要" if level.requires_manual_review else "不需要",
            level.default_action,
        )
    console.print(levels_table)

    rules_table = Table(title="风险事件规则")
    rules_table.add_column("事件")
    rules_table.add_column("等级")
    rules_table.add_column("活跃")
    rules_table.add_column("影响节点")
    rules_table.add_column("相关标的")
    for rule in sorted(risk_events.event_rules, key=lambda item: item.event_id):
        if active_only and not rule.active:
            continue
        rules_table.add_row(
            rule.event_id,
            rule.level,
            "是" if rule.active else "否",
            ", ".join(rule.affected_nodes),
            ", ".join(rule.related_tickers),
        )
    console.print(rules_table)


@risk_events_app.command("validate")
def validate_risk_events(
    config_path: Annotated[
        Path,
        typer.Option(help="风险事件配置文件路径。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件校验报告输出路径。"),
    ] = None,
) -> None:
    """校验风险事件等级、产业链引用和动作规则。"""
    universe = load_universe()
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_risk_events_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_risk_events_config(
        risk_events=load_risk_events(config_path),
        industry_chain=load_industry_chain(industry_chain_path),
        watchlist=load_watchlist(watchlist_path),
        universe=universe,
        as_of=validation_date,
    )
    write_risk_events_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]风险事件校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"风险事件规则数：{len(report.config.event_rules)}；活跃：{report.active_rule_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("list-occurrences")
def list_risk_event_occurrences(
    input_path: Annotated[
        Path,
        typer.Option(help="风险事件发生记录 YAML 文件或目录路径。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
) -> None:
    """列出本地风险事件发生记录。"""
    store = load_risk_event_occurrence_store(input_path)

    table = Table(title="风险事件发生记录")
    table.add_column("Occurrence", overflow="fold")
    table.add_column("事件", overflow="fold")
    table.add_column("状态")
    table.add_column("触发日期")
    table.add_column("最近确认")
    table.add_column("证据数")
    table.add_column("文件", overflow="fold")
    for loaded in sorted(store.loaded, key=lambda item: item.occurrence.occurrence_id):
        occurrence = loaded.occurrence
        table.add_row(
            occurrence.occurrence_id,
            occurrence.event_id,
            occurrence.status,
            occurrence.triggered_at.isoformat(),
            occurrence.last_confirmed_at.isoformat(),
            str(len(occurrence.evidence_sources)),
            str(loaded.path),
        )
    console.print(table)
    if not store.loaded:
        console.print("未发现可读取的风险事件发生记录。")
    if store.load_errors:
        console.print(
            "[red]存在 "
            f"{len(store.load_errors)} 个加载错误，请运行 validate-occurrences 查看。[/red]"
        )


@risk_events_app.command("import-occurrences-csv")
def import_risk_event_occurrences_csv_command(
    input_path: Annotated[
        Path,
        typer.Option(help="人工复核后的风险事件发生记录 CSV 输入路径。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(help="写入风险事件发生记录 YAML 的目录。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于导入后校验 event_id。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="导入和校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown CSV 导入报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件发生记录校验报告输出路径。"),
    ] = None,
) -> None:
    """导入人工复核后的风险事件发生记录 CSV，并写入可审计 YAML。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    import_report_output = output_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"risk_event_occurrence_import_{import_date}.md"
    )
    validation_output = validation_report_path or default_risk_event_occurrence_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        import_date,
    )
    import_report = import_risk_event_occurrences_csv(input_path)
    write_risk_event_occurrence_import_report(import_report, import_report_output)

    status_style = (
        "green" if import_report.status == "PASS" else "yellow" if import_report.passed else "red"
    )
    console.print(
        f"[{status_style}]风险事件发生记录 CSV 导入状态："
        f"{import_report.status}[/{status_style}]"
    )
    console.print(f"导入报告：{import_report_output}")
    console.print(
        f"CSV 行数：{import_report.row_count}；"
        f"发生记录：{import_report.occurrence_count}"
    )
    console.print(f"错误数：{import_report.error_count}；警告数：{import_report.warning_count}")
    if not import_report.passed:
        raise typer.Exit(code=1)

    written_paths = write_risk_event_occurrences_yaml(import_report, output_dir)
    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(output_dir),
        risk_events=load_risk_events(risk_events_path),
        as_of=import_date,
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)
    write_risk_event_occurrence_review_report(review_report, validation_output)

    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(f"写入 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(
        f"[{validation_style}]风险事件发生记录校验状态："
        f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("import-prereview-csv")
def import_risk_event_prereview_csv_command(
    input_path: Annotated[
        Path,
        typer.Option(help="OpenAI 结构化预审结果 CSV 输入路径。"),
    ],
    queue_path: Annotated[
        Path,
        typer.Option(help="写入风险事件预审待复核队列 JSON 的路径。"),
    ] = DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于检查 matched_risk_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="导入和校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 预审导入报告输出路径。"),
    ] = None,
) -> None:
    """导入 OpenAI 风险事件预审结果，并写入人工复核队列。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_risk_event_prereview_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        import_date,
    )
    import_report = import_risk_event_prereview_csv(
        input_path,
        risk_events=load_risk_events(risk_events_path),
        as_of=import_date,
    )
    write_risk_event_prereview_import_report(import_report, report_path)

    status_style = (
        "green" if import_report.status == "PASS" else "yellow" if import_report.passed else "red"
    )
    console.print(
        f"[{status_style}]风险事件 OpenAI 预审导入状态："
        f"{import_report.status}[/{status_style}]"
    )
    console.print(f"导入报告：{report_path}")
    console.print(
        f"CSV 行数：{import_report.row_count}；"
        f"预审记录：{import_report.record_count}；"
        f"L2/L3 候选：{import_report.high_level_candidate_count}"
    )
    console.print(f"错误数：{import_report.error_count}；警告数：{import_report.warning_count}")
    if not import_report.passed:
        raise typer.Exit(code=1)

    written_path = write_risk_event_prereview_queue(import_report, queue_path)
    console.print(f"预审待复核队列：{written_path}")
    console.print("预审记录保持 llm_extracted / pending_review，不进入评分或仓位闸门。")


@risk_events_app.command("validate-occurrences")
def validate_risk_event_occurrences(
    input_path: Annotated[
        Path,
        typer.Option(help="风险事件发生记录 YAML 文件或目录路径。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    config_path: Annotated[
        Path,
        typer.Option(help="风险事件规则配置文件路径。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件发生记录报告输出路径。"),
    ] = None,
) -> None:
    """校验实际发生的风险事件记录，并生成可供日报评分引用的报告。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_risk_event_occurrence_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(input_path),
        risk_events=load_risk_events(config_path),
        as_of=validation_date,
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)
    write_risk_event_occurrence_review_report(review_report, report_path)

    status_style = (
        "green"
        if review_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(
        f"[{status_style}]风险事件发生记录状态：{review_report.status}[/{status_style}]"
    )
    console.print(f"报告：{report_path}")
    console.print(
        f"发生记录数：{validation_report.occurrence_count}；"
        f"活跃/观察：{validation_report.active_occurrence_count}；"
        f"可评分：{len(review_report.score_eligible_active_items)}；"
        f"可触发仓位闸门：{len(review_report.position_gate_eligible_active_items)}"
    )
    console.print(f"错误数：{validation_report.error_count}；警告数：{validation_report.warning_count}")

    if not validation_report.passed:
        raise typer.Exit(code=1)


@data_sources_app.command("list")
def list_data_sources(
    config_path: Annotated[
        Path,
        typer.Option(help="数据源目录配置文件路径。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    active_only: Annotated[
        bool,
        typer.Option("--active-only/--all", help="只显示活跃数据源，或显示全部数据源。"),
    ] = False,
) -> None:
    """列出数据源目录。"""
    data_sources = load_data_sources(config_path)

    table = Table(title="数据源目录")
    table.add_column("Source", overflow="fold")
    table.add_column("Provider", overflow="fold")
    table.add_column("类型")
    table.add_column("状态")
    table.add_column("领域", overflow="fold")
    table.add_column("缓存", overflow="fold")
    for source in sorted(data_sources.sources, key=lambda item: item.source_id):
        if active_only and source.status != "active":
            continue
        table.add_row(
            source.source_id,
            source.provider,
            _data_source_type_label(source.source_type),
            _data_source_status_label(source.status),
            ", ".join(source.domains),
            ", ".join(source.cache_paths),
        )
    console.print(table)


@data_sources_app.command("validate")
def validate_data_sources(
    config_path: Annotated[
        Path,
        typer.Option(help="数据源目录配置文件路径。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据源目录校验报告输出路径。"),
    ] = None,
) -> None:
    """校验数据源目录、审计字段和来源限制。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_data_sources_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_data_sources_config(
        config=load_data_sources(config_path),
        as_of=validation_date,
    )
    write_data_sources_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]数据源目录校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"数据源数量：{len(report.sources)}；活跃：{report.active_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@data_sources_app.command("health")
def data_source_health(
    config_path: Annotated[
        Path,
        typer.Option(help="数据源目录配置文件路径。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option(help="download_manifest.csv 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "download_manifest.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据源健康报告输出路径。"),
    ] = None,
) -> None:
    """生成 provider health 和 reconciliation 覆盖报告。"""
    evaluation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_data_source_health_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        evaluation_date,
    )
    report = build_data_source_health_report(
        config=load_data_sources(config_path),
        as_of=evaluation_date,
        manifest_path=manifest_path,
    )
    write_data_source_health_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]数据源健康状态：{report.status}[/{status_style}]")
    console.print(f"Provider health score：{report.health_score}")
    console.print(f"报告：{report_path}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("list-sec-companies")
def list_sec_companies(
    config_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    active_only: Annotated[
        bool,
        typer.Option("--active-only/--all", help="只显示活跃公司，或显示全部公司。"),
    ] = True,
) -> None:
    """列出 SEC companyfacts 下载配置。"""
    config = load_sec_companies(config_path)

    table = Table(title="SEC Company Facts 公司映射")
    table.add_column("Ticker")
    table.add_column("CIK")
    table.add_column("公司")
    table.add_column("活跃")
    table.add_column("Taxonomy")
    table.add_column("SEC 指标周期")
    for company in sorted(config.companies, key=lambda item: item.ticker):
        if active_only and not company.active:
            continue
        table.add_row(
            company.ticker,
            company.cik,
            company.company_name,
            "是" if company.active else "否",
            ", ".join(company.expected_taxonomies),
            ", ".join(company.sec_metric_periods),
        )
    console.print(table)


@fundamentals_app.command("download-sec-companyfacts")
def download_sec_companyfacts_command(
    config_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="SEC companyfacts 原始 JSON 输出目录。"),
    ] = PROJECT_ROOT / "data" / "raw" / "sec_companyfacts",
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔的 ticker；未提供时下载全部活跃配置。"),
    ] = None,
    user_agent: Annotated[
        str | None,
        typer.Option(
            "--user-agent",
            envvar="SEC_USER_AGENT",
            help="SEC fair access 要求的 User-Agent；也可使用 SEC_USER_AGENT 环境变量。",
        ),
    ] = None,
) -> None:
    """下载 SEC companyfacts 原始 JSON，并写入审计 manifest。"""
    if user_agent is None or not user_agent.strip():
        raise typer.BadParameter(
            "SEC companyfacts 下载必须提供 --user-agent 或 SEC_USER_AGENT；"
            "格式建议包含项目/组织名称和联系邮箱。"
        )

    selected_tickers = _parse_csv_items(tickers) if tickers else None
    summary = download_sec_companyfacts(
        config=load_sec_companies(config_path),
        output_dir=output_dir,
        provider=SecEdgarCompanyFactsProvider(user_agent=user_agent),
        tickers=selected_tickers,
    )

    console.print("[green]SEC companyfacts 缓存已更新。[/green]")
    console.print(f"公司数量：{summary.company_count}")
    console.print(f"事实数量：{summary.total_fact_count}")
    console.print(f"输出目录：{summary.output_dir}")
    console.print(f"下载审计清单：{summary.manifest_path}")


@fundamentals_app.command("validate-sec-companyfacts")
def validate_sec_companyfacts_command(
    config_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    input_dir: Annotated[
        Path,
        typer.Option(help="SEC companyfacts 原始 JSON 输入目录。"),
    ] = PROJECT_ROOT / "data" / "raw" / "sec_companyfacts",
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC companyfacts 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 SEC companyfacts 原始缓存、CIK、taxonomy 和 manifest。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_sec_companyfacts_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_sec_companyfacts_cache(
        config=load_sec_companies(config_path),
        input_dir=input_dir,
        as_of=validation_date,
    )
    write_sec_companyfacts_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]SEC companyfacts 校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"配置公司数：{report.file_count}；已缓存：{report.available_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("extract-sec-metrics")
def extract_sec_metrics_command(
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    input_dir: Annotated[
        Path,
        typer.Option(help="SEC companyfacts 原始 JSON 输入目录。"),
    ] = PROJECT_ROOT / "data" / "raw" / "sec_companyfacts",
    as_of: Annotated[
        str | None,
        typer.Option(help="抽取日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面指标报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC companyfacts 校验报告输出路径。"),
    ] = None,
) -> None:
    """在 SEC 缓存校验通过后抽取结构化基本面指标。"""
    extraction_date = _parse_date(as_of) if as_of else date.today()
    validation_output = validation_report_path or default_sec_companyfacts_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        extraction_date,
    )
    csv_output = output_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        extraction_date,
    )
    markdown_output = report_path or default_sec_fundamental_metrics_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        extraction_date,
    )

    sec_companies = load_sec_companies(sec_companies_path)
    validation_report = validate_sec_companyfacts_cache(
        config=sec_companies,
        input_dir=input_dir,
        as_of=extraction_date,
    )
    write_sec_companyfacts_validation_report(validation_report, validation_output)
    if not validation_report.passed:
        console.print("[red]SEC companyfacts 质量门禁失败，已停止指标抽取。[/red]")
        console.print(f"SEC 缓存校验报告：{validation_output}")
        console.print(
            f"错误数：{validation_report.error_count}；"
            f"警告数：{validation_report.warning_count}"
        )
        raise typer.Exit(code=1)

    report = build_sec_fundamental_metrics_report(
        companies=sec_companies,
        metrics=load_fundamental_metrics(metrics_path),
        input_dir=input_dir,
        as_of=extraction_date,
        validation_report=validation_report,
    )
    csv_path = write_sec_fundamental_metrics_csv(report, csv_output)
    markdown_path = write_sec_fundamental_metrics_report(
        report=report,
        validation_report_path=validation_output,
        output_csv_path=csv_path,
        output_path=markdown_output,
    )

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]SEC 基本面指标抽取状态：{report.status}[/{status_style}]")
    console.print(f"SEC 缓存校验报告：{validation_output}（{validation_report.status}）")
    console.print(f"指标 CSV：{csv_path}")
    console.print(f"指标报告：{markdown_path}")
    console.print(f"公司数：{report.company_count}；指标行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("validate-sec-metrics")
def validate_sec_metrics_command(
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    input_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输入路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面指标 CSV 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验已抽取的 SEC 基本面指标 CSV。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    csv_input = input_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        validation_date,
    )
    report_output = output_path or default_sec_fundamental_metrics_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_sec_fundamental_metrics_csv(
        companies=load_sec_companies(sec_companies_path),
        metrics=load_fundamental_metrics(metrics_path),
        input_path=csv_input,
        as_of=validation_date,
    )
    write_sec_fundamental_metrics_validation_report(report, report_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]SEC 基本面指标 CSV 校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_output}")
    console.print(f"输入文件：{csv_input}")
    console.print(
        f"覆盖率：{report.coverage:.0%}；"
        f"当日行数：{report.as_of_row_count}；"
        f"总行数：{report.row_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("build-sec-features")
def build_sec_features_command(
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径，用于先校验指标 CSV。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    feature_config_path: Annotated[
        Path,
        typer.Option(help="SEC 基本面特征公式配置文件路径。"),
    ] = DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    input_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面指标 CSV 输入路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="特征日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="SEC 基本面特征 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面特征报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 基本面指标 CSV 校验报告输出路径。"),
    ] = None,
) -> None:
    """在 SEC 指标 CSV 校验通过后构建基本面比率特征。"""
    feature_date = _parse_date(as_of) if as_of else date.today()
    csv_input = input_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        feature_date,
    )
    validation_output = (
        validation_report_path
        or default_sec_fundamental_metrics_validation_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            feature_date,
        )
    )
    feature_csv_output = output_path or default_sec_fundamental_features_csv_path(
        PROJECT_ROOT / "data" / "processed",
        feature_date,
    )
    feature_report_output = report_path or default_sec_fundamental_features_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        feature_date,
    )

    sec_companies = load_sec_companies(sec_companies_path)
    metrics = load_fundamental_metrics(metrics_path)
    validation_report = validate_sec_fundamental_metrics_csv(
        companies=sec_companies,
        metrics=metrics,
        input_path=csv_input,
        as_of=feature_date,
    )
    write_sec_fundamental_metrics_validation_report(validation_report, validation_output)
    if not validation_report.passed:
        console.print("[red]SEC 基本面指标 CSV 校验失败，已停止特征构建。[/red]")
        console.print(f"SEC 指标 CSV 校验报告：{validation_output}")
        console.print(
            f"错误数：{validation_report.error_count}；"
            f"警告数：{validation_report.warning_count}"
        )
        raise typer.Exit(code=1)

    report = build_sec_fundamental_features_report(
        companies=sec_companies,
        feature_config=load_fundamental_features(feature_config_path),
        input_path=csv_input,
        as_of=feature_date,
        validation_report=validation_report,
    )
    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    if not report.passed:
        markdown_path = write_sec_fundamental_features_report(
            report=report,
            validation_report_path=validation_output,
            output_csv_path=feature_csv_output,
            output_path=feature_report_output,
        )
        console.print(
            f"[{status_style}]SEC 基本面特征构建状态：{report.status}[/{status_style}]"
        )
        console.print(f"SEC 指标 CSV 校验报告：{validation_output}（{validation_report.status}）")
        console.print(f"基本面特征 CSV 未写入：{feature_csv_output}")
        console.print(f"基本面特征报告：{markdown_path}")
        console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
        raise typer.Exit(code=1)

    csv_path = write_sec_fundamental_features_csv(report, feature_csv_output)
    markdown_path = write_sec_fundamental_features_report(
        report=report,
        validation_report_path=validation_output,
        output_csv_path=csv_path,
        output_path=feature_report_output,
    )

    console.print(f"[{status_style}]SEC 基本面特征构建状态：{report.status}[/{status_style}]")
    console.print(f"SEC 指标 CSV 校验报告：{validation_output}（{validation_report.status}）")
    console.print(f"基本面特征 CSV：{csv_path}")
    console.print(f"基本面特征报告：{markdown_path}")
    console.print(f"公司数：{report.company_count}；特征行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@fundamentals_app.command("extract-tsm-ir-pdf-text")
def extract_tsm_ir_pdf_text_command(
    input_path: Annotated[
        Path,
        typer.Option(help="本地 TSMC IR Management Report 官方 PDF 输入路径。"),
    ],
    source_url: Annotated[
        str,
        typer.Option(help="TSMC Investor Relations 官方 PDF URL。"),
    ],
    output_path: Annotated[
        Path | None,
        typer.Option(help="抽取后的 Management Report 文本输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    extracted_at: Annotated[
        str | None,
        typer.Option(help="抽取时间，ISO datetime 或 YYYY-MM-DD；默认当前 UTC 时间。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown TSMC IR PDF 文本抽取报告输出路径。"),
    ] = None,
) -> None:
    """从 TSMC IR 官方 PDF 的可抽取文本层生成 Management Report 文本。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    extraction_datetime = _parse_datetime(extracted_at) if extracted_at else datetime.now(tz=UTC)
    text_output = output_path or (
        PROJECT_ROOT
        / "data"
        / "external"
        / "fundamentals"
        / "tsm_ir"
        / f"{input_path.stem}.txt"
    )
    markdown_output = report_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"tsm_ir_pdf_text_{report_date}.md"
    )

    report = extract_tsm_ir_pdf_text(
        input_path=input_path,
        source_url=source_url,
        output_path=text_output,
        extracted_at=extraction_datetime,
    )
    markdown_path = write_tsm_ir_pdf_text_extraction_report(report, markdown_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]TSMC IR PDF 文本抽取状态：{report.status}[/{status_style}]")
    console.print(f"Source URL：{source_url}")
    console.print(f"PDF：{input_path}")
    console.print(f"抽取文本：{text_output}")
    console.print(f"报告：{markdown_path}")
    console.print(f"页数：{report.page_count}；字符数：{report.character_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        console.print("[red]TSMC IR PDF 文本抽取未通过，未生成可用于导入的可信文本。[/red]")
        raise typer.Exit(code=1)


@fundamentals_app.command("import-tsm-ir-quarterly")
def import_tsm_ir_quarterly(
    input_path: Annotated[
        Path,
        typer.Option(help="TSMC IR Management Report 已抽取文本输入路径。"),
    ],
    source_url: Annotated[
        str,
        typer.Option(help="TSMC Investor Relations 官方季度页面或 Management Report URL。"),
    ],
    fiscal_year: Annotated[
        int,
        typer.Option(help="财年，例如 2026。"),
    ],
    fiscal_period: Annotated[
        str,
        typer.Option(help="财期，例如 Q1、Q2、Q3 或 Q4。"),
    ],
    as_of: Annotated[
        str | None,
        typer.Option(help="导入评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    captured_at: Annotated[
        str | None,
        typer.Option(help="采集时间，ISO datetime 或 YYYY-MM-DD；默认当前 UTC 时间。"),
    ] = None,
    filed_date: Annotated[
        str | None,
        typer.Option(
            help=(
                "TSMC IR Management Report 公开/披露日期，格式为 YYYY-MM-DD；"
                "未提供时使用 captured_at 日期。"
            ),
        ),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="TSMC IR 季度指标 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown TSMC IR 季度基本面报告输出路径。"),
    ] = None,
) -> None:
    """从 TSMC IR 官方 Management Report 文本导入季度基本面指标。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    captured_datetime = _parse_datetime(captured_at) if captured_at else datetime.now(tz=UTC)
    disclosed_date = _parse_date(filed_date) if filed_date else None
    csv_output = output_path or (
        PROJECT_ROOT / "data" / "processed" / "tsm_ir_quarterly_metrics.csv"
    )
    normalized_period = fiscal_period.upper()
    if normalized_period not in {"Q1", "Q2", "Q3", "Q4"}:
        raise typer.BadParameter("财期必须是 Q1、Q2、Q3 或 Q4。")
    markdown_output = report_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"tsm_ir_quarterly_{fiscal_year}_{normalized_period}_{import_date}.md"
    )
    report = parse_tsm_ir_management_report_text(
        text=input_path.read_text(encoding="utf-8"),
        source_url=source_url,
        fiscal_year=fiscal_year,
        fiscal_period=normalized_period,
        as_of=import_date,
        captured_at=captured_datetime,
        source_path=input_path,
        filed_date=disclosed_date,
    )
    markdown_path = write_tsm_ir_quarterly_report(report, markdown_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]TSMC IR 季度基本面导入状态：{report.status}[/{status_style}]")
    console.print(f"报告：{markdown_path}")
    console.print(f"指标行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        console.print("[red]TSMC IR 季度基本面报告未通过，指标 CSV 未写入。[/red]")
        raise typer.Exit(code=1)

    csv_path = write_tsm_ir_quarterly_metrics_csv(report, csv_output)
    console.print(f"指标 CSV：{csv_path}")


@fundamentals_app.command("import-tsm-ir-quarterly-batch")
def import_tsm_ir_quarterly_batch(
    manifest_path: Annotated[
        Path,
        typer.Option(
            help=(
                "TSMC IR 批量导入 manifest CSV，字段为 fiscal_year,"
                "fiscal_period,source_url,input_path。"
            ),
        ),
    ],
    as_of: Annotated[
        str | None,
        typer.Option(help="导入评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    captured_at: Annotated[
        str | None,
        typer.Option(help="采集时间，ISO datetime 或 YYYY-MM-DD；默认当前 UTC 时间。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="TSMC IR 季度指标 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown TSMC IR 批量季度基本面报告输出路径。"),
    ] = None,
) -> None:
    """按 manifest 批量导入多个 TSMC IR Management Report 文本。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    captured_datetime = _parse_datetime(captured_at) if captured_at else datetime.now(tz=UTC)
    csv_output = output_path or (
        PROJECT_ROOT / "data" / "processed" / "tsm_ir_quarterly_metrics.csv"
    )
    markdown_output = report_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"tsm_ir_quarterly_batch_{import_date}.md"
    )

    report = build_tsm_ir_quarterly_batch_import_report(
        manifest_path=manifest_path,
        as_of=import_date,
        captured_at=captured_datetime,
        output_path=csv_output,
    )
    markdown_path = write_tsm_ir_quarterly_batch_import_report(report, markdown_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(
        f"[{status_style}]TSMC IR 批量季度基本面导入状态："
        f"{report.status}[/{status_style}]"
    )
    console.print(f"Manifest：{manifest_path}")
    console.print(f"报告：{markdown_path}")
    console.print(f"季度条目数：{report.entry_count}；指标行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        console.print("[red]TSMC IR 批量季度基本面报告未通过，指标 CSV 未写入。[/red]")
        raise typer.Exit(code=1)

    csv_path = write_tsm_ir_quarterly_batch_metrics_csv(report, csv_output)
    console.print(f"指标 CSV：{csv_path}")


@fundamentals_app.command("fetch-tsm-ir-quarterly")
def fetch_tsm_ir_quarterly(
    source_url: Annotated[
        str,
        typer.Option(help="TSMC Investor Relations 官方季度页面 URL。"),
    ],
    fiscal_year: Annotated[
        int,
        typer.Option(help="财年，例如 2026。"),
    ],
    fiscal_period: Annotated[
        str,
        typer.Option(help="财期，例如 Q1、Q2、Q3 或 Q4。"),
    ],
    as_of: Annotated[
        str | None,
        typer.Option(help="导入评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    captured_at: Annotated[
        str | None,
        typer.Option(help="采集时间，ISO datetime 或 YYYY-MM-DD；默认当前 UTC 时间。"),
    ] = None,
    filed_date: Annotated[
        str | None,
        typer.Option(
            help=(
                "TSMC IR Management Report 公开/披露日期，格式为 YYYY-MM-DD；"
                "未提供时使用 captured_at 日期。"
            ),
        ),
    ] = None,
    source_text_path: Annotated[
        Path | None,
        typer.Option(help="保存下载到的 Management Report 文本路径。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="TSMC IR 季度指标 CSV 输出路径。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown TSMC IR 季度基本面报告输出路径。"),
    ] = None,
    timeout: Annotated[
        float,
        typer.Option(help="TSMC IR HTTP 请求超时时间，单位秒。"),
    ] = 30.0,
    user_agent: Annotated[
        str,
        typer.Option(
            "--user-agent",
            envvar="TSM_IR_USER_AGENT",
            help="TSMC IR HTTP User-Agent；也可使用 TSM_IR_USER_AGENT 环境变量。",
        ),
    ] = "ai-trading-system tsm-ir/0.1",
) -> None:
    """从 TSMC IR 官方季度页面发现并下载 Management Report 文本后导入指标。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    captured_datetime = _parse_datetime(captured_at) if captured_at else datetime.now(tz=UTC)
    disclosed_date = _parse_date(filed_date) if filed_date else None
    normalized_period = fiscal_period.upper()
    if normalized_period not in {"Q1", "Q2", "Q3", "Q4"}:
        raise typer.BadParameter("财期必须是 Q1、Q2、Q3 或 Q4。")

    text_output = source_text_path or (
        PROJECT_ROOT
        / "data"
        / "external"
        / "fundamentals"
        / "tsm_ir"
        / f"{fiscal_year}_{normalized_period}_management_report_{import_date}.txt"
    )
    csv_output = output_path or (
        PROJECT_ROOT / "data" / "processed" / "tsm_ir_quarterly_metrics.csv"
    )
    markdown_output = report_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"tsm_ir_quarterly_{fiscal_year}_{normalized_period}_{import_date}.md"
    )

    provider = TsmIrHttpProvider(timeout=timeout, user_agent=user_agent)
    try:
        management_resource = select_tsm_ir_management_report_resource(
            provider=provider,
            source_url=source_url,
        )
        management_text = provider.download_text(management_resource.url)
    except Exception as exc:
        console.print("[red]TSMC IR 官方页面或 Management Report 文本下载失败。[/red]")
        console.print(str(exc))
        raise typer.Exit(code=1) from exc

    text_output.parent.mkdir(parents=True, exist_ok=True)
    text_output.write_text(management_text, encoding="utf-8")
    report = parse_tsm_ir_management_report_text(
        text=management_text,
        source_url=management_resource.url,
        fiscal_year=fiscal_year,
        fiscal_period=normalized_period,
        as_of=import_date,
        captured_at=captured_datetime,
        source_path=text_output,
        filed_date=disclosed_date,
    )
    markdown_path = write_tsm_ir_quarterly_report(report, markdown_output)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]TSMC IR 季度基本面抓取状态：{report.status}[/{status_style}]")
    console.print(f"季度页面：{source_url}")
    console.print(f"Management Report URL：{management_resource.url}")
    console.print(f"原始文本：{text_output}")
    console.print(f"报告：{markdown_path}")
    console.print(f"指标行数：{report.row_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        console.print("[red]TSMC IR 季度基本面报告未通过，指标 CSV 未写入。[/red]")
        raise typer.Exit(code=1)

    csv_path = write_tsm_ir_quarterly_metrics_csv(report, csv_output)
    console.print(f"指标 CSV：{csv_path}")


@fundamentals_app.command("merge-tsm-ir-sec-metrics")
def merge_tsm_ir_sec_metrics(
    input_path: Annotated[
        Path,
        typer.Option(help="TSMC IR 季度指标 CSV 输入路径。"),
    ] = PROJECT_ROOT / "data" / "processed" / "tsm_ir_quarterly_metrics.csv",
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置文件路径；TSM 必须声明 quarterly。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    metrics_path: Annotated[
        Path,
        typer.Option(help="SEC 指标映射配置文件路径，用于合并后校验。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    sec_input_path: Annotated[
        Path | None,
        typer.Option(help="既有 SEC 基本面指标 CSV；未提供时使用 as_of 默认路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="合并评估日期，格式为 YYYY-MM-DD；默认使用 TSM IR 输入最新 as_of。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="合并后的 SEC-style 基本面指标 CSV 输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC 指标 CSV 校验报告输出路径。"),
    ] = None,
) -> None:
    """把 TSMC IR 季度指标合并到统一 SEC-style 基本面指标 CSV。"""
    if not input_path.exists():
        raise typer.BadParameter(f"TSMC IR 季度指标 CSV 不存在：{input_path}")

    all_tsm_rows = load_tsm_ir_quarterly_metric_rows_csv(input_path)
    if not all_tsm_rows:
        raise typer.BadParameter("TSMC IR 季度指标 CSV 没有可合并的指标行。")
    merge_date = _parse_date(as_of) if as_of else max(row.as_of for row in all_tsm_rows)
    tsm_rows = tuple(
        replace(row, as_of=merge_date)
        for row in select_tsm_ir_quarterly_metric_rows_as_of(
            all_tsm_rows,
            merge_date,
        )
    )
    if not tsm_rows:
        raise typer.BadParameter(
            f"TSMC IR 季度指标 CSV 不包含 {merge_date.isoformat()} 可用的已披露季度记录。"
        )

    sec_input = sec_input_path or default_sec_fundamental_metrics_csv_path(
        PROJECT_ROOT / "data" / "processed",
        merge_date,
    )
    csv_output = output_path or sec_input
    validation_output = (
        validation_report_path
        or default_sec_fundamental_metrics_validation_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            merge_date,
        )
    )
    sec_companies = load_sec_companies(sec_companies_path)
    conversion_report = build_tsm_ir_sec_metric_conversion_report(tsm_rows, sec_companies)
    if not conversion_report.passed:
        console.print("[red]TSMC IR 转换为 SEC-style 指标失败，已停止合并。[/red]")
        console.print(
            f"错误数：{conversion_report.error_count}；"
            f"警告数：{conversion_report.warning_count}"
        )
        raise typer.Exit(code=1)

    existing_rows = load_sec_fundamental_metric_rows_csv(sec_input)
    merged_rows = merge_tsm_ir_quarterly_rows_into_sec_metrics(
        existing_rows=existing_rows,
        tsm_rows=tsm_rows,
        tsm_company=sec_companies,
    )
    csv_path = write_sec_fundamental_metric_rows_csv(merged_rows, csv_output)
    validation_report = validate_sec_fundamental_metric_rows(
        companies=sec_companies,
        metrics=load_fundamental_metrics(metrics_path),
        rows=merged_rows,
        source_path=csv_path,
        as_of=merge_date,
    )
    write_sec_fundamental_metrics_validation_report(validation_report, validation_output)

    status_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(
        f"[{status_style}]TSMC IR 合并后 SEC 指标校验状态："
        f"{validation_report.status}[/{status_style}]"
    )
    console.print(f"合并日期：{merge_date.isoformat()}")
    console.print(f"TSMC IR 转换行数：{len(conversion_report.rows)}")
    console.print(f"既有 SEC 指标行数：{len(existing_rows)}；合并后行数：{len(merged_rows)}")
    console.print(f"输出 CSV：{csv_path}")
    console.print(f"校验报告：{validation_output}")
    console.print(
        f"错误数：{validation_report.error_count}；警告数：{validation_report.warning_count}"
    )
    if not validation_report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("fetch-fmp")
def fetch_fmp_valuations(
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔 ticker；未提供时使用 universe 的 AI core_watchlist。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="写入 FMP 估值快照 YAML 的目录。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    analyst_history_dir: Annotated[
        Path,
        typer.Option(help="读取并写入 FMP analyst-estimates 原始历史快照的目录。"),
    ] = DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR,
    as_of: Annotated[
        str | None,
        typer.Option(help="估值评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown FMP 拉取报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值快照校验报告输出路径。"),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 FMP API key 的环境变量名。"),
    ] = "FMP_API_KEY",
    analyst_estimate_limit: Annotated[
        int,
        typer.Option(help="每个 ticker 拉取的 annual analyst estimate 记录数。"),
    ] = 10,
) -> None:
    """从 Financial Modeling Prep 拉取估值和预期快照。"""
    fetch_date = _parse_date(as_of) if as_of else date.today()
    selected_tickers = (
        _parse_csv_items(tickers)
        if tickers
        else load_universe().ai_chain.get("core_watchlist", [])
    )
    api_key = os.getenv(api_key_env)
    if not api_key:
        console.print(f"[red]未找到环境变量 {api_key_env}，已停止 FMP 拉取。[/red]")
        raise typer.Exit(code=1)

    fetch_report_output = output_path or default_fmp_valuation_fetch_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )
    validation_output = validation_report_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )

    try:
        fetch_report = fetch_fmp_valuation_snapshots(
            selected_tickers,
            api_key,
            fetch_date,
            analyst_history_dir=analyst_history_dir,
            valuation_history_dir=output_dir,
            captured_at=fetch_date,
            analyst_estimate_limit=analyst_estimate_limit,
        )
    except ValueError as exc:
        console.print(f"[red]FMP 参数错误：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    write_fmp_valuation_fetch_report(fetch_report, fetch_report_output)
    status_style = (
        "green" if fetch_report.status == "PASS" else "yellow" if fetch_report.passed else "red"
    )
    console.print(f"[{status_style}]FMP 估值拉取状态：{fetch_report.status}[/{status_style}]")
    console.print(f"拉取报告：{fetch_report_output}")
    console.print(
        f"请求标的：{', '.join(fetch_report.requested_tickers)}；"
        f"返回记录：{fetch_report.row_count}；生成快照：{fetch_report.imported_count}"
    )
    console.print(f"错误数：{fetch_report.error_count}；警告数：{fetch_report.warning_count}")
    if not fetch_report.passed:
        raise typer.Exit(code=1)

    history_paths = write_fmp_analyst_estimate_history_snapshots(
        fetch_report.analyst_estimate_history_snapshots,
        analyst_history_dir,
    )
    written_paths = write_valuation_snapshots_as_yaml(fetch_report.snapshots, output_dir)
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(output_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=fetch_date,
    )
    write_valuation_validation_report(validation_report, validation_output)

    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(f"写入 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(f"写入 analyst history：{len(history_paths)} 个文件 -> {analyst_history_dir}")
    console.print(
        f"[{validation_style}]估值快照校验状态："
        f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("fetch-fmp-valuation-history")
def fetch_fmp_valuation_history(
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔 ticker；未提供时使用 universe 的 AI core_watchlist。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="写入历史估值快照 YAML 的目录。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    raw_output_dir: Annotated[
        Path,
        typer.Option(help="写入 FMP historical key-metrics/ratios 原始 JSON 的目录。"),
    ] = DEFAULT_FMP_HISTORICAL_VALUATION_RAW_DIR,
    as_of: Annotated[
        str | None,
        typer.Option(help="拉取评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown FMP 历史估值拉取报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值快照校验报告输出路径。"),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 FMP API key 的环境变量名。"),
    ] = "FMP_API_KEY",
    period: Annotated[
        str,
        typer.Option(help="FMP historical key-metrics/ratios period，annual 或 quarter。"),
    ] = "annual",
    limit: Annotated[
        int,
        typer.Option(help="每个 ticker 拉取的历史记录数，至少 3。"),
    ] = 5,
) -> None:
    """从 FMP 拉取 historical key-metrics/ratios，回填估值分位历史。"""
    fetch_date = _parse_date(as_of) if as_of else date.today()
    selected_tickers = (
        _parse_csv_items(tickers)
        if tickers
        else load_universe().ai_chain.get("core_watchlist", [])
    )
    api_key = os.getenv(api_key_env)
    if not api_key:
        console.print(f"[red]未找到环境变量 {api_key_env}，已停止 FMP 历史估值拉取。[/red]")
        raise typer.Exit(code=1)

    fetch_report_output = output_path or default_fmp_historical_valuation_fetch_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )
    validation_output = validation_report_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )

    try:
        fetch_report = fetch_fmp_historical_valuation_snapshots(
            selected_tickers,
            api_key,
            fetch_date,
            captured_at=fetch_date,
            period=period,
            limit=limit,
        )
    except ValueError as exc:
        console.print(f"[red]FMP 历史估值参数错误：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    write_fmp_historical_valuation_fetch_report(fetch_report, fetch_report_output)
    status_style = (
        "green" if fetch_report.status == "PASS" else "yellow" if fetch_report.passed else "red"
    )
    console.print(
        f"[{status_style}]FMP 历史估值拉取状态："
        f"{fetch_report.status}[/{status_style}]"
    )
    console.print(f"拉取报告：{fetch_report_output}")
    console.print(
        f"请求标的：{', '.join(fetch_report.requested_tickers)}；"
        f"返回记录：{fetch_report.row_count}；生成历史快照：{fetch_report.imported_count}"
    )
    console.print(f"错误数：{fetch_report.error_count}；警告数：{fetch_report.warning_count}")
    if not fetch_report.passed:
        raise typer.Exit(code=1)

    raw_paths = write_fmp_historical_valuation_raw_payloads(
        fetch_report.raw_payloads,
        raw_output_dir,
    )
    written_paths = write_valuation_snapshots_as_yaml(fetch_report.snapshots, output_dir)
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(output_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=fetch_date,
    )
    write_valuation_validation_report(validation_report, validation_output)

    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(f"写入原始历史 payload：{len(raw_paths)} 个文件 -> {raw_output_dir}")
    console.print(f"写入历史估值 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(
        f"[{validation_style}]估值快照校验状态："
        f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("validate-fmp-history")
def validate_fmp_analyst_history_command(
    input_path: Annotated[
        Path,
        typer.Option(help="FMP analyst-estimates 原始历史快照目录或 JSON 文件。"),
    ] = DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR,
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔 ticker；未提供时校验全部历史快照。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown FMP analyst history 校验报告输出路径。"),
    ] = None,
    max_snapshot_age_days: Annotated[
        int,
        typer.Option(help="历史快照新鲜度警告阈值，单位天。"),
    ] = 7,
) -> None:
    """校验 FMP analyst-estimates 原始历史缓存。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_fmp_analyst_history_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    selected_tickers = _parse_csv_items(tickers) if tickers else None
    try:
        report = validate_fmp_analyst_estimate_history(
            input_path,
            validation_date,
            tickers=selected_tickers,
            max_snapshot_age_days=max_snapshot_age_days,
        )
    except ValueError as exc:
        console.print(f"[red]FMP analyst history 参数错误：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    write_fmp_analyst_history_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]FMP analyst history 校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"快照数量：{report.snapshot_count}；覆盖标的：{report.ticker_count}；"
        f"记录数：{report.record_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("import-csv")
def import_valuation_csv(
    input_path: Annotated[
        Path,
        typer.Option(help="估值、预期和拥挤度结构化 CSV 输入路径。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(help="写入估值快照 YAML 的目录。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    as_of: Annotated[
        str | None,
        typer.Option(help="导入后校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown CSV 导入报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值快照校验报告输出路径。"),
    ] = None,
) -> None:
    """导入结构化估值 CSV，并写入可审计估值快照 YAML。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    import_report_output = output_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"valuation_import_{import_date}.md"
    )
    validation_output = validation_report_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        import_date,
    )
    import_report = import_valuation_snapshots_from_csv(input_path)
    write_valuation_csv_import_report(import_report, import_report_output)

    status_style = (
        "green" if import_report.status == "PASS" else "yellow" if import_report.passed else "red"
    )
    console.print(f"[{status_style}]估值 CSV 导入状态：{import_report.status}[/{status_style}]")
    console.print(f"导入报告：{import_report_output}")
    console.print(
        f"CSV 行数：{import_report.row_count}；导入快照：{import_report.imported_count}"
    )
    console.print(f"错误数：{import_report.error_count}；警告数：{import_report.warning_count}")
    if not import_report.passed:
        raise typer.Exit(code=1)

    written_paths = write_valuation_snapshots_as_yaml(import_report.snapshots, output_dir)
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(output_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=import_date,
    )
    write_valuation_validation_report(validation_report, validation_output)

    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow"
        if validation_report.passed
        else "red"
    )
    console.print(f"写入 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(
        f"[{validation_style}]估值快照校验状态："
        f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("list")
def list_valuations(
    input_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
) -> None:
    """列出估值、预期和拥挤度快照。"""
    store = load_valuation_snapshot_store(input_path)

    table = Table(title="估值与拥挤度快照")
    table.add_column("Snapshot")
    table.add_column("Ticker")
    table.add_column("日期")
    table.add_column("来源")
    table.add_column("估值分位")
    table.add_column("评估")
    table.add_column("文件")

    for loaded in sorted(store.loaded, key=lambda item: item.snapshot.snapshot_id):
        snapshot = loaded.snapshot
        percentile = (
            "n/a"
            if snapshot.valuation_percentile is None
            else f"{snapshot.valuation_percentile:.0f}"
        )
        table.add_row(
            snapshot.snapshot_id,
            snapshot.ticker,
            snapshot.as_of.isoformat(),
            _valuation_source_type_label(snapshot.source_type),
            percentile,
            _valuation_assessment_label(snapshot.overall_assessment),
            str(loaded.path),
        )

    console.print(table)
    if not store.loaded:
        console.print("未发现可读取的估值快照。")
    if store.load_errors:
        console.print(
            f"[red]存在 {len(store.load_errors)} 个加载错误，请运行 validate 查看。[/red]"
        )


@valuation_app.command("validate")
def validate_valuations(
    input_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值校验报告输出路径。"),
    ] = None,
) -> None:
    """校验估值、预期和拥挤度快照。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(input_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=validation_date,
    )
    write_valuation_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]估值快照校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"快照数量：{report.snapshot_count}；覆盖标的：{report.ticker_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("review")
def review_valuations(
    input_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值复核报告输出路径。"),
    ] = None,
) -> None:
    """复核估值、预期和拥挤度快照。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_valuation_review_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(input_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=review_date,
    )
    review_report = build_valuation_review_report(validation_report)
    write_valuation_review_report(review_report, report_path)

    status_style = "green" if review_report.status == "PASS" else (
        "yellow" if validation_report.passed else "red"
    )
    console.print(f"[{status_style}]估值复核状态：{review_report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"快照数量：{validation_report.snapshot_count}；"
        f"覆盖标的：{validation_report.ticker_count}"
    )
    console.print(
        f"校验错误数：{validation_report.error_count}；"
        f"校验警告数：{validation_report.warning_count}"
    )

    if not validation_report.passed:
        raise typer.Exit(code=1)


@app.command("review-trades")
def review_trades(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    input_path: Annotated[
        Path,
        typer.Option(help="交易记录 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT / "data" / "external" / "trades",
    as_of: Annotated[
        str | None,
        typer.Option(help="复盘日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    benchmarks: Annotated[
        str,
        typer.Option(help="逗号分隔的归因基准标的。"),
    ] = ",".join(DEFAULT_BENCHMARK_TICKERS),
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 交易复盘报告输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验配置中的完整 AI 产业链标的，而不只校验核心观察池。",
        ),
    ] = False,
) -> None:
    """通过数据质量门禁后复盘交易记录并做基础基准归因。"""
    universe = load_universe()
    review_date = _parse_date(as_of) if as_of else date.today()
    benchmark_tickers = _parse_csv_items(benchmarks)
    if not benchmark_tickers:
        raise typer.BadParameter("至少需要一个归因基准标的。")

    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    report_output = output_path or default_trade_review_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(
            universe,
            include_full_ai_chain=full_universe,
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=review_date,
        manifest_path=_download_manifest_path(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止交易复盘。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    validation_report = validate_trade_record_store(
        store=load_trade_record_store(input_path),
        universe=universe,
        watchlist=load_watchlist(),
        as_of=review_date,
    )
    review_report = build_trade_review_report(
        validation_report=validation_report,
        prices=pd.read_csv(prices_path),
        data_quality_report=data_quality_report,
        benchmark_tickers=tuple(benchmark_tickers),
    )
    write_trade_review_report(
        review_report,
        data_quality_report_path=quality_output,
        output_path=report_output,
    )

    status_style = "green" if review_report.status == "PASS" else (
        "yellow" if validation_report.passed else "red"
    )
    console.print(f"[{status_style}]交易复盘状态：{review_report.status}[/{status_style}]")
    console.print(f"报告：{report_output}")
    console.print(f"交易数：{validation_report.trade_count}")
    console.print(
        f"校验错误数：{validation_report.error_count}；"
        f"校验警告数：{validation_report.warning_count}"
    )
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")

    if not validation_report.passed:
        raise typer.Exit(code=1)


@app.command("build-features")
def build_features(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="特征日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path,
        typer.Option(help="特征 CSV 输出路径。"),
    ] = PROJECT_ROOT / "data" / "processed" / "features_daily.csv",
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 特征摘要输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="校验并构建配置中的完整 AI 产业链标的特征。",
        ),
    ] = False,
) -> None:
    """通过数据质量门禁后构建每日市场特征。"""
    universe = load_universe()
    data_quality_config = load_data_quality()
    feature_config = load_features()
    feature_date = _parse_date(as_of) if as_of else date.today()
    expected_price_tickers = configured_price_tickers(
        universe,
        include_full_ai_chain=full_universe,
    )
    expected_rate_series = configured_rate_series(universe)
    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        feature_date,
    )
    feature_report_output = report_path or default_feature_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        feature_date,
    )

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_price_tickers,
        expected_rate_series=expected_rate_series,
        quality_config=data_quality_config,
        as_of=feature_date,
        manifest_path=_download_manifest_path(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止特征构建。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    feature_set = build_market_features(
        prices=pd.read_csv(prices_path),
        rates=pd.read_csv(rates_path),
        config=feature_config,
        as_of=feature_date,
        core_watchlist=universe.ai_chain.get("core_watchlist", []),
    )
    features_output = write_features_csv(feature_set, output_path)
    feature_summary_output = write_feature_summary(
        feature_set,
        data_quality_report=data_quality_report,
        data_quality_report_path=quality_output,
        features_path=features_output,
        output_path=feature_report_output,
    )

    status_style = "green" if feature_set.status == "PASS" else "yellow"
    console.print(f"[{status_style}]特征构建状态：{feature_set.status}[/{status_style}]")
    console.print(f"特征数据：{features_output}（{feature_date} 共 {len(feature_set.rows)} 行）")
    console.print(f"特征摘要：{feature_summary_output}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")
    console.print(f"特征警告数：{len(feature_set.warnings)}")


@app.command("score-daily")
def score_daily(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="评分日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    features_path: Annotated[
        Path,
        typer.Option(help="特征 CSV 输出路径。"),
    ] = PROJECT_ROOT / "data" / "processed" / "features_daily.csv",
    scores_path: Annotated[
        Path,
        typer.Option(help="评分 CSV 输出路径。"),
    ] = PROJECT_ROOT / "data" / "processed" / "scores_daily.csv",
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 每日评分报告输出路径。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="JSON evidence bundle 输出路径。"),
    ] = None,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="JSON 决策快照输出路径。"),
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
    execution_policy_path: Annotated[
        Path,
        typer.Option(help="execution policy YAML 路径，用于日报执行建议。"),
    ] = DEFAULT_EXECUTION_POLICY_CONFIG_PATH,
    execution_policy_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown execution policy 校验报告输出路径。"),
    ] = None,
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
    thesis_path: Annotated[
        Path,
        typer.Option(help="交易 thesis YAML 文件或目录路径，用于写入日报复核摘要。"),
    ] = PROJECT_ROOT / "data" / "external" / "trade_theses",
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
    valuation_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径，用于写入日报复核摘要。"),
    ] = PROJECT_ROOT / "data" / "external" / "valuation_snapshots",
    trades_path: Annotated[
        Path,
        typer.Option(help="交易记录 YAML 文件或目录路径，用于写入日报复盘摘要。"),
    ] = PROJECT_ROOT / "data" / "external" / "trades",
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
    score_date = _parse_date(as_of) if as_of else date.today()
    benchmark_tickers = tuple(_parse_csv_items(review_benchmarks))
    if not benchmark_tickers:
        raise typer.BadParameter("日报交易复盘至少需要一个归因基准标的。")
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
    score_report_output = report_path or default_daily_score_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        score_date,
    )
    daily_trace_output = trace_bundle_path or default_report_trace_bundle_path(
        score_report_output
    )
    decision_snapshot_output = decision_snapshot_path or default_decision_snapshot_path(
        DEFAULT_DECISION_SNAPSHOT_DIR,
        score_date,
    )
    belief_state_output = belief_state_path or default_belief_state_path(
        DEFAULT_BELIEF_STATE_DIR,
        score_date,
    )
    belief_state_history_output = (
        belief_state_history_path or DEFAULT_BELIEF_STATE_HISTORY_PATH
    )
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

    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_price_tickers,
        expected_rate_series=expected_rate_series,
        quality_config=data_quality_config,
        as_of=score_date,
        manifest_path=_download_manifest_path(prices_path),
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
    feature_set = build_market_features(
        prices=prices_frame,
        rates=rates_frame,
        config=feature_config,
        as_of=score_date,
        core_watchlist=universe.ai_chain.get("core_watchlist", []),
    )
    features_output = write_features_csv(feature_set, features_path)
    feature_summary_output = write_feature_summary(
        feature_set,
        data_quality_report=data_quality_report,
        data_quality_report_path=quality_output,
        features_path=features_output,
        output_path=feature_report_output,
    )
    sec_companies = load_sec_companies(sec_companies_path)
    sec_metrics = load_fundamental_metrics(sec_metrics_path)
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
    if not sec_fundamental_feature_report.passed:
        write_sec_fundamental_features_report(
            report=sec_fundamental_feature_report,
            validation_report_path=sec_metrics_validation_output,
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
        validation_report_path=sec_metrics_validation_output,
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

    review_summary = _build_daily_review_summary(
        thesis_path=thesis_path,
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
    score_report = build_daily_score_report(
        feature_set=feature_set,
        data_quality_report=data_quality_report,
        rules=scoring_rules,
        total_risk_asset_min=portfolio.portfolio.total_risk_asset_min,
        total_risk_asset_max=portfolio.portfolio.total_risk_asset_max,
        max_total_ai_exposure=portfolio.position_limits.max_total_ai_exposure,
        review_summary=review_summary,
        fundamental_feature_report=sec_fundamental_feature_report,
        valuation_review_report=valuation_review_report,
        risk_event_occurrence_review_report=risk_event_occurrence_review_report,
    )
    previous_score_snapshot = load_previous_daily_score_snapshot(scores_path, score_date)
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
    daily_config_paths = _daily_trace_config_paths(
        sec_companies_path=sec_companies_path,
        sec_metrics_path=sec_metrics_path,
        fundamental_feature_config_path=fundamental_feature_config_path,
        risk_events_path=risk_events_path,
        execution_policy_path=execution_policy_path,
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
        sec_metrics_validation_report_path=sec_metrics_validation_output,
        sec_fundamental_feature_report_path=sec_fundamental_feature_report_output,
        sec_fundamental_features_path=sec_fundamental_features_output,
        risk_event_occurrence_report_path=risk_event_occurrence_report_output,
        belief_state_path=belief_state_output,
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
    daily_decision_snapshot_output = write_decision_snapshot(
        build_decision_snapshot(
            report=score_report,
            trace_bundle_path=daily_trace_output,
            market_regime=daily_market_regime,
            config_paths=daily_config_paths,
            belief_state_path=belief_state_output,
        ),
        decision_snapshot_output,
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
        previous_score_snapshot=previous_score_snapshot,
        belief_state_section=render_belief_state_summary(
            belief_state,
            belief_state_output,
        ),
        execution_action_label=execution_advisory.label,
        execution_action_id=execution_advisory.action_id,
        execution_advisory_section=execution_advisory_section,
        traceability_section=render_traceability_section(
            daily_trace_bundle,
            daily_trace_output,
        ),
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
    console.print(f"每日评分报告：{daily_report_output}")
    console.print(f"Evidence bundle：{daily_trace_output}")
    console.print(f"Decision snapshot：{daily_decision_snapshot_output}")
    console.print(f"Belief state：{belief_state_output}")
    console.print(f"Belief state history：{belief_state_history_output}")
    console.print(f"评分数据：{scores_output}")
    console.print(f"特征摘要：{feature_summary_output}")
    console.print(
        f"SEC 基本面特征：{sec_fundamental_features_output}"
        f"（{sec_fundamental_feature_report.status}）"
    )
    console.print(
        f"风险事件发生记录：{risk_event_occurrence_report_output}"
        f"（{risk_event_occurrence_review_report.status}）"
    )
    console.print(
        f"执行政策报告：{execution_policy_report_output}"
        f"（{execution_policy_report.status}）"
    )
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")


def _base_trace_config_paths() -> dict[str, Path]:
    return {
        "universe": DEFAULT_CONFIG_PATH,
        "portfolio": DEFAULT_PORTFOLIO_CONFIG_PATH,
        "data_quality": DEFAULT_DATA_QUALITY_CONFIG_PATH,
        "features": DEFAULT_FEATURE_CONFIG_PATH,
        "scoring_rules": DEFAULT_SCORING_RULES_CONFIG_PATH,
        "watchlist": DEFAULT_WATCHLIST_CONFIG_PATH,
        "watchlist_lifecycle": DEFAULT_WATCHLIST_LIFECYCLE_PATH,
        "industry_chain": DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
        "data_sources": DEFAULT_DATA_SOURCES_CONFIG_PATH,
    }


def _daily_trace_config_paths(
    *,
    sec_companies_path: Path,
    sec_metrics_path: Path,
    fundamental_feature_config_path: Path,
    risk_events_path: Path,
    execution_policy_path: Path,
) -> dict[str, Path]:
    return {
        **_base_trace_config_paths(),
        "market_regimes": DEFAULT_MARKET_REGIMES_CONFIG_PATH,
        "sec_companies": sec_companies_path,
        "fundamental_metrics": sec_metrics_path,
        "fundamental_features": fundamental_feature_config_path,
        "risk_events": risk_events_path,
        "execution_policy": execution_policy_path,
    }


def _backtest_trace_config_paths(
    *,
    regimes_path: Path,
    benchmark_policy_path: Path,
    sec_companies_path: Path,
    sec_metrics_path: Path,
    fundamental_feature_config_path: Path,
    risk_events_path: Path,
    watchlist_lifecycle_path: Path,
) -> dict[str, Path]:
    return {
        **_base_trace_config_paths(),
        "market_regimes": regimes_path,
        "benchmark_policy": benchmark_policy_path,
        "sec_companies": sec_companies_path,
        "fundamental_metrics": sec_metrics_path,
        "fundamental_features": fundamental_feature_config_path,
        "risk_events": risk_events_path,
        "watchlist_lifecycle": watchlist_lifecycle_path,
    }


def _build_daily_review_summary(
    thesis_path: Path,
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
            watchlist=watchlist,
            industry_chain=industry_chain,
            score_date=score_date,
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


def _build_daily_thesis_status(
    input_path: Path,
    watchlist: WatchlistConfig,
    industry_chain: IndustryChainConfig,
    score_date: date,
) -> DailyManualReviewStatus:
    validation_report = validate_trade_thesis_store(
        store=load_trade_thesis_store(input_path),
        watchlist=watchlist,
        industry_chain=industry_chain,
        as_of=score_date,
    )
    review_report = build_thesis_review_report(validation_report)

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
    warning_count = (
        rules_validation_report.warning_count + occurrence_validation.warning_count
    )
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


def _backtest_signal_dates(
    prices: pd.DataFrame,
    strategy_ticker: str,
    start: date,
    end: date,
) -> tuple[date, ...]:
    required_columns = {"date", "ticker", "adj_close"}
    missing = sorted(required_columns - set(prices.columns))
    if missing:
        raise typer.BadParameter(f"价格数据缺少必需字段：{', '.join(missing)}")

    frame = prices.loc[prices["ticker"].astype(str) == strategy_ticker].copy()
    if frame.empty:
        raise typer.BadParameter(f"回测缺少策略代理标的价格：{strategy_ticker}")

    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()].sort_values("_date")
    timestamps = list(frame["_date"])
    signal_dates: list[date] = []
    for index in range(len(timestamps) - 1):
        signal_date = pd.Timestamp(timestamps[index]).date()
        return_date = pd.Timestamp(timestamps[index + 1]).date()
        if signal_date >= start and return_date <= end:
            signal_dates.append(signal_date)
    if not signal_dates:
        raise typer.BadParameter("回测区间内没有可用的下一交易日收益")
    return tuple(signal_dates)


def _build_backtest_sec_fundamental_feature_reports(
    signal_dates: tuple[date, ...],
    sec_companies_path: Path,
    sec_metrics_path: Path,
    fundamental_feature_config_path: Path,
    sec_companyfacts_dir: Path,
    tsm_ir_input_path: Path,
    validation_as_of: date,
    validation_report_output: Path,
) -> dict[date, SecFundamentalFeaturesReport]:
    sec_companies = load_sec_companies(sec_companies_path)
    sec_metrics = load_fundamental_metrics(sec_metrics_path)
    feature_config = load_fundamental_features(fundamental_feature_config_path)
    companyfacts_validation_report = validate_sec_companyfacts_cache(
        sec_companies,
        input_dir=sec_companyfacts_dir,
        as_of=validation_as_of,
    )
    write_sec_companyfacts_validation_report(
        companyfacts_validation_report,
        validation_report_output,
    )
    if not companyfacts_validation_report.passed:
        console.print("[red]SEC companyfacts 缓存校验失败，已停止回测。[/red]")
        console.print(f"SEC companyfacts 校验报告：{validation_report_output}")
        console.print(
            f"错误数：{companyfacts_validation_report.error_count}；"
            f"警告数：{companyfacts_validation_report.warning_count}"
        )
        raise typer.Exit(code=1)

    tsm_ir_enabled = any(
        company.active
        and company.ticker.upper() == "TSM"
        and "quarterly" in company.sec_metric_periods
        for company in sec_companies.companies
    )
    tsm_ir_rows: tuple[TsmIrQuarterlyMetricRow, ...] = (
        load_tsm_ir_quarterly_metric_rows_csv(tsm_ir_input_path)
        if tsm_ir_enabled and tsm_ir_input_path.exists()
        else tuple()
    )
    reports: dict[date, SecFundamentalFeaturesReport] = {}
    for signal_date in signal_dates:
        metrics_report = build_sec_fundamental_metrics_report(
            companies=sec_companies,
            metrics=sec_metrics,
            input_dir=sec_companyfacts_dir,
            as_of=signal_date,
            validation_report=companyfacts_validation_report,
        )
        if not metrics_report.passed:
            console.print("[red]SEC point-in-time 指标抽取失败，已停止回测。[/red]")
            console.print(f"失败日期：{signal_date.isoformat()}")
            console.print(
                f"错误数：{metrics_report.error_count}；"
                f"警告数：{metrics_report.warning_count}"
            )
            raise typer.Exit(code=1)
        metric_rows = metrics_report.rows
        if tsm_ir_rows:
            try:
                metric_rows = merge_tsm_ir_quarterly_rows_into_sec_metrics_as_of(
                    existing_rows=metric_rows,
                    tsm_rows=tsm_ir_rows,
                    tsm_company=sec_companies,
                    as_of=signal_date,
                )
            except ValueError as exc:
                console.print(
                    "[red]TSMC IR point-in-time 指标合并失败，已停止回测。[/red]"
                )
                console.print(f"失败日期：{signal_date.isoformat()}")
                console.print(str(exc))
                raise typer.Exit(code=1) from exc
        metrics_source_path = (
            sec_companyfacts_dir / f"point_in_time_metrics_{signal_date.isoformat()}.csv"
        )
        metrics_validation_report = validate_sec_fundamental_metric_rows(
            companies=sec_companies,
            metrics=sec_metrics,
            rows=metric_rows,
            source_path=metrics_source_path,
            as_of=signal_date,
        )
        if not metrics_validation_report.passed:
            console.print("[red]SEC point-in-time 指标校验失败，已停止回测。[/red]")
            console.print(f"失败日期：{signal_date.isoformat()}")
            console.print(
                f"错误数：{metrics_validation_report.error_count}；"
                f"警告数：{metrics_validation_report.warning_count}"
            )
            raise typer.Exit(code=1)

        feature_report = build_sec_fundamental_features_report_from_metric_rows(
            companies=sec_companies,
            feature_config=feature_config,
            metric_rows=metric_rows,
            source_path=metrics_source_path,
            as_of=signal_date,
            validation_report=metrics_validation_report,
        )
        if not feature_report.passed:
            console.print("[red]SEC point-in-time 基本面特征构建失败，已停止回测。[/red]")
            console.print(f"失败日期：{signal_date.isoformat()}")
            console.print(
                f"错误数：{feature_report.error_count}；"
                f"警告数：{feature_report.warning_count}"
            )
            raise typer.Exit(code=1)
        reports[signal_date] = feature_report
    return reports


def _build_backtest_valuation_review_reports(
    signal_dates: tuple[date, ...],
    valuation_path: Path,
    universe: UniverseConfig,
    watchlist: WatchlistConfig,
) -> dict[date, ValuationReviewReport]:
    store = load_valuation_snapshot_store(valuation_path)
    reports: dict[date, ValuationReviewReport] = {}
    for signal_date in signal_dates:
        report = build_historical_valuation_review_report(
            store=store,
            universe=universe,
            watchlist=watchlist,
            as_of=signal_date,
        )
        if not report.validation_report.passed:
            console.print("[red]point-in-time 估值快照校验失败，已停止回测。[/red]")
            console.print(f"失败日期：{signal_date.isoformat()}")
            console.print(
                f"错误数：{report.validation_report.error_count}；"
                f"警告数：{report.validation_report.warning_count}"
            )
            raise typer.Exit(code=1)
        reports[signal_date] = report
    return reports


def _build_backtest_risk_event_occurrence_review_reports(
    signal_dates: tuple[date, ...],
    risk_events_path: Path,
    risk_event_occurrences_path: Path,
    universe: UniverseConfig,
    industry_chain: IndustryChainConfig,
    watchlist: WatchlistConfig,
    validation_as_of: date,
) -> dict[date, RiskEventOccurrenceReviewReport]:
    risk_events_config = load_risk_events(risk_events_path)
    rules_validation_report = validate_risk_events_config(
        risk_events=risk_events_config,
        industry_chain=industry_chain,
        watchlist=watchlist,
        universe=universe,
        as_of=validation_as_of,
    )
    if not rules_validation_report.passed:
        console.print("[red]风险事件规则校验失败，已停止回测。[/red]")
        console.print(
            f"错误数：{rules_validation_report.error_count}；"
            f"警告数：{rules_validation_report.warning_count}"
        )
        raise typer.Exit(code=1)

    store = load_risk_event_occurrence_store(risk_event_occurrences_path)
    reports: dict[date, RiskEventOccurrenceReviewReport] = {}
    for signal_date in signal_dates:
        report = build_historical_risk_event_occurrence_review_report(
            store=store,
            risk_events=risk_events_config,
            as_of=signal_date,
        )
        if not report.validation_report.passed:
            console.print(
                "[red]point-in-time 风险事件发生记录校验失败，已停止回测。[/red]"
            )
            console.print(f"失败日期：{signal_date.isoformat()}")
            console.print(
                f"错误数：{report.validation_report.error_count}；"
                f"警告数：{report.validation_report.warning_count}"
            )
            raise typer.Exit(code=1)
        reports[signal_date] = report
    return reports


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


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


def _risk_level_label(level: str) -> str:
    return {
        "low": "低",
        "medium": "中",
        "high": "高",
        "critical": "极高",
    }.get(level, level)


def _horizon_label(value: str) -> str:
    return {
        "short": "短期",
        "medium": "中期",
        "long": "长期",
    }.get(value, value)


def _relevance_label(value: str) -> str:
    return {
        "low": "低",
        "medium": "中",
        "high": "高",
    }.get(value, value)


def _thesis_direction_label(value: str) -> str:
    return {
        "long": "做多",
        "short": "做空",
        "hedge": "对冲",
        "watch": "观察",
    }.get(value, value)


def _thesis_status_label(value: str) -> str:
    return {
        "draft": "草稿",
        "active": "活跃",
        "warning": "警告",
        "challenged": "受挑战",
        "paused": "暂停",
        "closed": "已关闭",
        "invalidated": "已证伪",
    }.get(value, value)


def _thesis_review_frequency_label(value: str) -> str:
    return {
        "daily": "每日",
        "weekly": "每周",
        "monthly": "每月",
        "quarterly": "每季",
        "event_driven": "事件驱动",
    }.get(value, value)


def _valuation_source_type_label(value: str) -> str:
    return {
        "primary_filing": "一手披露",
        "paid_vendor": "付费供应商",
        "manual_input": "手工录入",
        "public_convenience": "公开便利源",
    }.get(value, value)


def _valuation_assessment_label(value: str) -> str:
    return {
        "cheap": "偏便宜",
        "reasonable": "合理",
        "expensive": "偏贵",
        "extreme": "极端",
        "unknown": "未知",
    }.get(value, value)


def _data_source_type_label(value: str) -> str:
    return {
        "primary_source": "一手来源",
        "paid_vendor": "付费供应商",
        "public_convenience": "公开便利源",
        "manual_input": "手工输入",
    }.get(value, value)


def _data_source_status_label(value: str) -> str:
    return {
        "active": "已启用",
        "planned": "计划中",
        "inactive": "停用",
    }.get(value, value)


if __name__ == "__main__":
    app()
