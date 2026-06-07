from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Annotated, Literal

import pandas as pd
import typer
from rich.console import Console

from ai_trading_system.backtest.audit import (
    build_backtest_audit_report,
    default_backtest_audit_report_path,
    write_backtest_audit_report,
)
from ai_trading_system.backtest.daily import (
    DEFAULT_BENCHMARK_TICKERS,
    BacktestRegimeContext,
    DailyBacktestResult,
    default_backtest_daily_path,
    default_backtest_input_coverage_path,
    default_backtest_report_path,
    prepare_daily_score_backtest_context,
    run_daily_score_backtest,
    write_backtest_daily_csv,
    write_backtest_input_coverage_csv,
    write_backtest_report,
)
from ai_trading_system.backtest.gate_attribution import (
    build_gate_event_attribution_report,
    default_gate_event_attribution_report_path,
    infer_input_coverage_path,
    write_gate_event_attribution_report,
)
from ai_trading_system.backtest.input_gaps import (
    build_backtest_input_gap_report,
    default_backtest_input_gap_report_path,
    write_backtest_input_gap_report,
)
from ai_trading_system.backtest.lag_sensitivity import (
    BacktestLagSensitivityReport,
    BacktestLagSensitivityScenario,
    default_backtest_lag_sensitivity_report_path,
    default_backtest_lag_sensitivity_summary_path,
    write_backtest_lag_sensitivity_report,
    write_backtest_lag_sensitivity_summary,
)
from ai_trading_system.backtest.pit_coverage import (
    build_backtest_pit_coverage_report,
    default_backtest_pit_coverage_report_path,
    write_backtest_pit_coverage_report,
)
from ai_trading_system.backtest.promotion_gate import (
    build_model_promotion_report,
    default_model_promotion_report_path,
    default_model_promotion_summary_path,
    render_model_promotion_report,
    write_model_promotion_report,
    write_model_promotion_summary,
)
from ai_trading_system.backtest.robustness import (
    BacktestRobustnessReport,
    BacktestRobustnessScenario,
    default_backtest_robustness_report_path,
    default_backtest_robustness_summary_path,
    fixed_total_asset_exposure_scenario,
    model_target_no_gate_scenario,
    module_subset_baseline_scenario,
    rebalance_interval_scenario,
    same_exposure_random_scenario,
    same_turnover_random_scenario,
    volatility_targeted_exposure_scenario,
    write_backtest_robustness_report,
    write_backtest_robustness_summary,
)
from ai_trading_system.benchmark_policy import (
    DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
    load_benchmark_policy,
    validate_benchmark_policy,
    write_benchmark_policy_report,
)
from ai_trading_system.cli_commands.risk_event_artifacts import (
    DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
)
from ai_trading_system.cli_commands.trace_config import backtest_trace_config_paths
from ai_trading_system.config import (
    DEFAULT_DATA_SOURCES_CONFIG_PATH,
    DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    DEFAULT_RISK_EVENTS_CONFIG_PATH,
    DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    PROJECT_ROOT,
    IndustryChainConfig,
    ScoringRulesConfig,
    UniverseConfig,
    WatchlistConfig,
    configured_price_tickers,
    configured_rate_series,
    load_backtest_validation_policy,
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
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
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
from ai_trading_system.fundamentals.sec_features import (
    SecFundamentalFeaturesReport,
    build_sec_fundamental_features_report_from_metric_rows,
)
from ai_trading_system.fundamentals.sec_metrics import (
    build_sec_fundamental_metrics_report,
    validate_sec_fundamental_metric_rows,
)
from ai_trading_system.fundamentals.sec_pit_backfill import DEFAULT_SEC_EDGAR_PROCESSED_DIR
from ai_trading_system.fundamentals.sec_pit_panel import (
    sec_pit_feature_panel_to_feature_reports,
)
from ai_trading_system.fundamentals.sec_validation import (
    default_sec_companyfacts_validation_report_path,
    validate_sec_companyfacts_cache,
    write_sec_companyfacts_validation_report,
)
from ai_trading_system.fundamentals.tsm_ir import (
    TsmIrQuarterlyMetricRow,
    load_tsm_ir_quarterly_metric_rows_csv,
    merge_tsm_ir_quarterly_rows_into_sec_metrics_as_of,
)
from ai_trading_system.historical_inputs import (
    build_historical_risk_event_occurrence_review_report,
    build_historical_valuation_review_report,
)
from ai_trading_system.pit_snapshots import (
    DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    validate_pit_snapshot_manifest,
)
from ai_trading_system.prediction_ledger import (
    DEFAULT_PREDICTION_OUTCOMES_PATH,
)
from ai_trading_system.report_traceability import (
    build_backtest_trace_bundle,
    default_report_trace_bundle_path,
    render_traceability_section,
    write_trace_bundle,
)
from ai_trading_system.risk_events import (
    RiskEventOccurrenceReviewReport,
    load_risk_event_occurrence_store,
    validate_risk_events_config,
)
from ai_trading_system.rule_governance import (
    DEFAULT_RULE_CARDS_PATH,
    build_rule_version_manifest,
    load_rule_card_store,
    validate_rule_card_store,
)
from ai_trading_system.scoring.position_model import (
    PositionBandRule,
)
from ai_trading_system.valuation import (
    ValuationReviewReport,
    load_valuation_snapshot_store,
)
from ai_trading_system.watchlist_lifecycle import (
    DEFAULT_WATCHLIST_LIFECYCLE_PATH,
    default_watchlist_lifecycle_report_path,
    load_watchlist_lifecycle,
    validate_watchlist_lifecycle,
    write_watchlist_lifecycle_report,
)
from ai_trading_system.weight_calibration import (
    load_calibration_overlays,
    load_weight_profile,
)

console = Console()


def register_backtest_commands(app: typer.Typer) -> None:
    app.command("backtest")(backtest)
    app.command("backtest-gate-attribution")(backtest_gate_attribution)
    app.command("backtest-input-gaps")(backtest_input_gaps)
    app.command("backtest-pit-coverage")(backtest_pit_coverage)


def backtest(
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
    rule_cards_path: Annotated[
        Path,
        typer.Option(help="rule card registry YAML 路径，用于记录本次回测规则版本。"),
    ] = DEFAULT_RULE_CARDS_PATH,
    cost_bps: Annotated[
        float | None,
        typer.Option(help="单边交易成本，单位 bps；默认读取 backtest validation policy。"),
    ] = None,
    spread_bps: Annotated[
        float,
        typer.Option(help="Bid-ask spread 假设，单位 bps；默认不额外扣除。"),
    ] = 0.0,
    slippage_bps: Annotated[
        float,
        typer.Option(help="线性滑点估算，单位 bps；默认不额外扣除。"),
    ] = 0.0,
    market_impact_bps: Annotated[
        float,
        typer.Option(help="市场冲击估算，单位 bps；默认不额外扣除。"),
    ] = 0.0,
    tax_bps: Annotated[
        float,
        typer.Option(help="卖出侧税费假设，单位 bps；默认不额外扣除。"),
    ] = 0.0,
    fx_bps: Annotated[
        float,
        typer.Option(help="换汇成本假设，单位 bps；默认不额外扣除。"),
    ] = 0.0,
    financing_annual_bps: Annotated[
        float,
        typer.Option(help="融资或持仓资金年化成本，单位 bps；默认不额外扣除。"),
    ] = 0.0,
    etf_delay_bps: Annotated[
        float,
        typer.Option(help="ETF 成交延迟或申赎成本假设，单位 bps；默认不额外扣除。"),
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
        float | None,
        typer.Option(
            help="审计用评分模块最低平均覆盖率阈值；默认读取 backtest validation policy。"
        ),
    ] = None,
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
    robustness_report_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Markdown 回测稳健性报告输出路径；提供时运行成本压力、起点后移、"
                "固定仓位、再平衡频率、趋势基线、权重扰动和买入持有基准对比。"
            ),
        ),
    ] = None,
    robustness_summary_path: Annotated[
        Path | None,
        typer.Option(help="机器可读回测稳健性 JSON 摘要输出路径。"),
    ] = None,
    robustness_report: Annotated[
        bool,
        typer.Option(
            "--robustness-report",
            help="按默认 outputs/backtests 路径生成回测稳健性报告。",
        ),
    ] = False,
    robustness_shift_days: Annotated[
        int | None,
        typer.Option(help="稳健性报告的起点后移天数；默认读取 backtest validation policy。"),
    ] = None,
    robustness_cost_stress_bps: Annotated[
        float | None,
        typer.Option(
            help="稳健性报告中交易执行成本压力的额外 bps；默认读取 backtest validation policy。"
        ),
    ] = None,
    robustness_weight_perturbation_pct: Annotated[
        float | None,
        typer.Option(
            help="稳健性报告中单模块权重上调/下调扰动比例；默认读取 backtest validation policy。"
        ),
    ] = None,
    robustness_random_seed_start: Annotated[
        int | None,
        typer.Option(
            help="稳健性报告中同换手率随机策略的起始随机种子；默认读取 backtest validation policy。"
        ),
    ] = None,
    robustness_random_seed_count: Annotated[
        int | None,
        typer.Option(
            help="稳健性报告中同换手率随机策略的种子数量；默认读取 backtest validation policy。"
        ),
    ] = None,
    robustness_oos_split_ratio: Annotated[
        float | None,
        typer.Option(
            help=(
                "稳健性报告中时间顺序样本外验证的 in-sample 切分比例；"
                "默认读取 backtest validation policy。"
            )
        ),
    ] = None,
    lag_sensitivity_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 回测滞后敏感性报告输出路径。"),
    ] = None,
    lag_sensitivity_summary_path: Annotated[
        Path | None,
        typer.Option(help="机器可读回测滞后敏感性 JSON 摘要输出路径。"),
    ] = None,
    lag_sensitivity_report: Annotated[
        bool,
        typer.Option(
            "--lag-sensitivity-report",
            help="按默认 outputs/backtests 路径生成回测滞后敏感性报告。",
        ),
    ] = False,
    lag_sensitivity_days: Annotated[
        str,
        typer.Option(help="逗号分隔的 feature/universe 滞后交易日列表。"),
    ] = "0,1,3,5,10,20",
    feature_availability_path: Annotated[
        Path,
        typer.Option(help="PIT feature availability catalog YAML 路径。"),
    ] = DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
    feature_availability_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown PIT 特征可见时间报告输出路径。"),
    ] = None,
    promotion_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 模型晋级门槛报告输出路径。"),
    ] = None,
    promotion_summary_path: Annotated[
        Path | None,
        typer.Option(help="机器可读模型晋级门槛 JSON 摘要输出路径。"),
    ] = None,
    promotion_report: Annotated[
        bool,
        typer.Option(
            "--promotion-report",
            help="按默认 outputs/backtests 路径生成模型晋级门槛报告。",
        ),
    ] = False,
    promotion_prediction_outcomes_path: Annotated[
        Path,
        typer.Option(help="prediction/shadow outcome CSV，用于模型晋级门槛评估。"),
    ] = DEFAULT_PREDICTION_OUTCOMES_PATH,
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
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "sec_companyfacts",
    tsm_ir_input_path: Annotated[
        Path,
        typer.Option(help="TSMC IR 季度指标 CSV，用于补齐 TSM point-in-time 回测基本面。"),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "tsm_ir_quarterly_metrics.csv",
    sec_companyfacts_validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown SEC companyfacts 缓存校验报告输出路径。"),
    ] = None,
    sec_fundamental_source: Annotated[
        Literal["legacy_companyfacts", "sec_pit_feature_panel"],
        typer.Option(
            "--sec-fundamental-source",
            help=(
                "回测基本面来源；legacy_companyfacts 使用 filed_date as-of，"
                "sec_pit_feature_panel 使用 TRADING-039 reconstructed PIT panel。"
            ),
        ),
    ] = "legacy_companyfacts",
    sec_pit_feature_panel_path: Annotated[
        Path,
        typer.Option(help="SEC reconstructed PIT feature panel CSV 路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "sec_pit_feature_panel.csv",
    valuation_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径，用于 point-in-time 回测评分。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "valuation_snapshots",
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
    weight_profile = load_weight_profile()
    calibration_overlays = load_calibration_overlays()
    backtest_validation_policy = load_backtest_validation_policy()
    robustness_policy = backtest_validation_policy.robustness
    cost_bps = (
        cost_bps
        if cost_bps is not None
        else backtest_validation_policy.execution_costs.default_cost_bps
    )
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
    robustness_shift_days = (
        robustness_shift_days
        if robustness_shift_days is not None
        else robustness_policy.default_shifted_start_days
    )
    robustness_cost_stress_bps = (
        robustness_cost_stress_bps
        if robustness_cost_stress_bps is not None
        else robustness_policy.default_cost_stress_increment_bps
    )
    robustness_weight_perturbation_pct = (
        robustness_weight_perturbation_pct
        if robustness_weight_perturbation_pct is not None
        else robustness_policy.default_weight_perturbation_pct
    )
    robustness_random_seed_start = (
        robustness_random_seed_start
        if robustness_random_seed_start is not None
        else robustness_policy.default_random_seed_start
    )
    robustness_random_seed_count = (
        robustness_random_seed_count
        if robustness_random_seed_count is not None
        else robustness_policy.default_random_seed_count
    )
    robustness_oos_split_ratio = (
        robustness_oos_split_ratio
        if robustness_oos_split_ratio is not None
        else robustness_policy.default_oos_split_ratio
    )
    minimum_component_coverage = (
        minimum_component_coverage
        if minimum_component_coverage is not None
        else backtest_validation_policy.data_credibility.component_coverage_min
    )
    if not benchmark_tickers:
        raise typer.BadParameter("至少需要一个基准标的。")
    if not 0.0 <= minimum_component_coverage <= 1.0:
        raise typer.BadParameter("审计覆盖率阈值必须在 0 到 1 之间。")
    if robustness_shift_days <= 0:
        raise typer.BadParameter("稳健性报告起点后移天数必须为正数。")
    if robustness_cost_stress_bps < 0:
        raise typer.BadParameter("稳健性报告成本压力 bps 不能为负数。")
    if not 0 < robustness_weight_perturbation_pct < 1:
        raise typer.BadParameter("稳健性报告权重扰动比例必须大于 0 且小于 1。")
    if robustness_random_seed_count <= 0:
        raise typer.BadParameter("同换手率随机策略种子数量必须为正数。")
    if not 0 < robustness_oos_split_ratio < 1:
        raise typer.BadParameter("样本外验证 in-sample 切分比例必须大于 0 且小于 1。")
    lag_days = _parse_backtest_lag_days(lag_sensitivity_days)
    should_run_promotion = (
        promotion_report or promotion_report_path is not None or promotion_summary_path is not None
    )
    should_write_promotion_markdown = promotion_report or promotion_report_path is not None
    should_run_robustness = (
        robustness_report
        or robustness_report_path is not None
        or robustness_summary_path is not None
        or should_run_promotion
    )
    should_write_robustness_markdown = robustness_report or robustness_report_path is not None
    should_run_lag_sensitivity = (
        lag_sensitivity_report
        or lag_sensitivity_report_path is not None
        or lag_sensitivity_summary_path is not None
        or should_run_promotion
    )
    should_write_lag_sensitivity_markdown = (
        lag_sensitivity_report or lag_sensitivity_report_path is not None
    )
    rule_governance_report = validate_rule_card_store(
        load_rule_card_store(rule_cards_path),
        as_of=date.today(),
    )
    if not rule_governance_report.passed:
        console.print("[red]规则治理校验失败，已停止回测。[/red]")
        console.print(
            f"错误数：{rule_governance_report.error_count}；"
            f"警告数：{rule_governance_report.warning_count}"
        )
        raise typer.Exit(code=1)
    backtest_rule_version_manifest = build_rule_version_manifest(
        rule_governance_report,
        applies_to="backtest",
    )
    if backtest_rule_version_manifest["production_rule_count"] == 0:
        console.print("[red]未找到适用于 backtest 的 production rule card，已停止回测。[/red]")
        raise typer.Exit(code=1)
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
    backtest_robustness_output = robustness_report_path or default_backtest_robustness_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        start_date,
        end_date,
    )
    backtest_robustness_summary_output = robustness_summary_path or (
        backtest_robustness_output.with_suffix(".json")
        if should_write_robustness_markdown
        else default_backtest_robustness_summary_path(
            PROJECT_ROOT / "outputs" / "backtests",
            start_date,
            end_date,
        )
    )
    backtest_lag_sensitivity_output = (
        lag_sensitivity_report_path
        or default_backtest_lag_sensitivity_report_path(
            PROJECT_ROOT / "outputs" / "backtests",
            start_date,
            end_date,
        )
    )
    backtest_lag_sensitivity_summary_output = lag_sensitivity_summary_path or (
        backtest_lag_sensitivity_output.with_suffix(".json")
        if should_write_lag_sensitivity_markdown
        else default_backtest_lag_sensitivity_summary_path(
            PROJECT_ROOT / "outputs" / "backtests",
            start_date,
            end_date,
        )
    )
    backtest_feature_availability_output = (
        feature_availability_report_path
        or default_feature_availability_report_path(
            PROJECT_ROOT / "outputs" / "backtests",
            quality_date,
        )
    )
    backtest_promotion_output = promotion_report_path or default_model_promotion_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        start_date,
        end_date,
    )
    backtest_promotion_summary_output = promotion_summary_path or (
        backtest_promotion_output.with_suffix(".json")
        if promotion_report or promotion_report_path is not None
        else default_model_promotion_summary_path(
            PROJECT_ROOT / "outputs" / "backtests",
            start_date,
            end_date,
        )
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
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
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
            decision_time=quality_date,
        )
    )
    if sec_fundamental_source == "sec_pit_feature_panel":
        feature_source_checks.append(
            _sec_pit_feature_source_check(
                feature_panel_path=sec_pit_feature_panel_path,
                decision_time=quality_date,
            )
        )
    backtest_feature_availability_report = build_feature_availability_report(
        input_path=feature_availability_path,
        as_of=quality_date,
        observed_sources=(
            "prices_daily",
            "rates_daily",
            "watchlist_lifecycle",
            fundamental_source_id,
            "valuation_snapshots",
            "risk_event_occurrences",
        ),
        required_sources=(
            "prices_daily",
            "rates_daily",
            "watchlist_lifecycle",
            fundamental_source_id,
            "valuation_snapshots",
            "risk_event_occurrences",
        ),
        source_checks=tuple(feature_source_checks),
    )
    write_feature_availability_report(
        backtest_feature_availability_report,
        backtest_feature_availability_output,
    )
    if not backtest_feature_availability_report.passed:
        console.print("[red]PIT 特征可见时间校验失败，已停止回测。[/red]")
        console.print(f"PIT 特征可见时间报告：{backtest_feature_availability_output}")
        console.print(
            f"错误数：{backtest_feature_availability_report.error_count}；"
            f"警告数：{backtest_feature_availability_report.warning_count}"
        )
        raise typer.Exit(code=1)
    backtest_feature_availability_section = render_feature_availability_section(
        backtest_feature_availability_report,
        backtest_feature_availability_output,
    )

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
    input_signal_dates = (
        _backtest_required_input_signal_dates(
            prices=prices_frame,
            strategy_ticker=strategy_ticker,
            start=start_date,
            end=end_date,
            lag_days=lag_days,
        )
        if should_run_lag_sensitivity
        else signal_dates
    )
    sec_fundamental_feature_reports = (
        _build_backtest_sec_pit_feature_reports(
            signal_dates=input_signal_dates,
            sec_companies_path=sec_companies_path,
            sec_pit_feature_panel_path=sec_pit_feature_panel_path,
        )
        if sec_fundamental_source == "sec_pit_feature_panel"
        else _build_backtest_sec_fundamental_feature_reports(
            signal_dates=input_signal_dates,
            sec_companies_path=sec_companies_path,
            sec_metrics_path=sec_metrics_path,
            fundamental_feature_config_path=fundamental_feature_config_path,
            sec_companyfacts_dir=sec_companyfacts_dir,
            tsm_ir_input_path=tsm_ir_input_path,
            validation_as_of=quality_date,
            validation_report_output=sec_companyfacts_validation_output,
        )
    )
    fundamental_validation_report_output = (
        None
        if sec_fundamental_source == "sec_pit_feature_panel"
        else sec_companyfacts_validation_output
    )
    valuation_review_reports = _build_backtest_valuation_review_reports(
        signal_dates=input_signal_dates,
        valuation_path=valuation_path,
        universe=universe,
        watchlist=watchlist,
    )
    risk_event_occurrence_review_reports = _build_backtest_risk_event_occurrence_review_reports(
        signal_dates=input_signal_dates,
        risk_events_path=risk_events_path,
        risk_event_occurrences_path=risk_event_occurrences_path,
        universe=universe,
        industry_chain=industry_chain,
        watchlist=watchlist,
        validation_as_of=quality_date,
    )

    backtest_regime_context = BacktestRegimeContext(
        regime_id=selected_regime.regime_id,
        name=selected_regime.name,
        start_date=selected_regime.start_date,
        anchor_date=selected_regime.anchor_date,
        anchor_event=selected_regime.anchor_event,
        description=selected_regime.description,
    )
    base_prepared_context = prepare_daily_score_backtest_context(
        prices=prices_frame,
        rates=rates_frame,
        feature_config=feature_config,
        data_quality_report=data_quality_report,
        core_watchlist=universe.ai_chain.get("core_watchlist", []),
        start=start_date,
        end=end_date,
        strategy_ticker=strategy_ticker,
        benchmark_tickers=tuple(benchmark_tickers),
        market_regime=backtest_regime_context,
        fundamental_feature_reports=sec_fundamental_feature_reports,
        valuation_review_reports=valuation_review_reports,
        risk_event_occurrence_review_reports=risk_event_occurrence_review_reports,
        watchlist_lifecycle=watchlist_lifecycle,
        benchmark_policy_report=benchmark_policy_report,
    )

    def run_configured_backtest(
        *,
        scenario_start: date,
        scenario_cost_bps: float,
        scenario_spread_bps: float,
        scenario_slippage_bps: float,
        scenario_market_impact_bps: float,
        scenario_tax_bps: float,
        scenario_fx_bps: float,
        scenario_financing_annual_bps: float,
        scenario_etf_delay_bps: float,
        scenario_end: date | None = None,
        scenario_feature_lag_days: int = 0,
        scenario_universe_lag_days: int = 0,
        scenario_scoring_rules: ScoringRulesConfig | None = None,
        scenario_weight_multipliers: dict[str, float] | None = None,
    ) -> DailyBacktestResult:
        prepared_context = (
            base_prepared_context
            if scenario_feature_lag_days == 0 and scenario_universe_lag_days == 0
            else None
        )
        return run_daily_score_backtest(
            prices=prices_frame,
            rates=rates_frame,
            feature_config=feature_config,
            scoring_rules=(
                scoring_rules if scenario_scoring_rules is None else scenario_scoring_rules
            ),
            portfolio_config=portfolio,
            data_quality_report=data_quality_report,
            core_watchlist=universe.ai_chain.get("core_watchlist", []),
            start=scenario_start,
            end=end_date if scenario_end is None else scenario_end,
            strategy_ticker=strategy_ticker,
            benchmark_tickers=tuple(benchmark_tickers),
            cost_bps=scenario_cost_bps,
            spread_bps=scenario_spread_bps,
            slippage_bps=scenario_slippage_bps,
            market_impact_bps=scenario_market_impact_bps,
            tax_bps=scenario_tax_bps,
            fx_bps=scenario_fx_bps,
            financing_annual_bps=scenario_financing_annual_bps,
            etf_delay_bps=scenario_etf_delay_bps,
            fundamental_feature_reports=sec_fundamental_feature_reports,
            valuation_review_reports=valuation_review_reports,
            risk_event_occurrence_review_reports=risk_event_occurrence_review_reports,
            watchlist_lifecycle=watchlist_lifecycle,
            industry_chain=industry_chain,
            watchlist=watchlist,
            benchmark_policy_report=benchmark_policy_report,
            market_regime=backtest_regime_context,
            feature_lag_days=scenario_feature_lag_days,
            universe_lag_days=scenario_universe_lag_days,
            prepared_context=prepared_context,
            weight_profile=weight_profile,
            calibration_overlays=calibration_overlays,
            weight_multipliers=scenario_weight_multipliers,
        )

    result = run_configured_backtest(
        scenario_start=start_date,
        scenario_cost_bps=cost_bps,
        scenario_spread_bps=spread_bps,
        scenario_slippage_bps=slippage_bps,
        scenario_market_impact_bps=market_impact_bps,
        scenario_tax_bps=tax_bps,
        scenario_fx_bps=fx_bps,
        scenario_financing_annual_bps=financing_annual_bps,
        scenario_etf_delay_bps=etf_delay_bps,
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
        sec_companyfacts_validation_report_path=fundamental_validation_report_output,
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
        config_paths=backtest_trace_config_paths(
            regimes_path=regimes_path,
            benchmark_policy_path=benchmark_policy_path,
            sec_companies_path=sec_companies_path,
            sec_metrics_path=sec_metrics_path,
            fundamental_feature_config_path=fundamental_feature_config_path,
            risk_events_path=risk_events_path,
            watchlist_lifecycle_path=watchlist_lifecycle_path,
            rule_cards_path=rule_cards_path,
            feature_availability_path=feature_availability_path,
        ),
        rule_version_manifest=backtest_rule_version_manifest,
        sec_companyfacts_validation_report_path=fundamental_validation_report_output,
        feature_availability_report_path=backtest_feature_availability_output,
        feature_availability_summary=feature_availability_summary_record(
            backtest_feature_availability_report,
            backtest_feature_availability_output,
        ),
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
        sec_companyfacts_validation_report_path=fundamental_validation_report_output,
        input_coverage_output_path=input_coverage_output,
        audit_report_path=audit_output,
        feature_availability_section=backtest_feature_availability_section,
        traceability_section=render_traceability_section(
            backtest_trace_bundle,
            backtest_trace_output,
        ),
    )
    robustness_report_data = None
    robustness_output = None
    robustness_summary_output = None
    if should_run_robustness:
        configured_position_bands = _configured_position_band_rules(scoring_rules)
        cost_stress_result = run_configured_backtest(
            scenario_start=start_date,
            scenario_cost_bps=cost_bps + robustness_cost_stress_bps,
            scenario_spread_bps=spread_bps + robustness_cost_stress_bps,
            scenario_slippage_bps=slippage_bps + robustness_cost_stress_bps,
            scenario_market_impact_bps=market_impact_bps + robustness_cost_stress_bps,
            scenario_tax_bps=tax_bps,
            scenario_fx_bps=fx_bps,
            scenario_financing_annual_bps=financing_annual_bps,
            scenario_etf_delay_bps=etf_delay_bps + robustness_cost_stress_bps,
        )
        robustness_scenarios = [
            BacktestRobustnessScenario(
                scenario_id="cost_stress_execution",
                label="成本压力",
                category="cost",
                description=(
                    "commission、spread、slippage、market impact 和 ETF delay "
                    f"各增加 {robustness_cost_stress_bps:.1f} bps；税费、FX "
                    "和融资保持基础假设；复用缓存 PIT 输入并调用同一回测执行路径。"
                ),
                result=cost_stress_result,
            ),
            fixed_total_asset_exposure_scenario(
                result,
                exposure=robustness_policy.fixed_total_asset_exposure,
            ),
            volatility_targeted_exposure_scenario(
                result,
                target_annual_volatility=(robustness_policy.volatility_target_annual_volatility),
                lookback_days=robustness_policy.volatility_target_lookback_days,
                fallback_exposure=robustness_policy.fixed_total_asset_exposure,
            ),
            model_target_no_gate_scenario(result),
        ]
        for interval_days in robustness_policy.rebalance_intervals:
            robustness_scenarios.append(
                rebalance_interval_scenario(result, interval_days=interval_days)
            )
        robustness_scenarios.extend(
            [
                module_subset_baseline_scenario(
                    result,
                    scenario_id="trend_only_baseline",
                    label="趋势-only 基线",
                    modules=("trend",),
                    weights=scoring_rules.weights,
                    position_bands=configured_position_bands,
                ),
                module_subset_baseline_scenario(
                    result,
                    scenario_id="trend_plus_risk_sentiment_baseline",
                    label="趋势 + 风险情绪基线",
                    modules=("trend", "risk_sentiment"),
                    weights=scoring_rules.weights,
                    position_bands=configured_position_bands,
                ),
                module_subset_baseline_scenario(
                    result,
                    scenario_id="alpha_only_score_baseline",
                    label="Alpha-only score 基线",
                    modules=("trend", "fundamentals"),
                    weights=scoring_rules.weights,
                    position_bands=configured_position_bands,
                    category="score_architecture_baseline",
                ),
                module_subset_baseline_scenario(
                    result,
                    scenario_id="risk_state_only_score_baseline",
                    label="Risk-state-only score 基线",
                    modules=("macro_liquidity", "risk_sentiment"),
                    weights=scoring_rules.weights,
                    position_bands=configured_position_bands,
                    category="score_architecture_baseline",
                ),
                module_subset_baseline_scenario(
                    result,
                    scenario_id="gate_modules_score_baseline",
                    label="Gate modules score 基线",
                    modules=("valuation", "policy_geopolitics"),
                    weights=scoring_rules.weights,
                    position_bands=configured_position_bands,
                    category="score_architecture_baseline",
                ),
            ]
        )
        for module_name in scoring_rules.weights:
            for direction, multiplier in (
                ("down", 1.0 - robustness_weight_perturbation_pct),
                ("up", 1.0 + robustness_weight_perturbation_pct),
            ):
                direction_label = "下调" if direction == "down" else "上调"
                scenario_result = run_configured_backtest(
                    scenario_start=start_date,
                    scenario_cost_bps=cost_bps,
                    scenario_spread_bps=spread_bps,
                    scenario_slippage_bps=slippage_bps,
                    scenario_market_impact_bps=market_impact_bps,
                    scenario_tax_bps=tax_bps,
                    scenario_fx_bps=fx_bps,
                    scenario_financing_annual_bps=financing_annual_bps,
                    scenario_etf_delay_bps=etf_delay_bps,
                    scenario_weight_multipliers={module_name: multiplier},
                )
                robustness_scenarios.append(
                    BacktestRobustnessScenario(
                        scenario_id=_weight_perturbation_scenario_id(
                            module_name,
                            direction,
                            robustness_weight_perturbation_pct,
                        ),
                        label=(
                            f"{module_name} 权重{direction_label} "
                            f"{robustness_weight_perturbation_pct:.0%}"
                        ),
                        category="module_weight_perturbation",
                        description=(
                            f"将 {module_name} 模块权重{direction_label} "
                            f"{robustness_weight_perturbation_pct:.0%}；"
                            "复用缓存 PIT 输入并调用同一评分/回测执行路径。"
                        ),
                        result=scenario_result,
                    )
                )
        shifted_start = start_date + timedelta(days=robustness_shift_days)
        for seed in range(
            robustness_random_seed_start,
            robustness_random_seed_start + robustness_random_seed_count,
        ):
            robustness_scenarios.append(same_turnover_random_scenario(result, seed=seed))
            robustness_scenarios.append(same_exposure_random_scenario(result, seed=seed))
        if shifted_start >= result.last_signal_date:
            robustness_scenarios.append(
                BacktestRobustnessScenario(
                    scenario_id="shifted_start",
                    label="起点后移",
                    category="window",
                    description="将请求起点后移后复用相同 point-in-time 输入。",
                    skipped_reason=(
                        f"后移 {robustness_shift_days} 天后的起点 "
                        f"{shifted_start.isoformat()} 已不早于基础回测最后信号日 "
                        f"{result.last_signal_date.isoformat()}。"
                    ),
                )
            )
        else:
            shifted_result = run_configured_backtest(
                scenario_start=shifted_start,
                scenario_cost_bps=cost_bps,
                scenario_spread_bps=spread_bps,
                scenario_slippage_bps=slippage_bps,
                scenario_market_impact_bps=market_impact_bps,
                scenario_tax_bps=tax_bps,
                scenario_fx_bps=fx_bps,
                scenario_financing_annual_bps=financing_annual_bps,
                scenario_etf_delay_bps=etf_delay_bps,
            )
            robustness_scenarios.append(
                BacktestRobustnessScenario(
                    scenario_id="shifted_start",
                    label="起点后移",
                    category="window",
                    description=(
                        f"将请求起点后移 {robustness_shift_days} 天，"
                        "复用缓存 PIT 输入并调用同一评分/回测执行路径。"
                    ),
                    result=shifted_result,
                )
            )
        oos_split = _backtest_oos_split_dates(
            signal_dates,
            split_ratio=robustness_oos_split_ratio,
        )
        if oos_split is None:
            skipped_reason = (
                "基础信号样本不足，无法按时间顺序切出至少 5 个 in-sample "
                "和 5 个 out-of-sample 信号日。"
            )
            robustness_scenarios.extend(
                [
                    BacktestRobustnessScenario(
                        scenario_id="in_sample_window",
                        label="in-sample 窗口",
                        category="out_of_sample_validation",
                        description="时间顺序样本外验证的前段窗口。",
                        skipped_reason=skipped_reason,
                    ),
                    BacktestRobustnessScenario(
                        scenario_id="out_of_sample_holdout",
                        label="out-of-sample holdout",
                        category="out_of_sample_validation",
                        description="时间顺序样本外验证的后段 holdout。",
                        skipped_reason=skipped_reason,
                    ),
                ]
            )
        else:
            in_sample_end, out_of_sample_start = oos_split
            in_sample_result = run_configured_backtest(
                scenario_start=start_date,
                scenario_end=in_sample_end,
                scenario_cost_bps=cost_bps,
                scenario_spread_bps=spread_bps,
                scenario_slippage_bps=slippage_bps,
                scenario_market_impact_bps=market_impact_bps,
                scenario_tax_bps=tax_bps,
                scenario_fx_bps=fx_bps,
                scenario_financing_annual_bps=financing_annual_bps,
                scenario_etf_delay_bps=etf_delay_bps,
            )
            out_of_sample_result = run_configured_backtest(
                scenario_start=out_of_sample_start,
                scenario_end=end_date,
                scenario_cost_bps=cost_bps,
                scenario_spread_bps=spread_bps,
                scenario_slippage_bps=slippage_bps,
                scenario_market_impact_bps=market_impact_bps,
                scenario_tax_bps=tax_bps,
                scenario_fx_bps=fx_bps,
                scenario_financing_annual_bps=financing_annual_bps,
                scenario_etf_delay_bps=etf_delay_bps,
            )
            robustness_scenarios.extend(
                [
                    BacktestRobustnessScenario(
                        scenario_id="in_sample_window",
                        label="in-sample 窗口",
                        category="out_of_sample_validation",
                        description=(
                            f"按 {robustness_oos_split_ratio:.0%} 时间顺序切分的"
                            f"前段窗口，区间截至 {in_sample_end.isoformat()}；"
                            "复用缓存 PIT 输入并调用同一评分/回测执行路径。"
                        ),
                        result=in_sample_result,
                    ),
                    BacktestRobustnessScenario(
                        scenario_id="out_of_sample_holdout",
                        label="out-of-sample holdout",
                        category="out_of_sample_validation",
                        description=(
                            f"按 {robustness_oos_split_ratio:.0%} 时间顺序切分的"
                            f"后段 holdout，起点为 {out_of_sample_start.isoformat()}；"
                            "复用缓存 PIT 输入并调用同一评分/回测执行路径。"
                        ),
                        result=out_of_sample_result,
                    ),
                ]
            )
        robustness_report_data = BacktestRobustnessReport(
            base_result=result,
            scenarios=tuple(robustness_scenarios),
            cost_stress_increment_bps=robustness_cost_stress_bps,
            shifted_start_days=robustness_shift_days,
            weight_perturbation_pct=robustness_weight_perturbation_pct,
            random_seed_start=robustness_random_seed_start,
            random_seed_count=robustness_random_seed_count,
            oos_split_ratio=robustness_oos_split_ratio,
            policy_metadata=backtest_validation_policy.policy_metadata.model_dump(mode="json"),
            policy=robustness_policy,
        )
        if should_write_robustness_markdown:
            robustness_output = write_backtest_robustness_report(
                robustness_report_data,
                backtest_robustness_output,
            )
        robustness_summary_output = write_backtest_robustness_summary(
            robustness_report_data,
            backtest_robustness_summary_output,
        )

    lag_sensitivity_report_data = None
    lag_sensitivity_output = None
    lag_sensitivity_summary_output = None
    if should_run_lag_sensitivity:
        lag_scenarios: list[BacktestLagSensitivityScenario] = []
        for lag_day in lag_days:
            if lag_day == 0:
                continue
            for feature_lag_days, universe_lag_days in (
                (lag_day, 0),
                (0, lag_day),
                (lag_day, lag_day),
            ):
                try:
                    scenario_result = run_configured_backtest(
                        scenario_start=start_date,
                        scenario_cost_bps=cost_bps,
                        scenario_spread_bps=spread_bps,
                        scenario_slippage_bps=slippage_bps,
                        scenario_market_impact_bps=market_impact_bps,
                        scenario_tax_bps=tax_bps,
                        scenario_fx_bps=fx_bps,
                        scenario_financing_annual_bps=financing_annual_bps,
                        scenario_etf_delay_bps=etf_delay_bps,
                        scenario_feature_lag_days=feature_lag_days,
                        scenario_universe_lag_days=universe_lag_days,
                    )
                    lag_scenarios.append(
                        BacktestLagSensitivityScenario(
                            feature_lag_days=feature_lag_days,
                            universe_lag_days=universe_lag_days,
                            rebalance_delay_days=1,
                            result=scenario_result,
                        )
                    )
                except ValueError as exc:
                    lag_scenarios.append(
                        BacktestLagSensitivityScenario(
                            feature_lag_days=feature_lag_days,
                            universe_lag_days=universe_lag_days,
                            rebalance_delay_days=1,
                            skipped_reason=str(exc),
                        )
                    )
        lag_sensitivity_report_data = BacktestLagSensitivityReport(
            base_result=result,
            scenarios=tuple(lag_scenarios),
            tested_lag_days=lag_days,
        )
        if should_write_lag_sensitivity_markdown:
            lag_sensitivity_output = write_backtest_lag_sensitivity_report(
                lag_sensitivity_report_data,
                backtest_lag_sensitivity_output,
            )
        lag_sensitivity_summary_output = write_backtest_lag_sensitivity_summary(
            lag_sensitivity_report_data,
            backtest_lag_sensitivity_summary_output,
        )

    promotion_report_data = None
    promotion_output = None
    promotion_summary_output = None
    if should_run_promotion:
        promotion_report_data = build_model_promotion_report(
            result=result,
            as_of=quality_date,
            robustness_report=robustness_report_data,
            robustness_report_path=robustness_output or robustness_summary_output,
            lag_sensitivity_report=lag_sensitivity_report_data,
            lag_sensitivity_report_path=(lag_sensitivity_output or lag_sensitivity_summary_output),
            prediction_outcomes_path=promotion_prediction_outcomes_path,
            rule_governance_status=rule_governance_report.status,
            promotion_policy=backtest_validation_policy.promotion,
            policy_metadata=backtest_validation_policy.policy_metadata.model_dump(mode="json"),
        )
        if should_write_promotion_markdown:
            promotion_output = write_model_promotion_report(
                promotion_report_data,
                backtest_promotion_output,
            )
        promotion_summary_output = write_model_promotion_summary(
            promotion_report_data,
            backtest_promotion_summary_output,
        )
        report_output = write_backtest_report(
            result,
            data_quality_report_path=quality_output,
            daily_output_path=daily_output,
            output_path=backtest_report_output,
            sec_companyfacts_validation_report_path=fundamental_validation_report_output,
            input_coverage_output_path=input_coverage_output,
            audit_report_path=audit_output,
            feature_availability_section=backtest_feature_availability_section,
            promotion_gate_section=render_model_promotion_report(promotion_report_data).replace(
                "# 模型晋级门槛报告", "## 模型晋级门槛", 1
            ),
            traceability_section=render_traceability_section(
                backtest_trace_bundle,
                backtest_trace_output,
            ),
        )

    console.print(f"[yellow]回测状态：{result.status}[/yellow]")
    audit_style = "green" if audit_report.status == "PASS" else "yellow"
    console.print(f"[{audit_style}]输入审计状态：{audit_report.status}[/{audit_style}]")
    if result.market_regime is not None:
        console.print(f"市场阶段：{result.market_regime.name}（{result.market_regime.regime_id}）")
    console.print(f"策略总收益：{result.strategy_metrics.total_return:.1%}")
    console.print(f"策略 CAGR：{result.strategy_metrics.cagr:.1%}")
    console.print(f"策略最大回撤：{result.strategy_metrics.max_drawdown:.1%}")
    console.print(f"回测报告：{report_output}")
    if robustness_output is not None:
        console.print(f"稳健性报告：{robustness_output}")
    if robustness_summary_output is not None:
        console.print(f"稳健性摘要：{robustness_summary_output}")
    if lag_sensitivity_output is not None:
        console.print(f"滞后敏感性报告：{lag_sensitivity_output}")
    if lag_sensitivity_summary_output is not None:
        console.print(f"滞后敏感性摘要：{lag_sensitivity_summary_output}")
    if promotion_output is not None:
        console.print(f"模型晋级门槛报告：{promotion_output}")
    if promotion_summary_output is not None:
        console.print(f"模型晋级门槛摘要：{promotion_summary_output}")
    console.print(
        f"PIT 特征可见时间报告：{backtest_feature_availability_output}"
        f"（{backtest_feature_availability_report.status}）"
    )
    console.print(f"观察池 lifecycle 报告：{watchlist_lifecycle_report_output}")
    console.print(f"Evidence bundle：{backtest_trace_output}")
    console.print(f"输入审计报告：{audit_output}")
    console.print(f"每日明细：{daily_output}")
    console.print(f"历史输入覆盖诊断：{input_coverage_output}")
    console.print(f"SEC 基本面切片：{result.fundamental_feature_report_count} 个 signal_date")
    console.print(f"估值快照切片：{result.valuation_review_report_count} 个 signal_date")
    console.print(
        "风险事件发生记录切片："
        f"{result.risk_event_occurrence_review_report_count} 个 signal_date"
    )
    if sec_fundamental_source == "sec_pit_feature_panel":
        console.print(f"SEC PIT feature panel：{sec_pit_feature_panel_path}")
    else:
        console.print(f"SEC companyfacts 校验报告：{sec_companyfacts_validation_output}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")
    console.print(
        "规则版本："
        f"{backtest_rule_version_manifest['production_rule_count']} 个 production rule cards"
        f"（{rule_governance_report.status}）"
    )
    console.print(f"基准政策状态：{benchmark_policy_report.status}")
    if benchmark_policy_report_path is not None:
        console.print(f"基准政策报告：{benchmark_policy_report_path}")
    if fail_on_audit_warning and audit_report.status != "PASS":
        console.print("[red]输入审计未达到 PASS，严格审计门禁已返回失败。[/red]")
        raise typer.Exit(code=1)


def backtest_gate_attribution(
    backtest_daily_path: Annotated[
        Path | None,
        typer.Option(help="backtest_daily CSV 路径；默认使用 outputs/backtests 最新文件。"),
    ] = None,
    input_coverage_path: Annotated[
        Path | None,
        typer.Option(help="backtest_input_coverage CSV 路径；默认按 daily 文件名推断。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 归因报告输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    left_tail_threshold: Annotated[
        float | None,
        typer.Option(
            help="左尾收益阈值，例如 -0.03 表示 -3%；默认读取 backtest_validation_policy。"
        ),
    ] = None,
) -> None:
    """基于已生成回测 CSV 输出 gate 与事件效果归因报告。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    selected_daily_path = backtest_daily_path or _latest_backtest_daily_path(
        PROJECT_ROOT / "outputs" / "backtests"
    )
    if selected_daily_path is None:
        console.print("[red]未找到 backtest_daily_*.csv；请先运行 aits backtest。[/red]")
        raise typer.Exit(code=1)
    selected_coverage_path = input_coverage_path or infer_input_coverage_path(selected_daily_path)
    report = build_gate_event_attribution_report(
        backtest_daily_path=selected_daily_path,
        input_coverage_path=selected_coverage_path,
        as_of=report_date,
        left_tail_threshold=left_tail_threshold,
    )
    report_output = output_path or default_gate_event_attribution_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        report,
    )
    report_output = write_gate_event_attribution_report(report, report_output)
    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]Gate/event 归因状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_output}")
    console.print(
        f"Gate 数：{len(report.gate_rows)}；"
        f"事件记录：{report.event_summary.risk_event_record_count}"
    )
    console.print("治理边界：本命令只读解释历史样本，不改变回测、评分或仓位闸门。")
    if not report.passed:
        raise typer.Exit(code=1)


def backtest_input_gaps(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径，用于确定回测 signal_date。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径，用于数据质量门禁。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    start: Annotated[
        str | None,
        typer.Option(
            "--from",
            help="诊断开始日期，格式为 YYYY-MM-DD；未提供时使用所选市场阶段起点。",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--to", help="诊断结束日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    strategy_ticker: Annotated[
        str,
        typer.Option(help="用于确定回测信号日的策略代理标的。"),
    ] = "SMH",
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
    valuation_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径，用于历史覆盖诊断。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "valuation_snapshots",
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于历史覆盖诊断。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    risk_event_occurrences_path: Annotated[
        Path,
        typer.Option(help="风险事件发生记录 YAML 文件或目录路径，用于历史覆盖诊断。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 历史输入缺口报告输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
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
    """诊断回测所需历史估值和风险事件输入覆盖缺口。"""
    universe = load_universe()
    industry_chain = load_industry_chain()
    watchlist = load_watchlist()
    quality_config = load_data_quality()
    market_regimes = load_market_regimes(regimes_path)
    selected_regime_id = regime or market_regimes.default_backtest_regime
    try:
        selected_regime = market_regime_by_id(market_regimes, selected_regime_id)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    start_date = _parse_date(start) if start else selected_regime.start_date
    end_date = _parse_date(end) if end else date.today()
    quality_date = _parse_date(quality_as_of) if quality_as_of else date.today()
    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        quality_date,
    )
    gap_output = output_path or default_backtest_input_gap_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        start_date,
        end_date,
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
                ]
            )
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=quality_config,
        as_of=quality_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止历史输入缺口诊断。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    prices_frame = pd.read_csv(prices_path)
    signal_dates = _backtest_signal_dates(
        prices=prices_frame,
        strategy_ticker=strategy_ticker,
        start=start_date,
        end=end_date,
    )
    valuation_reports = _build_backtest_valuation_review_reports(
        signal_dates=signal_dates,
        valuation_path=valuation_path,
        universe=universe,
        watchlist=watchlist,
    )
    risk_event_reports = _build_backtest_risk_event_occurrence_review_reports(
        signal_dates=signal_dates,
        risk_events_path=risk_events_path,
        risk_event_occurrences_path=risk_event_occurrences_path,
        universe=universe,
        industry_chain=industry_chain,
        watchlist=watchlist,
        validation_as_of=quality_date,
    )
    report = build_backtest_input_gap_report(
        signal_dates=signal_dates,
        requested_start=start_date,
        requested_end=end_date,
        valuation_reports=valuation_reports,
        risk_event_reports=risk_event_reports,
        valuation_path=valuation_path,
        risk_event_occurrences_path=risk_event_occurrences_path,
        market_regime=BacktestRegimeContext(
            regime_id=selected_regime.regime_id,
            name=selected_regime.name,
            start_date=selected_regime.start_date,
            anchor_date=selected_regime.anchor_date,
            anchor_event=selected_regime.anchor_event,
            description=selected_regime.description,
        ),
    )
    report_output = write_backtest_input_gap_report(report, gap_output)

    status_style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{status_style}]历史输入缺口状态：{report.status}[/{status_style}]")
    console.print(f"信号日数量：{len(report.signal_dates)}")
    console.print(f"估值缺口信号日：{report.valuation_gap_count}")
    console.print(f"风险事件/复核声明缺口信号日：{report.risk_event_gap_count}")
    console.print(f"历史输入缺口报告：{report_output}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")


def backtest_pit_coverage(
    manifest_path: Annotated[
        Path,
        typer.Option(help="PIT raw snapshot manifest CSV 路径。"),
    ] = DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    data_sources_path: Annotated[
        Path,
        typer.Option(help="数据源目录 YAML 路径，用于校验授权和 provider 信息。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown forward-only PIT 覆盖报告输出路径。"),
    ] = None,
    min_forward_days: Annotated[
        int | None,
        typer.Option(
            help=(
                "升级为 B 级 forward-only 样本所需的最小覆盖日期数；"
                "默认读取 backtest validation policy。"
            )
        ),
    ] = None,
    max_staleness_days: Annotated[
        int | None,
        typer.Option(help="最新快照最大允许日龄；默认读取 backtest validation policy。"),
    ] = None,
) -> None:
    """评估 forward-only PIT 快照积累进度和回测输入等级升级日期。"""
    coverage_date = _parse_date(as_of) if as_of else date.today()
    coverage_output = output_path or default_backtest_pit_coverage_report_path(
        PROJECT_ROOT / "outputs" / "backtests",
        coverage_date,
    )
    policy = load_backtest_validation_policy().pit_coverage
    resolved_min_forward_days = (
        min_forward_days if min_forward_days is not None else policy.min_forward_days
    )
    resolved_max_staleness_days = (
        max_staleness_days if max_staleness_days is not None else policy.max_staleness_days
    )
    if resolved_min_forward_days <= 0:
        raise typer.BadParameter("B 级最小覆盖日期数必须为正数。")
    if resolved_max_staleness_days < 0:
        raise typer.BadParameter("最新快照最大允许日龄不能为负数。")

    validation_report = validate_pit_snapshot_manifest(
        input_path=manifest_path,
        as_of=coverage_date,
        data_sources=load_data_sources(data_sources_path),
    )
    report = build_backtest_pit_coverage_report(
        validation_report,
        min_forward_days=resolved_min_forward_days,
        max_staleness_days=resolved_max_staleness_days,
    )
    report_output = write_backtest_pit_coverage_report(report, coverage_output)

    status_style = (
        "green" if report.status == "PASS" else "yellow" if report.status != "FAIL" else "red"
    )
    console.print(f"[{status_style}]PIT 覆盖验证状态：{report.status}[/{status_style}]")
    console.print(f"Manifest 状态：{report.manifest_status}")
    console.print(f"快照数：{report.snapshot_count}；原始记录数：{report.row_count}")
    console.print(f"覆盖验证报告：{report_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


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


def _perturbed_scoring_rules(
    rules: ScoringRulesConfig,
    *,
    module_name: str,
    multiplier: float,
) -> ScoringRulesConfig:
    if module_name not in rules.weights:
        raise typer.BadParameter(f"未找到评分模块权重：{module_name}")
    if multiplier <= 0:
        raise typer.BadParameter("权重扰动倍数必须为正数。")
    weights = dict(rules.weights)
    weights[module_name] = weights[module_name] * multiplier
    return rules.model_copy(update={"weights": weights})


def _weight_perturbation_scenario_id(
    module_name: str,
    direction: str,
    perturbation_pct: float,
) -> str:
    percent_token = f"{perturbation_pct * 100:g}".replace(".", "p")
    module_token = "".join(char if char.isalnum() else "_" for char in module_name.lower()).strip(
        "_"
    )
    return f"weight_perturb_{module_token}_{direction}_{percent_token}pct"


def _backtest_oos_split_dates(
    signal_dates: tuple[date, ...],
    *,
    split_ratio: float,
    min_in_sample_signals: int = 5,
    min_out_sample_signals: int = 5,
) -> tuple[date, date] | None:
    if len(signal_dates) < min_in_sample_signals + min_out_sample_signals:
        return None
    split_index = int(len(signal_dates) * split_ratio)
    split_index = max(min_in_sample_signals, split_index)
    split_index = min(split_index, len(signal_dates) - min_out_sample_signals)
    if split_index <= 0 or split_index >= len(signal_dates):
        return None
    return signal_dates[split_index - 1], signal_dates[split_index]


def _parse_backtest_lag_days(value: str) -> tuple[int, ...]:
    lag_days: list[int] = []
    for raw_item in value.split(","):
        item = raw_item.strip()
        if not item:
            continue
        try:
            lag_day = int(item)
        except ValueError as exc:
            raise typer.BadParameter("滞后交易日列表必须为逗号分隔整数。") from exc
        if lag_day < 0:
            raise typer.BadParameter("滞后交易日不能为负数。")
        lag_days.append(lag_day)
    if not lag_days:
        raise typer.BadParameter("至少需要一个滞后交易日。")
    return tuple(sorted(dict.fromkeys(lag_days)))


def _latest_backtest_daily_path(output_dir: Path) -> Path | None:
    candidates = [
        path
        for path in output_dir.glob("backtest_daily_*.csv")
        if path.name.startswith("backtest_daily_")
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _backtest_required_input_signal_dates(
    prices: pd.DataFrame,
    strategy_ticker: str,
    start: date,
    end: date,
    lag_days: tuple[int, ...],
) -> tuple[date, ...]:
    frame = prices.loc[prices["ticker"].astype(str) == strategy_ticker].copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()].sort_values("_date")
    timestamps = list(frame["_date"])
    trading_signal_dates = [
        pd.Timestamp(timestamps[index]).date() for index in range(len(timestamps) - 1)
    ]
    base_signal_dates = set(_backtest_signal_dates(prices, strategy_ticker, start, end))
    index_by_signal_date = {
        signal_date: index for index, signal_date in enumerate(trading_signal_dates)
    }
    required_dates = set(base_signal_dates)
    for signal_date in base_signal_dates:
        index = index_by_signal_date[signal_date]
        for lag_day in lag_days:
            if index >= lag_day:
                required_dates.add(trading_signal_dates[index - lag_day])
    return tuple(sorted(required_dates))


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
                f"错误数：{metrics_report.error_count}；" f"警告数：{metrics_report.warning_count}"
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
                console.print("[red]TSMC IR point-in-time 指标合并失败，已停止回测。[/red]")
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
                f"错误数：{feature_report.error_count}；" f"警告数：{feature_report.warning_count}"
            )
            raise typer.Exit(code=1)
        reports[signal_date] = feature_report
    return reports


def _build_backtest_sec_pit_feature_reports(
    signal_dates: tuple[date, ...],
    sec_companies_path: Path,
    sec_pit_feature_panel_path: Path,
) -> dict[date, SecFundamentalFeaturesReport]:
    if not sec_pit_feature_panel_path.exists():
        console.print("[red]SEC PIT feature panel 不存在，已停止回测。[/red]")
        console.print(f"SEC PIT feature panel：{sec_pit_feature_panel_path}")
        raise typer.Exit(code=1)
    sec_companies = load_sec_companies(sec_companies_path)
    tickers = [company.ticker for company in sec_companies.companies if company.active]
    try:
        return sec_pit_feature_panel_to_feature_reports(
            sec_pit_feature_panel_path,
            signal_dates,
            tickers,
        )
    except ValueError as exc:
        console.print("[red]SEC PIT feature panel 可见时间校验失败，已停止回测。[/red]")
        console.print(f"SEC PIT feature panel：{sec_pit_feature_panel_path}")
        console.print(str(exc))
        raise typer.Exit(code=1) from exc


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
            console.print("[red]point-in-time 风险事件发生记录校验失败，已停止回测。[/red]")
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


def _configured_position_band_rules(
    scoring_rules: ScoringRulesConfig,
) -> tuple[PositionBandRule, ...]:
    return tuple(
        PositionBandRule(
            min_score=band.min_score,
            min_position=band.min_position,
            max_position=band.max_position,
            label=band.label,
        )
        for band in scoring_rules.position_bands
    )


def _parse_csv_items(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]
