from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.config import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_DATA_QUALITY_CONFIG_PATH,
    DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    load_fundamental_features,
    load_fundamental_metrics,
    load_sec_companies,
)
from ai_trading_system.fundamentals.sec_filing_timeline import build_filing_timeline_csv
from ai_trading_system.fundamentals.sec_pit_backfill import (
    DEFAULT_SEC_EDGAR_PROCESSED_DIR,
    DEFAULT_SEC_EDGAR_RAW_DIR,
    DEFAULT_SEC_PIT_BACKFILL_CONFIG_PATH,
    DEFAULT_SEC_PIT_REPORT_DIR,
    SecPitEdgarProvider,
    fetch_sec_pit_raw,
    load_sec_pit_backfill_config,
    run_sec_pit_backfill,
)
from ai_trading_system.fundamentals.sec_pit_baseline_comparison import (
    DEFAULT_BASELINE_SCORE_DIR,
    DEFAULT_SEC_PIT_BASELINE_COMPARISON_OUTPUT_DIR,
    DEFAULT_SEC_PIT_EVALUATION_DIR,
    run_sec_pit_baseline_comparison,
)
from ai_trading_system.fundamentals.sec_pit_evaluation import (
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SEC_PIT_EVALUATION_OUTPUT_DIR,
    DEFAULT_SEC_PIT_EVALUATION_POLICY_PATH,
    DEFAULT_SEC_PIT_FEATURE_PANEL_PATH,
    run_sec_pit_evaluation,
)
from ai_trading_system.fundamentals.sec_pit_metrics import build_mapped_metrics_csv
from ai_trading_system.fundamentals.sec_pit_panel import (
    build_fundamental_pit_daily_panel_csv,
    build_fundamental_pit_intervals_csv,
    build_sec_pit_feature_panel_csv,
)
from ai_trading_system.fundamentals.sec_pit_validation import (
    validate_and_write_sec_pit_artifacts,
)
from ai_trading_system.fundamentals.sec_xbrl_facts import build_xbrl_facts_long_csv

console = Console()
sec_pit_app = typer.Typer(
    help="SEC EDGAR reconstructed filing-time PIT backfill。", no_args_is_help=True
)


@sec_pit_app.command("fetch-raw")
def fetch_raw_command(
    tickers: Annotated[
        list[str] | None,
        typer.Option("--ticker", help="只处理指定 ticker；可重复。"),
    ] = None,
    user_agent: Annotated[
        str | None,
        typer.Option(help="SEC fair access User-Agent；默认读取 SEC_USER_AGENT。"),
    ] = None,
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    raw_dir: Annotated[
        Path,
        typer.Option(help="SEC EDGAR raw JSON 输出目录。"),
    ] = DEFAULT_SEC_EDGAR_RAW_DIR,
    config_path: Annotated[
        Path,
        typer.Option(help="SEC PIT backfill policy 配置路径。"),
    ] = DEFAULT_SEC_PIT_BACKFILL_CONFIG_PATH,
    use_cache: Annotated[
        bool,
        typer.Option("--use-cache/--no-cache", help="是否使用 external request cache。"),
    ] = True,
) -> None:
    """下载 submissions 和 companyfacts raw JSON，并写入 manifest。"""
    policy = load_sec_pit_backfill_config(config_path)
    provider = SecPitEdgarProvider(
        user_agent=_resolve_user_agent(user_agent),
        max_requests_per_second=policy.max_requests_per_second,
    )
    summary = fetch_sec_pit_raw(
        sec_companies=load_sec_companies(sec_companies_path),
        provider=provider,
        raw_dir=raw_dir,
        tickers=tickers,
        use_cache=use_cache,
    )
    console.print(f"SEC PIT raw files: {summary.file_count}")
    console.print(f"SEC PIT tickers: {summary.ticker_count}")
    console.print(f"Raw manifest: {summary.manifest_path}")


@sec_pit_app.command("build-filing-timeline")
def build_filing_timeline_command(
    start: Annotated[
        str,
        typer.Option("--from", help="开始日期 YYYY-MM-DD。"),
    ],
    end: Annotated[
        str,
        typer.Option("--to", help="结束日期 YYYY-MM-DD。"),
    ],
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    raw_dir: Annotated[
        Path,
        typer.Option(help="SEC EDGAR raw JSON 输入目录。"),
    ] = DEFAULT_SEC_EDGAR_RAW_DIR,
    output_path: Annotated[
        Path,
        typer.Option(help="filing_timeline.csv 输出路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "filing_timeline.csv",
) -> None:
    """从 submissions raw JSON 构建 filing timeline。"""
    path = build_filing_timeline_csv(
        sec_companies=load_sec_companies(sec_companies_path),
        raw_dir=raw_dir,
        start=_parse_date(start),
        end=_parse_date(end),
        output_path=output_path,
    )
    console.print(f"Filing timeline: {path}")


@sec_pit_app.command("build-facts")
def build_facts_command(
    end: Annotated[
        str,
        typer.Option("--to", help="结束日期 YYYY-MM-DD。"),
    ],
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    raw_dir: Annotated[
        Path,
        typer.Option(help="SEC EDGAR raw JSON 输入目录。"),
    ] = DEFAULT_SEC_EDGAR_RAW_DIR,
    filing_timeline_path: Annotated[
        Path,
        typer.Option(help="filing_timeline.csv 输入路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "filing_timeline.csv",
    output_path: Annotated[
        Path,
        typer.Option(help="xbrl_facts_long.csv 输出路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "xbrl_facts_long.csv",
) -> None:
    """从 companyfacts raw JSON 构建 XBRL fact long table。"""
    path = build_xbrl_facts_long_csv(
        sec_companies=load_sec_companies(sec_companies_path),
        raw_dir=raw_dir,
        filing_timeline_path=filing_timeline_path,
        end=_parse_date(end),
        output_path=output_path,
    )
    console.print(f"XBRL facts: {path}")


@sec_pit_app.command("build-metrics")
def build_metrics_command(
    end: Annotated[
        str,
        typer.Option("--to", help="结束日期 YYYY-MM-DD。"),
    ],
    facts_path: Annotated[
        Path,
        typer.Option(help="xbrl_facts_long.csv 输入路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "xbrl_facts_long.csv",
    metrics_path: Annotated[
        Path,
        typer.Option(help="fundamental_metrics.yaml 路径。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    config_path: Annotated[
        Path,
        typer.Option(help="SEC PIT backfill policy 配置路径。"),
    ] = DEFAULT_SEC_PIT_BACKFILL_CONFIG_PATH,
    output_path: Annotated[
        Path,
        typer.Option(help="mapped_metrics_long.csv 输出路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "mapped_metrics_long.csv",
) -> None:
    """将 XBRL facts 映射为项目基本面指标。"""
    path = build_mapped_metrics_csv(
        facts_path=facts_path,
        metrics=load_fundamental_metrics(metrics_path),
        policy=load_sec_pit_backfill_config(config_path),
        end=_parse_date(end),
        output_path=output_path,
    )
    console.print(f"Mapped metrics: {path}")


@sec_pit_app.command("build-panel")
def build_panel_command(
    start: Annotated[
        str,
        typer.Option("--from", help="开始日期 YYYY-MM-DD。"),
    ],
    end: Annotated[
        str,
        typer.Option("--to", help="结束日期 YYYY-MM-DD。"),
    ],
    mapped_metrics_path: Annotated[
        Path,
        typer.Option(help="mapped_metrics_long.csv 输入路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "mapped_metrics_long.csv",
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    features_path: Annotated[
        Path,
        typer.Option(help="fundamental_features.yaml 路径。"),
    ] = DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    processed_dir: Annotated[
        Path,
        typer.Option(help="SEC PIT processed 输出目录。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR,
) -> None:
    """构建 PIT intervals、daily panel 和 SEC PIT feature panel。"""
    intervals_path = build_fundamental_pit_intervals_csv(
        mapped_metrics_path=mapped_metrics_path,
        output_path=processed_dir / "fundamental_pit_intervals.csv",
    )
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    daily_panel_path = build_fundamental_pit_daily_panel_csv(
        intervals_path=intervals_path,
        start=start_date,
        end=end_date,
        output_path=processed_dir / "fundamental_pit_daily_panel.csv",
    )
    feature_panel_path = build_sec_pit_feature_panel_csv(
        intervals_path=intervals_path,
        features=load_fundamental_features(features_path),
        sec_companies=load_sec_companies(sec_companies_path),
        start=start_date,
        end=end_date,
        output_path=processed_dir / "sec_pit_feature_panel.csv",
    )
    console.print(f"PIT intervals: {intervals_path}")
    console.print(f"PIT daily panel: {daily_panel_path}")
    console.print(f"SEC PIT feature panel: {feature_panel_path}")


@sec_pit_app.command("validate")
def validate_command(
    as_of: Annotated[
        str,
        typer.Option(help="验证日期 YYYY-MM-DD。"),
    ],
    raw_manifest_path: Annotated[
        Path,
        typer.Option(help="sec_edgar_raw_manifest.csv 路径。"),
    ] = DEFAULT_SEC_EDGAR_RAW_DIR
    / "manifest"
    / "sec_edgar_raw_manifest.csv",
    filing_timeline_path: Annotated[
        Path,
        typer.Option(help="filing_timeline.csv 路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "filing_timeline.csv",
    facts_path: Annotated[
        Path,
        typer.Option(help="xbrl_facts_long.csv 路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "xbrl_facts_long.csv",
    mapped_metrics_path: Annotated[
        Path,
        typer.Option(help="mapped_metrics_long.csv 路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "mapped_metrics_long.csv",
    intervals_path: Annotated[
        Path,
        typer.Option(help="fundamental_pit_intervals.csv 路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "fundamental_pit_intervals.csv",
    feature_panel_path: Annotated[
        Path,
        typer.Option(help="sec_pit_feature_panel.csv 路径。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR
    / "sec_pit_feature_panel.csv",
    output_dir: Annotated[
        Path,
        typer.Option(help="验证报告输出目录。"),
    ] = DEFAULT_SEC_PIT_REPORT_DIR,
    config_path: Annotated[
        Path,
        typer.Option(help="SEC PIT backfill policy 配置路径。"),
    ] = DEFAULT_SEC_PIT_BACKFILL_CONFIG_PATH,
) -> None:
    """验证 SEC PIT backfill artifact 并写出 JSON/Markdown 报告。"""
    artifacts = validate_and_write_sec_pit_artifacts(
        as_of=_parse_date(as_of),
        raw_manifest_path=raw_manifest_path,
        filing_timeline_path=filing_timeline_path,
        facts_path=facts_path,
        mapped_metrics_path=mapped_metrics_path,
        intervals_path=intervals_path,
        feature_panel_path=feature_panel_path,
        output_dir=output_dir,
        policy=load_sec_pit_backfill_config(config_path),
    )
    console.print(f"SEC PIT validation: {artifacts['sec_pit_validation_json']}")
    _exit_if_validation_failed(artifacts["sec_pit_validation_json"])


@sec_pit_app.command("backfill")
def backfill_command(
    start: Annotated[
        str,
        typer.Option("--from", help="开始日期 YYYY-MM-DD。"),
    ],
    end: Annotated[
        str,
        typer.Option("--to", help="结束日期 YYYY-MM-DD。"),
    ],
    tickers: Annotated[
        list[str] | None,
        typer.Option("--ticker", help="只处理指定 ticker；可重复。"),
    ] = None,
    user_agent: Annotated[
        str | None,
        typer.Option(help="SEC fair access User-Agent；默认读取 SEC_USER_AGENT。"),
    ] = None,
    raw_dir: Annotated[
        Path,
        typer.Option(help="SEC EDGAR raw JSON 目录。"),
    ] = DEFAULT_SEC_EDGAR_RAW_DIR,
    processed_dir: Annotated[
        Path,
        typer.Option(help="SEC PIT processed 输出目录。"),
    ] = DEFAULT_SEC_EDGAR_PROCESSED_DIR,
    report_dir: Annotated[
        Path,
        typer.Option(help="SEC PIT report 输出目录。"),
    ] = DEFAULT_SEC_PIT_REPORT_DIR,
    sec_companies_path: Annotated[
        Path,
        typer.Option(help="SEC company CIK 配置路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    metrics_path: Annotated[
        Path,
        typer.Option(help="fundamental_metrics.yaml 路径。"),
    ] = DEFAULT_FUNDAMENTAL_METRICS_CONFIG_PATH,
    features_path: Annotated[
        Path,
        typer.Option(help="fundamental_features.yaml 路径。"),
    ] = DEFAULT_FUNDAMENTAL_FEATURES_CONFIG_PATH,
    config_path: Annotated[
        Path,
        typer.Option(help="SEC PIT backfill policy 配置路径。"),
    ] = DEFAULT_SEC_PIT_BACKFILL_CONFIG_PATH,
    use_cache: Annotated[
        bool,
        typer.Option("--use-cache/--no-cache", help="是否使用 external request cache。"),
    ] = True,
    full_pipeline: Annotated[
        bool,
        typer.Option(
            "--full-pipeline/--build-from-existing-raw",
            help="full-pipeline 会先抓取 raw；build-from-existing-raw 只重建下游 artifact。",
        ),
    ] = True,
) -> None:
    """执行 SEC EDGAR reconstructed PIT backfill 全链路或从已有 raw 重建。"""
    policy = load_sec_pit_backfill_config(config_path)
    provider = (
        SecPitEdgarProvider(
            user_agent=_resolve_user_agent(user_agent),
            max_requests_per_second=policy.max_requests_per_second,
        )
        if full_pipeline
        else None
    )
    artifacts = run_sec_pit_backfill(
        start=_parse_date(start),
        end=_parse_date(end),
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        report_dir=report_dir,
        sec_companies_path=sec_companies_path,
        metrics_path=metrics_path,
        features_path=features_path,
        config_path=config_path,
        provider=provider,
        user_agent=user_agent,
        tickers=tickers,
        use_cache=use_cache,
        full_pipeline=full_pipeline,
    )
    for name, path in artifacts.items():
        console.print(f"{name}: {path}")
    validation_path = artifacts.get("sec_pit_validation_json")
    if validation_path is not None:
        _exit_if_validation_failed(validation_path)


@sec_pit_app.command("evaluate")
def evaluate_command(
    start: Annotated[
        str,
        typer.Option("--start", help="评估开始日期 YYYY-MM-DD。"),
    ],
    end: Annotated[
        str,
        typer.Option("--end", help="评估结束日期 YYYY-MM-DD。"),
    ],
    feature_panel: Annotated[
        Path,
        typer.Option("--feature-panel", help="TRADING-039 SEC PIT feature panel CSV。"),
    ] = DEFAULT_SEC_PIT_FEATURE_PANEL_PATH,
    universe: Annotated[
        Path,
        typer.Option("--universe", help="SEC company universe 配置路径。"),
    ] = DEFAULT_SEC_COMPANIES_CONFIG_PATH,
    benchmark: Annotated[
        str,
        typer.Option("--benchmark", help="用于 forward excess return 的 benchmark ticker。"),
    ] = "QQQ",
    tickers: Annotated[
        list[str] | None,
        typer.Option(
            "--tickers",
            help="覆盖 universe 的 ticker 列表；可重复，也可用逗号或空格分隔。",
        ),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="SEC PIT evaluation 输出目录。"),
    ] = DEFAULT_SEC_PIT_EVALUATION_OUTPUT_DIR,
    prices_path: Annotated[
        Path,
        typer.Option(help="缓存价格 CSV，用于 validate-data gate 和 forward return。"),
    ] = DEFAULT_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option(help="缓存宏观利率 CSV，用于 validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    market_universe_path: Annotated[
        Path,
        typer.Option(help="市场 universe 配置路径，用于数据质量门禁的 macro series。"),
    ] = DEFAULT_CONFIG_PATH,
    data_quality_config_path: Annotated[
        Path,
        typer.Option(help="数据质量 policy 配置路径。"),
    ] = DEFAULT_DATA_QUALITY_CONFIG_PATH,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="数据质量报告输出路径；默认写入 output-dir。"),
    ] = None,
    quality_as_of: Annotated[
        str | None,
        typer.Option(help="数据质量校验日期 YYYY-MM-DD；默认使用 --end。"),
    ] = None,
    policy_path: Annotated[
        Path,
        typer.Option(help="SEC PIT evaluation policy 配置路径。"),
    ] = DEFAULT_SEC_PIT_EVALUATION_POLICY_PATH,
    regime: Annotated[
        str | None,
        typer.Option(
            "--regime",
            help="市场阶段 ID，默认使用 config/market_regimes.yaml 的 default_backtest_regime。",
        ),
    ] = None,
    market_regimes_path: Annotated[
        Path,
        typer.Option(help="市场阶段配置路径。"),
    ] = DEFAULT_MARKET_REGIMES_CONFIG_PATH,
) -> None:
    """评估 SEC PIT feature 的解释力、PIT 安全性和 shadow 候选分层。"""
    artifacts = run_sec_pit_evaluation(
        start=_parse_date(start),
        end=_parse_date(end),
        feature_panel_path=feature_panel,
        universe_path=universe,
        benchmark=benchmark,
        output_dir=output_dir,
        prices_path=prices_path,
        rates_path=rates_path,
        market_universe_path=market_universe_path,
        data_quality_config_path=data_quality_config_path,
        quality_report_path=quality_report_path,
        quality_as_of=_parse_date(quality_as_of) if quality_as_of else None,
        policy_path=policy_path,
        market_regimes_path=market_regimes_path,
        regime=regime,
        tickers=tickers,
    )
    console.print(f"SEC PIT evaluation status: {artifacts.status}")
    console.print(f"Evaluation summary JSON: {artifacts.summary_json_path}")
    console.print(f"Evaluation summary report: {artifacts.summary_markdown_path}")
    console.print(f"Feature effectiveness: {artifacts.feature_effectiveness_path}")
    console.print(f"Signal attribution: {artifacts.signal_attribution_path}")
    console.print(f"Shadow candidate weights: {artifacts.shadow_candidate_weights_path}")
    console.print(f"Data quality report: {artifacts.data_quality_report_path}")
    console.print(f"Run log: {artifacts.run_log_path}")
    if artifacts.status in {"DATA_QUALITY_FAILED"}:
        raise typer.Exit(code=2)


@sec_pit_app.command("compare-baseline")
def compare_baseline_command(
    start: Annotated[
        str,
        typer.Option("--start", help="比较开始日期 YYYY-MM-DD。"),
    ],
    end: Annotated[
        str,
        typer.Option("--end", help="比较结束日期 YYYY-MM-DD。"),
    ],
    sec_pit_evaluation_dir: Annotated[
        Path,
        typer.Option(
            "--sec-pit-evaluation-dir",
            help="TRADING-040 SEC PIT evaluation artifact 目录。",
        ),
    ] = DEFAULT_SEC_PIT_EVALUATION_DIR,
    baseline_score_dir: Annotated[
        Path,
        typer.Option("--baseline-score-dir", help="score-daily baseline artifact 目录或 CSV。"),
    ] = DEFAULT_BASELINE_SCORE_DIR,
    benchmark: Annotated[
        str,
        typer.Option("--benchmark", help="用于 relative return 的 benchmark ticker。"),
    ] = "QQQ",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="SEC PIT baseline comparison 输出目录。"),
    ] = DEFAULT_SEC_PIT_BASELINE_COMPARISON_OUTPUT_DIR,
    tickers: Annotated[
        list[str] | None,
        typer.Option(
            "--tickers",
            help="覆盖 artifact universe 的 ticker 列表；可重复，也可用逗号或空格分隔。",
        ),
    ] = None,
    strict: Annotated[
        bool,
        typer.Option("--strict", help="缺少 baseline / SEC PIT evaluation / overlap 时返回失败。"),
    ] = False,
) -> None:
    """比较 SEC PIT enhanced output 与现有 baseline 的 decision-level 影响。"""
    artifacts = run_sec_pit_baseline_comparison(
        start=_parse_date(start),
        end=_parse_date(end),
        sec_pit_evaluation_dir=sec_pit_evaluation_dir,
        baseline_score_dir=baseline_score_dir,
        benchmark=benchmark,
        output_dir=output_dir,
        tickers=_flatten_ticker_options(tickers) if tickers else None,
    )
    console.print(f"SEC PIT baseline comparison status: {artifacts.status}")
    console.print(f"Summary JSON: {artifacts.summary_json_path}")
    console.print(f"Summary report: {artifacts.summary_markdown_path}")
    console.print(f"Decision impact: {artifacts.decision_impact_path}")
    console.print(f"Rank shift: {artifacts.rank_shift_path}")
    console.print(f"Incremental alpha: {artifacts.incremental_alpha_path}")
    console.print(f"Run log: {artifacts.run_log_path}")
    if strict and artifacts.status != "OK":
        raise typer.Exit(code=2)


def _resolve_user_agent(value: str | None) -> str:
    user_agent = value or os.getenv("SEC_USER_AGENT")
    if user_agent is None or not user_agent.strip():
        raise typer.BadParameter(
            "SEC User-Agent 不能为空；请传 --user-agent 或设置 SEC_USER_AGENT。"
        )
    return user_agent


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"日期格式必须是 YYYY-MM-DD：{value}") from exc


def _exit_if_validation_failed(path: Path) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    status = str(payload.get("status") or "FAIL")
    console.print(f"SEC PIT validation status: {status}")
    if status == "FAIL":
        raise typer.Exit(code=2)


def _flatten_ticker_options(values: list[str]) -> list[str]:
    flattened: list[str] = []
    for value in values:
        flattened.extend(part.strip().upper() for part in str(value).replace(",", " ").split())
    return [item for item in flattened if item]
