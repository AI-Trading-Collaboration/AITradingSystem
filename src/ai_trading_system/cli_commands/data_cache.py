from __future__ import annotations

import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.download import (
    default_download_failure_report_path,
    download_daily_data,
    write_download_failure_report,
)
from ai_trading_system.data.market_data import (
    FmpPriceProvider,
    MarketstackPriceProvider,
    YFinancePriceProvider,
)
from ai_trading_system.data.quality import (
    default_quality_report_path,
    marketstack_reconciliation_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.data_refresh_audit import write_validate_data_audit_sidecar
from ai_trading_system.external_request_cache import sanitize_diagnostic_text

console = Console()


def register_data_cache_commands(app: typer.Typer) -> None:
    app.command("download-data")(download_data)
    app.command("validate-data")(validate_data)


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
    ] = PROJECT_ROOT
    / "data"
    / "raw",
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="包含配置中的完整 AI 产业链标的，而不只下载核心观察池。",
        ),
    ] = False,
    price_provider: Annotated[
        str,
        typer.Option(help="主价格源：fmp 或 yahoo。默认 fmp。"),
    ] = "fmp",
    fmp_api_key_env: Annotated[
        str,
        typer.Option(help="读取 FMP API key 的环境变量名。"),
    ] = "FMP_API_KEY",
    with_marketstack: Annotated[
        bool,
        typer.Option(
            "--with-marketstack/--without-marketstack",
            help="是否同时下载 Marketstack 第二行情源缓存。",
        ),
    ] = True,
    marketstack_api_key_env: Annotated[
        str,
        typer.Option(help="读取 Marketstack API key 的环境变量名。"),
    ] = "MARKETSTACK_API_KEY",
    marketstack_tail_catch_up: Annotated[
        bool,
        typer.Option(
            "--marketstack-tail-catch-up/--no-marketstack-tail-catch-up",
            help=(
                "Marketstack 第二源额度不足且只存在 missed-day tail gap 时，"
                "是否使用受控逐交易日 catch-up。"
            ),
        ),
    ] = True,
    failure_report_path: Annotated[
        Path | None,
        typer.Option(help="下载失败诊断报告路径；默认写入 outputs/reports。"),
    ] = None,
) -> None:
    """下载市场日线价格和 FRED 宏观序列到本地 CSV 缓存。"""
    universe = load_universe()
    start_date = _parse_date(start)
    end_date = _parse_date(end) if end else date.today()
    normalized_price_provider = price_provider.strip().lower()
    if normalized_price_provider == "fmp":
        fmp_api_key = os.getenv(fmp_api_key_env, "")
        if not fmp_api_key:
            console.print(f"[red]未设置 {fmp_api_key_env}，无法下载 FMP 主价格源。[/red]")
            raise typer.Exit(code=1)
        primary_price_provider = FmpPriceProvider(api_key=fmp_api_key)
    elif normalized_price_provider == "yahoo":
        primary_price_provider = YFinancePriceProvider()
    else:
        raise typer.BadParameter("主价格源必须是 fmp 或 yahoo。")

    marketstack_provider = None
    if with_marketstack:
        api_key = os.getenv(marketstack_api_key_env, "")
        if not api_key:
            console.print(
                f"[red]未设置 {marketstack_api_key_env}，无法下载 Marketstack 第二行情源。[/red]"
            )
            raise typer.Exit(code=1)
        marketstack_provider = MarketstackPriceProvider(api_key=api_key)

    try:
        summary = download_daily_data(
            universe,
            start=start_date,
            end=end_date,
            output_dir=output_dir,
            include_full_ai_chain=full_universe,
            price_provider=primary_price_provider,
            secondary_price_provider=marketstack_provider,
            marketstack_tail_catch_up=marketstack_tail_catch_up,
        )
    except Exception as exc:
        report_path = failure_report_path or default_download_failure_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            end_date,
        )
        write_download_failure_report(
            output_path=report_path,
            start=start_date,
            end=end_date,
            raw_output_dir=output_dir,
            include_full_ai_chain=full_universe,
            price_provider_name=normalized_price_provider,
            with_marketstack=with_marketstack,
            error=exc,
        )
        console.print("[red]数据缓存更新失败，已停止。[/red]")
        console.print(f"下载失败诊断报告：{report_path}")
        console.print(f"脱敏错误摘要：{sanitize_diagnostic_text(str(exc))}")
        raise typer.Exit(code=1) from exc

    console.print("[green]数据缓存已更新。[/green]")
    console.print(f"主价格源：{normalized_price_provider}")
    console.print(f"价格数据：{summary.prices_path}（{summary.price_rows} 行）")
    if summary.secondary_prices_path is not None:
        console.print(
            f"Marketstack 第二行情源：{summary.secondary_prices_path}"
            f"（{summary.secondary_price_rows} 行）"
        )
    console.print(f"FRED 宏观序列：{summary.rates_path}（{summary.rate_rows} 行）")
    console.print(f"下载审计清单：{summary.manifest_path}")
    console.print(f"价格标的：{', '.join(summary.price_tickers)}")
    console.print(f"FRED 宏观序列：{', '.join(summary.rate_series)}")
    for budget in summary.request_budget_statuses:
        violation_reasons = _budget_violation_reasons(budget)
        tail_catch_up = budget.get("tail_catch_up")
        tail_catch_up_applied = (
            tail_catch_up.get("applied")
            if isinstance(tail_catch_up, dict)
            else None
        )
        quota_cycle_reset = budget.get("quota_cycle_reset")
        quota_cycle_reset_status = (
            quota_cycle_reset.get("stale_header_status")
            if isinstance(quota_cycle_reset, dict)
            else None
        )
        console.print(
            "请求预算："
            f"{budget.get('provider')} / {budget.get('api_family')} "
            f"profile={budget.get('budget_profile')} "
            f"status={budget.get('status')} "
            f"estimated_increment_usage={budget.get('estimated_increment_usage')} "
            f"quota_remaining={budget.get('quota_remaining')} "
            f"violation_reasons={','.join(violation_reasons)} "
            f"tail_catch_up_applied={tail_catch_up_applied} "
            f"quota_cycle_reset_status={quota_cycle_reset_status}"
        )
    for cache_summary in summary.request_cache_summaries:
        console.print(
            "请求缓存："
            f"{cache_summary.get('provider')} / {cache_summary.get('api_family')} "
            f"hits={cache_summary.get('cache_hits')} "
            f"misses={cache_summary.get('cache_misses')} "
            f"live={cache_summary.get('live_request_count')} "
            f"quota_remaining={cache_summary.get('quota_remaining')}"
        )


def validate_data(
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化 FRED 宏观序列 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
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
    backtest_manifest_path: Annotated[
        Path | None,
        typer.Option(
            "--backtest-manifest",
            help="可选 backtest_input_manifest.json，用于 manifest context 下的数据门禁诊断。",
        ),
    ] = None,
) -> None:
    """校验缓存数据并写入 Markdown 质量报告。"""
    universe = load_universe()
    quality_config = load_data_quality()
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )

    started_at = datetime.now(tz=UTC)
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
        backtest_manifest_path=backtest_manifest_path,
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(report, report_path)
    audit_record_path = write_validate_data_audit_sidecar(
        report=report,
        report_path=report_path,
        started_at=started_at,
        ended_at=datetime.now(tz=UTC),
    )

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]数据质量状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"Data refresh audit record：{audit_record_path}")
    if report.marketstack_reconciliation_records:
        reconciliation_path = marketstack_reconciliation_path(report_path)
        console.print(f"Marketstack reconciliation：{reconciliation_path}")
    console.print(
        f"错误数：{report.error_count}；"
        f"警告数：{report.warning_count}；"
        f"信息数：{report.info_count}"
    )

    if not report.passed:
        raise typer.Exit(code=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _download_manifest_path(prices_path: Path) -> Path:
    return prices_path.parent / "download_manifest.csv"


def _budget_violation_reasons(budget: dict[str, object]) -> list[str]:
    for key in (
        "quota_cycle_reset",
        "owner_approved_tail_catch_up",
        "owner_approved_overage",
    ):
        approval = budget.get(key)
        if isinstance(approval, dict):
            reasons = approval.get("violation_reasons")
            if isinstance(reasons, list):
                return [str(reason) for reason in reasons]
    return []


def _marketstack_prices_path(prices_path: Path) -> Path:
    return prices_path.parent / "prices_marketstack_daily.csv"


def _requires_marketstack_prices(prices_path: Path) -> bool:
    default_prices_path = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
    try:
        return prices_path.resolve() == default_prices_path.resolve()
    except OSError:
        return prices_path == default_prices_path
