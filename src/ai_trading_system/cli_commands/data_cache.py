from __future__ import annotations

import json
import os
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.contracts.data_quality_execution import (
    DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
    MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID,
    DataQualityDateWindow,
    DataQualityExecutionContractError,
)
from ai_trading_system.data.download import (
    default_download_failure_report_path,
    download_daily_data,
    write_download_failure_report,
)
from ai_trading_system.data.market_data import (
    FmpPriceProvider,
    MarketstackPriceProvider,
    PriceDataProvider,
    YFinancePriceProvider,
)
from ai_trading_system.data.quality import (
    default_quality_report_path,
    marketstack_reconciliation_path,
    write_data_quality_report,
)
from ai_trading_system.data.quality_execution import (
    CanonicalDataQualityExecutionRequest,
    DataQualityExecutionError,
    run_canonical_data_quality_execution,
)
from ai_trading_system.data.quality_execution_discovery import (
    publish_default_data_quality_execution_discovery,
)
from ai_trading_system.data_refresh_audit import write_validate_data_audit_sidecar
from ai_trading_system.external_request_cache import (
    invalidate_external_request_cache,
    sanitize_diagnostic_text,
)
from ai_trading_system.trading_calendar import resolve_default_data_quality_as_of

console = Console()

# Project-level invariant from the reviewed Wave12 requirement and AGENTS.md.
# A pre-primary historical as-of remains valid only as a bounded sensitivity date.
DEFAULT_DATA_QUALITY_WINDOW_START = date(2021, 2, 22)
AUTO_DATA_QUALITY_EXECUTION_PROFILE_ID = "auto"
ALLOWED_DATA_QUALITY_EXECUTION_PROFILE_IDS = frozenset(
    {
        AUTO_DATA_QUALITY_EXECUTION_PROFILE_ID,
        DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
        MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID,
    }
)


def register_data_cache_commands(app: typer.Typer) -> None:
    app.command("download-data")(download_data)
    app.command("validate-data")(validate_data)
    app.command("invalidate-external-request-cache")(invalidate_external_request_cache_command)


def invalidate_external_request_cache_command(
    metadata_path: Annotated[
        Path,
        typer.Option(
            "--metadata-path",
            help="目标 v2 metadata.json；request identity 只从脱敏 metadata 读取。",
        ),
    ],
    expected_generation_id: Annotated[
        str, typer.Option("--expected-generation-id", help="当前 v2 generation id。")
    ],
    expected_body_sha256: Annotated[
        str, typer.Option("--expected-body-sha256", help="当前 body SHA-256。")
    ],
    actor: Annotated[str, typer.Option(help="执行失效的可审计主体。")],
    reason: Annotated[str, typer.Option(help="非空失效原因。")],
    reference: Annotated[str, typer.Option(help="task/ticket/incident 引用。")],
) -> None:
    """以 generation/body CAS 显式失效缓存；不删除证据、不发起网络请求。"""

    try:
        identity = _external_request_cache_identity_from_metadata(metadata_path)
        result = invalidate_external_request_cache(
            provider=str(identity["provider"]),
            api_family=str(identity["api_family"]),
            method=str(identity["method"]),
            url=str(identity["url"]),
            expected_generation_id=expected_generation_id,
            expected_body_sha256=expected_body_sha256,
            actor=actor,
            reason=reason,
            reference=reference,
            cache_dir=identity["cache_dir"],
            params=identity["params"],
            headers=identity["headers"],
            json_payload=identity["json_payload"],
        )
    except (OSError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print("[green]External request cache generation 已显式失效。[/green]")
    console.print(f"cache_key={result.cache_key}")
    console.print(f"generation_id={result.generation_id}")
    console.print(f"body_sha256={result.body_sha256}")
    console.print(f"invalidation_path={result.invalidation_path}")
    console.print(f"lifecycle_event_path={result.lifecycle_event_path}")
    console.print("production_effect=none")
    console.print("next_business_request_may_contact_provider=true")


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
    primary_price_provider: PriceDataProvider
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
            tail_catch_up.get("applied") if isinstance(tail_catch_up, dict) else None
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
        typer.Option(
            help=(
                "校验日期，格式为 YYYY-MM-DD；默认最近已完成且已过 "
                "provider-ready buffer 的美股交易日。"
            )
        ),
    ] = None,
    execution_profile: Annotated[
        str,
        typer.Option(
            "--execution-profile",
            help=(
                "执行 profile：auto、daily_default.v1 或 manual.v1；auto 在显式 "
                "--as-of 时使用 manual.v1，仅默认输入的日常执行可自动使用 daily_default.v1。"
            ),
        ),
    ] = AUTO_DATA_QUALITY_EXECUTION_PROFILE_ID,
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
    normalized_prices_path = _resolve_project_input_path(prices_path)
    normalized_rates_path = _resolve_project_input_path(rates_path)
    normalized_backtest_manifest_path = (
        _resolve_project_input_path(backtest_manifest_path)
        if backtest_manifest_path is not None
        else None
    )
    resolved_execution_profile = _resolve_data_quality_execution_profile(
        execution_profile=execution_profile,
        as_of_was_explicit=as_of is not None,
        prices_path=normalized_prices_path,
        rates_path=normalized_rates_path,
        full_universe=full_universe,
        backtest_manifest_path=normalized_backtest_manifest_path,
    )
    universe = load_universe()
    quality_config = load_data_quality()
    observed_at = datetime.now(tz=UTC)
    validation_date = (
        _parse_date(as_of) if as_of else resolve_default_data_quality_as_of(observed_at)
    )
    report_path = output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )

    consistency_starts = (
        quality_config.prices.consistency_start_date or DEFAULT_DATA_QUALITY_WINDOW_START,
        quality_config.rates.consistency_start_date or DEFAULT_DATA_QUALITY_WINDOW_START,
        validation_date,
    )
    requested_window = DataQualityDateWindow(min(consistency_starts), validation_date)
    require_secondary_prices = _requires_marketstack_prices(normalized_prices_path)
    secondary_candidate = _marketstack_prices_path(normalized_prices_path)
    secondary_prices_path = (
        secondary_candidate if require_secondary_prices or secondary_candidate.is_file() else None
    )
    try:
        request = CanonicalDataQualityExecutionRequest(
            as_of=validation_date,
            requested_window=requested_window,
            prices_path=normalized_prices_path,
            rates_path=normalized_rates_path,
            manifest_path=_download_manifest_path(normalized_prices_path),
            expected_price_tickers=tuple(
                configured_price_tickers(
                    universe,
                    include_full_ai_chain=full_universe,
                )
            ),
            expected_rate_series=tuple(configured_rate_series(universe)),
            secondary_prices_path=secondary_prices_path,
            require_secondary_prices=require_secondary_prices,
            backtest_manifest_path=normalized_backtest_manifest_path,
            execution_profile_id=resolved_execution_profile,
        )
        execution = run_canonical_data_quality_execution(request, project_root=PROJECT_ROOT)
    except DataQualityExecutionContractError as exc:
        console.print(f"[red]数据质量执行失败：{exc.code}；{exc.message}[/red]")
        raise typer.Exit(code=1) from exc

    report = execution.report
    try:
        write_data_quality_report(report, report_path)
        discovery = (
            publish_default_data_quality_execution_discovery(
                execution,
                project_root=PROJECT_ROOT,
            )
            if resolved_execution_profile == DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID
            else None
        )
        audit_record_path = write_validate_data_audit_sidecar(
            report=report,
            report_path=report_path,
            started_at=execution.receipt.started_at,
            ended_at=execution.receipt.ended_at,
            execution_result=execution,
            output_dir=(PROJECT_ROOT / "artifacts" / "data_refresh_audit" / "validation"),
        )
    except (DataQualityExecutionError, OSError) as exc:
        code = getattr(exc, "code", "DQ_RECEIPT_FIELDS_INVALID")
        message = getattr(exc, "message", str(exc))
        console.print(f"[red]数据质量证据发布失败：{code}；{message}[/red]")
        raise typer.Exit(code=1) from exc

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]数据质量状态：{report.status}[/{status_style}]")
    console.print(f"兼容报告：{report_path}")
    console.print(f"Canonical report：{execution.report_path}")
    console.print(f"Canonical receipt：{execution.receipt_path}")
    console.print(
        "请求/评估窗口："
        f"{execution.receipt.requested_window.start.isoformat()}.."
        f"{execution.receipt.requested_window.end.isoformat()} / "
        f"{execution.receipt.evaluated_window.start.isoformat()}.."
        f"{execution.receipt.evaluated_window.end.isoformat()}"
    )
    if discovery is not None:
        console.print(f"DQ discovery pointer：{discovery.pointer_path}")
    else:
        console.print("DQ discovery pointer：未发布（非 daily_default profile）")
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


def _external_request_cache_identity_from_metadata(
    metadata_path: Path,
) -> dict[str, Any]:
    try:
        resolved_path = metadata_path.resolve(strict=True)
        payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"无法读取合法 external request cache metadata：{metadata_path}") from exc
    if (
        resolved_path.name != "metadata.json"
        or not isinstance(payload, Mapping)
        or payload.get("schema_version") != "external_request_cache.v2"
        or len(resolved_path.parents) < 5
    ):
        raise ValueError("显式失效只接受标准路径下的 external_request_cache.v2 metadata.json")
    request_identity = payload.get("request_identity")
    if not isinstance(request_identity, Mapping):
        raise ValueError("metadata 缺少脱敏 request_identity")
    required = ("provider", "api_family", "method", "url")
    if any(not isinstance(payload.get(field), str) or not payload.get(field) for field in required):
        raise ValueError("metadata 缺少 provider/api_family/method/url")
    mappings: dict[str, Mapping[str, object]] = {}
    for field in ("params", "headers", "json_payload"):
        value = request_identity.get(field)
        if not isinstance(value, Mapping):
            raise ValueError(f"metadata request_identity.{field} 必须是 object")
        mappings[field] = value
    return {
        "provider": payload["provider"],
        "api_family": payload["api_family"],
        "method": payload["method"],
        "url": payload["url"],
        "cache_dir": resolved_path.parents[4],
        **mappings,
    }


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


def _resolve_project_input_path(path: Path) -> Path:
    candidate = path if path.is_absolute() else PROJECT_ROOT / path
    return candidate.resolve(strict=False)


def _requires_marketstack_prices(prices_path: Path) -> bool:
    default_prices_path = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
    try:
        return prices_path.resolve() == default_prices_path.resolve()
    except OSError:
        return prices_path == default_prices_path


def _is_daily_default_data_quality_profile(
    *,
    prices_path: Path,
    rates_path: Path,
    full_universe: bool,
    backtest_manifest_path: Path | None,
) -> bool:
    if full_universe or backtest_manifest_path is not None:
        return False
    expected_prices = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
    expected_rates = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"
    try:
        return (
            prices_path.resolve() == expected_prices.resolve()
            and rates_path.resolve() == expected_rates.resolve()
        )
    except OSError:
        return prices_path == expected_prices and rates_path == expected_rates


def _resolve_data_quality_execution_profile(
    *,
    execution_profile: str,
    as_of_was_explicit: bool,
    prices_path: Path,
    rates_path: Path,
    full_universe: bool,
    backtest_manifest_path: Path | None,
) -> str:
    normalized_profile = execution_profile.strip().lower()
    if normalized_profile not in ALLOWED_DATA_QUALITY_EXECUTION_PROFILE_IDS:
        allowed = ", ".join(sorted(ALLOWED_DATA_QUALITY_EXECUTION_PROFILE_IDS))
        raise typer.BadParameter(f"execution profile 必须是以下值之一：{allowed}。")

    is_daily_default_shape = _is_daily_default_data_quality_profile(
        prices_path=prices_path,
        rates_path=rates_path,
        full_universe=full_universe,
        backtest_manifest_path=backtest_manifest_path,
    )
    if normalized_profile == DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID:
        if not is_daily_default_shape:
            raise typer.BadParameter(
                "daily_default.v1 只允许默认 prices/rates、非 full-universe "
                "且无 backtest manifest。"
            )
        return DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID
    if normalized_profile == MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID:
        return MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID
    if not as_of_was_explicit and is_daily_default_shape:
        return DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID
    return MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID
