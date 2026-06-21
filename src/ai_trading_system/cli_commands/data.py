from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.cache_catalog import (
    DEFAULT_CACHE_CATALOG_DIR,
    DEFAULT_CACHE_CATALOG_POLICY_PATH,
    build_and_write_cache_catalog,
    load_cache_catalog_payload,
    load_cache_catalog_policy,
    resolve_cache_catalog_path,
    validate_cache_catalog_artifact,
)
from ai_trading_system.cache_catalog import (
    DEFAULT_DATA_REFRESH_AUDIT_DIR as CACHE_CATALOG_REFRESH_AUDIT_DIR,
)
from ai_trading_system.cache_catalog import (
    DEFAULT_VALIDATE_DATA_AUDIT_DIR as CACHE_CATALOG_VALIDATE_DATA_AUDIT_DIR,
)
from ai_trading_system.cli_commands.data_artifacts import (
    _parse_date,
    _resolve_market_data_freshness_path,
    _resolve_market_data_refresh_path,
)
from ai_trading_system.config import (
    DEFAULT_DATA_SOURCES_CONFIG_PATH,
    PROJECT_ROOT,
    load_data_sources,
)
from ai_trading_system.data_foundation import (
    DEFAULT_ASSET_MASTER_OUTPUT_ROOT,
    DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT,
    DEFAULT_DATA_FOUNDATION_ACCEPTANCE_REPORT_PATH,
    DEFAULT_DATA_SOURCE_QUALIFICATION_OUTPUT_ROOT,
    DEFAULT_PIT_FEATURE_STORE_OUTPUT_ROOT,
    audit_pit_feature_snapshot,
    audit_universe,
    build_pit_feature_snapshot,
    build_tradability_calendar,
    query_pit_feature,
    run_data_foundation_acceptance,
    run_data_source_qualification_remediation,
    show_universe,
    validate_asset_master,
)
from ai_trading_system.data_refresh_audit import (
    DEFAULT_DATA_REFRESH_AUDIT_DIR,
    DEFAULT_VALIDATION_AUDIT_DIR,
    build_and_write_data_refresh_audit,
    load_data_refresh_audit_payload,
    resolve_data_refresh_audit_path,
    validate_data_refresh_audit_artifact,
)
from ai_trading_system.data_source_fallback_policy import (
    DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    DEFAULT_DATA_SOURCE_FALLBACK_POLICY_PATH,
    build_and_write_data_source_fallback_policy,
    latest_data_source_fallback_policy_summary,
    load_data_source_fallback_policy,
    load_data_source_fallback_policy_payload,
    resolve_data_source_fallback_policy_path,
    validate_data_source_fallback_policy_artifact,
)
from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    run_backtest_input_diagnostics,
)
from ai_trading_system.trading_engine.data.price_history_repair import (
    build_price_history_repair_provider,
    repair_backtest_price_history,
)
from ai_trading_system.trading_engine.data_registry_consistency import (
    run_data_registry_consistency,
    validate_backtest_manifest_consistency,
)
from ai_trading_system.trading_engine.market_data_freshness import (
    DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH,
    load_market_data_freshness_payload,
    run_market_data_freshness,
    validate_market_data_freshness_payload,
)
from ai_trading_system.trading_engine.market_data_refresh import (
    DEFAULT_MARKET_DATA_REFRESH_CONFIG_PATH,
    load_market_data_refresh_payload,
    run_market_data_refresh,
    validate_market_data_refresh_payload,
)
from ai_trading_system.trading_engine.parameters import DEFAULT_SHADOW_BACKTEST_CONFIG_PATH
from ai_trading_system.trading_engine.price_cache_reconcile import (
    refresh_backtest_manifest,
    run_price_cache_reconcile,
)

console = Console()
data_app = typer.Typer(help="缓存数据诊断和 backtest input repair planning。", no_args_is_help=True)
refresh_audit_app = typer.Typer(help="Data refresh audit trail 治理报告。")
fallback_policy_app = typer.Typer(help="Data source fallback policy 治理报告。")
cache_catalog_app = typer.Typer(help="Checksum and cache catalog 治理报告。")
pit_feature_store_app = typer.Typer(help="PIT feature store and snapshot registry。")
asset_master_app = typer.Typer(help="Asset master and tradability calendar。")
universe_app = typer.Typer(help="Research universe as-of view and audit。")
foundation_acceptance_app = typer.Typer(help="TRADING-734 data foundation acceptance。")
source_qualification_app = typer.Typer(help="Data source qualification remediation。")
data_app.add_typer(refresh_audit_app, name="refresh-audit")
data_app.add_typer(fallback_policy_app, name="fallback-policy")
data_app.add_typer(cache_catalog_app, name="cache-catalog")
data_app.add_typer(pit_feature_store_app, name="pit-feature-store")
data_app.add_typer(asset_master_app, name="asset-master")
data_app.add_typer(universe_app, name="universe")
data_app.add_typer(foundation_acceptance_app, name="foundation-acceptance")
data_app.add_typer(source_qualification_app, name="source-qualification")


@foundation_acceptance_app.command("run")
def foundation_acceptance_run_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Data foundation acceptance 输出目录。"),
    ] = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT,
) -> None:
    payload = run_data_foundation_acceptance(output_root=output_root)
    _print_foundation_payload(payload)


@source_qualification_app.command("remediate")
def source_qualification_remediate_command(
    acceptance_report: Annotated[
        Path,
        typer.Option("--acceptance-report", help="TRADING-734 acceptance report JSON。"),
    ] = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_REPORT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Source qualification remediation 输出目录。"),
    ] = DEFAULT_DATA_SOURCE_QUALIFICATION_OUTPUT_ROOT,
) -> None:
    payload = run_data_source_qualification_remediation(
        acceptance_report_path=acceptance_report,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@pit_feature_store_app.command("build-snapshot")
def pit_feature_store_build_snapshot_command(
    as_of_date: Annotated[str, typer.Option("--as-of-date", help="Snapshot as-of date。")],
    decision_time: Annotated[str, typer.Option("--decision-time", help="Decision timestamp。")],
    asset_universe: Annotated[
        str,
        typer.Option("--asset-universe", help="Universe id。"),
    ] = "data_foundation_minimum",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="PIT feature store 输出目录。"),
    ] = DEFAULT_PIT_FEATURE_STORE_OUTPUT_ROOT,
) -> None:
    payload = build_pit_feature_snapshot(
        as_of_date=as_of_date,
        decision_time=decision_time,
        asset_universe=asset_universe,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@pit_feature_store_app.command("audit")
def pit_feature_store_audit_command(
    snapshot_id: Annotated[str, typer.Option("--snapshot-id", help="Snapshot id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="PIT feature store 输出目录。"),
    ] = DEFAULT_PIT_FEATURE_STORE_OUTPUT_ROOT,
) -> None:
    payload = audit_pit_feature_snapshot(snapshot_id=snapshot_id, output_root=output_root)
    _print_foundation_payload(payload)


@pit_feature_store_app.command("query")
def pit_feature_store_query_command(
    feature_id: Annotated[str, typer.Option("--feature-id", help="Feature id。")],
    asset_id: Annotated[str, typer.Option("--asset-id", help="Asset id。")],
    as_of_date: Annotated[str, typer.Option("--as-of-date", help="As-of date。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="PIT feature store 输出目录。"),
    ] = DEFAULT_PIT_FEATURE_STORE_OUTPUT_ROOT,
) -> None:
    payload = query_pit_feature(
        feature_id=feature_id,
        asset_id=asset_id,
        as_of_date=as_of_date,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@asset_master_app.command("validate")
def asset_master_validate_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Asset master 输出目录。"),
    ] = DEFAULT_ASSET_MASTER_OUTPUT_ROOT,
) -> None:
    payload = validate_asset_master(output_root=output_root)
    _print_foundation_payload(payload)


@asset_master_app.command("build-tradability-calendar")
def asset_master_build_tradability_calendar_command(
    universe: Annotated[
        str,
        typer.Option("--universe", help="Universe id。"),
    ] = "data_foundation_minimum",
    date_range: Annotated[
        str,
        typer.Option("--date-range", help="Date range start:end。"),
    ] = "2022-12-01:2022-12-01",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Asset master 输出目录。"),
    ] = DEFAULT_ASSET_MASTER_OUTPUT_ROOT,
) -> None:
    payload = build_tradability_calendar(
        universe=universe,
        date_range=date_range,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@universe_app.command("show")
def universe_show_command(
    universe: Annotated[str, typer.Option("--universe", help="Universe id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Asset master 输出目录。"),
    ] = DEFAULT_ASSET_MASTER_OUTPUT_ROOT,
) -> None:
    payload = show_universe(universe=universe, output_root=output_root)
    _print_foundation_payload(payload)


@universe_app.command("audit")
def universe_audit_command(
    universe: Annotated[str, typer.Option("--universe", help="Universe id。")],
    date_range: Annotated[
        str,
        typer.Option("--date-range", help="Date range start:end。"),
    ] = "2022-12-01:2022-12-01",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Asset master 输出目录。"),
    ] = DEFAULT_ASSET_MASTER_OUTPUT_ROOT,
) -> None:
    payload = audit_universe(universe=universe, date_range=date_range, output_root=output_root)
    _print_foundation_payload(payload)


@data_app.command("diagnose-backtest-inputs")
def data_diagnose_backtest_inputs_command(
    latest: Annotated[
        bool,
        typer.Option(help="使用价格缓存中的最新可用日期。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="诊断日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="shadow backtest 配置路径。"),
    ] = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
) -> None:
    """诊断 shadow backtest 输入数据并生成结构化质量报告。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    run = run_backtest_input_diagnostics(as_of=run_date, config_path=config_path)
    summary = run.payload.get("summary", {})
    status = summary.get("overall_status", "UNKNOWN") if isinstance(summary, dict) else "UNKNOWN"
    style = "green" if status == "OK" else "yellow" if status == "LIMITED" else "red"
    console.print(f"[{style}]Backtest input diagnostics：{status}[/{style}]")
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    console.print(f"Snapshot manifest：{run.manifest_path}")
    if isinstance(summary, dict):
        console.print(
            f"blocking_errors={summary.get('blocking_errors', 0)}；"
            f"warnings={summary.get('warnings', 0)}；"
            f"can_run_shadow_backtest={summary.get('can_run_shadow_backtest', False)}"
        )
    console.print("production_effect=none；不修改 production 参数或 promotion 规则")


@data_app.command("inspect-registry")
def data_inspect_registry_command(
    latest: Annotated[
        bool,
        typer.Option(help="解析 latest data registry / manifest 状态。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="registry 诊断日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="shadow backtest 配置路径。"),
    ] = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
) -> None:
    """检查 repair、manifest、validate-data 与 portfolio sensitivity 的数据视图一致性。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    run = run_data_registry_consistency(as_of=run_date, config_path=config_path)
    metadata = run.payload.get("metadata", {})
    latest_resolution = run.payload.get("latest_resolution", {})
    path_consistency = run.payload.get("path_consistency", {})
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    style = "green" if status == "OK" else "yellow" if status == "LIMITED" else "red"
    console.print(f"[{style}]Data registry consistency：{status}[/{style}]")
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    if isinstance(latest_resolution, dict):
        console.print(
            "latest_resolution="
            f"{latest_resolution.get('status', 'UNKNOWN')}；"
            f"market_data={latest_resolution.get('resolved_market_data_date', '')}；"
            f"manifest={latest_resolution.get('resolved_backtest_manifest_date', '')}"
        )
    if isinstance(path_consistency, dict):
        console.print(
            "price_cache_paths="
            f"{path_consistency.get('status', 'UNKNOWN')}；"
            f"validate_data_read_path={path_consistency.get('validate_data_read_path', '')}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@data_app.command("validate-backtest-manifest")
def data_validate_backtest_manifest_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 latest valid backtest input manifest 与价格缓存一致性。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="manifest 校验日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="shadow backtest 配置路径。"),
    ] = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
) -> None:
    """验证 backtest_input_manifest 与实际价格缓存、symbol mapping 是否一致。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    result = validate_backtest_manifest_consistency(as_of=run_date, config_path=config_path)
    status = str(result.get("status") or "UNKNOWN")
    style = "green" if status == "OK" else "yellow" if status == "LIMITED" else "red"
    console.print(f"[{style}]Backtest manifest consistency：{status}[/{style}]")
    manifest = result.get("backtest_manifest", {})
    if isinstance(manifest, dict):
        console.print(f"manifest：{manifest.get('path', '')}")
        console.print(f"manifest_validation={manifest.get('validation_status', 'UNKNOWN')}")
    for asset in result.get("asset_registry", []):
        if not isinstance(asset, dict):
            continue
        symbol = asset.get("canonical_symbol", "")
        source_symbol = asset.get("source_symbol", "")
        code = asset.get("error_code", "OK")
        if code == "OK":
            suffix = f" via {source_symbol}" if source_symbol and source_symbol != symbol else ""
            console.print(f"{symbol}: OK{suffix}")
        else:
            console.print(f"[red]{symbol}: {code}[/red] - {asset.get('diagnosis', '')}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
    if status == "FAILED":
        raise typer.Exit(code=1)


@data_app.command(
    "reconcile-price-cache",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def data_reconcile_price_cache_command(
    ctx: typer.Context,
    latest: Annotated[
        bool,
        typer.Option(help="为 latest data registry mismatch 执行或规划 reconcile。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="reconcile 诊断日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="shadow backtest 配置路径。"),
    ] = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(help="只输出修复计划，不改写价格缓存。"),
    ] = False,
    refresh_manifest_only: Annotated[
        bool,
        typer.Option(help="只刷新 backtest input manifest，不注册 repaired artifacts。"),
    ] = False,
    register_repaired_only: Annotated[
        bool,
        typer.Option(help="只注册 repaired artifacts，不刷新 backtest input manifest。"),
    ] = False,
    symbols: Annotated[
        list[str] | None,
        typer.Option(
            "--symbols",
            help="指定 reconcile 资产；可重复。也兼容 `--symbols GOOGL BRK.B SGOV`。",
        ),
    ] = None,
) -> None:
    """执行 price cache / manifest reconcile；dry-run 只输出计划。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    if refresh_manifest_only and register_repaired_only:
        raise typer.BadParameter("--refresh-manifest-only 不能和 --register-repaired-only 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    requested_symbols = tuple([*(symbols or []), *[str(item) for item in ctx.args]])
    run = run_price_cache_reconcile(
        as_of=run_date,
        config_path=config_path,
        dry_run=dry_run,
        refresh_manifest_only=refresh_manifest_only,
        register_repaired_only=register_repaired_only,
        symbols=requested_symbols,
    )
    result = run.payload
    metadata = result.get("metadata", {})
    status = str(result.get("status") or "UNKNOWN")
    if isinstance(metadata, dict):
        status = str(metadata.get("status") or status)
    style = (
        "green"
        if status in {"OK", "NOT_REQUIRED"}
        else (
            "yellow"
            if status
            in {
                "DRY_RUN",
                "LIMITED",
            }
            else "red"
        )
    )
    console.print(f"[{style}]Price cache reconcile：{status}[/{style}]")
    if run.json_path is not None:
        console.print(f"JSON：{run.json_path}")
    if run.markdown_path is not None:
        console.print(f"Markdown：{run.markdown_path}")
    console.print(f"Price cache registry：{run.registry_path}")
    for step in result.get("planned_actions", []):
        if not isinstance(step, dict):
            continue
        console.print(
            "- "
            f"action={step.get('action', '')}；"
            f"symbols={', '.join(str(item) for item in step.get('symbols', [])) or 'n/a'}"
        )
    for item in result.get("repaired_artifact_inspection", []):
        if not isinstance(item, dict):
            continue
        console.print(
            f"{item.get('canonical_symbol')}: {item.get('status')} "
            f"via {item.get('source_symbol')} rows={item.get('rows')} "
            f"error_code={item.get('error_code')}"
        )
    after = result.get("after", {})
    if isinstance(after, dict):
        console.print(
            "after="
            f"latest_resolution={after.get('latest_resolution', 'UNKNOWN')}；"
            f"market_data={after.get('market_data_date', '')}；"
            f"manifest={after.get('manifest_date', '')}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
    if status == "FAILED":
        raise typer.Exit(code=1)


@data_app.command("refresh-backtest-manifest")
def data_refresh_backtest_manifest_command(
    latest: Annotated[
        bool,
        typer.Option(help="刷新 latest backtest input manifest。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="manifest 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="shadow backtest 配置路径。"),
    ] = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(help="只显示将写入的 manifest，不实际生成。"),
    ] = False,
) -> None:
    """刷新 backtest input manifest；dry-run 不写 artifact。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    run = refresh_backtest_manifest(
        as_of=run_date,
        config_path=config_path,
        dry_run=dry_run,
    )
    metadata = run.payload.get("metadata", {})
    status = str(metadata.get("status") if isinstance(metadata, dict) else "UNKNOWN")
    style = "green" if status == "OK" else "yellow" if status == "DRY_RUN" else "red"
    console.print(f"[{style}]Backtest manifest refresh：{status}[/{style}]")
    console.print(f"target_manifest_date={run.payload.get('target_manifest_date', '')}")
    if run.diagnostic_run is not None:
        console.print(f"Diagnostic JSON：{run.diagnostic_run.json_path}")
        console.print(f"Snapshot manifest：{run.diagnostic_run.manifest_path}")
    else:
        console.print(f"would_write_manifest={run.payload.get('would_write_manifest', '')}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
    if status == "FAILED":
        raise typer.Exit(code=1)


@data_app.command("freshness")
def data_freshness_command(
    latest: Annotated[
        bool,
        typer.Option(help="使用 raw price cache latest date 作为 tracking date。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="freshness tracking 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    market: Annotated[
        str,
        typer.Option("--market", help="市场代码；当前支持 US。"),
    ] = "US",
    config_path: Annotated[
        Path,
        typer.Option("--config", help="market data freshness 配置路径。"),
    ] = DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(help="写入 outputs/dry_runs/data_freshness，不改正式 artifacts。"),
    ] = False,
) -> None:
    """生成 market data freshness 和 tracking readiness 报告。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    try:
        run = run_market_data_freshness(
            as_of=run_date,
            market=market,
            config_path=config_path,
            dry_run=dry_run,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    metadata = run.payload.get("metadata", {}) if isinstance(run.payload, dict) else {}
    freshness = run.payload.get("freshness", {}) if isinstance(run.payload, dict) else {}
    data_dates = run.payload.get("data_dates", {}) if isinstance(run.payload, dict) else {}
    readiness = run.payload.get("tracking_readiness", {}) if isinstance(run.payload, dict) else {}
    status = freshness.get("status", metadata.get("status", "UNKNOWN"))
    style = "green" if status in {"OK", "NON_TRADING_DAY"} else "yellow"
    if status in {"MISSING", "FAILED", "MARKET_CALENDAR_UNKNOWN"}:
        style = "red"
    console.print(f"[{style}]Market data freshness：{status}[/{style}]")
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    if isinstance(data_dates, dict):
        console.print(
            "data_dates="
            f"tracking_date={data_dates.get('tracking_date', 'UNKNOWN')}；"
            f"effective_data_date={data_dates.get('effective_data_date', 'UNKNOWN')}；"
            f"latest_manifest_date={data_dates.get('latest_manifest_date', 'UNKNOWN')}"
        )
    if isinstance(freshness, dict):
        console.print(
            "freshness_status="
            f"{freshness.get('status', 'UNKNOWN')}；"
            f"lag_trading_days={freshness.get('lag_trading_days', 'UNKNOWN')}；"
            f"lag_calendar_days={freshness.get('lag_calendar_days', 'UNKNOWN')}"
        )
    if isinstance(readiness, dict):
        console.print(
            "tracking_readiness="
            f"{readiness.get('readiness', 'UNKNOWN')}；"
            f"tracking_status_recommendation="
            f"{readiness.get('tracking_status_recommendation', 'UNKNOWN')}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@data_app.command("validate-freshness")
def data_validate_freshness_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 market data freshness artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="freshness 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 market_data_freshness_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 market data freshness report schema、安全字段和 readiness 输出。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_market_data_freshness_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_market_data_freshness_payload(source_path)
    issues = validate_market_data_freshness_payload(payload)
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    freshness = payload.get("freshness", {}) if isinstance(payload, dict) else {}
    readiness = payload.get("tracking_readiness", {}) if isinstance(payload, dict) else {}
    status = (
        freshness.get("status", metadata.get("status", "UNKNOWN"))
        if isinstance(freshness, dict)
        else "UNKNOWN"
    )
    style = "green" if not issues and status in {"OK", "NON_TRADING_DAY"} else "yellow"
    if issues or status in {"MISSING", "FAILED", "MARKET_CALENDAR_UNKNOWN"}:
        style = "red"
    console.print(f"[{style}]Market data freshness validation：{status}[/{style}]")
    console.print(f"source：{source_path}")
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    if isinstance(freshness, dict):
        console.print(f"freshness_status={freshness.get('status', 'UNKNOWN')}")
    if isinstance(readiness, dict):
        console.print(f"tracking_readiness={readiness.get('readiness', 'UNKNOWN')}")
        console.print(
            "tracking_status_recommendation="
            f"{readiness.get('tracking_status_recommendation', 'UNKNOWN')}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
    if status in {"FAILED", "MISSING", "MARKET_CALENDAR_UNKNOWN"}:
        raise typer.Exit(code=1)


@data_app.command(
    "refresh-market",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def data_refresh_market_command(
    ctx: typer.Context,
    latest: Annotated[
        bool,
        typer.Option(help="使用最新 market data freshness report 生成或执行 refresh。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="refresh 目标日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="market data refresh 配置路径。"),
    ] = DEFAULT_MARKET_DATA_REFRESH_CONFIG_PATH,
    symbols: Annotated[
        list[str] | None,
        typer.Option(
            "--symbols",
            help="指定 refresh 资产；可重复。也兼容 `--symbols GOOGL BRK.B SGOV`。",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(help="只生成 refresh plan，不写价格缓存、registry 或 manifest。"),
    ] = False,
    plan_only: Annotated[
        bool,
        typer.Option(help="只生成 refresh plan，不执行 recovery。"),
    ] = False,
) -> None:
    """刷新 stale market data 并输出 freshness recovery summary。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    requested_symbols = tuple([*(symbols or []), *[str(item) for item in ctx.args]])
    run = run_market_data_refresh(
        as_of=run_date,
        symbols=requested_symbols,
        config_path=config_path,
        dry_run=dry_run,
        plan_only=plan_only,
    )
    metadata = run.payload.get("metadata", {}) if isinstance(run.payload, dict) else {}
    status = str(metadata.get("status") or "UNKNOWN")
    style = "green" if status in {"OK", "NOT_NEEDED"} else "yellow"
    if status in {"FAILED", "BLOCKED"}:
        style = "red"
    mode = "PLAN" if dry_run or plan_only else "EXECUTE"
    console.print(f"[{style}]Market data refresh：{status} ({mode})[/{style}]")
    console.print(f"Plan JSON：{run.plan_path}")
    if run.json_path is not None:
        console.print(f"JSON：{run.json_path}")
    if run.markdown_path is not None:
        console.print(f"Markdown：{run.markdown_path}")
    freshness_input = run.payload.get("freshness_input", {})
    if not isinstance(freshness_input, dict) or not freshness_input:
        before = run.payload.get("before", {})
        actions = run.payload.get("actions", {})
        if isinstance(before, dict) and isinstance(actions, dict):
            freshness_input = {
                "freshness_status": before.get("freshness_status", "UNKNOWN"),
                "required_target_date": actions.get("target_date", ""),
            }
    if isinstance(freshness_input, dict):
        console.print(
            "freshness_input="
            f"status={freshness_input.get('freshness_status', 'UNKNOWN')}；"
            f"target_date={freshness_input.get('required_target_date', '')}"
        )
    actions = run.payload.get("actions", {})
    if isinstance(actions, dict):
        fetched_assets = (
            ", ".join(str(item) for item in actions.get("fetched_assets", [])) or "none"
        )
        console.print(
            "actions="
            f"target_date={actions.get('target_date', '')}；"
            f"fetched_assets={fetched_assets}；"
            f"manifest_refreshed={actions.get('refreshed_backtest_manifest', False)}"
        )
    after = run.payload.get("after", {})
    if isinstance(after, dict):
        console.print(
            "after="
            f"freshness_status={after.get('freshness_status', 'UNKNOWN')}；"
            f"tracking_readiness={after.get('tracking_readiness', 'UNKNOWN')}；"
            f"candidate_tracking_status={after.get('candidate_tracking_status', 'UNKNOWN')}"
        )
    for item in run.payload.get("asset_results", []):
        if isinstance(item, dict):
            console.print(
                f"{item.get('symbol', '')}: {item.get('status', '')} "
                f"via {item.get('source_symbol', '')} source={item.get('source', '')}"
            )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
    if status in {"FAILED", "BLOCKED"}:
        raise typer.Exit(code=1)


@data_app.command("validate-refresh")
def data_validate_refresh_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 market data refresh artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="refresh 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 market_data_refresh_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 market data refresh report schema、安全字段和 recovery 输出。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_market_data_refresh_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_market_data_refresh_payload(source_path)
    issues = validate_market_data_refresh_payload(payload)
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    status = str(metadata.get("status") or "UNKNOWN")
    style = "green" if not issues and status in {"OK", "NOT_NEEDED"} else "yellow"
    if issues or status in {"FAILED", "BLOCKED"}:
        style = "red"
    console.print(f"[{style}]Market data refresh validation：{status}[/{style}]")
    console.print(f"source：{source_path}")
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    console.print(f"refresh_status={status}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
    if status in {"FAILED", "BLOCKED"}:
        raise typer.Exit(code=1)


@fallback_policy_app.command("run")
def data_source_fallback_policy_run_command(
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="fallback policy 评估日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option(help="data_sources.yaml 路径。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    policy_path: Annotated[
        Path,
        typer.Option(help="data source fallback policy YAML 路径。"),
    ] = DEFAULT_DATA_SOURCE_FALLBACK_POLICY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="Data source fallback policy artifact 根目录。"),
    ] = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    unavailable_source_id: Annotated[
        list[str] | None,
        typer.Option(
            "--unavailable-source-id",
            help="显式标记不可用的 source_id；可重复。",
        ),
    ] = None,
    fallback_used_source_id: Annotated[
        list[str] | None,
        typer.Option(
            "--fallback-used-source-id",
            help="显式标记已使用且已披露 metadata 的 fallback source_id；可重复。",
        ),
    ] = None,
    fallback_reason: Annotated[
        list[str] | None,
        typer.Option(
            "--fallback-reason",
            help="fallback reason，格式为 source_id=reason 或 data_type=reason；可重复。",
        ),
    ] = None,
) -> None:
    """生成 paper-shadow research data source fallback policy report。"""
    evaluation_date = _parse_date(as_of) if as_of else date.today()
    payload, paths = build_and_write_data_source_fallback_policy(
        config=load_data_sources(config_path),
        policy=load_data_source_fallback_policy(policy_path),
        as_of=evaluation_date,
        output_dir=output_dir,
        unavailable_source_ids=unavailable_source_id or [],
        fallback_used_source_ids=fallback_used_source_id or [],
        fallback_reasons=_parse_key_value_options(fallback_reason or []),
    )
    _print_fallback_policy_summary(payload, paths.get("report_json"))
    if payload.get("status") == "FAIL":
        raise typer.Exit(code=1)


@fallback_policy_app.command("report")
def data_source_fallback_policy_report_command(
    report_id: Annotated[
        str | None,
        typer.Option(help="要读取的 fallback policy report_id；不传时可用 --latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取 latest fallback policy artifact。"),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option(help="Data source fallback policy artifact 根目录。"),
    ] = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
) -> None:
    """读取 paper-shadow research data source fallback policy report。"""
    report_path = resolve_data_source_fallback_policy_path(
        report_id=report_id,
        latest=latest or report_id is None,
        output_dir=output_dir,
    )
    payload = load_data_source_fallback_policy_payload(report_path)
    _print_fallback_policy_summary(payload, report_path)
    if payload.get("status") == "FAIL":
        raise typer.Exit(code=1)


@fallback_policy_app.command("validate")
def data_source_fallback_policy_validate_command(
    report_id: Annotated[
        str | None,
        typer.Option(help="要校验的 fallback policy report_id；不传时可用 --latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="校验 latest fallback policy artifact。"),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option(help="Data source fallback policy artifact 根目录。"),
    ] = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
) -> None:
    """校验 fallback policy states、eligibility、metadata 和安全边界。"""
    validation, report_path = validate_data_source_fallback_policy_artifact(
        report_id=report_id,
        latest=latest or report_id is None,
        output_dir=output_dir,
    )
    status_style = (
        "green" if validation.status == "PASS" else "yellow" if validation.passed else "red"
    )
    console.print(
        f"[{status_style}]Data source fallback policy validation status="
        f"{validation.status}[/{status_style}]"
    )
    console.print(f"report_id={validation.report_id}")
    console.print(f"report={report_path}")
    console.print(f"source_group_count={validation.source_group_count}")
    console.print(f"error_count={validation.error_count}; warning_count={validation.warning_count}")
    console.print("production_effect=none；校验只读 existing artifact。")
    if not validation.passed:
        raise typer.Exit(code=1)


@cache_catalog_app.command("run")
def cache_catalog_run_command(
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="cache catalog 评估日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option(help="data_sources.yaml 路径。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    policy_path: Annotated[
        Path,
        typer.Option(help="cache catalog YAML 路径。"),
    ] = DEFAULT_CACHE_CATALOG_POLICY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="Cache catalog artifact 根目录。"),
    ] = DEFAULT_CACHE_CATALOG_DIR,
    expected_checksum: Annotated[
        list[str] | None,
        typer.Option(
            "--expected-checksum",
            help="显式 checksum 断言，格式为 entry_id=sha256；可重复。",
        ),
    ] = None,
    previous_catalog_path: Annotated[
        Path | None,
        typer.Option(help="显式 previous cache catalog JSON；缺省读取 latest。"),
    ] = None,
    refresh_audit_report_path: Annotated[
        Path | None,
        typer.Option(help="显式 data refresh audit JSON；缺省读取 latest。"),
    ] = None,
    refresh_audit_output_dir: Annotated[
        Path,
        typer.Option(help="Data refresh audit artifact 根目录。"),
    ] = CACHE_CATALOG_REFRESH_AUDIT_DIR,
    validation_audit_dir: Annotated[
        Path,
        typer.Option(help="validate-data audit sidecar 根目录。"),
    ] = CACHE_CATALOG_VALIDATE_DATA_AUDIT_DIR,
) -> None:
    """生成 read-only checksum/cache catalog。"""
    evaluation_date = _parse_date(as_of) if as_of else date.today()
    payload, paths = build_and_write_cache_catalog(
        config=load_data_sources(config_path),
        policy=load_cache_catalog_policy(policy_path),
        as_of=evaluation_date,
        output_dir=output_dir,
        expected_checksums=_parse_key_value_options(
            expected_checksum or [],
            option_name="--expected-checksum",
        ),
        previous_catalog_path=previous_catalog_path,
        refresh_audit_report_path=refresh_audit_report_path,
        refresh_audit_output_dir=refresh_audit_output_dir,
        validation_audit_dir=validation_audit_dir,
    )
    _print_cache_catalog_summary(payload, paths.get("catalog_json"))
    if payload.get("status") == "FAIL":
        raise typer.Exit(code=1)


@cache_catalog_app.command("report")
def cache_catalog_report_command(
    catalog_id: Annotated[
        str | None,
        typer.Option(help="要读取的 cache catalog id；不传时可用 --latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取 latest cache catalog artifact。"),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option(help="Cache catalog artifact 根目录。"),
    ] = DEFAULT_CACHE_CATALOG_DIR,
) -> None:
    """读取 checksum/cache catalog report。"""
    catalog_path = resolve_cache_catalog_path(
        catalog_id=catalog_id,
        latest=latest or catalog_id is None,
        output_dir=output_dir,
    )
    payload = load_cache_catalog_payload(catalog_path)
    _print_cache_catalog_summary(payload, catalog_path)
    if payload.get("status") == "FAIL":
        raise typer.Exit(code=1)


@cache_catalog_app.command("validate")
def cache_catalog_validate_command(
    catalog_id: Annotated[
        str | None,
        typer.Option(help="要校验的 cache catalog id；不传时可用 --latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="校验 latest cache catalog artifact。"),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option(help="Cache catalog artifact 根目录。"),
    ] = DEFAULT_CACHE_CATALOG_DIR,
) -> None:
    """校验 cache catalog schema、checksum、missing entry 和安全边界。"""
    validation, catalog_path = validate_cache_catalog_artifact(
        catalog_id=catalog_id,
        latest=latest or catalog_id is None,
        output_dir=output_dir,
    )
    status_style = (
        "green" if validation.status == "PASS" else "yellow" if validation.passed else "red"
    )
    console.print(
        f"[{status_style}]Cache catalog validation status={validation.status}" f"[/{status_style}]"
    )
    console.print(f"catalog_id={validation.catalog_id}")
    console.print(f"catalog={catalog_path}")
    console.print(f"entry_count={validation.entry_count}")
    console.print(f"error_count={validation.error_count}; warning_count={validation.warning_count}")
    console.print("production_effect=none；校验只读 existing artifact。")
    if not validation.passed:
        raise typer.Exit(code=1)


@refresh_audit_app.command("report")
def data_refresh_audit_report_command(
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="audit 评估日期，格式为 YYYY-MM-DD。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="Data refresh audit artifact 根目录。"),
    ] = DEFAULT_DATA_REFRESH_AUDIT_DIR,
    validation_audit_dir: Annotated[
        Path,
        typer.Option(help="validate-data audit sidecar 根目录。"),
    ] = DEFAULT_VALIDATION_AUDIT_DIR,
    market_refresh_root: Annotated[
        Path | None,
        typer.Option(
            help="market_data_refresh_summary.json 根目录；默认使用项目 artifact 根目录。"
        ),
    ] = None,
    price_cache_path: Annotated[
        Path,
        typer.Option(help="只读 price cache 路径，用于 skipped refresh checksum/row count。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    fallback_policy_report_path: Annotated[
        Path | None,
        typer.Option(help="显式 fallback policy report JSON 路径；缺省读取 latest。"),
    ] = None,
    fallback_policy_output_dir: Annotated[
        Path,
        typer.Option(help="Data source fallback policy artifact 根目录。"),
    ] = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    cache_catalog_report_path: Annotated[
        Path | None,
        typer.Option(help="显式 cache catalog JSON 路径；缺省读取 latest。"),
    ] = None,
    cache_catalog_output_dir: Annotated[
        Path,
        typer.Option(help="Cache catalog artifact 根目录。"),
    ] = DEFAULT_CACHE_CATALOG_DIR,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="只读取 latest artifact，不生成新 audit。"),
    ] = False,
) -> None:
    """生成或读取 paper-shadow data refresh audit trail。"""
    if latest:
        audit_path = resolve_data_refresh_audit_path(latest=True, output_dir=output_dir)
        payload = load_data_refresh_audit_payload(audit_path)
        paths = {"audit_json": audit_path}
    else:
        evaluation_date = _parse_date(as_of) if as_of else None
        if evaluation_date is None:
            evaluation_date = date.today()
        payload, paths = build_and_write_data_refresh_audit(
            as_of=evaluation_date,
            output_dir=output_dir,
            validation_audit_dir=validation_audit_dir,
            market_refresh_root=market_refresh_root,
            price_cache_path=price_cache_path,
            fallback_policy_report_path=fallback_policy_report_path,
            fallback_policy_output_dir=fallback_policy_output_dir,
            cache_catalog_report_path=cache_catalog_report_path,
            cache_catalog_output_dir=cache_catalog_output_dir,
        )

    summary = payload.get("summary", {})
    status = str(payload.get("status", "UNKNOWN"))
    status_style = "green" if status == "PASS" else "yellow" if status != "FAIL" else "red"
    console.print(f"[{status_style}]Data refresh audit status={status}[/{status_style}]")
    console.print(f"audit_id={payload.get('audit_id')}")
    if isinstance(summary, dict):
        console.print(f"audit_record_count={summary.get('audit_record_count')}")
        console.print(
            "record_counts="
            f"failed:{summary.get('failed_record_count')}, "
            f"skipped:{summary.get('skipped_record_count')}, "
            f"warnings:{summary.get('warning_count')}, "
            f"errors:{summary.get('error_count')}"
        )
        console.print(f"next_action={summary.get('next_action')}")
    cache_catalog = payload.get("cache_catalog_summary", {})
    if isinstance(cache_catalog, dict):
        console.print(
            "cache_catalog="
            f"integrity={cache_catalog.get('cache_integrity_status', 'MISSING')}; "
            f"missing_required={cache_catalog.get('missing_required_count', 0)}; "
            f"checksum_mismatch={cache_catalog.get('checksum_mismatch_count', 0)}"
        )
    console.print(f"report={paths.get('audit_json')}")
    console.print("production_effect=none；只读治理报告，不刷新数据、不补造 cache、不触发 broker。")

    if status == "FAIL":
        raise typer.Exit(code=1)


@refresh_audit_app.command("validate")
def validate_data_refresh_audit_command(
    audit_id: Annotated[
        str | None,
        typer.Option(help="要校验的 audit_id；不传时可用 --latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="校验 latest data refresh audit。"),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option(help="Data refresh audit artifact 根目录。"),
    ] = DEFAULT_DATA_REFRESH_AUDIT_DIR,
) -> None:
    """校验 data refresh audit schema、status、checksum、record counts 和安全边界。"""
    validation, audit_path = validate_data_refresh_audit_artifact(
        audit_id=audit_id,
        latest=latest or audit_id is None,
        output_dir=output_dir,
    )
    status_style = (
        "green" if validation.status == "PASS" else "yellow" if validation.passed else "red"
    )
    console.print(
        f"[{status_style}]Data refresh audit validation status={validation.status}"
        f"[/{status_style}]"
    )
    console.print(f"audit_id={validation.audit_id}")
    console.print(f"audit={audit_path}")
    console.print(f"audit_record_count={validation.audit_record_count}")
    console.print(f"error_count={validation.error_count}; warning_count={validation.warning_count}")
    console.print("production_effect=none；校验只读 existing artifact。")

    if not validation.passed:
        raise typer.Exit(code=1)


def _parse_key_value_options(
    values: list[str],
    *,
    option_name: str = "--fallback-reason",
) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise typer.BadParameter(f"{option_name} must use key=value format")
        key, reason = value.split("=", 1)
        key = key.strip()
        if not key:
            raise typer.BadParameter(f"{option_name} key cannot be empty")
        parsed[key] = reason.strip()
    return parsed


def _print_fallback_policy_summary(
    payload: dict[str, object],
    report_path: Path | None,
) -> None:
    summary = latest_data_source_fallback_policy_summary(report_path=report_path)
    status = str(payload.get("status", "UNKNOWN"))
    style = "green" if status == "PASS" else "yellow" if status != "FAIL" else "red"
    console.print(f"[{style}]Data source fallback policy status={status}[/{style}]")
    console.print(f"report_id={payload.get('report_id')}")
    console.print(f"fallback_status={summary.get('fallback_status')}")
    console.print(f"source_group_count={summary.get('source_group_count')}")
    console.print(f"fallback_used_count={summary.get('fallback_used_count')}")
    console.print(f"blocking_source_count={summary.get('blocking_source_count')}")
    console.print(f"fallback_used_sources={summary.get('fallback_used_sources')}")
    console.print(f"blocking_data_types={summary.get('blocking_data_types')}")
    console.print(f"next_action={summary.get('next_action')}")
    console.print(f"report={report_path}")
    console.print(
        "production_effect=none；只读 fallback policy，不刷新数据、不补造 cache、不触发 broker。"
    )


def _print_cache_catalog_summary(
    payload: dict[str, object],
    report_path: Path | None,
) -> None:
    summary = payload.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    status = str(payload.get("status", "UNKNOWN"))
    style = "green" if status == "PASS" else "yellow" if status != "FAIL" else "red"
    console.print(f"[{style}]Cache catalog status={status}[/{style}]")
    console.print(f"catalog_id={payload.get('catalog_id')}")
    console.print(f"cache_integrity_status={payload.get('cache_integrity_status')}")
    console.print(f"entry_count={summary.get('entry_count')}")
    console.print(f"missing_required_count={summary.get('missing_required_count')}")
    console.print(f"checksum_mismatch_count={summary.get('checksum_mismatch_count')}")
    console.print(
        "checksum_changed_without_refresh_count="
        f"{summary.get('checksum_changed_without_refresh_count')}"
    )
    console.print(f"blocking_entry_ids={summary.get('blocking_entry_ids')}")
    console.print(f"next_action={summary.get('next_action')}")
    console.print(f"report={report_path}")
    console.print(
        "production_effect=none；只读 cache catalog，不刷新数据、不修复 cache、不触发 broker。"
    )


@data_app.command("recover-freshness")
def data_recover_freshness_command(
    latest: Annotated[
        bool,
        typer.Option(help="使用 latest freshness/tracking 日期执行 recovery。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="recovery 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    refresh_config_path: Annotated[
        Path,
        typer.Option("--refresh-config", help="market data refresh 配置路径。"),
    ] = DEFAULT_MARKET_DATA_REFRESH_CONFIG_PATH,
    freshness_config_path: Annotated[
        Path,
        typer.Option("--freshness-config", help="market data freshness 配置路径。"),
    ] = DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH,
) -> None:
    """执行 freshness -> refresh -> manifest/freshness/tracking recovery 链路。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    freshness_run = run_market_data_freshness(
        as_of=run_date,
        config_path=freshness_config_path,
        dry_run=False,
    )
    freshness = freshness_run.payload.get("freshness", {})
    freshness_status = (
        freshness.get("status", "UNKNOWN") if isinstance(freshness, dict) else "UNKNOWN"
    )
    console.print(f"freshness_before={freshness_status}；report={freshness_run.json_path}")
    refresh_run = run_market_data_refresh(
        as_of=freshness_run.as_of,
        config_path=refresh_config_path,
        dry_run=False,
    )
    metadata = (
        refresh_run.payload.get("metadata", {}) if isinstance(refresh_run.payload, dict) else {}
    )
    status = str(metadata.get("status") or "UNKNOWN")
    style = "green" if status in {"OK", "NOT_NEEDED"} else "yellow"
    if status in {"FAILED", "BLOCKED"}:
        style = "red"
    console.print(f"[{style}]Freshness recovery：{status}[/{style}]")
    console.print(f"Refresh JSON：{refresh_run.json_path}")
    after = refresh_run.payload.get("after", {})
    if isinstance(after, dict):
        console.print(
            f"freshness_status={after.get('freshness_status', 'UNKNOWN')}；"
            f"tracking_readiness={after.get('tracking_readiness', 'UNKNOWN')}；"
            f"tracking_status={after.get('candidate_tracking_status', 'UNKNOWN')}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
    if status in {"FAILED", "BLOCKED"}:
        raise typer.Exit(code=1)


@data_app.command(
    "repair-backtest-inputs",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def data_repair_backtest_inputs_command(
    ctx: typer.Context,
    latest: Annotated[
        bool,
        typer.Option(help="使用价格缓存中的最新可用日期。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="repair planning 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="shadow backtest 配置路径。"),
    ] = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(help="只输出 repair plan，不下载或改写外部数据。"),
    ] = False,
    price_only: Annotated[
        bool,
        typer.Option("--price-only", help="只修复价格历史，不尝试生成 signal snapshots。"),
    ] = False,
    symbols: Annotated[
        list[str] | None,
        typer.Option(
            "--symbols",
            help="指定修复资产；可重复。也兼容 `--symbols GOOGL BRK.B SGOV`。",
        ),
    ] = None,
    price_provider: Annotated[
        str,
        typer.Option(help="价格 repair provider：fmp 或 yahoo。默认使用 active 主源 fmp。"),
    ] = "fmp",
    fmp_api_key_env: Annotated[
        str,
        typer.Option(help="读取 FMP API key 的环境变量名。"),
    ] = "FMP_API_KEY",
) -> None:
    """修复 shadow backtest 输入价格历史；dry-run 只输出 repair plan。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    extra_symbol_args = [str(item) for item in ctx.args]
    requested_symbols = tuple([*(symbols or []), *extra_symbol_args])
    run = run_backtest_input_diagnostics(as_of=run_date, config_path=config_path)
    repair_plan = run.payload.get("repair_plan", {})
    status = repair_plan.get("status", "UNKNOWN") if isinstance(repair_plan, dict) else "UNKNOWN"
    mode = "DRY_RUN" if dry_run else "EXECUTE"
    style = "green" if status == "NOT_REQUIRED" else "yellow"
    console.print(f"[{style}]Backtest input repair plan：{status} ({mode})[/{style}]")
    console.print(f"Diagnostic JSON：{run.json_path}")
    console.print(f"Diagnostic Markdown：{run.markdown_path}")
    if isinstance(repair_plan, dict):
        steps = repair_plan.get("steps", [])
        if isinstance(steps, list) and steps:
            for step in steps:
                if isinstance(step, dict):
                    console.print(
                        f"- step {step.get('step')}: {step.get('action')} "
                        f"required={step.get('required', False)}"
                    )
        else:
            console.print("- repair plan 为空。")
    if dry_run:
        console.print("production_effect=none；不修改 production 参数或 promotion 规则")
        return

    try:
        provider = build_price_history_repair_provider(
            provider_name=price_provider,
            fmp_api_key=os.getenv(fmp_api_key_env, ""),
        )
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from exc

    repair = repair_backtest_price_history(
        as_of=run_date,
        config_path=config_path,
        symbols=requested_symbols,
        price_provider=provider,
        provider_name=price_provider,
        price_only=price_only,
    )
    result_style = "green" if repair.status in {"REPAIRED", "NOT_REQUIRED"} else "yellow"
    console.print(f"[{result_style}]Price history repair：{repair.status}[/{result_style}]")
    for result in repair.asset_results:
        console.print(
            f"- {result.symbol}: {result.status}; source_symbol={result.source_symbol}; "
            f"rows={result.rows_written}; error={result.error or 'none'}"
        )
    final_summary = repair.final_diagnostics.payload.get("summary", {})
    if isinstance(final_summary, dict):
        console.print(
            f"final_status={final_summary.get('overall_status', 'UNKNOWN')}；"
            f"backtest_mode={final_summary.get('backtest_mode', 'UNKNOWN')}；"
            f"can_run_shadow_backtest={final_summary.get('can_run_shadow_backtest', False)}；"
            f"can_promote_candidate={final_summary.get('can_promote_candidate', False)}"
        )
    console.print(f"Price cache：{repair.price_cache_path}")
    console.print(f"Download manifest：{repair.manifest_path}")
    console.print(f"Final diagnostic JSON：{repair.final_diagnostics.json_path}")
    console.print(f"Final snapshot manifest：{repair.final_diagnostics.manifest_path}")
    console.print("production_effect=none；不修改 production 参数或 promotion 规则")


def _print_foundation_payload(payload: dict[str, object]) -> None:
    status = str(payload.get("status", "UNKNOWN"))
    style = "green" if status == "PASS" else "yellow" if "WARNING" in status else "red"
    console.print(
        f"[{style}]{payload.get('title', payload.get('report_type'))}：{status}[/{style}]"
    )
    summary = payload.get("summary")
    if isinstance(summary, dict):
        for key in sorted(summary):
            console.print(f"{key}={summary[key]}")
    artifact_paths = payload.get("artifact_paths")
    if isinstance(artifact_paths, dict):
        for label, path in artifact_paths.items():
            console.print(f"{label}={path}")
    console.print("production_effect=none；broker_action=none；validation_only=true")
