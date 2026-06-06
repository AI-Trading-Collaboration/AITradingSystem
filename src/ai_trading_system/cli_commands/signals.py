from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.parameters import (
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    DEFAULT_SIGNAL_ABLATION_CONFIG_PATH,
)
from ai_trading_system.trading_engine.signal_ablation import (
    latest_signal_ablation_path,
    load_signal_ablation_payload,
    render_signal_ablation_diagnostics,
    run_signal_ablation,
    validate_signal_ablation_payload,
)
from ai_trading_system.trading_engine.signal_calibration import (
    DEFAULT_SIGNAL_CALIBRATION_PROFILES_PATH,
    run_signal_calibration,
)
from ai_trading_system.trading_engine.signal_snapshots import (
    default_signal_snapshot_json_path,
    default_signal_snapshot_root,
    latest_signal_snapshot_path,
    load_signal_snapshot_payload,
    run_signal_snapshot_build,
    signal_snapshot_summary,
    validate_signal_snapshot_payload,
)

console = Console()
signals_app = typer.Typer(help="Shadow backtest signal snapshot 构建和校验。", no_args_is_help=True)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD 格式。") from exc


def _resolve_signal_snapshot_path(*, latest: bool, as_of: str | None) -> Path:
    root = default_signal_snapshot_root()
    if latest:
        latest_path = latest_signal_snapshot_path(root)
        if latest_path is None:
            raise typer.BadParameter("未找到 signal snapshot artifact。")
        return latest_path
    if not as_of:
        raise typer.BadParameter("需要 --latest、--date/--as-of 或 --input-path。")
    return default_signal_snapshot_json_path(root, _parse_date(as_of))


def _resolve_signal_ablation_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "signal_ablation"
    if latest:
        latest_path = latest_signal_ablation_path(root)
        if latest_path is None:
            raise typer.BadParameter("未找到 signal ablation artifact。")
        return latest_path
    if not as_of:
        raise typer.BadParameter("需要 --latest、--date/--as-of 或 --input-path。")
    return root / _parse_date(as_of).isoformat() / "signal_ablation_summary.json"


@signals_app.command("build-snapshot")
def signals_build_snapshot_command(
    latest: Annotated[
        bool,
        typer.Option(help="使用价格缓存中的最新可用日期。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="信号快照日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="shadow backtest 配置路径。"),
    ] = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(help="写入 outputs/dry_runs/signal_snapshots，不写正式 artifacts。"),
    ] = False,
    price_derived_only: Annotated[
        bool,
        typer.Option(help="只构建 price-derived signals，不补 neutral fallback。"),
    ] = False,
) -> None:
    """构建 shadow backtest signal snapshot。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    run = run_signal_snapshot_build(
        as_of=run_date,
        config_path=config_path,
        dry_run=dry_run,
        price_derived_only=price_derived_only,
    )
    summary = signal_snapshot_summary(run.payload)
    status = str(summary.get("status", "UNKNOWN"))
    style = "green" if status == "OK" else "yellow" if status == "LIMITED" else "red"
    console.print(f"[{style}]Signal snapshot：{status}[/{style}]")
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    console.print(
        f"real_signals={summary.get('real_signal_count', 0)}；"
        f"fallback_signals={summary.get('fallback_signal_count', 0)}；"
        f"missing_signals={summary.get('missing_signal_count', 0)}；"
        f"can_run_full_signal_backtest={summary.get('can_run_full_signal_backtest', False)}"
    )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@signals_app.command("validate-snapshot")
def signals_validate_snapshot_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新 signal snapshot artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="信号快照日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 signal_snapshot.json 路径。"),
    ] = None,
) -> None:
    """校验 signal snapshot JSON schema 和只读安全字段。"""
    source_path = input_path or _resolve_signal_snapshot_path(latest=latest, as_of=as_of)
    payload = load_signal_snapshot_payload(source_path)
    issues = validate_signal_snapshot_payload(payload)
    summary = signal_snapshot_summary(payload)
    status = str(summary.get("status", "UNKNOWN"))
    style = "green" if status == "OK" else "yellow" if status == "LIMITED" else "red"
    console.print(f"[{style}]Signal snapshot validation：{status}[/{style}]")
    console.print(f"source：{source_path}")
    console.print(
        f"real_signals={summary.get('real_signal_count', 0)}；"
        f"fallback_signals={summary.get('fallback_signal_count', 0)}；"
        f"missing_signals={summary.get('missing_signal_count', 0)}；"
        f"proxy_signals={summary.get('proxy_signal_count', 0)}"
    )
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@signals_app.command(
    "ablation",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def signals_ablation_command(
    ctx: typer.Context,
    latest: Annotated[
        bool,
        typer.Option(help="使用 prices_daily.csv 中最新可用日期。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="ablation 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="signal ablation 配置路径。"),
    ] = DEFAULT_SIGNAL_ABLATION_CONFIG_PATH,
    signals: Annotated[
        list[str] | None,
        typer.Option(
            "--signals",
            help="指定 signal，可重复；也兼容 `--signals trend_momentum sector_strength`。",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(help="写入 outputs/dry_runs/signal_ablation，不写正式 artifacts。"),
    ] = False,
    debug: Annotated[
        bool,
        typer.Option(help="输出逐信号 snapshot -> score -> portfolio -> metrics 诊断。"),
    ] = False,
) -> None:
    """运行 remove-one-signal ablation 并生成只读贡献验证报告。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    requested_signals = tuple([*(signals or []), *[str(item) for item in ctx.args]])
    try:
        run = run_signal_ablation(
            as_of=run_date,
            signals=requested_signals or None,
            config_path=config_path,
            dry_run=dry_run,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    metadata = run.payload.get("metadata", {})
    summary = run.payload.get("summary", {})
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    style = "green" if status == "OK" else "yellow" if status == "LIMITED" else "red"
    console.print(f"[{style}]Signal ablation：{status}[/{style}]")
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    if isinstance(summary, dict):
        console.print(
            f"positive_signals={len(summary.get('positive_signals', []))}；"
            f"negative_signals={len(summary.get('negative_signals', []))}；"
            f"fallback_signals={len(summary.get('fallback_signals', []))}；"
            "can_support_candidate_promotion="
            f"{summary.get('can_support_candidate_promotion', False)}"
        )
        reason = summary.get("no_promotion_credit_reason")
        if reason:
            console.print(f"no_promotion_credit_reason={reason}")
    if debug:
        console.print("")
        console.print(render_signal_ablation_diagnostics(run.payload))
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@signals_app.command(
    "calibrate",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def signals_calibrate_command(
    ctx: typer.Context,
    latest: Annotated[
        bool,
        typer.Option(help="使用 prices_daily.csv 中最新可用日期。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="calibration 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="signal calibration profile 配置路径。"),
    ] = DEFAULT_SIGNAL_CALIBRATION_PROFILES_PATH,
    profile: Annotated[
        str | None,
        typer.Option("--profile", help="指定单一 calibration profile。"),
    ] = None,
    profiles: Annotated[
        list[str] | None,
        typer.Option(
            "--profiles",
            help="指定多个 calibration profile；也兼容 `--profiles a b c`。",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(help="写入 outputs/dry_runs/signal_calibration，不写正式 artifacts。"),
    ] = False,
) -> None:
    """运行 trend/sector signal calibration 并生成只读比较报告。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    requested_profiles = tuple(
        dict.fromkeys(
            [
                *([profile] if profile else []),
                *(profiles or []),
                *[str(item) for item in ctx.args],
            ]
        )
    )
    try:
        run = run_signal_calibration(
            as_of=run_date,
            profile_names=requested_profiles or None,
            config_path=config_path,
            dry_run=dry_run,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    metadata = run.payload.get("metadata", {})
    ranking = run.payload.get("ranking", {})
    promotion = run.payload.get("promotion_impact", {})
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    style = "green" if status == "OK" else "yellow" if status == "LIMITED" else "red"
    console.print(f"[{style}]Signal calibration：{status}[/{style}]")
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    console.print(f"Recommended profile：{run.recommended_profile_path}")
    if isinstance(ranking, dict):
        console.print(
            f"profiles_tested={len(run.payload.get('profiles', []))}；"
            f"best_profile={ranking.get('best_profile', 'UNKNOWN')}"
        )
        reason = ranking.get("reason")
        if reason:
            console.print(f"reason={reason}")
    if isinstance(promotion, dict):
        console.print(
            "can_support_candidate_promotion="
            f"{promotion.get('can_support_candidate_promotion', False)}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@signals_app.command("explain-ablation")
def signals_explain_ablation_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 signal ablation artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="ablation 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 signal_ablation_summary.json 路径。"),
    ] = None,
) -> None:
    """解释既有 signal ablation artifact 的诊断链路。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_signal_ablation_path(latest=latest, as_of=as_of)
    payload = load_signal_ablation_payload(source_path)
    issues = validate_signal_ablation_payload(payload)
    if issues:
        raise typer.BadParameter("signal ablation JSON 校验失败：" + "; ".join(issues))
    console.print(render_signal_ablation_diagnostics(payload))
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@signals_app.command("validate-ablation")
def signals_validate_ablation_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 signal ablation artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="ablation 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 signal_ablation_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 signal ablation JSON schema 和只读安全字段。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_signal_ablation_path(latest=latest, as_of=as_of)
    payload = load_signal_ablation_payload(source_path)
    issues = validate_signal_ablation_payload(payload)
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    style = "green" if not issues and status == "OK" else "yellow" if not issues else "red"
    console.print(f"[{style}]Signal ablation validation：{status}[/{style}]")
    console.print(f"source：{source_path}")
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    diagnostics = payload.get("diagnostics", {}) if isinstance(payload, dict) else {}
    diagnostics_present = isinstance(diagnostics, dict) and bool(diagnostics)
    real_signals_used = (
        diagnostics.get("all_real_signals_used_in_score")
        if isinstance(diagnostics, dict)
        else False
    )
    classification_reasons = (
        diagnostics.get("classification_reasons_present")
        if isinstance(diagnostics, dict)
        else False
    )
    console.print(
        f"status={status}；diagnostics_present={str(diagnostics_present).lower()}；"
        f"real_signals_used_in_score={str(real_signals_used is True).lower()}；"
        f"classification_reasons_present={str(classification_reasons is True).lower()}；"
        "production_effect=none；manual_review_required=true；auto_promotion=false"
    )
