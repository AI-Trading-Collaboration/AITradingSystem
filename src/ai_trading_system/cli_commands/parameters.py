from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.cli_commands.parameter_artifacts import (
    _parse_date,
    _resolve_shadow_backtest_summary_path,
    _resolve_weight_stability_path,
    _resolve_weight_stability_readiness_path,
    _resolve_weight_tuning_failure_path,
    _resolve_weight_tuning_path,
)
from ai_trading_system.trading_engine.parameters import (
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    run_shadow_parameter_backtest,
)
from ai_trading_system.trading_engine.parameters.weight_stability import (
    DEFAULT_WEIGHT_STABILITY_CONFIG_PATH,
    load_weight_stability_payload,
    render_weight_stability_explanation,
    run_weight_stability,
    validate_weight_stability_payload,
)
from ai_trading_system.trading_engine.parameters.weight_stability_readiness import (
    DEFAULT_WEIGHT_STABILITY_READINESS_CONFIG_PATH,
    load_weight_stability_readiness_payload,
    render_weight_stability_readiness_explanation,
    run_weight_stability_readiness,
    validate_weight_stability_readiness_payload,
)
from ai_trading_system.trading_engine.parameters.weight_tuning import (
    DEFAULT_WEIGHT_TUNING_CONFIG_PATH,
    load_weight_tuning_payload,
    render_weight_tuning_explanation,
    run_weight_tuning,
    validate_weight_tuning_payload,
)
from ai_trading_system.trading_engine.parameters.weight_tuning_failure import (
    load_weight_tuning_failure_payload,
    render_weight_tuning_failure_explanation,
    run_weight_tuning_failure_attribution,
    validate_weight_tuning_failure_payload,
)
from ai_trading_system.trading_engine.reports.shadow_backtest_report import (
    load_shadow_backtest_payload,
    validate_shadow_backtest_payload,
)

console = Console()
parameters_app = typer.Typer(help="生产参数快照、shadow 回测和晋升复核。", no_args_is_help=True)


@parameters_app.command("shadow-backtest")
def parameters_shadow_backtest_command(
    latest: Annotated[
        bool,
        typer.Option(help="使用 prices_daily.csv 中最新可用日期。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="回测评估日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="shadow backtest 配置路径。"),
    ] = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(help="写入 outputs/dry_runs/shadow_backtest，不写正式 artifacts。"),
    ] = False,
) -> None:
    """运行 observe-only shadow 参数 walk-forward 回测。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    run = run_shadow_parameter_backtest(
        as_of=run_date,
        config_path=config_path,
        dry_run=dry_run,
    )
    metadata = run.payload.get("metadata", {})
    decision = run.payload.get("promotion_decision", {})
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    promotion_status = (
        decision.get("status", "UNKNOWN") if isinstance(decision, dict) else "UNKNOWN"
    )
    backtest_mode = (
        metadata.get("backtest_mode", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    )
    style = "green" if status == "OK" else "yellow"
    console.print(f"[{style}]Shadow parameter backtest：{status}[/{style}]")
    console.print(
        f"as_of：{run.as_of.isoformat()}；backtest_mode={backtest_mode}；"
        f"promotion_status={promotion_status}"
    )
    if run.artifacts is not None:
        console.print(f"JSON：{run.artifacts.summary_json}")
        console.print(f"Markdown：{run.artifacts.summary_markdown}")
        console.print(f"Promotion decision：{run.artifacts.promotion_json}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@parameters_app.command("validate-shadow-backtest")
def validate_shadow_backtest_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 shadow backtest JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="校验日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 shadow_backtest_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 shadow backtest JSON schema 和只读安全字段。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_shadow_backtest_summary_path(latest=latest, as_of=as_of)
    payload = load_shadow_backtest_payload(source_path)
    issues = validate_shadow_backtest_payload(payload)
    if issues:
        console.print("[red]Shadow backtest validation：FAIL[/red]")
        for issue in issues:
            console.print(f"- {issue}")
        raise typer.Exit(code=1)
    console.print("[green]Shadow backtest validation：PASS[/green]")
    console.print(f"JSON：{source_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@parameters_app.command(
    "tune-weights",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def parameters_tune_weights_command(
    ctx: typer.Context,
    latest: Annotated[
        bool,
        typer.Option(help="使用 latest valid backtest input manifest 对应日期。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="调参日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="weight tuning 配置路径。"),
    ] = DEFAULT_WEIGHT_TUNING_CONFIG_PATH,
    portfolio_profile: Annotated[
        str | None,
        typer.Option("--portfolio-profile", help="portfolio candidate profile 名称。"),
    ] = None,
    signals: Annotated[
        list[str] | None,
        typer.Option(
            "--signals",
            help="指定可调 signal，可重复；也兼容 `--signals a b c`。",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(help="写入 outputs/dry_runs/weight_tuning，不写正式 artifacts。"),
    ] = False,
) -> None:
    """运行 TRADING-059 restricted shadow signal weight tuning。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    requested_signals = tuple([*(signals or []), *[str(item) for item in ctx.args]])
    try:
        run = run_weight_tuning(
            as_of=run_date,
            config_path=config_path,
            portfolio_profile=portfolio_profile,
            signals=requested_signals or None,
            dry_run=dry_run,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    metadata = run.payload.get("metadata", {})
    recommended = run.payload.get("recommended_candidate", {})
    search = run.payload.get("search", {})
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    candidate_status = (
        recommended.get("status", "UNKNOWN") if isinstance(recommended, dict) else "UNKNOWN"
    )
    style = "green" if status == "OK" else "yellow" if status == "LIMITED" else "red"
    console.print(f"[{style}]Restricted weight tuning：{status}[/{style}]")
    console.print(
        f"as_of={run.as_of.isoformat()}；candidate_status={candidate_status}；"
        f"candidates_evaluated={search.get('candidates_evaluated', 0)}"
    )
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    console.print(f"Recommended shadow weights：{run.recommended_weights_path}")
    console.print(f"Candidates：{run.candidates_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@parameters_app.command("validate-weight-tuning")
def validate_weight_tuning_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 weight tuning JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="校验日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 weight_tuning_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 weight tuning JSON schema 和只读安全字段。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_weight_tuning_path(latest=latest, as_of=as_of)
    payload = load_weight_tuning_payload(source_path)
    issues = validate_weight_tuning_payload(payload)
    if issues:
        console.print("[red]Weight tuning validation：FAIL[/red]")
        for issue in issues:
            console.print(f"- {issue}")
        raise typer.Exit(code=1)
    metadata = payload.get("metadata", {})
    recommended = payload.get("recommended_candidate", {})
    safety = payload.get("safety", {})
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    candidate_status = (
        recommended.get("status", "UNKNOWN") if isinstance(recommended, dict) else "UNKNOWN"
    )
    fallback_free = (
        safety.get("fallback_signals_free_tuned", "UNKNOWN")
        if isinstance(safety, dict)
        else "UNKNOWN"
    )
    production_modified = (
        safety.get("production_config_modified", "UNKNOWN")
        if isinstance(safety, dict)
        else "UNKNOWN"
    )
    console.print("[green]Weight tuning validation：PASS[/green]")
    console.print(f"JSON：{source_path}")
    console.print(
        f"status={status}；weight_candidate_status={candidate_status}；"
        f"fallback_signals_free_tuned={str(fallback_free).lower()}；"
        f"production_config_modified={str(production_modified).lower()}"
    )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@parameters_app.command(
    "tune-weights-stable",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def parameters_tune_weights_stable_command(
    ctx: typer.Context,
    latest: Annotated[
        bool,
        typer.Option(help="使用 latest valid backtest input manifest 对应日期。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="稳定调参日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="weight stability 配置路径。"),
    ] = DEFAULT_WEIGHT_STABILITY_CONFIG_PATH,
    portfolio_profile: Annotated[
        str | None,
        typer.Option("--portfolio-profile", help="portfolio candidate profile 名称。"),
    ] = None,
    signals: Annotated[
        list[str] | None,
        typer.Option(
            "--signals",
            help="指定可调 signal，可重复；也兼容 `--signals a b c`。",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(help="写入 outputs/dry_runs/weight_stability，不写正式 artifacts。"),
    ] = False,
) -> None:
    """运行 TRADING-061 stable shadow signal weight tuning。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    requested_signals = tuple([*(signals or []), *[str(item) for item in ctx.args]])
    try:
        run = run_weight_stability(
            as_of=run_date,
            config_path=config_path,
            portfolio_profile=portfolio_profile,
            signals=requested_signals or None,
            dry_run=dry_run,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    metadata = run.payload.get("metadata", {})
    recommended = run.payload.get("recommended_candidate", {})
    search = run.payload.get("search_summary", {})
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    candidate_status = (
        recommended.get("status", "UNKNOWN") if isinstance(recommended, dict) else "UNKNOWN"
    )
    style = "green" if candidate_status in {"watch", "shadow_candidate_only"} else "yellow"
    console.print(f"[{style}]Stable weight tuning：{status}[/{style}]")
    console.print(
        f"as_of={run.as_of.isoformat()}；candidate_status={candidate_status}；"
        f"candidates_backtested={search.get('candidates_backtested', 0)}；"
        "rejected_by_stability="
        f"{search.get('candidates_rejected_by_stability', 0)}"
    )
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    console.print(f"Candidates：{run.candidates_path}")
    if run.readiness_path is not None:
        readiness = run.payload.get("input_readiness", {})
        run_reason = metadata.get("reason", "") if isinstance(metadata, dict) else ""
        readiness_reason = readiness.get("reason", "") if isinstance(readiness, dict) else ""
        console.print(f"Input readiness：{run.readiness_path}")
        if run_reason:
            console.print(f"reason={run_reason}")
        if readiness_reason:
            console.print(f"input_readiness_reason={readiness_reason}")
    if run.recommended_weights_path is not None:
        console.print(f"Recommended stable shadow weights：{run.recommended_weights_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@parameters_app.command("diagnose-weight-stability-inputs")
def diagnose_weight_stability_inputs_command(
    latest: Annotated[
        bool,
        typer.Option(help="诊断最新 stable weight tuning 输入 readiness。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="诊断日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="weight stability 配置路径。"),
    ] = DEFAULT_WEIGHT_STABILITY_READINESS_CONFIG_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(help="写入 outputs/dry_runs/weight_stability_readiness。"),
    ] = False,
) -> None:
    """生成 TRADING-061A stable weight tuning input readiness 诊断。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    try:
        run = run_weight_stability_readiness(
            as_of=run_date,
            config_path=config_path,
            dry_run=dry_run,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _print_weight_stability_readiness_run(run)


@parameters_app.command("recover-weight-stability-inputs")
def recover_weight_stability_inputs_command(
    latest: Annotated[
        bool,
        typer.Option(help="为最新 stable weight tuning 输入生成恢复计划。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="恢复计划日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="weight stability 配置路径。"),
    ] = DEFAULT_WEIGHT_STABILITY_READINESS_CONFIG_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(help="计划模式；不修改 cache 或 manifest。"),
    ] = False,
) -> None:
    """生成 stable weight tuning input recovery 计划；第一版不自动改 cache。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    try:
        run = run_weight_stability_readiness(
            as_of=run_date,
            config_path=config_path,
            dry_run=dry_run,
            recovery_mode=True,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _print_weight_stability_readiness_run(run)
    console.print("recovery_auto_executed=false")


@parameters_app.command("validate-weight-stability-readiness")
def validate_weight_stability_readiness_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 weight stability readiness JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="校验日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 weight_stability_readiness_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 TRADING-061A input readiness JSON 和只读安全字段。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_weight_stability_readiness_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_weight_stability_readiness_payload(source_path)
    issues = validate_weight_stability_readiness_payload(payload)
    if issues:
        console.print("[red]Weight stability readiness validation：FAIL[/red]")
        for issue in issues:
            console.print(f"- {issue}")
        raise typer.Exit(code=1)
    console.print("[green]Weight stability readiness validation：PASS[/green]")
    console.print(f"JSON：{source_path}")
    console.print(render_weight_stability_readiness_explanation(payload))


@parameters_app.command("validate-weight-stability")
def validate_weight_stability_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 weight stability JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="校验日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 weight_stability_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 TRADING-061 stable weight tuning JSON 和只读安全字段。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_weight_stability_path(latest=latest, as_of=as_of)
    payload = load_weight_stability_payload(source_path)
    issues = validate_weight_stability_payload(payload)
    if issues:
        console.print("[red]Weight stability validation：FAIL[/red]")
        for issue in issues:
            console.print(f"- {issue}")
        raise typer.Exit(code=1)
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    safety = payload.get("safety", {}) if isinstance(payload, dict) else {}
    search = payload.get("search_summary", {}) if isinstance(payload, dict) else {}
    console.print("[green]Weight stability validation：PASS[/green]")
    console.print(f"JSON：{source_path}")
    console.print(
        f"status={metadata.get('status', 'UNKNOWN')}；"
        f"candidates_backtested={search.get('candidates_backtested', 0)}；"
        "production_config_modified="
        f"{str(safety.get('production_config_modified', 'UNKNOWN')).lower()}"
    )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@parameters_app.command("explain-weight-stability")
def explain_weight_stability_command(
    latest: Annotated[
        bool,
        typer.Option(help="解释最新正式 weight stability JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="解释日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 weight_stability_summary.json 路径。"),
    ] = None,
) -> None:
    """输出 stable weight tuning 摘要。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_weight_stability_path(latest=latest, as_of=as_of)
    payload = load_weight_stability_payload(source_path)
    issues = validate_weight_stability_payload(payload)
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    console.print(render_weight_stability_explanation(payload))


@parameters_app.command("explain-weight-tuning")
def explain_weight_tuning_command(
    latest: Annotated[
        bool,
        typer.Option(help="解释最新正式 weight tuning JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="解释日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 weight_tuning_summary.json 路径。"),
    ] = None,
) -> None:
    """输出 weight tuning 推荐、guardrail 和 promotion 影响摘要。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_weight_tuning_path(latest=latest, as_of=as_of)
    payload = load_weight_tuning_payload(source_path)
    issues = validate_weight_tuning_payload(payload)
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    console.print(render_weight_tuning_explanation(payload))


@parameters_app.command("explain-weight-tuning-failure")
def explain_weight_tuning_failure_command(
    latest: Annotated[
        bool,
        typer.Option(help="解释最新正式 weight tuning failure attribution。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="解释日期，格式为 YYYY-MM-DD。"),
    ] = None,
    summary_path: Annotated[
        Path | None,
        typer.Option("--summary", help="显式 weight_tuning_summary.json 路径。"),
    ] = None,
    debug: Annotated[
        bool,
        typer.Option(help="额外写出候选 debug JSON。"),
    ] = False,
) -> None:
    """生成并输出 TRADING-059A weight tuning failure attribution 摘要。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    if summary_path is not None and (latest or as_of):
        raise typer.BadParameter("--summary 不能和 --latest/--date 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    run = run_weight_tuning_failure_attribution(
        as_of=run_date,
        summary_path=summary_path,
        debug=debug,
    )
    issues = validate_weight_tuning_failure_payload(run.payload)
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    console.print(render_weight_tuning_failure_explanation(run.payload))
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    if run.debug_path is not None:
        console.print(f"Debug：{run.debug_path}")


@parameters_app.command("validate-weight-tuning-failure")
def validate_weight_tuning_failure_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 weight tuning failure attribution JSON。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="校验日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 weight_tuning_failure_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 TRADING-059A failure attribution JSON 和只读安全字段。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_weight_tuning_failure_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_weight_tuning_failure_payload(source_path)
    issues = validate_weight_tuning_failure_payload(payload)
    if issues:
        console.print("[red]Weight tuning failure validation：FAIL[/red]")
        for issue in issues:
            console.print(f"- {issue}")
        raise typer.Exit(code=1)
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    root_cause = payload.get("root_cause", {}) if isinstance(payload, dict) else {}
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    category = root_cause.get("category", "mixed") if isinstance(root_cause, dict) else "mixed"
    production_modified = (
        metadata.get("production_config_modified", "UNKNOWN")
        if isinstance(metadata, dict)
        else "UNKNOWN"
    )
    console.print("[green]Weight tuning failure validation：PASS[/green]")
    console.print(f"JSON：{source_path}")
    console.print(
        f"status={status}；root_cause_category={category}；"
        f"production_config_modified={str(production_modified).lower()}"
    )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


def _print_weight_stability_readiness_run(run: object) -> None:
    payload = getattr(run, "payload", {})
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    eligibility = payload.get("stable_tuning_eligibility", {}) if isinstance(payload, dict) else {}
    checks = payload.get("readiness_checks", {}) if isinstance(payload, dict) else {}
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    can_run = eligibility.get("can_run", False) if isinstance(eligibility, dict) else False
    blocking = (
        ", ".join(str(item) for item in eligibility.get("blocking_checks", []))
        if isinstance(eligibility, dict)
        else ""
    )
    style = "green" if can_run else "yellow"
    console.print(f"[{style}]Weight stability readiness：{status}[/{style}]")
    run_as_of = run.as_of  # type: ignore[attr-defined]
    json_path = run.json_path  # type: ignore[attr-defined]
    markdown_path = run.markdown_path  # type: ignore[attr-defined]
    console.print(
        f"as_of={run_as_of.isoformat()}；"
        f"can_run={str(can_run).lower()}；blocking_checks={blocking or 'none'}"
    )
    if isinstance(eligibility, dict):
        console.print(f"reason={eligibility.get('reason', '')}")
    if isinstance(checks, dict):
        freshness = checks.get("freshness", {})
        signal = checks.get("signal_snapshot", {})
        manifest = checks.get("backtest_manifest", {})
        price = checks.get("price_coverage", {})
        if all(isinstance(item, dict) for item in (freshness, signal, manifest, price)):
            console.print(
                f"freshness={freshness.get('status', 'UNKNOWN')}；"
                f"signal_snapshot={signal.get('status', 'UNKNOWN')}；"
                f"backtest_manifest={manifest.get('status', 'UNKNOWN')}；"
                f"price_coverage={price.get('status', 'UNKNOWN')}"
            )
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
