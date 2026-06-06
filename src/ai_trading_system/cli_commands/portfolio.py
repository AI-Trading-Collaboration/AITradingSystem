from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.cli_commands.portfolio_artifacts import (
    _parse_date,
    _resolve_portfolio_candidate_review_decision_path,
    _resolve_portfolio_candidate_tracking_path,
    _resolve_portfolio_candidates_path,
    _resolve_portfolio_sensitivity_path,
    _resolve_portfolio_tracking_review_path,
    _resolve_portfolio_turnover_attribution_path,
)
from ai_trading_system.config import PROJECT_ROOT, load_industry_chain, load_watchlist
from ai_trading_system.portfolio_exposure import (
    build_portfolio_exposure_report,
    default_portfolio_exposure_report_path,
    write_portfolio_exposure_report,
)
from ai_trading_system.trading_engine.portfolio_candidate_review import (
    DEFAULT_PORTFOLIO_CANDIDATE_REVIEW_CONFIG_PATH,
    decide_portfolio_candidate,
    load_portfolio_candidate_review_payload,
    run_portfolio_candidate_review,
    validate_portfolio_candidate_review_decision_payload,
)
from ai_trading_system.trading_engine.portfolio_candidate_tracking import (
    DEFAULT_PORTFOLIO_CANDIDATE_TRACKING_CONFIG_PATH,
    load_portfolio_candidate_tracking_payload,
    run_portfolio_candidate_tracking,
    validate_portfolio_candidate_tracking_payload,
)
from ai_trading_system.trading_engine.portfolio_candidates import (
    DEFAULT_PORTFOLIO_CANDIDATE_PROFILES_PATH,
    load_portfolio_candidates_payload,
    run_portfolio_candidates,
    validate_portfolio_candidates_payload,
)
from ai_trading_system.trading_engine.portfolio_sensitivity import (
    DEFAULT_PORTFOLIO_SENSITIVITY_PROFILES_PATH,
    load_portfolio_sensitivity_payload,
    run_portfolio_sensitivity,
    validate_portfolio_sensitivity_payload,
)
from ai_trading_system.trading_engine.portfolio_tracking_review import (
    DEFAULT_PORTFOLIO_TRACKING_REVIEW_CONFIG_PATH,
    load_portfolio_tracking_review_payload,
    run_portfolio_tracking_review,
    validate_portfolio_tracking_review_payload,
)
from ai_trading_system.trading_engine.portfolio_turnover_attribution import (
    load_portfolio_turnover_attribution_payload,
    render_portfolio_turnover_attribution_explanation,
    run_portfolio_turnover_attribution,
    validate_portfolio_turnover_attribution_payload,
)

console = Console()
portfolio_app = typer.Typer(help="真实组合持仓和暴露解释。", no_args_is_help=True)
DEFAULT_PORTFOLIO_POSITIONS_PATH = (
    PROJECT_ROOT / "data" / "external" / "portfolio_positions" / "current_positions.csv"
)


@portfolio_app.command("exposure")
def portfolio_exposure_command(
    input_path: Annotated[
        Path,
        typer.Option(help="真实持仓 CSV 路径。"),
    ] = DEFAULT_PORTFOLIO_POSITIONS_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 组合暴露报告输出路径。"),
    ] = None,
) -> None:
    """基于真实持仓文件生成只读组合暴露分解。"""
    evaluation_date = _parse_date(as_of) if as_of else date.today()
    report = build_portfolio_exposure_report(
        input_path=input_path,
        as_of=evaluation_date,
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
    )
    report_path = output_path or default_portfolio_exposure_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        evaluation_date,
    )
    write_portfolio_exposure_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]组合暴露状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"总市值：{report.total_market_value:.2f}；"
        f"AI 名义暴露：{report.ai_market_value:.2f}；"
        f"AI 占比：{report.ai_exposure_pct_total:.1%}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@portfolio_app.command(
    "sensitivity",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def portfolio_sensitivity_command(
    ctx: typer.Context,
    latest: Annotated[
        bool,
        typer.Option(help="使用 prices_daily.csv 中最新可用日期。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="sensitivity 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="portfolio sensitivity profile 配置路径。"),
    ] = DEFAULT_PORTFOLIO_SENSITIVITY_PROFILES_PATH,
    profile: Annotated[
        str | None,
        typer.Option("--profile", help="指定单一 sensitivity profile。"),
    ] = None,
    profiles: Annotated[
        list[str] | None,
        typer.Option(
            "--profiles",
            help="指定多个 sensitivity profile；也兼容 `--profiles a b c`。",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(help="写入 outputs/dry_runs/portfolio_sensitivity，不写正式 artifacts。"),
    ] = False,
) -> None:
    """运行 portfolio sensitivity diagnostics 并生成只读比较报告。"""
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
        run = run_portfolio_sensitivity(
            as_of=run_date,
            profile_names=requested_profiles or None,
            config_path=config_path,
            dry_run=dry_run,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    metadata = run.payload.get("metadata", {})
    ranking = run.payload.get("ranking", {})
    diagnosis = run.payload.get("diagnosis", {})
    promotion = run.payload.get("promotion_impact", {})
    data_gate = run.payload.get("data_gate", {})
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    style = "green" if status == "OK" else "yellow" if status == "LIMITED" else "red"
    console.print(f"[{style}]Portfolio sensitivity：{status}[/{style}]")
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
    if isinstance(diagnosis, dict):
        console.print(
            f"primary_bottleneck={diagnosis.get('primary_bottleneck', 'UNKNOWN')}；"
            "portfolio_is_too_insensitive="
            f"{diagnosis.get('portfolio_is_too_insensitive', False)}"
        )
    if isinstance(promotion, dict):
        console.print(
            "can_support_candidate_promotion="
            f"{promotion.get('can_support_candidate_promotion', False)}"
        )
    if isinstance(data_gate, dict):
        console.print(
            "data_gate="
            f"{data_gate.get('status', 'UNKNOWN')}；"
            f"error_code={data_gate.get('error_code', 'OK')}；"
            f"latest_resolution={data_gate.get('latest_resolution_status', 'UNKNOWN')}"
        )
        reason = data_gate.get("reason")
        if reason:
            console.print(f"data_gate_reason={reason}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@portfolio_app.command("validate-sensitivity")
def portfolio_validate_sensitivity_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 portfolio sensitivity artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="sensitivity 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_sensitivity_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 portfolio sensitivity JSON schema 和只读安全字段。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_portfolio_sensitivity_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_sensitivity_payload(source_path)
    issues = validate_portfolio_sensitivity_payload(payload)
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    style = "green" if not issues and status == "OK" else "yellow" if not issues else "red"
    console.print(f"[{style}]Portfolio sensitivity validation：{status}[/{style}]")
    console.print(f"source：{source_path}")
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    promotion = payload.get("promotion_impact", {}) if isinstance(payload, dict) else {}
    console.print(
        "can_support_candidate_promotion="
        f"{promotion.get('can_support_candidate_promotion', False)}"
    )
    data_gate = payload.get("data_gate", {}) if isinstance(payload, dict) else {}
    if isinstance(data_gate, dict):
        console.print(
            "data_gate="
            f"{data_gate.get('status', 'UNKNOWN')}；"
            f"error_code={data_gate.get('error_code', 'OK')}；"
            f"latest_resolution={data_gate.get('latest_resolution_status', 'UNKNOWN')}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@portfolio_app.command(
    "candidates",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def portfolio_candidates_command(
    ctx: typer.Context,
    latest: Annotated[
        bool,
        typer.Option(help="使用 latest valid backtest manifest 对应日期。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="candidate evaluation 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="portfolio candidate profile 配置路径。"),
    ] = DEFAULT_PORTFOLIO_CANDIDATE_PROFILES_PATH,
    profile: Annotated[
        str | None,
        typer.Option("--profile", help="指定单一 candidate profile。"),
    ] = None,
    profiles: Annotated[
        list[str] | None,
        typer.Option(
            "--profiles",
            help="指定多个 candidate profile；也兼容 `--profiles a b c`。",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(help="写入 outputs/dry_runs/portfolio_candidates，不写正式 artifacts。"),
    ] = False,
) -> None:
    """运行 portfolio construction candidate profile evaluation。"""
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
        run = run_portfolio_candidates(
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
    data_gate = run.payload.get("data_gate", {})
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    style = "green" if status == "OK" else "yellow" if status == "LIMITED" else "red"
    console.print(f"[{style}]Portfolio candidates：{status}[/{style}]")
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    console.print(f"Recommended candidate：{run.recommended_candidate_path}")
    if isinstance(ranking, dict):
        console.print(
            f"profiles_tested={len(run.payload.get('profiles', []))}；"
            f"candidates_tested={len(run.payload.get('candidates', []))}；"
            f"best_profile={ranking.get('best_profile', 'UNKNOWN')}"
        )
        reason = ranking.get("reason")
        if reason:
            console.print(f"reason={reason}")
    if isinstance(data_gate, dict):
        console.print(
            "data_gate="
            f"{data_gate.get('status', 'UNKNOWN')}；"
            f"error_code={data_gate.get('error_code', 'OK')}；"
            f"latest_resolution={data_gate.get('latest_resolution_status', 'UNKNOWN')}"
        )
        reason = data_gate.get("reason")
        if reason:
            console.print(f"data_gate_reason={reason}")
    if isinstance(promotion, dict):
        console.print(
            "can_support_candidate_promotion="
            f"{promotion.get('can_support_candidate_promotion', False)}"
        )
    if "portfolio sensitivity summary is missing" in str(run.payload.get("warnings", [])):
        console.print("请先运行：aits portfolio sensitivity --latest")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@portfolio_app.command("validate-candidates")
def portfolio_validate_candidates_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 portfolio candidates artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="candidate evaluation 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_candidates_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 portfolio candidates JSON schema 和只读安全字段。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_portfolio_candidates_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_candidates_payload(source_path)
    issues = validate_portfolio_candidates_payload(payload)
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    style = "green" if not issues and status == "OK" else "yellow" if not issues else "red"
    console.print(f"[{style}]Portfolio candidates validation：{status}[/{style}]")
    console.print(f"source：{source_path}")
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    promotion = payload.get("promotion_impact", {}) if isinstance(payload, dict) else {}
    console.print(
        "can_support_candidate_promotion="
        f"{promotion.get('can_support_candidate_promotion', False)}"
    )
    recommended = payload.get("recommended_candidate", {}) if isinstance(payload, dict) else {}
    if isinstance(recommended, dict):
        console.print(f"recommended_profile={recommended.get('profile_name', 'UNKNOWN')}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@portfolio_app.command("explain-turnover")
def portfolio_explain_turnover_command(
    latest: Annotated[
        bool,
        typer.Option(help="解释最新正式 weight tuning turnover failure。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="turnover attribution 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    weight_tuning_path: Annotated[
        Path | None,
        typer.Option("--weight-tuning", help="显式 weight_tuning_summary.json 路径。"),
    ] = None,
    near_miss_only: Annotated[
        bool,
        typer.Option(help="只分析 near-miss turnover candidates。"),
    ] = False,
    debug: Annotated[
        bool,
        typer.Option(help="额外写出候选 debug JSON。"),
    ] = False,
) -> None:
    """生成 TRADING-060 portfolio turnover / cost drag attribution。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    if weight_tuning_path is not None and (latest or as_of):
        raise typer.BadParameter("--weight-tuning 不能和 --latest/--date 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    run = run_portfolio_turnover_attribution(
        as_of=run_date,
        weight_tuning_path=weight_tuning_path,
        near_miss_only=near_miss_only,
        debug=debug,
    )
    issues = validate_portfolio_turnover_attribution_payload(run.payload)
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    console.print(render_portfolio_turnover_attribution_explanation(run.payload))
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    if run.debug_path is not None:
        console.print(f"Debug：{run.debug_path}")


@portfolio_app.command("validate-turnover-attribution")
def portfolio_validate_turnover_attribution_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 portfolio turnover attribution artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="turnover attribution 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_turnover_attribution_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 TRADING-060 turnover attribution JSON 和 safety 字段。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_portfolio_turnover_attribution_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_turnover_attribution_payload(source_path)
    issues = validate_portfolio_turnover_attribution_payload(payload)
    if issues:
        console.print("[red]Portfolio turnover attribution validation：FAIL[/red]")
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
    console.print("[green]Portfolio turnover attribution validation：PASS[/green]")
    console.print(f"JSON：{source_path}")
    console.print(
        f"status={status}；root_cause_category={category}；"
        f"production_config_modified={str(production_modified).lower()}"
    )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@portfolio_app.command("review-candidate")
def portfolio_review_candidate_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 recommended portfolio candidate。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="review package 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    candidate_path: Annotated[
        Path | None,
        typer.Option("--candidate", help="显式 recommended_portfolio_candidate.yaml 路径。"),
    ] = None,
    reviewer: Annotated[
        str | None,
        typer.Option(help="人工 review package 记录的 reviewer 名称。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="portfolio candidate review 配置路径。"),
    ] = DEFAULT_PORTFOLIO_CANDIDATE_REVIEW_CONFIG_PATH,
) -> None:
    """生成 portfolio candidate manual review package 和 pending decision。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    try:
        run = run_portfolio_candidate_review(
            as_of=run_date,
            candidate_path=candidate_path,
            reviewer=reviewer,
            config_path=config_path,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    metadata = run.package_payload.get("metadata", {})
    candidate = run.package_payload.get("candidate", {})
    evidence = run.package_payload.get("evidence_summary", {})
    decision = run.decision_payload.get("decision", {})
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    decision_status = decision.get("status", "UNKNOWN") if isinstance(decision, dict) else "UNKNOWN"
    style = "yellow" if status == "PENDING_REVIEW" else "green"
    console.print(f"[{style}]Portfolio candidate review package：{status}[/{style}]")
    console.print(f"Package JSON：{run.package_json_path}")
    console.print(f"Package Markdown：{run.package_markdown_path}")
    console.print(f"Decision JSON：{run.decision_json_path}")
    console.print(f"Decision Markdown：{run.decision_markdown_path}")
    if isinstance(candidate, dict):
        console.print(f"candidate_profile={candidate.get('profile_name', 'UNKNOWN')}")
    if isinstance(evidence, dict):
        console.print(
            "data_gate="
            f"{evidence.get('data_gate', 'UNKNOWN')}；"
            f"signal_quality={evidence.get('signal_snapshot_status', 'UNKNOWN')}；"
            f"best_profile={evidence.get('best_profile', 'UNKNOWN')}"
        )
    console.print(f"review_status={decision_status}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@portfolio_app.command("decide-candidate")
def portfolio_decide_candidate_command(
    decision: Annotated[
        str,
        typer.Option(
            help=(
                "人工决策：watch / rejected / needs_more_data / " "approved_for_shadow_candidate。"
            ),
        ),
    ],
    latest: Annotated[
        bool,
        typer.Option(help="读取最新 review package。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="decision 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    candidate_path: Annotated[
        Path | None,
        typer.Option("--candidate", help="显式 recommended_portfolio_candidate.yaml 路径。"),
    ] = None,
    reviewer: Annotated[
        str | None,
        typer.Option(help="人工 reviewer 名称。"),
    ] = None,
    reason: Annotated[
        str | None,
        typer.Option(help="人工 decision reason。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="portfolio candidate review 配置路径。"),
    ] = DEFAULT_PORTFOLIO_CANDIDATE_REVIEW_CONFIG_PATH,
) -> None:
    """提交 portfolio candidate manual review decision。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    try:
        run = decide_portfolio_candidate(
            decision=decision,
            as_of=run_date,
            candidate_path=candidate_path,
            reviewer=reviewer,
            reason=reason,
            config_path=config_path,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    decision_payload = run.decision_payload.get("decision", {})
    candidate = run.decision_payload.get("candidate", {})
    safety = run.decision_payload.get("safety", {})
    status = (
        decision_payload.get("status", "UNKNOWN")
        if isinstance(decision_payload, dict)
        else "UNKNOWN"
    )
    style = "green" if status in {"watch", "approved_for_shadow_candidate"} else "yellow"
    console.print(f"[{style}]Portfolio candidate decision：{status}[/{style}]")
    console.print(f"Decision JSON：{run.decision_json_path}")
    console.print(f"Decision Markdown：{run.decision_markdown_path}")
    if isinstance(candidate, dict):
        console.print(f"candidate_profile={candidate.get('profile_name', 'UNKNOWN')}")
    if isinstance(decision_payload, dict):
        console.print(f"allowed_next_step={decision_payload.get('allowed_next_step', 'UNKNOWN')}")
        console.print(f"reason={decision_payload.get('reason', '')}")
    if isinstance(safety, dict):
        console.print(
            "production_config_modified=" f"{safety.get('production_config_modified', 'UNKNOWN')}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@portfolio_app.command("validate-review")
def portfolio_validate_review_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 portfolio candidate review decision。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="review decision 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_candidate_review_decision.json 路径。"),
    ] = None,
) -> None:
    """校验 portfolio candidate review decision schema 和 safety 字段。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_portfolio_candidate_review_decision_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_candidate_review_payload(source_path)
    issues = validate_portfolio_candidate_review_decision_payload(payload)
    decision = payload.get("decision", {}) if isinstance(payload, dict) else {}
    safety = payload.get("safety", {}) if isinstance(payload, dict) else {}
    status = decision.get("status", "UNKNOWN") if isinstance(decision, dict) else "UNKNOWN"
    style = (
        "green"
        if not issues and status in {"watch", "approved_for_shadow_candidate"}
        else ("yellow" if not issues else "red")
    )
    console.print(f"[{style}]Portfolio candidate review validation：{status}[/{style}]")
    console.print(f"source：{source_path}")
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    if isinstance(safety, dict):
        console.print(
            "production_config_modified=" f"{safety.get('production_config_modified', 'UNKNOWN')}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@portfolio_app.command("track-candidate")
def portfolio_track_candidate_command(
    latest: Annotated[
        bool,
        typer.Option(help="使用最新价格日期并 roll-forward 最近 review decision。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="tracking 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    review_path: Annotated[
        Path | None,
        typer.Option("--review", help="显式 portfolio_candidate_review_decision.json 路径。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="portfolio candidate tracking 配置路径。"),
    ] = DEFAULT_PORTFOLIO_CANDIDATE_TRACKING_CONFIG_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(help="写入 outputs/dry_runs/portfolio_candidate_tracking，不更新正式 state。"),
    ] = False,
) -> None:
    """跟踪已人工 review 的 portfolio candidate shadow profile。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    try:
        run = run_portfolio_candidate_tracking(
            as_of=run_date,
            review_path=review_path,
            config_path=config_path,
            dry_run=dry_run,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    metadata = run.payload.get("metadata", {}) if isinstance(run.payload, dict) else {}
    candidate = run.payload.get("candidate", {}) if isinstance(run.payload, dict) else {}
    date_resolution = (
        run.payload.get("date_resolution", {}) if isinstance(run.payload, dict) else {}
    )
    data_gate = run.payload.get("data_gate", {}) if isinstance(run.payload, dict) else {}
    freshness = (
        run.payload.get("market_data_freshness", {}) if isinstance(run.payload, dict) else {}
    )
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    tracking_status = (
        candidate.get("tracking_status", "UNKNOWN") if isinstance(candidate, dict) else "UNKNOWN"
    )
    style = "green" if status == "OK" else "yellow" if status == "DEGRADED" else "red"
    console.print(f"[{style}]Portfolio candidate tracking：{status}[/{style}]")
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    console.print(f"Daily state：{run.daily_state_path}")
    if run.active_state_path is not None:
        console.print(f"Active state：{run.active_state_path}")
    if isinstance(candidate, dict):
        console.print(f"candidate_profile={candidate.get('profile_name', 'UNKNOWN')}")
        console.print(f"tracking_status={tracking_status}")
        console.print(f"review_status={candidate.get('review_status', 'UNKNOWN')}")
    if isinstance(date_resolution, dict):
        console.print(
            "date_resolution="
            f"tracking_date={date_resolution.get('tracking_date', 'UNKNOWN')}；"
            f"effective_data_date={date_resolution.get('effective_data_date', 'UNKNOWN')}；"
            f"roll_forward_status={date_resolution.get('roll_forward_status', 'UNKNOWN')}"
        )
        reason = date_resolution.get("reason")
        if reason:
            console.print(f"roll_forward_reason={reason}")
    if isinstance(data_gate, dict):
        console.print(
            "data_gate="
            f"{data_gate.get('status', 'UNKNOWN')}；"
            f"manifest_date={data_gate.get('manifest_date', 'UNKNOWN')}；"
            f"price_cache_registry={data_gate.get('price_cache_registry_status', 'UNKNOWN')}"
        )
    if isinstance(freshness, dict):
        console.print(
            "market_data_freshness_status="
            f"{freshness.get('status', 'MISSING')}；"
            f"tracking_readiness={freshness.get('tracking_readiness', 'unknown')}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@portfolio_app.command("validate-tracking")
def portfolio_validate_tracking_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 portfolio candidate tracking artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="tracking 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_candidate_tracking_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 portfolio candidate tracking summary schema 和 safety 字段。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_portfolio_candidate_tracking_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_candidate_tracking_payload(source_path)
    issues = validate_portfolio_candidate_tracking_payload(payload)
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    candidate = payload.get("candidate", {}) if isinstance(payload, dict) else {}
    freshness = payload.get("market_data_freshness", {}) if isinstance(payload, dict) else {}
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    tracking_status = (
        candidate.get("tracking_status", "UNKNOWN") if isinstance(candidate, dict) else "UNKNOWN"
    )
    style = "green" if not issues and status == "OK" else "yellow" if not issues else "red"
    console.print(f"[{style}]Portfolio candidate tracking validation：{status}[/{style}]")
    console.print(f"source：{source_path}")
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    if isinstance(candidate, dict):
        console.print(f"candidate_profile={candidate.get('profile_name', 'UNKNOWN')}")
        console.print(f"tracking_status={tracking_status}")
    if isinstance(freshness, dict):
        console.print("market_data_freshness_status=" f"{freshness.get('status', 'MISSING')}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@portfolio_app.command("tracking-status")
def portfolio_tracking_status_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 portfolio candidate tracking artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="tracking 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_candidate_tracking_summary.json 路径。"),
    ] = None,
) -> None:
    """查看 portfolio candidate shadow tracking 状态。"""
    source_path = input_path or _resolve_portfolio_candidate_tracking_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_candidate_tracking_payload(source_path)
    candidate = payload.get("candidate", {}) if isinstance(payload, dict) else {}
    date_resolution = payload.get("date_resolution", {}) if isinstance(payload, dict) else {}
    data_gate = payload.get("data_gate", {}) if isinstance(payload, dict) else {}
    freshness = payload.get("market_data_freshness", {}) if isinstance(payload, dict) else {}
    if isinstance(candidate, dict):
        console.print(f"candidate_profile={candidate.get('profile_name', 'UNKNOWN')}")
        console.print(f"review_status={candidate.get('review_status', 'UNKNOWN')}")
        console.print(f"tracking_status={candidate.get('tracking_status', 'UNKNOWN')}")
    if isinstance(date_resolution, dict):
        console.print(f"tracking_date={date_resolution.get('tracking_date', 'UNKNOWN')}")
        console.print(
            f"effective_data_date={date_resolution.get('effective_data_date', 'UNKNOWN')}"
        )
        console.print(
            f"roll_forward_status={date_resolution.get('roll_forward_status', 'UNKNOWN')}"
        )
    if isinstance(data_gate, dict):
        console.print(f"data_gate={data_gate.get('status', 'UNKNOWN')}")
    if isinstance(freshness, dict):
        console.print("market_data_freshness_status=" f"{freshness.get('status', 'MISSING')}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@portfolio_app.command("review-tracking")
def portfolio_review_tracking_command(
    latest: Annotated[
        bool,
        typer.Option(help="使用最新 active shadow candidate tracking state。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="tracking review 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    candidate_profile: Annotated[
        str | None,
        typer.Option("--candidate", help="指定 candidate profile 名称。"),
    ] = None,
    window: Annotated[
        str | None,
        typer.Option(help="review 窗口：latest_day / 5d / 20d / since-start。"),
    ] = None,
    show_window_progress: Annotated[
        bool,
        typer.Option(
            "--show-window-progress",
            help="显示 tracking window 阶段、剩余天数和结论许可。",
        ),
    ] = False,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="portfolio tracking review 配置路径。"),
    ] = DEFAULT_PORTFOLIO_TRACKING_REVIEW_CONFIG_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(help="写入 outputs/dry_runs/portfolio_tracking_reviews。"),
    ] = False,
) -> None:
    """复核 active shadow candidate 的 rolling tracking performance。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    try:
        run = run_portfolio_tracking_review(
            as_of=run_date,
            candidate_profile=candidate_profile,
            window=window,
            config_path=config_path,
            dry_run=dry_run,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    metadata = run.payload.get("metadata", {}) if isinstance(run.payload, dict) else {}
    candidate = run.payload.get("candidate", {}) if isinstance(run.payload, dict) else {}
    readiness = run.payload.get("data_readiness", {}) if isinstance(run.payload, dict) else {}
    tracking_window = (
        run.payload.get("tracking_window", {}) if isinstance(run.payload, dict) else {}
    )
    recommendation = run.payload.get("recommendation", {}) if isinstance(run.payload, dict) else {}
    performance = run.payload.get("performance_review", {}) if isinstance(run.payload, dict) else {}
    relative = performance.get("relative_performance", {}) if isinstance(performance, dict) else {}
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    rec_status = (
        recommendation.get("status", "UNKNOWN") if isinstance(recommendation, dict) else "UNKNOWN"
    )
    style = "green" if status == "OK" else "yellow" if status == "LIMITED" else "red"
    console.print(f"[{style}]Portfolio tracking review：{status}[/{style}]")
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    if isinstance(candidate, dict):
        console.print(f"candidate_profile={candidate.get('profile_name', 'UNKNOWN')}")
        console.print(f"tracking_status={candidate.get('tracking_status', 'UNKNOWN')}")
        console.print(f"tracking_days={candidate.get('tracking_days', 0)}")
    if isinstance(readiness, dict):
        console.print(
            "data_readiness="
            f"data_gate={readiness.get('data_gate', 'UNKNOWN')}；"
            f"freshness_status={readiness.get('freshness_status', 'UNKNOWN')}；"
            f"effective_data_date={readiness.get('effective_data_date', '')}"
        )
    if isinstance(relative, dict):
        console.print(
            "relative_performance="
            f"excess_return={relative.get('excess_return', 0.0)}；"
            f"drawdown_delta={relative.get('drawdown_delta', 0.0)}；"
            f"turnover_delta={relative.get('turnover_delta', 0.0)}"
        )
    if isinstance(recommendation, dict):
        console.print(f"recommendation={rec_status}")
        console.print(f"reason={recommendation.get('reason', '')}")
    if show_window_progress and isinstance(tracking_window, dict):
        _print_portfolio_tracking_window_progress(
            candidate=candidate if isinstance(candidate, dict) else {},
            tracking_window=tracking_window,
            recommendation=recommendation if isinstance(recommendation, dict) else {},
            metadata=metadata if isinstance(metadata, dict) else {},
            safety=run.payload.get("safety", {}) if isinstance(run.payload, dict) else {},
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@portfolio_app.command("tracking-window-status")
def portfolio_tracking_window_status_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 portfolio tracking review artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="tracking review 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_tracking_review_summary.json 路径。"),
    ] = None,
) -> None:
    """显示 active shadow candidate 的 tracking window 进度。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    if input_path is None and not latest and as_of is None:
        latest = True
    source_path = input_path or _resolve_portfolio_tracking_review_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_tracking_review_payload(source_path)
    issues = validate_portfolio_tracking_review_payload(payload)
    if issues:
        raise typer.BadParameter("portfolio tracking review JSON 校验失败：" + "; ".join(issues))
    candidate = payload.get("candidate", {}) if isinstance(payload, dict) else {}
    tracking_window = payload.get("tracking_window", {}) if isinstance(payload, dict) else {}
    recommendation = payload.get("recommendation", {}) if isinstance(payload, dict) else {}
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    safety = payload.get("safety", {}) if isinstance(payload, dict) else {}
    _print_portfolio_tracking_window_progress(
        candidate=candidate if isinstance(candidate, dict) else {},
        tracking_window=tracking_window if isinstance(tracking_window, dict) else {},
        recommendation=recommendation if isinstance(recommendation, dict) else {},
        metadata=metadata if isinstance(metadata, dict) else {},
        safety=safety if isinstance(safety, dict) else {},
    )


@portfolio_app.command("validate-tracking-review")
def portfolio_validate_tracking_review_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 portfolio tracking review artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="tracking review 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_tracking_review_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 portfolio tracking review schema 和安全字段。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_portfolio_tracking_review_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_tracking_review_payload(source_path)
    issues = validate_portfolio_tracking_review_payload(payload)
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    recommendation = payload.get("recommendation", {}) if isinstance(payload, dict) else {}
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    rec_status = (
        recommendation.get("status", "UNKNOWN") if isinstance(recommendation, dict) else "UNKNOWN"
    )
    style = "green" if not issues and status == "OK" else "yellow" if not issues else "red"
    console.print(f"[{style}]Portfolio tracking review validation：{status}[/{style}]")
    console.print(f"source：{source_path}")
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    console.print(f"recommendation={rec_status}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


def _print_portfolio_tracking_window_progress(
    *,
    candidate: dict[str, object],
    tracking_window: dict[str, object],
    recommendation: dict[str, object],
    metadata: dict[str, object],
    safety: dict[str, object],
) -> None:
    console.print(f"candidate_profile={candidate.get('profile_name', 'UNKNOWN')}")
    console.print(f"tracking_days={tracking_window.get('tracking_days', 0)}")
    console.print(f"stage={tracking_window.get('stage', 'UNKNOWN')}")
    console.print(f"recommendation={recommendation.get('status', 'UNKNOWN')}")
    console.print("days_until_short_review=" f"{tracking_window.get('days_until_short_review', 0)}")
    console.print(
        "days_until_extended_review=" f"{tracking_window.get('days_until_extended_review', 0)}"
    )
    console.print(
        "can_form_short_window_conclusion="
        f"{str(tracking_window.get('can_form_short_window_conclusion') is True).lower()}"
    )
    console.print(
        "can_form_extended_review_conclusion="
        f"{str(tracking_window.get('can_form_extended_review_conclusion') is True).lower()}"
    )
    console.print(
        f"done_condition_met={str(tracking_window.get('done_condition_met') is True).lower()}"
    )
    console.print(f"production_effect={metadata.get('production_effect', 'none')}")
    console.print(
        "manual_review_required=" f"{str(metadata.get('manual_review_required') is True).lower()}"
    )
    console.print(f"auto_promotion={str(safety.get('auto_promotion') is True).lower()}")
