from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    DEFAULT_PARAMETER_SWEEP_PROFILE_CONFIG_PATH,
    DEFAULT_SWEEP_OUTPUT_DIR,
    DynamicV3ParameterResearchError,
    build_sweep_leaderboard_payload,
    build_sweep_report_payload,
    latest_sweep_id,
    run_parameter_sweep,
    run_parameter_sweep_profile,
    sweep_profile_list_payload,
    sweep_status_payload,
    validate_sweep_artifact,
    validate_sweep_profiles_payload,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    load_optional_json_payload,
    mapping_obj,
    parse_date,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import dynamic_v3_sweep_app


@dynamic_v3_sweep_app.command("profile-list")
def dynamic_v3_sweep_profile_list_command(
    profile_config_path: Annotated[
        Path, typer.Option("--profile-config", help="sweep profile config。")
    ] = DEFAULT_PARAMETER_SWEEP_PROFILE_CONFIG_PATH,
) -> None:
    """列出 TRADING-104 sweep execution profiles。"""
    payload = sweep_profile_list_payload(profile_config_path=profile_config_path)
    typer.echo(f"status={payload['status']}")
    for row in payload["profiles"]:
        typer.echo(
            f"{row['profile']} evaluator_mode={row['evaluator_mode']} "
            f"max_candidates={row['max_candidates']} ci_safe={str(row['ci_safe']).lower()}"
        )
    typer.echo("production_candidate_generated=false")


@dynamic_v3_sweep_app.command("profile-validate")
def dynamic_v3_sweep_profile_validate_command(
    profile_config_path: Annotated[
        Path, typer.Option("--profile-config", help="sweep profile config。")
    ] = DEFAULT_PARAMETER_SWEEP_PROFILE_CONFIG_PATH,
) -> None:
    """校验 TRADING-104 sweep execution profiles。"""
    payload = validate_sweep_profiles_payload(profile_config_path=profile_config_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_sweep_app.command("run-profile")
def dynamic_v3_sweep_run_profile_command(
    profile: Annotated[str, typer.Option("--profile", help="profile name。")],
    as_of: Annotated[str | None, typer.Option("--as-of", help="sweep as-of date。")] = None,
    end: Annotated[str | None, typer.Option("--end", help="sweep end date。")] = None,
    profile_config_path: Annotated[
        Path, typer.Option("--profile-config", help="sweep profile config。")
    ] = DEFAULT_PARAMETER_SWEEP_PROFILE_CONFIG_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="real evaluator 标准化 ETF daily price cache。")
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="real evaluator FRED rates cache。")
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    output_dir: Annotated[
        Path, typer.Option("--output", "--output-dir", help="sweep artifact root。")
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """按 TRADING-104 profile 运行 sweep。"""
    try:
        result = run_parameter_sweep_profile(
            profile=profile,
            profile_config_path=profile_config_path,
            as_of=parse_date(as_of) if as_of else None,
            end=parse_date(end) if end else None,
            prices_path=prices_path,
            rates_path=rates_path,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    manifest = result["manifest"]
    data_quality = mapping_obj(manifest.get("data_quality"))
    typer.echo(f"sweep_id={result['sweep_id']}")
    typer.echo(f"sweep_dir={result['sweep_dir']}")
    typer.echo(f"profile={profile}")
    typer.echo(f"status={result['status']}")
    typer.echo(f"evaluator_mode={manifest.get('evaluator_mode')}")
    typer.echo(f"not_for_investment_decision={manifest.get('not_for_investment_decision')}")
    typer.echo(f"data_quality_status={data_quality.get('status')}")
    typer.echo(f"completed_count={manifest['completed_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_sweep_app.command("run")
def dynamic_v3_sweep_run_command(
    config_path: Annotated[
        Path, typer.Option("--config", "--config-path", help="parameter sweep config。")
    ] = DEFAULT_PARAMETER_SWEEP_CONFIG_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="sweep as-of date。")] = None,
    end: Annotated[str | None, typer.Option("--end", help="sweep end date。")] = None,
    workers: Annotated[
        int | None, typer.Option("--workers", help="worker count recorded in manifest。")
    ] = None,
    evaluator: Annotated[
        str | None,
        typer.Option(
            "--evaluator",
            "--evaluator-mode",
            help="sweep evaluator：tiny_fixture_proxy 或 real_dynamic_v3_rescue。",
        ),
    ] = None,
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="real evaluator 标准化 ETF daily price cache。")
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option(
            "--rates-path",
            help="real evaluator FRED rates cache for validate-data gate。",
        ),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option(
            "--data-quality-output-path",
            help="real evaluator validate-data markdown path。",
        ),
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output", "--output-dir", help="sweep artifact root。")
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    resume: Annotated[str | None, typer.Option("--resume", help="resume sweep_id。")] = None,
) -> None:
    """运行 TRADING-094 batch parameter sweep；不 promotion。"""
    try:
        result = run_parameter_sweep(
            config_path=config_path,
            as_of=parse_date(as_of) if as_of else None,
            end=parse_date(end) if end else None,
            workers=workers,
            evaluator_mode=evaluator,
            prices_path=prices_path,
            rates_path=rates_path,
            data_quality_output_path=data_quality_output_path,
            output_dir=output_dir,
            resume=resume,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    manifest = result["manifest"]
    typer.echo(f"sweep_id={result['sweep_id']}")
    typer.echo(f"sweep_dir={result['sweep_dir']}")
    typer.echo(f"status={result['status']}")
    typer.echo(f"evaluator_mode={manifest.get('evaluator_mode')}")
    typer.echo(f"evaluator_version={manifest.get('evaluator_version')}")
    data_quality = mapping_obj(manifest.get("data_quality"))
    typer.echo(f"data_quality_status={data_quality.get('status')}")
    typer.echo(f"completed_count={manifest['completed_count']}")
    typer.echo(f"failed_count={manifest['failed_count']}")
    typer.echo(f"observe_only_count={manifest['observe_only_count']}")
    typer.echo(f"review_required_count={manifest['review_required_count']}")
    typer.echo(f"rejected_count={manifest['rejected_count']}")
    typer.echo("production_candidate_generated=false")
    typer.echo("production_effect=none")


@dynamic_v3_sweep_app.command("status")
def dynamic_v3_sweep_status_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="sweep id。")],
    output_dir: Annotated[
        Path, typer.Option("--output", "--output-dir", help="sweep artifact root。")
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """展示 TRADING-094 sweep status。"""
    try:
        payload = sweep_status_payload(sweep_id=sweep_id, output_dir=output_dir)
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    manifest, checkpoint = payload["manifest"], payload["checkpoint"]
    typer.echo(f"sweep_id={sweep_id}")
    typer.echo(f"status={manifest.get('status')}")
    typer.echo(f"candidate_count={manifest.get('candidate_count')}")
    typer.echo(f"completed_count={manifest.get('completed_count')}")
    typer.echo(f"failed_count={manifest.get('failed_count')}")
    typer.echo(f"last_candidate_index={checkpoint.get('last_candidate_index')}")


@dynamic_v3_sweep_app.command("validate")
def dynamic_v3_sweep_validate_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="sweep id。")],
    output_dir: Annotated[
        Path, typer.Option("--output", "--output-dir", help="sweep artifact root。")
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """校验 TRADING-094 sweep artifacts。"""
    payload = validate_sweep_artifact(sweep_id=sweep_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"evaluator_mode={payload.get('evaluator_mode')}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_sweep_app.command("leaderboard")
def dynamic_v3_sweep_leaderboard_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest sweep pointer。")
    ] = False,
    sweep_id: Annotated[str | None, typer.Option("--sweep-id", help="sweep id。")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output", "--output-dir", help="sweep artifact root。")
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """展示 TRADING-095 sweep leaderboard 摘要。"""
    resolved_sweep_id = resolve_dynamic_v3_sweep_id(latest=latest, sweep_id=sweep_id)
    path = output_dir / resolved_sweep_id / "leaderboard.json"
    payload = load_optional_json_payload(path) or build_sweep_leaderboard_payload(
        sweep_dir=output_dir / resolved_sweep_id
    )
    typer.echo(f"sweep_id={resolved_sweep_id}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"evaluator_mode={payload.get('evaluator_mode')}")
    typer.echo(f"candidate_count={payload.get('candidate_count')}")
    top = payload.get("top_eligible_candidates") or []
    if top:
        first = top[0]
        typer.echo(f"top_candidate={first.get('candidate_id')}")
        typer.echo(f"top_gate={first.get('gate')}")
        typer.echo(f"top_score={first.get('score')}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_sweep_app.command("report")
def dynamic_v3_sweep_report_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="sweep id。")],
    output_dir: Annotated[
        Path, typer.Option("--output", "--output-dir", help="sweep artifact root。")
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """展示 TRADING-095 sweep report 摘要。"""
    try:
        payload = build_sweep_report_payload(sweep_dir=output_dir / sweep_id)
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    summary = payload["leaderboard_summary"]
    typer.echo(f"sweep_id={sweep_id}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"top_candidate={summary.get('top_candidate')}")
    typer.echo(f"sweep_report={output_dir / sweep_id / 'sweep_report.md'}")
    typer.echo("production_candidate_generated=false")


def resolve_dynamic_v3_sweep_id(*, latest: bool, sweep_id: str | None) -> str:
    if sweep_id:
        return sweep_id
    if latest:
        resolved = latest_sweep_id()
        if resolved:
            return resolved
    raise typer.BadParameter("--sweep-id or --latest is required")
