from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_refresh as system_target,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_smoothed_data_readiness_app,
    dynamic_v3_smoothed_post_refresh_validate_app,
    dynamic_v3_smoothed_retry_resume_app,
    dynamic_v3_smoothed_sample_growth_app,
    dynamic_v3_smoothed_source_refresh_app,
)


def _records_obj(value: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in value] if isinstance(value, list) else []


@dynamic_v3_smoothed_source_refresh_app.command("plan")
def dynamic_v3_smoothed_source_refresh_plan_command(
    refresh_plan_id: Annotated[
        str,
        typer.Option("--refresh-plan-id", help="smoothed refresh plan id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed source refresh artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
    refresh_plan_dir: Annotated[
        Path,
        typer.Option("--refresh-plan-dir", help="smoothed refresh plan artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_REFRESH_PLAN_DIR,
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="source refresh config。"),
    ] = system_target.DEFAULT_SMOOTHED_SOURCE_REFRESH_CONFIG_PATH,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache", help="cached ETF price CSV。"),
    ] = system_target.DEFAULT_PRICE_CACHE_PATH,
    marketstack_cache_path: Annotated[
        Path | None,
        typer.Option("--marketstack-cache", help="optional Marketstack price CSV。"),
    ] = None,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="cached rates CSV。"),
    ] = system_target.DEFAULT_RATES_CACHE_PATH,
) -> None:
    """生成 TRADING-281 smoothed source refresh dry-run artifact。"""
    result = system_target.run_smoothed_source_refresh(
        refresh_plan_id=refresh_plan_id,
        execute_refresh=False,
        refresh_plan_dir=refresh_plan_dir,
        output_dir=output_dir,
        config_path=config_path,
        price_cache_path=price_cache_path,
        marketstack_cache_path=marketstack_cache_path,
        rates_path=rates_path,
    )
    results = _mapping_obj(result["source_refresh_results"])
    typer.echo(f"refresh_execution_id={result['refresh_execution_id']}")
    typer.echo(f"refresh_execution_dir={result['refresh_execution_dir']}")
    typer.echo(f"refresh_status={results.get('refresh_status')}")
    typer.echo("execute_refresh=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_source_refresh_app.command("execute")
def dynamic_v3_smoothed_source_refresh_execute_command(
    refresh_plan_id: Annotated[
        str,
        typer.Option("--refresh-plan-id", help="smoothed refresh plan id。"),
    ],
    execute_refresh: Annotated[
        bool,
        typer.Option("--execute-refresh", help="required explicit write permission。"),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed source refresh artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
    refresh_plan_dir: Annotated[
        Path,
        typer.Option("--refresh-plan-dir", help="smoothed refresh plan artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_REFRESH_PLAN_DIR,
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="source refresh config。"),
    ] = system_target.DEFAULT_SMOOTHED_SOURCE_REFRESH_CONFIG_PATH,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache", help="cached ETF price CSV。"),
    ] = system_target.DEFAULT_PRICE_CACHE_PATH,
    marketstack_cache_path: Annotated[
        Path | None,
        typer.Option("--marketstack-cache", help="optional Marketstack price CSV。"),
    ] = None,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="cached rates CSV。"),
    ] = system_target.DEFAULT_RATES_CACHE_PATH,
) -> None:
    """执行 TRADING-281 smoothed source refresh，必须显式传入 --execute-refresh。"""
    if not execute_refresh:
        raise typer.BadParameter("execute requires explicit --execute-refresh")
    result = system_target.run_smoothed_source_refresh(
        refresh_plan_id=refresh_plan_id,
        execute_refresh=True,
        refresh_plan_dir=refresh_plan_dir,
        output_dir=output_dir,
        config_path=config_path,
        price_cache_path=price_cache_path,
        marketstack_cache_path=marketstack_cache_path,
        rates_path=rates_path,
    )
    results = _mapping_obj(result["source_refresh_results"])
    source_rows = _records_obj(results.get("sources"))
    refreshed = sum(1 for row in source_rows if row.get("status") == "REFRESHED")
    typer.echo(f"refresh_execution_id={result['refresh_execution_id']}")
    typer.echo(f"refresh_execution_dir={result['refresh_execution_dir']}")
    typer.echo(f"refresh_status={results.get('refresh_status')}")
    typer.echo(f"refreshed_source_count={refreshed}")
    typer.echo(f"refresh_error={results.get('refresh_error') or ''}")
    typer.echo("execute_refresh=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_source_refresh_app.command("report")
def dynamic_v3_smoothed_source_refresh_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest source refresh。"),
    ] = False,
    refresh_execution_id: Annotated[
        str | None,
        typer.Option("--refresh-execution-id", help="source refresh execution id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed source refresh artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
) -> None:
    """展示 TRADING-281 source refresh 摘要。"""
    payload = system_target.smoothed_source_refresh_report_payload(
        refresh_execution_id=refresh_execution_id,
        latest=latest,
        output_dir=output_dir,
    )
    results = _mapping_obj(payload.get("source_refresh_results"))
    typer.echo(f"refresh_execution_id={payload['refresh_execution_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"refresh_status={results.get('refresh_status')}")
    typer.echo(f"execute_refresh={payload.get('execute_refresh')}")
    typer.echo(f"report_path={payload['smoothed_source_refresh_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-source-refresh")
def dynamic_v3_validate_smoothed_source_refresh_command(
    refresh_execution_id: Annotated[
        str,
        typer.Option("--refresh-execution-id", help="source refresh execution id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed source refresh artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
) -> None:
    """校验 TRADING-281 smoothed source refresh artifact。"""
    payload = system_target.validate_smoothed_source_refresh_artifact(
        refresh_execution_id=refresh_execution_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_post_refresh_validate_app.command("run")
def dynamic_v3_smoothed_post_refresh_validate_run_command(
    refresh_execution_id: Annotated[
        str,
        typer.Option("--refresh-execution-id", help="source refresh execution id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed post-refresh artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
    refresh_execution_dir: Annotated[
        Path,
        typer.Option("--refresh-execution-dir", help="source refresh artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
    preflight_dir: Annotated[
        Path,
        typer.Option("--preflight-dir", help="smoothed preflight artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache", help="cached ETF price CSV。"),
    ] = system_target.DEFAULT_PRICE_CACHE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="cached rates CSV。"),
    ] = system_target.DEFAULT_RATES_CACHE_PATH,
) -> None:
    """运行 TRADING-282 post-refresh validation gate。"""
    result = system_target.run_smoothed_post_refresh_validation(
        refresh_execution_id=refresh_execution_id,
        output_dir=output_dir,
        refresh_execution_dir=refresh_execution_dir,
        preflight_dir=preflight_dir,
        price_cache_path=price_cache_path,
        rates_path=rates_path,
    )
    decision = _mapping_obj(result["post_refresh_decision"])
    data_validation = _mapping_obj(result["post_refresh_data_validation"])
    preflight = _mapping_obj(result["post_refresh_preflight_result"])
    typer.echo(f"post_refresh_id={result['post_refresh_id']}")
    typer.echo(f"post_refresh_dir={result['post_refresh_dir']}")
    typer.echo(f"retry_decision={decision.get('retry_decision')}")
    typer.echo(f"validate_data_status={data_validation.get('validate_data_status')}")
    typer.echo(f"freshness_status={preflight.get('freshness_status')}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_post_refresh_validate_app.command("report")
def dynamic_v3_smoothed_post_refresh_validate_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest post-refresh validation。"),
    ] = False,
    post_refresh_id: Annotated[
        str | None,
        typer.Option("--post-refresh-id", help="post-refresh id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed post-refresh artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
) -> None:
    """展示 TRADING-282 post-refresh validation 摘要。"""
    payload = system_target.smoothed_post_refresh_validation_report_payload(
        post_refresh_id=post_refresh_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping_obj(payload.get("post_refresh_decision"))
    typer.echo(f"post_refresh_id={payload['post_refresh_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"retry_decision={decision.get('retry_decision')}")
    typer.echo(f"freshness_status={payload.get('freshness_status')}")
    typer.echo(f"report_path={payload['smoothed_post_refresh_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-post-refresh")
def dynamic_v3_validate_smoothed_post_refresh_command(
    post_refresh_id: Annotated[
        str,
        typer.Option("--post-refresh-id", help="post-refresh id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed post-refresh artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
) -> None:
    """校验 TRADING-282 smoothed post-refresh validation artifact。"""
    payload = system_target.validate_smoothed_post_refresh_artifact(
        post_refresh_id=post_refresh_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_retry_resume_app.command("run")
def dynamic_v3_smoothed_retry_resume_run_command(
    post_refresh_id: Annotated[
        str,
        typer.Option("--post-refresh-id", help="post-refresh id。"),
    ],
    binding_id: Annotated[
        str | None,
        typer.Option("--binding-id", help="smoothed forward binding id。"),
    ] = None,
    switch_plan_id: Annotated[
        str | None,
        typer.Option("--switch-plan-id", help="paper shadow switch plan id。"),
    ] = None,
    owner_promotion_id: Annotated[
        str | None,
        typer.Option("--owner-promotion-id", help="owner promotion id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed retry resume artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
    post_refresh_dir: Annotated[
        Path,
        typer.Option("--post-refresh-dir", help="post-refresh artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
    bootstrap_retry_dir: Annotated[
        Path,
        typer.Option("--bootstrap-retry-dir", help="bootstrap retry artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache", help="cached ETF price CSV。"),
    ] = system_target.DEFAULT_PRICE_CACHE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="cached rates CSV。"),
    ] = system_target.DEFAULT_RATES_CACHE_PATH,
) -> None:
    """运行 TRADING-283 retry resume。"""
    result = system_target.run_smoothed_retry_resume(
        post_refresh_id=post_refresh_id,
        post_refresh_dir=post_refresh_dir,
        output_dir=output_dir,
        bootstrap_retry_dir=bootstrap_retry_dir,
        binding_id=binding_id,
        switch_plan_id=switch_plan_id,
        owner_promotion_id=owner_promotion_id,
        price_cache_path=price_cache_path,
        rates_path=rates_path,
    )
    summary = _mapping_obj(result["resume_summary"])
    precondition = _mapping_obj(result["resume_precondition_check"])
    typer.echo(f"resume_id={result['resume_id']}")
    typer.echo(f"resume_dir={result['resume_dir']}")
    typer.echo(f"resume_status={summary.get('resume_status')}")
    typer.echo(f"can_resume={precondition.get('can_resume')}")
    typer.echo(f"updated_windows={summary.get('updated_windows')}")
    typer.echo(f"can_execute_switch={summary.get('can_execute_switch')}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_retry_resume_app.command("report")
def dynamic_v3_smoothed_retry_resume_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest retry resume。"),
    ] = False,
    resume_id: Annotated[
        str | None,
        typer.Option("--resume-id", help="resume id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed retry resume artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
) -> None:
    """展示 TRADING-283 retry resume 摘要。"""
    payload = system_target.smoothed_retry_resume_report_payload(
        resume_id=resume_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("resume_summary"))
    typer.echo(f"resume_id={payload['resume_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"resume_status={summary.get('resume_status')}")
    typer.echo(f"updated_windows={summary.get('updated_windows')}")
    typer.echo(f"report_path={payload['smoothed_retry_resume_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-retry-resume")
def dynamic_v3_validate_smoothed_retry_resume_command(
    resume_id: Annotated[str, typer.Option("--resume-id", help="resume id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed retry resume artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
) -> None:
    """校验 TRADING-283 smoothed retry resume artifact。"""
    payload = system_target.validate_smoothed_retry_resume_artifact(
        resume_id=resume_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("can_execute_switch=false")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_sample_growth_app.command("build")
def dynamic_v3_smoothed_sample_growth_build_command(
    resume_id: Annotated[str, typer.Option("--resume-id", help="resume id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed sample growth artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR,
    resume_dir: Annotated[
        Path,
        typer.Option("--resume-dir", help="smoothed retry resume artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
) -> None:
    """生成 TRADING-284 sample growth dashboard。"""
    result = system_target.build_smoothed_sample_growth(
        resume_id=resume_id,
        resume_dir=resume_dir,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result["sample_growth_summary"])
    progress = _mapping_obj(summary.get("progress"))
    typer.echo(f"growth_id={result['growth_id']}")
    typer.echo(f"growth_dir={result['growth_dir']}")
    typer.echo(f"growth_status={summary.get('growth_status')}")
    typer.echo(f"forward_progress={progress.get('forward')}")
    typer.echo(f"sideways_progress={progress.get('sideways')}")
    typer.echo(f"recovery_progress={progress.get('recovery')}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_sample_growth_app.command("report")
def dynamic_v3_smoothed_sample_growth_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest sample growth。"),
    ] = False,
    growth_id: Annotated[
        str | None,
        typer.Option("--growth-id", help="sample growth id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed sample growth artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR,
) -> None:
    """展示 TRADING-284 sample growth 摘要。"""
    payload = system_target.smoothed_sample_growth_report_payload(
        growth_id=growth_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("sample_growth_summary"))
    progress = _mapping_obj(summary.get("progress"))
    typer.echo(f"growth_id={payload['growth_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"growth_status={summary.get('growth_status')}")
    typer.echo(f"forward_progress={progress.get('forward')}")
    typer.echo(f"report_path={payload['sample_growth_dashboard_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-sample-growth")
def dynamic_v3_validate_smoothed_sample_growth_command(
    growth_id: Annotated[str, typer.Option("--growth-id", help="growth id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed sample growth artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR,
) -> None:
    """校验 TRADING-284 smoothed sample growth artifact。"""
    payload = system_target.validate_smoothed_sample_growth_artifact(
        growth_id=growth_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_data_readiness_app.command("pack")
def dynamic_v3_smoothed_data_readiness_pack_command(
    refresh_execution_id: Annotated[
        str,
        typer.Option("--refresh-execution-id", help="source refresh execution id。"),
    ],
    post_refresh_id: Annotated[
        str,
        typer.Option("--post-refresh-id", help="post-refresh id。"),
    ],
    resume_id: Annotated[str, typer.Option("--resume-id", help="resume id。")],
    growth_id: Annotated[str, typer.Option("--growth-id", help="growth id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed data readiness artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_DATA_READINESS_DIR,
    refresh_execution_dir: Annotated[
        Path,
        typer.Option("--refresh-execution-dir", help="source refresh artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_SOURCE_REFRESH_DIR,
    post_refresh_dir: Annotated[
        Path,
        typer.Option("--post-refresh-dir", help="post-refresh artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_POST_REFRESH_VALIDATION_DIR,
    resume_dir: Annotated[
        Path,
        typer.Option("--resume-dir", help="retry resume artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_RETRY_RESUME_DIR,
    growth_dir: Annotated[
        Path,
        typer.Option("--growth-dir", help="sample growth artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_SAMPLE_GROWTH_DIR,
) -> None:
    """生成 TRADING-285 owner data readiness status pack。"""
    result = system_target.pack_smoothed_data_readiness(
        refresh_execution_id=refresh_execution_id,
        post_refresh_id=post_refresh_id,
        resume_id=resume_id,
        growth_id=growth_id,
        refresh_execution_dir=refresh_execution_dir,
        post_refresh_dir=post_refresh_dir,
        resume_dir=resume_dir,
        growth_dir=growth_dir,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result["owner_data_readiness_summary"])
    typer.echo(f"readiness_id={result['readiness_id']}")
    typer.echo(f"readiness_dir={result['readiness_dir']}")
    typer.echo(f"current_status={summary.get('current_status')}")
    typer.echo(f"recommended_owner_action={summary.get('recommended_owner_action')}")
    typer.echo(f"retry_status={summary.get('retry_status')}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_data_readiness_app.command("report")
def dynamic_v3_smoothed_data_readiness_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest data readiness。"),
    ] = False,
    readiness_id: Annotated[
        str | None,
        typer.Option("--readiness-id", help="data readiness id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed data readiness artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_DATA_READINESS_DIR,
) -> None:
    """展示 TRADING-285 data readiness 摘要。"""
    payload = system_target.smoothed_data_readiness_report_payload(
        readiness_id=readiness_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("owner_data_readiness_summary"))
    typer.echo(f"readiness_id={payload['readiness_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"current_status={summary.get('current_status')}")
    typer.echo(f"recommended_owner_action={summary.get('recommended_owner_action')}")
    typer.echo(f"report_path={payload['smoothed_data_readiness_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-data-readiness")
def dynamic_v3_validate_smoothed_data_readiness_command(
    readiness_id: Annotated[
        str,
        typer.Option("--readiness-id", help="data readiness id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed data readiness artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_DATA_READINESS_DIR,
) -> None:
    """校验 TRADING-285 smoothed data readiness artifact。"""
    payload = system_target.validate_smoothed_data_readiness_artifact(
        readiness_id=readiness_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)



