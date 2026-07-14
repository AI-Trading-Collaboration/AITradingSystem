from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_freshness as system_target,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_smoothed_blocked_explain_app,
    dynamic_v3_smoothed_bootstrap_retry_app,
    dynamic_v3_smoothed_data_preflight_app,
    dynamic_v3_smoothed_latest_emission_app,
    dynamic_v3_smoothed_refresh_plan_app,
)


def _records_obj(value: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in value] if isinstance(value, list) else []


def _texts(value: object) -> list[str]:
    return [str(item) for item in value] if isinstance(value, list) else []


@dynamic_v3_smoothed_data_preflight_app.command("run")
def dynamic_v3_smoothed_data_preflight_run_command(
    requested_as_of: Annotated[
        str | None,
        typer.Option("--requested-as-of", help="requested as_of date。"),
    ] = None,
    requested_week_ending: Annotated[
        str | None,
        typer.Option("--requested-week-ending", help="requested week ending date。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed data preflight artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache", help="cached ETF price CSV。"),
    ] = system_target.DEFAULT_PRICE_CACHE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
) -> None:
    """运行 TRADING-276 smoothed data freshness preflight。"""
    result = system_target.run_smoothed_data_preflight(
        requested_as_of=_parse_date(requested_as_of) if requested_as_of else None,
        requested_week_ending=(
            _parse_date(requested_week_ending) if requested_week_ending else None
        ),
        output_dir=output_dir,
        price_cache_path=price_cache_path,
        rates_path=rates_path,
    )
    snapshot = result["data_freshness_snapshot"]
    typer.echo(f"preflight_id={result['preflight_id']}")
    typer.echo(f"preflight_dir={result['preflight_dir']}")
    typer.echo(f"requested_as_of={snapshot.get('requested_as_of')}")
    typer.echo(f"requested_week_ending={snapshot.get('requested_week_ending')}")
    typer.echo(f"latest_valid_as_of={snapshot.get('latest_valid_as_of')}")
    typer.echo(f"freshness_status={snapshot.get('freshness_status')}")
    typer.echo(f"blocking_errors={','.join(_texts(snapshot.get('blocking_errors')))}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_data_preflight_app.command("report")
def dynamic_v3_smoothed_data_preflight_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest data preflight。"),
    ] = False,
    preflight_id: Annotated[
        str | None,
        typer.Option("--preflight-id", help="preflight id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed data preflight artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
) -> None:
    """展示 TRADING-276 smoothed data freshness preflight 摘要。"""
    payload = system_target.smoothed_data_preflight_report_payload(
        preflight_id=preflight_id,
        latest=latest,
        output_dir=output_dir,
    )
    snapshot = _mapping_obj(payload.get("data_freshness_snapshot"))
    typer.echo(f"preflight_id={payload['preflight_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"latest_valid_as_of={snapshot.get('latest_valid_as_of')}")
    typer.echo(f"freshness_status={snapshot.get('freshness_status')}")
    typer.echo(f"blocking_errors={','.join(_texts(snapshot.get('blocking_errors')))}")
    typer.echo(f"report_path={payload['smoothed_data_preflight_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-data-preflight")
def dynamic_v3_validate_smoothed_data_preflight_command(
    preflight_id: Annotated[str, typer.Option("--preflight-id", help="preflight id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed data preflight artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
) -> None:
    """校验 TRADING-276 smoothed data preflight artifact。"""
    payload = system_target.validate_smoothed_data_preflight_artifact(
        preflight_id=preflight_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_latest_emission_app.command("run")
def dynamic_v3_smoothed_latest_emission_run_command(
    preflight_id: Annotated[str, typer.Option("--preflight-id", help="preflight id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed latest emission artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_LATEST_EMISSION_DIR,
    preflight_dir: Annotated[
        Path,
        typer.Option("--preflight-dir", help="smoothed data preflight artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache", help="cached ETF price CSV。"),
    ] = system_target.DEFAULT_PRICE_CACHE_PATH,
) -> None:
    """运行 TRADING-277 latest-available smoothed daily emission fallback。"""
    result = system_target.run_smoothed_latest_emission(
        preflight_id=preflight_id,
        preflight_dir=preflight_dir,
        output_dir=output_dir,
        price_cache_path=price_cache_path,
    )
    resolution = result["latest_emission_resolution"]
    links = result["latest_emission_artifact_links"]
    typer.echo(f"latest_emission_id={result['latest_emission_id']}")
    typer.echo(f"latest_emission_dir={result['latest_emission_dir']}")
    typer.echo(f"requested_as_of={resolution['requested_as_of']}")
    typer.echo(f"resolved_as_of={resolution['resolved_as_of']}")
    typer.echo(f"emitted_event_count={links['emitted_event_count']}")
    typer.echo(f"outcome_update_allowed={resolution['outcome_update_allowed']}")
    typer.echo(f"future_data_used={resolution['future_data_used']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_latest_emission_app.command("report")
def dynamic_v3_smoothed_latest_emission_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest emission fallback。"),
    ] = False,
    latest_emission_id: Annotated[
        str | None,
        typer.Option("--latest-emission-id", help="latest emission id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed latest emission artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_LATEST_EMISSION_DIR,
) -> None:
    """展示 TRADING-277 latest-available emission 摘要。"""
    payload = system_target.smoothed_latest_emission_report_payload(
        latest_emission_id=latest_emission_id,
        latest=latest,
        output_dir=output_dir,
    )
    resolution = _mapping_obj(payload.get("latest_emission_resolution"))
    links = _mapping_obj(payload.get("latest_emission_artifact_links"))
    typer.echo(f"latest_emission_id={payload['latest_emission_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"requested_as_of={resolution.get('requested_as_of')}")
    typer.echo(f"resolved_as_of={resolution.get('resolved_as_of')}")
    typer.echo(f"emitted_event_count={links.get('emitted_event_count')}")
    typer.echo(f"outcome_update_allowed={resolution.get('outcome_update_allowed')}")
    typer.echo(f"report_path={payload['smoothed_latest_emission_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-latest-emission")
def dynamic_v3_validate_smoothed_latest_emission_command(
    latest_emission_id: Annotated[
        str,
        typer.Option("--latest-emission-id", help="latest emission id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed latest emission artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_LATEST_EMISSION_DIR,
) -> None:
    """校验 TRADING-277 smoothed latest emission artifact。"""
    payload = system_target.validate_smoothed_latest_emission_artifact(
        latest_emission_id=latest_emission_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("outcome_update_allowed=false")
    typer.echo("future_data_used=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_blocked_explain_app.command("run")
def dynamic_v3_smoothed_blocked_explain_run_command(
    preflight_id: Annotated[str, typer.Option("--preflight-id", help="preflight id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed blocked explain artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR,
    preflight_dir: Annotated[
        Path,
        typer.Option("--preflight-dir", help="smoothed data preflight artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
) -> None:
    """运行 TRADING-278 blocked due / weekly run explanation pack。"""
    result = system_target.run_smoothed_blocked_explain(
        preflight_id=preflight_id,
        preflight_dir=preflight_dir,
        output_dir=output_dir,
    )
    payload = result["blocked_command_explanations"]
    commands = _records_obj(payload.get("blocked_commands"))
    typer.echo(f"explain_id={result['explain_id']}")
    typer.echo(f"explain_dir={result['explain_dir']}")
    typer.echo(f"blocked_command_count={len(commands)}")
    typer.echo("safe_next_action=refresh_sources_then_retry")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_blocked_explain_app.command("report")
def dynamic_v3_smoothed_blocked_explain_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest blocked explain。"),
    ] = False,
    explain_id: Annotated[str | None, typer.Option("--explain-id", help="explain id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed blocked explain artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR,
) -> None:
    """展示 TRADING-278 blocked explanation 摘要。"""
    payload = system_target.smoothed_blocked_explain_report_payload(
        explain_id=explain_id,
        latest=latest,
        output_dir=output_dir,
    )
    explanations = _records_obj(
        _mapping_obj(payload.get("blocked_command_explanations")).get("blocked_commands")
    )
    typer.echo(f"explain_id={payload['explain_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"blocked_command_count={len(explanations)}")
    typer.echo(f"report_path={payload['smoothed_blocked_explain_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-blocked-explain")
def dynamic_v3_validate_smoothed_blocked_explain_command(
    explain_id: Annotated[str, typer.Option("--explain-id", help="explain id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed blocked explain artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR,
) -> None:
    """校验 TRADING-278 smoothed blocked explain artifact。"""
    payload = system_target.validate_smoothed_blocked_explain_artifact(
        explain_id=explain_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_refresh_plan_app.command("run")
def dynamic_v3_smoothed_refresh_plan_run_command(
    preflight_id: Annotated[str, typer.Option("--preflight-id", help="preflight id。")],
    explain_id: Annotated[str, typer.Option("--explain-id", help="explain id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed refresh plan artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_REFRESH_PLAN_DIR,
    preflight_dir: Annotated[
        Path,
        typer.Option("--preflight-dir", help="smoothed data preflight artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_DATA_PREFLIGHT_DIR,
    explain_dir: Annotated[
        Path,
        typer.Option("--explain-dir", help="smoothed blocked explain artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_BLOCKED_EXPLAIN_DIR,
) -> None:
    """运行 TRADING-279 source refresh and rerun plan。"""
    result = system_target.run_smoothed_refresh_plan(
        preflight_id=preflight_id,
        explain_id=explain_id,
        preflight_dir=preflight_dir,
        explain_dir=explain_dir,
        output_dir=output_dir,
    )
    requirements = result["source_refresh_requirements"]
    stale = [
        row
        for row in _records_obj(requirements.get("source_requirements"))
        if row.get("status") != "FRESH"
    ]
    typer.echo(f"refresh_plan_id={result['refresh_plan_id']}")
    typer.echo(f"refresh_plan_dir={result['refresh_plan_dir']}")
    typer.echo(f"stale_source_count={len(stale)}")
    typer.echo(f"all_required_sources_fresh={requirements['all_required_sources_fresh']}")
    typer.echo("external_refresh_executed=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_refresh_plan_app.command("report")
def dynamic_v3_smoothed_refresh_plan_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest refresh plan。"),
    ] = False,
    refresh_plan_id: Annotated[
        str | None,
        typer.Option("--refresh-plan-id", help="refresh plan id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed refresh plan artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_REFRESH_PLAN_DIR,
) -> None:
    """展示 TRADING-279 refresh plan 摘要。"""
    payload = system_target.smoothed_refresh_plan_report_payload(
        refresh_plan_id=refresh_plan_id,
        latest=latest,
        output_dir=output_dir,
    )
    requirements = _mapping_obj(payload.get("source_refresh_requirements"))
    rerun = _mapping_obj(payload.get("rerun_command_plan"))
    typer.echo(f"refresh_plan_id={payload['refresh_plan_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"all_required_sources_fresh={requirements.get('all_required_sources_fresh')}")
    typer.echo(f"rerun_allowed_now={rerun.get('rerun_allowed_now')}")
    typer.echo(f"report_path={payload['smoothed_refresh_plan_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-refresh-plan")
def dynamic_v3_validate_smoothed_refresh_plan_command(
    refresh_plan_id: Annotated[
        str,
        typer.Option("--refresh-plan-id", help="refresh plan id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed refresh plan artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_REFRESH_PLAN_DIR,
) -> None:
    """校验 TRADING-279 smoothed refresh plan artifact。"""
    payload = system_target.validate_smoothed_refresh_plan_artifact(
        refresh_plan_id=refresh_plan_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("external_refresh_executed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_bootstrap_retry_app.command("run")
def dynamic_v3_smoothed_bootstrap_retry_run_command(
    requested_as_of: Annotated[
        str | None,
        typer.Option("--requested-as-of", help="requested as_of date。"),
    ] = None,
    requested_week_ending: Annotated[
        str | None,
        typer.Option("--requested-week-ending", help="requested week ending date。"),
    ] = None,
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
        typer.Option("--output-dir", help="smoothed bootstrap retry artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache", help="cached ETF price CSV。"),
    ] = system_target.DEFAULT_PRICE_CACHE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
) -> None:
    """运行 TRADING-280 smoothed bootstrap retry runner。"""
    result = system_target.run_smoothed_bootstrap_retry(
        requested_as_of=_parse_date(requested_as_of) if requested_as_of else None,
        requested_week_ending=(
            _parse_date(requested_week_ending) if requested_week_ending else None
        ),
        binding_id=binding_id,
        switch_plan_id=switch_plan_id,
        owner_promotion_id=owner_promotion_id,
        output_dir=output_dir,
        price_cache_path=price_cache_path,
        rates_path=rates_path,
    )
    summary = result["retry_summary"]
    preflight = result["retry_preflight_result"]
    typer.echo(f"retry_id={result['retry_id']}")
    typer.echo(f"retry_dir={result['retry_dir']}")
    typer.echo(f"retry_status={summary['retry_status']}")
    typer.echo(f"preflight_status={preflight['preflight_status']}")
    typer.echo(f"blocking_errors={','.join(_texts(preflight.get('blocking_errors')))}")
    typer.echo(f"updated_windows={summary['updated_windows']}")
    typer.echo(f"can_execute_switch={summary['can_execute_switch']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_smoothed_bootstrap_retry_app.command("report")
def dynamic_v3_smoothed_bootstrap_retry_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest retry runner。"),
    ] = False,
    retry_id: Annotated[str | None, typer.Option("--retry-id", help="retry id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed bootstrap retry artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR,
) -> None:
    """展示 TRADING-280 bootstrap retry 摘要。"""
    payload = system_target.smoothed_bootstrap_retry_report_payload(
        retry_id=retry_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("retry_summary"))
    typer.echo(f"retry_id={payload['retry_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"retry_status={summary.get('retry_status')}")
    typer.echo(f"updated_windows={summary.get('updated_windows')}")
    typer.echo(f"can_execute_switch={summary.get('can_execute_switch')}")
    typer.echo(f"report_path={payload['smoothed_bootstrap_retry_report_path']}")


@dynamic_v3_rescue_app.command("validate-smoothed-bootstrap-retry")
def dynamic_v3_validate_smoothed_bootstrap_retry_command(
    retry_id: Annotated[str, typer.Option("--retry-id", help="retry id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed bootstrap retry artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_BOOTSTRAP_RETRY_DIR,
) -> None:
    """校验 TRADING-280 smoothed bootstrap retry artifact。"""
    payload = system_target.validate_smoothed_bootstrap_retry_artifact(
        retry_id=retry_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("can_execute_switch=false")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
