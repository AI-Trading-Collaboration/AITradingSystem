from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CANDIDATE_CLUSTER_DIR,
    DEFAULT_OBSERVE_POOL_DIR,
    DEFAULT_SHADOW_MONITOR_RUN_DIR,
    DEFAULT_SHADOW_SHORTLIST_DIR,
    DEFAULT_SHORTLIST_DIR,
    activate_shadow_monitoring,
    build_shadow_shortlist,
    build_shadow_shortlist_monitoring_pack,
    candidate_cluster_report_payload,
    run_candidate_clustering,
    run_shadow_shortlist_monitor,
    shadow_monitor_run_report_payload,
    shadow_shortlist_report_payload,
    shortlist_report_payload,
    validate_candidate_cluster_artifact,
    validate_shadow_monitor_run_artifact,
    validate_shadow_shortlist_artifact,
    validate_shortlist_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj, parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_candidate_cluster_app,
    dynamic_v3_rescue_app,
    dynamic_v3_shadow_monitor_run_app,
    dynamic_v3_shadow_shortlist_app,
    dynamic_v3_shortlist_app,
)


@dynamic_v3_shortlist_app.command("build")
def dynamic_v3_shortlist_build_command(
    observe_pool_id: Annotated[str, typer.Option("--observe-pool-id", help="observe pool id。")],
    target_size: Annotated[int, typer.Option("--target-size", help="target shortlist size。")] = 10,
    max_size: Annotated[int, typer.Option("--max-size", help="max shortlist size。")] = 20,
    min_size: Annotated[int, typer.Option("--min-size", help="min shortlist size。")] = 5,
    observe_pool_dir: Annotated[
        Path,
        typer.Option("--observe-pool-dir", help="observe pool artifact root。"),
    ] = DEFAULT_OBSERVE_POOL_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shortlist artifact root。"),
    ] = DEFAULT_SHORTLIST_DIR,
) -> None:
    """生成 TRADING-126 shadow shortlist。"""
    result = build_shadow_shortlist(
        observe_pool_id=observe_pool_id,
        target_size=target_size,
        max_size=max_size,
        min_size=min_size,
        observe_pool_dir=observe_pool_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"shortlist_id={result['shortlist_id']}")
    typer.echo(f"shortlist_dir={result['shortlist_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"observe_pool_candidate_count={manifest['observe_pool_candidate_count']}")
    typer.echo(f"shortlist_count={manifest['shortlist_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_shortlist_app.command("report")
def dynamic_v3_shortlist_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest shortlist pointer。"),
    ] = False,
    shortlist_id: Annotated[
        str | None,
        typer.Option("--shortlist-id", help="shortlist id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shortlist artifact root。"),
    ] = DEFAULT_SHORTLIST_DIR,
) -> None:
    """展示 TRADING-126 shortlist 摘要。"""
    payload = shortlist_report_payload(
        shortlist_id=shortlist_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"shortlist_id={payload['shortlist_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"shortlist_count={payload['shortlist_count']}")
    typer.echo(f"report_path={payload['shortlist_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-shortlist")
def dynamic_v3_validate_shortlist_command(
    shortlist_id: Annotated[str, typer.Option("--shortlist-id", help="shortlist id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shortlist artifact root。"),
    ] = DEFAULT_SHORTLIST_DIR,
) -> None:
    """校验 TRADING-126 shortlist artifact。"""
    payload = validate_shortlist_artifact(shortlist_id=shortlist_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_candidate_cluster_app.command("run")
def dynamic_v3_candidate_cluster_run_command(
    shortlist_id: Annotated[str, typer.Option("--shortlist-id", help="shortlist id。")],
    shortlist_dir: Annotated[
        Path,
        typer.Option("--shortlist-dir", help="shortlist artifact root。"),
    ] = DEFAULT_SHORTLIST_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate cluster artifact root。"),
    ] = DEFAULT_CANDIDATE_CLUSTER_DIR,
) -> None:
    """生成 TRADING-127 candidate cluster。"""
    result = run_candidate_clustering(
        shortlist_id=shortlist_id,
        shortlist_dir=shortlist_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"cluster_id={result['cluster_id']}")
    typer.echo(f"cluster_dir={result['cluster_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"cluster_count={manifest['cluster_count']}")
    typer.echo(f"representative_count={manifest['representative_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_candidate_cluster_app.command("report")
def dynamic_v3_candidate_cluster_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest candidate cluster pointer。"),
    ] = False,
    cluster_id: Annotated[str | None, typer.Option("--cluster-id", help="cluster id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate cluster artifact root。"),
    ] = DEFAULT_CANDIDATE_CLUSTER_DIR,
) -> None:
    """展示 TRADING-127 candidate cluster 摘要。"""
    payload = candidate_cluster_report_payload(
        cluster_id=cluster_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"cluster_id={payload['cluster_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"cluster_count={payload['cluster_count']}")
    typer.echo(f"report_path={payload['candidate_cluster_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-candidate-cluster")
def dynamic_v3_validate_candidate_cluster_command(
    cluster_id: Annotated[str, typer.Option("--cluster-id", help="cluster id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate cluster artifact root。"),
    ] = DEFAULT_CANDIDATE_CLUSTER_DIR,
) -> None:
    """校验 TRADING-127 candidate cluster artifact。"""
    payload = validate_candidate_cluster_artifact(cluster_id=cluster_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_shadow_shortlist_app.command("build")
def dynamic_v3_shadow_shortlist_build_command(
    shortlist_id: Annotated[str, typer.Option("--shortlist-id", help="shortlist id。")],
    cluster_id: Annotated[str, typer.Option("--cluster-id", help="cluster id。")],
    shortlist_dir: Annotated[
        Path,
        typer.Option("--shortlist-dir", help="shortlist artifact root。"),
    ] = DEFAULT_SHORTLIST_DIR,
    cluster_dir: Annotated[
        Path,
        typer.Option("--cluster-dir", help="candidate cluster artifact root。"),
    ] = DEFAULT_CANDIDATE_CLUSTER_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
) -> None:
    """生成 TRADING-128 shadow shortlist monitoring pack。"""
    result = build_shadow_shortlist_monitoring_pack(
        shortlist_id=shortlist_id,
        cluster_id=cluster_id,
        shortlist_dir=shortlist_dir,
        cluster_dir=cluster_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"shadow_shortlist_id={result['shadow_shortlist_id']}")
    typer.echo(f"shadow_shortlist_dir={result['shadow_shortlist_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"shadow_candidate_count={manifest['shadow_candidate_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_shadow_shortlist_app.command("report")
def dynamic_v3_shadow_shortlist_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest shadow shortlist pointer。"),
    ] = False,
    shadow_shortlist_id: Annotated[
        str | None,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
) -> None:
    """展示 TRADING-128 shadow shortlist 摘要。"""
    payload = shadow_shortlist_report_payload(
        shadow_shortlist_id=shadow_shortlist_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"shadow_shortlist_id={payload['shadow_shortlist_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"shadow_candidate_count={payload['shadow_candidate_count']}")
    typer.echo(f"report_path={payload['shadow_shortlist_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-shadow-shortlist")
def dynamic_v3_validate_shadow_shortlist_command(
    shadow_shortlist_id: Annotated[
        str,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
) -> None:
    """校验 TRADING-128 shadow shortlist artifact。"""
    payload = validate_shadow_shortlist_artifact(
        shadow_shortlist_id=shadow_shortlist_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_shadow_monitor_run_app.command("activate")
def dynamic_v3_shadow_monitor_activate_command(
    shadow_shortlist_id: Annotated[
        str,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ],
    shadow_shortlist_dir: Annotated[
        Path,
        typer.Option("--shadow-shortlist-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
) -> None:
    """激活 TRADING-131 shadow shortlist monitoring。"""
    result = activate_shadow_monitoring(
        shadow_shortlist_id=shadow_shortlist_id,
        shadow_shortlist_dir=shadow_shortlist_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"activation_id={result['activation_id']}")
    typer.echo(f"activation_dir={result['activation_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"monitoring_status={manifest['monitoring_status']}")
    typer.echo(f"candidate_count={manifest['candidate_count']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_shadow_monitor_run_app.command("run")
def dynamic_v3_shadow_monitor_run_from_shortlist_command(
    shadow_shortlist_id: Annotated[
        str,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ],
    as_of: Annotated[str, typer.Option("--as-of", help="monitor as-of date。")],
    shadow_shortlist_dir: Annotated[
        Path,
        typer.Option("--shadow-shortlist-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
) -> None:
    """生成 TRADING-131 shadow monitor daily / weekly artifact。"""
    result = run_shadow_shortlist_monitor(
        shadow_shortlist_id=shadow_shortlist_id,
        as_of=parse_date(as_of),
        shadow_shortlist_dir=shadow_shortlist_dir,
        output_dir=output_dir,
    )
    summary = result["summary"]
    typer.echo(f"monitor_run_id={result['monitor_run_id']}")
    typer.echo(f"monitor_run_dir={result['monitor_run_dir']}")
    typer.echo(f"active_count={summary['active_count']}")
    typer.echo(f"summary_recommendation={summary['summary_recommendation']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_shadow_monitor_run_app.command("report")
def dynamic_v3_shadow_monitor_run_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest shadow monitor run。"),
    ] = False,
    monitor_run_id: Annotated[
        str | None,
        typer.Option("--monitor-run-id", help="monitor run id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
) -> None:
    """展示 TRADING-131 shadow monitor run 摘要。"""
    payload = shadow_monitor_run_report_payload(
        monitor_run_id=monitor_run_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = mapping_obj(payload.get("shadow_monitor_summary"))
    typer.echo(f"monitor_run_id={payload['monitor_run_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"active_count={summary.get('active_count')}")
    typer.echo(f"summary_recommendation={summary.get('summary_recommendation')}")
    typer.echo(f"report_path={payload['shadow_monitor_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-shadow-monitor-run")
def dynamic_v3_validate_shadow_monitor_run_command(
    monitor_run_id: Annotated[
        str,
        typer.Option("--monitor-run-id", help="monitor run id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
) -> None:
    """校验 TRADING-131 shadow monitor run artifact。"""
    payload = validate_shadow_monitor_run_artifact(
        monitor_run_id=monitor_run_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
