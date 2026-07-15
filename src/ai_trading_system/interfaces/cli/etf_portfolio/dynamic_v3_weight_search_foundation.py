from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.etf_portfolio import (
    dynamic_v3_weight_search_foundation as weight_batch_search,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_weight_batch_backfill_app,
    dynamic_v3_weight_experiment_batch2_app,
    dynamic_v3_weight_search_space_app,
)


@dynamic_v3_weight_search_space_app.command("validate")
def dynamic_v3_weight_search_space_validate_command(
    config: Annotated[
        Path,
        typer.Option("--config", help="weight search space config。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SEARCH_SPACE_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight search space artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SEARCH_SPACE_DIR,
) -> None:
    """校验并登记 TRADING-286 weight search space。"""
    result = weight_batch_search.run_weight_search_space_validation(
        config_path=config,
        output_dir=output_dir,
    )
    typer.echo(f"search_space_id={result['search_space_id']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"families={','.join(result['manifest']['families'])}")
    typer.echo("broker_action_allowed=false")
    if result["manifest"]["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_weight_search_space_app.command("report")
def dynamic_v3_weight_search_space_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest search space。"),
    ] = False,
    search_space_id: Annotated[
        str | None,
        typer.Option("--search-space-id", help="search space id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight search space artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SEARCH_SPACE_DIR,
) -> None:
    payload = weight_batch_search.weight_search_space_report_payload(
        search_space_id=search_space_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"search_space_id={payload['search_space_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"families={','.join(payload['families'])}")
    typer.echo(f"report_path={payload['weight_search_space_report_path']}")


@dynamic_v3_rescue_app.command("validate-weight-search-space")
def dynamic_v3_validate_weight_search_space_command(
    search_space_id: Annotated[
        str,
        typer.Option("--search-space-id", help="search space id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight search space artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SEARCH_SPACE_DIR,
) -> None:
    payload = weight_batch_search.validate_weight_search_space_artifact(
        search_space_id=search_space_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_weight_experiment_batch2_app.command("build")
def dynamic_v3_weight_experiment_batch2_build_command(
    search_space_id: Annotated[
        str,
        typer.Option("--search-space-id", help="search space id。"),
    ],
    source_backfill_id: Annotated[
        str | None,
        typer.Option("--source-backfill-id", help="paper shadow backfill id override。"),
    ] = None,
    search_space_dir: Annotated[
        Path,
        typer.Option("--search-space-dir", help="weight search space artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_SEARCH_SPACE_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Batch-2 matrix artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR,
) -> None:
    result = weight_batch_search.build_weight_experiment_batch2(
        search_space_id=search_space_id,
        source_backfill_id=source_backfill_id,
        search_space_dir=search_space_dir,
        output_dir=output_dir,
    )
    typer.echo(f"batch2_matrix_id={result['batch2_matrix_id']}")
    typer.echo(f"variant_count={result['manifest']['variant_count']}")
    typer.echo(f"families={','.join(result['manifest']['families_covered'])}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_weight_experiment_batch2_app.command("report")
def dynamic_v3_weight_experiment_batch2_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest Batch-2 matrix。"),
    ] = False,
    matrix_id: Annotated[str | None, typer.Option("--matrix-id", help="matrix id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Batch-2 matrix artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR,
) -> None:
    payload = weight_batch_search.weight_experiment_batch2_report_payload(
        matrix_id=matrix_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"batch2_matrix_id={payload['batch2_matrix_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"variant_count={payload['variant_count']}")
    typer.echo(f"report_path={payload['batch2_matrix_report_path']}")


@dynamic_v3_rescue_app.command("validate-weight-experiment-batch2")
def dynamic_v3_validate_weight_experiment_batch2_command(
    matrix_id: Annotated[str, typer.Option("--matrix-id", help="matrix id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="Batch-2 matrix artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR,
) -> None:
    payload = weight_batch_search.validate_weight_experiment_batch2_artifact(
        matrix_id=matrix_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_weight_batch_backfill_app.command("run")
def dynamic_v3_weight_batch_backfill_run_command(
    matrix_id: Annotated[str, typer.Option("--matrix-id", help="Batch-2 matrix id。")],
    matrix_dir: Annotated[
        Path,
        typer.Option("--matrix-dir", help="Batch-2 matrix artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR,
    baseline_backfill_dir: Annotated[
        Path,
        typer.Option("--baseline-backfill-dir", help="paper shadow backfill root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight batch backfill artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
    price_cache_path: Annotated[
        Path | None,
        typer.Option("--price-cache-path", help="price cache override。"),
    ] = None,
    rates_cache_path: Annotated[
        Path,
        typer.Option("--rates-cache-path", help="rates cache path。"),
    ] = system_target.DEFAULT_RATES_CACHE_PATH,
) -> None:
    result = weight_batch_search.run_weight_batch_backfill(
        matrix_id=matrix_id,
        matrix_dir=matrix_dir,
        baseline_backfill_dir=baseline_backfill_dir,
        output_dir=output_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
    )
    manifest = result["manifest"]
    typer.echo(f"batch_backfill_id={result['batch_backfill_id']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"date_range={manifest['date_start']}..{manifest['date_end']}")
    typer.echo(f"data_quality_status={manifest['data_quality_status']}")
    typer.echo(f"variants_completed={manifest['variants_completed']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_weight_batch_backfill_app.command("resume")
def dynamic_v3_weight_batch_backfill_resume_command(
    backfill_id: Annotated[
        str,
        typer.Option("--backfill-id", help="batch backfill id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight batch backfill artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
) -> None:
    result = weight_batch_search.resume_weight_batch_backfill(
        backfill_id=backfill_id,
        output_dir=output_dir,
    )
    progress = _mapping_obj(result["progress"])
    typer.echo(f"batch_backfill_id={result['batch_backfill_id']}")
    typer.echo(f"resume_status={result['resume_status']}")
    typer.echo(f"variants_completed={progress.get('variants_completed')}")


@dynamic_v3_weight_batch_backfill_app.command("report")
def dynamic_v3_weight_batch_backfill_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest batch backfill。"),
    ] = False,
    backfill_id: Annotated[
        str | None,
        typer.Option("--backfill-id", help="batch backfill id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight batch backfill artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
) -> None:
    payload = weight_batch_search.weight_batch_backfill_report_payload(
        backfill_id=backfill_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"batch_backfill_id={payload['batch_backfill_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"data_quality_status={payload['data_quality_status']}")
    typer.echo(f"variants_completed={payload['variants_completed']}")
    typer.echo(f"report_path={payload['batch_backfill_report_path']}")


@dynamic_v3_rescue_app.command("validate-weight-batch-backfill")
def dynamic_v3_validate_weight_batch_backfill_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="batch backfill id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight batch backfill artifact root。"),
    ] = weight_batch_search.DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
) -> None:
    payload = weight_batch_search.validate_weight_batch_backfill_artifact(
        backfill_id=backfill_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
