from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
    DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    DEFAULT_SHADOW_AGING_DIR,
    run_shadow_aging,
    shadow_aging_report_payload,
    validate_shadow_aging_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CONSENSUS_DRIFT_DIR,
    DEFAULT_SHADOW_MONITOR_RUN_DIR,
    DEFAULT_SHADOW_SHORTLIST_DIR,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_shadow_aging_app,
)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


@dynamic_v3_shadow_aging_app.command("run")
def dynamic_v3_shadow_aging_run_command(
    shadow_shortlist_id: Annotated[
        str,
        typer.Option("--shadow-shortlist-id", help="shadow shortlist id。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper portfolio config。"),
    ] = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow aging artifact root。"),
    ] = DEFAULT_SHADOW_AGING_DIR,
    shadow_shortlist_dir: Annotated[
        Path,
        typer.Option("--shadow-shortlist-dir", help="shadow shortlist artifact root。"),
    ] = DEFAULT_SHADOW_SHORTLIST_DIR,
    shadow_monitor_run_dir: Annotated[
        Path,
        typer.Option("--shadow-monitor-run-dir", help="shadow monitor run artifact root。"),
    ] = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    consensus_drift_dir: Annotated[
        Path,
        typer.Option("--consensus-drift-dir", help="consensus drift artifact root。"),
    ] = DEFAULT_CONSENSUS_DRIFT_DIR,
    advisory_outcome_dir: Annotated[
        Path,
        typer.Option("--advisory-outcome-dir", help="advisory outcome artifact root。"),
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
) -> None:
    """生成 TRADING-139 shadow candidate aging v2。"""
    result = run_shadow_aging(
        shadow_shortlist_id=shadow_shortlist_id,
        config_path=config_path,
        output_dir=output_dir,
        shadow_shortlist_dir=shadow_shortlist_dir,
        shadow_monitor_run_dir=shadow_monitor_run_dir,
        consensus_drift_dir=consensus_drift_dir,
        advisory_outcome_dir=advisory_outcome_dir,
    )
    summary = result["promotion_clock_v2_summary"]
    typer.echo(f"aging_id={result['aging_id']}")
    typer.echo(f"aging_dir={result['aging_dir']}")
    typer.echo(f"eligible_for_review_count={summary['eligible_for_review_count']}")
    typer.echo(f"downgrade_recommended_count={summary['downgrade_recommended_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_shadow_aging_app.command("report")
def dynamic_v3_shadow_aging_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest shadow aging。"),
    ] = False,
    aging_id: Annotated[
        str | None,
        typer.Option("--aging-id", help="aging id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow aging artifact root。"),
    ] = DEFAULT_SHADOW_AGING_DIR,
) -> None:
    """展示 TRADING-139 shadow aging 摘要。"""
    payload = shadow_aging_report_payload(
        aging_id=aging_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping(payload.get("promotion_clock_v2_summary"))
    typer.echo(f"aging_id={payload['aging_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"eligible_for_review_count={summary.get('eligible_for_review_count')}")
    typer.echo(f"downgrade_recommended_count={summary.get('downgrade_recommended_count')}")
    typer.echo(f"report_path={payload['shadow_aging_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-shadow-aging")
def dynamic_v3_validate_shadow_aging_command(
    aging_id: Annotated[
        str,
        typer.Option("--aging-id", help="aging id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="shadow aging artifact root。"),
    ] = DEFAULT_SHADOW_AGING_DIR,
) -> None:
    """校验 TRADING-139 shadow aging artifact。"""
    payload = validate_shadow_aging_artifact(
        aging_id=aging_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_shadow_aging_report_command",
    "dynamic_v3_shadow_aging_run_command",
    "dynamic_v3_validate_shadow_aging_command",
]
