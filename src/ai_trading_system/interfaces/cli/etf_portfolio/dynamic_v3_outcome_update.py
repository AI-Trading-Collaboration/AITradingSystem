from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_OUTCOME_UPDATE_DIR,
    DEFAULT_OUTCOME_UPDATE_REVIEW_DIR,
    outcome_update_report_payload,
    run_outcome_update,
    validate_outcome_update_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
    DEFAULT_PAPER_PORTFOLIO_DIR,
    DEFAULT_RATES_CACHE_PATH,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_outcome_update_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_outcome_update_app.command("run")
def dynamic_v3_outcome_update_run_command(
    update_review_id: Annotated[
        str, typer.Option("--update-review-id", help="outcome update review id。")
    ],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="outcome update artifact root。")
    ] = DEFAULT_OUTCOME_UPDATE_DIR,
    review_dir: Annotated[
        Path, typer.Option("--review-dir", help="outcome update review artifact root。")
    ] = DEFAULT_OUTCOME_UPDATE_REVIEW_DIR,
    advisory_outcome_dir: Annotated[
        Path, typer.Option("--advisory-outcome-dir", help="advisory outcome artifact root。")
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
    paper_portfolio_dir: Annotated[
        Path, typer.Option("--paper-portfolio-dir", help="paper portfolio artifact root。")
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="cached ETF price path。")
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="cached rates path。")
    ] = DEFAULT_RATES_CACHE_PATH,
) -> None:
    """事务化执行通过 review 的 safe outcome update。"""
    result = run_outcome_update(
        update_review_id=update_review_id,
        output_dir=output_dir,
        review_dir=review_dir,
        advisory_outcome_dir=advisory_outcome_dir,
        paper_portfolio_dir=paper_portfolio_dir,
        prices_path=prices_path,
        rates_path=rates_path,
    )
    delta = result["outcome_status_delta"]
    typer.echo(f"outcome_update_id={result['outcome_update_id']}")
    typer.echo(f"outcome_update_dir={result['outcome_update_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"transaction_status={result['transaction']['status']}")
    typer.echo(f"updated_count={delta['updated_count']}")
    typer.echo(f"skipped_count={delta['skipped_count']}")
    typer.echo(f"forward_available_before={delta['before']['forward_available']}")
    typer.echo(f"forward_available_after={delta['after']['forward_available']}")
    typer.echo(f"forward_pending_before={delta['before']['forward_pending']}")
    typer.echo(f"forward_pending_after={delta['after']['forward_pending']}")
    typer.echo("future_data_used_in_decision=false")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")


@dynamic_v3_outcome_update_app.command("report")
def dynamic_v3_outcome_update_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest outcome update。")
    ] = False,
    update_id: Annotated[str | None, typer.Option("--update-id", help="update id。")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="outcome update artifact root。")
    ] = DEFAULT_OUTCOME_UPDATE_DIR,
) -> None:
    """展示 safe outcome update 摘要。"""
    payload = outcome_update_report_payload(
        update_id=update_id,
        latest=latest,
        output_dir=output_dir,
    )
    delta = payload["outcome_status_delta"]
    typer.echo(f"outcome_update_id={payload['outcome_update_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"transaction_status={payload['transaction']['status']}")
    typer.echo(f"updated_count={delta['updated_count']}")
    typer.echo(f"skipped_count={delta['skipped_count']}")
    typer.echo(f"forward_available_before={delta['before']['forward_available']}")
    typer.echo(f"forward_available_after={delta['after']['forward_available']}")
    typer.echo(f"report_path={payload['outcome_update_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-outcome-update")
def dynamic_v3_validate_outcome_update_command(
    update_id: Annotated[str, typer.Option("--update-id", help="outcome update id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="outcome update artifact root。")
    ] = DEFAULT_OUTCOME_UPDATE_DIR,
) -> None:
    """校验 TRADING-157 transactional outcome update artifact。"""
    payload = validate_outcome_update_artifact(update_id=update_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_outcome_update_run_command",
    "dynamic_v3_outcome_update_report_command",
    "dynamic_v3_validate_outcome_update_command",
]
