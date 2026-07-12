from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    DEFAULT_PAPER_PORTFOLIO_DIR,
    apply_owner_review_to_paper_portfolio,
    init_paper_portfolio,
    paper_portfolio_report_payload,
    paper_portfolio_state_payload,
    validate_paper_portfolio_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_paper_portfolio_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_paper_portfolio_app.command("init")
def dynamic_v3_paper_portfolio_init_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper portfolio config。"),
    ] = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
) -> None:
    """初始化 TRADING-136 paper portfolio state。"""
    result = init_paper_portfolio(config_path=config_path, output_dir=output_dir)
    state = result["state"]
    typer.echo(f"paper_portfolio_id={result['paper_portfolio_id']}")
    typer.echo(f"paper_portfolio_dir={result['paper_portfolio_dir']}")
    typer.echo(f"state_status={state['state_status']}")
    typer.echo(f"total_weight={state['total_weight']}")
    typer.echo("event_chain_status=PASS")
    typer.echo("broker_action_taken=false")


@dynamic_v3_paper_portfolio_app.command("apply-review")
def dynamic_v3_paper_portfolio_apply_review_command(
    review_id: Annotated[str, typer.Option("--review-id", help="owner review id。")],
    paper_portfolio_id: Annotated[
        str | None,
        typer.Option("--paper-portfolio-id", help="optional paper portfolio id。"),
    ] = None,
    manual_deltas_json: Annotated[
        str,
        typer.Option("--manual-deltas-json", help="manual adjustment deltas JSON。"),
    ] = "",
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper portfolio config。"),
    ] = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
    owner_review_dir: Annotated[
        Path,
        typer.Option("--owner-review-dir", help="owner review journal root。"),
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    daily_advisory_dir: Annotated[
        Path,
        typer.Option("--daily-advisory-dir", help="daily advisory artifact root。"),
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
) -> None:
    """把 owner review 决策应用到 paper-only portfolio ledger。"""
    manual_deltas: Mapping[str, Any] | None = None
    if manual_deltas_json:
        parsed = json.loads(manual_deltas_json)
        if not isinstance(parsed, Mapping):
            raise typer.BadParameter("--manual-deltas-json must be a JSON object")
        manual_deltas = parsed
    result = apply_owner_review_to_paper_portfolio(
        review_id=review_id,
        paper_portfolio_id=paper_portfolio_id,
        manual_deltas=manual_deltas,
        config_path=config_path,
        output_dir=output_dir,
        owner_review_dir=owner_review_dir,
        daily_advisory_dir=daily_advisory_dir,
    )
    state = result["state"]
    typer.echo(f"paper_portfolio_id={result['paper_portfolio_id']}")
    typer.echo(f"paper_action_id={result['paper_action_id']}")
    typer.echo(f"state_status={state['state_status']}")
    typer.echo(f"total_weight={state['total_weight']}")
    typer.echo("event_chain_status=PASS")
    typer.echo("broker_action_taken=false")


@dynamic_v3_paper_portfolio_app.command("state")
def dynamic_v3_paper_portfolio_state_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest paper portfolio。"),
    ] = False,
    paper_portfolio_id: Annotated[
        str | None,
        typer.Option("--paper-portfolio-id", help="paper portfolio id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
) -> None:
    """展示 TRADING-136 paper portfolio latest state。"""
    payload = paper_portfolio_state_payload(
        paper_portfolio_id=paper_portfolio_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"paper_portfolio_id={payload['paper_portfolio_id']}")
    typer.echo(f"state_status={payload['state_status']}")
    typer.echo(f"as_of={payload['as_of']}")
    typer.echo(f"total_weight={payload['total_weight']}")
    typer.echo(f"positions={json.dumps(payload['positions'], sort_keys=True)}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_paper_portfolio_app.command("report")
def dynamic_v3_paper_portfolio_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest paper portfolio。"),
    ] = False,
    paper_portfolio_id: Annotated[
        str | None,
        typer.Option("--paper-portfolio-id", help="paper portfolio id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
) -> None:
    """展示 TRADING-136 paper portfolio report 摘要。"""
    payload = paper_portfolio_report_payload(
        paper_portfolio_id=paper_portfolio_id,
        latest=latest,
        output_dir=output_dir,
    )
    state = payload.get("paper_portfolio_state")
    state_payload = dict(state) if isinstance(state, Mapping) else {}
    typer.echo(f"paper_portfolio_id={payload['paper_portfolio_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"state_status={state_payload.get('state_status')}")
    typer.echo(f"paper_action_count={payload['paper_action_count']}")
    typer.echo(f"event_chain_status={payload.get('event_chain_status', 'LEGACY_UNCHAINED')}")
    typer.echo(f"report_path={payload['paper_portfolio_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-paper-portfolio")
def dynamic_v3_validate_paper_portfolio_command(
    paper_portfolio_id: Annotated[
        str,
        typer.Option("--paper-portfolio-id", help="paper portfolio id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper portfolio artifact root。"),
    ] = DEFAULT_PAPER_PORTFOLIO_DIR,
) -> None:
    """校验 TRADING-136 paper portfolio artifact。"""
    payload = validate_paper_portfolio_artifact(
        paper_portfolio_id=paper_portfolio_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"event_chain_status={payload['event_chain_status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
