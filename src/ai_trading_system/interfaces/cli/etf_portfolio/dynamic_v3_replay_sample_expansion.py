from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_REPLAY_INVENTORY_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_REPLAY_SAMPLE_EXPANSION_DIR,
    DEFAULT_REPLAY_SAMPLE_EXPANSION_POLICY_PATH,
    replay_sample_expansion_report_payload,
    run_replay_sample_expansion,
    validate_replay_sample_expansion_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_RATES_CACHE_PATH,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_replay_sample_expansion_app,
    dynamic_v3_rescue_app,
)


def _parse_expansion_date(value: str, option_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must use YYYY-MM-DD") from exc


@dynamic_v3_replay_sample_expansion_app.command("run")
def dynamic_v3_replay_sample_expansion_run_command(
    start: Annotated[str, typer.Option("--start", help="AI regime replay scan start。")],
    end: Annotated[str, typer.Option("--end", help="replay scan end。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="replay sample expansion artifact root。")
    ] = DEFAULT_REPLAY_SAMPLE_EXPANSION_DIR,
    daily_advisory_dir: Annotated[
        Path, typer.Option("--daily-advisory-dir", help="daily advisory artifact root。")
    ] = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    owner_review_dir: Annotated[
        Path, typer.Option("--owner-review-dir", help="owner review journal root。")
    ] = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    replay_inventory_dir: Annotated[
        Path, typer.Option("--replay-inventory-dir", help="replay inventory artifact root。")
    ] = DEFAULT_REPLAY_INVENTORY_DIR,
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="cached ETF price path。")
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="cached rates path。")
    ] = DEFAULT_RATES_CACHE_PATH,
    policy_path: Annotated[
        Path, typer.Option("--policy-path", help="reviewed replay expansion policy。")
    ] = DEFAULT_REPLAY_SAMPLE_EXPANSION_POLICY_PATH,
) -> None:
    """扩展 validated historical replay candidate 样本。"""
    result = run_replay_sample_expansion(
        start=_parse_expansion_date(start, "--start"),
        end=_parse_expansion_date(end, "--end"),
        output_dir=output_dir,
        daily_advisory_dir=daily_advisory_dir,
        owner_review_dir=owner_review_dir,
        replay_inventory_dir=replay_inventory_dir,
        prices_path=prices_path,
        rates_path=rates_path,
        policy_path=policy_path,
    )
    summary = result["pit_classification_summary"]
    typer.echo(f"expansion_id={result['expansion_id']}")
    typer.echo(f"expansion_dir={result['expansion_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"new_replay_events={summary['total_expanded_events']}")
    typer.echo(f"pit_safe_count={summary['pit_safe_count']}")
    typer.echo(f"pit_warning_count={summary['pit_warning_count']}")
    typer.echo(f"pit_unsafe_count={summary['pit_unsafe_count']}")
    typer.echo("pit_unsafe_allowed_in_default_replay=false")
    typer.echo("production_effect=none")


@dynamic_v3_replay_sample_expansion_app.command("report")
def dynamic_v3_replay_sample_expansion_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest replay sample expansion。")
    ] = False,
    expansion_id: Annotated[
        str | None, typer.Option("--expansion-id", help="expansion id。")
    ] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="replay sample expansion artifact root。")
    ] = DEFAULT_REPLAY_SAMPLE_EXPANSION_DIR,
) -> None:
    """展示 replay sample expansion 摘要。"""
    payload = replay_sample_expansion_report_payload(
        expansion_id=expansion_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = payload["pit_classification_summary"]
    typer.echo(f"expansion_id={payload['expansion_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"new_replay_events={summary['total_expanded_events']}")
    typer.echo(f"pit_safe_count={summary['pit_safe_count']}")
    typer.echo(f"pit_warning_count={summary['pit_warning_count']}")
    typer.echo(f"pit_unsafe_count={summary['pit_unsafe_count']}")
    typer.echo(f"report_path={payload['replay_sample_expansion_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-replay-sample-expansion")
def dynamic_v3_validate_replay_sample_expansion_command(
    expansion_id: Annotated[str, typer.Option("--expansion-id", help="expansion id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="replay sample expansion artifact root。")
    ] = DEFAULT_REPLAY_SAMPLE_EXPANSION_DIR,
) -> None:
    """校验 TRADING-152 replay sample expansion artifact。"""
    payload = validate_replay_sample_expansion_artifact(
        expansion_id=expansion_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_replay_sample_expansion_run_command",
    "dynamic_v3_replay_sample_expansion_report_command",
    "dynamic_v3_validate_replay_sample_expansion_command",
]
