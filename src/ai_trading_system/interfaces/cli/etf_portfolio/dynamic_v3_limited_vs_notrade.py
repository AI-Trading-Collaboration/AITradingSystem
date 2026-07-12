from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_BACKFILL_REPAIR_DIR,
    DEFAULT_BACKFILLED_OUTCOME_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_LIMITED_VS_NOTRADE_DIR,
    DEFAULT_LIMITED_VS_NOTRADE_POLICY_PATH,
    limited_vs_notrade_report_payload,
    run_limited_vs_notrade_evaluation,
    validate_limited_vs_notrade_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_limited_vs_notrade_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_limited_vs_notrade_app.command("run")
def dynamic_v3_limited_vs_notrade_run_command(
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="limited-vs-notrade artifact root。")
    ] = DEFAULT_LIMITED_VS_NOTRADE_DIR,
    advisory_outcome_dir: Annotated[
        Path, typer.Option("--advisory-outcome-dir", help="advisory outcome artifact root。")
    ] = DEFAULT_ADVISORY_OUTCOME_DIR,
    backfill_dir: Annotated[
        Path, typer.Option("--backfill-dir", help="backfilled outcome artifact root。")
    ] = DEFAULT_BACKFILLED_OUTCOME_DIR,
    repair_dir: Annotated[
        Path, typer.Option("--repair-dir", help="backfill repair artifact root。")
    ] = DEFAULT_BACKFILL_REPAIR_DIR,
    policy_path: Annotated[
        Path, typer.Option("--policy-path", help="reviewed paired-comparison policy。")
    ] = DEFAULT_LIMITED_VS_NOTRADE_POLICY_PATH,
) -> None:
    """运行 validated limited_adjustment vs no_trade 专项评估。"""
    result = run_limited_vs_notrade_evaluation(
        output_dir=output_dir,
        advisory_outcome_dir=advisory_outcome_dir,
        backfill_dir=backfill_dir,
        repair_dir=repair_dir,
        policy_path=policy_path,
    )
    comparison = result["window_comparison_metrics"]
    first = comparison["by_window"][0]
    typer.echo(f"focus_id={result['focus_id']}")
    typer.echo(f"focus_dir={result['focus_dir']}")
    typer.echo(f"status={result['manifest']['status']}")
    typer.echo(f"available_count={result['manifest']['available_count']}")
    typer.echo(f"win_rate={first['win_rate']}")
    typer.echo(f"avg_relative_return={first['avg_relative_return']}")
    typer.echo(f"confidence={first['confidence']}")
    typer.echo(f"recommendation={comparison['overall_recommendation']}")
    typer.echo("auto_policy_apply=false")
    typer.echo("production_effect=none")


@dynamic_v3_limited_vs_notrade_app.command("report")
def dynamic_v3_limited_vs_notrade_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest limited-vs-notrade artifact。")
    ] = False,
    focus_id: Annotated[str | None, typer.Option("--focus-id", help="focus id。")]=None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="limited-vs-notrade artifact root。")
    ] = DEFAULT_LIMITED_VS_NOTRADE_DIR,
) -> None:
    """展示 limited_adjustment vs no_trade 摘要。"""
    payload = limited_vs_notrade_report_payload(
        focus_id=focus_id, latest=latest, output_dir=output_dir
    )
    comparison = payload["window_comparison_metrics"]
    first = comparison["by_window"][0]
    typer.echo(f"focus_id={payload['focus_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"available_count={payload['available_count']}")
    typer.echo(f"win_rate={first['win_rate']}")
    typer.echo(f"avg_relative_return={first['avg_relative_return']}")
    typer.echo(f"confidence={first['confidence']}")
    typer.echo(f"recommendation={comparison['overall_recommendation']}")
    typer.echo(f"report_path={payload['limited_vs_notrade_report_path']}")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-limited-vs-notrade")
def dynamic_v3_validate_limited_vs_notrade_command(
    focus_id: Annotated[str, typer.Option("--focus-id", help="focus id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="limited-vs-notrade artifact root。")
    ] = DEFAULT_LIMITED_VS_NOTRADE_DIR,
) -> None:
    """校验 TRADING-154 limited-vs-notrade artifact。"""
    payload = validate_limited_vs_notrade_artifact(focus_id=focus_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("auto_policy_apply=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_limited_vs_notrade_run_command",
    "dynamic_v3_limited_vs_notrade_report_command",
    "dynamic_v3_validate_limited_vs_notrade_command",
]
