from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio import dynamic_v3_system_target_history as system_target
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_paper_shadow_backfill_app,
    dynamic_v3_paper_shadow_regime_review_app,
    dynamic_v3_paper_shadow_rolling_eval_app,
    dynamic_v3_paper_shadow_stability_app,
    dynamic_v3_rescue_app,
    dynamic_v3_system_target_selection_review_app,
)


@dynamic_v3_paper_shadow_backfill_app.command("config-validate")
def dynamic_v3_paper_shadow_backfill_config_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper shadow backfill config。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH,
) -> None:
    """校验 TRADING-214 paper shadow historical backfill config。"""
    payload = system_target.validate_paper_shadow_backfill_config(config_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("not_pit_safe=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_paper_shadow_backfill_app.command("run")
def dynamic_v3_paper_shadow_backfill_run_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper shadow backfill config。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper shadow backfill artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
) -> None:
    """运行 TRADING-214 paper shadow historical backfill。"""
    result = system_target.run_paper_shadow_backfill(
        config_path=config_path,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"backfill_id={result['backfill_id']}")
    typer.echo(f"backfill_dir={result['backfill_dir']}")
    typer.echo(f"date_range={manifest['date_start']}..{manifest['date_end']}")
    typer.echo(f"rebalance_count={manifest['rebalance_count']}")
    typer.echo(f"tracked_methods={','.join(manifest['tracked_methods'])}")
    typer.echo(f"data_quality={manifest['data_quality_status']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_paper_shadow_backfill_app.command("report")
def dynamic_v3_paper_shadow_backfill_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest paper shadow backfill。"),
    ] = False,
    backfill_id: Annotated[
        str | None,
        typer.Option("--backfill-id", help="backfill id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper shadow backfill artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
) -> None:
    """展示 TRADING-214 paper shadow backfill report。"""
    payload = system_target.paper_shadow_backfill_report_payload(
        backfill_id=backfill_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"backfill_id={payload['backfill_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"date_range={payload['date_start']}..{payload['date_end']}")
    typer.echo(f"rebalance_count={payload['rebalance_count']}")
    typer.echo(f"tracked_methods={','.join(payload['tracked_methods'])}")
    typer.echo(f"data_quality={payload['data_quality_status']}")
    typer.echo(f"report_path={payload['paper_shadow_backfill_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-paper-shadow-backfill")
def dynamic_v3_validate_paper_shadow_backfill_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="paper shadow backfill artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
) -> None:
    """校验 TRADING-214 paper shadow backfill artifact。"""
    payload = system_target.validate_paper_shadow_backfill_artifact(
        backfill_id=backfill_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_paper_shadow_rolling_eval_app.command("run")
def dynamic_v3_paper_shadow_rolling_eval_run_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rolling eval artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR,
) -> None:
    """运行 TRADING-215 paper shadow rolling target evaluation。"""
    result = system_target.run_paper_shadow_rolling_eval(
        backfill_id=backfill_id,
        output_dir=output_dir,
    )
    stability = result["rolling_rank_stability"]
    methods = _mapping_obj(stability).get("methods") or []
    typer.echo(f"rolling_eval_id={result['rolling_eval_id']}")
    typer.echo(f"rolling_eval_dir={result['rolling_eval_dir']}")
    typer.echo(f"window_count={result['manifest']['window_count']}")
    typer.echo(f"method_count={len(methods)}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_paper_shadow_rolling_eval_app.command("report")
def dynamic_v3_paper_shadow_rolling_eval_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest rolling eval。"),
    ] = False,
    rolling_eval_id: Annotated[
        str | None,
        typer.Option("--rolling-eval-id", help="rolling eval id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rolling eval artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR,
) -> None:
    """展示 TRADING-215 rolling eval report。"""
    payload = system_target.paper_shadow_rolling_eval_report_payload(
        rolling_eval_id=rolling_eval_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"rolling_eval_id={payload['rolling_eval_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"window_count={payload['window_count']}")
    typer.echo(f"report_path={payload['paper_shadow_rolling_eval_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-paper-shadow-rolling-eval")
def dynamic_v3_validate_paper_shadow_rolling_eval_command(
    rolling_eval_id: Annotated[
        str,
        typer.Option("--rolling-eval-id", help="rolling eval id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="rolling eval artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR,
) -> None:
    """校验 TRADING-215 paper shadow rolling eval artifact。"""
    payload = system_target.validate_paper_shadow_rolling_eval_artifact(
        rolling_eval_id=rolling_eval_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_paper_shadow_regime_review_app.command("run")
def dynamic_v3_paper_shadow_regime_review_run_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="regime review artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR,
) -> None:
    """运行 TRADING-216 paper shadow regime performance review。"""
    result = system_target.run_paper_shadow_regime_review(
        backfill_id=backfill_id,
        output_dir=output_dir,
    )
    summary = result["regime_method_summary"]
    typer.echo(f"regime_review_id={result['regime_review_id']}")
    typer.echo(f"regime_review_dir={result['regime_review_dir']}")
    typer.echo(
        f"defensive_limited_adjustment_status={summary['defensive_limited_adjustment_status']}"
    )
    typer.echo("broker_action_allowed=false")


@dynamic_v3_paper_shadow_regime_review_app.command("report")
def dynamic_v3_paper_shadow_regime_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest regime review。"),
    ] = False,
    regime_review_id: Annotated[
        str | None,
        typer.Option("--regime-review-id", help="regime review id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="regime review artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR,
) -> None:
    """展示 TRADING-216 regime review report。"""
    payload = system_target.paper_shadow_regime_review_report_payload(
        regime_review_id=regime_review_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("regime_method_summary"))
    typer.echo(f"regime_review_id={payload['regime_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(
        "defensive_limited_adjustment_status="
        + str(summary.get("defensive_limited_adjustment_status"))
    )
    typer.echo(f"report_path={payload['paper_shadow_regime_review_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-paper-shadow-regime-review")
def dynamic_v3_validate_paper_shadow_regime_review_command(
    regime_review_id: Annotated[
        str,
        typer.Option("--regime-review-id", help="regime review id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="regime review artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR,
) -> None:
    """校验 TRADING-216 paper shadow regime review artifact。"""
    payload = system_target.validate_paper_shadow_regime_review_artifact(
        regime_review_id=regime_review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_paper_shadow_stability_app.command("run")
def dynamic_v3_paper_shadow_stability_run_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="stability artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_STABILITY_DIR,
) -> None:
    """运行 TRADING-217 target method stability and turnover diagnostics。"""
    result = system_target.run_paper_shadow_stability(
        backfill_id=backfill_id,
        output_dir=output_dir,
    )
    typer.echo(f"stability_id={result['stability_id']}")
    typer.echo(f"stability_dir={result['stability_dir']}")
    typer.echo(f"jump_event_count={len(result['weight_path_jump_events'])}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_paper_shadow_stability_app.command("report")
def dynamic_v3_paper_shadow_stability_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest stability diagnostics。"),
    ] = False,
    stability_id: Annotated[
        str | None,
        typer.Option("--stability-id", help="stability id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="stability artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_STABILITY_DIR,
) -> None:
    """展示 TRADING-217 stability diagnostics report。"""
    payload = system_target.paper_shadow_stability_report_payload(
        stability_id=stability_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"stability_id={payload['stability_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"jump_event_count={len(payload['weight_path_jump_events'])}")
    typer.echo(f"report_path={payload['paper_shadow_stability_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-paper-shadow-stability")
def dynamic_v3_validate_paper_shadow_stability_command(
    stability_id: Annotated[str, typer.Option("--stability-id", help="stability id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="stability artifact root。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_STABILITY_DIR,
) -> None:
    """校验 TRADING-217 paper shadow stability artifact。"""
    payload = system_target.validate_paper_shadow_stability_artifact(
        stability_id=stability_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_system_target_selection_review_app.command("run")
def dynamic_v3_system_target_selection_review_run_command(
    backfill_id: Annotated[str, typer.Option("--backfill-id", help="backfill id。")],
    rolling_eval_id: Annotated[
        str,
        typer.Option("--rolling-eval-id", help="rolling eval id。"),
    ],
    regime_review_id: Annotated[
        str,
        typer.Option("--regime-review-id", help="regime review id。"),
    ],
    stability_id: Annotated[str, typer.Option("--stability-id", help="stability id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="selection review artifact root。"),
    ] = system_target.DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR,
) -> None:
    """运行 TRADING-218 system target method selection review。"""
    result = system_target.run_system_target_selection_review(
        backfill_id=backfill_id,
        rolling_eval_id=rolling_eval_id,
        regime_review_id=regime_review_id,
        stability_id=stability_id,
        output_dir=output_dir,
    )
    decision = result["selection_decision"]
    typer.echo(f"selection_review_id={result['selection_review_id']}")
    typer.echo(f"selection_review_dir={result['selection_review_dir']}")
    typer.echo(f"recommended_research_method={decision['recommended_research_method']}")
    typer.echo(f"decision_status={decision['decision_status']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_system_target_selection_review_app.command("report")
def dynamic_v3_system_target_selection_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest selection review。"),
    ] = False,
    selection_review_id: Annotated[
        str | None,
        typer.Option("--selection-review-id", help="selection review id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="selection review artifact root。"),
    ] = system_target.DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR,
) -> None:
    """展示 TRADING-218 system target selection review report。"""
    payload = system_target.system_target_selection_review_report_payload(
        selection_review_id=selection_review_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping_obj(payload.get("selection_decision"))
    typer.echo(f"selection_review_id={payload['selection_review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommended_research_method={decision.get('recommended_research_method')}")
    typer.echo(f"decision_status={decision.get('decision_status')}")
    typer.echo(f"report_path={payload['system_target_selection_review_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-system-target-selection-review")
def dynamic_v3_validate_system_target_selection_review_command(
    selection_review_id: Annotated[
        str,
        typer.Option("--selection-review-id", help="selection review id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="selection review artifact root。"),
    ] = system_target.DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR,
) -> None:
    """校验 TRADING-218 system target selection review artifact。"""
    payload = system_target.validate_system_target_selection_review_artifact(
        selection_review_id=selection_review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
