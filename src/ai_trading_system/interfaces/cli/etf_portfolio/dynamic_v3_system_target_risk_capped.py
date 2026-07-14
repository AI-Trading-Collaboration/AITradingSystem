from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_risk_capped as risk_capped,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_risk_capped_backfill_app,
    dynamic_v3_risk_capped_comparison_app,
    dynamic_v3_risk_capped_limited_app,
    dynamic_v3_risk_capped_review_app,
)


@dynamic_v3_risk_capped_limited_app.command("config-validate")
def dynamic_v3_risk_capped_limited_config_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="risk-capped config。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH,
) -> None:
    """校验 TRADING-229 risk-capped limited config。"""
    payload = risk_capped.validate_risk_capped_limited_config(config_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("research_target_only=true")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_risk_capped_limited_app.command("report-config")
def dynamic_v3_risk_capped_limited_report_config_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="risk-capped config。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="risk-capped config artifact root。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_CONFIG_DIR,
) -> None:
    """生成 TRADING-229 risk-capped config manifest/report。"""
    result = risk_capped.build_risk_capped_limited_config_report(
        config_path=config_path,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"config_validation_id={result['config_validation_id']}")
    typer.echo(f"config_dir={result['config_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"report_path={manifest['risk_capped_config_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-risk-capped-limited-config")
def dynamic_v3_validate_risk_capped_limited_config_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="risk-capped config。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH,
) -> None:
    """校验 TRADING-229 risk-capped limited config。"""
    payload = risk_capped.validate_risk_capped_limited_config(config_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_risk_capped_limited_app.command("generate")
def dynamic_v3_risk_capped_limited_generate_command(
    target_id: Annotated[str, typer.Option("--target-id", help="model target id。")],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="risk-capped config。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_LIMITED_CONFIG_PATH,
    regime_context: Annotated[
        str,
        typer.Option("--regime-context", help="current regime context。"),
    ] = "normal",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="risk-capped limited artifact root。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_LIMITED_DIR,
) -> None:
    """生成 TRADING-230 risk-capped limited target weights。"""
    result = risk_capped.generate_risk_capped_limited_target(
        target_id=target_id,
        config_path=config_path,
        regime_context=regime_context,
        output_dir=output_dir,
    )
    summary = result["cap_reason_summary"]
    typer.echo(f"risk_capped_id={result['risk_capped_id']}")
    typer.echo(f"risk_capped_dir={result['risk_capped_dir']}")
    typer.echo(f"cap_status={summary['cap_status']}")
    typer.echo(f"cap_event_count={summary['total_cap_events']}")
    typer.echo(f"total_reallocated_to_cash={summary['total_reallocated_to_cash']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_risk_capped_limited_app.command("report")
def dynamic_v3_risk_capped_limited_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest risk-capped target。"),
    ] = False,
    risk_capped_id: Annotated[
        str | None,
        typer.Option("--risk-capped-id", help="risk-capped id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="risk-capped limited artifact root。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_LIMITED_DIR,
) -> None:
    """展示 TRADING-230 risk-capped limited target 摘要。"""
    payload = risk_capped.risk_capped_limited_report_payload(
        risk_capped_id=risk_capped_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("cap_reason_summary"))
    typer.echo(f"risk_capped_id={payload['risk_capped_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"cap_event_count={summary.get('total_cap_events')}")
    typer.echo(f"report_path={payload['risk_capped_limited_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-risk-capped-limited")
def dynamic_v3_validate_risk_capped_limited_command(
    risk_capped_id: Annotated[
        str,
        typer.Option("--risk-capped-id", help="risk-capped id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="risk-capped limited artifact root。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_LIMITED_DIR,
) -> None:
    """校验 TRADING-230 risk-capped limited artifact。"""
    payload = risk_capped.validate_risk_capped_limited_artifact(
        risk_capped_id=risk_capped_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_risk_capped_backfill_app.command("run")
def dynamic_v3_risk_capped_backfill_run_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper shadow backfill config。"),
    ] = risk_capped.DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="risk-capped backfill artifact root。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_BACKFILL_DIR,
) -> None:
    """运行 TRADING-231 risk-capped paper shadow backfill。"""
    result = risk_capped.run_risk_capped_backfill(
        config_path=config_path, output_dir=output_dir
    )
    summary = result["risk_capped_backfill_summary"]
    typer.echo(f"risk_capped_backfill_id={result['risk_capped_backfill_id']}")
    typer.echo(f"risk_capped_backfill_dir={result['risk_capped_backfill_dir']}")
    typer.echo(f"date_range={summary['date_start']}..{summary['date_end']}")
    typer.echo(f"cap_event_count={summary['cap_event_count']}")
    typer.echo(f"data_quality={summary['data_quality']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_risk_capped_backfill_app.command("report")
def dynamic_v3_risk_capped_backfill_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest risk-capped backfill。"),
    ] = False,
    backfill_id: Annotated[
        str | None,
        typer.Option(
            "--backfill-id",
            "--risk-capped-backfill-id",
            help="risk-capped backfill id。",
        ),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="risk-capped backfill artifact root。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_BACKFILL_DIR,
) -> None:
    """展示 TRADING-231 risk-capped backfill 摘要。"""
    payload = risk_capped.risk_capped_backfill_report_payload(
        backfill_id=backfill_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("risk_capped_backfill_summary"))
    typer.echo(f"risk_capped_backfill_id={payload['risk_capped_backfill_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"cap_event_count={summary.get('cap_event_count')}")
    typer.echo(f"avg_semiconductor_weight={summary.get('avg_semiconductor_weight')}")
    typer.echo(f"data_quality={summary.get('data_quality')}")
    typer.echo(f"report_path={payload['risk_capped_backfill_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-risk-capped-backfill")
def dynamic_v3_validate_risk_capped_backfill_command(
    backfill_id: Annotated[
        str,
        typer.Option(
            "--backfill-id",
            "--risk-capped-backfill-id",
            help="risk-capped backfill id。",
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="risk-capped backfill artifact root。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_BACKFILL_DIR,
) -> None:
    """校验 TRADING-231 risk-capped backfill artifact。"""
    payload = risk_capped.validate_risk_capped_backfill_artifact(
        backfill_id=backfill_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_risk_capped_comparison_app.command("run")
def dynamic_v3_risk_capped_comparison_run_command(
    risk_capped_backfill_id: Annotated[
        str,
        typer.Option("--risk-capped-backfill-id", help="risk-capped backfill id。"),
    ],
    baseline_backfill_id: Annotated[
        str,
        typer.Option("--baseline-backfill-id", help="baseline paper-shadow backfill id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="risk-capped comparison artifact root。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_COMPARISON_DIR,
) -> None:
    """运行 TRADING-232 risk-capped vs limited comparison。"""
    result = risk_capped.run_risk_capped_comparison(
        risk_capped_backfill_id=risk_capped_backfill_id,
        baseline_backfill_id=baseline_backfill_id,
        output_dir=output_dir,
    )
    metrics = result["risk_capped_vs_limited_metrics"]
    typer.echo(f"comparison_id={result['comparison_id']}")
    typer.echo(f"comparison_dir={result['comparison_dir']}")
    typer.echo(f"conclusion={metrics['conclusion']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_risk_capped_comparison_app.command("report")
def dynamic_v3_risk_capped_comparison_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest risk-capped comparison。"),
    ] = False,
    comparison_id: Annotated[
        str | None,
        typer.Option("--comparison-id", help="comparison id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="risk-capped comparison artifact root。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_COMPARISON_DIR,
) -> None:
    """展示 TRADING-232 risk-capped comparison 摘要。"""
    payload = risk_capped.risk_capped_comparison_report_payload(
        comparison_id=comparison_id,
        latest=latest,
        output_dir=output_dir,
    )
    metrics = _mapping_obj(payload.get("risk_capped_vs_limited_metrics"))
    values = _mapping_obj(metrics.get("metrics"))
    typer.echo(f"comparison_id={payload['comparison_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"return_delta_vs_limited={values.get('total_return_delta')}")
    typer.echo(f"drawdown_delta_vs_limited={values.get('max_drawdown_delta')}")
    typer.echo(f"semiconductor_exposure_delta={values.get('avg_semiconductor_weight_delta')}")
    typer.echo(f"conclusion={metrics.get('conclusion')}")
    typer.echo(f"report_path={payload['risk_capped_comparison_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-risk-capped-comparison")
def dynamic_v3_validate_risk_capped_comparison_command(
    comparison_id: Annotated[str, typer.Option("--comparison-id", help="comparison id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="risk-capped comparison artifact root。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_COMPARISON_DIR,
) -> None:
    """校验 TRADING-232 risk-capped comparison artifact。"""
    payload = risk_capped.validate_risk_capped_comparison_artifact(
        comparison_id=comparison_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_risk_capped_review_app.command("pack")
def dynamic_v3_risk_capped_review_pack_command(
    comparison_id: Annotated[str, typer.Option("--comparison-id", help="comparison id。")],
    risk_capped_backfill_id: Annotated[
        str,
        typer.Option("--risk-capped-backfill-id", help="risk-capped backfill id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="risk-capped review artifact root。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_REVIEW_DIR,
) -> None:
    """生成 TRADING-233 risk-capped research method review pack。"""
    result = risk_capped.build_risk_capped_review_pack(
        comparison_id=comparison_id,
        risk_capped_backfill_id=risk_capped_backfill_id,
        output_dir=output_dir,
    )
    decision = result["risk_capped_decision"]
    typer.echo(f"review_id={result['review_id']}")
    typer.echo(f"review_dir={result['review_dir']}")
    typer.echo(f"decision={decision['decision']}")
    typer.echo(f"decision_confidence={decision['decision_confidence']}")
    typer.echo("requires_forward_confirmation=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_risk_capped_review_app.command("report")
def dynamic_v3_risk_capped_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest risk-capped review。"),
    ] = False,
    review_id: Annotated[str | None, typer.Option("--review-id", help="review id。")]=None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="risk-capped review artifact root。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_REVIEW_DIR,
) -> None:
    """展示 TRADING-233 risk-capped review 摘要。"""
    payload = risk_capped.risk_capped_review_report_payload(
        review_id=review_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping_obj(payload.get("risk_capped_decision"))
    typer.echo(f"review_id={payload['review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo(f"decision_confidence={decision.get('decision_confidence')}")
    typer.echo("requires_forward_confirmation=true")
    typer.echo(f"report_path={payload['risk_capped_review_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-risk-capped-review")
def dynamic_v3_validate_risk_capped_review_command(
    review_id: Annotated[str, typer.Option("--review-id", help="review id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="risk-capped review artifact root。"),
    ] = risk_capped.DEFAULT_RISK_CAPPED_REVIEW_DIR,
) -> None:
    """校验 TRADING-233 risk-capped review artifact。"""
    payload = risk_capped.validate_risk_capped_review_artifact(
        review_id=review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("requires_forward_confirmation=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
