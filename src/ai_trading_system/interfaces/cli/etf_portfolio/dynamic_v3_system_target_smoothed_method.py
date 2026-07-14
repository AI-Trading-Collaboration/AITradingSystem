from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_method as system_target,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    mapping_obj as _mapping_obj,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_smoothed_backfill_app,
    dynamic_v3_smoothed_comparison_app,
    dynamic_v3_smoothed_limited_app,
    dynamic_v3_smoothed_review_app,
)


def _records_obj(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


@dynamic_v3_smoothed_limited_app.command("config-validate")
def dynamic_v3_smoothed_limited_config_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="smoothed limited config。"),
    ] = system_target.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
) -> None:
    """校验 TRADING-246 smoothed limited config。"""
    payload = system_target.validate_smoothed_limited_config(config_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("research_target_only=true")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_limited_app.command("report-config")
def dynamic_v3_smoothed_limited_report_config_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="smoothed limited config。"),
    ] = system_target.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed config artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_CONFIG_DIR,
) -> None:
    """生成 TRADING-246 smoothed limited config manifest/report。"""
    result = system_target.build_smoothed_limited_config_report(
        config_path=config_path,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"config_validation_id={result['config_validation_id']}")
    typer.echo(f"config_dir={result['config_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo("enabled_variants=" + ",".join(manifest["enabled_variants"]))
    typer.echo(f"report_path={manifest['smoothed_limited_config_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-limited-config")
def dynamic_v3_validate_smoothed_limited_config_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="smoothed limited config。"),
    ] = system_target.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
) -> None:
    """校验 TRADING-246 smoothed limited config。"""
    payload = system_target.validate_smoothed_limited_config(config_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_limited_app.command("generate")
def dynamic_v3_smoothed_limited_generate_command(
    target_id: Annotated[str, typer.Option("--target-id", help="model target id。")],
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="smoothed limited config。"),
    ] = system_target.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH,
    regime_context: Annotated[
        str,
        typer.Option("--regime-context", help="current regime context。"),
    ] = "normal",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed limited artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_LIMITED_DIR,
) -> None:
    """生成 TRADING-247 smoothed limited target weights。"""
    result = system_target.generate_smoothed_limited_target(
        target_id=target_id,
        config_path=config_path,
        regime_context=regime_context,
        output_dir=output_dir,
    )
    summary = _mapping_obj(result["weight_jump_reduction_summary"])
    typer.echo(f"smoothed_id={result['smoothed_id']}")
    typer.echo(f"smoothed_dir={result['smoothed_dir']}")
    typer.echo(f"smoothing_event_count={len(result['smoothing_events'])}")
    typer.echo(f"lag_event_count={len(result['lag_events'])}")
    typer.echo(f"jump_reduction_summary={summary.get('target_methods')}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_limited_app.command("report")
def dynamic_v3_smoothed_limited_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest smoothed target。"),
    ] = False,
    smoothed_id: Annotated[
        str | None,
        typer.Option("--smoothed-id", help="smoothed id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed limited artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_LIMITED_DIR,
) -> None:
    """展示 TRADING-247 smoothed limited target 摘要。"""
    payload = system_target.smoothed_limited_report_payload(
        smoothed_id=smoothed_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"smoothed_id={payload['smoothed_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"smoothing_event_count={len(payload['smoothing_events'])}")
    typer.echo(f"lag_event_count={len(payload['lag_events'])}")
    typer.echo(f"report_path={payload['smoothed_limited_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-limited")
def dynamic_v3_validate_smoothed_limited_command(
    smoothed_id: Annotated[str, typer.Option("--smoothed-id", help="smoothed id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed limited artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_LIMITED_DIR,
) -> None:
    """校验 TRADING-247 smoothed limited artifact。"""
    payload = system_target.validate_smoothed_limited_artifact(
        smoothed_id=smoothed_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("not_official_target_weights=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_backfill_app.command("run")
def dynamic_v3_smoothed_backfill_run_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="paper shadow backfill config。"),
    ] = system_target.DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed backfill artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_BACKFILL_DIR,
) -> None:
    """运行 TRADING-248 smoothed paper shadow backfill。"""
    result = system_target.run_smoothed_backfill(config_path=config_path, output_dir=output_dir)
    summary = result["smoothed_backfill_summary"]
    risk_backfill = _mapping_obj(result.get("source_risk_capped_backfill"))
    typer.echo(f"smoothed_backfill_id={result['smoothed_backfill_id']}")
    typer.echo(f"smoothed_backfill_dir={result['smoothed_backfill_dir']}")
    typer.echo(f"paired_risk_capped_backfill_id={risk_backfill.get('backfill_id')}")
    typer.echo(f"date_range={summary['date_start']}..{summary['date_end']}")
    typer.echo(f"smoothing_event_count={summary['smoothing_event_count']}")
    typer.echo(f"lag_event_count={summary['lag_event_count']}")
    typer.echo(f"data_quality={summary['data_quality']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_smoothed_backfill_app.command("report")
def dynamic_v3_smoothed_backfill_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest smoothed backfill。"),
    ] = False,
    backfill_id: Annotated[
        str | None,
        typer.Option("--backfill-id", "--smoothed-backfill-id", help="smoothed backfill id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed backfill artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_BACKFILL_DIR,
) -> None:
    """展示 TRADING-248 smoothed backfill 摘要。"""
    payload = system_target.smoothed_backfill_report_payload(
        backfill_id=backfill_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("smoothed_backfill_summary"))
    typer.echo(f"smoothed_backfill_id={payload['smoothed_backfill_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"smoothing_event_count={summary.get('smoothing_event_count')}")
    typer.echo(f"lag_event_count={summary.get('lag_event_count')}")
    typer.echo(f"data_quality={summary.get('data_quality')}")
    typer.echo(f"report_path={payload['smoothed_backfill_report_path']}")
    typer.echo("broker_action_taken=false")


@dynamic_v3_rescue_app.command("validate-smoothed-backfill")
def dynamic_v3_validate_smoothed_backfill_command(
    backfill_id: Annotated[
        str,
        typer.Option("--backfill-id", "--smoothed-backfill-id", help="smoothed backfill id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed backfill artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_BACKFILL_DIR,
) -> None:
    """校验 TRADING-248 smoothed backfill artifact。"""
    payload = system_target.validate_smoothed_backfill_artifact(
        backfill_id=backfill_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_taken=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_comparison_app.command("run")
def dynamic_v3_smoothed_comparison_run_command(
    smoothed_backfill_id: Annotated[
        str,
        typer.Option("--smoothed-backfill-id", help="smoothed backfill id。"),
    ],
    baseline_backfill_id: Annotated[
        str,
        typer.Option("--baseline-backfill-id", help="baseline paper-shadow backfill id。"),
    ],
    risk_capped_backfill_id: Annotated[
        str,
        typer.Option("--risk-capped-backfill-id", help="risk-capped backfill id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed comparison artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_COMPARISON_DIR,
) -> None:
    """运行 TRADING-249 smoothed comparison。"""
    result = system_target.run_smoothed_comparison(
        smoothed_backfill_id=smoothed_backfill_id,
        baseline_backfill_id=baseline_backfill_id,
        risk_capped_backfill_id=risk_capped_backfill_id,
        output_dir=output_dir,
    )
    metrics = _mapping_obj(result["smoothed_vs_limited_metrics"])
    primary = next(
        (
            row
            for row in _records_obj(metrics.get("comparisons"))
            if row.get("method_a") == "smooth_weights_3d_limited_adjustment"
            and row.get("method_b") == "limited_adjustment"
        ),
        {},
    )
    typer.echo(f"comparison_id={result['comparison_id']}")
    typer.echo(f"comparison_dir={result['comparison_dir']}")
    typer.echo(f"conclusion={primary.get('conclusion')}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_comparison_app.command("report")
def dynamic_v3_smoothed_comparison_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest smoothed comparison。"),
    ] = False,
    comparison_id: Annotated[
        str | None,
        typer.Option("--comparison-id", help="comparison id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed comparison artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_COMPARISON_DIR,
) -> None:
    """展示 TRADING-249 smoothed comparison 摘要。"""
    payload = system_target.smoothed_comparison_report_payload(
        comparison_id=comparison_id,
        latest=latest,
        output_dir=output_dir,
    )
    metrics = _mapping_obj(payload.get("smoothed_vs_limited_metrics"))
    primary = next(
        (
            row
            for row in _records_obj(metrics.get("comparisons"))
            if row.get("method_a") == "smooth_weights_3d_limited_adjustment"
            and row.get("method_b") == "limited_adjustment"
        ),
        {},
    )
    typer.echo(f"comparison_id={payload['comparison_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"return_delta_vs_limited={primary.get('total_return_delta')}")
    typer.echo(f"drawdown_delta_vs_limited={primary.get('max_drawdown_delta')}")
    typer.echo(f"turnover_delta_vs_limited={primary.get('turnover_delta')}")
    typer.echo(f"conclusion={primary.get('conclusion')}")
    typer.echo(f"report_path={payload['smoothed_comparison_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-comparison")
def dynamic_v3_validate_smoothed_comparison_command(
    comparison_id: Annotated[str, typer.Option("--comparison-id", help="comparison id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed comparison artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_COMPARISON_DIR,
) -> None:
    """校验 TRADING-249 smoothed comparison artifact。"""
    payload = system_target.validate_smoothed_comparison_artifact(
        comparison_id=comparison_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_review_app.command("pack")
def dynamic_v3_smoothed_review_pack_command(
    comparison_id: Annotated[str, typer.Option("--comparison-id", help="comparison id。")],
    smoothed_backfill_id: Annotated[
        str,
        typer.Option("--smoothed-backfill-id", help="smoothed backfill id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed review artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_REVIEW_DIR,
) -> None:
    """生成 TRADING-250 smoothed research method review pack。"""
    result = system_target.build_smoothed_review_pack(
        comparison_id=comparison_id,
        smoothed_backfill_id=smoothed_backfill_id,
        output_dir=output_dir,
    )
    decision = result["smoothed_decision"]
    typer.echo(f"review_id={result['review_id']}")
    typer.echo(f"review_dir={result['review_dir']}")
    typer.echo(f"decision={decision['decision']}")
    typer.echo(f"recommended_method={decision['recommended_method']}")
    typer.echo(f"decision_confidence={decision['decision_confidence']}")
    typer.echo("requires_forward_confirmation=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_review_app.command("report")
def dynamic_v3_smoothed_review_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest smoothed review。"),
    ] = False,
    review_id: Annotated[str | None, typer.Option("--review-id", help="review id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed review artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_REVIEW_DIR,
) -> None:
    """展示 TRADING-250 smoothed review 摘要。"""
    payload = system_target.smoothed_review_report_payload(
        review_id=review_id,
        latest=latest,
        output_dir=output_dir,
    )
    decision = _mapping_obj(payload.get("smoothed_decision"))
    typer.echo(f"review_id={payload['review_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"decision={decision.get('decision')}")
    typer.echo(f"recommended_method={decision.get('recommended_method')}")
    typer.echo(f"decision_confidence={decision.get('decision_confidence')}")
    typer.echo("requires_forward_confirmation=true")
    typer.echo(f"report_path={payload['smoothed_review_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-review")
def dynamic_v3_validate_smoothed_review_command(
    review_id: Annotated[str, typer.Option("--review-id", help="review id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed review artifact root。"),
    ] = system_target.DEFAULT_SMOOTHED_REVIEW_DIR,
) -> None:
    """校验 TRADING-250 smoothed review artifact。"""
    payload = system_target.validate_smoothed_review_artifact(
        review_id=review_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("requires_forward_confirmation=true")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
