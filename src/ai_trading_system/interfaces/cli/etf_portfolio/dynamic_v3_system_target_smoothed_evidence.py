from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_evidence as smoothed_evidence,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_smoothed_confirmation_app,
    dynamic_v3_smoothed_regime_validation_app,
    dynamic_v3_smoothed_review_attribution_app,
    dynamic_v3_smoothed_watch_pack_app,
    dynamic_v3_smoothing_benefit_lag_app,
)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


def _status_summary(rows: Any, field: str) -> str:
    return ",".join(
        f"{row.get('method')}:{row.get(field)}" for row in _records(rows)
    )


@dynamic_v3_smoothed_review_attribution_app.command("run")
def dynamic_v3_smoothed_review_attribution_run_command(
    review_id: Annotated[str, typer.Option("--review-id", help="smoothed review id。")],
    comparison_id: Annotated[str, typer.Option("--comparison-id", help="comparison id。")],
    backfill_id: Annotated[
        str,
        typer.Option("--backfill-id", "--smoothed-backfill-id", help="smoothed backfill id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed review attribution artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR,
) -> None:
    """运行 TRADING-251 smoothed review reason attribution。"""
    result = smoothed_evidence.run_smoothed_review_attribution(
        review_id=review_id,
        comparison_id=comparison_id,
        backfill_id=backfill_id,
        output_dir=output_dir,
    )
    breakdown = result["smoothed_decision_reason_breakdown"]
    typer.echo(f"attribution_id={result['attribution_id']}")
    typer.echo(f"attribution_dir={result['attribution_dir']}")
    typer.echo(f"decision={breakdown['decision']}")
    typer.echo(f"confidence={breakdown['confidence']}")
    typer.echo(f"recommended_method={breakdown['recommended_method']}")
    typer.echo(f"supporting_reason_count={len(breakdown['supporting_reasons'])}")
    typer.echo(f"blocking_reason_count={len(breakdown['blocking_reasons'])}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_review_attribution_app.command("report")
def dynamic_v3_smoothed_review_attribution_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest attribution。"),
    ] = False,
    attribution_id: Annotated[
        str | None,
        typer.Option("--attribution-id", help="attribution id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed review attribution artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR,
) -> None:
    """展示 TRADING-251 attribution 摘要。"""
    payload = smoothed_evidence.smoothed_review_attribution_report_payload(
        attribution_id=attribution_id,
        latest=latest,
        output_dir=output_dir,
    )
    breakdown = _mapping(payload.get("smoothed_decision_reason_breakdown"))
    typer.echo(f"attribution_id={payload['attribution_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"decision={breakdown.get('decision')}")
    typer.echo(f"confidence={breakdown.get('confidence')}")
    typer.echo(f"recommended_method={breakdown.get('recommended_method')}")
    typer.echo(f"why_not_promote={breakdown.get('why_not_promote')}")
    typer.echo(f"report_path={payload['smoothed_review_attribution_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-review-attribution")
def dynamic_v3_validate_smoothed_review_attribution_command(
    attribution_id: Annotated[str, typer.Option("--attribution-id", help="attribution id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed review attribution artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHED_REVIEW_ATTRIBUTION_DIR,
) -> None:
    """校验 TRADING-251 smoothed review attribution artifact。"""
    payload = smoothed_evidence.validate_smoothed_review_attribution_artifact(
        attribution_id=attribution_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothing_benefit_lag_app.command("run")
def dynamic_v3_smoothing_benefit_lag_run_command(
    smoothed_backfill_id: Annotated[
        str,
        typer.Option("--smoothed-backfill-id", help="smoothed backfill id。"),
    ],
    comparison_id: Annotated[str, typer.Option("--comparison-id", help="comparison id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothing benefit/lag artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
) -> None:
    """运行 TRADING-252 smoothing benefit vs lag drilldown。"""
    result = smoothed_evidence.run_smoothing_benefit_lag_drilldown(
        smoothed_backfill_id=smoothed_backfill_id,
        comparison_id=comparison_id,
        output_dir=output_dir,
    )
    methods = result["benefit_lag_tradeoff_matrix"].get("methods")
    typer.echo(f"drilldown_id={result['drilldown_id']}")
    typer.echo(f"drilldown_dir={result['drilldown_dir']}")
    typer.echo(f"tradeoff_statuses={_status_summary(methods, 'tradeoff_status')}")
    typer.echo("candidate_role_fixed=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothing_benefit_lag_app.command("report")
def dynamic_v3_smoothing_benefit_lag_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest drilldown。"),
    ] = False,
    drilldown_id: Annotated[
        str | None,
        typer.Option("--drilldown-id", help="drilldown id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothing benefit/lag artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
) -> None:
    """展示 TRADING-252 benefit / lag 摘要。"""
    payload = smoothed_evidence.smoothing_benefit_lag_report_payload(
        drilldown_id=drilldown_id,
        latest=latest,
        output_dir=output_dir,
    )
    methods = _mapping(payload.get("benefit_lag_tradeoff_matrix")).get("methods")
    typer.echo(f"drilldown_id={payload['drilldown_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"tradeoff_statuses={_status_summary(methods, 'tradeoff_status')}")
    typer.echo(f"report_path={payload['smoothing_benefit_lag_report_path']}")
    typer.echo("candidate_role_fixed=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothing-benefit-lag")
def dynamic_v3_validate_smoothing_benefit_lag_command(
    drilldown_id: Annotated[str, typer.Option("--drilldown-id", help="drilldown id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothing benefit/lag artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHING_BENEFIT_LAG_DIR,
) -> None:
    """校验 TRADING-252 smoothing benefit / lag artifact。"""
    payload = smoothed_evidence.validate_smoothing_benefit_lag_artifact(
        drilldown_id=drilldown_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_regime_validation_app.command("run")
def dynamic_v3_smoothed_regime_validation_run_command(
    smoothed_backfill_id: Annotated[
        str,
        typer.Option("--smoothed-backfill-id", help="smoothed backfill id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed regime validation artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
) -> None:
    """运行 TRADING-253 smoothed sideways / recovery regime validation。"""
    result = smoothed_evidence.run_smoothed_regime_validation(
        smoothed_backfill_id=smoothed_backfill_id,
        output_dir=output_dir,
    )
    sideways = result["sideways_validation_summary"].get("methods")
    recovery = result["recovery_lag_validation_summary"].get("methods")
    typer.echo(f"regime_validation_id={result['regime_validation_id']}")
    typer.echo(f"regime_validation_dir={result['regime_validation_dir']}")
    typer.echo(f"sideways_statuses={_status_summary(sideways, 'sideways_status')}")
    typer.echo(f"recovery_lag_statuses={_status_summary(recovery, 'lag_status')}")
    typer.echo("candidate_role_fixed=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_regime_validation_app.command("report")
def dynamic_v3_smoothed_regime_validation_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest regime validation。"),
    ] = False,
    regime_validation_id: Annotated[
        str | None,
        typer.Option("--regime-validation-id", help="regime validation id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed regime validation artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
) -> None:
    """展示 TRADING-253 regime validation 摘要。"""
    payload = smoothed_evidence.smoothed_regime_validation_report_payload(
        regime_validation_id=regime_validation_id,
        latest=latest,
        output_dir=output_dir,
    )
    sideways = _mapping(payload.get("sideways_validation_summary")).get("methods")
    recovery = _mapping(payload.get("recovery_lag_validation_summary")).get("methods")
    typer.echo(f"regime_validation_id={payload['regime_validation_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"sideways_statuses={_status_summary(sideways, 'sideways_status')}")
    typer.echo(f"recovery_lag_statuses={_status_summary(recovery, 'lag_status')}")
    typer.echo(f"report_path={payload['smoothed_regime_validation_report_path']}")
    typer.echo("candidate_role_fixed=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-regime-validation")
def dynamic_v3_validate_smoothed_regime_validation_command(
    regime_validation_id: Annotated[
        str,
        typer.Option("--regime-validation-id", help="regime validation id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed regime validation artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHED_REGIME_VALIDATION_DIR,
) -> None:
    """校验 TRADING-253 smoothed regime validation artifact。"""
    payload = smoothed_evidence.validate_smoothed_regime_validation_artifact(
        regime_validation_id=regime_validation_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_confirmation_app.command("register")
def dynamic_v3_smoothed_confirmation_register_command(
    review_id: Annotated[str, typer.Option("--review-id", help="smoothed review id。")],
    regime_validation_id: Annotated[
        str,
        typer.Option("--regime-validation-id", help="regime validation id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed confirmation artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
) -> None:
    """登记 TRADING-254 smoothed forward confirmation targets。"""
    result = smoothed_evidence.register_smoothed_confirmation_targets(
        review_id=review_id,
        regime_validation_id=regime_validation_id,
        output_dir=output_dir,
    )
    targets = result["smoothed_confirmation_targets"]
    typer.echo(f"confirmation_id={result['confirmation_id']}")
    typer.echo(f"confirmation_dir={result['confirmation_dir']}")
    typer.echo(f"evidence_status={targets['status']}")
    typer.echo(f"candidate_method={targets['candidate_method']}")
    typer.echo(
        "registered_targets="
        + ",".join(row["target_id"] for row in targets["targets"])
    )
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_confirmation_app.command("report")
def dynamic_v3_smoothed_confirmation_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest confirmation。"),
    ] = False,
    confirmation_id: Annotated[
        str | None,
        typer.Option("--confirmation-id", help="confirmation id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed confirmation artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
) -> None:
    """展示 TRADING-254 smoothed confirmation targets。"""
    payload = smoothed_evidence.smoothed_confirmation_report_payload(
        confirmation_id=confirmation_id,
        latest=latest,
        output_dir=output_dir,
    )
    targets = _mapping(payload.get("smoothed_confirmation_targets"))
    typer.echo(f"confirmation_id={payload['confirmation_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"evidence_status={targets.get('status')}")
    typer.echo(f"candidate_method={targets.get('candidate_method')}")
    typer.echo(
        "registered_targets="
        + ",".join(str(row.get("target_id")) for row in _records(targets.get("targets")))
    )
    typer.echo(f"report_path={payload['smoothed_confirmation_report_path']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-confirmation")
def dynamic_v3_validate_smoothed_confirmation_command(
    confirmation_id: Annotated[str, typer.Option("--confirmation-id", help="confirmation id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed confirmation artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHED_FORWARD_CONFIRMATION_DIR,
) -> None:
    """校验 TRADING-254 smoothed confirmation artifact。"""
    payload = smoothed_evidence.validate_smoothed_confirmation_artifact(
        confirmation_id=confirmation_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_smoothed_watch_pack_app.command("run")
def dynamic_v3_smoothed_watch_pack_run_command(
    review_attribution_id: Annotated[
        str,
        typer.Option("--review-attribution-id", help="review attribution id。"),
    ],
    benefit_lag_id: Annotated[str, typer.Option("--benefit-lag-id", help="benefit lag id。")],
    regime_validation_id: Annotated[
        str,
        typer.Option("--regime-validation-id", help="regime validation id。"),
    ],
    confirmation_id: Annotated[str, typer.Option("--confirmation-id", help="confirmation id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed watch pack artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHED_WATCH_PACK_DIR,
) -> None:
    """运行 TRADING-255 smoothed operational watch pack。"""
    result = smoothed_evidence.run_smoothed_watch_pack(
        review_attribution_id=review_attribution_id,
        benefit_lag_id=benefit_lag_id,
        regime_validation_id=regime_validation_id,
        confirmation_id=confirmation_id,
        output_dir=output_dir,
    )
    summary = result["smoothed_watch_summary"]
    typer.echo(f"watch_pack_id={result['watch_pack_id']}")
    typer.echo(f"watch_pack_dir={result['watch_pack_dir']}")
    typer.echo(f"candidate_method={summary['candidate_method']}")
    typer.echo(f"current_decision={summary['current_decision']}")
    typer.echo(f"recommended_action={summary['recommended_action']}")
    typer.echo(f"forward_confirmation_status={summary['forward_confirmation_status']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_smoothed_watch_pack_app.command("report")
def dynamic_v3_smoothed_watch_pack_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest watch pack。"),
    ] = False,
    watch_pack_id: Annotated[
        str | None,
        typer.Option("--watch-pack-id", help="watch pack id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed watch pack artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHED_WATCH_PACK_DIR,
) -> None:
    """展示 TRADING-255 smoothed watch pack 摘要。"""
    payload = smoothed_evidence.smoothed_watch_pack_report_payload(
        watch_pack_id=watch_pack_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping(payload.get("smoothed_watch_summary"))
    typer.echo(f"watch_pack_id={payload['watch_pack_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_method={summary.get('candidate_method')}")
    typer.echo(f"current_decision={summary.get('current_decision')}")
    typer.echo(f"recommended_action={summary.get('recommended_action')}")
    typer.echo(f"forward_confirmation_status={summary.get('forward_confirmation_status')}")
    typer.echo(f"report_path={payload['smoothed_watch_pack_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-smoothed-watch-pack")
def dynamic_v3_validate_smoothed_watch_pack_command(
    watch_pack_id: Annotated[str, typer.Option("--watch-pack-id", help="watch pack id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="smoothed watch pack artifact root。"),
    ] = smoothed_evidence.DEFAULT_SMOOTHED_WATCH_PACK_DIR,
) -> None:
    """校验 TRADING-255 smoothed watch pack artifact。"""
    payload = smoothed_evidence.validate_smoothed_watch_pack_artifact(
        watch_pack_id=watch_pack_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
